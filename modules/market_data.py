# market_data.py
"""
시장 데이터 조회 및 캐싱 전용 모듈
오더북, 가격, OHLCV, 거래량 조회
"""
import asyncio
import aiohttp
import pyupbit
import random
import pandas as pd
import time as _time
import logging
from typing import Optional, Tuple
from collections import deque, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class MarketData:
    """시장 데이터 조회 및 캐싱"""
    
    def __init__(self, session: aiohttp.ClientSession, config, rate_limiter):
        self.session = session
        self.config = config
        self.rate_limiter = rate_limiter
        self.KST = ZoneInfo('Asia/Seoul')
        
        # 캐시
        self.orderbook_cache = {}  # (ticker, depth) -> (timestamp, data)
        self.price_cache = {}  # ticker -> price
        self.ohlcv_cache = {}  # ticker -> {'ts': float, 'df': DataFrame}
        
        # 백오프 관리
        self.orderbook_fail_at = defaultdict(float)
        self.orderbook_backoff_idx = defaultdict(int)
        self._backoff_steps = [0.25, 0.5, 0.8, 1.2, 2.0, 3.2, 5.0]
        
        # 404 쿨다운
        self.orderbook_404_count = defaultdict(int)
        self.orderbook_cooldown_until = defaultdict(float)
        
        # Rate limiting
        self.orderbook_times = deque(maxlen=64)
        self.orderbook_rate_lock = asyncio.Lock()
        self.orderbook_lock = asyncio.Semaphore(30)
        
        # Metrics
        self.metrics = defaultdict(int)
    
    async def get_price(self, ticker: str) -> Optional[float]:
        """
        안전한 가격 조회 (WS 캐시 → REST 오더북 → 틱가 캐시 → pyupbit)
        """
        # 1) WS 오더북 캐시 (≤0.7초)
        try:
            ts, ob_cached = self.orderbook_cache.get((ticker, 2), (0.0, None))
            if ob_cached and (_time.time() - ts <= 0.7) and ob_cached.get("orderbook_units"):
                b0, a0 = self._best_prices_from_ob(ob_cached)
                return float((b0 + a0) / 2.0)
        except Exception:
            pass
        
        # 2) REST 오더북 폴백
        try:
            ob = await self.get_orderbook(ticker, depth=2)
            if ob and ob.get("orderbook_units"):
                b0, a0 = self._best_prices_from_ob(ob)
                return float((b0 + a0) / 2.0)
        except Exception:
            pass
        
        # 3) 틱가 캐시
        if ticker in self.price_cache:
            return float(self.price_cache[ticker])
        
        # 4) pyupbit 최후 폴백
        try:
            cur = await asyncio.to_thread(pyupbit.get_current_price, ticker)
            return float(cur) if cur else None
        except Exception:
            return None
    
    async def get_orderbook(self, ticker: str, depth: int = 2, retry: int = 2) -> Optional[dict]:
        """
        REST API 오더북 조회 (캐싱 + 백오프 + 404 쿨다운)
        """
        now = _time.time()
        
        # 404 쿨다운 체크
        cool_until = self.orderbook_cooldown_until.get(ticker, 0.0)
        if _time.monotonic() < cool_until:
            self.metrics['orderbook_404_cooldown'] += 1
            return None
        
        # 캐시 체크
        ts, data = self.orderbook_cache.get((ticker, depth), (0, None))
        min_interval = 0.15 + random.random() * 0.08
        
        if now - ts < min_interval and data:
            return data
        
        # 백오프 체크
        if data is None:
            last_fail = self.orderbook_fail_at.get((ticker, depth), 0.0)
            idx = self.orderbook_backoff_idx.get((ticker, depth), 0)
            cooldown = self._backoff_steps[min(idx, len(self._backoff_steps) - 1)]
            
            if now - last_fail < cooldown:
                self.metrics['orderbook_cooldown_skip'] += 1
                return None
        
        # API 호출
        async with self.orderbook_lock:
            url = 'https://api.upbit.com/v1/orderbook'
            params = {'markets': ticker}
            headers = {'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'}
            
            for attempt in range(retry):
                if attempt > 0:
                    await asyncio.sleep(0.05 + random.random() * 0.15)
                
                # Rate limit
                async with self.orderbook_rate_lock:
                    now = _time.time()
                    while len(self.orderbook_times) >= self.config.MAX_RPS and now - self.orderbook_times[0] < 1:
                        await asyncio.sleep(0.05)
                        now = _time.time()
                    self.orderbook_times.append(now)
                
                try:
                    if self.session is None:
                        self.orderbook_fail_at[ticker, depth] = _time.time()
                        self.orderbook_backoff_idx[ticker, depth] = min(
                            self.orderbook_backoff_idx.get((ticker, depth), 0) + 1,
                            len(self._backoff_steps) - 1
                        )
                        break
                    
                    await self.rate_limiter.acquire(1)
                    
                    async with self.session.get(url, params=params, headers=headers) as res:
                        if res.status != 200:
                            if res.status == 429:
                                self.metrics['orderbook_429'] += 1
                                self._mark_fail(ticker, depth)
                                break
                            elif res.status == 404:
                                self.metrics['orderbook_status_404'] += 1
                                self._handle_404(ticker)
                                break
                            elif res.status == 503:
                                self.metrics['orderbook_status_503'] += 1
                                self._mark_fail(ticker, depth)
                                break
                            else:
                                self.metrics[f'orderbook_status_{res.status}'] += 1
                                raise Exception(f'HTTP {res.status}')
                        
                        jd = await res.json()
                        if jd:
                            d = jd[0]
                            d['orderbook_units'] = d['orderbook_units'][:depth]
                            self.orderbook_cache[ticker, depth] = (_time.time(), d)
                            self.orderbook_backoff_idx[ticker, depth] = 0
                            return d
                
                except asyncio.TimeoutError:
                    self.metrics['orderbook_timeout'] += 1
                    self._mark_fail(ticker, depth)
                    await asyncio.sleep(0.1 + 0.1 * random.random())
                except Exception as e:
                    self.metrics['orderbook_err'] += 1
                    self._mark_fail(ticker, depth)
                    await asyncio.sleep(0.1 + 0.1 * random.random())
        
        self.metrics['orderbook_none'] += 1
        return None
    
    async def get_ohlcv(self, ticker: str, interval: str = 'minute1', 
                    count: int = 1, max_retry: int = 4) -> Optional[pd.DataFrame]:
        """
        OHLCV 조회 (캐싱 + 재시도)
        """
        backoff = 0.5
        for attempt in range(1, max_retry + 1):
            try:
                to_ts = datetime.now(self.KST) - timedelta(seconds=random.uniform(3.0, 5.0))
                to_str = to_ts.strftime('%Y-%m-%d %H:%M:%S')
                df = await asyncio.to_thread(pyupbit.get_ohlcv, ticker, interval, count, to=to_str)
                
                if df is not None and (not df.empty):
                    return df
                
                logger.warning(f'[빈응답] {ticker} {interval} → 재시도 {attempt}/{max_retry}')
            except Exception as e:
                logger.warning(f'[일시오류] {ticker} {interval} {type(e).__name__}: {e} → 재시도 {attempt}/{max_retry}')
            
            await asyncio.sleep(backoff)
            backoff *= 1.8
        
        logger.warning(f'[최종실패] {ticker} {interval} → 이번 사이클 제외')
        return None

    async def get_ohlcv_cached(self, ticker: str, interval: str = 'minute1',
                            need: int = 20, ttl: float = 10.0) -> Optional[pd.DataFrame]:
        """
        OHLCV 캐시 조회 (TTL 기반)
        """
        now = _time.time()
        cache_key = f"{ticker}_{interval}"
        c = self.ohlcv_cache.get(cache_key)
        
        if c and now - c['ts'] <= ttl and c['df'] is not None and len(c['df']) >= need:
            return c['df']
        
        try:
            df = await asyncio.to_thread(pyupbit.get_ohlcv, ticker, interval, need)
            if df is not None and not df.empty:
                self.ohlcv_cache[cache_key] = {'ts': now, 'df': df.copy()}
                return df
        except Exception:
            pass
        
        return None
    
    async def check_order_depth(self, ticker: str, amount: float) -> bool:
        """
        주문 심도 충분성 검증
        """
        # WS 캐시 우선
        ob = None
        try:
            ts_ob, ob0 = self.orderbook_cache.get((ticker, 5), (0, None))
            if ob0 and (_time.time() - ts_ob) <= 1.0 and ob0.get('orderbook_units'):
                ob = ob0
        except Exception:
            ob = None
        
        # REST 폴백
        if ob is None:
            ob = await self.get_orderbook(ticker, depth=5)
        
        if not ob:
            return False
        
        units = ob['orderbook_units']
        depth_sum = sum((u['ask_price'] * u['ask_size'] for u in units))
        ratio = depth_sum / max(amount, 1e-9)
        
        # 시간대 하한
        kst_hour = datetime.now(self.KST).hour
        base_min = 3.0
        night_min = 3.5
        min_ratio = night_min if 2 <= kst_hour < 6 else base_min
        
        passed = ratio >= min_ratio
        return passed
    
    def _best_prices_from_ob(self, ob: dict) -> Tuple[float, float]:
        """오더북에서 최우선 호가 추출"""
        u0 = ob['orderbook_units'][0]
        return (float(u0['bid_price']), float(u0['ask_price']))
    
    def _mark_fail(self, ticker: str, depth: int):
        """실패 마킹 및 백오프 증가"""
        self.orderbook_fail_at[ticker, depth] = _time.time()
        self.orderbook_backoff_idx[ticker, depth] = min(
            self.orderbook_backoff_idx.get((ticker, depth), 0) + 1,
            len(self._backoff_steps) - 1
        )
    
    def _handle_404(self, ticker: str):
        """404 응답 처리 (쿨다운 설정)"""
        n = self.orderbook_404_count[ticker] = self.orderbook_404_count.get(ticker, 0) + 1
        backoff = min(
            self.config.ORDERBOOK_404_BASE * (2 ** min(n, 5)),
            self.config.ORDERBOOK_404_MAX
        )
        self.orderbook_cooldown_until[ticker] = _time.monotonic() + backoff
        logger.debug(f"[{ticker}] orderbook 404 → cooldown {backoff:.1f}s (n={n})")
    
    def update_price_cache(self, ticker: str, price: float):
        """틱가 캐시 업데이트 (WS 데이터용)"""
        self.price_cache[ticker] = float(price)