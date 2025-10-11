# position_manager.py
"""
포지션 관리 전용 모듈
TP/SL 판단, 트레일링 스톱, 다단계 익절, Explode 모드, 부분청산
"""
import asyncio
import logging
import time as _time
from typing import Optional, Tuple
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class PositionManager:
    """포지션 생명주기 관리"""
    
    def __init__(self, config, market_data, indicators):
        self.config = config
        self.market_data = market_data
        self.indicators = indicators
        self.KST = ZoneInfo('Asia/Seoul')
        
        # 포지션 상태
        self.hold = {}  # ticker -> position_info
        self.hold_since = {}  # ticker -> datetime
        
        # Explode 모드 (급등 추종)
        self.explode_mode = defaultdict(bool)
        self.explode_high = defaultdict(lambda: None)
        self.explode_since = defaultdict(lambda: None)
        
        # 트레일링 스톱
        self.trail_armed = defaultdict(bool)
        self.trail_anchor = defaultdict(float)
        self.trail_stop = defaultdict(lambda: None)
        
        # 부분청산 추적
        self.tp1_done = defaultdict(bool)
        self.surge_cool_until = defaultdict(float)
        
        # 통계
        self.metrics = defaultdict(int)
    
    async def should_sell(self, ticker: str, price: float, hold_info: dict) -> Tuple[str, str]:
        """
        매도 판단 (TP/SL/시간/Explode)
        반환: (action, reason)
        - action: 'sell', 'partial', 'hold', 'adjust_sl'
        - reason: 상세 사유
        """
        entry = float(hold_info.get('entry', 0) or hold_info.get('avgpx', 0))
        if entry <= 0:
            return ('hold', 'invalid_entry')
        
        qty = float(hold_info.get('qty', 0))
        buy_ts = float(hold_info.get('buy_ts', 0))
        now = _time.time()
        hold_sec = now - buy_ts
        
        # 1. Explode 모드 체크
        if self.explode_mode[ticker]:
            action = await self._check_explode_exit(ticker, price, entry, hold_sec)
            if action:
                return action
        
        # 2. 손절
        sl = float(hold_info.get('sl', entry * (1 - self.config.SL_PCT_DEFAULT)))
        if price <= sl:
            return ('sell', f'손절 (SL={sl:,.0f})')
        
        # 3. 익절
        tp = float(hold_info.get('tp', entry * (1 + self.config.TP_PCT_DEFAULT)))
        if price >= tp:
            return ('sell', f'익절 (TP={tp:,.0f})')
        
        # 4. 다단계 익절 판단
        profit_pct = (price / entry - 1) * 100.0
        position_value = qty * price
        
        decision = self.staged_profit_decision(entry, price, position_value)
        if decision['action'] == 'partial':
            return ('partial', decision['reason'])
        elif decision['action'] == 'adjust_sl':
            # SL 상향 (실제 조정은 bot에서)
            return ('hold', decision['reason'])
        
        # 5. Surge (급등 후 부분청산)
        if await self._check_surge_partial(ticker, entry, price):
            return ('partial', 'surge_partial_33pct')
        
        # 6. 시간 기반 청산
        max_hold = self.config.MAX_HOLD_SEC
        if hold_sec > max_hold:
            if profit_pct >= 0:
                return ('sell', f'시간청산 ({hold_sec/60:.0f}분, +{profit_pct:.1f}%)')
            else:
                return ('sell', f'시간손절 ({hold_sec/60:.0f}분, {profit_pct:.1f}%)')
        
        # 7. 홀드
        return ('hold', f'보유중 ({profit_pct:+.2f}%)')
    
    async def _check_explode_exit(self, ticker: str, price: float, 
                                  entry: float, hold_sec: float) -> Optional[Tuple[str, str]]:
        """
        Explode 모드 출구 조건
        - 고점 대비 EXPLODE_TRAIL_PCT 하락 시 익절
        - EXPLODE_MAX_HOLD_SEC 초과 시 강제 청산
        """
        high = self.explode_high.get(ticker) or price
        
        if price > high:
            self.explode_high[ticker] = price
            high = price
        
        drawdown_from_high = 1 - (price / high)
        
        if drawdown_from_high >= self.config.EXPLODE_TRAIL_PCT:
            profit = (price / entry - 1) * 100.0
            return ('sell', f'폭주익절 (고점-{drawdown_from_high:.2%}, +{profit:.1f}%)')
        
        if hold_sec > self.config.EXPLODE_MAX_HOLD_SEC:
            return ('sell', f'폭주시간초과 ({hold_sec/60:.0f}분)')
        
        return None
    
    def staged_profit_decision(self, entry_price: float, cur_price: float, 
                              position_value: float) -> dict:
        """
        다단계 이익실현 로직
        - 8%: 30% 부분익절 또는 SL 상향(본전+4%)
        - 10%: 50% 부분익절 또는 트레일(-8%)
        - 12%: 30% 부분익절 또는 트레일 강화(-10%)
        - 15%: 트레일 강화(-10%)
        - 20%+: 트레일(-12%)
        """
        MIN_PARTIAL_VALUE = 20000
        profit_pct = (cur_price / max(entry_price, 1e-9) - 1) * 100.0
        
        trail_8 = cur_price * 0.92
        trail_10 = cur_price * 0.90
        trail_12 = cur_price * 0.88
        
        if profit_pct >= 20:
            return {
                "action": "adjust_sl",
                "new_sl": max(entry_price * 1.10, trail_12),
                "reason": "20%+ 러너 트레일(-12%)"
            }
        
        if profit_pct >= 15:
            return {
                "action": "adjust_sl",
                "new_sl": max(entry_price * 1.06, trail_10),
                "reason": "15% 달성 - 트레일 강화"
            }
        
        if profit_pct >= 12:
            if position_value >= MIN_PARTIAL_VALUE * 2:
                return {"action": "partial", "ratio": 0.3, "reason": "12% 달성 - 30% 매도"}
            return {
                "action": "adjust_sl",
                "new_sl": max(entry_price * 1.05, trail_10),
                "reason": "12% 달성 - 트레일"
            }
        
        if profit_pct >= 10:
            if position_value >= MIN_PARTIAL_VALUE:
                return {"action": "partial", "ratio": 0.5, "reason": "10% 달성 - 50% 매도"}
            return {
                "action": "adjust_sl",
                "new_sl": max(entry_price * 1.04, trail_8),
                "reason": "10% 달성 - 트레일"
            }
        
        if profit_pct >= 8:
            if position_value >= MIN_PARTIAL_VALUE:
                return {"action": "partial", "ratio": 0.3, "reason": "8% 달성 - 30% 매도"}
            return {
                "action": "adjust_sl",
                "new_sl": entry_price * 1.04,
                "reason": "8% 달성 - 손절 상향"
            }
        
        if profit_pct >= 5:
            if position_value >= MIN_PARTIAL_VALUE * 2:
                return {"action": "partial", "ratio": 0.2, "reason": "5% 달성 - 20% 매도"}
            return {
                "action": "adjust_sl",
                "new_sl": entry_price * 1.03,
                "reason": "5% 달성 - 손절 상향(소액)"
            }
        
        if profit_pct >= 3:
            return {
                "action": "adjust_sl",
                "new_sl": entry_price * 1.02,
                "reason": "3% 달성 - 손절 상향"
            }
        
        if profit_pct >= 1.5:
            return {
                "action": "adjust_sl",
                "new_sl": entry_price * 1.005,
                "reason": "1.5% 달성 - 손절 본전"
            }
        
        return {"action": "hold"}
    
    async def _check_surge_partial(self, ticker: str, entry: float, price: float) -> bool:
        """
        Surge 부분청산 조건
        - 최근 N분 수익률 > SURGE_PCT
        - 쿨다운 체크
        """
        now = _time.time()
        
        if now < self.surge_cool_until[ticker]:
            return False
        
        try:
            df = await self.market_data.get_ohlcv_cached(ticker, 'minute1', need=self.config.SURGE_WIN_MIN + 1)
            if df is None or len(df) < self.config.SURGE_WIN_MIN:
                return False
            
            p0 = float(df['close'].iloc[-(self.config.SURGE_WIN_MIN + 1)])
            p1 = float(df['close'].iloc[-1])
            
            surge_pct = (p1 / p0 - 1)
            
            if surge_pct >= self.config.SURGE_PCT:
                self.surge_cool_until[ticker] = now + self.config.SURGE_COOL_SEC
                return True
        except Exception:
            pass
        
        return False
    
    def activate_explode_mode(self, ticker: str, entry: float):
        """폭주 모드 활성화"""
        self.explode_mode[ticker] = True
        self.explode_high[ticker] = entry
        self.explode_since[ticker] = _time.time()
        logger.info(f"🔥 [{ticker}] 폭주모드 활성화 (entry={entry:,.0f})")
    
    def deactivate_explode_mode(self, ticker: str):
        """폭주 모드 비활성화"""
        self.explode_mode[ticker] = False
        self.explode_high[ticker] = None
        self.explode_since[ticker] = None
    
    def can_add_position(self, ticker: str, current_qty: float) -> bool:
        """추가 매수 가능 여부"""
        max_entries = self.config.MAX_ENTRIES_PER_TICKER  # 예: 3
        entry_count = self.hold.get(ticker, {}).get('entry_count', 1)
        return entry_count < max_entries
    
    def update_position(self, ticker: str, qty: float, avgpx: float, 
                       tp: Optional[float] = None, sl: Optional[float] = None):
        """포지션 정보 업데이트"""
        now = _time.time()
        
        self.hold[ticker] = {
            'position_id': f'{ticker}-{int(now)}',
            'entry': avgpx,
            'avgpx': avgpx,
            'avg': avgpx,
            'qty': round(qty, 8),
            'value': round(qty * avgpx, 2),
            'tp': tp or avgpx * (1 + self.config.TP_PCT_DEFAULT),
            'sl': sl or avgpx * (1 - self.config.SL_PCT_DEFAULT),
            'ts': now,
            'buy_ts': now,
            'source': 'order_fill'
        }
        
        if ticker not in self.hold_since:
            self.hold_since[ticker] = datetime.now(self.KST)
