# -*- coding: utf-8 -*-
import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Tuple
from collections import defaultdict, deque

import pyupbit
import pandas as pd
import numpy as np

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

# --------------------------------------------------------------------
# Defaults (환경변수로 덮어쓰기 가능)
# --------------------------------------------------------------------
DEFAULT_LIQ_PCTL = float(os.getenv("LIQ_PCTL", "0.15"))  # 하위 15퍼센타일로 완화
DEFAULT_LIQ_LOOKBACK_MIN = int(os.getenv("LIQ_LOOKBACK_MIN", "120"))  # 2시간으로 단축
DEFAULT_LIQ_LOOKBACK_15S = int(os.getenv("LIQ_LOOKBACK_15S", "960"))   # 15초 기준 lookback(=4h)

class MarketAnalyzer:
    """시장 분석 및 최적 코인 선택"""

    # ---------------------------------------------------------------
    # 초기화
    # ---------------------------------------------------------------
    def __init__(self):
        # 최소 일일 거래대금/시총 기준
        self.min_volume_krw = 1_000_000_000   # 10억원
        self.min_market_cap = 50_000_000_000  # 500억원 (참고값, 실제 필터엔 미사용)

        # 제외 티커
        self.exclude_coins = [
            'KRW-USDT', 'KRW-USDC', 'KRW-BUSD', 'KRW-DAI',  # 스테이블
            'KRW-WEMIX', 'KRW-BORA',                       # 변동성 과다 가능 (전략에 맞게 조정)
        ]

        # 캐싱
        self.cache: Dict[str, Dict] = {}
        self.cache_time: Dict[str, float] = {}

        # 유동성(15초 체결대금) 히스토리 버퍼
        self.last_15s_turnover = defaultdict(lambda: deque(maxlen=DEFAULT_LIQ_LOOKBACK_15S))
        self.liq_pctl = DEFAULT_LIQ_PCTL
        self.liq_lookback_min = DEFAULT_LIQ_LOOKBACK_MIN

    # ---------------------------------------------------------------
    # 유틸리티
    # ---------------------------------------------------------------
    @staticmethod
    def _nanpercentile(arr: np.ndarray, q: float) -> float:
        if arr.size == 0:
            return 0.0
        return float(np.nanpercentile(arr.astype(float), q * 100.0))

    @staticmethod
    def _bars_per_day_from_index(idx) -> float:
        """OHLC 인덱스 간격으로 일중 바 수 추정 (분해능 자동감지)."""
        if idx is None or len(idx) < 3:
            return 288.0
        # pandas DatetimeIndex 가정
        dt = (idx[-1] - idx[-2]).total_seconds()
        if dt <= 0:
            return 288.0
        minutes = max(1.0, dt / 60.0)
        return max(1.0, 1440.0 / minutes)  # 분당 1바 가정

    @classmethod
    def _annualized_vol(cls, close: pd.Series, tail: int = 50) -> float:
        """분주기 자동감지에 근거한 연환산 변동성(%)"""
        ret = close.pct_change().dropna()
        if len(ret) < tail:
            return np.nan
        bpd = cls._bars_per_day_from_index(close.index)
        vol = ret.tail(tail).std() * np.sqrt(bpd) * 100.0
        return float(vol)

    # ---------------------------------------------------------------
    # 15초 체결대금(유동성) 관련
    # ---------------------------------------------------------------
    def update_15s_turnover(self, coin: str, krw_turnover_15s: float):
        """웹소켓 등 외부에서 계산한 15초 체결대금 값을 주기적으로 push."""
        self.last_15s_turnover[coin].append(float(krw_turnover_15s))

    def prefilter_15s_turnover_min(self, coin: str, vol_prev_15s: float) -> bool:
        """
        퍼센타일 기반 15초 유동성 게이트 (WS 실시간 값 사용).
        - 최근 히스토리가 부족(<60개)이면 과도한 차단 방지를 위해 통과.
        - 하위 liq_pctl 퍼센타일 미만이면 스킵.
        """
        hist = self.last_15s_turnover[coin]
        if len(hist) < 60:
            return True
        eff_pctl = self.liq_pctl
        try:
            lc = globals().get('last_btc_regime', {}) or {}
            if lc.get('trend') == 'up':  # Strong
                eff_pctl = max(0.0, self.liq_pctl - 0.05)
        except Exception:
            pass

        thr = self._nanpercentile(np.array(hist, dtype=float), eff_pctl)
        if vol_prev_15s < thr:
            logger.info(
                f"[{coin}] skip: vol_prev={vol_prev_15s:,.0f} < p{int(eff_pctl*100)}={thr:,.0f} (window=15s)"
            )
            return False
        return True

    @staticmethod
    def _est_15s_turnover_from_m1(df_m1: pd.DataFrame) -> float:
        """
        1분봉 데이터만 있는 경우 15초 체결대금을 보수적으로 근사.
        - 1분 체결대금(close*volume)의 1/4로 추정.
        """
        if df_m1 is None or df_m1.empty or 'close' not in df_m1 or 'volume' not in df_m1:
            return 0.0
        last_min_turnover = float(df_m1['close'].iloc[-1] * df_m1['volume'].iloc[-1])
        return max(0.0, last_min_turnover * 0.25)

    def prefilter_15s_turnover_min_from_m1(self, coin: str, df_m1: pd.DataFrame) -> Tuple[bool, float, float]:
        """
        분봉만으로 퍼센타일 임계 계산 후 15초 추정 체결대금 게이트.
        반환: (통과여부, 현재15초추정, 임계15초)
        """
        if df_m1 is None or len(df_m1) < max(30, self.liq_lookback_min):
            return True, 0.0, 0.0  # 데이터 부족 시 통과

        # 최근 lookback 분의 분봉 체결대금 분포
        min_turnover = (df_m1['close'] * df_m1['volume']).astype(float)
        look = min_turnover.tail(self.liq_lookback_min)
        thr_15s = self._nanpercentile(look.to_numpy(), self.liq_pctl) * 0.25
        cur_15s = self._est_15s_turnover_from_m1(df_m1)

        ok = cur_15s >= thr_15s
        if not ok:
            logger.info(f"[{coin}] skip: est15s={cur_15s:,.0f} < p{int(self.liq_pctl*100)}={thr_15s:,.0f} (window=15s, pctile)")
        return ok, float(cur_15s), float(thr_15s)

    # ---------------------------------------------------------------
    # 티커/리스트업
    # ---------------------------------------------------------------
    def get_all_tickers(self) -> List[str]:
        """모든 KRW 마켓 코인 목록(제외리스트 반영)."""
        try:
            tickers = pyupbit.get_tickers(fiat="KRW")
            tickers = [t for t in tickers if t not in self.exclude_coins]
            print(f"Total {len(tickers)} KRW pairs found")
            return tickers
        except Exception as e:
            print(f"Error getting tickers: {e}")
            return []

    # ---------------------------------------------------------------
    # 코인별 분석
    # ---------------------------------------------------------------
    def analyze_coin(self, ticker: str) -> Dict:
        """
        개별 코인 분석:
        - 일봉(30) + 1분봉(288) 사용
        - 연환산 변동성은 분해능 자동감지로 보정
        """
        try:
            # 5분 캐싱
            if ticker in self.cache and time.time() - self.cache_time[ticker] < 300:
                return self.cache[ticker]

            df_day = pyupbit.get_ohlcv(ticker, interval="day", count=30)
            if df_day is None or len(df_day) < 7:
                return None

            # 1분봉(주석 수정: 기존 '5분봉' 표기 혼선)
            df_m1 = pyupbit.get_ohlcv(ticker, interval="minute1", count=288)
            if df_m1 is None or len(df_m1) < 100:
                return None

            current_price = float(df_day['close'].iloc[-1])

            # 거래대금 분석
            volume_krw_1d = float(df_day['volume'].iloc[-1] * current_price)
            volume_krw_7d_avg = float((df_day['volume'][-7:] * df_day['close'][-7:]).mean())
            volume_krw_30d_avg = float((df_day['volume'] * df_day['close']).mean())

            # 변동성 (분해능 자동 보정)
            volatility_intraday = self._annualized_vol(df_m1['close'], tail=50)   # 분봉 기반
            volatility_7d = (df_day['close'][-7:].pct_change().std() * np.sqrt(252) * 100.0)

            # 추세
            returns_1d = (df_day['close'].iloc[-1] / df_day['close'].iloc[-2] - 1.0) * 100.0
            returns_7d = (df_day['close'].iloc[-1] / df_day['close'].iloc[-8] - 1.0) * 100.0
            returns_30d = (df_day['close'].iloc[-1] / df_day['close'].iloc[0] - 1.0) * 100.0

            # 기술적 평균
            ma7 = float(df_day['close'][-7:].mean())
            ma30 = float(df_day['close'].mean())

            # 스프레드/활동성
            spread = (df_m1['high'] - df_m1['low']) / df_m1['close']
            avg_spread = float(spread.replace([np.inf, -np.inf], np.nan).dropna().mean() * 100.0)

            price_changes = df_m1['close'].diff() != 0
            activity_ratio = float((price_changes.sum() / len(price_changes)) * 100.0)

            result = {
                'ticker': ticker,
                'price': current_price,
                'volume_krw_1d': volume_krw_1d,
                'volume_krw_7d_avg': volume_krw_7d_avg,
                'volume_krw_30d_avg': volume_krw_30d_avg,
                'volatility_1d': volatility_intraday,  # 이름 유지(내부는 intraday 보정)
                'volatility_7d': float(volatility_7d),
                'returns_1d': float(returns_1d),
                'returns_7d': float(returns_7d),
                'returns_30d': float(returns_30d),
                'ma7': ma7,
                'ma30': ma30,
                'trend_score': 1 if current_price > ma7 > ma30 else -1,
                'avg_spread': avg_spread,
                'activity_ratio': activity_ratio,
                'score': 0.0  # 종합점수는 별도 함수에서 계산
            }

            # 캐시
            self.cache[ticker] = result
            self.cache_time[ticker] = time.time()
            return result

        except Exception as e:
            logger.exception(f"Error analyzing {ticker}: {e}")
            return None

    # ---------------------------------------------------------------
    # 스코어링
    # ---------------------------------------------------------------
    def calculate_scores(self, coins: List[Dict]) -> List[Dict]:
        """각 코인에 종합 점수 부여"""
        if not coins:
            return []

        df = pd.DataFrame(coins)

        # 각 지표별 순위
        df['volume_rank'] = df['volume_krw_1d'].rank(pct=True)
        df['liquidity_rank'] = df['volume_krw_7d_avg'].rank(pct=True)
        df['activity_rank'] = df['activity_ratio'].rank(pct=True)

        # 적정 변동성: 목표 5%p 부근(전략에 맞게 조정 가능)
        target_vol = 5.0
        df['volatility_optimal'] = 1 - (df['volatility_1d'] - target_vol).abs() / (2 * target_vol)
        df['volatility_optimal'] = df['volatility_optimal'].clip(0, 1)

        # 스프레드는 낮을수록 우수
        df['spread_rank'] = 1 - df['avg_spread'].rank(pct=True)

        # 추세 점수
        df['trend_rank'] = (df['returns_7d'] > 0).astype(int) * 0.5 + (df['trend_score'] > 0).astype(int) * 0.5

        # 종합 점수(가중치)
        df['score'] = (
            df['volume_rank'] * 0.30 +        # 거래량 30%
            df['liquidity_rank'] * 0.20 +     # 유동성 20%
            df['activity_rank'] * 0.15 +      # 활동성 15%
            df['volatility_optimal'] * 0.15 + # 변동성 15%
            df['spread_rank'] * 0.10 +        # 스프레드 10%
            df['trend_rank'] * 0.10           # 추세 10%
        ) * 100.0

        df = df.sort_values('score', ascending=False)
        return df.to_dict('records')

    # ---------------------------------------------------------------
    # 상위 코인 선택
    # ---------------------------------------------------------------
    def get_top_coins(self, top_n: int = 10, min_volume: float = None) -> List[Tuple[str, Dict]]:
        """거래량/활동성/변동성/추세 기반 상위 코인 선택."""
        print(f"\n{'='*60}")
        print(f"Market Analysis - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")

        if min_volume is None:
            min_volume = self.min_volume_krw

        all_tickers = self.get_all_tickers()

        analyzed_coins: List[Dict] = []
        print(f"\nAnalyzing {len(all_tickers)} coins...")

        for i, ticker in enumerate(all_tickers):
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(all_tickers)}")

            analysis = self.analyze_coin(ticker)
            if analysis and analysis['volume_krw_1d'] >= min_volume:
                analyzed_coins.append(analysis)

            time.sleep(0.1)  # API rate limit 보호

        print(f"\nFound {len(analyzed_coins)} coins with volume > {min_volume/1e9:.1f}B KRW")

        scored = self.calculate_scores(analyzed_coins)
        top_coins = scored[:top_n]

        print(f"\n📊 Top {top_n} Coins by Score:")
        print(f"{'Rank':<5} {'Ticker':<12} {'Price':<12} {'Volume(B)':<10} {'1D%':<8} {'Score':<8}")
        print("-" * 60)
        for i, coin in enumerate(top_coins, 1):
            print(f"{i:<5} {coin['ticker']:<12} {coin['price']:>11,.0f} "
                  f"{coin['volume_krw_1d']/1e9:>9.1f} {coin['returns_1d']:>7.2f}% "
                  f"{coin['score']:>7.1f}")

        return [(coin['ticker'], coin) for coin in top_coins]

    # ---------------------------------------------------------------
    # 잔고 기반 추천(기존 로직 유지, 문구만 약간 정리)
    # ---------------------------------------------------------------
    def get_trading_recommendations(self, balance: float) -> Dict:
        """잔고에 따른 거래 추천."""
        if balance is None or balance <= 0:
            balance = 1_000_000  # 기본 100만원

        if balance < 100_000:
            min_volume = 1_000_000_000  # 10억원
            max_coins = 1
        elif balance < 1_000_000:
            min_volume = 5_000_000_000  # 50억원
            max_coins = 3
        else:
            min_volume = 10_000_000_000  # 100억원
            max_coins = 5

        top_coins = self.get_top_coins(top_n=max_coins*2, min_volume=min_volume)

        rec = {'aggressive': [], 'balanced': [], 'conservative': []}
        for ticker, analysis in top_coins:
            if analysis['volatility_1d'] > 7:
                rec['aggressive'].append(ticker)
            elif analysis['volatility_1d'] < 3:
                rec['conservative'].append(ticker)
            else:
                rec['balanced'].append(ticker)

        for k in rec:
            rec[k] = rec[k][:max_coins]

        print(f"\n💡 Trading Recommendations (Balance: {balance:,.0f} KRW):")
        print(f"  Aggressive: {', '.join(rec['aggressive'])}")
        print(f"  Balanced: {', '.join(rec['balanced'])}")
        print(f"  Conservative: {', '.join(rec['conservative'])}")

        return rec


# --------------------------------------------------------------------
# 독립 실행
# --------------------------------------------------------------------
def update_auto_trader():
    """auto_trader.py를 업데이트하여 MarketAnalyzer 사용 (샘플)."""
    print("\n🔄 Updating auto_trader.py to use MarketAnalyzer...")

    analyzer = MarketAnalyzer()
    top_coins = analyzer.get_top_coins(top_n=10)
    recommendations = analyzer.get_trading_recommendations(balance=1_000_000)

    print(f"\n✅ Analysis Complete!")
    print(f"Recommended coins for auto trading:")

    # 균형잡힌 포트폴리오 예시 구성
    selected = []
    if recommendations['balanced']:
        selected.extend(recommendations['balanced'][:2])
    if recommendations['aggressive']:
        selected.extend(recommendations['aggressive'][:1])
    if len(selected) < 3 and recommendations['conservative']:
        selected.extend(recommendations['conservative'][:1])

    print(f"\nFinal Selection: {', '.join(selected)}")
    return selected


if __name__ == "__main__":
    analyzer = MarketAnalyzer()
    top_coins = analyzer.get_top_coins(top_n=10, min_volume=5_000_000_000)

    print("\n" + "="*60)
    recommendations = analyzer.get_trading_recommendations(balance=1_000_000)
    print("="*60)
