# indicators.py
"""
기술적 지표 계산 전용 모듈
RSI, ATR, EMA, MACD, Bollinger Bands, 스프레드 계산 등
"""
import numpy as np
import pandas as pd
import pyupbit
import asyncio
from typing import Optional, Tuple
from collections import deque
import time as _time

class Indicators:
    """기술적 지표 계산 유틸리티"""
    
    @staticmethod
    def compute_rsi(prices: list, period: int = 14, wilder: bool = False) -> float:
        """RSI 계산 (Wilder/EMA 방식 통합)"""
        if wilder:
            return Indicators._compute_rsi_wilder(prices, period)
        
        if len(prices) < period + 1:
            return 50.0
        
        deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        gains = sum((d for d in deltas[-period:] if d > 0))
        losses = abs(sum((d for d in deltas[-period:] if d < 0)))
        
        if losses == 0:
            return 100.0
        
        rs = gains / losses
        return 100 - 100 / (1 + rs)
    
    @staticmethod
    def _compute_rsi_wilder(prices: list, period: int = 14) -> float:
        """Wilder 방식 RSI"""
        if len(prices) < period + 1:
            return 50.0
        
        deltas = [prices[i + 1] - prices[i] for i in range(len(prices) - 1)]
        gains = [max(d, 0) for d in deltas]
        losses = [abs(min(d, 0)) for d in deltas]
        
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        if avg_loss == 0:
            return 100.0
        
        rs = avg_gain / avg_loss
        return 100 - 100 / (1 + rs)
    
    @staticmethod
    def _calc_rsi_from_series(s: pd.Series, period: int = 14) -> float:
        """pandas Series 기반 RSI"""
        s = s.astype(float)
        delta = s.diff()
        up = np.where(delta > 0, delta, 0.0)
        dn = np.where(delta < 0, -delta, 0.0)
        
        roll_up = pd.Series(up).ewm(alpha=1/period, adjust=False).mean()
        roll_dn = pd.Series(dn).ewm(alpha=1/period, adjust=False).mean()
        
        rs = roll_up / (roll_dn + 1e-12)
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return float(rsi.iloc[-1])
    
    @staticmethod
    def upbit_tick_step(krw_price: float) -> float:
        """업비트 호가 단위"""
        p = float(krw_price or 0.0)
        if p >= 2_000_000: return 1000.0
        if p >= 1_000_000: return 500.0
        if p >= 500_000: return 100.0
        if p >= 100_000: return 50.0
        if p >= 10_000: return 10.0
        if p >= 1_000: return 1.0
        if p >= 100: return 0.1
        return 0.01
    
    @staticmethod
    def allowed_spread_pct_by_price(price: float, k_tick: float = 2.0, 
                                    floor: float = 0.0003, cap: float = 0.005) -> float:
        """가격 기반 허용 스프레드(%)"""
        step = Indicators.upbit_tick_step(price)
        tick_ratio = step / max(float(price) or 1.0, 1e-9)
        dyn = k_tick * tick_ratio
        return min(cap, max(floor, dyn))
    
    @staticmethod
    def dynamic_spread_cap(ticker: str, price: float, 
                          recent_spreads: Optional[deque] = None,
                          regime: str = 'Normal',
                          hour_kst: int = 0,
                          major_tickers: set = None) -> float:
        """
        최근 분포 + 레짐/시간대 반영 동적 스프레드 상한
        """
        base_floor = Indicators.allowed_spread_pct_by_price(price)
        
        # 최근 분포 분위수
        vals = []
        if recent_spreads:
            tail = list(recent_spreads)[-120:]
            for x in tail:
                try:
                    xf = float(x)
                    if xf >= 0.0 and xf == xf:  # NaN 필터
                        vals.append(xf)
                except:
                    continue
        
        # 레짐별 분위수
        perc = 0.85
        if regime.lower() == 'up':
            perc = 0.90
        elif regime.lower() == 'down':
            perc = 0.80
        
        if len(vals) >= 15:
            arr = sorted(vals)
            idx = max(0, min(len(arr) - 1, int(len(arr) * perc) - 1))
            pct_spread = max(arr[idx], base_floor)
        else:
            pct_spread = base_floor * 1.3
        
        # 레짐/시간대 가중
        regime_mult = 1.2 if regime == 'Volatile' else 1.0
        time_mult = 1.15 if (2 <= hour_kst < 6) else 1.0
        cap = pct_spread * regime_mult * time_mult
        
        # 메이저/알트별 캡
        is_major = (major_tickers and ticker in major_tickers)
        cap_fixed = 0.0010 if is_major else 0.0015
        
        cap = max(base_floor, cap)
        cap = min(cap, cap_fixed, 0.005)
        return float(cap)
    
    @staticmethod
    async def get_htf_indicators(ticker: str, tf: str = '5m', bars: int = 120) -> dict:
        """
        상위 타임프레임 지표 (MACD, RSI)
        """
        from config_manager import config  # 순환 import 방지
        
        tf_map = {'5m': 'minute5', '15m': 'minute15', '30m': 'minute30', 
                  '60m': 'minute60', '240m': 'minute240'}
        interval = tf_map.get(tf, 'minute5')
        
        try:
            df = await asyncio.to_thread(pyupbit.get_ohlcv, ticker, interval, bars)
        except:
            return {'macd': None, 'macd_signal': None, 'rsi': None}
        
        if df is None or df.empty or len(df['close']) < 35:
            return {'macd': None, 'macd_signal': None, 'rsi': None}
        
        close = [float(x) for x in df['close'].tolist()]
        
        # EMA 계산
        def ema(seq, span):
            k = 2 / (span + 1)
            v = seq[0]
            for x in seq[1:]:
                v = x * k + v * (1 - k)
            return v
        
        ema12 = ema(close[-90:], 12)
        ema26 = ema(close[-90:], 26)
        macd = ema12 - ema26
        macd_signal = ema([macd] * 26 + [macd], 9)
        
        rsi = Indicators.compute_rsi(close, period=14)
        
        return {
            'macd': float(macd), 
            'macd_signal': float(macd_signal), 
            'rsi': float(rsi)
        }
    
    @staticmethod
    def calc_tp_sl_by_atr(entry: float, atr: float, spread_pct: float, 
                         ticker: str = None, hour_kst: int = 0,
                         atr_sl_k_base: float = 0.90, atr_sl_k_night: float = 1.10,
                         atr_sl_per_ticker: dict = None, initial_sl_pct: float = 0.0035,
                         fee_pct: float = 0.0005) -> Tuple[float, float]:
        """
        ATR 기반 TP/SL 계산
        """
        atr_pct = atr / max(entry, 1e-9)
        
        # 손절폭
        dyn_sl_pct = atr_pct * 2.5
        k = atr_sl_k_base
        
        if ticker and atr_sl_per_ticker and ticker in atr_sl_per_ticker:
            k = atr_sl_per_ticker[ticker]
        
        if 2 <= hour_kst < 6:
            k *= atr_sl_k_night
        
        dyn_sl_pct *= k
        sl_pct_core = max(initial_sl_pct, min(0.0100, dyn_sl_pct))
        sl_pct = sl_pct_core + spread_pct + fee_pct
        
        # 익절폭
        base_tp_pct = max(0.006, min(0.015, atr_pct * 2.0))
        tp = entry * (1 + base_tp_pct)
        sl = entry * (1 - sl_pct)
        
        return (tp, sl)
    
    @staticmethod
    def adjust_for_orderbook(tp: float, sl: float, entry: float, 
                            spread_pct: float, burstiness_val: float) -> Tuple[float, float]:
        """호가 상황 기반 TP/SL 조정"""
        if spread_pct > 0.005 or burstiness_val < 0.4:
            tp = max(tp, entry * 1.015)
            sl = min(sl, entry * 0.991)
        return (tp, sl)
    
    @staticmethod
    def adjust_for_slippage(tp: float, sl: float, slip_deque: deque, entry: float) -> Tuple[float, float]:
        """슬리피지 기반 TP/SL 조정"""
        avg_slip = sum(slip_deque) / len(slip_deque) if slip_deque else 0.0
        if avg_slip <= 0:
            return (tp, sl)
        
        tp_pct = tp / entry - 1.0
        sl_pct = 1.0 - sl / entry
        
        tp_pct += min(avg_slip * 1.5, 0.005)
        sl_pct = max(sl_pct - min(avg_slip * 0.5, 0.003), 0.002)
        
        tp = entry * (1 + tp_pct)
        sl = entry * (1 - sl_pct)
        return (tp, sl)
    
    @staticmethod
    def is_bollinger_breakout(prices: list, window: int = 20) -> bool:
        """볼린저 밴드 돌파 여부"""
        if len(prices) < window:
            return False
        
        mean = sum(prices[-window:]) / window
        std = (sum(((p - mean) ** 2 for p in prices[-window:])) / window) ** 0.5
        upper_band = mean + 2 * std
        return prices[-1] > upper_band
