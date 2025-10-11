# risk_control.py
"""
리스크 관리 전용 모듈
Kelly 사이징, 일일손실 추적, 연속손실 쿨다운, 재진입 제한
"""
import asyncio
import logging
import time as _time
from typing import Optional
from collections import defaultdict, deque
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class RiskControl:
    """리스크 관리 및 포지션 사이징"""
    
    def __init__(self, config):
        self.config = config
        self.KST = ZoneInfo('Asia/Seoul')
        
        # 일일 손실 추적
        self.daily_loss_krw = 0.0
        self.daily_loss_reset_at = None
        self.initial_balance = 0.0
        
        # 연속손실 추적
        self.consecutive_losses = defaultdict(int)
        self.cooldown_until = defaultdict(float)
        self.last_trade_result = defaultdict(lambda: None)  # 'win', 'loss', None
        
        # Kelly 크라이터리온 데이터
        self.trade_history = deque(maxlen=self.config.KELLY_LOOKBACK)
        self.win_rate = 0.5
        self.avg_win_ratio = 1.5
        
        # 포트폴리오 리스크
        self.total_exposure = 0.0
        self.position_count = 0
        
        # 통계
        self.metrics = defaultdict(int)
    
    def can_trade(self, ticker: str) -> bool:
        """
        거래 가능 여부 체크
        - 연속손실 쿨다운
        - 일일손실 한도
        - 포지션 수 제한
        """
        now = _time.time()
        
        # 1. 연속손실 쿨다운
        if now < self.cooldown_until[ticker]:
            self.metrics['cooldown_block'] += 1
            remain = int(self.cooldown_until[ticker] - now)
            logger.debug(f"[{ticker}] 연속손실 쿨다운 중 (남은시간: {remain}s)")
            return False
        
        # 2. 일일손실 한도
        if self._check_daily_loss_limit():
            self.metrics['daily_loss_limit'] += 1
            logger.warning(f"일일손실 한도 도달: {self.daily_loss_krw:,.0f}원")
            return False
        
        # 3. 포지션 수 제한
        if self.position_count >= self.config.MAX_OPEN_POSITIONS:
            self.metrics['max_positions'] += 1
            return False
        
        # 4. 포트폴리오 리스크 한도
        if self.total_exposure >= abs(self.config.PORTFOLIO_RISK_LIMIT):
            self.metrics['portfolio_risk_limit'] += 1
            return False
        
        return True
    
    def record_trade_result(self, ticker: str, pnl_pct: float, pnl_krw: float):
        """
        거래 결과 기록 및 연속손실 추적
        """
        now = _time.time()
        is_win = pnl_pct > 0
        
        # 거래 히스토리
        self.trade_history.append({
            'ticker': ticker,
            'pnl_pct': pnl_pct,
            'pnl_krw': pnl_krw,
            'is_win': is_win,
            'timestamp': now
        })
        
        # 일일손실 누적
        if pnl_krw < 0:
            self.daily_loss_krw += abs(pnl_krw)
        
        # 연속손실 추적
        if is_win:
            self.consecutive_losses[ticker] = 0
            self.last_trade_result[ticker] = 'win'
        else:
            self.consecutive_losses[ticker] += 1
            self.last_trade_result[ticker] = 'loss'
            
            # 연속손실 쿨다운 발동
            if self.consecutive_losses[ticker] >= self.config.MAX_CONSECUTIVE_LOSSES:
                cooldown_sec = self.config.COOLDOWN_SEC
                self.cooldown_until[ticker] = now + cooldown_sec
                logger.warning(
                    f"[{ticker}] 연속손실 {self.consecutive_losses[ticker]}회 → "
                    f"{cooldown_sec/60:.0f}분 쿨다운"
                )
        
        # Kelly 갱신
        self._update_kelly_stats()
    
    def kelly_position_size(self, capital: float, volatility: float = 0.02) -> float:
        """Kelly Criterion 기반 포지션 사이징"""
        if not self.config.KELLY_ENABLED:
            return capital * self.config.RISK_PER_TRADE_PCT
        
        if len(self.trade_history) < 10:
            # 초기 데이터 부족 시 보수적이지만 합리적인 크기
            initial_size_pct = max(self.config.RISK_PER_TRADE_PCT * 3, 0.05)  # 최소 5%
            initial_size = capital * initial_size_pct
            
            # 최소/최대 제한
            initial_size = max(initial_size, self.config.MIN_ORDER_AMOUNT * 2)  # 최소 10,000원
            initial_size = min(initial_size, capital * self.config.MAX_POSITION_PCT)
            
            logger.debug(f"Kelly 초기모드: {initial_size:,.0f}원 ({initial_size_pct:.1%})")
            return initial_size
        
        p = self.win_rate
        q = 1 - p
        b = self.avg_win_ratio
        
        # Kelly 비율
        kelly_f = (p * b - q) / max(b, 0.1)
        
        # 축소 적용 (Half-Kelly 등)
        kelly_f *= self.config.KELLY_SHRINK
        
        # 상한 적용
        kelly_f = min(kelly_f, self.config.KELLY_MAX_F)
        
        # Blend with fixed risk
        fixed_f = self.config.RISK_PER_TRADE_PCT
        blended_f = kelly_f * self.config.KELLY_BLEND + fixed_f * (1 - self.config.KELLY_BLEND)
        
        # 변동성 조정
        volatility_adj = max(0.5, min(1.5, 0.02 / max(volatility, 0.01)))
        final_f = blended_f * volatility_adj
        
        position_size = capital * final_f
        
        logger.debug(
            f"Kelly 사이징: p={p:.2f}, b={b:.2f}, kelly_f={kelly_f:.3f}, "
            f"final_f={final_f:.3f}, size={position_size:,.0f}원"
        )
        
        return position_size
    
    def _update_kelly_stats(self):
        """Kelly 통계 갱신 (승률, 손익비)"""
        if len(self.trade_history) < 5:
            return
        
        wins = [t for t in self.trade_history if t['is_win']]
        losses = [t for t in self.trade_history if not t['is_win']]
        
        self.win_rate = len(wins) / len(self.trade_history)
        
        if wins and losses:
            avg_win = sum((abs(t['pnl_pct']) for t in wins)) / len(wins)
            avg_loss = sum((abs(t['pnl_pct']) for t in losses)) / len(losses)
            self.avg_win_ratio = avg_win / max(avg_loss, 0.001)
        else:
            self.avg_win_ratio = 1.5  # 기본값
    
    def _check_daily_loss_limit(self) -> bool:
        """일일손실 한도 체크 (자정 리셋)"""
        now_kst = datetime.now(self.KST)
        
        # 자정 리셋
        if self.daily_loss_reset_at is None or now_kst.date() > self.daily_loss_reset_at.date():
            self.daily_loss_krw = 0.0
            self.daily_loss_reset_at = now_kst
            return False
        
        # 한도 체크
        if self.initial_balance > 0:
            loss_pct = self.daily_loss_krw / self.initial_balance
            if loss_pct >= self.config.MAX_DAILY_LOSS_PCT:
                return True
        
        # 절대값 한도
        if self.daily_loss_krw >= self.config.MAX_DAILY_LOSS_KRW * 1000:
            return True
        
        return False
    
    def update_exposure(self, position_value: float, is_add: bool = True):
        """포트폴리오 익스포저 업데이트"""
        if is_add:
            self.total_exposure += position_value
            self.position_count += 1
        else:
            self.total_exposure = max(0, self.total_exposure - position_value)
            self.position_count = max(0, self.position_count - 1)
    
    def set_initial_balance(self, balance: float):
        """초기 잔고 설정 (일일손실 계산용)"""
        if self.initial_balance == 0:
            self.initial_balance = balance
    
    def get_max_position_size(self, capital: float) -> float:
        """최대 포지션 크기"""
        return capital * self.config.MAX_POSITION_PCT
    
    def reset_cooldown(self, ticker: str):
        """쿨다운 초기화 (수동 리셋용)"""
        self.cooldown_until[ticker] = 0.0
        self.consecutive_losses[ticker] = 0
    
    def is_daily_loss_limit_reached(self) -> bool:
        """일일 손실 한도 도달 여부"""
        return self.daily_loss_krw >= (self.initial_balance * self.config.DAILY_LOSS_LIMIT_PCT)
    
    def get_daily_loss(self) -> float:
        """현재 일일 손실 금액 반환"""
        return self.daily_loss_krw
