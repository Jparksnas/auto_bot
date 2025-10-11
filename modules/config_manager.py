# config_manager.py
"""
전역 설정 및 환경변수를 관리하는 중앙 집중식 모듈
"""
import os
from dotenv import load_dotenv
try:
    from modules.config_loader import Config
except ImportError:
    from config import Config

load_dotenv()

class TradingConfig:
    """거래 전략 설정 관리 클래스"""
    
    def __init__(self):
        # ===== API 인증 =====
        self.UPBIT_ACCESS_KEY = os.getenv('UPBIT_ACCESS_KEY')
        self.UPBIT_SECRET_KEY = os.getenv('UPBIT_SECRET_KEY')
        self.TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
        self.TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
        
        # ===== 주문 금액 및 제한 =====
        self.MIN_KRW_FOR_ENTRY = int(os.getenv('MIN_KRW_FOR_ENTRY', '5000'))        
        self.SMALL_BALANCE_CLEANUP_KRW = 5000
        
        # ===== 리스크 프로파일 =====
        self.RISK_LEVEL = str(os.getenv('RISK_LEVEL', 'medium')).lower()
        self._RISK_PRESET = {
            'low': dict(
                max_pos_pct=0.10, max_daily_loss_pct=0.02, stop_loss_pct=0.010, 
                take_profit_pct=0.020, max_open=2, hold_sec=180, 
                cons_loss=3, cons_cooldown_sec=7200
            ),
            'medium': dict(
                max_pos_pct=0.20, max_daily_loss_pct=0.05, stop_loss_pct=0.015, 
                take_profit_pct=0.030, max_open=3, hold_sec=300, 
                cons_loss=4, cons_cooldown_sec=3600
            ),
            'high': dict(
                max_pos_pct=0.30, max_daily_loss_pct=0.08, stop_loss_pct=0.020, 
                take_profit_pct=0.050, max_open=4, hold_sec=600, 
                cons_loss=5, cons_cooldown_sec=1800
            ),
        }
        self.RISK = self._RISK_PRESET.get(self.RISK_LEVEL, self._RISK_PRESET['medium'])
        
        # ===== 포지션 관리 =====
        self.MAX_OPEN_POSITIONS = self.RISK.get('max_open')
        self.MAX_ACTIVE_HOLDINGS = int(os.getenv('MAX_CONCURRENT_POSITIONS', '3'))
        self.MAX_ENTRIES_PER_TICKER = 5
        self.MAX_POSITION_PCT = float(os.getenv('MAX_POSITION_PCT', '0.15'))
        
        # ===== 손익 관리 =====
        self.MAX_DAILY_LOSS_PCT = self.RISK.get('max_daily_loss_pct')
        self.DAILY_LOSS_LIMIT_PCT = float(os.getenv('DAILY_LOSS_LIMIT_PCT', '0.03'))
        self.SL_PCT_DEFAULT = self.RISK.get('stop_loss_pct')
        self.TP_PCT_DEFAULT = self.RISK.get('take_profit_pct')
        self.INITIAL_SL_PCT = 0.0035
        self.PORTFOLIO_RISK_LIMIT = -0.08
        
        # ===== 매수 조건 =====
        self.BUY_ALLOW_WEAKER = os.getenv('BUY_ALLOW_WEAKER', 'false').lower() == 'true'
        self.BUY_BASE_THRESHOLD = float(os.getenv('BUY_BASE_THRESHOLD', '1.0'))
        self.BUY_ATR_REF = float(os.getenv('BUY_ATR_REF', '0.005'))
        self.BUY_ATR_SENS = float(os.getenv('BUY_ATR_SENS', '0.5'))
        self.BUY_NIGHT_RELAX = float(os.getenv('BUY_NIGHT_RELAX', '0.05'))
        
        # ===== Fast Spike 전략 =====
        self.FAST_SPIKE_MULT = float(os.getenv('FAST_SPIKE_MULT', '3.0'))
        self.FAST_SPIKE_STRONG_DELTA = float(os.getenv('FAST_SPIKE_STRONG_DELTA', '-0.2'))
        self.FAST_SPIKE_NORMAL_DELTA = float(os.getenv('FAST_SPIKE_NORMAL_DELTA', '-0.1'))
        self.FAST_SPIKE_WEAK_DELTA = float(os.getenv('FAST_SPIKE_WEAK_DELTA', '0.0'))
        self.FAST_SPIKE_EFF_MIN = float(os.getenv('FAST_SPIKE_EFF_MIN', '1.8'))
        self.FAST_SPIKE_EFF_MAX = float(os.getenv('FAST_SPIKE_EFF_MAX', '4.0'))
        self.MIN_SPIKE_VOL = 10000000
        self.SPIKE_MULT = 3.5
        
        # ===== 슬리피지 및 스프레드 =====
        self.SLIPPAGE_DEFAULT = 0.0005
        self.SLIPPAGE_DEFAULT_QUOTE = 0.001
        self.SLIPPAGE_TOLERANCE_PCT = float(os.getenv('SLIPPAGE_TOLERANCE_PCT', '0.0015'))
        self.SLIPPAGE_GATE_ENABLED = bool(int(os.getenv('SLIPPAGE_GATE_ENABLED', '0')))
        self.MAX_SPREAD_PCT = float(os.getenv('MAX_SPREAD_PCT', '0.005'))
        self.SPREAD_PCT_FLOOR = float(os.getenv('SPREAD_PCT_FLOOR', '0.0003'))
        self.SPREAD_PCT_K_TICK = float(os.getenv('SPREAD_PCT_K_TICK', '2.0'))
        self.SPREAD_GUARD_ENABLED = bool(int(os.getenv('SPREAD_GUARD_ENABLED', '0')))
        
        # ===== 수익률 및 최소 엣지 =====
        self.MIN_EXPECTED_PCT = float(os.getenv('MIN_EXPECTED_PCT', '0.0012'))
        self.MIN_EDGE_BUFFER_PCT = float(os.getenv('MIN_EDGE_BUFFER_PCT', '0.0002'))
        self.MIN_ABS_PROFIT_KRW = int(os.getenv('MIN_ABS_PROFIT_KRW', '20'))
        
        # ===== 유동성 및 거래량 =====
        self.MIN_VOL_1M = int(os.getenv('MIN_VOL_1M', '8000000'))
        self.SR_MIN_1M_KRW = float(os.getenv('SR_MIN_1M_KRW', '1000000'))
        self.VOL15S_MIN_BASE = int(os.getenv('VOL15S_MIN_BASE', '500000'))
        self.VOL15S_MIN_CAP = int(os.getenv('VOL15S_MIN_CAP', '3000000'))
        self.VOL_EMA_PERIOD_MIN = int(os.getenv('VOL_EMA_PERIOD_MIN', '3'))
        self.VOL_EMA_ALPHA = 2 / (self.VOL_EMA_PERIOD_MIN + 1)
        
        # ===== RSI 및 기술적 지표 =====
        self.RSI_PERIOD = int(os.getenv('RSI_PERIOD', '14'))
        self.RSI_ENTRY = float(os.getenv('RSI_ENTRY', '30'))
        self.USE_WILDER_RSI = False
        
        # ===== Surge 전략 =====
        self.SURGE_WIN_MIN = int(os.getenv('SURGE_WIN_MIN', '5'))
        self.SURGE_PCT = float(os.getenv('SURGE_PCT', '0.20'))
        self.SURGE_PARTIAL = float(os.getenv('SURGE_PARTIAL', '0.33'))
        self.SURGE_COOL_SEC = int(os.getenv('SURGE_COOL_SEC', '300'))
        
        # ===== ATR 기반 손절 =====
        self.ATR_SL_K_BASE = float(os.getenv('ATR_SL_K_BASE', '0.90'))
        self.ATR_SL_K_NIGHT = float(os.getenv('ATR_SL_K_NIGHT', '1.10'))
        self.NIGHT_FROM, self.NIGHT_TO = 2, 6
        self.ATR_SL_PER_TICKER = {
            kv.split(':')[0]: float(kv.split(':')[1])
            for kv in os.getenv('ATR_SL_PER_TICKER', '').split(',')
            if kv and ':' in kv
        }
        self.ATR_BASE = 0.005
        self.ATR_N = 10
        self.VOL_MULT = 2.5
        
        # ===== Explode (폭주) 모드 =====
        self.EXPLODE_ARM_PCT = float(os.getenv('EXPLODE_ARM_PCT', '0.008'))
        self.EXPLODE_ARM_SECS = int(os.getenv('EXPLODE_ARM_SECS', '45'))
        self.EXPLODE_TRAIL_PCT = float(os.getenv('EXPLODE_TRAIL_PCT', '0.006'))
        self.EXPLODE_HOLD_UNTIL_BREAK = os.getenv('EXPLODE_HOLD_UNTIL_BREAK', '1') == '1'
        self.EXPLODE_MAX_HOLD_SEC = int(os.getenv('EXPLODE_MAX_HOLD_SEC', '900'))
        
        # ===== 재진입 제한 =====
        self.REENTRY_COOLDOWN_ENABLED = bool(int(os.getenv('REENTRY_COOLDOWN_ENABLED', '0')))
        self.MAX_CONSECUTIVE_LOSSES = int(self.RISK.get('cons_loss', 3))
        self.COOLDOWN_SEC = int(self.RISK.get('cons_cooldown_sec', 3600))
        
        # ===== 주문 실행 설정 =====
        self.USE_LIMIT_PROTECT = bool(Config.get('USE_LIMIT_PROTECT', True))
        self.LIMIT_IOC_TIMEOUT = float(Config.get('LIMIT_IOC_TIMEOUT', 1.2))
        self.BUY_RETRY_LADDER_BPS = [0, 3, 6, 9]
        self.PARTIAL_FILL_WAIT_SEC = 0.30
        self.MARKET_FALLBACK_BPS_CAP = 9
        self.BUY_MARKET_USES_AMOUNT = True
        self.MAX_SLIP_BPS_SELL = int(Config.get('MAX_SLIP_BPS_SELL', 5))
        self.MAX_SLIP_BPS_BUY = int(Config.get('MAX_SLIP_BPS_BUY', 5))
        
        # ===== 유니버스 관리 =====
        self.UNIVERSE_MIN_VOL24H = int(os.getenv('UNIVERSE_MIN_VOL24H', '8000000000'))
        self.UNIVERSE_REEVAL_SEC = int(os.getenv('UNIVERSE_REEVAL_SEC', '14400'))
        self.UNIVERSE_TARGET_COUNT = int(os.getenv('UNIVERSE_TARGET_COUNT', '18'))
        
        # ===== Kelly 사이징 =====
        self.KELLY_ENABLED = os.getenv('KELLY_ENABLED', '1') == '1'
        self.KELLY_SHRINK = float(os.getenv('KELLY_SHRINK', '0.50'))
        self.KELLY_MAX_F = float(os.getenv('KELLY_MAX_F', '0.020'))
        self.KELLY_BLEND = float(os.getenv('KELLY_BLEND', '0.50'))
        self.KELLY_LOOKBACK = int(os.getenv('KELLY_LOOKBACK', '100'))
        
        # ===== 포지션 사이징 개선 =====
        self.RISK_PER_TRADE_PCT = float(os.getenv('RISK_PER_TRADE_PCT', '0.05'))  # 5% (초기값 상향)
        self.INITIAL_POSITION_SIZE_PCT = float(os.getenv('INITIAL_POSITION_SIZE_PCT', '0.08'))  # 8% (신규 추가)
        self.MIN_ORDER_AMOUNT = int(os.getenv('MIN_ORDER_AMOUNT', '5000'))  # ← 5,000원으로 수정
        self.MAX_PER_TRADE = int(os.getenv('MAX_PER_TRADE', '300000'))
        
        # ===== 기타 전략 =====
        self.STRATEGY_MODE = str(Config.get('STRATEGY_MODE', os.getenv('STRATEGY_MODE', 'trend'))).lower()
        self.ALLOW_BTC_SCALP = False
        self.HARD_GATE_BTC_DOWN = bool(int(os.getenv('HARD_GATE_BTC_DOWN', '0')))
        self.USE_CORR_BLOCK = bool(Config.get('USE_CORR_BLOCK', True))
        self.CORRELATION_CAP = float(os.getenv('CORRELATION_CAP', '0.70'))
        
        # ===== 시간 관리 =====
        self.MAX_HOLD_SEC = 60 * 60
        self.ROTATE_MIN_HOLD_SEC = int(os.getenv('ROTATE_MIN_HOLD_SEC', str(30*60)))
        self.ROTATE_SCORE_MARGIN = float(os.getenv('ROTATE_SCORE_MARGIN', '0.07'))
        self.ROTATE_SELL_PCT = float(os.getenv('ROTATE_SELL_PCT', '0.025'))
        self.TIME_EXIT_MODE = 'adaptive'
        self.CLEANUP_INTERVAL_SEC = 15 * 60
        self.BOOT_GRACE_SEC = int(os.getenv('BOOT_GRACE_SEC', '90'))
        
        # ===== 로깅 및 디버그 =====
        self.VERBOSE = bool(int(os.getenv('VERBOSE', '0')))
        self.SIMULATE_TRADES = False
        self.LOG_SAMPLE_RATES = {'trade_decisions': 0.2, 'sell_decisions': 0.35, 'depth_checks': 0.3, 'slippage_checks': 0.35}
        self.DETAIL_LOG_MIN_SEC = 1.0
        self.BUY_LOG_DEBOUNCE_SEC = 0.8
        self.DECISION_SUMMARY_INTERVAL_SEC = int(os.getenv('DECISION_SUMMARY_INTERVAL_SEC', Config.get('DECISION_SUMMARY_INTERVAL_SEC', 120)))
        
        # ===== 네트워크 및 Rate Limiting =====
        self.MAX_RPS = 10
        self.PING_INTERVAL = 15
        self.PING_TIMEOUT = 8
        self.CLOSE_TIMEOUT = 5
        self.ORDERBOOK_404_BASE = float(os.getenv('ORDERBOOK_404_BASE', '1.0'))
        self.ORDERBOOK_404_MAX = float(os.getenv('ORDERBOOK_404_MAX', '60.0'))
        
        # ===== 레짐 및 시장 상태 =====
        self.REGIME = Config.get('REGIME', 'Normal')
        self._reg_mult = {'Calm': 0.8, 'Normal': 1.0, 'Volatile': 1.25}.get(self.REGIME, 1.0)
        
        # ===== 오더북 백오프 =====
        self._ORDERBOOK_BACKOFF_STEPS = [0.25, 0.5, 0.8, 1.2, 2.0, 3.2, 5.0]
        
        # ===== Trend EMA =====
        self.TREND_EMA_FAST = int(os.getenv('TREND_EMA_FAST', '20'))
        self.TREND_EMA_SLOW = int(os.getenv('TREND_EMA_SLOW', '50'))
        self.TREND_EMA_BASE = int(os.getenv('TREND_EMA_BASE', '200'))
        self.TREND_RSI_MIN = int(os.getenv('TREND_RSI_MIN', '52'))
        self.TREND_ATR_STOP_MULT = float(os.getenv('TREND_ATR_STOP_MULT', '2.0'))
        self.TREND_ADD_ON_PCT = float(os.getenv('TREND_ADD_ON_PCT', '0.006'))
        self.TREND_ADD_ON_MAX = int(os.getenv('TREND_ADD_ON_MAX', '2'))
        
        # ===== Trail Stop =====
        self.TRAIL_ARM_PCT = 0.0025
        self.TRAIL_STEP = 0.0025
        
        # ===== 수수료 =====
        self.UPBIT_FEE_MAP = {'KRW': 0.0005, 'BTC': 0.0025, 'USDT': 0.0025}
        
        # ===== AI 필터 =====
        self.AI_MIN_PROB = float(os.getenv('AI_MIN_PROB', '0.55'))
        
        # ===== Alt Gate =====
        self.ALT_GATE = {
            'block_alt_when_btc': True,
            'canary_whitelist': {'KRW-ETH'},
            'allow_strong_momentum_override': True,
            'max_concurrent_whitelist': 1
        }
        
        # ===== 기타 상수 =====
        self.CACHE_MS = 300
        self.BASE_WINDOW = 60
        self.TRADE_WINDOW = 15
        self.FLUSH_THRESHOLD = 2000
        self.PNL_LOSS_LIMIT = -0.005
        self.MAX_DAILY_LOSS_KRW = 3000
        self.CANARY_RATIO = 1.0
        self.MICRO_TP_ADDON = 0.001
        self.EMA20_BUY_MULT = float(os.getenv('EMA20_BUY_MULT', '0.998'))
        self.MIN_STOP_PCT_FLOOR = 0.003
        self.POS_COOLDOWN_AFTER_IOC_SEC = float(Config.get('POS_COOLDOWN_AFTER_IOC_SEC', 1.5))
        self.STALE_HOT_SECS = 120
        self.SYNC_INTERVAL_SEC = 5
        self.USE_MONGODB = os.getenv('USE_MONGODB', 'false').lower() == 'true'
        
        # ===== 유니버스 재평가 ====
        self.DYNAMIC_SCAN_ENABLED = os.getenv('DYNAMIC_SCAN_ENABLED', 'true').lower() == 'true'
        self.DYNAMIC_SCAN_INTERVAL_SEC = int(os.getenv('DYNAMIC_SCAN_INTERVAL_SEC', '30'))
        self.DYNAMIC_SCAN_THRESHOLD_PCT = float(os.getenv('DYNAMIC_SCAN_THRESHOLD_PCT', '0.05'))

        # ===== 환경 검증 =====
        self._validate_required_env()
    
    def get(self, key: str, default=None):
        """
        딕셔너리 스타일 get() 메서드 (하위 호환)
        예: config.get('MAX_OPEN_POSITIONS', 3)
        """
        return getattr(self, key, default)
    
    def __getitem__(self, key: str):
        """
        딕셔너리 스타일 인덱싱 지원
        예: config['MAX_OPEN_POSITIONS']
        """
        if not hasattr(self, key):
            raise KeyError(f"'{key}' not found in config")
        return getattr(self, key)
    
    def __setitem__(self, key: str, value):
        """
        딕셔너리 스타일 할당 지원
        예: config['MAX_OPEN_POSITIONS'] = 5
        """
        setattr(self, key, value)
    
    def __contains__(self, key: str) -> bool:
        """
        'in' 연산자 지원
        예: if 'MAX_OPEN_POSITIONS' in config:
        """
        return hasattr(self, key)
    
    def keys(self):
        """설정 키 목록 반환"""
        return [k for k in dir(self) if not k.startswith('_') and not callable(getattr(self, k))]
    
    def to_dict(self) -> dict:
        """설정을 딕셔너리로 변환"""
        return {k: getattr(self, k) for k in self.keys()}
    
    def _validate_required_env(self):
        """필수 환경변수 검증"""
        required = ['UPBIT_ACCESS_KEY', 'UPBIT_SECRET_KEY', 'TELEGRAM_TOKEN', 'TELEGRAM_CHAT_ID']
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise RuntimeError(f'필수 환경변수 누락: {", ".join(missing)}')
    
    def get_regime_multiplier(self) -> float:
        """레짐 기반 배수 반환"""
        return self._reg_mult
    
    def get_spread_cap(self, ticker: str, is_major: bool = False) -> float:
        """티커별 스프레드 상한"""
        if is_major:
            return float(Config.get('SPREAD_CAP_MAJOR', 0.0010))
        return float(Config.get('SPREAD_CAP_ALT', 0.0015))

# 싱글톤 인스턴스
config = TradingConfig()
