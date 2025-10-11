# main.py (리팩토링 완료본 - 약 800~1000줄)
"""
Upbit 자동매매 봇 (모듈화 버전)
메인 실행 스크립트 - 오케스트레이션 레이어
"""
# ============================================================
# 1. 표준 라이브러리 Import
# ============================================================
import asyncio
import ssl
import time as _time
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional
import certifi
import json
import sys
import os
from pathlib import Path
from threading import Timer
import sqlite3 

# ============================================================
# SQLite 로그 핸들러 (추가)
# ============================================================
class SQLiteHandler(logging.Handler):
    """SQLite 데이터베이스에 로그를 저장하는 핸들러"""
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                level TEXT,
                message TEXT
            )
        ''')
        self.conn.commit()

    def emit(self, record):
        try:
            log_entry = self.format(record)
            self.cursor.execute(
                "INSERT INTO logs (timestamp, level, message) VALUES (?, ?, ?)",
                (record.created, record.levelname, log_entry)
            )
            self.conn.commit()
        except Exception:
            self.handleError(record)

    def close(self):
        self.conn.close()
        super().close()

# ============================================================
# 0. 로그 폴더 생성 및 콘솔 출력 설정
# ============================================================
# 로그 폴더 생성
LOG_DIR = Path("log")
LOG_DIR.mkdir(exist_ok=True)

class Tee:
    """콘솔과 파일에 동시 출력하는 클래스"""
    def __init__(self, *files):
        self.files = files
    
    def write(self, data):
        for f in self.files:
            try:
                f.write(data)
                f.flush()
            except Exception:
                pass
    
    def flush(self):
        for f in self.files:
            try:
                f.flush()
            except Exception:
                pass

# 콘솔 출력 파일 경로 (log 폴더 내)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
console_log_path = LOG_DIR / f"console_{timestamp}.txt"
console_file = open(console_log_path, 'w', encoding='utf-8', buffering=1)

# 원래 stdout 백업
_original_stdout = sys.stdout
_original_stderr = sys.stderr

# 콘솔과 파일에 동시 출력
sys.stdout = Tee(_original_stdout, console_file)
sys.stderr = Tee(_original_stderr, console_file)

print(f"[시작] 콘솔 출력이 {console_log_path}에도 저장됩니다.")

# ============================================================
# 2. 외부 라이브러리 Import
# ============================================================
import pyupbit
import websockets
import aiohttp

# ============================================================
# 3. 내부 모듈 Import (새로 분할된 모듈들)
# ============================================================
from modules.config_manager import config
from modules.indicators import Indicators as ind
from modules.market_data import MarketData
from modules.order_manager import OrderManager
from modules.position_manager import PositionManager
from modules.risk_control import RiskControl
from utils.utils import Utils

# ============================================================
# 4. 기존 프로젝트 모듈 Import
# ============================================================
from modules.message_bot import Message_Bot
from modules.mongodb_connect import MongodbConnect
from modules.market_analyzer import MarketAnalyzer
from modules.strategy_engine import decide_order, RiskManager
try:
    from modules.ai_filter import AISignal, extract_features
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    AISignal = None

DEBUG_ON_DURATION = 120  # 오류 발생 시 DEBUG 모드 활성화 지속 시간(초)
debug_timer = [None]     # 리스트로 감싸서 nonlocal로 사용

# ============================================================
# 5. 로깅 설정
# ============================================================
def setup_logging():
    """로깅 설정 (콘솔 최소화 + 파일 상세 + SQLite DB)"""
    log_format = '%(asctime)s [%(levelname)s] %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # ===== 콘솔: WARNING 이상만 =====
    console_handler = logging.StreamHandler(_original_stdout)
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter('%(levelname)s | %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # ===== 파일1: INFO 이상 (일별 로테이션) =====
    file_handler = TimedRotatingFileHandler(
        LOG_DIR / 'trading_bot.log',
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # ===== 파일2: 핵심 이벤트만 (매수/매도 실행, TP/SL) =====
    today = datetime.now().strftime('%Y%m%d')
    critical_log_path = LOG_DIR / f"critical_trades_{today}.txt"
    critical_handler = logging.FileHandler(critical_log_path, encoding='utf-8')
    critical_handler.setLevel(logging.WARNING)  # WARNING 이상만
    critical_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # ===== SQLite DB: 전체 로그 (INFO 이상 모두 저장) =====
    db_path = LOG_DIR / f"trading_log_{today}.db"
    sqlite_handler = SQLiteHandler(db_path)
    sqlite_handler.setLevel(logging.INFO)
    sqlite_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # ===== 디버그 모드 활성화 함수 =====
    DEBUG_ON_DURATION = 120
    debug_timer = [None]
    
    def activate_debug_mode():
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        for h in logger.handlers:
            if isinstance(h, (logging.FileHandler, SQLiteHandler)):
                h.setLevel(logging.DEBUG)
        print(f"[ALERT] DEBUG 모드 활성화 (INFO→DEBUG)")
        
        def revert_level():
            logger.setLevel(logging.INFO)
            for h in logger.handlers:
                if isinstance(h, (logging.FileHandler, SQLiteHandler)):
                    h.setLevel(logging.INFO)
            print(f"[ALERT] DEBUG 모드 해제 (DEBUG→INFO)")
        
        if debug_timer[0]:
            debug_timer[0].cancel()
        debug_timer[0] = Timer(DEBUG_ON_DURATION, revert_level)
        debug_timer[0].start()
    
    # ===== 로거 설정 =====
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.handlers = []
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(critical_handler)
    logger.addHandler(sqlite_handler)  # SQLite 핸들러 추가
    
    logger.activate_debug_mode = activate_debug_mode
    
    # ===== 매매 판단 전용 로거 추가 =====
    trade_logger = logging.getLogger('trade_decision')
    trade_logger.setLevel(logging.INFO)
    trade_logger.propagate = False  # 메인 로거로 전파 방지
    
    # CSV 파일 핸들러 (매일 로테이션)
    today = datetime.now().strftime("%Y%m%d")
    trade_log_path = LOG_DIR / f'trading_decision_{today}.csv'
    trade_handler = logging.FileHandler(trade_log_path, encoding='utf-8')
    trade_handler.setFormatter(logging.Formatter('%(asctime)s,%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    trade_logger.addHandler(trade_handler)
    
    logger.info(f"📊 매매 판단 로그: {trade_log_path}")
    
    return logger

logger = setup_logging()
logger.info("=" * 60)
logger.info("자동매매 봇 초기화 완료")
logger.info(f"전략 모드: {config.STRATEGY_MODE}")
logger.info(f"리스크 레벨: {config.RISK_LEVEL}")
logger.info("=" * 60)

# ===== 매매 판단 로거 선언 =====
trade_logger = logging.getLogger('trade_decision')

# CSV 헤더 작성 (첫 실행 시)
today = datetime.now().strftime("%Y%m%d")
trade_log_path = LOG_DIR / f'trading_decision_{today}.csv'
if not trade_log_path.exists() or trade_log_path.stat().st_size == 0:
    with open(trade_log_path, 'w', encoding='utf-8') as f:
        f.write("timestamp,action,ticker,price,qty,value,reason,strategy,rsi,macd,signal,bb_upper,bb_mid,bb_lower,volume_ratio,pnl_pct,pnl_krw,entry,tp,sl\n")

# ============================================================
# 6. Rate Limiter 클래스
# ============================================================
class RateLimiter:
    """API Rate Limiting"""
    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.call_times = deque()
        self.lock = asyncio.Lock()
    
    async def acquire(self, n: int = 1):
        async with self.lock:
            now = _time.time()
            
            # 오래된 호출 제거
            while self.call_times and now - self.call_times[0] > self.period:
                self.call_times.popleft()
            
            # Rate limit 대기
            if len(self.call_times) + n > self.max_calls:
                wait = self.period - (now - self.call_times[0])
                if wait > 0:
                    await asyncio.sleep(wait)
            
            # 기록
            for _ in range(n):
                self.call_times.append(_time.time())

# ============================================================
# 7. 메인 봇 클래스
# ============================================================
class TradingBot:
    """자동매매 봇 메인 클래스"""
    
    def __init__(self):
        self.KST = ZoneInfo('Asia/Seoul')
        self.config = config  # config_manager의 config 인스턴스
        self.start_time = datetime.now(self.KST)
        
        # API 클라이언트
        self.upbit = pyupbit.Upbit(config.UPBIT_ACCESS_KEY, config.UPBIT_SECRET_KEY)
        
        # HTTP 세션
        self.http_session = None
        self.http_rate = RateLimiter(max_calls=config.MAX_RPS, period=1.0)
        
        # 메신저
        self.messenger = Message_Bot(config.TELEGRAM_TOKEN, config.TELEGRAM_CHAT_ID)
        
        # 데이터베이스
        self.db = None
        if getattr(config, 'USE_MONGODB', False):  # ← 이전 수정사항
            try:
                self.db = MongodbConnect()
            except Exception as e:
                logger.warning(f"MongoDB 연결 실패: {e}")
        
        # 모듈 초기화 (분할된 모듈들)
        self.market_data = None
        self.order_mgr = None
        self.pos_mgr = None
        self.risk_ctrl = None
        
        # 기존 모듈
        self.market_analyzer = MarketAnalyzer()
        self.risk_manager = RiskManager()  # ← 수정: 인자 제거
        
        # AI 필터
        self.ai_signal = AISignal() if AI_AVAILABLE else None
        
        # 상태 관리
        self.universe = []  # 거래 유니버스
        self.hold = {}  # 보유 포지션
        self.balance = 0.0
        self.ws_connected = False
        self.last_sync_ts = 0.0
        
        # WebSocket 캐시
        self.ws_price_cache = {}
        self.ws_orderbook_cache = {}
        
        # 메트릭
        self.metrics = defaultdict(int)
        
        # 작업 큐
        self.signal_queue = asyncio.Queue(maxsize=100)
        
        self.ws_reconnect_needed = False
        self.error_counter = defaultdict(int)    # 예: orderbook 오류 등 반복 메시지 카운터
        self.last_error_log_time = defaultdict(float)  # 마지막 요약 로그 시점
        self.errorcounter = defaultdict(int)  # 반복 오류 카운터
        self.lasterrorlogtime = defaultdict(float)  # 마지막 요약 로그 시점
        self.last_signal_log_time = defaultdict(float)
        self.running = True
        self.initial_balance = 0
        self.last_daily_loss_warning = 0  # 추가
        self.selling_in_progress = set()
                
        logger.info("=" * 60)
        logger.info("자동매매 봇 초기화 완료")
        logger.info(f"전략 모드: {config.STRATEGY_MODE}")
        logger.info(f"리스크 레벨: {config.RISK_LEVEL}")
        logger.info("=" * 60)
    
    async def initialize(self):
        """비동기 초기화"""
        # HTTP 세션
        self.http_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=30, limit_per_host=15),
            timeout=aiohttp.ClientTimeout(total=8)
        )
        
        # 모듈 초기화
        self.market_data = MarketData(self.http_session, config, self.http_rate)
        self.risk_ctrl = RiskControl(config)
        self.pos_mgr = PositionManager(config, self.market_data, ind)
        self.order_mgr = OrderManager(
            self.upbit, config, self.messenger, 
            self.market_data, self.risk_ctrl
        )
        
        # main_instance 전달
        self.order_mgr.main_instance = self  # ← 추가!

        # 초기 잔고 동기화
        await self.sync_balances()
        
        # 유니버스 구성
        await self.build_universe()
        
        logger.info(f"✅ 초기화 완료 | 잔고: {Utils.fmt_krw(self.balance)}원 | 유니버스: {len(self.universe)}개")
    
    async def run(self):
        """메인 실행 루프"""
        try:
            await self.initialize()
            
            # 초기 잔고 조회
            krw_balance = await asyncio.to_thread(self.upbit.get_balance, "KRW")
            self.initial_balance = krw_balance  # ← 이 줄 추가
            logger.info(f"초기 잔고: {self.initial_balance:,.0f}원")
            
            # 병렬 작업 시작
            tasks = [
                asyncio.create_task(self.websocket_handler(), name='websocket'),
                asyncio.create_task(self.signal_processor(), name='signal_processor'),
                asyncio.create_task(self.position_monitor(), name='position_monitor'),
                asyncio.create_task(self.emergency_stop_loss_checker(), name='emergency_sl'),  # 추가
                asyncio.create_task(self.periodic_sync(), name='periodic_sync'),
                asyncio.create_task(self.metrics_reporter(), name='metrics'),
                asyncio.create_task(self.dynamic_scanner(), name='dynamic_scanner'), 
            ]
            await asyncio.gather(*tasks)
        
        except asyncio.CancelledError:
            logger.warning("작업 취소됨")
        except Exception as e:
            logger.error(f"실행 루프 오류: {e}", exc_info=True)
        finally:
            await self.cleanup()  # 정리 작업 호출
    
    async def dynamic_scanner(self):
        """전체 종목 주기적 스캔"""
        while True:
            try:
                await asyncio.sleep(30)
                
                all_tickers = pyupbit.get_tickers(fiat="KRW")
                
                for ticker in all_tickers:
                    if ticker in self.universe:
                        continue
                    try:
                        df = await asyncio.to_thread(
                            pyupbit.get_ohlcv, ticker, interval='minute1', count=5
                        )
                        self.errorcounter[ticker] = 0  # 정상 응답 시 카운터 초기화(선택)
                    except Exception:
                        self.errorcounter[ticker] += 1
                        now = _time.time()
                        if self.errorcounter[ticker] % 10 == 0:
                            logger.warning(f"[{ticker}] 최근 10회 연속 orderbook 404 오류 발생 (마지막: {datetime.fromtimestamp(now)})")
                            self.lasterrorlogtime[ticker] = now
                        continue
                    if df is None or len(df) < 5:
                        continue
                    change = (df['close'].iloc[-1] / df['close'].iloc[0] - 1)
                    if change >= 0.05: # 5분간 5% 급등
                        logger.info(f"🆕 [{ticker}] 급등 감지: {change:.2%}")
                        self.universe.append(ticker)
                        self.ws_reconnect_needed = True
            
            except Exception as e:
                logger.error(f"동적 스캔 오류: {e}")
    
    async def websocket_handler(self):
        """WebSocket 연결 및 데이터 수신"""
        ws_url = "wss://api.upbit.com/websocket/v1"
        
        while True:
            try:
                # 유니버스가 비어있으면 대기
                if not self.universe:
                    logger.warning("유니버스가 비어있어 WebSocket 연결 보류 (10초 대기)")
                    await asyncio.sleep(10)
                    continue
                
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                
                async with websockets.connect(
                    ws_url,
                    ssl=ssl_context,
                    ping_interval=config.PING_INTERVAL,
                    ping_timeout=config.PING_TIMEOUT,
                    close_timeout=config.CLOSE_TIMEOUT
                ) as ws:
                    self.ws_connected = True
                    logger.info(f"📡 WebSocket 연결 성공 (유니버스: {len(self.universe)}개)")
                    
                    # 구독 메시지
                    subscribe_msg = [
                        {"ticket": "upbit-bot"},
                        {"type": "ticker", "codes": self.universe, "isOnlyRealtime": True},
                        {"type": "orderbook", "codes": self.universe, "isOnlyRealtime": True}
                    ]
                    await ws.send(json.dumps(subscribe_msg))
                    
                    # 데이터 수신 루프
                    while True:
                        data = await ws.recv()
                        await self._process_ws_data(data)
                        self.metrics['ws_received'] += 1
            
            except asyncio.CancelledError:
                logger.info("WebSocket 핸들러 종료")
                break
            except Exception as e:
                self.ws_connected = False
                logger.error(f"WebSocket 오류: {e}")
                await asyncio.sleep(5)
    
    async def _process_ws_data(self, data: bytes):
        """WebSocket 데이터 처리"""
        try:
            msg = json.loads(data)
            msg_type = msg.get('type')
            ticker = msg.get('code')
            
            if msg_type == 'ticker':
                price = float(msg.get('trade_price', 0))
                self.ws_price_cache[ticker] = price
                self.market_data.update_price_cache(ticker, price)
                
                # ===== 디버그 로그 제거 =====
                # change_rate = float(msg.get('signed_change_rate', 0))
                # if abs(change_rate) >= 0.01:
                #     logger.info(f"📈 [{ticker}] 변동: {change_rate:+.2%} | 가격: {price:,.0f}원")
                
                # ===== 보유 종목은 무조건 큐에 추가 =====
                if ticker in self.hold:
                    await self.signal_queue.put(('ticker', ticker, msg))
                else:
                    # 신호 후보 체크
                    is_candidate = await self._is_signal_candidate(ticker, msg)
                    if is_candidate:
                        await self.signal_queue.put(('ticker', ticker, msg))
            
            elif msg_type == 'orderbook':
                self.ws_orderbook_cache[(ticker, 2)] = (_time.time(), msg)
              
        except Exception as e:
            Utils.throttled_log('ws_process_error', f"WS 처리 오류: {e}", interval=10)
            

    
    async def _is_signal_candidate(self, ticker: str, msg: dict) -> bool:
        """신호 후보 필터링 (개선)"""
        try:
            change_rate = float(msg.get('signed_change_rate', 0))
            acc_volume = float(msg.get('acc_trade_volume_24h', 0))
            
            # 조건 1: 급등/급락 (1.5% → 완화)
            if abs(change_rate) >= 0.015:
                logger.debug(f"[{ticker}] 조건1 통과: 변동률 {change_rate:+.2%}")
                return True
            
            # 조건 2: 거래량 급증 (50억 이상)
            if acc_volume >= 5_000_000_000:
                logger.debug(f"[{ticker}] 조건2 통과: 거래량 {acc_volume:,.0f}원")
                return True
            
            # 조건 3: 5분 단위 변동 체크 (추가)
            change_5m = float(msg.get('change_rate', 0))  # 5분 변동률
            if abs(change_5m) >= 0.01:
                logger.debug(f"[{ticker}] 조건3 통과: 5분 변동 {change_5m:+.2%}")
                return True
        
        except Exception as e:
            logger.debug(f"[{ticker}] 신호 체크 오류: {e}")
        
        return False
    
    async def signal_processor(self):
        """신호 처리 워커"""
        logger.info("🔄 신호 처리 워커 시작")
        
        while True:
            try:
                msg_type, ticker, data = await self.signal_queue.get() 
                
                # 매수 가능 여부 체크
                if ticker not in self.hold:
                    if not self.risk_ctrl.can_trade(ticker):
                        continue 
                    
                    should_buy = await self._evaluate_buy_signal(ticker, data)
                    if should_buy:
                        logger.info(f"[매수신호] {ticker} | 신호 판단 근거: {data}")
                        logger.info(f"[매수진입] {ticker} | 데이터: {data}")
                        await self._execute_buy(ticker, data)
                    else:
                        now = _time.time()
                        if ticker not in self.last_signal_log_time or (now - self.last_signal_log_time[ticker] > 60):
                            logger.info(f"[신호무시] {ticker} | 매수 조건 미충족")
                            self.last_signal_log_time[ticker] = now
                
                # 보유 중이면 매도 판단
                else:
                    await self._evaluate_sell_signal(ticker)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"신호 처리 오류: {e}", exc_info=True)
            finally:
                self.signal_queue.task_done()
    
    async def _evaluate_buy_signal(self, ticker: str, data: dict) -> bool:
        """매수 신호 평가 (개선)"""
        try:
            price = float(data.get('trade_price', 0))
            change_rate = float(data.get('signed_change_rate', 0))
            
            logger.debug(f"[{ticker}] 평가: 가격={price:,.0f}, 변동={change_rate:+.2%}")
            
            # 1. Fast Spike 체크 (조건 완화)
            spike_threshold = config.FAST_SPIKE_MULT * 0.01  # 기본 3%
            if change_rate >= spike_threshold * 0.5:  # 절반으로 완화 (1.5%)
                logger.info(f"🚀 [{ticker}] Fast Spike 감지: {change_rate:.2%}")
                return True
            
            # 2. 전략 엔진 평가 (간소화)
            try:
                decision = await decide_order(ticker, price, self.market_data, config)
                if decision and decision.get('action') == 'buy':
                    logger.info(f"✅ [{ticker}] 전략 엔진 매수 신호")
                    return True
            except Exception as e:
                logger.debug(f"[{ticker}] 전략 엔진 오류: {e}")
            
            # 3. 기본 조건 (추가): 1% 이상 + 거래량
            if abs(change_rate) >= 0.01:
                acc_vol = float(data.get('acc_trade_volume_24h', 0))
                if acc_vol >= 3_000_000_000:  # 30억 이상
                    logger.info(f"📊 [{ticker}] 기본 조건 충족: {change_rate:+.2%}, 거래량 {acc_vol/1e9:.1f}B")
                    return True
        
        except Exception as e:
            logger.error(f"[{ticker}] 매수 평가 오류: {e}", exc_info=True)
        
        return False
    
    async def _execute_buy(self, ticker: str, data: dict):
        """매수 실행 (체결 정보 저장 개선)"""
        try:
            # ===== 1. 포지션 크기 계산 =====
            position_size = self.risk_ctrl.kelly_position_size(self.balance, volatility=0.02)
            
            # ===== 2. 매수 사유 정의 =====
            reasons = ['fast_spike']
            
            # 최대 포지션 제한
            max_position = self.risk_ctrl.get_max_position_size(self.balance)
            position_size = min(position_size, max_position)
            
            # 최소 금액 적용 (기본 5,000원 → 더 큰 값 사용)
            min_order = max(config.MIN_ORDER_AMOUNT, self.balance * 0.05)  # 잔고의 5%
            position_size = max(position_size, min_order)
            
            # 설정된 최대 거래금액 제한
            position_size = min(position_size, config.MAX_PER_TRADE)
            
            # ===== 3. 매수 로깅 =====
            logger.warning(f"💰 [{ticker}] 매수 시도: {position_size:,.0f}원 / 잔고: {self.balance:,.0f}원")
            
            # ===== 매매 판단 로그 (매수 시도) =====
            try:
                current_price = float(data.get('trade_price', 0))
                trade_logger.info(
                    f"BUY_ATTEMPT,{ticker},{current_price:.2f},0,{position_size:.0f},"
                    f"{','.join(reasons)},FAST_SPIKE,0,0,0,0,0"
                )
            except Exception as e:
                logger.debug(f"[{ticker}] 매매 판단 로그 실패: {e}")
    
            # ===== 4. 매수 실행 =====
            result = await self.order_mgr.buy(
                ticker,
                position_size,
                reasons=reasons,
                strategy='fast_spike'
            )
            
            if not result:
                logger.error(f"❌ [{ticker}] 매수 실패: result가 None")
                return
            
            # ===== 5. 체결 정보 추출 (중요!) =====
            filled_qty = float(result.get('executed_volume', 0))
            avg_price = float(result.get('avg_price', 0))
            
            # ===== 6. avg_price 검증 =====
            if avg_price == 0:
                logger.error(f"❌ [{ticker}] 체결가가 0, 매수 실패로 간주")
                return
            
            if filled_qty == 0:
                logger.error(f"❌ [{ticker}] 체결 수량이 0, 매수 실패로 간주")
                return
            
            # ===== 7. TP/SL 계산 =====
            atr = 0.02 * avg_price  # 간략화 (실제로는 indicator에서 계산)
            spread_pct = 0.001
            tp, sl = ind.calc_tp_sl_by_atr(avg_price, atr, spread_pct, ticker)
            
            # ===== 8. hold 딕셔너리에 저장 (중요!) =====
            position_value = filled_qty * avg_price
            
            self.hold[ticker] = {
                'position_id': f"{ticker}-{int(_time.time())}",
                'entry': avg_price,      # ← 체결가 저장!
                'avgpx': avg_price,
                'avg': avg_price,
                'qty': filled_qty,
                'value': position_value,
                'tp': tp,
                'sl': sl,
                'ts': _time.time(),
                'buy_ts': _time.time(),
                'source': 'order_fill'
            }
            
            # ===== 9. position_manager 동기화 =====
            self.pos_mgr.update_position(ticker, filled_qty, avg_price, tp, sl)
            
            # ===== 10. 리스크 노출 업데이트 =====
            self.risk_ctrl.update_exposure(position_value, is_add=True)
            
            # ===== 11. 로깅 =====
            logger.info(f"[매수체결] {ticker} | 수량: {filled_qty:.8f} | 체결가: {avg_price:,.0f} | 총액: {position_value:,.0f}")
            logger.info(f"[포지션저장] {ticker} | TP: {tp:,.0f} | SL: {sl:,.0f} | entry: {avg_price:,.0f}")
            
            # ===== 12. 텔레그램 알림 (형식 개선) =====
            reason_str = ', '.join(reasons) if isinstance(reasons, list) else str(reasons)

            # 종목명 추출 (KRW-BTC -> BTC)
            coin_name = ticker.replace('KRW-', '')

            msg = f"""💰 매수 : {coin_name}
            매수금액 : {position_value:,}원
            사유 : {reason_str}"""

            if hasattr(self, 'messenger') and self.messenger:
                try:
                    await asyncio.to_thread(self.messenger.send_message, msg)
                except Exception as e:
                    logger.warning(f"텔레그램 메시지 전송 실패: {e}")
            
        except Exception as e:
            logger.error(f"[{ticker}] 매수 실행 오류: {e}", exc_info=True)
    
    async def _evaluate_sell_signal(self, ticker: str):
        """매도 시그널 평가"""
        try:
            if ticker not in self.hold:
                return
            
            hold_info = self.hold[ticker]
            
            # 현재가 조회 (market_data 사용)
            current_price = await self.market_data.get_price(ticker)
            if not current_price:
                # WebSocket 캐시에서 가져오기
                current_price = self.ws_price_cache.get(ticker)
            
            if not current_price:
                return
            
            entry = float(hold_info.get('entry', 0))
            
            # 하드 손실 리미트 (-15%)
            if entry > 0:
                loss_pct = (current_price / entry - 1) * 100.0 
                
                if loss_pct <= -15.0:
                    logger.warning(f"🚨 [{ticker}] 하드 손실 리미트 도달: {loss_pct:.2f}%")
                    try:
                        await self._execute_sell(ticker, current_price, f'하드손절(-15%)')  # ← 수정
                        if ticker in self.hold:
                            logger.warning(f"✅ [{ticker}] 하드 손절 완료, 포지션 제거")
                    except Exception as sell_error:
                        logger.error(f"[{ticker}] 하드 손절 실행 실패: {sell_error}", exc_info=True)
                    return
                        
            try:
                # ===== 타임아웃 추가 (5초) =====
                action, reason = await asyncio.wait_for(
                    self.pos_mgr.should_sell(ticker, current_price, hold_info),
                    timeout=5.0
                )
                logger.warning(f"[DEBUG-SELL] {ticker} should_sell 완료: action={action}, reason={reason}")
            except asyncio.TimeoutError:
                logger.error(f"[DEBUG-SELL] {ticker} should_sell 타임아웃 (5초 초과)")
                return
            except Exception as e:
                logger.error(f"[DEBUG-SELL] {ticker} should_sell 오류: {e}", exc_info=True)
                return
            
            # 손익률 계산
            pnl_pct = (current_price / entry - 1) * 100.0 if entry > 0 else 0
            
            # ===== 매매 판단 로그 (매도 시그널) =====
            trade_logger.info(
                f"SELL_SIGNAL,{ticker},{current_price:.2f},0,0,{action},"
                f"{reason},0,0,0,0,0,0,0,{pnl_pct:+.2f},0,{entry:.2f},"
                f"{hold_info.get('tp', 0):.2f},{hold_info.get('sl', 0):.2f}"
            )
        
            logger.info(f"[매도신호] {ticker} | 가격: {current_price:.0f} | action: {action} | 사유: {reason}")
            
            if action == 'sell':
                logger.warning(f"[매도진입] {ticker} | 가격: {current_price:.0f} | 사유: {reason}")
                await self._execute_sell(ticker, current_price, reason)
            elif action == 'partial':
                logger.warning(f"[부분청산] {ticker} | 가격: {current_price:.0f} | 사유: {reason}")
                await self._execute_sell(ticker, current_price, reason, partial=True)
                
        except Exception as e:
            logger.error(f"[{ticker}] 매도 시그널 평가 오류: {e}", exc_info=True)
    
    async def _execute_sell(self, ticker: str, price: float, reason: str, partial: bool = False):
        """매도 실행 (체결 정보 검증 강화)"""
        try:
            # ===== 1. 중복 매도 방지 =====
            if ticker in self.selling_in_progress:
                logger.warning(f"[{ticker}] 이미 매도 진행 중, 스킵")
                return None
            
            self.selling_in_progress.add(ticker)  # 매도 시작
            
            try:
                # ===== 2. hold 존재 확인 =====
                if ticker not in self.hold:
                    logger.warning(f"[{ticker}] hold에 없음, 매도 중단")
                    return None
                
                # ===== 3. 진입 정보 저장 (손익 계산용) =====
                hold_info = self.hold[ticker]
                entry_price = float(hold_info.get('entry', 0))
                position_value = float(hold_info.get('value', 0))
                
                if entry_price == 0:
                    logger.error(f"❌ [{ticker}] entry_price가 0, 손익 계산 불가")
                    # 그래도 매도는 진행
                
                # ===== 4. 매도 실행 =====
                qty_ratio = 0.5 if partial else None
                logger.warning(f"[매도시도] {ticker} | 가격: {price:,.0f} | 사유: {reason} | 부분청산: {partial}")
                
                result = await self.order_mgr.sell(
                    ticker, 
                    price, 
                    reason=reason, 
                    qty_ratio=qty_ratio,
                    final_stage=False
                )
                
                if not result:
                    # ✅ 긴급 손절은 정상 동작이므로 에러 레벨 낮춤
                    if '긴급손절' in reason:
                        logger.debug(f"[{ticker}] 긴급 손절 완료 (사유: {reason})")
                    else:
                        logger.error(f"❌ [{ticker}] 매도 실패: order_mgr.sell이 None 반환 (사유: {reason})")
                    return None
                
                # ===== 5. 체결 정보 추출 =====
                filled_qty = float(result.get('executed_volume', 0))
                avg_price = float(result.get('avg_price', 0))
                paid_fee = float(result.get('paid_fee', 0))
                
                # ===== 6. 검증 =====
                if avg_price == 0 or filled_qty == 0:
                    logger.error(f"❌ [{ticker}] 매도 체결가/수량이 0 (체결가: {avg_price}, 수량: {filled_qty})")
                    return None
                
                # ===== 7. 손익 계산 =====
                total_value = filled_qty * avg_price
                
                if entry_price > 0:
                    pnl_pct = (avg_price / entry_price - 1) * 100.0
                    pnl_krw = filled_qty * (avg_price - entry_price) - paid_fee
                else:
                    pnl_pct = 0.0
                    pnl_krw = 0.0
                    logger.warning(f"[{ticker}] entry_price=0, 손익 계산 불가")
                
                # ===== 8. 거래 결과 기록 =====
                self.risk_ctrl.record_trade_result(ticker, pnl_pct, pnl_krw)
                
                # ===== 9. 로깅 =====
                logger.warning(f"[매도체결] {ticker} | 수량: {filled_qty:.8f} | 체결가: {avg_price:,.0f} | 총액: {total_value:,.0f} | 손익: {pnl_pct:+.2f}% ({pnl_krw:+,.0f}원)")
                
                # ===== 매매 판단 로그 (매수 체결) =====
                trade_logger.info(
                    f"SELL_FILLED,{ticker},{avg_price:.2f},{filled_qty:.8f},"
                    f"{total_value:.0f},{reason},0,{pnl_pct:+.2f},{pnl_krw:+.0f},"
                    f"{entry_price:.2f},0,0"
                )

                # ===== 10. 텔레그램 알림 (형식 개선) =====
                emoji = "💰" if pnl_krw >= 0 else "📉"

                # 종목명 추출 (KRW-BTC -> BTC)
                coin_name = ticker.replace('KRW-', '')

                msg = f"""{emoji} 매도 : {coin_name}
                매도금액 : {total_value:,}원
                매도손익 : {pnl_krw:+,.0f}원 ({pnl_pct:+.2f}%)
                사유 : {reason}"""

                if hasattr(self, 'messenger') and self.messenger:
                    try:
                        await asyncio.to_thread(self.messenger.send_message, msg)
                    except Exception as e:
                        logger.warning(f"텔레그램 메시지 전송 실패: {e}")
                
                # ===== 11. 포지션 정리 (전량 청산 시) =====
                if not partial:
                    # 리스크 노출 업데이트
                    self.risk_ctrl.update_exposure(position_value, is_add=False)
                    
                    # hold에서 제거
                    if ticker in self.hold:
                        del self.hold[ticker]
                        logger.info(f"[{ticker}] hold에서 제거 완료")
                    
                    # explode 모드 해제
                    if hasattr(self.pos_mgr, 'deactivate_explode_mode'):
                        self.pos_mgr.deactivate_explode_mode(ticker)
                else:
                    # 부분 청산 시 수량만 업데이트
                    if ticker in self.hold:
                        remaining_qty = self.hold[ticker].get('qty', 0) - filled_qty
                        self.hold[ticker]['qty'] = max(0, remaining_qty)
                        self.hold[ticker]['value'] = remaining_qty * entry_price
                        logger.info(f"[{ticker}] 부분 청산 후 남은 수량: {remaining_qty:.8f}")
                
                return result
            
            finally:
                # ===== 12. 매도 완료 후 제거 =====
                self.selling_in_progress.discard(ticker)
        
        except Exception as e:
            logger.error(f"[{ticker}] 매도 실행 오류: {e}", exc_info=True)
            self.selling_in_progress.discard(ticker)  # 에러 시에도 제거
            return None
    
    async def position_monitor(self):
        """포지션 모니터링 (중복 매도 방지 + 긴급 손절)"""
        last_daily_loss_warning = 0
        last_eval_time = {}
        last_emergency_sell = {}  # ← 긴급 손절 쿨다운 추가
        
        while self.running:
            try:
                await asyncio.sleep(1)
                now = _time.time()
                
                # ===== 1. invalid entry 제거 =====
                for ticker in list(self.hold.keys()):
                    hold_info = self.hold.get(ticker, {})
                    entry = float(hold_info.get('entry', 0))
                    qty = float(hold_info.get('qty', 0))
                    
                    # entry=0 또는 qty=0인 경우 제거
                    if entry == 0 or qty == 0:
                        logger.warning(f"[CLEANUP] {ticker} invalid entry/qty 감지, hold에서 제거: entry={entry}, qty={qty}")
                        if ticker in self.hold:
                            del self.hold[ticker]
                        continue
                
                logger.info(f"[DEBUG-MON] position_monitor 실행 중, hold 개수: {len(self.hold)}")
                
                # ===== 2. 일일 손실 체크 (60초마다) =====
                daily_loss = self.risk_ctrl.daily_loss_krw
                daily_loss_limit = self.initial_balance * self.config.DAILY_LOSS_LIMIT_PCT
                
                if daily_loss >= daily_loss_limit: 
                    if now - last_daily_loss_warning >= 300:
                        logger.warning(f"⚠️ 일일손실 한도 도달: {daily_loss:,.0f}원 / 한도: {daily_loss_limit:,.0f}원")
                        last_daily_loss_warning = now
                    continue
                
                # ===== 3. 긴급 손절 체크 (모든 포지션) =====
                for ticker in list(self.hold.keys()):
                    try:
                        # ===== 중복 매도 방지 =====
                        if ticker in self.selling_in_progress:
                            #logger.debug(f"[{ticker}] 이미 매도 진행 중, 긴급 손절 스킵")
                            continue
                        
                        # ===== 5초마다 1번만 평가 =====
                        if now - last_eval_time.get(ticker, 0) < 5:
                            continue
                        
                        # ===== 긴급 손절 쿨다운 (5초) =====
                        if now - last_emergency_sell.get(ticker, 0) < 5:
                            continue
                        
                        hold_info = self.hold.get(ticker, {})
                        entry = float(hold_info.get('entry', 0))
                        sl = float(hold_info.get('sl', 0))
                        
                        if entry == 0 or sl == 0:
                            continue
                        
                        # 현재가 조회
                        current_price = self.ws_price_cache.get(ticker)
                        if not current_price:
                            continue
                        
                        # ===== 긴급 손절 조건 확인 =====
                        if current_price <= sl:
                            logger.warning(f"🚨 [{ticker}] 긴급 손절: {current_price:,.0f} <= SL {sl:,.0f}")
                            
                            # ✅ 쿨다운 업데이트 (매도 시도 전)
                            last_emergency_sell[ticker] = now
                            
                            # ✅ 에러 로그 제거 (불필요)
                            await self._execute_sell(ticker, current_price, f"긴급손절 (SL={sl:.0f})")
                            continue
                        
                    except Exception as e:
                        logger.error(f"[{ticker}] 긴급 손절 체크 오류: {e}", exc_info=True)
                
                # ===== 4. 일반 매도 신호 평가 (5초마다) =====
                for ticker in list(self.hold.keys()):
                    try:
                        # ===== 중복 매도 방지 =====
                        if ticker in self.selling_in_progress:
                            continue
                        
                        # ===== 5초마다 1번만 평가 =====
                        if now - last_eval_time.get(ticker, 0) < 5:
                            continue
                        
                        last_eval_time[ticker] = now
                        await self._evaluate_sell_signal(ticker)
                        
                    except Exception as e:
                        logger.error(f"[{ticker}] 매도 평가 오류: {e}", exc_info=True)
                        
            except Exception as e:
                logger.error(f"포지션 모니터링 오류: {e}", exc_info=True)
                if not self.running:
                    break
    
    async def emergency_stop_loss_checker(self):
        """긴급 손절 체크 (실시간)"""
        while self.running:   
            try:
                await asyncio.sleep(0.5)  # 0.5초마다 체크
                
                # 일일 손실 체크 (1분에 1번만 경고)
                #now = _time.time()
                #if self.risk_ctrl.is_daily_loss_limit_reached():  # ← 수정
                #    if now - self.last_daily_loss_warning >= 60:
                #        daily_loss = self.risk_ctrl.get_daily_loss()  # ← 수정
                #        logger.warning(f"⚠️ 일일 손실 한도 도달: {daily_loss:,.0f}원")
                #        self.last_daily_loss_warning = now
                
                for ticker in list(self.hold.keys()):
                    price = self.ws_price_cache.get(ticker)
                    if not price:
                        logger.debug(f"[DEBUG] {ticker} 가격 캐시 없음")
                        continue
                    
                    hold_info = self.hold[ticker]
                    entry = float(hold_info.get('entry', 0))
                    sl = float(hold_info.get('sl', entry * (1 - config.SL_PCT_DEFAULT)))
                    
                    # 손절가 도달 즉시 매도
                    if price <= sl:
                        logger.warning(f"⚠️ [{ticker}] 긴급 손절 실행: 현재가 {price:,.0f} <= SL {sl:,.0f}")
                        # ===== DEBUG: 매도 전 상태 체크 =====
                        logger.warning(f"[DEBUG] {ticker} 매도 시도 전 상태:")
                        logger.warning(f"  - hold_info: {hold_info}")
                        logger.warning(f"  - price: {price}")
                        logger.warning(f"  - entry: {entry}")
                        logger.warning(f"  - sl: {sl}")
                        # ===== 매도 실행 후 hold에서 즉시 제거 =====
                        try:
                            result = await self._execute_sell(ticker, price, f'긴급손절 (SL={sl:,.0f})') 
                            # ===== DEBUG: 매도 결과 체크 =====
                            logger.warning(f"[DEBUG] {ticker} 매도 결과: {result}")
                            # 매도 후 hold에서 제거 (중복 실행 방지)
                            if result:
                                logger.warning(f"✅ [{ticker}] 긴급 손절 완료")
                                # hold에서 제거
                                if ticker in self.hold:
                                    del self.hold[ticker]
                            else:
                                logger.error(f"❌ [{ticker}] 긴급 손절 실패: result가 None")
                        except Exception as sell_error:
                            logger.error(f"[{ticker}] 긴급 손절 실행 실패: {sell_error}", exc_info=True)                                 
                        
            except Exception as e:
                logger.error(f"긴급 손절 체크 오류: {e}", exc_info=True)
                if not self.running:
                    break
    
    async def cleanup(self):
        """정리 작업 (asyncio 종료 오류 해결)"""
        logger.info("봇 종료 중...")
        self.running = False
        
        try:
            # ===== 1. WebSocket 연결 종료 =====
            try:
                if hasattr(self, 'ws') and self.ws:
                    await self.ws.close()
                    logger.info("WebSocket 연결 종료")
            except Exception as e:
                logger.error(f"WebSocket 종료 오류: {e}")
            
            # ===== 2. 모든 포지션 강제 청산 (선택사항) =====
            if self.hold:
                logger.warning(f"보유 포지션 {len(self.hold)}개 감지")
                for ticker in list(self.hold.keys()):
                    try:
                        price = await self.market_data.get_price(ticker)
                        if price:
                            await self._execute_sell(ticker, price, '강제청산(종료)', partial=False)
                    except Exception as e:
                        logger.error(f"[{ticker}] 강제청산 실패: {e}")
            
            # ===== 3. 대기 중인 Task 취소 (핵심 수정!) =====
            try:
                current_task = asyncio.current_task()
                tasks = [
                    t for t in asyncio.all_tasks() 
                    if t != current_task and not t.done()
                ]
                
                if tasks:
                    logger.info(f"취소할 Task 개수: {len(tasks)}")
                    
                    # Task 취소
                    for task in tasks:
                        task.cancel()
                    
                    # Task 취소 대기 (타임아웃 5초)
                    done, pending = await asyncio.wait(tasks, timeout=5.0)
                    
                    if pending:
                        logger.warning(f"⚠️ 취소되지 않은 Task: {len(pending)}개")
                        # 강제 취소
                        for task in pending:
                            task.cancel()
                    else:
                        logger.info(f"✅ 모든 Task 취소 완료: {len(done)}개")
            
            except Exception as e:
                logger.error(f"Task 취소 오류: {e}")
            
            # ===== 4. SQLite 연결 종료 =====
            try:
                for handler in logger.handlers:
                    if isinstance(handler, SQLiteHandler):
                        handler.close()
                        logger.info("SQLite 핸들러 종료")
            except Exception as e:
                logger.error(f"SQLite 핸들러 종료 오류: {e}")
            
            logger.warning("✅ 정리 작업 완료")
            
        except Exception as e:
            logger.error(f"정리 작업 오류: {e}", exc_info=True)
    
    async def periodic_sync(self):
        """주기적 동기화 (잔고, 유니버스)"""
        while True:
            try:
                await asyncio.sleep(config.SYNC_INTERVAL_SEC)
                
                now = _time.time()
                if now - self.last_sync_ts >= 60:
                    await self.sync_balances()
                    self.last_sync_ts = now
                
                # 유니버스 재평가 (4시간마다)
                if now % config.UNIVERSE_REEVAL_SEC < config.SYNC_INTERVAL_SEC:
                    await self.build_universe()
            
            except Exception as e:
                logger.error(f"동기화 오류: {e}")
    
    async def sync_balances(self):
        """잔고 동기화"""
        try:
            balances = await asyncio.to_thread(self.upbit.get_balances)
            krw_balance = 0.0
            
            for b in balances:
                currency = b['currency']
                if currency == 'KRW':
                    krw_balance = float(b['balance'])
                else:
                    ticker = f"KRW-{currency}"
                    qty = float(b['balance'])
                    avg_price = float(b.get('avg_buy_price', 0))
                    
                    if qty > 0 and ticker not in self.hold:
                        # 외부 포지션 감지
                        price = await self.market_data.get_price(ticker)
                        if price:
                            self.pos_mgr.update_position(ticker, qty, avg_price or price)
                            self.hold[ticker] = self.pos_mgr.hold[ticker]
            
            self.balance = krw_balance
            self.risk_ctrl.set_initial_balance(krw_balance)
        
        except Exception as e:
            logger.error(f"잔고 동기화 오류: {e}")
    
    async def build_universe(self):
        """거래 유니버스 구성"""
        try:
            # MarketAnalyzer의 실제 메서드 사용 (비동기 래핑)
            top_coins = await asyncio.to_thread(
                self.market_analyzer.get_top_coins,
                top_n=config.UNIVERSE_TARGET_COUNT,
                min_volume=config.UNIVERSE_MIN_VOL24H
            )
            
            # top_coins는 List[Tuple[str, Dict]] 형태 반환
            # 예: [('KRW-BTC', {...}), ('KRW-ETH', {...})]
            if top_coins:
                self.universe = [ticker for ticker, _ in top_coins]
                logger.info(f"🌐 유니버스 갱신: {len(self.universe)}개")
                if len(self.universe) >= 5:
                    logger.info(f"   상위 5개: {', '.join(self.universe[:5])}")
                else:
                    logger.info(f"   전체: {', '.join(self.universe)}")
            else:
                raise Exception("분석된 코인이 없습니다")
        
        except Exception as e:
            logger.error(f"유니버스 구성 오류: {e}", exc_info=True)
            
            # 폴백: 기본 메이저 코인 리스트
            self.universe = [
                'KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-SOL', 'KRW-DOGE',
                'KRW-ADA', 'KRW-AVAX', 'KRW-MATIC', 'KRW-LINK', 'KRW-DOT',
                'KRW-ATOM', 'KRW-NEAR', 'KRW-APT', 'KRW-SUI', 'KRW-ARB'
            ]
            logger.warning(f"⚠️ 폴백 유니버스 사용: {len(self.universe)}개")
    
    async def metrics_reporter(self):
        """메트릭 리포팅"""
        while True:
            try:
                await asyncio.sleep(60)
                
                msg = f"📊 메트릭 | WS수신: {self.metrics['ws_received']} | "
                msg += f"포지션: {len(self.hold)} | 잔고: {Utils.fmt_krw(self.balance)}원"
                
                Utils.throttled_log('metrics', msg, interval=300)
            
            except Exception as e:
                logger.error(f"메트릭 리포팅 오류: {e}")
    
    async def cleanup(self):
        """정리 작업"""
        logger.info("🧹 정리 작업 시작...")
        
        if self.http_session:
            await self.http_session.close()
        
        logger.info("✅ 정리 완료")
        

# ============================================================
# 8. 메인 엔트리 포인트
# ============================================================
def main():
    """메인 엔트리 포인트"""
    bot = TradingBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("🛑 프로그램 종료")
    except Exception as e:
        logger.critical(f"❌ 치명적 오류: {e}", exc_info=True)
        logging.activate_debug_mode()   # 오류 감지 시 DEBUG 로그 임시 활성화

if __name__ == '__main__':
    bot = None
    try:
        bot = TradingBot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.warning("🛑 KeyboardInterrupt 감지, 정상 종료 시작...")
        if bot:
            # 비동기 정리 작업
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.stop()
                    loop.close()
            except Exception as e:
                logger.error(f"이벤트 루프 종료 오류: {e}")
    except Exception as e:
        logger.error(f"치명적 오류: {e}", exc_info=True)
    finally:
        logger.warning("🔚 프로그램 종료")
        sys.exit(0)
