import pyupbit, os, datetime, time, traceback
from dotenv import load_dotenv
from market_research import volatility_breakout
from mongodb_connect import MongodbConnect
from message_bot import Message_Bot
from strategy_engine import decide_order, RiskManager, calc_market_snapshot, calc_market_index
from market_analyzer import MarketAnalyzer
from correlation_manager import CorrelationManager
import pytz
import math 

load_dotenv()

# ✅ 누락된 의존성
import pandas as pd
import numpy as np

# ✅ 간단 RSI 유틸(외부에서 안 가져온다면 이 파일에 정의)
def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).rolling(period).mean()
    roll_down = pd.Series(down, index=series.index).rolling(period).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

# --- Helper: price/volume normalization for Upbit tick size & min trade ---
def _normalize_price(price, market='KRW'):
    try:
        return pyupbit.get_tick_size(price, market=market)
    except Exception:
        # fallback: round to 2 decimal places if API unavailable
        return round(price, 2)

def _normalize_volume(krw_amount, price):
    # Upbit minimum order is roughly 5,000 KRW per trade.
    if price <= 0:
        return 0
    vol = krw_amount / price
    return float(vol)

access_key = os.getenv("UPBIT_ACCESS_KEY")
secret_key= os.getenv("UPBIT_SECRET_KEY")
 
messanger = Message_Bot()

MIN_ORDER_AMOUNT = int(os.getenv("MIN_ORDER_AMOUNT", "5000"))
DAILY_LOSS_LIMIT_PCT = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.05"))
MAX_CONCURRENT_POSITIONS = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))

db = MongodbConnect("coin")
KST = pytz.timezone('Asia/Seoul')

def get_kst_now():
    return datetime.datetime.now(KST)

class Upbit_User:
    
    # 9시가 되면 
    # 1. 보유한 코인을 모두 매도한다. -> self.coin 을 초기화한다.
    # 2. 잔고를 확인하고 최대 투자 개수를 파악한다. (최대 10개)
    # 2. 시장 조사를 실시한다. -> 투자 대상 코인 목록을 불러온다 {coint: {}}
    # 3. 시드는 total balance // 투자 코인 개수로 정한다.
    # 4. 코인 별 {target_price: int, realtime_price: int} 로 초기화 한다.
    # 5. 목표 코인에 대한 가격을 1초마다 확인한다.
    
    def __init__(self, access_key, secret_key):
        self.user = pyupbit.Upbit(access_key, secret_key)
        self.last_reset_date = None   # 일별 리셋 중복 방지

        # 🆕 보유시간/스테일 판정 기본값(환경변수로 조정)
        self.max_hold_sec   = int(os.getenv("MAX_HOLD_SEC", "3600"))   # 과거 호환
        self.max_hold_min   = int(os.getenv("MAX_HOLD_MIN", "120"))    # 🆕 120분 기본
        self.stale_pnl_band = float(os.getenv("STALE_PNL_BAND_PCT", "0.004"))  # 🆕 ±0.4%

        # DB에서 기존 상태 로드(없으면 기본)
        try:
            self.coin = db.asset.find_one({"title": "coin_asset"})["own"]
        except Exception:
            self.coin = {}

        self.today = get_kst_now()
        self.budget = float(self.user.get_balance("KRW") or 0)
        self.start_of_day_krw = self.budget

        # 🆕 현금 리저브(예: 15% 유지)
        self.reserve_pct = float(os.getenv("CASH_RESERVE_PCT", "0.15"))

        # 동시 포지션 개수 및 1포지션당 배분 금액(리저브 반영)
        self.max_positions   = MAX_CONCURRENT_POSITIONS
        self.investment_size = self.max_positions
        investable_krw       = max(0.0, self.budget * (1.0 - self.reserve_pct))  # 🆕
        self.investment_amount = max(
            MIN_ORDER_AMOUNT,
            ((investable_krw // self.investment_size) // 10) * 10
        )

        # ✅ 필수 매니저 초기화 (누락 보완)
        self.risk = RiskManager()                  # 일손실 한도/거래결과 기록 등에 사용
        self.analyzer = MarketAnalyzer()           # 상위 종목 스캔/재평가
        self.correlation = CorrelationManager(     # 포트폴리오 상관관계 제한
            max_correlation=float(os.getenv("CORRELATION_CAP", "0.7"))
        )

        # __init__ (상단 other fields 초기화 바로 아래)
        self.last_insufficient = {}   # 코인별 자금부족 쿨다운 타임스탬프
        
        # ✅ 유동성 게이트 파라미터(환경변수로 조정 가능)
        self.liq_pctl = float(os.getenv("LIQ_PCTL", "0.40"))        # 예: 하위 40퍼센타일
        self.liq_lookback = int(os.getenv("LIQ_LOOKBACK_MIN", "240")) # 최근 240분(=4h) 분포

        # 런타임 상태
        self.daily_pnl = 0.0
        self.last_ticker_update = self.today  # 재평가 타이머 시작
        # 재평가 주기/유동성 하한을 환경변수에서 읽음(없으면 기존 기본 유지)
        self.universe_reeval_sec = int(os.getenv("UNIVERSE_REEVAL_SEC", str(4*3600)))
        self.universe_min_vol = int(os.getenv("UNIVERSE_MIN_VOL24H", "5000000000"))  # 100억 → 50억으로 완화

        messanger.send_message(f"{self.today.year}년 {self.today.month}월 {self.today.day}일 자동매매 봇 연결 완료")
        if self.coin:
            messanger.send_message(f"복원된 코인: {', '.join(self.coin.keys())}")

    def staged_profit_taking(self, *, entry_price: float, current_price: float, position_value: float):
        """
        다단계 이익실현/손절 상향 로직(러너 모드 포함)
        - 10%: 30~50% 부분 익절 + 트레일 진입(≈ 고점대비 -8% 수준)
        - 12%: (보유가치 충분 시) 20~30% 추가 익절
        - 15%: 트레일 강화(최소 본전+6% 또는 고점대비 -10%)
        - 20%+: 잔여 물량 트레일(고점대비 -12%)로 수익 극대화
        """
        profit_pct = (current_price / max(1e-9, entry_price) - 1) * 100.0
        MIN_PARTIAL_VALUE = 20000  # 부분익절 최소가치

        # 러너 트레일 기준(가격 기준; 루프 외부에서 peak 기반 상향 트레일 병행)
        trail_8  = current_price * 0.92
        trail_10 = current_price * 0.90
        trail_12 = current_price * 0.88

        if profit_pct >= 20:
            new_sl = max(entry_price * 1.10, trail_12)
            return {"action": "adjust_sl", "new_sl": new_sl, "reason": "20%+ 러너 트레일(-12%)"}

        if profit_pct >= 15:
            new_sl = max(entry_price * 1.06, trail_10)
            return {"action": "adjust_sl", "new_sl": new_sl, "reason": "15% 달성 - 트레일 강화"}

        if profit_pct >= 12:
            if position_value >= MIN_PARTIAL_VALUE * 2:
                return {"action": "partial_sell", "ratio": 0.3, "reason": "12% 달성 - 30% 매도"}
            new_sl = max(entry_price * 1.05, trail_10)
            return {"action": "adjust_sl", "new_sl": new_sl, "reason": "12% 달성 - 트레일"}

        if profit_pct >= 10:
            if position_value >= MIN_PARTIAL_VALUE:
                # 50% 부분익절 (SL 상향은 루프에서 병행 적용)
                return {"action": "partial_sell", "ratio": 0.5, "reason": "10% 달성 - 50% 매도"}
            new_sl = max(entry_price * 1.04, trail_8)
            return {"action": "adjust_sl", "new_sl": new_sl, "reason": "10% 달성 - 트레일"}

        if profit_pct >= 8:
            if position_value >= MIN_PARTIAL_VALUE:
                return {"action": "partial_sell", "ratio": 0.3, "reason": "8% 달성 - 30% 매도"}
            return {"action": "adjust_sl", "new_sl": entry_price * 1.04, "reason": "8% 달성 - 손절 상향"}

        if profit_pct >= 5:
            if position_value >= MIN_PARTIAL_VALUE * 2:
                return {"action": "partial_sell", "ratio": 0.2, "reason": "5% 달성 - 20% 매도"}
            new_sl = entry_price * 1.03
            return {"action": "adjust_sl", "new_sl": new_sl, "reason": "5% 달성 - 손절 상향(소액)"}

        if profit_pct >= 3:
            return {"action": "adjust_sl", "new_sl": entry_price * 1.02, "reason": "3% 달성 - 손절 상향"}

        if profit_pct >= 1.5:
            return {"action": "adjust_sl", "new_sl": entry_price * 1.005, "reason": "1.5% 달성 - 손절 본전"}

        return {"action": "hold"}

    def partial_sell_coin(self, coin, cur_price, ratio: float):
        """보유 수량의 일부를 시장가 매도하고 포지션 상태 갱신"""
        holding_qty = float(self.user.get_balance(coin) or 0)
        qty_to_sell = round(holding_qty * max(0.0, min(1.0, ratio)), 6)
        if qty_to_sell <= 0:
            return None
        self.user.sell_market_order(coin, qty_to_sell)
        pnl = (cur_price - (self.coin[coin].get("buy_price") or cur_price)) * qty_to_sell
        self.risk.record_trade_result(pnl > 0, (pnl / max(1.0, cur_price * qty_to_sell)) * 100)
        # 보유 수량 갱신
        # ✅ 체결 후 실제 잔고 재조회로 동기화
        new_qty = float(self.user.get_balance(coin) or 0)
        self.coin[coin]["qty"] = max(0.0, new_qty)
        messanger.send_message(f"🔁 부분매도: {coin} {qty_to_sell:.6f}개 @ {cur_price:.1f}")
        # 잔여 없음이면 포지션 삭제
        if self.coin[coin]["qty"] <= 0:
            self.delete_coin_info(coin)
            return coin
        return None

    def adjust_budget(self):
        self.budget = float(self.user.get_balance("KRW") or 0.0)
        investable_krw = max(0.0, self.budget * (1.0 - getattr(self, "reserve_pct", 0.0)))  # 🆕
        self.investment_amount = max(
            MIN_ORDER_AMOUNT,
            ((investable_krw // self.investment_size) // 10) * 10
        )
        messanger.send_message(f"잔고 : {self.budget:.0f} (투자가능 {investable_krw:.0f}, 1포지션 {self.investment_amount:.0f})")  # 🆕
            
        if self.budget <= 10000:
            return True
        
        return False
    
    def start_research(self, day):
        messanger.send_message(f"> {day.year}년 {day.month}월 {day.day}일 종목 스캔 시작")
        day = datetime.datetime(day.year, day.month, day.day) + datetime.timedelta(hours=9)

        # 유동성 필터 기반 상위 종목 선정 (거래대금 10억원 이상)
        _cands = self.analyzer.get_top_coins(top_n=self.max_positions, min_volume=10_000_000_000)
        tickers = [t if isinstance(t, str) else t[0] for t in _cands][:self.max_positions]

        self.coin = {t: {"invest": False, "buy_price": None, "qty": 0.0, "tp": None, "sl": None} for t in tickers}
        db.asset.update_one({"title": "coin_asset"}, {"$set": {"own": self.coin}}, upsert=True)
        messanger.send_message(f"스캔 완료. 유니버스: {', '.join(tickers)}")

        # 상관관계 매트릭스 업데이트
        try:
            self.correlation.update_correlation_matrix(tickers)
        except Exception:
            pass

        self.research_status = True
        
    def buy_coin(self, coin, price, order_value, tp, sl):
        # 업비트는 KRW 금액 지정 매수
        if order_value < MIN_ORDER_AMOUNT:
            messanger.send_message(f"[{coin}] 최소 주문금액({MIN_ORDER_AMOUNT} KRW) 미만 → 스킵")
            return False

        # buy_coin
        FEE = 0.0005
        SAFE_BUF = 1.002  # 호가/미세변동 안전버퍼

        avail = float(self.user.get_balance("KRW") or 0.0)
        max_affordable = max(0.0, (avail / (1.0 + FEE)) / SAFE_BUF)

        order_value = min(order_value, max_affordable)

        if order_value < MIN_ORDER_AMOUNT:
            messanger.send_message(f"[{coin}] 가용KRW 부족(수수료/버퍼 반영). 주문금액 {order_value:.0f} KRW < 최소 {MIN_ORDER_AMOUNT} → 스킵")
            # 자금 부족 쿨다운 마킹
            self.last_insufficient[coin] = time.time()
            return False

        try:
            res = self.user.buy_market_order(coin, order_value)
        except Exception as e:
            if "InsufficientFunds" in str(e):
                self.last_insufficient[coin] = time.time()
                messanger.send_message(f"[{coin}] KRW 부족(예상 수수료/호가 변동). {int(SAFE_BUF*1000-1000)}bp 버퍼 적용 필요 → {coin} {30}s 쿨다운")
                return False
            raise

        if res is None:
            # 거래소 측 미체결/거절 시에도 과도한 재시도 방지
            self.last_insufficient[coin] = time.time()
            return False

        qty = order_value / price
        self.coin[coin].update({
            "buy_price": price,
            "invest": True,
            "qty": qty,
            "tp": tp,
            "sl": sl,
            "entry_ts": time.time()   # ✅ 보유 시작 시각
        })
        db.asset.update_one({"title": "coin_asset"}, {"$set": {"own": self.coin}})

        messanger.send_message(f"✅ 매수: {coin} {qty:.6f}개 @ {price:.1f} | TP {tp:.1f} / SL {sl:.1f}")
        return True
     
    def decide_and_trade(self, coin):
        # 1분봉 200개로 스냅샷 산출
        need = max(200, self.liq_lookback)
        df = pyupbit.get_ohlcv(coin, interval="minute1", count=need)
        if df is None or df.empty:
            return

        # 시장 상태 & 단계지수
        snap = calc_market_snapshot(df)
        market_index = calc_market_index(snap)

        # 최신 가격 / 잔고
        price = float(df["close"].iloc[-1])
        krw = float(self.user.get_balance("KRW") or 0)

        # 일 손실 한도 도달 시 중단
        limit_krw = (self.start_of_day_krw or krw) * DAILY_LOSS_LIMIT_PCT
        if self.risk.daily_pnl < -limit_krw:
            messanger.send_message(f"⛔ 일일 손실 한도 도달: {-self.risk.daily_pnl:,.0f} / {limit_krw:,.0f} KRW → 중단")
            return

        # 전략 플랜 산출 (ATR 기반 TP/SL 포함)
        plan_info = decide_order(df, balance_krw=krw, risk_manager=self.risk)
        plan_dict = (plan_info or {}).get("plan")
        if not plan_dict:
            return
        qty = float(plan_dict.get("qty") or 0.0)
        tp  = plan_dict.get("tp")
        sl  = plan_dict.get("sl")
        
        # ✅ 유동성 퍼센타일 게이트 (15s 근사)
        ok, est15s, thr15s = self.analyzer.prefilter_15s_turnover_min_from_m1(coin, df)
        if not ok:
            messanger.send_message(
                f"[{coin}] skip: est15s={est15s:,.0f} < p{int(self.analyzer.liq_pctl*100)}={thr15s:,.0f} (15s, pctile)"
            )
            return

        # ✅ 최근 자금부족 쿨다운 확인 (30초)
        cool = self.last_insufficient.get(coin, 0.0)
        if time.time() - cool < 30.0:
            remain = int(30 - (time.time() - cool))
            messanger.send_message(f"⏳ [쿨다운] 최근 자금 부족으로 {coin} 매수 스킵 ({remain}s)")
            return

        # 상관관계 검사: 현재 보유 중인 티커와 후보 간 상관관계 체크
        invested = [t for t, info in self.coin.items() if info.get("invest")]
        if invested:
            try:
                self.correlation.update_correlation_matrix(invested + [coin])
                corr = self.correlation.check_portfolio_correlation(invested, coin)
                if not corr["allowed"]:
                    messanger.send_message(f"[{coin}] 상관관계 초과 → 스킵: {corr['reason']}")
                    return
            except Exception:
                pass

        # 리스크 매니저 포지션 크기 제한 및 최소주문금액
        trade_value = min(self.investment_amount, qty * price)
        
        # 🆕 리저브 보존: 투자 후에도 리저브가 유지되도록 가용 KRW 확인
        avail_krw = float(self.user.get_balance("KRW") or 0.0)
        need_krw  = trade_value
        reserve   = float(getattr(self, "reserve_pct", 0.0)) * (self.budget or avail_krw)
        post_cash = avail_krw - need_krw

        if need_krw < MIN_ORDER_AMOUNT:
            messanger.send_message(f"[{coin}] 주문금액 {need_krw:.0f} KRW (최소 {MIN_ORDER_AMOUNT}) → 스킵")
            return

        # 🆕 리저브 침해 시 로테이션 시도
        if post_cash < reserve:
            # 로테이션 후보: 가장 오래 보유했고, 손익이 밴드 내(±0.4%)인 종목
            candidate = None
            oldest_age = -1.0
            now_ts = time.time()

            for h_coin, h_info in list(self.coin.items()):
                if not h_info.get("invest"):
                    continue
                entry_ts = float(h_info.get("entry_ts") or 0.0)
                if entry_ts <= 0 or (now_ts - entry_ts) < max(30, 60 * 30):  # 최소 30분 보유 후만 로테이션
                    continue
                h_entry = float(h_info.get("buy_price") or price)
                try:
                    h_price = float(pyupbit.get_current_price(h_coin) or 0.0)
                except Exception:
                    h_price = price
                pnl_abs = abs((h_price / max(1e-9, h_entry)) - 1.0)
                if pnl_abs <= getattr(self, "stale_pnl_band", 0.004):  # ±0.4% 밴드 내
                    age = now_ts - entry_ts
                    if age > oldest_age:
                        oldest_age = age
                        candidate = (h_coin, h_price)

            if candidate:
                h_coin, h_price = candidate
                # 부분매도(50%)로 현금 확보
                try:
                    self.partial_sell_coin(h_coin, h_price, ratio=0.5)
                    messanger.send_message(f"🔄 로테이션: {h_coin} 50% 정리 → {coin} 진입 재시도")
                    # 잔고 갱신
                    avail_krw = float(self.user.get_balance("KRW") or 0.0)
                    post_cash = avail_krw - need_krw
                except Exception:
                    pass

        # 로테이션 후에도 리저브 침해면 스킵
        if post_cash < reserve:
            messanger.send_message(f"[{coin}] 리저브 유지 위해 스킵 (post_cash {post_cash:.0f} < reserve {reserve:.0f})")
            return

        # 최종 주문 실행
        self.buy_coin(coin, price, trade_value, tp=tp, sl=sl)

    def sell_coin(self, coin, cur_price, reason="EXIT"):
        qty = float(self.user.get_balance(coin) or 0)
        if qty <= 0:
            # 보유수량 없다면 포지션 정보만 정리
            self.delete_coin_info(coin)
            return coin

        self.user.sell_market_order(coin, qty)
        entry = self.coin.get(coin, {}).get("buy_price") or cur_price
        pnl = (cur_price - entry) * qty
        self.risk.record_trade_result(pnl > 0, (pnl / max(1.0, entry * qty)) * 100)

        messanger.send_message(f"🔁 매도[{reason}]: {coin} {qty:.6f}개 @ {cur_price:.1f} | PnL: {pnl:,.0f} KRW")
        # ✅ 내부에서도 한 번 정리 (이중 호출돼도 안전)
        self.delete_coin_info(coin)
        return coin

    def delete_coin_info(self, coin):
        if coin in self.coin:
            del self.coin[coin]
            db.asset.update_one({"title": "coin_asset"}, {"$set": {"own": self.coin}})


    def start(self):
        try:
            while True:
                now = get_kst_now()

                # 매일 09:00 리셋: 전량 정리, 예산/리스크 초기화, 종목 재선정
                if now.hour == 9 and now.minute == 0 and now.second <= 5:
                    if self.last_reset_date != now.date():
                        to_del = []
                        for coin in list(self.coin.keys()):
                            # 보유분만 실제 매도
                            if self.coin[coin].get("invest"):
                                cur = float(pyupbit.get_current_price(coin) or 0)
                                sold = self.sell_coin(coin, cur, reason="DAILY_RESET")
                                to_del.append(sold)
                            else:
                                to_del.append(coin)

                        for c in to_del:
                            self.delete_coin_info(c)

                        # 예산/리스크 초기화
                        self.budget = float(self.user.get_balance("KRW") or 0)
                        self.investment_amount = max(MIN_ORDER_AMOUNT, ((self.budget // self.investment_size) // 10) * 10)
                        self.start_of_day_krw = self.budget  # ✅ 일손실 한도 기준 고정
                        self.risk.reset_daily()
                        self.daily_pnl = 0.0
                        self.today = now
                        self.start_research(day=self.today)
                        self.last_ticker_update = now
                        self.last_reset_date = now.date()  # ✅ 오늘 리셋 완료 플래그

                # 주기적으로 유니버스 재평가(환경변수 사용)
                if (now - self.last_ticker_update).total_seconds() >= self.universe_reeval_sec:
                    try:
                        new_pairs = self.analyzer.get_top_coins(
                            top_n=self.max_positions,
                            min_volume=self.universe_min_vol
                        )
                        new_list = [t if isinstance(t, str) else t[0] for t in new_pairs]

                        invested = [t for t, v in self.coin.items() if v.get("invest")]
                        keep = set(invested)

                        add = [c for c in new_list if c not in keep][:max(0, self.max_positions - len(keep))]
                        drop = [t for t, v in self.coin.items() if (not v.get("invest")) and (t not in new_list)]

                        for d in drop:
                            self.delete_coin_info(d)
                        for a in add:
                            self.coin.setdefault(a, {"invest": False, "buy_price": None, "qty": 0.0, "tp": None, "sl": None})
                        if add or drop:
                            db.asset.update_one({"title": "coin_asset"}, {"$set": {"own": self.coin}}, upsert=True)
                            messanger.send_message(f"🔄 종목 재평가: 추가 {add} / 제거 {drop}")
                    except Exception:
                        pass
                    self.last_ticker_update = now

                # 각 종목 처리
                for coin, info in list(self.coin.items()):
                    try:
                        price = float(pyupbit.get_current_price(coin) or 0)
                        if price <= 0:
                            continue
                    except Exception:
                        continue

                    # 보유 중이면 다단계 이익실현/SL 상향 → TP/SL 최종 체크
                    if info.get("invest"):
                        entry = info.get("buy_price") or price
                        pos_qty = float(self.user.get_balance(coin) or 0)
                        pos_val = pos_qty * price
                        # 1) 피크 업데이트
                        peak = info.get("peak") or entry
                        if price > peak:
                            self.coin[coin]["peak"] = price
                            peak = price

                        # 2) 단계 수익/트레일 결정
                        act = self.staged_profit_taking(entry_price=entry, current_price=price, position_value=pos_val)
                        pnl_ratio = (price / max(1e-9, entry)) - 1.0
                        
                        # 🔧 러너 트레일(피크 기준) 보강: 10%/15% 구간
                        if pnl_ratio >= 0.15:
                            runner_sl = max(info.get("sl") or 0.0, entry * 1.06, peak * 0.90)  # 고점대비 -10% or 본전+6%
                            if runner_sl > (info.get("sl") or 0.0):
                                self.coin[coin]["sl"] = runner_sl
                                messanger.send_message(f"🏃 러너 트레일 강화: {coin} SL → {runner_sl:.1f}")
                        elif pnl_ratio >= 0.10:
                            runner_sl = max(info.get("sl") or 0.0, entry * 1.04, peak * 0.92)  # 고점대비 -8% or 본전+4%
                            if runner_sl > (info.get("sl") or 0.0):
                                self.coin[coin]["sl"] = runner_sl
                                messanger.send_message(f"🏃 러너 트레일 진입: {coin} SL → {runner_sl:.1f}")

                        # 기존 소프트 트레일 로직 유지
                        if pnl_ratio >= 0.008:
                            new_sl = max(info.get("sl") or 0.0, entry * 1.001)
                            if new_sl > (info.get("sl") or 0.0):
                                self.coin[coin]["sl"] = new_sl
                                messanger.send_message(f"🛡️ 소프트 트레일: {coin} SL → {new_sl:.1f}")
                        
                        # 🆕 시간 기반 스테일 정리 (회전율 확보)
                        try:
                            entry_ts = float(info.get("entry_ts") or 0.0)
                            age_min  = (time.time() - entry_ts) / 60.0 if entry_ts > 0 else 0.0
                            entry_px = info.get("buy_price") or price
                            pnl_abs  = abs((price / max(1e-9, entry_px)) - 1.0)

                            if age_min >= self.max_hold_min and pnl_abs <= self.stale_pnl_band:
                                sold = self.sell_coin(coin, price, reason="STALE_EXIT")
                                self.delete_coin_info(sold)
                                continue
                        except Exception:
                            pass                            

                        # (B) 모멘텀 방어적 종료 체크 (EMA9 하향 + Stoch K<D + RSI 둔화)
                        try: 
                            df_m1 = pyupbit.get_ohlcv(coin, interval="minute1", count=50)
                            # 스토캐스틱(14) 계산이 가능하도록 최소 20봉 확보
                            if df_m1 is not None and len(df_m1) >= 20:
                                close = df_m1["close"]
                                low = df_m1["low"]
                                high = df_m1["high"]

                                # EMA9
                                ema9 = close.ewm(span=9, adjust=False).mean().iloc[-1]

                                # Stochastic %K(14)와 %D(최근 3개 K 평균)
                                low14  = low.rolling(14).min()
                                high14 = high.rolling(14).max()

                                # 마지막 값이 NaN이거나 분모가 0이면 기본값(50)로 안전처리
                                if pd.isna(low14.iloc[-1]) or pd.isna(high14.iloc[-1]) or (high14.iloc[-1] == low14.iloc[-1]):
                                    k = 50.0
                                    d = 50.0
                                else:
                                    k_series_full = 100.0 * (close - low14) / (high14 - low14)
                                    k = float(k_series_full.iloc[-1])
                                    d = float(k_series_full.iloc[-3:].mean())  # 최근 3개 K 평균

                                    if np.isnan(k): k = 50.0
                                    if np.isnan(d): d = 50.0

                                # RSI(14)
                                rsi_val = float(compute_rsi(close, 14).iloc[-1])
                                if np.isnan(rsi_val): rsi_val = 50.0

                                # 모멘텀 둔화 방어 청산 조건
                                if (price < ema9) and (k < d) and (rsi_val < 55):
                                    sold = self.partial_sell_coin(coin, price, ratio=0.5)  # ✅ 인자명 ratio로 통일
                                    if sold:
                                        continue
                                    messanger.send_message(f"⚠️ 모멘텀 둔화 방어청산 50%: {coin}")
                        except Exception:
                            pass  # 실패해도 치명적 아님

                        # (C) 시간 기반 종료 (자본 효율)
                        held_sec = time.time() - info.get("entry_ts", time.time())
                        if held_sec > self.max_hold_sec:  # 기본 1시간
                            # TP/SL 미충족 & 수익률 미미(±0.2% 이내)면 전량 또는 부분 청산
                            if abs(pnl_ratio) < 0.002:
                                sold = self.sell_coin(coin, price, reason="TIMEOUT")
                                self.delete_coin_info(sold)
                                continue
                        # 3) staged 결과 반영 (new_sl 동시 지원)
                        if act["action"] == "partial_sell":
                            sold = self.partial_sell_coin(coin, price, act["ratio"])
                            # act에 new_sl이 함께 오는 경우를 대비해 SL도 즉시 반영
                            if "new_sl" in act:
                                self.coin[coin]["sl"] = max(self.coin[coin].get("sl") or 0.0, act["new_sl"])
                            if sold:
                                continue
                        elif act["action"] == "full_sell":
                            sold = self.sell_coin(coin, price, reason=act["reason"])
                            self.delete_coin_info(sold)
                            continue
                        elif act["action"] == "adjust_sl":
                            self.coin[coin]["sl"] = act["new_sl"]
                            messanger.send_message(f"🛡️ SL 상향: {coin} → {act['new_sl']:.1f} ({act['reason']})")
                            
                        # 2) TP/SL 최종 확인
                        tp, sl = info.get("tp"), info.get("sl")
                        if tp is not None and price >= tp:
                            sold = self.sell_coin(coin, price, reason="TP")
                            continue
                        elif sl is not None and price <= sl:
                            sold = self.sell_coin(coin, price, reason="SL")
                            continue

                    # 미보유면 전략 의사결정 → 진입 검토
                    self.decide_and_trade(coin)

                time.sleep(0.2)

        except Exception as error:
            messanger.send_message(f"오류 발생으로 중단됩니다. \n{error} \n{traceback.format_exc()}")

def _calc_atr(self, df: pd.DataFrame, period: int = 14) -> float:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - prev_c).abs(),
        (l - prev_c).abs()
    ], axis=1).max(axis=1)
    return float(tr.rolling(period, min_periods=period).mean().iloc[-1])

def _entry_signal(self, df: pd.DataFrame) -> dict:
    """
    Returns: {"ok": bool, "mode": "breakout"|"pullback"|None,
              "break_level": float|None, "reason": str}
    """
    if df is None or len(df) < 60:
        return {"ok": False, "mode": None, "break_level": None, "reason": "insufficient bars"}

    close = df["close"]
    high  = df["high"]
    vol   = df["volume"]

    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()

    rsi14 = compute_rsi(close, 14)
    vol_ma60 = vol.rolling(60, min_periods=30).mean()

    last = float(close.iloc[-1])
    last_vol = float(vol.iloc[-1])
    last_rsi = float(rsi14.iloc[-1]) if not np.isnan(rsi14.iloc[-1]) else 50.0

    # 돌파 모드
    hh60 = float(high.rolling(60, min_periods=30).max().iloc[-2])  # 직전봉까지의 60분 고가
    breakout = (last > hh60) and (last_vol >= 1.5 * float(vol_ma60.iloc[-1])) and (last_rsi > 55)

    # 추세 되돌림 모드
    pullback = (float(ema20.iloc[-1]) > float(ema50.iloc[-1])) \
               and (last > float(ema20.iloc[-1])) \
               and (45 <= last_rsi <= 60) \
               and (last_vol >= 1.2 * float(vol_ma60.iloc[-1]))

    # 과열 추격 방지 (몸통 > 1.2%)
    body_pct = abs(float(df["close"].iloc[-1] - df["open"].iloc[-1]) / max(1e-9, float(df["open"].iloc[-1])))
    if body_pct > 0.012 and not breakout:
        return {"ok": False, "mode": None, "break_level": None, "reason": "overextended body"}

    if breakout:
        return {"ok": True, "mode": "breakout", "break_level": hh60, "reason": "hh60_vol_spike_rsi"}
    if pullback:
        return {"ok": True, "mode": "pullback", "break_level": None, "reason": "trend_reclaim"}

    return {"ok": False, "mode": None, "break_level": None, "reason": "no signal"}

if __name__ == "__main__":
    Upbit_User(access_key=access_key, secret_key=secret_key).start()
