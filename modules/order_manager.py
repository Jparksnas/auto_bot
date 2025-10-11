# order_manager.py

"""
매수/매도 주문 실행 및 검증 전용 모듈
IOC 래더, 슬리피지 게이트, 부분청산, 시장가 폴백 등
"""

import asyncio
import logging
import time as _time
import random
from typing import Optional, Tuple
from collections import defaultdict
import pyupbit

logger = logging.getLogger(__name__)

class OrderManager:
    """주문 실행 및 관리"""
    
    def __init__(self, upbit, config, messenger, market_data, risk_control):
        self.upbit = upbit
        self.config = config
        self.messenger = messenger
        self.market_data = market_data
        self.risk_control = risk_control
        
        # 주문 상태 추적
        self.pending_orders = {}  # uuid -> order_info
        self.last_buy_ts = defaultdict(float)
        self.buy_counter = defaultdict(int)
        
        # 슬리피지 추적
        self.slippage_record = defaultdict(lambda: defaultdict(list))
        
        # 통계
        self.metrics = defaultdict(int)
    
    # ===== 체결 확인 함수 추가 =====
    async def wait_for_fill(self, order_uuid: str, timeout: float = 10.0) -> Optional[dict]:
        """
        주문 체결 대기
        """
        start_time = _time.time()
        
        while _time.time() - start_time < timeout:
            try:
                # 주문 조회
                order = await asyncio.to_thread(self.upbit.get_order, order_uuid)
                
                if not order:
                    logger.warning(f"[{order_uuid}] 주문 조회 실패")
                    await asyncio.sleep(0.5)
                    continue
                
                state = order.get('state')
                
                # ===== DEBUG: 체결 정보 출력 =====
                if state == 'done':
                    logger.info(f"[{order_uuid}] 체결 완료!")
                    logger.info(f"[DEBUG] 주문 정보: {order}")  # ← 전체 응답 확인
                    
                    # avg_price 필드 확인
                    avg_price = order.get('avg_price')
                    if avg_price is None or avg_price == '0':
                        # 대체 필드 확인 (trades 정보)
                        trades = order.get('trades', [])
                        if trades:
                            total_value = sum(float(t.get('funds', 0)) for t in trades)
                            total_volume = sum(float(t.get('volume', 0)) for t in trades)
                            avg_price = total_value / total_volume if total_volume > 0 else 0
                            logger.warning(f"[{order_uuid}] avg_price=0, trades로 계산: {avg_price}")
                    
                    return order
                    
                elif state == 'cancel':
                    logger.warning(f"[{order_uuid}] 주문 취소됨")
                    return None
                elif state in ['wait', 'watch']:
                    # 대기 중
                    await asyncio.sleep(0.5)
                    continue
                else:
                    logger.warning(f"[{order_uuid}] 알 수 없는 상태: {state}")
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"[{order_uuid}] 체결 확인 오류: {e}")
                await asyncio.sleep(0.5)
        
        # 타임아웃
        logger.error(f"[{order_uuid}] 체결 타임아웃 ({timeout}초)")
        return None
    
    async def buy(self, ticker: str, amount: float, reasons: list = None,
                  strategy: Optional[str] = None) -> Optional[dict]:
        """
        매수 실행 (검증 + IOC 래더 + 시장가 폴백)
        """
        now = _time.time()
        
        # 1. 쿨다운 체크
        cooldown = self.config.POS_COOLDOWN_AFTER_IOC_SEC
        if now - self.last_buy_ts[ticker] < cooldown:
            logger.debug(f"[{ticker}] 매수 쿨다운 중 ({cooldown}s)")
            return None
        
        # 2. 심도 검증
        ok_depth = await self.market_data.check_order_depth(ticker, amount)
        if not ok_depth:
            self.metrics['buy_depth_fail'] += 1
            logger.info(f"[{ticker}] 주문 심도 부족 → 매수 중단")
            return None
        
        # 3. 슬리피지 게이트 (옵션)
        if self.config.SLIPPAGE_GATE_ENABLED:
            slip_ok = await self._check_slippage_gate(ticker, amount)
            if not slip_ok:
                self.metrics['buy_slippage_fail'] += 1
                logger.info(f"[{ticker}] 슬리피지 게이트 탈락")
                return None
        
        # 4. IOC 래더 시도
        if self.config.USE_LIMIT_PROTECT:
            result = await self._execute_ioc_ladder(ticker, amount, reasons, strategy)
            if result:
                self.last_buy_ts[ticker] = _time.time()
                self.buy_counter[ticker] += 1
                return result
        
        # 5. 시장가 폴백
        logger.info(f"[{ticker}] IOC 실패 → 시장가 폴백")
        result = await self._execute_market_buy(ticker, amount, reasons, strategy)
        if result:
            self.last_buy_ts[ticker] = _time.time()
            self.buy_counter[ticker] += 1
        
        return result
    
    async def sell(self, ticker: str, price: float, reason: str = '', 
               qty_ratio: Optional[float] = None, final_stage: bool = False) -> Optional[dict]:
        """
        매도 실행 (전량/부분청산) + 체결 확인
        """
        try:
            # ===== 1. 포지션 정보 가져오기 =====
            logger.info(f"[{ticker}] 매도 시작 - 사유: {reason}")
            
            info = await self._get_position_info(ticker)
            
            if not info:
                logger.warning(f"[{ticker}] 포지션 정보 없음 → 매도 중단")
                return None
            
            qty = float(info.get('qty', 0.0))
            
            if qty <= 0:
                logger.warning(f"[{ticker}] 보유 수량 0 이하 → 매도 중단")
                return None
            
            # ===== 2. 청산 수량 계산 =====
            if qty_ratio is not None:
                sell_qty = qty * float(qty_ratio)
                logger.info(f"[{ticker}] 부분 청산: {qty_ratio*100:.0f}% → 매도 수량 {sell_qty:.8f}")
            else:
                sell_qty = qty
                logger.info(f"[{ticker}] 전량 청산 → 매도 수량 {sell_qty:.8f}")
            
            # ===== 3. 최소주문금액 검증 =====
            cur_price = await self.market_data.get_price(ticker)
            
            if not cur_price:
                logger.error(f"[{ticker}] 현재가 조회 실패 → 매도 중단")
                return None
            
            order_value = sell_qty * cur_price
            min_order = self.config.MIN_ORDER_AMOUNT
            
            # 손절/하드손절은 최소 주문금액 무시
            is_stop_loss = '손절' in reason or 'SL' in reason or '하드' in reason
            
            if not is_stop_loss and not final_stage and (order_value < min_order):
                logger.warning(f"[{ticker}] 주문 금액 부족 ({order_value:,.0f}원 < {min_order:,.0f}원) → 매도 보류")
                return None
            
            if is_stop_loss and (order_value < min_order):
                logger.warning(f"🚨 [{ticker}] 손절이므로 최소 주문금액 무시하고 강제 매도")
            
            # ===== 4. 시장가 매도 주문 =====
            logger.info(f"[{ticker}] 시장가 매도 주문 실행 (수량: {sell_qty:.8f})")
            
            result = await asyncio.to_thread(
                self.upbit.sell_market_order,
                ticker,
                sell_qty
            )
            
            if not result or 'uuid' not in result:
                logger.error(f"❌ [{ticker}] 매도 주문 실패: result가 None")
                return None
            
            order_uuid = result['uuid']
            logger.info(f"✅ [{ticker}] 매도 주문 접수: {order_uuid}")
            
            # ===== 5. 체결 대기 (중요!) =====
            filled_order = await self.wait_for_fill(order_uuid, timeout=10.0)
            
            if not filled_order:
                logger.error(f"❌ [{ticker}] 매도 체결 실패 또는 타임아웃")
                return None
            
            # ===== 6. 체결 정보 추출 =====
            filled_qty = float(filled_order.get('executed_volume', 0))
            avg_price = float(filled_order.get('avg_price', 0))
            paid_fee = float(filled_order.get('paid_fee', 0))
            trades_count = filled_order.get('trades_count', 0)
            
            # ===== avg_price 검증 추가 (중요!) =====
            if avg_price == 0:
                trades = filled_order.get('trades', [])
                if trades:
                    total_value = sum(float(t.get('funds', 0)) for t in trades)
                    total_volume = sum(float(t.get('volume', 0)) for t in trades)
                    avg_price = total_value / total_volume if total_volume > 0 else 0
                    logger.warning(f"[{ticker}] 매도 avg_price=0, trades로 계산: {avg_price}")
                    
            # ===== 7. 검증 =====
            if avg_price == 0 or filled_qty == 0:
                logger.error(f"❌ [{ticker}] 매도 체결가/수량이 0 (체결가: {avg_price}, 수량: {filled_qty})")
                return None
            
            logger.info(f"✅ [{ticker}] 매도 체결: {filled_qty:.8f} @ {avg_price:,.0f}원 (수수료: {paid_fee:,.0f}원, 체결횟수: {trades_count})")
            
            # ===== 8. 텔레그램 알림 =====
            await self._notify_sell(ticker, filled_qty, avg_price, reason)
            
            # ===== 9. 체결 정보 반환 (중요!) =====
            return {
                'uuid': order_uuid,
                'market': ticker,
                'executed_volume': filled_qty,
                'avg_price': avg_price,  # ← 중요!
                'trades_count': trades_count,
                'paid_fee': paid_fee,
                'state': 'done'
            }
            
        except Exception as e:
            logger.error(f"❌ [{ticker}] 매도 오류: {e}", exc_info=True)
            self.metrics['sell_error'] += 1
            return None
    
    async def _execute_ioc_ladder(self, ticker: str, amount: float,
                              reasons: list, strategy: str) -> Optional[dict]:
        """
        IOC 래더 (avg_price 검증 강화)
        """
        ob = await self.market_data.get_orderbook(ticker, depth=2)
        if not ob or not ob.get('orderbook_units'):
            return None
        
        ask0 = float(ob['orderbook_units'][0]['ask_price'])
        
        for bps in self.config.BUY_RETRY_LADDER_BPS:
            price = ask0 * (1 + bps / 10000.0)
            
            try:
                # pyupbit IOC 주문 (타임아웃 설정)
                result = await asyncio.wait_for(
                    asyncio.to_thread(
                        self.upbit.buy_limit_order,
                        ticker,
                        price,
                        amount / price
                    ),
                    timeout=self.config.LIMIT_IOC_TIMEOUT
                )
                
                if result and result.get('uuid'):
                    order_uuid = result['uuid']
                    logger.info(f"[{ticker}] IOC 주문 접수: {order_uuid} (+{bps}bps)")
                    
                    # ===== 체결 대기 =====
                    filled_order = await self.wait_for_fill(order_uuid, timeout=5.0)
                    
                    if filled_order:
                        filled_qty = float(filled_order.get('executed_volume', 0))
                        avg_price = float(filled_order.get('avg_price', 0))
                        
                        # ===== avg_price 검증 (중요!) =====
                        if avg_price == 0:
                            trades = filled_order.get('trades', [])
                            if trades:
                                total_value = sum(float(t.get('funds', 0)) for t in trades)
                                total_volume = sum(float(t.get('volume', 0)) for t in trades)
                                avg_price = total_value / total_volume if total_volume > 0 else 0
                                logger.warning(f"[{ticker}] IOC avg_price=0, trades로 계산: {avg_price}")
                        
                        if avg_price == 0 or filled_qty == 0:
                            logger.warning(f"[{ticker}] IOC 체결가/수량이 0")
                            continue
                        
                        if filled_qty * avg_price >= self.config.MIN_ORDER_AMOUNT:
                            logger.info(f"✅ [{ticker}] IOC 체결: {filled_qty:.8f} @ {avg_price:.2f} (+{bps}bps)")
                            await self._notify_buy(ticker, filled_qty, avg_price, reasons)
                            
                            return {
                                'uuid': order_uuid,
                                'market': ticker,
                                'executed_volume': filled_qty,
                                'avg_price': avg_price,
                                'state': 'done'
                            }
            
            except asyncio.TimeoutError:
                logger.debug(f"[{ticker}] IOC 타임아웃 (+{bps}bps)")
                continue
            except Exception as e:
                logger.debug(f"[{ticker}] IOC 실패 (+{bps}bps): {e}")
                continue
        
        return None
    
    async def _execute_market_buy(self, ticker: str, amount: float,
                              reasons: list, strategy: str) -> Optional[dict]:
        """
        시장가 매수 (avg_price 검증 강화)
        """
        try:
            logger.info(f"[{ticker}] 시장가 매수 주문 실행...")
            
            # 잔고 확인 추가
            krw_balance = await asyncio.to_thread(self.upbit.get_balance, 'KRW')
            if not krw_balance or krw_balance < amount:
                logger.error(f"[{ticker}] 잔고 부족: {krw_balance:,.0f}원 < {amount:,.0f}원")
                return None
            
            if self.config.BUY_MARKET_USES_AMOUNT:
                result = await asyncio.to_thread(
                    self.upbit.buy_market_order,
                    ticker,
                    amount
                )
            else:
                ob = await self.market_data.get_orderbook(ticker, depth=1)
                if not ob:
                    return None
                ask0 = float(ob['orderbook_units'][0]['ask_price'])
                max_price = ask0 * (1 + self.config.MARKET_FALLBACK_BPS_CAP / 10000.0)
                result = await asyncio.to_thread(
                    self.upbit.buy_limit_order,
                    ticker,
                    max_price,
                    amount / max_price
                )
            
            if not result or 'uuid' not in result:
                logger.error(f"❌ [{ticker}] 시장가 매수 주문 실패")
                return None
            
            order_uuid = result['uuid']
            logger.info(f"✅ [{ticker}] 시장가 매수 주문 접수: {order_uuid}")
            
            # ===== 체결 대기 =====
            filled_order = await self.wait_for_fill(order_uuid, timeout=10.0)
            
            if not filled_order:
                logger.error(f"❌ [{ticker}] 시장가 매수 체결 실패 또는 타임아웃")
                return None
            
            # ===== 체결 정보 추출 =====
            filled_qty = float(filled_order.get('executed_volume', 0))
            avg_price = float(filled_order.get('avg_price', 0))
            
            # ===== avg_price 검증 (중요!) =====
            if avg_price == 0:
                # trades 정보에서 계산
                trades = filled_order.get('trades', [])
                if trades:
                    total_value = sum(float(t.get('funds', 0)) for t in trades)
                    total_volume = sum(float(t.get('volume', 0)) for t in trades)
                    avg_price = total_value / total_volume if total_volume > 0 else 0
                    logger.warning(f"[{ticker}] avg_price=0, trades로 계산: {avg_price}")
            
            if avg_price == 0 or filled_qty == 0:
                logger.error(f"❌ [{ticker}] 체결가/수량이 0")
                return None
            
            logger.info(f"✅ [{ticker}] 시장가 체결: {filled_qty:.8f} @ {avg_price:,.0f}원")
            
            await self._notify_buy(ticker, filled_qty, avg_price, reasons)
            
            # ===== 체결 정보 반환 =====
            return {
                'uuid': order_uuid,
                'market': ticker,
                'executed_volume': filled_qty,
                'avg_price': avg_price,
                'state': 'done'
            }
            
        except Exception as e:
            logger.error(f"❌ [{ticker}] 시장가 매수 실패: {e}", exc_info=True)
            self.metrics['buy_market_error'] += 1
            return None
    
    async def _check_slippage_gate(self, ticker: str, amount: float) -> bool:
        """
        슬리피지 게이트 검증
        """
        ob = await self.market_data.get_orderbook(ticker, depth=5)
        if not ob:
            return False
        
        units = ob['orderbook_units']
        ask0 = float(units[0]['ask_price'])
        
        # VWAP 계산
        cum_qty = 0.0
        cum_value = 0.0
        
        for u in units:
            ap = float(u['ask_price'])
            aq = float(u['ask_size'])
            cum_qty += aq
            cum_value += ap * aq
            
            if cum_value >= amount:
                break
        
        vwap = cum_value / max(cum_qty, 1e-9)
        slip_pct = (vwap - ask0) / max(ask0, 1e-9)
        threshold = self.config.SLIPPAGE_TOLERANCE_PCT
        
        passed = slip_pct <= threshold
        
        if not passed:
            logger.debug(f"[{ticker}] 슬리피지 초과: {slip_pct:.4f} > {threshold:.4f}")
        
        return passed
    
    async def _get_position_info(self, ticker: str) -> Optional[dict]:
        """
        보유 정보 조회 (hold 우선, 없으면 Upbit API)
        """
        try:
            # ===== 1. hold 딕셔너리 우선 확인 =====
            if hasattr(self, 'main_instance') and hasattr(self.main_instance, 'hold'):
                hold_info = self.main_instance.hold.get(ticker)
                if hold_info:
                    qty = float(hold_info.get('qty', 0))
                    if qty > 0:
                        logger.info(f"[{ticker}] hold에서 포지션 정보 가져옴: qty={qty}")
                        return {
                            'qty': qty,
                            'locked': 0.0,
                            'currency': ticker.replace('KRW-', '')
                        }
            
            # ===== 2. Upbit API 조회 =====
            balances = await asyncio.to_thread(self.upbit.get_balances)
            
            if not balances:
                return None
            
            # ticker에서 코인 심볼 추출 (KRW-BTC -> BTC)
            currency = ticker.replace('KRW-', '')
            
            # 해당 코인의 잔고 찾기
            for balance in balances:
                if balance.get('currency') == currency:
                    qty = float(balance.get('balance', 0))
                    locked = float(balance.get('locked', 0))
                    
                    if qty > 0:
                        return {
                            'qty': qty,
                            'locked': locked,
                            'currency': currency
                        }
            
            return None
            
        except Exception as e:
            logger.error(f"[{ticker}] _get_position_info 오류: {e}", exc_info=True)
            return None
    
    async def _notify_buy(self, ticker: str, qty: float, price: float, reasons: list):
        """매수 텔레그램 알림"""
        msg = f"🔵 매수: {ticker}\n"
        msg += f"수량: {qty:.8f}\n"
        msg += f"가격: {price:,.0f}원\n"
        
        if reasons:
            msg += f"사유: {', '.join(reasons[:3])}"
        
        try:
            await asyncio.to_thread(self.messenger.send_message, msg)
        except Exception:
            pass
    
    async def _notify_sell(self, ticker: str, qty: float, price: float, reason: str):
        """매도 텔레그램 알림"""
        msg = f"🔴 매도: {ticker}\n"
        msg += f"수량: {qty:.8f}\n"
        msg += f"가격: {price:,.0f}원\n"
        msg += f"사유: {reason}"
        
        try:
            await asyncio.to_thread(self.messenger.send_message, msg)
        except Exception:
            pass
