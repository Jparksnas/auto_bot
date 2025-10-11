# utils.py
"""
공통 유틸리티 함수
로깅, 포맷팅, 안전 호출, 쓰로틀링, 재시도 로직
"""
import asyncio
import logging
import time as _time
from typing import Optional, Callable, Any
from collections import defaultdict
from functools import wraps

logger = logging.getLogger(__name__)

class Utils:
    """공통 유틸리티"""
    
    # 쓰로틀링 상태
    _throttle_state = defaultdict(lambda: {'last_ts': 0.0, 'count': 0})
    
    @staticmethod
    async def safe_call(fn: Callable, *args, retries: int = 3, 
                       backoff: float = 1.0, **kwargs) -> Optional[Any]:
        """
        안전한 함수 호출 (재시도 + 백오프)
        """
        for attempt in range(1, retries + 1):
            try:
                if asyncio.iscoroutinefunction(fn):
                    return await fn(*args, **kwargs)
                else:
                    return fn(*args, **kwargs)
            except Exception as e:
                if attempt >= retries:
                    logger.error(f"[safe_call] 최종 실패 after {retries}회: {fn.__name__} - {e}")
                    return None
                
                wait = backoff * (1.5 ** (attempt - 1))
                logger.warning(f"[safe_call] 재시도 {attempt}/{retries}: {fn.__name__} - {e} (대기: {wait:.1f}s)")
                await asyncio.sleep(wait)
        
        return None
    
    @staticmethod
    def throttled_log(key: str, msg: str, interval: float = 5.0, 
                     level: str = 'info', max_burst: int = 3):
        """
        쓰로틀링 로그 (동일 키 반복 방지)
        """
        now = _time.time()
        state = Utils._throttle_state[key]
        
        elapsed = now - state['last_ts']
        
        # 버스트 허용
        if elapsed < interval and state['count'] >= max_burst:
            return
        
        # 로그 출력
        log_func = getattr(logger, level, logger.info)
        log_func(msg)
        
        # 상태 갱신
        if elapsed >= interval:
            state['count'] = 1
            state['last_ts'] = now
        else:
            state['count'] += 1
    
    @staticmethod
    def fmt_krw(x: float) -> str:
        """원화 포맷팅"""
        return f"{x:,.0f}" if x else "0"
    
    @staticmethod
    def fmt_pct(x: float, sign: bool = True) -> str:
        """퍼센트 포맷팅"""
        if sign:
            return f"{x:+.2f}%"
        return f"{x:.2f}%"
    
    @staticmethod
    def fmt_duration(seconds: float) -> str:
        """시간 포맷팅 (초 → 분:초)"""
        m, s = divmod(int(seconds), 60)
        return f"{m}분 {s}초" if m else f"{s}초"
    
    @staticmethod
    def truncate_str(s: str, max_len: int = 50) -> str:
        """문자열 자르기"""
        return s if len(s) <= max_len else s[:max_len - 3] + '...'
    
    @staticmethod
    def batch_list(lst: list, batch_size: int) -> list:
        """리스트 배치 분할"""
        return [lst[i:i + batch_size] for i in range(0, len(lst), batch_size)]
    
    @staticmethod
    async def gather_with_concurrency(n: int, *tasks):
        """동시성 제한 gather"""
        semaphore = asyncio.Semaphore(n)
        
        async def bounded_task(task):
            async with semaphore:
                return await task
        
        return await asyncio.gather(*[bounded_task(t) for t in tasks])
    
    @staticmethod
    def rate_limit(max_calls: int, period: float):
        """
        Rate limiting 데코레이터
        """
        call_times = []
        
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                now = _time.time()
                
                # 오래된 기록 제거
                while call_times and now - call_times[0] > period:
                    call_times.pop(0)
                
                # Rate limit 체크
                if len(call_times) >= max_calls:
                    wait = period - (now - call_times[0])
                    if wait > 0:
                        await asyncio.sleep(wait)
                
                call_times.append(_time.time())
                return await func(*args, **kwargs)
            
            return wrapper
        return decorator
    
    @staticmethod
    def log_sample(key: str, rate: float = 0.1) -> bool:
        """
        샘플링 로그 (일부만 출력)
        """
        import random
        return random.random() < rate
    
    @staticmethod
    def dict_subset(d: dict, keys: list) -> dict:
        """딕셔너리 서브셋 추출"""
        return {k: d[k] for k in keys if k in d}
    
    @staticmethod
    def safe_float(x, default: float = 0.0) -> float:
        """안전한 float 변환"""
        try:
            return float(x) if x is not None else default
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def clamp(x: float, min_val: float, max_val: float) -> float:
        """값 범위 제한"""
        return max(min_val, min(max_val, x))
