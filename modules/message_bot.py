import time
import hashlib
import requests
import os
from dotenv import load_dotenv

load_dotenv()
# TELEGRAM_BOT_TOKEN(문서)과 TELEGRAM_TOKEN(현행) 모두 지원
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

class Message_Bot:
    """
    Telegram 메시지 전송 헬퍼
    - 최소 전송 간격(throttle_sec) 이하 반복 전송 차단
    - 동일 메시지를 short_window 동안 한 번만 전송(중복 억제)
    """

    def __init__(self, token: str = TELEGRAM_TOKEN,
                 chat_id: str = TELEGRAM_CHAT_ID,
                 throttle_sec: float = 1.5,
                 short_window: int = 30):
        self.token = token
        self.chat_id = chat_id
        self.throttle = throttle_sec
        self.short_window = short_window
        self._last_sent_ts = 0.0
        self._msg_cache = {}  # {msg_hash: last_ts}

    def _too_fast(self) -> bool:
        return time.time() - self._last_sent_ts < self.throttle

    def _is_duplicate(self, text: str) -> bool:
        h = hashlib.sha1(text.encode()).hexdigest()
        now = time.time()
        last = self._msg_cache.get(h, 0)
        if now - last < self.short_window:
            return True
        self._msg_cache[h] = now
        for k, t_ in list(self._msg_cache.items()):
            if now - t_ > self.short_window:
                self._msg_cache.pop(k, None)
        return False

    def send_message(self, text: str):
        if self._too_fast() or self._is_duplicate(text):
            return  # 전송 생략

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}
        try:
            resp = requests.post(url, json=payload)
            if not resp.ok:
                print(f"[Telegram 전송 실패] {resp.json()}")
            self._last_sent_ts = time.time()
        except Exception as e:
            print(f"[Telegram 전송 오류] {e}")