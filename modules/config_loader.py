import json, asyncio
import os

# ✅ 모듈 파일 기준으로 config.json 고정
_BASE_DIR = os.path.dirname(__file__)
_CONFIG_PATH = os.path.join(_BASE_DIR, "config.json")

class Config:
    _data = {}
    _lock = asyncio.Lock()

    @classmethod
    async def load(cls):
        async with cls._lock:
            if not os.path.exists(_CONFIG_PATH):
                cls._data = {}
                with open(_CONFIG_PATH, "w") as f:
                    json.dump(cls._data, f, indent=2)
            else:
                with open(_CONFIG_PATH, "r") as f:
                    cls._data = json.load(f)

    @classmethod
    def get(cls, key, default=None):
        return cls._data.get(key, default)

    @classmethod
    async def set_and_save(cls, key, val):
        async with cls._lock:
            cls._data[key] = val
            with open(_CONFIG_PATH, "w") as f:
                json.dump(cls._data, f, indent=2)