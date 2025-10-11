from pymongo import MongoClient
import os
from dotenv import load_dotenv
import datetime
import certifi

load_dotenv()

class MongodbConnect:
    def __init__(self, db_name):
        try:
            client = MongoClient(
                os.getenv("db_address"),
                tls=True,
                tlsCAFile=certifi.where(),
            )
            self.db = client[db_name]
            self.trades = self.db["trades"]  # ✅ 여기에 trades 속성 추가
            print(f"✅ MongoDB Atlas '{db_name}' 연결 성공")
        except Exception as error:
            print("❌ MongoDB 연결 실패:", error)
    
    # ★ 컬렉션 속성 포워딩 지원
    def __getattr__(self, name: str):
        # 예: db.trades → self.db['trades']
        try:
            return self.db[name]
        except Exception as e:
            raise AttributeError(f"'MongodbConnect' has no attribute '{name}'") from e

    def insert_one(self, collection_name, data):
        try:
            print(f"➡️ '{collection_name}' 컬렉션에 데이터 삽입 시도 중: {data}")
            result = self.db[collection_name].insert_one(data)
            print(f"📦 삽입 성공, ID: {result.inserted_id}")
        except Exception as e:
            print("❌ 데이터 삽입 실패:", e)

if __name__ == "__main__":
    db = MongodbConnect("coin")
    db.insert_one("trades", {
        "type": "test",
        "ticker": "KRW-BTC",
        "message": "Atlas 테스트 삽입",
        "datetime": datetime.datetime.now().isoformat()
    })
