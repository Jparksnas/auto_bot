import sqlite3
import pandas as pd
import os

# DB 파일 경로 확인
db_path = 'log/trading_log_20251011.db'
if not os.path.exists(db_path):
    print(f"에러: {db_path} 파일이 존재하지 않습니다.")
    print("main.py를 먼저 실행하여 DB 파일을 생성하세요.")
    exit()

# DB 연결
conn = sqlite3.connect(db_path)

# 테이블 목록 확인
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("DB에 존재하는 테이블:", tables)

# 전체 로그 조회
try:
    df = pd.read_sql_query("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 100", conn)
    print("\n전체 로그 (최근 100개):")
    print(df)
except Exception as e:
    print(f"로그 조회 실패: {e}")

# 특정 레벨만 조회 (예: WARNING 이상)
try:
    df_critical = pd.read_sql_query("SELECT * FROM logs WHERE level IN ('WARNING', 'ERROR', 'CRITICAL') ORDER BY timestamp DESC", conn)
    print("\n핵심 로그 (WARNING 이상):")
    print(df_critical)
    
    # CSV로 추출
    df_critical.to_csv('trading_log_critical.csv', index=False)
    print("\n핵심 로그를 'trading_log_critical.csv'로 저장했습니다.")
except Exception as e:
    print(f"핵심 로그 조회 실패: {e}")

conn.close()