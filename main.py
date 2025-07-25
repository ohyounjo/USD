import requests
import pandas as pd
import matplotlib.pyplot as plt
import schedule
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, Column, Float, DateTime, MetaData

# MySQL 연결 설정 (사용자에 맞게 수정)
DB_USER = 'root'
DB_PASS = ''
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'market_data'

# SQLAlchemy 엔진 생성
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
metadata = MetaData()

# 테이블 정의
market_table = Table(
    'market_prices', metadata,
    Column('timestamp', DateTime, primary_key=True),
    Column('dxy', Float),
    Column('usdkrw', Float),
    Column('usdt_bithumb', Float),
)

# 테이블 생성 (없을 경우)
metadata.create_all(engine)


# 데이터 수집 함수
def fetch_and_store():
    try:
        timestamp = datetime.now()

        # 1. 달러 인덱스 (DXY)
        dxy_resp = requests.get("https://api.investing.com/api/financialdata/169", headers={"User-Agent": "Mozilla/5.0"})
        dxy_json = dxy_resp.json()
        dxy = float(dxy_json["data"]["last"]["value"])

        # 2. 원/달러 환율 (USD/KRW)
        fx_resp = requests.get("https://api.investing.com/api/financialdata/2111", headers={"User-Agent": "Mozilla/5.0"})
        fx_json = fx_resp.json()
        usdkrw = float(fx_json["data"]["last"]["value"])

        # 3. 빗썸 USDT 가격
        usdt_resp = requests.get("https://api.bithumb.com/public/ticker/USDT_KRW")
        usdt_json = usdt_resp.json()
        usdt_bithumb = float(usdt_json["data"]["closing_price"])

        # 저장
        with engine.connect() as conn:
            conn.execute(market_table.insert().values(
                timestamp=timestamp,
                dxy=dxy,
                usdkrw=usdkrw,
                usdt_bithumb=usdt_bithumb,
            ))

        print(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] DXY={dxy} USD/KRW={usdkrw} USDT(Bithumb)={usdt_bithumb}")

    except Exception as e:
        print("❌ Error during fetch/store:", e)


# 그래프 그리기 함수
def draw_graph():
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM market_prices", conn, parse_dates=["timestamp"])

        # 30일 전부터 필터
        cutoff = datetime.now() - timedelta(days=30)
        df = df[df['timestamp'] >= cutoff]

        if df.empty:
            print("⏳ 30일 내 데이터가 없습니다.")
            return

        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['dxy'], label='DXY')
        plt.plot(df['timestamp'], df['usdkrw'], label='USD/KRW')
        plt.plot(df['timestamp'], df['usdt_bithumb'], label='USDT (Bithumb)')

        plt.xlabel("시간")
        plt.ylabel("가격")
        plt.title("시장 지표 - 30일 데이터")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print("❌ Error during plotting:", e)


# 1분마다 실행
schedule.every(1).minutes.do(fetch_and_store)

# 첫 실행 직후 그래프를 자동 생성하려면 여기에 호출
# draw_graph()

print("✅ 프로그램 시작. 1분마다 수집합니다.")
while True:
    schedule.run_pending()
    time.sleep(1)
