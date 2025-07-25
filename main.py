import requests
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import schedule
import time
from datetime import datetime, timedelta

# SQLite DB 초기화
conn = sqlite3.connect("usd_data.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS usd_prices (
    timestamp TEXT PRIMARY KEY,
    dxy REAL,
    usdkrw REAL,
    usdt REAL
)
""")
conn.commit()


# 데이터 수집 함수
def fetch_and_store_data():
    try:
        # 1. 달러지수 (DXY) - TradingView API (공식 아님, fallback용 사용)
        dxy_resp = requests.get("https://api.polygon.io/v1/last_quote/currencies/USD/DXY?apiKey=YOUR_API_KEY")
        dxy = dxy_resp.json().get('last', {}).get('ask', None)
        if not dxy:  # 대체 API 사용
            dxy_resp = requests.get("https://api.exchangerate.host/latest?base=USD")
            dxy = dxy_resp.json().get('rates', {}).get('EUR', 0)

        # 2. 원달러환율
        usdkrw_resp = requests.get("https://api.exchangerate.host/latest?base=USD&symbols=KRW")
        usdkrw = usdkrw_resp.json()['rates']['KRW']

        # 3. 빗썸의 USDT 가격
        usdt_resp = requests.get("https://api.bithumb.com/public/ticker/USDT_KRW")
        usdt = float(usdt_resp.json()['data']['closing_price'])

        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("""
        INSERT OR IGNORE INTO usd_prices (timestamp, dxy, usdkrw, usdt)
        VALUES (?, ?, ?, ?)
        """, (now, dxy, usdkrw, usdt))
        conn.commit()

        print(f"[{now}] 저장 완료 | DXY: {dxy:.2f}, USD/KRW: {usdkrw:.2f}, USDT: {usdt:.2f}")
    except Exception as e:
        print(f"[에러] {e}")


# 그래프 그리기 함수 (마지막 30일)
def plot_graph():
    try:
        df = pd.read_sql_query("SELECT * FROM usd_prices", conn, parse_dates=['timestamp'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # 30일 윈도우로 필터링
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        df = df[df['timestamp'] >= thirty_days_ago]

        if df.empty:
            print("데이터가 충분하지 않습니다.")
            return

        plt.figure(figsize=(14, 6))
        plt.plot(df['timestamp'], df['dxy'], label="달러지수 (DXY)")
        plt.plot(df['timestamp'], df['usdkrw'], label="USD/KRW")
        plt.plot(df['timestamp'], df['usdt'], label="USDT (빗썸)")

        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d\n%H:%M'))
        plt.xticks(rotation=45)
        plt.legend()
        plt.title("30일간 USD 관련 지표")
        plt.xlabel("시간")
        plt.ylabel("가격")
        plt.tight_layout()
        plt.grid(True)
        plt.show()
    except Exception as e:
        print(f"[그래프 에러] {e}")


# 스케줄 등록
schedule.every(1).minutes.do(fetch_and_store_data)

print("수집 시작... (1분 간격)")
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("종료됨")
    conn.close()
