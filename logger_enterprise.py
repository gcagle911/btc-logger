import requests
import csv
import time
from datetime import datetime
from flask import Flask, send_file
import threading

CSV_FILE = 'BTC_bid_ask_spread_2025-06-27.csv'

# Create CSV with full headers if not exists
try:
    with open(CSV_FILE, 'x', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp', 'symbol', 'price', 'bid', 'ask',
            'spread', 'midprice', 'bid_volume', 'ask_volume', 'exchange'
        ])
except FileExistsError:
    pass

def fetch_coinbase_data():
    ticker_url = 'https://api.coinbase.com/v2/exchange-rates?currency=BTC'
    book_url = 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=1'

    ticker = requests.get(ticker_url).json()
    book = requests.get(book_url).json()

    price = float(ticker['data']['rates']['USD'])

    if 'bids' in book and 'asks' in book and book['bids'] and book['asks']:
        bid = float(book['bids'][0][0])
        ask = float(book['asks'][0][0])
        bid_volume = float(book['bids'][0][1])
        ask_volume = float(book['asks'][0][1])
    else:
        bid = price * 0.999
        ask = price * 1.001
        bid_volume = 0.0
        ask_volume = 0.0

    spread = ask - bid
    midprice = (bid + ask) / 2
    timestamp = datetime.utcnow().isoformat()

    return [
        timestamp, 'BTCUSD', price, bid, ask,
        spread, midprice, bid_volume, ask_volume, 'Coinbase'
    ]

def log_data():
    try:
        data = fetch_coinbase_data()
        with open(CSV_FILE, 'a', newline='') as f:
            csv.writer(f).writerow(data)
        print(f"[ENTERPRISE] Logged: {data}")
    except Exception as e:
        print(f"Error: {e}")

# Optional: Flask access to the new CSV (optional, disable if unnecessary)
app = Flask(__name__)

@app.route('/enterprise.csv')
def serve_enterprise_csv():
    return send_file(CSV_FILE)

def start_server():
    app.run(host='0.0.0.0', port=10001)

if __name__ == "__main__":
    threading.Thread(target=start_server, daemon=True).start()
    print("Starting enterprise logger...")
    while True:
        log_data()
        time.sleep(60)
