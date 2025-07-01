from flask_cors import CORS, cross_origin
import requests
import csv
import time
from datetime import datetime
from flask import Flask, send_file
import threading

CSV_FILE = 'data.csv'

# Create CSV if not exists
try:
    with open(CSV_FILE, 'x', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'price', 'bid', 'ask', 'spread', 'volume'])
except FileExistsError:
    pass

def fetch_coinbase_data():
    book_url = 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=1'
    book = requests.get(book_url).json()

    if 'bids' in book and 'asks' in book and book['bids'] and book['asks']:
        bid = float(book['bids'][0][0])
        ask = float(book['asks'][0][0])
    else:
        return None  # or handle as needed

    mid_price = round((bid + ask) / 2, 2)
    spread = round(ask - bid, 8)
    volume = 0.0
    timestamp = datetime.utcnow().isoformat()

    return [timestamp, mid_price, bid, ask, spread, volume]

def log_data():
    try:
        data = fetch_coinbase_data()
        with open(CSV_FILE, 'a', newline='') as f:
            csv.writer(f).writerow(data)
        print(f"Logged: {data}")
    except Exception as e:
        print(f"Error: {e}")

# --- Flask server setup ---
app = Flask(__name__)
CORS(app)  # ✅ this line allows Vercel to read your CSV

@app.route('/')
def home():
    return '<h1>BTC Logger</h1><a href="/data.csv">Download CSV</a>'

@app.route('/data.csv')
@cross_origin()  # ✅ this forces CORS to work
def serve_csv():
    return send_file(CSV_FILE, mimetype='text/csv')

def start_server():
    app.run(host='0.0.0.0', port=10000)

# --- Main Program ---
if __name__ == "__main__":
    threading.Thread(target=start_server, daemon=True).start()
    print("Starting logger...")
    while True:
        log_data()
        time.sleep(60)
