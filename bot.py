import redis
import json
import os
import ccxt
import time
import threading
import requests
from flask import Flask, send_file
import glob
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import pandas as pd
import numpy as np

# === CONFIG ===
BOT_TOKEN = os.getenv('BOT_TOKEN', '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE')
CHAT_ID = os.getenv('CHAT_ID', '655537138')
REDIS_HOST = os.getenv('REDIS_HOST', 'climbing-narwhal-53855.upstash.io')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'AdJfAAIjcDEzNDdhYTU4OGY1ZDc0ZWU3YmQzY2U0MTVkNThiNzU0OXAxMA')
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 0.5
MAX_SMALL_BODY_PCT = 1.5
MIN_LOWER_WICK_PCT = 10.0
MAX_WORKERS = 3
BATCH_DELAY = 5.0
CAPITAL = 10.0
SL_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 30
CLOSED_TRADE_CSV = '/tmp/closed_trades.csv'
RSI_PERIOD = 14
ADX_PERIOD = 14
ZIGZAG_DEPTH = 12
ZIGZAG_DEVIATION = 5.0
ZIGZAG_BACKSTEP = 3
ZIGZAG_TOLERANCE = 0.005
NUM_CHUNKS = 4

# === PROXY ===
proxies = {
    "http": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712",
    "https": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712"
}

# === Redis Client ===
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
    ssl=True
)

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        redis_client.set('open_trades', json.dumps(open_trades, default=str))
        print("Trades saved to Redis")
        send_telegram("✅ Trades saved to Redis")
    except Exception as e:
        print(f"Error saving trades to Redis: {e}")
        send_telegram(f"❌ Error saving trades to Redis: {e}")

def load_trades():
    global open_trades
    try:
        data = redis_client.get('open_trades')
        if data:
            open_trades = json.loads(data)
            print(f"Loaded {len(open_trades)} trades from Redis")
        else:
            open_trades = {}
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Redis: {e}")
        open_trades = {}
    except Exception as e:
        print(f"Error loading trades from Redis: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = load_closed_trades()
        all_closed_trades.append(closed_trade)
        redis_client.set('closed_trades', json.dumps(all_closed_trades, default=str))
        print("Closed trade saved to Redis")
        send_telegram("✅ Closed trade saved to Redis")
    except Exception as e:
        print(f"Error saving closed trades to Redis: {e}")
        send_telegram(f"❌ Error saving closed trades to Redis: {e}")

def load_closed_trades():
    try:
        data = redis_client.get('closed_trades')
        if data:
            return json.loads(data)
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from Redis: {e}")
        return []
    except Exception as e:
        print(f"Error loading closed trades from Redis: {e}")
        return []

# === TELEGRAM ===
def send_telegram(msg, retries=3):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    for attempt in range(retries):
        try:
            response = requests.post(url, data=data, proxies=proxies, timeout=5).json()
            print(f"Telegram sent: {msg}")
            return response.get('result', {}).get('message_id')
        except Exception as e:
            print(f"Telegram error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, proxies=proxies, timeout=5)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === CSV EXPORT ===
def export_to_csv():
    try:
        all_closed_trades = load_closed_trades()
        closed_trades_df = pd.DataFrame(all_closed_trades)
        if not closed_trades_df.empty:
            mode = 'a' if os.path.exists(CLOSED_TRADE_CSV) else 'w'
            header = not os.path.exists(CLOSED_TRADE_CSV)
            closed_trades_df.to_csv(CLOSED_TRADE_CSV, mode=mode, header=header, index=False)
            print(f"Closed trades appended to {CLOSED_TRADE_CSV}")
            send_telegram(f"📊 Closed trades appended to {CLOSED_TRADE_CSV}")
            send_csv_to_telegram(CLOSED_TRADE_CSV)
        else:
            print("No closed trades to export")
            send_telegram("📊 No closed trades to export")
    except Exception as e:
        print(f"Error in export_to_csv: {e}")
        send_telegram(f"❌ Error in export_to_csv: {e}")

def send_csv_to_telegram(filename):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(filename, 'rb') as f:
            data = {'chat_id': CHAT_ID, 'caption': f"CSV: {filename}"}
            files = {'document': f}
            requests.post(url, data=data, files=files, proxies=proxies)
            print(f"Sent {filename} to Telegram")
            send_telegram(f"📎 Sent {filename} to Telegram")
    except Exception as e:
        print(f"Error sending {filename} to Telegram: {e}")
        send_telegram(f"❌ Error sending {filename} to Telegram: {e}")

# === INIT ===
exchange = ccxt.binance({
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_API_SECRET'),
    'options': {'defaultType': 'future'},
    'proxies': proxies,
    'enableRateLimit': True
})
app = Flask(__name__)

sent_signals = {}
open_trades = {}
closed_trades = []

# === FLASK ENDPOINTS ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

@app.route('/list_files')
def list_files():
    try:
        files = glob.glob('/tmp/*.csv')
        if not files:
            return "No CSV files found", 404
        file_list = "<br>".join([os.path.basename(f) for f in files])
        return f"Available CSV files:<br>{file_list}"
    except Exception as e:
        return f"Error listing files: {str(e)}", 500

@app.route('/download/<filename>')
def download_file(filename):
    try:
        file_path = f"/tmp/{filename}"
        if not os.path.exists(file_path):
            return f"File {filename} not found", 404
        return send_file(file_path, as_attachment=True)
    except Exception as e:
        return f"Error downloading file: {str(e)}", 500

# === CANDLE HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

# === INDICATOR CALCULATIONS ===
def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period + 1:
        return None
    deltas = np.diff(closes)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)
    avg_gain = np.mean(gains[-period:])
    avg_loss = np.mean(losses[-period:])
    if avg_loss == 0:
        return 100 if avg_gain > 0 else 50
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_adx(candles, period=14):
    if len(candles) < period + 1:
        return None
    highs = np.array([c[2] for c in candles])
    lows = np.array([c[3] for c in candles])
    closes = np.array([c[4] for c in candles])
    plus_dm = np.zeros(len(candles) - 1)
    minus_dm = np.zeros(len(candles) - 1)
    tr = np.zeros(len(candles) - 1)
    for i in range(1, len(candles)):
        high_diff = highs[i] - highs[i-1]
        low_diff = lows[i-1] - lows[i]
        plus_dm[i-1] = high_diff if high_diff > low_diff and high_diff > 0 else 0
        minus_dm[i-1] = low_diff if low_diff > 0 else 0
        tr[i-1] = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
    atr = np.mean(tr[-period:])
    if atr == 0:
        return 0
    plus_di = 100 * np.mean(plus_dm[-period:]) / atr
    minus_di = 100 * np.mean(minus_dm[-period:]) / atr
    dx = abs(plus_di - minus_di) / (plus_di + minus_di) if (plus_di + minus_di) != 0 else 0
    return 100 * dx

def calculate_zigzag(candles, depth=12, deviation=5.0, backstep=3):
    highs = np.array([c[2] for c in candles])
    lows = np.array([c[3] for c in candles])
    swing_points = []
    last_high = last_low = None
    direction = 0
    dev = deviation / 100

    for i in range(depth, len(candles) - backstep):
        is_low = True
        for j in range(max(0, i - depth), min(len(candles), i + backstep + 1)):
            if j != i and lows[i] > lows[j]:
                is_low = False
                break
        if is_low:
            if last_low is None or (direction == 1 and lows[i] < last_low[1]):
                if last_low is None or (lows[i] != 0 and (highs[i] - lows[i]) / lows[i] >= dev):
                    last_low = (i, lows[i])
                    if direction == 1:
                        swing_points.append((last_high[0], last_high[1], 'high'))
                        swing_points.append((last_low[0], last_low[1], 'low'))
                        direction = -1
                    elif direction == 0:
                        direction = -1

        is_high = True
        for j in range(max(0, i - depth), min(len(candles), i + backstep + 1)):
            if j != i and highs[i] < highs[j]:
                is_high = False
                break
        if is_high:
            if last_high is None or (direction == -1 and highs[i] > last_high[1]):
                if last_low is None or (lows[i] != 0 and (highs[i] - lows[i]) / lows[i] >= dev):
                    last_high = (i, highs[i])
                    if direction == -1:
                        swing_points.append((last_low[0], last_low[1], 'low'))
                        swing_points.append((last_high[0], last_high[1], 'high'))
                        direction = 1
                    elif direction == 0:
                        direction = 1

    return swing_points

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_red_0 = (
        is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_green and small_red_1 and small_red_0 and volume_decreasing

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    )
    small_green_0 = (
        is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    )
    volume_decreasing = c1[5] > c0[5]
    return big_red and small_green_1 and small_green_0 and volume_decreasing

# === SYMBOLS ===
def get_symbols():
    markets = exchange.load_markets()
    symbols = [
        s for s in markets
        if s.endswith('USDT') and
           markets[s]['contract'] and
           markets[s].get('active') and
           markets[s].get('info', {}).get('status') == 'TRADING' and
           len(s.split('/')[0]) <= 10
    ]
    print(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
    send_telegram(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
    return symbols

# === CANDLE CLOSE ===
def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL CHECK ===
def check_tp_sl():
    global closed_trades
    while True:
        try:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    last = ticker['last']
                    pnl = 0
                    hit = ""
                    if trade['side'] == 'buy':
                        if last >= trade['tp']:
                            pnl = (trade['tp'] - trade['entry']) / trade['entry'] * 100
                            hit = "✅ TP hit"
                        elif last <= trade['sl']:
                            pnl = (trade['sl'] - trade['entry']) / trade['entry'] * 100
                            hit = "❌ SL hit"
                    else:
                        if last <= trade['tp']:
                            pnl = (trade['entry'] - trade['tp']) / trade['entry'] * 100
                            hit = "✅ TP hit"
                        elif last >= trade['sl']:
                            pnl = (trade['entry'] - trade['sl']) / trade['entry'] * 100
                            hit = "❌ SL hit"
                    if hit:
                        profit = CAPITAL * pnl / 100
                        closed_trade = {
                            'symbol': sym,
                            'side': trade['side'],
                            'entry': trade['entry'],
                            'tp': trade['tp'],
                            'sl': trade['sl'],
                            'pnl': profit,
                            'pnl_pct': pnl,
                            'category': trade['category'],
                            'ema_status': str(trade['ema_status']),
                            'rsi': trade['rsi'],
                            'rsi_category': trade['rsi_category'],
                            'adx': trade['adx'],
                            'adx_category': trade['adx_category'],
                            'big_candle_rsi': trade['big_candle_rsi'],
                            'big_candle_rsi_status': trade['big_candle_rsi_status'],
                            'zigzag_status': trade['zigzag_status'],
                            'zigzag_price': trade['zigzag_price'],
                            'close_time': get_ist_time().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        closed_trades.append(closed_trade)
                        save_closed_trades(closed_trade)
                        export_to_csv()  # Export immediately after closing a trade
                        ema_status = trade['ema_status']
                        new_msg = (
                            f"{sym} - {'RISING' if trade['side'] == 'buy' else 'FALLING'} PATTERN\n"
                            f"{'Above' if trade['side'] == 'buy' else 'Below'} 21 ema - {ema_status['price_ema21']}\n"
                            f"ema 9 {'above' if trade['side'] == 'buy' else 'below'} 21 - {ema_status['ema9_ema21']}\n"
                            f"RSI (14) - {trade['rsi']:.2f} ({trade['rsi_category']})\n"
                            f"Big Candle RSI - {trade['big_candle_rsi']:.2f} ({trade['big_candle_rsi_status']})\n"
                            f"ADX (14) - {trade['adx']:.2f} ({trade['adx_category']})\n"
                            f"Zig Zag - {trade['zigzag_status']}\n"
                            f"entry - {trade['entry']}\n"
                            f"tp - {trade['tp']}\n"
                            f"sl - {trade['sl']:.4f}\n"
                            f"Profit/Loss: {pnl:.2f}% (${profit:.2f})\n{hit}"
                        )
                        edit_telegram_message(trade['msg_id'], new_msg)
                        del open_trades[sym]
                        save_trades()
                except Exception as e:
                    print(f"TP/SL check error on {sym}: {e}")
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            print(f"TP/SL loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        for attempt in range(3):
            candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=50)
            if len(candles) < 30:
                print(f"{symbol}: Skipped, insufficient candles ({len(candles)})")
                return
            if attempt < 2 and candles[-1][0] > candles[-2][0]:
                break
            time.sleep(1)

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        adx = calculate_adx(candles, period=ADX_PERIOD)
        big_candle_rsi = calculate_rsi(candles[:-3], period=RSI_PERIOD)
        swing_points = calculate_zigzag(candles, depth=ZIGZAG_DEPTH, deviation=ZIGZAG_DEVIATION, backstep=ZIGZAG_BACKSTEP)
        if ema21 is None or ema9 is None or rsi is None or adx is None or big_candle_rsi is None:
            print(f"{symbol}: Skipped, indicator calculation failed")
            return

        rising = detect_rising_three(candles)
        falling = detect_falling_three(candles)
        if not (rising or falling):
            c2, c1, c0 = candles[-4], candles[-3], candles[-2]
            avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
            reasons = []
            if is_bullish(c2) and not rising:
                if body_pct(c2) < MIN_BIG_BODY_PCT:
                    reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
                if c2[5] <= avg_volume:
                    reasons.append("Big candle volume not above average")
                if not (is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 1 not bearish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if not (is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 0 not bearish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if c1[5] <= c0[5]:
                    reasons.append("Volume not decreasing")
            elif is_bearish(c2) and not falling:
                if body_pct(c2) < MIN_BIG_BODY_PCT:
                    reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
                if c2[5] <= avg_volume:
                    reasons.append("Big candle volume not above average")
                if not (is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 1 not bullish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if not (is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 0 not bullish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if c1[5] <= c0[5]:
                    reasons.append("Volume not decreasing")
            if reasons:
                print(f"{symbol}: No pattern detected. Reasons: {', '.join(reasons)}")
            return

        side = 'buy' if rising else 'sell'
        entry = candles[-1][4]
        sl = entry * (1 - SL_PCT) if rising else entry * (1 + SL_PCT)
        tp = entry * (1 + 2 * SL_PCT) if rising else entry * (1 - 2 * SL_PCT)
        ema_status = {
            'price_ema21': candles[-1][4] > ema21 if rising else candles[-1][4] < ema21,
            'ema9_ema21': ema9 > ema21 if rising else ema9 < ema21
        }
        category = (
            'two_green' if ema_status['price_ema21'] and ema_status['ema9_ema21']
            else 'one_green_one_caution' if ema_status['price_ema21'] or ema_status['ema9_ema21']
            else 'two_cautions'
        )
        rsi_category = (
            'overbought' if rsi > 70
            else 'oversold' if rsi < 30
            else 'neutral'
        )
        adx_category = 'strong' if adx >= 25 else 'weak'
        big_candle_rsi_status = (
            'overbought' if big_candle_rsi > 75 and rising
            else 'oversold' if big_candle_rsi < 25 and falling
            else 'neutral'
        )
        zigzag_status = 'none'
        zigzag_price = None
        if swing_points:
            last_swing = swing_points[-1]
            zigzag_price = last_swing[1]
            zigzag_status = last_swing[2]
            if zigzag_status == 'low' and abs(candles[-1][4] - zigzag_price) / zigzag_price <= ZIGZAG_TOLERANCE:
                zigzag_status = 'swing_low' if rising else 'none'
            elif zigzag_status == 'high' and abs(candles[-1][4] - zigzag_price) / zigzag_price <= ZIGZAG_TOLERANCE:
                zigzag_status = 'swing_high' if falling else 'none'
            else:
                zigzag_status = 'none'

        alert = {
            'symbol': symbol,
            'side': side,
            'entry': entry,
            'tp': tp,
            'sl': sl,
            'category': category,
            'ema_status': ema_status,
            'rsi': rsi,
            'rsi_category': rsi_category,
            'adx': adx,
            'adx_category': adx_category,
            'big_candle_rsi': big_candle_rsi,
            'big_candle_rsi_status': big_candle_rsi_status,
            'zigzag_status': zigzag_status,
            'zigzag_price': zigzag_price
        }
        alert_queue.put(alert)
    except Exception as e:
        print(f"Error processing {symbol}: {e}")

# === MAIN SCAN ===
def run_bot():
    try:
        redis_client.ping()
        print("✅ Redis connection successful")
        send_telegram("✅ Redis connection successful")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        send_telegram(f"❌ Redis connection failed: {e}")
        return

    load_trades()
    threading.Thread(target=check_tp_sl, daemon=True).start()
    print(f"BOT STARTED at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Number of open trades: {len(open_trades)}")
    send_telegram(f"BOT STARTED at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}\nNumber of open trades: {len(open_trades)}")

    alert_queue = queue.Queue()
    while True:
        try:
            symbols = get_symbols()
            chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
            symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
            for chunk in symbol_chunks:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(process_symbol, symbol, alert_queue) for symbol in chunk]
                    as_completed(futures)
                time.sleep(BATCH_DELAY)

            while not alert_queue.empty():
                alert = alert_queue.get()
                symbol = alert['symbol']
                if symbol not in sent_signals:
                    sent_signals[symbol] = []
                if symbol not in open_trades:
                    msg = (
                        f"{symbol} - {'RISING' if alert['side'] == 'buy' else 'FALLING'} PATTERN\n"
                        f"{'Above' if alert['side'] == 'buy' else 'Below'} 21 ema - {alert['ema_status']['price_ema21']}\n"
                        f"ema 9 {'above' if alert['side'] == 'buy' else 'below'} 21 - {alert['ema_status']['ema9_ema21']}\n"
                        f"RSI (14) - {alert['rsi']:.2f} ({alert['rsi_category']})\n"
                        f"Big Candle RSI - {alert['big_candle_rsi']:.2f} ({alert['big_candle_rsi_status']})\n"
                        f"ADX (14) - {alert['adx']:.2f} ({alert['adx_category']})\n"
                        f"Zig Zag - {alert['zigzag_status']}\n"
                        f"entry - {alert['entry']}\n"
                        f"tp - {alert['tp']}\n"
                        f"sl - {alert['sl']:.4f}"
                    )
                    msg_id = send_telegram(msg)
                    if msg_id:
                        alert['msg_id'] = msg_id
                        open_trades[symbol] = alert
                        save_trades()
                        sent_signals[symbol].append(msg)

            export_to_csv()
            print(f"Number of open trades after scan: {len(open_trades)}")
            send_telegram(f"Number of open trades after scan: {len(open_trades)}")
            time.sleep(get_next_candle_close() - time.time())
        except Exception as e:
            print(f"Scan loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
            send_telegram(f"❌ Scan loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        redis_client.ping()
        print("✅ Redis connection successful")
        send_telegram("✅ Redis connection successful")
    except Exception as e:
        print(f"Redis connection failed: {e}")
        send_telegram(f"❌ Redis connection failed: {e}")
        exit(1)
    threading.Thread(target=run_bot, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))
