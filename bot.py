import pandas as pd
import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import json
import os
import numpy as np

# === CONFIG ===
BOT_TOKEN = os.getenv('BOT_TOKEN', '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE')
CHAT_ID = os.getenv('CHAT_ID', '655537138')
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5  # Reduced to avoid rate limits
BATCH_DELAY = 2.5
NUM_CHUNKS = 4
CAPITAL = 10.0
SL_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
RSI_PERIOD = 14
ADX_PERIOD = 14
ZIGZAG_DEPTH = 12
ZIGZAG_DEVIATION = 5.0  # 5% price change
ZIGZAG_BACKSTEP = 3
ZIGZAG_TOLERANCE = 0.005  # 0.5% tolerance for swing alignment

# === PROXY ===
proxies = {
    "http": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712",
    "https": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712"
}

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)
        print(f"Trades saved to {TRADE_FILE}")
    except Exception as e:
        print(f"Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                loaded = json.load(f)
                open_trades = {k: v for k, v in loaded.items()}
            print(f"Loaded {len(open_trades)} trades from {TRADE_FILE}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in {TRADE_FILE}: {e}")
        open_trades = {}
    except Exception as e:
        print(f"Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = load_closed_trades()
        all_closed_trades.append(closed_trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(all_closed_trades, f, default=str)
        print(f"Closed trade saved to {CLOSED_TRADE_FILE}")
    except Exception as e:
        print(f"Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                else:
                    print(f"Error: {CLOSED_TRADE_FILE} contains invalid data format")
                    return []
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in {CLOSED_TRADE_FILE}: {e}")
        return []
    except Exception as e:
        print(f"Error loading closed trades: {e}")
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
                time.sleep(2 ** attempt)  # Exponential backoff
    return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    try:
        requests.post(url, data=data, proxies=proxies, timeout=5)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === INIT ===
exchange = ccxt.binance({
    'options': {'defaultType': 'future'},
    'proxies': proxies
})
app = Flask(__name__)

sent_signals = {}
open_trades = {}
closed_trades = []

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
    direction = 0  # 0: undefined, 1: up, -1: down
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
singular
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
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active') and markets[s].get('info', {}).get('status') == 'TRADING']

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
                            'pnl': profit,
                            'pnl_pct': pnl,
                            'category': trade['category'],
                            'ema_status': trade['ema_status'],
                            'rsi': trade['rsi'],
                            'rsi_category': trade['rsi_category'],
                            'adx': trade['adx'],
                            'adx_category': trade['adx_category'],
                            'big_candle_rsi': trade['big_candle_rsi'],
                            'big_candle_rsi_status': trade['big_candle_rsi_status'],
                            'zigzag_status': trade['zigzag_status'],
                            'zigzag_price': trade['zigzag_price']
                        }
                        closed_trades.append(closed_trade)
                        save_closed_trades(closed_trade)
                        ema_status = trade['ema_status']
                        new_msg = (
                            f"{sym} - {'RISING' if trade['side'] == 'buy' else 'FALLING'} PATTERN\n"
                            f"{'Above' if trade['side'] == 'buy' else 'Below'} 21 ema - {ema_status['price_ema21']}\n"
                            f"ema 9 {'above' if trade['side'] == 'buy' else 'below'} 21 - {ema_status['ema9_ema21']}\n"
                            f"RSI (14) - {trade['rsi']:.2f} ({trade['rsi_category']})\n"
                            f"Big Candle RSI - {trade['big_candle_rsi']:.2f} ({trade['big_candle_rsi, 'big_candle_rsi_status': trade['big_candle_rsi_status'],
                            'zigzag_status': trade['zigzag_status'],
                            'zigzag_price': trade['zigzag_price']
                        }
                        closed_trades.append(closed_trade)
                        save_closed_trades(closed_trade)
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
            print(f"TP/SL loop error: {e}")
            time.sleep(5)

# === PROCESS SYMBOL ===
def process_symbol(symbol, alert_queue):
    try:
        for attempt in range(3):
            candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=50)
            if len(candles) < 30:
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
            return

        signal_time = candles[-2][0]
        entry_price = candles[-2][4]
        big_candle_close = candles[-4][4]
        big_candle_low = candles[-4][3]
        big_candle_high = candles[-4][2]

        rsi_category = (
            'Overbought' if rsi > 70 else
            'Oversold' if rsi < 30 else
            'Neutral'
        )
        adx_category = 'Strong Trend' if adx >= 25 else 'Weak Trend'
        big_candle_rsi_status = 'High Momentum' if (big_candle_rsi > 75 or big_candle_rsi < 25) else 'Normal'

        zigzag_status = 'No Swing'
        zigzag_price = None
        for point in swing_points:
            idx, price, swing_type = point
            if swing_type == 'low' and abs(big_candle_low - price) / big_candle_low <= ZIGZAG_TOLERANCE:
                zigzag_status = f"Swing Low (Price: {price:.2f})"
                zigzag_price = price
                break
            elif swing_type == 'high' and abs(big_candle_high - price) / big_candle_high <= ZIGZAG_TOLERANCE:
                zigzag_status = f"Swing High (Price: {price:.2f})"
                zigzag_price = price
                break

        if detect_rising_three(candles):
            sl = entry_price * (1 - 0.015)
            if sent_signals.get((symbol, 'rising')) == signal_time:
                return
            sent_signals[(symbol, 'rising')] = signal_time
            price_above_ema21 = entry_price > ema21
            ema9_above_ema21 = ema9 > ema21
            ema_status = {
                'price_ema21': '✅' if price_above_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_above_ema21 else '⚠️'
            }
            category = (
                'two_green' if sum(1 for v in ema_status.values() if v == '✅') == 2 else
                'one_green_one_caution' if sum(1 for v in ema_status.values() if v == '✅') == 1 else
                'two_cautions'
            )
            big_candle_rsi_status = 'High Momentum' if big_candle_rsi > 75 else 'Normal'
            msg = (
                f"{symbol} - RISING PATTERN\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 above 21 - {ema_status['ema9_ema21']}\n"
                f"RSI (14) - {rsi:.2f} ({rsi_category})\n"
                f"Big Candle RSI - {big_candle_rsi:.2f} ({big_candle_rsi_status})\n"
                f"ADX (14) - {adx:.2f} ({adx_category})\n"
                f"Zig Zag - {zigzag_status}\n"
                f"entry - {entry_price}\n"
                f"tp - {big_candle_close}\n"
                f"sl - {sl:.4f}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, rsi, rsi_category, adx, adx_category, big_candle_rsi, big_candle_rsi_status, zigzag_status, zigzag_price))

        elif detect_falling_three(candles):
            sl = entry_price * (1 + 0.015)
            if sent_signals.get((symbol, 'falling')) == signal_time:
                return
            sent_signals[(symbol, 'falling')] = signal_time
            price_below_ema21 = entry_price < ema21
            ema9_below_ema21 = ema9 < ema21
            ema_status = {
                'price_ema21': '✅' if price_below_ema21 else '⚠️',
                'ema9_ema21': '✅' if ema9_below_ema21 else '⚠️'
            }
            category = (
                'two_green' if sum(1 for v in ema_status.values() if v == '✅') == 2 else
                'one_green_one_caution' if sum(1 for v in ema_status.values() if v == '✅') == 1 else
                'two_cautions'
            )
            big_candle_rsi_status = 'High Momentum' if big_candle_rsi < 25 else 'Normal'
            msg = (
                f"{symbol} - FALLING PATTERN\n"
                f"Below 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 below 21 - {ema_status['ema9_ema21']}\n"
                f"RSI (14) - {rsi:.2f} ({rsi_category})\n"
                f"Big Candle RSI - {big_candle_rsi:.2f} ({big_candle_rsi_status})\n"
                f"ADX (14) - {adx:.2f} ({adx_category})\n"
                f"Zig Zag - {zigzag_status}\n"
                f"entry - {entry_price}\n"
                f"tp - {big_candle_close}\n"
                f"sl - {sl:.4f}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category, rsi, rsi_category, adx, adx_category, big_candle_rsi, big_candle_rsi_status, zigzag_status, zigzag_price))

    except ccxt.RateLimitExceeded:
        print(f"Rate limit exceeded for {symbol}, retrying after delay")
        time.sleep(5)
    except Exception as e:
        print(f"Error on {symbol}: {e}")

# === PROCESS BATCH ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, alert_queue): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            future.result()

# === CSV EXPORT ===
def export_to_csv():
    try:
        # Load closed trades
        all_closed_trades = load_closed_trades()
        
        # Prepare closed trades DataFrame
        closed_trades_df = pd.DataFrame(all_closed_trades)
        if not closed_trades_df.empty:
            closed_trades_df = closed_trades_df[[
                'symbol', 'pnl', 'pnl_pct', 'category', 'ema_status', 'rsi', 
                'rsi_category', 'adx', 'adx_category', 'big_candle_rsi', 
                'big_candle_rsi_status', 'zigzag_status', 'zigzag_price'
            ]]
            closed_trades_df['ema_status'] = closed_trades_df['ema_status'].apply(lambda x: str(x))
        
        # Prepare open trades DataFrame
        open_trades_df = pd.DataFrame([
            {
                'symbol': sym,
                'side': trade['side'],
                'entry': trade['entry'],
                'tp': trade['tp'],
                'sl': trade['sl'],
                'category': trade['category'],
                'ema_status': str(trade['ema_status']),
                'rsi': trade['rsi'],
                'rsi_category': trade['rsi_category'],
                'adx': trade['adx'],
                'adx_category': trade['adx_category'],
                'big_candle_rsi': trade['big_candle_rsi'],
                'big_candle_rsi_status': trade['big_candle_rsi_status'],
                'zigzag_status': trade['zigzag_status'],
                'zigzag_price': trade['zigzag_price']
            } for sym, trade in open_trades.items()
        ])
        
        # Prepare summary metrics
        two_green_trades = [t for t in all_closed_trades if t['category'] == 'two_green']
        one_green_trades = [t for t in all_closed_trades if t['category'] == 'one_green_one_caution']
        two_cautions_trades = [t for t in all_closed_trades if t['category'] == 'two_cautions']
        adx_low_overbought = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Overbought']
        adx_low_neutral = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Neutral']
        adx_low_oversold = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Oversold']
        adx_high_overbought = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Overbought']
        adx_high_neutral = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Neutral']
        adx_high_oversold = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Oversold']
        rsi_high_rising = [t for t in all_closed_trades if t['big_candle_rsi_status'] == 'High Momentum' and t['big_candle_rsi'] > 75]
        rsi_low_falling = [t for t in all_closed_trades if t['big_candle_rsi_status'] == 'High Momentum' and t['big_candle_rsi'] < 25]
        zigzag_swing_low = [t for t in all_closed_trades if 'Swing Low' in t['zigzag_status']]
        zigzag_swing_high = [t for t in all_closed_trades if 'Swing High' in t['zigzag_status']]
        
        def get_category_metrics(trades):
            count = len(trades)
            wins = sum(1 for t in trades if t['pnl'] > 0)
            losses = sum(1 for t in trades if t['pnl'] < 0)
            pnl = sum(t['pnl'] for t in trades)
            pnl_pct = sum(t['pnl_pct'] for t in trades)
            win_rate = (wins / count * 100) if count > 0 else 0.00
            return {
                'Count': count,
                'Wins': wins,
                'Losses': losses,
                'PnL ($)': round(pnl, 2),
                'PnL (%)': round(pnl_pct, 2),
                'Win Rate (%)': round(win_rate, 2)
            }
        
        # Summary metrics
        summary_data = {
            'Category': [
                'Two Green Ticks',
                'One Green, One Caution',
                'Two Cautions',
                'ADX < 25, RSI Overbought',
                'ADX < 25, RSI Neutral',
                'ADX < 25, RSI Oversold',
                'ADX ≥ 25, RSI Overbought',
                'ADX ≥ 25, RSI Neutral',
                'ADX ≥ 25, RSI Oversold',
                'Big Candle RSI > 75 (Rising)',
                'Big Candle RSI < 25 (Falling)',
                'Zig Zag Swing Low (Rising)',
                'Zig Zag Swing High (Falling)'
            ],
            **{k: [
                get_category_metrics(two_green_trades)[k],
                get_category_metrics(one_green_trades)[k],
                get_category_metrics(two_cautions_trades)[k],
                get_category_metrics(adx_low_overbought)[k],
                get_category_metrics(adx_low_neutral)[k],
                get_category_metrics(adx_low_oversold)[k],
                get_category_metrics(adx_high_overbought)[k],
                get_category_metrics(adx_high_neutral)[k],
                get_category_metrics(adx_high_oversold)[k],
                get_category_metrics(rsi_high_rising)[k],
                get_category_metrics(rsi_low_falling)[k],
                get_category_metrics(zigzag_swing_low)[k],
                get_category_metrics(zigzag_swing_high)[k]
            ] for k in ['Count', 'Wins', 'Losses', 'PnL ($)', 'PnL (%)', 'Win Rate (%)']}
        }
        summary_df = pd.DataFrame(summary_data)
        
        # Calculate total and cumulative PnL
        total_pnl = sum(t['pnl'] for t in all_closed_trades)
        total_pnl_pct = sum(t['pnl_pct'] for t in all_closed_trades)
        if all_closed_trades:
            symbol_pnl = {}
            for trade in all_closed_trades:
                sym = trade['symbol']
                symbol_pnl[sym] = symbol_pnl.get(sym, 0) + trade['pnl']
            top_symbol = max(symbol_pnl.items(), key=lambda x: x[1], default=(None, 0))
            top_symbol_name, top_symbol_pnl = top_symbol
            top_symbol_pnl_pct = sum(t['pnl_pct'] for t in all_closed_trades if t['symbol'] == top_symbol_name) if top_symbol_name else 0
        else:
            top_symbol_name, top_symbol_pnl, top_symbol_pnl_pct = None, 0, 0
        
        # Additional summary metrics
        additional_summary = pd.DataFrame({
            'Metric': ['Total PnL ($)', 'Total PnL (%)', 'Top Symbol', 'Top Symbol PnL ($)', 'Top Symbol PnL (%)', 'Open Trades'],
            'Value': [
                round(total_pnl, 2),
                round(total_pnl_pct, 2),
                top_symbol_name or 'None',
                round(top_symbol_pnl, 2),
                round(top_symbol_pnl_pct, 2),
                len(open_trades)
            ]
        })
        
        # Write to CSV files
        timestamp = get_ist_time().strftime("%Y%m%d_%H%M%S")
        if not closed_trades_df.empty:
            closed_trades_file = f'closed_trades_{timestamp}.csv'
            closed_trades_df.to_csv(closed_trades_file, index=False)
            print(f"Closed trades CSV saved: {closed_trades_file}")
            send_telegram(f"📊 Closed trades CSV generated: {closed_trades_file}")
        
        if not open_trades_df.empty:
            open_trades_file = f'open_trades_{timestamp}.csv'
            open_trades_df.to_csv(open_trades_file, index=False)
            print(f"Open trades CSV saved: {open_trades_file}")
            send_telegram(f"📊 Open trades CSV generated: {open_trades_file}")
        
        summary_file = f'summary_{timestamp}.csv'
        summary_df.to_csv(summary_file, index=False)
        additional_summary.to_csv(summary_file, mode='a', index=False, header=True)
        print(f"Summary CSV saved: {summary_file}")
        send_telegram(f"📊 Summary CSV generated: {summary_file}")
        
    except (PermissionError, OSError) as e:
        print(f"File access error in CSV export: {e}")
        send_telegram(f"❌ File access error generating CSV files: {e}")
    except Exception as e:
        print(f"Unexpected error exporting to CSV: {e}")
        send_telegram(f"❌ Unexpected error generating CSV files: {e}")

# === SCAN LOOP ===
def scan_loop():
    global closed_trades
    load_trades()
    symbols = get_symbols()
    print(f"🔍 Scanning {len(symbols)} Binance Futures symbols...")
    alert_queue = queue.Queue()

    chunk_size = math.ceil(len(symbols) / NUM_CHUNKS)
    symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]

    def send_alerts():
        while True:
            try:
                symbol, msg, ema_status, category, rsi, rsi_category, adx, adx_category, big_candle_rsi, big_candle_rsi_status, zigzag_status, zigzag_price = alert_queue.get(timeout=5)
                mid = send_telegram(msg)
                if mid and symbol not in open_trades:
                    trade = {
                        'side': 'buy' if 'RISING' in msg else 'sell',
                        'entry': float(msg.split('entry - ')[1].split('\n')[0]),
                        'tp': float(msg.split('tp - ')[1].split('\n')[0]),
                        'sl': float(msg.split('sl - ')[1].split('\n')[0]),
                        'msg': msg,
                        'msg_id': mid,
                        'ema_status': ema_status,
                        'category': category,
                        'rsi': rsi,
                        'rsi_category': rsi_category,
                        'adx': adx,
                        'adx_category': adx_category,
                        'big_candle_rsi': big_candle_rsi,
                        'big_candle_rsi_status': big_candle_rsi_status,
                        'zigzag_status': zigzag_status,
                        'zigzag_price': zigzag_price
                    }
                    open_trades[symbol] = trade
                    save_trades()
                alert_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Alert thread error: {e}")
                time.sleep(1)

    threading.Thread(target=send_alerts, daemon=True).start()
    threading.Thread(target=check_tp_sl, daemon=True).start()

    while True:
        next_close = get_next_candle_close()
        wait_time = max(0, next_close - time.time())
        print(f"⏳ Waiting {wait_time:.1f} seconds for next 15m candle close...")
        time.sleep(wait_time)

        for i, chunk in enumerate(symbol_chunks):
            print(f"Processing batch {i+1}/{NUM_CHUNKS}...")
            process_batch(chunk, alert_queue)
            if i < NUM_CHUNKS - 1:
                time.sleep(BATCH_DELAY)

        print("✅ Scan complete.")
        num_open = len(open_trades)
        print(f"📊 Number of open trades: {num_open}")

        # Load all closed trades for cumulative summary
        all_closed_trades = load_closed_trades()

        # Compile PnL summary
        two_green_trades = [t for t in all_closed_trades if t['category'] == 'two_green']
        one_green_trades = [t for t in all_closed_trades if t['category'] == 'one_green_one_caution']
        two_cautions_trades = [t for t in all_closed_trades if t['category'] == 'two_cautions']
        adx_low_overbought = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Overbought']
        adx_low_neutral = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Neutral']
        adx_low_oversold = [t for t in all_closed_trades if t['adx_category'] == 'Weak Trend' and t['rsi_category'] == 'Oversold']
        adx_high_overbought = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Overbought']
        adx_high_neutral = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Neutral']
        adx_high_oversold = [t for t in all_closed_trades if t['adx_category'] == 'Strong Trend' and t['rsi_category'] == 'Oversold']
        rsi_high_rising = [t for t in all_closed_trades if t['big_candle_rsi_status'] == 'High Momentum' and t['big_candle_rsi'] > 75]
        rsi_low_falling = [t for t in all_closed_trades if t['big_candle_rsi_status'] == 'High Momentum' and t['big_candle_rsi'] < 25]
        zigzag_swing_low = [t for t in all_closed_trades if 'Swing Low' in t['zigzag_status']]
        zigzag_swing_high = [t for t in all_closed_trades if 'Swing High' in t['zigzag_status']]

        # Calculate metrics for each category
        def get_category_metrics(trades):
            count = len(trades)
            wins = sum(1 for t in trades if t['pnl'] > 0)
            losses = sum(1 for t in trades if t['pnl'] < 0)
            pnl = sum(t['pnl'] for t in trades)
            pnl_pct = sum(t['pnl_pct'] for t in trades)
            win_rate = (wins / count * 100) if count > 0 else 0.00
            return count, wins, losses, pnl, pnl_pct, win_rate

        two_green_count, two_green_wins, two_green_losses, two_green_pnl, two_green_pnl_pct, two_green_win_rate = get_category_metrics(two_green_trades)
        one_green_count, one_green_wins, one_green_losses, one_green_pnl, one_green_pnl_pct, one_green_win_rate = get_category_metrics(one_green_trades)
        two_cautions_count, two_cautions_wins, two_cautions_losses, two_cautions_pnl, two_cautions_pnl_pct, two_cautions_win_rate = get_category_metrics(two_cautions_trades)
        adx_low_ob_count, adx_low_ob_wins, adx_low_ob_losses, adx_low_ob_pnl, adx_low_ob_pnl_pct, adx_low_ob_win_rate = get_category_metrics(adx_low_overbought)
        adx_low_n_count, adx_low_n_wins, adx_low_n_losses, adx_low_n_pnl, adx_low_n_pnl_pct, adx_low_n_win_rate = get_category_metrics(adx_low_neutral)
        adx_low_os_count, adx_low_os_wins, adx_low_os_losses, adx_low_os_pnl, adx_low_os_pnl_pct, adx_low_os_win_rate = get_category_metrics(adx_low_oversold)
        adx_high_ob_count, adx_high_ob_wins, adx_high_ob_losses, adx_high_ob_pnl, adx_high_ob_pnl_pct, adx_high_ob_win_rate = get_category_metrics(adx_high_overbought)
        adx_high_n_count, adx_high_n_wins, adx_high_n_losses, adx_high_n_pnl, adx_high_n_pnl_pct, adx_high_n_win_rate = get_category_metrics(adx_high_neutral)
        adx_high_os_count, adx_high_os_wins, adx_high_os_losses, adx_high_os_pnl, adx_high_os_pnl_pct, adx_high_os_win_rate = get_category_metrics(adx_high_oversold)
        rsi_high_rising_count, rsi_high_rising_tp, rsi_high_rising_sl, rsi_high_rising_pnl, rsi_high_rising_pnl_pct, rsi_high_rising_win_rate = get_category_metrics(rsi_high_rising)
        rsi_low_falling_count, rsi_low_falling_tp, rsi_low_falling_sl, rsi_low_falling_pnl, rsi_low_falling_pnl_pct, rsi_low_falling_win_rate = get_category_metrics(rsi_low_falling)
        zigzag_low_count, zigzag_low_tp, zigzag_low_sl, zigzag_low_pnl, zigzag_low_pnl_pct, zigzag_low_win_rate = get_category_metrics(zigzag_swing_low)
        zigzag_high_count, zigzag_high_tp, zigzag_high_sl, zigzag_high_pnl, zigzag_high_pnl_pct, zigzag_high_win_rate = get_category_metrics(zigzag_swing_high)

        total_pnl = sum(t['pnl'] for t in all_closed_trades)
        total_pnl_pct = sum(t['pnl_pct'] for t in all_closed_trades)
        if all_closed_trades:
            symbol_pnl = {}
            for trade in all_closed_trades:
                sym = trade['symbol']
                symbol_pnl[sym] = symbol_pnl.get(sym, 0) + trade['pnl']
            top_symbol = max(symbol_pnl.items(), key=lambda x: x[1], default=(None, 0))
            top_symbol_name, top_symbol_pnl = top_symbol
            top_symbol_pnl_pct = sum(t['pnl_pct'] for t in all_closed_trades if t['symbol'] == top_symbol_name) if top_symbol_name else 0
        else:
            top_symbol_name, top_symbol_pnl, top_symbol_pnl_pct = None, 0, 0

        # Format Telegram message
        timestamp = get_ist_time().strftime("%I:%M %p IST, %B %d, %Y")
        summary_msg = (
            f"🔍 Scan Completed at {timestamp}\n"
            f"📊 Trade Summary (Closed Trades):\n"
            f"- ✅✅ Two Green Ticks: {two_green_count} trades (Wins: {two_green_wins}, Losses: {two_green_losses}), PnL: ${two_green_pnl:.2f} ({two_green_pnl_pct:.2f}%), Win Rate: {two_green_win_rate:.2f}%\n"
            f"- ✅⚠️ One Green, One Caution: {one_green_count} trades (Wins: {one_green_wins}, Losses: {one_green_losses}), PnL: ${one_green_pnl:.2f} ({one_green_pnl_pct:.2f}%), Win Rate: {one_green_win_rate:.2f}%\n"
            f"- ⚠️⚠️ Two Cautions: {two_cautions_count} trades (Wins: {two_cautions_wins}, Losses: {two_cautions_losses}), PnL: ${two_cautions_pnl:.2f} ({two_cautions_pnl_pct:.2f}%), Win Rate: {two_cautions_win_rate:.2f}%\n"
            f"- ADX < 25 (Weak Trend):\n"
            f"  - RSI Overbought (>70): {adx_low_ob_count} trades (Wins: {adx_low_ob_wins}, Losses: {adx_low_ob_losses}), PnL: ${adx_low_ob_pnl:.2f} ({adx_low_ob_pnl_pct:.2f}%), Win Rate: {adx_low_ob_win_rate:.2f}%\n"
            f"  - RSI Neutral (30–70): {adx_low_n_count} trades (Wins: {adx_low_n_wins}, Losses: {adx_low_n_losses}), PnL: ${adx_low_n_pnl:.2f} ({adx_low_n_pnl_pct:.2f}%), Win Rate: {adx_low_n_win_rate:.2f}%\n"
            f"  - RSI Oversold (<30): {adx_low_os_count} trades (Wins: {adx_low_os_wins}, Losses: {adx_low_os_losses}), PnL: ${adx_low_os_pnl:.2f} ({adx_low_os_pnl_pct:.2f}%), Win Rate: {adx_low_os_win_rate:.2f}%\n"
            f"- ADX ≥ 25 (Strong Trend):\n"
            f"  - RSI Overbought (>70): {adx_high_ob_count} trades (Wins: {adx_high_ob_wins}, Losses: {adx_high_ob_losses}), PnL: ${adx_high_ob_pnl:.2f} ({adx_high_ob_pnl_pct:.2f}%), Win Rate: {adx_high_ob_win_rate:.2f}%\n"
            f"  - RSI Neutral (30–70): {adx_high_n_count} trades (Wins: {adx_high_n_wins}, Losses: {adx_high_n_losses}), PnL: ${adx_high_n_pnl:.2f} ({adx_high_n_pnl_pct:.2f}%), Win Rate: {adx_high_n_win_rate:.2f}%\n"
            f"  - RSI Oversold (<30): {adx_high_os_count} trades (Wins: {adx_high_os_wins}, Losses: {adx_high_os_losses}), PnL: ${adx_high_os_pnl:.2f} ({adx_high_os_pnl_pct:.2f}%), Win Rate: {adx_high_os_win_rate:.2f}%\n"
            f"- Big Candle RSI > 75 (Rising Three):\n"
            f"  - Total Trades: {rsi_high_rising_count} trades (TP Hits: {rsi_high_rising_tp}, SL Hits: {rsi_high_rising_sl}), PnL: ${rsi_high_rising_pnl:.2f} ({rsi_high_rising_pnl_pct:.2f}%), Win Rate: {rsi_high_rising_win_rate:.2f}%\n"
            f"- Big Candle RSI < 25 (Falling Three):\n"
            f"  - Total Trades: {rsi_low_falling_count} trades (TP Hits: {rsi_low_falling_tp}, SL Hits: {rsi_low_falling_sl}), PnL: ${rsi_low_falling_pnl:.2f} ({rsi_low_falling_pnl_pct:.2f}%), Win Rate: {rsi_low_falling_win_rate:.2f}%\n"
            f"- Zig Zag Swing Low (Rising Three):\n"
            f"  - Total Trades: {zigzag_low_count} trades (TP Hits: {zigzag_low_tp}, SL Hits: {zigzag_low_sl}), PnL: ${zigzag_low_pnl:.2f} ({zigzag_low_pnl_pct:.2f}%), Win Rate: {zigzag_low_win_rate:.2f}%\n"
            f"- Zig Zag Swing High (Falling Three):\n"
            f"  - Total Trades: {zigzag_high_count} trades (TP Hits: {zigzag_high_tp}, SL Hits: {zigzag_high_sl}), PnL: ${zigzag_high_pnl:.2f} ({zigzag_high_pnl_pct:.2f}%), Win Rate: {zigzag_high_win_rate:.2f}%\n"
            f"💰 Total PnL: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
            f"📈 Cumulative PnL: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
            f"🏆 Top Symbol: {top_symbol_name or 'None'} with ${top_symbol_pnl:.2f} ({top_symbol_pnl_pct:.2f}%)\n"
            f"🔄 Open Trades: {num_open}"
        )
        send_telegram(summary_msg)
        send_telegram(f"Number of open trades after scan: {num_open}")

        # Export to CSV
        export_to_csv()

        # Do not reset closed_trades to preserve in-memory trades for next cycle
        # closed_trades = []

# === FLASK ===
@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

# === RUN ===
def run_bot():
    load_trades()
    num_open = len(open_trades)
    startup_msg = f"BOT STARTED\nNumber of open trades: {num_open}"
    send_telegram(startup_msg)
    threading.Thread(target=scan_loop, daemon=True).start()
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

if __name__ == "__main__":
    run_bot()
