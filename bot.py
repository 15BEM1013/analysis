import redis
import json
import os
import ccxt
import time
import threading
import requests
import glob
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import pandas as pd
import numpy as np
import logging

# === CONFIG ===
BOT_TOKEN = os.getenv('BOT_TOKEN', '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE')
CHAT_ID = os.getenv('CHAT_ID', '655537138')
REDIS_HOST = os.getenv('REDIS_HOST', 'climbing-narwhal-53855.upstash.io')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'AdJfAAIjcDEzNDdhYTU4OGY1ZDc0ZWU3YmQzY2U0MTVkNThiNzU0OXAxMA')
TIMEFRAMES = ['30m', '1h']
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 10
BATCH_DELAY = 2.5
CAPITAL = 10.0
SL_PCT = 1.5 / 100
TP_SL_CHECK_INTERVAL = 60  # Increased to reduce Telegram spam
CLOSED_TRADE_CSV = '/tmp/closed_trades.csv'
RSI_PERIOD = 14
ADX_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
TELEGRAM_ERROR_COOLDOWN = 300  # 5 minutes cooldown for error messages
LOCAL_STORAGE_FILE = '/tmp/trades.json'  # Fallback storage

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === Redis Client ===
redis_client = None
try:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        ssl=True
    )
    redis_client.ping()
    logger.info("Connected to Redis")
except Exception as e:
    logger.error(f"Redis connection failed: {e}. Falling back to local storage.")
    redis_client = None

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
def save_trades():
    try:
        valid_trades = {
            key: trade for key, trade in open_trades.items()
            if all(k in trade for k in ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']) and
            trade['timeframe'] in TIMEFRAMES
        }
        if len(valid_trades) < len(open_trades):
            logger.warning(f"Removed {len(open_trades) - len(valid_trades)} invalid trades before saving")
        if redis_client:
            redis_client.set('open_trades', json.dumps(valid_trades, default=str))
            logger.info("Trades saved to Redis")
            send_telegram("✅ Trades saved to Redis")
        else:
            with open(LOCAL_STORAGE_FILE, 'w') as f:
                json.dump(valid_trades, f, default=str)
            logger.info("Trades saved to local storage")
    except Exception as e:
        logger.error(f"Error saving trades: {e}")
        send_telegram(f"❌ Error saving trades: {e}")

def load_trades():
    global open_trades
    try:
        if redis_client:
            data = redis_client.get('open_trades')
            if data:
                loaded_trades = json.loads(data)
                open_trades = {
                    key: trade for key, trade in loaded_trades.items()
                    if isinstance(trade, dict) and
                    all(k in trade for k in ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']) and
                    trade['timeframe'] in TIMEFRAMES
                }
                if len(open_trades) < len(loaded_trades):
                    logger.warning(f"Removed {len(loaded_trades) - len(open_trades)} invalid trades during load")
                    save_trades()  # Save cleaned trades
                logger.info(f"Loaded {len(open_trades)} valid trades from Redis")
            else:
                open_trades = {}
        else:
            if os.path.exists(LOCAL_STORAGE_FILE):
                with open(LOCAL_STORAGE_FILE, 'r') as f:
                    loaded_trades = json.load(f)
                    open_trades = {
                        key: trade for key, trade in loaded_trades.items()
                        if isinstance(trade, dict) and
                        all(k in trade for k in ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']) and
                        trade['timeframe'] in TIMEFRAMES
                    }
                    logger.info(f"Loaded {len(open_trades)} valid trades from local storage")
            else:
                open_trades = {}
    except Exception as e:
        logger.error(f"Error loading trades: {e}")
        send_telegram(f"❌ Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = load_closed_trades()
        trade_id = f"{closed_trade['symbol']}:{closed_trade['close_time']}:{closed_trade['entry']}:{closed_trade['pnl']}"
        if redis_client and redis_client.sismember('exported_trades', trade_id):
            logger.info(f"Trade {trade_id} already closed, skipping")
            return
        all_closed_trades.append(closed_trade)
        if redis_client:
            redis_client.set('closed_trades', json.dumps(all_closed_trades, default=str))
            redis_client.sadd('exported_trades', trade_id)
            logger.info(f"Closed trade saved to Redis: {trade_id}")
            send_telegram(f"✅ Closed trade saved to Redis: {trade_id}")
        else:
            with open(CLOSED_TRADE_CSV, 'a') as f:
                pd.DataFrame([closed_trade]).to_csv(f, index=False, header=not os.path.exists(CLOSED_TRADE_CSV))
            logger.info(f"Closed trade saved to CSV: {trade_id}")
    except Exception as e:
        logger.error(f"Error saving closed trades: {e}")
        send_telegram(f"❌ Error saving closed trades: {e}")

def load_closed_trades():
    try:
        if redis_client:
            data = redis_client.get('closed_trades')
            if data:
                trades = json.loads(data)
                unique_trades = []
                seen_ids = set()
                for trade in trades:
                    trade_id = f"{trade['symbol']}:{trade['close_time']}:{trade['entry']}:{trade['pnl']}"
                    if trade_id not in seen_ids:
                        unique_trades.append(trade)
                        seen_ids.add(trade_id)
                if len(trades) != len(unique_trades):
                    logger.warning(f"Removed {len(trades) - len(unique_trades)} duplicate closed trades from Redis")
                    redis_client.set('closed_trades', json.dumps(unique_trades, default=str))
                return unique_trades
        else:
            if os.path.exists(CLOSED_TRADE_CSV):
                return pd.read_csv(CLOSED_TRADE_CSV).to_dict('records')
        return []
    except Exception as e:
        logger.error(f"Error loading closed trades: {e}")
        return []

# === TELEGRAM ===
last_error_time = 0
def send_telegram(msg, retries=3):
    global last_error_time
    current_time = time.time()
    if "❌" in msg and current_time - last_error_time < TELEGRAM_ERROR_COOLDOWN:
        logger.info(f"Suppressed Telegram error: {msg[:50]}...")
        return None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    proxies = {
        'http': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712',
        'https': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712'
    }
    for attempt in range(retries):
        try:
            response = requests.post(url, data=data, proxies=proxies, timeout=5).json()
            if response.get('ok'):
                logger.info(f"Telegram sent: {msg[:50]}...")
                if "❌" in msg:
                    last_error_time = current_time
                return response.get('result', {}).get('message_id')
            else:
                if response.get('error_code') == 401:
                    logger.error(f"Telegram Unauthorized error: {response.get('description')}. Check BOT_TOKEN and CHAT_ID.")
                    return None
                logger.warning(f"Telegram API error: {response.get('description')}")
        except Exception as e:
            logger.warning(f"Telegram error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    logger.error(f"Failed to send Telegram message after {retries} attempts: {msg[:50]}...")
    return None

def edit_telegram_message(message_id, new_text):
    if not message_id:
        return send_telegram(new_text)
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    proxies = {
        'http': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712',
        'https': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712'
    }
    try:
        response = requests.post(url, data=data, proxies=proxies, timeout=5).json()
        if response.get('ok'):
            logger.info(f"Telegram updated: {new_text[:50]}...")
        else:
            logger.warning(f"Telegram edit error: {response.get('description')}")
    except Exception as e:
        logger.error(f"Edit error: {e}")
        send_telegram(f"❌ Telegram edit error: {e}")

def send_csv_to_telegram(filename):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    proxies = {
        'http': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712',
        'https': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712'
    }
    try:
        if not os.path.exists(filename):
            logger.error(f"File {filename} does not exist")
            send_telegram(f"❌ File {filename} does not exist")
            return
        with open(filename, 'rb') as f:
            data = {'chat_id': CHAT_ID, 'caption': f"CSV: {filename}"}
            files = {'document': f}
            response = requests.post(url, data=data, files=files, proxies=proxies, timeout=10).json()
            if response.get('ok'):
                logger.info(f"Sent {filename} to Telegram")
                send_telegram(f"📎 Sent {filename} to Telegram")
            else:
                logger.warning(f"Telegram send CSV error: {response.get('description')}")
                send_telegram(f"❌ Telegram send CSV error: {response.get('description')}")
    except Exception as e:
        logger.error(f"Error sending {filename} to Telegram: {e}")
        send_telegram(f"❌ Error sending {filename} to Telegram: {e}")

# === INIT ===
exchange = ccxt.binance({
    'apiKey': os.getenv('BINANCE_API_KEY'),
    'secret': os.getenv('BINANCE_API_SECRET'),
    'options': {'defaultType': 'future'},
    'enableRateLimit': True,
    'proxies': {
        'http': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712',
        'https': 'http://tytogvbu:wb64rnowfoby@207.244.217.165:6712'
    }
})

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

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def calculate_obv(candles):
    closes = np.array([c[4] for c in candles])
    volumes = np.array([c[5] for c in candles])
    obv = [0]
    for i in range(1, len(candles)):
        if closes[i] > closes[i-1]:
            obv.append(obv[-1] + volumes[i])
        elif closes[i] < closes[i-1]:
            obv.append(obv[-1] - volumes[i])
        else:
            obv.append(obv[-1])
    obv_trend = 'Up' if obv[-1] > obv[-5] else 'Down' if obv[-1] < obv[-5] else 'Flat'
    return obv_trend

def calculate_macd(candles, fast=12, slow=26, signal=9):
    closes = np.array([c[4] for c in candles])
    if len(closes) < slow:
        return None, None, None
    ema_fast = pd.Series(closes).ewm(span=fast, adjust=False).mean().values
    ema_slow = pd.Series(closes).ewm(span=slow, adjust=False).mean().values
    macd_line = ema_fast - ema_slow
    signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
    macd_status = 'Bullish' if macd_line[-1] > signal_line[-1] else 'Bearish' if macd_line[-1] < signal_line[-1] else 'Neutral'
    return macd_line[-1], signal_line[-1], macd_status

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT
    small_red_1 = (
        is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c1) >= MIN_LOWER_WICK_PCT and
        c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    small_red_0 = (
        is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        lower_wick_pct(c0) >= MIN_LOWER_WICK_PCT and
        c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3
    )
    volume_decreasing = c1[5] > c0[5]
    return big_green and small_red_1 and small_red_0 and volume_decreasing

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT
    small_green_1 = (
        is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and
        c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    small_green_0 = (
        is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and
        c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3
    )
    volume_decreasing = c1[5] > c0[5]
    return big_red and small_green_1 and small_green_0 and volume_decreasing

# === SYMBOLS ===
def get_symbols():
    try:
        markets = exchange.load_markets()
        symbols = [
            s for s in markets
            if s.endswith('USDT') and
               markets[s]['contract'] and
               markets[s].get('active') and
               markets[s].get('info', {}).get('status') == 'TRADING' and
               len(s.split('/')[0]) <= 10
        ]
        logger.info(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
        send_telegram(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        send_telegram(f"❌ Error fetching symbols: {e}")
        return []

# === CANDLE CLOSE ===
def get_next_candle_close(timeframe):
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    if timeframe == '30m':
        seconds_to_next = (30 * 60) - (seconds % (30 * 60))
        if seconds_to_next < 5:
            seconds_to_next += 30 * 60
    elif timeframe == '1h':
        seconds_to_next = (60 * 60) - (seconds % (60 * 60))
        if seconds_to_next < 5:
            seconds_to_next += 60 * 60
    else:
        seconds_to_next = 15 * 60  # Fallback for unexpected timeframes
    return time.time() + seconds_to_next

# === TP/SL CHECK ===
def check_tp_sl():
    global open_trades
    while True:
        try:
            trades_to_remove = []
            batched_messages = []
            for sym, trade in list(open_trades.items()):
                try:
                    # Validate trade data
                    if not all(k in trade for k in ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']):
                        logger.warning(f"Invalid trade detected for {sym}: missing required keys")
                        trades_to_remove.append(sym)
                        continue
                    if trade['timeframe'] not in TIMEFRAMES:
                        logger.warning(f"Invalid trade detected for {sym}: invalid timeframe {trade['timeframe']}")
                        trades_to_remove.append(sym)
                        continue

                    symbol = sym.split(':')[0]
                    ticker = exchange.fetch_ticker(symbol)
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
                            'symbol': symbol,
                            'side': trade['side'],
                            'entry': trade['entry'],
                            'tp': trade['tp'],
                            'sl': trade['sl'],
                            'pnl': profit,
                            'pnl_pct': pnl,
                            'category': trade.get('category'),
                            'ema_status': trade.get('ema_status', {}),
                            'rsi': trade.get('rsi'),
                            'rsi_category': trade.get('rsi_category'),
                            'adx': trade.get('adx'),
                            'adx_category': trade.get('adx_category'),
                            'big_candle_rsi': trade.get('big_candle_rsi'),
                            'big_candle_rsi_status': trade.get('big_candle_rsi_status'),
                            'signal_time': trade.get('signal_time'),
                            'signal_weekday': trade.get('signal_weekday'),
                            'obv_trend': trade.get('obv_trend'),
                            'macd_line': trade.get('macd_line'),
                            'macd_signal': trade.get('macd_signal'),
                            'macd_status': trade.get('macd_status'),
                            'close_time': get_ist_time().strftime('%Y-%m-%d %H:%M:%S'),
                            'first_candle_pattern': trade.get('first_candle_pattern'),
                            'first_candle_lower_wick': trade.get('first_candle_lower_wick'),
                            'first_candle_upper_wick': trade.get('first_candle_upper_wick'),
                            'first_candle_body': trade.get('first_candle_body'),
                            'first_candle_wick_tick': trade.get('first_candle_wick_tick'),
                            'first_candle_body_tick': trade.get('first_candle_body_tick'),
                            'second_candle_tp_touched': trade.get('second_candle_tp_touched'),
                            'timeframe': trade['timeframe']
                        }
                        trade_id = f"{closed_trade['symbol']}:{closed_trade['close_time']}:{closed_trade['entry']}:{closed_trade['pnl']}"
                        if not redis_client or not redis_client.sismember('exported_trades', trade_id):
                            save_closed_trades(closed_trade)
                            trades_to_remove.append(sym)
                            ema_status = trade.get('ema_status', {})
                            new_msg = (
                                f"{symbol} ({trade['timeframe']}) - {'RISING' if trade['side'] == 'buy' else 'FALLING'} PATTERN\n"
                                f"Signal Time: {trade.get('signal_time', 'N/A')} ({trade.get('signal_weekday', 'N/A')})\n"
                                f"{'Above' if trade['side'] == 'buy' else 'Below'} 21 ema - {ema_status.get('price_ema21', 'N/A')}\n"
                                f"ema 9 {'above' if trade['side'] == 'buy' else 'below'} 21 - {ema_status.get('ema9_ema21', 'N/A')}\n"
                                f"RSI (14) - {trade.get('rsi', 'N/A'):.2f} ({trade.get('rsi_category', 'N/A')})\n"
                                f"Big Candle RSI - {trade.get('big_candle_rsi', 'N/A'):.2f} ({trade.get('big_candle_rsi_status', 'N/A')})\n"
                                f"ADX (14) - {trade.get('adx', 'N/A'):.2f} ({trade.get('adx_category', 'N/A')})\n"
                                f"OBV Trend - {trade.get('obv_trend', 'N/A')}\n"
                                f"MACD - {trade.get('macd_status', 'N/A')} (Line: {trade.get('macd_line', 'N/A'):.2f}, Signal: {trade.get('macd_signal', 'N/A'):.2f})\n"
                                f"1st Small Candle: {trade.get('first_candle_pattern', 'N/A')}, Lower: {trade.get('first_candle_lower_wick', 'N/A'):.2f}%, "
                                f"Upper: {trade.get('first_candle_upper_wick', 'N/A'):.2f}% {trade.get('first_candle_wick_tick', 'N/A')}\n"
                                f"Body: {trade.get('first_candle_body', 'N/A'):.2f}% {trade.get('first_candle_body_tick', 'N/A')}\n"
                                f"2nd Small Candle Touched TP: {trade.get('second_candle_tp_touched', 'N/A')}\n"
                                f"entry - {trade['entry']}\n"
                                f"tp - {trade['tp']}\n"
                                f"sl - {trade['sl']:.4f}\n"
                                f"Profit/Loss: {pnl:.2f}% (${profit:.2f})\n{hit}"
                            )
                            edit_telegram_message(trade.get('msg_id'), new_msg)
                        else:
                            logger.info(f"Trade {trade_id} already closed, skipping TP/SL")
                except Exception as e:
                    logger.error(f"TP/SL check error on {sym}: {e}")
                    batched_messages.append(f"❌ TP/SL check error on {sym}: {e}")
            for sym in trades_to_remove:
                del open_trades[sym]
            if trades_to_remove:
                save_trades()
                logger.info(f"Removed {len(trades_to_remove)} closed trades from open_trades")
                send_telegram(f"Removed {len(trades_to_remove)} closed trades from open_trades")
            if batched_messages:
                send_telegram("\n".join(batched_messages[:5]))  # Limit to 5 errors per batch
            time.sleep(TP_SL_CHECK_INTERVAL)
        except Exception as e:
            logger.error(f"TP/SL loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
            send_telegram(f"❌ TP/SL loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
            time.sleep(5)

# === EXPORT TO CSV ===
def export_to_csv():
    try:
        all_closed_trades = load_closed_trades()
        closed_trades_df = pd.DataFrame(all_closed_trades)
        total_pnl = closed_trades_df['pnl'].sum() if not closed_trades_df.empty else 0.0
        total_trades = len(closed_trades_df) if not closed_trades_df.empty else 0
        win_trades = len(closed_trades_df[closed_trades_df['pnl'] > 0]) if not closed_trades_df.empty else 0
        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0.0
        avg_win = closed_trades_df[closed_trades_df['pnl'] > 0]['pnl'].mean() if win_trades > 0 else 0.0
        avg_loss = closed_trades_df[closed_trades_df['pnl'] <= 0]['pnl'].mean() if (total_trades - win_trades) > 0 else 0.0
        win_rate_rising = closed_trades_df[closed_trades_df['side'] == 'buy']['pnl'].gt(0).mean() * 100 if not closed_trades_df[closed_trades_df['side'] == 'buy'].empty else 0.0
        win_rate_falling = closed_trades_df[closed_trades_df['side'] == 'sell']['pnl'].gt(0).mean() * 100 if not closed_trades_df[closed_trades_df['side'] == 'sell'].empty else 0.0
        win_rate_obv_up = closed_trades_df[closed_trades_df['obv_trend'] == 'Up']['pnl'].gt(0).mean() * 100 if not closed_trades_df[closed_trades_df['obv_trend'] == 'Up'].empty else 0.0
        win_rate_macd_bullish = closed_trades_df[closed_trades_df['macd_status'] == 'Bullish']['pnl'].gt(0).mean() * 100 if not closed_trades_df[closed_trades_df['macd_status'] == 'Bullish'].empty else 0.0
        
        if not closed_trades_df.empty:
            exported_trades = set(redis_client.smembers('exported_trades') if redis_client else [])
            closed_trades_df['trade_id'] = closed_trades_df['symbol'] + ':' + closed_trades_df['close_time'] + ':' + closed_trades_df['entry'].astype(str) + ':' + closed_trades_df['pnl'].astype(str)
            new_trades_df = closed_trades_df[~closed_trades_df['trade_id'].isin(exported_trades)]
            if not new_trades_df.empty:
                mode = 'a' if os.path.exists(CLOSED_TRADE_CSV) else 'w'
                header = not os.path.exists(CLOSED_TRADE_CSV)
                new_trades_df.drop(columns=['trade_id']).to_csv(CLOSED_TRADE_CSV, mode=mode, header=header, index=False)
                logger.info(f"Appended {len(new_trades_df)} new closed trades to {CLOSED_TRADE_CSV}")
                send_telegram(f"📊 Appended {len(new_trades_df)} new closed trades to {CLOSED_TRADE_CSV}")
                send_csv_to_telegram(CLOSED_TRADE_CSV)
                if redis_client:
                    for trade_id in new_trades_df['trade_id']:
                        redis_client.sadd('exported_trades', trade_id)
            else:
                logger.info("No new closed trades to export")
                send_telegram("📊 No new closed trades to export")
        else:
            logger.info("No closed trades to export")
            send_telegram("📊 No closed trades to export")
        summary_msg = (
            f"🔍 Scan Completed at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📊 Total Trades: {total_trades}\n"
            f"🏆 Win Rate: {win_rate:.2f}% ({win_trades}/{total_trades})\n"
            f"💰 Total PnL: ${total_pnl:.2f} ({total_pnl / CAPITAL * 100:.2f}%)\n"
            f"📈 Avg Win: ${avg_win:.2f}\n"
            f"📉 Avg Loss: ${avg_loss:.2f}\n"
            f"📊 Rising Three Win Rate: {win_rate_rising:.2f}%\n"
            f"📊 Falling Three Win Rate: {win_rate_falling:.2f}%\n"
            f"📊 OBV Up Win Rate: {win_rate_obv_up:.2f}%\n"
            f"📊 MACD Bullish Win Rate: {win_rate_macd_bullish:.2f}%"
        )
        send_telegram(summary_msg)
    except Exception as e:
        logger.error(f"Error in export_to_csv: {e}")
        send_telegram(f"❌ Error in export_to_csv: {e}")

# === PROCESS SYMBOL ===
def process_symbol(symbol, timeframe, alert_queue):
    try:
        for attempt in range(3):
            candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=50)
            if len(candles) < 30:
                logger.info(f"{symbol} ({timeframe}): Skipped, insufficient candles ({len(candles)})")
                return
            if attempt < 2 and candles[-1][0] > candles[-2][0]:
                break
            time.sleep(1)

        signal_time = candles[-2][0]
        signal_entry_time = get_ist_time().strftime('%Y-%m-%d %H:%M:%S')
        signal_weekday = get_ist_time().strftime('%A')
        signal_key = (symbol, 'rising', timeframe) if detect_rising_three(candles) else (symbol, 'falling', timeframe)
        if signal_key in sent_signals and sent_signals[signal_key] == signal_time:
            return

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        adx = calculate_adx(candles, period=ADX_PERIOD)
        big_candle_rsi = calculate_rsi(candles[:-3], period=RSI_PERIOD)
        obv_trend = calculate_obv(candles)
        macd_line, macd_signal, macd_status = calculate_macd(candles, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
        if any(v is None for v in [ema21, ema9, rsi, adx, big_candle_rsi, macd_line]):
            logger.info(f"{symbol} ({timeframe}): Skipped, indicator calculation failed")
            return

        rising = detect_rising_three(candles)
        falling = detect_falling_three(candles)
        if not (rising or falling):
            c2, c1, c0 = candles[-4], candles[-3], candles[-2]
            reasons = []
            if is_bullish(c2) and not rising:
                if body_pct(c2) < MIN_BIG_BODY_PCT:
                    reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
                if not (is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 1 not bearish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if not (is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 0 not bearish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if c1[5] <= c0[5]:
                    reasons.append("Volume not decreasing")
            elif is_bearish(c2) and not falling:
                if body_pct(c2) < MIN_BIG_BODY_PCT:
                    reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
                if not (is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 1 not bullish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if not (is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT):
                    reasons.append(f"Small candle 0 not bullish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}%")
                if c1[5] <= c0[5]:
                    reasons.append("Volume not decreasing")
            if reasons:
                logger.info(f"{symbol} ({timeframe}): No pattern detected. Reasons: {', '.join(reasons)}")
            return

        entry = candles[-2][4]
        ema_status = {
            'price_ema21': '✅' if (rising and entry > ema21) or (falling and entry < ema21) else '⚠️',
            'ema9_ema21': '✅' if (rising and ema9 > ema21) or (falling and ema9 < ema21) else '⚠️'
        }
        category = 'two_green' if ema_status['price_ema21'] == '✅' and ema_status['ema9_ema21'] == '✅' else None
        if not category:
            return

        rsi_category = 'Overbought' if rsi > 70 else 'Oversold' if rsi < 30 else 'Neutral'
        adx_category = 'Strong Trend' if adx >= 25 else 'Weak Trend'
        big_candle_rsi_status = 'Overbought' if big_candle_rsi > 75 else 'Oversold' if big_candle_rsi < 25 else 'Normal'

        first_candle = candles[-3]
        open_price, high, low, close = first_candle[1], first_candle[2], first_candle[3], first_candle[4]
        body = abs(open_price - close)
        upper_wick = high - max(open_price, close)
        lower_wick = min(open_price, close) - low
        total_range = high - low
        body_pct_val = (body / total_range * 100) if total_range > 0 else 0
        upper_wick_pct_val = (upper_wick / total_range * 100) if total_range > 0 else 0
        lower_wick_pct_val = (lower_wick / total_range * 100) if total_range > 0 else 0

        def detect_candle_pattern(candle, is_bullish, pattern_type):
            body_pct = (abs(candle[1] - candle[4]) / (candle[2] - candle[3]) * 100) if (candle[2] - candle[3]) > 0 else 0
            upper_wick_pct = ((candle[2] - max(candle[1], candle[4])) / (candle[2] - candle[3]) * 100) if (candle[2] - candle[3]) > 0 else 0
            lower_wick_pct = ((min(candle[1], candle[4]) - candle[3]) / (candle[2] - candle[3]) * 100) if (candle[2] - candle[3]) > 0 else 0
            wick_tick = '✅'
            body_tick = '✅' if body_pct >= 10 else '⚠️'
            pressure = None

            if pattern_type == 'rising':
                if upper_wick_pct >= 2.5 * lower_wick_pct:
                    wick_tick = '❌'
                elif lower_wick_pct > 2.5 * upper_wick_pct:
                    wick_tick = '🟣'
            elif pattern_type == 'falling':
                if lower_wick_pct >= 2.5 * upper_wick_pct:
                    wick_tick = '❌'
                elif upper_wick_pct > 2.5 * lower_wick_pct:
                    wick_tick = '🟣'

            if body_pct < 5 or body_pct == 0:
                if is_bullish:
                    if lower_wick_pct > 70 and upper_wick_pct < 10:
                        return "Dragonfly Doji", wick_tick, body_tick, pressure
                    elif upper_wick_pct > 70 and lower_wick_pct < 10:
                        return "Gravestone Doji", wick_tick, body_tick, pressure
                else:
                    if upper_wick_pct > 70 and lower_wick_pct < 10:
                        return "Gravestone Doji", wick_tick, body_tick, pressure
                    elif lower_wick_pct > 70 and upper_wick_pct < 10:
                        return "Dragonfly Doji", wick_tick, body_tick, pressure
                return "Doji", wick_tick, body_tick, pressure
            if is_bullish:
                if lower_wick_pct > 70 and upper_wick_pct < 10:
                    return "Dragonfly Doji", wick_tick, body_tick, pressure
                elif upper_wick_pct > 70 and lower_wick_pct < 10:
                    return "Gravestone Doji", wick_tick, body_tick, pressure
            else:
                if upper_wick_pct > 70 and lower_wick_pct < 10:
                    return "Gravestone Doji", wick_tick, body_tick, pressure
                elif lower_wick_pct > 70 and upper_wick_pct < 10:
                    return "Dragonfly Doji", wick_tick, body_tick, pressure
            return "Doji", wick_tick, body_tick, pressure

        pattern, wick_tick, body_tick, pressure = detect_candle_pattern(first_candle, is_bullish(first_candle), 'rising' if rising else 'falling')

        if rising:
            sent_signals[(symbol, 'rising', timeframe)] = signal_time
            tp = candles[-4][4]
            sl = entry * (1 - SL_PCT)
            second_candle = candles[-2]
            tp_touched = second_candle[2] >= tp
            second_tick = '✅' if not tp_touched else '❌'
        else:
            sent_signals[(symbol, 'falling', timeframe)] = signal_time
            tp = candles[-4][4]
            sl = entry * (1 + SL_PCT)
            second_candle = candles[-2]
            tp_touched = second_candle[3] <= tp
            second_tick = '✅' if not tp_touched else '❌'

        trade = {
            'side': 'buy' if rising else 'sell',
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
            'signal_time': signal_entry_time,
            'signal_weekday': signal_weekday,
            'obv_trend': obv_trend,
            'macd_line': macd_line,
            'macd_signal': macd_signal,
            'macd_status': macd_status,
            'first_candle_pattern': pattern,
            'first_candle_lower_wick': lower_wick_pct_val,
            'first_candle_upper_wick': upper_wick_pct_val,
            'first_candle_body': body_pct_val,
            'first_candle_wick_tick': wick_tick,
            'first_candle_body_tick': body_tick,
            'second_candle_tp_touched': second_tick,
            'timeframe': timeframe
        }

        pattern_msg = (
            f"1st Small Candle: {pattern}, Lower: {lower_wick_pct_val:.2f}%, Upper: {upper_wick_pct_val:.2f}% {wick_tick}\n"
            f"Body: {body_pct_val:.2f}% {body_tick}"
        )
        if pressure and wick_tick == '✅':
            pattern_msg += f" ({pressure})"

        msg = (
            f"{symbol} ({timeframe}) - {'RISING' if rising else 'FALLING'} PATTERN\n"
            f"Signal Time: {signal_entry_time} ({signal_weekday})\n"
            f"{'Above' if rising else 'Below'} 21 ema - {ema_status['price_ema21']}\n"
            f"ema 9 {'above' if rising else 'below'} 21 - {ema_status['ema9_ema21']}\n"
            f"RSI (14) - {rsi:.2f} ({rsi_category})\n"
            f"Big Candle RSI - {big_candle_rsi:.2f} ({big_candle_rsi_status})\n"
            f"ADX (14) - {adx:.2f} ({adx_category})\n"
            f"OBV Trend - {obv_trend}\n"
            f"MACD - {macd_status} (Line: {macd_line:.2f}, Signal: {macd_signal:.2f})\n"
            f"{pattern_msg}\n"
            f"2nd Small Candle Touched TP: {second_tick}\n"
            f"entry - {entry}\n"
            f"tp - {tp}\n"
            f"sl - {sl:.4f}"
        )
        trade['msg_id'] = send_telegram(msg)
        open_trades[f"{symbol}:{timeframe}"] = trade
        save_trades()
        alert_queue.put((symbol, trade))
    except Exception as e:
        logger.error(f"Error processing {symbol} ({timeframe}): {e}")
        send_telegram(f"❌ Error processing {symbol} ({timeframe}): {e}")

# === MAIN LOOP ===
def run_bot():
    try:
        load_trades()
        alert_queue = queue.Queue()
        threading.Thread(target=check_tp_sl, daemon=True).start()
        while True:
            symbols = get_symbols()
            if not symbols:
                logger.error("No symbols fetched, retrying in 60 seconds")
                time.sleep(60)
                continue
            chunk_size = math.ceil(len(symbols) / MAX_WORKERS)
            chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
            for timeframe in TIMEFRAMES:
                logger.info(f"Scanning for timeframe: {timeframe}")
                send_telegram(f"Starting scan for timeframe: {timeframe}")
                for chunk in chunks:
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = [executor.submit(process_symbol, symbol, timeframe, alert_queue) for symbol in chunk]
                        for future in as_completed(futures):
                            future.result()
                    time.sleep(BATCH_DELAY)
                export_to_csv()
                logger.info(f"Number of open trades after {timeframe} scan: {len(open_trades)}")
                send_telegram(f"Number of open trades after {timeframe} scan: {len(open_trades)}")
            sleep_time = min([get_next_candle_close(tf) for tf in TIMEFRAMES]) - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
    except Exception as e:
        logger.error(f"Scan loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
        send_telegram(f"❌ Scan loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
        time.sleep(5)

# === START ===
if __name__ == "__main__":
    logger.info("Starting bot...")
    send_telegram("✅ Bot started")
    run_bot()
