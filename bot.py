import redis
import json
import os
import ccxt
import time
import threading
import requests
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
MIN_BIG_BODY_PCT = 0.5
MAX_SMALL_BODY_PCT = 0.5
MIN_LOWER_WICK_PCT = 15.0
MAX_WORKERS = 10
BATCH_DELAY = 2.5
CAPITAL = 10.0
SL_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 60
CLOSED_TRADE_CSV = '/tmp/closed_trades.csv'
RSI_PERIOD = 14
ADX_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
TELEGRAM_ERROR_COOLDOWN = 300
TELEGRAM_MAX_ERRORS = 5
LOCAL_STORAGE_FILE = '/tmp/trades.json'
REDIS_RETRIES = 3
REDIS_RETRY_DELAY = 5
DEBUG_MODE = True

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === TELEGRAM ===
telegram_enabled = True
telegram_error_count = 0
last_error_time = 0
def send_telegram(msg, retries=3):
    global telegram_enabled, telegram_error_count, last_error_time
    if not telegram_enabled:
        logger.info(f"Telegram disabled: {msg[:50]}...")
        return None
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
                    telegram_error_count += 1
                    logger.error(f"Telegram Unauthorized error: {response.get('description')}. Check BOT_TOKEN and CHAT_ID.")
                    if telegram_error_count >= TELEGRAM_MAX_ERRORS:
                        telegram_enabled = False
                        logger.error("Disabling Telegram notifications due to repeated Unauthorized errors.")
                    return None
                logger.warning(f"Telegram API error: {response.get('description')}")
        except Exception as e:
            logger.warning(f"Telegram error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    logger.error(f"Failed to send Telegram message after {retries} attempts: {msg[:50]}...")
    return None

def edit_telegram_message(message_id, new_text):
    if not telegram_enabled or not message_id:
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
    if not telegram_enabled:
        logger.info(f"Telegram disabled, skipping CSV send: {filename}")
        return
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

def test_telegram():
    test_msg = "✅ Bot Telegram test message"
    message_id = send_telegram(test_msg)
    if message_id:
        logger.info("Telegram connection successful")
        return True
    logger.error("Telegram connection failed. Notifications may be disabled.")
    return False

# === REDIS CLIENT ===
redis_client = None
def init_redis():
    global redis_client
    for attempt in range(REDIS_RETRIES):
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
            return True
        except Exception as e:
            logger.error(f"Redis connection attempt {attempt+1}/{REDIS_RETRIES} failed: {e}")
            if attempt < REDIS_RETRIES - 1:
                time.sleep(REDIS_RETRY_DELAY)
    logger.error("Failed to connect to Redis after retries. Using local storage.")
    return False

# === TIME ZONE HELPER ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === TRADE PERSISTENCE ===
REQUIRED_TRADE_KEYS = ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']
def save_trades():
    try:
        valid_trades = {}
        for key, trade in open_trades.items():
            missing_keys = [k for k in REQUIRED_TRADE_KEYS if k not in trade]
            if missing_keys:
                logger.warning(f"Invalid trade for {key}: missing keys {missing_keys}")
                continue
            if trade['timeframe'] not in TIMEFRAMES:
                logger.warning(f"Invalid trade for {key}: invalid timeframe {trade['timeframe']}")
                continue
            valid_trades[key] = trade
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
        open_trades = {}
        if redis_client:
            data = redis_client.get('open_trades')
            if data:
                loaded_trades = json.loads(data)
                for key, trade in loaded_trades.items():
                    missing_keys = [k for k in REQUIRED_TRADE_KEYS if k not in trade]
                    if missing_keys:
                        logger.warning(f"Invalid trade for {key}: missing keys {missing_keys}")
                        continue
                    if trade['timeframe'] not in TIMEFRAMES:
                        logger.warning(f"Invalid trade for {key}: invalid timeframe {trade['timeframe']}")
                        continue
                    open_trades[key] = trade
                if len(open_trades) < len(loaded_trades):
                    logger.warning(f"Removed {len(loaded_trades) - len(open_trades)} invalid trades during load")
                    save_trades()
                logger.info(f"Loaded {len(open_trades)} valid trades from Redis")
        else:
            if os.path.exists(LOCAL_STORAGE_FILE):
                with open(LOCAL_STORAGE_FILE, 'r') as f:
                    loaded_trades = json.load(f)
                    for key, trade in loaded_trades.items():
                        missing_keys = [k for k in REQUIRED_TRADE_KEYS if k not in trade]
                        if missing_keys:
                            logger.warning(f"Invalid trade for {key}: missing keys {missing_keys}")
                            continue
                        if trade['timeframe'] not in TIMEFRAMES:
                            logger.warning(f"Invalid trade for {key}: invalid timeframe {trade['timeframe']}")
                            continue
                        open_trades[key] = trade
                    logger.info(f"Loaded {len(open_trades)} valid trades from local storage")
    except Exception as e:
        logger.error(f"Error loading trades: {e}")
        send_telegram(f"❌ Error loading trades: {e}")
        open_trades = {}

def cleanup_corrupted_trades():
    global open_trades
    try:
        corrupted = []
        for key, trade in list(open_trades.items()):
            missing_keys = [k for k in REQUIRED_TRADE_KEYS if k not in trade]
            if missing_keys or trade['timeframe'] not in TIMEFRAMES:
                corrupted.append(key)
                logger.warning(f"Removing corrupted trade {key}: {missing_keys or 'invalid timeframe'}")
        for key in corrupted:
            del open_trades[key]
        if corrupted:
            save_trades()
            logger.info(f"Cleaned up {len(corrupted)} corrupted trades")
            send_telegram(f"🧹 Cleaned up {len(corrupted)} corrupted trades")
    except Exception as e:
        logger.error(f"Error during trade cleanup: {e}")
        send_telegram(f"❌ Error during trade cleanup: {e}")

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
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0
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
    if DEBUG_MODE and not (big_green and small_red_1 and small_red_0 and volume_decreasing):
        reasons = []
        if not big_green:
            reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
        if not small_red_1:
            reasons.append(f"Small candle 1 not bearish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}% or wick {lower_wick_pct(c1):.2f}% < {MIN_LOWER_WICK_PCT}%")
        if not small_red_0:
            reasons.append(f"Small candle 0 not bearish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}% or wick {lower_wick_pct(c0):.2f}% < {MIN_LOWER_WICK_PCT}%")
        if not volume_decreasing:
            reasons.append("Volume not decreasing")
        logger.debug(f"Rising three failed: {reasons}")
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
    if DEBUG_MODE and not (big_red and small_green_1 and small_green_0 and volume_decreasing):
        reasons = []
        if not big_red:
            reasons.append(f"Big candle body {body_pct(c2):.2f}% < {MIN_BIG_BODY_PCT}%")
        if not small_green_1:
            reasons.append(f"Small candle 1 not bullish or body {body_pct(c1):.2f}% > {MAX_SMALL_BODY_PCT}%")
        if not small_green_0:
            reasons.append(f"Small candle 0 not bullish or body {body_pct(c0):.2f}% > {MAX_SMALL_BODY_PCT}%")
        if not volume_decreasing:
            reasons.append("Volume not decreasing")
        logger.debug(f"Falling three failed: {reasons}")
    return big_red and small_green_1 and small_green_0 and volume_decreasing

# === SYMBOLS ===
def get_symbols():
    try:
        markets = exchange.load_markets()
        symbols = [
            s for s in markets
            if s.endswith('USDT') and
               markets[s].get('contract') and
               markets[s].get('active') and
               markets[s].get('info', {}).get('status') == 'TRADING' and
               len(s.split('/')[0]) <= 10
        ]
        logger.info(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
        send_telegram(f"✅ Fetched {len(symbols)} symbols: {symbols[:5]}...")
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
        seconds_to_next = 15 * 60
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
                    missing_keys = [k for k in REQUIRED_TRADE_KEYS if k not in trade]
                    if missing_keys:
                        logger.warning(f"Invalid trade for {sym}: missing keys {missing_keys}")
                        batched_messages.append(f"❌ Invalid trade for {sym}: missing keys {missing_keys}")
                        trades_to_remove.append(sym)
                        continue
                    if trade['timeframe'] not in TIMEFRAMES:
                        logger.warning(f"Invalid trade for {sym}: invalid timeframe {trade['timeframe']}")
                        batched_messages.append(f"❌ Invalid trade for {sym}: invalid timeframe {trade['timeframe']}")
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
                            pnl = (trade rosbags = []
            for bag in baggage:
                if bag['type'] == 'message':
                    message = bag['message']
                    chat_id = str(bag['chat_id'])
                    send_message = partial(
                        bot.send_message, chat_id=chat_id, parse_mode='HTML'
                    )
                    if message.reply_to_message:
                        reply_to_message_id = message.reply_to_message.message_id
                        send_message = partial(
                            send_message, reply_to_message_id=reply_to_message_id
                        )

                    if '/start' in message.text.lower():
                        send_message(
                            "Welcome to the Trading Bot! I'll notify you about trading signals based on the Rising and Falling Three patterns."
                        )
                    elif '/help' in message.text.lower():
                        send_message(
                            "Available commands:\n"
                            "/start - Start the bot\n"
                            "/help - Show this help message\n"
                            "/status - Get current bot status\n"
                            "/trades - List open trades\n"
                            "/closed - List recently closed trades\n"
                            "/stats - Show trading statistics"
                        )
                    elif '/status' in message.text.lower():
                        send_message(f"Bot is running. Open trades: {len(open_trades)}. Last scan: {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}")
                    elif '/trades' in message.text.lower():
                        if open_trades:
                            msg = "Open Trades:\n" + "\n".join(
                                f"{k}: {v['side']} @ {v['entry']}, TP: {v['tp']}, SL: {v['sl']}, Timeframe: {v['timeframe']}"
                                for k, v in open_trades.items()
                            )
                            send_message(msg)
                        else:
                            send_message("No open trades")
                    elif '/closed' in message.text.lower():
                        closed = load_closed_trades()[-5:]
                        if closed:
                            msg = "Recent Closed Trades:\n" + "\n".join(
                                f"{t['symbol']}: {t['side']} @ {t['entry']}, PNL: {t['pnl']:.2f} ({t['pnl_pct']:.2f}%), Timeframe: {t['timeframe']}"
                                for t in closed
                            )
                            send_message(msg)
                        else:
                            send_message("No closed trades")
                    elif '/stats' in message.text.lower():
                        closed = load_closed_trades()
                        closed_df = pd.DataFrame(closed)
                        total_trades = len(closed_df)
                        win_trades = len(closed_df[closed_df['pnl'] > 0]) if not closed_df.empty else 0
                        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
                        total_pnl = closed_df['pnl'].sum() if not closed_df.empty else 0
                        msg = (
                            f"Stats:\n"
                            f"Total Trades: {total_trades}\n"
                            f"Win Rate: {win_rate:.2f}%\n"
                            f"Total PNL: ${total_pnl:.2f}"
                        )
                        send_message(msg)
                except Exception as e:
                    logger.error(f"Error processing Telegram update: {e}")
                    send_message(f"❌ Error: {e}")
        except Exception as e:
            logger.error(f"Error in Telegram bot: {e}")

def run_bot():
    try:
        if not init_redis():
            logger.warning("Running with local storage due to Redis failure")
        if not test_telegram():
            logger.warning("Telegram notifications may be limited")
        load_trades()
        cleanup_corrupted_trades()
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
                            try:
                                future.result()
                            except Exception as e:
                                logger.error(f"Error in future for {timeframe}: {e}")
                    time.sleep(BATCH_DELAY)
                export_to_csv()
                logger.info(f"Open trades after {timeframe} scan: {len(open_trades)}")
                send_telegram(f"Open trades after {timeframe} scan: {len(open_trades)}")
            sleep_time = min([get_next_candle_close(tf) for tf in TIMEFRAMES]) - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
    except Exception as e:
        logger.error(f"Scan loop error at {get_ist_time().strftime('%Y-%m-%d %H:%M:%S')}: {e}")
        send_telegram(f"❌ Scan loop error: {e}")
        time.sleep(5)

# === START ===
if __name__ == "__main__":
    logger.info("Starting bot...")
    send_telegram("✅ Bot started")
    run_bot()
