import redis
import json
import os
import requests
import time
import threading
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import queue
import pandas as pd
import numpy as np
import logging
import argparse

# === CONFIG ===
# Replace these with your actual credentials
BOT_TOKEN = os.getenv('BOT_TOKEN', 'your_actual_bot_token')
CHAT_ID = os.getenv('CHAT_ID', 'your_actual_chat_id')
REDIS_HOST = os.getenv('REDIS_HOST', 'your_redis_host')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'your_redis_password')
PROXY_HOST = os.getenv('PROXY_HOST', 'your_proxy_host')
PROXY_PORT = os.getenv('PROXY_PORT', 'your_proxy_port')
PROXY_USERNAME = os.getenv('PROXY_USERNAME', 'your_proxy_username')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', 'your_proxy_password')

# Trading parameters
TIMEFRAMES = ['30m']
MIN_BIG_BODY_PCT = 0.5
MAX_SMALL_BODY_PCT = 0.5
MIN_LOWER_WICK_PCT = 15.0
MAX_WORKERS = 10
BATCH_DELAY = 2.5
CAPITAL = 10.0
SL_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 60

# File paths
CLOSED_TRADE_CSV = '/tmp/closed_trades.csv'
LOCAL_STORAGE_FILE = '/tmp/trades.json'

# Technical indicators
RSI_PERIOD = 14
ADX_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Telegram settings
TELEGRAM_ERROR_COOLDOWN = 60
TELEGRAM_MAX_ERRORS = 5

# System settings
REDIS_RETRIES = 3
REDIS_RETRY_DELAY = 5
DEBUG_MODE = True
SUPPRESS_DUPLICATE_PATTERNS = True
SAVE_INTERVAL = 60

# === GLOBAL VARIABLES ===
open_trades = {}
sent_signals = {}
redis_client = None
LAST_SAVE_TIME = 0

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# === TELEGRAM UTILITIES ===
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
    
    # Only use proxy if credentials are provided
    proxies = None
    if all([PROXY_HOST, PROXY_PORT, PROXY_USERNAME, PROXY_PASSWORD]):
        proxies = {
            'http': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}',
            'https': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}'
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
                    logger.error(f"Telegram Unauthorized error: {response.get('description')}")
                    if telegram_error_count >= TELEGRAM_MAX_ERRORS:
                        telegram_enabled = False
                        logger.error("Telegram notifications disabled due to errors")
                    return None
                logger.warning(f"Telegram API error: {response.get('description')}")
        except Exception as e:
            logger.warning(f"Telegram error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    
    logger.error(f"Failed to send Telegram message after {retries} attempts: {msg[:50]}...")
    return None

def test_telegram():
    """Test Telegram connectivity"""
    test_msg = "✅ Bot Telegram connection test"
    try:
        message_id = send_telegram(test_msg)
        if message_id:
            logger.info("Telegram connection test successful")
            return True
        logger.error("Telegram test failed - no message ID returned")
        return False
    except Exception as e:
        logger.error(f"Telegram connection test failed: {e}")
        return False

# === REDIS UTILITIES ===
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
            if redis_client.ping():
                logger.info("Redis connection successful")
                return True
        except Exception as e:
            logger.error(f"Redis connection attempt {attempt+1}/{REDIS_RETRIES} failed: {e}")
            if attempt < REDIS_RETRIES - 1:
                time.sleep(REDIS_RETRY_DELAY)
    
    logger.error("Failed to connect to Redis after retries. Using local storage.")
    return False

# === TIME UTILITIES ===
def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

def get_next_candle_close(timeframe):
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    if timeframe == '30m':
        seconds_to_next = (30 * 60) - (seconds % (30 * 60))
        if seconds_to_next < 5:
            seconds_to_next += 30 * 60
    else:
        seconds_to_next = 15 * 60
    return time.time() + seconds_to_next

# === TRADE PERSISTENCE ===
REQUIRED_TRADE_KEYS = ['symbol', 'side', 'entry', 'tp', 'sl', 'timeframe']

def save_trades():
    global LAST_SAVE_TIME
    
    current_time = time.time()
    if current_time - LAST_SAVE_TIME < SAVE_INTERVAL:
        return
        
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
        
        str_sent_signals = {f"{k[0]}:{k[1]}:{k[2]}": v for k, v in sent_signals.items()}
        
        if redis_client:
            redis_client.set('open_trades', json.dumps(valid_trades, default=str))
            redis_client.set('sent_signals', json.dumps(str_sent_signals, default=str))
            logger.debug("Trades and signals saved to Redis")
        else:
            with open(LOCAL_STORAGE_FILE, 'w') as f:
                json.dump({
                    'open_trades': valid_trades,
                    'sent_signals': sent_signals
                }, f, default=str)
            logger.debug("Trades saved to local storage")
            
        LAST_SAVE_TIME = current_time
    except Exception as e:
        logger.error(f"Error saving trades: {e}")
        send_telegram(f"❌ Error saving trades: {e}")

def load_trades():
    global open_trades, sent_signals
    try:
        if redis_client:
            data = redis_client.get('open_trades')
            signals_data = redis_client.get('sent_signals')
            if data:
                open_trades = json.loads(data)
            if signals_data:
                loaded_signals = json.loads(signals_data)
                sent_signals = {tuple(k.split(':')): v for k, v in loaded_signals.items()}
        else:
            if os.path.exists(LOCAL_STORAGE_FILE):
                with open(LOCAL_STORAGE_FILE, 'r') as f:
                    data = json.load(f)
                    open_trades = data.get('open_trades', {})
                    sent_signals = data.get('sent_signals', {})
        
        logger.info(f"Loaded {len(open_trades)} trades and {len(sent_signals)} signals")
    except Exception as e:
        logger.error(f"Error loading trades: {e}")
        send_telegram(f"❌ Error loading trades: {e}")
        open_trades = {}
        sent_signals = {}

def reset_all_trades():
    global open_trades, sent_signals, LAST_SAVE_TIME
    
    try:
        open_trades = {}
        sent_signals = {}
        LAST_SAVE_TIME = 0
        
        if redis_client:
            redis_client.delete('open_trades')
            redis_client.delete('sent_signals')
            redis_client.delete('closed_trades')
            redis_client.delete('exported_trades')
            redis_client.delete('closed_trade_ids')
            logger.info("Cleared all trade data from Redis")
        
        for file_path in [LOCAL_STORAGE_FILE, CLOSED_TRADE_CSV]:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Removed local file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing {file_path}: {e}")
        
        send_telegram("🔄 All trade history has been reset - starting fresh")
        return True
    except Exception as e:
        logger.error(f"Error resetting trades: {e}")
        send_telegram(f"❌ Error resetting trades: {e}")
        return False

# === MARKET DATA ===
def get_symbols():
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url, timeout=10).json()
        symbols = [
            s['symbol'] for s in response['symbols']
            if s['symbol'].endswith('USDT') and
               s['contractType'] == 'PERPETUAL' and
               s['status'] == 'TRADING' and
               len(s['symbol'].split('USDT')[0]) <= 10
        ]
        logger.info(f"Fetched {len(symbols)} symbols")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        send_telegram(f"❌ Error fetching symbols: {e}")
        return []

def fetch_ohlcv(symbol, timeframe, limit=50):
    try:
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={timeframe}&limit={limit}"
        response = requests.get(url, timeout=10).json()
        if isinstance(response, list):
            return [[float(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in response]
        logger.error(f"Invalid OHLCV response for {symbol}")
        return []
    except Exception as e:
        logger.error(f"Error fetching OHLCV for {symbol}: {e}")
        return []

def fetch_ticker(symbol):
    try:
        url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=5).json()
        if 'price' in response:
            return {'last': float(response['price'])}
        logger.error(f"Invalid ticker response for {symbol}")
        return {'last': 0}
    except Exception as e:
        logger.error(f"Error fetching ticker for {symbol}: {e}")
        return {'last': 0}

# === TECHNICAL INDICATORS ===
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
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100 if c[1] != 0 else 0
def lower_wick_pct(c):
    if is_bearish(c) and (c[1] - c[4]) != 0:
        return (c[1] - c[3]) / (c[1] - c[4]) * 100
    return 0

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

# === TRADE MANAGEMENT ===
def check_tp_sl():
    global open_trades
    while True:
        try:
            trades_to_remove = []
            for sym, trade in list(open_trades.items()):
                try:
                    if not all(k in trade for k in REQUIRED_TRADE_KEYS):
                        trades_to_remove.append(sym)
                        continue
                        
                    symbol = sym.split(':')[0]
                    ticker = fetch_ticker(symbol)
                    last = ticker['last']
                    
                    if last == 0:
                        continue
                        
                    close_trade = False
                    if trade['side'] == 'buy' and (last >= trade['tp'] or last <= trade['sl']):
                        close_trade = True
                    elif trade['side'] == 'sell' and (last <= trade['tp'] or last >= trade['sl']):
                        close_trade = True
                        
                    if close_trade:
                        trade_id = f"{trade['symbol']}_{trade['entry']}_{trade.get('signal_time', '')}"
                        
                        if redis_client:
                            if not redis_client.sismember('closed_trade_ids', trade_id):
                                redis_client.sadd('closed_trade_ids', trade_id)
                                trades_to_remove.append(sym)
                        else:
                            trades_to_remove.append(sym)
                            
                except Exception as e:
                    logger.error(f"TP/SL error for {sym}: {e}")
                    
            for sym in trades_to_remove:
                if sym in open_trades:
                    del open_trades[sym]
                    
            if trades_to_remove:
                save_trades()
                
            time.sleep(TP_SL_CHECK_INTERVAL)
            
        except Exception as e:
            logger.error(f"TP/SL loop error: {e}")
            time.sleep(5)

def export_to_csv():
    try:
        all_closed = []
        if redis_client:
            closed_data = redis_client.get('closed_trades')
            if closed_data:
                all_closed = json.loads(closed_data)
        else:
            if os.path.exists(CLOSED_TRADE_CSV):
                all_closed = pd.read_csv(CLOSED_TRADE_CSV).to_dict('records')
        
        if all_closed:
            df = pd.DataFrame(all_closed)
            mode = 'a' if os.path.exists(CLOSED_TRADE_CSV) else 'w'
            header = not os.path.exists(CLOSED_TRADE_CSV)
            df.to_csv(CLOSED_TRADE_CSV, mode=mode, header=header, index=False)
            logger.info(f"Exported {len(df)} trades to CSV")
    except Exception as e:
        logger.error(f"Error in export_to_csv: {e}")
        send_telegram(f"❌ Error in export_to_csv: {e}")

# === TRADE PROCESSING ===
def process_symbol(symbol, timeframe, alert_queue):
    try:
        candles = fetch_ohlcv(symbol, timeframe, limit=50)
        if len(candles) < 30:
            logger.debug(f"{symbol} ({timeframe}): Insufficient candles ({len(candles)})")
            return

        signal_time = candles[-2][0]
        signal_entry_time = get_ist_time().strftime("%Y-%m-%d %H:%M:%S")
        signal_weekday = get_ist_time().strftime('%A')
        
        pattern_key = f"{symbol}_{timeframe}_{candles[-2][0]}"
        if SUPPRESS_DUPLICATE_PATTERNS and pattern_key in sent_signals:
            logger.debug(f"Suppressed duplicate pattern for {symbol} {timeframe}")
            return

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        rsi = calculate_rsi(candles, period=RSI_PERIOD)
        adx = calculate_adx(candles, period=ADX_PERIOD)
        big_candle_rsi = calculate_rsi(candles[:-3], period=14)
        obv_trend = calculate_obv(candles)
        macd_line, macd_signal, macd_status = calculate_macd(candles, fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIGNAL)
        
        if any(v is None for v in [ema21, ema9, rsi, adx, big_candle_rsi, macd_line]):
            logger.debug(f"{symbol} ({timeframe}): Indicator calculation failed")
            return

        rising = detect_rising_three(candles)
        falling = detect_falling_three(candles)
        
        if not (rising or falling):
            return

        entry = candles[-2][4]
        ema_status = {
            'price_ema21': '✅' if (rising and entry > ema21) or (falling and entry < ema21) else '⚠️',
            'ema9_ema21': '✅' if (rising and ema9 > ema21) or (falling and ema9 < ema21) else '⚠️'
        }
        
        if ema_status['price_ema21'] != '✅' or ema_status['ema9_ema21'] != '✅':
            return

        # Create trade object
        trade = {
            'side': 'buy' if rising else 'sell',
            'entry': entry,
            'symbol': symbol,
            'tp': candles[-4][4],  # Big candle's close
            'sl': entry * (1 - SL_PCT) if rising else entry * (1 + SL_PCT),
            'timeframe': timeframe,
            'ema_status': ema_status,
            'rsi': rsi,
            'adx': adx,
            'big_candle_rsi': big_candle_rsi,
            'obv_trend': obv_trend,
            'macd_line': macd_line,
            'macd_signal': macd_signal,
            'macd_status': macd_status,
            'signal_time': signal_entry_time,
            'signal_weekday': signal_weekday
        }

        msg = (
            f"{symbol} ({timeframe}) - {'RISING' if rising else 'FALLING'} PATTERN\n"
            f"Signal Time: {signal_entry_time} ({signal_weekday})\n"
            f"EMA 21: {ema_status['price_ema21']}\n"
            f"EMA 9: {ema_status['ema9_ema21']}\n"
            f"RSI (14): {rsi:.2f}\n"
            f"ADX: {adx:.2f}\n"
            f"Big Candle RSI: {big_candle_rsi:.2f}\n"
            f"OBV Trend: {obv_trend}\n"
            f"MACD: {macd_status} (Line: {macd_line:.2f}, Signal: {macd_signal:.2f})\n"
            f"entry: {entry:.4f}\n"
            f"tp: {trade['tp']:.4f}\n"
            f"sl: {trade['sl']:.4f}"
        )
        
        trade['msg_id'] = send_telegram(msg)
        open_trades[f"{symbol}:{timeframe}"] = trade
        sent_signals[(symbol, 'rising' if rising else 'falling', timeframe)] = signal_time
        save_trades()
        alert_queue.put((symbol, trade))
        
    except Exception as e:
        logger.error(f"Error processing {symbol} ({timeframe}): {e}")
        send_telegram(f"❌ Error processing {symbol} ({timeframe}): {e}")

# === MAIN BOT LOOP ===
def run_bot(reset=False, debug=False):
    global DEBUG_MODE
    
    if debug:
        DEBUG_MODE = True
        logger.setLevel(logging.DEBUG)
    
    try:
        if not init_redis():
            logger.warning("Running with local storage due to Redis failure")
        if not test_telegram():
            logger.warning("Telegram notifications may be limited")
        
        if reset:
            if not reset_all_trades():
                logger.error("Failed to reset trades, exiting")
                return
        
        load_trades()
        
        alert_queue = queue.Queue()
        threading.Thread(target=check_tp_sl, daemon=True).start()
        
        while True:
            symbols = get_symbols()
            if not symbols:
                logger.error("No symbols fetched, retrying in 60 seconds")
                time.sleep(60)
                continue
                
            for timeframe in TIMEFRAMES:
                logger.info(f"Scanning for timeframe: {timeframe}")
                send_telegram(f"Starting scan for timeframe: {timeframe}")
                
                chunk_size = math.ceil(len(symbols) / MAX_WORKERS)
                chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
                
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
                
            sleep_time = min([get_next_candle_close(tf) for tf in TIMEFRAMES]) - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        send_telegram(f"❌ Bot crashed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--reset', action='store_true', help='Reset all trade history')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()
    
    logger.info("Starting bot...")
    send_telegram("✅ Bot started" + (" with RESET" if args.reset else "") + (" in DEBUG mode" if args.debug else ""))
    run_bot(reset=args.reset, debug=args.debug)
