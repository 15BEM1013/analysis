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
BOT_TOKEN = os.getenv('BOT_TOKEN', 'your_bot_token_here')
CHAT_ID = os.getenv('CHAT_ID', 'your_chat_id_here')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis_host_here')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'redis_password_here')
PROXY_HOST = os.getenv('PROXY_HOST', 'proxy_host_here')
PROXY_PORT = os.getenv('PROXY_PORT', 'proxy_port_here')
PROXY_USERNAME = os.getenv('PROXY_USERNAME', 'proxy_username_here')
PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', 'proxy_password_here')
TIMEFRAMES = ['30m']
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
TELEGRAM_ERROR_COOLDOWN = 60
TELEGRAM_MAX_ERRORS = 5
LOCAL_STORAGE_FILE = '/tmp/trades.json'
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

        # ... rest of your pattern detection and trade creation logic ...

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
