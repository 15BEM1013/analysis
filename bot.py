import ccxt
import redis
import pandas as pd
import numpy as np
from datetime import datetime
import threading
import time
from google.cloud import storage
import telebot
from flask import Flask
import logging
import os
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN', '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE')
CHAT_ID = os.getenv('CHAT_ID', '655537138')
REDIS_HOST = os.getenv('REDIS_HOST', 'climbing-narwhal-53855.upstash.io')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'AdJfAAIjcDEzNDdhYTU4OGY1ZDc0ZWU3YmQzY2U0MTVkNThiNzU0OXAxMA')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'caring-465016-trading-bot')

# Initialize clients
logger.info("Initializing Redis client...")
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=int(REDIS_PORT),
    password=REDIS_PASSWORD,
    ssl=True,
    decode_responses=True
)
logger.info("Redis client initialized")

logger.info("Initializing GCS client...")
gcs_client = storage.Client()
bucket = gcs_client.bucket(GCS_BUCKET_NAME)
logger.info("GCS client initialized")

logger.info("Initializing Binance client...")
proxies = {
    "http": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712",
    "https": "http://sgkgjbve:x9swvp7b0epc@207.244.217.165:6712"
}
binance = ccxt.binance({
    'proxies': proxies
})
logger.info("Binance client initialized")

# Initialize Telegram bot
bot = telebot.TeleBot(BOT_TOKEN)

# Global variables
symbols = []
trades = {}

def send_telegram_message(message):
    try:
        bot.send_message(CHAT_ID, message)
        logger.info(f"Telegram sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

def calculate_rsi(data, periods=14):
    try:
        delta = data['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        logger.error(f"Error calculating RSI: {e}")
        return None

def fetch_symbols():
    global symbols
    try:
        exchange_info = binance.fetch_markets()
        symbols = [market['symbol'] for market in exchange_info if market['symbol'].endswith('/USDT')]
        logger.info(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
        send_telegram_message(f"Fetched {len(symbols)} symbols: {symbols[:5]}...")
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        send_telegram_message(f"❌ Error fetching symbols: {e}")

def fetch_candles(symbol, timeframe='1h', limit=100):
    try:
        candles = binance.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        logger.error(f"Error fetching candles for {symbol}: {e}")
        send_telegram_message(f"❌ Error fetching candles for {symbol}: {e}")
        return None

def check_rising_three(candles):
    try:
        if len(candles) < 5:
            return False
        c1, c2, c3, c4, c5 = candles[-5:]
        if (c1['close'] > c1['open'] and
            c2['close'] < c2['open'] and
            c3['close'] < c3['open'] and
            c4['close'] < c4['open'] and
            c5['close'] > c5['open'] and
            c5['close'] > c1['close']):
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking rising three: {e}")
        return False

def check_falling_three(candles):
    try:
        if len(candles) < 5:
            return False
        c1, c2, c3, c4, c5 = candles[-5:]
        if (c1['close'] < c1['open'] and
            c2['close'] > c2['open'] and
            c3['close'] > c3['open'] and
            c4['close'] > c4['open'] and
            c5['close'] < c5['open'] and
            c5['close'] < c1['close']):
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking falling three: {e}")
        return False

def save_to_gcs(symbol, data):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        blob = bucket.blob(f"trades/{symbol}/{timestamp}.csv")
        blob.upload_from_string(data.to_csv(index=False))
        logger.info(f"Saved {symbol} data to GCS")
    except Exception as e:
        logger.error(f"Error saving to GCS: {e}")
        send_telegram_message(f"❌ Error saving {symbol} to GCS: {e}")

def load_trades():
    global trades
    try:
        trades = redis_client.hgetall('trades')
        logger.info("Loaded trades from Redis")
    except Exception as e:
        logger.error(f"Error loading trades: {e}")
        send_telegram_message(f"❌ Error loading trades: {e}")

def check_tp_sl(symbol, entry_price, current_price):
    try:
        tp = entry_price * 1.03  # 3% take profit
        sl = entry_price * 0.97  # 3% stop loss
        if current_price >= tp:
            return 'TP'
        elif current_price <= sl:
            return 'SL'
        return None
    except Exception as e:
        logger.error(f"Error checking TP/SL for {symbol}: {e}")
        send_telegram_message(f"❌ Error checking TP/SL for {symbol}: {e}")
        return None

def bot_thread():
    logger.info("Starting bot thread...")
    while True:
        try:
            for symbol in symbols:
                candles = fetch_candles(symbol)
                if candles is None:
                    continue
                rsi = calculate_rsi(candles)
                if rsi is None:
                    continue
                current_price = candles['close'].iloc[-1]
                if check_rising_three(candles.tail(5).to_dict('records')):
                    message = f"{symbol} - RISING THREE PATTERN DETECTED at {current_price:.2f}, RSI: {rsi.iloc[-1]:.2f}"
                    send_telegram_message(message)
                    trades[symbol] = {'entry_price': current_price, 'timestamp': str(datetime.now())}
                    redis_client.hset('trades', symbol, json.dumps(trades[symbol]))
                    save_to_gcs(symbol, candles)
                elif check_falling_three(candles.tail(5).to_dict('records')):
                    message = f"{symbol} - FALLING THREE PATTERN DETECTED at {current_price:.2f}, RSI: {rsi.iloc[-1]:.2f}"
                    send_telegram_message(message)
                    trades[symbol] = {'entry_price': current_price, 'timestamp': str(datetime.now())}
                    redis_client.hset('trades', symbol, json.dumps(trades[symbol]))
                    save_to_gcs(symbol, candles)
                if symbol in trades:
                    result = check_tp_sl(symbol, trades[symbol]['entry_price'], current_price)
                    if result:
                        message = f"{symbol} - {result} hit at {current_price:.2f}"
                        send_telegram_message(message)
                        redis_client.hdel('trades', symbol)
                        del trades[symbol]
                time.sleep(1)
            time.sleep(60)
        except Exception as e:
            logger.error(f"Bot thread error: {e}")
            send_telegram_message(f"❌ Bot thread error: {e}")
            time.sleep(60)

@app.route('/')
def home():
    return "✅ Rising & Falling Three Pattern Bot is Live!"

if __name__ == '__main__':
    logger.info("Starting application...")
    try:
        logger.info("Attempting Redis connection...")
        redis_client.ping()
        logger.info("✅ Redis connection successful")
        send_telegram_message("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        send_telegram_message(f"❌ Redis connection failed: {e}")
        exit(1)
    load_trades()
    fetch_symbols()
    threading.Thread(target=bot_thread, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)
