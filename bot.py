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
import talib
import numpy as np
import logging

# === CONFIG ===
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 5
BATCH_DELAY = 2.0
NUM_CHUNKS = 8
CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 4.5 / 100  # Fixed 4.5% forever
TP_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
MAX_OPEN_TRADES = 5
RSI_PERIOD = 14
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]
ACCOUNT_SIZE = 1000.0

# === EMOJI SYSTEM (Exactly as you wanted) ===
EMOJI_TWO_GREEN = "Two Green Ticks"
EMOJI_ONE_MIXED = "One Green Tick, One Yellow Light"
EMOJI_TWO_CAUTION = "Two Yellow Lights"
EMOJI_NEUTRAL = "Neutral"
EMOJI_SELLING = "Selling Pressure"
EMOJI_BUYING = "Buying Pressure"

# === PROXY CONFIGURATION ===
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '31.59.20.176',    'port': 6754, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '23.95.150.145',   'port': 6114, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '198.23.239.134',  'port': 6540, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '45.38.107.97',    'port': 6014, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '107.172.163.27',  'port': 6543, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '64.137.96.74',    'port': 6641, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '216.10.27.159',   'port': 6837, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.111.67.146',  'port': 5611, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    {'host': '142.147.128.93',  'port': 6593, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()

def get_ist_time():
    ist = pytz.timezone('Asia/Kolkata')
    return datetime.now(ist)

# === PERSISTENCE ===
def save_trades():
    with trade_lock:
        try:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
        except Exception as e:
            print(f"Save error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
    except: open_trades = {}

def save_closed_trade(trade):
    try:
        trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                trades = json.load(f)
        trades.append(trade)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(trades, f, default=str)
    except Exception as e:
        print(f"Closed save error: {e}")

# === TELEGRAM ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        response = requests.post(url, data=data, timeout=10, proxies=proxies).json()
        return response.get('result', {}).get('message_id')
    except: return None

def edit_telegram_message(msg_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies)
    except: pass

# === EXCHANGE INIT ===
def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            session = requests.Session()
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            exchange.load_markets()
            return exchange, proxies
        except: continue
    # Fallback
    exchange = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    exchange.load_markets()
    return exchange, None

app = Flask(__name__)
sent_signals = {}
open_trades = {}
last_summary_time = 0

try:
    exchange, proxies = initialize_exchange()
except: exit(1)

# === HELPERS ===
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100
def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except: return price

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    multiplier = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for close in closes[period:]:
        ema = (close - ema) * multiplier + ema
    return ema

def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period: return None
    return talib.RSI(closes, timeperiod=period)[-1]

def analyze_first_small_candle(candle, pattern_type):
    body = body_pct(candle)
    upper_wick = (candle[2] - max(candle[1], candle[4])) / candle[1] * 100
    lower_wick = (min(candle[1], candle[4]) - candle[3]) / candle[1] * 100
    wick_ratio = upper_wick / lower_wick if lower_wick > 0 else float('inf')
    wick_ratio_reverse = lower_wick / upper_wick if upper_wick > 0 else float('inf')

    if body < 0.1:
        if pattern_type == 'rising' and wick_ratio >= 2.5:
            return {'text': f"{EMOJI_SELLING}", 'status': 'selling_pressure', 'body_pct': body}
        elif pattern_type == 'rising' and wick_ratio_reverse >= 2.5:
            return {'text': f"{EMOJI_BUYING}", 'status': 'buying_pressure', 'body_pct': body}
        elif pattern_type == 'falling' and wick_ratio_reverse >= 2.5:
            return {'text': f"{EMOJI_BUYING}", 'status': 'buying_pressure', 'body_pct': body}
        elif pattern_type == 'falling' and wick_ratio >= 2.5:
            return {'text': f"{EMOJI_SELLING}", 'status': 'selling_pressure', 'body_pct': body}
    return {'text': f"{EMOJI_NEUTRAL}", 'status': 'neutral', 'body_pct': body}

# === PATTERN DETECTION ===
def detect_rising_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_green = is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_red_1 = is_bearish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_red_0 = is_bearish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] > c2[3] + (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_green and small_red_1 and small_red_0

def detect_falling_three(candles):
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_volume = sum(c[5] for c in candles[-6:-1]) / 5
    big_red = is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_volume
    small_green_1 = is_bullish(c1) and body_pct(c1) <= MAX_SMALL_BODY_PCT and c1[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c1[5] < c2[5]
    small_green_0 = is_bullish(c0) and body_pct(c0) <= MAX_SMALL_BODY_PCT and c0[4] < c2[2] - (c2[2] - c2[3]) * 0.3 and c0[5] < c2[5]
    return big_red and small_green_1 and small_green_0

def get_symbols():
    markets = exchange.load_markets()
    return [s for s in markets if 'USDT' in s and markets[s]['contract'] and markets[s].get('active')]

def get_next_candle_close():
    now = get_ist_time()
    seconds = now.minute * 60 + now.second
    seconds_to_next = (15 * 60) - (seconds % (15 * 60))
    if seconds_to_next < 5:
        seconds_to_next += 15 * 60
    return time.time() + seconds_to_next

# === TP/SL CHECK + DCA + DURATION TRACKING ===
def check_tp():
    while True:
        time.sleep(TP_CHECK_INTERVAL)
        with trade_lock:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    current_price = ticker['last']
                    entry_time = trade['entry_time']
                    duration_sec = int(time.time() * 1000) - entry_time
                    hours = duration_sec // 3600000
                    minutes = (duration_sec % 3600000) // 60000
                    duration_str = f"{hours}h {minutes}m" if hours else f"{minutes}m"

                    # DCA Logic
                    if trade['adds_done'] < 2:
                        for i, (pct, amount) in enumerate(ADD_LEVELS[:2]):
                            if trade['adds_done'] > i: continue
                            trigger_price = trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct)
                            if (trade['side'] == 'buy' and current_price <= trigger_price) or (trade['side'] == 'sell' and current_price >= trigger_price):
                                new_qty = trade['quantity'] + amount / current_price
                                new_avg = (trade['quantity'] * trade['average_entry'] + amount) / new_qty
                                new_tp = round_price(sym, new_avg * (1 + TP_PCT) if trade['side'] == 'buy' else new_avg * (1 - TP_PCT))
                                trade.update({
                                    'adds_done': i + 1,
                                    'quantity': new_qty,
                                    'average_entry': new_avg,
                                    'total_invested': trade['total_invested'] + amount,
                                    'tp': new_tp,
                                    'dca_messages': trade['dca_messages'] + [f"DCA{i+1} @ {current_price:.4f}"]
                                })
                                send_update(sym, trade)
                                save_trades()

                    # Fixed SL Check (4.5% from initial entry)
                    sl_price = trade['initial_entry'] * (1 - SL_PCT) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + SL_PCT)
                    tp_hit = (trade['side'] == 'buy' and current_price >= trade['tp']) or (trade['side'] == 'sell' and current_price <= trade['tp'])
                    sl_hit = (trade['side'] == 'buy' and current_price <= sl_price) or (trade['side'] == 'sell' and current_price >= sl_price)

                    if tp_hit or sl_hit:
                        hit_type = "TP hit" if tp_hit else "SL hit"
                        pnl_pct = (current_price - trade['average_entry']) / trade['average_entry'] * 100 * (1 if trade['side'] == 'buy' else -1)
                        profit = trade['total_invested'] * (pnl_pct * LEVERAGE / 100)
                        final_msg = f"{trade['emoji_category']} {sym} - {'BUY' if trade['side']=='buy' else 'SELL'} CLOSED\n" \
                                    f"Exit: {hit_type} @ {current_price:.4f}\n" \
                                    f"Profit: {profit:+.2f} ({pnl_pct*LEVERAGE:+.2f}%)\n" \
                                    f"Duration: {duration_str}"
                        edit_telegram_message(trade['msg_id'], final_msg)
                        save_closed_trade({**trade, 'pnl': profit, 'duration': duration_str, 'hit': hit_type})
                        del open_trades[sym]
                        save_trades()
                except Exception as e:
                    logging.error(f"Check error {sym}: {e}")

# === SIGNAL PROCESSING ===
def process_symbol(symbol, alert_queue):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if not ema21 or not ema9: return

        close_prev = candles[-3][4]
        close_curr = candles[-2][4]
        signal_time = candles[-2][0]

        if detect_rising_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time

            price_above_ema21 = close_prev > ema21
            ema9_above_ema21 = ema9 > ema21
            green_count = sum([price_above_ema21, ema9_above_ema21])
            category_emoji = EMOJI_TWO_GREEN if green_count == 2 else EMOJI_ONE_MIXED if green_count == 1 else EMOJI_TWO_CAUTION
            side = 'sell'
            entry = close_curr
            tp = round_price(symbol, entry * (1 - TP_PCT))
            sl_fixed = round_price(symbol, entry * (1 + SL_PCT))

            msg = f"{category_emoji} {symbol} - SELL\n" \
                  f"{analysis['text']}\n" \
                  f"Entry: {entry}\n" \
                  f"TP: {tp}\n" \
                  f"SL: {sl_fixed} (Fixed 4.5%)"

            alert_queue.put((symbol, msg, side, entry, tp, sl_fixed, category_emoji, analysis['text']))

        elif detect_falling_three(candles):
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time

            price_below_ema21 = close_prev < ema21
            ema9_below_ema21 = ema9 < ema21
            green_count = sum([price_below_ema21, ema9_below_ema21])
            category_emoji = EMOJI_TWO_GREEN if green_count == 2 else EMOJI_ONE_MIXED if green_count == 1 else EMOJI_TWO_CAUTION
            side = 'buy'
            entry = close_curr
            tp = round_price(symbol, entry * (1 + TP_PCT))
            sl_fixed = round_price(symbol, entry * (1 - SL_PCT))

            msg = f"{category_emoji} {symbol} - BUY\n" \
                  f"{analysis['text']}\n" \
                  f"Entry: {entry}\n" \
                  f"TP: {tp}\n" \
                  f"SL: {sl_fixed} (Fixed 4.5%)"

            alert_queue.put((symbol, msg, side, entry, tp, sl_fixed, category_emoji, analysis['text']))

    except Exception as e:
        pass

# === HOURLY SUMMARY ===
def send_hourly_summary():
    global last_summary_time
    if time.time() - last_summary_time < SUMMARY_INTERVAL: return
    last_summary_time = time.time()
    try:
        trades = json.load(open(CLOSED_TRADE_FILE)) if os.path.exists(CLOSED_TRADE_FILE) else []
        last_hour = time.time() - 3600
        recent = [t for t in trades if t.get('close_time', 0) / 1000 > last_hour]

        stats = {EMOJI_TWO_GREEN: {'tp':0, 'sl':0, 'pnl':0}, 
                 EMOJI_ONE_MIXED: {'tp':0, 'sl':0, 'pnl':0}, 
                 EMOJI_TWO_CAUTION: {'tp':0, 'sl':0, 'pnl':0}}

        for t in recent:
            cat = t.get('emoji_category', EMOJI_TWO_CAUTION)
            if cat not in stats: continue
            pnl = t.get('pnl', 0)
            hit = t.get('hit', '')
            stats[cat]['pnl'] += pnl
            if 'TP' in hit: stats[cat]['tp'] += 1
            if 'SL' in hit: stats[cat]['sl'] += 1

        msg = f"HOURLY SUMMARY ({get_ist_time().strftime('%d %b %H:%M')} IST)\n\n"
        total_pnl = sum(s['pnl'] for s in stats.values())
        msg += f"Closed: {len(recent)} | PnL: ${total_pnl:+.2f}\n\n"
        for emoji, s in stats.items():
            if s['tp'] + s['sl'] == 0: continue
            msg += f"{emoji}\n→ {s['tp']} TP | {s['sl']} SL | ${s['pnl']:+.2f}\n"
        msg += f"\nOpen trades: {len(open_trades)}/5"
        send_telegram(msg)
    except: pass

# === MAIN LOOP ===
def scan_loop():
    load_trades()
    symbols = get_symbols()
    chunks = [symbols[i:i + len(symbols)//NUM_CHUNKS + 1] for i in range(0, len(symbols), len(symbols)//NUM_CHUNKS)]
    alert_queue = queue.Queue()

    def alert_handler():
        while True:
            try:
                sym, msg, side, entry, tp, sl, emoji_cat, pressure = alert_queue.get(timeout=1)
                with trade_lock:
                    if len(open_trades) >= MAX_OPEN_TRADES:
                        continue
                    mid = send_telegram(msg)
                    if mid:
                        open_trades[sym] = {
                            'side': side, 'initial_entry': entry, 'average_entry': entry,
                            'tp': tp, 'sl_fixed': sl, 'msg_id': mid, 'emoji_category': emoji_cat,
                            'pressure': pressure, 'entry_time': int(time.time()*1000),
                            'quantity': CAPITAL/entry, 'total_invested': CAPITAL,
                            'adds_done': 0, 'dca_messages': [], 'close_time': 0
                        }
                        save_trades()
            except queue.Empty:
                send_hourly_summary()
                time.sleep(1)

    threading.Thread(target=alert_handler, daemon=True).start()
    threading.Thread(target=check_tp, daemon=True).start()

    while True:
        time.sleep(max(0, get_next_candle_close() - time.time()))
        for chunk in chunks:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
                for sym in chunk:
                    exec.submit(process_symbol, sym, alert_queue)
            time.sleep(BATCH_DELAY)

@app.route('/')
def home(): return "Bot is running with Two Green Ticks & Fixed 4.5% SL!"

def run_bot():
    load_trades()
    send_telegram(f"BOT STARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=scan_loop, daemon=True).start()
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
