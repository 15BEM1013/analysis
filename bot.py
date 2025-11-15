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

# ====================== CONFIG ======================
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0

# Anti-ban settings
MAX_WORKERS = 3
BATCH_DELAY = 8.0
NUM_CHUNKS = 8
TP_CHECK_INTERVAL = 60

# Trading parameters
CAPITAL = 20.0
LEVERAGE = 10
TP_PCT = 0.01          # 1%
SL_PCT = 0.045         # 4.5% FIXED
ADD_LEVELS = [(0.015, 20.0), (0.030, 20.0)]   # 1.5% → +$20, 3% → +$20

TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'
RSI_PERIOD = 14
BODY_SIZE_THRESHOLD = 0.1
SUMMARY_INTERVAL = 3600

# Proxy list (keep yours)
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'vmrcabza', 'password': '2tmwim0mjpmI'},
    # ... (rest of your proxies)
]

# ====================== SETUP ======================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
trade_lock = threading.Lock()
app = Flask(__name__)

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

def initialize_exchange():
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            exchange = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
            })
            exchange.load_markets()
            logging.info(f"Connected via proxy {proxy['host']}")
            return exchange, proxies
        except:
            continue
    # Fallback direct
    exchange = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    exchange.load_markets()
    return exchange, None

exchange, proxies = initialize_exchange()

open_trades = {}
sent_signals = {}
last_summary_time = 0

# ====================== HELPERS ======================
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        r = requests.post(url, data=data, proxies=proxies, timeout=10)
        return r.json().get('result', {}).get('message_id')
    except:
        return None

def edit_telegram_message(msg_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, proxies=proxies, timeout=10)
    except:
        pass

def round_price(symbol, price):
    try:
        market = exchange.market(symbol)
        tick_size = float(market['info']['filters'][0]['tickSize'])
        precision = int(round(-math.log10(tick_size)))
        return round(price, precision)
    except:
        return round(price, 8)

def save_trades():
    with trade_lock:
        with open(TRADE_FILE, 'w') as f:
            json.dump(open_trades, f, default=str)

def load_trades():
    global open_trades
    if os.path.exists(TRADE_FILE):
        with open(TRADE_FILE, 'r') as f:
            open_trades = json.load(f)

def save_closed_trade(trade):
    trades = []
    if os.path.exists(CLOSED_TRADE_FILE):
        with open(CLOSED_TRADE_FILE, 'r') as f:
            trades = json.load(f)
    trades.append(trade)
    with open(CLOSED_TRADE_FILE, 'w') as f:
        json.dump(trades, f, default=str)

# ====================== PATTERN & ANALYSIS ======================
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def calculate_ema(candles, period=21):
    closes = [c[4] for c in candles]
    if len(closes) < period: return None
    ema = sum(closes[:period]) / period
    multiplier = 2 / (period + 1)
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(candles, period=14):
    closes = np.array([c[4] for c in candles])
    if len(closes) < period + 1: return None
    return talib.RSI(closes, timeperiod=period)[-1]

def detect_rising_three(candles):
    if len(candles) < 25: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    return (is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bearish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[5] < c2[5] for c in [c1, c0]))

def detect_falling_three(candles):
    if len(candles) < 25: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-6:-1]) / 5
    return (is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bullish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[5] < c2[5] for c in [c1, c0]))

# ====================== MESSAGE BUILDERS ======================
def build_initial_message(symbol, side, entry, tp, sl):
    direction = "BUY" if side == "buy" else "SELL"
    dca1_price = round_price(symbol, entry * (1 - 0.015) if side == "buy" else entry * (1 + 0.015))
    dca2_price = round_price(symbol, entry * (1 - 0.030) if side == "buy" else entry * (1 + 0.030))
    sign = "-" if side == "buy" else "+"
    msg = f"{symbol} - {direction}\n" \
          f"Entry: {entry}\n" \
          f"Invested: $20 / $60\n" \
          f"Leverage: 10x\n\n" \
          f"DCA1 @ {dca1_price} ({sign}1.5%) → +$20\n" \
          f"DCA2 @ {dca2_price} ({sign}3.0%) → +$20\n\n" \
          f"TP: {tp} (+1%)\n" \
          f"SL: {sl} (-4.5%) ← fixed"
    return msg

def build_updated_message(trade):
    symbol = trade['symbol']
    side = trade['side']
    direction = "BUY" if side == "buy" else "SELL"
    entry = trade['initial_entry']
    invested = trade['total_invested']
    avg_entry = trade['average_entry']
    tp = trade['tp']
    sl = trade['fixed_sl']
    adds = trade['adds_done']

    status = ""
    if adds == 0:
        status = ""
    elif adds == 1:
        status = "→ DCA1 added"
    else:
        status = "→ fully loaded"

    dca_lines = []
    for i, (pct, _) in enumerate(ADD_LEVELS):
        price = round_price(symbol, entry * (1 - pct) if side == "buy" else entry * (1 + pct))
        done = "Done" if i < adds else ""
        dca_lines.append(f"DCA{i+1} → +$20 {done}")

    msg = f"{symbol} - {direction}\n" \
          f"Entry: {entry} {status}\n" \
          f"Invested: ${invested} / $60\n" \
          f"Leverage: 10x\n\n" \
          f"{chr(10).join(dca_lines)}\n\n" \
          f"TP: {tp} (+1%)\n" \
          f"SL: {sl} (-4.5%) ← fixed"
    return msg

# ====================== TP/SL + DCA CHECK ======================
def check_tp_sl():
    global open_trades
    while True:
        time.sleep(TP_CHECK_INTERVAL)
        with trade_lock:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    price = ticker['last']

                    # DCA logic
                    added = False
                    for i, (pct, amount) in enumerate(ADD_LEVELS):
                        if trade['adds_done'] > i:
                            continue
                        trigger_price = trade['initial_entry'] * (1 - pct) if trade['side'] == 'buy' else trade['initial_entry'] * (1 + pct)
                        if (trade['side'] == 'buy' and price <= trigger_price) or (trade['side'] == 'sell' and price >= trigger_price):
                            # Add DCA
                            new_qty = trade['quantity'] + amount / price
                            new_avg = (trade['quantity'] * trade['average_entry'] + amount) / new_qty
                            new_tp = round_price(sym, new_avg * (1 + TP_PCT) if trade['side'] == 'buy' else new_avg * (1 - TP_PCT))

                            trade['adds_done'] += 1
                            trade['total_invested'] += amount
                            trade['quantity'] = new_qty
                            trade['average_entry'] = new_avg
                            trade['tp'] = new_tp
                            added = True

                    if added:
                        edit_telegram_message(trade['msg_id'], build_updated_message(trade))
                        save_trades()
                        continue

                    # TP/SL check
                    hit = None
                    hit_price = price
                    if (trade['side'] == 'buy' and price >= trade['tp']) or (trade['side'] == 'sell' and price <= trade['tp']):
                        hit = "TP"
                    elif (trade['side'] == 'buy' and price <= trade['fixed_sl']) or (trade['side'] == 'sell' and price >= trade['fixed_sl']):
                        hit = "SL"

                    if hit:
                        duration_min = int((time.time() * 1000 - trade['entry_time']) / 60000)
                        pnl_pct = (price - trade['average_entry']) / trade['average_entry'] * 100 if trade['side'] == 'buy' else (trade['average_entry'] - price) / trade['average_entry'] * 100
                        pnl_leveraged = pnl_pct * LEVERAGE
                        profit = trade['total_invested'] * pnl_leveraged / 100

                        emoji = "TP hit" if hit == "TP" else "SL hit"
                        final_msg = f"{sym} - {'BUY' if trade['side']=='buy' else 'SELL'} {emoji}\n" \
                                    f"Duration: {duration_min} min\n" \
                                    f"Exit: {hit_price}\n" \
                                    f"Profit: {pnl_leveraged:+.2f}% (${profit:+.2f})\n" \
                                    f"Invested: ${trade['total_invested']}\n" \
                                    f"Hit: {hit} hit"

                        edit_telegram_message(trade['msg_id'], final_msg)

                        # Save closed trade
                        closed = {
                            'symbol': sym,
                            'side': trade['side'],
                            'profit': profit,
                            'pnl_pct': pnl_leveraged,
                            'category': trade['category'],
                            'hit': hit,
                            'adds_done': trade['adds_done'],
                            'duration_min': duration_min,
                            'timestamp': int(time.time())
                        }
                        save_closed_trade(closed)
                        del open_trades[sym]
                        save_trades()

                except Exception as e:
                    logging.error(f"Check error {sym}: {e}")

# ====================== HOURLY SUMMARY ======================
def send_hourly_summary():
    global last_summary_time
    while True:
        if time.time() - last_summary_time >= SUMMARY_INTERVAL:
            trades = []
            if os.path.exists(CLOSED_TRADE_FILE):
                with open(CLOSED_TRADE_FILE, 'r') as f:
                    all_closed = json.load(f)
                    start = last_summary_time
                    trades = [t for t in all_closed if t['timestamp'] >= start]

            if not trades:
                last_summary_time = time.time()
                continue

            total_profit = sum(t['profit'] for t in trades)
            cats = {'two_green': [], 'one_green': [], 'two_cautions': []}
            for t in trades:
                cats[t.get('category', 'two_cautions')].append(t)

            lines = [f"Hourly PNL Summary ({get_ist_time().strftime('%H:%M')} - {(get_ist_time().strftime('%H:%M'))})\n",
                     f"Total Profit: ${total_profit:+.2f}\n"]

            for name, label in [('two_green', '2 Green signals'), ('one_green', '1 Green + 1 Caution'), ('two_cautions', '2 Caution signals')]:
                lst = cats[name]
                if not lst: continue
                profit = sum(t['profit'] for t in lst)
                tp = sum(1 for t in lst if t['hit'] == 'TP')
                sl = len(lst) - tp
                dca2 = sum(1 for t in lst if t['adds_done'] >= 2)
                dca1_only = sum(1 for t in lst if t['adds_done'] == 1)
                dca_text = "2 took DCA2" if dca2 else ("1 took only DCA1" if dca1_only else "no DCA")
                lines.append(f"{label}: → {len(lst)} trades | ${profit:+.2f} | {tp} TP, {sl} SL | {dca_text}")

            send_telegram("\n".join(lines))
            last_summary_time = time.time()
        time.sleep(60)

# ====================== SCAN LOOP ======================
def process_symbol(symbol, q):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        ema21 = calculate_ema(candles, 21)
        ema9 = calculate_ema(candles, 9)
        if ema21 is None or ema9 is None: return

        close_prev = candles[-2][4]
        signal_time = candles[-2][0]

        if detect_rising_three(candles):
            if sent_signals.get((symbol, 'rising')) == signal_time: return
            sent_signals[(symbol, 'rising')] = signal_time
            side = 'sell'
            category = 'two_green' if close_prev > ema21 and ema9 > ema21 else 'one_green' if (close_prev > ema21 or ema9 > ema21) else 'two_cautions'
        elif detect_falling_three(candles):
            if sent_signals.get((symbol, 'falling')) == signal_time: return
            sent_signals[(symbol, 'falling')] = signal_time
            side = 'buy'
            category = 'two_green' if close_prev < ema21 and ema9 < ema21 else 'one_green' if (close_prev < ema21 or ema9 < ema21) else 'two_cautions'
        else:
            return

        entry = round_price(symbol, close_prev)
        tp = round_price(symbol, entry * (1 + TP_PCT) if side == 'buy' else entry * (1 - TP_PCT))
        sl = round_price(symbol, entry * (1 - SL_PCT) if side == 'buy' else entry * (1 + SL_PCT))

        msg = build_initial_message(symbol, side, entry, tp, sl)
        msg_id = send_telegram(msg)

        if msg_id:
            with trade_lock:
                open_trades[symbol] = {
                    'symbol': symbol,
                    'side': side,
                    'initial_entry': entry,
                    'average_entry': entry,
                    'tp': tp,
                    'fixed_sl': sl,
                    'total_invested': CAPITAL,
                    'quantity': CAPITAL / entry,
                    'adds_done': 0,
                    'msg_id': msg_id,
                    'category': category,
                    'entry_time': int(time.time() * 1000)
                }
            save_trades()

    except Exception as e:
        logging.error(f"Error {symbol}: {e}")

def scan_loop():
    load_trades()
    symbols = [s for s in exchange.load_markets() if 'USDT' in s and exchange.markets[s]['contract'] and exchange.markets[s]['active']]
    chunks = [symbols[i::NUM_CHUNKS] for i in range(NUM_CHUNKS)]

    while True:
        next_close = time.time() + (900 - (time.time() % 900)) + 5
        time.sleep(max(0, next_close - time.time()))

        q = queue.Queue()
        for chunk in chunks:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exec:
                for sym in chunk:
                    exec.submit(process_symbol, sym, q)
            time.sleep(BATCH_DELAY)

        logging.info("Scan completed")

# ====================== START ======================
@app.route('/')
def home():
    return "Bot is running!"

if __name__ == "__main__":
    threading.Thread(target=check_tp_sl, daemon=True).start()
    threading.Thread(target=send_hourly_summary, daemon=True).start()
    threading.Thread(target=scan_loop, daemon=True).start()
    send_telegram("Bot started – taking every signal!")
    app.run(host='0.0.0.0', port=8080)
