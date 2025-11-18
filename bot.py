import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor
import math
import queue
import json
import os
import talib
import numpy as np
import logging

# ================================== CONFIG ==================================
BOT_TOKEN = '7402265241:AAHRDxd12LRizl1qTsQggEEoJ-BeWME3ERo'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
BODY_SIZE_THRESHOLD = 0.1
CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 6.0 / 100
TP_CHECK_INTERVAL = 25
MAX_OPEN_TRADES = 5
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]  # DCA3 = final SL
SUMMARY_INTERVAL = 3600

# ============================= PROXY CONFIG ================================
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '31.59.20.176', 'port': 6754, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '23.95.150.145', 'port': 6114, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '45.38.107.97', 'port': 6014, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    # add more if you want
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# ============================= LOGGING & GLOBALS =============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()
open_trades = {}
sent_signals = {}
proxies = None
last_summary_time = 0
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

# ============================= TIME HELPERS =============================
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# ============================= PERSISTENCE =============================
def save_trades():
    with trade_lock:
        try:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
        except Exception as e:
            logging.error(f"Save error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
            logging.info(f"Loaded {len(open_trades)} open trades")
    except Exception as e:
        logging.error(f"Load error: {e}")

def save_closed_trades(closed):
    try:
        trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                trades = json.load(f)
        trades.append(closed)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(trades, f, default=str)
    except Exception as e:
        logging.error(f"Closed save error: {e}")

# ============================= TELEGRAM =============================
def send_telegram(msg):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        r = requests.post(url, data=data, timeout=10, proxies=proxies or {})
        return r.json().get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(msg_id, new_text):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies or {})
    except Exception as e:
        logging.error(f"Edit error: {e}")

# ============================= EXCHANGE =============================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    global proxies
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=1)
            session.mount('https://', HTTPAdapter(max_retries=retry))
            ex = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            ex.load_markets()
            logging.info(f"Connected via proxy {proxy['host']}")
            return ex, proxies
        except Exception as e:
            logging.error(f"Proxy failed: {e}")
    # Direct connection fallback
    ex = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    ex.load_markets()
    proxies = None
    logging.info("Connected directly")
    return ex, None

exchange, proxies = initialize_exchange()

# ============================= HELPERS
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def analyze_first_small_candle(candle, pattern_type):
    o, h, l, c = candle[1], candle[2], candle[3], candle[4]
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    total = body + upper + lower or 0.0001
    body_pct_val = body / o * 100
    upper_pct = upper / total * 100
    lower_pct = lower / total * 100

    if body_pct_val > BODY_SIZE_THRESHOLD:
        return {'text': 'Body too big', 'status': 'invalid', 'body_pct': body_pct_val}

    if upper_pct / lower_pct >= 2.5:
        pressure = "Selling pressure" if pattern_type == 'rising' else "Buying pressure"
    elif lower_pct / upper_pct >= 2.5:
        pressure = "Buying pressure" if pattern_type == 'rising' else "Selling pressure"
    else:
        pressure = "Neutral"
    return {
        'text': f"{pressure}\nUpper: {upper_pct:.1f}%\nLower: {lower_pct:.1f}%\nBody: {body_pct_val:.2f}%",
        'status': pressure.lower().split()[0],
        'body_pct': body_pct_val
    }

def round_price(symbol, price):
    try:
        info = exchange.market(symbol)['info']
        tick = float(next(f['tickSize'] for f in info['filters'] if f['filterType'] == 'PRICE_FILTER'))
        return round(price, -int(math.log10(tick)))
    except:
        return round(price, 8)

# ============================= PATTERN =============================
def detect_rising_three(candles):  # Bearish continuation → SELL
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bearish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] > c2[3] for c in [c1, c0]))

def detect_falling_three(candles):  # Bullish continuation → BUY
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bullish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] < c2[2] for c in [c1, c0]))

# ============================= TP/SL/DCA MONITOR =============================
def check_tp():
    while True:
        time.sleep(TP_CHECK_INTERVAL)
        with trade_lock:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    price = round_price(sym, ticker['last'])

                    avg = trade['average_entry']
                    side = trade['side']
                    adds_done = trade.get('adds_done', 0)

                    # DCA 1 & 2
                    for i, (pct, (pct, amt) in enumerate(ADD_LEVELS[:2]):
                        if adds_done > i: continue
                        trigger = avg * (1 - pct) if side == 'buy' else avg * (1 + pct)
                        if (side == 'buy' and price <= trigger) or (side == 'sell' and price >= trigger):
                            new_qty = trade['quantity'] + amt / price
                            new_avg = (trade['quantity'] * avg + amt) / new_qty
                            trade.update({
                                'adds_done': i+1,
                                'quantity': new_qty,
                                'average_entry': new_avg,
                                'total_invested': trade['total_invested'] + amt,
                                'tp': round_price(sym, new_avg * (1 + TP_PCT) if side=='buy' else new_avg * (1 - TP_PCT)),
                                'sl': round_price(sym, new_avg * (1 - SL_PCT) if side=='buy' else new_avg * (1 + SL_PCT)),
                                'dca_messages': trade.get('dca_messages', []) + [f"${amt} @ {price}"],
                                'dca_status': trade.get('dca_status', ["Pending"]*3)
                            })
                            trade['dca_status'][i] = "Added"
                            save_trades()
                            update_trade_message(sym, trade)
                            break

                    # DCA3 = Final SL
                    if adds_done < 2:
                        pct, _ = ADD_LEVELS[2]
                        trigger = avg * (1 - pct) if side == 'buy' else avg * (1 + pct)
                        if (side == 'buy' and price <= trigger) or (side == 'sell' and price >= trigger):
                            close_trade(sym, trade, "DCA3/SL Hit", price)
                            continue

                    # Normal TP/SL
                    if (side == 'buy' and price >= trade['tp']) or (side == 'sell' and price <= trade['tp']):
                        close_trade(sym, trade, "TP Hit", price)
                    elif (side == 'buy' and price <= trade['sl']) or (side == 'sell' and price >= trade['sl']):
                        close_trade(sym, trade, "SL Hit", price)

                except Exception as e:
                    logging.error(f"Monitor error {sym}: {e}")

def close_trade(sym, trade, reason, exit_price):
    pnl_pct = (exit_price - trade['average_entry']) / trade['average_entry'] * 100
    if trade['side'] == 'sell': pnl_pct = -pnl_pct
    leveraged_pnl = pnl_pct * LEVERAGE
    profit = round(trade['total_invested'] * leveraged_pnl / 100, 2)

    closed = {
        'symbol': sym, 'pnl': profit, 'pnl_pct': round(leveraged_pnl, 2),
        'reason': reason, 'invested': trade['total_invested'],
        'time': get_ist_time().strftime('%d %b %Y %H:%M')
    }
    save_closed_trades(closed)

    final_msg = trade['msg'] + f"\n\n{reason}\nExit: {exit_price}\nP&L: {leveraged_pnl:+.2f}% (${profit:+.2f})"
    edit_telegram_message(trade['msg_id'], final_msg)
    del open_trades[sym]
    save_trades()

def update_trade_message(sym, trade):
    sideEmoji = "🟢 LONG" if trade['side'] == 'buy' else "🔴 SHORT"
    lines = [
        f"🚨 {sym} - {sideEmoji}",
        f"Initial: {trade['initial_entry']}",
        f"Avg Entry: {trade['average_entry']:.6f}",
        f"Invested: ${trade['total_invested']:.2f}",
        ""
    ]
    for i, (pct, amt) in enumerate(ADD_LEVELS):
        price = round_price(sym, trade['initial_entry'] * (1 - pct if trade['side']=='buy' else 1 + pct))
        status = trade.get('dca_status', ["Pending"]*3)[i]
        emoji = "✅" if status == 'Added' else "⏳" if status == "Pending' else "❌ SL"
        label = "DCA1 +$5" if i==0 else "DCA2 +$10" if i==1 else "DCA3/SL"
        lines.append(f"{label}: {price} {emoji}")

    lines += ["", f"🎯 TP: {trade['tp']}", f"🛑 SL: {trade['sl']}"]
    if trade.get('dca_messages'):
        lines.append(f"Added: {', '.join(trade['dca_messages'])}")

    new_msg = "\n".join(lines)
    trade['msg'] = new_msg
    edit_telegram_message(trade['msg_id'], new_msg)

# ============================= MAIN SCAN =============================
alert_queue = queue.PriorityQueue()

def process_symbol(symbol, q):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        close_prev = candles[-2][4]
        ema9 = talib.EMA(np.array([c[4] for c in candles]), 9)[-1]
        ema21 = talib.EMA(np.array([c[4] for c in candles]), 21)[-1]
        if not ema9 or not ema21: return

        signal_time = candles[-2][0]

        if detect_rising_three(candles):  # SELL
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'sell')) == signal_time: return
            sent_signals[(symbol, 'sell')] = signal_time

            greens = sum([close_prev < ema21, ema9 < ema21])  # good for short!
            if greens == 2:
                cat = "🟢🟢 TWO GREEN – STRONG"; prio = 3
            elif greens == 1:
                cat = "🟢🔴 ONE GREEN ONE CAUTION"; prio = 2
            else:
                cat = "🔴🔴 TWO CAUTIONS – WEAK"; prio = 1

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 - TP_PCT))
            sl = round_price(symbol, entry * (1 + SL_PCT))

            msg = f"""🚨 {symbol} - 🔴 SHORT (Rising Three)

📍 Entry: {entry}
🎯 TP: {tp}
🛑 SL: {sl}

{cat}

{analysis['text']}"""
            q.put((prio, symbol, msg, 'sell', entry, tp, sl))

        elif detect_falling_three(candles):  # BUY
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'buy')) == signal_time: return
            sent_signals[(symbol, 'buy')] = signal_time

            greens = sum([close_prev > ema21, ema9 > ema21])  # good for long!
            if greens == 2:
                cat = "🟢🟢 TWO GREEN – STRONG"; prio = 3
            elif greens == 1:
                cat = "🟢🔴 ONE GREEN ONE CAUTION"; prio = 2
            else:
                cat = "🔴🔴 TWO CAUTIONS – WEAK"; prio = 1

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 + TP_PCT))
            sl = round_price(symbol, entry * (1 - SL_PCT))

            msg = f"""🚨 {symbol} - 🟢 LONG (Falling Three)

📍 Entry: {entry}
🎯 TP: {tp}
🛑 SL: {sl}

{cat}

{analysis['text']}"""
            q.put((prio, symbol, msg, 'buy', entry, tp, sl))

    except Exception as e:
        logging.error(f"Scan error {symbol}: {e}")

def alert_handler():
    while True:
        prio, sym, msg, side, entry, tp, sl = alert_queue.get()
        with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                worst = min(open_trades.items(), key=lambda x: x[1].get('prio', 1))
                edit_telegram_message(worst[1]['msg_id'], worst[0] + " - CANCELED")
                del open_trades[worst[0]]

            msg_id = send_telegram(msg)
            if msg_id:
                open_trades[sym] = {
                    'side': side,
                    'initial_entry': entry,
                    'average_entry': entry,
                    'tp': tp, 'sl': sl,
                    'msg': msg, 'msg_id': msg_id,
                    'prio': prio,
                    'total_invested': CAPITAL,
                    'quantity': CAPITAL / entry,
                    'adds_done': 0,
                    'dca_status': ["Pending"]*3,
                    'dca_messages': []
                }
                save_trades()
                update_trade_message(sym, open_trades[sym])

# ============================= MAIN LOOP =============================
app = Flask(__name__)
@app.route('/')
def home():
    return f"Rising & Falling Three Bot Running | {get_ist_time().strftime('%d %b %Y %I:%M %p')}"

if __name__ == "__main__":
    load_trades()
    send_telegram(f"🤖 BOT RESTARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=check_tp, daemon=True).start()
    threading.Thread(target=alert_handler, daemon=True).start()

    symbols = [s for s in exchange.load_markets() if s.endswith('USDT') and exchange.markets[s]['active'] and exchange.markets[s]['contract']]

    while True:
        next_close = time.time() + (900 - (time.time() % 900)) + 300
        time.sleep(max(0, next_close - time.time()))

        for sym in symbols:
            process_symbol(sym, alert_queue)

        # Hourly summary
        if time.time() - last_summary_time > SUMMARY_INTERVAL:
            trades = json.load(open(CLOSED_TRADE_FILE)) if os.path.exists(CLOSED_TRADE_FILE) else []
            total = sum(t['pnl'] for t in trades)
            wins = sum(1 for t in trades if t['pnl'] > 0)
            wr = len(trades) and round(wins/len(trades)*100, 1) or 0
            send_telegram(f"📊 SUMMARY\nTrades: {len(trades)}\nWin Rate: {wr}%\nProfit: ${total:+.2f}\nOpen: {len(open_trades)}")
            last_summary_time = time.time()

    app.run(host='0.0.0.0', port=8080)import ccxt
import time
import threading
import requests
from flask import Flask
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor
import math
import queue
import json
import os
import talib
import numpy as np
import logging

# ================================== CONFIG ==================================
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '15m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
BODY_SIZE_THRESHOLD = 0.1
CAPITAL = 20.0
LEVERAGE = 5
TP_PCT = 1.0 / 100
SL_PCT = 6.0 / 100
TP_CHECK_INTERVAL = 25
MAX_OPEN_TRADES = 5
ADD_LEVELS = [(0.015, 5.0), (0.03, 10.0), (0.045, 0.0)]  # DCA3 = final SL
SUMMARY_INTERVAL = 3600

# ============================= PROXY CONFIG ================================
PROXY_LIST = [
    {'host': '142.111.48.253', 'port': 7030, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '31.59.20.176', 'port': 6754, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '23.95.150.145', 'port': 6114, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '198.23.239.134', 'port': 6540, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    {'host': '45.38.107.97', 'port': 6014, 'username': 'uwxesntt', 'password': 'lnbyh3x661he'},
    # add more if you want
]

def get_proxy_config(proxy):
    return {
        "http": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}",
        "https": f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
    }

# ============================= LOGGING & GLOBALS =============================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
trade_lock = threading.Lock()
open_trades = {}
sent_signals = {}
proxies = None
last_summary_time = 0
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

# ============================= TIME HELPERS =============================
def get_ist_time():
    return datetime.now(pytz.timezone('Asia/Kolkata'))

# ============================= PERSISTENCE =============================
def save_trades():
    with trade_lock:
        try:
            with open(TRADE_FILE, 'w') as f:
                json.dump(open_trades, f, default=str)
        except Exception as e:
            logging.error(f"Save error: {e}")

def load_trades():
    global open_trades
    try:
        if os.path.exists(TRADE_FILE):
            with open(TRADE_FILE, 'r') as f:
                open_trades = json.load(f)
            logging.info(f"Loaded {len(open_trades)} open trades")
    except Exception as e:
        logging.error(f"Load error: {e}")

def save_closed_trades(closed):
    try:
        trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                trades = json.load(f)
        trades.append(closed)
        with open(CLOSED_TRADE_FILE, 'w') as f:
            json.dump(trades, f, default=str)
    except Exception as e:
        logging.error(f"Closed save error: {e}")

# ============================= TELEGRAM =============================
def send_telegram(msg):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg, 'disable_web_page_preview': True}
    try:
        r = requests.post(url, data=data, timeout=10, proxies=proxies or {})
        return r.json().get('result', {}).get('message_id')
    except Exception as e:
        logging.error(f"Telegram send error: {e}")
        return None

def edit_telegram_message(msg_id, new_text):
    global proxies
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': msg_id, 'text': new_text, 'disable_web_page_preview': True}
    try:
        requests.post(url, data=data, timeout=10, proxies=proxies or {})
    except Exception as e:
        logging.error(f"Edit error: {e}")

# ============================= EXCHANGE =============================
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def initialize_exchange():
    global proxies
    for proxy in PROXY_LIST:
        try:
            proxies = get_proxy_config(proxy)
            session = requests.Session()
            retry = Retry(total=3, backoff_factor=1)
            session.mount('https://', HTTPAdapter(max_retries=retry))
            ex = ccxt.binance({
                'options': {'defaultType': 'future'},
                'proxies': proxies,
                'enableRateLimit': True,
                'session': session
            })
            ex.load_markets()
            logging.info(f"Connected via proxy {proxy['host']}")
            return ex, proxies
        except Exception as e:
            logging.error(f"Proxy failed: {e}")
    # Direct connection fallback
    ex = ccxt.binance({'options': {'defaultType': 'future'}, 'enableRateLimit': True})
    ex.load_markets()
    proxies = None
    logging.info("Connected directly")
    return ex, None

exchange, proxies = initialize_exchange()

# ============================= HELPERS
def is_bullish(c): return c[4] > c[1]
def is_bearish(c): return c[4] < c[1]
def body_pct(c): return abs(c[4] - c[1]) / c[1] * 100

def analyze_first_small_candle(candle, pattern_type):
    o, h, l, c = candle[1], candle[2], candle[3], candle[4]
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    total = body + upper + lower or 0.0001
    body_pct_val = body / o * 100
    upper_pct = upper / total * 100
    lower_pct = lower / total * 100

    if body_pct_val > BODY_SIZE_THRESHOLD:
        return {'text': 'Body too big', 'status': 'invalid', 'body_pct': body_pct_val}

    if upper_pct / lower_pct >= 2.5:
        pressure = "Selling pressure" if pattern_type == 'rising' else "Buying pressure"
    elif lower_pct / upper_pct >= 2.5:
        pressure = "Buying pressure" if pattern_type == 'rising' else "Selling pressure"
    else:
        pressure = "Neutral"
    return {
        'text': f"{pressure}\nUpper: {upper_pct:.1f}%\nLower: {lower_pct:.1f}%\nBody: {body_pct_val:.2f}%",
        'status': pressure.lower().split()[0],
        'body_pct': body_pct_val
    }

def round_price(symbol, price):
    try:
        info = exchange.market(symbol)['info']
        tick = float(next(f['tickSize'] for f in info['filters'] if f['filterType'] == 'PRICE_FILTER'))
        return round(price, -int(math.log10(tick)))
    except:
        return round(price, 8)

# ============================= PATTERN =============================
def detect_rising_three(candles):  # Bearish continuation → SELL
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bullish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bearish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] > c2[3] for c in [c1, c0]))

def detect_falling_three(candles):  # Bullish continuation → BUY
    if len(candles) < 5: return False
    c2, c1, c0 = candles[-4], candles[-3], candles[-2]
    avg_vol = sum(c[5] for c in candles[-7:-2]) / 5
    return (is_bearish(c2) and body_pct(c2) >= MIN_BIG_BODY_PCT and c2[5] > avg_vol and
            all(is_bullish(c) and body_pct(c) <= MAX_SMALL_BODY_PCT and c[4] < c2[2] for c in [c1, c0]))

# ============================= TP/SL/DCA MONITOR =============================
def check_tp():
    while True:
        time.sleep(TP_CHECK_INTERVAL)
        with trade_lock:
            for sym, trade in list(open_trades.items()):
                try:
                    ticker = exchange.fetch_ticker(sym)
                    price = round_price(sym, ticker['last'])

                    avg = trade['average_entry']
                    side = trade['side']
                    adds_done = trade.get('adds_done', 0)

                    # DCA 1 & 2
                    for i, (pct, (pct, amt) in enumerate(ADD_LEVELS[:2]):
                        if adds_done > i: continue
                        trigger = avg * (1 - pct) if side == 'buy' else avg * (1 + pct)
                        if (side == 'buy' and price <= trigger) or (side == 'sell' and price >= trigger):
                            new_qty = trade['quantity'] + amt / price
                            new_avg = (trade['quantity'] * avg + amt) / new_qty
                            trade.update({
                                'adds_done': i+1,
                                'quantity': new_qty,
                                'average_entry': new_avg,
                                'total_invested': trade['total_invested'] + amt,
                                'tp': round_price(sym, new_avg * (1 + TP_PCT) if side=='buy' else new_avg * (1 - TP_PCT)),
                                'sl': round_price(sym, new_avg * (1 - SL_PCT) if side=='buy' else new_avg * (1 + SL_PCT)),
                                'dca_messages': trade.get('dca_messages', []) + [f"${amt} @ {price}"],
                                'dca_status': trade.get('dca_status', ["Pending"]*3)
                            })
                            trade['dca_status'][i] = "Added"
                            save_trades()
                            update_trade_message(sym, trade)
                            break

                    # DCA3 = Final SL
                    if adds_done < 2:
                        pct, _ = ADD_LEVELS[2]
                        trigger = avg * (1 - pct) if side == 'buy' else avg * (1 + pct)
                        if (side == 'buy' and price <= trigger) or (side == 'sell' and price >= trigger):
                            close_trade(sym, trade, "DCA3/SL Hit", price)
                            continue

                    # Normal TP/SL
                    if (side == 'buy' and price >= trade['tp']) or (side == 'sell' and price <= trade['tp']):
                        close_trade(sym, trade, "TP Hit", price)
                    elif (side == 'buy' and price <= trade['sl']) or (side == 'sell' and price >= trade['sl']):
                        close_trade(sym, trade, "SL Hit", price)

                except Exception as e:
                    logging.error(f"Monitor error {sym}: {e}")

def close_trade(sym, trade, reason, exit_price):
    pnl_pct = (exit_price - trade['average_entry']) / trade['average_entry'] * 100
    if trade['side'] == 'sell': pnl_pct = -pnl_pct
    leveraged_pnl = pnl_pct * LEVERAGE
    profit = round(trade['total_invested'] * leveraged_pnl / 100, 2)

    closed = {
        'symbol': sym, 'pnl': profit, 'pnl_pct': round(leveraged_pnl, 2),
        'reason': reason, 'invested': trade['total_invested'],
        'time': get_ist_time().strftime('%d %b %Y %H:%M')
    }
    save_closed_trades(closed)

    final_msg = trade['msg'] + f"\n\n{reason}\nExit: {exit_price}\nP&L: {leveraged_pnl:+.2f}% (${profit:+.2f})"
    edit_telegram_message(trade['msg_id'], final_msg)
    del open_trades[sym]
    save_trades()

def update_trade_message(sym, trade):
    sideEmoji = "🟢 LONG" if trade['side'] == 'buy' else "🔴 SHORT"
    lines = [
        f"🚨 {sym} - {sideEmoji}",
        f"Initial: {trade['initial_entry']}",
        f"Avg Entry: {trade['average_entry']:.6f}",
        f"Invested: ${trade['total_invested']:.2f}",
        ""
    ]
    for i, (pct, amt) in enumerate(ADD_LEVELS):
        price = round_price(sym, trade['initial_entry'] * (1 - pct if trade['side']=='buy' else 1 + pct))
        status = trade.get('dca_status', ["Pending"]*3)[i]
        emoji = "✅" if status == 'Added' else "⏳" if status == "Pending' else "❌ SL"
        label = "DCA1 +$5" if i==0 else "DCA2 +$10" if i==1 else "DCA3/SL"
        lines.append(f"{label}: {price} {emoji}")

    lines += ["", f"🎯 TP: {trade['tp']}", f"🛑 SL: {trade['sl']}"]
    if trade.get('dca_messages'):
        lines.append(f"Added: {', '.join(trade['dca_messages'])}")

    new_msg = "\n".join(lines)
    trade['msg'] = new_msg
    edit_telegram_message(trade['msg_id'], new_msg)

# ============================= MAIN SCAN =============================
alert_queue = queue.PriorityQueue()

def process_symbol(symbol, q):
    try:
        candles = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=30)
        if len(candles) < 25: return

        close_prev = candles[-2][4]
        ema9 = talib.EMA(np.array([c[4] for c in candles]), 9)[-1]
        ema21 = talib.EMA(np.array([c[4] for c in candles]), 21)[-1]
        if not ema9 or not ema21: return

        signal_time = candles[-2][0]

        if detect_rising_three(candles):  # SELL
            analysis = analyze_first_small_candle(candles[-3], 'rising')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'sell')) == signal_time: return
            sent_signals[(symbol, 'sell')] = signal_time

            greens = sum([close_prev < ema21, ema9 < ema21])  # good for short!
            if greens == 2:
                cat = "🟢🟢 TWO GREEN – STRONG"; prio = 3
            elif greens == 1:
                cat = "🟢🔴 ONE GREEN ONE CAUTION"; prio = 2
            else:
                cat = "🔴🔴 TWO CAUTIONS – WEAK"; prio = 1

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 - TP_PCT))
            sl = round_price(symbol, entry * (1 + SL_PCT))

            msg = f"""🚨 {symbol} - 🔴 SHORT (Rising Three)

📍 Entry: {entry}
🎯 TP: {tp}
🛑 SL: {sl}

{cat}

{analysis['text']}"""
            q.put((prio, symbol, msg, 'sell', entry, tp, sl))

        elif detect_falling_three(candles):  # BUY
            analysis = analyze_first_small_candle(candles[-3], 'falling')
            if analysis['body_pct'] > BODY_SIZE_THRESHOLD: return
            if sent_signals.get((symbol, 'buy')) == signal_time: return
            sent_signals[(symbol, 'buy')] = signal_time

            greens = sum([close_prev > ema21, ema9 > ema21])  # good for long!
            if greens == 2:
                cat = "🟢🟢 TWO GREEN – STRONG"; prio = 3
            elif greens == 1:
                cat = "🟢🔴 ONE GREEN ONE CAUTION"; prio = 2
            else:
                cat = "🔴🔴 TWO CAUTIONS – WEAK"; prio = 1

            entry = round_price(symbol, close_prev)
            tp = round_price(symbol, entry * (1 + TP_PCT))
            sl = round_price(symbol, entry * (1 - SL_PCT))

            msg = f"""🚨 {symbol} - 🟢 LONG (Falling Three)

📍 Entry: {entry}
🎯 TP: {tp}
🛑 SL: {sl}

{cat}

{analysis['text']}"""
            q.put((prio, symbol, msg, 'buy', entry, tp, sl))

    except Exception as e:
        logging.error(f"Scan error {symbol}: {e}")

def alert_handler():
    while True:
        prio, sym, msg, side, entry, tp, sl = alert_queue.get()
        with trade_lock:
            if len(open_trades) >= MAX_OPEN_TRADES:
                worst = min(open_trades.items(), key=lambda x: x[1].get('prio', 1))
                edit_telegram_message(worst[1]['msg_id'], worst[0] + " - CANCELED")
                del open_trades[worst[0]]

            msg_id = send_telegram(msg)
            if msg_id:
                open_trades[sym] = {
                    'side': side,
                    'initial_entry': entry,
                    'average_entry': entry,
                    'tp': tp, 'sl': sl,
                    'msg': msg, 'msg_id': msg_id,
                    'prio': prio,
                    'total_invested': CAPITAL,
                    'quantity': CAPITAL / entry,
                    'adds_done': 0,
                    'dca_status': ["Pending"]*3,
                    'dca_messages': []
                }
                save_trades()
                update_trade_message(sym, open_trades[sym])

# ============================= MAIN LOOP =============================
app = Flask(__name__)
@app.route('/')
def home():
    return f"Rising & Falling Three Bot Running | {get_ist_time().strftime('%d %b %Y %I:%M %p')}"

if __name__ == "__main__":
    load_trades()
    send_telegram(f"🤖 BOT RESTARTED\nOpen trades: {len(open_trades)}")
    threading.Thread(target=check_tp, daemon=True).start()
    threading.Thread(target=alert_handler, daemon=True).start()

    symbols = [s for s in exchange.load_markets() if s.endswith('USDT') and exchange.markets[s]['active'] and exchange.markets[s]['contract']]

    while True:
        next_close = time.time() + (900 - (time.time() % 900)) + 300
        time.sleep(max(0, next_close - time.time()))

        for sym in symbols:
            process_symbol(sym, alert_queue)

        # Hourly summary
        if time.time() - last_summary_time > SUMMARY_INTERVAL:
            trades = json.load(open(CLOSED_TRADE_FILE)) if os.path.exists(CLOSED_TRADE_FILE) else []
            total = sum(t['pnl'] for t in trades)
            wins = sum(1 for t in trades if t['pnl'] > 0)
            wr = len(trades) and round(wins/len(trades)*100, 1) or 0
            send_telegram(f"📊 SUMMARY\nTrades: {len(trades)}\nWin Rate: {wr}%\nProfit: ${total:+.2f}\nOpen: {len(open_trades)}")
            last_summary_time = time.time()

    app.run(host='0.0.0.0', port=8080)
