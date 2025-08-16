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

# === CONFIG ===
BOT_TOKEN = '7662307654:AAG5-juB1faNaFZfC8zjf4LwlZMzs6lEmtE'
CHAT_ID = '655537138'
TIMEFRAME = '30m'
MIN_BIG_BODY_PCT = 1.0
MAX_SMALL_BODY_PCT = 1.0
MIN_LOWER_WICK_PCT = 20.0
MAX_WORKERS = 10
BATCH_DELAY = 2.5
NUM_CHUNKS = 4
CAPITAL = 10.0
SL_PCT = 1.0 / 100
TP_SL_CHECK_INTERVAL = 30
TRADE_FILE = 'open_trades.json'
CLOSED_TRADE_FILE = 'closed_trades.json'

# Proxy Configuration
PROXY_HOST = '207.244.217.165'
PROXY_PORT = '6712'
PROXY_USERNAME = 'tytogvbu'
PROXY_PASSWORD = 'wb64rnowfoby'

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
    except Exception as e:
        print(f"Error loading trades: {e}")
        open_trades = {}

def save_closed_trades(closed_trade):
    try:
        all_closed_trades = []
        if os.path.exists(CLOSED_TRADE_FILE):
            with open(CLOSED_TRADE_FILE, 'r') as f:
                all_closed_trades = json.load(f)
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
                return json.load(f)
        return []
    except Exception as e:
        print(f"Error loading closed trades: {e}")
        return []

# === TELEGRAM WITH PROXY ===
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {'chat_id': CHAT_ID, 'text': msg}
    proxies = {
        'http': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}',
        'https': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}'
    }
    try:
        response = requests.post(url, data=data, proxies=proxies, timeout=5).json()
        print(f"Telegram sent: {msg}")
        return response.get('result', {}).get('message_id')
    except Exception as e:
        print(f"Telegram error: {e}")
        return None

def edit_telegram_message(message_id, new_text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText"
    data = {'chat_id': CHAT_ID, 'message_id': message_id, 'text': new_text}
    proxies = {
        'http': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}',
        'https': f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}'
    }
    try:
        requests.post(url, data=data, proxies=proxies, timeout=5)
        print(f"Telegram updated: {new_text}")
    except Exception as e:
        print(f"Edit error: {e}")

# === INIT ===
exchange = ccxt.binance({'options': {'defaultType': 'future'}})
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

# === EMA ===
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
                            'ema_status': trade['ema_status']
                        }
                        closed_trades.append(closed_trade)
                        save_closed_trades(closed_trade)
                        ema_status = trade['ema_status']
                        new_msg = (
                            f"{sym} - {'RISING' if trade['side'] == 'buy' else 'FALLING'} PATTERN\n"
                            f"{'Above' if trade['side'] == 'buy' else 'Below'} 21 ema - {ema_status['price_ema21']}\n"
                            f"ema 9 {'above' if trade['side'] == 'buy' else 'below'} 21 - {ema_status['ema9_ema21']}\n"
                            f"entry - {trade['entry']}\n"
                            f"tp - {trade['tp']}: {'✅' if 'TP' in hit else ''}\n"
                            f"sl - {trade['sl']:.4f}: {'❌' if 'SL' in hit else ''}\n"
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
            candles = exchange.fetch_ohlcv(symbol, timeframe=TIMEFRAME, limit=30)
            if len(candles) < 25:
                return
            if attempt < 2 and candles[-1][0] > candles[-2][0]:
                break
            time.sleep(1)

        ema21 = calculate_ema(candles, period=21)
        ema9 = calculate_ema(candles, period=9)
        if ema21 is None or ema9 is None:
            return

        signal_time = candles[-2][0]
        entry_price = candles[-2][4]
        big_candle_close = candles[-4][4]

        if detect_rising_three(candles):
            sl = entry_price * (1 - 0.015)  # Fixed 1.5% SL
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
            msg = (
                f"{symbol} - RISING PATTERN\n"
                f"Above 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 above 21 - {ema_status['ema9_ema21']}\n"
                f"entry - {entry_price}\n"
                f"tp - {big_candle_close}\n"
                f"sl - {sl:.4f}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category))

        elif detect_falling_three(candles):
            sl = entry_price * (1 + 0.015)  # Fixed 1.5% SL
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
            msg = (
                f"{symbol} - FALLING PATTERN\n"
                f"Below 21 ema - {ema_status['price_ema21']}\n"
                f"ema 9 below 21 - {ema_status['ema9_ema21']}\n"
                f"entry - {entry_price}\n"
                f"tp - {big_candle_close}\n"
                f"sl - {sl:.4f}\n"
                f"Trade going on..."
            )
            alert_queue.put((symbol, msg, ema_status, category))

    except ccxt.RateLimitExceeded:
        time.sleep(5)
    except Exception as e:
        print(f"Error on {symbol}: {e}")

# === PROCESS BATCH ===
def process_batch(symbols, alert_queue):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol, alert_queue): symbol for symbol in symbols}
        for future in as_completed(future_to_symbol):
            future.result()

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
                symbol, msg, ema_status, category = alert_queue.get(timeout=1)
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
                        'category': category
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

        total_pnl = two_green_pnl + one_green_pnl + two_cautions_pnl
        total_pnl_pct = two_green_pnl_pct + one_green_pnl_pct + two_cautions_pnl_pct
        cumulative_pnl = total_pnl
        cumulative_pnl_pct = total_pnl_pct

        # Find top symbol
        if all_closed_trades:
            symbol_pnl = {}
            for trade in all_closed_trades:
                sym = trade['symbol']
                symbol_pnl[sym] = symbol_pnl.get(sym, 0) + trade['pnl']
            top_symbol = max(symbol_pnl.items(), key=lambda x: x[1], default=(None, 0))
            top_symbol_name, top_symbol_pnl = top_symbol
            top_symbol_pnl_pct = sum(t['pnl_pct'] for t in all_closed_trades if t['symbol'] == top_symbol_name)
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
            f"💰 Total PnL: ${total_pnl:.2f} ({total_pnl_pct:.2f}%)\n"
            f"📈 Cumulative PnL: ${cumulative_pnl:.2f} ({cumulative_pnl_pct:.2f}%)\n"
            f"🏆 Top Symbol: {top_symbol_name or 'None'} with ${top_symbol_pnl:.2f} ({top_symbol_pnl_pct:.2f}%)\n"
            f"🔄 Open Trades: {num_open}"
        )
        send_telegram(summary_msg)
        send_telegram(f"Number of open trades after scan: {num_open}")

        # Reset closed_trades after generating the summary
        closed_trades = []

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
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    run_bot()
