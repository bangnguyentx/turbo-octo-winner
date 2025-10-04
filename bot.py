# bot.py
# Quick Lottery (Quick 6-digit) — Telegram bot
# - 6 digits (0-9) per round, sent sequentially
# - Bets: /N<amt> (Nhỏ), /L<amt> (Lớn), /C<amt> (Chẵn), /Le<amt> (Lẻ), /S<digits> <amt> (Number bet)
# - Admin force: /Nho, /Lon, /Chan, /Le  (one-shot)
# - History: last 15 rounds vertical, icons: ⚪ Nhỏ, ⚫ Lớn, 🟠 Chẵn, 🔵 Lẻ
# - DB: SQLite (keeps users, bets, history, pot, promo)
# - Designed to run on Render (keep port open)
#
# IMPORTANT:
# - Set BOT_TOKEN in environment variable in Render.
# - Do not paste bot token here in chat. Replace BOT_TOKEN placeholder if testing locally.

import os
import sys
import sqlite3
import random
import traceback
import logging
import threading
import http.server
import socketserver
import asyncio
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
import secrets

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup,
    KeyboardButton, ChatPermissions
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, Application
)

# -----------------------
# Keep port open (for Render)
# -----------------------
def keep_port_open():
    PORT = int(os.getenv("PORT", "10000"))
    handler = http.server.SimpleHTTPRequestHandler
    try:
        with socketserver.TCPServer(("", PORT), handler) as httpd:
            print(f"[keep_port_open] serving on port {PORT}")
            httpd.serve_forever()
    except Exception as e:
        print(f"[keep_port_open] {e}")

threading.Thread(target=keep_port_open, daemon=True).start()

# -----------------------
# Config (can be overridden by env)
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "7760459637,6942793864").split(",") if x.strip()]
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", "60"))
MIN_BET = int(os.getenv("MIN_BET", "1000"))
START_BONUS = int(os.getenv("START_BONUS", "80000"))
START_BONUS_REQUIRED_ROUNDS = int(os.getenv("START_BONUS_REQUIRED_ROUNDS", "8"))
WIN_MULTIPLIER = float(os.getenv("WIN_MULTIPLIER", "1.97"))  # multiplier for size/parity bets
HOUSE_RATE = float(os.getenv("HOUSE_RATE", "0.03"))
DB_FILE = os.getenv("DB_FILE", "tx_bot_data.db")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "15"))
DICE_SPIN_GIF_URL = os.getenv("DICE_SPIN_GIF_URL", "")  # optional

# multipliers for number bets by length (index = length)
NUMBER_MULTIPLIERS = {1: 9.2, 2: 90, 3: 900, 4: 9000, 5: 80000, 6: 100000}

# icons
ICON_SMALL = "⚪"
ICON_BIG = "⚫"
ICON_EVEN = "🟠"
ICON_ODD = "🔵"

# logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# DB helpers
# -----------------------
def get_db_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        balance REAL DEFAULT 0,
        total_deposited REAL DEFAULT 0,
        total_bet_volume REAL DEFAULT 0,
        current_streak INTEGER DEFAULT 0,
        best_streak INTEGER DEFAULT 0,
        created_at TEXT,
        start_bonus_given INTEGER DEFAULT 0,
        start_bonus_progress INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS groups (
        chat_id INTEGER PRIMARY KEY,
        title TEXT,
        approved INTEGER DEFAULT 0,
        running INTEGER DEFAULT 0,
        bet_mode TEXT DEFAULT 'random',
        forced_outcome TEXT DEFAULT NULL, -- 'small','big','even','odd' or NULL
        last_round INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS bets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        round_id TEXT,
        user_id INTEGER,
        bet_type TEXT, -- 'size','parity','number'
        bet_value TEXT, -- 'small'/'big' or 'even'/'odd' or digits string like '123'
        amount REAL,
        timestamp TEXT
    );

    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        round_index INTEGER,
        round_id TEXT,
        result_size TEXT,  -- 'small' or 'big'
        result_parity TEXT, -- 'even' or 'odd'
        digits TEXT, -- '4,5,7,8,9,1'
        timestamp TEXT
    );

    CREATE TABLE IF NOT EXISTS pot (
        id INTEGER PRIMARY KEY CHECK (id=1),
        amount REAL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS promo_codes (
        code TEXT PRIMARY KEY,
        amount REAL,
        wager_required INTEGER,
        used INTEGER DEFAULT 0,
        created_by INTEGER,
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS promo_redemptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT,
        user_id INTEGER,
        amount REAL,
        wager_required INTEGER,
        wager_progress INTEGER DEFAULT 0,
        last_counted_round TEXT DEFAULT '',
        active INTEGER DEFAULT 1,
        redeemed_at TEXT
    );
    """)
    cur.execute("INSERT OR IGNORE INTO pot(id, amount) VALUES (1, 0)")
    conn.commit()
    conn.close()

def db_execute(query: str, params: Tuple = ()):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    lastrowid = cur.lastrowid
    conn.close()
    return lastrowid

def db_query(query: str, params: Tuple = ()):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows

# -----------------------
# Helpers
# -----------------------
def now_iso():
    return datetime.utcnow().isoformat()

def ensure_user(user_id: int, username: str = "", first_name: str = ""):
    rows = db_query("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not rows:
        db_execute(
            "INSERT INTO users(user_id, username, first_name, balance, total_deposited, total_bet_volume, current_streak, best_streak, created_at, start_bonus_given, start_bonus_progress) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, username or "", first_name or "", 0.0, 0.0, 0.0, 0, 0, now_iso(), 0, 0)
        )

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    rows = db_query("SELECT * FROM users WHERE user_id=?", (user_id,))
    return dict(rows[0]) if rows else None

def add_balance(user_id: int, amount: float):
    ensure_user(user_id, "", "")
    u = get_user(user_id)
    new_bal = (u["balance"] or 0.0) + amount
    db_execute("UPDATE users SET balance=? WHERE user_id=?", (new_bal, user_id))
    return new_bal

def set_balance(user_id: int, amount: float):
    ensure_user(user_id, "", "")
    db_execute("UPDATE users SET balance=? WHERE user_id=?", (amount, user_id))

def add_to_pot(amount: float):
    rows = db_query("SELECT amount FROM pot WHERE id=1")
    current = rows[0]["amount"] if rows else 0.0
    new = current + amount
    db_execute("UPDATE pot SET amount=? WHERE id=1", (new,))

def get_pot_amount() -> float:
    rows = db_query("SELECT amount FROM pot WHERE id=1")
    return rows[0]["amount"] if rows else 0.0

def reset_pot():
    db_execute("UPDATE pot SET amount=? WHERE id=1", (0.0,))

# -----------------------
# Lottery (6-digit) logic
# -----------------------
def roll_one_digit() -> int:
    return random.randint(0, 9)

def roll_six_digits() -> List[int]:
    return [roll_one_digit() for _ in range(6)]

def classify_by_last_digit(digits: List[int]) -> Tuple[str, str]:
    """Return (size, parity) where size='small'|'big', parity='even'|'odd' based on last digit"""
    last = digits[-1]
    size = "small" if 0 <= last <= 5 else "big"
    parity = "even" if last % 2 == 0 else "odd"
    return size, parity

def icons_for_result(size: str, parity: str) -> str:
    parts = []
    parts.append(ICON_SMALL if size == "small" else ICON_BIG)
    parts.append(ICON_EVEN if parity == "even" else ICON_ODD)
    return " ".join(parts)

# -----------------------
# UI / Private menu
# -----------------------
MAIN_MENU = ReplyKeyboardMarkup(
    [[KeyboardButton("Quick lottery")]],
    resize_keyboard=True
)

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    greeted = False
    if u and u.get("start_bonus_given", 0) == 0:
        add_balance(user.id, START_BONUS)
        db_execute("UPDATE users SET total_deposited=COALESCE(total_deposited,0)+?, start_bonus_given=1, start_bonus_progress=0 WHERE user_id=?", (START_BONUS, user.id))
        greeted = True

    text = f"Xin chào {user.first_name or 'bạn'}! 👋\nGame Quick Lottery chạy mỗi {ROUND_SECONDS}s.\n"
    if greeted:
        text += f"Bạn đã nhận {START_BONUS:,}₫ miễn phí (một lần). Để rút, cược tối thiểu {START_BONUS_REQUIRED_ROUNDS} vòng.\n\n"
    text += "Menu:\n- Quick lottery\n\n(Lưu ý: Menu chỉ hiện trong tin nhắn riêng với bot.)"
    await update.message.reply_text(text, reply_markup=MAIN_MENU)

async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().lower()
    if txt == "quick lottery":
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("Quick lottery", callback_data="game_quick")]])
        await update.message.reply_text("Chọn game:", reply_markup=kb)
    elif txt in ("nạp tiền", "nap tien", "nạp"):
        await update.message.reply_text("Liên hệ để nạp: @HOANGDUNGG789")
    elif txt in ("rút tiền", "rut tien", "ruttien"):
        await ruttien_handler(update, context)
    elif txt in ("số dư", "so du", "sodu"):
        u = get_user(update.effective_user.id)
        bal = int(u["balance"]) if u else 0
        await update.message.reply_text(f"Số dư hiện tại: {bal:,}₫")
    else:
        await update.message.reply_text("Dùng menu để chọn game hoặc các lệnh hợp lệ trong riêng bot.")

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "game_quick":
        await q.message.reply_text("Quick Lottery: chơi trong nhóm. Các lệnh cược (chỉ dùng trong nhóm):\n"
                                  "/N<tiền> = Nhỏ (0-5)\n"
                                  "/L<tiền> = Lớn (6-9)\n"
                                  "/C<tiền> = Chẵn\n"
                                  "/Le<tiền> = Lẻ\n"
                                  "/S<dãy> <tiền> = Đặt cược số (1-6 chữ số). Ví dụ: /S91 1000\n"
                                  "Tỷ lệ: Nhỏ/Lớn/Chẵn/Lẻ x1.97 | Số: theo số chữ số (1→x9.2, 2→x90, ...)")
    else:
        await q.message.reply_text("Chưa hỗ trợ.")

# -----------------------
# Withdraw placeholder
# -----------------------
async def ruttien_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Dùng: /ruttien <Ngân hàng> <Số TK> <Số tiền>")
        return
    await update.message.reply_text("Yêu cầu rút đã được lưu. (Xử lý offline bởi admin.)")

# -----------------------
# Betting handler (group-only)
# Accepts: /N<amt>, /L<amt>, /C<amt>, /Le<amt>, /S<digits> <amt>
# -----------------------
# -----------------------
async def bet_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    text = msg.text.strip()
    chat = update.effective_chat
    user = update.effective_user

    # Only allow in groups
    if chat.type not in ("group", "supergroup"):
        await msg.reply_text("Lệnh cược chỉ dùng trong nhóm.")
        return

    # Check group is approved & running
    g = db_query("SELECT approved, running FROM groups WHERE chat_id=?", (chat.id,))
    if not g or g[0]["approved"] != 1 or g[0]["running"] != 1:
        await msg.reply_text("Nhóm này chưa được admin duyệt hoặc chưa bật /batdau.")
        return

    # parse
    # allow both /N1000 and N1000 etc
    txt = text
    if txt.startswith("/"):
        txt = txt[1:]
    parts = txt.split()
    cmd = parts[0]

    # #1 Size bets: N / L
    prefix = cmd[0].lower()
    bet_type = None
    bet_value = None
    amount = None

    try:
        if prefix in ("n", "l") and cmd[1:].isdigit():
            # /N1000 or /L1000
            amount = int(cmd[1:])
            bet_type = "size"
            bet_value = "small" if prefix == "n" else "big"
        elif (cmd.lower().startswith("c") or cmd.lower().startswith("le")):
            # /C1000 or /Le1000
            # support /c1000 and /le1000
            if cmd.lower().startswith("c") and cmd[1:].isdigit():
                amount = int(cmd[1:])
                bet_type = "parity"
                bet_value = "even"
            elif cmd.lower().startswith("le") and cmd[2:].isdigit():
                amount = int(cmd[2:])
                bet_type = "parity"
                bet_value = "odd"
            else:
                # possibly /Le 1000 (with space)
                if cmd.lower().startswith("le") and len(parts) >= 2 and parts[1].isdigit():
                    amount = int(parts[1])
                    bet_type = "parity"
                    bet_value = "odd"
        elif cmd.lower().startswith("s"):
            # number bets: could be /S123 1000  or /S1231000 (we expect /S<digits> <amt> or /S<digits><amt>)
            # prefer form: /S<digits> <amount>
            after = cmd[1:]
            if after.isdigit() and len(parts) >= 2 and parts[1].isdigit():
                # form: /S123 1000
                bet_digits = after
                amount = int(parts[1])
                bet_type = "number"
                bet_value = bet_digits
            else:
                # maybe combined: /S1231000 (ambiguous). Try to split last up to 6 digits for bet_digits
                # We'll try: take leading digits as bet_digits up to 6, trailing as amount
                rest = cmd[1:]
                # find split between bet_digits (1..6) and amount (>=MIN_BET)
                found = False
                for l in range(1, 7):  # bet_digits length
                    if len(rest) > l:
                        bd = rest[:l]
                        am = rest[l:]
                        if bd.isdigit() and am.isdigit():
                            if int(am) >= MIN_BET:
                                bet_type = "number"
                                bet_value = bd
                                amount = int(am)
                                found = True
                                break
                if not found:
                    # maybe form: /S 123 1000 (with space after S)
                    if len(parts) >= 3 and parts[0].lower() == "s" and parts[1].isdigit() and parts[2].isdigit():
                        bet_type = "number"
                        bet_value = parts[1]
                        amount = int(parts[2])
        else:
            # not a bet we support in group
            return
    except Exception:
        await msg.reply_text("❌ Cú pháp cược không hợp lệ. Ví dụ: /N1000, /L1000, /C1000, /Le1000, /S91 1000")
        return

    # final validation
    if not bet_type or not bet_value or not isinstance(amount, int):
        await msg.reply_text("❌ Cú pháp cược không hợp lệ. Ví dụ: /N1000, /L1000, /C1000, /Le1000, /S91 1000")
        return

    if amount < MIN_BET:
        await msg.reply_text(f"⚠️ Đặt cược tối thiểu {MIN_BET:,}₫")
        return

    # check user balance
    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    if not u or (u["balance"] or 0.0) < amount:
        await msg.reply_text("❌ Số dư không đủ.")
        return

    # deduct immediately and record bet
    new_balance = (u["balance"] or 0.0) - amount
    new_total = (u["total_bet_volume"] or 0.0) + amount
    db_execute("UPDATE users SET balance=?, total_bet_volume=? WHERE user_id=?", (new_balance, new_total, user.id))

    # store bet
    now_ts = int(datetime.utcnow().timestamp())
    round_epoch = now_ts // ROUND_SECONDS
    round_id = f"{chat.id}_{round_epoch}"
    db_execute(
        "INSERT INTO bets(chat_id, round_id, user_id, bet_type, bet_value, amount, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (chat.id, round_id, user.id, bet_type, bet_value, amount, now_iso())
    )

    # update promo progress if any
    try:
        await update_promo_wager_progress(context, user.id, round_id)
    except Exception:
        logger.exception("promo progress fail")

    # reply without showing balance
    readable = ""
    if bet_type == "size":
        readable = f"{'Nhỏ' if bet_value=='small' else 'Lớn'}"
    elif bet_type == "parity":
        readable = f"{'Chẵn' if bet_value=='even' else 'Lẻ'}"
    else:
        readable = f"Số `{bet_value}`"
    await msg.reply_text(f"✅ Đã đặt {readable} — {amount:,}₫ cho phiên hiện tại.")

# -----------------------
# Admin commands to force result for next round
# /Nho, /Lon, /Chan, /Le  (one-shot)
# -----------------------
async def admin_force_outcome_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin mới được dùng lệnh này.")
        return
    text = update.message.text.strip().lower()
    chat = update.effective_chat
    if chat.type not in ("group", "supergroup"):
        await update.message.reply_text("Lệnh này chỉ dùng trực tiếp trong nhóm để ép kết quả cho nhóm đó.")
        return
    if text.startswith("/nho"):
        val = "small"
    elif text.startswith("/lon"):
        val = "big"
    elif text.startswith("/chan"):
        val = "even"
    elif text.startswith("/le"):
        val = "odd"
    else:
        await update.message.reply_text("Lệnh không hợp lệ. Dùng /Nho, /Lon, /Chan, /Le")
        return

    db_execute("UPDATE groups SET forced_outcome=? WHERE chat_id=?", (val, chat.id))
    await update.message.reply_text(f"✅ Đã ép kết quả cho phiên tiếp theo: {val}")

# -----------------------
# Helpers for promo (kept from original)
# -----------------------
async def update_promo_wager_progress(context: ContextTypes.DEFAULT_TYPE, user_id: int, round_id: str):
    try:
        rows = db_query("SELECT id, code, wager_required, wager_progress, last_counted_round, active, amount FROM promo_redemptions WHERE user_id=? AND active=1", (user_id,))
        if not rows:
            return
        for r in rows:
            rid = r["id"]; last = r["last_counted_round"] or ""
            if str(last) == str(round_id):
                continue
            new_progress = (r["wager_progress"] or 0) + 1
            active = 1
            if new_progress >= (r["wager_required"] or 0):
                active = 0
            db_execute("UPDATE promo_redemptions SET wager_progress=?, last_counted_round=?, active=? WHERE id=?", (new_progress, str(round_id), active, rid))
            if active == 0:
                try:
                    await context.bot.send_message(chat_id=user_id, text=f"✅ Bạn đã hoàn thành yêu cầu cược cho code {r['code']}! Tiền {int(r['amount']):,}₫ hiện đã hợp lệ.")
                except Exception:
                    pass
    except Exception:
        logger.exception("update_promo_wager_progress failed")

# -----------------------
# History formatting
# -----------------------
def format_history_block(chat_id: int, limit: int = MAX_HISTORY) -> str:
    rows = db_query("SELECT round_index, digits, result_size, result_parity FROM history WHERE chat_id=? ORDER BY id DESC LIMIT ?", (chat_id, limit))
    if not rows:
        return ""
    lines = []
    # reversed so oldest first
    for r in reversed(rows):
        idx = r["round_index"]
        digits = r["digits"] or ""
        size = r["result_size"] or ""
        parity = r["result_parity"] or ""
        icons = icons_for_result(size, parity)
        # each line like: "12345 — ⚪ 🟠"
        lines.append(f"{idx}: {digits} — {icons}")
    return "\n".join(lines)

# -----------------------
# Rounds engine
# -----------------------
async def send_countdown(bot, chat_id: int, seconds: int):
    try:
        if seconds == 30:
            await bot.send_message(chat_id=chat_id, text="⏰ Còn 30 giây trước khi quay kết quả — nhanh tay cược!")
        elif seconds == 10:
            await bot.send_message(chat_id=chat_id, text="⚠️ Còn 10 giây! Sắp khóa cược.")
        elif seconds == 5:
            await bot.send_message(chat_id=chat_id, text="🔒 Còn 5 giây — Chat bị khóa để chốt cược.")
            await lock_group_chat(bot, chat_id)
    except Exception:
        pass

async def lock_group_chat(bot, chat_id: int):
    try:
        perms = ChatPermissions(can_send_messages=False)
        await bot.set_chat_permissions(chat_id=chat_id, permissions=perms)
    except Exception:
        pass

async def unlock_group_chat(bot, chat_id: int):
    try:
        perms = ChatPermissions(
            can_send_messages=True,
            can_send_media_messages=True,
            can_send_polls=True,
            can_send_other_messages=True,
            can_add_web_page_previews=True
        )
        await bot.set_chat_permissions(chat_id=chat_id, permissions=perms)
    except Exception:
        pass

async def run_round_for_group(app: Application, chat_id: int, round_epoch: int):
    """
    Run one round for group:
    - determine round_id
    - collect bets
    - if forced outcome present, attempt to get digits matching it
    - send GIF (optional) and each digit sequentially
    - calculate result (size & parity by last digit)
    - calculate winners (size/parity/number rules) and pay out
    - update pot: losers -> pot; house_share -> pot
    - special: none (kept simple)
    """
    try:
        round_index = int(round_epoch)
        round_id = f"{chat_id}_{round_epoch}"

        # fetch bets for this round
        bets_rows = db_query("SELECT user_id, bet_type, bet_value, amount FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        bets = [dict(r) for r in bets_rows] if bets_rows else []

        # fetch group forced outcome if any (one-shot)
        grows = db_query("SELECT bet_mode, forced_outcome FROM groups WHERE chat_id=?", (chat_id,))
        forced = None
        if grows:
            forced = grows[0]["forced_outcome"]

        # announce
        try:
            await app.bot.send_message(chat_id=chat_id, text=f"🎲 Phiên {round_index} — Đang tung 6 chữ số...")
        except Exception:
            pass

        # optional GIF
        if DICE_SPIN_GIF_URL:
            try:
                await app.bot.send_animation(chat_id=chat_id, animation=DICE_SPIN_GIF_URL, caption="🔄 Quay nhanh...")
                await asyncio.sleep(0.8)
            except Exception:
                pass

        # generate digits
        digits = []
        special = None
        # If forced, try to generate digits whose last digit yields forced property (small/big/even/odd)
        if forced in ("small", "big", "even", "odd"):
            attempts = 0
            while attempts < 500:
                cand = roll_six_digits()
                size, parity = classify_by_last_digit(cand)
                ok = False
                if forced == "small" and size == "small":
                    ok = True
                elif forced == "big" and size == "big":
                    ok = True
                elif forced == "even" and parity == "even":
                    ok = True
                elif forced == "odd" and parity == "odd":
                    ok = True
                if ok:
                    digits = cand
                    break
                attempts += 1
            if not digits:
                digits = roll_six_digits()
            # consume forced outcome (one-shot) -> reset in DB
            try:
                db_execute("UPDATE groups SET forced_outcome=NULL WHERE chat_id=?", (chat_id,))
            except Exception:
                pass
        else:
            digits = roll_six_digits()

        # send digits sequentially (1s interval)
        for d in digits:
            try:
                await app.bot.send_message(chat_id=chat_id, text=f"{d}")
            except Exception:
                pass
            await asyncio.sleep(1.0)

        # compute final classification
        size, parity = classify_by_last_digit(digits)
        digits_str = "".join(str(d) for d in digits)

        # persist history
        try:
            db_execute(
                "INSERT INTO history(chat_id, round_index, round_id, result_size, result_parity, digits, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (chat_id, round_index, round_id, size, parity, digits_str, now_iso())
            )
        except Exception:
            logger.exception("Failed to insert history")

        # compute winners/losers
        winners = []  # list of tuples (uid, payout, amt_bet)
        losers_total = 0.0

        for b in bets:
            uid = int(b["user_id"])
            btype = b["bet_type"]
            bval = b["bet_value"]
            amt = float(b["amount"] or 0.0)
            win = False
            payout = 0.0

            if btype == "size":
                if (bval == "small" and size == "small") or (bval == "big" and size == "big"):
                    win = True
                    payout = amt * WIN_MULTIPLIER
            elif btype == "parity":
                if (bval == "even" and parity == "even") or (bval == "odd" and parity == "odd"):
                    win = True
                    payout = amt * WIN_MULTIPLIER
            elif btype == "number":
                # bval is a string of digits user bet (1..6 digits)
                # win if bval appears as contiguous substring in digits_str
                if isinstance(bval, str) and bval != "" and bval in digits_str:
                    ln = len(bval)
                    ln = max(1, min(6, ln))
                    mult = NUMBER_MULTIPLIERS.get(ln, 0)
                    payout = amt * mult
                    win = True
            else:
                # unknown bet type: treat as loss
                win = False

            if win:
                winners.append((uid, payout, amt))
            else:
                losers_total += amt

        # add losers_total to pot
        try:
            if losers_total > 0:
                db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (losers_total,))
        except Exception:
            logger.exception("Failed to add losers to pot")

        # pay winners (and house share into pot)
        winners_paid = []
        for uid, payout, amt in winners:
            try:
                house_share = amt * HOUSE_RATE
                if house_share > 0:
                    try:
                        db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (house_share,))
                    except Exception:
                        logger.exception("Failed adding house share")
                ensure_user(uid, "", "")
                # update user's balance and streaks
                try:
                    db_execute(
                        """
                        UPDATE users SET
                            balance = COALESCE(balance,0) + ?,
                            current_streak = COALESCE(current_streak,0) + 1,
                            best_streak = CASE WHEN COALESCE(current_streak,0) + 1 > COALESCE(best_streak,0) THEN COALESCE(current_streak,0) + 1 ELSE COALESCE(best_streak,0) END
                        WHERE user_id = ?
                        """,
                        (payout, uid)
                    )
                except Exception:
                    u = get_user(uid) or {"balance": 0, "current_streak": 0, "best_streak": 0}
                    new_balance = (u.get("balance") or 0) + payout
                    new_cur = (u.get("current_streak") or 0) + 1
                    new_best = max(u.get("best_streak") or 0, new_cur)
                    db_execute("UPDATE users SET balance=?, current_streak=?, best_streak=? WHERE user_id=?", (new_balance, new_cur, new_best, uid))
                winners_paid.append((uid, payout, amt))
            except Exception:
                logger.exception(f"Error paying winner {uid}")

        # reset streak for losers
        try:
            for b in bets:
                uid = int(b["user_id"])
                # if lost, reset - detect if user won in winners list
                if not any(w[0] == uid for w in winners_paid):
                    db_execute("UPDATE users SET current_streak=0 WHERE user_id=?", (uid,))
        except Exception:
            logger.exception("Failed resetting streaks")

        # delete bets for this round
        try:
            db_execute("DELETE FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        except Exception:
            logger.exception("Failed to delete bets for round")

        # prepare and send result message + history block (vertical)
        display = "Nhỏ" if size == "small" else "Lớn"
        icons = icons_for_result(size, parity)
        history_block = format_history_block(chat_id, MAX_HISTORY)
        msg = f"▶️ Phiên {round_index} — Kết quả: {display} {icons}\n"
        msg += f"Số: {' '.join(str(d) for d in digits)} — (chuỗi: {digits_str})\n"
        if history_block:
            msg += f"\nLịch sử (tối đa {MAX_HISTORY}):\n{history_block}\n"

        if winners_paid:
            msg += f"\nNgười thắng: {len(winners_paid)} người."
        else:
            msg += f"\nKhông có người thắng trong phiên này."

        try:
            await app.bot.send_message(chat_id=chat_id, text=msg)
        except Exception:
            logger.exception("Cannot send round result to group")

        # admin summary
        if winners_paid:
            admin_summary = f"Round {round_index} | Group {chat_id}\nResult: {size}/{parity} | {digits_str}\nWinners:\n"
            for uid, payout, amt in winners_paid:
                admin_summary += f"- {uid}: đặt {int(amt):,} -> nhận {int(payout):,}\n"
            for aid in ADMIN_IDS:
                try:
                    await app.bot.send_message(chat_id=aid, text=admin_summary)
                except Exception:
                    pass

        # unlock chat
        try:
            await unlock_group_chat(app.bot, chat_id)
        except Exception:
            pass

    except Exception as e:
        logger.exception(f"Exception in run_round_for_group: {e}")
        for aid in ADMIN_IDS:
            try:
                await app.bot.send_message(chat_id=aid, text=f"ERROR run_round_for_group for {chat_id}: {e}\n{traceback.format_exc()}")
            except Exception:
                pass

# rounds orchestrator
async def rounds_loop(app: Application):
    logger.info("Rounds loop started")
    await asyncio.sleep(2)
    while True:
        try:
            now_ts = int(datetime.utcnow().timestamp())
            next_epoch_ts = ((now_ts // ROUND_SECONDS) + 1) * ROUND_SECONDS
            rem = next_epoch_ts - now_ts

            # schedule countdowns
            if rem > 30:
                await asyncio.sleep(rem - 30)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows:
                    asyncio.create_task(send_countdown(app.bot, r["chat_id"], 30))
                await asyncio.sleep(20)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows:
                    asyncio.create_task(send_countdown(app.bot, r["chat_id"], 10))
                await asyncio.sleep(5)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows:
                    asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                await asyncio.sleep(5)
            else:
                if rem > 10:
                    await asyncio.sleep(rem - 10)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows:
                        asyncio.create_task(send_countdown(app.bot, r["chat_id"], 10))
                    await asyncio.sleep(5)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows:
                        asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                    await asyncio.sleep(5)
                elif rem > 5:
                    await asyncio.sleep(rem - 5)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows:
                        asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                    await asyncio.sleep(5)
                else:
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows:
                        asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                    await asyncio.sleep(rem)

            round_epoch = int(datetime.utcnow().timestamp()) // ROUND_SECONDS
            rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
            tasks = []
            for r in rows:
                tasks.append(asyncio.create_task(run_round_for_group(app, r["chat_id"], round_epoch)))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception:
            logger.exception("Exception in rounds_loop")
            for aid in ADMIN_IDS:
                try:
                    await app.bot.send_message(chat_id=aid, text=f"ERROR rounds_loop:\n{traceback.format_exc()}")
                except Exception:
                    pass

# -----------------------
# Group approval commands (unchanged logic)
# -----------------------
async def batdau_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group","supergroup"):
        await update.message.reply_text("/batdau chỉ dùng trong nhóm.")
        return
    title = chat.title or ""
    rows = db_query("SELECT chat_id FROM groups WHERE chat_id=?", (chat.id,))
    if not rows:
        db_execute("INSERT INTO groups(chat_id, title, approved, running, bet_mode, forced_outcome, last_round) VALUES (?, ?, 0, 0, 'random', NULL, ?)", (chat.id, title, 0))
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Duyệt", callback_data=f"approve|{chat.id}"),
         InlineKeyboardButton("Từ chối", callback_data=f"deny|{chat.id}")]
    ])
    text = f"Yêu cầu bật bot cho nhóm:\n{title}\nchat_id: {chat.id}\nNgười yêu cầu: {update.effective_user.id}"
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=text, reply_markup=kb)
        except Exception:
            logger.exception("Cannot notify admin for group approval")
    await update.message.reply_text("Đã gửi yêu cầu tới admin để duyệt.")

async def approve_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    parts = (query.data or "").split("|")
    if len(parts) != 2:
        await query.edit_message_text("Dữ liệu không hợp lệ.")
        return
    action, chat_id_s = parts
    try:
        chat_id = int(chat_id_s)
    except:
        await query.edit_message_text("chat_id không hợp lệ.")
        return
    if query.from_user.id not in ADMIN_IDS:
        await query.edit_message_text("Chỉ admin mới thao tác.")
        return
    if action == "approve":
        db_execute("UPDATE groups SET approved=1, running=1 WHERE chat_id=?", (chat_id,))
        await query.edit_message_text(f"Đã duyệt và bật chạy cho nhóm {chat_id}.")
        try:
            await context.bot.send_message(chat_id=chat_id, text=f"Bot đã được admin duyệt — bắt đầu chạy phiên mỗi {ROUND_SECONDS}s. Gõ /batdau để yêu cầu chạy lại.")
        except:
            pass
    else:
        db_execute("UPDATE groups SET approved=0, running=0 WHERE chat_id=?", (chat_id,))
        await query.edit_message_text(f"Đã từ chối cho nhóm {chat_id}.")

# -----------------------
# Admin utility handlers (kept simplified)
# -----------------------
async def addmoney_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Cú pháp: /addmoney <user_id> <amount>")
        return
    try:
        uid = int(args[0]); amt = float(args[1])
    except:
        await update.message.reply_text("Tham số không hợp lệ.")
        return
    ensure_user(uid, "", "")
    new_bal = add_balance(uid, amt)
    db_execute("UPDATE users SET total_deposited=COALESCE(total_deposited,0)+? WHERE user_id=?", (amt, uid))
    await update.message.reply_text(f"Đã cộng {int(amt):,}₫ cho user {uid}. Số dư hiện: {int(new_bal):,}₫")
    try:
        await context.bot.send_message(chat_id=uid, text=f"Bạn vừa được admin cộng {int(amt):,}₫. Số dư: {int(new_bal):,}₫")
    except:
        pass

async def top10_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    rows = db_query("SELECT user_id, total_deposited FROM users ORDER BY total_deposited DESC LIMIT 10")
    text = "Top 10 nạp nhiều nhất:\n"
    for i, r in enumerate(rows, start=1):
        text += f"{i}. {r['user_id']} — {int(r['total_deposited'] or 0):,}₫\n"
    await update.message.reply_text(text)

async def balances_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    rows = db_query("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT 50")
    text = "Top balances:\n"
    for r in rows:
        text += f"- {r['user_id']}: {int(r['balance'] or 0):,}₫\n"
    await update.message.reply_text(text)

# -----------------------
# Promo code handlers (kept)
# -----------------------
async def admin_create_code_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    if not context.args or len(context.args) < 2:
        await update.message.reply_text("Cú pháp: /code <amount> <wager_rounds>")
        return
    try:
        amount = int(float(context.args[0])); wager_required = int(context.args[1])
    except:
        await update.message.reply_text("Tham số không hợp lệ.")
        return
    code = secrets.token_hex(4).upper()
    created_at = now_iso()
    db_execute("INSERT INTO promo_codes(code, amount, wager_required, used, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)",
               (code, amount, wager_required, 0, update.effective_user.id, created_at))
    await update.message.reply_text(f"Đã tạo code `{code}` — {int(amount):,}₫ — phải cược {wager_required} vòng. Người dùng nhập /nhancode {code}", parse_mode="Markdown")

async def redeem_code_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Cú pháp: /nhancode <CODE>")
        return
    code = context.args[0].strip().upper()
    rows = db_query("SELECT code, amount, wager_required, used FROM promo_codes WHERE code=?", (code,))
    if not rows:
        await update.message.reply_text("Code không tồn tại.")
        return
    row = rows[0]
    if row["used"] == 1:
        await update.message.reply_text("Code đã được sử dụng.")
        return
    db_execute("UPDATE promo_codes SET used=1 WHERE code=?", (code,))
    amount = row["amount"]; wager = int(row["wager_required"])
    ensure_user(update.effective_user.id, update.effective_user.username or "", update.effective_user.first_name or "")
    add_balance(update.effective_user.id, amount)
    db_execute("INSERT INTO promo_redemptions(code, user_id, amount, wager_required, wager_progress, last_counted_round, active, redeemed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
               (code, update.effective_user.id, amount, wager, 0, "", 1, now_iso()))
    await update.message.reply_text(f"Bạn nhận {int(amount):,}₫ từ code {code}. Phải cược {wager} vòng để hợp lệ.")

# -----------------------
# Startup / Shutdown
# -----------------------
async def on_startup(app: Application):
    logger.info("Bot starting up...")
    init_db()
    for aid in ADMIN_IDS:
        try:
            await app.bot.send_message(chat_id=aid, text="✅ Bot đã khởi động và sẵn sàng.")
        except Exception as e:
            logger.warning(f"Không gửi được tin nhắn startup cho admin {aid}: {e}")
    # start rounds loop
    loop = asyncio.get_running_loop()
    loop.create_task(rounds_loop(app))

async def on_shutdown(app: Application):
    logger.info("Bot shutting down...")
    for aid in ADMIN_IDS:
        try:
            await app.bot.send_message(chat_id=aid, text="⚠️ Bot đang tắt (shutdown).")
        except Exception as e:
            logger.warning(f"Không gửi được tin nhắn shutdown cho admin {aid}: {e}")

# -----------------------
# Main
# -----------------------
def main():
    if not BOT_TOKEN or BOT_TOKEN == "PUT_YOUR_BOT_TOKEN_HERE":
        print("❌ ERROR: BOT_TOKEN not set. Please set BOT_TOKEN env variable.")
        return

    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # private / user handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_handler))
    app.add_handler(CallbackQueryHandler(callback_query_handler, pattern=r"^game_.*"))

    # group and bets
    app.add_handler(CommandHandler("batdau", batdau_handler))
    app.add_handler(CallbackQueryHandler(approve_callback_handler, pattern=r"^(approve|deny)\|"))

    # bets: intercept text messages starting with /N /L /C /Le /S in groups
    app.add_handler(MessageHandler(filters.Regex(r"^/([NnLlCcSs]|Le|le).+"), bet_message_handler))
    # also allow without slash variant (rare)
    app.add_handler(MessageHandler(filters.Regex(r"^([NnLlCcSs]|Le|le).+"), bet_message_handler))

    # admin
    app.add_handler(CommandHandler("addmoney", addmoney_handler))
    app.add_handler(CommandHandler("top10", top10_handler))
    app.add_handler(CommandHandler("balances", balances_handler))
    app.add_handler(CommandHandler("code", admin_create_code_handler))
    app.add_handler(CommandHandler("nhancode", redeem_code_handler))

    # admin force outcome commands (must be executed inside the group)
    app.add_handler(CommandHandler("Nho", admin_force_outcome_handler))
    app.add_handler(CommandHandler("Lon", admin_force_outcome_handler))
    app.add_handler(CommandHandler("Chan", admin_force_outcome_handler))
    app.add_handler(CommandHandler("Le", admin_force_outcome_handler))

    # withdraw
    app.add_handler(CommandHandler("ruttien", ruttien_handler))

    # lifecycle
    app.post_init = on_startup
    app.post_shutdown = on_shutdown

    try:
        logger.info("🚀 Bot starting... run_polling()")
        app.run_polling(poll_interval=1.0, timeout=20)
    except Exception as e:
        logger.exception(f"Fatal error in main(): {e}")
        for aid in ADMIN_IDS:
            try:
                app.bot.send_message(chat_id=aid, text=f"❌ Bot crashed: {e}")
            except Exception:
                pass

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Fatal error in main()")
