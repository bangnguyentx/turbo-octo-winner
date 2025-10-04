# ===== PHẦN 1/4 =====
# bot.py — Quick Lottery Telegram Bot (Full v2025)

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
from datetime import datetime, timedelta
from typing import List, Tuple, Optional, Dict, Any

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ChatPermissions
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, Application
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
# Config
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "7760459637,6942793864").split(",") if x.strip()]
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", "60"))
MIN_BET = int(os.getenv("MIN_BET", "1000"))
START_BONUS = int(os.getenv("START_BONUS", "80000"))
START_BONUS_REQUIRED_ROUNDS = int(os.getenv("START_BONUS_REQUIRED_ROUNDS", "8"))
WIN_MULTIPLIER = float(os.getenv("WIN_MULTIPLIER", "1.97"))
HOUSE_RATE = float(os.getenv("HOUSE_RATE", "0.03"))
DB_FILE = os.getenv("DB_FILE", "tx_bot_data.db")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "15"))
ROOM_LINK = "https://t.me/+fuJI5Vc_MO0wZjQ1"

NUMBER_MULTIPLIERS = {1: 9.2, 2: 90, 3: 900, 4: 9000, 5: 80000, 6: 100000}

ICON_SMALL = "⚪"
ICON_BIG = "⚫"
ICON_EVEN = "🟠"
ICON_ODD = "🔵"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# DB Helpers
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
        forced_outcome TEXT DEFAULT NULL,
        last_round INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS bets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        round_id TEXT,
        user_id INTEGER,
        bet_type TEXT,
        bet_value TEXT,
        amount REAL,
        timestamp TEXT
    );

    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        round_index INTEGER,
        round_id TEXT,
        result_size TEXT,
        result_parity TEXT,
        digits TEXT,
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
    # ===== PHẦN 2/4 =====
# UI / Private Menu

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🎰 Quick Lottery")],
        [KeyboardButton("💰 Nạp tiền"), KeyboardButton("🏧 Rút tiền")],
        [KeyboardButton("💳 Nạp thẻ"), KeyboardButton("💼 Số dư")]
    ],
    resize_keyboard=True
)

# -----------------------
# Start handler
# -----------------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    greeted = False

    if u and u.get("start_bonus_given", 0) == 0:
        add_balance(user.id, START_BONUS)
        db_execute(
            "UPDATE users SET total_deposited=COALESCE(total_deposited,0)+?, start_bonus_given=1, start_bonus_progress=0 WHERE user_id=?",
            (START_BONUS, user.id)
        )
        greeted = True

    text = f"""🎉 *Chào mừng {user.first_name or 'bạn'} đến với Quick Lottery!* 🎉

📝 *Hướng dẫn cơ bản*:
- Mỗi vòng quay gồm 6 chữ số (0–9), tung ra tuần tự mỗi {ROUND_SECONDS}s.
- Bạn có thể cược:
  • `/N<tiền>` = Nhỏ (0–5)  
  • `/L<tiền>` = Lớn (6–9)  
  • `/C<tiền>` = Chẵn  
  • `/Le<tiền>` = Lẻ  
  • `/S<dãy> <tiền>` = Cược số (1–6 chữ số, ví dụ /S91 1000)
- Tỷ lệ thưởng:
  • Nhỏ / Lớn / Chẵn / Lẻ: x1.97  
  • Số 1 chữ số: x9.2  
  • Số 2 chữ số: x90  
  • Số 3 chữ số: x900  
  • Số 4 chữ số: x9000  
  • Số 5 chữ số: x80.000  
  • Số 6 chữ số: x100.000

🏆 *Thưởng khởi đầu*:
- Bạn nhận ngay {START_BONUS:,}₫ để trải nghiệm miễn phí 🎁
- Để rút thưởng, bạn cần cược tối thiểu {START_BONUS_REQUIRED_ROUNDS} vòng.

📌 *Quan trọng*:
- Menu chỉ hiển thị khi bạn chat riêng với bot.
- Tham gia nhóm chơi chính tại: 👉 [ROOM CHÍNH]({ROOM_LINK})

Chúc bạn may mắn 🍀 và thắng lớn!"""

    await update.message.reply_text(text, reply_markup=MAIN_MENU, parse_mode="Markdown")

# -----------------------
# Menu handler (private chat)
# -----------------------
async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip().lower()
    user_id = update.effective_user.id

    if txt == "🎰 quick lottery".lower():
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("👉 Bắt đầu chơi", callback_data="game_quick")]])
        msg = (
            "🎲 *Quick Lottery — Hướng dẫn chơi*\n\n"
            "1️⃣ Trong nhóm, sử dụng các lệnh cược:\n"
            "• /N<tiền> = Nhỏ (0–5)\n"
            "• /L<tiền> = Lớn (6–9)\n"
            "• /C<tiền> = Chẵn\n"
            "• /Le<tiền> = Lẻ\n"
            "• /S<dãy> <tiền> = Cược số (ví dụ /S91 1000)\n\n"
            "💰 Tỷ lệ: Nhỏ/Lớn/Chẵn/Lẻ x1.97 | Số: theo độ dài.\n"
            f"📌 Tham gia nhóm chơi tại: 👉 [Room chính]({ROOM_LINK})"
        )
        await update.message.reply_text(msg, reply_markup=kb, parse_mode="Markdown")

    elif txt in ("💰 nạp tiền", "nạp tiền", "nap tien", "nạp"):
        await update.message.reply_text("💰 Vui lòng liên hệ admin để nạp tiền: @HOANGDUNGG789")

    elif txt in ("🏧 rút tiền", "rut tien", "rút tiền", "ruttien"):
        await update.message.reply_text(
            "🏧 *Hướng dẫn rút tiền*\n\n"
            "Dùng lệnh:\n`/ruttien <Ngân hàng> <Số TK> <Số tiền>`\n\n"
            "⚠️ Rút tối thiểu 100.000₫ và tối đa 1.000.000₫ mỗi ngày.\n"
            "Admin sẽ xác nhận thủ công.",
            parse_mode="Markdown"
        )

    elif txt in ("💼 số dư", "so du", "sodu", "số dư"):
        u = get_user(user_id)
        bal = int(u["balance"]) if u else 0
        await update.message.reply_text(f"💼 Số dư hiện tại: {bal:,}₫")

    elif txt in ("💳 nạp thẻ", "nap the", "nạp thẻ"):
        await update.message.reply_text(
            "💳 *Hướng dẫn nạp thẻ*\n\n"
            "Gửi lệnh:\n`/napthe <Mã thẻ> <Seri> <Số tiền> <Loại thẻ>`\n\n"
            "📌 Ví dụ: `/napthe 123456789 987654321 100000 Viettel`",
            parse_mode="Markdown"
        )

    else:
        await update.message.reply_text("❗ Vui lòng chọn chức năng từ menu.")

# -----------------------
# Nạp thẻ handler
# -----------------------
async def napthe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    user = update.effective_user

    if len(args) < 4:
        await update.message.reply_text("❌ Sai cú pháp.\nDùng: /napthe <Mã thẻ> <Seri> <Số tiền> <Loại thẻ>")
        return

    ma, seri, sotien, loai = args[0], args[1], args[2], " ".join(args[3:])
    try:
        sotien_int = int(sotien)
    except Valu
    eError:
        await update.message.reply_text("❌ Số tiền không hợp lệ.")
        return

    # Gửi cho admin xử lý
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=aid,
                text=f"📥 *Yêu cầu nạp thẻ mới*\n\n👤 ID: `{user.id}`\n👤 Tên: {user.first_name}\n💳 Mã: `{ma}`\n🔢 Seri: `{seri}`\n💰 Số tiền: {sotien_int:,}₫\n🏷 Loại: {loai}",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Không gửi được cho admin {aid}: {e}")

    await update.message.reply_text("✅ Thông tin nạp thẻ đã được gửi. Vui lòng đợi admin xác nhận.")

# -----------------------
# Rút tiền handler
# -----------------------
async def ruttien_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    user = update.effective_user

    if len(args) < 3:
        await update.message.reply_text("❌ Sai cú pháp.\nDùng: /ruttien <Ngân hàng> <Số TK> <Số tiền>")
        return

    bank, stk, amount_str = args[0], args[1], args[2]
    try:
        amount = int(amount_str)
    except ValueError:
        await update.message.reply_text("❌ Số tiền không hợp lệ.")
        return

    if amount < 100000:
        await update.message.reply_text("⚠️ Số tiền rút tối thiểu là 100.000₫.")
        return
    if amount > 1000000:
        await update.message.reply_text("⚠️ Số tiền rút tối đa là 1.000.000₫ mỗi ngày.")
        return

    # Gửi cho admin xác nhận
    for aid in ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Đã chuyển", callback_data=f"rut_duyet:{user.id}:{amount}"),
                 InlineKeyboardButton("❌ Từ chối", callback_data=f"rut_tuchoi:{user.id}:{amount}")]
            ])
            await context.bot.send_message(
                chat_id=aid,
                text=f"🏧 *Yêu cầu rút tiền*\n👤 ID: `{user.id}`\n👤 Tên: {user.first_name}\n🏦 Ngân hàng: {bank}\n💳 STK: {stk}\n💰 Số tiền: {amount:,}₫",
                parse_mode="Markdown",
                reply_markup=kb
            )
        except Exception as e:
            logger.error(f"Không gửi được yêu cầu rút cho admin {aid}: {e}")

    await update.message.reply_text("✅ Yêu cầu rút đã gửi. Vui lòng chờ admin duyệt.")
    # ===== PHẦN 3/4 =====
# -----------------------
# Lottery logic (6 digits)
# -----------------------
def roll_one_digit() -> int:
    return random.randint(0, 9)

def roll_six_digits() -> List[int]:
    return [roll_one_digit() for _ in range(6)]

def classify_by_last_digit(digits: List[int]) -> Tuple[str, str]:
    last = digits[-1]
    size = "small" if 0 <= last <= 5 else "big"
    parity = "even" if last % 2 == 0 else "odd"
    return size, parity

def icons_for_result(size: str, parity: str) -> str:
    icons = []
    icons.append(ICON_SMALL if size == "small" else ICON_BIG)
    icons.append(ICON_EVEN if parity == "even" else ICON_ODD)
    return " ".join(icons)

# -----------------------
# History formatting
# -----------------------
def format_history_block(chat_id: int, limit: int = MAX_HISTORY) -> str:
    rows = db_query(
        "SELECT round_index, digits, result_size, result_parity FROM history WHERE chat_id=? ORDER BY id DESC LIMIT ?",
        (chat_id, limit)
    )
    if not rows:
        return "Chưa có lịch sử."
    lines = []
    for r in reversed(rows):
        idx = r["round_index"]
        digits = r["digits"] or ""
        icons = icons_for_result(r["result_size"], r["result_parity"])
        lines.append(f"{idx}: {digits} — {icons}")
    return "\n".join(lines)

# -----------------------
# Group lock/unlock
# -----------------------
async def lock_group_chat(bot, chat_id: int):
    try:
        perms = ChatPermissions(can_send_messages=False)
        await bot.set_chat_permissions(chat_id=chat_id, permissions=perms)
    except Exception as e:
        logger.warning(f"Không khóa được chat nhóm {chat_id}: {e}")

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
    except Exception as e:
        logger.warning(f"Không mở được chat nhóm {chat_id}: {e}")

# -----------------------
# Betting handler (group-only)
# -----------------------
async def bet_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return

    chat = update.effective_chat
    user = update.effective_user
    text = msg.text.strip()

    # Only allow in groups
    if chat.type not in ("group", "supergroup"):
        return

    # Check group approved & running
    g = db_query("SELECT approved, running FROM groups WHERE chat_id=?", (chat.id,))
    if not g or g[0]["approved"] != 1 or g[0]["running"] != 1:
        return

    if text.startswith("/"):
        text = text[1:]
    parts = text.split()
    cmd = parts[0].lower()

    bet_type = None
    bet_value = None
    amount = None

    try:
        if cmd[0] in ("n", "l") and cmd[1:].isdigit():
            amount = int(cmd[1:])
            bet_type = "size"
            bet_value = "small" if cmd[0] == "n" else "big"
        elif cmd.startswith("c") and cmd[1:].isdigit():
            amount = int(cmd[1:])
            bet_type = "parity"
            bet_value = "even"
        elif cmd.startswith("le"):
            rest = cmd[2:]
            if rest.isdigit():
                amount = int(rest)
                bet_type = "parity"
                bet_value = "odd"
            elif len(parts) >= 2 and parts[1].isdigit():
                amount = int(parts[1])
                bet_type = "parity"
                bet_value = "odd"
        elif cmd.startswith("s"):
            after = cmd[1:]
            if after.isdigit() and len(parts) >= 2 and parts[1].isdigit():
                bet_value = after
                amount = int(parts[1])
                bet_type = "number"
            else:
                rest = after
                for l in range(1, 7):
                    if len(rest) > l:
                        bd = rest[:l]
                        am = rest[l:]
                        if bd.isdigit() and am.isdigit() and int(am) >= MIN_BET:
                            bet_value = bd
                            amount = int(am)
                            bet_type = "number"
                            break
    except Exception:
        await msg.reply_text("❌ Sai cú pháp cược.")
        return

    if not bet_type or not bet_value or not amount:
        return
    if amount < MIN_BET:
        await msg.reply_text(f"⚠️ Cược tối thiểu {MIN_BET:,}₫")
        return

    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    if not u or (u["balance"] or 0.0) < amount:
        await msg.reply_text("❌ Số dư không đủ.")
        return

    new_bal = (u["balance"] or 0.0) - amount
    new_total = (u["total_bet_volume"] or 0.0) + amount
    db_execute("UPDATE users SET balance=?, total_bet_volume=? WHERE user_id=?", (new_bal, new_total, user.id))

    now_ts = int(datetime.utcnow().timestamp())
    round_epoch = now_ts // ROUND_SECONDS
    round_id = f"{chat.id}_{round_epoch}"
    db_execute(
        "INSERT INTO bets(chat_id, round_id, user_id, bet_type, bet_value, amount, timestamp) VALUES (?,?,?,?,?,?,?)",
        (chat.id, round_id, user.id, bet_type, bet_value, amount, now_iso())
    )

    label = {"size": ("Nhỏ" if bet_value=="small" else "Lớn"), "parity": ("Chẵn" if bet_value=="even" else "Lẻ"), "number": f"Số {bet_value}"}[bet_type]
    await msg.reply_text(f"✅ Đặt {label} {amount:,}₫ thành công.")

# -----------------------
# Countdown helper
# -----------------------
async def send_countdown(bot, chat_id: int, seconds: int):
    try:
        if seconds == 30:
            await bot.send_message(chat_id=chat_id, text="⏰ Còn 30s — Nhanh tay cược!")
        elif seconds == 10:
            await bot.send_message(chat_id=chat_id, text="⚠️ Còn 10s! Sắp khóa cược.")
        elif seconds == 5:
            await bot.send_message(chat_id=chat_id, text="🔒 Còn 5s — Chat bị khóa để chốt cược.")
            await lock_group_chat(bot, chat_id)
    except Exception:
        pass

# ===== PHẦN 4/4 =====
# -----------------------
# Trả thưởng cho người thắng
# -----------------------
def calc_payout(bet_type: str, bet_value: str, amount: float, digits: List[int], size: str, parity: str) -> float:
    if bet_type == "size":
        if bet_value == size:
            return amount * WIN_MULTIPLIER
    elif bet_type == "parity":
        if bet_value == parity:
            return amount * WIN_MULTIPLIER
    elif bet_type == "number":
        drawn_str = "".join(str(d) for d in digits)
        if drawn_str.endswith(bet_value):  # số trúng phải nằm cuối
            mult = NUMBER_MULTIPLIERS.get(len(bet_value), 0)
            return amount * mult
    return 0.0

# -----------------------
# Chạy vòng quay cho 1 nhóm
# -----------------------
async def run_round_for_group(app: Application, chat_id: int, round_epoch: int):
    bot = app.bot
    round_id = f"{chat_id}_{round_epoch}"

    # Ép kết quả (nếu có)
    forced = db_query("SELECT forced_outcome FROM groups WHERE chat_id=?", (chat_id,))
    forced_val = forced[0]["forced_outcome"] if forced else None

    # Tung số
    digits = roll_six_digits()
    size, parity = classify_by_last_digit(digits)

    # Nếu admin ép kết quả thì điều chỉnh số cuối cùng cho phù hợp
    if forced_val:
        last_digit = digits[-1]
        if forced_val == "small" and last_digit > 5:
            digits[-1] = random.randint(0, 5)
        elif forced_val == "big" and last_digit <= 5:
            digits[-1] = random.randint(6, 9)
        elif forced_val == "even" and last_digit % 2 != 0:
            digits[-1] = random.choice([0,2,4,6,8])
        elif forced_val == "odd" and last_digit % 2 == 0:
            digits[-1] = random.choice([1,3,5,7,9])
        size, parity = classify_by_last_digit(digits)
        db_execute("UPDATE groups SET forced_outcome=NULL WHERE chat_id=?", (chat_id,))

    # Gửi từng số ra
    try:
        await bot.send_message(chat_id=chat_id, text="🎲 Bắt đầu quay 6 chữ số!")
        for d in digits:
            await asyncio.sleep(1)
            await bot.send_message(chat_id=chat_id, text=f"{d}")
    except Exception as e:
        logger.error(f"Lỗi gửi số: {e}")

    # Lưu lịch sử
    round_idx = int(datetime.utcnow().timestamp())
    digits_str = "".join(str(x) for x in digits)
    db_execute(
        "INSERT INTO history(chat_id, round_index, round_id, result_size, result_parity, digits, timestamp) VALUES (?,?,?,?,?,?,?)",
        (chat_id, round_idx, round_id, size, parity, digits_str, now_iso())
    )

    # Lấy toàn bộ cược
    bets = db_query("SELECT user_id, bet_type, bet_value, amount FROM bets WHERE round_id=? AND chat_id=?", (round_id, chat_id))
    winners = []
    for b in bets:
        payout = calc_payout(b["bet_type"], b["bet_value"], b["amount"], digits, size, parity)
        if payout > 0:
            winners.append((b["user_id"], payout))

    # Trả thưởng
    for uid, payout in winners:
        add_balance(uid, payout)
        try:
            await bot.send_message(chat_id=uid, text=f"🎉 Bạn đã thắng {payout:,.0f}₫! Vòng {digits_str}")
        except Exception:
            pass

    # Gửi kết quả vòng
    icons = icons_for_result(size, parity)
    hist_block = format_history_block(chat_id)
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=f"✅ Kết quả: {digits_str} — {icons}\n\n📜 *Lịch sử gần đây*:\n{hist_block}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Gửi kết quả lỗi: {e}")

    # Mở lại chat cho vòng tiếp theo
    await asyncio.sleep(1)
    await unlock_group_chat(bot, chat_id)

# -----------------------
# Engine loop
# -----------------------
async def lottery_engine(app: Application):
    logger.info("🎯 Lottery engine started")
    while True:
        try:
            chats = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
            if chats:
                now_ts = int(datetime.utcnow().timestamp())
                round_epoch = now_ts // ROUND_SECONDS
                for c in chats:
                    chat_id = c["chat_id"]
                    # Gửi countdowns
                    sec_to_next = ROUND_SECONDS - (now_ts % ROUND_SECONDS)
                    if sec_to_next in (30, 10, 5):
                        await send_countdown(app.bot, chat_id, sec_to_next)
                    # Đúng thời điểm quay
                    if now_ts % ROUND_SECONDS == 0:
                        asyncio.create_task(run_round_for_group(app, chat_id, round_epoch))
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Engine loop error: {e}")
            await asyncio.sleep(5)

# -----------------------
# Admin ép kết quả
# -----------------------
async def admin_force_outcome_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    txt = update.message.text.strip().lower()
    chat = update.effective_chat
    val = None
    if txt.startswith("/nho"): val = "small"
    elif txt.startswith("/lon"): val = "big"
    elif txt.startswith("/chan"): val = "even"
    elif txt.startswith("/le"): val = "odd"
    if val:
        db_execute("UPDATE groups SET forced_outcome=? WHERE chat_id=?", (val, chat.id))
        await update.message.reply_text(f"✅ Đã ép kết quả vòng sau: {val}")

# -----------------------
# Main
# -----------------------
def main():
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_handler))
    app.add_handler(CommandHandler("napthe", napthe_handler))
    app.add_handler(CommandHandler("ruttien", ruttien_handler))
    app.add_handler(MessageHandler(filters.COMMAND, bet_message_handler))
    app.add_handler(CommandHandler(["nho","lon","chan","le"], admin_force_outcome_handler))

    asyncio.create_task(lottery_engine(app))
    app.run_polling()

if __name__ == "__main__":
    main()
