
# bot.py — Quick Lottery (full)
# See conversation for feature requirements.
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
import secrets
from datetime import datetime, date, timezone
from typing import List, Tuple, Optional, Dict, Any

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, ChatPermissions
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler,
    MessageHandler, CallbackQueryHandler, filters, Application
)

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

BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_YOUR_BOT_TOKEN_HERE")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "7760459637,6942793864").split(",") if x.strip()]
ROUND_SECONDS = int(os.getenv("ROUND_SECONDS", "60"))
MIN_BET = int(os.getenv("MIN_BET", "1000"))
START_BONUS = int(os.getenv("START_BONUS", "80000"))
WIN_MULTIPLIER = float(os.getenv("WIN_MULTIPLIER", "1.97"))
HOUSE_RATE = float(os.getenv("HOUSE_RATE", "0.03"))
DB_FILE = os.getenv("DB_FILE", "tx_bot_data.db")
MAX_HISTORY = int(os.getenv("MAX_HISTORY", "15"))
NUMBER_MULTIPLIERS = {1: 9.2, 2: 90, 3: 900, 4: 9000, 5: 80000, 6: 100000}
ICON_SMALL = "⚪"
ICON_BIG = "⚫"
ICON_EVEN = "🟠"
ICON_ODD = "🔵"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("quick_lottery_bot")

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
        start_bonus_progress INTEGER DEFAULT 0,
        last_withdraw_date TEXT DEFAULT NULL
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

    CREATE TABLE IF NOT EXISTS deposits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        code TEXT,
        seri TEXT,
        amount REAL,
        card_type TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT
    );

    CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        bank TEXT,
        acc_number TEXT,
        amount REAL,
        status TEXT DEFAULT 'pending',
        created_at TEXT
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

def now_iso():
    return datetime.utcnow().isoformat()

def ensure_user(user_id: int, username: str = "", first_name: str = ""):
    rows = db_query("SELECT user_id FROM users WHERE user_id=?", (user_id,))
    if not rows:
        db_execute(
            "INSERT INTO users(user_id, username, first_name, balance, total_deposited, total_bet_volume, current_streak, best_streak, created_at, start_bonus_given, start_bonus_progress) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, username or "", first_name or "", 0.0, 0.0, 0.0, 0, 0, now_iso(), 0, 0)
        )
        logger.info(f"New user created: {user_id}")

def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    rows = db_query("SELECT * FROM users WHERE user_id=?", (user_id,))
    return dict(rows[0]) if rows else None

def add_balance(user_id: int, amount: float):
    ensure_user(user_id)
    u = get_user(user_id)
    new_bal = (u["balance"] or 0.0) + amount
    db_execute("UPDATE users SET balance=? WHERE user_id=?", (new_bal, user_id))
    logger.info(f"Added balance for {user_id}: +{amount} -> {new_bal}")
    return new_bal

def set_balance(user_id: int, amount: float):
    ensure_user(user_id)
    db_execute("UPDATE users SET balance=? WHERE user_id=?", (amount, user_id))
    logger.info(f"Set balance for {user_id} -> {amount}")

def add_to_pot(amount: float):
    try:
        db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (amount,))
    except Exception:
        logger.exception("Failed to add to pot")

def get_pot_amount() -> float:
    rows = db_query("SELECT amount FROM pot WHERE id=1")
    return rows[0]["amount"] if rows else 0.0

def reset_pot():
    db_execute("UPDATE pot SET amount=? WHERE id=1", (0.0,))

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
    return f"{ICON_SMALL if size=='small' else ICON_BIG} {ICON_EVEN if parity=='even' else ICON_ODD}"

MAIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🎰 Quick Lottery")],
        [KeyboardButton("💰 Nạp tiền"), KeyboardButton("🏧 Rút tiền")],
        [KeyboardButton("💵 Số dư"), KeyboardButton("🧾 Nạp thẻ")]
    ],
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

    text = (
        f"🎉 Kính chào {user.first_name or 'Quý khách'}!\n\n"
        "Chào mừng bạn đến với Quick Lottery — trò chơi quay 6 chữ số nhanh và đơn giản.\n\n"
        "Luật chơi tóm tắt:\n"
        "- Mỗi vòng diễn ra sau 60 giây.\n"
        "- Bot sẽ tung 6 chữ số (0-9) từng số một.\n"
        "- Kết quả lớn/nhỏ/chẵn/lẻ được xác định bởi chữ số cuối cùng.\n\n"
        "Các lệnh cược (chỉ trong nhóm):\n"
        "/N<tiền> — Cược Nhỏ (0-5)\n"
        "/L<tiền> — Cược Lớn (6-9)\n"
        "/C<tiền> — Cược Chẵn\n"
        "/Le<tiền> — Cược Lẻ\n"
        "/S<dãy> <tiền> — Cược theo dãy số (1–6 chữ số)\n\n"
        f"Nếu bạn là user mới, bạn đã nhận {START_BONUS:,}₫ thưởng khởi đầu.\n"
        "Sử dụng menu để truy cập nạp/rút và xem số dư.\n\n"
        "Chúc bạn chơi vui vẻ và may mắn! 🍀"
    )
    await update.message.reply_text(text, reply_markup=MAIN_MENU)

async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    txt = update.message.text.strip().lower()
    uid = update.effective_user.id

    if "quick lottery" in txt or "🎰" in txt:
        guide = (
            "🎰 Quick Lottery — Hướng dẫn chi tiết\n\n"
            "Cách cược:\n"
            "- /N<tiền> — Nhỏ (0–5)\n"
            "- /L<tiền> — Lớn (6–9)\n"
            "- /C<tiền> — Chẵn\n"
            "- /Le<tiền> — Lẻ\n"
            "- /S<dãy> <tiền> — Đặt cược theo dãy số (ví dụ /S91 1000)\n\n"
            "Tỷ lệ:\n"
            "- Nhỏ/Lớn/Chẵn/Lẻ ×1.97\n"
            "- Số: theo số chữ số (1→x9.2, 2→x90, 3→x900, 4→x9000, 5→x80000, 6→x100000)\n\n"
            "Tham gia phòng chơi chính: https://t.me/+fuJI5Vc_MO0wZjQ1\n"
        )
        await update.message.reply_text(guide)
    elif "nạp tiền" in txt or "💰" in txt:
        await update.message.reply_text("Dùng /napthe để gửi mã thẻ hoặc liên hệ admin để nạp.")
    elif "rút tiền" in txt or "🏧" in txt:
        await update.message.reply_text(f"Rút tiền: /ruttien <Ngân hàng> <Số TK> <Số tiền>\nTối thiểu {100000:,}₫ — Tối đa {1000000:,}₫ / ngày — 1 lần/ngày")
    elif "số dư" in txt or "💵" in txt:
        u = get_user(uid)
        bal = int(u["balance"]) if u else 0
        await update.message.reply_text(f"Số dư hiện tại: {bal:,}₫")
    elif "nạp thẻ" in txt or "🧾" in txt:
        await update.message.reply_text("Gửi theo cú pháp: /napthe <mã thẻ> <seri> <số tiền> <loại thẻ>")
    else:
        await update.message.reply_text("Chọn chức năng từ menu hoặc gõ lệnh tương ứng.")

async def napthe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 4:
        await update.message.reply_text("❌ Cú pháp: /napthe <mã thẻ> <seri> <số tiền> <loại thẻ>")
        return
    code, seri, amount_s, card_type = args[0], args[1], args[2], " ".join(args[3:])
    try:
        amount = int(amount_s)
    except:
        await update.message.reply_text("❌ Số tiền không hợp lệ.")
        return
    uid = update.effective_user.id
    ensure_user(uid, update.effective_user.username or "", update.effective_user.first_name or "")
    db_execute("INSERT INTO deposits(user_id, code, seri, amount, card_type, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?)", (uid, code, seri, amount, card_type, now_iso()))
    text_admin = f"📥 Yêu cầu NẠP THẺ\nUser: {uid}\nMã: {code}\nSeri: {seri}\nSố tiền: {amount:,}₫\nLoại: {card_type}"
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=text_admin)
        except Exception:
            logger.exception("Failed to notify admin for deposit")
    await update.message.reply_text("✅ Yêu cầu nạp thẻ đã gửi admin. Vui lòng chờ xử lý.")

async def ruttien_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    uid = update.effective_user.id
    u = get_user(uid)
    if not u:
        await update.message.reply_text("Bạn chưa có tài khoản.")
        return
    if len(args) < 3:
        await update.message.reply_text(f"Rút tiền: /ruttien <Ngân hàng> <Số TK> <Số tiền>\nTối thiểu {100000:,}₫ — Tối đa {1000000:,}₫ / ngày — 1 lần/ngày")
        return
    bank, acc_number, amt_s = args[0], args[1], args[2]
    try:
        amount = int(amt_s)
    except:
        await update.message.reply_text("Số tiền không hợp lệ.")
        return
    if amount < 100000:
        await update.message.reply_text(f"Tối thiểu rút 100000₫.")
        return
    if amount > 1000000:
        await update.message.reply_text(f"Tối đa rút 1000000₫ mỗi ngày.")
        return
    today = date.today().isoformat()
    if u.get("last_withdraw_date") == today:
        await update.message.reply_text("Bạn đã rút hôm nay. Mỗi ngày chỉ được rút 1 lần.")
        return
    if (u["balance"] or 0) < amount:
        await update.message.reply_text("Số dư không đủ.")
        return
    set_balance(uid, (u["balance"] or 0) - amount)
    db_execute("INSERT INTO withdrawals(user_id, bank, acc_number, amount, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)", (uid, bank, acc_number, amount, now_iso()))
    db_execute("UPDATE users SET last_withdraw_date=? WHERE user_id=?", (today, uid))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ Đã chuyển", callback_data=f"wd_ok|{uid}|{amount}"), InlineKeyboardButton("❌ Từ chối", callback_data=f"wd_no|{uid}|{amount}")]])
    text_admin = f"📤 Yêu cầu RÚT TIỀN\nUser: {uid}\nNgân hàng: {bank}\nTK: {acc_number}\nSố tiền: {amount:,}₫"
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=text_admin, reply_markup=kb)
        except Exception:
            logger.exception("Failed to notify admin for withdrawal")
    await update.message.reply_text("✅ Yêu cầu rút đã gửi admin.")

async def withdraw_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data.split("|")
    if len(data) != 3:
        return
    action, uid_s, amt_s = data
    uid, amt = int(uid_s), int(amt_s)
    if q.from_user.id not in ADMIN_IDS:
        await q.edit_message_text("❌ Không có quyền.")
        return
    if action == "wd_ok":
        db_execute("UPDATE withdrawals SET status='done' WHERE user_id=? AND amount=? AND status='pending'", (uid, amt))
        await q.edit_message_text(f"✅ Đã chuyển {amt:,}₫ cho user {uid}")
        try:
            await context.bot.send_message(chat_id=uid, text=f"✅ Rút {amt:,}₫ đã được chuyển.")
        except Exception:
            pass
    else:
        db_execute("UPDATE withdrawals SET status='rejected' WHERE user_id=? AND amount=? AND status='pending'", (uid, amt))
        db_execute("UPDATE users SET balance=COALESCE(balance,0)+? WHERE user_id=?", (amt, uid))
        await q.edit_message_text(f"❌ Đã từ chối rút {amt:,}₫ cho user {uid}")
        try:
            await context.bot.send_message(chat_id=uid, text=f"❌ Yêu cầu rút {amt:,}₫ của bạn đã bị từ chối. Tiền đã được hoàn lại.")
        except Exception:
            pass

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

# Betting handler (group)
async def bet_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text:
        return
    chat = update.effective_chat
    user = update.effective_user
    text = msg.text.strip()

    if chat.type not in ("group", "supergroup"):
        await msg.reply_text("Lệnh cược chỉ dùng trong nhóm.")
        return

    g = db_query("SELECT approved, running FROM groups WHERE chat_id=?", (chat.id,))
    if not g or g[0]["approved"] != 1 or g[0]["running"] != 1:
        await msg.reply_text("Nhóm này chưa được admin duyệt hoặc chưa bật /batdau.")
        return

    txt = text
    if txt.startswith("/"):
        txt = txt[1:]
    parts = txt.split()
    cmd = parts[0]

    prefix = cmd[0].lower()
    bet_type = None; bet_value = None; amount = None
    try:
        if prefix in ("n","l") and cmd[1:].isdigit():
            amount = int(cmd[1:]); bet_type = "size"; bet_value = "small" if prefix=="n" else "big"
        elif cmd.lower().startswith("c") and cmd[1:].isdigit():
            amount = int(cmd[1:]); bet_type = "parity"; bet_value = "even"
        elif cmd.lower().startswith("le") and (cmd[2:].isdigit() or (len(parts)>=2 and parts[1].isdigit())):
            if cmd[2:].isdigit(): amount=int(cmd[2:])
            else: amount=int(parts[1])
            bet_type="parity"; bet_value="odd"
        elif cmd.lower().startswith("s"):
            after = cmd[1:]
            if after.isdigit() and len(parts)>=2 and parts[1].isdigit():
                bet_type="number"; bet_value=after; amount=int(parts[1])
            else:
                rest = cmd[1:]; found=False
                for l in range(1,7):
                    if len(rest)>l:
                        bd=rest[:l]; am=rest[l:]
                        if bd.isdigit() and am.isdigit() and int(am)>=MIN_BET:
                            bet_type="number"; bet_value=bd; amount=int(am); found=True; break
                if not found and len(parts)>=3 and parts[0].lower()=="s" and parts[1].isdigit() and parts[2].isdigit():
                    bet_type="number"; bet_value=parts[1]; amount=int(parts[2])
        else:
            return
    except Exception:
        await msg.reply_text("❌ Cú pháp cược không hợp lệ.")
        return

    if not bet_type or not bet_value or not isinstance(amount, int):
        await msg.reply_text("❌ Cú pháp cược không hợp lệ.")
        return
    if amount < MIN_BET:
        await msg.reply_text(f"⚠️ Đặt cược tối thiểu {MIN_BET:,}₫")
        return

    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    if not u or (u["balance"] or 0) < amount:
        await msg.reply_text("❌ Số dư không đủ.")
        return

    set_balance(user.id, (u["balance"] or 0) - amount)
    db_execute("UPDATE users SET total_bet_volume = COALESCE(total_bet_volume,0)+? WHERE user_id=?", (amount, user.id))

    now_ts = int(datetime.utcnow().timestamp())
    round_epoch = (now_ts // ROUND_SECONDS) + 1
    round_id = f"{chat.id}_{round_epoch}"
    db_execute("INSERT INTO bets(chat_id, round_id, user_id, bet_type, bet_value, amount, timestamp) VALUES (?,?,?,?,?,?,?)", (chat.id, round_id, user.id, bet_type, bet_value, amount, now_iso()))

    try:
        await update_promo_wager_progress(context, user.id, round_id)
    except Exception:
        logger.exception("promo progress fail")

    readable = "Nhỏ" if bet_type=="size" and bet_value=="small" else "Lớn" if bet_type=="size" and bet_value=="big" else "Chẵn" if bet_type=="parity" and bet_value=="even" else "Lẻ" if bet_type=="parity" and bet_value=="odd" else f"Số {bet_value}"
    await msg.reply_text(f"✅ Đã đặt {readable} — {amount:,}₫ cho phiên sắp tới.")

# Admin force outcome handler
async def admin_force_outcome_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    text = update.message.text.strip().lower()
    chat = update.effective_chat
    if chat.type not in ("group","supergroup"):
        await update.message.reply_text("Lệnh này chỉ dùng trong nhóm.")
        return
    if text.startswith("/nho"): val="small"
    elif text.startswith("/lon"): val="big"
    elif text.startswith("/chan"): val="even"
    elif text.startswith("/le"): val="odd"
    else:
        await update.message.reply_text("Lệnh không hợp lệ. Dùng /Nho, /Lon, /Chan, /Le")
        return
    db_execute("UPDATE groups SET forced_outcome=? WHERE chat_id=?", (val, chat.id))
    await update.message.reply_text(f"✅ Đã ép kết quả cho phiên tiếp theo: {val}")

def format_history_block(chat_id: int, limit: int = MAX_HISTORY) -> str:
    rows = db_query("SELECT round_index, digits, result_size, result_parity FROM history WHERE chat_id=? ORDER BY id DESC LIMIT ?", (chat_id, limit))
    if not rows: return ""
    lines=[]
    for r in reversed(rows):
        idx=r["round_index"]; digits=r["digits"] or ""; size=r["result_size"] or ""; parity=r["result_parity"] or ""
        icons = icons_for_result(size, parity)
        lines.append(f"{idx}: {digits} — {icons}")
    return "\n".join(lines)

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
        perms = ChatPermissions(can_send_messages=True, can_send_media_messages=True, can_send_polls=True, can_send_other_messages=True, can_add_web_page_previews=True)
        await bot.set_chat_permissions(chat_id=chat_id, permissions=perms)
    except Exception:
        pass

async def run_round_for_group(app: Application, chat_id: int, round_epoch: int):
    try:
        round_index = int(round_epoch)
        round_id = f"{chat_id}_{round_epoch}"
        bets_rows = db_query("SELECT id, user_id, bet_type, bet_value, amount FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        bets = [dict(r) for r in bets_rows] if bets_rows else []
        g = db_query("SELECT forced_outcome FROM groups WHERE chat_id=?", (chat_id,))
        forced = g[0]["forced_outcome"] if g else None
        try:
            await app.bot.send_message(chat_id=chat_id, text=f"🎲 Phiên {round_index} — Đang quay...")
        except Exception:
            pass
        digits=[]
        if forced in ("small","big","even","odd"):
            attempts=0
            while attempts<500:
                cand=roll_six_digits()
                size, parity = classify_by_last_digit(cand)
                ok = (forced=="small" and size=="small") or (forced=="big" and size=="big") or (forced=="even" and parity=="even") or (forced=="odd" and parity=="odd")
                if ok:
                    digits=cand; break
                attempts+=1
            if not digits: digits=roll_six_digits()
            try:
                db_execute("UPDATE groups SET forced_outcome=NULL WHERE chat_id=?", (chat_id,))
            except Exception:
                logger.exception("Failed clearing forced")
        else:
            digits=roll_six_digits()
        for d in digits:
            try: await app.bot.send_message(chat_id=chat_id, text=str(d))
            except: pass
            await asyncio.sleep(1)
        size, parity = classify_by_last_digit(digits)
        digits_str = "".join(str(d) for d in digits)
        try:
            db_execute("INSERT INTO history(chat_id, round_index, round_id, result_size, result_parity, digits, timestamp) VALUES (?,?,?,?,?,?,?)", (chat_id, round_index, round_id, size, parity, digits_str, now_iso()))
        except Exception:
            logger.exception("Failed insert history")
        winners=[]; losers_total=0.0
        for b in bets:
            uid=int(b["user_id"]); btype=b["bet_type"]; bval=b["bet_value"]; amt=float(b["amount"] or 0.0)
            win=False; payout=0.0
            if btype=="size" and ((bval=="small" and size=="small") or (bval=="big" and size=="big")):
                win=True; payout=amt*WIN_MULTIPLIER
            elif btype=="parity" and ((bval=="even" and parity=="even") or (bval=="odd" and parity=="odd")):
                win=True; payout=amt*WIN_MULTIPLIER
            elif btype=="number" and isinstance(bval,str) and bval!="" and bval in digits_str:
                ln=max(1,min(6,len(bval))); mult=NUMBER_MULTIPLIERS.get(ln,0); payout=amt*mult; win=True
            if win:
                winners.append((b["id"], uid, payout, amt))
            else:
                losers_total+=amt
        if losers_total>0:
            try: db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (losers_total,))
            except: logger.exception("Failed add losers")
        winners_paid=[]
        for bet_id, uid, payout, bet_amt in winners:
            try:
                house_share = bet_amt * HOUSE_RATE
                if house_share>0:
                    try: db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (house_share,))
                    except: logger.exception("Failed adding house share")
                ensure_user(uid)
                success=False
                for attempt in range(3):
                    try:
                        u=get_user(uid)
                        if not u: raise Exception("Missing user")
                        new_bal=(u["balance"] or 0.0)+payout
                        db_execute("UPDATE users SET balance=?, current_streak=COALESCE(current_streak,0)+1, best_streak=CASE WHEN COALESCE(current_streak,0)+1>COALESCE(best_streak,0) THEN COALESCE(current_streak,0)+1 ELSE COALESCE(best_streak,0) END WHERE user_id=?", (new_bal, uid))
                        success=True
                        break
                    except Exception:
                        logger.exception(f"Payout attempt failed for {uid}")
                        await asyncio.sleep(0.05)
                if not success:
                    logger.error(f"Failed to pay {uid} after retries")
                else:
                    winners_paid.append((uid, payout, bet_amt))
            except Exception:
                logger.exception("Critical payout error")
        try:
            for b in bets:
                uid=int(b["user_id"])
                if not any(w[0]==uid for w in winners_paid):
                    db_execute("UPDATE users SET current_streak=0 WHERE user_id=?", (uid,))
        except Exception:
            logger.exception("Failed reset streaks")
        try:
            db_execute("DELETE FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        except Exception:
            logger.exception("Failed delete bets")
        display = "Nhỏ" if size=="small" else "Lớn"
        icons = icons_for_result(size, parity)
        history_block = format_history_block(chat_id, MAX_HISTORY)
        msg = f"▶️ Phiên {round_index} — Kết quả: {display} {icons}\nSố: {' '.join(str(d) for d in digits)} — (chuỗi: {digits_str})"
        if history_block: msg += f"\n\nLịch sử:\n{history_block}"
        if winners_paid: msg += f"\n\n🏆 Người thắng: {len(winners_paid)}"
        else: msg += "\n\nKhông có người thắng."
        try: await app.bot.send_message(chat_id=chat_id, text=msg)
        except: logger.exception("Failed send result")
        try:
            logger.info(f"Round {round_index} | Group {chat_id} | digits={digits_str} | size={size} | parity={parity}")
            logger.info(f"Total bets: {len(bets)} | Winners: {len(winners_paid)}")
            for uid, payout, bet_amt in winners_paid:
                logger.info(f" - Paid user {uid}: bet={int(bet_amt):,} -> payout={int(payout):,}")
        except Exception:
            pass
        # ✅ Mở lại chat sau khi công bố kết quả
        try:
            await asyncio.sleep(1)
            await unlock_group_chat(app.bot, chat_id)
        except Exception:
            logger.exception("Failed to unlock group chat after result")
    except Exception as e:
        logger.exception(f"Exception in run_round_for_group: {e}")
        for aid in ADMIN_IDS:
            try:
                await app.bot.send_message(chat_id=aid, text=f"ERROR run_round_for_group for {chat_id}: {e}\n{traceback.format_exc()}")
            except Exception:
                pass

async def rounds_loop(app: Application):
    logger.info("Rounds loop started")
    await asyncio.sleep(2)
    while True:
        try:
            now_ts = int(datetime.utcnow().timestamp())
            next_epoch_ts = ((now_ts // ROUND_SECONDS) + 1) * ROUND_SECONDS
            rem = next_epoch_ts - now_ts
            if rem > 30:
                await asyncio.sleep(rem - 30)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 30))
                await asyncio.sleep(20)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 10))
                await asyncio.sleep(5)
                rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                await asyncio.sleep(5)
            else:
                if rem > 10:
                    await asyncio.sleep(rem - 10)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 10))
                    await asyncio.sleep(5)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                    await asyncio.sleep(5)
                elif rem > 5:
                    await asyncio.sleep(rem - 5)
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
                    await asyncio.sleep(5)
                else:
                    rows = db_query("SELECT chat_id FROM groups WHERE approved=1 AND running=1")
                    for r in rows: asyncio.create_task(send_countdown(app.bot, r["chat_id"], 5))
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
            await asyncio.sleep(1)

async def batdau_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ("group","supergroup"):
        await update.message.reply_text("/batdau chỉ dùng trong nhóm.")
        return
    title = chat.title or ""
    rows = db_query("SELECT chat_id FROM groups WHERE chat_id=?", (chat.id,))
    if not rows:
        db_execute("INSERT INTO groups(chat_id, title, approved, running, bet_mode, forced_outcome, last_round) VALUES (?, ?, 0, 0, 'random', NULL, ?)", (chat.id, title, 0))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Duyệt", callback_data=f"approve|{chat.id}"), InlineKeyboardButton("Từ chối", callback_data=f"deny|{chat.id}")]])
    text = f"Yêu cầu bật bot cho nhóm:\n{title}\nchat_id: {chat.id}\nNgười yêu cầu: {update.effective_user.id}"
    for aid in ADMIN_IDS:
        try: await context.bot.send_message(chat_id=aid, text=text, reply_markup=kb)
        except: logger.exception("Cannot notify admin")
    await update.message.reply_text("Đã gửi yêu cầu tới admin để duyệt.")

async def approve_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = (q.data or "").split("|")
    if len(parts) != 2:
        await q.edit_message_text("Dữ liệu không hợp lệ.")
        return
    action, chat_id_s = parts
    try: chat_id=int(chat_id_s)
    except: await q.edit_message_text("chat_id không hợp lệ."); return
    if q.from_user.id not in ADMIN_IDS:
        await q.edit_message_text("Chỉ admin mới thao tác.")
        return
    if action=="approve":
        db_execute("UPDATE groups SET approved=1, running=1 WHERE chat_id=?", (chat_id,))
        await q.edit_message_text(f"Đã duyệt và bật chạy cho nhóm {chat_id}.")
        try: await context.bot.send_message(chat_id=chat_id, text=f"Bot đã được admin duyệt — bắt đầu chạy phiên mỗi {ROUND_SECONDS}s.")
        except: pass
    else:
        db_execute("UPDATE groups SET approved=0, running=0 WHERE chat_id=?", (chat_id,))
        await q.edit_message_text(f"Đã từ chối cho nhóm {chat_id}.")

async def addmoney_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin.")
        return
    args=context.args
    if len(args)<2:
        await update.message.reply_text("Cú pháp: /addmoney <user_id> <amount>")
        return
    try:
        uid=int(args[0]); amt=float(args[1])
    except:
        await update.message.reply_text("Tham số không hợp lệ."); return
    ensure_user(uid)
    new_bal=add_balance(uid, amt)
    db_execute("UPDATE users SET total_deposited=COALESCE(total_deposited,0)+? WHERE user_id=?", (amt, uid))
    await update.message.reply_text(f"Đã cộng {int(amt):,}₫ cho user {uid}. Số dư hiện: {int(new_bal):,}₫")
    try: await context.bot.send_message(chat_id=uid, text=f"Bạn vừa được admin cộng {int(amt):,}₫. Số dư: {int(new_bal):,}₫")
    except: pass

async def top10_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin."); return
    rows=db_query("SELECT user_id, total_deposited FROM users ORDER BY total_deposited DESC LIMIT 10")
    text="Top 10 nạp nhiều nhất:\n"
    for i,r in enumerate(rows, start=1): text+=f"{i}. {r['user_id']} — {int(r['total_deposited'] or 0):,}₫\n"
    await update.message.reply_text(text)

async def balances_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("Chỉ admin."); return
    rows=db_query("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT 50")
    text="Top balances:\n"
    for r in rows: text+=f"- {r['user_id']}: {int(r['balance'] or 0):,}₫\n"
    await update.message.reply_text(text)

async def on_startup(app: Application):
    logger.info("Bot starting up...")
    init_db()
    for aid in ADMIN_IDS:
        try: await app.bot.send_message(chat_id=aid, text="✅ Bot đã khởi động và sẵn sàng.")
        except: logger.exception("Cannot notify admin on startup")
    loop=asyncio.get_running_loop()
    loop.create_task(rounds_loop(app))

async def on_shutdown(app: Application):
    logger.info("Bot shutting down...")

def main():
    if not BOT_TOKEN or BOT_TOKEN.startswith("PUT_"):
        print("ERROR: BOT_TOKEN not configured.")
        sys.exit(1)
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_handler))
    app.add_handler(CallbackQueryHandler(approve_callback_handler, pattern=r"^(approve|deny)\|"))
    app.add_handler(CommandHandler("napthe", napthe_handler))
    app.add_handler(CommandHandler("ruttien", ruttien_handler))
    app.add_handler(CallbackQueryHandler(withdraw_admin_callback))
    app.add_handler(CommandHandler("batdau", batdau_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^/([NnLlCcSs]|Le|le).+"), bet_message_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^([NnLlCcSs]|Le|le).+"), bet_message_handler))
    app.add_handler(CommandHandler("addmoney", addmoney_handler))
    app.add_handler(CommandHandler("top10", top10_handler))
    app.add_handler(CommandHandler("balances", balances_handler))
    app.add_handler(CommandHandler(["nho","lon","chan","le"], admin_force_outcome_handler))
    app.post_init = on_startup
    app.post_shutdown = on_shutdown
    try:
        logger.info("Bot is starting... run_polling()")
        app.run_polling(poll_interval=1.0, timeout=20)
    except Exception:
        logger.exception("Fatal error in main()")

if __name__ == "__main__":
    main()
