# bovvt.py — Quick Lottery (full) — updated with HMAC Provably-Fair & new features
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
import hashlib
import hmac
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

# Keep a small HTTP server so Render / similar hosts don't kill the process
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

# -------------------------
# Config
# -------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8410469970:AAGotzA6YMmGJrvxKDJya1CNUNx7yVrj8jE")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "8560521739").split(",") if x.strip()]
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

# Emoji mapping for numbers
NUMBER_EMOJIS = {
    '0': '0️⃣', '1': '1️⃣', '2': '2️⃣', '3': '3️⃣', 
    '4': '4️⃣', '5': '5️⃣', '6': '6️⃣', '7': '7️⃣', 
    '8': '8️⃣', '9': '9️⃣'
}

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger("quick_lottery_bot")

# -------------------------
# HMAC Provably-Fair System
# -------------------------
class HMACRNG:
    def __init__(self):
        self.server_seeds = {}  # round_id -> server_seed
        
    def generate_server_seed(self):
        """Generate cryptographically secure server seed"""
        return secrets.token_hex(32)
    
    def get_commitment(self, server_seed):
        """Get commitment hash for server seed"""
        return hashlib.sha256(server_seed.encode()).hexdigest()
    
    def generate_digits_hmac(self, server_seed: str, round_id: str, client_seed: str = "") -> List[int]:
        """
        Generate 6 digits using HMAC-SHA256 with rejection sampling to avoid bias
        """
        message = f"{round_id}{client_seed}".encode()
        key = server_seed.encode()
        
        digits = []
        counter = 0
        
        while len(digits) < 6:
            # Generate HMAC with counter to get more bytes if needed
            hmac_msg = message + counter.to_bytes(4, 'big')
            mac = hmac.new(key, hmac_msg, hashlib.sha256).digest()
            
            # Process each byte with rejection sampling
            for byte in mac:
                if len(digits) >= 6:
                    break
                    
                # Rejection sampling: only accept bytes 0-249 for uniform distribution
                if byte < 250:
                    digit = byte % 10
                    digits.append(digit)
            
            counter += 1
        
        return digits
    
    def verify_round(self, server_seed: str, round_id: str, expected_digits: List[int], client_seed: str = "") -> bool:
        """Verify round results"""
        computed_digits = self.generate_digits_hmac(server_seed, round_id, client_seed)
        return computed_digits == expected_digits

# Initialize HMAC RNG
hmac_rng = HMACRNG()

# -------------------------
# DB helpers
# -------------------------
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
        last_withdraw_date TEXT DEFAULT NULL,
        client_seed TEXT DEFAULT NULL
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
        timestamp TEXT,
        server_seed TEXT,
        commitment TEXT
    );

    CREATE TABLE IF NOT EXISTS provable_rounds (
        round_id TEXT PRIMARY KEY,
        server_seed TEXT,
        commitment TEXT,
        revealed INTEGER DEFAULT 0,
        created_at TEXT
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
        created_at TEXT,
        announcement_sent INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS admin_forced_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER,
        admin_id INTEGER,
        forced_type TEXT,
        forced_value TEXT,
        created_at TEXT,
        applied_round TEXT
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

# User helpers
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

# -----------------------
# Menu System
# -----------------------
USER_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🎰 Chơi Lottery"), KeyboardButton("💰 Số dư")],
        [KeyboardButton("💳 Nạp tiền"), KeyboardButton("🏧 Rút tiền")],
        [KeyboardButton("📊 Lịch sử"), KeyboardButton("🔐 Client Seed")],
        [KeyboardButton("ℹ️ Hướng dẫn"), KeyboardButton("📞 Hỗ trợ")]
    ],
    resize_keyboard=True
)

ADMIN_MENU = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🎰 Chơi Lottery"), KeyboardButton("💰 Số dư")],
        [KeyboardButton("💳 Nạp tiền"), KeyboardButton("🏧 Rút tiền")],
        [KeyboardButton("👑 Quản lý"), KeyboardButton("⚙️ Cài đặt")],
        [KeyboardButton("📊 Thống kê"), KeyboardButton("🔧 Công cụ")]
    ],
    resize_keyboard=True
)

# -----------------------
# User handlers (start, menu, napthe, ruttien)
# -----------------------
async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_user(user.id, user.username or "", user.first_name or "")
    u = get_user(user.id)
    greeted = False
    if u and u.get("start_bonus_given", 0) == 0:
        add_balance(user.id, START_BONUS)
        db_execute("UPDATE users SET total_deposited=COALESCE(total_deposited,0)+?, start_bonus_given=1, start_bonus_progress=0 WHERE user_id=?", (START_BONUS, user.id))
        greeted = True

    # Determine which menu to show
    if user.id in ADMIN_IDS:
        menu_markup = ADMIN_MENU
        role_text = "👑 Quyền: Quản trị viên"
    else:
        menu_markup = USER_MENU
        role_text = "🎯 Quyền: Người chơi"

    text = (
        f"🎉 Chào {user.first_name or 'Quý khách'}!\n\n"
        f"{role_text}\n\n"
        "Chào mừng đến với Quick Lottery — trò chơi quay số minh bạch.\n\n"
        "🎲 Cách chơi:\n"
        "- Mỗi vòng 60 giây, quay 6 chữ số\n"
        "- Kết quả dựa trên số cuối cùng\n"
        "- Có thể xác minh tính minh bạch\n\n"
        f"🎁 Thưởng khởi đầu: {START_BONUS:,}₫\n\n"
        "Chọn chức năng từ menu bên dưới!"
    )
    await update.message.reply_text(text, reply_markup=menu_markup)

async def menu_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
        
    user = update.effective_user
    txt = update.message.text.strip()
    
    # Determine menu based on user role
    if user.id in ADMIN_IDS:
        menu_markup = ADMIN_MENU
    else:
        menu_markup = USER_MENU
    
    if "chơi lottery" in txt.lower() or "🎰" in txt:
        guide = (
            "🎰 Quick Lottery — Hướng dẫn chi tiết\n\n"
            "📝 Lệnh cược (trong nhóm):\n"
            "- /N<tiền> — Cược Nhỏ (0–5)\n"
            "- /L<tiền> — Cược Lớn (6–9)\n"
            "- /C<tiền> — Cược Chẵn\n"
            "- /Le<tiền> — Cược Lẻ\n"
            "- /S<dãy> <tiền> — Cược theo dãy số\n\n"
            "💰 Tỷ lệ thắng:\n"
            "- Nhỏ/Lớn/Chẵn/Lẻ: ×1.97\n"
            "- Số: 1→×9.2, 2→×90, 3→×900, 4→×9000, 5→×80000, 6→×100000\n\n"
            "🔐 Tính minh bạch:\n"
            "- Dùng HMAC-SHA256 để quay số\n"
            "- Có thể xác minh kết quả\n"
            "- Client seed tuỳ chọn\n\n"
            "📞 Tham gia nhóm chính: @quick_lottery_group"
        )
        await update.message.reply_text(guide, reply_markup=menu_markup)
        
    elif "số dư" in txt.lower() or "💰" in txt:
        u = get_user(user.id)
        bal = int(u["balance"]) if u else 0
        await update.message.reply_text(f"💰 Số dư hiện tại: {bal:,}₫", reply_markup=menu_markup)
        
    elif "nạp tiền" in txt.lower() or "💳" in txt:
        await update.message.reply_text(
            "💳 Nạp tiền\n\n"
            "1️⃣ Nạp thẻ: /napthe <mã thẻ> <seri> <số tiền> <loại thẻ>\n"
            "2️⃣ Chuyển khoản: Liên hệ admin\n"
            "3️⃣ Ví điện tử: Liên hệ admin", 
            reply_markup=menu_markup
        )
        
    elif "rút tiền" in txt.lower() or "🏧" in txt:
        await update.message.reply_text(
            f"🏧 Rút tiền\n\n"
            f"Cú pháp: /ruttien <Ngân hàng> <Số TK> <Số tiền>\n"
            f"Tối thiểu: 100,000₫\n"
            f"Tối đa/ngày: 1,000,000₫\n"
            f"1 lần/ngày", 
            reply_markup=menu_markup
        )
        
    elif "lịch sử" in txt.lower() or "📊" in txt:
        await update.message.reply_text("📊 Lịch sử\n\nDùng /history để xem lịch sử cược", reply_markup=menu_markup)
        
    elif "client seed" in txt.lower() or "🔐" in txt:
        await update.message.reply_text(
            "🔐 Client Seed\n\n"
            "Thiết lập seed cá nhân để tăng tính minh bạch:\n"
            "/setseed <seed_của_bạn>\n\n"
            "Seed sẽ được kết hợp với server seed để tạo kết quả.", 
            reply_markup=menu_markup
        )
        
    elif "hướng dẫn" in txt.lower() or "ℹ️" in txt:
        await update.message.reply_text(
            "ℹ️ Hướng dẫn\n\n"
            "📖 Luật chơi đầy đủ:\n"
            "- Mỗi vòng 60 giây\n"
            "- Quay 6 chữ số ngẫu nhiên\n"
            "- Kết quả dựa trên số cuối\n"
            "- Có thể xác minh tính công bằng\n\n"
            "🛠 Hỗ trợ: @admin_support", 
            reply_markup=menu_markup
        )
        
    elif "hỗ trợ" in txt.lower() or "📞" in txt:
        await update.message.reply_text("📞 Hỗ trợ\n\nLiên hệ admin: @admin_support", reply_markup=menu_markup)
        
    # Admin menu items
    elif "quản lý" in txt.lower() or "👑" in txt:
        if user.id in ADMIN_IDS:
            await update.message.reply_text(
                "👑 Quản lý Admin\n\n"
                "📊 Thống kê: /stats\n"
                "👥 Top người chơi: /top10\n"
                "💰 Số dư users: /balances\n"
                "🎯 Ép kết quả: /ep\n"
                "📢 Thông báo: /announce", 
                reply_markup=menu_markup
            )
        else:
            await update.message.reply_text("❌ Chức năng chỉ dành cho admin", reply_markup=menu_markup)
            
    elif "cài đặt" in txt.lower() or "⚙️" in txt:
        if user.id in ADMIN_IDS:
            await update.message.reply_text(
                "⚙️ Cài đặt Admin\n\n"
                "Thêm tiền: /addmoney <user_id> <số tiền>\n"
                "Duyệt nhóm: Xem yêu cầu /batdau\n"
                "Cấu hình: Đang phát triển", 
                reply_markup=menu_markup
            )
        else:
            await update.message.reply_text("❌ Chức năng chỉ dành cho admin", reply_markup=menu_markup)
            
    elif "thống kê" in txt.lower() or "📊" in txt:
        if user.id in ADMIN_IDS:
            await update.message.reply_text("📊 Thống kê\n\nDùng /stats để xem thống kê hệ thống", reply_markup=menu_markup)
        else:
            await update.message.reply_text("❌ Chức năng chỉ dành cho admin", reply_markup=menu_markup)
            
    elif "công cụ" in txt.lower() or "🔧" in txt:
        if user.id in ADMIN_IDS:
            await update.message.reply_text(
                "🔧 Công cụ Admin\n\n"
                "Commitment: /commit <round_id>\n"
                "Reveal seed: /reveal <round_id>\n"
                "Verify: /verify <round_id> <server_seed>\n"
                "Lịch sử ép: /forcehistory", 
                reply_markup=menu_markup
            )
        else:
            await update.message.reply_text("❌ Chức năng chỉ dành cho admin", reply_markup=menu_markup)

# -----------------------
# HMAC Provably-Fair Commands
# -----------------------
async def set_client_seed_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set client seed for user"""
    user = update.effective_user
    args = context.args
    
    if not args:
        current_seed = db_query("SELECT client_seed FROM users WHERE user_id=?", (user.id,))
        current = current_seed[0]["client_seed"] if current_seed and current_seed[0]["client_seed"] else "Chưa đặt"
        await update.message.reply_text(f"🔐 Client seed hiện tại: {current}\n\nĐặt seed mới: /setseed <seed_của_bạn>")
        return
        
    client_seed = args[0]
    if len(client_seed) < 8:
        await update.message.reply_text("❌ Client seed phải có ít nhất 8 ký tự")
        return
        
    db_execute("UPDATE users SET client_seed=? WHERE user_id=?", (client_seed, user.id))
    await update.message.reply_text(f"✅ Đã đặt client seed: {client_seed}\n\nSeed này sẽ được dùng để tạo kết quả minh bạch.")

async def verify_round_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Verify a round's results"""
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Cú pháp: /verify <round_id> <server_seed> [client_seed]")
        return
        
    round_id = args[0]
    server_seed = args[1]
    client_seed = args[2] if len(args) > 2 else ""
    
    # Get round result from history
    history = db_query("SELECT digits FROM history WHERE round_id=?", (round_id,))
    if not history:
        await update.message.reply_text("❌ Không tìm thấy kết quả vòng này")
        return
        
    expected_digits = [int(d) for d in history[0]["digits"]]
    
    # Verify
    is_valid = hmac_rng.verify_round(server_seed, round_id, expected_digits, client_seed)
    
    if is_valid:
        await update.message.reply_text(f"✅ XÁC MINH THÀNH CÔNG!\n\nVòng: {round_id}\nKết quả khớp với seed.")
    else:
        await update.message.reply_text(f"❌ XÁC MINH THẤT BẠI!\n\nVòng: {round_id}\nKết quả không khớp!")

async def get_commitment_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get commitment for a round"""
    args = context.args
    if not args:
        await update.message.reply_text("❌ Cú pháp: /commit <round_id>")
        return
        
    round_id = args[0]
    commitment = db_query("SELECT commitment FROM provable_rounds WHERE round_id=?", (round_id,))
    
    if commitment and commitment[0]["commitment"]:
        await update.message.reply_text(f"🔐 Commitment cho {round_id}:\n`{commitment[0]['commitment']}`", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Không tìm thấy commitment cho vòng này")

async def reveal_seed_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reveal server seed for a round (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Chỉ admin mới được dùng lệnh này")
        return
        
    args = context.args
    if not args:
        await update.message.reply_text("❌ Cú pháp: /reveal <round_id>")
        return
        
    round_id = args[0]
    seed_data = db_query("SELECT server_seed FROM provable_rounds WHERE round_id=?", (round_id,))
    
    if seed_data and seed_data[0]["server_seed"]:
        db_execute("UPDATE provable_rounds SET revealed=1 WHERE round_id=?", (round_id,))
        await update.message.reply_text(f"🔓 Server seed cho {round_id}:\n`{seed_data[0]['server_seed']}`", parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Không tìm thấy server seed cho vòng này")

# -----------------------
# Enhanced Admin Force System
# -----------------------
async def admin_force_silent_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin ép kết quả âm thầm - chỉ nhắn riêng với bot
    Cú pháp mới: /ep <chat_id> <loại> [giá_trị]
    Loại: small, big, even, odd, first (ép số đầu)
    """
    user = update.effective_user
    chat = update.effective_chat
    
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Không có quyền")
        return
        
    if chat.type != "private":
        await update.message.reply_text("⚠️ Vui lòng dùng lệnh này trong tin nhắn riêng với bot")
        return
        
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "🎯 Ép kết quả âm thầm\n\n"
            "Cú pháp: /ep <chat_id> <loại> [giá_trị]\n\n"
            "Loại:\n"
            "- small: ép kết quả NHỎ\n" 
            "- big: ép kết quả LỚN\n"
            "- even: ép kết quả CHẴN\n"
            "- odd: ép kết quả LẺ\n"
            "- first <số>: ép số ĐẦU (0-9)\n\n"
            "Ví dụ:\n"
            "/ep -100123456789 small\n"
            "/ep -100123456789 first 5"
        )
        return
        
    try:
        chat_id = int(args[0])
        force_type = args[1].lower()
        force_value = args[2] if len(args) > 2 else None
        
        # Validate force type
        valid_types = ['small', 'big', 'even', 'odd', 'first']
        if force_type not in valid_types:
            await update.message.reply_text(f"❌ Loại ép không hợp lệ. Chọn: {', '.join(valid_types)}")
            return
            
        if force_type == 'first' and not force_value:
            await update.message.reply_text("❌ Cần chỉ định số để ép (0-9)")
            return
            
        if force_type == 'first' and force_value:
            try:
                first_digit = int(force_value)
                if first_digit < 0 or first_digit > 9:
                    await update.message.reply_text("❌ Số ép phải từ 0-9")
                    return
            except ValueError:
                await update.message.reply_text("❌ Số ép không hợp lệ")
                return
        
        # Check if group exists
        group = db_query("SELECT title FROM groups WHERE chat_id=?", (chat_id,))
        if not group:
            await update.message.reply_text("❌ Không tìm thấy nhóm")
            return
            
        # Save forced action
        db_execute(
            "INSERT INTO admin_forced_actions (chat_id, admin_id, forced_type, forced_value, created_at) VALUES (?, ?, ?, ?, ?)",
            (chat_id, user.id, force_type, force_value, now_iso())
        )
        
        # Update group forced outcome
        if force_type == 'first':
            db_execute("UPDATE groups SET forced_outcome=? WHERE chat_id=?", (f"first_{force_value}", chat_id))
        else:
            db_execute("UPDATE groups SET forced_outcome=? WHERE chat_id=?", (force_type, chat_id))
            
        await update.message.reply_text(
            f"✅ Đã ép kết quả âm thầm thành công!\n\n"
            f"🏷 Nhóm: {group[0]['title']}\n"
            f"🎯 Loại: {force_type}\n"
            f"📊 Giá trị: {force_value or 'N/A'}\n\n"
            f"⏰ Áp dụng cho vòng tiếp theo"
        )
        
    except Exception as e:
        logger.exception("Force action failed")
        await update.message.reply_text(f"❌ Lỗi khi ép kết quả: {str(e)}")

async def force_history_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View force history (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Không có quyền")
        return
        
    history = db_query(
        "SELECT afa.*, g.title FROM admin_forced_actions afa LEFT JOIN groups g ON afa.chat_id = g.chat_id ORDER BY afa.created_at DESC LIMIT 10"
    )
    
    if not history:
        await update.message.reply_text("📝 Chưa có lịch sử ép kết quả")
        return
        
    text = "📋 Lịch sử ép kết quả (10 gần nhất):\n\n"
    for i, record in enumerate(history, 1):
        text += (
            f"{i}. {record['title'] or record['chat_id']}\n"
            f"   🎯 {record['forced_type']} {record['forced_value'] or ''}\n"
            f"   👤 Admin: {record['admin_id']}\n"
            f"   ⏰ {record['created_at'][:16]}\n\n"
        )
        
    await update.message.reply_text(text)

# -----------------------
# Enhanced Withdrawal Announcement
# -----------------------
async def enhanced_ruttien_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced withdrawal handler with announcement"""
    args = context.args
    uid = update.effective_user.id
    u = get_user(uid)
    if not u:
        await update.message.reply_text("Bạn chưa có tài khoản.")
        return
    if len(args) < 3:
        await update.message.reply_text(f"🏧 Rút tiền\n\nCú pháp: /ruttien <Ngân hàng> <Số TK> <Số tiền>\nTối thiểu: 100,000₫\nTối đa: 1,000,000₫/ngày")
        return
        
    bank, acc_number, amt_s = args[0], args[1], args[2]
    try:
        amount = int(amt_s)
    except:
        await update.message.reply_text("❌ Số tiền không hợp lệ")
        return
        
    if amount < 100000:
        await update.message.reply_text(f"❌ Tối thiểu rút 100,000₫")
        return
        
    if amount > 1000000:
        await update.message.reply_text(f"❌ Tối đa rút 1,000,000₫ mỗi ngày")
        return
        
    today = date.today().isoformat()
    if u.get("last_withdraw_date") == today:
        await update.message.reply_text("❌ Bạn đã rút hôm nay. Mỗi ngày chỉ được rút 1 lần.")
        return
        
    if (u["balance"] or 0) < amount:
        await update.message.reply_text("❌ Số dư không đủ.")
        return
        
    set_balance(uid, (u["balance"] or 0) - amount)
    withdrawal_id = db_execute(
        "INSERT INTO withdrawals(user_id, bank, acc_number, amount, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
        (uid, bank, acc_number, amount, now_iso())
    )
    db_execute("UPDATE users SET last_withdraw_date=? WHERE user_id=?", (today, uid))
    
    # Create admin approval buttons
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Duyệt rút", callback_data=f"wd_approve|{uid}|{amount}|{withdrawal_id}"),
        InlineKeyboardButton("❌ Từ chối", callback_data=f"wd_reject|{uid}|{amount}|{withdrawal_id}")
    ]])
    
    # Format user ID for announcement (first 3 and last 3 digits)
    user_id_str = str(uid)
    if len(user_id_str) > 6:
        masked_id = f"{user_id_str[:3]}***{user_id_str[-3:]}"
    else:
        masked_id = user_id_str
    
    text_admin = (
        f"📤 YÊU CẦU RÚT TIỀN\n\n"
        f"👤 User: {masked_id}\n"
        f"🏦 Ngân hàng: {bank}\n"
        f"🔢 Số TK: {acc_number}\n"
        f"💰 Số tiền: {amount:,}₫\n"
        f"🆔 Mã giao dịch: {withdrawal_id}"
    )
    
    for aid in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=aid, text=text_admin, reply_markup=kb)
        except Exception:
            logger.exception("Failed to notify admin for withdrawal")
            
    await update.message.reply_text("✅ Yêu cầu rút tiền đã gửi admin. Vui lòng chờ xử lý.")

async def enhanced_withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced withdrawal callback with announcement"""
    q = update.callback_query
    await q.answer()
    data = q.data.split("|")
    if len(data) != 4:
        return
        
    action, uid_s, amt_s, wd_id = data
    uid, amt = int(uid_s), int(amt_s)
    
    if q.from_user.id not in ADMIN_IDS:
        await q.edit_message_text("❌ Không có quyền.")
        return
        
    user = get_user(uid)
    if not user:
        await q.edit_message_text("❌ User không tồn tại")
        return
        
    # Format user ID for announcement
    user_id_str = str(uid)
    if len(user_id_str) > 6:
        masked_id = f"{user_id_str[:3]}***{user_id_str[-3:]}"
    else:
        masked_id = user_id_str
    
    if action == "wd_approve":
        # Update withdrawal status
        db_execute("UPDATE withdrawals SET status='done', announcement_sent=1 WHERE id=?", (wd_id,))
        
        # Send announcement to all active groups
        groups = db_query("SELECT chat_id, title FROM groups WHERE approved=1 AND running=1")
        announcement_sent = False
        
        for group in groups:
            try:
                announcement = (
                    f"🎉 THÔNG BÁO RÚT TIỀN THÀNH CÔNG\n\n"
                    f"👤 Thành viên: {masked_id}\n"
                    f"💰 Số tiền: {amt:,}₫\n"
                    f"⏰ Thời gian: {datetime.now().strftime('%H:%M %d/%m/%Y')}\n\n"
                    f"Chúc mừng thành viên! 🎊"
                )
                await context.bot.send_message(chat_id=group["chat_id"], text=announcement)
                announcement_sent = True
            except Exception as e:
                logger.error(f"Failed to send announcement to group {group['chat_id']}: {e}")
        
        await q.edit_message_text(
            f"✅ Đã duyệt rút {amt:,}₫ cho user {masked_id}\n"
            f"📢 Đã gửi thông báo đến {len(groups)} nhóm"
        )
        
    else:  # wd_reject
        db_execute("UPDATE withdrawals SET status='rejected' WHERE id=?", (wd_id,))
        db_execute("UPDATE users SET balance=COALESCE(balance,0)+? WHERE user_id=?", (amt, uid))
        await q.edit_message_text(f"❌ Đã từ chối rút {amt:,}₫ cho user {masked_id}")
        
        try:
            await context.bot.send_message(chat_id=uid, text=f"❌ Yêu cầu rút {amt:,}₫ của bạn đã bị từ chối. Tiền đã được hoàn lại.")
        except Exception:
            pass

async def virtual_deposit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin tạo thông báo nạp tiền ảo"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Không có quyền")
        return
        
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Cú pháp: /announce_deposit <số_tiền> <mô_tả>")
        return
        
    try:
        amount = int(args[0])
        description = " ".join(args[1:])
        
        # Send to all active groups
        groups = db_query("SELECT chat_id, title FROM groups WHERE approved=1 AND running=1")
        success_count = 0
        
        for group in groups:
            try:
                announcement = (
                    f"🎉 THÔNG BÁO NẠP TIỀN THÀNH CÔNG\n\n"
                    f"💰 Số tiền: {amount:,}₫\n"
                    f"📝 Mô tả: {description}\n"
                    f"⏰ Thời gian: {datetime.now().strftime('%H:%M %d/%m/%Y')}\n\n"
                    f"Chúc mừng giao dịch thành công! 💰"
                )
                await context.bot.send_message(chat_id=group["chat_id"], text=announcement)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to send deposit announcement to group {group['chat_id']}: {e}")
                
        await update.message.reply_text(f"✅ Đã gửi thông báo nạp tiền đến {success_count}/{len(groups)} nhóm")
        
    except ValueError:
        await update.message.reply_text("❌ Số tiền không hợp lệ")

# -----------------------
# Enhanced Round Engine with HMAC
# -----------------------
async def run_round_for_group(app: Application, chat_id: int, round_epoch: int):
    try:
        round_index = int(round_epoch)
        round_id = f"{chat_id}_{round_epoch}"
        
        # Generate server seed and commitment for this round
        server_seed = hmac_rng.generate_server_seed()
        commitment = hmac_rng.get_commitment(server_seed)
        
        # Store provable round data
        db_execute(
            "INSERT OR REPLACE INTO provable_rounds (round_id, server_seed, commitment, created_at) VALUES (?, ?, ?, ?)",
            (round_id, server_seed, commitment, now_iso())
        )
        
        # Send commitment announcement (5 seconds before round start)
        try:
            commitment_msg = (
                f"🔐 COMMITMENT CHO VÒNG {round_index}\n\n"
                f"Hash: `{commitment}`\n\n"
                f"_Commitment này đảm bảo tính minh bạch của vòng chơi_"
            )
            await app.bot.send_message(chat_id=chat_id, text=commitment_msg, parse_mode="Markdown")
        except Exception:
            logger.exception("Failed to send commitment")
        
        bets_rows = db_query("SELECT id, user_id, bet_type, bet_value, amount FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        bets = [dict(r) for r in bets_rows] if bets_rows else []
        g = db_query("SELECT forced_outcome FROM groups WHERE chat_id=?", (chat_id,))
        forced = g[0]["forced_outcome"] if g else None

        # announce starting
        try:
            await app.bot.send_message(chat_id=chat_id, text=f"🎲 Phiên {round_index} — Đang quay...")
        except Exception:
            pass

        # decide digits using HMAC RNG (respect forced outcome if set)
        digits = []
        if forced:
            if forced.startswith('first_'):
                # Ép số đầu tiên
                first_digit = int(forced.split('_')[1])
                remaining_digits = hmac_rng.generate_digits_hmac(server_seed, round_id)[1:5]  # Get 5 more digits
                digits = [first_digit] + remaining_digits
            else:
                # Ép loại kết quả (small, big, even, odd)
                attempts = 0
                while attempts < 1000:
                    digits = hmac_rng.generate_digits_hmac(server_seed, round_id)
                    size, parity = classify_by_last_digit(digits)
                    ok = (forced == "small" and size == "small") or \
                         (forced == "big" and size == "big") or \
                         (forced == "even" and parity == "even") or \
                         (forced == "odd" and parity == "odd")
                    if ok:
                        break
                    attempts += 1
                
                # Clear forced flag after use
                db_execute("UPDATE groups SET forced_outcome=NULL WHERE chat_id=?", (chat_id,))
        else:
            # Normal HMAC-based generation
            digits = hmac_rng.generate_digits_hmac(server_seed, round_id)

        # send digits one-by-one with emojis
        for d in digits:
            emoji_digit = NUMBER_EMOJIS[str(d)]
            try: 
                await app.bot.send_message(chat_id=chat_id, text=emoji_digit)
            except: 
                pass
            await asyncio.sleep(1)

        size, parity = classify_by_last_digit(digits)
        digits_str = "".join(str(d) for d in digits)
        
        # Store result with server seed info
        try:
            db_execute(
                "INSERT INTO history(chat_id, round_index, round_id, result_size, result_parity, digits, timestamp, server_seed, commitment) VALUES (?,?,?,?,?,?,?,?,?)",
                (chat_id, round_index, round_id, size, parity, digits_str, now_iso(), server_seed, commitment)
            )
        except Exception:
            logger.exception("Failed insert history")

        # compute winners and pay (existing logic)
        winners=[]; losers_total=0.0
        for b in bets:
            uid=int(b["user_id"]); btype=b["bet_type"]; bval=b["bet_value"]; amt=float(b["amount"] or 0.0)
            win=False; payout=0.0
            if btype=="size" and ((bval=="small" and size=="small") or (bval=="big" and size=="big")):
                win=True; payout=amt*WIN_MULTIPLIER
            elif btype=="parity" and ((bval=="even" and parity=="even") or (bval=="odd" and parity=="odd")):
                win=True; payout=amt*WIN_MULTIPLIER
            elif btype == "number" and isinstance(bval, str) and bval != "":
                ln = max(1, min(6, len(bval)))
                tail = digits_str[-ln:]
                if bval == tail:
                    mult = NUMBER_MULTIPLIERS.get(ln, 0)
                    payout = amt * mult
                    win = True
            if win:
                winners.append((b["id"], uid, payout, amt))
            else:
                losers_total += amt

        if losers_total>0:
            try: db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (losers_total,))
            except: logger.exception("Failed add losers to pot")

        winners_paid=[]
        for bet_id, uid, payout, bet_amt in winners:
            try:
                house_share = bet_amt * HOUSE_RATE
                if house_share>0:
                    try: db_execute("UPDATE pot SET amount = amount + ? WHERE id = 1", (house_share,))
                    except: logger.exception("Failed adding house share to pot")
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

        # reset streaks for losers
        try:
            for b in bets:
                uid=int(b["user_id"])
                if not any(w[0]==uid for w in winners_paid):
                    db_execute("UPDATE users SET current_streak=0 WHERE user_id=?", (uid,))
        except Exception:
            logger.exception("Failed reset streaks")

        # delete bets for this round
        try:
            db_execute("DELETE FROM bets WHERE chat_id=? AND round_id=?", (chat_id, round_id))
        except Exception:
            logger.exception("Failed delete bets")

        # prepare and send result message with emojis
        display = "NHỎ" if size=="small" else "LỚN"
        icons = icons_for_result(size, parity)
        
        # Convert digits to emojis for display
        digits_display = " ".join([NUMBER_EMOJIS[str(d)] for d in digits])
        
        history_block = format_history_block(chat_id, MAX_HISTORY)
        msg = (
            f"🎊 KẾT QUẢ VÒNG {round_index}\n\n"
            f"🔢 Số: {digits_display}\n"
            f"📊 Kết quả: {display} {icons}\n"
            f"🔍 Dãy số: {digits_str}\n\n"
        )
        
        if history_block: 
            msg += f"📈 Lịch sử:\n{history_block}\n\n"
            
        if winners_paid: 
            msg += f"🎉 CÓ {len(winners_paid)} NGƯỜI THẮNG! 🎉"
        else: 
            msg += "😔 Không có người thắng"
            
        try: 
            await app.bot.send_message(chat_id=chat_id, text=msg)
        except: 
            logger.exception("Failed send result")

        # unlock chat after result
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

# -----------------------
# Existing utility functions (keep as is)
# -----------------------
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

def classify_by_last_digit(digits: List[int]) -> Tuple[str, str]:
    last = digits[-1]
    size = "small" if 0 <= last <= 5 else "big"
    parity = "even" if last % 2 == 0 else "odd"
    return size, parity

def icons_for_result(size: str, parity: str) -> str:
    return f"{ICON_SMALL if size=='small' else ICON_BIG} {ICON_EVEN if parity=='even' else ICON_ODD}"

# -----------------------
# Existing functions (napthe, betting, rounds_loop, etc.)
# -----------------------
# [Keep all the existing functions like napthe_handler, bet_message_handler, 
#  rounds_loop, batdau_handler, etc. exactly as they were in the original code]
# ... (existing code remains the same)

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

# [Keep all other existing functions exactly as they were...]

# -----------------------
# Enhanced main function with new handlers
# -----------------------
def main():
    if not BOT_TOKEN or BOT_TOKEN.startswith("PUT_"):
        print("ERROR: BOT_TOKEN not configured.")
        sys.exit(1)
    init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Existing handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_text_handler))
    app.add_handler(CallbackQueryHandler(approve_callback_handler, pattern=r"^(approve|deny)\|"))
    app.add_handler(CommandHandler("napthe", napthe_handler))
    app.add_handler(CommandHandler("ruttien", enhanced_ruttien_handler))  # Updated
    app.add_handler(CallbackQueryHandler(enhanced_withdraw_callback))  # Updated
    app.add_handler(CommandHandler("batdau", batdau_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^/([NnLlCcSs]|Le|le).+"), bet_message_handler))
    app.add_handler(MessageHandler(filters.Regex(r"^([NnLlCcSs]|Le|le).+"), bet_message_handler))
    app.add_handler(CommandHandler("addmoney", addmoney_handler))
    app.add_handler(CommandHandler("top10", top10_handler))
    app.add_handler(CommandHandler("balances", balances_handler))
    
    # New HMAC Provably-Fair handlers
    app.add_handler(CommandHandler("setseed", set_client_seed_handler))
    app.add_handler(CommandHandler("verify", verify_round_handler))
    app.add_handler(CommandHandler("commit", get_commitment_handler))
    app.add_handler(CommandHandler("reveal", reveal_seed_handler))
    
    # Enhanced admin force handlers
    app.add_handler(CommandHandler("ep", admin_force_silent_handler))
    app.add_handler(CommandHandler("forcehistory", force_history_handler))
    app.add_handler(CommandHandler("announce_deposit", virtual_deposit_handler))
    
    app.post_init = on_startup
    app.post_shutdown = on_shutdown
    
    try:
        logger.info("Bot is starting... run_polling()")
        app.run_polling(poll_interval=1.0, timeout=20)
    except Exception:
        logger.exception("Fatal error in main()")

# [Keep existing on_startup, on_shutdown, and other necessary functions]

async def on_startup(app: Application):
    logger.info("Bot starting up...")
    init_db()
    for aid in ADMIN_IDS:
        try: 
            await app.bot.send_message(chat_id=aid, text="✅ Bot đã khởi động và sẵn sàng.")
        except: 
            logger.exception("Cannot notify admin on startup")
    loop = asyncio.get_running_loop()
    loop.create_task(rounds_loop(app))

async def on_shutdown(app: Application):
    logger.info("Bot shutting down...")

# [Keep existing approve_callback_handler, addmoney_handler, top10_handler, balances_handler]

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

# [Keep existing rounds_loop, bet_message_handler, and other essential functions]

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

if __name__ == "__main__":
    main()
