"""
Telegram lookup bot — CARMDI
- Plate search: "B1000" (1 letter + number)
- Number-only: "2259" → all regions with ActualNB=2259
- Phone search (legacy): exact on normalized variants, fallback to suffix (7/6)
- DB autoload: downloads plates.db at startup if missing (900 MB OK)
- Emoji UI, duplicate chooser, logs user texts only (quiet HTTP)
"""

import os
import re
import ssl
import sqlite3
import asyncio
import logging
import urllib.request
from dataclasses import dataclass
from typing import List, Dict, Any

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.constants import ChatAction
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)



# ===== CONFIG =====
BOT_TOKEN = "8e5m8a0l5df3s2y2bw63gkodur546ywixrs5wqpvnxgmr74k2wof1wieuk2zo9td"

# Direct download link to your DB (Dropbox: change ?dl=0 -> ?dl=1)
# Example: "https://www.dropbox.com/s/<id>/plates.db?dl=1"
# Avoid Google Drive for 900MB unless you know how to bypass the virus-scan page.
DB_URL  = "https://wetransfer.com/download/local-bartender?id=eyJhcGlCYXNlIjoiL2FwaS92NCIsImRkUHJveHlCYXNlIjoiaHR0cHM6Ly9sb2NhbC1iYXJ0ZW5kZXItZGQtcHJveHkud2V0cmFuc2Zlci5uZXQvYXBpIiwidHJhbnNmZXJJZCI6IjlmYzc3ZDJkOThjZTQyMzg3NmFiNTJiMTY0ZmYwZGI4MjAyNTA4MDgyMDU1MzMiLCJzZWNyZXQiOiJkMDBkMWIiLCJpbnRlbnQiOiJlbnRpcmVfdHJhbnNmZXIiLCJzaG91bGRVc2VOZXdIYW5kbGVGbG93IjoidHJ1ZSIsInJlY2lwaWVudElkIjoiMTg1N2RlM2NkNmNiMDdjZjFhNjUyMTQzNWUyNTU0NjIyMDI1MDgwODIwNTU0MSIsImxvY2FsU3RvcmFnZUlkIjoiNTdhMzBmZGMtNTc5NS00OGI4LWI4NzEtZTI2YmFlZTM5NjFmIn0%3D"
DB_PATH = "./plates.db"

TABLE_NAME    = "CARMDI"
SERIES_COLUMN = "CodeDesc"   # region letter
NUMBER_COLUMN = "ActualNB"   # numeric part
PHONE_COLUMN  = "TelProp"

# ===== LOGGING =====
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")
# Quiet PTB/httpx internals:
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext._application").setLevel(logging.WARNING)
logging.getLogger("telegram.request").setLevel(logging.WARNING)
logging.getLogger("telegram.request._httpxrequest").setLevel(logging.WARNING)

# ===== Startup: ensure DB is present (download if missing) =====
def ensure_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    if os.path.exists(DB_PATH):
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        logger.info("DB found at %s (%.1f MB)", DB_PATH, size_mb)
        return

    if not DB_URL or DB_URL.startswith("PASTE_"):
        raise RuntimeError("DB file is missing and DB_URL is not set. Please set DB_URL to a direct download link.")

    logger.info("DB not found. Downloading from %s …", DB_URL)

    def _progress(blocks, block_size, total):
        if total <= 0:
            return
        done = blocks * block_size
        pct = min(100, int(done * 100 / total))
        mb_done = done // (1024 * 1024)
        mb_total = total // (1024 * 1024)
        print(f"\r⬇️  Downloading DB… {pct}% ({mb_done}/{mb_total} MB)", end="", flush=True)

    ctx = ssl.create_default_context()
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))
    urllib.request.install_opener(opener)
    tmp_path = DB_PATH + ".part"
    try:
        urllib.request.urlretrieve(DB_URL, tmp_path, _progress)
        print()
        os.replace(tmp_path, DB_PATH)
        size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
        logger.info("DB downloaded to %s (%.1f MB)", DB_PATH, size_mb)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        finally:
            raise

# ===== Display config =====
DISPLAY_COLUMNS = [
    "MarqueDesc", "TypeDesc", "CouleurDesc",
    "PRODDATE", "PreMiseCirc",
    "Prenom", "Nom", "Addresse", "TelProp",
    "Chassis", "Moteur",
]

LABEL_MAP: Dict[str, str] = {
    "MarqueDesc":   "Make",
    "TypeDesc":     "Model",
    "CouleurDesc":  "Color",
    "PRODDATE":     "Production Year",
    "PreMiseCirc":  "First Registration",
    "Prenom":       "First name",
    "Nom":          "Last name",
    "Addresse":     "Address",
    "TelProp":      "Phone",
    "Chassis":      "VIN",
    "Moteur":       "Engine No.",
}


import os, threading
from http.server import BaseHTTPRequestHandler, HTTPServer

def start_health_server():
    port = int(os.getenv("PORT", "0") or 0)
    if port <= 0:
        return
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        def log_message(self, *args, **kwargs):
            pass  # silence access logs
    srv = HTTPServer(("", port), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    print(f"🌐 Health server listening on :{port}")



# ===== Patterns =====
PLATE_REGEX = re.compile(r"^\s*([A-Za-z])\s*[-_ ]?\s*(\d{1,6})\s*$")   # e.g., B1000
NUMBER_ONLY_REGEX = re.compile(r"^\s*(\d{1,6})\s*$")                   # e.g., 2259
PHONE_LIKE_REGEX = re.compile(r"(?:\D*\d){6,}")                        # ≥6 digits anywhere

RATE_LIMIT_SECONDS = 1.0

# ===== Legacy phone search helpers =====
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def gen_phone_variants(digits: str) -> List[str]:
    """
    Generate plausible normalized variants:
    - raw digits
    - strip leading 00961 or 961
    - strip single leading 0
    - both (strip code, then 0)
    """
    cand = set()
    def add(x: str):
        if x:
            cand.add(x)
    add(digits)

    d2 = digits
    if d2.startswith("00961"):
        d2 = d2[5:]; add(d2)
    elif d2.startswith("961"):
        d2 = d2[3:]; add(d2)

    def strip0(x: str) -> str:
        return x[1:] if x.startswith("0") else x

    add(strip0(digits))
    add(strip0(d2))

    return sorted(cand, key=lambda x: (-len(x), x))

def phone_norm_sql_expr(col: str) -> str:
    """
    Light SQLite normalization: remove common separators only (avoid parser overflow).
    """
    expr = col
    for ch in ("+", "-", " ", "(", ")", "/", "."):
        expr = f"REPLACE({expr}, '{ch}', '')"
    return expr

# ===== Models =====
@dataclass
class PlateRecord:
    plate: str
    row: Dict[str, Any]   # includes "_rowid_"

# ===== DB (run in background thread via asyncio.to_thread) =====
def query_plate_sync(letter: str, number: str) -> List[PlateRecord]:
    sql = f"""
        SELECT ROWID AS _rowid_, *
        FROM {TABLE_NAME}
        WHERE UPPER({SERIES_COLUMN}) = ?
          AND CAST({NUMBER_COLUMN} AS INTEGER) = ?
        LIMIT 50
    """
    params = (letter.upper(), int(number))
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()

    out: List[PlateRecord] = []
    for r in rows:
        d = dict(r)
        plate = f"{d.get(SERIES_COLUMN, '')}{d.get(NUMBER_COLUMN, '')}"
        out.append(PlateRecord(plate=plate, row=d))
    return out

def query_number_only_sync(number: str) -> List[PlateRecord]:
    sql = f"""
        SELECT ROWID AS _rowid_, *
        FROM {TABLE_NAME}
        WHERE CAST({NUMBER_COLUMN} AS INTEGER) = ?
        LIMIT 100
    """
    params = (int(number),)
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()

    out: List[PlateRecord] = []
    for r in rows:
        d = dict(r)
        plate = f"{d.get(SERIES_COLUMN, '')}{d.get(NUMBER_COLUMN, '')}"
        out.append(PlateRecord(plate=plate, row=d))
    return out

def query_phone_sync(raw_phone: str) -> List[PlateRecord]:
    """
    Legacy working behavior:
    - normalize user input to digits
    - build variants (+961/00961/leading 0 handling)
    - exact match on normalized DB phone
    - fallback: suffix LIKE (last 7 or 6 digits)
    """
    digits = only_digits(raw_phone)
    if not digits or len(digits) < 6:
        return []

    variants = gen_phone_variants(digits)
    suffix7 = digits[-7:] if len(digits) >= 7 else None
    suffix6 = digits[-6:] if len(digits) >= 6 else None
    norm = phone_norm_sql_expr(PHONE_COLUMN)

    placeholders = ",".join("?" for _ in variants) or "?"
    exact_sql = f"""
        WITH t AS (
          SELECT ROWID AS _rowid_, *, {norm} AS phone_norm
          FROM {TABLE_NAME}
        )
        SELECT * FROM t
        WHERE phone_norm IN ({placeholders})
        LIMIT 50
    """

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(exact_sql, variants).fetchall()

        if not rows and (suffix7 or suffix6):
            clauses = []
            params = []
            if suffix7:
                clauses.append("phone_norm LIKE ?")
                params.append(f"%{suffix7}")
            if suffix6:
                clauses.append("phone_norm LIKE ?")
                params.append(f"%{suffix6}")

            suffix_sql = f"""
                WITH t AS (
                  SELECT ROWID AS _rowid_, *, {norm} AS phone_norm
                  FROM {TABLE_NAME}
                )
                SELECT * FROM t
                WHERE {' OR '.join(clauses)}
                LIMIT 50
            """
            rows = conn.execute(suffix_sql, params).fetchall()

    out: List[PlateRecord] = []
    for r in rows:
        d = dict(r)
        plate = f"{d.get(SERIES_COLUMN, '')}{d.get(NUMBER_COLUMN, '')}"
        out.append(PlateRecord(plate=plate, row=d))
    return out

# ===== UI helpers =====
def normalize_plate(letter: str, number: str) -> str:
    return f"{letter.upper()}{int(number)}"

def _fmt_owner(row: Dict[str, Any]) -> str:
    first = str(row.get("Prenom", "") or "").strip()
    last  = str(row.get("Nom", "") or "").strip()
    full  = f"{first} {last}".strip()
    return full if full else ""

def pretty_result(rec: PlateRecord) -> str:
    row = rec.row
    plate = f"{row.get(SERIES_COLUMN,'')}{row.get(NUMBER_COLUMN,'')}"
    owner = _fmt_owner(row)

    lines = [
        f"🔹 <b>Plate:</b> {plate}",
        f"🏳️ <b>Region:</b> {row.get(SERIES_COLUMN,'')}",
        f"🔢 <b>Number:</b> {row.get(NUMBER_COLUMN,'')}",
        "────────────",
    ]

    veh_cols = ["MarqueDesc", "TypeDesc", "CouleurDesc", "PRODDATE", "PreMiseCirc"]
    veh_lines = []
    for col in veh_cols:
        val = row.get(col)
        if val not in (None, ""):
            icon = {
                "MarqueDesc": "🚘",
                "TypeDesc": "📄",
                "CouleurDesc": "🎨",
                "PRODDATE": "📅",
                "PreMiseCirc": "🛣️",
            }.get(col, "•")
            veh_lines.append(f"{icon} <b>{LABEL_MAP.get(col, col)}:</b> {val}")
    if veh_lines:
        lines.append("🚗 <b>Vehicle</b>")
        lines.extend(veh_lines)

    owner_lines = []
    if owner:
        owner_lines.append(f"👤 <b>Name:</b> {owner}")
    for col in ["Addresse", "TelProp"]:
        val = row.get(col)
        if val not in (None, ""):
            icon = {"Addresse": "📍", "TelProp": "📞"}.get(col, "•")
            owner_lines.append(f"{icon} <b>{LABEL_MAP.get(col, col)}:</b> {val}")
    if owner_lines:
        lines.append("────────────")
        lines.append("👤 <b>Owner</b>")
        lines.extend(owner_lines)

    id_lines = []
    for col in ["Chassis", "Moteur"]:
        val = row.get(col)
        if val not in (None, ""):
            icon = {"Chassis": "🔑", "Moteur": "⚙️"}.get(col, "•")
            id_lines.append(f"{icon} <b>{LABEL_MAP.get(col, col)}:</b> {val}")
    if id_lines:
        lines.append("────────────")
        lines.append("🆔 <b>Identifiers</b>")
        lines.extend(id_lines)

    return "\n".join(lines)

def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🔍 Examples"), KeyboardButton("❓ Help")]],
        resize_keyboard=True,
        input_field_placeholder="Type B1000, 2259, or a phone number",
    )

def result_keyboard(plate_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔎 New search", switch_inline_query_current_chat=""),
        InlineKeyboardButton("📋 Copy code", callback_data=f"copy::{plate_code}"),
    ]])

def summarize_row_for_button(row: Dict[str, Any]) -> str:
    make  = str(row.get("MarqueDesc", "") or "").strip()
    model = str(row.get("TypeDesc", "") or "").strip()
    year  = str(row.get("PRODDATE", "") or "").strip()
    owner = _fmt_owner(row)
    base = " ".join(x for x in [make, model] if x) or "Record"
    if year:
        base = f"{base} ({year})"
    if owner:
        base = f"{base} — {owner}"
    return (base[:61] + "…") if len(base) > 62 else base

_last_seen: Dict[int, float] = {}
async def rate_limited(uid: int) -> bool:
    import time
    now = time.monotonic()
    if now - _last_seen.get(uid, 0) < RATE_LIMIT_SECONDS:
        return True
    _last_seen[uid] = now
    return False

async def send_typing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await context.bot.send_chat_action(update.effective_chat.id, ChatAction.TYPING)
    except Exception:
        pass

# ===== Handlers =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info("/start from %s (id=%s) chat=%s", user.username or user.full_name, user.id, update.effective_chat.id)
    await update.message.reply_html(
        "<b>Welcome!</b>\n"
        "Send a plate like <b>B1000</b>, a plain number like <b>2259</b> (all regions), or a <b>phone number</b>.",
        reply_markup=main_keyboard(),
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info("/help from %s (id=%s) chat=%s", user.username or user.full_name, user.id, update.effective_chat.id)
    await update.message.reply_html(
        "<b>How to use</b>\n"
        "• Plate: <code>B1000</code> (1 letter + digits). Dashes/spaces ok: <code>B-1000</code>, <code>B 1000</code>\n"
        "• Number only: <code>2259</code> → all regions with that number\n"
        "• Phone: any format; I normalize (exact → suffix fallback).",
        reply_markup=main_keyboard(),
    )

async def examples_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info("Pressed 'Examples' button: %s (id=%s) chat=%s", user.username or user.full_name, user.id, update.effective_chat.id)
    await update.message.reply_html(
        "Try: <code>B1000</code>, <code>2259</code>, <code>03/681764</code>, <code>+9613681764</code>",
        reply_markup=main_keyboard(),
    )

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data or ""
    await q.answer()  # don't log callbacks
    if data.startswith("copy::"):
        return await q.message.reply_text(data.split("::", 1)[1])
    if data.startswith("show::"):
        rowid = data.split("::", 1)[1]
        cache: Dict[str, Dict[str, Any]] = context.user_data.get("last_results", {})
        row = cache.get(rowid)
        if not row:
            return await q.message.reply_html("⚠️ Selection expired. Please search again.")
        rec = PlateRecord(plate=f"{row.get(SERIES_COLUMN,'')}{row.get(NUMBER_COLUMN,'')}", row=row)
        return await q.message.reply_html(pretty_result(rec), reply_markup=result_keyboard(rec.plate))

async def on_text_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (update.message.text or "")
    user = update.effective_user
    logger.info("Text button from %s (id=%s): %r", user.username or user.full_name, user.id, txt)
    low = txt.lower()
    if "help" in low:
        return await help_cmd(update, context)
    if "example" in low:
        return await examples_button(update, context)

async def present_results(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          results: List[PlateRecord], query_label: str):
    if not results:
        return await update.message.reply_html(
            f"No results for <b>{query_label}</b>.",
            reply_markup=main_keyboard(),
        )
    if len(results) == 1:
        rec = results[0]
        return await update.message.reply_html(
            pretty_result(rec),
            reply_markup=result_keyboard(rec.plate),
        )

    cache: Dict[str, Dict[str, Any]] = {}
    buttons: List[List[InlineKeyboardButton]] = []
    row_buttons: List[InlineKeyboardButton] = []

    for rec in results[:10]:
        row = rec.row
        rowid = str(row.get("_rowid_"))
        cache[rowid] = row
        label = summarize_row_for_button(row) or rec.plate
        row_buttons.append(InlineKeyboardButton(label, callback_data=f"show::{rowid}"))
        if len(row_buttons) == 2:
            buttons.append(row_buttons)
            row_buttons = []
    if row_buttons:
        buttons.append(row_buttons)

    context.user_data["last_results"] = cache
    head = f"🔎 <b>{len(results)} matches</b> for <code>{query_label}</code> — pick one:"
    return await update.message.reply_html(head, reply_markup=InlineKeyboardMarkup(buttons))

async def on_plate_or_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await rate_limited(update.effective_user.id):
        return

    text = (update.message.text or "").strip()
    user = update.effective_user
    logger.info("Text from %s (id=%s) chat=%s: %r", user.username or user.full_name, user.id, update.effective_chat.id, text)

    # 1) Plate?
    m = PLATE_REGEX.match(text)
    if m:
        letter, number = m.group(1), m.group(2)
        await send_typing(update, context)
        try:
            results = await asyncio.to_thread(query_plate_sync, letter, number)
            logger.info("Plate results for %s: %d row(s)", normalize_plate(letter, number), len(results))
        except Exception as e:
            logger.exception("DB error (plate) for %s%s", letter.upper(), number)
            return await update.message.reply_html(f"<b>Database error:</b> {e}")
        return await present_results(update, context, results, normalize_plate(letter, number))

    # 2) Number-only?
    m2 = NUMBER_ONLY_REGEX.match(text)
    if m2:
        number_only = m2.group(1)
        await send_typing(update, context)
        try:
            results = await asyncio.to_thread(query_number_only_sync, number_only)
            logger.info("Number-only results for %s: %d row(s)", number_only, len(results))
        except Exception as e:
            logger.exception("DB error (number-only) for %s", number_only)
            return await update.message.reply_html(f"<b>Database error:</b> {e}")
        return await present_results(update, context, results, f"ActualNB {int(number_only)}")

    # 3) Phone?
    if PHONE_LIKE_REGEX.search(text):
        await send_typing(update, context)
        try:
            results = await asyncio.to_thread(query_phone_sync, text)
            logger.info("Phone results for %r: %d row(s)", text, len(results))
        except Exception as e:
            logger.exception("DB error (phone) for %r", text)
            return await update.message.reply_html(f"<b>Database error:</b> {e}")
        return await present_results(update, context, results, f"phone {text}")

    return await update.message.reply_html(
        "I didn’t recognize that. Send a plate like <b>B1000</b>, a number like <b>2259</b>, or a <b>phone number</b>.",
        reply_markup=main_keyboard(),
    )

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error with update %s", getattr(update, "update_id", update))

def build_app():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.Regex("^(❓ Help|🔍 Examples)$") & filters.TEXT, on_text_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_plate_or_phone))
    app.add_error_handler(on_error)
    return app

if __name__ == "__main__":
    print("✅ Bot is starting…")
    ensure_db()  # keep this if you’re using the auto-download
    start_health_server()  # <-- add this line
    app = build_app()
    print("🚀 Bot is running. Press Ctrl+C to stop.")
    try:
        app.run_polling()
    finally:
        print("✅ Clean exit.")
