import asyncio
import atexit
import csv
import io
import json
import os
import re
import signal
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import html
import logging
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    Update,
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder

# Load .env from project root before any config-dependent imports
_PROJECT_ROOT = Path(__file__).resolve().parent
_ENV_PATH = _PROJECT_ROOT / ".env"
_ENV_EXAMPLE = _PROJECT_ROOT / ".env.example"
if not _ENV_PATH.exists() and _ENV_EXAMPLE.exists():
    try:
        shutil.copy(_ENV_EXAMPLE, _ENV_PATH)
    except Exception:
        pass
load_dotenv(_ENV_PATH)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(".env loaded from: %s", _ENV_PATH.resolve())

BASE_DIR = _PROJECT_ROOT
ENV_FILE = _ENV_PATH
ENV_EXAMPLE = _ENV_EXAMPLE

import db
from config import ADMIN_IDS, is_admin
from filters import AdminOnly

LOCK_FILE = BASE_DIR / "bot.lock"
REMINDERS_ENABLED = os.getenv("REMINDERS_ENABLED", "1").strip() in ("1", "true", "yes", "on")
PAGE_SIZE = 5

# User sort preference: user_id -> "newest" | "author" | "category" | "manual"
_user_sort_prefs: dict[int, str] = {}

# Admin books filter state (admin_id -> filter dict)
_admin_books_filter: dict[int, dict] = {}

_DEFAULT_ADMIN_FILTER = {"q": "", "category": None, "only_out_of_stock": False}

# Add-book template: last category + cover (per admin)
_add_book_last: dict[int, dict] = {}


def _get_admin_filter(admin_id: int) -> dict:
    return _admin_books_filter.get(admin_id, _DEFAULT_ADMIN_FILTER.copy())


def _get_sort_mode(user_id: int) -> str:
    return _user_sort_prefs.get(user_id, db.SORT_NEWEST)


def is_pid_running(pid: int) -> bool:
    try:
        pid = int(pid)
    except Exception:
        return False
    if os.name == "nt":
        try:
            res = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
            return str(pid) in res.stdout
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False


def create_lock() -> None:
    if LOCK_FILE.exists():
        try:
            data = LOCK_FILE.read_text(encoding="utf-8").strip()
            existing_pid = int(data.split()[0]) if data else None
        except Exception:
            existing_pid = None
        if existing_pid and is_pid_running(existing_pid):
            print("Another bot instance appears to be running (pid=%s). Exiting." % existing_pid)
            sys.exit(1)
        else:
            try:
                LOCK_FILE.unlink()
            except Exception:
                pass
    try:
        LOCK_FILE.write_text(f"{os.getpid()} {datetime.now().isoformat()}", encoding="utf-8")
    except Exception:
        pass


def remove_lock() -> None:
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except Exception:
        pass


atexit.register(remove_lock)


def _sigterm_handler(signum, frame):
    remove_lock()
    sys.exit(0)


signal.signal(signal.SIGINT, _sigterm_handler)
try:
    signal.signal(signal.SIGTERM, _sigterm_handler)
except Exception:
    pass


# ====== FSM States ======
class AddBookStates(StatesGroup):
    title = State()
    author = State()
    category = State()
    category_other = State()
    year = State()
    cover_type = State()
    qty = State()
    rent_fee = State()
    deposit = State()
    photo = State()
    preview = State()


class EditBookStates(StatesGroup):
    choose_field = State()
    title = State()
    rent_fee = State()
    qty = State()
    photo = State()


class SearchStates(StatesGroup):
    query = State()


class AdminPenaltyStates(StatesGroup):
    amount = State()


class AdminBooksFilterStates(StatesGroup):
    search_query = State()


class AdminPenaltyEditStates(StatesGroup):
    choose_action = State()
    per_day = State()
    fixed = State()
    note = State()


class AdminBroadcastStates(StatesGroup):
    message = State()
    confirm = State()


# Cheklov: batch size va delay (Telegram rate limit)
BROADCAST_BATCH_SIZE = 25
BROADCAST_DELAY_SEC = 1.0
BROADCAST_MAX_USERS = 500


# ====== Keyboards ======
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="📚 Kitoblar"),
        KeyboardButton(text="ℹ️ Qoidalar"),
    )
    builder.row(KeyboardButton(text="📖 Mening ijaralarim"))
    return builder.as_markup(resize_keyboard=True)


def admin_menu_keyboard() -> ReplyKeyboardMarkup:
    """Admin main menu as ReplyKeyboard."""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="➕ Kitob qo'shish"),
        KeyboardButton(text="📚 Kitoblarim"),
    )
    builder.row(
        KeyboardButton(text="🧹 Tozalash"),
        KeyboardButton(text="📦 Ijaralar"),
    )
    builder.row(
        KeyboardButton(text="⏰ Kechikkanlar"),
        KeyboardButton(text="💰 Jarima"),
    )
    builder.row(
        KeyboardButton(text="📊 Userlar statistikasi"),
        KeyboardButton(text="📢 E'lon"),
    )
    builder.row(KeyboardButton(text="📤 Export"))
    return builder.as_markup(resize_keyboard=True)


def admin_menu_inline_keyboard() -> InlineKeyboardMarkup:
    """Admin quick inline menu (for callbacks)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Kitob qo'shish", callback_data="admin_add_book")],
        [InlineKeyboardButton(text="📚 Kitoblarim", callback_data="admin_books")],
        [InlineKeyboardButton(text="🧹 Tozalash", callback_data="admin_wipe")],
        [InlineKeyboardButton(text="📦 Ijaralar", callback_data="admin_rentals")],
    ])


def _page_cb(page: int, category: Optional[str], q: Optional[str], sort: str) -> str:
    cat = "all" if category is None else category
    return f"books_p_{page}:{cat}:{q or ''}:{sort}"


def books_list_keyboard(
    books: list,
    page: int,
    total_pages: int,
    category: Optional[str] = None,
    q: Optional[str] = None,
    sort_mode: str = "newest",
) -> InlineKeyboardMarkup:
    rows = []
    for b in books:
        title = (b.get("title", "") or "Noma'lum")[:40]
        rows.append([
            InlineKeyboardButton(
                text=f"📘 {title}",
                callback_data=f"book_{b['id']}",
            ),
        ])
        stock = db.get_book_stock(b["id"]) or {}
        available = stock.get("available", 0)
        if available > 0:
            rows.append([
                InlineKeyboardButton(
                    text="📥 Ijaraga olish",
                    callback_data=f"rent_{b['id']}",
                ),
            ])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️ Oldingi", callback_data=_page_cb(page - 1, category, q, sort_mode)))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="Keyingi ▶️", callback_data=_page_cb(page + 1, category, q, sort_mode)))
    rows.append(nav)
    rows.append([
        InlineKeyboardButton(text="🏷 Kategoriyalar", callback_data="books_cat"),
        InlineKeyboardButton(text="🔙 Orqaga", callback_data="books_back"),
    ])
    rows.append([
        InlineKeyboardButton(text="🔎 Qidiruv", callback_data="books_search"),
        InlineKeyboardButton(text="↕️ Tartiblash", callback_data=f"books_sort:{'all' if category is None else category}:{q or ''}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_books_keyboard(books: list, page: int, total_pages: int, filter_state: dict | None = None) -> InlineKeyboardMarkup:
    rows = []
    if filter_state is not None:
        out_text = "❌ Mavjud emas ✓" if filter_state.get("only_out_of_stock") else "❌ Mavjud emas"
        rows.append([
            InlineKeyboardButton(text="🔎 Qidiruv", callback_data="admin_books_filter_search"),
            InlineKeyboardButton(text="🏷 Kategoriya", callback_data="admin_books_filter_cat"),
            InlineKeyboardButton(text=out_text, callback_data="admin_books_filter_oos"),
            InlineKeyboardButton(text="♻️ Tozalash", callback_data="admin_books_filter_clear"),
        ])
    for b in books:
        title = (b.get("title", "") or "Noma'lum")[:30]
        rows.append([
            InlineKeyboardButton(text=f"📘 {title}", callback_data=f"admin_book_{b['id']}"),
        ])
        rows.append([
            InlineKeyboardButton(text="✏️ Edit", callback_data=f"admin_edit_{b['id']}"),
            InlineKeyboardButton(text="🗑 Delete", callback_data=f"admin_del_{b['id']}"),
        ])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"admin_books_p_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"admin_books_p_{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_del_confirm_keyboard(book_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, o'chirish", callback_data=f"admin_del_confirm_{book_id}")],
        [InlineKeyboardButton(text="❌ Bekor", callback_data="admin_books")],
    ])


def admin_edit_keyboard(book_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Nomi", callback_data=f"edit_field_title_{book_id}")],
        [InlineKeyboardButton(text="💰 Ijara narxi", callback_data=f"edit_field_rent_{book_id}")],
        [InlineKeyboardButton(text="📦 Soni", callback_data=f"edit_field_qty_{book_id}")],
        [
            InlineKeyboardButton(text="📷 Rasm qo'shish", callback_data=f"edit_field_photo_{book_id}"),
            InlineKeyboardButton(text="🗑 Rasmni o'chirish", callback_data=f"edit_field_remove_{book_id}"),
        ],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_books")],
    ])


PAGE_SIZE_OVERDUE = 10


def admin_overdue_keyboard(overdue_list: list, page: int, total_pages: int) -> InlineKeyboardMarkup:
    """Keyboard for overdue rentals list. Each item: Eslatma, Jarima, Qaytarildi."""
    rows = []
    for r in overdue_list:
        rid = r.get("rental_id", r.get("id"))
        title = (r.get("book_title") or "?")[:25]
        rows.append([
            InlineKeyboardButton(text=f"✉️ Eslatma — {title}", callback_data=f"overdue_ping_{rid}"),
        ])
        rows.append([
            InlineKeyboardButton(text="💸 Jarima", callback_data=f"penalty_edit_{rid}_{page}"),
            InlineKeyboardButton(text="✅ Qaytarildi", callback_data=f"rental_return_{rid}"),
        ])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"overdue_p_{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"overdue_p_{page+1}"))
    rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_penalty_edit_keyboard(rental_id: int, from_page: int) -> InlineKeyboardMarkup:
    """Keyboard for penalty edit menu."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Jarimani yoq/o'chir", callback_data=f"penalty_toggle_{rental_id}")],
        [InlineKeyboardButton(text="✍️ Kunlik jarima", callback_data=f"penalty_perday_{rental_id}")],
        [InlineKeyboardButton(text="🧾 Fiks jarima", callback_data=f"penalty_fixed_{rental_id}")],
        [InlineKeyboardButton(text="🗑 Fiksni o'chirish", callback_data=f"penalty_clear_fixed_{rental_id}")],
        [InlineKeyboardButton(text="📝 Izoh", callback_data=f"penalty_note_{rental_id}")],
        [InlineKeyboardButton(text="⬅️ Orqaga", callback_data=f"penalty_back_{from_page}")],
    ])


def admin_rentals_keyboard(rentals: list) -> InlineKeyboardMarkup:
    rows = []
    for r in rentals:
        status = r.get("status", "")
        title = (r.get("book_title", "") or "?")[:30]
        rows.append([
            InlineKeyboardButton(text=f"📖 {title} (ID:{r['id']})", callback_data=f"rental_{r['id']}"),
        ])
        if status == "requested":
            rows.append([
                InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"rental_ok_{r['id']}"),
                InlineKeyboardButton(text="❌ Rad etish", callback_data=f"rental_no_{r['id']}"),
            ])
        elif status in ("approved", "active"):
            rows.append([
                InlineKeyboardButton(text="✅ Qaytarildi", callback_data=f"rental_return_{r['id']}"),
            ])
    if not rows:
        rows.append([InlineKeyboardButton(text="(Ijaralar yo'q)", callback_data="noop")])
    rows.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def rental_period_keyboard(book_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 kun", callback_data=f"period_{book_id}_7"),
            InlineKeyboardButton(text="14 kun", callback_data=f"period_{book_id}_14"),
        ],
        [InlineKeyboardButton(text="30 kun", callback_data=f"period_{book_id}_30")],
    ])


# ====== User Handlers ======
async def cmd_start(message: Message):
    text = (
        "Assalomu alaykum!\n\n"
        "Bu bot orqali kitoblarni ijaraga olishingiz mumkin.\n\n"
        "Quyidagi tugmalardan birini tanlang."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


async def show_rules(message: Message):
    text = (
        "ℹ️ <b>Ijara qoidalari:</b>\n\n"
        "• Muddat: 7, 14 yoki 30 kun.\n"
        "• Kitobni muddatida qaytarish majburiy.\n"
        "• Zarar yoki yo'qotishda javobgarlik sizda.\n"
        "• Savollar uchun admin bilan bog'laning."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


async def show_books_menu(message: Message):
    """Show categories and search for books."""
    cats = db.get_categories()
    if not cats:
        await message.answer("Hozircha kitoblar mavjud emas.", reply_markup=main_menu_keyboard())
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"cat_{c}")] for c in cats
    ] + [
        [InlineKeyboardButton(text="📚 Barcha kitoblar", callback_data="cat_all")],
        [InlineKeyboardButton(text="🔎 Qidiruv", callback_data="books_search")],
    ])
    await message.answer("📚 Kitoblar yoki kategoriyani tanlang:", reply_markup=kb)


async def cb_books_cat(callback: CallbackQuery):
    """Show categories (from books list)."""
    cats = db.get_categories()
    if not cats:
        await callback.answer("Kategoriyalar yo'q.")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"cat_{c}")] for c in cats
    ] + [
        [InlineKeyboardButton(text="📚 Barcha kitoblar", callback_data="cat_all")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="books_back")],
    ])
    await callback.message.edit_text("🏷 Kategoriyani tanlang:", reply_markup=kb)
    await callback.answer()


async def cb_books_category(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("cat_"):
        await callback.answer()
        return
    cat = data.replace("cat_", "") if data != "cat_all" else None
    sort_mode = _get_sort_mode(callback.from_user.id)
    books = db.list_books(offset=0, limit=PAGE_SIZE, category=cat, sort_mode=sort_mode)
    total = db.count_books(category=cat)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if not books:
        await callback.message.edit_text("Kitoblar topilmadi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="books_back")],
        ]))
        await callback.answer()
        return
    text = f"📚 <b>Kitoblar</b> — Sahifa 1/{total_pages}\n\n"
    for b in books:
        stock = db.get_book_stock(b["id"]) or {}
        av, tot = stock.get("available", 0), stock.get("total", 0)
        text += f"• {html.escape(b['title'])} — {html.escape(b['author'])}\n"
        text += f"  💰 {b.get('rent_fee', 0)} so'm/kun | 📦 Mavjud: {av} / {tot}\n\n"
    await callback.message.edit_text(text, reply_markup=books_list_keyboard(books, 1, total_pages, category=cat, sort_mode=sort_mode))
    await callback.answer()


async def cb_books_page(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("books_p_"):
        await callback.answer()
        return
    try:
        parts = data.replace("books_p_", "").split(":")
        page = int(parts[0])
        cat_str = parts[1] if len(parts) > 1 else ""
        q = parts[2] if len(parts) > 2 else None
        sort_mode = parts[3] if len(parts) > 3 and parts[3] else _get_sort_mode(callback.from_user.id)
        cat = None if (not cat_str or cat_str == "all") else cat_str
    except (IndexError, ValueError):
        page, cat, q, sort_mode = 1, None, None, _get_sort_mode(callback.from_user.id)
    offset = (page - 1) * PAGE_SIZE
    books = db.list_books(offset=offset, limit=PAGE_SIZE, category=cat, q=q, sort_mode=sort_mode)
    total = db.count_books(category=cat, q=q)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if not books:
        await callback.answer("Sahifa bo'sh.")
        return
    text = f"📚 <b>Kitoblar</b> — Sahifa {page}/{total_pages}\n\n"
    for b in books:
        stock = db.get_book_stock(b["id"]) or {}
        av, tot = stock.get("available", 0), stock.get("total", 0)
        text += f"• {html.escape(b['title'])} — {html.escape(b['author'])}\n"
        text += f"  💰 {b.get('rent_fee', 0)} so'm/kun | 📦 Mavjud: {av} / {tot}\n\n"
    await callback.message.edit_text(text, reply_markup=books_list_keyboard(books, page, total_pages, category=cat, q=q, sort_mode=sort_mode))
    await callback.answer()


def _sort_choice_keyboard(cat: Optional[str], q: Optional[str], is_admin: bool) -> InlineKeyboardMarkup:
    cat_str = "all" if cat is None else cat
    base = f"sort_sel:{cat_str}:{q or ''}:"
    rows = [
        [InlineKeyboardButton(text="🆕 Yangi avval", callback_data=base + db.SORT_NEWEST)],
        [InlineKeyboardButton(text="📝 Muallif A–Z", callback_data=base + db.SORT_AUTHOR)],
        [InlineKeyboardButton(text="🏷 Kategoriya + nom", callback_data=base + db.SORT_CATEGORY)],
    ]
    if is_admin and db.has_any_manual_order():
        rows.append([InlineKeyboardButton(text="📋 Qoʻlda tartib", callback_data=base + db.SORT_MANUAL)])
    rows.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="books_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def cb_books_sort(callback: CallbackQuery):
    """Show sort mode choices."""
    data = callback.data or ""
    if not data.startswith("books_sort:"):
        await callback.answer()
        return
    try:
        parts = data.replace("books_sort:", "").split(":", 2)
        cat_str = parts[0] if parts else "all"
        q = parts[1] if len(parts) > 1 else ""
        cat = None if cat_str == "all" else cat_str
    except (IndexError, ValueError):
        cat, q = None, ""
    is_admin_user = is_admin(callback.from_user.id)
    kb = _sort_choice_keyboard(cat, q or None, is_admin_user)
    await callback.message.edit_text("↕️ Tartiblash usulini tanlang:", reply_markup=kb)
    await callback.answer()


async def cb_sort_sel(callback: CallbackQuery):
    """Apply sort mode and refresh book list."""
    data = callback.data or ""
    if not data.startswith("sort_sel:"):
        await callback.answer()
        return
    try:
        parts = data.replace("sort_sel:", "").split(":", 2)
        cat_str = parts[0] if parts else "all"
        q = parts[1] if len(parts) > 1 else ""
        sort_mode = parts[2] if len(parts) > 2 else db.SORT_NEWEST
        cat = None if cat_str == "all" else cat_str
    except (IndexError, ValueError):
        cat, q, sort_mode = None, "", db.SORT_NEWEST
    _user_sort_prefs[callback.from_user.id] = sort_mode
    books = db.list_books(offset=0, limit=PAGE_SIZE, category=cat, q=q or None, sort_mode=sort_mode)
    total = db.count_books(category=cat, q=q or None)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if not books:
        await callback.message.edit_text("Kitoblar topilmadi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="books_back")],
        ]))
        await callback.answer()
        return
    text = f"📚 <b>Kitoblar</b> — Sahifa 1/{total_pages}\n\n"
    for b in books:
        stock = db.get_book_stock(b["id"]) or {}
        av, tot = stock.get("available", 0), stock.get("total", 0)
        text += f"• {html.escape(b['title'])} — {html.escape(b['author'])}\n"
        text += f"  💰 {b.get('rent_fee', 0)} so'm/kun | 📦 Mavjud: {av} / {tot}\n\n"
    await callback.message.edit_text(text, reply_markup=books_list_keyboard(books, 1, total_pages, category=cat, q=q or None, sort_mode=sort_mode))
    await callback.answer()


async def cb_books_search_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Kitob nomi yoki muallif yozing:")
    await state.set_state(SearchStates.query)
    await state.update_data(from_callback=True)
    await callback.answer()


async def search_query_handler(message: Message, state: FSMContext):
    q = (message.text or "").strip()
    if not q:
        await message.answer("Iltimos, qidiruv so'zini kiriting.")
        return
    await state.clear()
    sort_mode = _get_sort_mode(message.from_user.id)
    books = db.list_books(offset=0, limit=PAGE_SIZE, q=q, sort_mode=sort_mode)
    total = db.count_books(q=q)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if not books:
        await message.answer("Hech narsa topilmadi.", reply_markup=main_menu_keyboard())
        return
    text = f"🔎 <b>Qidiruv natijalari</b> — Sahifa 1/{total_pages}\n\n"
    for b in books:
        stock = db.get_book_stock(b["id"]) or {}
        av, tot = stock.get("available", 0), stock.get("total", 0)
        text += f"• {html.escape(b['title'])} — {html.escape(b['author'])}\n"
        text += f"  💰 {b.get('rent_fee', 0)} so'm/kun | 📦 Mavjud: {av} / {tot}\n\n"
    await message.answer(text, reply_markup=books_list_keyboard(books, 1, total_pages, q=q, sort_mode=sort_mode), parse_mode=ParseMode.HTML)


async def cb_rent_book(callback: CallbackQuery):
    data = callback.data or ""
    if not (data.startswith("rent_") or data.startswith("book_")):
        await callback.answer()
        return
    try:
        book_id = int(data.replace("rent_", "").replace("book_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    book = db.get_book(book_id)
    if not book:
        await callback.answer("Kitob topilmadi.", show_alert=True)
        return
    stock = db.get_book_stock(book_id) or {}
    available = stock.get("available", 0)
    total = stock.get("total", 0)
    text = (
        f"📘 <b>{html.escape(book['title'])}</b>\n"
        f"Muallif: {html.escape(book['author'])}\n"
        f"Narx: {book.get('rent_fee', 0)} so'm/kun\n"
        f"📦 Mavjud: {available} / {total}\n\n"
    )
    if available <= 0:
        text += "❌ Hozircha mavjud emas"
        kb = None
    else:
        text += "Ijara muddatini tanlang:"
        kb = rental_period_keyboard(book_id)
    if book.get("photo_id"):
        await callback.message.answer_photo(book["photo_id"], caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await callback.message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_rental_period(callback: CallbackQuery, state: FSMContext):
    data = callback.data or ""
    if not data.startswith("period_"):
        await callback.answer()
        return
    try:
        _, book_id_str, days_str = data.split("_")
        book_id = int(book_id_str)
        days = int(days_str)
    except (ValueError, IndexError):
        await callback.answer("Xatolik.")
        return
    book = db.get_book(book_id)
    if not book:
        await callback.answer("Kitob topilmadi.", show_alert=True)
        return
    stock = db.get_book_stock(book_id) or {}
    if stock.get("available", 0) <= 0:
        await callback.answer("❌ Kechirasiz, bu kitob hozir mavjud emas.", show_alert=True)
        return
    due = (datetime.now().date() + timedelta(days=days)).strftime("%Y-%m-%d")
    rental_id = db.create_rental_request(callback.from_user.id, book_id, due)
    admin_text = (
        "📚 <b>Yangi ijara so'rovi</b>\n\n"
        f"👤 Foydalanuvchi: {callback.from_user.id} (@{callback.from_user.username or '—'})\n"
        f"📖 Kitob: {book['title']} ({book['author']})\n"
        f"📅 Qaytarish: {due}\n"
        f"🆔 ID: {rental_id}"
    )
    for admin_id in ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"rental_ok_{rental_id}"),
                    InlineKeyboardButton(text="❌ Rad etish", callback_data=f"rental_no_{rental_id}"),
                ],
            ])
            await callback.bot.send_message(admin_id, admin_text, reply_markup=kb)
        except Exception as e:
            logger.warning("Admin notify failed: %s", e)
    await callback.message.answer(
        "✅ So'rovingiz yuborildi. Admin tasdiqlagach xabar olasiz.",
        reply_markup=main_menu_keyboard(),
    )
    await callback.answer()


async def my_rentals(message: Message):
    rentals = [r for r in db.list_rentals() if r.get("user_id") == message.from_user.id]
    if not rentals:
        await message.answer("Sizda hozircha ijaralar yo'q.", reply_markup=main_menu_keyboard())
        return
    text = "📖 <b>Sizning ijaralaringiz:</b>\n\n"
    now = datetime.now(timezone.utc)
    for r in rentals:
        status = r.get("status", "?")
        status_uz = {
            "requested": "⏳ Ko'rilmoqda",
            "approved": "✅ Tasdiqlangan",
            "active": "📖 Faol",
            "rejected": "❌ Rad etilgan",
            "returned": "Qaytarilgan ✅",
        }.get(status, status)
        text += f"• {r.get('book_title', '?')} — {status_uz}\n"
        text += f"  Qaytarish: {r.get('due_ts', '?')}"
        if r.get("returned_at"):
            text += f"\n  Qaytarildi: {r.get('returned_at', '')[:10]}"
        if status in ("active", "approved"):
            penalty = db.compute_penalty(r, now)
            if penalty > 0:
                text += f"\n  Jarima: {penalty:,} so'm"
        text += "\n\n"
    await message.answer(text, reply_markup=main_menu_keyboard(), parse_mode=ParseMode.HTML)


# ====== Admin Handlers ======
async def cmd_admin(message: Message):
    await message.answer("⚙️ Admin menyu:", reply_markup=admin_menu_keyboard())


async def admin_add_book_msg(message: Message, state: FSMContext):
    """Handle '➕ Kitob qo'shish' text button."""
    await state.clear()
    await message.answer("Kitob nomini kiriting:")
    await state.set_state(AddBookStates.title)


async def admin_books_msg(message: Message):
    """Handle '📚 Kitoblarim' text button."""
    admin_id = message.from_user.id if message.from_user else 0
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await message.answer(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)


async def admin_wipe_msg(message: Message):
    """Handle '🧹 Tozalash' text button."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, hammasini o'chir", callback_data="wipe_confirm")],
        [InlineKeyboardButton(text="❌ Bekor", callback_data="wipe_cancel")],
    ])
    await message.answer("⚠️ Barcha kitoblar va ijaralar o'chiriladi. Tasdiqlaysizmi?", reply_markup=kb)


def _format_admin_books_filter_header(f: dict) -> str:
    """Build filter header line for admin books list."""
    parts = []
    if f.get("q"):
        parts.append(f"q='{f['q']}'")
    if f.get("category"):
        parts.append(f"kategoriya='{f['category']}'")
    if f.get("only_out_of_stock"):
        parts.append("mavjud_emas=ON")
    if not parts:
        return "Filtr: yo'q"
    return "Filtr: " + " | ".join(parts)


def _build_admin_books_list(admin_id: int, page: int = 1) -> tuple[str, list, int, dict]:
    """Fetch filtered books and build list text. Returns (text, books, total_pages, filter_state)."""
    f = _get_admin_filter(admin_id)
    q = (f.get("q") or "").strip().lower() or None
    cat = f.get("category")
    oos = f.get("only_out_of_stock", False)
    books, total = db.list_books_admin(q=q, category=cat, only_out_of_stock=oos, page=page, page_size=PAGE_SIZE)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    header = _format_admin_books_filter_header(f)
    text = f"📚 <b>Kitoblarim</b> — {page}/{total_pages}\n{header}\n\n"
    for b in books:
        stock = db.get_book_stock(b["id"]) or {}
        st = f"📦 Jami: {stock.get('total', 0)} | 🔒 Band: {stock.get('rented', 0)} | "
        st += f"✅ Mavjud: {stock.get('available', 0)}" if stock.get("available", 0) > 0 else "❌ Mavjud emas"
        text += f"📘 {b['title']}\n  {st}\n\n"
    return text, books, total_pages, f


def _admin_rentals_text(rentals: list) -> str:
    """Build admin rentals list text."""
    text = "📦 <b>Ijaralar</b>\n\n"
    if not rentals:
        text += "So'rovlar va faol ijaralar yo'q."
    else:
        for r in rentals:
            st = r.get("status", "?")
            st_uz = {"requested": "⏳ So'rov", "approved": "✅ Tasdiqlangan", "active": "📖 Faol"}.get(st, st)
            text += f"• {r.get('book_title')} — User {r['user_id']} — {r.get('due_ts')} ({st_uz})\n"
    return text


def _format_overdue_list(overdue_list: list, page: int, total_pages: int, total: int) -> str:
    """Build overdue list text with overdue_days computed in Python."""
    now = datetime.now(timezone.utc)
    text = f"⏰ <b>Kechikkanlar</b> ({total} ta) — Sahifa {page}/{total_pages}\n\n"
    for i, r in enumerate(overdue_list, 1):
        due_str = r.get("due_date") or r.get("due_ts") or ""
        overdue_days = 1
        if due_str:
            try:
                due_date = datetime.fromisoformat(due_str[:10] + "T00:00:00+00:00")
                delta = now - due_date
                overdue_days = max(1, int(delta.total_seconds() / 86400))
            except Exception:
                pass
        due_pretty = due_str[:10] if due_str else "muddat belgilanmagan"
        text += (
            f"{i}) 📕 {r.get('book_title', '?')} — {r.get('book_author', '?')}\n"
            f"   👤 user: {r.get('user_id', '?')}\n"
            f"   ⏳ Kechikdi: {overdue_days} kun\n"
            f"   📅 Muddat: {due_pretty}\n"
        )
        r_with_due = {**r, "due_ts": due_str}
        computed = db.compute_penalty(r_with_due, datetime.now(timezone.utc))
        if computed > 0:
            text += f"   💰 Hisoblangan jarima: {computed} so'm\n"
        text += "\n"
    return text


async def admin_stats_msg(message: Message):
    """Handle '📊 Userlar statistikasi' — show user stats menu."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Eng ko'p ijaraga olganlar", callback_data="admin_stats_top")],
        [InlineKeyboardButton(text="⏳ Qaytarmaganlar", callback_data="admin_stats_not_returned")],
        [InlineKeyboardButton(text="🚫 Blacklist (3+ kechikkan)", callback_data="admin_stats_blacklist")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
    ])
    await message.answer("📊 <b>Userlar bo'yicha statistika</b>\n\nTanlang:", reply_markup=kb, parse_mode=ParseMode.HTML)


async def cb_admin_stats_top(callback: CallbackQuery):
    """Eng ko'p ijaraga olgan userlar."""
    rows = db.list_top_renters(limit=15)
    text = "🏆 <b>Eng ko'p ijaraga olgan userlar</b>\n\n"
    if not rows:
        text += "Hozircha ma'lumot yo'q."
    else:
        for i, r in enumerate(rows, 1):
            text += f"{i}. User ID: <code>{r['user_id']}</code> — {r['rental_count']} ta ijara\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_stats_back")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_stats_not_returned(callback: CallbackQuery):
    """Qaytarmaganlar — users with overdue rentals."""
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = db.list_users_not_returned(now_iso)
    text = "⏳ <b>Qaytarmaganlar</b> (kechikkan ijarasi bor)\n\n"
    if not rows:
        text += "Hozircha qaytarmaganlar yo'q."
    else:
        for i, r in enumerate(rows, 1):
            titles = (r.get("book_titles") or "?")[:60]
            if len(r.get("book_titles") or "") > 60:
                titles += "..."
            text += f"{i}. User ID: <code>{r['user_id']}</code> — {r['overdue_count']} ta\n   📕 {titles}\n\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_stats_back")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_stats_blacklist(callback: CallbackQuery):
    """Blacklist — users with 3+ overdue incidents."""
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = db.list_blacklist_users(now_iso, min_overdue_count=3)
    text = "🚫 <b>Blacklist</b> (3 marta yoki undan ko'p kechikkan)\n\n"
    if not rows:
        text += "Blacklist bo'sh."
    else:
        for i, r in enumerate(rows, 1):
            text += f"{i}. User ID: <code>{r['user_id']}</code> — {r['overdue_count']} marta kechikkan\n"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_stats_back")],
    ])
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_stats_back(callback: CallbackQuery):
    """Back to stats menu."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏆 Eng ko'p ijaraga olganlar", callback_data="admin_stats_top")],
        [InlineKeyboardButton(text="⏳ Qaytarmaganlar", callback_data="admin_stats_not_returned")],
        [InlineKeyboardButton(text="🚫 Blacklist (3+ kechikkan)", callback_data="admin_stats_blacklist")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
    ])
    await callback.message.edit_text("📊 <b>Userlar bo'yicha statistika</b>\n\nTanlang:", reply_markup=kb, parse_mode=ParseMode.HTML)
    await callback.answer()


async def admin_export_msg(message: Message):
    """Handle '📤 Export' — choose CSV or JSON backup."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 CSV", callback_data="export_csv")],
        [InlineKeyboardButton(text="📋 JSON", callback_data="export_json")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
    ])
    await message.answer("📤 <b>Export (zaxira)</b>\n\nKitoblar va ijaralar. Formatni tanlang:", reply_markup=kb, parse_mode=ParseMode.HTML)


def _export_to_json() -> bytes:
    """Generate JSON export: books + rentals."""
    books = db.get_all_books_for_export()
    rentals = db.get_all_rentals_for_export()
    data = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "books": books,
        "rentals": rentals,
    }
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")


def _export_to_csv() -> bytes:
    """Generate CSV export: books and rentals as two sections."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["# BOOKS"])
    books = db.get_all_books_for_export()
    if books:
        writer.writerow(list(books[0].keys()))
        for row in books:
            writer.writerow([row.get(k, "") for k in books[0].keys()])
    writer.writerow([])
    writer.writerow(["# RENTALS"])
    rentals = db.get_all_rentals_for_export()
    if rentals:
        writer.writerow(list(rentals[0].keys()))
        for row in rentals:
            writer.writerow([row.get(k, "") for k in rentals[0].keys()])
    return buf.getvalue().encode("utf-8-sig")


async def cb_export_csv(callback: CallbackQuery):
    """Send CSV backup."""
    await callback.answer("Tayyorlanmoqda...")
    try:
        data = _export_to_csv()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        doc = BufferedInputFile(file=data, filename=f"kitob_ijara_backup_{ts}.csv")
        await callback.message.answer_document(doc, caption="📤 Kitoblar va ijaralar (CSV)")
    except Exception as e:
        logger.exception("Export CSV failed: %s", e)
        await callback.message.answer(f"Xato: {e}")


async def cb_export_json(callback: CallbackQuery):
    """Send JSON backup."""
    await callback.answer("Tayyorlanmoqda...")
    try:
        data = _export_to_json()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
        doc = BufferedInputFile(file=data, filename=f"kitob_ijara_backup_{ts}.json")
        await callback.message.answer_document(doc, caption="📤 Kitoblar va ijaralar (JSON)")
    except Exception as e:
        logger.exception("Export JSON failed: %s", e)
        await callback.message.answer(f"Xato: {e}")


async def admin_broadcast_msg(message: Message, state: FSMContext):
    """Handle '📢 E'lon' — prompt for broadcast message."""
    await state.clear()
    user_count = len(db.get_broadcast_user_ids(exclude_admin_ids=ADMIN_IDS))
    await message.answer(
        f"📢 <b>E'lon (Broadcast)</b>\n\n"
        f"Hozircha {user_count} ta userga yuborish mumkin (ijarada bo'lganlar).\n\n"
        f"<b>Cheklov:</b> {BROADCAST_MAX_USERS} ta userdan ortiq bo'lsa, faqat birinchi {BROADCAST_MAX_USERS} tasiga yuboriladi.\n\n"
        "E'lon matnini yuboring (yoki <b>Bekor</b> yozing):",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(AdminBroadcastStates.message)


async def admin_broadcast_message(message: Message, state: FSMContext):
    """Process broadcast message, show preview with Yuborish / Bekor qilish."""
    txt = (message.text or "").strip()
    if txt.lower() in ("bekor", "cancel", "/cancel"):
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=admin_menu_keyboard())
        return
    if not txt:
        await message.answer("Matn bo'sh bo'lmasligi kerak. Qayta kiriting yoki Bekor yozing.")
        return
    user_ids = db.get_broadcast_user_ids(exclude_admin_ids=ADMIN_IDS)
    total = len(user_ids)
    if total > BROADCAST_MAX_USERS:
        user_ids = user_ids[:BROADCAST_MAX_USERS]
        total = BROADCAST_MAX_USERS
    await state.update_data(broadcast_text=txt, broadcast_user_count=total)
    await state.set_state(AdminBroadcastStates.confirm)
    preview = txt[:400] + ("..." if len(txt) > 400 else "")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yuborish", callback_data="broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="broadcast_cancel")],
    ])
    await message.answer(
        f"📢 <b>Preview</b> — {total} ta userga yuboriladi:\n\n{html.escape(preview)}\n\n"
        "Tasdiqlaysizmi?",
        reply_markup=kb,
        parse_mode=ParseMode.HTML,
    )


async def cb_broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    """Send broadcast with cheklov (batch + delay)."""
    data = await state.get_data()
    txt = data.get("broadcast_text", "")
    total = data.get("broadcast_user_count", 0)
    if not txt or not total:
        await state.clear()
        await callback.answer("Sessiya tugadi.", show_alert=True)
        return
    user_ids = db.get_broadcast_user_ids(exclude_admin_ids=ADMIN_IDS)[:BROADCAST_MAX_USERS]
    await callback.message.edit_text("📢 Yuborilmoqda...")
    sent = 0
    failed = 0
    for i in range(0, len(user_ids), BROADCAST_BATCH_SIZE):
        batch = user_ids[i : i + BROADCAST_BATCH_SIZE]
        for uid in batch:
            try:
                await callback.bot.send_message(uid, txt, parse_mode=None)
                sent += 1
            except Exception as e:
                failed += 1
                logger.warning("Broadcast failed user_id=%s: %s", uid, e)
        if i + BROADCAST_BATCH_SIZE < len(user_ids):
            await asyncio.sleep(BROADCAST_DELAY_SEC)
    await state.clear()
    await callback.message.edit_text(
        f"✅ E'lon yuborildi.\n\nYuborilgan: {sent} ta\nXato: {failed} ta",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
        ]),
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()
    logger.info("Broadcast done: admin_id=%s sent=%s failed=%s", callback.from_user.id if callback.from_user else 0, sent, failed)


async def cb_broadcast_cancel(callback: CallbackQuery, state: FSMContext):
    """Cancel broadcast."""
    await state.clear()
    await callback.message.edit_text("❌ E'lon bekor qilindi.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
    ]))
    await callback.answer()


async def admin_penalty_msg(message: Message, state: FSMContext):
    """Handle '💰 Jarima' — show current penalty and prompt to change."""
    await state.clear()
    current = db.get_penalty_per_day()
    await message.answer(
        f"💰 <b>Jarima</b> (kechikkan kuniga)\n\n"
        f"Hozirgi: {current} so'm/kun\n\n"
        "Yangi summani kiriting (0 = jarimasiz):",
        parse_mode=ParseMode.HTML,
    )
    await state.set_state(AdminPenaltyStates.amount)


async def admin_penalty_amount(message: Message, state: FSMContext):
    """Process new penalty amount from admin."""
    txt = (message.text or "").strip().lower()
    if txt in ("bekor", "cancel", "/cancel"):
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=admin_menu_keyboard())
        return
    try:
        amount = int(txt)
        if amount < 0:
            raise ValueError("Manfiy bo'lmasligi kerak")
    except ValueError:
        await message.answer("Iltimos, 0 yoki undan katta butun son kiriting (masalan: 2000). Yoki 'Bekor' yozing.")
        return
    db.set_penalty_per_day(amount)
    await state.clear()
    await message.answer(
        f"✅ Jarima yangilandi: {amount} so'm/kun" if amount > 0 else "✅ Jarima o'chirildi (0 so'm/kun).",
        reply_markup=admin_menu_keyboard(),
    )


def _format_penalty_edit_text(rental: dict) -> str:
    """Build penalty edit menu text with current values."""
    rid = rental.get("id") or rental.get("rental_id")
    enabled = rental.get("penalty_enabled", 1) != 0
    per_day = rental.get("penalty_per_day") or 0
    fixed = rental.get("penalty_fixed")
    note = (rental.get("penalty_note") or "").strip() or "—"
    r_with_due = {**rental, "due_ts": rental.get("due_ts") or rental.get("due_date")}
    computed = db.compute_penalty(r_with_due, datetime.now(timezone.utc))
    return (
        f"💸 <b>Jarima</b> — Ijara #{rid}\n\n"
        f"📕 {rental.get('book_title', '?')}\n"
        f"Jarima yoqilgan: {'Ha' if enabled else 'Yo\'q'}\n"
        f"Kunlik: {per_day} so'm\n"
        f"Fiks: {fixed if fixed is not None else '—'}\n"
        f"Izoh: {note}\n\n"
        f"Hisoblangan jarima: {computed} so'm"
    )


async def admin_overdue_msg(message: Message):
    """Handle '⏰ Kechikkanlar' text button."""
    logger.info("Admin overdue list opened: user_id=%s", message.from_user.id if message.from_user else "?")
    now_iso = datetime.now(timezone.utc).isoformat()
    total = db.count_overdue_rentals(now_iso)
    if total == 0:
        await message.answer("✅ Hozircha kechikkan ijaralar yo'q.", reply_markup=admin_menu_keyboard())
        return
    total_pages = max(1, (total + PAGE_SIZE_OVERDUE - 1) // PAGE_SIZE_OVERDUE)
    overdue_list = db.list_overdue_rentals(now_iso, offset=0, limit=PAGE_SIZE_OVERDUE)
    text = _format_overdue_list(overdue_list, 1, total_pages, total)
    await message.answer(text, reply_markup=admin_overdue_keyboard(overdue_list, 1, total_pages), parse_mode=ParseMode.HTML)


async def admin_overdue_page(callback: CallbackQuery):
    """Pagination for overdue list."""
    data = callback.data or ""
    if not data.startswith("overdue_p_"):
        await callback.answer()
        return
    try:
        page = int(data.replace("overdue_p_", ""))
    except ValueError:
        page = 1
    logger.info("Admin overdue page: admin_id=%s page=%s", callback.from_user.id if callback.from_user else "?", page)
    now_iso = datetime.now(timezone.utc).isoformat()
    total = db.count_overdue_rentals(now_iso)
    total_pages = max(1, (total + PAGE_SIZE_OVERDUE - 1) // PAGE_SIZE_OVERDUE)
    overdue_list = db.list_overdue_rentals(now_iso, offset=(page - 1) * PAGE_SIZE_OVERDUE, limit=PAGE_SIZE_OVERDUE)
    if not overdue_list:
        await callback.answer("Sahifa bo'sh.")
        return
    text = _format_overdue_list(overdue_list, page, total_pages, total)
    await callback.message.edit_text(text, reply_markup=admin_overdue_keyboard(overdue_list, page, total_pages), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_overdue_ping(callback: CallbackQuery):
    """Send reminder to user for overdue rental."""
    data = callback.data or ""
    if not data.startswith("overdue_ping_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("overdue_ping_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    if rental.get("status") not in ("approved", "active"):
        await callback.answer("Bu ijara allaqachon yopilgan.", show_alert=True)
        return
    admin_id = callback.from_user.id if callback.from_user else 0
    logger.info("Overdue ping: admin_id=%s rental_id=%s user_id=%s book_id=%s", admin_id, rental_id, rental.get("user_id"), rental.get("book_id"))
    computed = db.compute_penalty(rental, datetime.now(timezone.utc))
    due_str = rental.get("due_ts") or ""
    overdue_days = 1
    if due_str:
        try:
            now = datetime.now(timezone.utc)
            due_date = datetime.fromisoformat(due_str[:10] + "T00:00:00+00:00")
            overdue_days = max(1, int((now - due_date).total_seconds() / 86400))
        except Exception:
            pass
    penalty_line = f"\n💰 Jarima: {computed} so'm" if computed > 0 else ""
    try:
        await callback.bot.send_message(
            rental["user_id"],
            f"⏰ <b>Eslatma:</b> Kitob qaytarish muddati o'tdi.\n\n"
            f"📖 Kitob: {rental.get('book_title')}\n"
            f"📅 Qaytarish sanasi: {rental.get('due_ts')}\n"
            f"⏳ Kechikdi: {overdue_days} kun{penalty_line}\n\n"
            "Iltimos, kitobni qaytarishni unutmang.",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer("✉️ Eslatma yuborildi.", show_alert=False)
    except Exception as e:
        logger.warning("Overdue ping failed: %s", e)
        await callback.answer("Foydalanuvchiga yuborish mumkin emas.", show_alert=True)


async def cb_penalty_edit(callback: CallbackQuery, state: FSMContext):
    """Open penalty edit menu for rental."""
    data = callback.data or ""
    if not data.startswith("penalty_edit_"):
        await callback.answer()
        return
    parts = data.replace("penalty_edit_", "").split("_")
    try:
        rental_id = int(parts[0])
        from_page = int(parts[1]) if len(parts) > 1 else 1
    except (ValueError, IndexError):
        await callback.answer("Xatolik.")
        return
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    await state.update_data(penalty_rental_id=rental_id, penalty_from_page=from_page)
    await state.set_state(AdminPenaltyEditStates.choose_action)
    text = _format_penalty_edit_text(rental)
    await callback.message.edit_text(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_penalty_toggle(callback: CallbackQuery, state: FSMContext):
    """Toggle penalty_enabled."""
    data = callback.data or ""
    if not data.startswith("penalty_toggle_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("penalty_toggle_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    admin_id = callback.from_user.id if callback.from_user else 0
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    new_val = 0 if rental.get("penalty_enabled", 1) != 0 else 1
    db.update_rental_penalty(rental_id, admin_id, penalty_enabled=new_val)
    logger.info("Penalty toggle: admin_id=%s rental_id=%s enabled=%s", admin_id, rental_id, new_val)
    rental = db.get_rental(rental_id)
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    text = _format_penalty_edit_text(rental)
    await callback.message.edit_text(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await callback.answer("✅ Yangilandi")


async def cb_penalty_perday(callback: CallbackQuery, state: FSMContext):
    """Prompt for penalty_per_day."""
    data = callback.data or ""
    if not data.startswith("penalty_perday_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("penalty_perday_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    await state.update_data(penalty_edit_field="per_day", penalty_rental_id=rental_id, penalty_from_page=from_page)
    await state.set_state(AdminPenaltyEditStates.per_day)
    await callback.message.answer("Kunlik jarima (so'm) kiriting (0 yoki undan katta butun son):")
    await callback.answer()


async def cb_penalty_fixed(callback: CallbackQuery, state: FSMContext):
    """Prompt for penalty_fixed."""
    data = callback.data or ""
    if not data.startswith("penalty_fixed_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("penalty_fixed_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    await state.update_data(penalty_edit_field="fixed", penalty_rental_id=rental_id, penalty_from_page=from_page)
    await state.set_state(AdminPenaltyEditStates.fixed)
    await callback.message.answer("Fiks jarima (so'm) kiriting (0 yoki undan katta butun son):")
    await callback.answer()


async def cb_penalty_clear_fixed(callback: CallbackQuery, state: FSMContext):
    """Clear penalty_fixed."""
    data = callback.data or ""
    if not data.startswith("penalty_clear_fixed_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("penalty_clear_fixed_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    admin_id = callback.from_user.id if callback.from_user else 0
    db.update_rental_penalty(rental_id, admin_id, clear_penalty_fixed=True)
    logger.info("Penalty clear fixed: admin_id=%s rental_id=%s", admin_id, rental_id)
    rental = db.get_rental(rental_id)
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    text = _format_penalty_edit_text(rental)
    await callback.message.edit_text(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await callback.answer("✅ Fiks o'chirildi")


async def cb_penalty_note(callback: CallbackQuery, state: FSMContext):
    """Prompt for penalty_note."""
    data = callback.data or ""
    if not data.startswith("penalty_note_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("penalty_note_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    sdata = await state.get_data()
    from_page = sdata.get("penalty_from_page", 1)
    await state.update_data(penalty_edit_field="note", penalty_rental_id=rental_id, penalty_from_page=from_page)
    await state.set_state(AdminPenaltyEditStates.note)
    await callback.message.answer("Izoh yozing (yoki 'Bekor' yozing):")
    await callback.answer()


async def cb_penalty_back(callback: CallbackQuery, state: FSMContext):
    """Back to overdue list."""
    data = callback.data or ""
    if not data.startswith("penalty_back_"):
        await callback.answer()
        return
    try:
        from_page = int(data.replace("penalty_back_", ""))
    except ValueError:
        from_page = 1
    await state.clear()
    now_iso = datetime.now(timezone.utc).isoformat()
    total = db.count_overdue_rentals(now_iso)
    if total == 0:
        await callback.message.edit_text("✅ Hozircha kechikkan ijaralar yo'q.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
        ]))
    else:
        total_pages = max(1, (total + PAGE_SIZE_OVERDUE - 1) // PAGE_SIZE_OVERDUE)
        overdue_list = db.list_overdue_rentals(now_iso, offset=(from_page - 1) * PAGE_SIZE_OVERDUE, limit=PAGE_SIZE_OVERDUE)
        text = _format_overdue_list(overdue_list, from_page, total_pages, total)
        await callback.message.edit_text(text, reply_markup=admin_overdue_keyboard(overdue_list, from_page, total_pages), parse_mode=ParseMode.HTML)
    await callback.answer()


async def penalty_edit_per_day(message: Message, state: FSMContext):
    """Process penalty_per_day input."""
    data = await state.get_data()
    rental_id = data.get("penalty_rental_id")
    if not rental_id:
        await state.clear()
        await message.answer("Sessiya tugadi.")
        return
    try:
        val = int((message.text or "").strip())
        if val < 0:
            raise ValueError("Manfiy bo'lmasligi kerak")
    except ValueError:
        await message.answer("0 yoki undan katta butun son kiriting.")
        return
    admin_id = message.from_user.id if message.from_user else 0
    db.update_rental_penalty(rental_id, admin_id, penalty_per_day=val)
    logger.info("Penalty per_day: admin_id=%s rental_id=%s val=%s", admin_id, rental_id, val)
    rental = db.get_rental(rental_id)
    from_page = data.get("penalty_from_page", 1)
    text = _format_penalty_edit_text(rental)
    await message.answer(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await state.set_state(AdminPenaltyEditStates.choose_action)
    await state.update_data(penalty_rental_id=rental_id, penalty_from_page=from_page)


async def penalty_edit_fixed(message: Message, state: FSMContext):
    """Process penalty_fixed input."""
    data = await state.get_data()
    rental_id = data.get("penalty_rental_id")
    if not rental_id:
        await state.clear()
        await message.answer("Sessiya tugadi.")
        return
    try:
        val = int((message.text or "").strip())
        if val < 0:
            raise ValueError("Manfiy bo'lmasligi kerak")
    except ValueError:
        await message.answer("0 yoki undan katta butun son kiriting.")
        return
    admin_id = message.from_user.id if message.from_user else 0
    db.update_rental_penalty(rental_id, admin_id, penalty_fixed=val)
    logger.info("Penalty fixed: admin_id=%s rental_id=%s val=%s", admin_id, rental_id, val)
    rental = db.get_rental(rental_id)
    from_page = data.get("penalty_from_page", 1)
    text = _format_penalty_edit_text(rental)
    await message.answer(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await state.set_state(AdminPenaltyEditStates.choose_action)
    await state.update_data(penalty_rental_id=rental_id, penalty_from_page=from_page)


async def penalty_edit_note(message: Message, state: FSMContext):
    """Process penalty_note input."""
    data = await state.get_data()
    rental_id = data.get("penalty_rental_id")
    if not rental_id:
        await state.clear()
        await message.answer("Sessiya tugadi.")
        return
    txt = (message.text or "").strip()
    if txt.lower() == "bekor":
        rental = db.get_rental(rental_id)
        from_page = data.get("penalty_from_page", 1)
        text = _format_penalty_edit_text(rental)
        await message.answer(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
        await state.set_state(AdminPenaltyEditStates.choose_action)
        await state.update_data(penalty_rental_id=rental_id, penalty_from_page=from_page)
        return
    admin_id = message.from_user.id if message.from_user else 0
    db.update_rental_penalty(rental_id, admin_id, penalty_note=txt)
    logger.info("Penalty note: admin_id=%s rental_id=%s", admin_id, rental_id)
    rental = db.get_rental(rental_id)
    from_page = data.get("penalty_from_page", 1)
    text = _format_penalty_edit_text(rental)
    await message.answer(text, reply_markup=admin_penalty_edit_keyboard(rental_id, from_page), parse_mode=ParseMode.HTML)
    await state.set_state(AdminPenaltyEditStates.choose_action)
    await state.update_data(penalty_rental_id=rental_id, penalty_from_page=from_page)


async def cmd_admin_rentals_msg(message: Message):
    """Handle '📦 Ijaralar' text button."""
    rentals = db.list_rentals_pending_admin()
    text = _admin_rentals_text(rentals)
    kb = admin_rentals_keyboard(rentals) if rentals else InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_back")],
    ])
    await message.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def cmd_wipe_all(message: Message):
    """Admin: wipe all books and rentals. Asks confirmation."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, hammasini o'chir", callback_data="wipe_confirm")],
        [InlineKeyboardButton(text="❌ Bekor", callback_data="wipe_cancel")],
    ])
    await message.answer("⚠️ Barcha kitoblar va ijaralar o'chiriladi. Tasdiqlaysizmi?", reply_markup=kb)


async def cmd_set_order(message: Message):
    """Admin: /set_order <book_id> <number> — set sort_order for manual ordering."""
    parts = (message.text or "").strip().split()
    if len(parts) < 3:
        await message.answer("Format: /set_order <book_id> <raqam>\nMisol: /set_order 5 10")
        return
    try:
        book_id = int(parts[1])
        sort_order = int(parts[2])
    except ValueError:
        await message.answer("book_id va raqam butun son bo'lishi kerak.")
        return
    if db.set_book_sort_order(book_id, sort_order):
        await message.answer(f"✅ Kitob ID {book_id} uchun sort_order = {sort_order} o'rnatildi.")
    else:
        await message.answer("Kitob topilmadi.")


async def cb_admin_wipe(callback: CallbackQuery):
    """Handle 'admin_wipe' from inline menu."""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, hammasini o'chir", callback_data="wipe_confirm")],
        [InlineKeyboardButton(text="❌ Bekor", callback_data="wipe_cancel")],
    ])
    try:
        await callback.message.edit_text("⚠️ Barcha kitoblar va ijaralar o'chiriladi. Tasdiqlaysizmi?", reply_markup=kb)
    except Exception:
        await callback.message.answer("⚠️ Barcha kitoblar va ijaralar o'chiriladi. Tasdiqlaysizmi?", reply_markup=kb)
    await callback.answer()


async def cb_wipe_confirm(callback: CallbackQuery):
    db.wipe_all()
    await callback.message.edit_text("🧹 Barcha kitoblar va ijaralar tozalandi.")
    await callback.answer()


async def cb_wipe_cancel(callback: CallbackQuery):
    await callback.message.edit_text("Bekor qilindi.")
    await callback.answer()


async def cb_admin_add_book(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Kitob nomini kiriting:")
    await state.set_state(AddBookStates.title)
    await callback.answer()


async def add_book_title(message: Message, state: FSMContext):
    await state.update_data(title=(message.text or "").strip())
    await state.set_state(AddBookStates.author)
    await message.answer("Muallifni kiriting:")


async def add_book_author(message: Message, state: FSMContext):
    await state.update_data(author=(message.text or "").strip())
    await state.set_state(AddBookStates.category)
    cats = db.get_categories_for_add()
    admin_id = message.from_user.id if message.from_user else 0
    last_cat = (_add_book_last.get(admin_id) or {}).get("category")
    if last_cat and last_cat in cats:
        cats = [last_cat] + [c for c in cats if c != last_cat]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"add_cat_{c}")] for c in cats
    ])
    await message.answer("Kategoriyani tanlang:", reply_markup=kb)


async def add_book_category_sel(callback: CallbackQuery, state: FSMContext):
    data = callback.data or ""
    if not data.startswith("add_cat_"):
        await callback.answer()
        return
    cat = data.replace("add_cat_", "")
    admin_id = callback.from_user.id if callback.from_user else 0
    if admin_id not in _add_book_last:
        _add_book_last[admin_id] = {}
    if cat != "Boshqa":
        _add_book_last[admin_id]["category"] = cat
    if cat == "Boshqa":
        await state.set_state(AddBookStates.category_other)
        await callback.message.answer("Kategoriya nomini yozing:")
    else:
        await state.update_data(category=cat)
        await state.set_state(AddBookStates.year)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="add_year_skip")],
        ])
        await callback.message.answer("Yil (ixtiyoriy):", reply_markup=kb)
    await callback.answer()


async def add_book_category_other(message: Message, state: FSMContext):
    cat = (message.text or "").strip()
    await state.update_data(category=cat)
    admin_id = message.from_user.id if message.from_user else 0
    if admin_id not in _add_book_last:
        _add_book_last[admin_id] = {}
    _add_book_last[admin_id]["category"] = cat
    await state.set_state(AddBookStates.year)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="add_year_skip")],
    ])
    await message.answer("Yil (ixtiyoriy):", reply_markup=kb)


def _add_book_cover_keyboard(admin_id: int) -> InlineKeyboardMarkup:
    """Cover type keyboard with last used first."""
    last = (_add_book_last.get(admin_id) or {}).get("cover_type")
    if last == "qattiq":
        rows = [[InlineKeyboardButton(text="Qattiq ✓", callback_data="cover_qattiq"), InlineKeyboardButton(text="Yumshoq", callback_data="cover_yumshoq")]]
    elif last == "yumshoq":
        rows = [[InlineKeyboardButton(text="Qattiq", callback_data="cover_qattiq"), InlineKeyboardButton(text="Yumshoq ✓", callback_data="cover_yumshoq")]]
    else:
        rows = [[InlineKeyboardButton(text="Qattiq", callback_data="cover_qattiq"), InlineKeyboardButton(text="Yumshoq", callback_data="cover_yumshoq")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def add_book_year_skip(callback: CallbackQuery, state: FSMContext):
    await state.update_data(year=0)
    await state.set_state(AddBookStates.cover_type)
    admin_id = callback.from_user.id if callback.from_user else 0
    await callback.message.answer("Muqova turi:", reply_markup=_add_book_cover_keyboard(admin_id))
    await callback.answer()


async def add_book_year(message: Message, state: FSMContext):
    txt = (message.text or "").strip()
    if txt == "-" or not txt:
        await state.update_data(year=0)
    else:
        try:
            await state.update_data(year=int(txt))
        except ValueError:
            await message.answer("Butun son kiriting yoki - yozing.")
            return
    await state.set_state(AddBookStates.cover_type)
    admin_id = message.from_user.id if message.from_user else 0
    await message.answer("Muqova turi:", reply_markup=_add_book_cover_keyboard(admin_id))


async def add_book_cover_type(callback: CallbackQuery, state: FSMContext):
    data = callback.data or ""
    cov = "qattiq" if data == "cover_qattiq" else "yumshoq"
    await state.update_data(cover_type=cov)
    admin_id = callback.from_user.id if callback.from_user else 0
    if admin_id not in _add_book_last:
        _add_book_last[admin_id] = {}
    _add_book_last[admin_id]["cover_type"] = cov
    await state.set_state(AddBookStates.qty)
    await callback.message.answer("Soni (1 dan katta):")
    await callback.answer()


async def add_book_qty(message: Message, state: FSMContext):
    try:
        qty = int((message.text or "").strip())
        if qty < 1:
            raise ValueError("Soni 1 dan kam bo'lmasligi kerak")
    except ValueError:
        await message.answer("1 dan katta butun son kiriting.")
        return
    await state.update_data(qty=qty)
    await state.set_state(AddBookStates.rent_fee)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="10 000", callback_data="add_rent_10000"),
            InlineKeyboardButton(text="15 000", callback_data="add_rent_15000"),
            InlineKeyboardButton(text="20 000", callback_data="add_rent_20000"),
        ],
    ])
    await message.answer("Ijara narxi (so'm/kun) — tez tugmalar yoki matn kiriting:", reply_markup=kb)


async def add_book_rent_fee(message: Message, state: FSMContext):
    try:
        fee = int((message.text or "").strip())
        if fee <= 0:
            raise ValueError("Ijara narxi 0 dan katta bo'lishi kerak")
    except ValueError:
        await message.answer("Iltimos, 0 dan katta butun son kiriting (masalan: 5000).")
        return
    await state.update_data(rent_fee=fee)
    await state.set_state(AddBookStates.deposit)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ O'tkazib yuborish (0)", callback_data="add_deposit_skip")],
    ])
    await message.answer("Depozit (so'm, ixtiyoriy):", reply_markup=kb)


async def add_book_rent_fee_quick(callback: CallbackQuery, state: FSMContext):
    """Handle quick rent_fee buttons: 10 000 / 15 000 / 20 000."""
    data = callback.data or ""
    if not data.startswith("add_rent_"):
        await callback.answer()
        return
    try:
        fee = int(data.replace("add_rent_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    if fee <= 0:
        await callback.answer("Xatolik.")
        return
    await state.update_data(rent_fee=fee)
    await state.set_state(AddBookStates.deposit)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ O'tkazib yuborish (0)", callback_data="add_deposit_skip")],
    ])
    await callback.message.edit_text(f"Ijara narxi: {fee:,} so'm/kun ✓")
    await callback.message.answer("Depozit (so'm, ixtiyoriy):", reply_markup=kb)
    await callback.answer()


async def add_book_deposit_skip(callback: CallbackQuery, state: FSMContext):
    await state.update_data(deposit=0)
    await state.set_state(AddBookStates.photo)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="add_book_photo_skip")],
    ])
    await callback.message.answer(
        "📸 Iltimos, kitob rasmini yuboring (yoki 'O'tkazib yuborish' tugmasini bosing)",
        reply_markup=kb,
    )
    await callback.answer()


async def add_book_deposit(message: Message, state: FSMContext):
    try:
        dep = int((message.text or "").strip()) if (message.text or "").strip() else 0
        if dep < 0:
            raise ValueError("Manfiy bo'lmasligi kerak")
    except ValueError:
        await message.answer("Butun son kiriting (0 yoki bo'sh qoldirish mumkin).")
        return
    await state.update_data(deposit=dep)
    await state.set_state(AddBookStates.photo)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data="add_book_photo_skip")],
    ])
    await message.answer(
        "📸 Iltimos, kitob rasmini yuboring (yoki 'O'tkazib yuborish' tugmasini bosing)",
        reply_markup=kb,
    )


def _add_book_preview_text(data: dict) -> str:
    year_t = f", {data.get('year') or 0}" if data.get("year") else ""
    photo_t = "Ha" if data.get("photo_id") else "Yo'q"
    return (
        f"📘 <b>{html.escape(data.get('title', ''))}</b>\n"
        f"Muallif: {html.escape(data.get('author', ''))}\n"
        f"Kategoriya: {html.escape(data.get('category', ''))}{year_t}\n"
        f"Muqova: {data.get('cover_type', 'yumshoq')}\n"
        f"Rasm: {photo_t}\n"
        f"Soni: {data.get('qty', 1)}\n"
        f"Ijara narxi: {data.get('rent_fee')} so'm/kun\n"
        f"Depozit: {data.get('deposit', 0)} so'm\n\n"
        "Saqlash mumkinmi?"
    )


async def _send_add_book_preview(target: Message, data: dict):
    text = _add_book_preview_text(data)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Saqlash", callback_data="add_book_save")],
        [InlineKeyboardButton(text="❌ Bekor", callback_data="add_book_cancel")],
    ])
    photo_id = data.get("photo_id")
    if photo_id:
        await target.answer_photo(photo=photo_id, caption=text, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await target.answer(text, reply_markup=kb, parse_mode=ParseMode.HTML)


async def add_book_photo(message: Message, state: FSMContext):
    """Accept photo, save largest file_id, go to preview."""
    if not message.photo:
        await message.answer("Iltimos, rasm yuboring yoki 'O'tkazib yuborish'ni bosing.")
        return
    photos = message.photo
    largest = max(photos, key=lambda p: (p.width or 0) * (p.height or 0))
    await state.update_data(photo_id=largest.file_id)
    await state.set_state(AddBookStates.preview)
    data = await state.get_data()
    await _send_add_book_preview(message, data)


async def add_book_photo_skip(callback: CallbackQuery, state: FSMContext):
    await state.update_data(photo_id=None)
    await state.set_state(AddBookStates.preview)
    data = await state.get_data()
    await _send_add_book_preview(callback.message, data)
    await callback.answer()


async def add_book_photo_reject(message: Message, state: FSMContext):
    """User sent non-photo in photo state."""
    await message.answer("Iltimos, rasm yuboring yoki 'O'tkazib yuborish' tugmasini bosing.")


async def add_book_save(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    book_id = db.add_book(
        title=data["title"],
        author=data["author"],
        category=data["category"],
        rent_fee=data["rent_fee"],
        deposit=data.get("deposit", 0),
        qty=data.get("qty", 1),
        year=data.get("year", 0),
        publisher="",
        cover_type=data.get("cover_type", "yumshoq"),
        photo_id=data.get("photo_id"),
    )
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer(f"✅ Kitob qo'shildi. ID: {book_id}")
    await callback.message.answer("Boshqa amallar:", reply_markup=admin_menu_keyboard())
    await callback.answer()


async def add_book_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("Bekor qilindi.")
    await callback.message.answer("Admin menyu:", reply_markup=admin_menu_keyboard())
    await callback.answer()


async def cb_admin_books(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    admin_id = callback.from_user.id if callback.from_user else 0
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_books_page(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("admin_books_p_"):
        await callback.answer()
        return
    try:
        page = int(data.replace("admin_books_p_", ""))
    except ValueError:
        page = 1
    admin_id = callback.from_user.id if callback.from_user else 0
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=page)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/{total_pages}\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, page, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_books_filter_search(callback: CallbackQuery, state: FSMContext):
    """Prompt for search query."""
    await state.update_data(admin_books_filter_return_page=1)
    await state.set_state(AdminBooksFilterStates.search_query)
    await callback.message.answer("Muallif yoki kitob nomini kiriting (qidiruv matni):")
    await callback.answer()


async def cb_admin_books_filter_cat(callback: CallbackQuery):
    """Show category picker."""
    cats = db.get_categories()
    rows = []
    for c in cats:
        rows.append([InlineKeyboardButton(text=c, callback_data=f"admin_books_cat_{c}")])
    rows.append([InlineKeyboardButton(text="Hammasi", callback_data="admin_books_cat_Hammasi")])
    rows.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="admin_books")])
    await callback.message.edit_text("Kategoriyani tanlang:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()


async def cb_admin_books_filter_cat_sel(callback: CallbackQuery):
    """Handle category selection."""
    data = callback.data or ""
    if not data.startswith("admin_books_cat_"):
        await callback.answer()
        return
    cat = data.replace("admin_books_cat_", "")
    admin_id = callback.from_user.id if callback.from_user else 0
    if admin_id not in _admin_books_filter:
        _admin_books_filter[admin_id] = _DEFAULT_ADMIN_FILTER.copy()
    _admin_books_filter[admin_id]["category"] = None if cat == "Hammasi" else cat
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_books_filter_oos(callback: CallbackQuery):
    """Toggle only_out_of_stock."""
    admin_id = callback.from_user.id if callback.from_user else 0
    if admin_id not in _admin_books_filter:
        _admin_books_filter[admin_id] = _DEFAULT_ADMIN_FILTER.copy()
    _admin_books_filter[admin_id]["only_out_of_stock"] = not _admin_books_filter[admin_id].get("only_out_of_stock", False)
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_admin_books_filter_clear(callback: CallbackQuery):
    """Reset all filters."""
    admin_id = callback.from_user.id if callback.from_user else 0
    _admin_books_filter[admin_id] = _DEFAULT_ADMIN_FILTER.copy()
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    await callback.answer()


async def admin_books_search_query(message: Message, state: FSMContext):
    """Process search query from FSM."""
    txt = (message.text or "").strip()
    if txt.lower() in ("bekor", "cancel", "/cancel"):
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=admin_menu_keyboard())
        return
    admin_id = message.from_user.id if message.from_user else 0
    if admin_id not in _admin_books_filter:
        _admin_books_filter[admin_id] = _DEFAULT_ADMIN_FILTER.copy()
    _admin_books_filter[admin_id]["q"] = txt.lower() if txt else ""
    await state.clear()
    text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
    if not books:
        text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
    await message.answer(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)


async def cb_admin_del_book(callback: CallbackQuery):
    """Show delete confirmation for admin_del_{id}."""
    data = callback.data or ""
    if not data.startswith("admin_del_"):
        await callback.answer()
        return
    try:
        book_id = int(data.replace("admin_del_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    book = db.get_book(book_id)
    if not book:
        await callback.answer("Kitob topilmadi.", show_alert=True)
        return
    title = (book.get("title") or "?")[:50]
    await callback.message.edit_text(
        f"🗑 <b>{html.escape(title)}</b> — o'chirishni tasdiqlaysizmi?",
        reply_markup=admin_del_confirm_keyboard(book_id),
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


async def cb_admin_del_confirm(callback: CallbackQuery):
    """actually delete book on admin_del_confirm_{id}."""
    data = callback.data or ""
    if not data.startswith("admin_del_confirm_"):
        await callback.answer()
        return
    try:
        book_id = int(data.replace("admin_del_confirm_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    if db.has_active_rentals(book_id):
        await callback.answer("❌ Bu kitob hozir ijarada, o'chirib bo'lmaydi.", show_alert=True)
        return
    if db.delete_book(book_id):
        await callback.answer("Kitob o'chirildi.", show_alert=True)
        admin_id = callback.from_user.id if callback.from_user else 0
        text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
        if not books:
            text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
        try:
            await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
        except Exception:
            await callback.message.answer(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
    else:
        await callback.answer("Topilmadi yoki o'chirilgan.", show_alert=True)


def _edit_book_back_text(book: dict) -> str:
    return f"✏️ <b>{html.escape(book.get('title', '?'))}</b> — tahrirlash"

async def cb_admin_edit(callback: CallbackQuery, state: FSMContext):
    """Show edit menu for admin_edit_{id}."""
    data = callback.data or ""
    if not data.startswith("admin_edit_"):
        await callback.answer()
        return
    try:
        book_id = int(data.replace("admin_edit_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    book = db.get_book(book_id)
    if not book:
        await callback.answer("Kitob topilmadi.", show_alert=True)
        return
    await state.update_data(edit_book_id=book_id)
    await state.set_state(EditBookStates.choose_field)
    text = _edit_book_back_text(book)
    await callback.message.edit_text(text, reply_markup=admin_edit_keyboard(book_id), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_edit_field(callback: CallbackQuery, state: FSMContext):
    """Handle edit_field_* callbacks."""
    data = callback.data or ""
    if not data.startswith("edit_field_"):
        await callback.answer()
        return
    rest = data.replace("edit_field_", "")
    # edit_field_title_5, edit_field_rent_5, edit_field_qty_5, edit_field_photo_5, edit_field_remove_5
    if rest.startswith("remove_"):
        field, book_id_s = "remove", rest.replace("remove_", "")
    elif rest.startswith("title_"):
        field, book_id_s = "title", rest.replace("title_", "")
    elif rest.startswith("rent_"):
        field, book_id_s = "rent", rest.replace("rent_", "")
    elif rest.startswith("qty_"):
        field, book_id_s = "qty", rest.replace("qty_", "")
    elif rest.startswith("photo_"):
        field, book_id_s = "photo", rest.replace("photo_", "")
    else:
        await callback.answer("Xatolik.")
        return
    try:
        book_id = int(book_id_s)
    except ValueError:
        await callback.answer("Xatolik.")
        return
    book = db.get_book(book_id)
    if not book:
        await callback.answer("Kitob topilmadi.", show_alert=True)
        return
    await state.update_data(edit_book_id=book_id)
    if field == "title":
        await state.set_state(EditBookStates.title)
        await callback.message.answer(f"Yangi nom kiriting (hozirgi: {html.escape(book.get('title', ''))}):")
    elif field == "rent":
        await state.set_state(EditBookStates.rent_fee)
        await callback.message.answer(f"Ijara narxi (so'm/kun) kiriting (hozirgi: {book.get('rent_fee')}):")
    elif field == "qty":
        await state.set_state(EditBookStates.qty)
        await callback.message.answer(f"Soni kiriting (hozirgi: {book.get('qty')}):")
    elif field == "photo":
        await state.set_state(EditBookStates.photo)
        await callback.message.answer("📸 Yangi rasm yuboring:")
    elif field == "remove":
        db.update_book(book_id, photo_id="")
        await state.clear()
        await callback.answer("Rasm o'chirildi.", show_alert=True)
        admin_id = callback.from_user.id if callback.from_user else 0
        text, books, total_pages, f = _build_admin_books_list(admin_id, page=1)
        if not books:
            text = f"📚 <b>Kitoblarim</b> — 0/1\n{_format_admin_books_filter_header(f)}\n\nKitoblar yo'q."
        try:
            await callback.message.edit_text(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
        except Exception:
            await callback.message.answer(text, reply_markup=admin_books_keyboard(books, 1, total_pages, filter_state=f), parse_mode=ParseMode.HTML)
        return
    await callback.answer()


async def edit_book_title(message: Message, state: FSMContext):
    data = await state.get_data()
    book_id = data.get("edit_book_id")
    if not book_id:
        await state.clear()
        await message.answer("Sessiya tugadi. /admin bosing.")
        return
    title = (message.text or "").strip()
    if not title:
        await message.answer("Nom bo'sh bo'lmasligi kerak.")
        return
    if db.update_book(book_id, title=title):
        await message.answer("✅ Nomi yangilandi.")
    book = db.get_book(book_id)
    await state.set_state(EditBookStates.choose_field)
    await message.answer(_edit_book_back_text(book or {}), reply_markup=admin_edit_keyboard(book_id), parse_mode=ParseMode.HTML)


async def edit_book_rent_fee(message: Message, state: FSMContext):
    data = await state.get_data()
    book_id = data.get("edit_book_id")
    if not book_id:
        await state.clear()
        await message.answer("Sessiya tugadi. /admin bosing.")
        return
    try:
        fee = int((message.text or "").strip())
        if fee <= 0:
            raise ValueError("0 dan katta bo'lishi kerak")
    except ValueError:
        await message.answer("0 dan katta butun son kiriting (masalan: 5000).")
        return
    if db.update_book(book_id, rent_fee=fee):
        await message.answer("✅ Ijara narxi yangilandi.")
    book = db.get_book(book_id)
    await state.set_state(EditBookStates.choose_field)
    await message.answer(_edit_book_back_text(book or {}), reply_markup=admin_edit_keyboard(book_id), parse_mode=ParseMode.HTML)


async def edit_book_qty(message: Message, state: FSMContext):
    data = await state.get_data()
    book_id = data.get("edit_book_id")
    if not book_id:
        await state.clear()
        await message.answer("Sessiya tugadi. /admin bosing.")
        return
    try:
        qty = int((message.text or "").strip())
        if qty < 1:
            raise ValueError("1 dan kam bo'lmasligi kerak")
    except ValueError:
        await message.answer("1 dan katta butun son kiriting.")
        return
    if db.update_book(book_id, qty=qty):
        await message.answer("✅ Soni yangilandi.")
    book = db.get_book(book_id)
    await state.set_state(EditBookStates.choose_field)
    await message.answer(_edit_book_back_text(book or {}), reply_markup=admin_edit_keyboard(book_id), parse_mode=ParseMode.HTML)


async def edit_book_photo(message: Message, state: FSMContext):
    data = await state.get_data()
    book_id = data.get("edit_book_id")
    if not book_id:
        await state.clear()
        await message.answer("Sessiya tugadi. /admin bosing.")
        return
    if not message.photo:
        await message.answer("Iltimos, rasm yuboring.")
        return
    photos = message.photo
    largest = max(photos, key=lambda p: (p.width or 0) * (p.height or 0))
    if db.update_book(book_id, photo_id=largest.file_id):
        await message.answer("✅ Rasm yangilandi.")
    book = db.get_book(book_id)
    await state.set_state(EditBookStates.choose_field)
    await message.answer(_edit_book_back_text(book or {}), reply_markup=admin_edit_keyboard(book_id), parse_mode=ParseMode.HTML)


async def edit_book_photo_reject(message: Message, state: FSMContext):
    """User sent non-photo in edit photo state."""
    await message.answer("Iltimos, rasm yuboring.")


async def cb_admin_rentals(callback: CallbackQuery):
    rentals = db.list_rentals_pending_admin()
    text = _admin_rentals_text(rentals)
    await callback.message.edit_text(text, reply_markup=admin_rentals_keyboard(rentals), parse_mode=ParseMode.HTML)
    await callback.answer()


async def cb_rental_ok(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("rental_ok_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("rental_ok_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    if rental.get("status") != "requested":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    # Regression test checklist:
    # - Two admins approving two pending rentals for the same book with qty=1 -> only one succeeds.
    admin_id = callback.from_user.id if callback.from_user else 0
    ok, reason = db.approve_rental_if_available(rental_id, admin_id)
    if not ok:
        if reason == "no_stock":
            # Mark as rejected so it doesn't stay pending.
            db.set_rental_status(rental_id, "rejected")
            try:
                await callback.bot.send_message(
                    rental["user_id"],
                    "❌ Kitob qolmadi, ijara tasdiqlanmadi.",
                )
            except Exception as e:
                logger.warning("User notify failed: %s", e)
            await callback.answer("❌ Nusxa qolmagan", show_alert=True)
        elif reason == "db_locked":
            await callback.answer("⏳ Band. Qayta urinib ko'ring.", show_alert=True)
        else:
            await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        rentals = db.list_rentals_pending_admin()
        await callback.message.edit_text(_admin_rentals_text(rentals), reply_markup=admin_rentals_keyboard(rentals), parse_mode=ParseMode.HTML)
        return

    # Re-fetch for freshest status/fields
    rental = db.get_rental(rental_id) or rental
    try:
        await callback.bot.send_message(
            rental["user_id"],
            f"✅ Ijara tasdiqlandi!\n\n"
            f"📖 Kitob: {rental.get('book_title')}\n"
            f"📅 Qaytarish sanasi: {rental.get('due_ts')}\n\n"
            "Kitobni belgilangan muddatda qaytarishni unutmang.",
        )
    except Exception as e:
        logger.warning("User notify failed: %s", e)
    await callback.answer("Tasdiqlandi.", show_alert=True)
    rentals = db.list_rentals_pending_admin()
    await callback.message.edit_text(_admin_rentals_text(rentals), reply_markup=admin_rentals_keyboard(rentals), parse_mode=ParseMode.HTML)


async def cb_rental_no(callback: CallbackQuery):
    data = callback.data or ""
    if not data.startswith("rental_no_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("rental_no_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    if rental.get("status") != "requested":
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return
    if not db.set_rental_status(rental_id, "rejected"):
        await callback.answer("Bu so'rov allaqachon ko'rib chiqilgan.", show_alert=True)
        return
    try:
        await callback.bot.send_message(
            rental["user_id"],
            f"❌ Ijara rad etildi.\n\nKitob: {rental.get('book_title')}\nQo'shimcha savollar uchun admin bilan bog'laning.",
        )
    except Exception as e:
        logger.warning("User notify failed: %s", e)
    await callback.answer("Rad etildi.", show_alert=True)
    rentals = db.list_rentals_pending_admin()
    await callback.message.edit_text(_admin_rentals_text(rentals), reply_markup=admin_rentals_keyboard(rentals), parse_mode=ParseMode.HTML)


async def cb_rental_return(callback: CallbackQuery):
    """Admin marks rental as returned. Frees inventory."""
    data = callback.data or ""
    if not data.startswith("rental_return_"):
        await callback.answer()
        return
    try:
        rental_id = int(data.replace("rental_return_", ""))
    except ValueError:
        await callback.answer("Xatolik.")
        return
    rental = db.get_rental(rental_id)
    if not rental:
        await callback.answer("Ijara topilmadi.", show_alert=True)
        return
    admin_id = callback.from_user.id if callback.from_user else 0
    if not db.close_rental_returned(rental_id, admin_id):
        await callback.answer("Bu ijara allaqachon yopilgan.", show_alert=True)
        return
    logger.info(
        "Rental returned: admin_id=%s rental_id=%s book_id=%s user_id=%s",
        admin_id, rental_id, rental.get("book_id"), rental.get("user_id"),
    )
    await callback.answer("✅ Qaytarildi", show_alert=False)
    user_msg = "✅ Kitob qaytarildi deb belgilandi. Rahmat!"
    updated = db.get_rental(rental_id)
    if updated:
        penalty = db.compute_penalty(updated, datetime.now(timezone.utc))
        if penalty > 0:
            user_msg = f"✅ Qaytarildi. Yakuniy jarima: {penalty:,} so'm"
    try:
        await callback.bot.send_message(
            rental["user_id"],
            user_msg,
        )
    except Exception as e:
        logger.warning("User notify failed: %s", e)
    rentals = db.list_rentals_pending_admin()
    await callback.message.edit_text(_admin_rentals_text(rentals), reply_markup=admin_rentals_keyboard(rentals), parse_mode=ParseMode.HTML)


async def cb_admin_back(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("⚙️ Admin menyu:", reply_markup=admin_menu_keyboard())
    await callback.answer()


async def cb_books_back(callback: CallbackQuery):
    await callback.message.edit_text("📚 Kitoblar yoki kategoriyani tanlang:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=c, callback_data=f"cat_{c}")] for c in db.get_categories()
    ] + [
        [InlineKeyboardButton(text="📚 Barcha kitoblar", callback_data="cat_all")],
        [InlineKeyboardButton(text="🔎 Qidiruv", callback_data="books_search")],
    ]))
    await callback.answer()


async def cb_noop(callback: CallbackQuery):
    await callback.answer()


async def unknown_command_handler(message: Message):
    """Unknown commands (e.g. /random) -> friendly fallback."""
    await message.answer("Tushunmadim, /start bosing.", reply_markup=main_menu_keyboard())


_GREETINGS = frozenset((
    "salom", "assalomu alaykum", "assalomu alaykum!",
    "hello", "hi", "hey", "privet", "привет",
))


async def unhandled_text_handler(message: Message):
    """Production fallback for unhandled non-command text."""
    txt = (message.text or "").strip().lower()
    if txt in _GREETINGS or any(txt.startswith(g) for g in _GREETINGS):
        text = "Assalomu alaykum! 🙂 Quyidagi menyudan tanlang."
    else:
        text = "Tushunmadim 🙂 Menyudan tanlang yoki /start bosing."
    await message.answer(text, reply_markup=main_menu_keyboard())


async def fallback_handler(message: Message):
    """Catch-all: photos, stickers, etc. that have no text."""
    await message.answer("Tushunmadim, /start bosing.", reply_markup=main_menu_keyboard())


def _chat_type_str(chat) -> str:
    """Human-readable chat type (e.g. 'private')."""
    t = getattr(chat, "type", None)
    return getattr(t, "value", str(t)) if t else "?"


async def _log_incoming_update(handler, event: Update, data: dict):
    """Outer middleware: log every incoming update before any handler. Runs even if no handlers match."""
    try:
        ev_type = event.event_type
    except Exception:
        ev_type = "unknown"
    extra = ""
    if ev_type == "message" and event.message:
        msg = event.message
        chat_t = _chat_type_str(msg.chat)
        chat_id = getattr(msg.chat, "id", "?")
        uid = getattr(msg.from_user, "id", "?") if msg.from_user else "?"
        txt = (msg.text or "(no text)")[:200]
        extra = f" chat_type={chat_t} chat_id={chat_id} from_user_id={uid} text={txt!r}"
    elif ev_type == "callback_query" and event.callback_query:
        cq = event.callback_query
        uid = getattr(cq.from_user, "id", "?") if cq.from_user else "?"
        chat_t = _chat_type_str(cq.message.chat) if cq.message else "?"
        chat_id = getattr(cq.message.chat, "id", "?") if cq.message else "?"
        data_val = (cq.data or "(no data)")[:80]
        extra = f" chat_type={chat_t} chat_id={chat_id} from_user_id={uid} data={data_val!r}"
    else:
        extra = f" (raw event)"
    logger.info("INCOMING %s%s", ev_type, extra)
    return await handler(event, data)


_PRIVATE = F.chat.type == ChatType.PRIVATE


async def reminder_loop(bot: Bot) -> None:
    """Background task: send due_1day and overdue_daily reminders. Runs hourly."""
    while True:
        try:
            now_dt = datetime.now(timezone.utc)
            today_str = now_dt.date().isoformat()

            # A) 1-day-before reminder
            due_soon = db.get_due_soon_rentals(now_dt)
            for r in due_soon:
                rental_id = r.get("rental_id") or r.get("id")
                if not rental_id:
                    continue
                if not db.can_send_notification(rental_id, "due_1day", today_str):
                    continue
                due_date_pretty = (r.get("due_date") or r.get("due_ts") or "?")[:10]
                text = (
                    "⏳ Eslatma: ertaga ijara muddati tugaydi.\n"
                    f"📕 {r.get('book_title', '?')}\n"
                    f"📅 Muddat: {due_date_pretty}"
                )
                try:
                    await bot.send_message(r["user_id"], text)
                    db.mark_notification_sent(rental_id, "due_1day", today_str)
                except Exception as e:
                    logger.warning("Reminder due_1day failed rental_id=%s user_id=%s: %s", rental_id, r.get("user_id"), e)

            # B) Overdue daily reminder
            overdue = db.get_overdue_rentals(now_dt)
            for r in overdue:
                rental_id = r.get("rental_id") or r.get("id")
                if not rental_id:
                    continue
                if not db.can_send_notification(rental_id, "overdue_daily", today_str):
                    continue
                due_str = r.get("due_date") or r.get("due_ts") or ""
                due_date_pretty = due_str[:10] if due_str else "?"
                overdue_days = 1
                if due_str:
                    try:
                        due_date = datetime.fromisoformat(due_str[:10] + "T00:00:00+00:00")
                        delta = now_dt - due_date
                        overdue_days = max(1, int(delta.total_seconds() / 86400))
                    except Exception:
                        pass
                r_with_due = {**r, "due_date": due_str, "due_ts": due_str}
                computed_penalty = db.compute_penalty(r_with_due, now_dt)
                per_day = r.get("penalty_per_day") or 0
                if per_day <= 0:
                    per_day = db.get_penalty_default()
                penalty_line = f"\n💰 Jarima: {computed_penalty:,} so'm" + (f" ({per_day} so'm/kun)" if per_day > 0 else "") if computed_penalty > 0 else ""
                text = (
                    "⚠️ Diqqat: ijara muddati o'tib ketdi.\n"
                    f"📕 {r.get('book_title', '?')}\n"
                    f"⏰ Kechikdi: {overdue_days} kun\n"
                    f"📅 Muddat: {due_date_pretty}\n"
                    f"{penalty_line}\n\n"
                    "Iltimos, qaytarish bo'yicha bog'laning."
                )
                try:
                    await bot.send_message(r["user_id"], text)
                    db.mark_notification_sent(rental_id, "overdue_daily", today_str)
                except Exception as e:
                    logger.warning("Reminder overdue_daily failed rental_id=%s user_id=%s: %s", rental_id, r.get("user_id"), e)
        except Exception as e:
            logger.exception("Reminder loop error: %s", e)
        await asyncio.sleep(3600)


def setup_router(dp: Dispatcher) -> None:
    dp.message.register(cmd_start, CommandStart(), _PRIVATE)
    dp.message.register(cmd_admin, Command("admin"), _PRIVATE, AdminOnly())
    dp.message.register(cmd_wipe_all, Command("wipe_all"), _PRIVATE, AdminOnly())
    dp.message.register(cmd_set_order, Command("set_order"), _PRIVATE, AdminOnly())
    dp.message.register(show_rules, F.text == "ℹ️ Qoidalar", _PRIVATE)
    dp.message.register(show_books_menu, F.text == "📚 Kitoblar", _PRIVATE)
    dp.message.register(my_rentals, F.text == "📖 Mening ijaralarim", _PRIVATE)
    # Admin ReplyKeyboard buttons
    dp.message.register(admin_add_book_msg, F.text == "➕ Kitob qo'shish", _PRIVATE, AdminOnly())
    dp.message.register(admin_books_msg, F.text == "📚 Kitoblarim", _PRIVATE, AdminOnly())
    dp.message.register(admin_wipe_msg, F.text == "🧹 Tozalash", _PRIVATE, AdminOnly())
    dp.message.register(cmd_admin_rentals_msg, F.text == "📦 Ijaralar", _PRIVATE, AdminOnly())
    dp.message.register(admin_overdue_msg, F.text == "⏰ Kechikkanlar", _PRIVATE, AdminOnly())
    dp.message.register(admin_penalty_msg, F.text == "💰 Jarima", _PRIVATE, AdminOnly())
    dp.message.register(admin_stats_msg, F.text == "📊 Userlar statistikasi", _PRIVATE, AdminOnly())
    dp.message.register(admin_broadcast_msg, F.text == "📢 E'lon", _PRIVATE, AdminOnly())
    dp.message.register(admin_export_msg, F.text == "📤 Export", _PRIVATE, AdminOnly())
    dp.callback_query.register(cb_export_csv, F.data == "export_csv", AdminOnly())
    dp.callback_query.register(cb_export_json, F.data == "export_json", AdminOnly())
    dp.message.register(admin_broadcast_message, AdminBroadcastStates.message, _PRIVATE, AdminOnly())
    dp.callback_query.register(cb_broadcast_confirm, AdminBroadcastStates.confirm, F.data == "broadcast_confirm", AdminOnly())
    dp.callback_query.register(cb_broadcast_cancel, AdminBroadcastStates.confirm, F.data == "broadcast_cancel", AdminOnly())
    dp.callback_query.register(cb_admin_stats_top, F.data == "admin_stats_top", AdminOnly())
    dp.callback_query.register(cb_admin_stats_not_returned, F.data == "admin_stats_not_returned", AdminOnly())
    dp.callback_query.register(cb_admin_stats_blacklist, F.data == "admin_stats_blacklist", AdminOnly())
    dp.callback_query.register(cb_admin_stats_back, F.data == "admin_stats_back", AdminOnly())
    dp.message.register(admin_penalty_amount, AdminPenaltyStates.amount, _PRIVATE, AdminOnly())

    # Add book FSM
    dp.callback_query.register(cb_admin_add_book, F.data == "admin_add_book", AdminOnly())
    dp.message.register(add_book_title, AddBookStates.title, _PRIVATE, AdminOnly())
    dp.message.register(add_book_author, AddBookStates.author, _PRIVATE, AdminOnly())
    dp.callback_query.register(add_book_category_sel, AddBookStates.category, F.data.startswith("add_cat_"), AdminOnly())
    dp.message.register(add_book_category_other, AddBookStates.category_other, _PRIVATE, AdminOnly())
    dp.message.register(add_book_year, AddBookStates.year, _PRIVATE, AdminOnly())
    dp.callback_query.register(add_book_year_skip, AddBookStates.year, F.data == "add_year_skip", AdminOnly())
    dp.callback_query.register(add_book_cover_type, AddBookStates.cover_type, F.data.in_(["cover_qattiq", "cover_yumshoq"]), AdminOnly())
    dp.message.register(add_book_qty, AddBookStates.qty, _PRIVATE, AdminOnly())
    dp.callback_query.register(add_book_rent_fee_quick, AddBookStates.rent_fee, F.data.startswith("add_rent_"), AdminOnly())
    dp.message.register(add_book_rent_fee, AddBookStates.rent_fee, _PRIVATE, AdminOnly())
    dp.callback_query.register(add_book_deposit_skip, AddBookStates.deposit, F.data == "add_deposit_skip", AdminOnly())
    dp.message.register(add_book_deposit, AddBookStates.deposit, _PRIVATE, AdminOnly())
    dp.message.register(add_book_photo, AddBookStates.photo, _PRIVATE, AdminOnly(), F.photo)
    dp.callback_query.register(add_book_photo_skip, AddBookStates.photo, F.data == "add_book_photo_skip", AdminOnly())
    dp.message.register(add_book_photo_reject, AddBookStates.photo, _PRIVATE, AdminOnly(), ~F.photo)
    dp.callback_query.register(add_book_save, AddBookStates.preview, F.data == "add_book_save", AdminOnly())
    dp.callback_query.register(add_book_cancel, AddBookStates.preview, F.data == "add_book_cancel", AdminOnly())

    # Edit book FSM
    dp.message.register(edit_book_title, EditBookStates.title, _PRIVATE, AdminOnly())
    dp.message.register(edit_book_rent_fee, EditBookStates.rent_fee, _PRIVATE, AdminOnly())
    dp.message.register(edit_book_qty, EditBookStates.qty, _PRIVATE, AdminOnly())
    dp.message.register(edit_book_photo, EditBookStates.photo, _PRIVATE, AdminOnly(), F.photo)
    dp.message.register(edit_book_photo_reject, EditBookStates.photo, _PRIVATE, AdminOnly(), ~F.photo)

    # Admin callbacks
    dp.callback_query.register(cb_admin_wipe, F.data == "admin_wipe", AdminOnly())
    dp.callback_query.register(cb_wipe_confirm, F.data == "wipe_confirm", AdminOnly())
    dp.callback_query.register(cb_wipe_cancel, F.data == "wipe_cancel", AdminOnly())
    dp.callback_query.register(cb_admin_books, F.data == "admin_books", AdminOnly())
    dp.callback_query.register(cb_admin_books_page, F.data.startswith("admin_books_p_"), AdminOnly())
    dp.callback_query.register(cb_admin_books_filter_search, F.data == "admin_books_filter_search", AdminOnly())
    dp.callback_query.register(cb_admin_books_filter_cat, F.data == "admin_books_filter_cat", AdminOnly())
    dp.callback_query.register(cb_admin_books_filter_cat_sel, F.data.startswith("admin_books_cat_"), AdminOnly())
    dp.callback_query.register(cb_admin_books_filter_oos, F.data == "admin_books_filter_oos", AdminOnly())
    dp.callback_query.register(cb_admin_books_filter_clear, F.data == "admin_books_filter_clear", AdminOnly())
    dp.message.register(admin_books_search_query, AdminBooksFilterStates.search_query, _PRIVATE, AdminOnly())
    dp.callback_query.register(cb_admin_del_confirm, F.data.startswith("admin_del_confirm_"), AdminOnly())
    dp.callback_query.register(cb_admin_del_book, F.data.startswith("admin_del_"), AdminOnly())
    dp.callback_query.register(cb_admin_edit, F.data.startswith("admin_edit_"), AdminOnly())
    dp.callback_query.register(cb_edit_field, F.data.startswith("edit_field_"), AdminOnly())
    dp.callback_query.register(cb_admin_rentals, F.data == "admin_rentals", AdminOnly())
    dp.callback_query.register(cb_rental_ok, F.data.startswith("rental_ok_"), AdminOnly())
    dp.callback_query.register(cb_rental_no, F.data.startswith("rental_no_"), AdminOnly())
    dp.callback_query.register(cb_rental_return, F.data.startswith("rental_return_"), AdminOnly())
    dp.callback_query.register(cb_overdue_ping, F.data.startswith("overdue_ping_"), AdminOnly())
    dp.callback_query.register(admin_overdue_page, F.data.startswith("overdue_p_"), AdminOnly())
    dp.callback_query.register(cb_penalty_edit, F.data.startswith("penalty_edit_"), AdminOnly())
    dp.callback_query.register(cb_penalty_toggle, F.data.startswith("penalty_toggle_"), AdminOnly())
    dp.callback_query.register(cb_penalty_perday, F.data.startswith("penalty_perday_"), AdminOnly())
    dp.callback_query.register(cb_penalty_fixed, F.data.startswith("penalty_fixed_"), AdminOnly())
    dp.callback_query.register(cb_penalty_clear_fixed, F.data.startswith("penalty_clear_fixed_"), AdminOnly())
    dp.callback_query.register(cb_penalty_note, F.data.startswith("penalty_note_"), AdminOnly())
    dp.callback_query.register(cb_penalty_back, F.data.startswith("penalty_back_"), AdminOnly())
    dp.callback_query.register(cb_admin_back, F.data == "admin_back", AdminOnly())

    # Penalty edit FSM (AdminPenaltyEditStates)
    dp.message.register(penalty_edit_per_day, AdminPenaltyEditStates.per_day, _PRIVATE, AdminOnly())
    dp.message.register(penalty_edit_fixed, AdminPenaltyEditStates.fixed, _PRIVATE, AdminOnly())
    dp.message.register(penalty_edit_note, AdminPenaltyEditStates.note, _PRIVATE, AdminOnly())

    # User books
    dp.callback_query.register(cb_books_cat, F.data == "books_cat")
    dp.callback_query.register(cb_books_category, F.data.startswith("cat_"))
    dp.callback_query.register(cb_books_page, F.data.startswith("books_p_"))
    dp.callback_query.register(cb_books_sort, F.data.startswith("books_sort:"))
    dp.callback_query.register(cb_sort_sel, F.data.startswith("sort_sel:"))
    dp.callback_query.register(cb_books_search_start, F.data == "books_search")
    dp.callback_query.register(cb_rent_book, (F.data.startswith("rent_") | F.data.startswith("book_")))
    dp.callback_query.register(cb_rental_period, F.data.startswith("period_"))
    dp.callback_query.register(cb_books_back, F.data == "books_back")
    dp.callback_query.register(cb_noop, F.data == "noop")

    dp.message.register(search_query_handler, SearchStates.query, _PRIVATE)

    # Unknown commands (/random, /abc) -> "Tushunmadim, /start bosing."
    dp.message.register(unknown_command_handler, _PRIVATE, F.text, F.text.startswith("/"))
    # Unhandled text: greetings -> warm reply; other -> friendly fallback
    _NON_CMD = F.text & ~F.text.startswith("/")
    dp.message.register(unhandled_text_handler, _NON_CMD, _PRIVATE)
    # Fallback: no text (photo, sticker, etc.)
    dp.message.register(fallback_handler, _PRIVATE)


async def main():
    raw = os.getenv("BOT_TOKEN", "") or ""
    token = raw.strip().strip("'\"")
    if not token:
        raise RuntimeError(
            "BOT_TOKEN muhiti o'zgaruvchisi (environment variable) o'rnatilmagan yoki bo'sh. "
            ".env faylida BOT_TOKEN=... qo'shing."
        )
    _token_re = re.compile(r"^\d{6,12}:[A-Za-z0-9_-]{30,}$")
    _regex_ok = bool(_token_re.match(token))
    _env_exists = "BOT_TOKEN" in os.environ
    _id_prefix = token.split(":")[0][:3] if ":" in token else ""
    _tail = token[-2:] if len(token) >= 2 else ""
    logger.info(
        "BOT_TOKEN loaded=%s len=%d has_colon=%s regex_ok=%s id_prefix=%s tail=%s",
        _env_exists, len(token), ":" in token, _regex_ok, _id_prefix, _tail,
    )
    if not _regex_ok:
        raise RuntimeError(
            "BOT_TOKEN format invalid. Expected: digits:alphanumeric (e.g. 123456789:ABC...). "
            "Get token from @BotFather and set in .env as BOT_TOKEN=..."
        )

    create_lock()
    logger.info("Starting bot process. Admins: %s", ADMIN_IDS)

    db.init_db()

    logger.info("Reminders enabled: %s", REMINDERS_ENABLED)

    try:
        bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    except Exception:
        logger.exception("Failed to create Bot object (token validation error)")
        raise

    await bot.delete_webhook(drop_pending_updates=True)
    try:
        me = await bot.get_me()
        logger.info("Bot ready: id=%s, username=@%s", me.id, me.username)
    except Exception as e:
        logger.error("get_me failed: %s: %s", type(e).__name__, e)
        raise

    dp = Dispatcher()
    dp.update.outer_middleware(_log_incoming_update)
    setup_router(dp)

    if REMINDERS_ENABLED:
        asyncio.create_task(reminder_loop(bot))

    await dp.start_polling(bot)


# ====== Admin security self-test checklist ======
# Run these manually to verify admin protection:
# 1. Non-admin: /admin -> "Bu buyruq faqat admin uchun."
# 2. Non-admin: /wipe_all, /set_order -> same message
# 3. Non-admin: tap admin_back, admin_books, admin_del_*, rental_ok_*, rental_no_* -> "Ruxsat yo'q." alert
# 4. Non-admin: cannot complete add_book FSM (each step blocked)
# 5. Admin: all above actions work normally
# Secured handlers: cmd_admin, cmd_wipe_all, cmd_set_order, cb_admin_add_book,
#   add_book_* (FSM), cb_admin_books, cb_admin_books_page,
#   cb_admin_del_book, cb_admin_rentals, cb_rental_ok, cb_rental_no, cb_admin_back


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        logger.error("Startup failed: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Startup failed with unexpected error")
        sys.exit(1)
