"""SQLite database for kitob ijara bot."""
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "bot.db"


def _get_db_timeout_seconds() -> float:
    """SQLite connection timeout seconds (env DB_TIMEOUT, default 10)."""
    raw = (os.getenv("DB_TIMEOUT", "") or "").strip()
    if not raw:
        return 10.0
    try:
        v = float(raw)
        return 10.0 if v <= 0 else v
    except ValueError:
        return 10.0


def _write_retry(callable_fn, *, attempts: int = 5) -> Any:
    """Retry helper for write ops hitting 'database is locked'.

    Backoff (seconds): 0.05, 0.1, 0.2, 0.4, 0.8 (max 5 attempts).
    Only retries sqlite3.OperationalError containing 'locked'.
    """
    delays = (0.05, 0.1, 0.2, 0.4, 0.8)
    last_err: Optional[Exception] = None
    for i in range(max(1, attempts)):
        try:
            return callable_fn()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" not in msg:
                raise
            last_err = e
            if i >= len(delays):
                break
            time.sleep(delays[i])
    # Re-raise last locked error if we ran out of retries.
    if last_err:
        raise last_err
    raise sqlite3.OperationalError("database is locked")


def _get_conn() -> sqlite3.Connection:
    # Reliability tweaks:
    # - timeout=10: reduce 'database is locked' errors under concurrent access
    # - check_same_thread=False: allow use across async callbacks/threads safely per-connection
    conn = sqlite3.connect(DB_PATH, timeout=_get_db_timeout_seconds(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # PRAGMAs are per-connection; keep them lightweight and consistent.
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA busy_timeout = 5000")
    except Exception:
        # If WAL isn't supported (e.g. some environments), continue with defaults.
        pass
    return conn


def _migrate_books_schema(conn: sqlite3.Connection) -> None:
    """Add new columns if they don't exist."""
    cur = conn.execute("PRAGMA table_info(books)")
    cols = {row[1] for row in cur.fetchall()}
    for col, stmt in [
        ("publisher", "ALTER TABLE books ADD COLUMN publisher TEXT DEFAULT ''"),
        ("year", "ALTER TABLE books ADD COLUMN year INTEGER DEFAULT 0"),
        ("cover_type", "ALTER TABLE books ADD COLUMN cover_type TEXT DEFAULT 'yumshoq'"),
        ("language", "ALTER TABLE books ADD COLUMN language TEXT DEFAULT 'uz'"),
        ("sort_order", "ALTER TABLE books ADD COLUMN sort_order INTEGER DEFAULT NULL"),
        ("photo_id", "ALTER TABLE books ADD COLUMN photo_id TEXT NULL"),
    ]:
        if col not in cols:
            conn.execute(stmt)
    conn.commit()


def _migrate_rentals_schema(conn: sqlite3.Connection) -> None:
    """Add new columns to rentals if they don't exist."""
    cur = conn.execute("PRAGMA table_info(rentals)")
    cols = {row[1] for row in cur.fetchall()}
    for col, stmt in [
        ("returned_at", "ALTER TABLE rentals ADD COLUMN returned_at TEXT NULL"),
        ("closed_by_admin_id", "ALTER TABLE rentals ADD COLUMN closed_by_admin_id INTEGER NULL"),
        ("approved_by_admin_id", "ALTER TABLE rentals ADD COLUMN approved_by_admin_id INTEGER NULL"),
        ("payment_method", "ALTER TABLE rentals ADD COLUMN payment_method TEXT DEFAULT NULL"),
        ("payment_status", "ALTER TABLE rentals ADD COLUMN payment_status TEXT NOT NULL DEFAULT 'pending'"),
        ("paid_at", "ALTER TABLE rentals ADD COLUMN paid_at TEXT DEFAULT NULL"),
        ("payment_proof_file_id", "ALTER TABLE rentals ADD COLUMN payment_proof_file_id TEXT DEFAULT NULL"),
        ("payment_confirmed_at", "ALTER TABLE rentals ADD COLUMN payment_confirmed_at TEXT DEFAULT NULL"),
        ("payment_confirmed_by", "ALTER TABLE rentals ADD COLUMN payment_confirmed_by INTEGER DEFAULT NULL"),
        ("pickup_date", "ALTER TABLE rentals ADD COLUMN pickup_date TEXT DEFAULT NULL"),
        ("pickup_slot", "ALTER TABLE rentals ADD COLUMN pickup_slot TEXT DEFAULT NULL"),
        ("period_days", "ALTER TABLE rentals ADD COLUMN period_days INTEGER NOT NULL DEFAULT 0"),
        ("rent_fee_total", "ALTER TABLE rentals ADD COLUMN rent_fee_total INTEGER NOT NULL DEFAULT 0"),
        ("rejected_reason", "ALTER TABLE rentals ADD COLUMN rejected_reason TEXT DEFAULT NULL"),
        ("penalty_enabled", "ALTER TABLE rentals ADD COLUMN penalty_enabled INTEGER NOT NULL DEFAULT 1"),
        ("penalty_per_day", "ALTER TABLE rentals ADD COLUMN penalty_per_day INTEGER NOT NULL DEFAULT 0"),
        ("penalty_fixed", "ALTER TABLE rentals ADD COLUMN penalty_fixed INTEGER NULL"),
        ("penalty_note", "ALTER TABLE rentals ADD COLUMN penalty_note TEXT NULL"),
        ("penalty_updated_at", "ALTER TABLE rentals ADD COLUMN penalty_updated_at TEXT NULL"),
        ("penalty_updated_by", "ALTER TABLE rentals ADD COLUMN penalty_updated_by INTEGER NULL"),
    ]:
        if col not in cols:
            conn.execute(stmt)
    conn.commit()


def approve_rental_if_available(rental_id: int, admin_id: int) -> tuple[bool, str]:
    """Atomically approve a pending rental iff stock is available.

    Uses BEGIN IMMEDIATE to prevent concurrent approvals from overselling stock.

    Returns:
      (True, "ok") on success
      (False, "not_found") if rental does not exist (or book row missing)
      (False, "wrong_status") if rental already processed
      (False, "not_available") if no copies available
      (False, "locked") if database is locked (after retries)
    """
    def _op() -> tuple[bool, str]:
        conn = _get_conn()
        # We want explicit transaction control for BEGIN IMMEDIATE.
        conn.isolation_level = None  # autocommit mode
        try:
            conn.execute("BEGIN IMMEDIATE")

            cur = conn.execute(
                "SELECT id, book_id, status FROM rentals WHERE id = ?",
                (rental_id,),
            )
            rental = cur.fetchone()
            if not rental:
                conn.execute("ROLLBACK")
                return False, "not_found"
            if rental["status"] != "requested":
                conn.execute("ROLLBACK")
                return False, "wrong_status"

            # Compute availability inside the same transaction.
            cur = conn.execute("SELECT qty FROM books WHERE id = ?", (rental["book_id"],))
            b = cur.fetchone()
            if not b:
                conn.execute("ROLLBACK")
                return False, "not_found"
            total_qty = int(b[0] or 0)

            cur = conn.execute(
                "SELECT COUNT(*) FROM rentals WHERE book_id = ? AND status IN ('approved', 'active')",
                (rental["book_id"],),
            )
            active = int(cur.fetchone()[0] or 0)
            available = total_qty - active
            if available <= 0:
                conn.execute("ROLLBACK")
                return False, "not_available"

            now_iso = datetime.now(timezone.utc).isoformat()
            cur = conn.execute(
                "UPDATE rentals SET status = 'approved', start_ts = ?, approved_by_admin_id = ? "
                "WHERE id = ? AND status = 'requested'",
                (now_iso, admin_id, rental_id),
            )
            if cur.rowcount <= 0:
                conn.execute("ROLLBACK")
                return False, "wrong_status"

            conn.execute("COMMIT")
            return True, "ok"
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    try:
        return _write_retry(_op)
    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower():
            return False, "locked"
        raise


def _create_rental_notifications_table(conn: sqlite3.Connection) -> None:
    """Create rental_notifications table if not exists."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS rental_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rental_id INTEGER NOT NULL,
            notif_type TEXT NOT NULL,
            last_sent_date TEXT NOT NULL,
            UNIQUE(rental_id, notif_type)
        )
    """)
    conn.commit()


def _create_settings_table(conn: sqlite3.Connection) -> None:
    """Create bot_settings table for penalty etc. Insert default penalty if empty."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    cur = conn.execute("SELECT value FROM bot_settings WHERE key = 'penalty_per_day'")
    if cur.fetchone() is None:
        conn.execute("INSERT INTO bot_settings (key, value) VALUES ('penalty_per_day', '2000')")
    # Shop settings defaults
    for k, v in [
        ("address", ""),
        ("contact", ""),
        ("work_hours", ""),
        ("click_link", ""),
        ("payme_link", ""),
    ]:
        cur = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (k,))
        if cur.fetchone() is None:
            conn.execute("INSERT INTO bot_settings (key, value) VALUES (?, ?)", (k, v))
    conn.commit()


def init_db() -> None:
    """Create tables if not exist."""
    conn = _get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                author TEXT NOT NULL,
                category TEXT NOT NULL,
                rent_fee INTEGER NOT NULL DEFAULT 0,
                deposit INTEGER NOT NULL DEFAULT 0,
                qty INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rentals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                book_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                start_ts TEXT,
                due_ts TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (book_id) REFERENCES books(id)
            )
        """)
        conn.commit()
        _migrate_books_schema(conn)
        _migrate_rentals_schema(conn)
        _create_rental_notifications_table(conn)
        _create_settings_table(conn)
        # Indexes (idempotent) — improves stock/overdue queries and reduces lock duration.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_book_status ON rentals(book_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_user_id ON rentals(user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_due_ts ON rentals(due_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_status_created ON rentals(status, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_payment_status ON rentals(payment_status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rentals_payment_confirmed_at ON rentals(payment_confirmed_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_books_category ON books(category)")
        conn.commit()
    finally:
        conn.close()


def wipe_all() -> None:
    """Delete all rentals first, then all books."""
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM rentals")
        conn.execute("DELETE FROM books")
        conn.commit()
    finally:
        conn.close()


def add_book(
    title: str,
    author: str,
    category: str,
    rent_fee: int,
    deposit: int = 0,
    qty: int = 1,
    year: int = 0,
    publisher: str = "",
    cover_type: str = "yumshoq",
    photo_id: Optional[str] = None,
) -> int:
    """Add a book. rent_fee required and must be > 0. Returns new book id."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO books (title, author, category, rent_fee, deposit, qty, created_at, year, publisher, cover_type, photo_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                title, author, category, rent_fee, deposit, max(1, qty), datetime.now().isoformat(),
                year,
                publisher,
                "qattiq" if cover_type == "qattiq" else "yumshoq",
                photo_id,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


SORT_NEWEST = "newest"
SORT_AUTHOR = "author"
SORT_CATEGORY = "category"
SORT_MANUAL = "manual"
SORT_TITLE = "title"


def list_books(
    offset: int = 0,
    limit: int = 10,
    category: Optional[str] = None,
    q: Optional[str] = None,
    sort_mode: str = SORT_NEWEST,
) -> list[dict[str, Any]]:
    """List books with optional filter and sort. sort_mode: newest, author, category, manual."""
    conn = _get_conn()
    try:
        params: list[Any] = []
        where = []
        if category:
            where.append("category = ?")
            params.append(category)
        if q:
            where.append("(title LIKE ? OR author LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        sql = "SELECT * FROM books"
        if where:
            sql += " WHERE " + " AND ".join(where)
        if sort_mode == SORT_NEWEST:
            sql += " ORDER BY COALESCE(year, 0) DESC, title ASC"
        elif sort_mode == SORT_TITLE:
            sql += " ORDER BY title ASC"
        elif sort_mode == SORT_AUTHOR:
            sql += " ORDER BY author ASC, title ASC"
        elif sort_mode == SORT_CATEGORY:
            sql += " ORDER BY category ASC, title ASC"
        elif sort_mode == SORT_MANUAL:
            sql += " ORDER BY sort_order ASC NULLS LAST, title ASC"
        else:
            sql += " ORDER BY COALESCE(year, 0) DESC, title ASC"
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        cur = conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def count_books(category: Optional[str] = None, q: Optional[str] = None) -> int:
    """Count books matching filter."""
    conn = _get_conn()
    try:
        params: list[Any] = []
        where = []
        if category:
            where.append("category = ?")
            params.append(category)
        if q:
            where.append("(title LIKE ? OR author LIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])
        sql = "SELECT COUNT(*) FROM books"
        if where:
            sql += " WHERE " + " AND ".join(where)
        cur = conn.execute(sql, params)
        return cur.fetchone()[0]
    finally:
        conn.close()


def list_books_admin(
    *,
    q: Optional[str] = None,
    category: Optional[str] = None,
    only_out_of_stock: bool = False,
    page: int = 1,
    page_size: int = 10,
) -> tuple[list[dict[str, Any]], int]:
    """List books for admin with filters. Returns (books, total_count).
    q: search in title/author (case-insensitive LIKE).
    category: filter by category.
    only_out_of_stock: only books with available=0 (qty - active_rentals <= 0).
    Uses subquery for rental count; filters then paginates."""
    conn = _get_conn()
    try:
        params: list[Any] = []
        where = []
        if q:
            q_like = f"%{q.lower()}%"
            where.append("(LOWER(b.title) LIKE ? OR LOWER(b.author) LIKE ?)")
            params.extend([q_like, q_like])
        if category:
            where.append("b.category = ?")
            params.append(category)
        if only_out_of_stock:
            where.append("(b.qty - COALESCE(r.rented, 0)) <= 0")
        from_clause = (
            "FROM books b "
            "LEFT JOIN ("
            "  SELECT book_id, COUNT(*) AS rented FROM rentals "
            "  WHERE status IN ('approved','active') GROUP BY book_id"
            ") r ON b.id = r.book_id"
        )
        where_sql = " WHERE " + " AND ".join(where) if where else ""
        count_sql = f"SELECT COUNT(*) {from_clause}{where_sql}"
        cur = conn.execute(count_sql, params)
        total = cur.fetchone()[0]
        order = " ORDER BY b.id DESC"
        offset = (page - 1) * page_size
        params.extend([page_size, offset])
        list_sql = f"SELECT b.* {from_clause}{where_sql}{order} LIMIT ? OFFSET ?"
        cur = conn.execute(list_sql, params)
        return [dict(row) for row in cur.fetchall()], total
    finally:
        conn.close()


def get_book(book_id: int) -> Optional[dict[str, Any]]:
    """Get book by id."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT * FROM books WHERE id = ?", (book_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_book_stock(book_id: int) -> Optional[dict[str, Any]]:
    """Get stock info for a book: total, rented, available.
    Active rentals: status IN ('approved', 'active').
    Returns None if book not found."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT qty FROM books WHERE id = ?", (book_id,))
        row = cur.fetchone()
        if not row:
            return None
        total = row[0] or 0
        cur2 = conn.execute(
            "SELECT COUNT(*) FROM rentals WHERE book_id = ? AND status IN ('approved', 'active')",
            (book_id,),
        )
        rented = cur2.fetchone()[0] or 0
        available = max(0, total - rented)
        return {"total": total, "rented": rented, "available": available}
    finally:
        conn.close()


def has_active_rentals(book_id: int) -> bool:
    """True if book has any active rentals (approved or active)."""
    stock = get_book_stock(book_id)
    return stock is not None and (stock.get("rented") or 0) > 0


def set_book_sort_order(book_id: int, sort_order: Optional[int]) -> bool:
    """Set sort_order for a book. Returns True if updated."""
    conn = _get_conn()
    try:
        cur = conn.execute("UPDATE books SET sort_order = ? WHERE id = ?", (sort_order, book_id))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def has_any_manual_order() -> bool:
    """True if any book has sort_order set."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM books WHERE sort_order IS NOT NULL LIMIT 1")
        return cur.fetchone() is not None
    finally:
        conn.close()


def update_book(
    book_id: int,
    *,
    title: Optional[str] = None,
    rent_fee: Optional[int] = None,
    qty: Optional[int] = None,
    photo_id: Optional[str] = None,
) -> bool:
    """Update book fields. None means no change. photo_id='' means remove."""
    conn = _get_conn()
    try:
        updates = []
        params: list[Any] = []
        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if rent_fee is not None:
            updates.append("rent_fee = ?")
            params.append(rent_fee)
        if qty is not None:
            updates.append("qty = ?")
            params.append(max(1, qty))
        if photo_id is not None:
            updates.append("photo_id = ?")
            params.append(photo_id if photo_id else None)
        if not updates:
            return False
        params.append(book_id)
        cur = conn.execute(
            f"UPDATE books SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def delete_book(book_id: int) -> bool:
    """Delete book. Returns False if book has active rentals.

    With foreign_keys=ON, deleting a book referenced by rentals will fail unless
    related rentals are removed first. We keep active rentals protected and
    remove only non-active rentals for the book before deleting it.
    """
    if has_active_rentals(book_id):
        return False
    conn = _get_conn()
    try:
        # Remove non-active rentals to satisfy FK integrity.
        conn.execute(
            "DELETE FROM rentals WHERE book_id = ? AND status NOT IN ('approved', 'active')",
            (book_id,),
        )
        cur = conn.execute("DELETE FROM books WHERE id = ?", (book_id,))
        conn.commit()
        return cur.rowcount > 0
    except sqlite3.IntegrityError:
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


DEFAULT_CATEGORIES = ["Badiiy", "Dasturlash", "Tarix"]


def get_categories() -> list[str]:
    """Get distinct categories from books."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT DISTINCT category FROM books ORDER BY category")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def get_categories_for_add() -> list[str]:
    """Categories for add-book form: from DB or defaults, plus Boshqa."""
    cats = get_categories()
    base = cats if cats else DEFAULT_CATEGORIES
    return base + ["Boshqa"] if "Boshqa" not in base else base


def create_rental_request(
    user_id: int,
    book_id: int,
    due_ts: str,
    *,
    period_days: int = 0,
    rent_fee_total: int = 0,
    payment_method: Optional[str] = None,
) -> int:
    """Create rental with status=requested. Returns rental id."""
    conn = _get_conn()
    try:
        pm = (payment_method or "").strip().lower() or None
        cur = conn.execute(
            "INSERT INTO rentals (user_id, book_id, status, due_ts, created_at, period_days, rent_fee_total, payment_method) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                user_id,
                book_id,
                "requested",
                due_ts,
                datetime.now().isoformat(),
                max(0, int(period_days or 0)),
                max(0, int(rent_fee_total or 0)),
                pm,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_rental_schedule(rental_id: int, *, pickup_date: Optional[str] = None, pickup_slot: Optional[str] = None) -> bool:
    """Update pickup_date/pickup_slot for rental."""
    updates = []
    params: list[Any] = []
    if pickup_date is not None:
        updates.append("pickup_date = ?")
        params.append(pickup_date)
    if pickup_slot is not None:
        updates.append("pickup_slot = ?")
        params.append(pickup_slot)
    if not updates:
        return False
    params.append(rental_id)

    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                f"UPDATE rentals SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def update_rental_period_and_total(rental_id: int, *, period_days: int, rent_fee_total: int) -> bool:
    """Store chosen rental period days and computed total fee."""
    period_days = max(0, int(period_days or 0))
    rent_fee_total = max(0, int(rent_fee_total or 0))

    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals SET period_days = ?, rent_fee_total = ? WHERE id = ?",
                (period_days, rent_fee_total, rental_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def list_rentals(status: Optional[str] = None) -> list[dict[str, Any]]:
    """List rentals, optionally filtered by status."""
    conn = _get_conn()
    try:
        if status:
            cur = conn.execute(
                "SELECT r.*, b.title AS book_title, b.author AS book_author "
                "FROM rentals r JOIN books b ON r.book_id = b.id WHERE r.status = ? ORDER BY r.id DESC",
                (status,),
            )
        else:
            cur = conn.execute(
                "SELECT r.*, b.title AS book_title, b.author AS book_author "
                "FROM rentals r JOIN books b ON r.book_id = b.id ORDER BY r.id DESC"
            )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_rental(rental_id: int) -> Optional[dict[str, Any]]:
    """Get rental by id with book info."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.*, b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id WHERE r.id = ?",
            (rental_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_setting(key: str) -> str:
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return str(row[0]) if row and row[0] is not None else ""
    finally:
        conn.close()


def set_setting(key: str, value: str) -> bool:
    key = (key or "").strip()
    if not key:
        return False
    value = value if value is not None else ""

    def _op() -> bool:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO bot_settings(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            conn.commit()
            return True
        finally:
            conn.close()

    return bool(_write_retry(_op))


def get_shop_settings() -> dict[str, str]:
    return {
        "address": get_setting("address"),
        "contact": get_setting("contact"),
        "work_hours": get_setting("work_hours"),
        "click_link": get_setting("click_link"),
        "payme_link": get_setting("payme_link"),
    }


def revenue_summary(start_date: str, end_date: str) -> dict[str, int]:
    """Revenue summary by created_at date range (inclusive), UTC dates YYYY-MM-DD.

    Returns: {"rental_count": int, "rent_fee_sum": int}
    rental_count counts rentals with status IN ('approved','active','returned').
    rent_fee_sum is SUM(books.rent_fee) for those rentals.
    """
    start_date = (start_date or "")[:10]
    end_date = (end_date or "")[:10]
    if not start_date or not end_date:
        return {"rental_count": 0, "rent_fee_sum": 0}
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) AS rental_count, COALESCE(SUM(b.rent_fee), 0) AS rent_fee_sum "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('approved','active','returned') "
            "AND substr(r.created_at, 1, 10) >= ? AND substr(r.created_at, 1, 10) <= ?",
            (start_date, end_date),
        )
        row = cur.fetchone()
        return {
            "rental_count": int(row["rental_count"] or 0),
            "rent_fee_sum": int(row["rent_fee_sum"] or 0),
        }
    finally:
        conn.close()


def set_rental_status(rental_id: int, status: str, start_ts: Optional[str] = None) -> bool:
    """Update rental status only if current status is 'requested'. Returns True if updated (idempotent)."""
    def _op() -> bool:
        conn = _get_conn()
        try:
            if start_ts:
                cur = conn.execute(
                    "UPDATE rentals SET status = ?, start_ts = ? WHERE id = ? AND status = 'requested'",
                    (status, start_ts, rental_id),
                )
            else:
                cur = conn.execute(
                    "UPDATE rentals SET status = ? WHERE id = ? AND status = 'requested'",
                    (status, rental_id),
                )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def list_rentals_pending_admin() -> list[dict[str, Any]]:
    """List rentals with status IN ('requested', 'approved', 'active') for admin view."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.*, b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('requested', 'approved', 'active') ORDER BY r.id DESC",
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def list_overdue_rentals(now_iso: str, offset: int = 0, limit: int = 10) -> list[dict[str, Any]]:
    """List overdue rentals: status IN ('approved','active'), due_ts < now.
    now_iso: ISO timestamp or YYYY-MM-DD string.
    Returns list with rental_id, user_id, book_id, due_date (due_ts), period_days (if computable), status, book_title, book_author."""
    now_date = now_iso[:10] if now_iso else ""  # YYYY-MM-DD
    if not now_date:
        return []
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.id AS rental_id, r.user_id, r.book_id, r.due_ts AS due_date, "
            "r.start_ts, r.status, r.returned_at, r.penalty_enabled, r.penalty_per_day, r.penalty_fixed, "
            "b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('approved', 'active') "
            "AND r.due_ts IS NOT NULL AND r.due_ts != '' AND r.due_ts < ? "
            "ORDER BY r.due_ts ASC "
            "LIMIT ? OFFSET ?",
            (now_date, limit, offset),
        )
        rows = [dict(row) for row in cur.fetchall()]
        for row in rows:
            period_days = None
            if row.get("start_ts") and row.get("due_date"):
                try:
                    start = datetime.fromisoformat(row["start_ts"].replace("Z", "+00:00"))
                    due_str = row["due_date"]
                    due = datetime.fromisoformat(due_str + "T00:00:00+00:00") if len(due_str) == 10 else datetime.fromisoformat(due_str.replace("Z", "+00:00"))
                    period_days = (due - start).days
                except Exception:
                    pass
            row["period_days"] = period_days
        return rows
    finally:
        conn.close()


def count_overdue_rentals(now_iso: str) -> int:
    """Count overdue rentals."""
    now_date = now_iso[:10] if now_iso else ""
    if not now_date:
        return 0
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM rentals r "
            "WHERE r.status IN ('approved', 'active') "
            "AND r.due_ts IS NOT NULL AND r.due_ts != '' AND r.due_ts < ?",
            (now_date,),
        )
        return cur.fetchone()[0] or 0
    finally:
        conn.close()


def get_due_soon_rentals(now_dt: datetime) -> list[dict[str, Any]]:
    """Return rentals where status active AND due_ts is tomorrow (YYYY-MM-DD).
    Skips NULL due_ts. Joins books for title/author. Limit 200."""
    tomorrow = (now_dt.date() + timedelta(days=1)).isoformat()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.id AS rental_id, r.user_id, r.book_id, r.due_ts AS due_date, "
            "r.status, b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('approved', 'active') "
            "AND r.due_ts IS NOT NULL AND r.due_ts != '' AND r.due_ts = ? "
            "ORDER BY r.id ASC LIMIT 200",
            (tomorrow,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_overdue_rentals(now_dt: datetime) -> list[dict[str, Any]]:
    """Return rentals where status active AND due_ts < now. Skips NULL due_ts.
    Joins books for title/author. Limit 200. Includes penalty columns for compute_penalty."""
    now_date = now_dt.date().isoformat()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.id AS rental_id, r.user_id, r.book_id, r.due_ts AS due_date, "
            "r.status, r.returned_at, r.penalty_enabled, r.penalty_per_day, r.penalty_fixed, "
            "b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('approved', 'active') "
            "AND r.due_ts IS NOT NULL AND r.due_ts != '' AND r.due_ts < ? "
            "ORDER BY r.due_ts ASC LIMIT 200",
            (now_date,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def can_send_notification(rental_id: int, notif_type: str, today_str: str) -> bool:
    """Returns True if no record or last_sent_date != today_str."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT last_sent_date FROM rental_notifications WHERE rental_id = ? AND notif_type = ?",
            (rental_id, notif_type),
        )
        row = cur.fetchone()
        if not row:
            return True
        return row[0] != today_str
    finally:
        conn.close()


def mark_notification_sent(rental_id: int, notif_type: str, today_str: str) -> None:
    """Upsert (insert or update) last_sent_date."""
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO rental_notifications (rental_id, notif_type, last_sent_date) "
            "VALUES (?, ?, ?) ON CONFLICT(rental_id, notif_type) DO UPDATE SET last_sent_date = excluded.last_sent_date",
            (rental_id, notif_type, today_str),
        )
        conn.commit()
    finally:
        conn.close()


DEFAULT_PENALTY_PER_DAY = 2000


def get_penalty_default() -> int:
    """Global default penalty per day from env PENALTY_PER_DAY_DEFAULT. 0 if not set."""
    try:
        return max(0, int(os.getenv("PENALTY_PER_DAY_DEFAULT", "0").strip()))
    except ValueError:
        return 0


def get_penalty_per_day() -> int:
    """Get penalty per overdue day (so'm/kun). Default 2000."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT value FROM bot_settings WHERE key = 'penalty_per_day'")
        row = cur.fetchone()
        if not row:
            return DEFAULT_PENALTY_PER_DAY
        try:
            return max(0, int(row[0]))
        except ValueError:
            return DEFAULT_PENALTY_PER_DAY
    finally:
        conn.close()


def set_penalty_per_day(amount: int) -> bool:
    """Set penalty per overdue day (so'm/kun). Returns True if updated."""
    amount = max(0, amount)
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO bot_settings (key, value) VALUES ('penalty_per_day', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = ?",
            (str(amount), str(amount)),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def compute_penalty(rental: dict, now_dt: datetime) -> int:
    """Compute penalty for rental. Uses returned_at as 'now' if status==returned."""
    if rental.get("penalty_enabled", 1) == 0:
        return 0
    if rental.get("penalty_fixed") is not None:
        try:
            return max(0, int(rental["penalty_fixed"]))
        except (ValueError, TypeError):
            pass
    due_str = rental.get("due_ts") or rental.get("due_date") or ""
    if not due_str:
        return 0
    cutoff_dt = now_dt
    if rental.get("status") == "returned" and rental.get("returned_at"):
        try:
            cutoff_dt = datetime.fromisoformat(str(rental["returned_at"]).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
    try:
        due_dt = datetime.fromisoformat(due_str[:10] + "T00:00:00+00:00")
    except (ValueError, TypeError):
        return 0
    overdue_days = max(0, (cutoff_dt - due_dt).days)
    per_day = rental.get("penalty_per_day") or 0
    if per_day <= 0:
        per_day = get_penalty_default()
    if per_day <= 0:
        return 0
    return overdue_days * per_day


def update_rental_penalty(
    rental_id: int,
    admin_id: int,
    *,
    penalty_enabled: Optional[int] = None,
    penalty_per_day: Optional[int] = None,
    penalty_fixed: Optional[int] = None,
    penalty_note: Optional[str] = None,
    clear_penalty_fixed: bool = False,
) -> bool:
    """Update penalty fields. Logs penalty_updated_at, penalty_updated_by."""
    updates = []
    params: list[Any] = []
    now_iso = datetime.now(timezone.utc).isoformat()
    if penalty_enabled is not None:
        updates.append("penalty_enabled = ?")
        params.append(1 if penalty_enabled else 0)
    if penalty_per_day is not None:
        updates.append("penalty_per_day = ?")
        params.append(max(0, penalty_per_day))
    if penalty_fixed is not None:
        updates.append("penalty_fixed = ?")
        params.append(max(0, penalty_fixed))
    if clear_penalty_fixed:
        updates.append("penalty_fixed = NULL")
    if penalty_note is not None:
        updates.append("penalty_note = ?")
        params.append(penalty_note)
    if not updates:
        return False
    updates.append("penalty_updated_at = ?")
    params.append(now_iso)
    updates.append("penalty_updated_by = ?")
    params.append(admin_id)
    params.append(rental_id)
    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                f"UPDATE rentals SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def close_rental_returned(rental_id: int, admin_id: int) -> bool:
    """Close rental as returned. Only if status IN ('approved','active').
    Sets status='returned', returned_at=now, closed_by_admin_id=admin_id.
    Returns True if updated, False otherwise."""
    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals SET status = 'returned', returned_at = ?, closed_by_admin_id = ? "
                "WHERE id = ? AND status IN ('approved', 'active')",
                (datetime.now(timezone.utc).isoformat(), admin_id, rental_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def set_rental_payment_method(rental_id: int, method: str) -> bool:
    """User chooses payment method for an approved rental (cash/card)."""
    method_norm = (method or "").strip().lower()
    if method_norm not in ("cash", "card"):
        return False

    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals SET payment_method = ?, payment_status = 'pending', paid_at = NULL "
                "WHERE id = ? AND status = 'approved' AND payment_status = 'pending'",
                (method_norm, rental_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def reset_rental_payment(rental_id: int) -> bool:
    """Reset payment selection for an approved rental."""
    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals SET payment_method = NULL, payment_status = 'pending', paid_at = NULL "
                "WHERE id = ? AND status = 'approved' AND payment_status = 'pending'",
                (rental_id,),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def confirm_rental_payment(rental_id: int, admin_id: int) -> bool:
    """Admin confirms payment: marks paid + activates rental."""
    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals "
                "SET payment_status = 'paid', paid_at = ?, status = 'active' "
                "WHERE id = ? "
                "AND status = 'approved' "
                "AND payment_status = 'pending' "
                "AND payment_method IN ('cash', 'card')",
                (datetime.now(timezone.utc).isoformat(), rental_id),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def reject_rental_payment(rental_id: int, admin_id: int) -> bool:
    """Admin rejects payment: keeps rental approved, resets payment fields."""
    def _op() -> bool:
        conn = _get_conn()
        try:
            cur = conn.execute(
                "UPDATE rentals "
                "SET payment_method = NULL, payment_status = 'pending', paid_at = NULL "
                "WHERE id = ? AND status = 'approved' AND payment_status = 'pending'",
                (rental_id,),
            )
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()

    return bool(_write_retry(_op))


def list_top_renters(limit: int = 10) -> list[dict[str, Any]]:
    """Users with most rentals (approved, active, returned). Returns [{user_id, rental_count}, ...]."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT user_id, COUNT(*) AS rental_count "
            "FROM rentals "
            "WHERE status IN ('approved', 'active', 'returned') "
            "GROUP BY user_id ORDER BY rental_count DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def list_users_not_returned(now_iso: str) -> list[dict[str, Any]]:
    """Users with overdue rentals (qaytarmaganlar). status IN (approved,active), due_ts < now.
    Returns [{user_id, overdue_count, book_titles, ...}]."""
    now_date = now_iso[:10] if now_iso else ""
    if not now_date:
        return []
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.user_id, COUNT(*) AS overdue_count, "
            "GROUP_CONCAT(b.title, '; ') AS book_titles "
            "FROM rentals r JOIN books b ON r.book_id = b.id "
            "WHERE r.status IN ('approved', 'active') "
            "AND r.due_ts IS NOT NULL AND r.due_ts != '' AND r.due_ts < ? "
            "GROUP BY r.user_id ORDER BY overdue_count DESC",
            (now_date,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_all_books_for_export() -> list[dict[str, Any]]:
    """All books for export (no limit)."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT * FROM books ORDER BY id")
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_all_rentals_for_export() -> list[dict[str, Any]]:
    """All rentals with book info for export."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT r.*, b.title AS book_title, b.author AS book_author "
            "FROM rentals r JOIN books b ON r.book_id = b.id ORDER BY r.id"
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_broadcast_user_ids(exclude_admin_ids: Optional[Any] = None) -> list[int]:
    """Distinct user_ids from rentals for broadcast. Excludes admins if set provided."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT DISTINCT user_id FROM rentals ORDER BY user_id")
        ids = [row[0] for row in cur.fetchall()]
        if exclude_admin_ids:
            ids = [uid for uid in ids if uid not in exclude_admin_ids]
        return ids
    finally:
        conn.close()


def list_blacklist_users(now_iso: str, min_overdue_count: int = 3) -> list[dict[str, Any]]:
    """Users with >= min_overdue_count overdue incidents (blacklist).
    Overdue = returned late (returned_at > due_ts) OR currently overdue (due_ts < now)."""
    now_date = now_iso[:10] if now_iso else ""
    if not now_date:
        return []
    conn = _get_conn()
    try:
        cur = conn.execute(
            "SELECT user_id, COUNT(*) AS overdue_count FROM ("
            "  SELECT user_id FROM rentals "
            "  WHERE status = 'returned' AND returned_at IS NOT NULL AND returned_at > due_ts "
            "  UNION ALL "
            "  SELECT user_id FROM rentals "
            "  WHERE status IN ('approved','active') AND due_ts IS NOT NULL AND due_ts != '' AND due_ts < ?"
            ") t GROUP BY user_id HAVING COUNT(*) >= ? ORDER BY overdue_count DESC",
            (now_date, min_overdue_count),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()
