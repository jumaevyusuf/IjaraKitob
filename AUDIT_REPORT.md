# KitobIjara Telegram Rental Bot — Full Audit Report

## 1) Overall Score: **7.5 / 10**

**Justification:** The bot has a solid core: admin protection, rental flow, overdue handling, penalty system, export, broadcast, and user stats. Main gaps: (1) race condition on approval when stock is 1 and multiple admins approve concurrently; (2) no DB connection pooling or explicit timeouts; (3) `rental_` callback has no handler (dead button); (4) `.env` file risk if committed; (5) broadcast users limited to those with rentals only.

---

## 2) Top 15 Issues (Ranked by Severity)

### Critical

#### 1. Race condition: stock oversell on concurrent approval
- **Where:** `main.py` — `cb_rental_ok` (lines ~2135–2168)
- **Why:** Two admins approve two requests for the last copy almost simultaneously. Both pass `stock.get("available", 0) <= 0` and both call `set_rental_status(..., "approved")`, leading to over-allocation.
- **Fix:** Use a single atomic UPDATE that only approves when stock is available. In `db.py`:
```python
def approve_rental_if_available(rental_id: int, start_ts: str) -> tuple[bool, bool]:
    """Approve only if book still has stock. Returns (approved, found)."""
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT 1 FROM rentals WHERE id = ? AND status = 'requested'", (rental_id,))
        if cur.fetchone() is None:
            return False, False
        cur = conn.execute("""
            UPDATE rentals SET status = 'approved', start_ts = ?
            WHERE id = ? AND status = 'requested'
            AND (SELECT b.qty - COALESCE((SELECT COUNT(*) FROM rentals r2 
                WHERE r2.book_id = rentals.book_id AND r2.status IN ('approved','active')), 0)
                FROM books b WHERE b.id = rentals.book_id) > 0
        """, (start_ts, rental_id))
        conn.commit()
        return cur.rowcount > 0, True
    finally:
        conn.close()
```
In `cb_rental_ok`, call this instead of the separate stock check + set_rental_status.

#### 2. BOT_TOKEN and .env exposure risk
- **Where:** `main.py` (lines 2528–2539), `.env` present in project
- **Why:** Logging token prefix/suffix; `.env` may be committed; token could leak in errors.
- **Fix:** (a) Add `.env` to `.gitignore` if not already; (b) avoid logging any token characters; (c) catch and mask token in `except` blocks before re-raising.

---

### High

#### 3. Callback `rental_{id}` has no handler
- **Where:** `main.py` — `admin_rentals_keyboard` (line 391) uses `callback_data=f"rental_{r['id']}"`
- **Why:** Button does nothing; user/admin taps and gets no response or fallback.
- **Fix:** Either add `cb_rental_detail` handler, or change to `noop`:
```python
# Change to: callback_data=f"noop"  # or add cb_rental_detail
```

#### 4. No validation that book exists before creating rental
- **Where:** `db.py` — `create_rental_request` (lines 406–418)
- **Why:** Malformed callback could insert rental for non-existent `book_id`; FK may not prevent it if not enforced.
- **Fix:** Add existence check:
```python
cur = conn.execute("SELECT 1 FROM books WHERE id = ?", (book_id,))
if cur.fetchone() is None:
    return 0  # or raise
```

#### 5. SQLite connections not pooled; possible lock contention
- **Where:** `db.py` — `_get_conn()` opens new connection per call (line 12)
- **Why:** Under load, many short-lived connections and no `timeout` can cause `database is locked`.
- **Fix:** Add timeout and consider a small pool or single connection per thread:
```python
def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn
```

---

### Medium

#### 6. Non-admin can trigger `rental_ok_` / `rental_no_` if they get the message
- **Where:** `main.py` — `cb_rental_ok`, `cb_rental_no` use `AdminOnly()`
- **Why:** AdminOnly is correctly applied; risk is low because only admins receive the message. If message is forwarded, non-admin gets "Ruxsat yo'q."
- **Status:** Mitigated. No change needed unless you want extra checks (e.g. `rental.user_id` or `chat_id`).

#### 7. `period_` callback parsing fragile
- **Where:** `main.py` — `cb_rental_period` (lines 663–665): `_, book_id_str, days_str = data.split("_")`
- **Why:** `period_5_7` works; `period_5_7_extra` would break; `period_1_2_3` (book_id=1, days=2) could be confused.
- **Fix:** Stricter parsing:
```python
parts = data.split("_")
if len(parts) != 3:
    await callback.answer("Xatolik.")
    return
_, book_id_str, days_str = parts
```

#### 8. Broadcast targets only users with rentals
- **Where:** `db.py` — `get_broadcast_user_ids` uses `SELECT DISTINCT user_id FROM rentals`
- **Why:** Users who only did `/start` are never reached.
- **Fix:** Add `bot_users` table populated on `/start`, or document this limitation clearly.

#### 9. Reminder loop has no jitter; possible burst
- **Where:** `main.py` — `reminder_loop` (lines 2343–2409)
- **Why:** All reminders fire at the same time each hour; could cause temporary API rate spikes.
- **Fix:** Add small random delay per user:
```python
await asyncio.sleep(random.uniform(0, 30))
await bot.send_message(...)
```

#### 10. Missing `photo_id` handling in `cb_rent_book`
- **Where:** `main.py` — `cb_rent_book` (lines 650–654)
- **Why:** If `kb` is None (out of stock), `answer_photo` still needs `reply_markup`. Code passes `kb` correctly; but if `photo_id` is empty string, `get("photo_id")` is falsy and falls to `answer`—acceptable.
- **Status:** Low risk. Ensure `photo_id` is never `""` when it should be None.

---

### Low

#### 11. `wipe_all` deletes all data with no confirmation
- **Where:** `main.py` — `cb_wipe_confirm`, `db.py` — `wipe_all`
- **Why:** Admin can wipe everything with one action; already has confirmation step.
- **Status:** Acceptable. Optional: add "Type WIPE to confirm" for extra safety.

#### 12. `edit_book_photo_reject` doesn’t return to edit menu
- **Where:** `main.py` — `edit_book_photo_reject` (line 2297)
- **Why:** User stays in photo state; must send a photo to proceed.
- **Fix:** Add "Skip" or "Back" option so user can leave the state.

#### 13. Export CSV uses single file with two sections
- **Where:** `main.py` — `_export_to_csv` (lines 970–985)
- **Why:** Combines books and rentals in one CSV; some tools prefer separate files.
- **Fix:** Optional: send two files (`books.csv`, `rentals.csv`) or a ZIP.

#### 14. FSM state not cleared on some errors
- **Where:** Various FSM handlers (e.g. `add_book_rent_fee` on invalid input)
- **Why:** User can get stuck in a state if validation fails repeatedly.
- **Fix:** On repeated failures (e.g. 3 times), clear state and offer "Bekor" or restart.

#### 15. `_admin_books_filter` and `_add_book_last` never pruned
- **Where:** `main.py` — in-memory dicts (lines 69–75)
- **Why:** Under many admins, memory grows slowly; low impact for single admin.
- **Fix:** Optionally cap size or add TTL for old entries.

---

## 3) User Journey Review

| Step | Flow | Status | Notes |
|------|------|--------|-------|
| /start | Greeting + main menu | OK | Clear, minimal |
| Browse | Kitoblar → category/search | OK | Pagination, sort, search work |
| Rent | Tap book → period → request | OK | Stock checked at rent and period |
| Approve | Admin gets message, taps Tasdiqlash | OK | Stock rechecked; race risk (see #1) |
| Reject | Admin taps Rad etish | OK | User notified |
| Return | Admin taps Qaytarildi | OK | User gets penalty in message |
| Overdue | Admin sees Kechikkanlar | OK | Pagination, penalty, reminders |
| Reminder | due_1day, overdue_daily | OK | Once/day per type via `rental_notifications` |
| My rentals | Mening ijaralarim | OK | Shows status, due date, penalty if overdue |

**Missing/Confusing:**
- No explicit "request pending" state in user flow; user only sees "So'rovingiz yuborildi."
- No way to cancel a pending request.
- "Mening ijaralarim" doesn’t distinguish "requested" vs "approved" vs "active" clearly for non-technical users.
- No in-app "contact admin" action.

---

## 4) Admin Journey Review

| Step | Flow | Status | Notes |
|------|------|--------|-------|
| /admin | Admin menu | OK | ReplyKeyboard |
| Add book | Tez qo'shish template | OK | Category/cover memory, rent quick buttons |
| Edit book | Kitoblarim → Edit | OK | Title, fee, qty, photo |
| Delete book | Kitoblarim → Delete | OK | Blocked if active rentals |
| Approve/Reject | Ijaralar or inline | OK | Admin messages with buttons |
| Return | Ijaralar or Kechikkanlar | OK | Marks returned, notifies user |
| Overdue | Kechikkanlar | OK | Pagination, Eslatma, Jarima, Qaytarildi |
| Export | Export → CSV/JSON | OK | Backup of books + rentals |
| Broadcast | E'lon | OK | Bekor, cheklov applied |
| Stats | Userlar statistikasi | OK | Top renters, qaytarmaganlar, blacklist |

**Friction:**
- "Ijaralar" lists all pending; no filter for "requested" only.
- No bulk actions (e.g. approve all).
- No way to edit rental due date after approval.
- Penalty edit is separate from overdue list; flow is acceptable but could be streamlined.

---

## 5) Security Checklist

| Check | Result |
|-------|--------|
| Only admin can modify DB | Yes — add/edit/delete book, approve/reject, return, wipe, penalty, export, broadcast all use `AdminOnly()` |
| Delete book with active rentals blocked | Yes — `db.delete_book` checks `has_active_rentals` first |
| Stock check on request | Yes — `cb_rent_book`, `cb_rental_period` check `available` |
| Stock check on approval | Yes — `cb_rental_ok` checks before `set_rental_status` |
| Race condition on approval | No — concurrent approvals can oversell (see issue #1) |
| Reminders once/day | Yes — `can_send_notification` uses `last_sent_date` vs `today_str` |
| Callbacks admin-only | Yes — rental_ok, rental_no, rental_return, penalty_*, overdue_*, admin_*全部 use AdminOnly |
| Private chat only | Yes — `AdminOnly` checks `_is_private_chat` |
| Token not logged | Partial — token prefix/suffix logged; full token not |

---

## 6) Database Review

### Table summary

| Table | Purpose |
|-------|---------|
| books | id, title, author, category, rent_fee, deposit, qty, year, publisher, cover_type, photo_id, sort_order, language, created_at |
| rentals | id, user_id, book_id, status, start_ts, due_ts, created_at, returned_at, closed_by_admin_id, penalty_* |
| rental_notifications | rental_id, notif_type, last_sent_date (UNIQUE) |
| bot_settings | key, value (penalty_per_day etc.) |

### Recommended indexes

```sql
CREATE INDEX IF NOT EXISTS idx_rentals_book_id ON rentals(book_id);
CREATE INDEX IF NOT EXISTS idx_rentals_status ON rentals(status);
CREATE INDEX IF NOT EXISTS idx_rentals_due_ts ON rentals(due_ts);
CREATE INDEX IF NOT EXISTS idx_rentals_user_id ON rentals(user_id);
CREATE INDEX IF NOT EXISTS idx_rentals_book_status ON rentals(book_id, status);  -- for stock count
CREATE INDEX IF NOT EXISTS idx_books_category ON books(category);
```

---

## 7) Prioritized Improvements

### Quick wins (5)

1. Add `rental_{id}` handler or change to `noop` — avoid dead button.
2. Add `timeout=10.0` to `sqlite3.connect` in `db.py`.
3. Add `.env` to `.gitignore` and ensure it’s never committed.
4. Add recommended indexes to `db.py` migrations.
5. Add book existence check in `create_rental_request`.

### Larger improvements (3)

1. Fix approval race: atomic approve-if-available in DB (see issue #1).
2. Track `/start` users for broadcast: add `bot_users` table and populate on start.
3. Import/restore from JSON backup: allow admin to restore from exported JSON.

---

## Regression Test Checklist (Manual)

### Admin permissions
1. Non-admin sends `/admin` → receives "Bu buyruq faqat admin uchun."
2. Non-admin taps "📚 Kitoblarim" (if exposed) → receives "Ruxsat yo'q" or equivalent.
3. Non-admin sends "➕ Kitob qo'shish" → receives "Bu buyruq faqat admin uchun."
4. Admin sends `/admin` → sees admin menu.
5. Non-admin in group chat taps admin callback (if any) → receives "Admin panel faqat shaxsiy chatda."

### Add / Edit / Delete books
6. Admin adds book with all fields → book appears in Kitoblarim.
7. Admin adds book with "Bekor" at search/category → flow cancels.
8. Admin edits book title → change reflected in list.
9. Admin edits book qty to 0 → validation rejects (qty >= 1).
10. Admin deletes book with no rentals → book removed.
11. Admin tries to delete book with active rental → blocked with message.
12. Admin uses rent quick buttons (10k/15k/20k) → rent_fee set correctly.

### Inventory and availability
13. Book with qty=1, 1 active rental → shows "Mavjud emas", no rent button.
14. Book with qty=2, 1 active rental → shows "Mavjud: 1/2", rent button visible.
15. User rents last copy → next user sees "Mavjud emas" on same book.

### Rent request, approval, rejection
16. User requests rental → sees "So'rovingiz yuborildi"; admin receives request.
17. Admin approves → user receives "Ijara tasdiqlandi" with due date.
18. Admin rejects → user receives "Ijara rad etildi."
19. Admin approves when stock=0 → receives "Kitob qolmadi"; user notified.
20. User requests for out-of-stock book (if UI allows) → request blocked or rejected.

### Return flow
21. Admin marks rental "Qaytarildi" → user receives message; rental status = returned.
22. Admin marks returned with penalty → user message includes "Yakuniy jarima: X so'm."
23. Admin marks returned with no penalty → user message omits penalty line.

### Overdue and reminders
24. Overdue rental appears in Kechikkanlar; reminder sent once per day per type.
25. Admin uses "💸 Jarima" from overdue list → can edit penalty, toggle, set fixed.

### Edge cases
26. Rental with missing/invalid due_ts → compute_penalty handles gracefully.
27. Book with no photo → rent flow works; no photo shown.
28. Two admins approve two requests for last copy (race) → ideally only one succeeds (requires fix #1).
29. Export CSV → file received with books and rentals sections.
30. Export JSON → file received with `books` and `rentals` keys.
31. Broadcast with text → users receive message; "Bekor qilish" cancels.
32. Admin filter: search "Orwell" → only matching books shown.
33. Admin filter: "Mavjud emas" → only out-of-stock books shown.
