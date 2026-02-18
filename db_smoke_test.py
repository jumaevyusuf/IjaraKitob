"""Minimal SQLite smoke test (no secrets).

Creates a temporary DB, initializes schema, inserts:
- 1 book with qty=1
- 2 rentals with status=requested for that book
Then tries to approve both; only one should succeed.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path


def main() -> int:
    # Import db from repo root
    repo_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(repo_root))
    import db  # noqa: E402

    with tempfile.TemporaryDirectory() as td:
        tmp_db = Path(td) / "test.db"
        db.DB_PATH = tmp_db

        # Ensure deterministic pragmas (optional)
        os.environ.setdefault("DB_TIMEOUT", "10")

        db.init_db()

        book_id = db.add_book(
            title="Test Book",
            author="Tester",
            category="test",
            rent_fee=1000,
            deposit=0,
            qty=1,
            year=2024,
            publisher="",
            cover_type="yumshoq",
            photo_id=None,
        )

        r1 = db.create_rental_request(user_id=101, book_id=book_id, due_ts="2099-01-01")
        r2 = db.create_rental_request(user_id=102, book_id=book_id, due_ts="2099-01-01")

        ok1, reason1 = db.approve_rental_if_available(r1, admin_id=1)
        ok2, reason2 = db.approve_rental_if_available(r2, admin_id=1)

        ok_count = int(ok1) + int(ok2)
        if ok_count != 1:
            print("FAIL: expected exactly one approval")
            print(f"  r1: ok={ok1} reason={reason1}")
            print(f"  r2: ok={ok2} reason={reason2}")
            return 1

        if ok1:
            assert reason2 in ("not_available", "wrong_status", "locked"), reason2
        if ok2:
            assert reason1 in ("not_available", "wrong_status", "locked"), reason1

        print("PASS")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

