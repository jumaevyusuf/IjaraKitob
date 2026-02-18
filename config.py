"""Bot config: admin IDs and helpers. Requires .env to be loaded by main.py first."""
import logging
import os
from typing import Set

logger = logging.getLogger(__name__)

_env_ids = os.getenv("ADMIN_IDS", "").strip()
_env_id = os.getenv("ADMIN_ID", "").strip()
if _env_ids:
    _env_raw = _env_ids
    _used_key = "ADMIN_IDS"
elif _env_id:
    _env_raw = _env_id
    _used_key = "ADMIN_ID"
else:
    _env_raw = ""
    _used_key = None

_env_admins = [x.strip() for x in _env_raw.split(",") if x.strip()] if _env_raw else []
_admin_set: Set[int] = set()
for a in _env_admins:
    try:
        _admin_set.add(int(a))
    except ValueError:
        continue

ADMIN_IDS: frozenset[int] = frozenset(_admin_set)

if _used_key:
    logger.info("Admin config: using %s (admins=%d)", _used_key, len(ADMIN_IDS))
else:
    logger.warning("ADMIN_IDS not set; admin features disabled.")


def is_admin(user_id: int) -> bool:
    """Single source of truth for admin check. Returns False when ADMIN_IDS is empty."""
    return user_id in ADMIN_IDS
