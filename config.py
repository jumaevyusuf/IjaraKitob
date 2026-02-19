"""Bot config: single admin only.

Hard-lock admin access to one Telegram ID to avoid misconfiguration risks.
"""

ADMIN_ID: int = 7700265732
ADMIN_IDS: frozenset[int] = frozenset({ADMIN_ID})


def is_admin(user_id: int) -> bool:
    return int(user_id) == ADMIN_ID
