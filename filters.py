"""Custom filters for kitob ijara bot."""
from datetime import datetime
from typing import Any, Union

import logging

from aiogram.enums import ChatType
from aiogram.filters import BaseFilter
from aiogram.types import CallbackQuery, Message

from config import is_admin

logger = logging.getLogger(__name__)


def log_blocked(user_id: int, action: str, detail: str = "") -> None:
    """Log blocked non-admin attempt. No secrets."""
    logger.warning(
        "Admin access blocked: user_id=%s action=%s detail=%s ts=%s",
        user_id, action, (detail or "")[:80], datetime.now().isoformat(),
    )


def _is_private_chat(event: Union[Message, CallbackQuery]) -> bool:
    """Check if event is from a private chat."""
    if isinstance(event, Message):
        return getattr(event.chat, "type", None) == ChatType.PRIVATE
    if isinstance(event, CallbackQuery) and event.message:
        return getattr(event.message.chat, "type", None) == ChatType.PRIVATE
    return False


class AdminOnly(BaseFilter):
    """Filter: admin users only, private chat only. Rejects with friendly message."""

    async def __call__(self, event: Union[Message, CallbackQuery], **kwargs: Any) -> bool:
        user_id = event.from_user.id if event.from_user else None
        if user_id is None:
            return False
        if not is_admin(user_id):
            action = "callback" if isinstance(event, CallbackQuery) else "message"
            detail = getattr(event, "data", None) or getattr(event, "text", None) or ""
            log_blocked(user_id, action, str(detail))
            if isinstance(event, CallbackQuery):
                await event.answer("Ruxsat yo'q.", show_alert=True)
            else:
                await event.answer("Bu buyruq faqat admin uchun.")
            return False
        if not _is_private_chat(event):
            if isinstance(event, CallbackQuery):
                await event.answer("Admin panel faqat shaxsiy chatda.", show_alert=True)
            else:
                await event.answer("Admin panel faqat shaxsiy chatda ishlaydi.")
            return False
        return True
