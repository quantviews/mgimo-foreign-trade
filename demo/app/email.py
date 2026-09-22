"""Verification email. Real SMTP when configured; otherwise log the link (dev)."""

from __future__ import annotations

import asyncio
import logging
import smtplib
from email.message import EmailMessage

from .config import settings

logger = logging.getLogger("demo.email")


def _build(to: str, link: str) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = "Демо-доступ к данным о внешней торговле (МГИМО / ИЭФ)"
    msg["From"] = settings.mail_from
    msg["To"] = to
    msg.set_content(
        "Здравствуйте!\n\n"
        "Вы запросили демо-доступ к ИИ-ассистенту по данным о внешней торговле "
        "(проект МГИМО и ИЭФ). Чтобы подтвердить адрес и открыть чат, перейдите "
        f"по ссылке (действует {settings.verify_ttl_hours} ч):\n\n{link}\n\n"
        "Если вы не запрашивали доступ, просто проигнорируйте это письмо.\n"
    )
    return msg


def _send_sync(msg: EmailMessage) -> None:
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as s:
        if settings.smtp_starttls:
            s.starttls()
        if settings.smtp_user:
            s.login(settings.smtp_user, settings.smtp_password)
        s.send_message(msg)


async def send_verification(to: str, link: str) -> None:
    if not settings.smtp_host:
        logger.warning("SMTP not configured; verification link for %s: %s", to, link)
        return
    await asyncio.to_thread(_send_sync, _build(to, link))
