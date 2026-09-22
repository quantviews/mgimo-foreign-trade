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
    # Port 465 = implicit SSL (Yandex, Gmail); otherwise plain + STARTTLS on 587.
    if settings.smtp_port == 465:
        with smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port, timeout=30) as s:
            if settings.smtp_user:
                s.login(settings.smtp_user, settings.smtp_password)
            s.send_message(msg)
        return
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=30) as s:
        if settings.smtp_starttls:
            s.starttls()
        if settings.smtp_user:
            s.login(settings.smtp_user, settings.smtp_password)
        s.send_message(msg)


async def send_verification(to: str, link: str) -> None:
    """Best-effort: a mail failure must not break registration; log the link."""
    if not settings.smtp_host or not settings.smtp_password:
        logger.warning("SMTP not fully configured; verification link for %s: %s", to, link)
        return
    try:
        await asyncio.to_thread(_send_sync, _build(to, link))
    except Exception as e:  # noqa: BLE001
        logger.warning("SMTP send failed (%s); verification link for %s: %s", e, to, link)


async def send_admin_notice(applicant: str, org: str | None, existing: bool) -> None:
    """Notify the team of each demo registration. Best-effort; never raises."""
    if not settings.notify_email:
        return
    if not settings.smtp_host or not settings.smtp_password:
        logger.warning("admin notice (no SMTP) for demo registration: %s", applicant)
        return
    msg = EmailMessage()
    msg["Subject"] = f"Демо-регистрация: {applicant}"
    msg["From"] = settings.mail_from
    msg["To"] = settings.notify_email
    domain = applicant.rsplit("@", 1)[-1] if "@" in applicant else ""
    msg.set_content(
        "Новая демо-регистрация ИИ-ассистента.\n\n"
        f"Адрес: {applicant}\n"
        f"Организация: {org or '-'}\n"
        f"Домен: {domain}\n"
        f"Статус: {'существующий пользователь' if existing else 'новый'}\n"
    )
    try:
        await asyncio.to_thread(_send_sync, msg)
    except Exception as e:  # noqa: BLE001
        logger.warning("admin notice send failed (%s) for %s", e, applicant)
