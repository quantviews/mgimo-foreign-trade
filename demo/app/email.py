"""Verification and notification email with primary and optional backup SMTP."""

from __future__ import annotations

import asyncio
import logging
import smtplib
import ssl
from copy import deepcopy
from email.message import EmailMessage

from .config import settings

logger = logging.getLogger("demo.email")


def _build(to: str, link: str) -> EmailMessage:
    validity = ("7 дней" if settings.verify_ttl_hours == 168
                else f"{settings.verify_ttl_hours} ч")
    msg = EmailMessage()
    msg["Subject"] = "Демо-доступ к данным о внешней торговле (МГИМО / ИЭФ)"
    msg["From"] = settings.mail_from
    msg["To"] = to
    msg.set_content(
        "Здравствуйте!\n\n"
        "Вы запросили демо-доступ к ИИ-ассистенту по данным о внешней торговле "
        "(проект МГИМО и ИЭФ). Чтобы подтвердить адрес и открыть чат, перейдите "
        f"по ссылке (действует {validity}, используется один раз):\n\n{link}\n\n"
        "Если вы не запрашивали доступ, просто проигнорируйте это письмо.\n"
    )
    return msg


def _send_sync(msg: EmailMessage, *, fallback: bool = False) -> None:
    host = settings.fallback_smtp_host if fallback else settings.smtp_host
    port = settings.fallback_smtp_port if fallback else settings.smtp_port
    user = settings.fallback_smtp_user if fallback else settings.smtp_user
    password = settings.fallback_smtp_password if fallback else settings.smtp_password
    if not host or not password:
        raise RuntimeError("SMTP not configured")
    if fallback:
        msg = deepcopy(msg)
        msg.replace_header("From", user)
        if not msg.get("Reply-To"):
            msg["Reply-To"] = settings.mail_from
    # Port 465 = implicit SSL (Yandex, Gmail); otherwise plain + STARTTLS on 587.
    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=10, context=ssl.create_default_context()) as s:
            if user:
                s.login(user, password)
            s.send_message(msg)
        return
    with smtplib.SMTP(host, port, timeout=10) as s:
        if fallback or settings.smtp_starttls:
            s.starttls(context=ssl.create_default_context())
        if user:
            s.login(user, password)
        s.send_message(msg)


async def _deliver(msg: EmailMessage) -> bool:
    for fallback in (False, True):
        if fallback and not (settings.fallback_smtp_user and settings.fallback_smtp_password):
            break
        provider = "fallback" if fallback else "primary"
        try:
            await asyncio.to_thread(_send_sync, msg, fallback=fallback)
            logger.info("SMTP accepted provider=%s to=%s", provider, msg["To"])
            return True
        except smtplib.SMTPRecipientsRefused:
            logger.warning("SMTP recipient refused provider=%s to=%s", provider, msg["To"])
            return False
        except (OSError, smtplib.SMTPException, RuntimeError) as exc:
            # Never log message bodies, verification links or SMTP credentials.
            logger.warning("SMTP failed provider=%s to=%s error=%s", provider, msg["To"], type(exc).__name__)
    return False


async def send_verification(to: str, link: str) -> bool:
    return await _deliver(_build(to, link))


async def send_admin_notice(applicant: str, org: str | None, existing: bool) -> None:
    """Notify the team of each demo registration. Best-effort; never raises."""
    if not settings.notify_email:
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
    await _deliver(msg)
