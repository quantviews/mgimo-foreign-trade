"""Corporate-domain gate.

Access is auto-granted, so we accept only work email: everything that is not a
known free-mail or disposable provider counts as corporate. An optional allowlist
overrides the blocklist for named domains.
"""

from __future__ import annotations

import re

from .config import settings

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Free consumer mail and common disposable providers. Not exhaustive; extend via
# MGIMO_DEMO_EXTRA_FREE_DOMAINS. The point is to keep out casual signups, not to
# be a perfect filter.
FREE_DOMAINS = {
    # global consumer
    "gmail.com", "googlemail.com", "yahoo.com", "ymail.com", "rocketmail.com",
    "outlook.com", "hotmail.com", "live.com", "msn.com", "icloud.com", "me.com",
    "mac.com", "aol.com", "gmx.com", "gmx.net", "mail.com", "zoho.com",
    "protonmail.com", "proton.me", "tutanota.com", "fastmail.com",
    # russian consumer
    "yandex.ru", "yandex.com", "ya.ru", "yandex.by", "yandex.kz",
    "mail.ru", "bk.ru", "inbox.ru", "list.ru", "internet.ru", "xmail.ru",
    "rambler.ru", "ro.ru", "lenta.ru", "autorambler.ru",
    # chinese consumer
    "qq.com", "163.com", "126.com", "sina.com", "sohu.com", "foxmail.com",
    # disposable / temporary
    "mailinator.com", "10minutemail.com", "guerrillamail.com", "sharklasers.com",
    "temp-mail.org", "tempmail.com", "trashmail.com", "getnada.com", "nada.email",
    "dropmail.me", "yopmail.com", "throwawaymail.com", "maildrop.cc", "mohmal.com",
}


def _csv(value: str) -> set[str]:
    return {d.strip().lower() for d in value.split(",") if d.strip()}


def domain_of(email: str) -> str:
    return email.rsplit("@", 1)[-1].strip().lower() if "@" in email else ""


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match((email or "").strip()))


def is_corporate(email: str) -> bool:
    """True if the address is a plausible work address (not free/disposable)."""
    d = domain_of(email)
    if not d or "." not in d:
        return False
    if d in _csv(settings.allow_domains):
        return True
    return d not in (FREE_DOMAINS | _csv(settings.extra_free_domains))
