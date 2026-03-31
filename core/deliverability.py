"""Suppression list, unsubscribe footer, and List-Unsubscribe headers for SMTP outbound mail."""

from __future__ import annotations

import os
import re
from email.message import Message
from email.utils import formataddr
from urllib.parse import quote

from dotenv import load_dotenv

load_dotenv()

_suppression_set: frozenset[str] | None = None


def load_suppression_set(path: str | None = None) -> set[str]:
    if path is None:
        path = os.getenv("SUPPRESSION_FILE", "suppression.txt").strip() or "suppression.txt"
    out: set[str] = set()
    if not os.path.isfile(path):
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.add(line.lower())
    return out


def get_suppression_set() -> frozenset[str]:
    global _suppression_set
    if _suppression_set is None:
        _suppression_set = frozenset(load_suppression_set())
    return _suppression_set


def is_suppressed(email: str) -> bool:
    e = email.strip().lower()
    if not e:
        return False
    return e in get_suppression_set()


def strip_control_chars(s: str) -> str:
    """Strip NUL and other C0 controls that can break JSON bodies for LLM/API requests."""
    if not s:
        return ""
    return "".join(c for c in s if ord(c) >= 32 or c in "\n\r\t")


def _display_name_from_email(sender_email: str) -> str:
    local = sender_email.strip().split("@", 1)[0]
    parts = re.split(r"[._\-]", local)
    base = parts[0] if parts and parts[0] else local
    if not base:
        return "Sender"
    return base[:1].upper() + base[1:].lower()


def smtp_from_header(display_name: str | None, sender_email: str) -> str:
    """RFC 5322 From value. Never emits a malformed ` <addr>` when display name is empty."""
    addr = sender_email.strip()
    name = (display_name or "").strip()
    if not name:
        name = _display_name_from_email(addr)
    return formataddr((name, addr))


def _signature_name_from_email(sender_email: str) -> str:
    """Derive a readable signature name from sender local-part."""
    local = sender_email.strip().split("@", 1)[0]
    if not local:
        return ""
    parts = [p for p in re.split(r"[._\-]+", local) if p]
    if not parts:
        return ""
    return " ".join(p[:1].upper() + p[1:].lower() for p in parts)


def append_unsubscribe_footer(body: str) -> str:
    raw = os.getenv("UNSUBSCRIBE_FOOTER_ENABLED", "true").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return body
    addr = (os.getenv("UNSUBSCRIBE_MAILTO") or os.getenv("UNSUBSCRIBE_EMAIL") or "").strip()
    if addr:
        subj = quote("Unsubscribe", safe="")
        line = f"\n\nTo opt out of future messages: mailto:{addr}?subject={subj}"
        return body.rstrip() + line
    return body.rstrip() + "\n\nTo opt out, reply with the word unsubscribe."


def append_signature_block(body: str, sender_email: str = "") -> str:
    """Append a plain-text signature block for consistent professional formatting."""
    enabled = os.getenv("SIGNATURE_ENABLED", "true").strip().lower()
    if enabled in ("0", "false", "no", "off"):
        return body.rstrip()

    name_env = (os.getenv("SIGNATURE_NAME") or "").strip()
    name = name_env or _signature_name_from_email(sender_email)
    company = (os.getenv("SIGNATURE_COMPANY") or "").strip()
    email = (os.getenv("SIGNATURE_EMAIL") or "").strip()
    title = (os.getenv("SIGNATURE_TITLE") or "").strip()
    closing = (os.getenv("SIGNATURE_CLOSING") or "Best,").strip()

    lines = [closing, name]
    if title:
        lines.append(title)
    lines.append(company)
    if email:
        lines.append(email)
    signature = "\n".join(lines)
    return body.rstrip() + f"\n\n{signature}"


def apply_list_unsubscribe_headers(msg: Message) -> None:
    parts: list[str] = []
    addr = (os.getenv("UNSUBSCRIBE_MAILTO") or os.getenv("UNSUBSCRIBE_EMAIL") or "").strip()
    if addr:
        subj = quote("Unsubscribe", safe="")
        parts.append(f"<mailto:{addr}?subject={subj}>")
    url = os.getenv("UNSUBSCRIBE_URL", "").strip()
    if url:
        parts.append(f"<{url}>")
    if parts:
        msg["List-Unsubscribe"] = ", ".join(parts)
