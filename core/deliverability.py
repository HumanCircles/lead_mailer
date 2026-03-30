"""Suppression list, unsubscribe footer, and List-Unsubscribe headers for SMTP outbound mail."""

from __future__ import annotations

import os
from email.message import Message
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
