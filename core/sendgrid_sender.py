import os
import json
import itertools
import requests
from dotenv import load_dotenv

load_dotenv()

_cfg_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")
with open(_cfg_path) as _f:
    _cfg = json.load(_f)

SENDGRID_API_KEY = _cfg.get("sendgrid_api_key") or os.getenv("SENDGRID_API_KEY", "")
FROM_NAME        = _cfg.get("from_name", "HireQuotient")

# Build sender pool from config or fall back to SENDER_POOL in .env
def _load_from_pool() -> list[str]:
    pool = _cfg.get("from_email_pool", [])
    if pool:
        return pool
    raw = os.getenv("SENDER_POOL", "")
    if raw:
        return [entry.split(":")[0].strip() for entry in raw.split(",") if ":" in entry]
    single = _cfg.get("from_email") or os.getenv("SENDER_EMAIL", "")
    if single:
        return [single]
    raise ValueError("Set from_email_pool in config.json or SENDER_POOL in .env")

_pool   = _load_from_pool()
_cycler = itertools.cycle(_pool)


def _next_from() -> str:
    return next(_cycler)


def send_email(to_email: str, subject: str, body: str) -> str:
    from_email = _next_from()

    payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": from_email, "name": FROM_NAME},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}],
    }

    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type":  "application/json",
        },
        json=payload,
        timeout=30,
    )

    if resp.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid {resp.status_code}: {resp.text[:200]}")

    return from_email
