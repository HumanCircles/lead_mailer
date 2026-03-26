import os
import time
import random
import smtplib
import itertools
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST   = os.getenv("SMTP_HOST", "mail.recruitagents.net")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "465"))
FROM_NAME   = os.getenv("FROM_NAME", "HireQuotient")
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "150"))


def _load_pool() -> list[tuple[str, str]]:
    raw = os.getenv("SENDER_POOL", "").strip()
    if not raw:
        raise ValueError("SENDER_POOL is not set in .env")
    pairs = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            email, pwd = entry.split(":", 1)
            pairs.append((email.strip(), pwd.strip()))
    if not pairs:
        raise ValueError("SENDER_POOL has no valid email:password entries")
    return pairs


_pool     = _load_pool()
_cycler   = itertools.cycle(_pool)
_counters: dict[str, int] = {e: 0 for e, _ in _pool}


def _next_sender() -> tuple[str, str]:
    for _ in range(len(_pool)):
        email, pwd = next(_cycler)
        if _counters[email] < DAILY_LIMIT:
            return email, pwd
    raise RuntimeError("All sender accounts have hit the daily limit.")


def send_email(to_email: str, subject: str, body: str) -> str:
    sender_email, app_password = _next_sender()

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{FROM_NAME} <{sender_email}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
        server.login(sender_email, app_password)
        server.sendmail(sender_email, to_email, msg.as_string())

    _counters[sender_email] += 1
    time.sleep(random.uniform(1.5, 4.0))
    return sender_email
