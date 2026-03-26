import os
import time
import random
import smtplib
import itertools
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT_PER_ACCOUNT", "150"))


def _load_pool() -> list[tuple[str, str]]:
    raw = os.getenv("SENDER_POOL", "").strip()
    if raw:
        pairs: list[tuple[str, str]] = []
        for entry in raw.split(","):
            entry = entry.strip()
            if ":" in entry:
                email, pwd = entry.split(":", 1)
                pairs.append((email.strip(), pwd.strip()))
        if pairs:
            return pairs

    email = os.getenv("SENDER_EMAIL", "")
    pwd   = os.getenv("SENDER_APP_PASSWORD", "")
    if email and pwd:
        return [(email, pwd)]

    raise ValueError("Set SENDER_POOL or SENDER_EMAIL + SENDER_APP_PASSWORD in .env")


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
    msg["From"]    = sender_email
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("mail.recruitagents.net", 465) as server:
        server.login(sender_email, app_password)
        server.sendmail(sender_email, to_email, msg.as_string())

    _counters[sender_email] += 1
    time.sleep(random.uniform(1.5, 4.0))
    return f"{sender_email}:{_counters[sender_email]}"
