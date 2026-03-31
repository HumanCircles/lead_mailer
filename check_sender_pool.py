import csv
import os
import smtplib
from datetime import datetime, timezone

from dotenv import dotenv_values


def _parse_sender_pool(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for entry in (raw or "").split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        email, pwd = entry.split(":", 1)
        email = email.strip()
        pwd = pwd.strip()
        if email and pwd:
            out.append((email, pwd))
    return out


def main() -> None:
    env = dotenv_values(".env")
    smtp_host = str(env.get("SMTP_HOST") or "mail.recruitagents.net").strip()
    smtp_port = int(str(env.get("SMTP_PORT") or "465").strip())
    timeout = int(str(env.get("SMTP_TIMEOUT") or "20").strip())
    sender_pool = _parse_sender_pool(str(env.get("SENDER_POOL") or ""))

    out_file = "sender_pool_inbox_check.csv"
    now = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, str]] = []

    for email, pwd in sender_pool:
        status = "ok"
        error = ""
        try:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout) as server:
                server.login(email, pwd)
        except Exception as e:
            status = "failed"
            error = str(e)
        rows.append(
            {
                "timestamp": now,
                "email": email,
                "status": status,
                "error": error,
            }
        )

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "email", "status", "error"])
        w.writeheader()
        w.writerows(rows)

    ok_count = sum(1 for r in rows if r["status"] == "ok")
    print(f"Checked {len(rows)} accounts. OK={ok_count}, failed={len(rows)-ok_count}")
    print(f"Report: {out_file}")


if __name__ == "__main__":
    main()
