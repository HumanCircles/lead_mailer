"""IST-aware rotating logger for the lead-mailer agent.

Usage
-----
    from core.logger import get_logger, IST

    log = get_logger()
    log.info("Started")

The logger is a module-level singleton; every call to ``get_logger()``
returns the same :class:`logging.Logger` instance.

Rotation happens at midnight IST; backup count controlled by LOG_KEEP_DAYS (default 7).
All timestamps use IST (UTC+5:30).
"""

from __future__ import annotations

import datetime
import logging
import os
import threading
from logging.handlers import TimedRotatingFileHandler
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Public constant
# ---------------------------------------------------------------------------

IST: ZoneInfo = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
_LOG_FILE = os.path.join(_LOG_DIR, "agent.log")
_LOGGER_NAME = "lead_mailer"

_logger: logging.Logger | None = None
_lock = threading.Lock()


class _ISTFormatter(logging.Formatter):
    """Stamps every log record with IST local time."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        dt = datetime.datetime.fromtimestamp(record.created, tz=IST)
        return dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S %Z")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_logger() -> logging.Logger:
    """Return the singleton Logger. Thread-safe via double-checked locking."""
    global _logger

    if _logger is not None:   # fast path — no lock
        return _logger

    with _lock:               # slow path — everything inside the lock
        if _logger is not None:
            return _logger

        os.makedirs(_LOG_DIR, exist_ok=True)

        logger = logging.getLogger(_LOGGER_NAME)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        # Guard against double-attach on module reload (Streamlit)
        if not logger.handlers:
            fmt = _ISTFormatter(
                fmt="%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S %Z",
            )

            keep_days = int(os.environ.get("LOG_KEEP_DAYS", "7"))
            fh = TimedRotatingFileHandler(
                filename=_LOG_FILE,
                when="midnight",
                interval=1,
                backupCount=keep_days,
                encoding="utf-8",
            )
            # Fire at IST midnight regardless of host timezone
            now_ist = datetime.datetime.now(tz=IST)
            next_midnight = (now_ist + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            fh.rolloverAt = int(next_midnight.timestamp())
            fh.setFormatter(fmt)
            fh.setLevel(logging.DEBUG)

            sh = logging.StreamHandler()
            sh.setFormatter(fmt)
            sh.setLevel(logging.WARNING)

            logger.addHandler(fh)
            logger.addHandler(sh)

        _logger = logger
        return _logger
