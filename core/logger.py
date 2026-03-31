"""IST-aware rotating logger for the lead-mailer agent.

Usage
-----
    from core.logger import get_logger, IST

    log = get_logger()
    log.info("Started")

The logger is a module-level singleton; every call to ``get_logger()``
returns the same :class:`logging.Logger` instance.

Rotation happens at midnight IST; backup count is controlled by LOG_KEEP_DAYS (default 7).
All timestamps written to the file use IST (UTC+5:30).
"""

from __future__ import annotations

import logging
import os
from logging.handlers import TimedRotatingFileHandler
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Public constant
# ---------------------------------------------------------------------------

IST: ZoneInfo = ZoneInfo("Asia/Kolkata")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
_LOG_FILE = os.path.join(_LOG_DIR, "agent.log")

_LOGGER_NAME = "lead_mailer"
_logger: logging.Logger | None = None


class _ISTFormatter(logging.Formatter):
    """Logging formatter that stamps every record with IST local time."""

    converter = None  # disable the default UTC/local converter

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        import datetime

        dt = datetime.datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_logger() -> logging.Logger:
    """Return the singleton :class:`logging.Logger` for the lead-mailer agent.

    Creates the logger (and the ``logs/`` directory) on first call; subsequent
    calls return the cached instance without reconfiguring it.
    """
    global _logger
    if _logger is not None:
        return _logger

    # Ensure the logs directory exists.
    os.makedirs(_LOG_DIR, exist_ok=True)

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if the module is somehow reloaded.
    if logger.handlers:
        _logger = logger
        return _logger

    fmt = _ISTFormatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )

    # --- File handler: midnight-IST rotation ---
    _keep_days = int(os.environ.get("LOG_KEEP_DAYS", "7"))
    file_handler = TimedRotatingFileHandler(
        filename=_LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=_keep_days,
        encoding="utf-8",
        utc=False,          # rotate at local midnight; we'll keep the process TZ as IST
        atTime=None,
    )
    # Override the rollover time so it fires at IST midnight regardless of
    # the host system timezone.
    import datetime

    now_ist = datetime.datetime.now(tz=IST)
    next_midnight_ist = (now_ist + datetime.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    file_handler.rolloverAt = int(next_midnight_ist.timestamp())
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)

    # --- Console handler ---
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.WARNING)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    _logger = logger
    return _logger
