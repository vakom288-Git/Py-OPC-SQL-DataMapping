"""
bdrv_client.py — SQL Server writer for OPC measurements via Sp_msr_value_send.

Provides:
  - send_value(ffc_id, msd_id, value, ts) — send one measurement to SQL Server
    by calling stored procedure Sp_msr_value_send; 1 call == 1 transaction.
  - drain_buffer(buf)                     — flush buffered measurements when
    DB becomes available again.

Stored procedure contract:
    EXEC Sp_msr_value_send p_ffc_id, p_msd_id, p_msr_value, p_msr_time
      p_ffc_id   int        — identifier from OPC tag mapping field id1
      p_msd_id   int        — identifier from OPC tag mapping field id2
      p_msr_value float(53) — numeric measurement value
      p_msr_time  datetime  — measurement time (UTC naive datetime by default)

Measurement time mode:
    Controlled by the environment variable MEASUREMENT_TIME_MODE:
      "utc"   (default) — timestamps are passed to the procedure in UTC.
                          BDRV converts UTC to Moscow time internally.
      "local"           — timestamps are passed in the local system time.
                          Use only if the target procedure does NOT perform
                          its own UTC→local conversion.

Cross-platform notes:
  - No Windows-specific paths or APIs; safe for future Astra Linux migration.
  - Timestamp is passed as a Python datetime object (not a locale-dependent
    string) to avoid driver/SQL-Server language-setting issues.
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING

from db_mssql import DBConnection

if TYPE_CHECKING:
    from json_buffer_manager import JSONBufferManager

logger = logging.getLogger("bdrv_client")

_SP_SQL = "EXEC Sp_msr_value_send ?, ?, ?, ?"

# Allowed values for MEASUREMENT_TIME_MODE.  Default is "utc" per spec:
# "В БДРВ всё время хранится в московском часовом поясе, время в параметрах
#  процедур, вызываемых из DCS указывается в UTC."
_VALID_MODES = ("utc", "local")
_DEFAULT_MODE = "utc"


def _get_time_mode() -> str:
    """Return the active measurement time mode (read from env each call)."""
    mode = os.environ.get("MEASUREMENT_TIME_MODE", _DEFAULT_MODE).lower().strip()
    if mode not in _VALID_MODES:
        logger.warning(
            "MEASUREMENT_TIME_MODE=%r is not recognised; falling back to %r",
            mode,
            _DEFAULT_MODE,
        )
        return _DEFAULT_MODE
    return mode


def to_db_naive(ts: Optional[datetime]) -> datetime:
    """
    Convert *ts* to a naive datetime suitable for pyodbc / SQL Server.

    The conversion mode is controlled by the ``MEASUREMENT_TIME_MODE``
    environment variable (default: ``"utc"``).

    UTC mode (default):
      - None           → ``datetime.utcnow()``
      - timezone-aware → converted to UTC, tzinfo stripped
      - naive          → returned unchanged (assumed already UTC)

    Local mode (``MEASUREMENT_TIME_MODE=local``):
      - None           → ``datetime.now()``
      - timezone-aware → converted to local system timezone, tzinfo stripped
      - naive          → returned unchanged (assumed already local)
    """
    mode = _get_time_mode()
    if mode == "local":
        if ts is None:
            return datetime.now()
        if ts.tzinfo is not None:
            # astimezone() with no argument uses the system local timezone.
            # Works on CPython 3.6+ on Windows and Linux.
            return ts.astimezone().replace(tzinfo=None)
        return ts
    else:  # "utc"
        if ts is None:
            return datetime.now(timezone.utc).replace(tzinfo=None)
        if ts.tzinfo is not None:
            return ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts


def to_local_naive(ts: Optional[datetime]) -> datetime:
    """
    Backward-compatibility wrapper: convert *ts* to a naive local datetime.

    Equivalent to calling ``to_db_naive`` with ``MEASUREMENT_TIME_MODE=local``.
    New code should use :func:`to_db_naive` directly.

    Rules:
      - None            → datetime.now() (current local time)
      - timezone-aware  → converted to local timezone, then tzinfo stripped.
                          astimezone() with no argument uses the system local
                          timezone; works on CPython 3.6+ on Windows and Linux.
      - naive           → returned unchanged (assumed already local)
    """
    if ts is None:
        return datetime.now()
    if ts.tzinfo is not None:
        return ts.astimezone().replace(tzinfo=None)
    return ts


def send_value(
    ffc_id: int,
    msd_id: int,
    value: float,
    ts: Optional[datetime] = None,
    timeout: int = 5,
) -> None:
    """
    Send one measurement to SQL Server by calling Sp_msr_value_send.

    Each call opens a connection, executes the stored procedure, and commits —
    guaranteeing 1 call == 1 transaction.

    Args:
        ffc_id:  p_ffc_id  — tag mapping field id1 (ТЗК identifier).
        msd_id:  p_msd_id  — tag mapping field id2 (instrument identifier).
        value:   p_msr_value — measurement value (float).
        ts:      OPC tag SourceTimestamp; converted to a naive datetime
                 (UTC by default) before sending. Uses current time if None.
        timeout: DB connection / query timeout in seconds.

    Raises:
        pyodbc.Error (or any DB exception) on failure — caller must catch and
        buffer the record.
    """
    db_ts = to_db_naive(ts)
    with DBConnection(timeout=timeout) as conn:
        cur = conn.cursor()
        cur.execute(_SP_SQL, (int(ffc_id), int(msd_id), float(value), db_ts))


def drain_buffer(
    buf: "JSONBufferManager",
    timeout: int = 5,
    limit: int = 1000,
) -> int:
    """
    Flush buffered measurements to SQL Server one record at a time.

    Stops on the first send failure so records remain ordered and are retried
    on the next call.

    Args:
        buf:     JSONBufferManager instance that holds buffered measurements.
        timeout: DB connection / query timeout in seconds.
        limit:   Maximum number of records to flush in one call.

    Returns:
        Number of records successfully sent and marked as synced.
    """
    records = buf.get_unsync_measurements(limit=limit)
    if not records:
        return 0

    sent_timestamps: list[str] = []
    for record in records:
        try:
            ts_str = record.get("msr_time")
            try:
                ts: Optional[datetime] = (
                    datetime.fromisoformat(ts_str) if ts_str else None
                )
            except (ValueError, TypeError):
                ts = None

            # msr_time in the buffer was already serialised by to_db_naive(),
            # so the stored ISO string is a naive datetime in the correct mode.
            # Passing it through to_db_naive() again would be a no-op for naive
            # inputs (both modes return them unchanged), preserving the value.
            db_ts = to_db_naive(ts)
            with DBConnection(timeout=timeout) as conn:
                cur = conn.cursor()
                cur.execute(
                    _SP_SQL,
                    (
                        int(record["ffc_id"]),
                        int(record["msd_id"]),
                        float(record["value"]),
                        db_ts,
                    ),
                )
            sent_timestamps.append(record["timestamp"])
        except Exception as exc:
            logger.warning("drain_buffer: send failed, stopping drain: %s", exc)
            break  # keep order — retry remaining records next time

    if sent_timestamps:
        buf.mark_measurements_synced(sent_timestamps)

    return len(sent_timestamps)
