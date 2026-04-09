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
      p_msr_time  datetime  — measurement time (naive UTC datetime by default)

Environment variables:
  MEASUREMENT_TIME_MODE   'utc' (default) or 'local'
      utc   — p_msr_time is sent as naive UTC datetime (БДРВ default).
      local — p_msr_time is sent as naive local datetime in the fixed offset
              defined by OPC_LOCAL_UTC_OFFSET.
  OPC_LOCAL_UTC_OFFSET    UTC offset of the OPC server's local clock when
      SourceTimestamp is naive (no tzinfo).  Format: '+HH:MM' or '-HH:MM'.
      Default: '+00:00' (UTC).  Example for UTC+6: '+06:00'.

Cross-platform notes:
  - No Windows-specific paths or APIs; safe for future Astra Linux migration.
  - Timestamp is passed as a Python datetime object (not a locale-dependent
    string) to avoid driver/SQL-Server language-setting issues.
  - Buffer stores msr_time as an ISO-8601 string with an explicit UTC offset
    (e.g. '2026-04-09T12:00:00+06:00') so the meaning is unambiguous.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING

from db_mssql import DBConnection

if TYPE_CHECKING:
    from json_buffer_manager import JSONBufferManager

logger = logging.getLogger("bdrv_client")

_SP_SQL = "EXEC Sp_msr_value_send ?, ?, ?, ?"

# ---------------------------------------------------------------------------
# Environment-driven configuration
# ---------------------------------------------------------------------------

MEASUREMENT_TIME_MODE: str = os.environ.get(
    "MEASUREMENT_TIME_MODE", "utc"
).strip().lower()

_OPC_LOCAL_UTC_OFFSET_RAW: str = os.environ.get(
    "OPC_LOCAL_UTC_OFFSET", "+00:00"
).strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_fixed_offset(s: str) -> timezone:
    """
    Parse a UTC offset string like '+06:00' or '-03:30' into a
    ``datetime.timezone`` object.

    Args:
        s: Offset string in the form ``[+-]HH:MM``.

    Returns:
        A ``datetime.timezone`` representing the fixed offset.

    Raises:
        ValueError: If the string cannot be parsed.
    """
    s = s.strip()
    if not s or s[0] not in ("+", "-"):
        raise ValueError(f"Invalid UTC offset: {s!r}. Expected '+HH:MM' or '-HH:MM'.")
    sign = 1 if s[0] == "+" else -1
    parts = s[1:].split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid UTC offset: {s!r}. Expected '+HH:MM' or '-HH:MM'.")
    hours, minutes = int(parts[0]), int(parts[1])
    delta = timedelta(hours=hours, minutes=minutes)
    return timezone(sign * delta)


try:
    _FIXED_OFFSET_TZ: timezone = parse_fixed_offset(_OPC_LOCAL_UTC_OFFSET_RAW)
except ValueError:
    logger.warning(
        "Invalid OPC_LOCAL_UTC_OFFSET=%r — falling back to UTC (+00:00)",
        _OPC_LOCAL_UTC_OFFSET_RAW,
    )
    _FIXED_OFFSET_TZ = timezone.utc


def ensure_aware_opc_ts(ts: Optional[datetime]) -> datetime:
    """
    Return a timezone-aware datetime for an OPC SourceTimestamp.

    Rules:
      - None       → current time in the fixed OPC offset (_FIXED_OFFSET_TZ).
      - tz-aware   → returned unchanged.
      - naive      → tzinfo set to the fixed OPC offset from OPC_LOCAL_UTC_OFFSET.

    Note: this function is mode-independent; MEASUREMENT_TIME_MODE only affects
    how the resulting aware datetime is later converted in to_db_naive().

    Args:
        ts: OPC SourceTimestamp (may be None, naive, or aware).

    Returns:
        A timezone-aware ``datetime``.
    """
    if ts is None:
        return datetime.now(tz=_FIXED_OFFSET_TZ)
    if ts.tzinfo is not None:
        return ts
    # Naive → attach the configured fixed offset
    return ts.replace(tzinfo=_FIXED_OFFSET_TZ)


def msr_time_to_iso(ts: Optional[datetime]) -> str:
    """
    Serialize an OPC SourceTimestamp to an offset-aware ISO-8601 string for
    buffer storage (e.g. ``'2026-04-09T12:00:00+06:00'``).

    The returned string always includes an explicit UTC offset so the meaning
    is unambiguous regardless of the active MEASUREMENT_TIME_MODE.

    Args:
        ts: OPC SourceTimestamp (may be None, naive, or aware).

    Returns:
        ISO-8601 string with explicit offset.
    """
    aware = ensure_aware_opc_ts(ts)
    return aware.isoformat()


def to_db_naive(ts: Optional[datetime]) -> datetime:
    """
    Convert an OPC SourceTimestamp to a naive datetime suitable for pyodbc /
    SQL Server, applying the active MEASUREMENT_TIME_MODE.

    Modes:
      utc   (default): ensure_aware → convert to UTC → strip tzinfo.
      local:           ensure_aware → convert to fixed OPC offset → strip tzinfo.

    Args:
        ts: OPC SourceTimestamp (may be None, naive, or aware).

    Returns:
        A naive ``datetime`` ready for the stored procedure parameter.
    """
    aware = ensure_aware_opc_ts(ts)
    if MEASUREMENT_TIME_MODE == "local":
        return aware.astimezone(_FIXED_OFFSET_TZ).replace(tzinfo=None)
    # Default: utc
    return aware.astimezone(timezone.utc).replace(tzinfo=None)


def to_local_naive(ts: Optional[datetime]) -> datetime:
    """
    Backward-compatible alias for ``to_db_naive``.

    .. deprecated::
        Use :func:`to_db_naive` in new code.
    """
    return to_db_naive(ts)


# Keep the private alias for backward compatibility
_to_local_naive = to_local_naive


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
        ts:      OPC tag SourceTimestamp; converted via to_db_naive before
                 sending. Uses current time if None.
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

    ``msr_time`` in each buffered record is expected to be an offset-aware
    ISO-8601 string (e.g. ``'2026-04-09T12:00:00+06:00'``).  It is parsed,
    then converted via :func:`to_db_naive` before sending.  If the field is
    missing or unparseable, ``ts=None`` is used (current time fallback).

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
