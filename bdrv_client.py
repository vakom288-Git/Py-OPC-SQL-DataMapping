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
      p_msr_time  datetime  — measurement time (UTC naive datetime per BDRV spec)

BDRV timezone notes:
  - BDRV stores time in Moscow timezone internally.
  - DCS stored-procedure parameters must specify time in UTC; BDRV converts
    between timezones automatically.
  - OPC SourceTimestamp arrives as local time at a fixed offset (UTC+6 by
    default) and is often naive (no tzinfo).

Environment configuration:
  MEASUREMENT_TIME_MODE   'utc' | 'local'   Default 'utc'.
      'utc'   → p_msr_time sent as UTC naive datetime (BDRV spec).
      'local' → p_msr_time sent as OPC-local naive datetime (other deployments).
  OPC_LOCAL_UTC_OFFSET    fixed offset string, e.g. '+06:00' or '-03:00'.
                          Default '+00:00'.
      Used to attach tzinfo when OPC SourceTimestamp arrives as a naive datetime.

Cross-platform notes:
  - No Windows-specific paths or APIs; safe for future Astra Linux migration.
  - Timestamp is passed as a Python datetime object (not a locale-dependent
    string) to avoid driver/SQL-Server language-setting issues.
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

MEASUREMENT_TIME_MODE: str = os.environ.get("MEASUREMENT_TIME_MODE", "utc").lower()
OPC_LOCAL_UTC_OFFSET: str = os.environ.get("OPC_LOCAL_UTC_OFFSET", "+00:00")


def _parse_utc_offset(offset_str: str) -> timezone:
    """
    Parse a fixed-offset string like '+06:00' or '-03:30' into a timezone.

    Args:
        offset_str: Offset string in the form '+HH:MM' or '-HH:MM'.

    Returns:
        A timezone object with the given fixed offset.

    Raises:
        ValueError: If the string cannot be parsed or values are out of range.
    """
    s = offset_str.strip()
    if not s or s[0] not in ("+", "-"):
        raise ValueError(
            f"Invalid UTC offset {offset_str!r}: must start with '+' or '-', "
            "e.g. '+06:00' or '-03:30'."
        )
    sign = 1 if s[0] == "+" else -1
    try:
        parts = s[1:].split(":")
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError) as exc:
        raise ValueError(
            f"Invalid UTC offset {offset_str!r}: expected format '+HH:MM' or '-HH:MM', "
            f"e.g. '+06:00'."
        ) from exc
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        raise ValueError(
            f"Invalid UTC offset {offset_str!r}: hours must be 0-23 and "
            f"minutes 0-59, got {hours}:{minutes:02d}."
        )
    total_minutes = sign * (hours * 60 + minutes)
    return timezone(timedelta(minutes=total_minutes))


# Fixed OPC-local timezone built once at import time from the env var.
_OPC_TZ: timezone = _parse_utc_offset(OPC_LOCAL_UTC_OFFSET)

# Validate MEASUREMENT_TIME_MODE at import time so misconfiguration is caught early.
if MEASUREMENT_TIME_MODE not in ("utc", "local"):
    raise ValueError(
        f"Invalid MEASUREMENT_TIME_MODE {MEASUREMENT_TIME_MODE!r}: "
        "must be 'utc' or 'local'."
    )

# ---------------------------------------------------------------------------
# Public time-conversion helpers
# ---------------------------------------------------------------------------


def parse_opc_ts(ts: Optional[datetime]) -> datetime:
    """
    Parse an OPC SourceTimestamp into a timezone-aware datetime.

    Rules:
      - None       → current UTC (aware) when MEASUREMENT_TIME_MODE='utc',
                     or current time in OPC local offset (aware) when mode='local'.
      - tz-aware   → returned unchanged.
      - naive      → tzinfo set to the fixed offset from OPC_LOCAL_UTC_OFFSET.

    Args:
        ts: OPC SourceTimestamp (datetime or None).

    Returns:
        A timezone-aware datetime.
    """
    if ts is None:
        if MEASUREMENT_TIME_MODE == "utc":
            return datetime.now(timezone.utc)
        return datetime.now(_OPC_TZ)
    if ts.tzinfo is not None:
        return ts
    # naive → treat as OPC local time with the configured fixed offset
    return ts.replace(tzinfo=_OPC_TZ)


def to_buffer_iso(ts: Optional[datetime]) -> str:
    """
    Return an ISO-8601 string with explicit UTC offset, suitable for buffering.

    The string always includes the timezone offset (e.g.
    '2026-04-09T12:00:00+06:00' or '2026-04-09T06:00:00+00:00') so it can be
    unambiguously parsed back with datetime.fromisoformat().

    Args:
        ts: OPC SourceTimestamp (datetime or None).

    Returns:
        ISO-8601 string with explicit offset.
    """
    return parse_opc_ts(ts).isoformat()


def to_sql_naive(aware_dt: datetime) -> datetime:
    """
    Convert a timezone-aware datetime to a naive datetime suitable for pyodbc.

    - MEASUREMENT_TIME_MODE='utc'   → convert to UTC, strip tzinfo (naive UTC).
    - MEASUREMENT_TIME_MODE='local' → convert to OPC local offset, strip tzinfo.

    Args:
        aware_dt: A timezone-aware datetime.

    Returns:
        A naive datetime in the appropriate timezone for the SQL parameter.
    """
    if MEASUREMENT_TIME_MODE == "utc":
        return aware_dt.astimezone(timezone.utc).replace(tzinfo=None)
    return aware_dt.astimezone(_OPC_TZ).replace(tzinfo=None)


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
        ts:      OPC tag SourceTimestamp; converted to a naive datetime per
                 MEASUREMENT_TIME_MODE before sending. Uses current time if None.
        timeout: DB connection / query timeout in seconds.

    Raises:
        pyodbc.Error (or any DB exception) on failure — caller must catch and
        buffer the record.
    """
    aware_ts = parse_opc_ts(ts)
    sql_ts = to_sql_naive(aware_ts)
    with DBConnection(timeout=timeout) as conn:
        cur = conn.cursor()
        cur.execute(_SP_SQL, (int(ffc_id), int(msd_id), float(value), sql_ts))


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
                if ts_str:
                    parsed_ts: Optional[datetime] = datetime.fromisoformat(ts_str)
                else:
                    parsed_ts = None
            except (ValueError, TypeError):
                parsed_ts = None

            # parse_opc_ts handles None, naive (legacy records), and aware datetimes.
            aware_ts = parse_opc_ts(parsed_ts)
            sql_ts = to_sql_naive(aware_ts)
            with DBConnection(timeout=timeout) as conn:
                cur = conn.cursor()
                cur.execute(
                    _SP_SQL,
                    (
                        int(record["ffc_id"]),
                        int(record["msd_id"]),
                        float(record["value"]),
                        sql_ts,
                    ),
                )
            sent_timestamps.append(record["timestamp"])
        except Exception as exc:
            logger.warning("drain_buffer: send failed, stopping drain: %s", exc)
            break  # keep order — retry remaining records next time

    if sent_timestamps:
        buf.mark_measurements_synced(sent_timestamps)

    return len(sent_timestamps)
