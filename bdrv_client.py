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
      p_msr_time  datetime  — measurement time (local, naive datetime)

Timestamp environment variables:
  MEASUREMENT_TIME_MODE  — controls how OPC SourceTimestamp is converted before
                           writing to SQL Server:
                           "utc"   — store UTC naive datetime (default)
                           "local" — convert to local system time (legacy)
  OPC_LOCAL_UTC_OFFSET   — fixed UTC offset that the OPC server uses for its
                           local clock, e.g. "+06:00".  Used when the OPC
                           server returns naive timestamps that are actually in
                           a specific timezone (not UTC and not the system tz).
                           When set, naive OPC timestamps are interpreted as
                           OPC-local time (shifted by this offset) before any
                           conversion.  Leave unset to use system local time.

Cross-platform notes:
  - No Windows-specific paths or APIs; safe for future Astra Linux migration.
  - Timestamp is passed as a Python datetime object (not a locale-dependent
    string) to avoid driver/SQL-Server language-setting issues.
"""

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional, TYPE_CHECKING

from db_mssql import DBConnection

if TYPE_CHECKING:
    from json_buffer_manager import JSONBufferManager

logger = logging.getLogger("bdrv_client")

_SP_SQL = "EXEC Sp_msr_value_send ?, ?, ?, ?"


def parse_fixed_offset(offset_str: str) -> timezone:
    """
    Parse a fixed UTC offset string such as "+06:00" or "-05:30" into a
    :class:`datetime.timezone` instance.

    Args:
        offset_str: String in the form ``±HH:MM`` (e.g. "+06:00", "-05:30").

    Returns:
        A :class:`datetime.timezone` with the given fixed offset.

    Raises:
        ValueError: if the string is not in the expected format.
    """
    offset_str = offset_str.strip()
    if len(offset_str) != 6 or offset_str[0] not in ('+', '-') or offset_str[3] != ':':
        raise ValueError(
            f"OPC_LOCAL_UTC_OFFSET must be in ±HH:MM format, got: {offset_str!r}"
        )
    sign = 1 if offset_str[0] == '+' else -1
    try:
        hours = int(offset_str[1:3])
        minutes = int(offset_str[4:6])
    except ValueError:
        raise ValueError(
            f"OPC_LOCAL_UTC_OFFSET must be in ±HH:MM format, got: {offset_str!r}"
        )
    return timezone(timedelta(hours=sign * hours, minutes=sign * minutes))


def _get_opc_fixed_tz() -> Optional[timezone]:
    """
    Return a fixed :class:`datetime.timezone` based on ``OPC_LOCAL_UTC_OFFSET``
    environment variable, or ``None`` if the variable is not set.
    """
    raw = os.environ.get("OPC_LOCAL_UTC_OFFSET", "").strip()
    if not raw:
        return None
    return parse_fixed_offset(raw)


def ensure_aware_opc_ts(ts: Optional[datetime]) -> Optional[datetime]:
    """
    Ensure *ts* carries timezone information, interpreting naive timestamps as
    OPC-local time according to ``OPC_LOCAL_UTC_OFFSET``.

    - If *ts* is ``None``, returns ``None``.
    - If *ts* is already timezone-aware, returns it unchanged.
    - If *ts* is naive and ``OPC_LOCAL_UTC_OFFSET`` is set, attaches the
      corresponding fixed UTC offset (so the datetime is made aware without
      shifting the wall-clock value).
    - If *ts* is naive and ``OPC_LOCAL_UTC_OFFSET`` is not set, assumes the
      local system timezone (uses ``datetime.astimezone()`` trick).
    """
    if ts is None:
        return None
    if ts.tzinfo is not None:
        return ts
    opc_tz = _get_opc_fixed_tz()
    if opc_tz is not None:
        return ts.replace(tzinfo=opc_tz)
    # Fall back: treat naive ts as system local time
    return ts.astimezone()


def msr_time_to_iso(ts: Optional[datetime]) -> str:
    """
    Serialize *ts* to an ISO-8601 string for buffer storage, preserving the
    UTC offset so that ``drain_buffer`` can reconstruct the exact moment later.

    - Naive timestamps are first made aware via :func:`ensure_aware_opc_ts`.
    - The result always includes a UTC offset (e.g. "2026-04-10T12:00:00+06:00").
    - If *ts* is ``None``, the current UTC time is used.
    """
    if ts is None:
        ts = datetime.now(timezone.utc)
    aware = ensure_aware_opc_ts(ts)
    if aware is None:
        aware = datetime.now(timezone.utc)
    return aware.isoformat()


def to_db_naive(ts: Optional[datetime]) -> datetime:
    """
    Convert *ts* to a naive UTC datetime suitable for passing to pyodbc /
    SQL Server stored procedure ``Sp_msr_value_send``.

    Rules:
      - ``None``           → current UTC time (naive).
      - timezone-aware     → converted to UTC, tzinfo stripped.
      - naive              → interpreted as OPC-local time via
                             :func:`ensure_aware_opc_ts`, then converted to
                             UTC and tzinfo stripped.

    ``MEASUREMENT_TIME_MODE`` env var (default "utc") has no effect on the
    conversion; it is reserved for future modes and logged at startup.
    """
    if ts is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    aware = ensure_aware_opc_ts(ts)
    if aware is None:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    return aware.astimezone(timezone.utc).replace(tzinfo=None)


def to_local_naive(ts: Optional[datetime]) -> datetime:
    """
    Convert *ts* to a naive **local system** datetime (legacy, kept for
    backward compatibility).

    Prefer :func:`to_db_naive` for new code.

    Rules:
      - ``None``          → ``datetime.now()`` (current local time).
      - timezone-aware    → converted to local system tz, tzinfo stripped.
      - naive             → returned unchanged (assumed already local).
    """
    if ts is None:
        return datetime.now()
    if ts.tzinfo is not None:
        return ts.astimezone().replace(tzinfo=None)
    return ts


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
        ts:      OPC tag SourceTimestamp; converted to UTC naive datetime
                 before sending (via to_db_naive). Uses current UTC time if None.
        timeout: DB connection / query timeout in seconds.

    Raises:
        pyodbc.Error (or any DB exception) on failure — caller must catch and
        buffer the record.
    """
    local_ts = to_db_naive(ts)
    with DBConnection(timeout=timeout) as conn:
        cur = conn.cursor()
        cur.execute(_SP_SQL, (int(ffc_id), int(msd_id), float(value), local_ts))


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

            local_ts = to_db_naive(ts)
            with DBConnection(timeout=timeout) as conn:
                cur = conn.cursor()
                cur.execute(
                    _SP_SQL,
                    (
                        int(record["ffc_id"]),
                        int(record["msd_id"]),
                        float(record["value"]),
                        local_ts,
                    ),
                )
            sent_timestamps.append(record["timestamp"])
        except Exception as exc:
            logger.warning("drain_buffer: send failed, stopping drain: %s", exc)
            break  # keep order — retry remaining records next time

    if sent_timestamps:
        buf.mark_measurements_synced(sent_timestamps)

    return len(sent_timestamps)
