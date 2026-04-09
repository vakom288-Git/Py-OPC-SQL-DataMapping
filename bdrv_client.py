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

Cross-platform notes:
  - No Windows-specific paths or APIs; safe for future Astra Linux migration.
  - Timestamp is passed as a Python datetime object (not a locale-dependent
    string) to avoid driver/SQL-Server language-setting issues.
"""

import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from db_mssql import DBConnection

if TYPE_CHECKING:
    from json_buffer_manager import JSONBufferManager

logger = logging.getLogger("bdrv_client")

_SP_SQL = "EXEC Sp_msr_value_send ?, ?, ?, ?"


def to_local_naive(ts: Optional[datetime]) -> datetime:
    """
    Convert *ts* to a naive local datetime suitable for pyodbc / SQL Server.

    Rules:
      - None            → datetime.now() (current local time)
      - timezone-aware  → converted to local timezone, then tzinfo stripped
      - naive           → returned unchanged (assumed already local)
    """
    if ts is None:
        return datetime.now()
    if ts.tzinfo is not None:
        # astimezone() with no argument uses the system local timezone.
        # Works on CPython 3.6+ on Windows and Linux.
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
        ts:      OPC tag SourceTimestamp; converted to local naive datetime
                 before sending. Uses current time if None.
        timeout: DB connection / query timeout in seconds.

    Raises:
        pyodbc.Error (or any DB exception) on failure — caller must catch and
        buffer the record.
    """
    local_ts = _to_local_naive(ts)
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

            local_ts = _to_local_naive(ts)
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
