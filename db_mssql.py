"""
db_mssql.py — Unified SQL Server access layer using pyodbc (ODBC Driver 17/18).

Provides:
  - get_connection()      — open a new connection from DB_CONFIG
  - execute()             — run a single parameterised T-SQL query
  - executemany()         — batch-execute a T-SQL query
  - health_ping()         — lightweight connectivity check
  - DBConnection context manager for explicit transaction control
"""

import logging
from typing import Any, Optional, Sequence, Union

import pyodbc

from opc_tags_list import DB_CONFIG

logger = logging.getLogger("db_mssql")

# Preferred ODBC drivers in priority order (18 first, 17 as fallback).
_PREFERRED_DRIVERS = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
]


def _get_driver() -> str:
    """Return the best available SQL Server ODBC driver name."""
    available = pyodbc.drivers()
    for preferred in _PREFERRED_DRIVERS:
        if preferred in available:
            return preferred
    # Fallback: any driver that mentions SQL Server
    for d in available:
        if "SQL Server" in d:
            return d
    # Default — will raise a clear error at connect time if not installed
    return _PREFERRED_DRIVERS[0]


def _build_connection_string(timeout: int = 5) -> str:
    """
    Build an ODBC connection string from DB_CONFIG without modifying DB_CONFIG.

    Mapping of DB_CONFIG keys to ODBC keywords:
      server + port          -> SERVER=host,port
      database               -> DATABASE=...
      user / password        -> UID=... / PWD=...
      trusted_connection     -> Trusted_Connection=yes  (only when user is empty)
      trust_server_certificate -> TrustServerCertificate=yes/no
      encrypt                -> Encrypt=yes/no
    """
    cfg = DB_CONFIG

    server = cfg["server"]
    port = cfg.get("port")
    if port:
        server = f"{server},{port}"

    parts: list[str] = [
        f"Driver={{{_get_driver()}}}",
        f"SERVER={server}",
        f"DATABASE={cfg['database']}",
        f"LoginTimeout={timeout}",
    ]

    user = cfg.get("user", "")
    password = cfg.get("password", "")

    if user:
        parts.append(f"UID={user}")
        parts.append(f"PWD={password}")
    else:
        trusted = cfg.get("trusted_connection", "")
        if trusted:
            parts.append("Trusted_Connection=yes")

    trust_cert = cfg.get("trust_server_certificate", "")
    if trust_cert:
        # Normalise any truthy/falsy value to yes/no
        parts.append(
            f"TrustServerCertificate={'yes' if str(trust_cert).lower() in ('yes', '1', 'true') else 'no'}"
        )

    encrypt = cfg.get("encrypt", "")
    if encrypt:
        parts.append(
            f"Encrypt={'yes' if str(encrypt).lower() in ('yes', '1', 'true') else 'no'}"
        )

    return ";".join(parts)


def get_connection(timeout: int = 5) -> pyodbc.Connection:
    """
    Create and return a new pyodbc connection from DB_CONFIG.

    Args:
        timeout: Login/connection timeout in seconds.

    Returns:
        An open pyodbc.Connection (autocommit=False).

    Raises:
        pyodbc.Error: If the connection cannot be established.
    """
    conn_str = _build_connection_string(timeout=timeout)
    conn = pyodbc.connect(conn_str, autocommit=False)
    # conn.timeout sets the default query timeout for all cursors on this
    # connection (seconds; 0 = no limit).  pyodbc does not support per-cursor
    # timeout; use SQL Server query hints (OPTION (QUERYTIMEOUT n)) if needed.
    conn.timeout = timeout
    return conn


class DBConnection:
    """
    Context-manager wrapper around a pyodbc connection with explicit
    transaction control.  Commits on clean exit, rolls back on exception.

    Usage:
        with DBConnection() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO ...", params)
        # auto-committed

        with DBConnection() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO ...", params)
            conn.commit()               # partial commit
            cur.execute("INSERT INTO ...", params2)
        # auto-committed at exit
    """

    def __init__(self, timeout: int = 5):
        self._timeout = timeout
        self._conn: Optional[pyodbc.Connection] = None

    def __enter__(self) -> pyodbc.Connection:
        self._conn = get_connection(timeout=self._timeout)
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is None:
            return False
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()
            self._conn = None
        return False  # do not suppress exceptions


def execute(
    query: str,
    params: Optional[Union[tuple, list]] = None,
    timeout: int = 5,
) -> list:
    """
    Execute a single T-SQL query and return all result rows.

    Args:
        query:   T-SQL query string with ? positional placeholders.
        params:  Tuple or list of query parameters (None for no parameters).
        timeout: Query timeout in seconds (applied to the cursor).

    Returns:
        List of result rows (empty list for non-SELECT statements).
    """
    with DBConnection(timeout=timeout) as conn:
        cur = conn.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        try:
            return cur.fetchall()
        except pyodbc.ProgrammingError:
            return []


def executemany(
    query: str,
    seq_of_params: Sequence[Union[tuple, list]],
    timeout: int = 5,
) -> None:
    """
    Execute a T-SQL query for each item in seq_of_params (batch operation).

    Args:
        query:          T-SQL query string with ? positional placeholders.
        seq_of_params:  Sequence of parameter tuples/lists.
        timeout:        Query timeout in seconds (applied to the cursor).
    """
    if not seq_of_params:
        return
    with DBConnection(timeout=timeout) as conn:
        cur = conn.cursor()
        cur.executemany(query, list(seq_of_params))


def health_ping(timeout: int = 5) -> bool:
    """
    Check whether the SQL Server is reachable.

    Returns:
        True if the server responded, False otherwise.
    """
    conn = None
    try:
        conn = get_connection(timeout=timeout)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        return True
    except Exception as e:
        logger.debug("health_ping failed: %s", e)
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
