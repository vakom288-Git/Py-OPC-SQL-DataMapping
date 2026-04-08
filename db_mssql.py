"""
db_mssql.py — Unified SQL Server access layer using mssql-python (TDS driver).

Provides:
  - get_connection()      — open a new connection from DB_CONFIG
  - execute()             — run a single parameterised T-SQL query
  - executemany()         — batch-execute a T-SQL query
  - health_ping()         — lightweight connectivity check
  - DBConnection context manager for explicit transaction control
"""

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence, Union

import mssql_python

from opc_tags_list import DB_CONFIG

logger = logging.getLogger("db_mssql")


def _build_kwargs() -> dict:
    """Convert DB_CONFIG into keyword arguments accepted by mssql_python.connect."""
    cfg = DB_CONFIG
    server = cfg["server"]
    port = cfg.get("port")
    if port:
        server = f"{server},{port}"

    kwargs: dict[str, Any] = {
        "server": server,
        "database": cfg["database"],
    }

    user = cfg.get("user", "")
    password = cfg.get("password", "")

    if user:
        kwargs["uid"] = user
    if password:
        kwargs["pwd"] = password

    trusted = cfg.get("trusted_connection", "")
    if trusted:
        kwargs["trusted_connection"] = trusted

    trust_cert = cfg.get("trust_server_certificate", "")
    if trust_cert:
        kwargs["trustservercertificate"] = trust_cert

    encrypt = cfg.get("encrypt", "")
    if encrypt:
        kwargs["encrypt"] = encrypt

    return kwargs


def get_connection(timeout: int = 5) -> mssql_python.Connection:
    """
    Create and return a new mssql_python connection from DB_CONFIG.

    Args:
        timeout: Login/connection timeout in seconds.

    Returns:
        An open mssql_python.Connection.

    Raises:
        mssql_python.DatabaseError: If the connection cannot be established.
    """
    kwargs = _build_kwargs()
    conn = mssql_python.connect(timeout=timeout, **kwargs)

    # Force DB context (workaround if driver ignores 'database' during login)
    db = DB_CONFIG.get("database")
    if db:
        cur = conn.cursor()
        cur.execute(f"USE [{db}]")
    return conn


class DBConnection:
    """
    Context-manager wrapper around a mssql_python connection with explicit
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
        self._conn: Optional[mssql_python.Connection] = None

    def __enter__(self) -> mssql_python.Connection:
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
    params: Optional[Union[tuple, dict]] = None,
    timeout: int = 5,
) -> list:
    """
    Execute a single T-SQL query and return all result rows.

    Supports arbitrary T-SQL including scalar UDF calls, e.g.:
        execute("SELECT dbo.MyUdf(?)", (value,))
        execute("INSERT INTO dbo.T (Col) VALUES (dbo.MyUdf(?))", (value,))

    Args:
        query:   T-SQL query string with ? positional placeholders.
        params:  Tuple or dict of query parameters (None for no parameters).
        timeout: Query timeout in seconds.

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
        except mssql_python.ProgrammingError:
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
        timeout:        Query timeout in seconds.
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
