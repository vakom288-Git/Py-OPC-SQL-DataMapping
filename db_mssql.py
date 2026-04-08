"""
db_mssql.py — SQL Server connection layer via pyodbc (ODBC Driver 17/18).

Connection is built from environment variables (server/port/database approach,
no DSN required). Supports Encrypt and TrustServerCertificate parameters
required by ODBC Driver 17/18.

Environment variables
---------------------
DB_SERVER              SQL Server hostname or IP address (default: localhost)
DB_PORT                TCP port                          (default: 1433)
DB_NAME                Database name                     (required)
DB_USER                SQL Server login username         (optional; omit for
                       Windows Authentication / Trusted Connection)
DB_PASSWORD            SQL Server login password         (optional)
DB_DRIVER              ODBC driver name                  (default: ODBC Driver 17 for SQL Server)
DB_ENCRYPT             Encrypt=yes/no                    (default: no)
DB_TRUST_CERT          TrustServerCertificate=yes/no     (default: yes)
DB_CONNECT_TIMEOUT     Login timeout in seconds          (default: 10)
"""

import contextlib
import logging
import os
from typing import Any, Generator, List, Optional, Sequence, Tuple

import pyodbc

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def _build_connection_string() -> str:
    """Assemble an ODBC connection string from environment variables."""
    driver = os.environ.get("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    server = os.environ.get("DB_SERVER", "localhost")
    port = os.environ.get("DB_PORT", "1433")
    database = os.environ.get("DB_NAME", "")
    user = os.environ.get("DB_USER", "")
    password = os.environ.get("DB_PASSWORD", "")
    encrypt = os.environ.get("DB_ENCRYPT", "no")
    trust_cert = os.environ.get("DB_TRUST_CERT", "yes")

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server},{port}",
    ]

    if database:
        parts.append(f"DATABASE={database}")

    if user:
        parts.append(f"UID={user}")
        parts.append(f"PWD={password}")
    else:
        parts.append("Trusted_Connection=yes")

    parts.append(f"Encrypt={encrypt}")
    parts.append(f"TrustServerCertificate={trust_cert}")

    return ";".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_connection() -> pyodbc.Connection:
    """Open and return a new pyodbc connection.

    The connection is created with *autocommit=False* so that the caller
    controls transaction boundaries explicitly (or via :func:`transaction`).

    Raises
    ------
    pyodbc.Error
        If the driver cannot establish the connection.
    """
    conn_str = _build_connection_string()
    timeout = int(os.environ.get("DB_CONNECT_TIMEOUT", "10"))
    conn = pyodbc.connect(conn_str, timeout=timeout, autocommit=False)
    return conn


def execute(
    query: str,
    params: Optional[Sequence[Any]] = None,
) -> List[Tuple]:
    """Execute *query* with optional *params* in a single auto-committed
    transaction and return all result rows (empty list for non-SELECT).

    Parameters
    ----------
    query:
        SQL statement (may contain ``?`` placeholders).
    params:
        Sequence of parameter values bound to ``?`` placeholders.

    Returns
    -------
    list of tuples
        Rows returned by the server; empty list if the statement produces
        no result set (INSERT / UPDATE / DELETE / EXEC without result).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if params is not None:
                cur.execute(query, params)
            else:
                cur.execute(query)

            rows: List[Tuple] = []
            try:
                rows = cur.fetchall()
            except pyodbc.ProgrammingError:
                pass  # no result set (DML / EXEC without SELECT)

            conn.commit()
            return rows


def executemany(
    query: str,
    seq_of_params: Sequence[Sequence[Any]],
) -> None:
    """Execute *query* once for each parameter tuple in *seq_of_params*,
    wrapping all rows in a single transaction (commit on success, rollback
    on any error).

    Parameters
    ----------
    query:
        SQL statement with ``?`` placeholders.
    seq_of_params:
        Sequence of parameter sequences, one per row.  Must be a concrete
        sequence (list / tuple) because pyodbc's ``fast_executemany`` mode
        requires random-access iteration over the data.

    Notes
    -----
    ``cursor.fast_executemany = True`` switches pyodbc from row-by-row
    round-trips to a single bulk RPC call (``sp_prepexec`` + TVP), which
    dramatically reduces latency for large batches.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                # fast_executemany sends all rows in one network round-trip
                # instead of one round-trip per row.
                cur.fast_executemany = True
                cur.executemany(query, seq_of_params)
                conn.commit()
            except Exception:
                conn.rollback()
                raise


def health_ping() -> bool:
    """Check whether the database is reachable.

    Returns
    -------
    bool
        ``True`` if a trivial query succeeds, ``False`` otherwise.
    """
    try:
        rows = execute("SELECT 1 AS ping")
        return len(rows) > 0
    except Exception as exc:
        logger.warning("health_ping failed: %s", exc)
        return False


@contextlib.contextmanager
def transaction() -> Generator[pyodbc.Cursor, None, None]:
    """Context manager that yields a cursor bound to a single transaction.

    Commits on clean exit, rolls back (and re-raises) on any exception.

    Usage
    -----
    ::

        with transaction() as cur:
            cur.execute("EXEC dbo.Sp_msr_value_send(?, ?, ?, ?)",
                        (ffc_id, msd_id, value, dt))

    Raises
    ------
    Exception
        Re-raises any exception that occurred inside the ``with`` block
        after rolling back the transaction.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
    finally:
        conn.close()
