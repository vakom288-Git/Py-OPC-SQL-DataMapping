# Py-OPC-SQL-DataMapping

OPC UA → SQL Server data mapping layer for industrial automation systems.

## SQL Server connection

The project uses **[pyodbc](https://github.com/mkleehammer/pyodbc)** to connect to Microsoft SQL Server.  
You must have **Microsoft ODBC Driver 17 or 18 for SQL Server** installed on the host machine.

### Installing the ODBC driver

See the official Microsoft documentation:
- [Install ODBC Driver on Linux](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- [Install ODBC Driver on Windows](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

### Python dependencies

```bash
pip install -r requirements.txt
```

`requirements.txt` contains:

| Package | Purpose |
|---------|---------|
| `pyodbc>=5.1.0` | SQL Server connectivity via ODBC Driver 17/18 |
| `asyncua>=1.1.5` | OPC UA client |

> **Note:** `pymssql` / `mssql-python` are **not** used. All SQL Server access goes through `pyodbc`.

---

## Environment variables

The connection is configured **without a DSN** — the driver string is assembled
from the following environment variables at runtime.

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_SERVER` | `localhost` | SQL Server hostname or IP address |
| `DB_PORT` | `1433` | SQL Server TCP port |
| `DB_NAME` | *(required)* | Target database name |
| `DB_USER` | *(empty)* | SQL Server login username (omit to use Windows Authentication) |
| `DB_PASSWORD` | *(empty)* | SQL Server login password |
| `DB_DRIVER` | `ODBC Driver 17 for SQL Server` | ODBC driver name (use `ODBC Driver 18 for SQL Server` for Driver 18) |
| `DB_ENCRYPT` | `no` | Whether to encrypt the connection (`yes` / `no`) |
| `DB_TRUST_CERT` | `yes` | Whether to trust the server certificate (`yes` / `no`) |
| `DB_CONNECT_TIMEOUT` | `10` | Login timeout in seconds |

### Example `.env`

```env
DB_SERVER=192.168.1.72
DB_PORT=1433
DB_NAME=DB_NEW_TEST
DB_USER=sa
DB_PASSWORD=YourStrongPassword!
DB_DRIVER=ODBC Driver 17 for SQL Server
DB_ENCRYPT=no
DB_TRUST_CERT=yes
DB_CONNECT_TIMEOUT=10
```

---

## Module `db_mssql.py`

Public API:

| Function / symbol | Description |
|-------------------|-------------|
| `get_connection()` | Open a new `pyodbc.Connection` (autocommit=False) |
| `execute(query, params)` | Run one statement, return result rows |
| `executemany(query, seq_of_params)` | Batch execution in a single transaction |
| `health_ping()` | Returns `True` if the database is reachable |
| `transaction()` | Context manager — commit on success, rollback on exception |

### Quick check

```bash
python -m py_compile db_mssql.py
```

```python
import os, db_mssql

os.environ.update({"DB_SERVER": "192.168.1.72", "DB_NAME": "DB_NEW_TEST",
                   "DB_USER": "sa", "DB_PASSWORD": "secret"})

print(db_mssql.health_ping())  # True if server is reachable
```
