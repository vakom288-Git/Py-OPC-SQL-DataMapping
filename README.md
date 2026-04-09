# Py-OPC-SQL-DataMapping

OPC UA → SQL Server data mapping service. Reads tag values from an OPC UA server
and writes them to a Microsoft SQL Server database using **pyodbc** (ODBC Driver 17/18).

## Requirements

* Python 3.10+
* [Microsoft ODBC Driver 17 or 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)
  installed on the host running this service.
* Python dependencies (see `requirements.txt`):
  ```
  asyncua>=1.1.5
  pyodbc>=4.0.39
  ```

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

All settings live in `opc_tags_list.py`.

### OPC UA

```python
OPC_URL  = "opc.tcp://<host>:<port>"
OPC_USER = ""   # leave empty for anonymous access
OPC_PASS = ""
```

### SQL Server (`DB_CONFIG`)

```python
DB_CONFIG = {
    "server":                 "192.168.1.72",
    "port":                   1433,
    "database":               "my_database",
    "user":                   os.environ.get("DB_USER", ""),
    "password":               os.environ.get("DB_PASSWORD", ""),
    "trusted_connection":     "",   # set to "yes" for Windows auth (requires empty user/password)
    "trust_server_certificate": "yes",
}
```

Credentials are read from the environment variables **`DB_USER`** and **`DB_PASSWORD`**.
Set them before starting the service:

```bash
export DB_USER=sa
export DB_PASSWORD=yourStrongPassword
python opc_client.py
```

On Windows:
```cmd
set DB_USER=sa
set DB_PASSWORD=yourStrongPassword
python opc_client.py
```

The ODBC connection is built automatically inside `db_mssql.py` from `DB_CONFIG`.
By default **ODBC Driver 18 for SQL Server** is used; the module falls back to
Driver 17 if 18 is not installed.

## Data Definition Language (DDL)

Replace the placeholders with your actual table schema and column names.

```sql
CREATE TABLE example (
    id   INT PRIMARY KEY,
    name VARCHAR(100)
);
```

## Stored Procedure: Sp_msr_value_send

Measurements are written to SQL Server by calling the stored procedure
`Sp_msr_value_send` once per measurement value (one call = one transaction).

### Parameters

| # | Parameter     | Type       | Description                                         |
|---|---------------|------------|-----------------------------------------------------|
| 1 | `p_ffc_id`    | `int`      | ТЗК identifier — from OPC tag mapping field `id1`  |
| 2 | `p_msd_id`    | `int`      | Instrument identifier — from OPC tag mapping `id2` |
| 3 | `p_msr_value` | `float(53)`| Measurement value                                   |
| 4 | `p_msr_time`  | `datetime` | Measurement time (local naive datetime)             |

### Timestamp handling

The OPC tag `SourceTimestamp` is used as `p_msr_time`.  If the timestamp is
timezone-aware (e.g., UTC from the OPC server), it is converted to the **local
system time** and the timezone information is removed before passing to
`pyodbc`.  The SQL Server then handles the resulting local datetime as-is.

No locale-dependent string formatting is used — `p_msr_time` is always passed
as a Python `datetime` object.

### Example SQL call (for reference only — actual calls are parameterised)

```sql
EXEC Sp_msr_value_send 19, 5, 9565, '07.10.2015 18:15:03';
```

### Buffering on failure

If the DB is unreachable when a measurement arrives, the record is saved to
`buffer/measurements.json` (via `JSONBufferManager.add_measurement`).  When
the DB becomes available again, buffered measurements are drained in order via
`bdrv_client.drain_buffer()` — each record is still sent as its own
transaction through the same `Sp_msr_value_send` procedure.