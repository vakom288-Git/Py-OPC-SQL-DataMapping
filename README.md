# Py-OPC-SQL-DataMapping

## Installation
To install the Py-OPC-SQL-DataMapping project, run the following command:
```bash
pip install -r requirements.txt
```

## Configuration
All configuration is done in `opc_tags_list.py`:

- `OPC_URL` / `OPC_USER` / `OPC_PASS` — OPC UA server connection
- `DB_DSN` — pyodbc connection string for SQL Server (ODBC Driver 17/18)
- `EVENT_TAGS` / `ANALOG_TAGS` — lists of OPC node IDs to monitor
- `ANALOG_SAVE_INTERVAL`, `DB_BATCH_SIZE`, `JSON_BUFFER_MAX_MB` — tuning parameters

## SQL Server Connection String
The application uses **pyodbc** with ODBC Driver 17 or 18 for SQL Server.
Edit `DB_DSN` in `opc_tags_list.py`:

```python
# Windows Authentication (Integrated Security)
DB_DSN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.1.72,1433;"
    "DATABASE=DB_NEW_TEST;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# SQL Server Login (username/password)
DB_DSN = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.1.72,1433;"
    "DATABASE=DB_NEW_TEST;"
    "UID=sa;"
    "PWD=your_password_here;"
    "TrustServerCertificate=yes;"
)
```

Sensitive credentials should not be committed to version control — use environment variables or a secrets manager.

## Runtime Buffer
When the SQL Server is unavailable, data is buffered to the `./buffer/` directory (JSON files). The buffer is automatically replayed when the connection is restored. The `buffer/` directory is excluded from version control via `.gitignore`.