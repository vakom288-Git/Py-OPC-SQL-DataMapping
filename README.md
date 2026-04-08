# PAC-Py-OPC-SQL

## Installation
To install the PAC-Py-OPC-SQL project, run the following command:
```bash
pip install pac-py-opc-sql
```

## Configuration
You need to configure the application to connect to your OPC and SQL databases. Configuration can be done in the `config_local.py` file.

## SQL Authentication Examples
### Development
For development, use the following SQL connection parameters:
```python
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://user:password@localhost/dev_db'
```

### Production
For production, the connection string might look like this:
```python
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://user:password@production_server/prod_db'
```

## Local Secrets Configuration (`config_local.py`)
The `config_local.py` file should contain sensitive information and should not be tracked by version control. It's used to override the default configurations.

## Data Definition Language (DDL)
When using the application, you might need to setup your database schema with the following DDL commands:
```sql
CREATE TABLE example (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);
```

Please replace the placeholders with your actual database information and make sure to follow security best practices when managing your credentials.