# TDengine ADO Python Test Scripts

This directory contains Python scripts for testing TDengine database operations using ADO (ActiveX Data Objects).

## Files Overview

### Utility Module
- **`ado_utils.py`** - Common utility functions extracted from test scripts:
  - `create_table()` - Creates test database and table
  - `insert_data()` - Safely inserts data with error handling
  - `connect_to_database()` - Creates database connection
  - `close_connection()` - Safely closes database connection

### Test Scripts
- **`ado_insert.py`** - Tests data insertion and querying operations
- **`ado_query.py`** - Tests advanced querying with filtering and sorting

## Refactoring Changes

The common functions `create_table` and `insert_data` have been extracted from the individual test files into the shared `ado_utils.py` module. This provides:

### Benefits:
1. **Code Reusability** - Common functions can be used across multiple test scripts
2. **Maintainability** - Single place to update database schema or connection logic
3. **Consistency** - All scripts use the same error handling and connection patterns
4. **Cleaner Code** - Individual test scripts focus on their specific test scenarios

### Usage Pattern:
```python
from ado_utils import create_table, insert_data, connect_to_database, close_connection

# Connect to database
conn = connect_to_database("DSN_NAME")

# Create table if needed
create_table(conn)

# Insert data safely
insert_data(conn, 'now', 'device_001', 25.5, 60.0, True)

# Always close connection
close_connection(conn)
```

## Dependencies
- `win32com.client` - For ADO operations
- `pythoncom` - For COM initialization (where needed)

## Table Schema
The scripts create a test table with the following structure:
```sql
CREATE TABLE test.devices (
    ts TIMESTAMP,
    device_id NCHAR(20),
    temperature FLOAT,
    humidity FLOAT,
    status BOOL
);
```
