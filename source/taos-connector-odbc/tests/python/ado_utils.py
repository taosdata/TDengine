"""
TDengine utility functions for ADO operations
Common functions for database table creation and data insertion
"""

import datetime
import time
import pytz
import win32com.client

def create_table(conn, dbname='test', precision='ms'):
    """Ensure table exists"""
    try:
        conn.Execute(f"DROP DATABASE IF EXISTS {dbname};")
        # Create test database
        conn.Execute(f"CREATE DATABASE IF NOT EXISTS {dbname} PRECISION '{precision}';")

        # Create test table
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {dbname}.devices (
                  ts TIMESTAMP,
                  device_id NCHAR(20),
                  temperature FLOAT,
                  humidity FLOAT,
                  status BOOL
            );
            """
        conn.Execute(create_table_sql)
        print("Successfully created/verified database and table structure")
        return True
    except Exception as e:
        print(f"Failed to create table: {str(e)}")
        return False


def insert_data(conn, ts, device_id, temperature, humidity, dbname='test', status=True):
    """Safely insert data (using parameterized SQL)"""
    try:
        # Use parameterized SQL to prevent SQL injection
        # Insert test data
        insert_sql = f"""        
        INSERT INTO {dbname}.devices (ts, device_id, temperature, humidity, status)         
        VALUES             
            ({ts}, '{device_id}', {temperature}, {humidity}, true);        
        """
        conn.Execute(insert_sql)

        # Execute insert
        print(f"Successfully inserted device: {device_id}")
        return True
    except Exception as e:
        print(f"Failed to insert device {device_id}: {str(e)}")

        # Extract detailed error information
        try:
            errors = conn.Errors
            for i in range(errors.Count):
                err = errors.Item(i)
                print(f"   ADO Error #{i + 1}: {err.Description} (Code: 0x{err.Number:08X})")
        except:
            pass

        return False


def connect_to_database(dsn_name="TAOS_ODBC_WS_DSN"):
    """Create and return a database connection"""
    try:
        conn = win32com.client.Dispatch("ADODB.Connection")
        conn_str = f"DSN={dsn_name};"
        conn.Open(conn_str)
        print("Successfully connected to TDengine database!")
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {str(e)}")
        return None


def close_connection(conn):
    """Safely close database connection"""
    if conn and conn.State != 0:
        try:
            conn.Close()
            print("Database connection closed")
        except Exception as e:
            print(f"Error closing connection: {str(e)}")

def get_data_time(precision='ms'):
    current_ts = datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
    base_time = current_ts.strftime('%Y-%m-%d %H:%M:%S')

    if precision == 'ns':
        nanoseconds = int(time.time_ns() % 1_000_000_000)
        return f"{base_time}.{nanoseconds:09d}"
    elif precision == 'us':
        microseconds = current_ts.microsecond
        return f"{base_time}.{microseconds:06d}"
    elif precision == 'ms':
        milliseconds = current_ts.microsecond // 1000
        return f"{base_time}.{milliseconds:03d}"
    else:
        return f"{base_time}"
