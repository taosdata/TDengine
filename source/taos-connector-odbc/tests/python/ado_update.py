import win32com.client
import time
import datetime
from ado_utils import create_table, insert_data, connect_to_database, close_connection

# Import ADODB constants
adCursorType = 2
adLockType = 2

def query_recent_records(conn, dbname='test', precision='ms', isModify=True, limit=5):
    """Query recent records"""
    try:
        rs = win32com.client.Dispatch("ADODB.Recordset")
        query = f"SELECT * FROM {dbname}.devices ORDER BY ts DESC LIMIT {limit};"
        rs.Open(
            Source=query,
            ActiveConnection=conn,
            CursorType=adCursorType,
            LockType=adLockType
        )

        records = []
        if not rs.EOF:
            if isModify:
                rs.Fields("device_id").Value = 'device_id_new_1'
                rs.Fields("temperature").Value = 1234
                rs.Fields("humidity").Value = 12233
                rs.Update()
            while not rs.EOF:
                record = {
                    'ts': rs.Fields("ts").Value,
                    'device_id': rs.Fields("device_id").Value,
                    'temperature': rs.Fields("temperature").Value,
                    'humidity': rs.Fields("humidity").Value,
                    'status': rs.Fields("status").Value
                }

                records.append(record)
                rs.MoveNext()

        return records
    except Exception as e:
        print(f"Failed to query records: {str(e)}")
        return []
    finally:
        if 'rs' in locals() and rs.State != 0:
            rs.Close()

def execute(dbname='test', precision='ms'):
    """Main program"""
    conn = None
    try:
        # Create main connection
        conn = connect_to_database("TAOS_ODBC_WS_DSN")
        if not conn:
            return
        print("Successfully connected to TDengine database!")

        # Ensure table exists
        if not create_table(conn, dbname, precision):
            return

        print("Main program: Starting to monitor data changes and add new records...")

        # Main loop: read data every 3 seconds and add a record
        for i in range(3):
            print(f"\n--- Main program polling #{i + 1} ---")

            # Insert new record
            device_id = f"main_device_{i + 1:03d}"
            if insert_data(conn, f'now +{i}a', device_id, 26.5 + i, 58.2 + i, dbname):
                print(f"Successfully added main program device: {device_id}")

        # Query and display recent records
        query_recent_records(conn, dbname, precision)
        records = query_recent_records(conn, dbname, precision, False)
        if records:
            print(f"Recent {len(records)} records:")
            for idx, record in enumerate(records):
                status = "Normal" if record['status'] else "Abnormal"
                print(
                    f"   #{idx + 1}: {record['ts']} | {record['device_id']} | "
                    f"Temperature: {record['temperature']:.1f} | Humidity: {record['humidity']:.1f} | "
                    f"Status: {status}"
                )

        else:
            print("No records found")

    except Exception as e:
        print(f"Main program error: {str(e)}")
    finally:
        # Ensure connection is closed
        if conn and conn.State != 0:
            try:
                close_connection(conn)
                print("Main program: Connection closed")
            except Exception as e:
                print(f"Error closing connection: {str(e)}")

if __name__ == "__main__":
    execute(dbname='test', precision='ms')
    execute(dbname='test_us', precision='us')
    execute(dbname='test_ns', precision='ns')
