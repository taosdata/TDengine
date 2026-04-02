import win32com.client
import pythoncom
import os
from ado_utils import create_table, insert_data, connect_to_database, close_connection

# Define ADO constants
adUseClient = 3
adCursorType = 2
adLockType = 2
adCmdText = 1

adFilterNone = 0
adFilterPendingRecords = 1
adFilterAffectedRecords = 2
adFilterFetchedRecords = 3
adFilterConflictingRecords = 5

def connect_tdengine(dbname='test'):
    print("Attempting to connect to TDengine database...")

    try:
        # Create connection using utility function
        conn = connect_to_database("TAOS_ODBC_WS_DSN")
        if not conn:
            return
        
        # Create ADO recordset object
        rs = win32com.client.Dispatch("ADODB.Recordset")

        # Ensure table exists and insert test data
        if not create_table(conn):
            return

        # Insert test data using utility function
        insert_data(conn, 'now', 'device_001', 25.3, 45.2, dbname, True)
        insert_data(conn, 'now-10s', 'device_002', 26.8, 42.7, dbname, False)
        insert_data(conn, 'now-20s', 'device_003', 24.1, 47.9, dbname, True)
        insert_data(conn, 'now-30s', 'device_003', 25.1, 47.9, dbname, True)
        insert_data(conn, 'now-40s', 'device_003', 26.1, 47.9, dbname, True)
        insert_data(conn, 'now-50s', 'device_003', 26.1, 47.9, dbname, True)

        # Query data
        query = "SELECT * FROM test.devices;"
        rs.Open(query, conn)

        # Display results
        print("\nQuery results:")
        print(f"{'Timestamp':<25} {'Device ID':<15} {'Temp':<8} {'Humidity':<8} {'Status'}")
        print("-" * 65)

        while not rs.EOF:
            ts = rs.Fields("ts").Value
            device_id = rs.Fields("device_id").Value
            temperature = rs.Fields("temperature").Value
            humidity = rs.Fields("humidity").Value
            status = "Normal" if rs.Fields("status").Value else "Abnormal"

            print(f"{ts:<25} {device_id:<15} {temperature:<8.1f} {humidity:<8.1f} {status}")
            rs.MoveNext()

        # Clean up resources
        rs.Close()
        close_connection(conn)

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # Extract more error information
        if 'conn' in locals():
            errors = conn.Errors
            for i in range(errors.Count):
                err = errors.Item(i)
                print(f"  ADO Error #{i + 1}:")
                print(f"    Description: {err.Description}")
                print(f"    Code: {err.Number} (0x{err.Number:08X})")
                print(f"    Source: {err.Source}")

        # Ensure connection is closed
        if 'rs' in locals() and rs.State != 0:
            rs.Close()
        close_connection(conn if 'conn' in locals() else None)

def query_tdengine(dbname='test'):
    try:
        # Create connection using utility function
        conn = connect_to_database("TAOS_ODBC_WS_DSN")
        if not conn:
            return
            
        rs = win32com.client.Dispatch("ADODB.Recordset")
        rs.CursorLocation = adUseClient  # Must set to client cursor
        query = f"SELECT * FROM {dbname}.devices;"
        rs.Open(query, conn, adCursorType, adLockType)

        print(f"Total records: {rs.RecordCount}")
        rs.Filter = "device_id = 'device_003'"
        rs.Sort = "temperature DESC"

        print("\nFilter: device_id = 'device_003' Sort: temperature DESC")
        print(f"Filtered record count: {rs.RecordCount}")
        while not rs.EOF:
            ts = rs.Fields("ts").Value
            device_id = rs.Fields("device_id").Value
            temperature = rs.Fields("temperature").Value
            humidity = rs.Fields("humidity").Value
            status = "Normal" if rs.Fields("status").Value else "Abnormal"

            print(f"{ts:<25} {device_id:<15} {temperature:<8.1f} {humidity:<8.1f} {status}")
            rs.MoveNext()

    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Ensure connection is closed
        if 'rs' in locals() and rs.State != 0:
            rs.Close()
        close_connection(conn if 'conn' in locals() else None)

if __name__ == "__main__":
    # Initialize COM environment
    pythoncom.CoInitialize()
    connect_tdengine()
    query_tdengine()
    pythoncom.CoUninitialize()