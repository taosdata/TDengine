# ANCHOR: connect
from datetime import datetime
import pandas
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, TIMESTAMP, String

def connect():
    """Create a connection to TDengine using SQLAlchemy"""
    engine = create_engine(f"taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    print("Connected to TDengine successfully.")
    return conn
# ANCHOR_END: connect

# ANCHOR: pandas_to_sql_example
def pandas_to_sql_example(conn):
    """Test writing data to TDengine using pandas DataFrame.to_sql() method and verify the results"""
    try:
        conn.execute(text("CREATE DATABASE IF NOT EXISTS power"))
        conn.execute(text(
            "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"))
        conn.execute(text("USE power"))

        data = {
            "ts": [1729653691000, "2024-09-19 10:00:00", datetime(2024, 9, 20, 10, 11, 12, 456)],
            "current": [11.5, 12.3, 13.7],
            "voltage": [220, 230, 240],
            "phase": [1.0, 1.1, 1.2],
            "location": ["california.losangeles", "california.sandiego", "california.sanfrancisco"],
            "groupid": [2, 2, 3],
            "tbname": ["california", "sandiego", "sanfrancisco"]
        }
        df = pandas.DataFrame(data)
        rows_affected = df.to_sql("meters", conn, if_exists="append", index=False,
                                  dtype={
                                      "ts": TIMESTAMP,
                                      "current": Float,
                                      "voltage": Integer,
                                      "phase": Float,
                                      "location": String,
                                      "groupid": Integer,
                                  })
        assert rows_affected == 3, f"Expected to insert 3 rows, affected {rows_affected} rows"
    except Exception as err:
        print(f"Failed to insert data into power.meters, ErrMessage:{err}")
        raise err
# ANCHOR_END: pandas_to_sql_example

# ANCHOR: pandas_read_sql_example
def pandas_read_sql_example(conn):
    """Test reading data from TDengine using pandas read_sql() method"""
    try:
        sql = text("SELECT * FROM power.meters WHERE current > :current AND phase > :phase")
        sql_df = pandas.read_sql(
            sql=sql,
            con=conn,
            params={"current": 10, "phase": 1}
        )
        print(sql_df.head(3))
        print("Read data from TDengine successfully.")
    except Exception as err:
        print(f"Failed to read data from power.meters, ErrMessage:{err}")
        raise err
# ANCHOR_END: pandas_read_sql_example

# ANCHOR: pandas_read_sql_table_example
def pandas_read_sql_table_example(conn):
    """Test reading data from TDengine using pandas read_sql_table() method"""
    try:
        table_df = pandas.read_sql_table(
            table_name='meters',
            con=conn,
            index_col='ts',
            parse_dates=['ts'],
            chunksize=1000,  # optional, read data rows in chunks
            columns=[
                'ts',
                'current',
                'voltage',
                'phase',
                'location',
                'groupid'
            ],
        )

        total_rows = 0
        for i, chunk in enumerate(table_df, start=1):
            print(f"Processing chunk {i}")
            for index, row in chunk.iterrows():
                total_rows += 1
                print(f"no: {total_rows}")
                print(f"ts: {index}")
                print(f"current: {row['current']}")
                print(f"voltage: {row['voltage']}")
                print(f"phase: {row['phase']}")
                print(f"location: {row['location']}")
                print(f"groupid: {row['groupid']}")

        print("Read data from TDengine successfully using read_sql_table.")
    except Exception as err:
        print(f"Failed to read data from power.meters using read_sql_table, ErrMessage:{err}")
        raise err
# ANCHOR_END: pandas_read_sql_table_example

if __name__ == "__main__":
    conn = None
    try:
        conn = connect()
        pandas_to_sql_example(conn)
        pandas_read_sql_example(conn)
        pandas_read_sql_table_example(conn)
    finally:
        if conn:
            conn.close()
