import pandas
import taos
import taosrest
import utils
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, TIMESTAMP, String
from utils import tear_down_database, PORT

load_dotenv()

host = "localhost"
port = PORT


def test_insert_test_data():
    conn = taos.connect(host=host, port=port, user=utils.test_username(), password=utils.test_password())
    c = conn.cursor()
    c.execute("drop database if exists test")
    c.execute("create database test")
    c.execute(
        "CREATE STABLE IF NOT EXISTS test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"
    )
    c.execute("create table test.tb (ts timestamp, c1 int, c2 double)")
    c.execute("insert into test.tb values (now+1s, -100, -200.3) (now+10s, -101, -340.2423424)")
    c.execute("insert into test.tb values (now+2s, -100, -200.3) (now+20s, 101, 1.2423424)")
    c.execute("insert into test.tb values (now+3s, -100, -200.3) (now+30s, 102, 2.2423424)")
    c.execute("insert into test.tb values (now+4s, -100, -200.3) (now+40s, 103, 3.2423424)")


def test_pandas_read_from_rest_connection():
    if taos.IS_V3:
        return
    conn = taosrest.connect(url=f"{host}:6041")
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_native_connection():
    if taos.IS_V3:
        return
    conn = taos.connect(host=host, port=port, user=utils.test_username(), password=utils.test_password())
    df: pandas.DataFrame = pandas.read_sql("select * from test.tb", conn)
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_sqlalchemy_taos():
    if taos.IS_V3:
        return
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    df: pandas.DataFrame = pandas.read_sql(text("select * from test.tb"), conn)
    conn.close()
    assert isinstance(df.ts[0], datetime)
    assert df.shape == (2, 3)


def test_pandas_read_from_sqlalchemy_stmt():
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}?timezone=Asia/Shanghai"
    )
    conn = engine.connect()
    sql = text("SELECT * FROM test.tb WHERE c1 > :c1 AND c2 > :c2")
    df = pandas.read_sql(sql=sql, con=conn, params={"c1": 100, "c2": 0})  # 实际参数值（根据需求修改）
    assert df.shape[0] == 3
    assert sorted(df["c1"].tolist()) == [101, 102, 103]
    print(df)


def test_pandas_tosql_auto_create_table():
    # Super table must exist using this method
    data = {
        "ts": [1626861392589, 1626861392590, 1626861392591],
        "current": [11.5, 12.3, 13.7],
        "voltage": [220, 230, 240],
        "phase": [1.0, 1.1, 1.2],
        "location": ["california.losangeles", "california.sandiego", "california.sanfrancisco"],
        "groupid": [2, 2, 3],
        "tbname": ["california", "sandiego", "xxxx"],
    }
    df = pandas.DataFrame(data)
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}/test?timezone=Asia/Shanghai"
    )
    rows_affected = df.to_sql(
        "meters",
        engine.connect(),
        if_exists="append",
        index=False,
        dtype={
            "ts": TIMESTAMP,  # TDengine timestamp type
            "current": Float,  # TDengine integer type
            "voltage": Integer,  # TDengine float type
            "phase": Float,
            "location": String,
            "groupid": Integer,
        },
    )
    assert rows_affected == 3, f"Expected to insert 3 rows, affected {rows_affected} rows"


def test_pandas_tosql():
    """Test writing data to TDengine using pandas DataFrame.to_sql() method and verify the results"""
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}/test?timezone=Asia/Shanghai"
    )

    # Prepare test data
    data = {
        "ts": [1626861392589, "2024-09-19 10:00:00", datetime(2024, 9, 20, 10, 11, 12, 456)],
        "c1": [7, 8, 9],
        "c2": [1.1, 1.2, 1.3],
    }
    df = pandas.DataFrame(data)

    # Create connection
    conn = engine.connect()

    try:
        # 1. Create table structure first
        conn.execute(text("CREATE TABLE IF NOT EXISTS tb1 (ts timestamp, c1 int, c2 double)"))

        # 2. Get row count before insertion
        result_before = conn.execute(text("SELECT COUNT(*) FROM tb1"))
        count_before = result_before.fetchone()[0]
        print(f"Before insertion: tb1 table has {count_before} rows")

        # 3. Insert data using pandas to_sql
        rows_affected = df.to_sql(
            "tb1",
            conn,
            if_exists="append",
            index=False,
            dtype={
                "ts": TIMESTAMP,  # TDengine timestamp type
                "c1": Integer,  # TDengine integer type
                "c2": Float,  # TDengine float type
            },
        )

        # 4. Verify row count after insertion
        result_after = conn.execute(text("SELECT COUNT(*) FROM tb1"))
        count_after = result_after.fetchone()[0]
        print(f"After insertion: tb1 table has {count_after} rows")

        # 5. Assert row count change
        expected_new_rows = len(df)
        actual_new_rows = count_after - count_before
        assert (
            actual_new_rows == expected_new_rows
        ), f"Expected to insert {expected_new_rows} rows, actually inserted {actual_new_rows} rows"
        assert (
            rows_affected == expected_new_rows
        ), f"Expected to insert {expected_new_rows} rows, affected {rows_affected} rows"

        # 6. Read and verify the inserted specific data
        query_result = conn.execute(text("SELECT * FROM tb1 WHERE c1 IN (7, 8, 9) ORDER BY ts"))
        inserted_data = query_result.fetchall()

        # 7. Verify the inserted data content
        assert len(inserted_data) == 3, f"Expected to query 3 rows, actually queried {len(inserted_data)} rows"

        # Verify each row's data
        for i, row in enumerate(inserted_data):
            ts, c1, c2 = row
            expected_c1 = data["c1"][i]
            expected_c2 = data["c2"][i]

            assert c1 == expected_c1, f"Row {i + 1} c1 value mismatch: expected {expected_c1}, actual {c1}"
            assert (
                abs(c2 - expected_c2) < 0.0001
            ), f"Row {i + 1} c2 value mismatch: expected {expected_c2}, actual {c2}"

        # 8. Verify using pandas read
        result_df = pandas.read_sql(text("SELECT * FROM tb1 WHERE c1 IN (7, 8, 9) ORDER BY ts"), conn)

        # Verify DataFrame structure
        assert (
            result_df.shape[0] == 3
        ), f"Pandas read result row count incorrect: expected 3 rows, actual {result_df.shape[0]} rows"
        assert (
            result_df.shape[1] == 3
        ), f"Pandas read result column count incorrect: expected 3 columns, actual {result_df.shape[1]} columns"

        # Verify data types
        assert isinstance(result_df.ts[0], datetime), "Timestamp column should be datetime type"

        # Verify data values
        assert result_df["c1"].tolist() == [7, 8, 9], f"c1 column values mismatch: {result_df['c1'].tolist()}"
        assert all(
            abs(a - b) < 0.0001 for a, b in zip(result_df["c2"].tolist(), [1.1, 1.2, 1.3])
        ), f"c2 column values mismatch: {result_df['c2'].tolist()}"

        print("✅ pandas to_sql write verification successful!")
        print(f"Successfully inserted {len(df)} rows into tb1 table")
        print(f"Inserted data:\n{result_df}")

    finally:
        conn.close()


def test_pandas_read_sql_table():
    test_insert_test_data()
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}/test?timezone=Asia/Shanghai"
    )
    chunk_size = 1
    chunks = pandas.read_sql_table(
        table_name="tb",
        con=engine,
        index_col="ts",
        chunksize=chunk_size,
        parse_dates=["ts"],
        columns=["ts", "c1", "c2"],
    )

    total_rows = 0
    data = []
    for i, chunk in enumerate(chunks):
        print(f"Chunk {i}:")
        for index, row in chunk.iterrows():
            total_rows += 1
            print(f"Row {total_rows}:")
            print(f"  Timestamp (index): {index}")
            print(f"  Column c1: {row['c1']}")
            print(f"  Column c2: {row['c2']}")
            print("---")
            data.append(row["c1"])

    assert sorted(data) == [-101, -100, -100, -100, -100, 101, 102, 103]
    print(f"Total processed {total_rows} rows of data")


def test_pandas_tosql_simple_verification():
    """Simplified version: demonstrate several common methods to verify pandas to_sql write results"""
    engine = create_engine(
        f"taos://{utils.test_username()}:{utils.test_password()}@{host}:{PORT}/test?timezone=Asia/Shanghai"
    )

    # Prepare test data
    test_data = pandas.DataFrame({"ts": [1626861400000, 1626861410000], "c1": [100, 200], "c2": [10.5, 20.5]})

    conn = engine.connect()

    try:
        # Create test table
        conn.execute(text("CREATE TABLE IF NOT EXISTS tb_verify (ts timestamp, c1 int, c2 double)"))

        # Method 1: Write data using to_sql
        test_data.to_sql("tb_verify", conn, if_exists="append", index=False)

        # Method 2: Verify total row count
        total_count = conn.execute(text("SELECT COUNT(*) FROM tb_verify")).fetchone()[0]
        assert total_count >= 2, f"Table should have at least 2 rows, actual {total_count} rows"

        # Method 3: Verify specific conditional data
        specific_data = conn.execute(text("SELECT * FROM tb_verify WHERE c1 IN (100, 200) ORDER BY c1")).fetchall()
        assert len(specific_data) == 2, "Should query 2 rows of specific data"

        # Method 4: Read and compare using pandas
        result_df = pandas.read_sql(text("SELECT * FROM tb_verify WHERE c1 IN (100, 200) ORDER BY c1"), conn)

        # Compare data values
        assert result_df["c1"].tolist() == [100, 200], "c1 column data mismatch"
        assert result_df["c2"].tolist() == [10.5, 20.5], "c2 column data mismatch"

        print("✅ Simplified verification passed")

    finally:
        # Cleanup
        conn.close()


def teardown_module(module):
    conn = taos.connect(host=host, port=port, user=utils.test_username(), password=utils.test_password())
    db_name = "test"
    tear_down_database(conn, db_name)
    conn.close()


if __name__ == "__main__":
    test_insert_test_data()
    test_pandas_read_from_rest_connection()
