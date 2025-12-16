import time
import taos

conn = taos.connect()

total_batches = 100
tables_per_batch = 100

def prepare_database():
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test")

    # set cachesize to 1 mb to make lru chche over limit
    cursor.execute("CREATE DATABASE IF NOT EXISTS test cachemodel 'both' cachesize 1")
    cursor.execute("USE test")

    # make decimal precision over 18 , so it will be stored in str 
    cursor.execute("CREATE STABLE IF NOT EXISTS stb (ts TIMESTAMP, a INT, b FLOAT, c DECIMAL(32, 2)) TAGS (e_id INT)")
    cursor.close()

def test_auto_create_tables():
    cursor = conn.cursor()
    cursor.execute("USE test")

    start_time = time.time()
    for _ in range(1):
        for batch in range(total_batches):
            start_id = batch * tables_per_batch
            end_id = start_id + tables_per_batch

            sql_parts = []
            for i in range(start_id, end_id):
                sql_part = f"t_{i} USING stb TAGS ({i}) VALUES ('now()', 1, 2.0,NULL)"
                sql_parts.append(sql_part)

            full_sql = "INSERT INTO " + " ".join(sql_parts)
            cursor.execute(full_sql)
    
    cursor.execute("FLUSH DATABASE test")

    cursor.close()

def precreate_tables():
    cursor = conn.cursor()
    cursor.execute("USE test")

    for batch in range(total_batches):
        start_id = batch * tables_per_batch
        end_id = start_id + tables_per_batch

        for i in range(start_id, end_id):
            sql_part = f"CREATE TABLE t_{i} USING stb TAGS ({i})"
            cursor.execute(sql_part)


    cursor.close()

def test_delete_table_data():
    cursor = conn.cursor()
    cursor.execute("DELETE FROM test.stb")
    cursor.execute("FLUSH DATABASE test")

    cursor.close()

if __name__ == "__main__":
    prepare_database()
    precreate_tables()
    auto_create_time = test_auto_create_tables()

    for i in range(10):
        test_delete_table_data()
        auto_create_time = test_auto_create_tables()