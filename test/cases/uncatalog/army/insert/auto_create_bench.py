import time
import taos

conn = taos.connect()

total_batches = 100
tables_per_batch = 100

def prepare_database():
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS test")
    cursor.execute("CREATE DATABASE IF NOT EXISTS test")
    cursor.execute("USE test")
    cursor.execute("CREATE STABLE IF NOT EXISTS stb (ts TIMESTAMP, a INT, b FLOAT, c BINARY(10)) TAGS (e_id INT)")
    cursor.close()

def test_auto_create_tables():
    """测试场景1：自动建表插入"""
    cursor = conn.cursor()
    cursor.execute("USE test")
    print("开始测试自动建表插入...")

    start_time = time.time()
    for _ in range(100):
        for batch in range(total_batches):
            # 生成当前批次的子表ID范围
            start_id = batch * tables_per_batch
            end_id = start_id + tables_per_batch

            # 构建批量插入SQL
            sql_parts = []
            for i in range(start_id, end_id):
                sql_part = f"t_{i} USING stb TAGS ({i}) VALUES ('2024-01-01 00:00:00', 1, 2.0, 'test')"
                sql_parts.append(sql_part)

            # 执行批量插入
            full_sql = "INSERT INTO " + " ".join(sql_parts)
            cursor.execute(full_sql)


    elapsed = time.time() - start_time
    print(f"自动建表插入耗时: {elapsed:.2f} 秒")

    cursor.close()
    return elapsed

def precreate_tables():
    """预处理：创建所有子表结构"""
    cursor = conn.cursor()
    cursor.execute("USE test")

    print("\n开始预创建子表...")
    start_time = time.time()

    for batch in range(total_batches):
        start_id = batch * tables_per_batch
        end_id = start_id + tables_per_batch

        for i in range(start_id, end_id):
            sql_part = f"CREATE TABLE t_{i} USING stb TAGS ({i})"
            cursor.execute(sql_part)

    elapsed = time.time() - start_time
    print(f"子表预创建耗时: {elapsed:.2f} 秒")

    cursor.close()

def test_direct_insert():
    """测试场景2：直接插入已存在的子表"""
    cursor = conn.cursor()
    cursor.execute("USE test")

    print("\n开始测试直接插入...")
    start_time = time.time()
    for _ in range(100):
        for batch in range(total_batches):
            start_id = batch * tables_per_batch
            end_id = start_id + tables_per_batch

            # 构建批量插入SQL
            sql_parts = []
            for i in range(start_id, end_id):
                sql_part = f"t_{i} VALUES ('2024-01-01 00:00:01', 1, 2.0, 'test')"
                sql_parts.append(sql_part)

            # 执行批量插入
            full_sql = "INSERT INTO " + " ".join(sql_parts)
            cursor.execute(full_sql)

    elapsed = time.time() - start_time
    print(f"直接插入耗时: {elapsed:.2f} 秒")

    cursor.close()
    return elapsed

if __name__ == "__main__":
    # 初始化数据库环境
    prepare_database()
    # 预创建所有子表
    precreate_tables()
    # 测试场景1：自动建表插入
    auto_create_time = test_auto_create_tables()
    # # 清理环境并重新初始化
    # prepare_database()
    # # 预创建所有子表
    # precreate_tables()
    # # 测试场景2：直接插入
    # direct_insert_time = test_direct_insert()

    # 打印最终结果
    print("\n测试结果对比:")
    print(f"自动建表插入耗时: {auto_create_time:.2f} 秒")
    # print(f"直接插入耗时:     {direct_insert_time:.2f} 秒")
    # print(f"性能差异:       {auto_create_time/direct_insert_time:.1f} 倍")