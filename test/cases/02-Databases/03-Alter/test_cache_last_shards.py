from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
import threading


class TestCacheLastShards:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_cacheshardbits_create(self):
        """create database with cacheshardbits option

        1. Create database with auto-calculated shards (-1, default)
        2. Create database with explicit shard values (0, 1, 2, 3, 4)
        3. Verify databases are created successfully

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_create")

        # Test with auto-calculated shards (default)
        tdSql.execute("drop database if exists db_auto_shards")
        tdSql.execute("create database db_auto_shards cachemodel 'both' cachesize 16")
        tdSql.query("select * from information_schema.ins_databases where name='db_auto_shards'")
        tdSql.checkRows(1)
        tdLog.info("Database created with auto shards")

        # Test with explicit shard values
        test_cases = [
            (0, "db_shard_0"),   # 1 shard (2^0)
            (1, "db_shard_1"),   # 2 shards (2^1)
            (2, "db_shard_2"),   # 4 shards (2^2)
            (3, "db_shard_3"),   # 8 shards (2^3)
            (4, "db_shard_4"),   # 16 shards (2^4)
        ]

        for shard_bits, dbname in test_cases:
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 cacheshardbits {shard_bits}")
            tdSql.query(f"select * from information_schema.ins_databases where name='{dbname}'")
            tdSql.checkRows(1)
            tdLog.info(f"Database {dbname} created with cacheshardbits={shard_bits}")

        # Cleanup
        tdSql.execute("drop database if exists db_auto_shards")
        for _, dbname in test_cases:
            tdSql.execute(f"drop database if exists {dbname}")

    def test_cacheshardbits_alter(self):
        """alter database cacheshardbits option

        1. Create database with cacheshardbits=1
        2. Create tables and insert data
        3. Query to populate cache
        4. Alter cacheshardbits to 3
        5. Insert more data and verify cache still works

        Catalog:
            - Database:Alter

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_alter")

        dbname = "db_alter_shards"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 16 cacheshardbits 1")

        # Create tables and insert data
        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(10):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        # Query to populate cache
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("Initial last query completed")

        # Alter cacheshardbits
        tdSql.execute(f"alter database {dbname} cacheshardbits 3")
        tdLog.info(f"Altered {dbname} to cacheshardbits=3")

        # Insert more data after alter
        for i in range(10):
            tdSql.execute(f"insert into tb{i} values(now+1s, {i+100}, {(i+100)*1.5})")

        # Query again to verify cache still works
        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("Last query after alter completed")

        # Verify the new values are returned
        tdSql.query("select last(c1) from tb0")
        tdSql.checkData(0, 0, 100)

        # Cleanup
        tdSql.execute(f"drop database if exists {dbname}")

    def test_cacheshardbits_concurrent(self):
        """concurrent access with multiple shards

        1. Create database with cacheshardbits=3 (8 shards)
        2. Create 50 tables with data
        3. Run concurrent queries from multiple threads
        4. Verify all queries succeed

        Catalog:
            - Database:Query

        Since: v3.3.6.0

        Labels: common

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_concurrent")

        dbname = "db_concurrent_shards"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} cachemodel 'both' cachesize 32 cacheshardbits 3")

        tdSql.execute(f"use {dbname}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float, c3 binary(20)) tags(t1 int)")

        # Create multiple tables
        num_tables = 50
        for i in range(num_tables):
            tdSql.execute(f"create table tb{i} using st tags({i})")

        # Insert data
        for i in range(num_tables):
            for j in range(10):
                tdSql.execute(f"insert into tb{i} values(now+{j}s, {i*100+j}, {(i*100+j)*1.5}, 'data_{i}_{j}')")

        # Concurrent queries
        def query_last_cache(table_id):
            try:
                import taos
                conn = taos.connect()
                cursor = conn.cursor()
                cursor.execute(f"use {dbname}")

                for _ in range(20):
                    cursor.execute(f"select last(*) from tb{table_id}")
                    result = cursor.fetchall()
                    if not result:
                        tdLog.exit(f"Query failed for tb{table_id}")

                cursor.close()
                conn.close()
            except Exception as e:
                tdLog.exit(f"Thread error: {str(e)}")

        # Create threads for concurrent access
        threads = []
        for i in range(10):
            t = threading.Thread(target=query_last_cache, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        tdLog.info("Concurrent access test completed successfully")

        # Cleanup
        tdSql.execute(f"drop database if exists {dbname}")

    def test_cacheshardbits_invalid(self):
        """invalid cacheshardbits values

        1. Test that invalid shard bits (>= 20) are rejected
        2. Test negative values (except -1)
        3. Verify proper error messages

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_invalid")

        # Test invalid values >= 20
        tdSql.error("create database db_invalid1 cacheshardbits 20")
        tdSql.error("create database db_invalid2 cacheshardbits 25")
        tdSql.error("create database db_invalid3 cacheshardbits 100")

        # Test negative values (except -1 which is valid for auto-calculation)
        tdSql.error("create database db_invalid4 cacheshardbits -2")
        tdSql.error("create database db_invalid5 cacheshardbits -10")

        tdLog.info("Invalid values properly rejected")

    def test_cacheshardbits_with_cache_models(self):
        """cacheshardbits with different cache models

        1. Test cacheshardbits with cachemodel 'none'
        2. Test cacheshardbits with cachemodel 'last_row'
        3. Test cacheshardbits with cachemodel 'last_value'
        4. Test cacheshardbits with cachemodel 'both'

        Catalog:
            - Database:Create

        Since: v3.3.6.0

        Labels: common,ci

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_with_cache_models")

        cache_models = ['none', 'last_row', 'last_value', 'both']

        for model in cache_models:
            dbname = f"db_cache_{model}"
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(f"create database {dbname} cachemodel '{model}' cachesize 16 cacheshardbits 2")
            tdSql.query(f"select * from information_schema.ins_databases where name='{dbname}'")
            tdSql.checkRows(1)
            tdLog.info(f"Database {dbname} created with cachemodel='{model}' and cacheshardbits=2")

            # Create table and insert data
            tdSql.execute(f"use {dbname}")
            tdSql.execute("create stable st(ts timestamp, c1 int) tags(t1 int)")
            tdSql.execute("create table tb1 using st tags(1)")
            tdSql.execute("insert into tb1 values(now, 100)")

            # Query based on cache model
            if model != 'none':
                tdSql.query("select last(*) from st")
                tdSql.checkRows(1)

            # Cleanup
            tdSql.execute(f"drop database if exists {dbname}")

    def test_cacheshardbits_performance(self):
        """compare performance with different shard counts

        1. Create database with 1 shard
        2. Create database with 4 shards
        3. Insert data and query
        4. Compare basic functionality (not strict performance measurement)

        Catalog:
            - Database:Query

        Since: v3.3.6.0

        Labels: common

        Jira: TD-336

        History:
            - 2026-03-18 Created for cacheshardbits feature

        """

        tdLog.info("========== test_cacheshardbits_performance")

        # Test with 1 shard
        db1 = "db_perf_1shard"
        tdSql.execute(f"drop database if exists {db1}")
        tdSql.execute(f"create database {db1} cachemodel 'both' cachesize 32 cacheshardbits 0")
        tdSql.execute(f"use {db1}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(100):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("1 shard test completed")

        # Test with 4 shards
        db2 = "db_perf_4shards"
        tdSql.execute(f"drop database if exists {db2}")
        tdSql.execute(f"create database {db2} cachemodel 'both' cachesize 32 cacheshardbits 2")
        tdSql.execute(f"use {db2}")
        tdSql.execute("create stable st(ts timestamp, c1 int, c2 float) tags(t1 int)")

        for i in range(100):
            tdSql.execute(f"create table tb{i} using st tags({i})")
            tdSql.execute(f"insert into tb{i} values(now, {i}, {i*1.5})")

        tdSql.query("select last(*) from st")
        tdSql.checkRows(1)
        tdLog.info("4 shards test completed")

        # Cleanup
        tdSql.execute(f"drop database if exists {db1}")
        tdSql.execute(f"drop database if exists {db2}")
