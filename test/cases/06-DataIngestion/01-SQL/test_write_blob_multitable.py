from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestWriteBlobMultiTable:
    """Regression test for SIGSEGV in fillVgroupDataCxt when aSubmitBlobData is NULL.

    Bug: parInsertUtil.c fillVgroupDataCxt() crashed because aSubmitBlobData
    was only initialized when the FIRST table in a vgroup had hasBlob=true.

    Minimal reproduction: Create a database with vgroups=1, then create two
    supertables (one WITHOUT blob, one WITH blob). Their child tables share
    the same vgroup (vgId=1). When insMergeTableDataCxt groups tables by vgId,
    a non-blob child table processed first leaves aSubmitBlobData=NULL, then
    a blob child table crashes on taosArrayPush(NULL, ...).
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_blob_crash_vgroup_shared(self):
        """Two supertables (with/without blob) share one vgroup -> crash

        1. Create database with vgroups=1
        2. Create supertable WITHOUT blob
        3. Create supertable WITH blob
        4. Multi-table INSERT into both supertables' child tables
        5. Both sets of child tables land in same vgId -> triggers crash
           if non-blob table is processed first in hash iteration

        Since: v3.3.8.0
        Labels: common,ci,blob
        """
        dbname = "blb1"
        tdSql.execute(f"create database if not exists {dbname} vgroups 1")
        tdSql.execute(f"use {dbname}")

        # Supertable WITHOUT blob
        tdSql.execute(
            "create stable if not exists st_noblob (ts timestamp, val int) tags (t1 int)"
        )
        # Supertable WITH blob
        tdSql.execute(
            "create stable if not exists st_blob (ts timestamp, val int, data blob) tags (t1 int)"
        )

        # Multi-table INSERT: mix child tables from both supertables
        # All land in vgId=1 (vgroups=1)
        # Insert multiple non-blob tables to increase probability they are
        # processed first in hash iteration
        sql = (
            "insert into "
            "n1 using st_noblob tags(1) values ('2024-01-01 00:00:00', 1) "
            "n2 using st_noblob tags(2) values ('2024-01-01 00:00:01', 2) "
            "n3 using st_noblob tags(3) values ('2024-01-01 00:00:02', 3) "
            "b1 using st_blob  tags(1) values ('2024-01-01 00:00:03', 4, 'blob_1') "
            "n4 using st_noblob tags(4) values ('2024-01-01 00:00:04', 5) "
            "b2 using st_blob  tags(2) values ('2024-01-01 00:00:05', 6, 'blob_2') "
        )
        tdSql.execute(sql)

        # Verify data
        tdSql.query("select count(*) from st_noblob")
        tdSql.checkData(0, 0, 4)

        tdSql.query("select count(*) from st_blob")
        tdSql.checkData(0, 0, 2)

        tdSql.query("select * from st_blob order by ts")
        for i in range(tdSql.getRows()):
            tdLog.info(f"row {i}: {tdSql.getData(i, 0)}, {tdSql.getData(i, 1)}, {tdSql.getData(i, 2)}")

        tdSql.query("select * from st_noblob order by ts")
        for i in range(tdSql.getRows()):
            tdLog.info(f"row {i}: {tdSql.getData(i, 0)}, {tdSql.getData(i, 1)}")

    def test_blob_crash_more_tables(self):
        """More tables to ensure hash iteration hits non-blob first

        Same as above but with many more non-blob child tables to make
        it almost certain that a non-blob table is processed before any
        blob table in the hash iteration.

        Since: v3.3.8.0
        Labels: common,ci,blob
        """
        dbname = "blb2"
        tdSql.execute(f"create database if not exists {dbname} vgroups 1")
        tdSql.execute(f"use {dbname}")

        tdSql.execute(
            "create stable if not exists st_noblob (ts timestamp, val int) tags (t1 int)"
        )
        tdSql.execute(
            "create stable if not exists st_blob (ts timestamp, val int, data blob) tags (t1 int)"
        )

        # 50 non-blob child tables + 10 blob child tables
        parts = []
        for i in range(50):
            parts.append(
                f"n{i} using st_noblob tags({i}) values "
                f"({1700000000000 + i}, {i})"
            )
        for i in range(10):
            parts.append(
                f"b{i} using st_blob tags({i}) values "
                f"({1700000001000 + i}, {i}, 'blob_{i}')"
            )
        sql = "insert into " + " ".join(parts)
        tdSql.execute(sql)

        tdSql.query("select count(*) from st_noblob")
        tdSql.checkData(0, 0, 50)

        tdSql.query("select count(*) from st_blob")
        tdSql.checkData(0, 0, 10)

    def test_blob_crash_normal_table(self):
        """Minimal repro: two normal tables (one int, one blob) in same vgroup

        INSERT INTO t1 VALUES(...) t2 VALUES(...) where t1 has no blob
        and t2 has blob, both in vgroups=1. If t1 is iterated first,
        aSubmitBlobData stays NULL and t2 crashes on taosArrayPush(NULL).

        Since: v3.3.8.0
        Labels: common,ci,blob
        """
        dbname = "blb3"
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname} vgroups 1")
        tdSql.execute(f"use {dbname}")

        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute("create table t2 (ts timestamp, v blob)")

        tdSql.execute("insert into t1 values (now, 1) t2 values (now, 'hello')")

        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, 1)

        tdSql.query("select count(*) from t2")
        tdSql.checkData(0, 0, 1)
