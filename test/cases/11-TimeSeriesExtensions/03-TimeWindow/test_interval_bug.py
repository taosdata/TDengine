from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestInterval:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_interval(self):
        """时间窗口填充 1y 的故障

        1. 创建一个特定选项的数据
        2. 创建超级表、子表并写入数据
        3. 写入仅一条数据
        4. 进行 1y 的时间窗口查询

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/compute/interval1.sim

        """

        tdSql.execute(
            f"CREATE DATABASE `alphacloud_alphaess` BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;"
        )

        tdSql.execute(f"use alphacloud_alphaess;")

        tdSql.execute(
            f"create stable st(ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `uk` VARCHAR(64) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY ) tags(ta int,tb int,tc int);"
        )

        tdSql.execute(f"create table t1 using st tags(1,1,1);")

        tdSql.execute(
            f'insert into t1 values ("1970-01-29 05:04:53.000"," 22::     ");'
        )

        tdSql.query(f"select _wstart, count(*) from st  interval(1y);")

        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 1)
