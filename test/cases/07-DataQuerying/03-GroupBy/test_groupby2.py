from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestGroupBy2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_groupby2(self):
        """Group By

        1.

        Catalog:
            - Query:GroupBy

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/query/groupby.sim

        """

        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create table test(pos_time TIMESTAMP,target_id INT ,data DOUBLE) tags(scene_id BIGINT,data_stage VARCHAR(64),data_source VARCHAR(64));"
        )

        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232060000,413254290,1);"
        )
        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232061000,413254290,2);"
        )
        tdSql.execute(
            f"insert into _413254290_108_1001_ using test tags(108,'1001','') values(1667232062000,413254290,3);"
        )
        tdSql.execute(
            f"insert into _413254000_108_1001_ using test tags(109,'1001','') values(1667232060000,413254290,3);"
        )
        tdSql.execute(
            f"insert into _413254000_108_1001_ using test tags(109,'1001','') values(1667232062000,413254290,3);"
        )

        tdSql.query(
            f"select target_name,max(time_diff) AS time_diff,(count(1)) AS track_count from (select tbname as target_name,diff(pos_time) time_diff from test where tbname in ('_413254290_108_1001_','_413254000_108_1001_') partition by tbname) a group by target_name;"
        )
        tdSql.checkRows(2)
