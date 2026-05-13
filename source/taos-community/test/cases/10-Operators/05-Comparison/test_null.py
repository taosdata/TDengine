from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestFuncScalarNull:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_scalar_null(self):
        """Operator null

        1. Usage of NULL in the IN operator
        2. Comparison of NULL values
        3. Operations involving NULL values

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-9 Simon Guan Migrated from tsim/query/scalarNull.sim

        """

        tdLog.info(f"======== step1")
        tdSql.execute(f"create database db1 vgroups 3;")
        tdSql.execute(f"use db1;")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:00', 1, \"a\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:01', 2, \"b\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:02', 3, \"c\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:03', 4, \"d\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:04', 5, \"e\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:05', 6, \"f\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:06', null, null);")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:07', null, \"g\");")
        tdSql.execute(f"insert into tb1 values ('2022-04-26 15:15:08', 7, null);")

        tdSql.query(f"select * from tb1 where f1 in (1,2,3);")
        tdSql.checkRows(3)

        tdSql.query(f"select * from tb1 where f1 <>3;")
        tdSql.checkRows(6)

        tdSql.query(f"select * from tb1 where f1 in (1,2,3,null);")
        tdSql.checkRows(3)

        tdSql.query(f"select * from tb1 where f1 not in (1,2,3,null);")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1 in (null);")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1 = null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1 <> null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1 + 3 <> null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1+1 <>3+null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where f1+1*null <>3+null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where null;")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where null = null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from tb1 where null <> null;")
        tdSql.checkRows(0)

        # TODO: ENABLE IT
        # sql select * from tb1 where not (null <> null);
        # if $rows != 9 then
        #  return -1
        # endi

        # TODO: MOVE IT TO NORMAL CASE
        tdSql.error(f"select * from tb1 where not (null);")
