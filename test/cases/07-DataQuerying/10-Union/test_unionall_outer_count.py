from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestUnionAllOuterCountRegression:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_unionall_outer_count_regression(self):
        """union all subquery with outer count aggregation

        1. Reproduce planner path: outer count(*) on union all with one agg branch.

        Catalog:
            - Query:Union

        Since: v3.3.6.0

        Labels: regression,common,ci

        Jira: None

        History:
            - 2026-3-17 Tony Zhang add regression for empty partial agg targets during split

        """

        db = "union_outer_count_reg"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} cachemodel 'both'")
        tdSql.execute(f"use {db}")

        # first stable
        tdSql.execute(
            """
            CREATE STABLE `plan` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', 
            `val` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium')
            TAGS (`senid` VARCHAR(255), `inv` INT, `senid_name` VARCHAR(255))
            """
        )
        tdSql.execute("""CREATE table `plan_1` using `plan` tags ('126300009501', 1, '本年1月计划')""")
        tdSql.execute("""CREATE table `plan_2` using `plan` tags ('126300009502', 2, '本年2月计划')""")
        tdSql.execute("""CREATE table `plan_3` using `plan` tags ('126300009503', 3, '本年3月计划')""")
        tdSql.execute("""CREATE table `plan_4` using `plan` tags ('126300009504', 4, '本年4月计划')""")
        tdSql.execute("""CREATE table `plan_5` using `plan` tags ('126300009505', 5, '本年5月计划')""")
        tdSql.execute("""CREATE table `plan_6` using `plan` tags ('126300009506', 6, '本年6月计划')""")
        tdSql.execute("""CREATE table `plan_7` using `plan` tags ('126300009507', 7, '本年7月计划')""")
        tdSql.execute("""CREATE table `plan_8` using `plan` tags ('126300009508', 8, '本年8月计划')""")
        tdSql.execute("""CREATE table `plan_9` using `plan` tags ('126300009509', 9, '本年9月计划')""")
        tdSql.execute("""CREATE table `plan_10` using `plan` tags ('126300009510', 10, '本年10月计划')""")
        tdSql.execute("""CREATE table `plan_11` using `plan` tags ('126300009511', 11, '本年11月计划')""")
        tdSql.execute("""CREATE table `plan_12` using `plan` tags ('126300009512', 12, '本年12月计划')""")

        tdSql.execute("""insert into `plan_1` values ('2025-01-01 00:00:00', 100)""")
        tdSql.execute("""insert into `plan_2` values ('2025-02-01 00:00:00', 200)""")
        tdSql.execute("""insert into `plan_3` values ('2025-03-01 00:00:00', 300)""")
        tdSql.execute("""insert into `plan_4` values ('2025-04-01 00:00:00', 400)""")
        tdSql.execute("""insert into `plan_5` values ('2025-05-01 00:00:00', 500)""")
        tdSql.execute("""insert into `plan_6` values ('2025-06-01 00:00:00', 600)""")
        tdSql.execute("""insert into `plan_7` values ('2025-07-01 00:00:00', 700)""")
        tdSql.execute("""insert into `plan_8` values ('2025-08-01 00:00:00', 800)""")
        tdSql.execute("""insert into `plan_9` values ('2025-09-01 00:00:00', 900)""")
        tdSql.execute("""insert into `plan_10` values ('2025-10-01 00:00:00', 1000)""")
        tdSql.execute("""insert into `plan_11` values ('2025-11-01 00:00:00', 1100)""")
        tdSql.execute("""insert into `plan_12` values ('2025-12-01 00:00:00', 1200)""")

        # second stable
        tdSql.execute(
            """
            CREATE STABLE `hf` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', 
            `val` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium')
            TAGS (`senid` VARCHAR(255), `senid_name` VARCHAR(255))
            """
        )
        tdSql.execute("""CREATE table `hf_1` using `hf` tags ('4004101060', '本年发电计划')""")
        tdSql.execute("""insert into `hf_1` values ('2025-01-01 04:30:00', 3000)""")

        q1 = """
            select
                '1263000094' senid,
                (case when inv = 1 then '1月'
                when inv = 2 then '2月'
                when inv = 3 then '3月'
                when inv = 4 then '4月'
                when inv = 5 then '5月'
                when inv = 6 then '6月'
                when inv = 7 then '7月'
                when inv = 8 then '8月'
                when inv = 9 then '9月'
                when inv = 10 then '10月'
                when inv = 11 then '11月'
                when inv = 12 then '12月'
                end) point_name,
                ts time,
                val v
            from plan
            where senid like '1263000095%'
              and to_char(ts, 'yyyy') < to_char(today, 'yyyy')
              and inv in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        """
        q2 = """
            select
                '1263000094' senid,
                'year_plan' point_name,
                last(ts) tm,
                sum(val) v
            from plan
            where senid like '1263000094%'
              and to_char(ts, 'yyyy') < to_char(today, 'yyyy')
              and inv in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        """
        q3 = """
            select
                senid,  
                senid_name point_name,
                ts tm,
                val v
            from hf
            where senid = '4004101060'
              and ts >= '2025-01-01 04:00:00.000'
              and ts <= '2025-01-01 05:00:00.000'
        """

        tdSql.query(q1, show=True)
        tdSql.query(q2, show=True)
        tdSql.query(q3, show=True)
        # simple queries are ok
        tdSql.query(f"{q1} union all {q2} union all {q3}", show=True)
        tdSql.query(f"select * from ({q1} union all {q2} union all {q3}) u", show=True)

        # this nested query previously caused a core dump before the fix
        tdSql.query(f"select count(*) from ({q1} union all {q2} union all {q3}) tt", show=True)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 13)
