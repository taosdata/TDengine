import sys
import os

from new_test_framework.utils import tdLog, tdSql

class TestFunElapsed:
    #
    # ------------------ case 1 ------------------
    #    
    def setup_class(cls):
        cls.dbname = 'db'
        cls.table_dic = {
            "super_table": ["st1", "st2", "st_empty"],
            "child_table": ["ct1_1", "ct1_2", "ct1_empty", "ct2_1", "ct2_2", "ct2_empty"],
            "tags_value": [("2023-03-01 15:00:00", 1, 'bj'), ("2023-03-01 15:10:00", 2, 'sh'), ("2023-03-01 15:20:00", 3, 'sz'), ("2023-03-01 15:00:00", 4, 'gz'), ("2023-03-01 15:10:00", 5, 'cd'), ("2023-03-01 15:20:00", 6, 'hz')],
            "common_table": ["t1", "t2", "t_empty"]
        }
        cls.start_ts = 1677654000000 # 2023-03-01 15:00:00.000
        cls.row_num = 100
        # db
        tdSql.execute(f"create database {cls.dbname};")
        tdSql.execute(f"use {cls.dbname};")
        tdLog.debug(f"Create database {cls.dbname}")

        # commont table
        for common_table in cls.table_dic["common_table"]:
            tdSql.execute(f"create table {common_table} (ts timestamp, c_ts timestamp, c_int int, c_bigint bigint, c_double double, c_nchar nchar(16));")
            tdLog.debug("Create common table %s" % common_table)

        # super table
        for super_table in cls.table_dic["super_table"]:
            tdSql.execute(f"create stable {super_table} (ts timestamp, c_ts timestamp, c_int int, c_bigint bigint, c_double double, c_nchar nchar(16)) tags (t1 timestamp, t2 int, t3 binary(16));")
            tdLog.debug("Create super table %s" % super_table)

        # child table
        for i in range(len(cls.table_dic["child_table"])):
            if cls.table_dic["child_table"][i].startswith("ct1"):
                tdSql.execute("create table {} using {} tags('{}', {}, '{}');".format(cls.table_dic["child_table"][i], "st1", cls.table_dic["tags_value"][i][0], cls.table_dic["tags_value"][i][1], cls.table_dic["tags_value"][i][2]))
            elif cls.table_dic["child_table"][i].startswith("ct2"):
                tdSql.execute("create table {} using {} tags('{}', {}, '{}');".format(cls.table_dic["child_table"][i], "st2", cls.table_dic["tags_value"][i][0], cls.table_dic["tags_value"][i][1], cls.table_dic["tags_value"][i][2]))

        # insert data
        table_list = ["t1", "t2", "ct1_1", "ct1_2", "ct2_1", "ct2_2"]
        for t in table_list:
            sql = "insert into {} values".format(t)
            for i in range(cls.row_num):
                sql += "({}, {}, {}, {}, {}, '{}'),".format(cls.start_ts + i * 1000, cls.start_ts + i * 1000, 32767+i, 65535+i, i, t + str(i))
            sql += ";"
            tdSql.execute(sql)
            tdLog.debug("Insert data into table %s" % t)

    def do_normal_query(self):
        """Agg-special: Elapsed

        test Elapsed function

        Catalog:
            - Function:Aggregate
            
        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        # only one timestamp
        tdSql.query("select elapsed(ts) from t1 group by c_ts;")
        tdSql.checkRows(self.row_num)
        tdSql.checkData(0, 0, 0)

        tdSql.query("select elapsed(ts, 1m) from t1 group by c_ts;")
        tdSql.checkRows(self.row_num)
        tdSql.checkData(0, 0, 0)

        # child table with group by
        tdSql.query("select elapsed(ts) from ct1_2 group by tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 99000)

        # empty super table
        tdSql.query("select elapsed(ts, 1s) from st_empty group by tbname;")
        tdSql.checkRows(0)

        # empty child table
        tdSql.query("select elapsed(ts, 1s) from ct1_empty group by tbname;")
        tdSql.checkRows(0)

        # empty common table
        tdSql.query("select elapsed(ts, 1s) from t_empty group by tbname;")
        tdSql.checkRows(0)

        # unit as second
        tdSql.query("select elapsed(ts, 1s) from st2 group by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 99)

        # unit as minute
        tdSql.query("select elapsed(ts, 1m) from st2 group by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.65)

        # unit as hour
        tdSql.query("select elapsed(ts, 1h) from st2 group by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 0.0275)

    def do_query_with_filter(self):
        """test elapsed function with filter

        test elapsed function with filter

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-9-14 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        end_ts = 1677654000000 + 1000 * 99
        query_list = [
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts >= 1677654000000 group by tbname;",
                "res": [(99.0, ), (99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts >= 1677654000000 and c_ts >= 1677654000000 group by tbname;",
                "res": [(99.0, ), (99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts >= 1677654000000 and c_ts >= 1677654000000 and t1='2023-03-01 15:10:00.000' group by tbname;",
                "res": [(99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from st_empty where ts >= 1677654000000 and c_ts >= 1677654000000 and t1='2023-03-01 15:10:00.000' group by tbname;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_1 where ts >= 1677654000000 group by tbname;",
                "res": [(99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_2 where ts >= 1677654000000 and c_ts >= 1677654000000 group by tbname;",
                "res": [(99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_empty where ts >= 1677654000000 and c_ts >= 1677654000000 group by tbname;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from t1 where ts >= 1677654000000 group by tbname;",
                "res": [(99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from t2 where ts >= 1677654000000 and c_ts >= 1677654000000 group by tbname;",
                "res": [(99.0, )]
            },
            {
                "sql": "select elapsed(ts, 1s) from t_empty where ts >= 1677654000000 and c_ts >= 1677654000000 group by tbname;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_ts > {} group by tbname;".format(end_ts),
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_ts > {} and t1='2023-03-01 15:10:00' group by tbname;".format(end_ts),
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_int < 1 group by tbname;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_int >= 1 and t1='2023-03-01 15:10:00' group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_int <> 1 and t1='2023-03-01 15:10:00' group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_nchar like 'ct2_%' and t1='2023-03-01 15:10:00' group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_nchar like 'ct1_%' and t1='2023-03-01 15:10:00' group by tbname;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_nchar match '^ct2_' and t1='2023-03-01 15:10:00' group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and c_nchar nmatch '^ct1_' and t1='2023-03-01 15:10:00' group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st2 where ts >= 1677654000000 and t3 like 'g%' group by tbname;",
                "res": [(99,)]
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    def do_query_with_other_function(self):
        """test elapsed function with other function

        test elapsed function with avg, count, leastsquares, spread, sum, hyperloglog, twa

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        query_list = [
            {
                "sql": "select avg(c_int), count(*), elapsed(ts, 1s), leastsquares(c_int, 0, 1), spread(c_bigint), sum(c_int), hyperloglog(c_int) from st1;",
                "res": [(32816.5, 200, 99.0, '{slop:0.499962, intercept:32766.753731}', 99.0, 6563300, 100)]
            },
            {
                "sql": "select twa(c_int) * elapsed(ts, 1s) from ct1_1;",
                "res": [(3.248833500000000e+06,)]
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    def do_query_with_join(self):
        """test elapsed function with join

        test elapsed function with join

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        query_list = [
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, st2 where st1.ts = st2.ts;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, st_empty where st1.ts = st_empty.ts and st1.c_ts = st_empty.c_ts;",
                "res": []
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, ct1_1 where st1.ts = ct1_1.ts;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ct1.ts, 1s) from ct1_1 ct1, ct1_2 ct2 where ct1.ts = ct2.ts;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ct1.ts, 1s) from ct1_1 ct1, ct1_empty ct2 where ct1.ts = ct2.ts;",
                "res": []
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, ct1_empty where st1.ts = ct1_empty.ts;",
                "res": []
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, t1 where st1.ts = t1.ts;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, t_empty where st1.ts = t_empty.ts;",
                "res": []
            },
            {
                "sql": "select elapsed(ct1.ts, 1s) from ct1_1 ct1, t1 t2 where ct1.ts = t2.ts;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ct1.ts, 1s) from ct1_1 ct1, t_empty t2 where ct1.ts = t2.ts;",
                "res": []  
            },
            {
                "sql": "select elapsed(st1.ts, 1s) from st1, st2, st_empty where st1.ts=st2.ts and st2.ts=st_empty.ts;",
                "res": []
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    def do_query_with_union(self):
        """test elapsed function with union

        test elapsed function with union

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        query_list = [
            {
                "sql": "select elapsed(ts, 1s) from st1 union select elapsed(ts, 1s) from st2;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 union all select elapsed(ts, 1s) from st2;",
                "res": [(99,),(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 union all select elapsed(ts, 1s) from st_empty;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_1 union all select elapsed(ts, 1s) from ct1_2;",
                "res": [(99,),(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_1 union select elapsed(ts, 1s) from ct1_2;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_1 union select elapsed(ts, 1s) from ct1_empty;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts < '2023-03-01 15:05:00.000' union select elapsed(ts, 1s) from ct1_1 where ts >= '2023-03-01 15:01:00.000';",
                "res": [(39,),(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from ct1_empty union select elapsed(ts, 1s) from t_empty;",
                "res": []
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 group by tbname union select elapsed(ts, 1s) from st2 group by tbname;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 group by tbname union all select elapsed(ts, 1s) from st2 group by tbname;",
                "res": [(99,),(99,),(99,),(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st_empty group by tbname union all select elapsed(ts, 1s) from st2 group by tbname;",
                "res": [(99,),(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from t1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000' interval(10s) fill(next) union select elapsed(ts, 1s) from st2 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:49.000' interval(5s) fill(prev);",
                "res": [(9,), (None,), (4,), (5,),(10,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 group by tbname union select elapsed(ts, 1s) from st2 group by tbname union select elapsed(ts, 1s) from st_empty group by tbname;",
                "res": [(99,)]
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    #def test_query_with_window(self):
    #    """test elapsed function with window

    #    test elapsed function with interval, fill, session

    #    Since: v3.3.0.0

    #    Labels: elapsed, interval, fill, session

    #    History:
    #        - 2024-6-5 Alex Duan Created
    #        - 2025-5-08 Huo Hong Migrated to new test framework

    #    """
    #    query_list = [
    #        {
    #            "sql": "select elapsed(ts, 1s) from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' interval(10s) fill(next);",
    #            "res": [(10,),(10,)()]
    #        },
    #        {
    #            "sql": "select elapsed(ts, 1s) from (select * from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:20.000' and c_int > 100) where ts >= '2023-03-01 15:01:00.000' and ts < '2023-03-01 15:02:00.000' interval(10s) fill(prev);",
    #            "res": [(10,)(10,)(),(),(),()]
    #        },
    #        {
    #            "sql": "select elapsed(ts, 1s) from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' session(ts, 2s);",
    #            "res": [(20,)]
    #        },
    #        {
    #            "sql": "select elapsed(ts, 1s) from st_empty where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' session(ts, 2s);",
    #            "res": []
    #        }
    #    ]
    #    sql_list = []
    #    res_list = []
    #    for item in query_list:
    #        sql_list.append(item["sql"])
    #        res_list.append(item["res"])
    #    tdSql.queryAndCheckResult(sql_list, res_list)

    def do_nested_query(self):
        """test elapsed function with nested query

        test elapsed function with nested query

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        query_list = [
            {
                "sql": "select elapsed(ts, 1s) from (select * from st1 where c_int > 10 and ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000');",
                "res": [(99,)]
            },
            {
                "sql": "select sum(v) from (select elapsed(ts, 1s) as v from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' interval(10s) fill(next));",
                "res": [(20,)]
            },
            {
                "sql": "select avg(v) from (select elapsed(ts, 1s) as v from st2 group by tbname order by v);",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from (select * from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000') where c_int > 10;",
                "res": [(99,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from (select * from st1 where c_int > 10 and ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000') where c_int < 20;",
                "res": []
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    def do_abnormal_query(self):
        """test unsupported elapsed sql command

        test unsupported elapsed sql command

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        # incorrect parameter
        table_list = self.table_dic["super_table"] + self.table_dic["child_table"] + self.table_dic["common_table"]
        incorrect_parameter_list = ["()", "(null)", "(*)", "(c_ts)", "(c_ts, 1s)", "(c_int)", "(c_bigint)", "(c_double)", "(c_nchar)", "(ts, null)",
                                    "(ts, *)", "(2024-01-09 17:00:00)", "(2024-01-09 17:00:00, 1s)", "(t1)", "(t1, 1s)", "(t2)", "(t3)"]
        for table in table_list:
            for param in incorrect_parameter_list:
                if table.startswith("st"):
                    tdSql.error("select elapsed{} from {} group by tbname order by ts;".format(param, table))
                else:
                    tdSql.error("select elapsed{} from {};".format(param, table))
                tdSql.error("select elapsed{} from {} group by ".format(param, table))

        # query with unsupported function, like leastsquares、diff、derivative、top、bottom、last_row、interp
        unsupported_sql_list = [
            "select elapsed(leastsquares(c_int, 1, 2)) from st1 group by tbname;",
            "select elapsed(diff(ts)) from st1;",
            "select elapsed(derivative(ts, 1s, 1)) from st1 group by tbname order by ts;",
            "select elapsed(top(ts, 5)) from st1 group by tbname order by ts;",
            "select top(elapsed(ts), 5) from st1 group by tbname order by ts;",
            "select elapsed(bottom(ts)) from st1 group by tbname order by ts;",
            "select bottom(elapsed(ts)) from st1 group by tbname order by ts;",
            "select elapsed(last_row(ts)) from st1 group by tbname order by ts;",
            "select elapsed(interp(ts, 0)) from st1 group by tbname order by ts;"
        ]
        tdSql.errors(unsupported_sql_list)

        # nested aggregate function
        nested_sql_list = [
            "select avg(elapsed(ts, 1s)) from st1 group by tbname order by ts;",
            "select elapsed(avg(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(sum(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(count(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(min(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(max(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(first(ts), 1s) from st1 group by tbname order by ts;",
            "select elapsed(last(ts), 1s) from st1 group by tbname order by ts;"
        ]
        tdSql.errors(nested_sql_list)

        # other error
        other_sql_list = [
            "select elapsed(ts, 1s) from t1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000' interval(10s) fill(next) union select elapsed(ts, 1s) from st2 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:49.000' interval(5s) fill(prev) group by tbname;",
            "select elapsed(time ,1s) from (select elapsed(ts,1s) time from st1);",
            "select elapsed(ts , 1s) from (select elapsed(ts, 1s) ts from st2);",
            "select elapsed(time, 1s) from (select elapsed(ts, 1s) time from st1 group by tbname);",
            "select elapsed(ts , 1s) from (select elapsed(ts, 1s) ts from st2 group by tbname);",
            "select elapsed(ts, 1s) from (select * from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:40.000' interval(10s) fill(next)) where c_int > 10;"
        ]
        tdSql.errors(other_sql_list)

    def do_ns_precision(self):
        """test elapsed function with ns precision

        test elapsed function with ns precision

        Since: v3.3.0.0

        Labels: elapsed

        History:
            - 2024-9-10 Jing Sima Created

        """
        tdSql.execute("CREATE DATABASE test BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 14400m WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ns' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0;")
        tdSql.execute("use test")
        tdSql.execute("CREATE TABLE t_test (sdbkey TIMESTAMP, name NCHAR(10), sales INT)")
        tdSql.execute("insert into t_test values(1790000000000000000, 'name1',1)")
        tdSql.execute("insert into t_test values(1790000000000000001, 'name2',1)")
        tdSql.query("select elapsed(sdbkey,1b) from t_test")
        tdSql.checkData(0, 0, 1)
        tdSql.execute("insert into t_test values(1790000000000001000, 'name2',1)")
        tdSql.query("select elapsed(sdbkey,1b) from t_test")
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select elapsed(sdbkey,1u) from t_test")
        tdSql.checkData(0, 0, 1)

    #
    # ------------------ test_elapsed.py ------------------
    #
    def prepare_db(self,dbname,vgroupVar):

        tdLog.info (" ====================================== prepare db ==================================================")
        tdSql.execute('drop database if exists testdb ;')
        tdSql.execute('create database %s keep 36500 vgroups %d ;'%(dbname,vgroupVar))

    def prepare_data(self,dbname):

        tdLog.info (" ====================================== prepare data ==================================================")

        # tdSql.execute('drop database if exists testdb ;')
        # tdSql.execute('create database testdb keep 36500;')
        tdSql.execute('use %s;'%dbname)

        tdSql.execute('create stable stable_1(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
             q_double double , bin_chars binary(20)) tags(loc nchar(20) ,ind int,tstag timestamp);')
        tdSql.execute('create stable stable_2(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
             q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')
        # create empty stables
        tdSql.execute('create stable stable_empty(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
             q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')
        tdSql.execute('create stable stable_sub_empty(ts timestamp ,tscol timestamp, q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint, q_float float ,\
             q_double double, bin_chars binary(20) ) tags(loc nchar(20),ind int,tstag timestamp);')

        # create empty sub_talbes and regular tables
        tdSql.execute('create table sub_empty_1 using stable_sub_empty tags("sub_empty_1",3,"2015-01-01 00:02:00")')
        tdSql.execute('create table sub_empty_2 using stable_sub_empty tags("sub_empty_2",3,"2015-01-01 00:02:00")')
        tdSql.execute('create table regular_empty (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')

        tdSql.execute('create table sub_table1_1 using stable_1 tags("sub1_1",1,"2015-01-01 00:00:00")')
        tdSql.execute('create table sub_table1_2 using stable_1 tags("sub1_2",2,"2015-01-01 00:01:00")')
        tdSql.execute('create table sub_table1_3 using stable_1 tags("sub1_3",3,"2015-01-01 00:02:00")')

        tdSql.execute('create table sub_table2_1 using stable_2 tags("sub2_1",1,"2015-01-01 00:00:00")')
        tdSql.execute('create table sub_table2_2 using stable_2 tags("sub2_2",2,"2015-01-01 00:01:00")')
        tdSql.execute('create table sub_table2_3 using stable_2 tags("sub2_3",3,"2015-01-01 00:02:00")')

        tdSql.execute('create table regular_table_1 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double, bin_chars binary(20)) ;')
        tdSql.execute('create table regular_table_2 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')
        tdSql.execute('create table regular_table_3 (ts timestamp , tscol timestamp ,q_int int , q_bigint bigint , q_smallint smallint , q_tinyint tinyint , q_float float , q_double double , bin_chars binary(20)) ;')

        tablenames = ["sub_table1_1","sub_table1_2","sub_table1_3","sub_table2_1","sub_table2_2","sub_table2_3","regular_table_1","regular_table_2","regular_table_3"]

        tdLog.info("insert into records ")

        for tablename in tablenames:

            for i in range(self.num):
                sql= 'insert into %s values(%d, %d,%d, %d, %d, %d, %f, %f, "%s")' % (tablename,self.ts + i*10000, self.ts + i*10,2147483647-i, 9223372036854775807-i, 32767-i, 127-i, i, i,("bintest"+str(i)))
                print(sql)
                tdSql.execute(sql)

        tdLog.info("=============================================data prepared done!=========================")

    def abnormal_common_test(self):

        tdLog.info (" ====================================== elapsed illeagal params ==================================================")

        tablenames = ["sub_table1_1","sub_table1_2","sub_table1_3","sub_table2_1","sub_table2_2","sub_table2_3","regular_table_1","regular_table_2","regular_table_3"]

        abnormal_list = ["()","(NULL)","(*)","(abc)","( , )","(NULL,*)","( ,NULL)","(%)","(+)","(*,)","(*, /)","(ts,*)" "(ts,tbname*10)","(ts,tagname)",
        "(ts,2d+3m-2s,NULL)","(ts+10d,NULL)" ,"(ts,now -1m%1d)","(ts+10d,_c0)","(ts+10d,)","(ts,%)","(ts, , m)","(ts,abc)","(ts,/)","(ts,*)","(ts,1s,100)",
        "(ts,1s,abc)","(ts,1s,_c0)","(ts,1s,*)","(ts,1s,NULL)","(ts,,_c0)","(ts,tbname,ts)","(ts,0,tbname)","('2021-11-18 00:00:10')","('2021-11-18 00:00:10', 1s)",
        "('2021-11-18T00:00:10+0800', '1s')","('2021-11-18T00:00:10Z', '1s')","('2021-11-18T00:00:10+0800', 10000000d,)","('ts', ,2021-11-18T00:00:10+0800, )"]

        for tablename in tablenames:
            for abnormal_param in abnormal_list:

                if tablename.startswith("stable"):
                    basic_sql= "select elapsed" + abnormal_param + " from " + tablename + " group by tbname ,ind order by tbname;"  #stables
                else:
                    basic_sql= "select elapsed" + abnormal_param + " from " + tablename + ";"  # regular table
                tdSql.error(basic_sql)

    def abnormal_use_test(self):

        tdLog.info (" ====================================== elapsed use abnormal ==================================================")

        sqls_list = ["select elapsed(ts) from regular_empty group by tbname,ind order by desc; ",
                    "select elapsed(ts) from regular_empty group by tbname,ind order by desc; ",
                    "select elapsed(ts) from regular_table_1 group by tbname,ind order by desc; ",
                    "select elapsed(ts) from sub_table1_1  group by tbname,ind order by desc; ",
                    "select elapsed(ts) from sub_table1_1  group by tbname,ind order by desc; ",
                    # "select elapsed(ts,1s) from stable_empty group by ts order by ts;",
                    "select elapsed(ts,1s) from stable_1 group by ind order by ts;",
                    "select elapsed(ts,1s) from stable_2 group by tstag order by ts;",
                    "select elapsed(ts,1s) from stable_1 group by tbname,tstag,tscol order by ts;",
                    "select elapsed(ts,1s),ts from stable_1 group by tbname ,ind order by ts;",
                    "select ts,elapsed(ts,1s),tscol*100 from stable_1 group by tbname ,ind order by ts;",
                    "select elapsed(ts) from stable_1 group by tstag order by ts;",
                    "select elapsed(ts) from sub_empty_1 group by tbname,ind ,tscol order by ts desc;",
                    "select tbname, tscol,elapsed(ts) from sub_table1_1 group by tbname ,ind order by ts desc;",
                    "select elapsed(tscol) from sub_table1_1 order by ts desc;",
                    "select elapsed(tstag) from sub_table1_1 order by ts desc;",
                    "select elapsed(ind) from sub_table1_1 order by ts desc;",
                    "select elapsed(tscol) from sub_empty_1 order by ts desc;",
                    "select elapsed(tstag) from sub_empty_1 order by ts desc;",
                    "select elapsed(ind) from sub_table1_1 order by ts desc;",
                    "select elapsed(ind,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(tscol,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(tstag,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(q_int,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(loc,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(q_bigint,1s) from sub_table1_1 order by ts desc;",
                    "select elapsed(bin_chars,1s) from sub_table1_1 order by ts desc;"]
        for sql in sqls_list :
            tdSql.error(sql)

    def query_filter(self):

        tdLog.info (" ====================================== elapsed query filter ==================================================")

        for i in range(self.num):
            ts_start_time = self.ts + i*10000
            ts_col_start_time = self.ts + i*10
            ts_tag_time = "2015-01-01 00:01:00"
            ts_end_time = self.ts + (self.num-1-i)*10000
            ts_col_end_time = self.ts + (self.num-1-i)*10

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts >= %d group by tbname " %(ts_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(3)
            tdSql.checkData(0,0,float((self.num -i-1)*10))
            tdSql.checkData(1,0,float((self.num -i-1)*10))
            tdSql.checkData(2,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts >= %d  " %(ts_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(1)
            tdSql.checkData(0,0,float((self.num -i-1)*10))


            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts >= %d  and tscol >= %d and tstag='2015-01-01 00:01:00'group by tbname " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(1)
            tdSql.checkData(0,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts >= %d  and tscol >= %d  " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(1)
            tdSql.checkData(0,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts >= %d  and tscol > %d and tstag='2015-01-01 00:01:00' group by tbname" %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts >= %d  and tscol > %d " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts > %d  and tscol > %d and tstag < '2015-01-01 00:01:00' group by tbname " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts > %d  and tscol > %d  " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts > %d  and tscol <= %d and tstag < '2015-01-01 00:01:00' group by tbname" %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(0)

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts > %d  and tscol <= %d " %(ts_start_time,ts_col_start_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(0)

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts < %d  and tscol <= %d and tstag < '2015-01-01 00:01:00' group by tbname" %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts < %d  and tscol <= %d " %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts < %d  and tscol <= %d  group by tbname " %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(3)
                tdSql.checkData(0,0,float((self.num -i-2)*10))
                tdSql.checkData(1,0,float((self.num -i-2)*10))
                tdSql.checkData(2,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts < %d  and tscol <= %d   " %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  ts = %d  and tscol < %d  group by tbname " %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(0)

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts = %d  and tscol < %d  " %(ts_end_time,ts_col_end_time)
            tdSql.query(filter_sql)
            tdSql.checkRows(0)

            filter_sql = "select elapsed(ts,1s) from stable_1 where  q_tinyint != %d  and tscol < %d  group by tbname " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(3)
                tdSql.checkData(0,0,float((self.num -i-2)*10))
                tdSql.checkData(1,0,float((self.num -i-2)*10))
                tdSql.checkData(2,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  q_tinyint != %d  and tscol < %d   " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  q_tinyint != %d  and tscol <= %d  group by tbname " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(3)
                tdSql.checkData(0,0,float((self.num -i-1)*10))
                tdSql.checkData(1,0,float((self.num -i-1)*10))
                tdSql.checkData(2,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  q_tinyint != %d  and tscol <= %d   " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  q_tinyint <> %d  and tscol < %d  group by tbname " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(3)
                tdSql.checkData(0,0,float((self.num -i-2)*10))
                tdSql.checkData(1,0,float((self.num -i-2)*10))
                tdSql.checkData(2,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  q_tinyint <> %d  and tscol < %d   " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num-1:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-2)*10))

            filter_sql = "select elapsed(ts,1s) from stable_1 where  q_tinyint <> %d  and tscol <= %d  group by tbname " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(3)
                tdSql.checkData(0,0,float((self.num -i-1)*10))
                tdSql.checkData(1,0,float((self.num -i-1)*10))
                tdSql.checkData(2,0,float((self.num -i-1)*10))

            filter_sql = "select elapsed(ts,1s) from sub_table1_1 where  q_tinyint <> %d  and tscol <= %d  " %(i,ts_col_end_time)
            tdSql.query(filter_sql)

            if i == self.num:
                tdSql.checkRows(0)
            else:
                tdSql.checkRows(1)
                tdSql.checkData(0,0,float((self.num -i-1)*10))

            # filter between and
            tdSql.query("select elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and q_tinyint between 125 and 127 and tscol <= '2015-01-01 00:01:00.000'  ")
            tdSql.checkData(0,0,20)
            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and \
            q_tinyint between 125 and 127 and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,20)
            tdSql.checkData(1,0,20)
            tdSql.checkData(2,0,20)

            # filter in and or
            tdSql.query("select elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and q_tinyint between 125 and 127 and tscol <= '2015-01-01 00:01:00.000'  ")
            tdSql.checkData(0,0,20)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and q_tinyint between 125 and 127 and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,20)
            tdSql.checkData(1,0,20)
            tdSql.checkData(2,0,20)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and q_tinyint in (125,126,127) and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,20)
            tdSql.checkData(1,0,20)
            tdSql.checkData(2,0,20)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars in ('bintest0','bintest1') and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,10)
            tdSql.checkData(1,0,10)
            tdSql.checkData(2,0,10)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars in ('bintest0','bintest1') and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,10)
            tdSql.checkData(1,0,10)
            tdSql.checkData(2,0,10)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars like 'bintest_' and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,60)
            tdSql.checkData(1,0,60)
            tdSql.checkData(2,0,60)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars like 'bintest_' and tscol <= '2015-01-01 00:01:00.000' group by tbname ")
            tdSql.checkData(0,0,60)
            tdSql.checkData(1,0,60)
            tdSql.checkData(2,0,60)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars is not null  and tscol <= '2015-01-01 00:01:00.000' group by tbname; ")
            tdSql.checkData(0,0,60)
            tdSql.checkData(1,0,60)
            tdSql.checkData(2,0,60)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars is null  and tscol <= '2015-01-01 00:01:00.000' group by tbname; ")
            tdSql.checkRows(0)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars match '^b'  and tscol <= '2015-01-01 00:01:00.000' group by tbname; ")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,60)
            tdSql.checkData(1,0,60)
            tdSql.checkData(2,0,60)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars nmatch '^a'  and tscol <= '2015-01-01 00:01:00.000' group by tbname; ")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,60)
            tdSql.checkData(1,0,60)
            tdSql.checkData(2,0,60)

            tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and bin_chars ='bintest1' or bin_chars ='bintest2' and tscol <= '2015-01-01 00:01:00.000' group by tbname; ")
            tdSql.checkRows(3)
            tdSql.query("select elapsed(ts,1s) from stable_1 where (ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000')  or (ts between '2015-01-01 00:01:00.000'  and '2015-01-01 00:02:00.000') group by tbname; ")
            tdSql.checkRows(3)
            tdSql.checkData(0,0,90)
            tdSql.checkData(1,0,90)
            tdSql.checkData(2,0,90)

    def query_interval(self):

        tdLog.info (" ====================================== elapsed interval sliding fill ==================================================")

        # empty interval
        tdSql.query("select max(q_int)*10 from stable_empty where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev);")
        tdSql.checkRows(0)
        tdSql.query("select max(q_int)*10 from sub_empty_2 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev);")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s)*10 from stable_empty where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev) group by tbname;")
        tdSql.checkRows(0)
        tdSql.query("select elapsed(ts,1s)*10 from sub_empty_2 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev);")
        tdSql.checkRows(0)

        for i in range(self.num):
            ts_start_time = self.ts + i*10000
            ts_col_start_time = self.ts + i*10
            ts_tag_time = "2015-01-01 00:01:00"
            ts_end_time = self.ts + (self.num-1-i)*10000
            ts_col_end_time = self.ts + (self.num-1-i)*10


            # only interval
            interval_sql = "select elapsed(ts,1s) from stable_1 where  ts <=%d interval(10s) group by tbname " %(ts_start_time)
            tdSql.query(interval_sql)
            tdSql.checkRows(3*(i+1))

            interval_sql = "select elapsed(ts,1s) from sub_table1_1 where  ts <=%d interval(10s) " %(ts_start_time)
            tdSql.query(interval_sql)
            tdSql.checkRows(i+1)
            for x in range(i+1):
                if x == i:
                    tdSql.checkData(x,1,0)
                else :
                    tdSql.checkData(x,1,10)

        # interval and fill , fill_type =  ["NULL","value,100","prev","next","linear"]

        # interval (1s)  and time range is outer records

        tdSql.query("select elapsed(ts,1s)*10 from stable_empty where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev) group by tbname;")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s)*10 from sub_empty_2 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev);")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(prev) group by tbname;")
        tdSql.checkRows(180)
        tdSql.checkData(0,1,100)
        tdSql.checkData(9,1,0)
        tdSql.checkData(59,1,0)
        tdSql.checkData(60,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(next) group by tbname;")
        tdSql.checkRows(180)
        tdSql.checkData(0,1,100)
        tdSql.checkData(9,1,0)
        tdSql.checkData(10,1,None)
        tdSql.checkData(59,1,None)
        tdSql.checkData(60,1,100)
        tdSql.checkData(61,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(linear) group by tbname;")
        tdSql.checkRows(180)
        tdSql.checkData(0,1,100)
        tdSql.checkData(9,1,0)
        tdSql.checkData(10,1,None)
        tdSql.checkData(59,1,None)
        tdSql.checkData(60,1,100)
        tdSql.checkData(61,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(NULL) group by tbname;")
        tdSql.checkRows(180)
        tdSql.checkData(0,1,100)
        tdSql.checkData(9,1,0)
        tdSql.checkData(10,1,None)
        tdSql.checkData(59,1,None)
        tdSql.checkData(60,1,100)
        tdSql.checkData(61,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(10s) fill(value ,2) group by tbname;")
        tdSql.checkRows(180)
        tdSql.checkData(0,1,100)
        tdSql.checkData(9,1,0)
        tdSql.checkData(10,1,20)
        tdSql.checkData(59,1,20)
        tdSql.checkData(60,1,10)
        tdSql.checkData(61,1,100)

        # interval (20s)  and time range is outer records
        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(20s) fill(prev) group by tbname,ind ;")
        tdSql.checkRows(90)
        tdSql.checkData(0,1,200)
        tdSql.checkData(4,1,100)
        tdSql.checkData(5,1,100)
        tdSql.checkData(29,1,100)
        tdSql.checkData(30,1,200)
        tdSql.checkData(31,1,200)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(20s) fill(next) group by tbname,ind ;")
        tdSql.checkRows(90)
        tdSql.checkData(0,1,200)
        tdSql.checkData(4,1,100)
        tdSql.checkData(5,1,None)
        tdSql.checkData(29,1,None)
        tdSql.checkData(30,1,200)
        tdSql.checkData(31,1,200)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(20s) fill(linear) group by tbname,ind ;")
        tdSql.checkRows(90)
        tdSql.checkData(0,1,200)
        tdSql.checkData(4,1,100)
        tdSql.checkData(5,1,None)
        tdSql.checkData(29,1,None)
        tdSql.checkData(30,1,200)
        tdSql.checkData(31,1,200)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(20s) fill(NULL) group by tbname,ind ;")
        tdSql.checkRows(90)
        tdSql.checkData(0,1,200)
        tdSql.checkData(4,1,100)
        tdSql.checkData(5,1,None)
        tdSql.checkData(29,1,None)
        tdSql.checkData(30,1,200)
        tdSql.checkData(31,1,200)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:10:00.000' interval(20s) fill(value ,2) group by tbname,ind ;")
        tdSql.checkRows(90)
        tdSql.checkData(0,1,200)
        tdSql.checkData(4,1,100)
        tdSql.checkData(5,1,20)
        tdSql.checkData(29,1,20)
        tdSql.checkData(30,1,200)
        tdSql.checkData(31,1,200)

        # interval (20s)  and time range is in records

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(prev) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(next) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(linear) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(NULL) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(value ,2 ) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2015-01-01 00:00:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) group by tbname,ind ;")
        tdSql.checkRows(9)
        tdSql.checkData(0,1,200)
        tdSql.checkData(2,1,100)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(8,1,100)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2014-12-31 23:59:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) fill(NULL) group by tbname,ind ;")
        tdSql.checkRows(18)
        tdSql.checkData(0,1,None)
        tdSql.checkData(2,1,None)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,None)
        tdSql.checkData(8,1,None)
        tdSql.checkData(9,1,200)

        # interval sliding

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2014-12-31 23:59:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) sliding(20s) fill(NULL) group by tbname,ind ;")
        tdSql.checkRows(18)
        tdSql.checkData(0,1,None)
        tdSql.checkData(2,1,None)
        tdSql.checkData(3,1,200)
        tdSql.checkData(5,1,100)
        tdSql.checkData(7,1,None)
        tdSql.checkData(8,1,None)
        tdSql.checkData(9,1,200)

        tdSql.query("select elapsed(ts,1s)*10 from stable_1 where ts >= '2014-12-31 23:59:00.000' and ts <'2015-01-01 00:01:00.000' interval(20s) sliding(10s) fill(NULL) group by tbname,ind ;")
        tdSql.checkRows(39)
        tdSql.checkData(0,1,None)
        tdSql.checkData(2,1,None)
        tdSql.checkData(6,1,100)
        tdSql.checkData(7,1,200)
        tdSql.checkData(12,1,0)
        tdSql.checkData(13,1,None)
        tdSql.checkData(15,1,None)
        tdSql.checkData(19,1,100)
        tdSql.checkData(20,1,200)
        tdSql.checkData(25,1,0)

    def query_mix_common(self):

        tdLog.info (" ======================================elapsed mixup with common col, it will not support =======================================")

        tdSql.query("select elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  and ind =1  group by tbname; ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,60)

        tdSql.query("select elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  ; ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,60)

        tdSql.error("select ts,elapsed(ts,1s) from sub_empty_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000' ; ")
        tdSql.error("select ts,elapsed(ts,1s) from stable_empty where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  group by tbname; ")

        tdSql.error("select ts,elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000' ; ")
        tdSql.error("select ts,elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  group by tbname; ")

        tdSql.error("select q_int,elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000' ; ")
        tdSql.error("select q_int,elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  group by tbname; ")

        tdSql.error("select ts,q_int,elapsed(ts,1s) from sub_table1_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000' ; ")
        tdSql.error("select ts,q_int,elapsed(ts,1s) from stable_1 where ts between '2015-01-01 00:00:00.000'  and '2015-01-01 00:01:00.000'  group by tbname; ")

    def query_mix_Aggregate(self):

        tdLog.info (" ====================================== elapsed mixup with aggregate ==================================================")

        tdSql.query("select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) from sub_table1_1 ; ")

        data = tdSql.getResult("select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) from sub_table1_1 ; ")

        querys = ["count(*)","avg(q_int)", "sum(q_double)","stddev(q_float)","LEASTSQUARES(q_int,0,1)", "elapsed(ts,1s)"]

        for index , query in enumerate(querys):
            sql = "select %s from sub_table1_1 " %(query)
            tdSql.query(sql)
            tdSql.checkData(0,0,data[0][index])

        tdSql.query("select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) from stable_1  group by tbname; ")

        # Arithmetic with elapsed for common table

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = ["count(*)","avg(q_int)", "sum(q_double)","stddev(q_float)", "elapsed(ts,1s)"]

        for operator in operators:

            query_datas=[]

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                query_data = tdSql.getResult("select %s from sub_table1_1;"%query)

                query_datas.append(query_data[0][0])
                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from sub_table1_1;"

            tdSql.query(sql_common)
            results= query_datas[0]
            if operator == "+":
                for data in query_datas[1:]:
                    results += data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "-":
                for data in query_datas[1:]:
                    results -= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "*":
                for data in query_datas[1:]:
                    results *= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "/":
                for data in query_datas[1:]:
                    results /= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "%":
                for data in query_datas[1:]:
                    results %= data
                tdSql.checkData(0,0,results)


        # Arithmetic with elapsed for super table

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = ["count(*)","avg(q_int)",  "sum(q_double)","stddev(q_float)", "elapsed(ts,1s)"]

        for operator in operators:

            query_datas=[]

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                query_data = tdSql.getResult("select %s from stable_1 group by tbname;"%query)

                query_datas.append(query_data[0][0])
                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from stable_1 group by tbname;"

            tdSql.query(sql_common)
            results= query_datas[0]
            if operator == "+":
                for data in query_datas[1:]:
                    results += data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)


            results= query_datas[0]
            if operator == "-":
                for data in query_datas[1:]:
                    results -= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "*":
                for data in query_datas[1:]:
                    results *= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "/":
                for data in query_datas[1:]:
                    results /= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "%":
                for data in query_datas[1:]:
                    results %= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

    def query_mix_select(self):

        tdLog.info (" ====================================== elapsed mixup with select function =================================================")

        querys = ["max(q_int)","min(q_int)" , "first(q_tinyint)", "first(*)","last(q_int)","last(*)","PERCENTILE(q_int,10)","APERCENTILE(q_int,10)","elapsed(ts,1s)"]


        querys_mix = ["max(q_int)","min(q_int)" , "first(q_tinyint)", "first(q_int)","last(q_int)","PERCENTILE(q_int,10)","APERCENTILE(q_int,10)","elapsed(ts,1s)"]

        tdSql.query("select max(q_int),min(q_int) , first(q_tinyint), first(q_int),last(q_int),PERCENTILE(q_int,10),APERCENTILE(q_int,10) ,elapsed(ts,1s) from sub_table1_1 ; ")

        data = tdSql.getResult("select max(q_int),min(q_int) , first(q_tinyint), first(q_int),last(q_int),PERCENTILE(q_int,10),APERCENTILE(q_int,10) ,elapsed(ts,1s) from sub_table1_1 ; ")

        for index , query in enumerate(querys_mix):
            sql = "select %s from sub_table1_1 " %(query)
            tdSql.query(sql)
            tdSql.checkData(0,0,data[0][index])

        tdSql.query("select max(q_int),min(q_int) , first(q_tinyint), first(q_int),last(q_int),APERCENTILE(q_int,10) ,elapsed(ts,1s) from stable_1 group by tbname ; ")

        data = tdSql.getResult("select max(q_int),min(q_int) , first(q_tinyint), first(q_int),last(q_int),APERCENTILE(q_int,10) ,elapsed(ts,1s) from stable_1 group by tbname ; ")

        querys_mix = ["max(q_int)","min(q_int)" , "first(q_tinyint)", "first(q_int)","last(q_int)","APERCENTILE(q_int,10)","elapsed(ts,1s)"]

        for index , query in enumerate(querys_mix):
            sql = "select %s from stable_1 group by tbname " %(query)
            tdSql.query(sql)
            tdSql.checkData(0,0,data[0][index])
            tdSql.checkData(1,0,data[0][index])
            tdSql.checkData(2,0,data[0][index])

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = querys_mix

        for operator in operators:

            query_datas=[]

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                query_data = tdSql.getResult("select %s from sub_table1_1;"%query)

                query_datas.append(query_data[0][0])
                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from sub_table1_1;"

            tdSql.query(sql_common)
            results= query_datas[0]
            if operator == "+":
                for data in query_datas[1:]:
                    results += data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "-":
                for data in query_datas[1:]:
                    results -= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "*":
                for data in query_datas[1:]:
                    results *= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "/":
                for data in query_datas[1:]:
                    results /= data
                tdSql.checkData(0,0,results)

            results= query_datas[0]
            if operator == "%":
                for data in query_datas[1:]:
                    results %= data
                tdSql.checkData(0,0,results)


        # Arithmetic with elapsed for super table

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = querys_mix

        for operator in operators:

            query_datas=[]

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                query_data = tdSql.getResult("select %s from stable_1 group by tbname;"%query)

                query_datas.append(query_data[0][0])
                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from stable_1 group by tbname;"

            tdSql.query(sql_common)
            results= query_datas[0]
            if operator == "+":
                for data in query_datas[1:]:
                    results += data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)


            results= query_datas[0]
            if operator == "-":
                for data in query_datas[1:]:
                    results -= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "*":
                for data in query_datas[1:]:
                    results *= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "/":
                for data in query_datas[1:]:
                    results /= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

            results= query_datas[0]
            if operator == "%":
                for data in query_datas[1:]:
                    results %= data
                tdSql.checkData(0,0,results)
                tdSql.checkData(1,0,results)
                tdSql.checkData(2,0,results)

    def query_mix_compute(self):

        tdLog.info (" ====================================== elapsed mixup with compute function =================================================")

        querys = ["diff(q_int)","DERIVATIVE(q_int,1s,1)","spread(ts)","spread(q_tinyint)","ceil(q_float)","floor(q_float)","round(q_float)"]

        for index , query in enumerate(querys):

            sql1 = "select elapsed(ts,1s),%s from sub_table1_1 " %(query)
            sql2 = "select elapsed(ts,1s),%s from stable_1  group by tbname" %(query)
            if query in ["diff(q_int)","DERIVATIVE(q_int,1s,1)","ceil(q_float)","floor(q_float)","round(q_float)"]:
                tdSql.error(sql1)
                tdSql.error(sql2)
                continue
            tdSql.query(sql1)
            tdSql.query(sql2)

        # only support mixup with spread

        sql = "select spread(ts)*10,spread(q_tinyint)-10,elapsed(ts,1s) from sub_table1_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\"  ;"
        tdSql.execute(sql)

        data = tdSql.getResult(sql)

        sql = "select spread(ts)*10,spread(q_tinyint)-10,elapsed(ts,1s) from stable_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ;"
        tdSql.execute(sql)

        querys_mix = ["spread(ts)","spread(q_tinyint)-10","elapsed(ts,1s)"]

        for index , query in enumerate(querys_mix):
            sql = "select %s from sub_table1_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ; " %(query)
            tdSql.query(sql)

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = querys_mix

        for operator in operators:

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from stable_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ;"

            tdSql.query(sql_common)

        for index , query in enumerate(querys_mix):
            sql = "select %s from stable_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\"  ; " %(query)
            tdSql.query(sql)

        operators = ["+" ,"-" , "*" ,"/" ,"%"]
        querys_oper = querys_mix

        for operator in operators:

            sql_common= "select "

            for index , query in enumerate(querys_oper):

                sql_common += " %s %s " %(query,operator)
            sql_common=sql_common[:-2] + " from stable_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ;"

            tdSql.query(sql_common)

    def query_mix_arithmetic(self):

        tdLog.info (" ====================================== elapsed mixup with arithmetic =================================================")

        tdSql.execute("select elapsed(ts,1s)+1 ,elapsed(ts,1s)-2,elapsed(ts,1s)*3,elapsed(ts,1s)/4,elapsed(ts,1s)%5 from sub_table1_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ; ")
        tdSql.execute("select elapsed(ts,1s)+1 ,elapsed(ts,1s)-2,elapsed(ts,1s)*3,elapsed(ts,1s)/4,elapsed(ts,1s)%5 from stable_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" ; ")

        # queries = ["elapsed(ts,1s)+1" ,"elapsed(ts,1s)-2","elapsed(ts,1s)*3","elapsed(ts,1s)/4","elapsed(ts,1s)%5" ]

        # for index ,query in enumerate(queries):
        #     sql = "select %s from sub_table1_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev) ;" % (query)
        #     data =  tdSql.getResult(sql)
        #     tdSql.query("select elapsed(ts,1s)+1 ,elapsed(ts,1s)-2,elapsed(ts,1s)*3,elapsed(ts,1s)/4,elapsed(ts,1s)%5 from sub_table1_1 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev) ; ")
        #     tdSql.checkData(0,index+1,data[0][1])

    def query_with_join(self):

        tdLog.info (" ====================================== elapsed mixup with join =================================================")

        tdSql.error("select elapsed(ts,1s) from stable_empty TABLE1 , stable_empty TABLE2  where TABLE1.ts =TABLE2.ts; ")
        tdSql.error("select elapsed(ts,1s) from stable_empty TABLE1 , stable_empty TABLE2  where TABLE1.ts =TABLE2.ts group by tbname; ")

        tdSql.execute("select elapsed(ts,1s) from sub_empty_1 TABLE1 , sub_empty_2 TABLE2  where TABLE1.ts =TABLE2.ts; ")
        tdSql.error("select elapsed(ts,1s) from stable_1 TABLE1 , stable_2 TABLE2  where TABLE1.ts =TABLE2.ts and TABLE1.ind =TABLE2.ind; ")
        tdSql.error("select elapsed(ts,1s) from stable_1 TABLE1 , stable_2 TABLE2  where TABLE1.ts =TABLE2.ts and TABLE1.ind =TABLE2.ind group by tbname,ind; ") # join not support group by

        tdSql.error("select elapsed(ts,1s) from sub_empty_1 TABLE1 , stable_2 TABLE2  where TABLE1.ts =TABLE2.ts and TABLE1.ind =TABLE2.ind ; ")
        tdSql.execute("select elapsed(ts,1s) from sub_empty_1 TABLE1 , sub_empty_2 TABLE2  where TABLE1.ts =TABLE2.ts ; ")

        tdSql.query("select elapsed(ts,1s) from sub_table1_1 TABLE1 , sub_table1_2 TABLE2  where TABLE1.ts =TABLE2.ts ; ")
        tdSql.checkData(0,0,90)

        tdSql.query("select elapsed(ts,1s) from sub_empty_1 TABLE1 , sub_table1_2 TABLE2  where TABLE1.ts =TABLE2.ts ; ")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s) from sub_empty_1 TABLE1 , regular_empty  TABLE2  where TABLE1.ts =TABLE2.ts ; ")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s) from sub_empty_1 TABLE1 , regular_table_1  TABLE2  where TABLE1.ts =TABLE2.ts ; ")
        tdSql.checkRows(0)

        tdSql.query("select elapsed(ts,1s) from sub_table1_3 TABLE1 , regular_table_1  TABLE2  where TABLE1.ts =TABLE2.ts ; ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query("select elapsed(ts,1s) from regular_table_1 ; ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

    def query_with_union(self):

        tdLog.info (" ====================================== elapsed mixup with union all =================================================")

        # union all with empty

        tdSql.query("select elapsed(ts,1s) from regular_table_1  union all  select elapsed(ts,1s) from regular_table_2;")

        tdSql.query("select elapsed(ts,1s) from regular_table_1  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)  union all \
        select elapsed(ts,1s) from regular_table_2 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")
        tdSql.checkRows(1200)
        tdSql.checkData(0,1,1)
        tdSql.checkData(500,1,0)

        tdSql.query("select elapsed(ts,1s) from sub_empty_1  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)  union all \
        select elapsed(ts,1s) from regular_table_2 where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")
        tdSql.checkRows(600)
        tdSql.checkData(0,1,1)
        tdSql.checkData(500,0,0)

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 union all select elapsed(ts,1s) from sub_empty_2;')
        tdSql.checkRows(0)

        tdSql.query('select elapsed(ts,1s) from regular_table_1 union all select elapsed(ts,1s) from sub_empty_1;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 union all select elapsed(ts,1s) from regular_table_1;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 union all select elapsed(ts,1s) from sub_table1_1;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from sub_table1_1 union all select elapsed(ts,1s) from sub_empty_1;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 union all select elapsed(ts,1s) from regular_table_1;')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.error('select elapsed(ts,1s) from sub_empty_1 union all select elapsed(ts,1s) from stable_sub_empty group by tbname;')

        tdSql.error('select elapsed(ts,1s) from regular_table_1 union all select elapsed(ts,1s) from stable_sub_empty group by tbname;')

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) union all select elapsed(ts,1s) from sub_empty_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev);')
        tdSql.checkRows(0)

        tdSql.error('select elapsed(ts,1s) from sub_empty_1   union all select elapsed(ts,1s) from stable_empty  group by tbname;')

        tdSql.error('select elapsed(ts,1s) from sub_empty_1  interval(1s)  union all select elapsed(ts,1s) from stable_empty interval(1s)  group by tbname;')

        # tdSql.error('select elapsed(ts,1s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) union all select elapsed(ts,1s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(1s) fill(prev) group by tbname;')

        tdSql.query("select elapsed(ts,1s) from stable_empty  group by tbname union all select elapsed(ts,1s) from stable_empty group by tbname ;")
        tdSql.checkRows(0)

        # case : TD-12229
        tdSql.query("select elapsed(ts,1s) from stable_empty  group by tbname union all select elapsed(ts,1s) from stable_1 group by tbname ;")
        tdSql.checkRows(3)

        tdSql.query("select elapsed(ts,1s) from stable_1  group by tbname union all select elapsed(ts,1s) from stable_1 group by tbname ;")
        tdSql.checkRows(6)
        tdSql.checkData(0,0,90)
        tdSql.checkData(5,0,90)

        tdSql.query("select elapsed(ts,1s) from stable_1  group by tbname union all select elapsed(ts,1s) from stable_2 group by tbname ;")
        tdSql.checkRows(6)
        tdSql.checkData(0,0,90)
        tdSql.checkData(5,0,90)

        tdSql.query('select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
             select elapsed(ts,1s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
        tdSql.checkRows(360)
        tdSql.checkData(0,1,10)
        tdSql.checkData(50,1,0)

        #case : TD-12229
        tdSql.query('select elapsed(ts,1s) from stable_empty group by tbname union all  select elapsed(ts,1s) from stable_2 group by tbname ;')
        tdSql.checkRows(3)

        tdSql.query('select elapsed(ts,1s) from stable_1 group by tbname union all  select elapsed(ts,1s) from stable_empty group by tbname ;')
        tdSql.checkRows(3)


        tdSql.query('select elapsed(ts,1s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
             select elapsed(ts,1s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
        tdSql.checkRows(180)

        tdSql.query('select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname union all\
             select elapsed(ts,1s) from stable_empty where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname ;')
        tdSql.checkRows(180)

        # union all with sub table and regular table

        # sub_table with sub_table

        tdSql.query('select elapsed(ts,1s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from sub_table2_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from sub_table1_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(120)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(60)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  union all\
             select elapsed(ts,1s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(60)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        # stable with stable

        tdSql.query('select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev)  group by tbname union all\
             select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) group by tbname;')
        tdSql.checkRows(360)
        tdSql.checkData(0,1,10)
        tdSql.checkData(12,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_2  interval(10s)  union all  select elapsed(ts,1s) from sub_empty_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev);')
        tdSql.checkRows(10)
        tdSql.checkData(0,1,10)
        tdSql.checkData(9,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_2  interval(10s)  union all  select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(10s) fill(prev) ;')
        tdSql.checkRows(70)
        tdSql.checkData(0,1,10)
        tdSql.checkData(9,1,0)

        tdSql.query('select elapsed(ts,1s) from regular_table_2  interval(10s) order by ts desc union all  select elapsed(ts,1s) from regular_table_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) order by ts asc;')
        tdSql.checkRows(70)
        tdSql.checkData(0,1,0)
        tdSql.checkData(1,1,10)
        tdSql.checkData(9,1,10)

        tdSql.query('select elapsed(ts,1s) from stable_1 group by tbname, ind  order by ts desc union all  select elapsed(ts,1s) from stable_2 group by tbname, ind  order by ts asc ;')
        tdSql.checkRows(6)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from stable_1 group by tbname, ind  order by ts desc union all  select elapsed(ts,1s) from stable_1 group by tbname, ind  order by ts asc ;')
        tdSql.checkRows(6)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from stable_1  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,1s) from stable_2 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
        tdSql.checkRows(210)
        tdSql.checkData(0,1,0)
        tdSql.checkData(1,1,10)
        tdSql.checkData(9,1,1)

        tdSql.query('select elapsed(ts,1s) from stable_2  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
        tdSql.checkRows(210)
        tdSql.checkData(0,1,0)
        tdSql.checkData(1,1,10)
        tdSql.checkData(9,1,10)

        tdSql.query('select elapsed(ts,1s) from stable_1  interval(10s) group by tbname,ind order by ts desc union all  select elapsed(ts,1s) from stable_1 where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  interval(10s) fill(prev) group by tbname,ind order by ts asc ;')
        tdSql.checkRows(210)
        tdSql.checkData(0,1,0)
        tdSql.checkData(1,1,10)
        tdSql.checkData(9,1,10)

    def query_nest(self):

        tdLog.info (" ====================================== elapsed query for nest =================================================")

        # ===============================================outer nest============================================

        # regular table

        # ts can't be used at outer query

        tdSql.query("select elapsed(ts,1s) from (select ts from stable_1 );")

        # case : TD-12164

        tdSql.error("select elapsed(ts,1s) from (select qint ts from stable_1 );")
        tdSql.error("select elapsed(tbname ,1s) from (select qint tbname from stable_1 );")
        tdSql.error("select elapsed(tsc ,1s) from (select q_int tsc from stable_1) ;")
        tdSql.error("select elapsed(tsv ,1s) from (select elapsed(ts,1s) tsv from stable_1);")
        tdSql.error("select elapsed(ts ,1s) from (select elapsed(ts,1s) ts from stable_1);")
        # # bug fix
        tdSql.error("select elapsed(tsc ,1s) from (select tscol tsc from stable_1) ;")

        #TD-19911
        tdSql.error("select elapsed(ts,1s,123) from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed() from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed(tscol,1s) from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed(ts,1n) from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed(ts,1y) from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed(ts,tscol) from (select ts,tbname from stable_1 order by ts asc );")
        tdSql.error("select elapsed(bin_chars,tscol) from (select ts,tbname from stable_1 order by ts asc );")

        # case TD-12276
        tdSql.query("select elapsed(ts,1s) from (select ts,tbname from regular_table_1 order by ts asc );")
        tdSql.checkData(0,0,90.000000000)

        tdSql.query("select elapsed(ts,1s) from (select ts,tbname  from regular_table_1 order by ts desc );")
        tdSql.checkData(0,0,90.000000000)

        tdSql.query("select elapsed(ts,1s) from (select ts ,max(q_int),tbname  from regular_table_1 order by ts  ) interval(1s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,q_int,tbname  from regular_table_1 order by ts  ) interval(10s);")

        # sub table

        tdSql.query("select elapsed(ts,1s) from (select ts from sub_table1_1  );")

        tdSql.query("select elapsed(ts,1s) from (select ts ,max(q_int),tbname  from sub_table1_1 order by ts  ) interval(1s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,q_int,tbname  from sub_table1_1 order by ts  ) interval(10s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,tbname,top(q_int,3)  from sub_table1_1   ) interval(10s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,tbname,bottom(q_int,3)  from sub_table1_1   ) interval(10s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,tbname from sub_table1_1   ) interval(10s);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,tbname from sub_table1_1   ) interval(10s);")

        tdSql.error("select elapsed(ts,1s) from (select ts ,count(*),tbname  from sub_table1_1 order by ts  ) interval(1s);")

        querys = ["count(*)","avg(q_int)", "sum(q_double)","stddev(q_float)","LEASTSQUARES(q_int,0,1)","elapsed(ts,1s)"]

        for query in querys:
            sql1 = "select elapsed(ts,1s) from (select %s from regular_table_1 order by ts  ) interval(1s); " % query
            sql2 = "select elapsed(ts,1s) from (select ts , tbname ,%s from regular_table_1 order by ts  ) interval(1s); " % query
            sql3 = "select elapsed(ts,1s) from (select ts , tbname ,%s from stable_1 group by tbname, ind order by ts  ) interval(1s); " % query
            sql4 = "select elapsed(ts,1s) from (select %s from sub_table2_1  order by ts  ) interval(1s); " % query
            sql5 = "select elapsed(ts,1s) from (select ts , tbname ,%s from sub_table2_1  order by ts  ) interval(1s); " % query

            tdSql.error(sql1)
            tdSql.error(sql2)
            tdSql.error(sql3)
            tdSql.error(sql4)
            tdSql.error(sql5)


        # case TD-12164
        tdSql.error( "select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from regular_table_1) ; " )
        tdSql.error( "select elapsed(ts ,1s) from (select elapsed(ts,1s) ts from regular_table_1) ; " )

        tdSql.error( "select elapsed(ts00 ,1s) from (select elapsed(ts,1s) ts00 from stable_1 group by tbname ) ; " )
        tdSql.error( "select elapsed(ts ,1s) from (select elapsed(ts,1s) ts from stable_1 group by tbname) ; " )


        # stable

        tdSql.error("select elapsed(ts,1s) from (select ts from stable_1 ) group by tbname ;")

        tdSql.error("select elapsed(ts,1s) from (select ts ,max(q_int),tbname  from stable_1 group by tbname order by ts ) interval(1s) group by tbname;")

        tdSql.error("select elapsed(ts,1s) from (select ts ,q_int,tbname  from stable_1 order by ts ) interval(1s) group by tbname;")

        # mixup with aggregate

        querys = ["max(q_int)","min(q_int)" , "first(q_tinyint)", "first(*)","last(q_int)","last(*)","top(q_double,1)",
        "bottom(q_float,1)","PERCENTILE(q_int,10)","APERCENTILE(q_int,10)" ,"elapsed(ts,1s)"]

        for index , query in enumerate(querys):

            sql1 = "select elapsed(ts,1s) from (select %s from sub_table1_1) where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(10s) fill(prev) ;  " %(query)
            sql2 = "select elapsed(ts,1s) from (select %s from stable_1 ) where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(10s) fill(prev)  group by tbname; " %(query)
            sql3 = "select elapsed(ts,1s) from (select %s from stable_1 group by tbname) where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(10s) fill(prev)  group by tbname; " %(query)

            if query in ["interp(q_int)"  ]:
                # print(sql1 )
                # print(sql2)
                tdSql.query(sql1)
                tdSql.error(sql2)
            else:
                tdSql.error(sql1)
                tdSql.error(sql2)
                tdSql.error(sql3)

        tdSql.query("select elapsed(ts,1s) from (select ts,tbname  from regular_table_1 order by ts  ) where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")

        tdSql.query("select elapsed(ts,1s) from (select ts ,max(q_int),tbname  from regular_table_1 order by ts  ) where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev);")

        # ===============================================inner nest============================================

        # sub table

        tdSql.query("select data from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from sub_table1_1 ); ")
        tdSql.checkData(0,0,90)

        # tdSql.query("select data from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from sub_table1_1 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)
        # tdSql.checkData(0,0,1)

        tdSql.query("select * from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 ); ")
        tdSql.checkData(0,5,90)

        # tdSql.query("select * from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)
        # tdSql.checkData(0,0,1)

        tdSql.query("select max(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 ); ")
        tdSql.checkData(0,0,90)

        # tdSql.query("select max(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(1)
        # tdSql.checkData(0,0,1)

        # tdSql.query("select max(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from sub_empty_2 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(0)

        # tdSql.query("select max(data),min(data),avg(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(1)

        # tdSql.query("select ceil(data),floor(data),round(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

        # tdSql.query("select spread(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(1)

        # tdSql.query("select diff(data) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(599)

        # tdSql.query("select DERIVATIVE(data ,1s ,1) from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,10s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(598)

        # tdSql.query("select ceil(data)from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

        # tdSql.query("select floor(data)from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

        # tdSql.query("select round(data)from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

        # tdSql.query("select data*10+2 from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

        # tdSql.query("select data*10+2 from (select count(*),avg(q_int) ,  sum(q_double),stddev(q_float),LEASTSQUARES(q_int,0,1), elapsed(ts,1s) data from regular_table_3 \
        #  where ts>=\"2015-01-01 00:00:00.000\"  and ts < \"2015-01-01 00:10:00.000\" interval(1s) fill(prev)); ")
        # tdSql.checkRows(600)

    def query_session_windows(self):

        # case TD-12344
        # session not support stable
        tdSql.error('select elapsed(ts,1s) from stable_1  where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  session(ts ,1s) group by tbname,ind order by ts asc ')

        tdSql.query('select elapsed(ts,1s) from sub_table1_1  session(ts,1w) ; ')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)
        tdSql.query('select elapsed(ts,1s) from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" session(ts,1w) ; ')
        tdSql.checkRows(1)
        tdSql.checkData(0,0,90)

        tdSql.query('select elapsed(ts,1s) from  ( select * from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000") session(ts,1w) ; ')

        tdSql.query('select elapsed(ts,1s) from  ( select ts ,q_int from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000") session(ts,1w) ; ')
        # tdSql.query('select elapsed(ts,1s) from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(20s) fill (next) session(ts,1w) ; ')

        tdSql.query('select elapsed(ts,1s) from sub_empty_1  where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  session(ts,1w) ; ')
        tdSql.checkRows(0)

        # windows state
        # not support stable

        tdSql.error('select elapsed(ts,1s) from stable_1  where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  state_window(q_int) group by tbname,ind order by ts asc ')

        tdSql.query('select elapsed(ts,1s) from sub_table1_1  state_window(q_int) ; ')
        tdSql.checkRows(10)
        tdSql.checkData(0,0,0)
        tdSql.query('select elapsed(ts,1s) from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" state_window(q_int) ; ')
        tdSql.checkRows(10)
        tdSql.checkData(0,0,0)

        tdSql.query('select elapsed(ts,1s) from  ( select * from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000") state_window(q_int) ; ')

        tdSql.query('select elapsed(ts,1s) from  ( select ts ,q_int from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000") state_window(q_int) ; ')

        tdSql.error('select elapsed(ts,1s) from sub_table1_1   where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000" interval(20s) fill (next) state_window(q_int) ; ')

        tdSql.query('select elapsed(ts,1s) from sub_empty_1  where ts>="2015-01-01 00:00:00.000"  and ts < "2015-01-01 00:10:00.000"  state_window(q_int); ')
        tdSql.checkRows(0)


    def continuous_query(self):
        tdSql.error('create table elapsed_t as select elapsed(ts) from sub_table1_1  interval(1m) sliding(30s);')
        tdSql.error('create table elapsed_tb as select elapsed(ts) from stable_1  interval(1m) sliding(30s) group by tbname;')
        tdSql.error('create table elapsed_tc as select elapsed(ts) from stable_1  interval(1s) sliding(5s) interval(1m) sliding(30s) group by tbname;')

    def query_precision(self):
        def generate_data(precision="ms"):
            tdSql.execute("create database if not exists db_%s keep 36500 precision '%s';" %(precision, precision))
            tdSql.execute("use db_%s;" %precision)
            tdSql.execute("create stable db_%s.st (ts timestamp , id int) tags(ind int);"%precision)
            tdSql.execute("create table db_%s.tb1 using st tags(1);"%precision)
            tdSql.execute("create table db_%s.tb2 using st tags(2);"%precision)

            if precision == "ms":
                start_ts = self.ts
                step = 10000
            elif precision == "us":
                start_ts = self.ts*1000
                step = 10000000
            elif precision == "ns":
                start_ts = self.ts*1000000
                step = 10000000000
            else:
                pass

            for i in range(10):

                sql1 = "insert into db_%s.tb1 values (%d,%d)"%(precision ,start_ts+i*step,i)
                sql2 = "insert into db_%s.tb1 values (%d,%d)"%(precision, start_ts+i*step,i)
                tdSql.execute(sql1)
                tdSql.execute(sql2)

        time_units = ["1s","1a","1u","1b"]

        precision_list = ["ms","us","ns"]
        for pres in precision_list:
            generate_data(pres)

            for index,unit in enumerate(time_units):

                if pres == "ms":
                    if unit in ["1u","1b"]:
                        tdSql.error("select elapsed(ts,%s) from db_%s.st group by tbname "%(unit,pres))
                        pass
                    else:
                        tdSql.query("select elapsed(ts,%s) from db_%s.st group by tbname "%(unit,pres))
                elif pres == "us" and unit in ["1b"]:
                    if unit in ["1b"]:
                        tdSql.error("select elapsed(ts,%s) from db_%s.st group by tbname "%(unit,pres))
                        pass
                    else:
                        tdSql.query("select elapsed(ts,%s) from db_%s.st group by tbname "%(unit,pres))
                else:

                    tdSql.query("select elapsed(ts,%s) from db_%s.st group by tbname "%(unit,pres))
                    basic_result = 90
                    tdSql.checkData(0,0,basic_result*pow(1000,index))

    def do_elapsed(self):
        # init
        self.ts = 1420041600000 
        self.num = 10

        tdSql.prepare()
        dbNameTest="testdbV1"
        self.prepare_db(dbNameTest,1)
        self.prepare_data(dbNameTest)
        self.abnormal_common_test()
        self.abnormal_use_test()
        self.query_filter()
        # self.query_interval()
        self.query_mix_common()
        self.query_mix_Aggregate()
        self.query_mix_select()
        self.query_mix_compute()
        self.query_mix_arithmetic()
        # self.query_with_join()
        # self.query_with_union()
        self.query_nest()
        self.query_session_windows()
        self.continuous_query()
        self.query_precision()

        dbNameTest="testdbV2"
        self.prepare_db(dbNameTest,2)
        self.prepare_data(dbNameTest)
        self.abnormal_common_test()
        self.abnormal_use_test()
        self.query_filter()
        # self.query_interval()
        self.query_mix_common()
        self.query_mix_Aggregate()
        self.query_mix_select()
        self.query_mix_compute()
        self.query_mix_arithmetic()
        # self.query_with_join()
        # self.query_with_union()
        self.query_nest()
        self.query_session_windows()
        self.continuous_query()
        # self.query_precision()
        
        print("do_elapsed ............................ [passed]\n")


    #
    # ------------------ main ------------------
    #
    def test_func_agg_elapsed(self):
        """ Fun: elapsed()

        1. Query on super/child/normal table
        2. Query with nested
        3. Query with join
        4. Query with union
        5. Query with other function
        6. Query with filter
        7. Query with tags
        8. Error cases

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-08 Huo  Hong Migrated to new test framework
            - 2025-9-24 Alex Duan Migrated from uncatalog/system-test/2-query/test_elapsed.py

        """
        # case 1
        self.do_normal_query()
        self.do_abnormal_query()
        self.do_nested_query()
        self.do_query_with_union()
        self.do_query_with_join()
        self.do_query_with_other_function()
        self.do_query_with_filter()
        print("\ndo case1 .............................. [passed]\n")
        # case2
        self.do_elapsed()