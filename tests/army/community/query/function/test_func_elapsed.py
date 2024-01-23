from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *


class TDTestCase(TBase):
    """Verify the elapsed function
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = 'db'
        self.table_dic = {
            "super_table": ["st1", "st2", "st_empty"],
            "child_table": ["ct1_1", "ct1_2", "ct1_empty", "ct2_1", "ct2_2", "ct2_empty"],
            "tags_value": [("2023-03-01 15:00:00", 1, 'bj'), ("2023-03-01 15:10:00", 2, 'sh'), ("2023-03-01 15:20:00", 3, 'sz'), ("2023-03-01 15:00:00", 4, 'gz'), ("2023-03-01 15:10:00", 5, 'cd'), ("2023-03-01 15:20:00", 6, 'hz')],
            "common_table": ["t1", "t2", "t_empty"]
        }
        self.start_ts = 1677654000000 # 2023-03-01 15:00:00.000
        self.row_num = 100

    def prepareData(self):
        # db
        tdSql.execute(f"create database {self.dbname};")
        tdSql.execute(f"use {self.dbname};")
        tdLog.debug(f"Create database {self.dbname}")

        # commont table
        for common_table in self.table_dic["common_table"]:
            tdSql.execute(f"create table {common_table} (ts timestamp, c_ts timestamp, c_int int, c_bigint bigint, c_double double, c_nchar nchar(16));")
            tdLog.debug("Create common table %s" % common_table)

        # super table
        for super_table in self.table_dic["super_table"]:
            tdSql.execute(f"create stable {super_table} (ts timestamp, c_ts timestamp, c_int int, c_bigint bigint, c_double double, c_nchar nchar(16)) tags (t1 timestamp, t2 int, t3 binary(16));")
            tdLog.debug("Create super table %s" % super_table)

        # child table
        for i in range(len(self.table_dic["child_table"])):
            if self.table_dic["child_table"][i].startswith("ct1"):
                tdSql.execute("create table {} using {} tags('{}', {}, '{}');".format(self.table_dic["child_table"][i], "st1", self.table_dic["tags_value"][i][0], self.table_dic["tags_value"][i][1], self.table_dic["tags_value"][i][2]))
            elif self.table_dic["child_table"][i].startswith("ct2"):
                tdSql.execute("create table {} using {} tags('{}', {}, '{}');".format(self.table_dic["child_table"][i], "st2", self.table_dic["tags_value"][i][0], self.table_dic["tags_value"][i][1], self.table_dic["tags_value"][i][2]))

        # insert data
        table_list = ["t1", "t2", "ct1_1", "ct1_2", "ct2_1", "ct2_2"]
        for t in table_list:
            sql = "insert into {} values".format(t)
            for i in range(self.row_num):
                sql += "({}, {}, {}, {}, {}, '{}'),".format(self.start_ts + i * 1000, self.start_ts + i * 1000, 32767+i, 65535+i, i, t + str(i))
            sql += ";"
            tdSql.execute(sql)
            tdLog.debug("Insert data into table %s" % t)

    def test_normal_query(self):
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

    def test_query_with_filter(self):
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

    def test_query_with_other_function(self):
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

    def test_query_with_join(self):
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

    def test_query_with_union(self):
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

    def test_query_with_window(self):
        query_list = [
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' interval(10s) fill(next);",
                "res": [(10,),(10,)()]
            },
            {
                "sql": "select elapsed(ts, 1s) from (select * from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:01:20.000' and c_int > 100) where ts >= '2023-03-01 15:01:00.000' and ts < '2023-03-01 15:02:00.000' interval(10s) fill(prev);",
                "res": [(10,)(10,)(),(),(),()]
            },
            {
                "sql": "select elapsed(ts, 1s) from st1 where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' session(ts, 2s);",
                "res": [(20,)]
            },
            {
                "sql": "select elapsed(ts, 1s) from st_empty where ts between '2023-03-01 15:00:00.000' and '2023-03-01 15:00:20.000' session(ts, 2s);",
                "res": []
            }
        ]
        sql_list = []
        res_list = []
        for item in query_list:
            sql_list.append(item["sql"])
            res_list.append(item["res"])
        tdSql.queryAndCheckResult(sql_list, res_list)

    def test_nested_query(self):
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

    def test_abnormal_query(self):
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

    def run(self):
        self.prepareData()
        self.test_normal_query()
        self.test_query_with_filter()
        self.test_query_with_other_function()
        self.test_query_with_join()
        self.test_query_with_union()
        self.test_abnormal_query()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
