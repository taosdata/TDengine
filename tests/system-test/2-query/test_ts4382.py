import random
import itertools
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the jira TS-4382
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # self.dbname = 'db'
        # self.stbname = 'st'
        # self.ctbname_list = ["ct1", "ct2"]
        # self.tag_value_list = ['{"instance":"100"}', '{"instance":"200"}']

        self.metadata_dic = {
            "db_tag_json": {
                "supertables": [
                    {
                        "name": "st",
                        "child_table_num": 2,
                        "columns": [
                            {
                                "name": "ts",
                                "type": "timestamp"
                            },
                            {
                                "name": "col1",
                                "type": "int"
                            }
                        ],
                        "tags": [
                            {
                                "name": "t1",
                                "type": "json"
                            }
                        ]
                    }
                ]
            },
            "db": {
                "supertables": [
                    {
                        "name": "st1",
                        "child_table_num": 2,
                        "columns": [
                            {
                                "name": "ts",
                                "type": "timestamp"
                            },
                            {
                                "name": "col1",
                                "type": "int"
                            },
                            {
                                "name": "col2",
                                "type": "bigint"
                            },
                            {
                                "name": "col3",
                                "type": "float"
                            },
                            {
                                "name": "col4",
                                "type": "double"
                            },
                            {
                                "name": "col5",
                                "type": "bool"
                            },
                            {
                                "name": "col6",
                                "type": "binary(16)"
                            },
                            {
                                "name": "col7",
                                "type": "nchar(16)"
                            }, 
                            {
                                "name": "col8",
                                "type": "geometry(512)"
                            },
                            {
                                "name": "col9",
                                "type": "varbinary(32)"
                            }
                        ],
                        "tags": [
                            {
                                "name": "t1",
                                "type": "timestamp"
                            },
                            {
                                "name": "t2",
                                "type": "int"
                            },
                            {
                                "name": "t3",
                                "type": "bigint"
                            },
                            {
                                "name": "t4",
                                "type": "float"
                            },
                            {
                                "name": "t5",
                                "type": "double"
                            },
                            {
                                "name": "t6",
                                "type": "bool"
                            },
                            {
                                "name": "t7",
                                "type": "binary(16)"
                            },
                            {
                                "name": "t8",
                                "type": "nchar(16)"
                            }, 
                            {
                                "name": "t9",
                                "type": "geometry(512)"
                            },
                            {
                                "name": "t10",
                                "type": "varbinary(32)"
                            }
                        ]
                    },
                    {
                        "name": "st2",
                        "child_table_num": 2,
                        "columns": [
                            {
                                "name": "ts",
                                "type": "timestamp"
                            },
                            {
                                "name": "col1",
                                "type": "int"
                            },
                            {
                                "name": "col2",
                                "type": "bigint"
                            },
                            {
                                "name": "col3",
                                "type": "float"
                            },
                            {
                                "name": "col4",
                                "type": "double"
                            },
                            {
                                "name": "col5",
                                "type": "bool"
                            },
                            {
                                "name": "col6",
                                "type": "binary(16)"
                            },
                            {
                                "name": "col7",
                                "type": "nchar(16)"
                            }, 
                            {
                                "name": "col8",
                                "type": "geometry(512)"
                            },
                            {
                                "name": "col9",
                                "type": "varbinary(32)"
                            }
                        ],
                        "tags": [
                            {
                                "name": "t1",
                                "type": "timestamp"
                            },
                            {
                                "name": "t2",
                                "type": "int"
                            },
                            {
                                "name": "t3",
                                "type": "bigint"
                            },
                            {
                                "name": "t4",
                                "type": "float"
                            },
                            {
                                "name": "t5",
                                "type": "double"
                            },
                            {
                                "name": "t6",
                                "type": "bool"
                            },
                            {
                                "name": "t7",
                                "type": "binary(16)"
                            },
                            {
                                "name": "t8",
                                "type": "nchar(16)"
                            }, 
                            {
                                "name": "t9",
                                "type": "geometry(512)"
                            },
                            {
                                "name": "t10",
                                "type": "varbinary(32)"
                            }
                        ]
                    }
                ]
            }
        }

    def prepareData(self):
        for db in self.metadata_dic.keys():
            if db == "db_tag_json":
                # db
                tdSql.execute("create database {};".format(db))
                tdSql.execute("use {};".format(db))
                tdLog.debug("Create database %s" % db)

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = "create table {} (".format(item["name"])
                    for column in item["columns"]:
                        sql += "{} {},".format(column["name"], column["type"])
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += "{} {},".format(tag["name"], tag["type"])
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug("Create super table {}".format(item["name"]))

                    # child table
                    tag_value_list = ['{"instance":"100"}', '{"instance":"200"}']
                    for i in range(item["child_table_num"]):
                        tdSql.execute("create table {} using {} tags('{}');".format("ct" + str(i+1), item["name"], tag_value_list[i]))
                        tdLog.debug("Create child table {} successfully".format("ct" + str(i+1)))

                    # insert data
                    if i == 0:
                        tdSql.execute("insert into {} values(now, 1)(now+1s, 2)".format("ct" + str(i+1)))
                    elif i == 1:
                        tdSql.execute("insert into {} values(now, null)(now+1s, null)".format("ct" + str(i+1)))
            elif db == "db":
                # create database db_empty
                tdSql.execute("create database db_empty;")
                tdSql.execute("use db_empty;")
                tdLog.debug("Create database db_empty successfully")

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = "create table {} (".format(item["name"])
                    for column in item["columns"]:
                        sql += "{} {},".format(column["name"], column["type"])
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += "{} {},".format(tag["name"], tag["type"])
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug("Create super table {}".format(item["name"]))

                    # child table
                    tag_value_list = [['2024-01-01 12:00:00.000', 1, 1111111111111, 1.11, 111111.1111, True, 'aaa', 'beijing', 'POINT (3.000000 6.000000)', '0x7661726331'],['2024-01-02 12:00:00.000', 2, 2222222222222, 2.22, 222222.2222, False, 'bbb', 'shanghai', 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)', '0x7f829000']]
                    for i in range(item["child_table_num"]):
                        sql = "create table {} using {} tags(".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3)), item["name"])
                        for tag in tag_value_list[i]:
                            if type(tag) == str:
                                sql += "'{}',".format(tag)
                            else:
                                sql += "{},".format(tag)
                        sql = sql[:-1] + ");"
                        tdSql.execute(sql)
                        tdLog.debug("Create child table {} successfully".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3))))

                # create database db_with_data
                tdSql.execute("create database db_with_data;")
                tdSql.execute("use db_with_data;")
                tdLog.debug("Create database db_with_data successfully")

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = "create table {} (".format(item["name"])
                    for column in item["columns"]:
                        sql += "{} {},".format(column["name"], column["type"])
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += "{} {},".format(tag["name"], tag["type"])
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug("Create super table {}".format(item["name"]))

                    # child table
                    tag_value_list = [['2024-01-01 12:00:00.000', 1, 1111111111111, 1.11, 111111.1111, True, 'aaa', 'beijing', 'POINT (3.000000 6.000000)', '0x7661726331'],['2024-01-02 12:00:00.000', 2, 2222222222222, 2.22, 222222.2222, False, 'bbb', 'shanghai', 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)', '0x7f829000']]
                    for i in range(item["child_table_num"]):
                        sql = "create table {} using {} tags(".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3)), item["name"])
                        for tag in tag_value_list[i]:
                            if type(tag) == str:
                                sql += "'{}',".format(tag)
                            else:
                                sql += "{},".format(tag)
                        sql = sql[:-1] + ");"
                        tdSql.execute(sql)
                        tdLog.debug("Create child table {} successfully".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3))))
                        
                        # insert into data
                        start_ts = 1677654000000 # 2023-03-01 15:00:00.000
                        sql = "insert into {} values".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3)))
                        binary_vlist = ["ccc", "ddd", "eee", "fff"]
                        nchar_vlist = ["guangzhou", "tianjing", "shenzhen", "hangzhou"]
                        geometry_vlist = ["POINT (4.0 8.0)", "POINT (3.0 5.0)", "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)", "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))"]
                        varbinary_vlist = ["0x7661726332", "0x7661726333", "0x7661726334", "0x7661726335"]
                        st_index = i if item["name"] == "st1" else (i+2)
                        for i in range(100):
                            sql += "({}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}')".format(start_ts + 1000 * i, str(i+1), str(i+1), str(i+1), str(i+1), True if i % 2 == 0 else False, binary_vlist[st_index % 4], nchar_vlist[st_index % 4], geometry_vlist[st_index % 4], varbinary_vlist[st_index % 4])
                        tdSql.execute(sql)
                        tdLog.debug("Insert into data into child table {} successfully".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3))))

    def test_tag_json(self):
        tdSql.execute("use db_tag_json;")
        sql_list = [
            # super table query with correct tag name of json type
            {
                "sql": "select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from st group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'instance' order by time;",
                "result_check": "0.0"
            },
            # child table query with incorrect tag name of json type
            {
                "sql": "select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from ct1 group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'name' order by time;",
                "result_check": "None"
            },
            # child table query with null value
            {
                "sql": "select ts, avg(col1) from ct2 group by ts, t1->'name' order by ts;",
                "result_check": "None"
            }
        ]
        for sql_dic in sql_list:
            tdSql.query(sql_dic["sql"])
            tdLog.debug("execute sql: %s" % sql_dic["sql"])
            for item in [row[1] for row in tdSql.queryResult]:
                if sql_dic["result_check"] in str(item):
                    tdLog.debug("Check query result of '{}' successfully".format(sql_dic["sql"]))
                    break

    def test_db_empty(self):
        tdSql.execute("use db_empty;")
        table_list = ["st1", "ct1"]
        column_list = ["col1", "col2", "col3", "col4", "col5"]
        tag_list = ["t2", "t3", "t4", "t5", "t6"]
        operator_list = ["+", "-", "*", "/"]
        fun_list = ["avg", "count", "sum", "spread"]
        
        # two columns with arithmetic operation
        for table in table_list:
            for columns in list(itertools.combinations(column_list + tag_list, 2)):
                operator = random.choice(operator_list)
                sql = "select ({} {} {}) as total from {};".format(columns[0], operator, columns[1], table)
                tdSql.query(sql)
                tdSql.checkRows(0)
        
        # aggregation function
        for table in table_list:
            for columns in list(itertools.combinations(column_list[:-1] + tag_list[:-1], 2)):
                fun = random.sample(fun_list, 2)
                sql = "select ({}({}) + {}({})) as total from {};".format(fun[0], columns[0], fun[1], columns[1], table)
                tdSql.query(sql)
                if "count" in fun:
                    # default config 'countAlwaysReturnValue' as 0
                    tdSql.checkRows(1)
                else:
                    tdSql.checkRows(0)

        # join
        table_list = ["st1", "st2", "ct1", "ct2", "ct3", "ct4"]
        column_list = ["col1", "col2", "col3", "col4", "col5"]
        tag_list = ["t2", "t3", "t4", "t5", "t6"]
        where_list = ["col1 > 100", "col2 < 237883294", "col3 >= 163.23", "col4 <= 674324.2374898237", "col5=true", "col6='aaa'",
                      "col7='beijing'", "col8!='POINT (3.000000 6.000000)'", "col9='0x7661726331'"]
        for table in list(itertools.combinations(table_list,2)):
            where = random.choice(where_list)
            column = random.choice(column_list)
            tag = random.choice(tag_list)
            sql = "select ({} + {}) total from {} join {} on {} where {};".format(table[0] + "." + column, table[1] + "." + tag, table[0], table[1], table[0]+ ".ts=" + table[1] + ".ts", table[0] + "." + where)
            tdSql.query(sql)
            tdSql.checkRows(0)
        
        # group by
        value_fun_list = ["sum(col1+col2)", "avg(col3+col4)", "count(col6+col7)", "stddev(col2+col4)", "spread(col2+col3)"]
        group_by_list = ["tbname", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"]
        for table in table_list:
            value_fun = random.choice(value_fun_list)
            where = random.choice(where_list)
            group_by = random.choice(group_by_list)
            sql = "select {} from {} where {} group by {};".format(value_fun, table, where, group_by)
            tdSql.query(sql)
            # default config 'countAlwaysReturnValue' as 0
            if "count" in value_fun and "st" in table:
                tdSql.checkRows(2)
            elif "count" in value_fun and "ct" in table:
                tdSql.checkRows(1)
            else:
                tdSql.checkRows(0)

        # window query
        for table in table_list:
            tag = random.choice(tag_list)
            if "st" in table:
                sql = "select _wstart, {}, avg(col3+col4) from {} where ts between '2024-03-01' and '2024-03-02' partition by {} interval(10s) sliding(5s) fill(linear);".format(tag, table, tag)
            elif "ct" in table:
                sql = "select _wstart, sum(col1+col2) from {} where ts between '2024-03-01' and '2024-03-02' partition by {} interval(10s) sliding(5s) fill(next);".format(table, tag)
            tdSql.query(sql)
            tdSql.checkRows(0)

        # nested query
        for table in table_list:
            sql_list = [
                "select (col1 + col2) from (select sum(col1) as col1, avg(col2) as col2 from {} where col1 > 100 and ts between '2024-03-01' and '2024-03-02' group by tbname);".format(table),
                "select last(ts), avg(col2 - col3) from (select first(ts) as ts, sum(col2) as col2, last(col3) as col3 from {} where col9 != 'abc' partition by tbname interval(10s) sliding(5s));".format(table),
                "select elapsed(ts, 1s), sum(c1 + c2) from (select * from (select ts, (col1+col2) as c1, (col3 * col4) as c2, tbname from {} where col1 > 100 and ts between '2024-03-01' and '2024-03-02')) group by tbname;".format(table)
            ]
            for sql in sql_list:
                tdSql.query(sql)
                tdSql.checkRows(0)

        # drop column/tag
        del_column_tag_list = ["col1", "t1"]
        error_sql_list = [
            "select first(t1), sum(col1) from st1 group by tbname;",
            "select last(ts), avg(col1) from st1 group by tbname;",
            "select count(col1) from (select * from st1 where ts between '2024-03-01' and '2024-03-02' and col1 > 100) group by tbname;",
        ]
        for item in del_column_tag_list:
            if "col" in item:
                sql = "alter table st1 drop column {};".format(item)
            elif "t" in item:
                sql = "alter table st1 drop tag {};".format(item)
            tdSql.execute(sql)
        tdLog.debug("Delete {} successfully".format(str(del_column_tag_list)))

        for table in table_list:
            for sql in error_sql_list:
                tdSql.error(sql)
        
        # modify column for common table
        tdSql.execute("create table t1 (ts timestamp, col1 int, col2 bigint, col3 float, col4 double, col5 bool, col6 binary(16), col7 nchar(16), col8 geometry(512), col9 varbinary(32));")
        tdSql.execute("insert into t1 values(now, 1, 1111111111111, 1.11, 111111.1111, True, 'aaa', 'beijing', 'POINT (3.000000 6.000000)', '0x7661726331');")
        tdSql.execute("alter table t1  rename column col1 col11;")
        tdSql.error("select col1 from t1 where ts <= now and col3=1.11;")
        tdSql.query("select col11 from t1 where ts <= now and col3=1.11;")
        assert(len(tdSql.queryResult) == 1 and tdSql.queryResult[0][0] == 1)

    def test_db_with_data(self):
        tdSql.execute("use db_with_data;")

        sql_list = [
            "select pow(col1, null) from st1 where ts > now;",
            "select pow(null, col1) from st1 where ts > now;",
            "select log(null, col2) from st1 where col1 > 1000;",
            "select log(col2, null) from st1 where col1 > 1000;",
            "select avg(col1 + t2) from ct1 where ts between '2025-03-01' and '2025-03-02' and t2 < 0;",
            "select char_length(col6) from st1 where ts > now;",
            "select concat(col6, col7) from st1 where ts > now;",
            "select char_length(concat(col6, col7)) from st1 where ts > now;",
            "select rtrim(ltrim(concat(col6, col7))) from st1 where ts > now;",
            "select lower(rtrim(ltrim(concat(col6, col7)))) from st1 where ts > now;",
            "select upper(rtrim(ltrim(concat(col6, col7)))) from st1 where ts > now;",
            "select substr(rtrim(ltrim(concat(col6, col7))), 1, 10) from st1 where ts > now;",
            "select avg(col1 - col2) as v from st1 where ts between '2022-03-01' and '2022-03-02';",
            "select avg(col1 * col3) as v from st1 where ts between '2022-03-01' and '2022-03-02' and col1 > 100 group by tbname;",
            "select sum(col1 / col4) as cv, avg(t2 + t3) as tv from st1 where ts between '2022-03-01' and '2022-03-02' and col1 > 100 group by tbname;",
            "select sum(v1+v2) from (select first(ts) as time, avg(col1+col2) as v1, max(col3) as v2 from st1 where ts > now group by (col1+col2) order by (col1+col2));",
            "select first(ts), count(*), avg(col2 * t3) from (select ts, col1, col2, col3, t1, t2, t3, tbname from st1 where ts between '2022-03-01' and '2022-03-02' and col1 > 100) group by tbname;",
            "select cast(t8 as nchar(32)), sum(col1), avg(col2) from st1 where ts > now group by cast(t8 as nchar(32));",
            "select to_char(time, 'yyyy-mm-dd'), sum(v2 - v1) from (select first(ts) as time, avg(col2 + col3) as v1, max(col4) as v2 from st1 where ts < now group by (col2+col3) order by (col2+col3)) where time > now group by to_char(time, 'yyyy-mm-dd');",
            "select count(time) * sum(v) from (select to_iso8601(ts, '+00:00') as time, abs(col1+col2) as v, tbname from st1 where ts between '2023-03-01' and '2023-03-02' and col1 > 100) group by tbname;",
            "select avg(v) from (select apercentile(col1, 50) as v from st1 where ts between '2023-03-01' and '2023-03-02' group by tbname) where v > 50;",
        ]
        for sql in sql_list:
            tdSql.query(sql)
            tdSql.checkRows(0)

        sql_list_with_res = [
            {
                "sql": "select total / v from (select elapsed(ts, 1s) as v, sum(col1) as total from st1 where ts between '2023-03-01' and '2023-03-02' interval(10s) fill(next));",
                "res": [8641, (11,)]
            },
            {
                "sql": "select to_char(time, 'yyyy-mm-dd'), sum(v2 - v1) from (select first(ts) as time, avg(col2 + col3) as v1, max(col4) as v2 from st1 where ts < now group by (col2+col3) order by (col2+col3)) group by to_char(time, 'yyyy-mm-dd');",
                "res": [1, ('2023-03-01', -5050,)]
            },
            {
                "sql": "select avg(v) from (select apercentile(col1, 50) as v from st1 where ts between '2023-03-01' and '2023-03-02' group by tbname) group by (50 -v);",
                "res": [1, (50,)]
            }
        ]
        for sql in sql_list_with_res:
            tdSql.query(sql["sql"])
            assert(len(tdSql.queryResult) == sql["res"][0] and tdSql.queryResult[0][0] == sql["res"][1][0])

    def run(self):
        self.prepareData()
        self.test_tag_json()
        self.test_db_empty()
        self.test_db_with_data()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
