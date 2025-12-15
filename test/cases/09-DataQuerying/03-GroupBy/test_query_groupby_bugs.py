import taos
import sys
import time
import socket
import os
import threading
import itertools
import random
import itertools

from new_test_framework.utils import tdLog, tdSql

class TestTS_3821:
    hostname = socket.gethostname()

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
    #
    # ------------------- 1 ----------------
    #
    def create_tables(self):
        tdSql.execute(f'''CREATE STABLE `s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators`
                          (`ts` TIMESTAMP, `event_time` TIMESTAMP, `wbli` DOUBLE, `vrc` DOUBLE, `csd` DOUBLE,
                          `oiv` DOUBLE, `tiv` DOUBLE, `flol` DOUBLE, `capacity` DOUBLE, `ispc` NCHAR(50)) TAGS
                          (`device_identification` NCHAR(64))''')
        tdSql.execute(f''' CREATE TABLE `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators`
                           USING `s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` (`device_identification`)
                           TAGS ("1000000000001001")''')

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:35.685','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:36.239','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:37.290','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:38.414','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")
        tdSql.execute(f"INSERT INTO `t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators` VALUES ('2023-08-06 17:47:39.471','2023-07-24 11:18:20.000', 17.199999999999999, 100.000000000000000, 100.000000000000000, NULL, 112.328999999999994, 132.182899999999989, 12.300000000000001, '符合条件')")

        tdLog.debug("insert data ............ [OK]")

    def FIX_TS_3821(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3821")

        tdSql.query(f'''select count(*),device_identification from s_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators
                        where 1=1 and device_identification ='1000000000001001' group by device_identification;''')
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, "1000000000001001")

        tdSql.query(f'''select count(*),device_identification from t_1000000000001001_e8d66f7af53e4c88866efbc44252a8cd_device_technical_indicators
                        group by device_identification;''')
        tdSql.checkRows(1)
        tdSql.checkCols(2)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(0, 1, "1000000000001001")

        print("do TS-3821 ............................ [passed]")


    #
    # ------------------- 2 ----------------
    #
    def prepareData2(self):
        # db
        tdSql.execute("create database db2")
        tdSql.execute("use db2")

        # super table
        tdSql.execute("create stable st(ts timestamp, c_ts_empty timestamp, c_int int, c_int_empty int, c_unsigned_int int unsigned, \
                      c_unsigned_int_empty int unsigned, c_bigint bigint, c_bigint_empty bigint, c_unsigned_bigint bigint unsigned, \
                      c_unsigned_bigint_empty bigint unsigned, c_float float, c_float_empty float, c_double double, c_double_empty double, \
                      c_binary binary(16), c_binary_empty binary(16), c_smallint smallint, c_smallint_empty smallint, \
                      c_smallint_unsigned smallint unsigned, c_smallint_unsigned_empty smallint unsigned, c_tinyint tinyint, \
                      c_tinyint_empty tinyint, c_tinyint_unsigned tinyint unsigned, c_tinyint_unsigned_empty tinyint unsigned, \
                      c_bool bool, c_bool_empty bool, c_nchar nchar(16), c_nchar_empty nchar(16), c_varchar varchar(16), \
                      c_varchar_empty varchar(16), c_varbinary varbinary(16), c_varbinary_empty varbinary(16)) \
                      tags(t_timestamp timestamp, t_timestamp_empty timestamp, t_int int, t_int_empty int, \
                      t_unsigned_int int unsigned, t_unsigned_int_empty int unsigned, t_bigint bigint, t_bigint_empty bigint, \
                      t_unsigned_bigint bigint unsigned, t_unsigned_bigint_empty bigint unsigned, t_float float, t_float_empty float, \
                      t_double double, t_double_empty double, t_binary binary(16), t_binary_empty binary(16), t_smallint smallint, \
                      t_smallint_empty smallint, t_smallint_unsigned smallint unsigned, t_smallint_unsigned_empty smallint unsigned, \
                      t_tinyint tinyint, t_tinyint_empty tinyint, t_tinyint_unsigned tinyint unsigned, t_tinyint_unsigned_empty tinyint unsigned, \
                      t_bool bool, t_bool_empty bool, t_nchar nchar(16), t_nchar_empty nchar(16), t_varchar varchar(16), \
                      t_varchar_empty varchar(16), t_varbinary varbinary(16), t_varbinary_empty varbinary(16));")

        # child tables
        start_ts = 1704085200000
        tags = [
            "'2024-01-01 13:00:01', null, 1, null, 1, null, 1111111111111111, null, 1111111111111111, null, 1.1, null, 1.11, null, 'aaaaaaaa', '', 1, null, 1, null, 1, null, 1, null, True, null, 'ncharaa', null, 'varcharaa', null, '0x7661726331', null",
            "'2024-01-01 13:00:02', null, 2, null, 2, null, 2222222222222222, null, 2222222222222222, null, 2.2, null, 2.22, null, 'bbbbbbbb', '', 2, null, 2, null, 2, null, 2, null, False, null, 'ncharbb', null, 'varcharbb', null, '0x7661726332', null",
            "'2024-01-01 13:00:03', null, 3, null, 3, null, 3333333333333333, null, 3333333333333333, null, 3.3, null, 3.33, null, 'cccccccc', '', 3, null, 3, null, 3, null, 3, null, True, null, 'ncharcc', null, 'varcharcc', null, '0x7661726333', null",
            "'2024-01-01 13:00:04', null, 4, null, 4, null, 4444444444444444, null, 4444444444444444, null, 4.4, null, 4.44, null, 'dddddddd', '', 4, null, 4, null, 4, null, 4, null, False, null, 'nchardd', null, 'varchardd', null, '0x7661726334', null",
            "'2024-01-01 13:00:05', null, 5, null, 5, null, 5555555555555555, null, 5555555555555555, null, 5.5, null, 5.55, null, 'eeeeeeee', '', 5, null, 5, null, 5, null, 5, null, True, null, 'ncharee', null, 'varcharee', null, '0x7661726335', null",
        ]
        for i in range(5):
            tdSql.execute(f"create table ct{i+1} using st tags({tags[i]});")

            # insert data
            data = "null, 1, null, 1, null, 1111111111111111, null, 1111111111111111, null, 1.1, null, 1.11, null, 'aaaaaaaa', null, 1, null, 1, null, 1, null, 1, null, True, null, 'ncharaa', null, 'varcharaa', null, '0x7661726331', null"
            for round in range(10):
                sql = f"insert into ct{i+1} values"
                for j in range(10):
                    sql += f"({start_ts + (round * 10 + j + 1) * 1000}, {data})"
                sql += ";"
                tdSql.execute(sql)

    def check_query_with_filter(self):
        # total row number
        tdSql.query("select count(*) from st;")
        total_rows = tdSql.queryResult[0][0]
        tdLog.debug("Total row number is %s" % total_rows)

        # start_ts and end_ts
        tdSql.query("select first(ts), last(ts) from st;")
        start_ts = tdSql.queryResult[0][0]
        end_ts = tdSql.queryResult[0][1]
        tdLog.debug("start_ts is %s, end_ts is %s" % (start_ts, end_ts))

        filter_dic = {
            "all_filter_list": ["ts <= now", "t_timestamp <= now", f"ts between '{start_ts}' and '{end_ts}'", 
                                f"t_timestamp between '{start_ts}' and '{end_ts}'", "c_ts_empty is null", 
                                "t_timestamp_empty is null", "ts > '1970-01-01 00:00:00'", "t_int in (1, 2, 3, 4, 5)",  
                                "c_int=1", "c_int_empty is null", "c_unsigned_int=1", "c_unsigned_int_empty is null",
                                "c_unsigned_int in (1, 2, 3, 4, 5)", "c_unsigned_int_empty is null", "c_bigint=1111111111111111",
                                "c_bigint_empty is null", "c_unsigned_bigint in (1111111111111111)", "c_unsigned_bigint_empty is null",
                               "c_float=1.1", "c_float_empty is null", "c_double=1.11", "c_double_empty is null", "c_binary='aaaaaaaa'",
                               "c_binary_empty is null", "c_smallint=1", "c_smallint_empty is null", "c_smallint_unsigned=1",
                               "c_smallint_unsigned_empty is null", "c_tinyint=1", "c_tinyint_empty is null", "c_tinyint_unsigned=1",
                               "c_tinyint_unsigned_empty is null", "c_bool=True", "c_bool_empty is null", "c_nchar='ncharaa'",
                               "c_nchar_empty is null", "c_varchar='varcharaa'", "c_varchar_empty is null", "c_varbinary='0x7661726331'",
                               "c_varbinary_empty is null"],
            "empty_filter_list": ["ts > now", "t_timestamp > now", "c_ts_empty is not null","t_timestamp_empty is not null",
                                  "ts <= '1970-01-01 00:00:00'", "c_ts_empty < '1970-01-01 00:00:00'", "c_int <> 1", "c_int_empty is not null", 
                                  "t_int in (10, 11)", "t_int_empty is not null"]
        }
        for filter in filter_dic["all_filter_list"]:
            tdLog.debug("Execute query with filter '%s'" % filter)
            tdSql.query(f"select * from st where {filter};")
            tdSql.checkRows(total_rows)

        for filter in filter_dic["empty_filter_list"]:
            tdLog.debug("Execute query with filter '%s'" % filter)
            tdSql.query(f"select * from st where {filter};")
            tdSql.checkRows(0)

    def check_query_with_groupby(self):
        tdSql.query("select count(*) from st group by tbname;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 100)

        tdSql.query("select count(c_unsigned_int_empty + c_int_empty * c_float_empty - c_double_empty + c_smallint_empty / c_tinyint_empty) from st where c_int_empty is null group by tbname;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 0)

        tdSql.query("select sum(t_unsigned_int_empty + t_int_empty * t_float_empty - t_double_empty + t_smallint_empty / t_tinyint_empty) from st where t_int_empty is null group by tbname;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)

        tdSql.query("select max(c_bigint_empty) from st group by tbname, t_bigint_empty, t_float_empty, t_double_empty;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        
        tdSql.query("select min(t_double) as v from st where c_nchar like '%aa%' and t_double is not null group by tbname, t_bigint_empty, t_float_empty, t_double_empty order by v limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1.11)
        
        tdSql.query("select top(c_float, 1) as v from st where c_nchar like '%aa%' group by tbname order by v desc slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1.1)

        tdSql.query("select first(ts) from st where c_varchar is not null partition by tbname order by ts slimit 1;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, '2024-01-01 13:00:01.000')
        
        tdSql.query("select first(c_nchar_empty) from st group by tbname;")
        tdSql.checkRows(0)
        
        tdSql.query("select first(ts), first(c_nchar_empty) from st group by tbname, ts order by ts slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-01-01 13:00:01.000')
        tdSql.checkData(0, 1, None)

        tdSql.query("select first(c_nchar_empty) from st group by t_timestamp_empty order by t_timestamp;")
        tdSql.checkRows(0)
        
        tdSql.query("select last(ts), last(c_nchar_empty) from st group by tbname, ts order by ts slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-01-01 13:00:01.000')
        tdSql.checkData(0, 1, None)
        
        tdSql.query("select elapsed(ts, 1s) t from st where c_int = 1 and c_nchar like '%aa%' group by tbname order by t desc slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 99)
        
        tdSql.query("select elapsed(ts, 1s) t from st where c_int_empty is not null and c_nchar like '%aa%' group by tbname order by t desc slimit 1 limit 1;")
        tdSql.checkRows(0)

    def check_query_with_join(self):
        tdSql.query("select count(*) from st as t1 join st as t2 on t1.ts = t2.ts and t1.c_float_empty is not null;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        
        tdSql.query("select count(t1.c_ts_empty) as v from st as t1 join st as t2 on t1.ts = t2.ts and t1.c_float_empty is null order by v desc;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        
        tdSql.query("select avg(t1.c_tinyint), sum(t2.c_bigint) from st t1, st t2 where t1.ts=t2.ts and t1.c_int > t2.c_int;")
        tdSql.checkRows(0)
        
        tdSql.query("select avg(t1.c_tinyint), sum(t2.c_bigint) from st t1, st t2 where t1.ts=t2.ts and t1.c_int <= t2.c_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 2777777777777777500)
        
        tdSql.query("select count(t1.c_float_empty) from st t1, st t2 where t1.ts=t2.ts and t1.c_int = t2.c_int and t1.t_int_empty=t2.t_int_empty;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    def check_query_with_window(self):
        # time window
        tdSql.query("select sum(c_int_empty) from st where ts > '2024-01-01 00:00:00.000' and ts <= '2024-01-01 14:00:00.000' interval(5m) sliding(1m) fill(value, 10);")
        tdSql.checkRows(845)
        tdSql.checkData(0, 0, 10)
        
        tdSql.query("select _wstart, _wend, sum(c_int) from st where ts > '2024-01-01 00:00:00.000' and ts <= '2024-01-01 14:00:00.000' interval(5m) sliding(1m);")
        tdSql.checkRows(6)

        # status window
        tdSql.error("select _wstart, count(*) from st state_window(t_bool);")
        tdSql.query("select _wstart, count(*) from st partition by tbname state_window(c_bool);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 100)

        # session window
        tdSql.query("select _wstart, count(*) from st partition by tbname, t_int session(ts, 1m);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 100)

        # event window
        tdSql.query("select _wstart, _wend, count(*) from (select * from st order by ts, tbname) event_window start with t_bool=true end with t_bool=false;")
        tdSql.checkRows(200)

    def check_query_with_union(self):
        tdSql.query("select count(ts) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        tdSql.query("select count(ts) from (select * from ct1 union all select * from ct2 union all select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 300)

        tdSql.query("select count(*) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        tdSql.query("select count(c_ts_empty) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query("select count(*) from (select ts from st union select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 101)

        tdSql.query("select count(*) from (select ts from st union all select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1000)

        tdSql.query("select count(ts) from (select ts from st union select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100)

        tdSql.query("select count(ts) from (select ts from st union all select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 500)

    def check_nested_query(self):
        tdSql.query("select elapsed(ts, 1s) from (select * from (select * from st where c_int = 1) where c_int_empty is null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 99)

        tdSql.query("select first(ts) as t, avg(c_int) as v from (select * from (select * from st where c_int = 1) where c_int_empty is null) group by t_timestamp order by t_timestamp desc slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-01-01 13:00:01.000')
        tdSql.checkData(0, 1, 1)

        tdSql.query("select max(c_tinyint) from (select c_tinyint, tbname from st where c_float_empty is null or t_int_empty is null) group by tbname order by c_tinyint desc slimit 1 limit 1;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query("select top(c_int, 3) from (select c_int, tbname from st where t_int in (2, 3)) group by tbname slimit 3;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)

    def FIX_TD_28163(self):
        self.prepareData2()
        self.check_query_with_filter()
        self.check_query_with_groupby()
        self.check_query_with_join()
        self.check_query_with_window()
        self.check_query_with_union()
        self.check_nested_query()

        print("do TD-28163 ........................... [passed]")

    #
    # ------------------- 3 ----------------
    #
    def init_class(self):
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

    def prepareData3(self):
        for db in self.metadata_dic.keys():
            if db == "db_tag_json":
                # db
                tdSql.execute(f"create database {db};")
                tdSql.execute(f"use {db};")
                tdLog.debug(f"Create database {db}")

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = f"create table {item['name']} ("
                    for column in item["columns"]:
                        sql += f"{column['name']} {column['type']},"
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += f"{tag['name']} {tag['type']},"
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug(f"Create super table {item['name']}")

                    # child table
                    tag_value_list = ['{"instance":"100"}', '{"instance":"200"}']
                    for i in range(item["child_table_num"]):
                        tdSql.execute(f"create table {'ct' + str(i+1)} using {item['name']} tags('{tag_value_list[i]}');")
                        tdLog.debug(f"Create child table {'ct' + str(i+1)} successfully")

                    # insert data
                    if i == 0:
                        tdSql.execute(f"insert into {'ct' + str(i+1)} values(now, 1)(now+1s, 2)")
                    elif i == 1:
                        tdSql.execute(f"insert into {'ct' + str(i+1)} values(now, null)(now+1s, null)")
            elif db == "db":
                # create database db_empty
                tdSql.execute("create database db_empty;")
                tdSql.execute("use db_empty;")
                tdLog.debug("Create database db_empty successfully")

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = f"create table {item['name']} ("
                    for column in item["columns"]:
                        sql += f"{column['name']} {column['type']},"
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += f"{tag['name']} {tag['type']},"
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug(f"Create super table {item['name']}")

                    # child table
                    tag_value_list = [['2024-01-01 12:00:00.000', 1, 1111111111111, 1.11, 111111.1111, True, 'aaa', 'beijing', 'POINT (3.000000 6.000000)', '0x7661726331'],['2024-01-02 12:00:00.000', 2, 2222222222222, 2.22, 222222.2222, False, 'bbb', 'shanghai', 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)', '0x7f829000']]
                    for i in range(item["child_table_num"]):
                        sql = f"create table {'ct' + (str(i+1) if item['name'] == 'st1' else str(i+3))} using {item['name']} tags("
                        for tag in tag_value_list[i]:
                            if type(tag) == str:
                                sql += f"'{tag}',"
                            else:
                                sql += f"{tag},"
                        sql = sql[:-1] + ");"
                        tdSql.execute(sql)
                        tdLog.debug(f"Create child table {'ct' + (str(i+1) if item['name'] == 'st1' else str(i+3))} successfully")

                # create database db_with_data
                tdSql.execute("create database db_with_data;")
                tdSql.execute("use db_with_data;")
                tdLog.debug("Create database db_with_data successfully")

                # super table
                for item in self.metadata_dic[db]["supertables"]:
                    sql = f"create table {item['name']} ("
                    for column in item["columns"]:
                        sql += f"{column['name']} {column['type']},"
                    sql = sql[:-1] + ") tags ("
                    for tag in item["tags"]:
                        sql += f"{tag['name']} {tag['type']},"
                    sql = sql[:-1] + ");"
                    tdLog.debug(sql)
                    tdSql.execute(sql)
                    tdLog.debug(f"Create super table {item['name']}")

                    # child table
                    tag_value_list = [['2024-01-01 12:00:00.000', 1, 1111111111111, 1.11, 111111.1111, True, 'aaa', 'beijing', 'POINT (3.000000 6.000000)', '0x7661726331'],['2024-01-02 12:00:00.000', 2, 2222222222222, 2.22, 222222.2222, False, 'bbb', 'shanghai', 'LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)', '0x7f829000']]
                    for i in range(item["child_table_num"]):
                        sql = f"create table {'ct' + (str(i+1) if item['name'] == 'st1' else str(i+3))} using {item['name']} tags("
                        for tag in tag_value_list[i]:
                            if type(tag) == str:
                                sql += f"'{tag}',"
                            else:
                                sql += f"{tag},"
                        sql = sql[:-1] + ");"
                        tdSql.execute(sql)
                        tdLog.debug(f"Create child table {'ct' + (str(i+1) if item['name'] == 'st1' else str(i+3))} successfully")

                        # insert into data
                        start_ts = 1677654000000 # 2023-03-01 15:00:00.000
                        sql = "insert into {} values".format("ct" + (str(i+1) if item["name"] == "st1" else str(i+3)))
                        binary_vlist = ["ccc", "ddd", "eee", "fff"]
                        nchar_vlist = ["guangzhou", "tianjing", "shenzhen", "hangzhou"]
                        geometry_vlist = ["POINT (4.0 8.0)", "POINT (3.0 5.0)", "LINESTRING (1.000000 1.000000, 2.000000 2.000000, 5.000000 5.000000)", "POLYGON ((3.000000 6.000000, 5.000000 6.000000, 5.000000 8.000000, 3.000000 8.000000, 3.000000 6.000000))"]
                        varbinary_vlist = ["0x7661726332", "0x7661726333", "0x7661726334", "0x7661726335"]
                        st_index = i if item["name"] == "st1" else (i+2)
                        for i in range(100):
                            sql += f"({start_ts + 1000 * i}, {str(i+1)}, {str(i+1)}, {str(i+1)}, {str(i+1)}, {True if i % 2 == 0 else False}, '{binary_vlist[st_index % 4]}', '{nchar_vlist[st_index % 4]}', '{geometry_vlist[st_index % 4]}', '{varbinary_vlist[st_index % 4]}')"
                        tdSql.execute(sql)
                        tdLog.debug(f"Insert into data into child table {'ct' + (str(i+1) if item['name'] == 'st1' else str(i+3))} successfully")

    def check_tag_json(self):
        tdSql.execute("use db_tag_json;")

        # super table query with correct tag name of json type
        tdSql.query("select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from st group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'instance' order by time;")
        tdSql.checkRows(2)
        
        # child table query with incorrect tag name of json type
        tdSql.query("select to_char(ts, 'yyyy-mm-dd hh24:mi:ss') as time, irate(col1) from ct1 group by to_char(ts, 'yyyy-mm-dd hh24:mi:ss'), t1->'name' order by time;")
        tdSql.checkRows(0)

        # child table query with null value
        tdSql.query("select ts, avg(col1) from ct2 group by ts, t1->'name' order by ts;")
        tdSql.checkRows(2)

    def check_db_empty(self):
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
                sql = f"select ({columns[0]} {operator} {columns[1]}) as total from {table};"
                tdSql.query(sql)
                tdSql.checkRows(0)
        
        # aggregation function
        for table in table_list:
            for columns in list(itertools.combinations(column_list[:-1] + tag_list[:-1], 2)):
                fun = random.sample(fun_list, 2)
                sql = f"select ({fun[0]}({columns[0]}) + {fun[1]}({columns[1]})) as total from {table};"
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
            sql = f"select ({table[0] + '.' + column} + {table[1] + '.' + tag}) total from {table[0]} join {table[1]} on {table[0]+ '.ts=' + table[1] + '.ts'} where {table[0] + '.' + where};"
            tdSql.query(sql)
            tdSql.checkRows(0)
        
        # group by
        value_fun_list = ["sum(col1+col2)", "avg(col3+col4)", "count(col6+col7)", "stddev(col2+col4)", "spread(col2+col3)"]
        group_by_list = ["tbname", "t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"]
        for table in table_list:
            value_fun = random.choice(value_fun_list)
            where = random.choice(where_list)
            group_by = random.choice(group_by_list)
            sql = f"select {value_fun} from {table} where {where} group by {group_by};"
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
                sql = f"select _wstart, {tag}, avg(col3+col4) from {table} where ts between '2024-03-01' and '2024-03-02' partition by {tag} interval(10s) sliding(5s) fill(linear);"
            elif "ct" in table:
                sql = f"select _wstart, sum(col1+col2) from {table} where ts between '2024-03-01' and '2024-03-02' partition by {tag} interval(10s) sliding(5s) fill(next);"
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
                sql = f"alter table st1 drop column {item};"
            elif "t" in item:
                sql = f"alter table st1 drop tag {item};"
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
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

    def check_db_with_data(self):
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

        tdSql.query("select total / v from (select elapsed(ts, 1s) as v, sum(col1) as total from st1 where ts between '2023-03-01' and '2023-03-02' interval(10s) fill(next));")
        tdSql.checkRows(8641)
        tdSql.checkData(0, 0, 11)

        tdSql.query("select to_char(time, 'yyyy-mm-dd'), sum(v2 - v1) from (select first(ts) as time, avg(col2 + col3) as v1, max(col4) as v2 from st1 where ts < now group by (col2+col3) order by (col2+col3)) group by to_char(time, 'yyyy-mm-dd');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2023-03-01')
        tdSql.checkData(0, 1, -5050)

        tdSql.query("select avg(v) from (select apercentile(col1, 50) as v from st1 where ts between '2023-03-01' and '2023-03-02' group by tbname) group by (50 -v);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 50)

        # drop or modify column/tag
        tdSql.execute("alter stable st1 drop column col7;")
        tdLog.debug("Drop column col7 successfully")
        tdSql.error("select count(*) from (select upper(col7) from st1);")

        tdSql.execute("alter stable st1 drop column col8;")
        tdLog.debug("Drop column col8 successfully")
        tdSql.error("select last(ts), avg(col1) from (select *, tbname from st1 where col8='POINT (3.0 6.0)') group by tbname;")

        tdSql.execute("alter stable st1 rename tag t8 t88;")
        tdLog.debug("Rename tag t8 to t88 successfully")
        tdSql.error("select count(*) from st1 t1, (select * from st1 where t8 is not null order by ts limit 10) t2 where t1.ts=t2.ts;")

        tdSql.execute("alter stable st1 rename tag t9 t99;")
        tdLog.debug("Rename tag t9 to t99 successfully")
        tdSql.error("select count(*) from st1 t1, (select * from st1 where t9='POINT (4.0 8.0)' limit 5) t2 where t1.ts=t2.ts;")

    def FIX_TS_4382(self):
        self.prepareData3()
        self.check_tag_json()
        self.check_db_empty()
        self.check_db_with_data()
    
        print("do TS-4382 ............................ [passed]")

    #
    # ------------------- 4 ----------------
    #
    def FIX_TS_(self):
        #
        # init data
        #


        #
        # check
        #

        print("do TS- ............................ [passed]")

    #
    # ------------------- 5 ----------------
    #
    def FIX_TS_(self):
        #
        # init data
        #


        #
        # check
        #

        print("do TS- ............................ [passed]")

    #
    # ------------------- 14 ----------------
    #
    def FIX_TS_(self):
        #
        # init data
        #


        #
        # check
        #

        print("do TS- ............................ [passed]")

    #
    # ------------------- 10 ----------------
    #
    def FIX_TS_(self):
        #
        # init data
        #


        #
        # check
        #

        print("do TS- ............................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_query_groupby_bugs(self):
        """Group by bugs

        1. Verify bug TS-3821 (tag value not show with group by query)
        2. Verify bug TD-28163 (group by query bugs with null values)
        3. Verify bug TS-4382 (group by query bugs with various data types)
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_ts_3821.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_test_td_28163.py
            - 2025-12-15 Alex Duan Migrated from cases/uncatalog/system-test/2-query/test_test_ts_4382.py

        """
        self.do_ts_3821()