import random
import itertools
from util.log import *
from util.cases import *
from util.sql import *
from util.sqlset import *
from util import constant
from util.common import *


class TDTestCase:
    """Verify the jira TD-28163
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def prepareData(self):
        # db
        tdSql.execute("create database if not exists db")
        tdSql.execute("use db")

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
            for round in range(100):
                sql = f"insert into ct{i+1} values"
                for j in range(100):
                    sql += f"({start_ts + (round * 100 + j + 1) * 1000}, {data})"
                sql += ";"
                tdSql.execute(sql)
        tdLog.debug("Prepare data successfully")

    def test_query_with_filter(self):
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

    def test_query_with_groupby(self):
        tdSql.query("select count(*) from st group by tbname;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 10000)

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
        tdSql.checkData(0, 0, 9999)
        
        tdSql.query("select elapsed(ts, 1s) t from st where c_int_empty is not null and c_nchar like '%aa%' group by tbname order by t desc slimit 1 limit 1;")
        tdSql.checkRows(0)

    def test_query_with_join(self):
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
        tdSql.checkData(0, 1, 1076616672134475760)
        
        tdSql.query("select count(t1.c_float_empty) from st t1, st t2 where t1.ts=t2.ts and t1.c_int = t2.c_int and t1.t_int_empty=t2.t_int_empty;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    def test_query_with_window(self):
        # time window
        tdSql.query("select sum(c_int_empty) from st where ts > '2024-01-01 00:00:00.000' and ts <= '2024-01-01 14:00:00.000' interval(5m) sliding(1m) fill(value, 10);")
        tdSql.checkRows(841)
        tdSql.checkData(0, 0, 10)
        
        tdSql.query("select _wstart, _wend, sum(c_int) from st where ts > '2024-01-01 00:00:00.000' and ts <= '2024-01-01 14:00:00.000' interval(5m) sliding(1m);")
        tdSql.checkRows(65)

        # status window
        tdSql.error("select _wstart, count(*) from st state_window(t_bool);")
        tdSql.query("select _wstart, count(*) from st partition by tbname state_window(c_bool);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10000)

        # session window
        tdSql.query("select _wstart, count(*) from st partition by tbname, t_int session(ts, 1m);")
        tdSql.checkRows(5)
        tdSql.checkData(0, 1, 10000)

        # event window
        tdSql.query("select _wstart, _wend, count(*) from (select * from st order by ts, tbname) event_window start with t_bool=true end with t_bool=false;")
        tdSql.checkRows(20000)

    def test_query_with_union(self):
        tdSql.query("select count(ts) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)

        tdSql.query("select count(ts) from (select * from ct1 union all select * from ct2 union all select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 30000)

        tdSql.query("select count(*) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)

        tdSql.query("select count(c_ts_empty) from (select * from ct1 union select * from ct2 union select * from ct3);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query("select count(*) from (select ts from st union select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10001)

        tdSql.query("select count(*) from (select ts from st union all select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 100000)

        tdSql.query("select count(ts) from (select ts from st union select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 10000)

        tdSql.query("select count(ts) from (select ts from st union all select c_ts_empty from st);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 50000)

    def test_nested_query(self):
        tdSql.query("select elapsed(ts, 1s) from (select * from (select * from st where c_int = 1) where c_int_empty is null);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 9999)

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

    def run(self):
        self.prepareData()
        self.test_query_with_filter()
        self.test_query_with_groupby()
        self.test_query_with_join()
        self.test_query_with_window()
        self.test_query_with_union()
        self.test_nested_query()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
