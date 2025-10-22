import datetime
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from dataclasses import dataclass, field
from typing import List, Any, Tuple
from new_test_framework.utils.tserror import TSDB_CODE_PAR_SYNTAX_ERROR

#  --------- system-test header --------
PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL = [BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [BOOL_COL, ]
TS_TYPE_COL = [TS_COL, ]

INT_TAG = "t_int"

ALL_COL = [PRIMARY_COL, INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, BINARY_COL, NCHAR_COL, BOOL_COL, TS_COL]
TAG_COL = [INT_TAG]
# insert data args：
TIME_STEP = 10000
NOW = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
STBNAME = f"{DBNAME}.stb1"
CTBNAME = f"{DBNAME}.ct1"
NTBNAME = f"{DBNAME}.nt1"

@dataclass
class DataSet:
    ts_data     : List[int]     = field(default_factory=list)
    int_data    : List[int]     = field(default_factory=list)
    bint_data   : List[int]     = field(default_factory=list)
    sint_data   : List[int]     = field(default_factory=list)
    tint_data   : List[int]     = field(default_factory=list)
    int_un_data : List[int]     = field(default_factory=list)
    bint_un_data: List[int]     = field(default_factory=list)
    sint_un_data: List[int]     = field(default_factory=list)
    tint_un_data: List[int]     = field(default_factory=list)
    float_data  : List[float]   = field(default_factory=list)
    double_data : List[float]   = field(default_factory=list)
    bool_data   : List[int]     = field(default_factory=list)
    binary_data : List[str]     = field(default_factory=list)
    nchar_data  : List[str]     = field(default_factory=list)

class TestJoin:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    #
    # ---------------- sim ---------------------
    #
    def do_sim_join(self):
        dbPrefix = "join_db"
        tbPrefix = "join_tb"
        mtPrefix = "join_mt"
        tbNum = 2
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== join.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tstart = 100000

        tdSql.execute(f"create database if not exists {db} keep 36500")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12))"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tg2 = "'abc'"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {tg2} )")

            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 100000

        tstart = 100000
        mt = mtPrefix + "1"
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(12), t3 int)"
        )

        i = 0
        tbPrefix = "join_1_tb"

        while i < tbNum:
            tb = tbPrefix + str(i)
            c = i
            t3 = i + 1
            binary = "'abc" + str(i) + "'"
            tdLog.info(f"{binary}")
            tdSql.execute(f"create table {tb} using {mt} tags( {i} , {binary} , {t3} )")
            x = 0
            while x < rowNum:
                ms = str(x) + "m"
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({tstart} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                tstart = tstart + 1
                x = x + 1
            i = i + 1
            tstart = 100000

        i1 = 1
        i2 = 0

        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        dbPrefix = "join_db"
        tbPrefix = "join_tb"
        mtPrefix = "join_mt"

        tb1 = tbPrefix + str(i1)
        tb2 = tbPrefix + str(i2)
        ts1 = tb1 + ".ts"
        ts2 = tb2 + ".ts"

        # single table join sql

        # select duplicate columns
        tdSql.query(f"select {ts1} , {ts2} from {tb1} , {tb2} where {ts1} = {ts2}")

        val = rowNum
        tdSql.checkRows(val)

        # select star1
        tdSql.query(
            f"select join_tb1.*, join_tb0.ts from {tb1} , {tb2} where {ts1} = {ts2}"
        )

        val = rowNum
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2}"
        )

        val = rowNum
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 0;"
        )

        val = 0
        tdSql.checkRows(val)

        # select star2
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} limit 0;"
        )

        val = 0
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts = 100000 limit 10;"
        )

        val = 1
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false limit 10;"
        )

        val = 10
        tdSql.checkRows(val)

        # select + where condition
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false limit 10 offset 1;"
        )
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, "1970-01-01 08:01:40.100")

        tdSql.checkData(1, 0, "1970-01-01 08:01:40.200")

        tdLog.info(f"tdSql.getData(0,6) = {tdSql.getData(0,6)}")
        tdLog.info(f"tdSql.getData(0,7) = {tdSql.getData(0,7)}")
        tdLog.info(f"tdSql.getData(0,8) = {tdSql.getData(0,8)}")
        tdLog.info(f"tdSql.getData(0,0) = {tdSql.getData(0,0)}")

        tdSql.checkData(0, 7, 0)

        # select + where condition   ======reverse query
        tdSql.query(
            f"select join_tb1.*, join_tb0.* from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true order by join_tb0.ts asc limit 1;"
        )

        val = 1
        tdSql.checkRows(val)

        val = "1970-01-01 08:01:40.001"
        tdLog.info(f"{tdSql.getData(0,0)}, {tdSql.getData(0,1)}")

        tdSql.checkData(0, 0, val)

        tdLog.info(f"1")
        # select + where condition + interval query
        tdLog.info(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart asc;"
        )
        tdSql.query(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart asc;"
        )
        val = 100
        tdSql.checkRows(val)

        tdLog.info(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart desc;"
        )
        tdSql.query(
            f"select count(join_tb1.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = true interval(10a) order by _wstart desc;"
        )
        val = 100
        tdSql.checkRows(val)

        # ===========================aggregation===================================
        # select + where condition
        tdSql.query(
            f"select count(join_tb1.*), count(join_tb0.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts >= 100000 and join_tb0.c7 = false;"
        )

        val = 10
        tdSql.checkData(0, 0, val)

        tdSql.checkData(0, 1, val)

        tdSql.query(
            f"select count(join_tb1.*) + count(join_tb0.*) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts >= 100000 and join_tb0.c7 = false;;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 20.000000000)

        tdSql.query(
            f"select count(join_tb1.*)/10 from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts >= 100000 and join_tb0.c7 = false;;"
        )
        tdSql.checkData(0, 0, 1.000000000)

        tdLog.info(f"3")
        # agg + where condition
        tdSql.query(
            f"select count(join_tb1.c3), count(join_tb0.ts) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.checkRows(1)

        tdLog.info(f"{tdSql.getData(0,0)}")

        tdSql.checkData(0, 0, 2)

        tdSql.checkData(0, 1, 2)

        tdLog.info(f"4")
        # agg + where condition
        tdSql.query(
            f"select count(join_tb1.c3), count(join_tb0.ts), sum(join_tb0.c1), first(join_tb0.c7), last(join_tb1.c3), first(join_tb0.*) from {tb1} , {tb2} where {ts1} = {ts2} and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )

        val = 2
        tdSql.checkData(0, 0, val)

        tdSql.checkData(0, 1, val)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 1)

        tdSql.checkData(0, 4, 2)

        tdLog.info(f"=============== join.sim -- error sql")

        tdSql.error(
            f"select count(join_tb1.c3), count(join_tb0.ts), sum(join_tb0.c1), first(join_tb0.c7), last(join_tb1.c3) from {tb1} , {tb2} where join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select count(join_tb1.c3), last(join_tb1.c3) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts or join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select count(join_tb3.*) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select first(join_tb1.*) from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 or join_tb0.c7 = true;"
        )
        tdSql.error(
            f"select join_tb3.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true;"
        )
        tdSql.query(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.ts and join_tb1.ts = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.c7 = join_tb0.c1;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts > join_tb0.ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts <> join_tb0.ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb2} where join_tb1.ts != join_tb0.ts and join_tb1.ts > 100000;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts and join_tb1.ts >= 100000;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts order by ts;"
        )
        tdSql.error(
            f"select join_tb1.* from {tb1} , {tb1} where join_tb1.ts = join_tb1.ts order by join_tb1.c7;"
        )
        tdSql.error(f"select * from join_tb0, join_tb1")
        tdSql.error(f"select last_row(*) from join_tb0, join_tb1")
        tdSql.error(f"select last_row(*) from {tb1}, {tb2} where join_tb1.ts < now")
        tdSql.error(
            f"select last_row(*) from {tb1}, {tb2} where join_tb1.ts = join_tb2.ts"
        )

        tdLog.info(
            f"==================================super table join =============================="
        )
        # select duplicate columns
        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )

        val = rowNum + rowNum
        tdLog.info(f"{val}")
        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(val)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.ts = 100000;"
        )

        val = 2
        tdSql.checkRows(val)

        tdSql.query(f"select join_mt1.* from join_mt1")

        tdLog.info(f"{tdSql.getRows()})")
        tdSql.checkRows(2000)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1;"
        )

        val = 2000
        tdSql.checkRows(val)

        tdSql.query(
            f"select join_mt0.* from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99;"
        )

        val = 20
        tdSql.checkRows(val)

        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99;"
        )

        val = 20
        tdSql.checkData(0, 0, val)
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )

        val = 2
        tdSql.checkData(0, 0, val)

        # agg
        tdSql.query(
            f"select sum(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )

        val = 198
        tdSql.checkData(0, 0, val)

        tdSql.query(
            f"select sum(join_mt0.c1)+sum(join_mt0.c1) from join_mt0, join_mt1 where join_mt0.ts = join_mt1.ts and join_mt0.t1=join_mt1.t1 and join_mt0.c2=99 and join_mt1.ts=100999;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 396.000000000)

        # first/last
        tdSql.query(
            f"select count(join_mt0.c1), sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts and join_mt0.t1=1 interval(10a) order by _wstart asc;"
        )

        val = 100
        tdSql.checkRows(val)

        val = 10
        tdSql.checkData(0, 0, val)

        val = 45.000000000
        tdLog.info(f"{tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, val)

        # order by first/last
        tdSql.query(
            f"select count(join_mt0.c1), sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1 and join_mt0.ts=join_mt1.ts and join_mt0.t1=1 interval(10a) order by _wstart desc;"
        )

        val = 100
        tdSql.checkRows(val)

        tdLog.info(f"================>TD-5600")
        tdSql.query(
            f"select first(join_tb0.c8),first(join_tb0.c9) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb1.ts>=100000 interval(1s) fill(linear);"
        )

        # ===============================================================
        tdSql.query(
            f"select first(join_tb0.c8),first(join_tb0.c9) from join_tb1 , join_tb0 where join_tb1.ts = join_tb0.ts and join_tb1.ts <= 100002 and join_tb0.c7 = true"
        )

        # ====================group by=========================================
        tdLog.info(f'=================>"group by not supported"')

        # ======================limit offset===================================
        # tag values not int
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts and join_mt0.t2=join_mt1.t2;"
        )

        # tag type not identical
        tdSql.query(
            f"select count(*) from join_mt0, join_mt1 where join_mt1.t2 = join_mt0.t1 and join_mt1.ts=join_mt0.ts;"
        )

        # table/super table join
        tdSql.query(
            f"select count(join_mt0.c1) from join_mt0, join_tb1 where join_mt0.ts=join_tb1.ts"
        )

        # multi-condition

        # self join
        tdSql.error(
            f"select count(join_mt0.c1), count(join_mt0.c2) from join_mt0, join_mt0 where join_mt0.ts=join_mt0.ts and join_mt0.t1=join_mt0.t1;"
        )

        # missing ts equals
        tdSql.error(
            f"select sum(join_mt1.c2) from join_mt0, join_mt1 where join_mt0.t1=join_mt1.t1;"
        )

        # missing tag equals
        tdSql.query(
            f"select count(join_mt1.c3) from join_mt0, join_mt1 where join_mt0.ts=join_mt1.ts;"
        )

        # tag values are identical error
        tdSql.execute(f"create table m1(ts timestamp, k int) tags(a int);")
        tdSql.execute(f"create table m2(ts timestamp, k int) tags(a int);")

        tdSql.execute(f"create table tm1 using m1 tags(1);")
        tdSql.execute(f"create table tm2 using m1 tags(1);")

        tdSql.execute(
            f"insert into tm1 using m1 tags(1) values(1000000, 1)(2000000, 2);"
        )
        tdSql.execute(
            f"insert into tm2 using m1 tags(1) values(1000000, 1)(2000000, 2);"
        )

        tdSql.execute(
            f"insert into um1 using m2 tags(1) values(1000001, 10)(2000000, 20);"
        )
        tdSql.execute(
            f"insert into um2 using m2 tags(9) values(1000001, 10)(2000000, 20);"
        )

        tdSql.query(f"select count(*) from m1,m2 where m1.a=m2.a and m1.ts=m2.ts;")

        tdLog.info(
            f"====> empty table/empty super-table join test, add for no result join test"
        )
        tdSql.execute(f"create database ux1;")
        tdSql.execute(f"use ux1;")
        tdSql.execute(
            f"create table m1(ts timestamp, k int) tags(a binary(12), b int);"
        )
        tdSql.execute(f"create table tm0 using m1 tags('abc', 1);")
        tdSql.execute(
            f"create table m2(ts timestamp, k int) tags(a int, b binary(12));"
        )

        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.checkRows(1)

        tdSql.execute(f"create table tm2 using m2 tags(2, 'abc');")
        tdSql.query(f"select count(*) from tm0, tm2 where tm0.ts=tm2.ts;")
        tdSql.checkRows(1)

        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.checkRows(1)

        tdSql.execute(f"drop table tm2;")
        tdSql.query(f"select count(*) from m1, m2 where m1.ts=m2.ts and m1.b=m2.a;")
        tdSql.execute(f"drop database ux1;")
        print("\n")
        print("do sim join ........................... [passed]")


    #
    # ---------------- main ---------------------
    #
    def do_sim_join2(self):
        dbPrefix = "db"
        tbPrefix1 = "tba"
        tbPrefix2 = "tbb"
        mtPrefix = "stb"
        tbNum = 10
        rowNum = 2

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt1 = mtPrefix + str(i)
        i = 1
        mt2 = mtPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt1} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )
        tdSql.execute(
            f"create table {mt2} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )

        tdLog.info(f"====== start create child tables and insert data")
        i = 0
        while i < tbNum:
            tb = tbPrefix1 + str(i)
            tdSql.execute(
                f"create table {tb} using {mt1} tags( {i} , 'aaaaaaaaaaaaaaaaaaaaaaaaaaa')"
            )

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc

                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1
            i = i + 1

        tdLog.info(f"=============== step2")
        i = 0
        while i < tbNum:
            tb = tbPrefix2 + str(i)
            tdSql.execute(
                f"create table {tb} using {mt2} tags( {i} , 'aaaaaaaaaaaaaaaaaaaaaaaaaaa')"
            )

            x = 0
            while x < rowNum:
                cc = x * 60000
                ms = 1601481600000 + cc

                tdSql.execute(f"insert into {tb} values ({ms} , {x} )")
                x = x + 1

            i = i + 1

        tdLog.info("data inserted")
        tdSql.query(f"select * from tba0 t1, tbb0 t2 where t1.ts=t2.ts;")
        tdSql.checkRows(2)

        tdSql.query(
            f"select * from stb0 t1, stb1 t2 where t1.ts=t2.ts and t1.tag2=t2.tag2;"
        )
        tdSql.checkRows(200)
        print("do sim join2 .......................... [passed]")

    #
    # ---------------- system-test ---------------------
    #
    def __query_condition(self,tbname):
        query_condition = []
        for char_col in CHAR_COL:
            query_condition.extend(
                (
                    f"{tbname}.{char_col}",
                    # f"upper( {tbname}.{char_col} )",
                )
            )
            query_condition.extend( f"cast( {tbname}.{un_char_col} as binary(16) ) " for un_char_col in NUM_COL)
        for num_col in NUM_COL:
            query_condition.extend(
                (
                    f"sin( {tbname}.{num_col} )",
                )
            )
            query_condition.extend( f"{tbname}.{num_col} + {tbname}.{num_col_1} " for num_col_1 in NUM_COL )

        query_condition.append(''' "test1234!@#$%^&*():'><?/.,][}{" ''')

        return query_condition

    def __join_condition(self, tb_list, filter=PRIMARY_COL, INNER=False, alias_tb1="tb1", alias_tb2="tb2"):
        table_reference = tb_list[0]
        join_condition = table_reference
        join = "inner join" if INNER else "join"
        for i in range(len(tb_list[1:])):
            join_condition += f" as {alias_tb1} {join} {tb_list[i+1]} as {alias_tb2} on {alias_tb1}.{filter}={alias_tb2}.{filter}"

        return join_condition

    def __where_condition(self, col=None, tbname=None, query_conditon=None):
        if query_conditon and isinstance(query_conditon, str):
            if query_conditon.startswith("count"):
                query_conditon = query_conditon[6:-1]
            elif query_conditon.startswith("max"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("sum"):
                query_conditon = query_conditon[4:-1]
            elif query_conditon.startswith("min"):
                query_conditon = query_conditon[4:-1]

        if query_conditon:
            return f" where {query_conditon} is not null"
        if col in NUM_COL:
            return f" where abs( {tbname}.{col} ) >= 0"
        if col in CHAR_COL:
            return f" where lower( {tbname}.{col} ) like 'bina%' or lower( {tbname}.{col} ) like '_cha%' "
        if col in BOOLEAN_COL:
            return f" where {tbname}.{col} in (false, true)  "
        if col in TS_TYPE_COL or col in PRIMARY_COL:
            return f" where cast( {tbname}.{col} as binary(16) ) is not null "

        return ""

    def __group_condition(self, col, having = None):
        if isinstance(col, str):
            if col.startswith("count"):
                col = col[6:-1]
            elif col.startswith("max"):
                col = col[4:-1]
            elif col.startswith("sum"):
                col = col[4:-1]
            elif col.startswith("min"):
                col = col[4:-1]
        return f" group by {col} having {having}" if having else f" group by {col} "

    def __gen_sql(self, select_clause, from_clause, where_condition="", group_condition=""):
        if isinstance(select_clause, str) and "on" not in from_clause and select_clause.split(".")[0] != from_clause.split(".")[0]:
            return
        return f"select {select_clause} from {from_clause} {where_condition} {group_condition}"

    @property
    def __join_tblist(self, dbname=DBNAME):
        return [
            # ["ct1", "ct2"],
            [f"{dbname}.ct1", f"{dbname}.ct4"],
            [f"{dbname}.ct1", f"{dbname}.nt1"],
            # ["ct2", "ct4"],
            # ["ct2", "nt1"],
            # ["ct4", "nt1"],
            # ["ct1", "ct2", "ct4"],
            # ["ct1", "ct2", "nt1"],
            # ["ct1", "ct4", "nt1"],
            # ["ct2", "ct4", "nt1"],
            # ["ct1", "ct2", "ct4", "nt1"],
        ]

    @property
    def __sqls_list(self):
        sqls = []
        __join_tblist = self.__join_tblist
        for join_tblist in __join_tblist:
            alias_tb = "tb1"
            # for join_tb in join_tblist:
            select_claus_list = self.__query_condition(alias_tb)
            for select_claus in select_claus_list:
                group_claus = self.__group_condition( col=select_claus)
                where_claus = self.__where_condition( query_conditon=select_claus )
                having_claus = self.__group_condition( col=select_claus, having=f"{select_claus} is not null" )
                sqls.extend(
                    (
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus, having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), where_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), group_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb), having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, alias_tb1=alias_tb)),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, group_claus),
                        self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, having_claus),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), where_claus, ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), having_claus ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb), group_claus ),
                        # self.__gen_sql(select_claus, self.__join_condition(join_tblist, INNER=True, alias_tb1=alias_tb) ),
                    )
                )
        return list(filter(None, sqls))

    def __join_check(self,):
        tdLog.printNoPrefix("==========current sql condition check , must return query ok==========")
        for i in range(len(self.__sqls_list)):
            tdSql.query(self.__sqls_list[i])
            # if i % 10 == 0 :
            #     tdLog.success(f"{i} sql is already executed success !")

    def __join_check_old(self, tblist, checkrows, join_flag=True):
        query_conditions = self.__query_condition(tblist[0])
        join_condition = self.__join_condition(tb_list=tblist) if join_flag else " "
        for condition in query_conditions:
            where_condition =  self.__where_condition(col=condition, tbname=tblist[0])
            group_having = self.__group_condition(col=condition, having=f"{condition} is not null " )
            group_no_having= self.__group_condition(col=condition )
            groups = ["", group_having, group_no_having]
            for group_condition in groups:
                if where_condition:
                    sql = f" select {condition} from {tblist[0]},{tblist[1]} where {join_condition} and {where_condition} {group_condition} "
                else:
                    sql = f" select {condition} from {tblist[0]},{tblist[1]} where {join_condition}  {group_condition} "

                if not join_flag :
                    tdSql.error(sql=sql)
                    break
                if len(tblist) == 2:
                    if "ct1" in tblist or "nt1" in tblist:
                        self.__join_current(sql, checkrows)
                    elif where_condition or "not null" in group_condition:
                        self.__join_current(sql, checkrows + 2 )
                    elif group_condition:
                        self.__join_current(sql, checkrows + 3 )
                    else:
                        self.__join_current(sql, checkrows + 5 )
                if len(tblist) > 2 or len(tblist) < 1:
                    tdSql.error(sql=sql)

    def __join_current(self, sql, checkrows):
        tdSql.query(sql=sql)
        # tdSql.checkRows(checkrows)

    def __test_error(self, dbname=DBNAME):
        # sourcery skip: extract-duplicate-method, move-assign-in-block
        tdLog.printNoPrefix("==========err sql condition check , must return error==========")
        err_list_1 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4"]
        err_list_2 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.nt1"]
        err_list_3 = [f"{dbname}.ct1", f"{dbname}.ct4", f"{dbname}.nt1"]
        err_list_4 = [f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]
        err_list_5 = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]
        self.__join_check_old(err_list_1, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_1} over==========")
        self.__join_check_old(err_list_2, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_2} over==========")
        self.__join_check_old(err_list_3, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_3} over==========")
        self.__join_check_old(err_list_4, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_4} over==========")
        self.__join_check_old(err_list_5, -1)
        tdLog.printNoPrefix(f"==========err sql condition check in {err_list_5} over==========")
        self.__join_check_old(["ct2", "ct4"], -1, join_flag=False)
        tdLog.printNoPrefix("==========err sql condition check in has no join condition over==========")

        tdSql.error( f"select c1, c2 from {dbname}.ct2, {dbname}.ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{INT_COL}=ct4.{INT_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{TS_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct2.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{TS_COL}" )
        tdSql.error( f"select ct2.c1, ct1.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL}" )
        tdSql.error( f"select ct2.c1, ct4.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and c1 is not null " )
        tdSql.error( f"select ct2.c1, ct4.c2 from {dbname}.ct2 as ct2, {dbname}.ct4 as ct4 where ct2.{PRIMARY_COL}=ct4.{PRIMARY_COL} and ct1.c1 is not null " )

        tbname = [f"{dbname}.ct1", f"{dbname}.ct2", f"{dbname}.ct4", f"{dbname}.nt1"]

        # for tb in tbname:
        #     for errsql in self.__join_err_check(tb):
        #         tdSql.error(sql=errsql)
        #     tdLog.printNoPrefix(f"==========err sql condition check in {tb} over==========")

    def all_test(self):
        self.__join_check()
        self.__test_error()

    def __create_tb(self, stb="stb1", ctb_num=20, ntbnum=1, dbname=DBNAME):
        create_stb_sql = f'''create table {dbname}.{stb}(
                ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
            ) tags ({INT_TAG} int)
            '''
        for i in range(ntbnum):

            create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                    ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                )
                '''
        tdSql.execute(create_stb_sql)
        tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __data_set(self, rows):
        data_set = DataSet()

        for i in range(rows):
            data_set.ts_data.append(NOW + 1 * (rows - i))
            data_set.int_data.append(rows - i)
            data_set.bint_data.append(11111 * (rows - i))
            data_set.sint_data.append(111 * (rows - i) % 32767)
            data_set.tint_data.append(11 * (rows - i) % 127)
            data_set.int_un_data.append(rows - i)
            data_set.bint_un_data.append(11111 * (rows - i))
            data_set.sint_un_data.append(111 * (rows - i) % 32767)
            data_set.tint_un_data.append(11 * (rows - i) % 127)
            data_set.float_data.append(1.11 * (rows - i))
            data_set.double_data.append(1100.0011 * (rows - i))
            data_set.bool_data.append((rows - i) % 2)
            data_set.binary_data.append(f'binary{(rows - i)}')
            data_set.nchar_data.append(f'nchar_测试_{(rows - i)}')

        return data_set

    def __insert_data(self, dbname=DBNAME):
        tdLog.printNoPrefix("==========step: start inser data into tables now.....")
        data = self.__data_set(rows=self.rows)

        # now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        null_data = '''null, null, null, null, null, null, null, null, null, null, null, null, null, null'''
        zero_data = "0, 0, 0, 0, 0, 0, 0, 'binary_0', 'nchar_0', 0, 0, 0, 0, 0"

        for i in range(self.rows):
            row_data = f'''
                {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.tint_un_data[i]},
                {data.sint_un_data[i]}, {data.int_un_data[i]}, {data.bint_un_data[i]}
            '''
            neg_row_data = f'''
                {-1 * data.int_data[i]}, {-1 * data.bint_data[i]}, {-1 * data.sint_data[i]}, {-1 * data.tint_data[i]}, {-1 * data.float_data[i]}, {-1 * data.double_data[i]},
                {data.bool_data[i]}, '{data.binary_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {1 * data.tint_un_data[i]},
                {1 * data.sint_un_data[i]}, {1 * data.int_un_data[i]}, {1 * data.bint_un_data[i]}
            '''

            tdSql.execute( f"insert into {dbname}.ct1 values ( {NOW - i * TIME_STEP}, {row_data} )" )
            tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - i * int(TIME_STEP * 0.6)}, {neg_row_data} )" )
            tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - i * int(TIME_STEP * 0.8) }, {row_data} )" )
            tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )" )

        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW + int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.6)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct2 values ( {NOW - self.rows * int(TIME_STEP * 0.29) }, {null_data} )" )

        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW + int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 0.8)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.ct4 values ( {NOW - self.rows * int(TIME_STEP * 0.39)}, {null_data} )" )

        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW + int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - (self.rows + 1) * int(TIME_STEP * 1.2)}, {null_data} )" )
        tdSql.execute( f"insert into {dbname}.nt1 values ( {NOW - self.rows * int(TIME_STEP * 0.59)}, {null_data} )" )

    def join_semantic_test(self, dbname=DBNAME):
        tdSql.query("select ct1.c_int from db.ct1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 semi join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 anti join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 outer join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 asof join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 window join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        
        tdSql.query("select ct1.c_int from db.ct1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left semi join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left anti join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(0)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left outer join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left asof join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 left window join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right semi join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right anti join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(0)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right outer join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right asof join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 right window join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        
        tdSql.query("select ct1.c_int from db.ct1 as ct1 full join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.checkRows(self.rows)

        tdSql.query("select ct1.c_int from db.ct1 as ct1 full join db1.ct1 as cy1 on ct1.ts=cy1.ts join db1.ct1 as cy2 on ct1.ts=cy2.ts")
        tdSql.checkRows(self.rows)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 full semi join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 full anti join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 full outer join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.query("select * from db.ct1 join db.ct2 join db.ct3 on ct2.ts=ct3.ts on ct1.ts=ct2.ts")
        tdSql.checkRows(0)
        tdSql.execute(f'create table db.ct1_2 using db.stb1 tags ( 102 )')
        tdSql.execute(f'create table db.ct1_3 using db.stb1 tags ( 103 )')
        tdSql.execute(f'insert into db.ct1_2 (select * from db.ct1)')
        tdSql.execute(f'insert into db.ct1_3 (select * from db.ct1)')
        tdSql.query("select * from db.ct1 join db.ct1_2 join db.ct1_3 on ct1_2.ts=ct1_3.ts on ct1.ts=ct1_2.ts")
        tdSql.checkRows(self.rows)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 full asof join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        tdSql.error("select ct1.c_int from db.ct1 as ct1 full window join db1.ct1 as cy1 on ct1.ts=cy1.ts", TSDB_CODE_PAR_SYNTAX_ERROR)
        
        tdSql.query("select ct1.c_int from db.ct1 as ct1 left join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.ct1 as ct1 right join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        
        tdSql.execute("drop table db.ct1_2")
        tdSql.execute("drop table db.ct1_3")
        
    def ts5863(self, dbname=DBNAME):
        tdSql.execute(f"CREATE STABLE {dbname}.`st_quality` (`ts` TIMESTAMP, `quality` INT, `val` NCHAR(64), `rts` TIMESTAMP) \
            TAGS (`cx` VARCHAR(10), `gyd` VARCHAR(10), `gx` VARCHAR(10), `lx` VARCHAR(10)) SMA(`ts`,`quality`,`val`)")
        
        tdSql.execute(f"create table {dbname}.st_q1 using {dbname}.st_quality tags ('cx', 'gyd', 'gx1', 'lx1')")
        
        sql1 = f"select t.val as batch_no, a.tbname as sample_point_code, min(cast(a.val as double)) as `min`, \
            max(cast(a.val as double)) as `max`, avg(cast(a.val as double)) as `avg` from {dbname}.st_quality t \
            left join {dbname}.st_quality a on a.ts=t.ts and a.cx=t.cx and a.gyd=t.gyd \
            where t.ts >= 1734574900000 and t.ts <=  1734575000000   \
            and t.tbname = 'st_q1'   \
            and a.tbname in ('st_q2', 'st_q3') \
            group by t.val, a.tbname"
        tdSql.query(sql1)
        tdSql.checkRows(0)
        
        tdSql.execute(f"create table {dbname}.st_q2 using {dbname}.st_quality tags ('cx2', 'gyd2', 'gx2', 'lx2')")
        tdSql.execute(f"create table {dbname}.st_q3 using {dbname}.st_quality tags ('cx', 'gyd', 'gx3', 'lx3')")
        tdSql.execute(f"create table {dbname}.st_q4 using {dbname}.st_quality tags ('cx', 'gyd', 'gx4', 'lx4')")
        
        tdSql.query(sql1)
        tdSql.checkRows(0)
        
        tdSql.execute(f"insert into {dbname}.st_q1 values (1734574900000, 1, '1', 1734574900000)")
        tdSql.query(sql1)
        tdSql.checkRows(0)
        tdSql.execute(f"insert into {dbname}.st_q2 values (1734574900000, 1, '1', 1734574900000)")
        tdSql.query(sql1)
        tdSql.checkRows(0)
        tdSql.execute(f"insert into {dbname}.st_q3 values (1734574900000, 1, '1', 1734574900000)")
        tdSql.query(sql1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'st_q3')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        
        tdSql.execute(f"insert into {dbname}.st_q1 values (1734574900001, 2, '2', 1734574900000)")
        tdSql.execute(f"insert into {dbname}.st_q3 values (1734574900001, 2, '2', 1734574900000)")
        sql2 = f"select t.val as batch_no, a.tbname as sample_point_code, min(cast(a.val as double)) as `min`, \
            max(cast(a.val as double)) as `max`, avg(cast(a.val as double)) as `avg` from {dbname}.st_quality t \
            left join {dbname}.st_quality a on a.ts=t.ts and a.cx=t.cx and a.gyd=t.gyd \
            where t.ts >= 1734574900000 and t.ts <=  1734575000000   \
            and t.tbname = 'st_q1'   \
            and a.tbname in ('st_q2', 'st_q3') \
            group by t.val, a.tbname order by batch_no"
        tdSql.query(sql2)
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'st_q3')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 'st_q3')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        sql3 = f"select  min(cast(a.val as double)) as `min`  from {dbname}.st_quality t left join {dbname}.st_quality \
            a on a.ts=t.ts and a.cx=t.cx where   t.tbname = 'st_q3' and a.tbname in ('st_q3', 'st_q2')"
        tdSql.execute(f"insert into {dbname}.st_q1 values (1734574900002, 2, '2', 1734574900000)")
        tdSql.execute(f"insert into {dbname}.st_q4 values (1734574900002, 2, '2', 1734574900000)")
        tdSql.execute(f"insert into {dbname}.st_q1 values (1734574900003, 3, '3', 1734574900000)")
        tdSql.execute(f"insert into {dbname}.st_q3 values (1734574900003, 3, '3', 1734574900000)") 
        tdSql.query(sql3)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        sql3 = f"select  min(cast(a.val as double)) as `min`, max(cast(a.val as double)) as `max`, avg(cast(a.val as double)) as `avg`  \
            from {dbname}.st_quality t left join {dbname}.st_quality a \
            on a.ts=t.ts and a.cx=t.cx where   t.tbname = 'st_q3' and a.tbname in ('st_q3', 'st_q2')"
        tdSql.query(sql3)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 3) 
        tdSql.checkData(0, 2, 2)
        tdSql.query(sql1)
        tdSql.checkRows(3)
        tdSql.query(sql2)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'st_q3')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, 'st_q3')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, 'st_q3')
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, 3)     
        
    def do_system_test_join(self):
        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb(dbname=DBNAME)

        tdLog.printNoPrefix("==========step2:insert data")
        self.rows = 10
        self.__insert_data(dbname=DBNAME)

        tdLog.printNoPrefix("==========step3:all check")
        tdSql.query(f"select count(*) from {DBNAME}.ct1")
        tdSql.checkData(0, 0, self.rows)
        self.all_test()

        tdLog.printNoPrefix("==========step4:cross db check")
        dbname1 = "db1"
        tdSql.execute(f"create database {dbname1} duration 172800m")
        tdSql.execute(f"use {dbname1}")
        self.__create_tb(dbname=dbname1)
        self.__insert_data(dbname=dbname1)
        
        self.join_semantic_test({dbname1})

        tdSql.query("select ct1.c_int from db.ct1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows)
        tdSql.query("select ct1.c_int from db.stb1 as ct1 join db1.ct1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows + int(self.rows * 0.6 //3)+ int(self.rows * 0.8 // 4))
        tdSql.query("select ct1.c_int from db.nt1 as ct1 join db1.nt1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(self.rows + 3)
        tdSql.query("select ct1.c_int from db.stb1 as ct1 join db1.stb1 as cy1 on ct1.ts=cy1.ts")
        tdSql.checkRows(50)

        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        self.all_test()
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        tdSql.execute(f"flush database {DBNAME}")
        tdSql.execute(f"flush database {dbname1}")
        # tdDnodes.stop(1)
        # tdDnodes.start(1)

        tdSql.execute("use db")
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        tdSql.query("select count(*) from db1.ct1")
        tdSql.checkData(0, 0, self.rows)

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()
        tdSql.query("select count(*) from db.ct1")
        tdSql.checkData(0, 0, self.rows)
        self.ts5863(dbname=dbname1)
        print("do system-test join ................... [passed]")


    #
    # ---------------- main ---------------------
    #
    def test_join(self):
        """Join basic

        1. Join with inner/left/right/outer/asof/semi/anti/full
        2. Join with tb1.ts = tb2.ts
        3. Join with tb1.int = tb2.int limit 
        4. Join with tb1.int = tb2.int and ts filter
        5. Join with multiple tables
        6. Join with group by
        7. Join with having condition
        8. Join error cases
        9. Cross database join test
        10. Join semantic test
        11. Verify bug TS-5863

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/parser/join.sim
            - 2025-10-21 Alex Duan Migrated from 25-JoinQueries/test_join2.py
            - 2025-10-21 Alex Duan Migrated from uncatalog/system-test/2-query/test_join.py

        """
        self.do_sim_join()
        self.do_sim_join2()
        self.do_system_test_join()