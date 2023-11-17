from datetime import datetime
import time

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

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

INT_TAG = "t_int"

TAG_COL = [INT_TAG]

## insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
DB1     = "db1"
DB2     = "db2"
DB3     = "db3"
DB4     = "db4"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    @property
    def create_databases_sql_err(self):
        return [
            # check grammar
            "create database db1 retentions",
            "create database db1 retentions 1s:1d",
            "create database db1 retentions 1s:1d,2s:2d",
            "create database db1 retentions 1s:1d,2s:2d,3s:3d",
            "create database db1 retentions 1s:1d,2s:2d,3s:3d,4s:4d",
            "create database db1 retentions -:1d,2s:2d,3s:3d,4s:4d",
            "create database db1 retentions --:1d",
            "create database db1 retentions -:-:1d",
            "create database db1 retentions 1d:-",
            "create database db1 retentions -:-",
            "create database db1 retentions +:1d",
            "create database db1 retentions :1d",
            "create database db1 retentions -:1d,-:2d",
            "create database db1 retentions -:1d,-:2d,-:3d",
            "create database db1 retentions -:1d,1s:-",
            "create database db1 retentions -:1d,15s:2d,-:3d",

            # check unit
            "create database db1 retentions -:1d,1b:1d",
            "create database db1 retentions -:1d,1u:1d",
            "create database db1 retentions -:1d,1a:1d",
            "create database db1 retentions -:1d,1n:1d",
            "create database db1 retentions -:1d,1y:1d",
            "create database db1 retentions -:1d,1s:86400s",
            "create database db1 retentions -:1d,1s:86400000a",
            "create database db1 retentions -:1d,1s:86400000000u",
            "create database db1 retentions -:1d,1s:86400000000000b",
            "create database db1 retentions -:1s,1s:2s",
            "create database db1 retentions -:1d,1s:1w",
            "create database db1 retentions -:1d,1s:1n",
            "create database db1 retentions -:1d,1s:1y",
            
            # check value range
            "create database db3 retentions -:-1d",
            "create database db3 retentions -:0d",
            "create database db3 retentions -:1439m",
            "create database db3 retentions -:365001d",
            "create database db3 retentions -:8760001h",
            "create database db3 retentions -:525600001m",
            "create database db3 retentions -:106581d precision 'ns'",
            "create database db3 retentions -:2557921h precision 'ns'",
            "create database db3 retentions -:153475201m precision 'ns'",
            # check relationships
            "create database db5 retentions -:1440m,1441m:1440m,2d:3d",
            "create database db5 retentions -:1d,2m:1d,1s:2d",
            "create database db5 retentions -:1440m,1s:2880m,2s:2879m",
            "create database db5 retentions -:1d,2s:2d,2s:3d",
            "create database db5 retentions -:1d,3s:2d,2s:3d",
            "create database db1 retentions -:1d,2s:3d,3s:2d",
            "create database db1 retentions -:1d,2s:3d,1s:2d",

        ]

    @property
    def create_databases_sql_current(self):
        return [
            f"create database {DB1} retentions -:1d",
            f"create database {DB2} retentions -:1d,2m:2d,3h:3d",
        ]

    @property
    def alter_database_sql(self):
        return [
            "alter database db1 retentions -:99d",
            "alter database db2 retentions -:97d,98h:98d,99h:99d,",
        ]

    @property
    def create_stable_sql_err(self, dbname=DB2):
        return [
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(ceil) watermark 1s max_delay 1m",
            f"create stable {dbname}.stb12 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(count) watermark  1min",
            f"create stable {dbname}.stb13 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay -1s",
            f"create stable {dbname}.stb14 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark -1m",
            f"create stable {dbname}.stb15 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) watermark 1m ",
            f"create stable {dbname}.stb16 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) max_delay 1m ",
            f"create stable {dbname}.stb21 ({PRIMARY_COL} timestamp, {INT_COL} int, {BINARY_COL} binary(16)) tags (tag1 int) rollup(avg) watermark 1s",
            f"create stable {dbname}.stb22 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) tags (tag1 int) rollup(avg) max_delay 1m",
            f"create table  {dbname}.ntb_1 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) rollup(avg) watermark 1s max_delay 1s",
            f"create table  {dbname}.ntb_2 ({PRIMARY_COL} timestamp, {INT_COL} int) " ,
            f"create stable {dbname}.stb23 ({PRIMARY_COL} timestamp, {INT_COL} int, {NCHAR_COL} nchar(16)) tags (tag1 int) " ,
            f"create stable {dbname}.stb24 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) " ,
            f"create stable {dbname}.stb25 ({PRIMARY_COL} timestamp, {INT_COL} int) " ,
            f"create stable {dbname}.stb26 ({PRIMARY_COL} timestamp, {INT_COL} int, {BINARY_COL} nchar(16)) " ,
            # only float/double allowd for avg/sum
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(avg)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BINT_COL} bigint) tags (tag1 int) rollup(avg)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BOOL_COL} bool) tags (tag1 int) rollup(avg)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BINARY_COL} binary(10)) tags (tag1 int) rollup(avg)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(sum)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BINT_COL} bigint) tags (tag1 int) rollup(sum)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BOOL_COL} bool) tags (tag1 int) rollup(sum)",
            f"create stable {dbname}.stb11 ({PRIMARY_COL} timestamp, {BINARY_COL} binary(10)) tags (tag1 int) rollup(sum)",


            # watermark, max_delay: [0, 900000], [ms, s, m, ?]
            f"create stable stb17 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 1u",
            f"create stable stb18 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 1b",
            f"create stable stb19 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 900001ms",
            f"create stable stb20 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 16m",
            f"create stable stb27 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 901s",
            f"create stable stb28 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 1h",
            f"create stable stb29 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) max_delay 0.2h",
            f"create stable stb30 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 0.002d",

        ]

    @property
    def create_stable_sql_current(self):
        return [
            f"create stable stb1 ({PRIMARY_COL} timestamp, {FLOAT_COL} float) tags (tag1 int) rollup(avg)",
            f"create stable stb2 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(min) watermark 5s max_delay 1m",
            f"create stable stb3 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(max) watermark 5s max_delay 1m",
            f"create stable stb4 ({PRIMARY_COL} timestamp, {DOUBLE_COL} double) tags (tag1 int) rollup(sum) watermark 5s max_delay 1m",
            f"create stable stb5 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(last) watermark 5s max_delay 1m",
            f"create stable stb6 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m",
            f"create stable stb7 ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) rollup(first) watermark 5s max_delay 1m sma({INT_COL})",
        ]

    def test_create_stb(self, db=DB2):
        tdSql.execute(f"use {db}")
        for err_sql in self.create_stable_sql_err:
            tdSql.error(err_sql)
        for cur_sql in self.create_stable_sql_current:
            tdSql.execute(cur_sql)
        tdSql.query("show stables")
        # assert "rollup" in tdSql.description
        tdSql.checkRows(len(self.create_stable_sql_current))

        tdSql.execute("use db")  # because db is a noraml database, not a rollup database, should not be able to create a rollup stable
        tdSql.error(f"create stable db.nor_db_rollup_stb ({PRIMARY_COL} timestamp, {INT_COL} int) tags (tag1 int) watermark 5s max_delay 1m")


    def test_create_databases(self):
        for err_sql in self.create_databases_sql_err:
            tdSql.error(err_sql)
        index = 0
        for cur_sql in self.create_databases_sql_current:
            tdSql.execute(cur_sql)
            if(index == 0):
                tdSql.query(f"show create database {DB1}")
            else:
                tdSql.query(f"show create database {DB2}")
            tdSql.checkEqual(len(tdSql.queryResult),1)
            tdLog.info("%s" % (tdSql.queryResult[0][1]))
            tdSql.checkEqual(tdSql.queryResult[0][1].find("RETENTIONS -:") > 0, True)
            index += 1
        for alter_sql in self.alter_database_sql:
            tdSql.error(alter_sql)

    def all_test(self):
        self.test_create_databases()
        self.test_create_stb()

    def __create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1, rsma=False, dbname=DBNAME, rsma_type="sum"):
        tdLog.printNoPrefix("==========step: create table")
        if rsma:
            if rsma_type.lower().strip() in ("last", "first"):
                create_stb_sql = f'''create table {dbname}.{stb}(
                        ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                        {FLOAT_COL} float, {DOUBLE_COL} double, {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                        {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned, {BINARY_COL} binary(16)
                    ) tags ({INT_TAG} int) rollup({rsma_type}) watermark 5s,5s max_delay 5s,5s
                    '''
            elif rsma_type.lower().strip() in ("sum", "avg"):
                create_stb_sql = f'''create table {dbname}.{stb}(
                        ts timestamp, {DOUBLE_COL} double, {DOUBLE_COL}_1 double, {DOUBLE_COL}_2 double, {DOUBLE_COL}_3 double, 
                        {FLOAT_COL} float, {DOUBLE_COL}_4 double, {FLOAT_COL}_1 float, {FLOAT_COL}_2 float, {FLOAT_COL}_3 float, 
                        {DOUBLE_COL}_5 double) tags ({INT_TAG} int) rollup({rsma_type}) watermark 5s,5s max_delay 5s,5s
                    '''
            else:
                create_stb_sql = f'''create table {dbname}.{stb}(
                        ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                        {FLOAT_COL} float, {DOUBLE_COL} double, {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                        {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                    ) tags ({INT_TAG} int) rollup({rsma_type}) watermark 5s,5s max_delay 5s,5s
                    '''
            tdSql.execute(create_stb_sql)
        else:
            create_stb_sql = f'''create table {dbname}.{stb}(
                    ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                    {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                    {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                    {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                    {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                ) tags ({INT_TAG} int)
                '''
            tdSql.execute(create_stb_sql)

            for i in range(ntbnum):
                create_ntb_sql = f'''create table {dbname}.nt{i+1}(
                        ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
                        {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
                        {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp,
                        {TINT_UN_COL} tinyint unsigned, {SINT_UN_COL} smallint unsigned,
                        {INT_UN_COL} int unsigned, {BINT_UN_COL} bigint unsigned
                    )
                    '''
                tdSql.execute(create_ntb_sql)

        for i in range(ctb_num):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.{stb} tags ( {i+1} )')

    def __insert_data(self, rows, ctb_num=20, dbname=DBNAME, rsma=False, rsma_type="sum"):
        tdLog.printNoPrefix("==========step: start insert data into tables now.....")
        # from ...pytest.util.common import DataSet
        data = DataSet()
        data.get_order_set(rows)

        for i in range(rows):
            if rsma:
                if rsma_type.lower().strip() in ("last", "first"):
                    row_data = f'''
                        {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                        {data.utint_data[i]}, {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}, '{data.vchar_data[i]}'
                    '''
                elif rsma_type.lower().strip() in ("sum", "avg"):
                    row_data = f'''
                        {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                        {data.utint_data[i]}, {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
                    '''
                else:
                    row_data = f'''
                        {data.double_data[i]}, {data.double_data[i]}, {data.double_data[i]}, {data.double_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                        {data.float_data[i]}, {data.float_data[i]}, {data.float_data[i]}, {data.double_data[i]}
                    '''
            else:
                row_data = f'''
                    {data.int_data[i]}, {data.bint_data[i]}, {data.sint_data[i]}, {data.tint_data[i]}, {data.float_data[i]}, {data.double_data[i]},
                    {data.bool_data[i]}, '{data.vchar_data[i]}', '{data.nchar_data[i]}', {data.ts_data[i]}, {data.utint_data[i]},
                    {data.usint_data[i]}, {data.uint_data[i]}, {data.ubint_data[i]}
                '''
                tdSql.execute( f"insert into {dbname}.{NTBNAME} values ( {NOW - i * int(TIME_STEP * 1.2)}, {row_data} )" )


            for j in range(ctb_num):
                tdSql.execute( f"insert into {dbname}.ct{j+1} values ( {NOW - i * TIME_STEP}, {row_data} )" )


    def run(self):
        self.rows = 10
        tdSql.prepare(dbname=DBNAME)

        tdLog.printNoPrefix("==========step0:all check")
        self.all_test()

        tdLog.printNoPrefix("==========step1:create table in normal database")
        tdSql.prepare()
        self.__create_tb()
        self.__insert_data(rows=self.rows)

        tdLog.printNoPrefix("==========step2:create table in rollup database")
        tdLog.printNoPrefix("==========step2.1 : rolluo func is not last/first")
        tdSql.prepare(dbname=DB3, **{"retentions": "-:1d, 3s:3d, 5s:5d"})

        db3_ctb_num = 10
        self.__create_tb(rsma=True, dbname=DB3, ctb_num=db3_ctb_num, stb=STBNAME)
        self.__insert_data(rows=self.rows, rsma=True, dbname=DB3, ctb_num=db3_ctb_num)
        time.sleep(6)
        tdSql.query(f"select count(*) from {DB3}.{STBNAME} where ts > now()-5m")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.rows * db3_ctb_num)
        tdSql.execute(f"flush database {DB3}")
        tdSql.query(f"select count(*) from {DB3}.{STBNAME} where ts > now()-5m")
        tdSql.checkData(0, 0, self.rows * db3_ctb_num)
        tdSql.checkRows(1)
        tdSql.query(f"select {FLOAT_COL} from {DB3}.{CTBNAME} where ts > now()-4d")
        # not stable
        #tdSql.checkData(0, 0, self.rows-1)
        tdSql.query(f"select {DOUBLE_COL} from {DB3}.{CTBNAME} where ts > now()-6d")
        # not stable
        # tdSql.checkData(0, 0, self.rows-1)

        # from ...pytest.util.sql import tdSql

        tdLog.printNoPrefix("==========step2.1.1 : alter stb schemaL drop column")
        tdSql.query(f"select {FLOAT_COL} from {DB3}.{STBNAME}")
        #tdSql.execute(f"alter stable {DB3}.stb1 drop column {BINT_COL}")
        # not support alter stable schema anymore
        tdSql.error(f"alter stable {DB3}.stb1 drop column {BINT_COL}")
        #tdSql.error(f"select {BINT_COL} from {DB3}.{STBNAME}")


        tdLog.printNoPrefix("==========step2.1.2 : alter stb schemaL add num_column")
        # not support alter stable schema anymore
        tdSql.error(f"alter stable {DB3}.stb1 add column {INT_COL}_1 int")
        tdSql.error(f"select {INT_COL}_1 from {DB3}.{STBNAME}")
        #tdSql.execute(f"alter stable {DB3}.stb1 add column {INT_COL}_1 int")
        #tdSql.query(f"select count({INT_COL}_1) from {DB3}.{STBNAME} where _c0 > now-5m")
        #tdSql.checkData(0, 0, 0)
        #tdSql.execute(f"insert into {DB3}.{CTBNAME} ({PRIMARY_COL}, {INT_COL}, {INT_COL}_1) values({NOW}+20s, 111, 112)")
        #time.sleep(7)
        #tdSql.query(f"select _rowts, {INT_COL}, {INT_COL}_1 from {DB3}.{CTBNAME} where _c0 > now()-1h and _c0>{NOW}")
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 1, 111)
        #tdSql.checkData(0, 2, 112)
#
        #tdSql.query(f"select _rowts, {INT_COL}, {INT_COL}_1 from {DB3}.{CTBNAME} where _c0 > now()-2d and _c0>{NOW}")
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 1, 111)
        #tdSql.checkData(0, 2, 112)
        #tdSql.query(f"select _rowts, {INT_COL}, {INT_COL}_1 from {DB3}.{CTBNAME} where _c0 > now()-7d and _c0>{NOW}")
        #tdSql.checkRows(1)
        #tdSql.checkData(0, 1, 111)
        #tdSql.checkData(0, 2, 112)
        tdLog.printNoPrefix("==========step2.1.3 : drop child-table")
        tdSql.execute(f"drop table {DB3}.{CTBNAME} ")


        tdLog.printNoPrefix("==========step2.2 : rolluo func is  last/first")
        tdSql.prepare(dbname=DB4, **{"retentions": "-:1d, 2m:3d, 3m:5d"})

        db4_ctb_num = 10
        tdSql.execute(f"use {DB4}")
        self.__create_tb(rsma=True, dbname=DB4, ctb_num=db4_ctb_num, rsma_type="last")
        self.__insert_data(rows=self.rows, rsma=True, dbname=DB4, ctb_num=db4_ctb_num, rsma_type="last")
        time.sleep(7)
        tdSql.query(f"select count(*) from {DB4}.stb1 where ts > now()-5m")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.rows * db4_ctb_num)
        tdSql.execute(f"flush database {DB4}")
        tdSql.query(f"select count(*) from {DB4}.stb1 where ts > now()-5m")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, self.rows * db4_ctb_num)
        tdSql.query(f"select {INT_COL} from {DB4}.ct1 where ts > now()-4d")
        tdSql.checkRows_range([1,2])
        # tdSql.checkData(0, 0, self.rows-1)
        tdSql.query(f"select {INT_COL} from {DB4}.ct1 where ts > now()-6d")
        tdSql.checkRows_range([1,2])
        # tdSql.checkData(0, 0, self.rows-1)
        # return

        tdSql.execute(f"drop database if exists {DB1} ")
        tdSql.execute(f"drop database if exists {DB2} ")
        # self.all_test()

        # tdDnodes.stop(1)
        # tdDnodes.start(1)
        tdSql.execute(f"flush database {DBNAME}")

        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        self.all_test()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
