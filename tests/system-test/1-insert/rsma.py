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
    def create_tb(self, stb=STBNAME, ctb_num=20, ntbnum=1, rsma=False, dbname=DBNAME, rsma_type="sum"):
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

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            tagValue = 'beijing'
            if (i % 2 == 0):
                tagValue = 'shanghai'
            sql += " %s%d using %s tags(%d, '%s')"%(ctbPrefix,i,stbName,i+1, tagValue)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return


    def run(self):
        self.rows = 10
        tdLog.printNoPrefix("==========step0:all check")
        dbname='d0'
        tdSql.execute(f"create database {dbname} retentions -:10d,1m:15d,1h:30d  STT_TRIGGER 1  vgroups 6;")
        tdSql.execute(f"create stable if not exists {dbname}.st_min (ts timestamp, c1 int) tags (proid int,city binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;;")
        tdSql.execute(f"create stable if not exists {dbname}.st_avg (ts timestamp, c1 double) tags (city binary(20),district binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;;")
        self.create_ctable(tdSql, dbname, 'st_min', 'ct_min', 10000)
        tdLog.printNoPrefix("==========step4:after wal, all check again ")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
