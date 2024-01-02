from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *


INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL = [INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
UN_NUM_COL = [BOOL_COL, BINARY_COL, NCHAR_COL, ]
TS_TYPE_COL = [TS_COL]

DBNAME = "db"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1')

    def __create_tb(self, dbname="db"):
        create_stb_sql  =  f'''create table {dbname}.stb1(
                ts timestamp, f1 int
            ) tags (tag1 binary(16300))
            '''
        tdSql.execute(create_stb_sql)

        tag_value = 'a'
        for i in range(1200):
            tag_value = tag_value + 'a'

        for i in range(8000):
            tdSql.execute(f"create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( '{tag_value}' )")

    def __query_data(self, rows, dbname="db"):
        tdSql.execute(
            f'''select count(*) from {dbname}.stb1 where tag1 like '%a'
            '''
        )
        tdSql.checkRows(0)

    def __ts4421(self, dbname="db", stbname='stb4421', ctbname='ctb4421'):
        TAG_TYPE   = ['varchar', 'nchar']
        TAG_LEN    = [2, 8, 200]
        TAG_VAL    = [0, -200, 123456789]
        TAG_RESULT = [True,False,False,True,True,False,True,True,True,True,False,False,True,True,False,True,True,True]

        nTagCtb = 0
        for tagType in TAG_TYPE:
            for tagLen in TAG_LEN:
                tdSql.execute(f'create stable {dbname}.{stbname}(ts timestamp, f1 int) tags(t1 %s(%d))'%(tagType, tagLen))
                for tagVal in TAG_VAL:
                    if TAG_RESULT[nTagCtb] == False:
                        tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%d)'%(tagVal))
                        tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%d) values(now,1)'%(tagVal))
                        tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags("%d")'%(tagVal))
                        tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags("%d") values(now,1)'%(tagVal))
                        tdSql.error(f"create table {dbname}.{ctbname} using {dbname}.{stbname} tags('%d')"%(tagVal))
                        tdSql.error(f"insert into {dbname}.{ctbname} using {dbname}.{stbname} tags('%d') values(now,1)"%(tagVal))
                    else:
                        # integer as tag value
                        tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.stb4421 tags(%d)'%(tagVal))
                        tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                        tdSql.query(f'select * from {dbname}.{ctbname} where t1="%s"'%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f'drop table {dbname}.{ctbname}')
                        tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%d) values(now,1)'%(tagVal))
                        tdSql.query(f'select * from {dbname}.{ctbname} where t1="%s"'%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f'drop table {dbname}.{ctbname}')
                        # string as tag value
                        tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.stb4421 tags("%d")'%(tagVal))
                        tdSql.execute(f'insert into {dbname}.{ctbname} values(now,1)')
                        tdSql.query(f'select * from {dbname}.{ctbname} where t1="%s"'%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f'drop table {dbname}.{ctbname}')
                        tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags("%d") values(now,1)'%(tagVal))
                        tdSql.query(f'select * from {dbname}.{ctbname} where t1="%s"'%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f'drop table {dbname}.{ctbname}')
                        tdSql.execute(f"create table {dbname}.{ctbname} using {dbname}.stb4421 tags('%d')"%(tagVal))
                        tdSql.execute(f"insert into {dbname}.{ctbname} values(now,1)")
                        tdSql.query(f"select * from {dbname}.{ctbname} where t1='%s'"%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f"drop table {dbname}.{ctbname}")
                        tdSql.execute(f"insert into {dbname}.{ctbname} using {dbname}.{stbname} tags('%d') values(now,1)"%(tagVal))
                        tdSql.query(f"select * from {dbname}.{ctbname} where t1='%s'"%(tagVal))
                        tdSql.checkRows(1)
                        tdSql.execute(f"drop table {dbname}.{ctbname}")
                    nTagCtb += 1
                tdSql.execute(f'drop table {dbname}.{stbname}')

    def run(self):
        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()

        tdLog.printNoPrefix("==========step2:query data")
        self.__query_data(10)

        tdLog.printNoPrefix("==========step3:check ts4421")
        self.__ts4421()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
