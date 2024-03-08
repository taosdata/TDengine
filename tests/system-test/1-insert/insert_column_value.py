import datetime
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *
from util.dnodes import *
from util.sqlset import *

DBNAME = "db"

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1')

    def __create_tb(self, dbname="db"):
        CREATE_STB_LIST = [ f"create table {dbname}.stb_vc (ts timestamp, c0 binary(50), c1 binary(50)) tags(t0 binary(50), t1 binary(50));",
                            f"create table {dbname}.stb_nc (ts timestamp, c0 nchar(50), c1 nchar(50)) tags(t0 nchar(50), t1 nchar(50));",
                            f"create table {dbname}.stb_ts (ts timestamp, c0 timestamp, c1 timestamp) tags(t0 timestamp, t1 timestamp);",
                            f"create table {dbname}.stb_bo (ts timestamp, c0 bool, c1 bool) tags(t0 bool, t1 bool);",
                            f"create table {dbname}.stb_vb (ts timestamp, c0 varbinary(50), c1 varbinary(50)) tags(t0 varbinary(50), t1 varbinary(50));",
                            f"create table {dbname}.stb_in (ts timestamp, c0 int, c1 int) tags(t0 int, t1 int);",
                            f"create table {dbname}.stb_fl (ts timestamp, c0 float, c1 float) tags(t0 float, t1 float);",
                            f"create table {dbname}.stb_db (ts timestamp, c0 float, c1 float) tags(t0 float, t1 float);",
                            f"create table {dbname}.stb_ge (ts timestamp, c0 geometry(512), c1 geometry(512)) tags(t0 geometry(512), t1 geometry(512));",
                            f"create table {dbname}.stb_js (ts timestamp, c0 int) tags(t0 json);" ]
        for _stb in CREATE_STB_LIST:
            tdSql.execute(_stb)
        tdSql.query(f'show {dbname}.stables')
        tdSql.checkRows(len(CREATE_STB_LIST))

    def __insert_query_common(self, dbname="db", stbname="", ctbname="", oklist=[], kolist=[], okv=None):
        for _l in kolist:
            for _e in _l:
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e))
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s)' %(_e))
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s)' %(_e, _e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s) values(now, {okv}, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv}) values(now, {okv}, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, {okv})' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, %s)' %(_e))
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, %s)' %(_e, _e))
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, {okv})')
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
        for _l in oklist:
            for _e in _l:
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 0s, %s, {okv})' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 1s, {okv}, %s)' %(_e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now + 2s, %s, %s)' %(_e, _e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(3)
                tdSql.execute(f'drop table {dbname}.{ctbname}')      
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s)' %(_e, _e))
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now, %s, %s)' %(_e, _e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv}) values(now, %s, {okv})' %(_e, _e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s) values(now, {okv}, %s)' %(_e, _e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s) values(now, %s, %s)' %(_e, _e, _e, _e))
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}')

    def __insert_query_exec(self):
        STR_EMPTY = ['\'\'', "\"\""]
        STR_INTEGER = ["\"123\"", '\'+12\'', '\'+0\'', '\'-0\'', '\'-128\'']
        STR_FLOAT = ['\'123.1\'', "\"-23.001\"", "\"+0.001\"" ]
        STR_FLOAT_E = ['\'1e1\'', "\"-0.1E+1\"", "\"1e-2\""]
        STR_MISC = ['\' \'', "\" \"", "\"123ab\"", '\'123d\'', '\'-12s\'', '\'\x012\'', '\'x12\'', '\'x\'', '\'NULL \'']
        STR_OPTR = ['\'1*10\'', '\'1+2\'', '\'-2-0\'','\'1%2\'', '\'2/0\'', '\'1&31\'']
        STR_TSK = ['\'now\'', '\'today\'']
        STR_TSK_MISC = ['\'now+1s\'', '\' now\'', '\'today \'', '\'today+1m\'', '\'today-1w\'']
        STR_TSKP = ['\'now()\'', '\'today()\'']
        STR_TSKP_MISC = ['\'now()+1s\'', '\' now()\'', '\'now( )\'', '\'today() \'', '\'today())\'', '\'today()+1m\'', '\'today()-1w\'']
        STR_BOOL = ['\'true\'', '\'false\'', '\'TRUE\'', '\'FALSE\'', '\'tRuE\'', '\'falsE\'']
        STR_TS = ["\"2024-02-01 00:00:01.001-08:00\"", "\'2024-02-01T00:00:01.001+09:00\'", "\"2024-02-01\"", "\'2024-02-02 00:00:01\'", "\'2024-02-02 00:00:01.009\'"]
        STR_VARBIN = ['\'\\x\'', '\'\\x12ab\'']
        STR_JSON = ['\'{\"k1\":\"v1\"}\'', '\'[]\'', '\'{}\'']
        STR_GEO = ['\'POINT(1.0 1.0)\'',   '\'LINESTRING(1.00 +2.0, 2.1 -3.2, 5.00 5.01)\'', '\'POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))\'' ]
        STR_NULL  = ['\'NuLl\'', '\'null\'', '\'NULL\'']

        RAW_INTEGER = ['123', '+0123', '-0128', '+0', '-0', '0']
        RAW_FLOAT = ['123.012', '-128.001', '-0.0', '0.0', '+0.0']
        RAW_FLOAT_E = ['1e-100', "-1E-10", '+0.1E+2']
        RAW_MISC = ['123abc', "123c", '-123d', '+', '-', ' *', ' /', '% ', '&', "|", "^", "&&", "||", "!", " =", 'now+1 s', 'now-1','now-1y','now+2 d',
                    'today+1 s', 'today-1','today-1y','today+2 d', 'now()+1 s', 'now()-1','now()-1y','now()+2 d', 'today()+1 s', 'today()-1','today()-1y','today()+2 d']
        RAW_OPTR = ['1*10', '1+2', '-2-0','1%2', '2/0', '1&31']
        RAW_TSK = [' now ', 'today ']
        RAW_TSK_OPTR = [' now +1s', 'today + 2d']
        RAW_TSKP = ['now( ) ', ' toDay() ']
        RAW_TSKP_OPTR = [' noW ( ) + 1s',  'nOw( ) + 2D', 'NOW () + 000s', ' today()+1M', 'today( ) - 1w ', 'TodaY ( ) - 1U ']
        RAW_BOOL = ['true', 'false', ' TRUE ', 'FALSE  ', '  tRuE', '  falsE    ']
        RAW_NULL = ['NuLl', 'null', 'NULL', ' NULL ']

        OK_VC = [STR_EMPTY, STR_INTEGER, STR_FLOAT, STR_FLOAT_E, STR_MISC, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, 
                 STR_JSON, STR_GEO, STR_NULL, RAW_INTEGER, RAW_FLOAT, RAW_FLOAT_E, RAW_TSK, RAW_BOOL, RAW_NULL]
        KO_VC = [RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR]
        OK_NC = []
        KO_NC = []
        OK_TS = [STR_TSK, STR_TSKP, STR_NULL, RAW_INTEGER, RAW_TSK, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, RAW_NULL]
        KO_TS = [STR_EMPTY, STR_INTEGER, STR_FLOAT, STR_FLOAT_E, STR_MISC, STR_OPTR, STR_TSK_MISC, STR_BOOL, STR_VARBIN,
                 STR_JSON,STR_GEO, STR_FLOAT, RAW_FLOAT_E, RAW_MISC, RAW_OPTR, RAW_BOOL]
        OK_BO = []
        KO_BO = []
        OK_VB = []
        KO_VB = []
        OK_IN = []
        KO_IN = []
        OK_FL = []
        KO_FL = []
        OK_DB = []
        KO_DB = []
        OK_GE = []
        KO_GE = []
        OK_JS = []
        KO_JS = []
        
        PARAM_LIST = [
                        ["db", "stb_vc", "ctb_vc", OK_VC, KO_VC, "\'vc\'"],
                        ["db", "stb_nc", "ctb_nc", OK_NC, KO_NC, "\'nc\'"],
                        ["db", "stb_ts", "ctb_ts", OK_TS, KO_TS, "now"],
                        ["db", "stb_bo", "ctb_bo", OK_BO, KO_BO, "true"],
                        ["db", "stb_vb", "ctb_vb", OK_VB, KO_VB, "\'\\x\'"],
                        ["db", "stb_in", "ctb_in", OK_IN, KO_IN, "1"],
                        ["db", "stb_fl", "ctb_fl", OK_FL, KO_FL, "1.0"],
                        ["db", "stb_db", "ctb_db", OK_DB, KO_DB, "1.0"],
                        ["db", "stb_ge", "ctb_ge", OK_GE, KO_GE, "\'POINT(1.0 1.0)\'"],
                        ["db", "stb_js", "ctb_js", OK_JS, KO_JS, "\'{\'k1\':\'v1\',\k2\':\'v2\'}\'"], 
                      ]

        for _pl in PARAM_LIST:
            self.__insert_query_common(_pl[0], _pl[1], _pl[2], _pl[3], _pl[4], _pl[5])


    def run(self):
        tdLog.printNoPrefix("==========step1:create table")
        self.__create_tb()
        self.__insert_query_exec()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())