import datetime, time
from enum import Enum
import binascii
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *
from util.dnodes import *
from util.sqlset import *


DBNAME = "db"

class TDDataType(Enum):
    NULL       = 0
    BOOL       = 1   
    TINYINT    = 2   
    SMALLINT   = 3   
    INT        = 4   
    BIGINT     = 5   
    FLOAT      = 6   
    DOUBLE     = 7   
    VARCHAR    = 8   
    TIMESTAMP  = 9   
    NCHAR      = 10  
    UTINYINT   = 11  
    USMALLINT  = 12  
    UINT       = 13  
    UBIGINT    = 14  
    JSON       = 15  
    VARBINARY  = 16  
    DECIMAL    = 17  
    BLOB       = 18  
    MEDIUMBLOB = 19
    BINARY     = 8   
    GEOMETRY   = 20  
    MAX        = 21


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        self.TIMESTAMP_MIN = -1000
        self.TIMESTAMP_BASE = 1706716800
        tdSql.init(conn.cursor())
        tdSql.execute(f'drop database if exists db')
        tdSql.execute(f'create database if not exists db vgroups 1 keep 10512000m')
        tdLog.printNoPrefix("create table")
        self.__create_tb()

    def __create_tb(self, dbname="db"):
        CREATE_STB_LIST = [ f"create table {dbname}.stb_vc (ts timestamp, c0 varchar(50), c1 varchar(50)) tags(t0 varchar(50), t1 varchar(50));",
                            f"create table {dbname}.stb_nc (ts timestamp, c0 nchar(50), c1 nchar(50)) tags(t0 nchar(50), t1 nchar(50));",
                            f"create table {dbname}.stb_bi (ts timestamp, c0 binary(50), c1 binary(50)) tags(t0 binary(50), t1 binary(50));",
                            f"create table {dbname}.stb_ts (ts timestamp, c0 timestamp, c1 timestamp) tags(t0 timestamp, t1 timestamp);",
                            f"create table {dbname}.stb_bo (ts timestamp, c0 bool, c1 bool) tags(t0 bool, t1 bool);",
                            f"create table {dbname}.stb_vb (ts timestamp, c0 varbinary(50), c1 varbinary(50)) tags(t0 varbinary(50), t1 varbinary(50));",
                            f"create table {dbname}.stb_in (ts timestamp, c0 int, c1 int) tags(t0 int, t1 int);",
                            f"create table {dbname}.stb_ui (ts timestamp, c0 int unsigned, c1 int unsigned) tags(t0 int unsigned, t1 int unsigned);",
                            f"create table {dbname}.stb_bin (ts timestamp, c0 bigint, c1 bigint) tags(t0 bigint, t1 bigint);",
                            f"create table {dbname}.stb_bui (ts timestamp, c0 bigint unsigned, c1 bigint unsigned) tags(t0 bigint unsigned, t1 bigint unsigned);",
                            f"create table {dbname}.stb_sin (ts timestamp, c0 smallint, c1 smallint) tags(t0 smallint, t1 smallint);",
                            f"create table {dbname}.stb_sui (ts timestamp, c0 smallint unsigned, c1 smallint unsigned) tags(t0 smallint unsigned, t1 smallint unsigned);",
                            f"create table {dbname}.stb_tin (ts timestamp, c0 tinyint, c1 tinyint) tags(t0 tinyint, t1 tinyint);",
                            f"create table {dbname}.stb_tui (ts timestamp, c0 tinyint unsigned, c1 tinyint unsigned) tags(t0 tinyint unsigned, t1 tinyint unsigned);",
                            f"create table {dbname}.stb_fl (ts timestamp, c0 float, c1 float) tags(t0 float, t1 float);",
                            f"create table {dbname}.stb_db (ts timestamp, c0 double, c1 double) tags(t0 double, t1 double);",
                            f"create table {dbname}.stb_ge (ts timestamp, c0 geometry(512), c1 geometry(512)) tags(t0 geometry(512), t1 geometry(512));",
                            f"create table {dbname}.stb_js (ts timestamp, c0 int) tags(t0 json);"]

        CREATE_NTB_LIST = [ f"create table {dbname}.ntb_vc (ts timestamp, c0 binary(50), c1 varchar(50));",
                            f"create table {dbname}.ntb_nc (ts timestamp, c0 nchar(50), c1 nchar(50));",
                            f"create table {dbname}.ntb_bi (ts timestamp, c0 binary(50), c1 binary(50));",
                            f"create table {dbname}.ntb_ts (ts timestamp, c0 timestamp, c1 timestamp);",
                            f"create table {dbname}.ntb_bo (ts timestamp, c0 bool, c1 bool);",
                            f"create table {dbname}.ntb_vb (ts timestamp, c0 varbinary(50), c1 varbinary(50));",
                            f"create table {dbname}.ntb_in (ts timestamp, c0 int, c1 int);",
                            f"create table {dbname}.ntb_ui (ts timestamp, c0 int unsigned, c1 int unsigned);",
                            f"create table {dbname}.ntb_bin (ts timestamp, c0 bigint, c1 bigint);",
                            f"create table {dbname}.ntb_bui (ts timestamp, c0 bigint unsigned, c1 bigint unsigned);",
                            f"create table {dbname}.ntb_sin (ts timestamp, c0 smallint, c1 smallint);",
                            f"create table {dbname}.ntb_sui (ts timestamp, c0 smallint unsigned, c1 smallint unsigned);",
                            f"create table {dbname}.ntb_tin (ts timestamp, c0 tinyint, c1 tinyint);",
                            f"create table {dbname}.ntb_tui (ts timestamp, c0 tinyint unsigned, c1 tinyint unsigned);",
                            f"create table {dbname}.ntb_fl (ts timestamp, c0 float, c1 float);",
                            f"create table {dbname}.ntb_db (ts timestamp, c0 double, c1 double);",
                            f"create table {dbname}.ntb_ge (ts timestamp, c0 geometry(512), c1 geometry(512));"]
        for _stb in CREATE_STB_LIST:
            tdSql.execute(_stb)
        tdSql.query(f'show {dbname}.stables')
        tdSql.checkRows(len(CREATE_STB_LIST))

        for _stb in CREATE_NTB_LIST:
            tdSql.execute(_stb)
        tdSql.query(f'show {dbname}.tables')
        tdSql.checkRows(len(CREATE_NTB_LIST))

    def _query_check_varchar(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                if check_item == okv or check_item == nv:
                    check_result = True
                if check_result == False and (okv[0:1] == '\'' or okv[0:1] == '\"'):
                    if check_item == okv[1:-1]:
                        check_result = True
                if check_result == False and (nv[0:1] == '\'' or nv[0:1] == '\"'):
                    if check_item == nv[1:-1]:
                        check_result = True
                if check_result == False:
                    if check_item == nv.strip().lower():
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_int(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                if check_item == okv or check_item == nv:
                    check_result = True
                if check_item == nv.strip().lower():
                    check_result = True
                if check_result == False and (okv.find('1') != -1 or okv.find('2') != -1):
                    if check_item != 0:
                        check_result = True
                if check_result == False and (nv.find('1') != -1 or nv.find('2') != -1):
                    if check_item != 0:
                        check_result = True
                if check_item == 0:
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_bool(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                elif result[i][j] == True:
                    check_item = "true"
                else:
                    check_item = "false"
                if check_item == okv.strip().lower() or check_item == nv.strip().lower():
                    check_result = True
                if check_result == False and (nv[0:1] == '\'' or nv[0:1] == '\"'):
                    if check_item == nv[1:-1].strip().lower():
                        check_result = True
                if check_result == False and (nv.find('1') != -1 or nv.find('2') != -1): # char 1 or 2 exist for non-zero values
                    if check_item == "true":
                        check_result = True
                else:
                    if check_item == "false":
                        check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_timestamp(self, result, okv, nv, row = 0, col = 0):
        for i in range(row):
            for j in range(1, col):
                check_result = False
                check_item   = result[i][j]
                if result[i][j] == None:
                    check_item = 'null'
                    if nv.lower().find(check_item) != -1:
                        check_result = True
                else:
                    check_item = int(result[i][j].timestamp())
                if check_result == False and nv.lower().find("now") != -1 or nv.lower().find("today") != -1 or nv.lower().find("now") != -1 or nv.lower().find("today") != -1: 
                    if check_item > self.TIMESTAMP_BASE:
                        check_result = True
                if check_result == False and check_item > self.TIMESTAMP_MIN:
                    check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check_varbinary(self, result, okv, nv, row = 0, col = 0):
        tdLog.info(f'[okv={okv}, nv={nv}')
        for i in range(row):
            for j in range(1, col):
                check_result = False
                if result[i][j]:
                    check_item   = result[i][j].decode('utf-8')
                else:
                    check_item   = ''
                # new_nv = None

                if nv[0:1] == '\'' or nv[0:1] == '\"':
                    nv = nv[1:-1]
                if okv[0:1] == '\'' or okv[0:1] == '\"':
                    okv = okv[1:-1]

                # if nv[0:2] == '\\x' or nv[0:2] == '\\X':
                #     nv = nv[1:]
                # elif nv == '' or nv =="":
                #     new_nv = nv
                # elif nv.isspace():
                #     new_nv = nv
                # else:
                #     hex_text = binascii.hexlify(nv.encode())
                #     new_nv = '/x' + hex_text.decode().upper()
                # tdLog.info(f"okv={okv}, nv={nv}, check_item={check_item}")
                if check_item == None:
                    check_item = 'null'
                if check_item in okv or check_item in nv:
                    check_result = True
                tdSql.checkEqual(check_result, True)

    def _query_check(self, dbname="db", stbname="", ctbname="", ntbname="",nRows = 0, okv = None, nv = None, dtype = TDDataType.NULL):
        result = None
        if stbname:
            if dtype != TDDataType.GEOMETRY: # geometry query by py connector need to be supported
                tdSql.query(f'select * from {dbname}.{stbname}')
                tdSql.checkRows(nRows)
                result = tdSql.queryResult
        else:
            tdSql.query(f'select * from {dbname}.{ntbname}')
            tdSql.checkRows(nRows)
            result = tdSql.queryResult
        

        if dtype == TDDataType.VARCHAR or  dtype == TDDataType.NCHAR or dtype == TDDataType.BINARY:
            self._query_check_varchar(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.TIMESTAMP:
            self._query_check_timestamp(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.BOOL:
            self._query_check_bool(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.VARBINARY:
            self._query_check_varbinary(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.INT or dtype == TDDataType.BIGINT or dtype == TDDataType.SMALLINT or dtype == TDDataType.TINYINT:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.UINT or dtype == TDDataType.UBIGINT or dtype == TDDataType.USMALLINT or dtype == TDDataType.UTINYINT:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.FLOAT or dtype == TDDataType.DOUBLE:
            self._query_check_int(result, okv, nv, nRows, 4)
        elif dtype == TDDataType.GEOMETRY: 
            pass
        else:
            tdLog.info(f"unknown data type %s" % (dtype))
    
        if ctbname != "":
            tdSql.execute(f'drop table {dbname}.{ctbname}')

    def __insert_query_common(self, dbname="db", stbname="", ctbname="", ntbname="", oklist=[], kolist=[], okv=None, dtype = TDDataType.NULL):
        tdLog.info(f'[Begin]{dbname}.{stbname} {ctbname}, oklist:%d, kolist:%d, TDDataType:%s'%(len(oklist), len(kolist), dtype))
        # tdSql.checkEqual(34, len(oklist) + len(kolist))

        for _l in kolist:
            for _e in _l:
                # tdLog.info(f'[ko:verify value "{_e}"]')
                # create sub-table manually, check tag
                tdLog.info('[ko:create sub-table manually, check tag]')
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv})' %(_e), show=True)
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s)' %(_e), show=True)
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, %s)' %(_e, _e), show=True)

                # create sub-table automatically, check tag
                tdLog.info('[ko:create sub-table automatically, check tag]')
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, %s) values(now, {okv}, {okv})' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s, {okv}) values(now, {okv}, {okv})' %(_e), show=True)
                
                # create sub-table automatically, check value
                tdLog.info('[ko:create sub-table automatically, check value]')
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, {okv})' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, %s)' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, %s, %s)' %(_e, _e), show=True)
                
                # check alter table tag
                tdLog.info('[ko:check alter table tag]')
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}, {okv}) values(now, {okv}, {okv})', show=True)
                # self._query_check(dbname,stbname, "", None, 1, okv, _e, dtype)
                # tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = {okv}', show=True)
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e), show=True)
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t1 = %s' %(_e), show=True)
                tdSql.execute(f'drop table {dbname}.{ctbname}')

                # insert into value by supper-table, check tag & value
                tdLog.info('[ko:insert into value by supper-table, check tag & value]')
                tdSql.error(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_1", %s, {okv}, now, {okv}, {okv})' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_2", {okv},{okv}, now + 1s, {okv}, %s)' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_3", %s, %s, now + 2s, %s, %s)' %(_e, _e, _e, _e), show=True)

                # insert into normal table, check value
                tdLog.info('[ko:insert into normal table, check value]')
                tdSql.error(f'insert into {dbname}.{ntbname} values(now, %s, {okv})' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ntbname} values(now, {okv}, %s)' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ntbname} values(now, %s, %s)' %(_e, _e), show=True)

        for _l in oklist:
            for _e in _l:
                tdLog.info(f'[ok:verify value "{_e}"]')
                # 1. create sub-table manually, check tag
                tdLog.info('[ok:create sub-table manually, check tag]')
                tdSql.execute(f'create table {dbname}.{ctbname}_1 using {dbname}.{stbname} tags({_e}, {okv})', show=True)
                tdSql.execute(f'create table {dbname}.{ctbname}_2 using {dbname}.{stbname} tags({okv}, %s)' %(_e), show=True)
                tdSql.execute(f'create table {dbname}.{ctbname}_3 using {dbname}.{stbname} tags(%s, %s)' %(_e, _e), show=True)
                

                # 1.1 insert into sub-table, check value
                tdLog.info('[ok:insert into sub-table, check value]')
                tdSql.execute(f'insert into {dbname}.{ctbname}_1 values(now + 0s, %s, {okv})' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname}_2 values(now + 1s, {okv}, %s)' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname}_3 values(now + 2s, %s, %s)' %(_e, _e), show=True)
                
                # 1.2 check alter table tag
                tdLog.info('[ok:check alter table tag]')
                tdSql.execute(f'alter table {dbname}.{ctbname}_1 set tag t1 = %s' %(_e), show=True)
                tdSql.execute(f'alter table {dbname}.{ctbname}_2 set tag t0 = %s' %(_e), show=True)

                # 1.3 check table data
                tdLog.info('[ok:check table data]')
                self._query_check(dbname, stbname, f'{ctbname}_1', None, 3, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_2', None, 2, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_3', None, 1, okv, _e, dtype)

                # 2. insert into value by creating sub-table automatically, check tag & value
                tdLog.info('[ok:insert into value by creating sub-table automatically, check tag & value]')
                tdSql.execute(f'insert into {dbname}.{ctbname}_1 using {dbname}.{stbname} tags(%s, {okv}) values(now, %s, {okv})' %(_e, _e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname}_2 using {dbname}.{stbname} tags({okv}, %s) values(now + 1s, {okv}, %s)' %(_e, _e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname}_3 using {dbname}.{stbname} tags(%s, %s) values(now + 2s, %s, %s)' %(_e, _e, _e, _e), show=True)

                self._query_check(dbname, stbname, f'{ctbname}_1', None, 3, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_2', None, 2, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_3', None, 1, okv, _e, dtype)

                # 3. insert into value by supper-table, check tag & value
                tdLog.info('[ok:insert into value by supper-table, check tag & value]')
                tdSql.execute(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_1", %s, {okv}, now, %s, {okv})' %(_e, _e), show=True)
                tdSql.execute(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_2", {okv}, %s, now + 1s, {okv}, %s)' %(_e, _e), show=True)
                tdSql.execute(f'insert into {dbname}.{stbname}(tbname, t0, t1, ts, c0, c1) values("{ctbname}_3", %s, %s, now + 2s, %s, %s)' %(_e, _e, _e, _e), show=True)

                self._query_check(dbname, stbname, f'{ctbname}_1', None, 3, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_2', None, 2, okv, _e, dtype)
                self._query_check(dbname, stbname, f'{ctbname}_3', None, 1, okv, _e, dtype)

                # 4. insert value into normal table
                tdLog.info('[ok:insert value into normal table]')
                tdSql.execute(f'insert into {dbname}.{ntbname} values(now, %s, {okv})' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ntbname} values(now + 1s, {okv}, %s)' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ntbname} values(now + 2s, %s, %s)' %(_e, _e), show=True)

                if dtype != TDDataType.GEOMETRY:
                    # self._query_check(dbname, None, None, ntbname, 3, okv, _e, dtype)
                    tdSql.query(f'select * from {dbname}.{ntbname}')
                    tdSql.checkRows(3)
                tdSql.query(f'delete from {dbname}.{ntbname}')

    def __insert_query_json(self, dbname="db", stbname="", ctbname="", oklist=[], kolist=[], okv=None):
        tdLog.info(f'{dbname}.{stbname} {ctbname}, oklist:%d, kolist:%d'%(len(oklist), len(kolist)))
        # tdSql.checkEqual(34, len(oklist) + len(kolist))

        for _l in kolist:
            for _e in _l:
                tdSql.error(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s)' %(_e), show=True)
                tdSql.error(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags(%s) values(now, 1)' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname} using {dbname}.{stbname} tags({okv}) values(now, 1)', show=True)
                tdSql.query(f'select * from {dbname}.{stbname}', show=True)
                tdSql.checkRows(1)
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = {okv}', show=True)
                tdSql.error(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e), show=True)
                tdSql.execute(f'drop table {dbname}.{ctbname}', show=True)
        for _l in oklist:
            for _e in _l:
                tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(%s)' %(_e), show=True)
                tdSql.execute(f'insert into {dbname}.{ctbname} values(now, 1)', show=True)
                tdSql.execute(f'alter table {dbname}.{ctbname} set tag t0 = %s' %(_e), show=True)
                tdSql.query(f'select * from {dbname}.{stbname}', show=True)
                tdSql.checkRows(1)
                tdSql.execute(f'drop table {dbname}.{ctbname}', show=True)
    
    def __insert_query_exec(self):
        STR_EMPTY = ['\'\'', "\"\"", '\' \'', "\"    \""]
        STR_CHINESE = ['\'年年岁岁，花相似\'']
        STR_CHINESE_ILLEGAL = ['\'洛阳城东桃李花，飞来飞去落谁家。幽闺女儿爱颜色，坐见落花长叹息。今岁花开君不待。明年花开复谁在。故人不共洛阳东，今来空对落花风。年年岁岁花相似，岁岁年年人不同\'']
        STR_INTEGER_P = ["\"42\"", '\'+42\'', '\'+0\'', '\'1\'','\'-0\'', '\'0x2A\'', '\'-0X0\'', '\'+0x0\'', '\'0B00101010\'', '\'-0b00\'']
        STR_INTEGER_M = ['\'-128\'', '\'-0X1\'', '\"-0x34\"', '\'-0b01\'', '\'-0B00101010\'']
        STR_FLOAT_P = ['\'42.1\'', "\"+0.003\"", "\'-0.0\'"]
        STR_FLOAT_M = ["\"-32.001\""]
        STR_FLOAT_E_P = ['\'1e1\'', "\"3e-2\"", "\"-3e-5\""]
        STR_FLOAT_E_M = ["\"-0.3E+1\""]
        STR_MISC = ["\"123ab\"", '\'123d\'', '\'-12s\'', '\'\x012\'', '\'x12\'',  '\'x\'', '\'NULL \'', '\' NULL\'', '\'True \'', '\' False\'', 
                    '\'0B0101 \'', '\' 0B0101\'', '\' -0x01 \'', '\'-0x02 \'']
        STR_OPTR = ['\'1*10\'', '\'1+2\'', '\'-2-0\'','\'1%2\'', '\'2/0\'', '\'1&31\'']
        STR_TSK = ['\'now\'', '\'today\'']
        STR_TSK_MISC = ['\'now+1s\'', '\' now\'', '\'today \'', '\'today+1m\'', '\'today-1w\'']
        STR_TSKP = ['\'now()\'', '\'today()\'']
        STR_TSKP_MISC = ['\'now()+1s\'', '\' now()\'', '\'now( )\'', '\'today() \'', '\'today())\'', '\'today()+1m\'', '\'today()-1w\'']
        STR_BOOL = ['\'true\'', '\'false\'', '\'TRUE\'', '\'FALSE\'', '\'tRuE\'', '\'falsE\'']
        STR_TS = ["\"2024-02-01 00:00:01.001-08:00\"", "\'2024-02-01T00:00:01.001+09:00\'", "\"2024-02-01\"", "\'2024-02-02 00:00:01\'", "\'2024-02-02 00:00:01.009\'"]
        STR_TS_ILLEGAL = ["\"2023-2024-02-01 00:00:01.001-08:00\"", "\'2024-02-01T99:00:01.001+09:00\'", "\"2024-02-31\"", "\'2024-02-02 00:88:01\'", "\'2024-02-02 00:00:77.009\'"]
        STR_VARBIN = ["\'\x12\'", "\'\x13\'", "\' \x14 \'", "\'\x12ab\'"]
        STR_JSON_O = ['\'{\"k1\":\"v1\"}\'', '\' {} \'']
        STR_JSON_A = ['\'[]\'', '\"{\'k1\': \'v1\',\'k2\'}\"', '\"{\'k1\': \'v1\'}}\"']
        STR_GEO = ["\' POINT(1.0 1.0)\'", "\'LINESTRING(1.00 +2.0, 2.1 -3.2, 5.00 5.01) \'", "\'POLYGON((1.0 1.0, -2.0 +2.0, 1.0 1.0))\'" ]
        STR_GEO_ILLEGAL = ['\' POINT(1.0)\'', '\'LINESTRING(1.00 +2.0, -3.2, 5.00 5.01) \'', '\'POLYGON((-2.0 +2.0, 1.0 1.0))\'' ]
        STR_NULL  = ['\'NuLl\'', '\'null\'', '\'NULL\'']
        STR_NONE  = ['\'NoNe\'', '\'none\'', '\'NONE\'']
        STR_BINARY = ["\'\x7f8290\'", "\'\X7f8290\'", "\'x7f8290\'", "\'\\x\'"] # bug TD-29193
        STR_BINARY_ILLEGAL = ["\'\\x7f829\'"]

        RAW_INTEGER_P = [' 42 ', '+042 ', ' +0', '0 ', '-0', '0', ' 0X2A', ' -0x0 ', '+0x0  ', '  0B00101010', ' -0b00']
        RAW_INTEGER_M = [' -42 ', ' -0128',' -0x1', '  -0X2A', '-0b01  ', ' -0B00101010 ']
        RAW_INTEGER_INT_BOUNDARY_ILLEGAL =['2147483648', '-2147483649']
        RAW_INTEGER_UINT_BOUNDARY_ILLEGAL =['4294967296', '-1']
        RAW_INTEGER_BINT_BOUNDARY_ILLEGAL =['9223372036854775808', '-9223372036854775809']
        RAW_INTEGER_UBINT_BOUNDARY_ILLEGAL =['18446744073709551616', '-1']
        RAW_INTEGER_SINT_BOUNDARY_ILLEGAL =['32768', '-32769']
        RAW_INTEGER_USINT_BOUNDARY_ILLEGAL =['65536', '-1']
        RAW_INTEGER_TINT_BOUNDARY_ILLEGAL =['128', '-129']
        RAW_INTEGER_UTINT_BOUNDARY_ILLEGAL =['256', '-1']
        RAW_FLOAT_P = [' 123.012', ' 0.0', ' +0.0', ' -0.0  ']
        RAW_FLOAT_M = ['-128.001 ']
        RAW_FLOAT_E_P = [' 1e-100', ' +0.1E+2', ' -0.1E-10']
        RAW_FLOAT_E_M = [" -1E2 "]
        RAW_FLOAT_E_M_SPE = ["-1e-100"]
        RAW_MISC = ['123abc', "123c", '-123d', '+', '-', ' *', ' /', '% ', '&', "|", "^", "&&", "||", "!", " =", ' None ', 'NONE', 'now+1 s', 'now-1','now-1y','now+2 d',
                    'today+1 s', 'today-1','today-1y','today+2 d', 'now()+1 s', 'now()-1','now()-1y','now()+2 d', 'today()+1 s', 'today()-1','today()-1y','today()+2 d']
        RAW_OPTR = ['1*10', '1+2', '-2-0','1%2', '2/0', '1&31']
        RAW_TSK = [' now ', 'today ']
        RAW_TSK_OPTR = [' now +1s', 'today + 2d']
        RAW_TSKP = ['now( ) ', ' toDay() ']
        RAW_TSKP_OPTR = [' noW ( ) + 1s',  'nOw( ) + 2D', 'NOW () + 000s', ' today()+1M', 'today( ) - 1w ', 'TodaY ( ) - 1U ']
        RAW_BOOL = ['true', 'false', ' TRUE ', 'FALSE  ', '  tRuE', '  falsE    ']
        RAW_NULL = ['NuLl', 'null ', ' NULL', ' NULL ']
        RAW_NONE = ['None', 'none ', ' NoNe', ' NONE ']
        RAW_BINARY_ILLEGAL = ['\\x7f8290', '\\X7f8290', 'x7f8290', '\\x', '\\x7f829']

        OK_VC = [STR_EMPTY, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, 
                 STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M, 
                 RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_TSK, RAW_BOOL, RAW_NULL, STR_CHINESE, STR_NONE]
        KO_VC = [RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_CHINESE_ILLEGAL, RAW_NONE]
        OK_NC = OK_VC
        KO_NC = KO_VC
        OK_BI = OK_VC
        KO_BI = KO_VC
        OK_TS = [STR_TSK, STR_INTEGER_P, STR_INTEGER_M, STR_TSKP, STR_TS, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_TSK, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, RAW_NULL]
        KO_TS = [STR_EMPTY, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK_MISC, STR_TSKP_MISC, STR_BOOL, STR_VARBIN,
                 STR_JSON_O, STR_JSON_A, STR_GEO, RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_MISC, RAW_OPTR, RAW_BOOL, STR_CHINESE, STR_NONE, STR_TS_ILLEGAL, 
                 STR_BINARY, RAW_NONE]
        OK_BO = [STR_BOOL, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M,RAW_BOOL, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        KO_BO = [STR_EMPTY,  STR_TSK, STR_TSKP, STR_TS,  STR_MISC, STR_OPTR, STR_TSK_MISC, STR_TSKP_MISC, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, 
                 RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, RAW_MISC, RAW_OPTR, STR_CHINESE, STR_NONE, STR_BINARY, RAW_NONE]
        OK_VB = [STR_EMPTY, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, 
                 STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, STR_NULL, RAW_NULL, STR_CHINESE, STR_NONE, STR_BINARY]     
        KO_VB = [RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_TSK, RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, 
                 STR_BINARY_ILLEGAL, STR_BINARY_ILLEGAL, RAW_BINARY_ILLEGAL, RAW_NONE]
        OK_IN = [STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_NULL, RAW_INTEGER_P, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M,
                 RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        BASE_KO_IN = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK,
                 RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_CHINESE, STR_NONE, STR_BINARY, RAW_NONE]
        KO_IN = BASE_KO_IN + [RAW_INTEGER_INT_BOUNDARY_ILLEGAL]
        OK_BIN = OK_IN
        KO_BIN = BASE_KO_IN +  [RAW_INTEGER_BINT_BOUNDARY_ILLEGAL]
        OK_SIN = OK_IN
        KO_SIN = BASE_KO_IN + [RAW_INTEGER_SINT_BOUNDARY_ILLEGAL]
        OK_TIN = OK_IN
        KO_TIN = BASE_KO_IN + [RAW_INTEGER_TINT_BOUNDARY_ILLEGAL]
        OK_UI = [STR_INTEGER_P, STR_FLOAT_P, STR_FLOAT_E_P, STR_NULL, RAW_INTEGER_P, RAW_FLOAT_P, RAW_FLOAT_E_P, RAW_NULL, RAW_FLOAT_E_M_SPE]
        BASE_KO_UI = [STR_EMPTY, STR_MISC, STR_INTEGER_M, STR_FLOAT_M, STR_FLOAT_E_M, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, 
                 STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, RAW_BOOL, RAW_INTEGER_M, RAW_FLOAT_M, RAW_FLOAT_E_M, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, 
                 STR_CHINESE, STR_NONE, RAW_NONE]
        KO_UI = BASE_KO_UI + [RAW_INTEGER_UINT_BOUNDARY_ILLEGAL]
        OK_UBINT = OK_UI
        KO_UBINT = BASE_KO_UI + [RAW_INTEGER_UBINT_BOUNDARY_ILLEGAL]
        OK_USINT = OK_UI
        KO_USINT = BASE_KO_UI + [RAW_INTEGER_USINT_BOUNDARY_ILLEGAL]
        OK_UTINT = OK_UI
        KO_UTINT = BASE_KO_UI + [RAW_INTEGER_UTINT_BOUNDARY_ILLEGAL]
        OK_FL = [RAW_INTEGER_P, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, STR_NULL, RAW_INTEGER_M, RAW_FLOAT_P, RAW_FLOAT_M,
                  RAW_FLOAT_E_P, RAW_FLOAT_E_M, RAW_NULL]
        KO_FL = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_VARBIN, STR_JSON_O, STR_JSON_A, STR_GEO, RAW_TSK, 
                 RAW_BOOL, RAW_MISC, RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_CHINESE, STR_NONE, STR_BINARY, RAW_NONE]
        OK_DB = OK_FL
        KO_DB = KO_FL
        OK_GE = [STR_GEO, STR_NULL, RAW_NULL]
        KO_GE = [STR_EMPTY, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_JSON_O, STR_JSON_A, STR_VARBIN, RAW_TSK, RAW_BOOL, RAW_MISC,
                 RAW_OPTR, RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, STR_CHINESE, STR_NONE, STR_BINARY, STR_GEO_ILLEGAL, RAW_NONE]
        OK_JS = [STR_EMPTY, STR_JSON_O, STR_NULL, RAW_NULL]
        KO_JS = [STR_JSON_A, STR_MISC, STR_OPTR, STR_TSK, STR_TSK_MISC, STR_TSKP, STR_TSKP_MISC, STR_BOOL, STR_TS, STR_GEO, STR_VARBIN, RAW_TSK, RAW_BOOL, RAW_MISC, RAW_OPTR, 
                 RAW_TSK_OPTR, RAW_TSKP, RAW_TSKP_OPTR, STR_INTEGER_P, STR_INTEGER_M, STR_FLOAT_P, STR_FLOAT_M, STR_FLOAT_E_P, STR_FLOAT_E_M, RAW_INTEGER_P, RAW_INTEGER_M, 
                 RAW_FLOAT_P, RAW_FLOAT_M, RAW_FLOAT_E_P, RAW_FLOAT_E_M, STR_CHINESE, STR_NONE, STR_BINARY, RAW_NONE]
        
        TEST = [["' POINT(1.0 1.0)'"]]
        PARAM_LIST = [
                        ["db", "stb_vc", "ctb_vc", "ntb_vc", OK_VC, KO_VC, "\'vc\'", TDDataType.VARCHAR],
                        ["db", "stb_nc", "ctb_nc", "ntb_nc", OK_NC, KO_NC, "\'nc\'", TDDataType.NCHAR],
                        ["db", "stb_bi", "ctb_bi", "ntb_bi", OK_BI, KO_BI, "\'bi\'", TDDataType.BINARY],
                        ["db", "stb_ts", "ctb_ts", "ntb_ts", OK_TS, KO_TS, "now", TDDataType.TIMESTAMP],
                        ["db", "stb_bo", "ctb_bo", "ntb_bo", OK_BO, KO_BO, "true", TDDataType.BOOL],
                        ["db", "stb_vb", "ctb_vb", "ntb_vb", OK_VB, KO_VB, "'hello'", TDDataType.VARBINARY],

                        ["db", "stb_in", "ctb_in", "ntb_in", OK_IN, KO_IN, "-1", TDDataType.INT],
                        ["db", "stb_ui", "ctb_ui", "ntb_ui", OK_UI, KO_UI, "1", TDDataType.UINT],
                        ["db", "stb_bin", "ctb_bin", "ntb_bin", OK_BIN, KO_BIN, "-1", TDDataType.BIGINT],
                        ["db", "stb_bui", "ctb_bui", "ntb_bui", OK_UBINT, KO_UBINT, "1", TDDataType.UBIGINT],
                        ["db", "stb_sin", "ctb_sin", "ntb_sin", OK_SIN, KO_SIN, "-1", TDDataType.SMALLINT],
                        ["db", "stb_sui", "ctb_sui", "ntb_sui", OK_USINT, KO_USINT, "1", TDDataType.USMALLINT],
                        ["db", "stb_tin", "ctb_tin", "ntb_tin", OK_TIN, KO_TIN, "-1", TDDataType.TINYINT],
                        ["db", "stb_tui", "ctb_tui", "ntb_tui", OK_UTINT, KO_UTINT, "1", TDDataType.UTINYINT],

                        ["db", "stb_fl", "ctb_fl", "ntb_fl", OK_FL, KO_FL, "1.0", TDDataType.FLOAT],
                        ["db", "stb_db", "ctb_db", "ntb_db", OK_DB, KO_DB, "1.0", TDDataType.DOUBLE],
                        ["db", "stb_ge", "ctb_ge", "ntb_ge", TEST, KO_GE, "\'POINT(100.0 100.0)\'", TDDataType.GEOMETRY] 
                      ]

        # check with common function
        for _pl in PARAM_LIST:
            self.__insert_query_common(_pl[0], _pl[1], _pl[2], _pl[3], _pl[4], _pl[5], _pl[6], _pl[7])
        # check json
        self.__insert_query_json("db", "stb_js", "ctb_js", OK_JS, KO_JS, "\'{\"k1\":\"v1\",\"k2\":\"v2\"}\'")

    def __insert_query_ts5184(self, dbname="db", stbname="stb_ts5184", ctbname="ctb_ts5184", ntbname="ntb_ts5184"):
        TB_LIST = [ ctbname, ntbname]
        tdSql.execute(f'create table {dbname}.{stbname} (ts timestamp, w_ts timestamp, opc nchar(100),quality int) tags(t0 int);')
        tdSql.execute(f'create table {dbname}.{ntbname} (ts timestamp, w_ts timestamp, opc nchar(100),quality int);')
        tdSql.execute(f'create table {dbname}.{ctbname} using {dbname}.{stbname} tags(1);')
        for _tb in TB_LIST:
            tdSql.execute(f'insert into {dbname}.{_tb} values(1721265436000,now(),"0",192) {dbname}.{_tb}(quality,w_ts,ts) values(192,now(),1721265326000) {dbname}.{_tb}(quality,w_ts,ts) values(190,now()+1s,1721265326000) {dbname}.{_tb} values(1721265436000,now()+2s,"1",191) {dbname}.{_tb}(quality,w_ts,ts) values(192,now()+3s,1721265326002) {dbname}.{_tb}(ts,w_ts,opc,quality) values(1721265436003,now()+4s,"3",193);');
            tdSql.query(f'select * from {dbname}.{_tb}', show=True)
            tdSql.checkRows(4)

    def run(self):
        self.__insert_query_exec()
        self.__insert_query_ts5184()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
