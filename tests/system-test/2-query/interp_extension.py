from random import randrange
import time
import threading
import secrets
from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
# from tmqCommon import *

class TDTestCase:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'debugFlag': 143}

    def __init__(self):
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), False)

    def create_database(self, tsql, dbName, dropFlag=1, vgroups=2, replica=1, duration: str = '1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s" % (dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s" % (
            dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s" % (dbName))
        return

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))
        return

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 INT, c4 double, c5 VARCHAR(255))' % (
            db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += f'(%d, %d,%d,%d,{random.random()},"varchar_%d"),' % (start_ts + j * ts_step + randrange(500), j %
                                                     10 + randrange(200), j % 10, j % 10, j % 10 + randrange(100))
        tsql.execute(sql)

    def insert_data(self, tsql, dbName, ctbPrefix, ctbNum, rowsPerTbl, batchNum, startTs, tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" % dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values " % (dbName, ctbPrefix, i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') " % (startTs + j*tsStep + randrange(
                        500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') " % (
                        startTs + j*tsStep + randrange(500), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " % (dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, db: str = 'test', ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts: int = 1537146000000, ts_step: int = 500):
        tdLog.printNoPrefix(
            "======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     db,
                    'dropFlag':   1,
                    'vgroups':    4,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'FLOAT', 'count': 1}, {'type': 'DOUBLE', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'tinyint', 'count': 1}, {'type': 'bool', 'count': 1}, {'type': 'binary', 'len': 10, 'count': 1}, {'type': 'nchar', 'len': 10, 'count': 1}],
                    'tagSchema':   [{'type': 'INT', 'count': 1}, {'type': 'nchar', 'len': 20, 'count': 1}, {'type': 'binary', 'len': 20, 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'DOUBLE', 'count': 1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = ctb_num
        paraDict['rowsPerTbl'] = rows_per_ctb

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"],
                             vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],
                           stbName=paraDict["stbName"], ctbPrefix=paraDict["ctbPrefix"],
                           ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],
                         ctbPrefix=paraDict["ctbPrefix"], ctbNum=paraDict["ctbNum"],
                         rowsPerTbl=paraDict["rowsPerTbl"], batchNum=paraDict["batchNum"],
                         startTs=paraDict["startTs"], tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb',
                            paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def run(self):
        self.init_data()
        self.test_interp_extension()

    def test_interp_fill_extension_near(self, dbname: str, tbname: str):
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)

    def test_interp_extension_irowts_origin(self, dbname: str, tbname: str):
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)

    def test_interp_fill_extension_timepoint_around(self, dbname: str, tbname: str):
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) fill(near, 0, 0)"
        tdSql.query(sql, queryTimes=1)

        ### must specify value
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) fill(near)"
        tdSql.error(sql, -2147473915)
        ### num of fill value mismatch
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) fill(near, 1)"
        tdSql.error(sql, -2147473915)

        ### range with around interval cannot specify two timepoints, currently not supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', '2020-02-01 00:02:00', 1h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        ### NULL/linear cannot specify other values
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        ### cannot have every clause with range around
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', 1h) every(1s) fill(prev, 1, 1)"
        tdSql.error(sql, -2147473827) ## TSDB_CODE_PAR_INVALID_INTERP_CLAUSE

        ### cannot specify near/prev/next values when using range
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from {dbname}.{tbname} range('2020-02-01 00:00:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        ### TODO test range with other units

    def test_interval_fill_extension(self, dbname: str, tbname: str):
        ## not allowed
        sql = f"select count(*) from {dbname}.{tbname} interval(1s) fill(near)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select count(*) from {dbname}.{tbname} interval(1s) fill(prev, 1)"
        tdSql.error(sql, -2147473920) ## syntax error
        sql = f"select count(*) from {dbname}.{tbname} interval(1s) fill(next, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

    def test_interp_extension(self):
        dbname = 'test'
        tbname = 'meters'
        self.test_interp_fill_extension_near(dbname, tbname)
        self.test_interp_extension_irowts_origin(dbname, tbname)
        self.test_interp_fill_extension_timepoint_around(dbname, tbname)
        self.test_interval_fill_extension(dbname, tbname)

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
