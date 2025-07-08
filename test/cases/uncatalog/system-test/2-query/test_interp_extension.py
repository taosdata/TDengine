from new_test_framework.utils import tdLog, tdSql
from new_test_framework.utils.common import TDCom, tdCom
import random

import queue
from random import randrange
import time
import threading
import secrets
from datetime import timezone
from tzlocal import get_localzone
# from tmqCommon import *

ROUND: int = 500

class TestInterpExtension:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'debugFlag': 143}
    check_failed: bool = False

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        
        cls.vgroups = 4
        cls.ctbNum = 10
        cls.rowsPerTbl = 10000
        cls.duraion = '1h'

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

    def test_interp_extension(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """

        self.init_data()
        self.check_interp_extension()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

    def datetime_add_tz(self, dt):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=get_localzone())
        return dt

    def binary_search_ts(self, select_results, ts):
        mid = 0
        try:
            found: bool = False
            start = 0
            end = len(select_results) - 1
            while start <= end:
                mid = (start + end) // 2
                if self.datetime_add_tz(select_results[mid][0]) == ts:
                    found = True
                    return mid
                elif self.datetime_add_tz(select_results[mid][0]) < ts:
                    start = mid + 1
                else:
                    end = mid - 1

            if not found:
                tdLog.exit(f"cannot find ts in select results {ts} {select_results}")
            return start
        except Exception as e:
            tdLog.debug(f"{select_results[mid][0]}, {ts}, {len(select_results)}, {select_results[mid]}")
            self.check_failed = True
            tdLog.exit(f"binary_search_ts error: {e}")

    def distance(self, ts1, ts2):
        return abs(self.datetime_add_tz(ts1) - self.datetime_add_tz(ts2))

    ## TODO pass last position to avoid search from the beginning
    def is_nearest(self, select_results, irowts_origin, irowts):
        if len(select_results) <= 1:
            return True
        try:
            #tdLog.debug(f"check is_nearest for: {irowts_origin} {irowts}")
            idx = self.binary_search_ts(select_results, irowts_origin)
            if idx == 0:
                #tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
                if not res:
                    tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            if idx == len(select_results) - 1:
                #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0])
                if not res:
                    tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
            res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0]) and self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
            if not res:
                tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
            return res
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"is_nearest error: {e}")

    ## interp_results: _irowts_origin, _irowts, ..., _isfilled
    ## select_all_results must be sorted by ts in ascending order
    def check_result_for_near(self, interp_results, select_all_results, sql, sql_select_all):
        #tdLog.debug(f"check_result_for_near for sql: {sql}, sql_select_all{sql_select_all}")
        for row in interp_results:
            if row[0].tzinfo is None or row[0].tzinfo.utcoffset(row[0]) is None:
                irowts_origin = row[0].replace(tzinfo=get_localzone())
                irowts = row[1].replace(tzinfo=get_localzone())
            else:
                irowts_origin = row[0]
                irowts = row[1]
            if not self.is_nearest(select_all_results, irowts_origin, irowts):
                self.check_failed = True
                tdLog.exit(f"interp result is not the nearest for row: {row}, {sql}")

    def query_routine(self, sql_queue: queue.Queue, output_queue: queue.Queue):
        try:
            tdcom = TDCom()
            cli = tdcom.newTdSql()
            while True:
                item = sql_queue.get()
                if item is None or self.check_failed:
                    output_queue.put(None)
                    break
                (sql, sql_select_all, _) = item
                cli.query(sql, queryTimes=1)
                interp_results = cli.queryResult
                if sql_select_all is not None:
                    cli.query(sql_select_all, queryTimes=1)
                output_queue.put((sql, interp_results, cli.queryResult, sql_select_all))
            cli.close()
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"query_routine error: {e}")

    def interp_check_near_routine(self, select_all_results, output_queue: queue.Queue):
        try:
            while True:
                item = output_queue.get()
                if item is None:
                    break
                (sql, interp_results, all_results, sql_select_all) = item
                if all_results is not None:
                    self.check_result_for_near(interp_results, all_results, sql, sql_select_all)
                else:
                    self.check_result_for_near(interp_results, select_all_results, sql, None)
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"interp_check_near_routine error: {e}")

    def create_qt_threads(self, sql_queue: queue.Queue, output_queue: queue.Queue, num: int):
        qts = []
        for _ in range(0, num):
            qt = threading.Thread(target=self.query_routine, args=(sql_queue, output_queue))
            qt.start()
            qts.append(qt)
        return qts

    def wait_qt_threads(self, qts: list):
        for qt in qts:
            qt.join()

    ### first(ts)               | last(ts)
    ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
    def check_interp_fill_extension_near(self):
        sql = f"select last(ts), c1, c2 from test.t0"
        tdSql.query(sql, queryTimes=1)
        lastRow = tdSql.queryResult[0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(61)
        for i in range(0, 61):
            tdSql.checkData(i, 0, lastRow[0])
            tdSql.checkData(i, 2, lastRow[1])
            tdSql.checkData(i, 3, lastRow[2])
            tdSql.checkData(i, 4, True)

        sql = f"select ts, c1, c2 from test.t0 where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' order by ts asc"
        tdSql.query(sql, queryTimes=1)
        select_all_results = tdSql.queryResult
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2018-09-17 09:00:00', '2018-09-17 09:00:05') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(6)
        self.check_result_for_near(tdSql.queryResult, select_all_results, sql, None)

        start = 1537146000000
        end = 1537151000000

        tdSql.query("select ts, c1, c2 from test.t0 order by ts asc", queryTimes=1)
        select_all_results = tdSql.queryResult

        qt_threads_num = 4
        sql_queue = queue.Queue()
        output_queue = queue.Queue()
        qts = self.create_qt_threads(sql_queue, output_queue, qt_threads_num)
        ct = threading.Thread(target=self.interp_check_near_routine, args=(select_all_results, output_queue))
        ct.start()
        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            every = random.randint(1, 15)
            #tdLog.debug(f"range_start: {range_start}, range_end: {range_end}")
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_start}, {range_end}) every({every}s) fill(near)"
            sql_queue.put((sql, None, None))

        ### no prev only, no next only, no prev and no next, have prev and have next
        for i in range(0, ROUND):
            range_point = random.randint(start, end)
            ## all data points are can be filled by near
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_point}, 1h) fill(near, 1, 2)"
            sql_queue.put((sql, None, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            every = random.randint(1, 15)
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_start}, {range_end}) every({every}s) fill(near)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            range_point = random.randint(start, end)
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_point}, 1h) fill(near, 1, 2)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))
        for i in range(0, qt_threads_num):
            sql_queue.put(None)
        self.wait_qt_threads(qts)
        ct.join()

        if self.check_failed:
            tdLog.exit("interp check near failed")

    def check_interp_extension_irowts_origin(self):
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)

        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(linear)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL_F)"
        tdSql.error(sql, -2147473833)

    def check_interp_fill_extension(self):
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 0, 0)"
        tdSql.query(sql, queryTimes=1)

        ### must specify value
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near)"
        tdSql.error(sql, -2147473915)
        ### num of fill value mismatch
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 1)"
        tdSql.error(sql, -2147473915)

        ### range with around interval cannot specify two timepoints, currently not supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00', 1h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473827) ## syntax error

        ### NULL/linear cannot specify other values
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        ### cannot have every clause with range around
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) every(1s) fill(prev, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        ### cannot specify near/prev/next values when using range
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        ### when range around interval is set, only prev/next/near is supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL)"
        tdSql.error(sql, -2147473861) ## TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear)"
        tdSql.error(sql, -2147473861) ## TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE

        ### range interval cannot be 0
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 0h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1y) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1n) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters where ts between '2020-02-01 00:00:00' and '2020-02-01 00:00:00' range('2020-02-01 00:00:00', 1h) fill(near, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)

        ### first(ts)               | last(ts)
        ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
        sql = "select to_char(first(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        first_ts = tdSql.queryResult[0][0]
        sql = "select to_char(last(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        last_ts = tdSql.queryResult[0][0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1d) fill(near, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:25:00', 1d) fill(prev, 3, 4)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-18 10:25:00.000')
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 08:25:00', 1d) fill(next, 5, 6)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-16 08:25:00.000')
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 6)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 09:00:01', 1d) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, first_ts)
        tdSql.checkData(0, 1, '2018-09-16 09:00:01')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:23:19', 1d) fill(prev, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, '2018-09-18 10:23:19')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('{last_ts}', 1a) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, last_ts)
        tdSql.checkData(0, 4, False)

    def check_interval_fill_extension(self):
        ## not allowed
        sql = f"select count(*) from test.meters interval(1s) fill(near)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select count(*) from test.meters interval(1s) fill(prev, 1)"
        tdSql.error(sql, -2147473920) ## syntax error
        sql = f"select count(*) from test.meters interval(1s) fill(next, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts_origin, count(*) from test.meters where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' interval(1s) fill(next)"
        tdSql.error(sql, -2147473918) ## invalid column name _irowts_origin

    def check_interp_fill_extension_stream(self):
        ## near is not supported
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(near);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        ## _irowts_origin is not support
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts_origin, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(prev);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(next, 1, 1);"
        tdSql.error(sql, -2147473915) ## cannot specify values

    def check_interp_extension(self):
        self.check_interp_fill_extension_near()
        self.check_interp_extension_irowts_origin()
        self.check_interp_fill_extension()
        self.check_interval_fill_extension()
        self.check_interp_fill_extension_stream()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()
