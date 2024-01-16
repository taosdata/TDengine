import datetime
import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf


class TDTestCase:
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def run(self):
        tdSql.execute("create database test_insert_timestamp PRECISION 'ns';")
        tdSql.execute("use test_insert_timestamp;")
        tdSql.execute("create stable st(ts timestamp, c1 int) tags(id int);")
        tdSql.execute("create table test_t using st tags(1);")

        expectErrInfo = "syntax error"
        # abnormal scenario: timestamp + timestamp
        tdSql.error("insert into test_t values(now + today(), 1 );", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(now - today(), 1 );", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(today() + now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(today() - now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + '2023-11-28 00:00:00.000', 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-11-28 00:00:00.000' + 1701111600000, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(1701111500000 + 1701111600000, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: timestamp + interval + interval
        tdSql.error("insert into test_t values(today() + 1d + 1s, 1);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: interval - timestamp
        tdSql.error("insert into test_t values(2h - now(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(2h - today(), 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: interval + interval
        tdSql.error("insert into test_t values(2h - 1h, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(2h + 1h, 1 ); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype - n
        tdSql.error("insert into test_t values(today() + 2n, 7); ", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype - y
        tdSql.error("insert into test_t values(today() - 2y, 8);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: non-support datatype
        tdSql.error("insert into test_t values('a1701619200000', 8);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('ss2023-12-05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(123456, 1);", expectErrInfo="Timestamp data out of range")
        tdSql.error("insert into test_t values(123.456, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(True, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(None, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(null, 1);", expectErrInfo=expectErrInfo, fullMatched=False)

        # abnormal scenario: incorrect format
        tdSql.error("insert into test_t values('2023-122-05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('2023-12--05 00:00:00.000' + '1701619200000', 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values('12/12/2023' + 10a, 1);", expectErrInfo=expectErrInfo, fullMatched=False)
        tdSql.error("insert into test_t values(1701619200000111, 1);", expectErrInfo="Timestamp data out of range", fullMatched=False)

        # normal scenario:timestamp + interval
        tdSql.execute("insert into test_t values(today() + 2b, 1);")
        tdSql.execute("insert into test_t values(1701619200000000000 + 2u, 2);")
        tdSql.execute("insert into test_t values(today + 2a, 3);")
        tdSql.execute("insert into test_t values('2023-12-05 23:59:59.999' + 2a, 4);")
        tdSql.execute("insert into test_t values(1701921599000000000 + 3a, 5);")

        # normal scenario:timestamp - interval
        tdSql.execute("insert into test_t values(today() - 2s, 6);")
        tdSql.execute("insert into test_t values(now() - 2m, 7);")
        tdSql.execute("insert into test_t values(today - 2h, 8);")
        tdSql.execute("insert into test_t values('2023-12-05 00:00:00.000000000' - 2a, 9);")
        tdSql.execute("insert into test_t values(1701669000000000000 - 2a, 10);")

        # normal scenario:interval + timestamp
        tdSql.execute("insert into test_t values(2d + now, 11);")
        tdSql.execute("insert into test_t values(2w + today, 12);")

        # normal scenario:timestamp
        tdSql.execute("insert into test_t values('2023-12-05 00:00:00.000', 13);")
        tdSql.execute("insert into test_t values(1701629100000000000, 14);")
        tdSql.execute("insert into test_t values(now() + 2s, 15);")
        tdSql.execute("insert into test_t values('2023-12-05 00:00:59.999999999+07:00' + 10a, 16);")
        tdSql.execute("insert into test_t values('2023-12-05T00:00:59.110+07:00' + 10a, 17);")
        tdSql.execute("insert into test_t values('2023-12-05' + 10a, 18);")
        tdSql.execute("insert into test_t values('2023-11-15', -15);")
        tdSql.execute("insert into test_t values(1701619200000000000 - 2a, -10);")
        tdSql.execute("insert into test_t values(1701619200000000000, -5);")
        tdSql.execute("insert into test_t values('2023-12-05 12:12:12' + 10a, 19);")

        # data verification
        tdSql.query(f'select ts,c1  from test_t order by c1;')
        tdSql.checkRows(22)
        tdSql.checkEqual(tdSql.queryResult[0][0], 1699977600000000000)     # c1=-15
        tdSql.checkEqual(tdSql.queryResult[1][0], 1701619199998000000)     # c1=-10
        tdSql.checkEqual(tdSql.queryResult[2][0], 1701619200000000000)     # c1=-5
        tdSql.checkEqual(tdSql.queryResult[3][0], self.__get_today_ts() + 2)     # c1=1
        tdSql.checkEqual(tdSql.queryResult[4][0], 1701619200000002000)     # c1=2
        tdSql.checkEqual(tdSql.queryResult[5][0], self.__get_today_ts() + 2000000)     # c1=3
        tdSql.checkEqual(tdSql.queryResult[6][0], 1701792000001000000)     # c1=4
        tdSql.checkEqual(tdSql.queryResult[7][0], 1701921599003000000)     # c1=5
        tdSql.checkEqual(tdSql.queryResult[8][0], self.__get_today_ts() - 2000000000)     # c1=6
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[9][0]), str(datetime.date.today()))    # c1=7
        tdSql.checkEqual(tdSql.queryResult[10][0], self.__get_today_ts() - 7200000000000)    # c1=8
        tdSql.checkEqual(tdSql.queryResult[11][0], 1701705599998000000)    # c1=9
        tdSql.checkEqual(tdSql.queryResult[12][0], 1701668999998000000)    # c1=10
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[13][0]), str(datetime.date.today() + datetime.timedelta(days=2)))    # c1=11
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[14][0]), str(datetime.date.today() + datetime.timedelta(days=14)))    # c1=12
        tdSql.checkEqual(tdSql.queryResult[15][0], 1701705600000000000)    # c1=13
        tdSql.checkEqual(tdSql.queryResult[16][0], 1701629100000000000)    # c1=14
        tdSql.checkEqual(self.__convert_ts_to_date(tdSql.queryResult[17][0]), str(datetime.date.today()))    # c1=15
        tdSql.checkEqual(tdSql.queryResult[18][0], 1701709260009999999)    # c1=16
        tdSql.checkEqual(tdSql.queryResult[19][0], 1701709259120000000)    # c1=17
        tdSql.checkEqual(tdSql.queryResult[20][0], 1701705600010000000)    # c1=18
        tdSql.checkEqual(tdSql.queryResult[21][0], 1701749532010000000)    # c1=19

        tdSql.execute("drop table if exists test_t ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists test_insert_timestamp;")

    def __convert_ts_to_date(self, ts: int) -> str:
        dt_object = datetime.datetime.fromtimestamp(ts / 1e9)

        formatted_date = dt_object.strftime('%Y-%m-%d')

        return formatted_date

    def __get_today_ts(self) -> int:
        return int(time.mktime(time.strptime(str(datetime.date.today()), "%Y-%m-%d"))) * 1000000000

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
