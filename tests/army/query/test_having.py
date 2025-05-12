from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
import random
import string

"""
    TD-32198: https://jira.taosdata.com:18080/browse/TD-32198
    Having关键字的专项测试,主要覆盖以下 4 种场景：
        1、普通聚合查询
        2、关联查询
        3、窗口切分查询
        4、流计算中的窗口切分查询
"""


class TDTestCase(TBase):

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def prepare_global_data(self):
        tdSql.execute("DROP DATABASE IF EXISTS db_td32198;")
        tdSql.execute("create database db_td32198;")
        tdSql.execute("use db_td32198;")

    def prepare_agg_data(self):
        # database for case TD-32198
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_1` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")
        # tdSql.execute("CREATE TABLE `ct_2` USING `meters` (`groupid`, `location`) TAGS (2, 'shanghai');")

        data = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 0, 0.4226180),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 2, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070),
            ('2020-06-01 02:30:00.000', 2.3981330, 12, 0.7760440),
            ('2020-06-01 02:45:00.000', 2.4574561, 14, 0.8291520),
            ('2020-06-01 03:00:00.000', 2.6980760, 14, 0.8760250),
            ('2020-06-01 03:15:00.000', 2.8189230, 10, 0.9063080),
            ('2020-06-01 03:30:00.000', 2.8190780, 6, 0.9396930),
            ('2020-06-01 03:45:00.000', 2.8977780, 10, 0.9859260),
            ('2020-06-01 04:00:00.000', 2.9544230, 4, 1.0048079),
            ('2020-06-01 04:15:00.000', 2.9885840, 14, 1.0061949),
            ('2020-06-01 04:30:00.000', 3.0999999, 6, 1.0200000),
            ('2020-06-01 04:45:00.000', 3.0885839, 10, 1.0161951),
            ('2020-06-01 05:00:00.000', 2.9544230, 18, 0.9848080),
            ('2020-06-01 05:15:00.000', 2.9977770, 2, 0.9859260),
            ('2020-06-01 05:30:00.000', 2.8190780, 0, 0.9496930),
            ('2020-06-01 05:45:00.000', 2.7189231, 18, 0.9163080),
            ('2020-06-01 06:00:00.000', 2.5980761, 10, 0.8860250)
        ]

        sql = "insert into ct_1 values";
        for t in data:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("sql: %s" % sql)

    def test_agg_having(self):
        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage;")
        tdSql.checkRows(8)
        tdSql.checkData(7, 2, 7)
        tdSql.checkData(7, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having count(voltage)>=4;");
        tdSql.checkRows(3)
        tdSql.checkData(2, 2, 7)
        tdSql.checkData(2, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having count(current)>=4;");
        tdSql.checkRows(3)
        tdSql.checkData(2, 2, 7)
        tdSql.checkData(2, 1, 126)

        tdSql.query("select voltage, sum(voltage), count(*) from ct_1 group by voltage having voltage >=14;");
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(1, 1, 126)

        tdSql.error("select voltage, count(*) from ct_1 group by voltage having current >1.0260600;");

    def prepare_join_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_join_1` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")
        tdSql.execute("CREATE TABLE `ct_join_2` USING `meters` (`groupid`, `location`) TAGS (2, 'shanghai');")

        # insert data for ts4806
        data_join_1 = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 10, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 0, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 2, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 6, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        data_join_2 = [
            ('2020-06-01 00:00:00.000', 0.3614670, 9, 0.0200000),
            ('2020-06-01 00:15:00.000', 0.1000000, 12, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 15, 0.1736480),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 01:00:00.000', 1.0260600, 15, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 7, 0.4226180),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 7, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.0260600, 13, 0.6527880),
            ('2020-06-01 02:15:00.000', 0.5209450, 18, 0.7271070)
        ]

        sql = "insert into ct_join_1 values";
        for t in data_join_1:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("ct_join_1 sql: %s" % sql)

        sql = "insert into ct_join_2 values";
        for t in data_join_2:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("ct_join_2 sql: %s" % sql)

    def test_join_having(self):
        tdSql.query("SELECT a.voltage, count(*) FROM ct_join_1 a JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having count(*) > 4;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 0, 18)

        tdSql.error("SELECT a.voltage, count(*) FROM ct_join_1 a JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having b.voltage > 14;")

        tdSql.query("SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having count(*) > 4;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 0, 18)

        tdSql.error("SELECT a.voltage, count(*) FROM ct_join_1 a left JOIN ct_join_2 b ON a.ts = b.ts \
                    group by a.voltage having b.voltage > 14;");

        tdSql.query("SELECT a.ts, a.voltage, avg(b.voltage) FROM ct_join_2 a LEFT WINDOW JOIN ct_join_1 b \
                    WINDOW_OFFSET(-15m, 15m) where a.voltage >=18 and b.voltage > 11 having avg(b.voltage) > 17;");
        tdSql.checkRows(1)

        tdSql.error("SELECT a.ts, a.voltage, avg(b.voltage) FROM ct_join_2 a LEFT WINDOW JOIN ct_join_1 b \
                    WINDOW_OFFSET(-15m, 15m) where a.voltage >=18 and b.voltage > 11 having b.voltage > 17;");

    def prepare_window_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_win` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")

        # insert data for ts4806
        data_win = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 18, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 14, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 12, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        sql = "insert into ct_win values";
        for t in data_win:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("data_win sql: %s" % sql)

    def test_window_having(self):
        tdSql.query("SELECT _WSTART, _WEND, COUNT(*) FROM ct_win INTERVAL(15m) having count(*) > 1;")
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)

        tdSql.error("SELECT _WSTART, _WEND, COUNT(*) FROM ct_win INTERVAL(15m) having voltage > 12;");

        tdSql.query("SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM ct_win \
                    STATE_WINDOW(voltage) having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)

        tdSql.error("SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM ct_win \
                    STATE_WINDOW(voltage) having phase > 0.26;");

        tdSql.query("SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM ct_win SESSION(ts, 10m) having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 5)

        tdSql.error("SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM ct_win SESSION(ts, 10m) having voltage > 12;");

        tdSql.query("select _wstart, _wend, count(*), first(voltage), last(voltage) from ct_win \
                    event_window start with voltage <= 12 end with voltage >= 17 having count(*) > 3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, 18)

        tdSql.error("select _wstart, _wend, count(*) from ct_win \
            event_window start with voltage <=12 end with voltage >= 17 having phase > 0.2;");

        tdSql.query(
            "select _wstart, _wend, count(*), sum(voltage) from ct_win count_window(4) having sum(voltage) > 57;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 61)

        tdSql.error("select _wstart, _wend, count(*), sum(voltage) from ct_win count_window(4) having voltage > 12;");


    def prepare_stream_window_data(self):
        # super table
        tdSql.execute("DROP STABLE IF EXISTS meters")
        tdSql.execute("CREATE STABLE `meters` (`ts` TIMESTAMP , `current` FLOAT , `voltage` INT , `phase` FLOAT ) \
            TAGS (`groupid` TINYINT, `location` VARCHAR(16));")

        # child table
        tdSql.execute("CREATE TABLE `ct_steam_win` USING `meters` (`groupid`, `location`) TAGS (1, 'beijing');")

        # insert data for ts4806
        data_win = [
            ('2020-06-01 00:00:00.000', 0.1000000, 12, 0.0200000),
            ('2020-06-01 00:10:00.000', 1.2278550, 9, 0.4226180),
            ('2020-06-01 00:15:00.000', 0.3614670, 18, 0.1071560),
            ('2020-06-01 00:30:00.000', 0.5209450, 18, 0.1736480),
            ('2020-06-01 00:40:00.000', 1.5230000, 18, 0.5200000),
            ('2020-06-01 00:45:00.000', 0.8764570, 18, 0.2588190),
            ('2020-06-01 00:50:00.000', 1.6507290, 11, 0.5835760),
            ('2020-06-01 01:00:00.000', 1.0260600, 14, 0.3620200),
            ('2020-06-01 01:15:00.000', 1.3678550, 14, 0.4226180),
            ('2020-06-01 01:20:00.000', 1.1213200, 13, 0.7271070),
            ('2020-06-01 01:30:00.000', 1.6000000, 12, 0.5200000),
            ('2020-06-01 01:45:00.000', 1.8207290, 12, 0.5835760),
            ('2020-06-01 02:00:00.000', 1.9283630, 18, 0.6527880),
            ('2020-06-01 02:05:00.000', 0.9283630, 18, 0.6527880),
            ('2020-06-01 02:15:00.000', 2.1213200, 18, 0.7271070)
        ]

        sql = "insert into ct_win values";
        for t in data_win:
            sql += "('{}', {}, {}, {}),".format(t[0], t[1], t[2], t[3])
        sql += ";"
        tdSql.execute(sql)
        tdLog.debug("data_win sql: %s" % sql)

    # 支持会话窗口、状态窗口、滑动窗口、事件窗口和计数窗口，
    # 其中，状态窗口、事件窗口 和 计数窗口 搭配超级表时必须与 partition by tbname 一起使用
    def test_stream_window_having(self):
        tdSql.execute("CREATE STREAM streams0 fill_history 1 INTO streamt0 AS \
            SELECT _WSTART, _WEND, COUNT(*) FROM meters PARTITION BY tbname INTERVAL(15m) having count(*) > 1;")
        tdSql.query("select * from streamt0;");
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 2)

        tdSql.error("CREATE STREAM streams10 fill_history 1 INTO streamt10 AS SELECT _WSTART, _WEND, COUNT(*) \
            FROM meters PARTITION BY tbname INTERVAL(15m) having voltage > 12;");


        tdSql.execute("CREATE STREAM streams1 fill_history 1 INTO streamt1 AS \
            SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM meters PARTITION BY tbname \
                    STATE_WINDOW(voltage) having count(*) > 3;");
        tdSql.query("select * from streamt1;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)

        tdSql.error("CREATE STREAM streams11 fill_history 1 INTO streamt11 AS \
            SELECT _wstart, _wend, COUNT(*) AS cnt, FIRST(ts) AS fst, voltage FROM meters PARTITION BY tbname \
                    STATE_WINDOW(voltage) having phase > 0.26;");


        tdSql.execute("CREATE STREAM streams2 fill_history 1 INTO streamt2 AS \
            SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM meters SESSION(ts, 10m) having count(*) > 3;");
        tdSql.query("select * from streamt2;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 5)

        tdSql.error("CREATE STREAM streams12 fill_history 1 INTO streamt12 AS \
            SELECT _wstart, _wend, COUNT(*), FIRST(ts) FROM meters SESSION(ts, 10m) having voltage > 12;");

        tdSql.execute("CREATE STREAM streams3 fill_history 1 INTO streamt3 AS \
            select _wstart, _wend, count(*), first(voltage), last(voltage) from meters PARTITION BY tbname \
                    event_window start with voltage <= 12 end with voltage >= 17 having count(*) > 3;");
        tdSql.query("select * from streamt3;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 7)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, 18)

        tdSql.error("CREATE STREAM streams13 fill_history 1 INTO streamt13 AS \
            select _wstart, _wend, count(*), first(voltage), last(voltage) from meters PARTITION BY tbname \
                    event_window start with voltage <= 12 end with voltage >= 17 having phase > 0.2;");

        tdSql.execute("CREATE STREAM streams4 fill_history 1 INTO streamt4 AS \
            select _wstart, _wend, count(*), sum(voltage) from meters  PARTITION BY tbname \
                      count_window(4) having sum(voltage) > 57;");
        tdSql.query("select * from streamt4;");
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 4)
        tdSql.checkData(0, 3, 61)

        tdSql.error("CREATE STREAM streams14 fill_history 1 INTO streamt14 AS \
            select _wstart, _wend, count(*), sum(voltage) from meters  PARTITION BY tbname \
                      count_window(4) having voltage > 12;");



    def run(self):
        self.prepare_global_data()

        self.prepare_agg_data()
        self.test_agg_having()

        self.prepare_join_data()
        self.test_join_having()

        self.prepare_window_data()
        self.test_window_having()

        '''
        self.prepare_stream_window_data()
        self.test_stream_window_having()
        '''

    def stop(self):
        tdSql.execute("drop database db_td32198;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
