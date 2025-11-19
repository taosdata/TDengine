from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestJoinNsDb:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_ns_db(self):
        """Join with ns precision

        1. Create two databases with ns precision
        2. Create stable and child table in two databases
        3. Insert data into two child tables with same timestamps
        4. Inner join two tables from two databases on timetruncate(ts) and tag columns
        5. Check join result rows is correct

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TS-6319

        History:
            - 2025-5-11 Ethan Liu add for test ns precision db join

        """

        tdSql.execute(f"create database if not exists sys_log PRECISION 'ns'")
        tdSql.execute(f"create database if not exists alarm_log PRECISION 'ns'")

        tdSql.execute(f"use sys_log")
        tdSql.execute(f"create stable if not exists syslog (ts timestamp, log_level int) TAGS(deviceid varchar(5))")
        tdSql.execute(f"create table if not exists syslog_d1 using syslog tags('d1')")
        tdSql.execute(f"insert into syslog_d1 values(1749635944420000000, 1)")
 
        tdSql.execute(f"use alarm_log")
        tdSql.execute(f"create stable if not exists syslog (ts timestamp, alarmid int) TAGS(deviceid varchar(5))")
        tdSql.execute(f"create table if not exists syslog_d1 using syslog tags('d1')")
        tdSql.execute(f"insert into syslog_d1 values(1749635944420000483, 7)")

        tdSql.execute(f"select timetruncate(sl.ts,1a)/1000000 AS ts, sl.log_level from sys_log.syslog sl INNER JOIN alarm_log.syslog vl ON timetruncate(sl.ts,1a)=timetruncate(vl.ts,1a) AND sl.deviceid = vl.deviceId WHERE vl.alarmId = 7")
        tdSql.checkRows(1)