from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestSession:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_session(self):
        """Session Window

        1. -

        Catalog:
            - Timeseries:EventWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/session.sim

        """

        vgroups = 4
        dbNamme = "d0"

        tdLog.info(f"====> create database {dbNamme} vgroups {vgroups}")
        tdSql.execute(f"create database {dbNamme} vgroups {vgroups}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)} {tdSql.getData(1,6)} {tdSql.getData(1,7)} {tdSql.getData(1,8)} {tdSql.getData(1,9)}"
        )
        # print $tdSql.getData(2,0) $tdSql.getData(2,1) $tdSql.getData(2,2) $tdSql.getData(2,3) $tdSql.getData(2,4) $tdSql.getData(2,5) $tdSql.getData(2,6) $tdSql.getData(2,7) $tdSql.getData(2,8) $tdSql.getData(2,9)

        tdSql.execute(f"use {dbNamme}")

        tdLog.info(f"=============== create super table, child table and insert data")
        tdSql.execute(
            f"create table if not exists st (ts timestamp, tagtype int) tags(dev nchar(50), tag2 binary(16))"
        )
        tdSql.execute(
            f'create table if not exists dev_001 using st tags("dev_01", "tag_01")'
        )
        tdSql.execute(
            f'create table if not exists dev_002 using st tags("dev_02", "tag_02")'
        )

        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:00.000', 1)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:00.005', 2)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:00.011', 3)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:01.011', 4)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:01.611', 5)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:00:02.612', 6)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:01:02.612', 7)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:02:02.612', 8)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 10:03:02.613', 9)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 11:00:00.000', 10)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 12:00:00.000', 11)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-13 13:00:00.001', 12)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-14 13:00:00.001', 13)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-15 14:00:00.000', 14)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-20 10:00:00.000', 15)")
        tdSql.execute(f"INSERT INTO dev_001 VALUES('2020-05-27 10:00:00.001', 16)")

        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.000', 1)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.005', 2)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.009', 3)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.0021', 4)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.031', 5)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.036', 6)")
        tdSql.execute(f"INSERT INTO dev_002 VALUES('2020-05-13 10:00:00.51', 7)")

        # vnode does not return the precision of the table
        tdSql.execute(f"create database d1 precision 'us'")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table dev_001 (ts timestamp ,i timestamp ,j int)")
        tdSql.execute(
            f"insert into dev_001 values(1623046993681000,now,1)(1623046993681001,now+1s,2)(1623046993681002,now+2s,3)(1623046993681004,now+5s,4)"
        )
        tdSql.execute(f"create table secondts(ts timestamp,t2 timestamp,i int)")
        tdSql.execute(
            f"insert into secondts values(1623046993681000,now,1)(1623046993681001,now+1s,2)(1623046993681002,now+2s,3)(1623046993681004,now+5s,4)"
        )

        self.query()
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        self.query()

    def query(self):
        
        dbNamme = "d0"
        tdSql.execute(f"use {dbNamme}")

        # session(ts,5a)
        tdLog.info(f"====> select count(*) from dev_001 session(ts,5a)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,5a)")
        tdLog.info(f"====> rows: {tdSql.getRows()})")
        
        tdSql.checkRows(15)

        tdSql.checkData(0, 1, 2)

        #
        # print ====> select count(*) from (select * from dev_001) session(ts,5a)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,5a)
        # if $rows != 15 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 2 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1s)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1s)")
        tdSql.checkRows(12)

        tdSql.checkData(0, 1, 5)

        # print ====> select count(*) from (select * from dev_001) session(ts,1s)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,1s)
        # if $rows != 12 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 5 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1000a)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1000a)")
        tdSql.checkRows(12)

        tdSql.checkData(0, 1, 5)

        # print ====> select count(*) from  (select * from dev_001) session(ts,1000a)
        # sql select _wstart, count(*) from  (select * from dev_001) session(ts,1000a)
        # if $rows != 12 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 5 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1m)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1m)")
        tdSql.checkRows(9)

        tdSql.checkData(0, 1, 8)

        # print ====> select count(*) from (select * from dev_001) session(ts,1m)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,1m)
        # if $rows != 9 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 8 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1h)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1h)")
        tdSql.checkRows(6)

        tdSql.checkData(0, 1, 11)

        # print ====> select count(*) from (select * from dev_001) session(ts,1h)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,1h)
        # if $rows != 6 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 11 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1d)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1d)")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 13)

        # print ====> select count(*) from (select * from dev_001) session(ts,1d)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,1d)
        # if $rows != 4 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 13 then
        #  return -1
        # endi

        tdLog.info(f"====> select count(*) from dev_001 session(ts,1w)")
        tdSql.query(f"select _wstart, count(*) from dev_001 session(ts,1w)")
        tdSql.checkRows(2)

        tdSql.checkData(0, 1, 15)

        # print ====> select count(*) from (select * from dev_001) session(ts,1w)
        # sql select _wstart, count(*) from (select * from dev_001) session(ts,1w)
        # if $rows != 2 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 15 then
        #  return -1
        # endi

        # print ====> leastsquares not supported yet.
        # print ====> select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1),spread(tagtype),stddev(tagtype),percentile(tagtype,0)  from dev_001 where ts <'2020-05-20 0:0:0' session(ts,1d)
        # sql select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1),spread(tagtype),stddev(tagtype),percentile(tagtype,0)  from dev_001 where ts <'2020-05-20 0:0:0' session(ts,1d)
        # if $rows != 2 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,3) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 7 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5) != 91 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,8) != @{slop:1.000000, intercept:0.000000}@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,9) != 12 then
        #  return -1
        # endi
        # $data0-10 != 3.741657387
        # $data0-11 !=  1
        # $data1-11 !=  14

        # print ====> select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1) from (select * from dev_001 where ts <'2020-05-20 0:0:0') session(ts,1d)
        # sql select count(*),first(tagtype),last(tagtype),avg(tagtype),sum(tagtype),min(tagtype),max(tagtype),leastsquares(tagtype, 1, 1) from (select * from dev_001 where ts <'2020-05-20 0:0:0') session(ts,1d)
        # if $rows != 2 then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,3) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 7 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5) != 91 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7) != 13 then
        #  return -1
        # endi
        # if $tdSql.getData(0,8) != @{slop:1.000000, intercept:0.000000}@ then
        #  return -1
        # endi

        tdLog.info(
            f"================> syntax error check not active ================> reactive"
        )
        tdSql.error(f"select * from dev_001 session(ts,1w)")
        tdLog.info(
            f"disable this temporarily, session can not be directly applied to super table."
        )
        # sql_error select count(*) from st session(ts,1w)
        tdSql.error(f"select count(*) from dev_001 group by tagtype session(ts,1w)")
        tdSql.error(f"sql select count(*) from dev_001 session(ts,1n)")
        tdSql.error(f"sql select count(*) from dev_001 session(ts,1y)")
        tdSql.error(f"sql select count(*) from dev_001 session(ts,0s)")
        tdSql.error(f"select count(*) from dev_001 session(i,1y)")
        tdSql.error(
            f"select count(*) from dev_001 session(ts,1d) where ts <'2020-05-20 0:0:0'"
        )

        # print ====> select count(*) from dev_001 session(ts,1u)
        # sql select _wstart, count(*) from dev_001 session(ts,1u)
        # print rows: $rows
        # print $tdSql.getData(0,0) $tdSql.getData(0,1) $tdSql.getData(0,2) $tdSql.getData(0,3)
        # print $tdSql.getData(1,0) $tdSql.getData(1,1) $tdSql.getData(1,2) $tdSql.getData(1,3)
        # print $tdSql.getData(2,0) $tdSql.getData(2,1) $tdSql.getData(2,2) $tdSql.getData(2,3)
        # print $tdSql.getData(3,0) $tdSql.getData(3,1) $tdSql.getData(3,2) $tdSql.getData(3,3)
        # if $rows != 4 then
        #  print expect 2, actual: $rows
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 1 then
        #  return -1
        # endi

        tdSql.error(f"select count(*) from dev_001 session(i,1s)")
        tdSql.error(f"select count(*) from secondts session(t2,2s)")
