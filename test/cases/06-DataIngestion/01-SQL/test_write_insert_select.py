from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestWriteInsertSelect:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_write_insert_select(self):
        """Write from select clause

        1. Insert into select from child table
        2. Insert into select from normal table
        3. Insert into select from super table
        
        Catalog:
            - DataIngestion

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-28 Simon Guan Migrated from tsim/insert/insert_select.sim

        """


        clusterComCheck.checkDnodes(2)
        self.Test1()
        tdStream.dropAllStreamsAndDbs()
        self.Test2()
        tdStream.dropAllStreamsAndDbs()
        self.Test3()
        tdStream.dropAllStreamsAndDbs()
        
    def Test1(self):
        tdLog.info(f"======== step1")
        tdSql.prepare(dbname="db1", vgroups=3)
        tdSql.execute(f"use db1;")
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values ('2022-07-07 10:01:01', 11, 'aaa');")
        tdSql.execute(f"insert into tb1 values ('2022-07-07 11:01:02', 12, 'bbb');")
        tdSql.execute(f"create table tb2 using st1 tags(2);")
        tdSql.execute(f"insert into tb2 values ('2022-07-07 10:02:01', 21, 'aaa');")
        tdSql.execute(f"insert into tb2 values ('2022-07-07 11:02:02', 22, 'bbb');")
        tdSql.execute(f"create table tb3 using st1 tags(3);")
        tdSql.execute(f"insert into tb3 values ('2022-07-07 10:03:01', 31, 'aaa');")
        tdSql.execute(f"insert into tb3 values ('2022-07-07 11:03:02', 32, 'bbb');")
        tdSql.execute(f"create table tb4 using st1 tags(4);")
        tdSql.execute(f"insert into tb4 select * from tb1;")
        tdSql.query(f"select * from tb4;")
        tdSql.checkRows(2)

        tdSql.execute(f"insert into tb4 select ts,f1,f2 from st1;")
        tdSql.query(f"select * from tb4;")
        tdSql.checkRows(6)

        tdSql.execute(
            f"create table tba (ts timestamp, f1 binary(10), f2 bigint, f3 double);"
        )
        tdSql.error(f"insert into tba select * from tb1;")
        tdSql.execute(f"insert into tba (ts,f2,f1) select * from tb1;")
        tdSql.query(f"select * from tba;")
        tdSql.checkRows(2)

        tdSql.execute(
            f"create table tbb (ts timestamp, f1 binary(10), f2 bigint, f3 double);"
        )
        tdSql.execute(f"insert into tbb (f2,f1,ts) select f1+1,f2,ts+3 from tb2;")
        tdSql.query(f"select * from tbb;")
        tdSql.checkRows(2)

        tdLog.info(f"======== step2")
        tdSql.execute(f"drop database if exists db1;")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create table t1(ts timestamp, a int, b int );")
        tdSql.execute(f"create table t2(ts timestamp, a int, b int );")
        tdSql.execute(f"insert into t1 values(1648791211000,1,2);")
        tdSql.execute(f"insert into t2 (ts, b, a) select ts, a, b from t1;")
        tdSql.query(f"select * from t2;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(0, 2, 1)

        tdSql.execute(f"insert into t2 (ts, b, a) select ts + 1, 11, 12 from t1;")
        tdSql.query(f"select * from t2;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(1, 2, 11)

    def Test2(self):
        tdLog.info(f"======== ctb not exists")
        tdSql.prepare(dbname="db2", vgroups=3)
        tdSql.execute(f"use db2;")
        tdSql.execute(
            f"CREATE STABLE IF NOT EXISTS dst_smeters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(groupId INT, location BINARY(24));"
        )
        tdSql.execute(f"CREATE TABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT);")
        tdSql.execute(f"INSERT INTO meters values('2021-04-19 08:00:07', 1, 1, 1)('2021-04-19 08:00:08', 2, 2, 2);")
        tdSql.execute(f"INSERT INTO dst_smeters(tbname, ts, current, voltage, location) select concat(tbname,'_', to_char(ts, 'SS')) as sub_table_name,ts, current, voltage,to_char(ts, 'SS') as location from meters partition by tbname;")
        tdSql.query(f"select * from dst_smeters;")
        tdSql.checkRows(2)
        tdSql.query(f"select location, groupId, ts, current, voltage, phase from meters_07;")
        tdSql.checkData(0, 0, "07")
        tdSql.checkData(0, 1, None)
        tdSql.checkRows(1)
        tdSql.query(f"select location, groupId, ts, current, voltage, phase from meters_08;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "08")
        tdSql.checkData(0, 1, None)


        tdLog.info(f"======== ctb exists")
        tdSql.execute(f"INSERT INTO dst_smeters(tbname, ts, current, voltage,location) select concat(tbname,'_', to_char(ts, 'SS')) as sub_table_name,ts+1000, current, voltage, to_char(ts+10000, 'SS') as location from meters partition by tbname;")
        tdSql.query(f"select location, groupId, ts, current, voltage, phase from meters_08;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "08")
        tdSql.checkData(1, 0, "08")
        tdSql.checkData(0, 1, None)
        tdSql.checkData(1, 1, None)
        tdSql.checkData(0, 2, "2021-04-19 08:00:08")
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 5, None)
        tdSql.checkData(1, 2, "2021-04-19 08:00:09")
        tdSql.checkData(1, 3, 2)
        tdSql.checkData(1, 4, 2)
        tdSql.checkData(1, 5, None)

        tdLog.info(f"======== ctb not exists and no tags")
        tdSql.execute(f"INSERT INTO dst_smeters(tbname, ts, current, voltage)select concat(tbname,'_', to_char(ts+10000, 'SS')) as sub_table_name,ts, current, voltage from meters partition by tbname;")
        tdSql.query(f"select location, groupId, ts, current, voltage, phase from meters_17;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, None)
        tdSql.checkData(0, 2, "2021-04-19 08:00:07")
        tdSql.checkData(0, 3, 1)
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, None)

        tdLog.info(f"======== no tbname")
        tdSql.error(f"INSERT INTO dst_smeters(ts, current, voltage, phase) select ts, current, voltage, phase from meters partition by tbname;")

        tdLog.info(f"======== no pk")
        tdSql.error(f"INSERT INTO dst_smeters(tbname, current, voltage,location) select concat(tbname,'_', to_char(ts, 'SS')) as sub_table_name, current, voltage, to_char(ts+10000, 'SS') as location from meters partition by tbname;")

        tdLog.info(f"======== tbname isn't in first field")
        tdSql.error(f"INSERT INTO dst_smeters(tbname, current, voltage,location) select concat(tbname,'_', to_char(ts, 'SS')) as sub_table_name, current, voltage, to_char(ts+10000, 'SS') as location from meters partition by tbname;")

    def Test3(self):
        tdLog.info(f"======== https://project.feishu.cn/taosdata_td/defect/detail/6570627479")
        tdSql.execute(f"drop database if exists db3;")
        tdSql.execute(f"create database db3 replica 1 vgroups 2 dnodes '1,2';")
        tdSql.execute(f"use db3;")
        tdSql.execute(
            f"CREATE STABLE IF NOT EXISTS dst_smeters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(groupId INT, location BINARY(24));"
        )

        tdSql.execute(f"CREATE TABLE IF NOT EXISTS meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT);")
        tdSql.execute(f"INSERT INTO meters values('2021-04-19 08:00:07', 9, 9, 9)('2021-04-19 08:00:08', 10, 10, 10)('2021-04-19 08:00:07', 9, 9, 9);")
        tdSql.execute(f"INSERT INTO db3.dst_smeters(tbname, groupId, location, ts, current, voltage, phase) select concat(tbname, to_char(ts, 'SS')), 1, 'Beijing', ts, current, voltage,phase as sub_table_name from db3.meters partition by tbname;")
        tdSql.query(f"select tbname, groupId, location, ts, current, voltage, phase from db3.dst_smeters order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "meters07") 
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, "Beijing")
        tdSql.checkData(0, 3, "2021-04-19 08:00:07")
        tdSql.checkData(0, 4, 9)
        tdSql.checkData(0, 5, 9)
        tdSql.checkData(0, 6, 9)
        tdSql.checkData(1, 0, "meters08")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, "Beijing")
        tdSql.checkData(1, 3, "2021-04-19 08:00:08")
        tdSql.checkData(1, 4, 10)
        tdSql.checkData(1, 5, 10)
        tdSql.checkData(1, 6, 10)
        
        tdLog.info(f"======== bugfix:6554558952")
        tdSql.execute(f"create stable ohlcv_1m (ts timestamp,open bigint unsigned,high bigint unsigned,low bigint unsigned,close bigint unsigned,volume bigint unsigned ) tags(symbol varchar(10));")
        tdSql.execute(f"create stable ohlcv_1d (ts timestamp,open bigint unsigned,high bigint unsigned,low bigint unsigned,close bigint unsigned,volume bigint unsigned ) tags(symbol varchar(10));")
        tdSql.execute(f"insert into oh1 using ohlcv_1m tags('AAPL') values('2025-12-01 00:00:00.000',1,1,1,1,1);")
        tdSql.execute(f"insert into oh2 using ohlcv_1m tags('AAPL') values('2025-12-01 00:00:00.000',2,2,2,2,2);")
        tdSql.execute(f" INSERT INTO ohlcv_1d(  tbname,ts)  SELECT concat('t_',tbname) as tb,  '2025-12-01T00:00:00.000-05:00' FROM ohlcv_1m WHERE symbol = 'AAPL' AND ts >= '2025-12-01T00:00:00.000' AND ts < '2025-12-02T00:00:00.000' PARTITION BY tbname, symbol;")
        tdSql.query(f"select tbname,ts from ohlcv_1d order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, "t_oh1")
        tdSql.checkData(0, 1, "2025-12-01 13:00:00.000")
        tdSql.checkData(1, 0, "t_oh2")
        tdSql.checkData(1, 1, "2025-12-01 13:00:00.000")

        tdSql.execute(f"INSERT INTO ohlcv_1d(tbname,ts) SELECT '1.34' as tb,  '2025-12-01T00:00:00.000-05:00' FROM ohlcv_1m WHERE symbol = 'AAPL' AND ts >= '2025-12-01T00:00:00.000' AND ts < '2025-12-02T00:00:00.000' PARTITION BY tbname, symbol;")
        tdSql.error(f"INSERT INTO ohlcv_1d(tbname,ts) SELECT 1.34 as tb,  '2025-12-01T00:00:00.000-05:00' FROM ohlcv_1m WHERE symbol = 'AAPL' AND ts >= '2025-12-01T00:00:00.000' AND ts < '2025-12-02T00:00:00.000' PARTITION BY tbname, symbol;")
