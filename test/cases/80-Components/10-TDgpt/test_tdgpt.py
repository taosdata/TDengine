import time
from new_test_framework.utils import (tdLog, tdSql)


class TestTDgptBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_not_exists_anode(self):
        """TDgpt anode
        
        1. Create anode with wrong address
        2. Expect create failed
        3. Show anodes, expect 0 rows
        4. Drop non-exist anode, expect error

        Since: v3.3.2.16

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-29  Haojun Liao Migrated from tsim/analysis/basic0.sim

        """

        tdSql.error("create anode '127.0.0.1:1101'")

        tdSql.query(f"show anodes")
        tdSql.checkRows(0)

        tdSql.error("drop anode 1")

    def test_analysis(self):
        """TDgpt analysis functions

        1. Create 1 anode
        2. Show anodes expect 1 row
        3. Show anodes full expect 17 rows
        4. Create database d0 with 1 vgroup
        5. Create stable stb with 6 columns and 1 tag
        6. Create child table ct1 using stb with tag value 1000
        7. Insert 17 rows into ct1
        8. Query forecast/anomaly_window on super/child/normal tables
        9. Query forecast with _frowts/_flow/_fhigh
        10. Query forecast with holtwinters/arima/moirai algorithms
        11. Query anomaly_window with iqr/ksigma/lof/shesd/grubbs algorithms
        12. Except query with not exist column
        13. Except query with not supported algorithm

        Since: v3.3.2.16

        Labels: common,ci

        Jira: None

        History:
            - 2025-07-29  Haojun Liao Migrated from tsim/analysis/basic0.sim

        """

        tdSql.execute("create anode '192.168.2.90:6090'")
        tdSql.query("show anodes")

        tdSql.checkRows(1)
        tdLog.info("create anode: 192.168.2.90:6090 successfully")

        tdSql.query("show anodes full")

        # there are 17 analysis models for forecasting or anomaly-detection.
        tdSql.checkRows(17)

        tdSql.execute("create database d0 vgroups 1")
        tdSql.query("select * from information_schema.ins_databases")

        tdSql.checkRows(3)

        tdSql.execute("use d0")

        tdSql.execute("create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double, c4 tinyint, c5 bigint, c6 varchar(12)) tags (t1 int unsigned)")
        tdSql.query("show stables")

        tdSql.checkRows(1)
        tdLog.info("create stable completed")

        tdSql.execute("create table ct1 using stb tags(1000)")
        tdLog.info("insert data")

        #     input_list = [5, 14, 15, 15, 14,    19, 17, 16, 20, 22,   8, 21, 28, 11, 9, 29, 40]
        tdSql.execute("insert into ct1(ts, c1, c2, c3, c4, c5, c6) values(now-1a, 5, 5, 5, 5, 5, 'a')(now+1a, 14, 14, 14, 14, 14, 'a')(now+2a, 15, 15, 15, 15, 15, 'a') ")
        tdSql.execute("insert into ct1 values(now+3a, 15, 15, 15, 15, 15, 'a')(now+4a, 14, 14, 14, 14, 14, 'a')(now+5a, 19, 19, 19, 19, 19, 'a')(now+6a, 17, 17, 17, 17, 17, 'a') ")
        tdSql.execute("insert into ct1 values(now+7a, 16, 16, 16, 16, 16, 'a')")

        tdLog.info("not enough rows case")
        tdSql.error("select forecast(c6, 'algo=holtwinters, rows=10') from ct1")

        tdSql.execute("insert into ct1 values(now+8a, 20, 20, 20, 20, 20, 'a')(now+9a, 22, 22, 22, 22, 22, 'a')")
        tdSql.execute("insert into ct1 values(now+10a, 8, 8, 8, 8, 8, 'a')(now+11a, 21, 21, 21, 21, 21, 'a')(now+12a, 28, 28, 28, 28, 28, 'a')(now+13a, 11, 11, 11, 11, 11, 'a')(now+14a, 9, 9, 9, 9, 9, 'a')")
        tdSql.execute("insert into ct1 values(now+15a, 29, 29, 29, 29, 29, 'a')(now+16a, 40, 40, 40, 40, 40, 'a')")

        tdSql.query("select count(*) from ct1")
        tdSql.checkData(0, 0, 17)


        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=iqr')")
        tdSql.checkData(0, 0, 1)


        tdSql.error("select forecast(c6, 'algo=holtwinters, rows=1025') from ct1")
        tdLog.info("=================  try every loaded anomaly detection algorithm")

        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=iqr')")
        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=ksigma')")
        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=lof')")
        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=shesd')")
        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=grubbs')")

        tdLog.info("=================  try every column type of column")
        tdSql.query("select count(*) from ct1 anomaly_window(c1, 'algo=ksigma,k=2')")
        tdSql.query("select count(*) from ct1 anomaly_window(c2, 'algo=ksigma,k=2')")
        tdSql.query("select count(*) from ct1 anomaly_window(c3, 'algo=ksigma,k=2')")
        tdSql.query("select count(*) from ct1 anomaly_window(c4, 'algo=ksigma,k=2')")
        tdSql.query("select count(*) from ct1 anomaly_window(c5, 'algo=ksigma,k=2')")

        tdLog.info("=================== invalid column type")
        tdSql.error("select count(*) from ct1 anomaly_window(c6, 'algo=ksigma,k=2');")
        tdSql.error("select forecast(c6, 'algo=holtwinters,conf=0.5,wncheck=1,period=0') from ct1")

        tdLog.info("==================== invalid timeout parameter, will reset the parameters.")
        tdSql.query("select forecast(c1, 'algo=holtwinters, timeout=6000') from ct1")
        tdSql.query("select forecast(c1, 'algo=holtwinters, timeout=0') from ct1")

        tdLog.info("=========================== valid timeout")
        tdSql.query("select forecast(c1, 'algo=holtwinters, timeout=120') from ct1;")

        tdSql.error("select forecast(c1, 'conf=0.5 ,algo = arima, rows=0') from ct1")
        tdSql.error("select forecast(c1, 'conf=0.5 ,algo = arima, rows=-10') from ct1")
        tdSql.error("select forecast(c1, 'conf=0.5 ,algo = arima, every=0') from ct1")

        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'algo=holtwinters, conf=0.5 ') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, ' algo=holtwinters , conf=0.5 ') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, ' algo = holtwinters , conf = 0.5 ') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'conf=0.5 ,algo = holtwinters, ') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'conf=0.5 ,algo = holtwinters, ,') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'conf=0.5 ,algo = holtwinters, ,  ,') from ct1")
        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'conf=0.5 ,algo = holtwinters, a    =') from ct1")

        tdSql.error("sql_error select _frowts, _flow, _fhigh, forecast(c1, 'conf=0.5 ,algo = holtwinters,     =   a ,') from ct1")

        tdLog.info("=================== valid column type")
        tdSql.query("select forecast(c1, 'conf=0.5 ,algo = arima') from ct1")
        tdSql.query("select forecast(c1, 'conf=0.5 ,algo = arima, rows=1') from ct1")
        tdSql.query("select forecast(c2, 'conf=0.5 ,algo = arima, rows=1') from ct1")
        tdSql.query("select forecast(c3, 'conf=0.5 ,algo = arima, rows=1') from ct1")
        tdSql.query("select forecast(c4, 'conf=0.5 ,algo = arima, rows=1') from ct1")
        tdSql.query("select forecast(c5, 'conf=0.5 ,algo = arima, rows=1') from ct1")


        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'algo=holtwinters,conf=0.5,wncheck=1,period=0,start=1700000000000,every=2') from ct1")
        tdSql.checkRows(10)

        tdSql.checkData(0, 3, 28.784811943)
        tdSql.checkData(0, 0, '2023-11-15 06:13:20.000')
        tdSql.checkData(1, 0, '2023-11-15 06:13:20.002')
        tdSql.checkData(2, 0, '2023-11-15 06:13:20.004')

        tdLog.info("test the every option and rows option")

        tdSql.query("select _frowts, _flow, _fhigh, forecast(c1, 'algo=holtwinters,conf=0.5,wncheck=1,period=0,start=1700000000000,every=100,rows=5') from ct1")
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, '2023-11-15 06:13:20.000')
        tdSql.checkData(1, 0, '2023-11-15 06:13:20.100')


        tdLog.info("==================== forecast on table with primary key")
        tdSql.execute("create table if not exists pkstb (ts timestamp, c1 int primary key, c2 float, c3 int) tags (t1 int unsigned)")
        tdSql.execute("create table pk_stb_ct1 using pkstb tags(1);")
        tdSql.execute("create table pk_stb_ct2 using pkstb tags(2);")
        tdSql.execute("create table pk_stb_ct3 using pkstb tags(3);")

        tdSql.execute("insert into pk_stb_ct1 values(now, 1, 1.0, 10)")
        tdSql.execute("insert into pk_stb_ct1 values(now+1s, 1, 2.0, 11)")
        tdSql.execute("insert into pk_stb_ct1 values(now+2s, 1, 3.0, 12)")
        tdSql.execute("insert into pk_stb_ct1 values(now+3s, 1, 4.0, 13)")
        tdSql.execute("insert into pk_stb_ct1 values(now+4s, 1, 5.0, 14)")
        tdSql.execute("insert into pk_stb_ct1 values(now+5s, 1, 6.0, 15)")
        tdSql.execute("insert into pk_stb_ct1 values(now+6s, 1, 7.0, 16)")
        tdSql.execute("insert into pk_stb_ct1 values(now+7s, 1, 8.0, 17)")
        tdSql.execute("insert into pk_stb_ct1 values(now+8s, 1, 9.0, 18)")
        tdSql.execute("insert into pk_stb_ct1 values(now+9s, 1, 10.0, 19)")
        tdSql.execute("insert into pk_stb_ct1 values(now+10s, 1, 11.0, 20)")
        tdSql.execute("insert into pk_stb_ct1 values(now+11s, 1, 12.0, 21)")

        tdSql.query("select forecast(c2) from pk_stb_ct1")
        tdSql.query("select forecast(c2, 'algo=arima') from pk_stb_ct1")
        tdSql.error("select forecast(c2, c3) from pk_stb_ct1")
        tdSql.error("select forecast(c2, c3, 'algo=arima') from pk_stb_ct1")


        tdLog.info("==================== co-variate query test and future co-variate query test")

        tdSql.query("select forecast(c2, c3, c1, 'algo=moirai') from pk_stb_ct1")

        tdSql.query("select forecast(c2, c2, c3, 'algo=moirai') from pk_stb_ct1")
        tdSql.query("select forecast(c2, c2, c2, c2, 'algo=moirai') from pk_stb_ct1")
        tdSql.query("select forecast(c2, c2, c2, c2, 'algo=holtwinters') from pk_stb_ct1")

        # not exist column
        tdSql.error("select forecast(c2, c3, c1, c4, 'algo=moirai') from pk_stb_ct1")

        # not support algorithm
        tdSql.error("select forecast(c2, c3, c1, 'algo=holtwinters') from pk_stb_ct1")

        # rows not match
        tdSql.error("select forecast(c2, c3, c1, 'algo=moirai,dynamic_real_1=[1 1 1 1], rows=22') from pk_stb_ct1")

        # missing columns in future dynamic real parameter
        tdSql.error(
            "select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[1 1 1],rows=3,dynamic_real_100_col=c4') from pk_stb_ct1")

        # name mismatch
        tdSql.error("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[1 1 1],rows=3,dynamic_real_100_col=c2') from pk_stb_ct1;")

        # name mismatch
        tdSql.error("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[1 1 1],rows=3,dynamic_real_1_col=c1') from pk_stb_ct1;")

        # invalid input - 1
        tdSql.error("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=(1 1 1),rows=3,dynamic_real_1_col=c1') from pk_stb_ct1;")

        # invalid input - 2
        tdSql.error("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[1, 1, 1],rows=3,dynamic_real_1_col=c1') from pk_stb_ct1;")

        # invalid input - 3
        tdSql.error('select forecast(c2, c3, c1, "algo=moirai, dynamic_real_100=[\'abc\'],rows=1,dynamic_real_1_col=c1") from pk_stb_ct1;')
        tdSql.error('select forecast(c2, c3, c1, "algo=moirai, dynamic_real_100=[1 110 31.92],rows=abc,dynamic_real_x_col=c1") from pk_stb_ct1;')

        tdLog.info("============== future dynamic real column test")

        tdSql.query("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[1 1 1],rows=3,dynamic_real_100_col=c3') from pk_stb_ct1")
        tdSql.checkRows(3)

        tdLog.info("============== too long parameter test")
        tdSql.query("select forecast(c2, c3, c1, 'algo=moirai, dynamic_real_100=[10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000 10000],rows=100,dynamic_real_100_col=c3') from pk_stb_ct1")
        tdSql.checkRows(100)


        tdSql.execute("drop anode 1")
        tdSql.query("show anodes")

        tdSql.checkRows(0)
        time.sleep(1)

        tdSql.error("select forecast(c5, 'conf=0.5 ,algo = arima, rows=1') from ct1")
        tdSql.error("select count(*) from ct1 anomaly_window(c1, 'algo=iqr')")

    def test_corr_table(self):
        """TDgpt corr() on tables
        
        1. corr function query test cases on normal tables or child tables
        2. Insert data into child/normal tables
        3. Query corr() function with different column types
        4. Query corr() function with invalid parameters
        5. Query corr() function with insufficient data

        Since: v3.3.8.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-09-25  Haojun Liao

        """

        tdSql.execute("create database if not exists d1 vgroups 3")
        tdSql.execute("use d1")
        tdSql.execute("create stable st_corr(ts timestamp, a int, b float, c double, d varchar(10), e tinyint, f int unsigned, g bool) tags(t1 int)")
        tdSql.execute("create table t_corr_1 using st_corr tags(1)")
        tdSql.execute("create table t_corr_2 using st_corr tags(2)")
        tdSql.execute("create table t_corr_3 using st_corr tags(3)")

        ## 数组 X‌: [3,5,7,9,11]
        ## 数组 Y‌: [1,3,6,8,10]
        tdSql.execute("insert into t_corr_1 values(now,    1, 2, 5, 'a', 3, 1, 1 )")
        tdSql.execute("insert into t_corr_1 values(now+1a, 2, 4, 4, 'a', 5, 3, 1)")
        tdSql.execute("insert into t_corr_1 values(now+2a, 3, 6, 3, 'a', 7, 6, 1)")
        tdSql.execute("insert into t_corr_1 values(now+3a, 4, 8, 2, 'a', 9, 8, 1)")
        tdSql.execute("insert into t_corr_1 values(now+4a, 5, 10,1, 'a', 11, 10, 1)")

        tdSql.query("select corr(a,b) from t_corr_1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0.999999999)

        tdSql.query("select corr(a, c) from t_corr_1")
        tdSql.checkData(0, 0, -0.99999999)

        tdSql.query("select corr(e, f) from t_corr_1")
        tdSql.checkData(0, 0, 0.997176)

        tdSql.query("select corr(a, cast(g as int)) from t_corr_1")
        tdSql.checkData(0, 0, 0)

        tdSql.query("select corr(1, 1) from t_corr_1")
        tdSql.checkData(0, 0, 0)

        tdSql.query("select corr(a, null) from t_corr_1")
        tdSql.checkData(0, 0, None)

        tdSql.query("select corr(null, a) from t_corr_1")
        tdSql.checkData(0, 0, None)

        tdSql.query("select corr(null, null)")
        tdSql.checkData(0, 0, None)

        tdSql.error("select corr(a) from t_corr_1")
        tdSql.error("select corr(a, d) from t_corr_1")
        tdSql.error("select corr(a, b, b) from t_corr_1")
        tdSql.error("select corr(a, g) from t_corr_1")

        tdSql.execute("drop database d1")

    def test_corr_stable(self):
        """TDgpt corr() on stable

        1. corr function query test cases on super table
        2. Insert data into child tables
        3. Query corr() function with different column types
        4. Query corr() function with invalid parameters
        5. Query corr() function with insufficient data

        
        Since: v3.3.8.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-09-25  Haojun Liao

        """

        tdSql.execute("create database if not exists d1 vgroups 3")
        tdSql.execute("use d1")

        tdSql.execute("create stable st_corr(ts timestamp, a int, b float, c double, d varchar(10), e tinyint, f int unsigned, g bool) tags(t1 int)")
        tdSql.execute("create table t_corr_1 using st_corr tags(1)")
        tdSql.execute("create table t_corr_2 using st_corr tags(2)")
        tdSql.execute("create table t_corr_3 using st_corr tags(3)")

        ## 数组 X‌: [3,5,7,9,11]
        ## 数组 Y‌: [1,3,6,8,10]
        tdSql.execute("insert into t_corr_1 values(now,    1, 2, 5, 'a', 3, 1, 1 )")
        tdSql.execute("insert into t_corr_1 values(now+1a, 2, 4, 4, 'a', 5, 3, 1)")
        tdSql.execute("insert into t_corr_1 values(now+2a, 3, 6, 3, 'a', 7, 6, 1)")
        tdSql.execute("insert into t_corr_1 values(now+3a, 4, 8, 2, 'a', 9, 8, 1)")
        tdSql.execute("insert into t_corr_1 values(now+4a, 5, 10,1, 'a', 11, 10, 1)")

        tdSql.execute("insert into t_corr_2 values(now,    1, 2, 5, 'a', 3, 1, 1 )")
        tdSql.execute("insert into t_corr_2 values(now+1a, 2, 4, 4, 'a', 5, 3, 1)")
        tdSql.execute("insert into t_corr_2 values(now+2a, 3, 6, 3, 'a', 7, 6, 1)")
        tdSql.execute("insert into t_corr_2 values(now+3a, 4, 8, 2, 'a', 9, 8, 1)")
        tdSql.execute("insert into t_corr_2 values(now+4a, 5, 10,1, 'a', 11, 10, 1)")

        tdSql.execute("insert into t_corr_3 values(now,    1, 2, 5, 'a', 3, 1, 1 )")
        tdSql.execute("insert into t_corr_3 values(now+1a, 2, 4, 4, 'a', 5, 3, 1)")
        tdSql.execute("insert into t_corr_3 values(now+2a, 3, 6, 3, 'a', 7, 6, 1)")
        tdSql.execute("insert into t_corr_3 values(now+3a, 4, 8, 2, 'a', 9, 8, 1)")
        tdSql.execute("insert into t_corr_3 values(now+4a, 5, 10,1, 'a', 11, 10, 1)")

        tdSql.query("select corr(a, b) from st_corr")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0.9999999)

    def test_fun_sca_corr(self):
        """ Fun: corr()

        1. Query with int and float data type parameter
        2. Query with constant/null/bool parameter
        3. Query with corr(cast(...
        4. Query on super/child/no table
        
        Catalog:
            - Functions:Scalar

        Since: v3.3.0.0

        Labels: common,ci,ignore

        History:
            - 2025-10-13 Alex Duan add doc

        """
        pass