# Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from taos.error import ProgrammingError
from new_test_framework.utils import tdLog, tdSql


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

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-07-29  Haojun Liao Migrated from tsim/analysis/basic0.sim

        """

        tdSql.error("create anode '127.0.0.1:1101'")

        tdSql.query(f"show anodes")
        tdSql.checkRows(0)

        tdSql.error("drop anode 1")

    def test_anode_url_security(self):
        """CREATE ANODE URL security checks

        Verify that the server rejects:
        1. Non-http/https schemes (file://, ftp://)
        2. Loopback addresses (127.0.0.1, ::1)
        3. RFC-1918 private addresses (10.x, 172.16.x, 192.168.x) and IPv6 ULA (fc00::/7)
        4. Link-local addresses (169.254.x.x, fe80::/10)
        5. Bare host:port without scheme

        Since: v3.3.9.0

        Labels: common,ci,security,functional

        Jira: TD-SEC-2026-038

        History:
            - 2026-09-20 Added for SSRF hardening

        """

        # disable the anodeAllowInternalUrl setting to test the security checks
        tdSql.execute("alter all dnodes 'anodeAllowInternalUrl' '0'")

        try:
            # non-http schemes must be rejected
            tdSql.error("create anode 'file:///etc/passwd'")
            tdSql.error("create anode 'ftp://192.168.1.1:6090'")

            # bare host:port without scheme must be rejected
            tdSql.error("create anode '192.168.1.1:6090'")

            # loopback addresses must be rejected
            tdSql.error("create anode 'http://127.0.0.1:6090'")
            tdSql.error("create anode 'http://127.1.2.3:6090'")
            tdSql.error("create anode 'http://localhost:6090'")
            tdSql.error("create anode 'http://[::1]:6090'")  # IPv6 loopback

            # link-local (cloud metadata endpoint) must be rejected
            tdSql.error("create anode 'http://169.254.169.254:6090'")

            # RFC-1918 private ranges must be rejected by default
            tdSql.error("create anode 'http://10.0.0.1:6090'")
            tdSql.error("create anode 'http://172.16.0.1:6090'")
            tdSql.error("create anode 'http://192.168.1.1:6090'")
            tdSql.error("create anode 'http://[fc00::1]:6090'")  # IPv6 ULA (RFC 4193 private)

            # no anode should have been created
            tdSql.query("show anodes")
            tdSql.checkRows(0)
        finally:
            # restore default configuration (anodeAllowInternalUrl=true) to avoid polluting subsequent tests
            tdSql.execute("alter all dnodes 'anodeAllowInternalUrl' '1'")

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

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-07-29  Haojun Liao Migrated from tsim/analysis/basic0.sim

        """

        tdSql.execute("create anode 'http://192.168.2.90:6090'")
        tdSql.query("show anodes")

        tdSql.checkRows(1)
        tdLog.info("create anode: http://192.168.2.90:6090 successfully")
        tdSql.query("show anodes full")

        # there are 17 analysis models for forecasting or anomaly-detection.
        tdSql.checkRows(17)

        tdSql.execute("create database if not exists d0 vgroups 1")
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

        Labels: common,ci,integration,functional

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

        Labels: common,ci,integration,functional

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

        Labels: common,ci,ignore,integration,functional

        History:
            - 2025-10-13 Alex Duan add doc

        """
        pass
    
    def test_imputation_function(self):
        """TDgpt imputation function on stable child tables.

        Create an anode, register the imputation algorithm, insert sample rows,
        and verify imputation() and related explain queries execute successfully.

        Since: v3.3.0.0

        Labels: common, ci, integration, functional

        Jira: None

        Catalog:
            - Functions:Scalar

        History:
            - 2025-10-13 Alex Duan add doc
        """
        tdSql.execute("create database if not exists imputation vgroups 3")
        tdSql.execute("use imputation")
        
        tdSql.execute("create stable st(ts timestamp, a int, b float, c double, d varchar(10), e tinyint, f int unsigned, g bool) tags(t1 int)")
        tdSql.execute("create table t_1 using st tags(1)")
        tdSql.execute("create table t_2 using st tags(2)")
        tdSql.execute("create table t_3 using st tags(3)")
        
        tdSql.execute("insert into t_1 values(1786565760000, 1, 1.0, 123.1, 'abc', 12, 0, 0)") 
        tdSql.execute("insert into t_1 values(1786566240000, 2, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786566960000, 3, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786567440000, 4, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786568160000, 5, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786568640000, 6, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786569360000, 7, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786569840000, 8, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786570560000, 9, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786571040000, 10, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786571760000, 11, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786572240000, 12, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786572960000, 13, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786573440000, 14, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786574160000, 15, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786574640000, 16, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786575360000, 17, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786575840000, 18, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786576560000, 19, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786577040000, 20, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786577760000, 21, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786578240000, 22, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786578960000, 23, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786579440000, 24, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786580160000, 25, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786580640000, 26, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786581360000, 27, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786581840000, 28, 1.0, 123.1, 'abc', 12, 0, 0)")
        tdSql.execute("insert into t_1 values(1786582560000, 29, 1.0, 123.1, 'abc', 12, 0, 0)")
        
        tdSql.query("show anodes")
        tdSql.checkRows(0)
        
        tdSql.execute("create anode 'http://192.168.2.90:6090'")
        tdLog.info("create anode: http://192.168.2.90:6090 successfully")
        
        tdSql.query("show anodes full")
        tdSql.printResult()
        
        tdSql.checkData(14, 1, "imputation")
        tdSql.checkData(14, 2, "moment")
        tdSql.checkData(14, 3, "READY")
        
        tdSql.query("select _improwts, imputation(a, 'algo=moment, freq=4m') from t_1;")
        tdSql.query("select _improwts, imputation(a, 'algo=moment, freq=4m') + 20 from t_1;")
        
        tdSql.query("explain verbose true select _frowts, forecast(a) from t_1;")
        tdSql.query("explain verbose true select forecast(a)+20 from t_1;")

    def test_show_anodes_full_many_algos(self):
        """show anodes full with more than 100 algorithms

        When an anode reports a large number of algorithms (> SHOW_STEP_SIZE=100),
        mndRetrieveAnodesFull must dynamically expand the result block capacity.
        Without the fix, writing past the pre-allocated 100 rows causes memory
        overflow.

        This test starts a local mock anode HTTP server that returns a JSON
        response containing 113 algorithms across all types, then verifies
        that show anodes full returns all 113 rows without crashing.

        When the license allows a second anode, both anodes are replaced by two
        mock anodes reporting 61 algorithms each: the first one fills part of a
        page and the second one forces the capacity expansion inside the same
        retrieve, so the cross-anode path is covered deterministically. On
        clusters whose anode quota is one, that second part is skipped.

        Since: v3.3.9.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-11-17  Haojun Liao  Added for mndRetrieveAnodesFull overflow fix

        """

        def build_algo(name, builtin=True, status="READY", params=None):
            return {"builtins": builtin, "desc": name, "name": name,
                    "params": params or {}, "status": status}

        def make_algo_list(ad_count, reg_count, cls_count, prefix=""):
            return {
                "forecast": [build_algo(n, status=s) for n, s in [
                    ("arima", "READY"), ("ces", "READY"), ("chronos", "UNAVAIL"),
                    ("ets", "READY"), ("tdtsfm_1", "UNAVAIL"), ("holtwinters", "READY"),
                    ("moirai", "UNAVAIL"), ("prophet", "READY"), ("theta", "READY"),
                    ("timemoe-fc", "UNAVAIL"), ("timesfm", "UNAVAIL"),
                ]],
                "anomaly-detection": (
                    [build_algo(n) for n in ["grubbs", "iqr", "ksigma", "lof"]] +
                    [build_algo(f"{prefix}ud_ad_{i}", builtin=False) for i in range(ad_count)]
                ),
                "imputation": [build_algo("moment", status="UNAVAIL")],
                "correlation": [build_algo("dtw"), build_algo("tlcc")],
                "regression": [build_algo(f"{prefix}ud_reg_{i}", builtin=False) for i in range(reg_count)],
                "classification": [build_algo(f"{prefix}ud_cls_{i}", builtin=False) for i in range(cls_count)],
                "clustering": [build_algo(n) for n in ["dbscan", "gmm", "kmeans"]],
                "series-diagnosis": [],
            }

        def algo_count(algo_list):
            return sum(len(v) for v in algo_list.values())

        def make_list_response(algo_list):
            return {
                "protocol": 1.0, "version": 0.1,
                "details": [{"type": t, "algo": a} for t, a in algo_list.items()],
            }

        status_response = {"protocol": 1.0, "status": "ready"}

        def start_mock_server(list_resp):
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_args):
                    pass

                def reply(self, data):
                    body = json.dumps(data).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def do_GET(self):
                    if self.path.endswith("/status"):
                        self.reply(status_response)
                    else:
                        self.reply(list_resp)

            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            return server

        # --- Case 1: single anode with 113 algos (> SHOW_STEP_SIZE=100) ---
        tdLog.info("=== Case 1: single anode, 113 algos ===")
        algos1 = make_algo_list(ad_count=24, reg_count=44, cls_count=24)
        expected1 = algo_count(algos1)
        assert expected1 > 100, f"expected > 100, got {expected1}"

        srv1 = start_mock_server(make_list_response(algos1))
        url1 = f"http://127.0.0.1:{srv1.server_port}"
        anode_ids = []
        try:
            # clean up any leftover anodes from prior tests
            tdSql.query("show anodes")
            for row in tdSql.queryResult:
                tdSql.execute(f"drop anode {row[0]}")

            tdSql.execute(f"create anode '{url1}'")
            tdSql.query("show anodes")
            tdSql.checkRows(1)
            anode_ids.append(tdSql.queryResult[0][0])

            tdSql.query("show anodes full")
            tdLog.info(f"single anode: show anodes full returned {tdSql.queryRows} rows")
            tdSql.checkRows(expected1)

            # rows follow enum order: anomaly-detection(0) → forecast(1) → imputation(2)
            # → correlation(3) → classification(4) → [motif(5) empty] → regression(6)
            # → clustering(7) → [series-diagnosis(8) empty]
            type_order = [
                ("anomaly-detection", 28),  # 4 builtin + 24 user-defined
                ("forecast",          11),
                ("imputation",         1),
                ("correlation",        2),
                ("classification",    24),
                # motif: 0 algos, no rows
                ("regression",        44),
                ("clustering",         3),
                # series-diagnosis: 0 algos, no rows
            ]

            row_idx = 0
            for type_name, count in type_order:
                for i in range(count):
                    tdSql.checkData(row_idx, 1, type_name)
                    row_idx += 1

            # spot-check first and last algo in key sections
            tdSql.checkData(0,  2, "grubbs")         # first anomaly-detection algo
            tdSql.checkData(3,  2, "lof")             # last builtin anomaly-detection
            tdSql.checkData(4,  2, "ud_ad_0")         # first user-defined anomaly-detection
            tdSql.checkData(28, 2, "arima")            # first forecast algo
            # imputation section starts at row 28+11=39
            tdSql.checkData(39, 2, "moment")           # the only imputation algo
            tdSql.checkData(39, 3, "UNAVAIL")          # moment is unavailable
            tdSql.checkData(40, 2, "dtw")              # first correlation algo
            # last row: clustering section ends at row 113-1=112
            tdSql.checkData(expected1 - 1, 2, "kmeans")
            tdSql.checkData(expected1 - 1, 1, "clustering")

            # --- Case 2: two anodes, 61 algos each (61+61 > 100, cross-page) ---
            # Both anodes must stay below SHOW_STEP_SIZE (100) algorithms, otherwise the first
            # anode fills a whole page on its own and the second one starts with numOfRows == 0,
            # where blockDataEnsureCapacity returns early and the expansion branch is never
            # reached. With 61 each, the first anode writes 61 rows, the loop continues in the
            # same retrieve and the second anode forces the capacity expansion at numOfRows == 61
            # - independent of the SDB hash iteration order. Case 1's 113-algorithm anode is
            # therefore dropped first.
            #
            # The anode quota is licensed: the default grant allows a single anode
            # (GRANT_UNIQ_DFT_TDGPT_ANODE_NUM is 1 when the license carries no TDgpt anode
            # count). Run this case only when the cluster accepts a second anode, skip it
            # otherwise, so environments with a larger quota keep the coverage.
            tdLog.info("=== Case 2: two anodes, 61 algos each ===")
            tdSql.execute(f"drop anode {anode_ids[0]}")
            anode_ids.clear()
            tdSql.query("show anodes")
            tdSql.checkRows(0)

            algos2 = make_algo_list(ad_count=10, reg_count=20, cls_count=10, prefix="b_")
            algos3 = make_algo_list(ad_count=10, reg_count=20, cls_count=10, prefix="c_")
            expected2 = algo_count(algos2)
            expected3 = algo_count(algos3)
            assert expected2 < 100 and expected3 < 100, \
                f"each anode must stay below one page, got {expected2}/{expected3}"
            assert expected2 + expected3 > 100, \
                f"the two anodes must overflow one page, got {expected2 + expected3}"

            srv2 = start_mock_server(make_list_response(algos2))
            srv3 = start_mock_server(make_list_response(algos3))
            url2 = f"http://127.0.0.1:{srv2.server_port}"
            url3 = f"http://127.0.0.1:{srv3.server_port}"
            try:
                skipped = False
                try:
                    tdSql.cursor.execute(f"create anode '{url2}'")
                    tdSql.cursor.execute(f"create anode '{url3}'")
                except ProgrammingError as err:
                    # 0x80000830 is TSDB_CODE_GRANT_ANODE_LIMITED
                    if (err.errno & 0xFFFFFFFF) != 0x80000830:
                        raise
                    tdLog.notice("license allows one anode only, the two-anode case is skipped")
                    skipped = True

                tdSql.query("show anodes")
                # register whatever was created so the outer cleanup drops it, also on skip
                anode_ids.extend(row[0] for row in tdSql.queryResult)
                if not skipped:
                    tdSql.checkRows(2)
                    id_by_url = {row[1]: row[0] for row in tdSql.queryResult}
                    id2 = id_by_url[url2]
                    id3 = id_by_url[url3]

                    tdSql.query("show anodes full")
                    total = expected2 + expected3
                    tdLog.info(f"two anodes: show anodes full returned {tdSql.queryRows} rows, expected {total}")
                    tdSql.checkRows(total)

                    # every row must stay bound to its own anode: the block expansion has to keep
                    # the rows already written for the first anode instead of shifting them
                    counts = {id2: 0, id3: 0}
                    for row in tdSql.queryResult:
                        rid, algo = row[0], row[2]
                        assert rid in (id2, id3), f"unexpected anode id {rid}, expected {id2} or {id3}"
                        assert row[1] != "", f"algo type is empty for row id={rid} — offset corruption?"
                        assert row[2] != "", f"algo name is empty for row id={rid} — offset corruption?"
                        counts[rid] += 1
                        if algo.startswith(("b_", "c_")):
                            owner = id2 if algo.startswith("b_") else id3
                            assert rid == owner, \
                                f"algo {algo} is filed under anode id {rid}, expected {owner} — row misalignment"
                    assert counts[id2] == expected2 and counts[id3] == expected3, \
                        f"rows per anode {counts}, expected {expected2}/{expected3}"
            finally:
                srv3.shutdown()
                srv2.shutdown()
        finally:
            for aid in reversed(anode_ids):
                tdSql.execute(f"drop anode {aid}")
            srv1.shutdown()

        