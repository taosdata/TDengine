import taos
import sys
import time
import socket
import os
import threading

from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql

class TestTS_3404:
    hostname = socket.gethostname()

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    #
    # ------------------- test_TS_3404.py ----------------
    #
    def create_tables(self):
        tdSql.execute(f"CREATE STABLE `stb5` (`ts` TIMESTAMP, `ip_value` FLOAT, `ip_quality` INT) TAGS (`t1` INT)")
        tdSql.execute(f"CREATE TABLE `t_11` USING `stb5` (`t1`) TAGS (1)")

    def insert_data(self):
        tdLog.debug("start to insert data ............")

        sql = "INSERT INTO `t_11` VALUES ('2023-05-10 09:30:47.722', 10.30000, 100)"
        sql += ", ('2023-05-10 09:30:56.383', 12.30000, 100)"
        sql += ", ('2023-05-10 09:48:55.778', 13.30000, 100)"
        sql += ", ('2023-05-10 09:51:50.821', 9.30000,  100)"
        sql += ", ('2023-05-10 09:58:07.162', 9.30000,  100)"
        sql += ", ('2023-05-10 13:41:16.075', 9.30000,  100)"
        sql += ", ('2023-05-13 14:12:58.318', 21.00000, 100)"
        sql += ", ('2023-05-13 14:13:21.328', 1.10000,  100)"
        sql += ", ('2023-05-13 14:35:24.258', 1.30000,  100)"
        sql += ", ('2023-05-13 16:56:49.033', 1.80000,  100)"
        tdSql.execute(sql)

    def do_ts_3404(self):
        tdSql.prepare()
        self.create_tables()
        self.insert_data()
        tdLog.printNoPrefix("======== test TS-3404")

        tdSql.query(f"select _irowts, interp(ip_value) from t_11 range('2023-05-13 14:00:00', '2023-05-13 15:00:00') every(300s) fill(linear);")
        tdSql.checkRows(13)

        tdSql.checkData(0,  0, '2023-05-13 14:00:00.000')
        tdSql.checkData(1,  0, '2023-05-13 14:05:00.000')
        tdSql.checkData(2,  0, '2023-05-13 14:10:00.000')
        tdSql.checkData(3,  0, '2023-05-13 14:15:00.000')
        tdSql.checkData(4,  0, '2023-05-13 14:20:00.000')
        tdSql.checkData(5,  0, '2023-05-13 14:25:00.000')
        tdSql.checkData(6,  0, '2023-05-13 14:30:00.000')
        tdSql.checkData(7,  0, '2023-05-13 14:35:00.000')
        tdSql.checkData(8,  0, '2023-05-13 14:40:00.000')
        tdSql.checkData(9,  0, '2023-05-13 14:45:00.000')
        tdSql.checkData(10, 0, '2023-05-13 14:50:00.000')
        tdSql.checkData(11, 0, '2023-05-13 14:55:00.000')
        tdSql.checkData(12, 0, '2023-05-13 15:00:00.000')

        tdSql.checkData(0,  1, 20.96512)
        tdSql.checkData(1,  1, 20.97857)
        tdSql.checkData(2,  1, 20.99201)
        tdSql.checkData(3,  1, 1.114917)
        tdSql.checkData(4,  1, 1.160271)
        tdSql.checkData(5,  1, 1.205625)
        tdSql.checkData(6,  1, 1.250978)
        tdSql.checkData(7,  1, 1.296333)
        tdSql.checkData(8,  1, 1.316249)
        tdSql.checkData(9,  1, 1.333927)
        tdSql.checkData(10, 1, 1.351607)
        tdSql.checkData(11, 1, 1.369285)
        tdSql.checkData(12, 1, 1.386964)

        print("do TS-3404 ............................ [passed]")

    #
    # ------------------- interp with a bound parameter ----------------
    #
    def do_interp_stmt(self):
        # A prepared statement keeps its parsed AST in pPrepareRoot and
        # translates a clone of it on every bind, so the RANGE / EVERY /
        # FILL clauses have to survive that clone. When they do not,
        # prepare succeeds and bind fails with "Missing RANGE clause,
        # EVERY clause or FILL clause", while the same query without a
        # placeholder works. STMT and STMT2 clone in two different places,
        # so both are exercised here.
        dbname = "interp_stmt_db"
        tdSql.prepare(dbname, drop=True)
        tdSql.execute("create table ct1 (ts timestamp, val double)")
        tdSql.execute(
            "insert into ct1 values"
            " ('2026-08-12 17:34:00', 1.0)"
            " ('2026-08-12 17:36:00', 3.0)"
            " ('2026-08-12 17:38:00', 5.0)"
        )

        # reference result, no placeholder and therefore no clone
        tdSql.query(
            "select _irowts, interp(val) from ct1 where val > 0"
            " range('2026-08-12 17:34:00', '2026-08-12 17:38:00')"
            " every(60s) fill(linear)"
        )
        tdSql.checkRows(5)

        query = (
            "select _irowts, interp(val) from ct1 where val > ?"
            " range('2026-08-12 17:34:00', '2026-08-12 17:38:00')"
            " every(60s) fill(linear)"
        )

        conn = taos.connect()
        try:
            conn.select_db(dbname)

            # STMT binds through qStmtBindParams
            stmt = conn.statement(query)
            params = taos.new_bind_params(1)
            params[0].double(0.0)
            stmt.bind_param(params)
            stmt.execute()
            rows = stmt.use_result().fetch_all()
            assert len(rows) == 5, f"stmt: expect 5 rows, got {len(rows)}"
            assert rows[0][1] == 1.0, f"stmt: expect 1.0, got {rows[0][1]}"
            assert rows[4][1] == 5.0, f"stmt: expect 5.0, got {rows[4][1]}"
            stmt.close()

            # STMT2 binds through qStmtBindParams2, a second clone site
            stmt2 = conn.statement2(query)
            stmt2.bind_param(None, None, [[[0.0]]])
            stmt2.execute()
            rows = stmt2.result().fetch_all()
            assert len(rows) == 5, f"stmt2: expect 5 rows, got {len(rows)}"
            assert rows[0][1] == 1.0, f"stmt2: expect 1.0, got {rows[0][1]}"
            assert rows[4][1] == 5.0, f"stmt2: expect 5.0, got {rows[4][1]}"
            stmt2.close()
        finally:
            conn.close()

        tdSql.execute(f"drop database {dbname}")
        print("do interp stmt ........................ [passed]")

    #
    # ------------------- main ----------------
    #
    def test_query_inerp_bugs(self):
        """Interp bugs

        1. Verify bug TS-3404 (timestamp precision cause wrong window function result)
        2. Verify interp with a bound parameter keeps its RANGE/EVERY/FILL clauses

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:
            - 2025-10-31 Alex Duan Migrated from uncatalog/system-test/99-TDcase/test_TS_3404.py
            - 2026-08-13 clone of the parsed AST dropped the interp clauses

        """
        self.do_ts_3404()
        self.do_interp_stmt()
    