###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import time

import taos
from util.log import *
from util.cases import *
from util.sql import *


DB_NAME = "test_reload_lc"
STB_NAME = "stb_reload"
CTB_NAME = "ctb_reload"
NTB_NAME = "ntb_reload"


class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), True)

    # ---------------------------------------------------------------
    # helpers
    # ---------------------------------------------------------------

    def _setup_db(self):
        """Create a database with cachemodel 'both' and populate tables."""
        tdSql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        tdSql.execute(
            f"CREATE DATABASE {DB_NAME} CACHEMODEL 'both' KEEP 3650"
        )
        tdSql.execute(f"USE {DB_NAME}")

        # super table
        tdSql.execute(
            f"CREATE STABLE {STB_NAME} (ts TIMESTAMP, c1 INT, c2 FLOAT) "
            f"TAGS (grp INT)"
        )

        # child table
        tdSql.execute(
            f"CREATE TABLE {CTB_NAME} USING {STB_NAME} TAGS(1)"
        )

        # normal table
        tdSql.execute(
            f"CREATE TABLE {NTB_NAME} (ts TIMESTAMP, c1 INT, c2 FLOAT)"
        )

        # insert a few rows so last/last_row caches have something
        for i in range(5):
            tdSql.execute(
                f"INSERT INTO {CTB_NAME} VALUES(NOW+{i}s, {i}, {i*1.1})"
            )
            tdSql.execute(
                f"INSERT INTO {NTB_NAME} VALUES(NOW+{i}s, {i}, {i*1.1})"
            )

    def _get_reload_uid(self):
        """Return the reloadUid from the last query result (first column, first row)."""
        if tdSql.queryRows == 0:
            return None
        return tdSql.queryResult[0][0]

    # ---------------------------------------------------------------
    # test cases
    # ---------------------------------------------------------------

    def test_reload_last_cache_database(self):
        """RELOAD LAST_CACHE ON DATABASE returns a positive integer reloadUid."""
        tdLog.printNoPrefix("=== test_reload_last_cache_database ===")

        tdSql.query(f"RELOAD LAST_CACHE ON DATABASE {DB_NAME}")
        tdSql.checkRows(1)
        uid = self._get_reload_uid()
        if uid is None or uid <= 0:
            tdLog.exit(
                f"Expected positive reloadUid from RELOAD LAST_CACHE ON DATABASE, got: {uid}"
            )
        tdLog.info(f"RELOAD LAST_CACHE ON DATABASE returned reloadUid={uid}")

    def test_reload_last_cache_database_variants(self):
        """All three cache-type variants execute without error on DATABASE."""
        tdLog.printNoPrefix("=== test_reload_last_cache_database_variants ===")

        for sql in [
            f"RELOAD LAST CACHE ON DATABASE {DB_NAME}",
            f"RELOAD LAST_ROW CACHE ON DATABASE {DB_NAME}",
            f"RELOAD LAST_CACHE ON DATABASE {DB_NAME}",
        ]:
            tdSql.query(sql)
            tdSql.checkRows(1)
            uid = self._get_reload_uid()
            if uid is None or uid <= 0:
                tdLog.exit(f"Expected positive reloadUid for: {sql}, got: {uid}")
            tdLog.info(f"'{sql}' returned reloadUid={uid}")

    def test_reload_last_cache_stable(self):
        """RELOAD LAST_CACHE ON STABLE returns a positive integer reloadUid."""
        tdLog.printNoPrefix("=== test_reload_last_cache_stable ===")

        tdSql.query(f"RELOAD LAST_CACHE ON STABLE {DB_NAME}.{STB_NAME}")
        tdSql.checkRows(1)
        uid = self._get_reload_uid()
        if uid is None or uid <= 0:
            tdLog.exit(
                f"Expected positive reloadUid from RELOAD LAST_CACHE ON STABLE, got: {uid}"
            )
        tdLog.info(f"RELOAD LAST_CACHE ON STABLE returned reloadUid={uid}")

    def test_reload_last_cache_stable_variants(self):
        """All three cache-type variants execute without error on STABLE."""
        tdLog.printNoPrefix("=== test_reload_last_cache_stable_variants ===")

        for sql in [
            f"RELOAD LAST CACHE ON STABLE {DB_NAME}.{STB_NAME}",
            f"RELOAD LAST_ROW CACHE ON STABLE {DB_NAME}.{STB_NAME}",
            f"RELOAD LAST_CACHE ON STABLE {DB_NAME}.{STB_NAME}",
        ]:
            tdSql.query(sql)
            tdSql.checkRows(1)
            uid = self._get_reload_uid()
            if uid is None or uid <= 0:
                tdLog.exit(f"Expected positive reloadUid for: {sql}, got: {uid}")
            tdLog.info(f"'{sql}' returned reloadUid={uid}")

    def test_reload_last_cache_table(self):
        """RELOAD LAST_CACHE ON TABLE returns a positive integer reloadUid."""
        tdLog.printNoPrefix("=== test_reload_last_cache_table ===")

        for tbl in [
            f"{DB_NAME}.{CTB_NAME}",
            f"{DB_NAME}.{NTB_NAME}",
        ]:
            tdSql.query(f"RELOAD LAST_CACHE ON TABLE {tbl}")
            tdSql.checkRows(1)
            uid = self._get_reload_uid()
            if uid is None or uid <= 0:
                tdLog.exit(
                    f"Expected positive reloadUid from RELOAD LAST_CACHE ON TABLE {tbl}, got: {uid}"
                )
            tdLog.info(f"RELOAD LAST_CACHE ON TABLE {tbl} returned reloadUid={uid}")

    def test_reload_last_cache_table_variants(self):
        """All three cache-type variants execute without error on TABLE."""
        tdLog.printNoPrefix("=== test_reload_last_cache_table_variants ===")

        tbl = f"{DB_NAME}.{CTB_NAME}"
        for sql in [
            f"RELOAD LAST CACHE ON TABLE {tbl}",
            f"RELOAD LAST_ROW CACHE ON TABLE {tbl}",
            f"RELOAD LAST_CACHE ON TABLE {tbl}",
        ]:
            tdSql.query(sql)
            tdSql.checkRows(1)
            uid = self._get_reload_uid()
            if uid is None or uid <= 0:
                tdLog.exit(f"Expected positive reloadUid for: {sql}, got: {uid}")
            tdLog.info(f"'{sql}' returned reloadUid={uid}")

    def test_reload_last_cache_table_column(self):
        """RELOAD LAST_CACHE ON TABLE ... COLUMN ... executes without error."""
        tdLog.printNoPrefix("=== test_reload_last_cache_table_column ===")

        tbl = f"{DB_NAME}.{CTB_NAME}"
        for sql in [
            f"RELOAD LAST CACHE ON TABLE {tbl} COLUMN c1",
            f"RELOAD LAST_ROW CACHE ON TABLE {tbl} COLUMN c1",
            f"RELOAD LAST_CACHE ON TABLE {tbl} COLUMN c1",
        ]:
            tdSql.query(sql)
            tdSql.checkRows(1)
            uid = self._get_reload_uid()
            if uid is None or uid <= 0:
                tdLog.exit(f"Expected positive reloadUid for: {sql}, got: {uid}")
            tdLog.info(f"'{sql}' returned reloadUid={uid}")

    def test_show_reloads(self):
        """SHOW RELOADS executes without error (may return 0 rows if tasks completed)."""
        tdLog.printNoPrefix("=== test_show_reloads ===")

        # Trigger a reload first so there may be something to show
        tdSql.query(f"RELOAD LAST_CACHE ON DATABASE {DB_NAME}")
        tdSql.checkRows(1)

        # SHOW RELOADS must not raise an exception; row count is not asserted
        # because the async task may have already finished
        tdSql.query("SHOW RELOADS")
        tdLog.info(f"SHOW RELOADS returned {tdSql.queryRows} row(s)")

    def test_show_reload_by_uid(self):
        """SHOW RELOAD <uid> executes without error for a valid uid."""
        tdLog.printNoPrefix("=== test_show_reload_by_uid ===")

        tdSql.query(f"RELOAD LAST_CACHE ON DATABASE {DB_NAME}")
        tdSql.checkRows(1)
        uid = self._get_reload_uid()
        if uid is None:
            tdLog.exit("Failed to obtain reloadUid for SHOW RELOAD test")

        # May return 0 rows if already completed, but must not crash
        tdSql.query(f"SHOW RELOAD {uid}")
        tdLog.info(f"SHOW RELOAD {uid} returned {tdSql.queryRows} row(s)")

    def test_drop_reload_invalid_uid(self):
        """DROP RELOAD with an invalid (non-existent) uid should return an error."""
        tdLog.printNoPrefix("=== test_drop_reload_invalid_uid ===")

        invalid_uid = 999999999
        tdSql.error(f"DROP RELOAD {invalid_uid}")
        tdLog.info(f"DROP RELOAD {invalid_uid} correctly returned an error")

    def test_reload_nonexistent_database(self):
        """RELOAD on a non-existent database should return an error."""
        tdLog.printNoPrefix("=== test_reload_nonexistent_database ===")

        tdSql.error("RELOAD LAST_CACHE ON DATABASE db_that_does_not_exist_xyz")
        tdLog.info("RELOAD on non-existent database correctly returned an error")

    def test_reload_nonexistent_table(self):
        """RELOAD on a non-existent table should return an error."""
        tdLog.printNoPrefix("=== test_reload_nonexistent_table ===")

        tdSql.error(f"RELOAD LAST_CACHE ON TABLE {DB_NAME}.tbl_does_not_exist_xyz")
        tdLog.info("RELOAD on non-existent table correctly returned an error")

    # ---------------------------------------------------------------
    # entry points
    # ---------------------------------------------------------------

    def run(self):
        self._setup_db()

        self.test_reload_last_cache_database()
        self.test_reload_last_cache_database_variants()
        self.test_reload_last_cache_stable()
        self.test_reload_last_cache_stable_variants()
        self.test_reload_last_cache_table()
        self.test_reload_last_cache_table_variants()
        self.test_reload_last_cache_table_column()
        self.test_show_reloads()
        self.test_show_reload_by_uid()
        self.test_drop_reload_invalid_uid()
        self.test_reload_nonexistent_database()
        self.test_reload_nonexistent_table()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
