# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
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

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, etool
import os
import platform
import tempfile


def _write_bom_csv(src_path, bom):
    """Copy src csv content to a temp file prefixed with a BOM header."""
    fd, dst = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "wb") as fw, open(src_path, "rb") as fr:
        fw.write(bom)
        fw.write(fr.read())
    if platform.system().lower() == "windows":
        dst = dst.replace("\\", "/")
    return dst


class TestCreateCtbUsingCsvFile:
    updatecfgDict = {
        "maxInsertBatchRows": 10
    }

    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS qxjtest")
        tdSql.execute("create database qxjtest buffer 200 vgroups 20 stt_trigger 1 pagesize 64 maxrows 7501 keep 90d duration 1d;")
        tdSql.execute("USE qxjtest")
        tdSql.execute("create stable qxjtest.gdcdata(ts timestamp, gdvalue float) tags (element varchar(10), height smallint, validtime tinyint, latitude smallint)")


    def create_ctb_using_csv_file(self):
        tdLog.info(f"create ctb using csv file")
        tdSql.execute("USE qxjtest")
        datafile =etool.getFilePath(__file__, "data", "create_ctb_127.csv")
        tdLog.info(f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) file '{datafile}';")
        tdSql.execute(f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) file '{datafile}';")

    def check_create_ctb_using_csv_file(self):
        tdLog.info(f"check create ctb using csv file")
        tdSql.execute("USE qxjtest")
        tdSql.query("show tables;")
        tdSql.checkRows(127)
        
    # run
    def test_subtable_create_using_csv(self):
        """Child table create using csv
        
        1. Create database with vgroups 20 stt_trigger 1
        2. Create super table according to csvfile format
        3. Create child table using csv file
        4. Check created child table number

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/create/test_create_ctb_using_csv_file.py

        """
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # create ctb using csv file
        self.create_ctb_using_csv_file()

        # check create ctb using csv file
        self.check_create_ctb_using_csv_file()

    # --- csv file starts with a BOM header ---
    def create_ctb_using_csv_with_utf8_bom(self):
        """A csv file starting with a UTF-8 BOM (EF BB BF) is processed normally."""
        tdLog.info(f"create ctb using csv file with utf-8 bom")
        tdSql.execute("USE qxjtest")
        datafile = etool.getFilePath(__file__, "data", "create_ctb_127.csv")
        bom_csv = _write_bom_csv(datafile, b"\xef\xbb\xbf")  # UTF-8 BOM
        try:
            tdSql.execute(
                f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) "
                f"file '{bom_csv}';")
        finally:
            os.remove(bom_csv)

    def create_ctb_using_csv_with_unsupported_bom(self):
        """A csv file starting with a non-UTF-8 BOM (e.g. UTF-16) is rejected."""
        tdLog.info(f"create ctb using csv file with unsupported bom")
        tdSql.execute("USE qxjtest")
        datafile = etool.getFilePath(__file__, "data", "create_ctb_127.csv")
        bom_csv = _write_bom_csv(datafile, b"\xff\xfe")  # UTF-16 LE BOM
        try:
            tdSql.error(
                f"create table if not exists using qxjtest.gdcdata(tbname,element,height,validtime,latitude) "
                f"file '{bom_csv}';",
                expectErrInfo="unsupported BOM", fullMatched=False)
        finally:
            os.remove(bom_csv)
        # nothing is created after the failure
        tdSql.query("show tables;")
        tdSql.checkRows(0)

    def test_subtable_create_using_csv_with_bom(self):
        """Child table create using csv with a BOM header

        1. A csv file starting with a UTF-8 BOM is processed normally
        2. A csv file starting with a non-UTF-8 BOM is rejected

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-9-7 Bomin Zhang Added csv BOM header tests

        """
        # prepare database
        self.prepare_database()

        # create ctb using csv file with utf-8 bom
        self.create_ctb_using_csv_with_utf8_bom()
        self.check_create_ctb_using_csv_file()

        # create ctb using csv file with unsupported bom
        self.prepare_database()
        self.create_ctb_using_csv_with_unsupported_bom()



