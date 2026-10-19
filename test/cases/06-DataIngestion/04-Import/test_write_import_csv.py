# Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
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

import os
import platform
import tempfile
import time

from new_test_framework.utils import tdLog, tdSql, etool


def _write_bom_csv(src_path, bom):
    """Copy src csv content to a temp file prefixed with a BOM header."""
    fd, dst = tempfile.mkstemp(suffix=".csv")
    with os.fdopen(fd, "wb") as fw, open(src_path, "rb") as fr:
        fw.write(bom)
        fw.write(fr.read())
    if platform.system().lower() == "windows":
        dst = dst.replace("\\", "/")
    return dst


class TestInsertFromCsv:
    #
    # ----------------- test_insert_from_csv.py -------------------
    #    
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def insert_from_csv(self):
        # init
        database = "test_insert_csv_db"
        table = "test_insert_csv_tbl"
        csv_file_path = etool.getFilePath(__file__, "data", "test_insert_from_csv.csv")
        
        # insert
        tdSql.execute(f"drop database if exists {database}")
        tdSql.execute(f"create database {database}")
        tdSql.execute(f"use {database}")
        tdSql.execute(f"create table {table} (ts timestamp, c1 nchar(16), c2 double, c3 int)")
        tdSql.execute(f"insert into {table} file '{csv_file_path}'")    
        tdSql.query(f"select count(*) from {table}")
        tdSql.checkData(0, 0, 5)

    # normal table 
    def do_ntb_import_csv(self):
        tdSql.prepare()        
        startTime_all = time.time() 
        self.insert_from_csv()
        endTime_all = time.time()
        print("total time %ds" % (endTime_all - startTime_all))
        print("do import to ntb ...................... [passed]")

    #
    # ----------------- test_insert_csv_file_with_row_split.py -------------------
    #
    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS `vehicle_prod2`")
        tdSql.execute("CREATE DATABASE `vehicle_prod2` BUFFER 32 CACHESIZE 1 CACHEMODEL 'last_row' COMP 2 DURATION 50d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 1 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 262144 SS_KEEPLOCAL 5256000m SS_COMPACT 0 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h ")
        tdSql.execute("USE vehicle_prod2")
        tdSql.execute("CREATE STABLE `up_topic` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `msg_id` BIGINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY, `topic_content` VARCHAR(50) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `command_code` VARCHAR(3) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `msg` VARCHAR(1500) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium', `data_source` TINYINT UNSIGNED ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`device_id` VARCHAR(50))")

    def create_ctb_using_csv_file(self):
        tdLog.info(f"create ctb using csv file")
        tdSql.execute("USE vehicle_prod2")
        datafile = etool.getFilePath(__file__, "data", "test.csv")
        tdLog.info(f"INSERT INTO vehicle_prod2.up_topic (tbname,ts,msg_id,topic_content,command_code,msg,data_source,device_id) FILE '{datafile}';")
        tdSql.execute(f"INSERT INTO vehicle_prod2.up_topic (tbname,ts,msg_id,topic_content,command_code,msg,data_source,device_id) FILE '{datafile}';")

    def check_create_ctb_using_csv_file(self):
        tdLog.info(f"check create ctb using csv file")
        tdSql.execute("USE vehicle_prod2")
        tdSql.query("select * from up_topic;")
        tdSql.checkRows(13)
        
    # super table 
    def do_stb_import_csv(self):
        # prepare database
        self.prepare_database()
        # create ctb using csv file
        self.create_ctb_using_csv_file()
        # check create ctb using csv file
        self.check_create_ctb_using_csv_file()
        print("do import to stb ...................... [passed]")

        
    def test_write_import_csv(self):
        """From csv file

        1. Create table and import data from csv file
        2. Check the imported data

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated fromuncatalog/army/insert/test_insert_csv_file_with_row_split.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_from_csv.py

        """
        self.do_ntb_import_csv()
        self.do_stb_import_csv()

    # --- csv file starts with a BOM header ---
    def do_ntb_import_csv_with_utf8_bom(self):
        """A csv file starting with a UTF-8 BOM (EF BB BF) is imported normally."""
        database = "test_insert_csv_bom_utf8_db"
        table = "test_insert_csv_bom_utf8_tbl"
        src_csv = etool.getFilePath(__file__, "data", "test_insert_from_csv.csv")
        bom_csv = _write_bom_csv(src_csv, b"\xef\xbb\xbf")  # UTF-8 BOM
        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database}")
            tdSql.execute(f"use {database}")
            tdSql.execute(f"create table {table} (ts timestamp, c1 nchar(16), c2 double, c3 int)")
            tdSql.execute(f"insert into {table} file '{bom_csv}'")
            tdSql.query(f"select count(*) from {table}")
            tdSql.checkData(0, 0, 5)
        finally:
            os.remove(bom_csv)
        print("import csv with UTF-8 BOM .................... [passed]")

    def do_ntb_import_csv_with_unsupported_bom(self):
        """A csv file starting with a non-UTF-8 BOM (e.g. UTF-16) is rejected."""
        database = "test_insert_csv_bom_err_db"
        table = "test_insert_csv_bom_err_tbl"
        src_csv = etool.getFilePath(__file__, "data", "test_insert_from_csv.csv")
        bom_csv = _write_bom_csv(src_csv, b"\xff\xfe")  # UTF-16 LE BOM
        try:
            tdSql.execute(f"drop database if exists {database}")
            tdSql.execute(f"create database {database}")
            tdSql.execute(f"use {database}")
            tdSql.execute(f"create table {table} (ts timestamp, c1 nchar(16), c2 double, c3 int)")
            tdSql.error(f"insert into {table} file '{bom_csv}'",
                        expectErrInfo="unsupported BOM", fullMatched=False)
            # no data is imported after the failure
            tdSql.query(f"select count(*) from {table}")
            tdSql.checkData(0, 0, 0)
        finally:
            os.remove(bom_csv)
        print("import csv with unsupported BOM ............. [passed]")

    def test_write_import_csv_with_bom(self):
        """Import from csv file with a BOM header

        1. A csv file starting with a UTF-8 BOM is imported normally
        2. A csv file starting with a non-UTF-8 BOM is rejected

        Since: v3.4.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-9-7 Bomin Zhang Added csv BOM header tests

        """
        self.do_ntb_import_csv_with_utf8_bom()
        self.do_ntb_import_csv_with_unsupported_bom()

