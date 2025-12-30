import os
import platform
import time

from new_test_framework.utils import tdLog, tdSql, etool


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

        Labels: common,ci

        Jira: None

        History:
            - 2025-9-16 Alex Duan Migrated fromuncatalog/army/insert/test_insert_csv_file_with_row_split.py
            - 2025-9-16 Alex Duan Migrated from uncatalog/system-test/1-insert/test_insert_from_csv.py

        """
        self.do_ntb_import_csv()
        self.do_stb_import_csv()

        tdLog.success(f"{__file__} successfully executed")