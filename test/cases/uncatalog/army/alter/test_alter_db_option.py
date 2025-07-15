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

from new_test_framework.utils import tdLog, tdSql, etool





class TestAlterDbOption:

    def prepare_database(self):
        tdLog.info(f"prepare database")
        tdSql.execute("DROP DATABASE IF EXISTS test")
        tdSql.execute("CREATE DATABASE IF NOT EXISTS test")
        tdSql.execute("USE test")

    def check_alter_buffer_size(self):
        tdLog.info(f"check alter buffer size")
        tdSql.execute("ALTER DATABASE test buffer 789")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 8, '789')
        tdSql.error("ALTER DATABASE test buffer 0",expectErrInfo="Invalid option", fullMatched=False)
        tdSql.error("ALTER DATABASE test buffer 16385",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_cache_model(self):
        tdLog.info(f"check alter cache model")
        tdSql.execute("ALTER DATABASE test cachemodel 'last_row'")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 18, "last_row")
        tdSql.execute("ALTER DATABASE test cachemodel 'last_value'")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 18, "last_value")
        tdSql.execute("ALTER DATABASE test cachemodel 'both'")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 18, "both")
        tdSql.execute("ALTER DATABASE test cachemodel 'none'")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 18, "none")

        tdSql.error("ALTER DATABASE test cachemodel 'hash_value'",expectErrInfo="Invalid option", fullMatched=False)        

    def check_alter_cache_size(self):
        tdLog.info(f"check alter cache size")
        tdSql.execute("ALTER DATABASE test cachesize 777")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 19, '777')

        tdSql.error("ALTER DATABASE test cachesize 0",expectErrInfo="Invalid option", fullMatched=False)
        tdSql.error("ALTER DATABASE test cachesize 65537",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_min_rows(self):
        tdLog.info(f"check alter min rows")
        tdSql.execute("ALTER DATABASE test minrows 231")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 11, '231')
        tdSql.error("ALTER DATABASE test minrows 5000",expectErrInfo="Invalid database options", fullMatched=False)
        
        tdSql.error("ALTER DATABASE test minrows 9",expectErrInfo="Invalid option", fullMatched=False)
        tdSql.error("ALTER DATABASE test minrows 1000001",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_pages(self):
        tdLog.info(f"check alter pages")
        tdSql.execute("ALTER DATABASE test pages 256")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 10, '256')
        tdSql.error("ALTER DATABASE test pages 0",expectErrInfo="Invalid option", fullMatched=False)


    def check_alter_wal_level(self):
        tdLog.info(f"check alter wal_level")
        tdSql.execute("ALTER DATABASE test wal_level 1")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 20, '1')
        
        tdSql.execute("ALTER DATABASE test wal_level 2")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 20, '2')
        
        tdSql.error("ALTER DATABASE test wal_level 3",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_fsync(self):
        tdLog.info(f"check alter wal_fsync_period")
        tdSql.execute("ALTER DATABASE test wal_fsync_period 1000")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 21, '1000')
        
        tdSql.error("ALTER DATABASE test wal_fsync_period 180001",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_stt_trigger(self):
        tdLog.info(f"check alter stt_trigger")
        tdSql.execute("ALTER DATABASE test stt_trigger 5")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 24, '5')
    
        
        tdSql.error("ALTER DATABASE test stt_trigger 18",expectErrInfo="Invalid option", fullMatched=False)

    def check_alter_wal_retention_period(self):
        tdLog.info(f"check alter wal_retention_period")
        tdSql.execute("ALTER DATABASE test wal_retention_period 3600")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 22, '3600')
        
        
    def check_alter_wal_retention_size(self):
        tdLog.info(f"check alter wal_retention_size")
        tdSql.execute("ALTER DATABASE test wal_retention_size 1000")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 23, '1000')

    def check_alter_ss_keeplocal(self):
        tdLog.info(f"check alter ss_keeplocal")
        tdSql.execute("ALTER DATABASE test ss_keeplocal 2880m")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 30, '2880m')

    def check_alter_ss_compact(self):
        tdLog.info(f"check alter ss_compact")
        tdSql.execute("ALTER DATABASE test ss_compact 1")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 31, '1')
        
        tdSql.execute("ALTER DATABASE test ss_compact 0")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 31, '0')

        tdSql.error("ALTER DATABASE test s3_compact 2",expectErrInfo="Invalid option", fullMatched=False)


    def check_alter_keep_time_offset(self):
        tdLog.info(f"check alter keep_time_offset")
        tdSql.execute("ALTER DATABASE test keep_time_offset 22")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 28, '22')

    def check_alter_compact_interval(self):
        tdLog.info(f"check alter compact_interval")
        tdSql.execute("ALTER DATABASE test compact_interval 1800d")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 34, '1800d')
        
        tdSql.execute("ALTER DATABASE test compact_interval 1h")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 34, '1h')
        
    def check_alter_compact_time_offset(self):
        tdLog.info(f"check alter compact_time_offset")
        tdSql.execute("ALTER DATABASE test compact_time_offset 18")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkData(2, 36, '18h')
        

    def check_alter_unsupport_option(self):
        tdLog.info(f"check alter unsupport option")
<<<<<<< HEAD:test/cases/uncatalog/army/alter/test_alter_db_option.py
        tdSql.error("ALTER DATABASE test COMP 1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test DURATION 1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test maxrows 1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test encrypt_algorithm 'sm4'",expectErrInfo="Encryption is not allowed to be changed after database is created", fullMatched=False)
        tdSql.error("ALTER DATABASE test vgroups 4",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test single_stable 1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test schemaless 1",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test table_prefix 't'",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test table_suffix 't'",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test dnodes 'dnode1'",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test precision 'ms'",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test strict 'on'",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test pagesize 4096",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test tsdb_pagesize 4096",expectErrInfo="syntax error", fullMatched=False)
        tdSql.error("ALTER DATABASE test retentions '1d:1d'",expectErrInfo="syntax error", fullMatched=False)
=======
        tdSql.error("ALTER DATABASE test COMP 1",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test DURATION 1",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test maxrows 1",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test encrypt_algorithm 'sm4'",expectErrInfo="Encryption is not allowed to be changed after database is created")
        tdSql.error("ALTER DATABASE test vgroups 4",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test single_stable 1",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test schemaless 1",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test table_prefix 't'",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test table_suffix 't'",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test ss_chunkpages 100",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test wal_roll_period 3600",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test wal_segment_size 1000",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test dnodes 'dnode1'",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test precision 'ms'",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test strict 'on'",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test pagesize 4096",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test tsdb_pagesize 4096",expectErrInfo="syntax error")
        tdSql.error("ALTER DATABASE test retentions '1d:1d'",expectErrInfo="syntax error")
>>>>>>> 3.0:tests/army/alter/alter_db_option.py

    # run
    def test_alter_db_option(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        
        History:
            - xxx
            - xxx
        """
        tdLog.debug(f"start to excute {__file__}")

        # prepare database
        self.prepare_database()

        # check alter buffer size
        self.check_alter_buffer_size()

        # check alter cache model
        self.check_alter_cache_model()

        # check alter cache size
        self.check_alter_cache_size()

        # check alter min rows
        self.check_alter_min_rows()
        
        # check alter pages
        self.check_alter_pages()
        
        # check alter wal_level
        self.check_alter_wal_level()
        
        # check alter wal_fsync_period
        self.check_alter_fsync()
        
        # check alter stt_trigger
        self.check_alter_stt_trigger()
        
        # check alter wal_retention_period
        self.check_alter_wal_retention_period()
        
        # check alter wal_retention_size
        self.check_alter_wal_retention_size()
        
        # check alter ss_keeplocal
        self.check_alter_ss_keeplocal()
        
        # check alter ss_compact
        self.check_alter_ss_compact()
        
        # check alter keep_time_offset
        self.check_alter_keep_time_offset()
        
        # check alter compact_interval
        self.check_alter_compact_interval()
        
        # check alter compact_time_offset
        self.check_alter_compact_time_offset()

        # check alter unsupport option
        self.check_alter_unsupport_option()

        tdLog.success(f"{__file__} successfully executed")

