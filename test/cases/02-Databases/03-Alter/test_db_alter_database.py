from new_test_framework.utils import tdLog, tdSql
import os
import time
import platform



class TestAlterDatabase:
 
    def setup_class(cls):
        #tdSql.init(conn.cursor(), logSql)
        tdLog.debug("start to execute %s" % __file__)
        #tdSql.init(conn.cursor(), logSql), logSql)
        if platform.system().lower() == 'windows':
            cls.buffer_boundary = [3, 16384]
        else:
            cls.buffer_boundary = [3, 4097, 8193, 12289, 16384]
        # remove the value > free_memory, 70% is the weight to calculate the max value
        # if platform.system() == "Linux" and platform.machine() == "aarch64":
            # mem = psutil.virtual_memory()
            # free_memory = mem.free * 0.7 / 1024 / 1024
            # for item in self.buffer_boundary:
            #     if item > free_memory:
            #         self.buffer_boundary.remove(item)

        cls.buffer_error = [cls.buffer_boundary[0] -
                             1, cls.buffer_boundary[-1]+1]
        # pages_boundary >= 64
        cls.pages_boundary = [64, 128, 512]
        cls.pages_error = [cls.pages_boundary[0]-1]

    def alter_buffer(self):
        tdSql.execute('create database db')
        if platform.system() == "Linux" and platform.machine() == "aarch64":
            tdLog.debug("Skip check points for Linux aarch64 due to environment settings")
        else:
            for buffer in self.buffer_boundary:
                tdSql.execute(f'alter database db buffer {buffer}')
                tdSql.query(
                    'select * from information_schema.ins_databases where name = "db"')
                tdSql.checkEqual(tdSql.queryResult[0][8], buffer)
        tdSql.execute('drop database db')
        tdSql.execute('create database db vgroups 10')
        for buffer in self.buffer_error:
            tdSql.error(f'alter database db buffer {buffer}')
        tdSql.execute('drop database db')

    def alter_pages(self):
        tdSql.execute('create database db')
        for pages in self.pages_boundary:
            tdSql.execute(f'alter database db pages {pages}')
            tdSql.query(
                'select * from information_schema.ins_databases where name = "db"')
            tdSql.checkEqual(tdSql.queryResult[0][10], pages)
        tdSql.execute('drop database db')
        tdSql.execute('create database db')
        tdSql.query(
            'select * from information_schema.ins_databases where name = "db"')
        # self.pages_error.append(tdSql.queryResult[0][10])
        for pages in self.pages_error:
            tdSql.error(f'alter database db pages {pages}')
        tdSql.execute('drop database db')

    def alter_encrypt_alrogithm(self):
        tdSql.execute('create database db')
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'sM4\''))
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'noNe\''))
        tdSql.checkEqual("Encryption is not allowed to be changed after database is created", tdSql.error('alter database db encrypt_algorithm \'\''))
        tdSql.checkEqual("Invalid option encrypt_algorithm: none ", tdSql.error('alter database db encrypt_algorithm \'none \''))
        tdSql.execute('drop database db')

    def alter_same_options(self):
        tdSql.execute('drop database if exists db')
        tdSql.execute('create database db')
        tdSql.query('select * from information_schema.ins_databases where name = "db"')
        
        db_options_items = ["replica","keep","buffer","pages","minrows","cachemodel","cachesize","wal_level","wal_fsync_period",
                      "wal_retention_period","wal_retention_size","stt_trigger", "compact_interval", "compact_time_range", "compact_time_offset"]
        db_options_result_idx = [4,7,8,10,11,18,19,20,21,22,23,24,34,35,36]
        
        self.option_result = []
        for idx in db_options_result_idx:
            self.option_result.append(tdSql.queryResult[0][idx])
        
        index = 0
        for option in db_options_items:
            if option == "cachemodel":
                option_sql = "alter database db %s '%s'" % (option, self.option_result[index] )
            else:
                option_sql = "alter database db %s %s" % (option, self.option_result[index] )
            tdLog.debug(option_sql)
            tdSql.query(option_sql)
            index += 1
        tdSql.execute('drop database db')

    def showCreateDbCheck(self, dbname, expectShowCreateDb, maxRetry=30, dropDbAndRecheck=False, dropDbAfterCheck=False):
        retry_count = 0
        success = False
        while retry_count < maxRetry and not success:
            try:
                tdSql.query(f"show create database {dbname};")
                tdSql.checkRows(1)
                tdSql.checkData(0, 0, dbname)
                tdSql.checkEqual(tdSql.queryResult[0][1], expectShowCreateDb)
                tdLog.info(f"check passed after {retry_count} seconds")
                success = True
            except Exception as e:
                retry_count += 1
                tdLog.info(f"Attempt to check {retry_count} time(s): show create database {dbname}")
                if retry_count >= maxRetry:
                    raise Exception(repr(e))
                time.sleep(1)
        if dropDbAndRecheck == True:
            tdSql.execute(f"drop database if exists {dbname}")
            tdSql.execute(expectShowCreateDb)
            tdSql.query(f"show create database {dbname};")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, dbname)
            tdSql.checkData(0, 1, expectShowCreateDb)
        if dropDbAfterCheck == True:
            tdSql.execute(f"drop database if exists {dbname}")

    def alter_keep_time_offset(self):
        tdSql.execute('drop database if exists db')
        tdSql.checkEqual("Invalid option keep_time_offset unit: d, only h allowed", tdSql.error('create database db keep_time_offset 0d'))
        tdSql.checkEqual("syntax error near \"-1\"", tdSql.error('create database db keep_time_offset -1'))
        tdSql.checkEqual("syntax error near \"-100h\"", tdSql.error('create database db keep_time_offset -100h'))
        tdSql.checkEqual("Invalid option keep_time_offset: 24 valid range: [0, 23]", tdSql.error('create database db keep_time_offset 24h'))
        tdSql.execute('create database db keep_time_offset 20h')
        self.showCreateDbCheck('db', "CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 20 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h", 30, True, False)
        tdSql.checkEqual("Invalid option keep_time_offset unit: d, only h allowed", tdSql.error('alter database db keep_time_offset 0d'))
        tdSql.checkEqual("syntax error near \"-1\"", tdSql.error('alter database db keep_time_offset -1'))
        tdSql.checkEqual("syntax error near \"-100h\"", tdSql.error('alter database db keep_time_offset -100h'))
        tdSql.checkEqual("Invalid option keep_time_offset: 24 valid range: [0, 23]", tdSql.error('alter database db keep_time_offset 24h'))
        
        tdLog.info('alter database db keep_time_offset 23h')
        tdSql.execute('alter database db keep_time_offset 23h')
        self.showCreateDbCheck('db', "CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 23 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h", 30, True, False)
        tdLog.info('alter database db keep_time_offset 0')
        tdSql.execute('alter database db keep_time_offset 0')
        self.showCreateDbCheck('db', "CREATE DATABASE `db` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h", 30, True, True)

    def test_db_alter_database(self):
        """Alter database

        1. Alter database buffer
        2. Alter database pages
        3. Alter database encrypt_algorithm
        4. Alter database with same options
        5. Alter database keep_time_offset


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-09-12 AlexDaun Migrated from uncatelog/system-test/test_alter_database.py
 
        """
        self.alter_buffer()
        self.alter_pages()
        self.alter_encrypt_alrogithm()
        self.alter_same_options()
        self.alter_keep_time_offset()
        
        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")