from new_test_framework.utils import tdLog, tdSql, tdDnodes
import sys
from math import inf

class TestShowCreateDb:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-11204]Difference improvement that can ignore negative
        '''
        return

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls._conn = cls.conn

    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use scd")

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

    def test_show_create_db(self):
        """Show create database

        1. Create three databases with different options
        2. Check "show create database dbname" output correctness
        3. Restart taosd and recheck the output correctness
        4. Drop and recreate the databases, recheck the output correctness

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-17 Alex Duan Migrated from uncatalog/develop-test/2-query/test_show_create_db.py
        
        """
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists scd")
        tdSql.execute("create database if not exists scd compact_interval 0 keep_time_offset 0")
        tdSql.execute('use scd')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tb1 using stb1 tags(1,'1',1.0);")

        tdSql.execute("create table tb2 using stb1 tags(2,'2',2.0);")

        tdSql.execute("create table tb3 using stb1 tags(3,'3',3.0);")

        tdSql.execute('create database scd2 stt_trigger 3 compact_interval 1 keep_time_offset 0h;')

        tdSql.execute('create database scd4 stt_trigger 13 compact_interval 12h compact_time_range -60,-10 compact_time_offset 23h;')

        self.showCreateDbCheck('scd', "CREATE DATABASE `scd` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h")

        self.showCreateDbCheck('scd2', "CREATE DATABASE `scd2` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 3 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 1d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h")

        self.showCreateDbCheck('scd4', "CREATE DATABASE `scd4` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 13 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 12h COMPACT_TIME_RANGE -60d,-10d COMPACT_TIME_OFFSET 23h")

        self.restartTaosd(1, dbname='scd')
        tdLog.info("recheck after restart taosd")
        self.showCreateDbCheck('scd', "CREATE DATABASE `scd` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h", 30, True, True)

        self.showCreateDbCheck('scd2', "CREATE DATABASE `scd2` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 3 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 1d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h", 30, True, True)

        self.showCreateDbCheck('scd4', "CREATE DATABASE `scd4` BUFFER 256 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 13 KEEP 3650d,3650d,3650d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 1 WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'none' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 12h COMPACT_TIME_RANGE -60d,-10d COMPACT_TIME_OFFSET 23h", 30, True, True)

        tdLog.success("%s successfully executed" % __file__)

