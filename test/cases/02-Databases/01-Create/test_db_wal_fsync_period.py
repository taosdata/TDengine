from new_test_framework.utils import tdLog, tdSql, tdDnodes
import datetime


PRIMARY_COL = "ts"

INT_COL     = "c1"
BINT_COL    = "c2"
SINT_COL    = "c3"
TINT_COL    = "c4"
FLOAT_COL   = "c5"
DOUBLE_COL  = "c6"
BOOL_COL    = "c7"

BINARY_COL  = "c8"
NCHAR_COL   = "c9"
TS_COL      = "c10"

NUM_COL     = [ INT_COL, BINT_COL, SINT_COL, TINT_COL, FLOAT_COL, DOUBLE_COL, ]
CHAR_COL    = [ BINARY_COL, NCHAR_COL, ]
BOOLEAN_COL = [ BOOL_COL, ]
TS_TYPE_COL = [ TS_COL, ]

class TestFsync:

    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def __kill_process(self, process_name):
        killCmd = f"ps -efww |grep -w {process_name}| grep -v grep | awk '{{print $2}}' | xargs kill -TERM > /dev/null 2>&1"

        psCmd = f"ps -efww |grep -w {process_name}| grep -v grep | awk '{{print $2}}'"
        while processID := subprocess.check_output(psCmd, shell=True):
            os.system(killCmd)
            time.sleep(1)

    def run_fsync_current(self):
        wal_index = 0
        fsync_index = 0
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryCols):
            if tdSql.cursor.description[i][0] == "wal_level":
                wal_index = i
            if tdSql.cursor.description[i][0] == "wal_fsync_period":
                fsync_index = i

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_level 1")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, wal_index, 1)

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_level 2")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_fsync_period 0")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 0)

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_fsync_period 3000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_fsync_period 180000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 180000)


        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_level 1 wal_fsync_period 6000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 6000)
                tdSql.checkData(i, wal_index, 1)

        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 wal_level 2 wal_fsync_period 3000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("alter database db1 wal_level 1")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)
                tdSql.checkData(i, wal_index, 1)

        tdSql.execute("alter database db1 wal_level 2")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("alter database db1 wal_fsync_period 0")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 0)
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("alter database db1 wal_fsync_period 3000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("alter database db1 wal_fsync_period 18000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 18000)
                tdSql.checkData(i, wal_index, 2)

        tdSql.execute("alter database db1 wal_level 1 wal_fsync_period 3000")
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0] == "db1":
                tdSql.checkData(i, fsync_index, 3000)
                tdSql.checkData(i, wal_index, 1)

        tdSql.execute("drop database db1 ")

    @property
    def fsync_create_err(self):
        return [
            #"create database db1 wal_level 0",
            "create database db1 wal_level 3",
            "create database db1 wal_level null",
            "create database db1 wal_level true",
            "create database db1 wal_level 1.1",
            "create database db1 wal_fsync_period -1",
            "create database db1 wal_fsync_period 180001",
            "create database db1 wal_fsync_period 10.111",
            "create database db1 wal_fsync_period true",
        ]

    @property
    def fsync_alter_err(self):
        return [
            #"alter database db1 wal_level 0",
            "alter database db1 wal_level 3",
            "alter database db1 wal_level null",
            "alter database db1 wal_level true",
            "alter database db1 wal_level 1.1",
            "alter database db1 wal_fsync_period -1",
            "alter database db1 wal_fsync_period 180001",
            "alter database db1 wal_fsync_period 10.111",
            "alter database db1 wal_fsync_period true",
        ]

    def run_fsync_err(self):
        for sql in self.fsync_create_err:
            tdSql.error(sql)
        tdSql.query("create database db1")
        for sql in self.fsync_alter_err:
            tdSql.error(sql)
        tdSql.query("drop database db1")

    def all_test(self):
        self.run_fsync_err()
        self.run_fsync_current()

    # def __create_tb(self):

    #     tdLog.printNoPrefix("==========step1:create table")
    #     create_stb_sql  =  f'''create table stb1(
    #             ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
    #              {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
    #              {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
    #         ) tags (t1 int)
    #         '''
    #     create_ntb_sql = f'''create table t1(
    #             ts timestamp, {INT_COL} int, {BINT_COL} bigint, {SINT_COL} smallint, {TINT_COL} tinyint,
    #              {FLOAT_COL} float, {DOUBLE_COL} double, {BOOL_COL} bool,
    #              {BINARY_COL} binary(16), {NCHAR_COL} nchar(32), {TS_COL} timestamp
    #         )
    #         '''
    #     tdSql.execute(create_stb_sql)
    #     tdSql.execute(create_ntb_sql)

    #     for i in range(4):
    #         tdSql.execute(f'create table ct{i+1} using stb1 tags ( {i+1} )')
    #         { i % 32767 }, { i % 127}, { i * 1.11111 }, { i * 1000.1111 }, { i % 2}

    # def __insert_data(self, rows):
    #     now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)


    def test_db_wal_fsync_period(self):
        """Database wal_fsync_period
        
        1. Create database with wal_fsync_period options
        2. Verify wal_fsync_period value from information_schema.ins_databases
        3. Verify wal_fsync_period value after alter database
        4. Verify wal_fsync_period value after restart taosd
        5. Verify error cases for wal_fsync_period option
        6. Verify error cases for alter wal_fsync_period option
        

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_fsync.py

        """
        tdSql.prepare()

        self.all_test()

        tdDnodes.stop(1)
        tdDnodes.start(1)

        # tdSql.execute("use db")

        tdLog.printNoPrefix("==========step4:after wal_level, all check again ")
        self.all_test()

        tdLog.success(f"{__file__} successfully executed")

