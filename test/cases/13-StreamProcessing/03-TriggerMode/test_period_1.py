import os
import threading
import time
import taos

from new_test_framework.utils import tdLog, clusterComCheck, tdStream, tdSql

source_table = "db.c1"

def do_write_data(conf, num_of_rows, num_of_tables=1):
    tdLog.info("start to write data to source table")
    start_ts = 1700000000000

    host = "localhost"
    user = "root"
    passwd = "taosdata"
    tz = "Asia/Shanghai"

    conn = taos.connect(
        host=host, user=user, password=passwd, config=conf, timezone=tz
    )

    cursor = conn.cursor()
    cursor.execute('use db')

    for j in range(num_of_tables):
        for i in range(num_of_rows):
            ts = start_ts + i
            cursor.execute(f"insert into db.c{j} values ('{ts}', {i}, '1', {i})")

    tdLog.info(f"insert {num_of_rows} rows for {num_of_tables} source_tables completed")
    cursor.close()

class TestStreamCheckpoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")


    def test_stream_dev_basic(self):
        """basic test

        Verification testing during the development process.

        Catalog:
            - Streams: 01-snode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-26

        """

        self.num_snode = 1
        self.num_vgroups = 2

        self.streams = []
        self.stream_id = 1

        self.create_env()
        self.prepare_source_table(1)

        self.num_of_tables = 1
        self.num_of_rows = 100000
        # self.write_data()

        try:
            self.create_and_check_stream_basic_1("sm1", "tb1")
        except Exception as e:
            print(f"stream error: {e}")
        
        self.clear_test_output("sm1", "tb1")
        
        self.write_data()
        
        try:
            self.create_and_check_stream_basic_2("sm2", "tb2")
        except Exception as e:
            print(f"stream error: {e}")
        
        self.clear_test_output("sm2", "tb2")
        
        self.write_data()
        
        try:
            self.create_and_check_stream_basic_3("sm3", "tb3")
        except Exception as e:
            print(f"stream error: {e}")
        
        self.clear_test_output("sm3", "tb3")
        
        self.write_data()
        try:
            self.create_and_check_stream_basic_4("sm4", "tb4")
        except Exception as e:
            print(f"stream error: {e}")
        
        self.clear_test_output("sm4", "tb4")

        self.write_data()
        try:
            self.create_and_check_stream_basic_5("sm5", "tb5")
        except Exception as e:
            print(f"stream error: {e}")

        self.num_of_tables = 1
        self.num_of_rows = 100000
        self.clear_test_output("sm5", "tb5")
        
        self.write_data()
        try:
            self.create_and_check_stream_basic_6("sm6", "tb6")
        except Exception as e:
            print(f"stream error: {e}")


        self.num_of_tables = 10
        self.num_of_rows = 10000
        
        self.clear_test_output("sm6", "tb6")
        self.write_data()
        try:
            self.create_and_check_stream_basic_7("sm7", "tb7")
        except Exception as e:
            print(f"stream error: {e}")

        self.num_of_tables = 10
        self.num_of_rows = 10000
        
        self.clear_test_output("sm7", "tb7")
        self.write_data()
        try:
            self.create_and_check_stream_basic_8("sm8", "tb8")
        except Exception as e:
            print(f"stream error: {e}")

        self.num_of_tables = 10
        self.num_of_rows = 10000
        
        self.clear_test_output("sm8", "tb8")
        self.write_data()
        try:
            self.create_and_check_stream_basic_9("sm9", "tb9")
        except Exception as e:
            print(f"stream error: {e}")

        self.num_of_tables = 10
        self.num_of_rows = 10000

        self.clear_test_output("sm9", "tb9")
        self.write_data()
        try:
            self.create_and_check_stream_basic_10("sm10", "tb10")
        except Exception as e:
            print(f"stream error: {e}")


    def create_env(self):
        tdLog.info(f"create {self.num_vgroups} snode(s)")
        for i in range(self.num_snode):
            tdStream.createSnode(i+1)

        self.create_database()

    def create_database(self) -> None:
        tdLog.info(f"create database")
        tdSql.prepare(dbname="db", vgroups=self.num_vgroups)
        clusterComCheck.checkDbReady("db")

    def prepare_query_data(self) -> None:
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

    def prepare_source_table(self, num_of_tables) -> None:
        tdLog.info("prepare tables for trigger")

        tdSql.execute("use db")

        stb = "create table if not exists source_table (ts timestamp, k int, c1 varchar(12), c2 double) tags(a int)"
        tdSql.execute(stb)

        for i in range(num_of_tables):
            tdSql.execute(f"create table if not exists c{i} using source_table tags({i})")


    def write_data(self) -> None:
        tdLog.info("write data to source table in other thread")
        conf = os.getcwd() + "/new_test_framework/utils/sim/dnode1/cfg"

        try:
            t = threading.Thread(target=do_write_data, args=(conf, self.num_of_rows, self.num_of_tables))
            t.start()
        except Exception as e:
            print("Error: unable to start thread, %s" % e)
            exit(-1)


    def wait_for_stream_completed(self) -> None:
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def check_results(self) -> None:
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def wait_for_insert_complete(self):
        while True:
            tdSql.query(f"select count(*) from db.source_table", None,queryTimes=50)
            val = tdSql.getData(0, 0)

            if val == self.num_of_rows * self.num_of_tables:
                print(f"insert completed, total rows:{self.num_of_rows * self.num_of_tables} for {self.num_of_tables} tables")
                break
            time.sleep(1)


    def wait_for_stream_done(self, tb_name):
        while True:
            tdSql.query(f"select last(c) from {tb_name}")

            if tdSql.getData(0, 0) == self.num_of_rows:
                print("stream completed")
                break
            else:
                print(f"cal rows:{tdSql.getData(0, 0)}, expected: {self.num_of_rows} stream not completed")
                time.sleep(2)

    def wait_for_stream_done_1(self, tb_name):
        while True:
            tdSql.query(f"select max(c) from {tb_name}")

            if tdSql.getData(0, 0) == self.num_of_rows - 1:
                print("stream completed")
                break
            else:
                print(f"cal result:{tdSql.getData(0, 0)}, expected result: {self.num_of_rows-1} stream not completed")
                time.sleep(2)

    def check_stream_result(self, tb_name, freq):
        tdSql.query(f"select ts, c from {tb_name}")

        for i in range(1, tdSql.getRows()):
            prev = tdSql.getData(i-1, 0)
            curr = tdSql.getData(i, 0)

            delta = curr - prev
            if delta.total_seconds() != freq:
                print(f"current:{curr}, prev:{prev}, delta: {delta.total_seconds()}, expect delta: {freq} error")
                raise Exception("stream results error")

    def check_stream_result_one_row(self, tb_name):
        tdSql.query(f"select c from {tb_name}")

        for i in range(1, tdSql.getRows()):
            prev = tdSql.getData(i-1, 0)
            curr = tdSql.getData(i, 0)

            delta = curr - prev
            if delta != 0:
                print(f"current:{curr}, prev:{prev}, expect equal error")
                raise Exception("stream results error")

    def check_stream_result_exec(self, sql:str, values:list):
        tdSql.query(sql)

        tdSql.checkRows(len(values))
        for i in range(len(values)):
            item = values[i]
            if tdSql.getData(item[0], item[1]) != item[2]:
                print(f"get:{tdSql.getData(item[0], item[1])}, expect:{item[2]}, expect equal error")
                raise Exception("stream results error")


    def clear_test_output(self, stream_name, dst_table):
        tdLog.info(f"clear test output")

        tdSql.execute(f"drop stream if exists {stream_name}")
        tdSql.execute(f"drop table if exists {dst_table}")
        tdSql.execute(f"drop table if exists db.source_table")

        self.prepare_source_table(self.num_of_tables)


    def create_and_check_stream_basic_1(self, stream_name, dst_table) ->None:
        """simple 1"""
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_2(self, stream_name, dst_table) ->None:
        """simple 2"""
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(10a) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=0.01)


    def create_and_check_stream_basic_3(self, stream_name, dst_table) ->None:
        """simple 3"""
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, now(), \'abcdefg\', top(k, 20) c, concat('abc', cast(_tlocaltime as varchar(1))) from source_table")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_4(self, stream_name, dst_table) ->None:
        """simple 4"""
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as select now() ts, count(*) c from source_table")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done(dst_table)
        # self.check_stream_result(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_5(self, stream_name, dst_table) ->None:
        """simple 5: pass"""
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as select _wstart ts, count(*) k, last(k) c from source_table interval(1s)")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=1)

    def create_and_check_stream_basic_6(self, stream_name, dst_table) -> None:
        """
        simple 6:
        maybe the same reason as simple 1 case, no results generated in the results tables
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as "
                      f"select cast(_tlocaltime/1000000 as timestamp) ts, _wstart ats, count(*) k, last(k) c "
                      f"from source_table interval(1s)")

        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_7(self, stream_name, dst_table) ->None:
        """simple 7:
           Error: No results generated
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname  into {dst_table} as "
                      f"select cast(_tlocaltime/1000000 as timestamp) ts, count(*) k, last(k) c, tbname tb, a"
                      f"from source_table group by tbname, a")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_8(self, stream_name, dst_table) ->None:
        """simple 8:
           No need to calculate the results, since it employs the scalar function
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
                      f"select ts, k c, c1, c2 "
                      f"from source_table partition by tbname")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)


    def create_and_check_stream_basic_9(self, stream_name, dst_table) ->None:
        """simple 9:
           ERROR: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
                      f"select  _wstart ts, _wend te, count(*),  max(k) c "
                      f"from source_table partition by tbname "
                      f"state_window(cast(c1 as int))")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result_one_row(tb_name=dst_table)


    def create_and_check_stream_basic_10(self, stream_name, dst_table) -> None:
        """simple 10:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"session(ts, 10s)")
        tdLog.info(f"create stream completed, and wait for it completed")

        time.sleep(5)
        self.wait_for_insert_complete()

        self.wait_for_stream_done_1(dst_table)
        self.check_stream_result_exec("select max(c) from db.tb10 group by tbname",
                                      [(0, 0, 9999), (1, 0, 9999), (2, 0, 9999), (3, 0, 9999), (4, 0, 9999),
                                       (5, 0, 9999), (6, 0, 9999), (7, 0, 9999), (8, 0, 9999), (9, 0, 9999)])
