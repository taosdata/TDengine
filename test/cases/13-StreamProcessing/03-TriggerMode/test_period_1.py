import threading
import time

import psutil
import taos

from new_test_framework.utils import tdLog, clusterComCheck, tdStream, tdSql

class WriteDataInfo:
    def __init__(self, num_of_rows, num_of_tables):
        self.num_of_rows = num_of_rows
        self.num_of_tables = num_of_tables

        self.start_write = False
        self.insert_complete = False
        # self.delete_data = False
        # self.update_data = False

def do_write_data_fn(conf, info: WriteDataInfo):
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

    for j in range(info.num_of_tables):
        for i in range(info.num_of_rows):
            ts = start_ts + i
            cursor.execute(f"insert into db.c{j} values ('{ts}', {i}, '1', {i})")

    tdLog.info(f"insert {info.num_of_rows} rows for {info.num_of_tables} source_tables completed")

    info.insert_complete = True
    cursor.close()


def _do_build_results():
    num_of_cols = tdSql.getCols()
    num_of_rows = tdSql.getRows()

    results = []
    for i in range(num_of_rows):
        row = []
        for j in range(num_of_cols):
            row.append(tdSql.getData(i, j))

        results.append(row)
    return results

def get_conf_dir(process_name):
    for proc in psutil.process_iter(attrs=['pid', 'name']):
        try:
            if process_name.lower() in proc.info['name'].lower():
                print(f"Found process: {proc.info}")

                process = psutil.Process(proc.info['pid'])
                cmdline = process.cmdline()

                if len(cmdline) == 3:
                    return cmdline[-1]
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return "/etc/taos/cfg"

def check_all_results(sql: str, values: list) -> None:
    tdSql.query(sql)

    tdSql.checkRows(len(values))
    tdSql.checkCols(len(values[0]))

    res = _do_build_results()
    if res != values:
        print(f"results: {res}, expect:{values}, not equal, failed")
        raise Exception("stream results error")


def clear_output(stream_name, dst_table):
    tdLog.info(f"clear test output")

    tdSql.execute(f"drop stream if exists {stream_name}")
    tdSql.execute(f"drop table if exists {dst_table}")
    tdSql.execute(f"drop table if exists db.source_table")


def wait_for_insert_complete(info: WriteDataInfo):
    time.sleep(10)
    info.start_write = True

    while not info.insert_complete:
        time.sleep(5)
        print("wait for inserting completed")


def wait_for_stream_done_r1(sql: str, expect: int):
    while True:
        tdSql.query(sql)
        if tdSql.getRows() == 0:
            print("stream not completed")
            time.sleep(2)
            continue

        if tdSql.getData(0, 0) == expect:
            print("stream completed")
            break
        else:
            print(f"cal result:{tdSql.getData(0, 0)}, expected result: {expect} stream not completed")
            time.sleep(2)
            
def check_ts_step(tb_name, freq):
    tdSql.query(f"select ts, c from {tb_name}")

    for i in range(1, tdSql.getRows()):
        prev = tdSql.getData(i - 1, 0)
        curr = tdSql.getData(i, 0)

        delta = curr - prev
        if delta.total_seconds() != freq:
            print(f"current:{curr}, prev:{prev}, delta: {delta.total_seconds()}, expect delta: {freq} error")
            raise Exception("stream results error")
        

class TestStreamCheckpoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test

        Verification testing during the development process.

        Catalog:
            - Streams: 03-TriggerMode
        Description:
            - create 10 streams, each stream has 10 source tables
            - write data to source tables
            - check stream results

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
        info = WriteDataInfo(1000, 10)

        # self.write_data(100000, 1)
        # try:
        #     self.create_and_check_stream_basic_1("sm1", "tb1")
        # except Exception as e:
        #     tdLog.error(f"case 1 error: {e}")
        #
        # clear_output("sm1", "tb1")
        # self.write_data(100000, 1)
        # try:
        #     self.create_and_check_stream_basic_2("sm2", "tb2")
        # except Exception as e:
        #     tdLog.error(f"case 2 error: {e}")
        #
        #
        # clear_output("sm2", "tb2")
        # self.write_data(100000, 1)
        #
        # try:
        #     self.create_and_check_stream_basic_3("sm3", "tb3")
        # except Exception as e:
        #     tdLog.error(f"case 3 error: {e}")
        #
        # clear_output("sm3", "tb3")
        # self.write_data(100000, 1)
        # try:
        #     self.create_and_check_stream_basic_4("sm4", "tb4")
        # except Exception as e:
        #     tdLog.error(f"case 4 error: {e}")
        #
        # clear_output("sm4", "tb4")
        # self.write_data(100000, 1)
        # try:
        #     self.create_and_check_stream_basic_5("sm5", "tb5")
        # except Exception as e:
        #     tdLog.error(f"case 5 error: {e}")
        #
        # clear_output("sm5", "tb5")
        # self.write_data(100000, 1)
        # try:
        #     self.create_and_check_stream_basic_6("sm6", "tb6")
        # except Exception as e:
        #     tdLog.error(f"case 6 error: {e}")
        #
        clear_output("sm6", "tb6")
        self.prepare_source_table(1000, 10, info)
        try:
            self.create_and_check_stream_basic_7("sm7", "tb7", info)
        except Exception as e:
            tdLog.error(f"case 7 error: {e}")
        #
        # clear_output("sm7", "tb7")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_8("sm8", "tb8")
        # except Exception as e:
        #     tdLog.error(f"case 8 error: {e}")
        #
        # clear_output("sm8", "tb8")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_9("sm9", "tb9")
        # except Exception as e:
        #     tdLog.error(f"case 9 error: {e}")
        #
        # clear_output("sm9", "tb9")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_10("sm10", "tb10")
        # except Exception as e:
        #     tdLog.error(f"case 10 error: {e}")
        #
        # clear_output("sm10", "tb10")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_11("sm11", "tb11")
        # except Exception as e:
        #     tdLog.error(f"case 11 error: {e}")
        #
        # clear_output("sm11", "tb11")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_12("sm12", "tb12")
        # except Exception as e:
        #     tdLog.error(f"case 12 error: {e}")
        #
        # clear_output("sm12", "tb12")
        # self.write_data(10000, 10)
        # try:
        #     self.create_and_check_stream_basic_13("sm13", "tb13")
        # except Exception as e:
        #     tdLog.error(f"case 13 error: {e}")
        
    def create_env(self):
        tdLog.info(f"create {self.num_snode} snode(s)")
        for i in range(self.num_snode):
            tdStream.createSnode(i + 1)

        self.create_database()

    def create_database(self) -> None:
        tdLog.info(f"create database with {self.num_vgroups} vgroups")
        tdSql.prepare(dbname="db", vgroups=self.num_vgroups)
        clusterComCheck.checkDbReady("db")

    def prepare_query_data(self) -> None:
        tdLog.info("prepare child tables for query")
        tdStream.prepareChildTables(tbBatch=1, rowBatch=1, rowsPerBatch=400)

        tdLog.info("prepare normal tables for query")
        tdStream.prepareNormalTables(tables=10, rowBatch=1)

    def prepare_source_table(self, num_of_rows, num_of_tables, info:WriteDataInfo) -> None:
        info.num_of_rows, info.num_of_tables = num_of_rows, num_of_tables

        tdLog.info("prepare source tables")
        tdSql.execute("use db")

        stb = "create table if not exists source_table (ts timestamp, k int, c1 varchar(12), c2 double) tags(a int)"
        tdSql.execute(stb)

        for i in range(num_of_tables):
            tdSql.execute(f"create table if not exists c{i} using source_table tags({i})")

        tdLog.info(f"create {num_of_tables} tables completed")


    def do_write_data(self, info: WriteDataInfo):
        tdLog.info(f"insert data after 10sec")

        time.sleep(10)
        conf = get_conf_dir("taosd")

        # start another thread to write data
        try:
            t = threading.Thread(target=do_write_data_fn, args=(conf, info))
            t.start()
        except Exception as e:
            print("Error: unable to start thread, %s" % e)
            exit(-1)

        # wait for insert completed
        wait_for_insert_complete(info)

    def wait_for_stream_completed(self) -> None:
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def check_results(self) -> None:
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()


    def create_and_check_stream_basic_1(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 1"""
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_2(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 2"""
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(10a) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=0.01)

    def create_and_check_stream_basic_3(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 3"""
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, now(), \'abcdefg\', top(k, 20) c, concat('abc', cast(_tlocaltime as varchar(1))) from source_table")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_4(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 4
            Error: only one result generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) into {dst_table} as select now() ts, count(*) c from source_table")

        self.do_write_data(info)
        wait_for_stream_done_r1(f"select last(c) from {dst_table}", info.num_of_rows)
        # check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_5(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 5: pass"""
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) into {dst_table} as select _wstart ts, count(*) k, last(k) c from source_table interval(1s)")
        
        self.do_write_data(info)

        wait_for_stream_done_r1(f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=1)

    def create_and_check_stream_basic_6(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """
        simple 6:
        maybe the same reason as simple 1 case, no results generated in the results tables
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(30s) into {dst_table} as "
                      f"select cast(_tlocaltime/1000000 as timestamp) ts, _wstart ats, count(*) k, last(k) c "
                      f"from source_table interval(1s)")

        self.do_write_data(info)
        
        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_7(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 7:
           Error: No results generated
        """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname, a into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) ts, count(1) k, last(k) c , %%tbname, _tprev_localtime "
            f"from source_table group by tbname, a")
        
        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_ts_step(tb_name=dst_table, freq=3)

    def create_and_check_stream_basic_8(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 8:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select ts, k c, c1, c2 "
            f"from source_table partition by tbname")
        
        self.do_write_data(info)
        
        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select count(*) from {dst_table}", [[info.num_of_tables * info.num_of_rows]])

    def create_and_check_stream_basic_9(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 9:
           ERROR: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select  _wstart ts, _wend te, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"state_window(cast(c1 as int))")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select max(c) from {dst_table} group by tbname",
                               [[9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999]])

    def create_and_check_stream_basic_10(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 10:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"session(ts, 10s)")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select max(c) from {dst_table} group by tbname",
                               [[9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999], [9999]])

    def create_and_check_stream_basic_11(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 10:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table count_window(10)"
        )

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select count(*) from {dst_table} ", [[10000]])

    def create_and_check_stream_basic_12(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 12:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"count_window(10) ")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select count(*) from {dst_table} ", [[10000]])

    def create_and_check_stream_basic_13(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 13:
           Error: no results generated
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(30s) from source_table partition by tbname, a into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table partition by a "
            f"count_window(10) ")

        self.do_write_data(info)

        wait_for_stream_done_r1(f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select count(*) from {dst_table} ", [[10000]])
