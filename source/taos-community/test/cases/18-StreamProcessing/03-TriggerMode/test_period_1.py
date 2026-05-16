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

def wait_for_stream_done(table_name:str, sql: str, expect: int):
    while True:
        tdSql.query(f"select count(*) from information_schema.ins_tables "
                    f"where table_name = '{table_name}' or stable_name='{table_name}'")
        if tdSql.getData(0, 0) == 0:
            print("results not generated, waiting...")
            time.sleep(1)
        else:
            break

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
    tdSql.query(f"select ts from {tb_name}")

    for i in range(1, tdSql.getRows()):
        prev = tdSql.getData(i - 1, 0)
        curr = tdSql.getData(i, 0)

        delta = curr - prev
        if delta.total_seconds() != freq:
            print(f"current:{curr}, prev:{prev}, delta: {delta.total_seconds()}, expect delta: {freq} error")
            raise Exception("stream results error")
        

def do_write_data(stream_name:str, info: WriteDataInfo):
    tdLog.info(f"insert data after 10sec")

    while True:
        tdSql.query(f"select status from information_schema.ins_streams where stream_name='{stream_name}'")
        if tdSql.getData(0, 0) != "Running":
            print("stream not running, waiting....")
            time.sleep(10)
        else:
            break

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

class TestStreamCheckpoint:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """Options: period
        
        1. Create 10 streams, each stream has 10 source tables
        2. Write data to source tables
        3. Check stream results
        4. Drop streams and tables
        
        

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-06-26

        """

        self.num_snode = 1
        self.num_vgroups = 4

        self.streams = []
        self.stream_id = 1

        self.create_env()
        info = WriteDataInfo(1000, 10)

        self.prepare_source_table(1000, 1, info)
        try:
            self.create_and_check_stream_basic_1("sm1", "tb1", info)
        except Exception as e:
            tdLog.error(f"case 1 error: {e}")

        # clear_output("sm1", "tb1")
        # self.prepare_source_table(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_2("sm2", "tb2", info)
        # except Exception as e:
        #     tdLog.error(f"case 2 error: {e}")
        #
        # clear_output("sm2", "tb2")
        # self.prepare_source_table(1000, 1, info)
        #
        # try:
        #     self.create_and_check_stream_basic_3("sm3", "tb3", info)
        # except Exception as e:
        #     tdLog.error(f"case 3 error: {e}")
        #
        # clear_output("sm3", "tb3")
        # self.prepare_source_table(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_4("sm4", "tb4", info)
        # except Exception as e:
        #     tdLog.error(f"case 4 error: {e}")
        #
        # clear_output("sm4", "tb4")
        # self.prepare_source_table(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_5("sm5", "tb5", info)
        # except Exception as e:
        #     tdLog.error(f"case 5 error: {e}")
        #
        # clear_output("sm5", "tb5")
        # self.prepare_source_table(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_6("sm6", "tb6", info)
        # except Exception as e:
        #     tdLog.error(f"case 6 error: {e}")
        #
        # clear_output("sm6", "tb6")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_7("sm7", "tb7", info)
        # except Exception as e:
        #     tdLog.error(f"case 7 error: {e}")
        #
        # clear_output("sm7", "tb7")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_8("sm8", "tb8", info)
        # except Exception as e:
        #     tdLog.error(f"case 8 error: {e}")
        #
        # clear_output("sm8", "tb8")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_9("sm9", "tb9", info)
        # except Exception as e:
        #     tdLog.error(f"case 9 error: {e}")
        #
        # clear_output("sm9", "tb9")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_10("sm10", "tb10", info)
        # except Exception as e:
        #     tdLog.error(f"case 10 error: {e}")
        #
        # clear_output("sm10", "tb10")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_11("sm11", "tb11", info)
        # except Exception as e:
        #     tdLog.error(f"case 11 error: {e}")

        # clear_output("sm11", "tb11")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_12("sm12", "tb12", info)
        # except Exception as e:
        #     tdLog.error(f"case 12 error: {e}")

        clear_output("sm12", "tb12")
        self.prepare_source_table(1000, 10, info)
        try:
            self.create_and_check_stream_basic_13("sm13", "tb13", info)
        except Exception as e:
            tdLog.error(f"case 13 error: {e}")


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


    def wait_for_stream_completed(self) -> None:
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def check_results(self) -> None:
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()


    def create_and_check_stream_basic_1(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 1: Pass"""
        tdLog.info(f"start exec stream {stream_name}")
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) into {dst_table} as select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=3)

    def create_and_check_stream_basic_2(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 2: Pass"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(10a) into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c from source_table")

        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select last(c) from {dst_table}", info.num_of_rows)
        # check_ts_step(tb_name=dst_table, freq=0.01)  # not precisely equals to 0.01

    def create_and_check_stream_basic_3(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 3:  Pass """
        tdSql.execute("use db")
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) ts, now(), \'abcdefg\', "
            f"top(k, 1) top_k_1, concat('abc', cast(_tlocaltime as varchar(1))) "
            f"from source_table ")

        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select max(top_k_1) from {dst_table}", info.num_of_rows - 1)
        check_ts_step(tb_name=dst_table, freq=3)

    def create_and_check_stream_basic_4(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 4: pass"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) into {dst_table} as "
            f"select _wstart ts, count(*) k, last(k) c "
            f"from source_table interval(100a)")
        
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select last(c) from {dst_table}", info.num_of_rows - 1)
        check_ts_step(tb_name=dst_table, freq=0.1)
        check_all_results(f"select k, c from {dst_table}",
                          [[100, 99], [100, 199], [100, 299], [100, 399], [100, 499], [100, 599], [100, 699],
                           [100, 799], [100, 899], [100, 999]])

    def create_and_check_stream_basic_5(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """ simple 5: Pass """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} PERIOD(3s) into {dst_table} as "
                      f"select cast(_tlocaltime/1000000 as timestamp) ts, _wstart wstart, count(*) k, last(k) c "
                      f"from source_table interval(1s)")

        do_write_data(stream_name, info)
        
        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_ts_step(tb_name=dst_table, freq=3)

    def create_and_check_stream_basic_6(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 6: Pass """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname, a into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) ts, count(1) k, last(k) c , %%tbname, cast(_tprev_localtime/1000000 as timestamp) "
            f"from source_table group by tbname, a")
        
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)
        # check_ts_step(tb_name=dst_table, freq=3)

    def create_and_check_stream_basic_7(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 7: Pass """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname into {dst_table} as "
            f"select ts, k c, c1, c2 "
            f"from source_table partition by tbname")
        
        do_write_data(stream_name, info)
        
        wait_for_stream_done(dst_table, f"select count(c) from {dst_table}", info.num_of_rows * info.num_of_tables)

    def create_and_check_stream_basic_8(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 8: Pass """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart ts, _wend te, cast(_tlocaltime/1000000 as timestamp) local_time, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"state_window(cast(c1 as int))")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)

    def create_and_check_stream_basic_9(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 9: Pass"""
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"session(ts, 10s)")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)

    def create_and_check_stream_basic_10(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 10: invalid results """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart st, _wend et, cast(_tlocaltime/1000000 as timestamp) local_ts, count(*),  "
            f"max(k) max_k, sum(k) sum_k "
            f"from source_table partition by tbname count_window(10) "
        )

        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_rows)
        check_all_results(f"select count(*) from {dst_table} where max_k={info.num_of_rows-1}", [[10]])


    def create_and_check_stream_basic_11(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 11: Pass """
        tdLog.info(f"start exec stream {stream_name}")
        tdSql.execute("create vtable vtb_1 (ts timestamp, col_1 int from c0.k, col_2 varchar(12) from c1.c1, "
                      "col_3 double from c2.c2)")

        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from vtb_1 partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*),  "
            f"max(col_1) max_col_1, sum(length(col_2)) sum_col_2, avg(col_3) avg_col_3, sum(length(substr(%%1, 1))) sum_part_1  "
            f"from vtb_1 partition by tbname "
        )

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(avg_col_3) from {dst_table}", 499.5)

    def create_and_check_stream_basic_12(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 12:  Pass """
        tdLog.info(f"start exec stream {stream_name}")
        tdSql.execute("use db")

        tdSql.execute(
            f"create table if not exists stb (cts timestamp, cint int, cfloat float, cdouble double, cdecimal decimal(11,3), "
            f"cvar varchar(12)) tags (tint int)")

        tdSql.query(f"show stables")

        tdSql.execute(f"create table ct1 using stb tags(1)")
        tdSql.execute(f"create table ct2 using stb tags(2)")
        tdSql.execute(f"create table ct3 using stb tags(3)")
        tdSql.execute(f"create table ct4 using stb tags(4)")
        tdSql.execute(f"create table ct5 using stb tags(5)")
        tdSql.execute(f"create table ct6 using stb tags(6)")
        tdSql.execute(f"create table ct7 using stb tags(7)")

        tdSql.query(f"show tables")
        # tdSql.checkRows(7)

        tdSql.execute(
            "create stable vstb_1(ts timestamp, col_1 int, col_2 float, col_3 double, col_4 varchar(12)) tags(tag_a int, tag_b double) virtual 1"
        )

        tdSql.execute(
            "create vtable vtb_1(db.ct1.cint, db.ct2.cfloat, db.ct3.cdouble, db.ct5.cvar) using vstb_1 tags(0, 1)")
        tdSql.execute(
            "create vtable vtb_2(db.ct2.cint, db.ct3.cfloat, db.ct4.cdouble, db.ct6.cvar) using vstb_1 tags(2, 3)")
        tdSql.execute(
            "create vtable vtb_3(db.ct3.cint, db.ct4.cfloat, db.ct5.cdouble, db.ct7.cvar) using vstb_1 tags(4, 5)")
        tdSql.execute(
            "create vtable vtb_4(db.ct4.cint, db.ct5.cfloat, db.ct6.cdouble, db.ct1.cvar) using vstb_1 tags(6, 7)")
        tdSql.execute(
            "create vtable vtb_5(db.ct5.cint, db.ct6.cfloat, db.ct7.cdouble, db.ct2.cvar) using vstb_1 tags(8, 9)")

        tdSql.execute(
            f"create stream {stream_name} PERIOD(3s) from vstb_1 partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*),  "
            f"max(col_1) max_col_1, sum(length(col_4)) sum_col_2, avg(col_3) avg_col_3, sum(length(substr(%%1, 1))) sum_part_1  "
            f"from vstb_1 partition by tbname "
        )

        sqls = [
            "insert into ct1 values ('2025-01-01 00:00:00', 1, 1.1, 3.14159, 1.0954327, 'abcdefg' );",
            "insert into ct1 values ('2025-01-01 00:00:03', 2, 2.2, 6.28318, 1.1912644, 'hijklmn' );",
            "insert into ct1 values ('2025-01-01 00:00:06', 3, 3.3, 9.42478, 1.2871093, 'opqrstu' );",
            "insert into ct1 values ('2025-01-01 00:00:09', 4, 4.4, 12.56637, 1.3826434, 'vwxyz' );",
            "insert into ct1 values ('2025-01-01 00:00:12', 5, 5.5, 15.70796, 1.4782644, '123456' );",
            "insert into ct1 values ('2025-01-01 00:00:15', 6, 6.6, 18.84956, 1.5740740, '789012' );",
            "insert into ct1 values ('2025-01-01 00:00:18', 7, 7.7, 22.07104, 1.6696434, '345678' );",
            "insert into ct1 values ('2025-01-01 00:00:21', 8, 8.8, 25.13274, 1.7653566, '901234' );",
            "insert into ct1 values ('2025-01-01 00:00:24', 9, 9.9, 28.29444, 1.8619690, '567890' );",

            "insert into ct2 values ('2025-01-01 00:00:00', 21, 21.1, 9.1, 1.123456, 'aaaaaa');",
            "insert into ct2 values ('2025-01-01 00:00:03', 22, 22.2, 9.2, 1.234567, 'bbbbbb');",
            "insert into ct2 values ('2025-01-01 00:00:06', 23, 23.3, 9.3, 1.345678, 'cccccc');",
            "insert into ct2 values ('2025-01-01 00:00:09', 24, 24.4, 9.4, 1.456789, 'dddddd');",
            "insert into ct2 values ('2025-01-01 00:00:12', 25, 25.5, 9.5, 1.567890, 'eeeeee');",
            "insert into ct2 values ('2025-01-01 00:00:15', 26, 26.6, 9.6, 1.678901, 'ffffff');",
            "insert into ct2 values ('2025-01-01 00:00:18', 27, 27.7, 9.7, 1.789012, 'gggggg');",
            "insert into ct2 values ('2025-01-01 00:00:21', 28, 28.8, 9.8, 1.890123, 'hhhhhh');",
            "insert into ct2 values ('2025-01-01 00:00:24', 29, 29.9, 9.9, 1.901234, 'iiiiii');",

            "insert into ct3 values ('2025-01-01 00:00:00', 31, 12.123, 31.111, 1.274, '-------');",
            "insert into ct3 values ('2025-01-01 00:00:03', 32, 12.222, 32.222, 1.274, '-------');",
            "insert into ct3 values ('2025-01-01 00:00:06', 33, 12.333, 33.333, 1.274, '+++++++');",
            "insert into ct3 values ('2025-01-01 00:00:09', 34, 12.333, 33.333, 1.274, '///////');",
            "insert into ct3 values ('2025-01-01 00:00:12', 35, 12.333, 33.333, 1.274, '///////');",
            "insert into ct3 values ('2025-01-01 00:00:15', 36, 12.333, 33.333, 1.274, '///////');",
            "insert into ct3 values ('2025-01-01 00:00:18', 37, 12.333, 33.333, 1.274, '///////');",
            "insert into ct3 values ('2025-01-01 00:00:21', 38, 12.333, 33.333, 1.274, '///////');",
            "insert into ct3 values ('2025-01-01 00:00:24', 39, 12.333, 33.333, 1.274, '///////');",

            "insert into ct4 values ('2025-01-01 00:00:00', 41, 22.98765, 12.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:03', 42, 23.98765, 12.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:06', 43, 24.98765, 13.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:09', 44, 25.98765, 13.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:12', 45, 26.98765, 14.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:15', 46, 27.98765, 14.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:18', 47, 28.98765, 15.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:21', 48, 29.98765, 15.31, 3.253, '++++++f');",
            "insert into ct4 values ('2025-01-01 00:00:24', 49, 30.98765, 15.31, 3.253, '++++++f');",

            "insert into ct5 values ('2025-01-01 00:00:00', 51, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:03', 52, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:06', 53, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:09', 54, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:12', 55, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:15', 56, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:18', 57, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:21', 58, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",
            "insert into ct5 values ('2025-01-01 00:00:24', 59, 15.234567, 1.11809, 21.7, 'a1s2d3f4');",

            "insert into ct6 values ('2025-01-01 00:00:00', 61, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:03', 62, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:06', 63, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:09', 64, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:12', 65, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:15', 66, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:18', 67, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:21', 68, 911.119, 110.011, 187.91234, '!!!!!!!!');",
            "insert into ct6 values ('2025-01-01 00:00:24', 69, 911.119, 110.011, 187.91234, '!!!!!!!!');",

            "insert into ct7 values ('2025-01-01 00:00:00', 71, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:03', 72, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:06', 73, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:09', 74, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:12', 75, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:15', 76, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:18', 77, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:21', 78, 123.4567, 98.7653, 1.1, '========');",
            "insert into ct7 values ('2025-01-01 00:00:24', 79, 123.4567, 98.7653, 1.1, '========');",
        ]

        tdSql.executes(sqls)

        # do_write_data(stream_name, info)
        # wait_for_stream_done(dst_table, f"select max(avg_col_3) from {dst_table}", 499.5)

    def create_and_check_stream_basic_13(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 13: """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.error(
            f"create stream {stream_name} PERIOD(9a) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} PERIOD(3651d) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} PERIOD(10s, -10s) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} PERIOD(10s, 100h) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} PERIOD(10s, 1h-20m) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} PERIOD(10s, 0.5d) from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} from source_table partition by tbname into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} period(10s) from information_schema.ins_tables into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} period(10s) into information_schema.abc as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} period(10s) from db.abc into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        tdSql.error(
            f"create stream {stream_name} period(10s) from db.ct20 into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
            f"from source_table partition by tbname "
        )

        for i in range(40):
            tdSql.execute(
                f"create stream {stream_name} period(10s) from db.c1 partition by a into {dst_table} as "
                f"select cast(_tlocaltime/1000000 as timestamp) local_ts, count(*)  "
                f"from source_table partition by tbname "
            )

            tdSql.execute(f"drop stream {stream_name}")
