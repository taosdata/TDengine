import threading
import time

import taos
from new_test_framework.utils import tdLog, clusterComCheck, tdStream, tdSql
from test_period_1 import check_all_results, wait_for_stream_done, check_ts_step, \
    clear_output, get_conf_dir, wait_for_insert_complete


class WriteDataInfo:
    def __init__(self, num_of_rows, num_of_tables):
        self.num_of_rows = num_of_rows
        self.num_of_tables = num_of_tables

        self.start_write = True
        self.insert_complete = False
        self.delete_data = False
        self.update_data = False


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

    while not info.start_write:
        time.sleep(1)
        print("wait for inserting ts data")

    for j in range(info.num_of_tables):
        for i in range(info.num_of_rows):
            ts = start_ts + i * 1000
            cursor.execute(f"insert into db.c{j} values ('{ts}', {i}, '2', {i})")

    tdLog.info(f"insert {info.num_of_rows} rows for {info.num_of_tables} source_tables completed")

    for j in range(info.num_of_tables):
        cursor.execute(f"insert into db.c{j} values (now, 0, '1', 0)")

    tdLog.info(f"write last rows for {info.num_of_tables} source_tables completed")

    if info.delete_data:
        tdLog.info(f"start to delete ts")
        for j in range(info.num_of_tables):
            for i in range(info.num_of_rows):
                if i % 2 == 0:
                    continue
                ts = start_ts + i * 1000
                cursor.execute(f"delete from db.c{j} where ts={ts}")

        tdLog.info(f"delete {info.num_of_rows/2} rows for {info.num_of_tables} source_tables completed")

    if info.update_data:
        tdLog.info(f"start to update ts related data")
        for j in range(info.num_of_tables):
            for i in range(info.num_of_rows):
                if i % 2 == 0:
                    continue

                ts = start_ts + i * 1000
                cursor.execute(f"insert into db.c{j} values({ts}, {i+1000}, '1', {i+1000})")

    info.insert_complete = True

    cursor.close()

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


class TestStreamTriggerSession:

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

        Labels: common,ci,skip

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

        self.prepare_source_table(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_1("sm1", "tb1")
        # except Exception as e:
        #     tdLog.error(f"case 1 error: {e}")

        # clear_output("sm1", "tb1")
        # self.prepare_source_table(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_2("sm2", "tb2", info)
        # except Exception as e:
        #     tdLog.error(f"case 2 error: {e}")

        # clear_output("sm2", "tb2")
        # self.prepare_source_table(100000, 1, info)
        #
        # try:
        #     self.create_and_check_stream_basic_3("sm3", "tb3", info)
        # except Exception as e:
        #     tdLog.error(f"case 3 error: {e}")

        # clear_output("sm3", "tb3")
        # self.prepare_source_table(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_4("sm4", "tb4", info)
        # except Exception as e:
        #     tdLog.error(f"case 4 error: {e}")

        # clear_output("sm4", "tb4")
        # self.prepare_source_table(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_5("sm5", "tb5", info)
        # except Exception as e:
        #     tdLog.error(f"case 5 error: {e}")
        #
        # clear_output("sm5", "tb5")
        # self.prepare_source_table(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_6("sm6", "tb6", info)
        # except Exception as e:
        #     tdLog.error(f"case 6 error: {e}")

        # clear_output("sm6", "tb6")
        # self.prepare_source_table(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_7("sm7", "tb7", info)
        # except Exception as e:
        #     tdLog.error(f"case 7 error: {e}")
        #
        # clear_output("sm7", "tb7")
        # self.prepare_source_table(10000, 10, info)
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

        # clear_output("sm9", "tb9")
        # self.prepare_source_table(1000, 10, False, info)
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

        # clear_output("sm12", "tb12")
        # self.prepare_source_table(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_13("sm13", "tb13", info)
        # except Exception as e:
        #     tdLog.error(f"case 13 error: {e}")

        # clear_output("sm13", "tb13")
        # self.prepare_source_table(5000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_14("sm14", "tb14", info)
        # except Exception as e:
        #     tdLog.error(f"case 14 error: {e}")

        clear_output("sm14", "tb14")
        self.prepare_source_table(5000, 10, info)
        try:
            self.create_and_check_stream_basic_15("sm15", "tb15", info)
        except Exception as e:
            tdLog.error(f"case 15 error: {e}")

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

    def prepare_source_table(self, num_of_rows, num_of_tables, info: WriteDataInfo) -> None:
        info.num_of_tables, info.num_of_rows = num_of_tables, num_of_rows
        tdLog.info("prepare tables for trigger")

        tdSql.execute("use db")

        stb = "create table if not exists source_table (ts timestamp, k int, c1 varchar(12), c2 double) tags(a int)"
        tdSql.execute(stb)

        for i in range(num_of_tables):
            tdSql.execute(f"create table if not exists c{i} using source_table tags({i})")
            
    def wait_for_stream_completed(self) -> None:
        tdLog.info(f"wait total:{len(self.streams)} streams run finish")
        tdStream.checkStreamStatus()

    def check_results(self) -> None:
        tdLog.info(f"check total:{len(self.streams)} streams result")
        for stream in self.streams:
            stream.checkResults()

    def create_and_check_stream_basic_1(self, stream_name, dst_table, info:WriteDataInfo) -> None:
        """simple 1 Pass """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 100s) from source_table into {dst_table} as "
            f"select cast(_tlocaltime/1000000 as timestamp) ts, count(*) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend")
        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select last(c) from {dst_table}", info.num_of_rows)
        # check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_2(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 2
            Error: results are incorrect
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 100a) from source_table into {dst_table} as "
            f"select _twstart, _twend, cast(_tlocaltime/1000000 as timestamp) ts, now(), \'abcdefg\', count(*), sum(k), first(k), max(k) c, concat('abc', cast(_tlocaltime as varchar(1))) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend")
        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_rows * info.num_of_tables)
        # check_ts_step(tb_name=dst_table, freq=30)


    def create_and_check_stream_basic_3(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 3: pass"""
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 30s) from source_table into {dst_table} as "
            f"select _wstart ts, count(*) k, last(k) c "
            f"from source_table interval(1s)")
        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select last(c) from {dst_table}", info.num_of_rows)
        check_ts_step(tb_name=dst_table, freq=1)

    def create_and_check_stream_basic_4(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """
        simple 6:
        Error: maybe report error???
        """
        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} session(ts, 30s) into {dst_table} as "
                      f"select cast(_tlocaltime/1000000 as timestamp) ts, _wstart ats, count(*) k, last(k) c "
                      f"from source_table interval(1s)")

        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)

        check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_5(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 5: Pass """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 30s) from source_table partition by tbname,a  into {dst_table} as "
            f"select _twstart ts, count(*) k, last(k) last_k, first(k) first_k "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(last_k) from tb5", info.num_of_rows - 1)
        # check_ts_step(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_6(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 6:
           Error: expect return error
        """
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 30s) from source_table partition by tbname into {dst_table} as "
            f"select ts, k c, c1, c2 "
            f"from source_table partition by tbname")
        tdLog.info(f"create stream completed, and wait for it completed")

        do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)

        check_all_results(f"select count(*) from {dst_table}", [[info.num_of_tables * info.num_of_rows]])

    def create_and_check_stream_basic_7(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 7:
           ERROR:
        """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 3s) from source_table partition by tbname into {dst_table} as "
            f"select  _twstart ts, _twend te, count(*),  max(k) c "
            f"from source_table partition by tbname "
            f"state_window(cast(c1 as int))")

        tdLog.info(f"create stream completed, start to write data after 10sec")

        info.delete_data = False
        info.update_data = False
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select max(c) from {dst_table} group by tbname",
                          [[999], [999], [999], [999], [999], [999], [999], [999], [999], [999]])

    def create_and_check_stream_basic_8(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 8:
           Error: recalculate failed
        """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 1100a) from source_table partition by tbname stream_options(DELETE_RECALC) into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, start to write data after 10sec")

        info.delete_data = True
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select max(c) from {dst_table}", info.num_of_rows - 1)
        check_all_results(f"select count(*) from {dst_table} ", [[5000]])


    def create_and_check_stream_basic_9(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 9: Pass
        """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 30s) from source_table partition by tbname into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c, sum(k), first(ts), last(ts) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, start to write data after 10sec")

        info.update_data = True
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables)
        check_all_results(f"select `count(*)`, `c`, `sum(k)` from {dst_table} ",
                          [[1000, 1999, 999500], [1000, 1999, 999500],
                           [1000, 1999, 999500], [1000, 1999, 999500],
                           [1000, 1999, 999500], [1000, 1999, 999500],
                           [1000, 1999, 999500], [1000, 1999, 999500],
                           [1000, 1999, 999500], [1000, 1999, 999500]])

    def create_and_check_stream_basic_10(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 10: Pass
        """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 3s) from source_table partition by tbname into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c, sum(k), first(ts), last(ts) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, start to write data after 10sec")
        tdStream.checkStreamStatus(stream_name)

        info.insert_complete = True
        tdSql.execute("insert into c0 values('2025-01-01 10:10:10.5', '1', '1', '1') ('2025-01-01 10:10:15.5', '2', '2', '2')"
                      "('2025-01-01 10:10:20.5', '3', '3', '3')('2025-01-01 10:10:25.5', '4', '4', '4')"
                      "('2025-01-01 10:10:30.5', '5', '5', '5')")

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 4)


    def create_and_check_stream_basic_11(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 11: invalid results """
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} session(ts, 3s) from source_table partition by tbname into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c, sum(k), first(ts), last(ts) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, start to write data after 10sec")
        tdStream.checkStreamStatus(stream_name)

        info.insert_complete = True
        tdSql.execute("insert into c0 values('2025-01-01 10:10:10.5', '1', '1', '1') ('2025-01-01 10:10:15.5', '2', '2', '2')"
                      "('2025-01-01 10:10:20.5', '3', '3', '3')('2025-01-01 10:10:25.5', '4', '4', '4')"
                      "('2025-01-01 10:10:30.5', '5', '5', '5')")

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 4)

        tdSql.execute("insert into c0 values('2025-01-01 10:10:13', '1', '1', '1')('2025-01-01 10:10:18', '1', '1', '1')"
                      "('2025-01-01 10:10:23', '1', '1', '1')('2025-01-01 10:10:28', '1', '1', '1')")

        time.sleep(5)
        check_all_results(f"select st, et, `count(*)`, c, `sum(k)` from {dst_table} ",
                          [['2025-01-01 10:10:10.500', '', 9, 5, 19]])


    def create_and_check_stream_basic_12(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 12: invalid results """
        tdSql.execute("use db")

        tdSql.execute("create vtable vtb_1 (ts timestamp, col_1 int from c0.k, col_2 varchar(12) from c1.c1, "
                      "col_3 double from c2.c2)")
        tdSql.execute("create vtable vtb_2 (ts timestamp, col_1 int from c3.k, col_2 varchar(12) from c4.c1, "
                      "col_3 double from c5.c2)")
        time.sleep(10)

        tdSql.execute(
            f"create stream {stream_name} session(ts, 1100a) from source_table partition by tbname into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(vtb_1.col_1) c, sum(vtb_2.col_3), first(c6.c1), spread(vtb_1.col_3)  "
            f"from vtb_1, vtb_2, c6 "
            f"where _c0 >= _twstart and _c0 <= _twend and vtb_1.ts=vtb_2.ts and vtb_1.ts = c6.ts ")

        tdLog.info(f"create stream completed, start to write data after 10sec")
        tdStream.checkStreamStatus(stream_name)
        do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 4)
        #
        # tdSql.execute("insert into c0 values('2025-01-01 10:10:13', '1', '1', '1')('2025-01-01 10:10:18', '1', '1', '1')"
        #               "('2025-01-01 10:10:23', '1', '1', '1')('2025-01-01 10:10:28', '1', '1', '1')")

        # time.sleep(5)
        # check_all_results(f"select st, et, `count(*)`, c, `sum(k)` from {dst_table} ",
        #                   [['2025-01-01 10:10:10.500', '', 9, 5, 19]])

        # todo random: session interval insert 10w records.

    def create_and_check_stream_basic_13(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 13: invalid results """
        tdSql.execute("use db")
        time.sleep(3)

        tdSql.error(f"create stream {stream_name} session(col_1, 1s) from source_table partition by tbname into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(vtb_1.col_1) c, sum(vtb_2.col_3), first(c6.c1), spread(vtb_1.col_3)  "
                    f"from vtb_1, vtb_2, c6 "
                    f"where _c0 >= _twstart and _c0 <= _twend and vtb_1.ts=vtb_2.ts and vtb_1.ts = c6.ts ")

        tdSql.error(f"create stream {stream_name} session(col_1, 1s) from source_table partition by tbname into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 "
                    f"where _c0 >= _twstart and _c0 <= _twend ")

        tdSql.error(f"create stream {stream_name} session(ts, -1s) from source_table partition by tbname into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 "
                    f"where _c0 >= _twstart and _c0 <= _twend ")

        tdSql.error(f"create stream {stream_name} session(ts, 1s) from source_table partition by k into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 ")

        tdSql.error(f"create stream {stream_name} session(ts, 1y) from source_table into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 ")

        tdSql.error(f"create stream {stream_name} session(ts, 0a) from source_table into {dst_table} as"
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 ")

        tdSql.error(f"create stream {stream_name} session(ts) from source_table into {dst_table} as "
                    f"select _twstart st, _twend et, count(*),  max(k) c "
                    f"from c6 ")

