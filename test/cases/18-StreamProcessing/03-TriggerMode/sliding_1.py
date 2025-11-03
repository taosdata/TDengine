import threading
import time

from new_test_framework.utils import tdLog, clusterComCheck, tdStream, tdSql
from test_period_1 import wait_for_insert_complete, check_all_results, wait_for_stream_done, check_ts_step, \
    clear_output, get_conf_dir, WriteDataInfo, do_write_data_fn


class TestStreamTriggerSliding:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """basic test

        Verification testing during the development process.

        Catalog:
            - Streams: 01-snode

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
        self.conf = ""

        self.create_env()
        info = WriteDataInfo(1000, 10)

        # self.prepare_tables(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_1("sm1", "tb1", info)
        # except Exception as e:
        #     tdLog.error(f"case 1 error: {e}")
        #
        # clear_output("sm1", "tb1")
        # self.prepare_tables(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_2("sm2", "tb2", info)
        # except Exception as e:
        #     tdLog.error(f"case 2 error: {e}")
        #
        # clear_output("sm2", "tb2")
        # self.prepare_tables(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_3("sm3", "tb3", info)
        # except Exception as e:
        #     tdLog.error(f"case 3 error: {e}")

        # clear_output("sm3", "tb3")
        # self.prepare_tables(1000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_4("sm4", "tb4", info)
        # except Exception as e:
        #     tdLog.error(f"case 4 error: {e}")

        # clear_output("sm4", "tb4")
        # self.prepare_tables(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_5("sm5", "tb5", info)
        # except Exception as e:
        #     tdLog.error(f"case 5 error: {e}")
        #
        # clear_output("sm5", "tb5")
        # self.prepare_tables(100000, 1, info)
        # try:
        #     self.create_and_check_stream_basic_6("sm6", "tb6", info)
        # except Exception as e:
        #     tdLog.error(f"case 6 error: {e}")

        # clear_output("sm6", "tb6")
        # self.prepare_tables(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_7("sm7", "tb7", info)
        # except Exception as e:
        #     tdLog.error(f"case 7 error: {e}")
        #
        # clear_output("sm7", "tb7")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_8("sm8", "tb8", info)
        # except Exception as e:
        #     tdLog.error(f"case 8 error: {e}")
        #
        # clear_output("sm8", "tb8")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_9("sm9", "tb9", info)
        # except Exception as e:
        #     tdLog.error(f"case 9 error: {e}")
        #
        # clear_output("sm9", "tb9")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_10("sm10", "tb10", info)
        # except Exception as e:
        #     tdLog.error(f"case 10 error: {e}")
        #
        # clear_output("sm10", "tb10")
        # self.prepare_tables(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_11("sm11", "tb11", info)
        # except Exception as e:
        #     tdLog.error(f"case 11 error: {e}")
        #
        # clear_output("sm11", "tb11")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_12("sm12", "tb12", info)
        # except Exception as e:
        #     tdLog.error(f"case 12 error: {e}")
        #
        # clear_output("sm12", "tb12")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_13("sm13", "tb13", info)
        # except Exception as e:
        #     tdLog.error(f"case 13 error: {e}")

        # clear_output("sm13", "tb13")
        # self.prepare_tables(1000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_14("sm14", "tb14", info)
        # except Exception as e:
        #     tdLog.error(f"case 14 error: {e}")

        # clear_output("sm14", "tb14")
        # self.prepare_tables(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_15("sm15", "tb15", info)
        # except Exception as e:
        #     tdLog.error(f"case 15 error: {e}")

        # clear_output("sm15", "tb15")
        # self.prepare_tables(10000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_16("sm16", "tb16", info)
        # except Exception as e:
        #     tdLog.error(f"case 16 error: {e}")

        # clear_output("sm16", "tb16")
        # self.prepare_tables(5000, 10, info)
        # try:
        #     self.create_and_check_stream_basic_17("sm17", "tb17", info)
        # except Exception as e:
        #     tdLog.error(f"case 17 error: {e}")

        clear_output("sm17", "tb17")
        self.prepare_tables(100, 10, info)
        try:
            self.create_and_check_stream_basic_18("sm18", "tb18", info)
        except Exception as e:
            tdLog.error(f"case 18 error: {e}")

    def create_env(self):
        tdLog.info(f"create {self.num_snode} snode(s)")
        for i in range(self.num_snode):
            tdStream.createSnode(i + 1)

        self.create_database()

    def create_database(self) -> None:
        tdLog.info(f"create database with {self.num_vgroups} vgroups")
        tdSql.prepare(dbname="db", vgroups=self.num_vgroups)
        clusterComCheck.checkDbReady("db")


    def prepare_tables(self, num_of_rows, num_of_tables, info: WriteDataInfo) -> None:
        info.num_of_rows, info.num_of_tables = num_of_rows, num_of_tables

        tdLog.info("write data to source table in other thread")
        
        self.conf = get_conf_dir("taosd")
        tdSql.execute("use db")

        stb = "create table if not exists source_table (ts timestamp, k int, c1 varchar(12), c2 double) tags(a int)"
        tdSql.execute(stb)

        for i in range(num_of_tables):
            tdSql.execute(f"create table if not exists c{i} using source_table tags({i})")

    def do_write_data(self, stream_name:str, info: WriteDataInfo):
        while True:
            tdSql.query(f"select status from information_schema.ins_streams where stream_name='{stream_name}'")
            if tdSql.getData(0, 0) != "Running":
                print("stream not running, waiting....")
                time.sleep(10)
            else:
                break

        conf = get_conf_dir("taosd")

        tdLog.info(f"stream {stream_name} is ready, start to insert data")
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
        """simple 1 Pass. """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} sliding(30s) from source_table into {dst_table} as select _tcurrent_ts ts, cast(_tlocaltime/1000000 as timestamp) exec_ts, count(*) c, sum(k) sum, "
            f"avg(c2) avg_val  from source_table")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 3)

        check_all_results(f"select c, `sum`, `avg_val` from {dst_table}",
                          [[10000, 49995000, 4999.5, ], [40000, 799980000, 19999.5, ], [70000, 2449965000, 34999.5, ]])

    def create_and_check_stream_basic_2(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 2: pass """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(3s) sliding(100a) from source_table into {dst_table} as "
            f"select _twstart ts, _twend, cast(_tlocaltime/1000000 as timestamp) exec_ts, count(*) c, sum(k) sum "
            f"from source_table where _c0 >= _twstart and _c0 < _twend")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 10)
        check_ts_step(tb_name=dst_table, freq=0.1)

    def create_and_check_stream_basic_3(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 3
            Error: calculated results are incorrect
            NOTE: not check the results yet.
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(10a) sliding(3s) from source_table into {dst_table} as "
            f"select _tprev_ts `prev`, _tcurrent_ts ts, _tnext_ts et, _tcurrent_ts current_ts, "
            f" first(ts), last(ts), count(*), first(k), last(k), sum(k), \'abcdefg\', concat('abc', cast(_tlocaltime as varchar(1))) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        
        self.do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 3)

        # check_results_by_compare(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_4(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 4
           Pass: Maybe correct, but too slow
        """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(10a) sliding(10a) from source_table into {dst_table} as "
            f"select _tcurrent_ts ts, _tprev_ts `prev`, _tnext_ts et, count(*) c, last(c1) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 100)
        # self.check_results_by_compare(tb_name=dst_table, freq=30)

    def create_and_check_stream_basic_5(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 5: invalid results"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(2s) sliding(2s) from source_table into {dst_table} as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend "
            )

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 5)


    def create_and_check_stream_basic_6(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """
        simple 6:
        maybe the same reason as simple 1 case, no results generated in the results tables
        """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(f"create stream {stream_name} interval(30s) sliding(30s) from source_table "
                      f"partition by tbname "
                      f"into {dst_table} as "
                      f"select _tcurrent_ts ts, count(*) k, last(k) c "
                      f"from source_table "
                      f"where _c0 >= _twstart and _c0 < _twend")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 4)
        check_all_results(f"select k, c, tag_tbname from {dst_table}",
                          [[10000, 10000, "c0"], [30000, 30000, 'c0'], [30000, 30000, 'c0'], [30000, 30000, 'c0']])

    def create_and_check_stream_basic_7(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 7: Pass
        """
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(30s) sliding(30s) from source_table partition by tbname, a into {dst_table} "
            f"as "
            f"select _tcurrent_ts ts, count(c1) c, last(k) last_val, tbname tb, %%tbname tx "
            f"from source_table group by tbname, a ")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 10)
        check_all_results(f"select c, last_val, tx from {dst_table}",
                          [[10000, 9999, "c0"], [10000, 9999, "c1"], [10000, 9999, "c2"],
                           [10000, 9999, "c3"], [10000, 9999, "c4"], [10000, 9999, "c5"],
                           [10000, 9999, "c6"], [10000, 9999, "c7"], [10000, 9999, "c8"],
                           [10000, 9999, "c9"]])

    def create_and_check_stream_basic_8(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 8: Pass
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(100a) sliding(100a) from source_table partition by tbname into {dst_table} as "
            f"select ts, k c, c1, c2 "
            f"from source_table partition by tbname")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables * info.num_of_rows)

    def create_and_check_stream_basic_9(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 9:
           Error: results error
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(30s) sliding(30s) from source_table partition by tbname into {dst_table} as "
            f"select  _tcurrent_ts ts, _wstart wst, _wend te, count(*),  max(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend "
            f"partition by tbname state_window(cast(c1 as int))")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 10)

    def create_and_check_stream_basic_10(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 10:
           Error: results error
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(3s) sliding(3s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart wst, _wend et, _twstart, _twend, count(*),  max(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend "
            f"partition by tbname session(ts, 1s)")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables)

    def create_and_check_stream_basic_11(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 11:
           Error: results error
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(30s) sliding(30s) from source_table partition by tbname into {dst_table} as "
            f"select _wstart wst, _wend et, _twstart, _twend, count(*) count_k, max(k) max_k, min(k) min_k, sum(k) sum_k "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend "
            f"partition by tbname count_window(10)")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables)

    def create_and_check_stream_basic_12(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 12:
           Error: results are incorrect
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(3s) sliding(3s) from source_table partition by tbname into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend "
            f"partition by tbname ")

        tdLog.info(f"create stream completed, and wait for stream completed")
        self.do_write_data(stream_name, info)

        tdSql.execute(f"create table if not exists c_new using source_table tags(100)")
        tdSql.execute(f"insert into c_new values(1710000000000, 999, '10', 999)(1710000010000, 1000, '10', 1000)"
                      f"(1710000020000, 2000, '20', 2000)")

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables)
        # self.check_all_results(f"select count(*) from {dst_table} ", [(10000,)])

    def create_and_check_stream_basic_13(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 13:
           Error: no results generated
        """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)
        
        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(3s) sliding(3s) from source_table partition by tbname, a into {dst_table} as "
            f"select _twstart st, _twend et, first(ts), last(ts), count(*),  max(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend  "
            f" partition by a ")

        self.do_write_data(stream_name, info)
        tdLog.info(f"create stream completed, and wait for stream completed")

        # tdSql.execute(f"alter table c2 set tag a=1")
        # tdSql.execute(f"insert into c2 values(1710000000000, 999, '10', 999)")

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", info.num_of_tables)

        # self.check_all_results(f"select count(*) from {dst_table} ", [(10000,)])

    def create_and_check_stream_basic_14(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 14: Pass """
        tdLog.info(f"start exec stream {stream_name}")
        time.sleep(10)

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(3s) sliding(3s) from source_table partition by tbname stream_options(DELETE_OUTPUT_TABLE) into {dst_table} as "
            f"select _twstart st, _twend et, count(*),  max(k) c, sum(k), first(ts), last(ts) "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 <= _twend group by tbname")

        tdLog.info(f"create stream completed, start to write data after 10sec")
        tdStream.checkStreamStatus(stream_name)

        info.insert_complete = True
        tdSql.execute("insert into c0 values('2025-01-01 10:10:10.5', '1', '1', '1') ('2025-01-01 10:10:15.5', '2', '2', '2')"
                      "('2025-01-01 10:10:20.5', '3', '3', '3')('2025-01-01 10:10:25.5', '4', '4', '4')"
                      "('2025-01-01 10:10:30.5', '5', '5', '5')")

        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 7)

        tdSql.execute("drop table c0")
        tdSql.execute("create table c0 using source_table tags(1234567)")

        tdSql.execute("insert into c0 values('2025-01-01 10:10:13', '1', '1', '1')('2025-01-01 10:10:18', '1', '1', '1')"
                      "('2025-01-01 10:10:23', '1', '1', '1')('2025-01-01 10:10:28', '1', '1', '1')")

        time.sleep(5)
        check_all_results(f"select count(*) from information_schema.ins_tables where stable_name='{dst_table}'",
                          [[1]])

    def create_and_check_stream_basic_15(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 5: invalid results"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(2s) sliding(2s) from source_table into {dst_table} as "
            f"select _twstart, _tcurrent_ts ts, _wstart wst, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend "
            f"interval(1s) order by wst desc limit 1 offset 0")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 5)


    def create_and_check_stream_basic_16(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 16: Pass"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name}_0 interval(10a) sliding(10a) from source_table stream_options(LOW_LATENCY_CALC) into {dst_table}_0 as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend ")

        tdSql.execute(
            f"create stream {stream_name}_1 interval(10a) sliding(10a) from source_table stream_options(LOW_LATENCY_CALC) into {dst_table}_1 as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend ")

        tdSql.execute(
            f"create stream {stream_name}_2 interval(10a) sliding(10a) from source_table stream_options(LOW_LATENCY_CALC) into {dst_table}_2 as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend ")

        tdSql.execute(
            f"create stream {stream_name}_3 interval(10a) sliding(10a) from source_table stream_options(LOW_LATENCY_CALC) into {dst_table}_3 as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend ")

        tdSql.execute(
            f"create stream {stream_name}_4 interval(10a) sliding(10a) from source_table stream_options(LOW_LATENCY_CALC) into {dst_table}_4 as "
            f"select _twstart, _tcurrent_ts ts, count(*) k, first(c2), sum(c2), last(k) c "
            f"from source_table "
            f"where _c0 >= _twstart and _c0 < _twend ")

        tdStream.checkStreamStatus(f"{stream_name}_0")
        tdStream.checkStreamStatus(f"{stream_name}_1")
        tdStream.checkStreamStatus(f"{stream_name}_2")
        tdStream.checkStreamStatus(f"{stream_name}_3")
        tdStream.checkStreamStatus(f"{stream_name}_4")

        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(f"{stream_name}_4", info)
        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}_0", 1000)

    def create_and_check_stream_basic_17(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 17: taosd crash"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("create vtable vtb_1 (ts timestamp, col_1 int from c0.k, col_2 varchar(12) from c1.c1, "
                      "col_3 double from c2.c2)")

        tdSql.execute("use db")
        tdSql.execute(
            f"create stream {stream_name} interval(100a) sliding(100a) "
            f"from vtb_1 stream_options(pre_filter(count(col_3) > substr(cast(now() as varchar), 1, 2)))  into {dst_table} as "
            f"select _twstart, _twend, _tcurrent_ts ts, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9, c0 "
            f"where _c0 >= _twstart and _c0 < _twend and c9.ts=c0.ts")

        tdStream.checkStreamStatus(stream_name)
        tdLog.info(f"create stream completed, and start to write data after 10sec")
        self.do_write_data(stream_name, info)
        wait_for_stream_done(dst_table, f"select count(*) from {dst_table}", 1000)

    def create_and_check_stream_basic_18(self, stream_name, dst_table, info: WriteDataInfo) -> None:
        """simple 18:"""
        tdLog.info(f"start exec stream {stream_name}")

        tdSql.execute("create vtable vtb_1 (ts timestamp, col_1 int from c0.k, col_2 varchar(12) from c1.c1, "
                      "col_3 double from c2.c2)")

        tdSql.execute("use db")

        tdSql.error(
            f"create stream {stream_name} sliding(1n) "
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} sliding(5s, -10s) "
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} sliding(0s, 10s) "
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} interval(10s, -1s) sliding(10s, 10s) "
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} interval(10s, 1s)"
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} sliding(10s) interval(10s, 1s)"
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} interval(10s) sliding(10s) "
            f"from vtb_1 partition by col_2 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")

        tdSql.error(
            f"create stream {stream_name} interval(1n) sliding(1n) "
            f"from vtb_1 into {dst_table} as "
            f"select _twstart, _twend, count(c9.*) k, first(c9.c2), sum(c9.c2), last(c9.k) c "
            f"from c9 where _c0 >= _twstart and _c0 < _twend ")


