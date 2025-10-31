from new_test_framework.utils import tdLog, tdSql, StreamItem, tdStream

class TestStateWindowZeroth:
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")

    def prepare_data(self):
        tdStream.createSnode()
        tdSql.execute("create database if not exists test_state_window_zeroth keep 3650", show=True)
        tdSql.execute("use test_state_window_zeroth", show=True)
        tdSql.execute("create table stb (ts timestamp, cint int, cbool bool, cstr binary(20), cfloat float) tags (gid int)", show=True)
        tdSql.execute("create table ctb1 using stb tags(1)", show=True)
        tdSql.execute("create table ctb2 using stb tags(2)", show=True)
        tdSql.execute("create table ntb (ts timestamp, cint int, cbool bool, cstr binary(20), cfloat float, cnchar nchar(20))", show=True)
        tdSql.execute('''insert into ctb1 values
                  ('2025-10-01 11:55:00',    1,  true, 'a', 1.1),
                  ('2025-10-01 11:56:00',    2, false, 'a', 2.2),
                  ('2025-10-01 11:57:00',    2, false, 'c', 3.3),
                  ('2025-10-01 11:58:00',    3, false, 'b', 4.4),
                  ('2025-10-01 11:59:00',    3,  true, 'd', 5.5),
                  ('2025-10-01 12:00:00', null, false, 'a', 6.6),
                  ('2025-10-01 12:01:00',    3,  true, 'b', 7.7),
                  ('2025-10-01 12:02:00',    1,  true, 'b', 8.8),
                  ('2025-10-01 12:03:00',    1, false, null, 9.9),
                  ('2025-10-01 12:04:00',    2, false, 'd', 10.10),
                  ('2025-10-01 12:05:00',    2,  true, 'd', 11.11)''', show=True)
        tdSql.execute('''insert into ctb2 values
                  ('2025-10-01 11:55:00',    1,  true, 'a', 1.1),
                  ('2025-10-01 11:56:00',    2, false, 'a', 2.2),
                  ('2025-10-01 11:57:00',    2, false, 'c', 3.3),
                  ('2025-10-01 11:58:00',    3, false, 'b', 4.4),
                  ('2025-10-01 11:59:00',    3,  true, 'd', 5.5),
                  ('2025-10-01 12:00:00', null, false, 'a', 6.6),
                  ('2025-10-01 12:01:00',    3,  true, 'b', 7.7),
                  ('2025-10-01 12:02:00',    1,  true, 'b', 8.8),
                  ('2025-10-01 12:03:00',    1, false, null, 9.9),
                  ('2025-10-01 12:04:00',    2, false, 'd', 10.10),
                  ('2025-10-01 12:05:00',    2,  true, 'd', 11.11)''', show=True)
        tdSql.execute('''insert into ntb values
                  ('2025-10-01 11:55:00',    1,  true, 'a', 1.1, '正常'),
                  ('2025-10-01 11:56:00',    2, false, 'a', 2.2, '异常'),
                  ('2025-10-01 11:57:00',    2, false, 'c', 3.3, '异常'),
                  ('2025-10-01 11:58:00',    3, false, 'b', 4.4, '正常'),
                  ('2025-10-01 11:59:00',    3,  true, 'd', 5.5, '正常'),
                  ('2025-10-01 12:00:00', null, false, 'a', 6.6, '未知'),
                  ('2025-10-01 12:01:00',    3,  true, 'b', 7.7, '正常'),
                  ('2025-10-01 12:02:00',    1,  true, 'b', 8.8, '正常'),
                  ('2025-10-01 12:03:00',    1, false, null, 9.9, '未知'),
                  ('2025-10-01 12:04:00',    2, false, 'd', 10.10, '正常'),
                  ('2025-10-01 12:05:00',    2,  true, 'd', 11.11, '正常')''', show=True)

    def check_zeroth_state_query(self):
        # invalid zeroth parameter
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, null)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, cint)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, cint+1)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, 1/1)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, 1.5)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cint, 0, asdf)", show=True)
        tdSql.error("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, null)", show=True)

        # normal table
        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, 0)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(4, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, 1)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, -1)", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(4, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, true)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, false)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cstr, 0, 'a')", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(4, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cstr, 0, 'A')", show=True)
        tdSql.checkRows(7)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(5, 2, 2)
        tdSql.checkData(6, 2, 2)

        # test cast 
        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, '2')", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, 'A')", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(4, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, true)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, '1.5')", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cint, 0, '100A')", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(4, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 'true')", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 'false')", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 0)", show=True)
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 1)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cbool, 0, 10)", show=True)
        tdSql.checkRows(3)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cstr, 0, 97)", show=True)
        tdSql.checkRows(7)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(5, 2, 2)
        tdSql.checkData(6, 2, 2)

        tdSql.query("select _wstart, _wend, count(*) from ntb state_window(cstr, 0, true)", show=True)
        tdSql.checkRows(7)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(4, 2, 1)
        tdSql.checkData(5, 2, 2)
        tdSql.checkData(6, 2, 2)

        tdSql.query("select _wstart, _wend, count(*), cnchar from ntb state_window(cnchar, 0, '未知')", show=True)
        tdSql.checkRows(5)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, '正常')
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, '异常')
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, '正常')
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 3, '正常')
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, '正常')

        # super table 
        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cint, 0, 0)", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cint, 0, 1)", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cint, 0, -1)", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cbool, 0, true)", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cbool, 0, false)", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cstr, 0, 'a')", show=True)

        tdSql.error("select _wstart, _wend, count(*) from stb state_window(cstr, 0, 'A')", show=True)

    def check_zeroth_state_stream_compute(self):
        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=0,
            stream='''create stream st0 count_window(1) from ctb1
                        into res_st0 as select _wstart, _wduration,
                        _wend, count(*) cnt_all, sum(cfloat) sum_cfloat
                        from ctb1 state_window(cint, 0, 1)''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat
                        from res_st0''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 state_window(cint, 0) having(cint != 1)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=1,
            stream='''create stream st1 count_window(1) from ctb1
                        into res_st1 as select _wstart, _wduration,
                        _wend, count(*) cnt_all, sum(cfloat) sum_cfloat
                        from ctb1 state_window(cbool, 0, false)''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat
                        from res_st1''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 state_window(cbool, 0) having(cbool != false)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=2,
            stream='''create stream st2 count_window(1) from ctb1
                        into res_st2 as select _wstart, _wduration,
                        _wend, count(*) cnt_all, sum(cfloat) sum_cfloat, cstr
                        from ctb1 state_window(cstr, 0, 'b')''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat, cstr
                        from res_st2''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat), cstr
                        from ctb1 state_window(cstr, 0) having(cstr != 'b')''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=3,
            stream='''create stream st3 count_window(1) from stb
                        partition by tbname into res_st3 as
                        select _wstart, _wduration, _wend, count(*) cnt_all,
                        sum(cfloat) sum_cfloat from %%tbname state_window(cint, 0, 2)''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat
                        from res_st3''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 state_window(cint, 0, 2)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=4,
            stream='''create stream st4 count_window(1) from stb
                        partition by tbname into res_st4 as
                        select _wstart, _wduration, _wend, count(*) cnt_all,
                        sum(cfloat) sum_cfloat from %%tbname state_window(cbool, 0, true)''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat
                        from res_st4''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 state_window(cbool, 0, true)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=5,
            stream='''create stream st5 count_window(1) from stb
                        partition by tbname into res_st5 as
                        select _wstart, _wduration, _wend, count(*) cnt_all,
                        sum(cfloat) sum_cfloat, cstr from %%tbname state_window(cstr, 0, 'a')''',
            res_query='''select _wstart, _wduration, _wend, cnt_all, sum_cfloat, cstr
                        from res_st5''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat), cstr
                        from ctb1 state_window(cstr, 0, 'a')''',
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute('insert into ctb1 values("2025-10-01 12:06:00", 3, true, "d", 12.12)', show=True)

        # check results
        for s in streams:
            s.checkResults()

    def check_zeroth_state_stream_trigger(self):
        # create streams
        streams: list[StreamItem] = []
        stream = StreamItem (
            id=6,
            stream='''create stream st6 state_window(cint, 0, 3) from ctb1 into
                        res_st6 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows;''',
            res_query='''select * from res_st6''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cint, 0) having(cint != 3)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=7,
            stream='''create stream st7 state_window(cbool, 0, true) from ctb1 into
                        res_st7 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows;''',
            res_query='''select * from res_st7''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cbool, 0) having(cbool != true)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=8,
            stream='''create stream st8 state_window(cstr, 0, 'c') from ctb1 into
                        res_st8 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows;''',
            res_query='''select * from res_st8''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cstr, 0) having(cstr != 'c')''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=9,
            stream='''create stream st9 state_window(cint, 0, 1) from stb partition by tbname
                        into res_st9 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname
                        where ts >= _twstart and ts <= _twend;''',
            res_query='''select wstart, wdur, wend, cnt_all, sum_cfloat from res_st9''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cint, 0) having(cint != 1)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=10,
            stream='''create stream st10 state_window(cbool, 0, true) from stb partition by tbname
                        into res_st10 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname
                        where ts >= _twstart and ts <= _twend;''',
            res_query='''select wstart, wdur, wend, cnt_all, sum_cfloat from res_st10''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cbool, 0) having(cbool != true)''',
        )
        streams.append(stream)

        stream = StreamItem (
            id=11,
            stream='''create stream st11 state_window(cstr, 0, 'd') from stb partition by tbname
                        into res_st11 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%tbname
                        where ts >= _twstart and ts <= _twend;''',
            res_query='''select wstart, wdur, wend, cnt_all, sum_cfloat from res_st11''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb1 where ts >= "2025-10-02" and ts < "2025-10-03"
                        state_window(cstr, 0) having(cstr != 'd')''',
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute('''insert into ctb1 values
                  ('2025-10-02 11:55:00',    1,  true, 'a', 1.1),
                  ('2025-10-02 11:56:00',    2, false, 'a', 2.2),
                  ('2025-10-02 11:57:00',    2, false, 'c', 3.3),
                  ('2025-10-02 11:58:00',    3, false, 'b', 4.4),
                  ('2025-10-02 11:59:00',    3,  true, 'd', 5.5),
                  ('2025-10-02 12:00:00', null, false, 'a', 6.6),
                  ('2025-10-02 12:01:00',    3,  true, 'b', 7.7),
                  ('2025-10-02 12:02:00',    1,  true, 'b', 8.8),
                  ('2025-10-02 12:03:00',    1, false, null, 9.9),
                  ('2025-10-02 12:04:00',    2, false, 'd', 10.10),
                  ('2025-10-02 12:05:00',    2,  true, 'd', 11.11),
                  ('2025-10-03 12:00:00',    4, false, 'e', 12.21)''', show=True)

        # check results
        for s in streams:
            s.checkResults()
    
    def check_zeroth_state_stream_trigger_history(self):
        stream = StreamItem (
            id=12,
            stream='''create stream st12 state_window(cbool, 0, true) from ctb2 STREAM_OPTIONS(fill_history)
                        into res_st12 as select _twstart wstart, _twduration wdur, _twend wend,
                        count(*) cnt_all, sum(cfloat) sum_cfloat from %%trows;''',
            res_query='''select wstart, wdur, wend, cnt_all, sum_cfloat from res_st12''',
            exp_query='''select _wstart, _wduration, _wend, count(*), sum(cfloat)
                        from ctb2 where ts >= "2025-10-01" and ts < "2025-10-02"
                        state_window(cbool) having(cbool != true)''',
        )
        stream.createStream()
        tdStream.checkStreamStatus()
        stream.checkResults()
        

    # run tests
    def test_state_window_zeroth(self):
        """summary: test zeroth parameter in state window

        description: test zeroth parameter in state window
            in both batch query and stream computing scenarios

        Since: v3.4.0.0

        Labels: state window, zeroth, stream

        Jira: TS-7129

        Catalog:
            - Query:Window

        History:
            - 2025-10-15 Tony Zhang: created

        """
        self.prepare_data()
        self.check_zeroth_state_query()
        self.check_zeroth_state_stream_compute()
        self.check_zeroth_state_stream_trigger()
        self.check_zeroth_state_stream_trigger_history()
