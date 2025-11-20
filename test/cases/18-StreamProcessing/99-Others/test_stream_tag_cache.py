from new_test_framework.utils import tdSql, tdLog, tdStream, StreamItem
from new_test_framework.utils.eutil import findTaosdLog

class TestStreamTagCache:
    def setup_class(cls):
        tdLog.info(f"start to execute {__file__}")

    def test_stream_tag_cache(self):
        """summary: test stable tag filter cache

        description:
            - check_all_types_basic:
                test tag cache with all data types in basic scenario
            - check_all_types_alter:
                test tag cache with all data types in alter table tag scenario
            - check_null_tag_value:
                test tag cache with null tag values
            - check_create_drop_ctable:
                test tag cache with create and drop child table operations
            - check fallback_to_normal_cache:
                test stable tag filter cache fallback to normal tag filter cache
            - todo(Tony Zhang): check virtual super table


        Since: v3.3.8.5

        Labels: stream, meta, tagCache

        Catalog:
            - StreamProcessing:Others

        History:
            - 2025-11-13 Tony Zhang: created this test

        """
        tdSql.execute("alter dnode 1 'stableTagFilterCache' '1'")
        tdSql.execute("alter dnode 1 'tagFilterCache' '1'")
        tdStream.createSnode()
        self.check_all_types_basic()
        self.check_all_types_alter()
        self.check_null_tag_value()
        self.check_create_drop_ctable()
        self.check_fallback_to_normal_cache()

    def stable_tag_cache_log_counter(self, db_name, tb_name, retry=5) -> int:
        tdSql.query(f"""select uid from information_schema.ins_stables
            where stable_name = '{tb_name}' and db_name = '{db_name}'""")
        suid = tdSql.getColData(0)[0]
        return findTaosdLog(f"suid:{suid}.*stable tag filter cache", retry=retry)

    def normal_tag_cache_log_counter(self, db_name, tb_name, retry=5) -> int:
        tdSql.query(f"""select uid from information_schema.ins_stables
            where stable_name = '{tb_name}' and db_name = '{db_name}'""")
        suid = tdSql.getColData(0)[0]
        return findTaosdLog(f"suid:{suid}.*normal tag filter cache", retry=retry)

    def check_all_types_basic(self):
        db_name = "test_basic"
        tb_name = "stb_basic"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"""
            create table {tb_name} (ts timestamp, v int) tags (t1 bool,
            t2 int, t3 double, t4 varchar(100), t5 nchar(100));
        """, show=1)
        tdSql.execute(f"create table ctb1 using {tb_name} tags (true, 1, 1.1, 'ABC', 'xyz');", show=1)
        tdSql.execute(f"create table ctb2 using {tb_name} tags (true, 2, 1.1, 'CAB', 'zxy');", show=1)
        tdSql.execute(f"create table ctb3 using {tb_name} tags (false, 1, 1.1, 'ABC', 'xyz');", show=1)
        tdSql.execute(f"create table ctb4 using {tb_name} tags (false, 2, 1.1, 'CAB', 'zxy');", show=1)

        # create streams
        streams: list[StreamItem] = []
        stream: StreamItem = StreamItem(
            id = 0,
            stream=f"""create stream st0 interval(10s) sliding(10s)
                from {tb_name} partition by t1, t3 into res0
                output_subtable(concat("basic0_", cast(t1 as varchar),
                "_", replace(cast(t3 as varchar), '.', '_')))
                as select _twstart as ts, _twend as te, sum(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t1 = %%1 and t3 = %%2""",
            res_query="select ts, te, v, t1, t3 from res0 order by ts limit 8",
            exp_query=f"""select _wstart, _wend, sum(v), t1, t3 from {tb_name}
                partition by t1, t3 interval(10s) order by _wstart limit 8"""
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 1,
            stream=f"""create stream st1 interval(10s) sliding(10s)
                from {tb_name} partition by t2, t3 into res1
                output_subtable(concat("basic1_", cast(t2 as varchar),
                "_", replace(cast(t3 as varchar), '.', '_')))
                as select _twstart as ts, _twend as te, sum(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t2 = %%1 and t3 = %%2""",
            res_query="select ts, te, v, t2, t3 from res1 order by ts",
            exp_query=f"""select _wstart, _wend, sum(v), t2, t3 from {tb_name}
                partition by t2, t3 interval(10s) order by _wstart limit 10"""
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 2,
            stream=f"""create stream st2 interval(10s) sliding(10s)
                from {tb_name} partition by t4, t5 into res2
                output_subtable(concat("basic2_", t4, "_", cast(t5 as varchar)))
                as select _twstart as ts, _twend as te, sum(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t4 = %%1 and t5 = %%2""",
            res_query="select ts, te, v, t4, t5 from res2 order by ts",
            exp_query=f"""select _wstart, _wend, sum(v), t4, t5 from {tb_name}
                partition by t4, t5 interval(10s) order by _wstart limit 10"""
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 3,
            stream=f"""create stream st3 interval(10s) sliding(10s)
                from {tb_name} partition by t2, t5 into res3
                output_subtable(concat("basic3_", cast(t2 as varchar),
                "_", cast(t5 as varchar)))
                as select _twstart as ts, _twend as te, sum(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t2 = %%1 and t5 = %%2""",
            res_query="select ts, te, v, t2, t5 from res3 order by ts",
            exp_query=f"""select _wstart, _wend, sum(v), t2, t5 from {tb_name}
                partition by t2, t5 interval(10s) order by _wstart limit 10"""
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert data
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:00', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:10', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:20', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:30', 4)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:40', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:50', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:01:00', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:01:10', 4)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:01:20', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:01:30', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:01:40', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:01:50', 4)")

        # check results
        for s in streams:
            s.checkResults()

        # check stable tagFilterCache size
        assert self.stable_tag_cache_log_counter(db_name, tb_name) == 8

    def check_all_types_alter(self):
        db_name = "test_alter"
        tb_name = "stb_alter"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"""
            create table {tb_name} (ts timestamp, v int) tags (t_fix int, t1 bool,
            t2 int, t3 double, t4 varchar(100), t5 nchar(100));
        """, show=1)
        tdSql.execute(f"create table ctb0 using {tb_name} tags (1, true,  1, 2.333, 'BJ', '涛思数据');", show=1)
        tdSql.execute(f"create table ctb1 using {tb_name} tags (1, true,  2, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb2 using {tb_name} tags (1, false, 1, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb3 using {tb_name} tags (1, false, 2, 2.333, 'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb4 using {tb_name} tags (1, false, 2, 1.1,   'BJ', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb5 using {tb_name} tags (1, false, 2, 1.1,   'SH', '涛思数据');", show=1)

        streams: list[StreamItem] = []
        stream: StreamItem = StreamItem(
            id = 4,
            stream=f"""create stream st4 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res4
                output_subtable(concat("alter4_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t1 = true""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 5,
            stream=f"""create stream st5 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res5
                output_subtable(concat("alter5_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t2 = 1""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 6,
            stream=f"""create stream st6 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res6
                output_subtable(concat("alter6_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t3 = 2.333""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 7,
            stream=f"""create stream st7 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res7
                output_subtable(concat("alter7_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t4 = 'BJ'""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 8,
            stream=f"""create stream st8 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res8
                output_subtable(concat("alter8_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t5 = '涛思数据'""",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert first banch of data
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:00', 0)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:05', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:05', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:05', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:05', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:05', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:10', 0)")

        # check results after first batch
        expect_cnt = 2
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+4}"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:00' as timestamp) as ts,
                cast('2025-12-12 12:00:10' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # alter tags and insert second batch of data
        tdSql.execute("alter table ctb1 set tag t1 = false")
        tdSql.execute("alter table ctb2 set tag t2 = 2")
        tdSql.execute("alter table ctb3 set tag t3 = 1.1")
        tdSql.execute("alter table ctb4 set tag t4 = 'SH'")
        tdSql.execute("alter table ctb5 set tag t5 = 'taosdata'")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:15', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:15', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:15', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:15', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:15', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:20', 0)")

        # check results after second batch
        expect_cnt = 1
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+4} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:10' as timestamp) as ts,
                cast('2025-12-12 12:00:20' as timestamp) as te,
                {expect_cnt} as v"""

        # alter tags back and insert third batch of data
        tdSql.execute("alter table ctb1 set tag t1 = true")
        tdSql.execute("alter table ctb2 set tag t2 = 1")
        tdSql.execute("alter table ctb3 set tag t3 = 2.333")
        tdSql.execute("alter table ctb4 set tag t4 = 'BJ'")
        tdSql.execute("alter table ctb5 set tag t5 = '涛思数据'")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:25', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:25', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:25', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:25', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:25', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:30', 0)")

        # check results after third batch
        expect_cnt = 2
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+4} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:20' as timestamp) as ts,
                cast('2025-12-12 12:00:30' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # check stable tagFilterCache size
        assert self.stable_tag_cache_log_counter(db_name, tb_name) == 5

    def check_null_tag_value(self):
        db_name = "test_null"
        tb_name = "stb_null"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"""
            create table {tb_name} (ts timestamp, v int) tags (t_fix int, t1 bool,
            t2 int, t3 double, t4 varchar(100), t5 nchar(100));
        """, show=1)
        tdSql.execute(f"create table ctb0 using {tb_name} tags (1, true,  1, 2.333, 'BJ', '涛思数据');", show=1)
        tdSql.execute(f"create table ctb1 using {tb_name} tags (1, true,  2, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb2 using {tb_name} tags (1, false, 1, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb3 using {tb_name} tags (1, false, 2, 2.333, 'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb4 using {tb_name} tags (1, false, 2, 1.1,   'BJ', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb5 using {tb_name} tags (1, false, 2, 1.1,   'SH', '涛思数据');", show=1)

        streams: list[StreamItem] = []
        stream: StreamItem = StreamItem(
            id = 9,
            stream=f"""create stream st9 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res9
                output_subtable(concat("null9_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t1 = true""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 10,
            stream=f"""create stream st10 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res10
                output_subtable(concat("null10_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t2 = 1""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 11,
            stream=f"""create stream st11 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res11
                output_subtable(concat("null11_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t3 = 2.333""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 12,
            stream=f"""create stream st12 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res12
                output_subtable(concat("null12_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t4 = 'BJ'""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 13,
            stream=f"""create stream st13 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res13
                output_subtable(concat("null13_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t5 = '涛思数据'""",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert first banch of data
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:00', 0)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:05', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:05', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:05', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:05', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:05', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:10', 0)")

        # check results after first batch
        expect_cnt = 2
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+9}"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:00' as timestamp) as ts,
                cast('2025-12-12 12:00:10' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # alter tags and insert second batch of data
        tdSql.execute("alter table ctb1 set tag t1 = null")
        tdSql.execute("alter table ctb2 set tag t2 = null")
        tdSql.execute("alter table ctb3 set tag t3 = null")
        tdSql.execute("alter table ctb4 set tag t4 = null")
        tdSql.execute("alter table ctb5 set tag t5 = null")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:15', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:15', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:15', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:15', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:15', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:20', 0)")

        # check results after second batch
        expect_cnt = 1
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+9} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:10' as timestamp) as ts,
                cast('2025-12-12 12:00:20' as timestamp) as te,
                {expect_cnt} as v"""

        # alter tags back and insert third batch of data
        tdSql.execute("alter table ctb1 set tag t1 = true")
        tdSql.execute("alter table ctb2 set tag t2 = 1")
        tdSql.execute("alter table ctb3 set tag t3 = 2.333")
        tdSql.execute("alter table ctb4 set tag t4 = 'BJ'")
        tdSql.execute("alter table ctb5 set tag t5 = '涛思数据'")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:25', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:25', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:25', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:25', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:25', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:30', 0)")

        # check results after third batch
        expect_cnt = 2
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+9} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:20' as timestamp) as ts,
                cast('2025-12-12 12:00:30' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # check stable tagFilterCache size
        assert self.stable_tag_cache_log_counter(db_name, tb_name) == 5

        # create a new child table with null t_fix tag and insert data
        tdSql.execute(f"create table ctb6 using {tb_name} tags (null, true, 1, 2.333, 'BJ', '涛思数据')", show=1)
        tdSql.execute("insert into ctb6 values('2025-12-12 12:00:35', 6)")
        tdSql.execute("insert into ctb6 values('2025-12-12 12:00:40', 6)")
        # nothing to check, just make sure no error occurs

    def check_create_drop_ctable(self):
        db_name = "test_create_drop"
        tb_name = "stb_table"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650", show=1)
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"""
            create table {tb_name} (ts timestamp, v int) tags (t_fix int, t1 bool,
            t2 int, t3 double, t4 varchar(100), t5 nchar(100));
        """, show=1)
        tdSql.execute(f"create table ctb0 using {tb_name} tags (1, true,  1, 2.333, 'BJ', '涛思数据');", show=1)

        streams: list[StreamItem] = []
        stream: StreamItem = StreamItem(
            id = 14,
            stream=f"""create stream st14 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res14
                output_subtable(concat("table14_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t1 = true""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 15,
            stream=f"""create stream st15 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res15
                output_subtable(concat("table15_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t2 = 1""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 16,
            stream=f"""create stream st16 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res16
                output_subtable(concat("table16_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t3 = 2.333""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 17,
            stream=f"""create stream st17 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res17
                output_subtable(concat("table17_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t4 = 'BJ'""",
        )
        streams.append(stream)

        stream: StreamItem = StreamItem(
            id = 18,
            stream=f"""create stream st18 interval(10s) sliding(10s)
                from {tb_name} partition by t_fix into res18
                output_subtable(concat("table18_", cast(t_fix as varchar)))
                as select _twstart as ts, _twend as te, count(v) as v from
                {tb_name} where ts >= _twstart and ts < _twend and
                t_fix = %%1 and t5 = '涛思数据'""",
        )
        streams.append(stream)

        # start streams
        for s in streams:
            s.createStream()
        tdStream.checkStreamStatus()

        # insert first banch of data
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:00', 0)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:10', 0)")

        # check results after first batch
        expect_cnt = 1
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+14}"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:00' as timestamp) as ts,
                cast('2025-12-12 12:00:10' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # create child tables and insert second batch of data
        tdSql.execute(f"create table ctb1 using {tb_name} tags (1, true,  2, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb2 using {tb_name} tags (1, false, 1, 1.1,   'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb3 using {tb_name} tags (1, false, 2, 2.333, 'SH', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb4 using {tb_name} tags (1, false, 2, 1.1,   'BJ', 'taosdata');", show=1)
        tdSql.execute(f"create table ctb5 using {tb_name} tags (1, false, 2, 1.1,   'SH', '涛思数据');", show=1)
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:15', 1)")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:15', 2)")
        tdSql.execute("insert into ctb3 values('2025-12-12 12:00:15', 3)")
        tdSql.execute("insert into ctb4 values('2025-12-12 12:00:15', 4)")
        tdSql.execute("insert into ctb5 values('2025-12-12 12:00:15', 5)")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:20', 0)")

        # check results after second batch
        expect_cnt = 2
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+14} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:10' as timestamp) as ts,
                cast('2025-12-12 12:00:20' as timestamp) as te,
                {expect_cnt} as v"""

        # drop tables and insert third batch of data
        tdSql.execute("drop table ctb1")
        tdSql.execute("drop table ctb2")
        tdSql.execute("drop table ctb3")
        tdSql.execute("drop table ctb4")
        tdSql.execute("drop table ctb5")
        tdSql.execute("insert into ctb0 values('2025-12-12 12:00:30', 0)")

        # check results after third batch
        expect_cnt = 1
        for i, s in enumerate(streams):
            s.res_query = f"select ts, te, v from res{i+14} order by ts desc limit 1"
            s.exp_query = f"""
                select cast('2025-12-12 12:00:20' as timestamp) as ts,
                cast('2025-12-12 12:00:30' as timestamp) as te,
                {expect_cnt} as v"""
            s.checkResults()

        # check stable tagFilterCache size
        assert self.stable_tag_cache_log_counter(db_name, tb_name) == 5

    def check_fallback_to_normal_cache(self):
        db_name = "test_fallback"
        tb_name = "stb_fallback"
        tdSql.execute(f"create database if not exists {db_name} vgroups 1 keep 3650")
        tdSql.execute(f"use {db_name}")
        tdSql.execute(f"create table {tb_name} (ts timestamp, v int) tags (t1 int);")
        tdSql.execute(f"create table ctb1 using {tb_name} tags (1);")
        tdSql.execute(f"create table ctb2 using {tb_name} tags (2);")
        tdSql.execute("insert into ctb2 values('2025-12-12 12:00:00', 2333);")

        # Create a stream with a non-optimizable WHERE clause (e.g., using '!=')
        stream: StreamItem = StreamItem(
            id = 19,
            stream=f"""create stream st_fallback interval(10s) sliding(10s)
                from {tb_name} partition by t1 into res_fallback
                as select _twstart as ts, _twend as te, sum(v) as v from
                {tb_name} where t1 != %%1""",
            res_query="select ts, te, v from res_fallback order by ts"
        )
        stream.createStream()
        tdStream.checkStreamStatus()

        # Insert data to trigger the stream
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:10', 1)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:20', 1)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:30', 1)")
        tdSql.execute("insert into ctb1 values('2025-12-12 12:00:40', 1)")
    
        stream.awaitRowStability(3)

        # Assert that stable cache was NOT used and normal cache WAS used
        assert self.stable_tag_cache_log_counter(db_name, tb_name, 5) == 0
        assert self.normal_tag_cache_log_counter(db_name, tb_name, 5) > 0
