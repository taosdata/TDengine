import time
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamCheckItem,
)


class TestStreamOldCaseCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_count(self):
        """Stream count

        Basic use cases of count window, include expired-data, out-of-order data, and data-deletion

        Catalog:
            - Streams:OldTsimCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-25 Simon Guan Migrated from tsim/stream/count0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/count1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/count2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/count3.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/countSliding0.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/countSliding1.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/countSliding2.sim
            - 2025-7-25 Simon Guan Migrated from tsim/stream/scalar.sim
            
        """

        tdStream.createSnode()

        streams = []
        streams.append(self.Count0())
        streams.append(self.Count02())
        streams.append(self.Count03())
        streams.append(self.Count21())
        streams.append(self.Count22())
        streams.append(self.Count31())
        streams.append(self.Sliding01())
        streams.append(self.Sliding02())
        streams.append(self.Sliding11())
        streams.append(self.Sliding21())
        tdStream.checkAll(streams)

    class Count01(StreamCheckItem):
        def __init__(self):
            self.db = "Count01"

        def create(self):
            tdSql.execute(f"create database count01 vgroups 1 buffer 8;")
            tdSql.execute(f"use count01;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 count_window(3) from t1 stream_options(max_delay(3s)|expired_time(0)|watermark(100s)) into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 where ts >= _wstart and ts < _twend;"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(0, 2) == 6
                and tdSql.getData(0, 3) == 3
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(1, 3) == 3,
            )

    class Count02(StreamCheckItem):
        def __init__(self):
            self.db = "Count02"

        def create(self):
            tdSql.execute(f"create database count02 vgroups 4 buffer 8;")
            tdSql.execute(f"use count02;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(3)"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t2 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t2 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1, 2;",
                lambda: tdSql.getRows() > 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(0, 2) == 6
                and tdSql.getData(0, 3) == 3
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(1, 3) == 3
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(2, 2) == 6
                and tdSql.getData(2, 3) == 3,
            )

    class Count03(StreamCheckItem):
        def __init__(self):
            self.db = "Count03"

        def create(self):
            tdSql.execute(f"create database count03 vgroups 1 buffer 8;")
            tdSql.execute(f"use count03;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(
                f"create stream streams3 trigger at_once FILL_HISTORY 1 IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt3 as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt3;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(0, 2) == 6
                and tdSql.getData(0, 3) == 3
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(1, 2) == 6
                and tdSql.getData(1, 3) == 3,
            )

    class Count21(StreamCheckItem):
        def __init__(self):
            self.db = "Count21"

        def create(self):
            tdSql.execute(f"create database count21 vgroups 1 buffer 8;")
            tdSql.execute(f"use count21;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() > 0 and tdSql.getData(0, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 1 and tdSql.getData(0, 1) == 3,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 2,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791212000, 0, 1, 1, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(2, 1) == 1,
            )

    class Count22(StreamCheckItem):
        def __init__(self):
            self.db = "Count22"

        def create(self):
            tdSql.execute(f"create database count22 vgroups 1 buffer 8;")
            tdSql.execute(f"use count22;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(3)"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t2 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791213009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1;;",
                lambda: tdSql.getRows() > 1
                and tdSql.getData(0, 1) == 2
                and tdSql.getData(1, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791213000, 0, 1, 1, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1;;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 3,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t2 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t2 values(1648791223009, 0, 3, 3, 1.0);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1;",
                lambda: tdSql.getRows() == 4,
            )

        def insert4(self):
            tdSql.execute(f"insert into t1 values(1648791212000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t2 values(1648791212000, 0, 1, 1, 1.0);")

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2 order by 1;",
                lambda: tdSql.getRows() == 6
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 3
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(3, 1) == 3
                and tdSql.getData(4, 1) == 1
                and tdSql.getData(5, 1) == 1,
            )

    class Count31(StreamCheckItem):
        def __init__(self):
            self.db = "Count31"

        def create(self):
            tdSql.execute(f"create database count31 vgroups 1 buffer 8;")
            tdSql.execute(f"use count31;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(3);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 4, 4, 4, 4.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 3,
            )

        def insert3(self):
            tdSql.execute(f"delete from t1 where ts = 1648791223001;")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt order by 1;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 3
                and tdSql.getData(1, 1) == 2,
            )

    class Sliding01(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding01"

        def create(self):
            tdSql.execute(f"create database sliding01 vgroups 1 buffer 8;")
            tdSql.execute(f"use sliding01;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(1648791233000, 0, 1, 1, 1.0) (1648791233001, 9, 2, 2, 1.1) (1648791233002, 9, 2, 2, 1.1) (1648791233009, 0, 3, 3, 1.0);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;", lambda: tdSql.getRows() == 6
            )

        def insert4(self):
            tdSql.execute(
                f"insert into t1 values(1648791243000, 0, 1, 1, 1.0) (1648791243001, 9, 2, 2, 1.1);"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 7,
            )
            tdSql.checkRows(7)

        def insert5(self):
            tdSql.execute(
                f"insert into t1 values(1648791253000, 0, 1, 1, 1.0) (1648791253001, 9, 2, 2, 1.1) (1648791253002, 9, 2, 2, 1.1);"
            )

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 9,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791263000, 0, 1, 1, 1.0);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 9,
            )

    class Sliding02(StreamCheckItem):
        def __init__(self):
            self.db = "Count00"

        def create(self):
            tdSql.execute(f"create database sliding02 vgroups 4;")
            tdSql.execute(f"use sliding02;")

            tdSql.execute(
                f"create stable st(ts timestamp, a int, b int, c int, d double) tags(ta int, tb int, tc int);"
            )
            tdSql.execute(f"create table t1 using st tags(1, 1, 1);")
            tdSql.execute(f"create table t2 using st tags(2, 2, 2);")
            tdSql.execute(
                f"create stream streams2 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt2 as select _wstart as s, count(*) c1, sum(b), max(c) from st partition by tbname count_window(4, 2);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 2
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(
                f"insert into t1 values(1648791233000, 0, 1, 1, 1.0) (1648791233001, 9, 2, 2, 1.1) (1648791233002, 9, 2, 2, 1.1) (1648791233009, 0, 3, 3, 1.0);"
            )

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 6,
            )

        def insert4(self):
            tdSql.execute(
                f"insert into t1 values(1648791243000, 0, 1, 1, 1.0) (1648791243001, 9, 2, 2, 1.1);"
            )

        def check4(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 7,
            )

        def insert5(self):
            tdSql.execute(
                f"insert into t1 values(1648791253000, 0, 1, 1, 1.0) (1648791253001, 9, 2, 2, 1.1) (1648791253002, 9, 2, 2, 1.1);"
            )

        def check5(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 9,
            )

        def insert6(self):
            tdSql.execute(f"insert into t1 values(1648791263000, 0, 1, 1, 1.0);")

        def check6(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt2;",
                lambda: tdSql.getRows() == 9,
            )

    class Sliding11(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding11"

        def create(self):
            tdSql.execute(f"create database sliding11 vgroups 1 buffer 8;")
            tdSql.execute(f"use sliding11;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

        def insert3(self):
            tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

    class Sliding21(StreamCheckItem):
        def __init__(self):
            self.db = "Sliding21"

        def create(self):
            tdSql.execute(f"create database sliding21 vgroups 1 buffer 8;")
            tdSql.execute(f"use sliding21;")

            tdSql.execute(
                f"create table t1(ts timestamp, a int, b int, c int, d double);"
            )
            tdSql.execute(
                f"create stream streams1 trigger at_once IGNORE EXPIRED 1 IGNORE UPDATE 0 WATERMARK 100s into streamt as select _wstart as s, count(*) c1, sum(b), max(c) from t1 count_window(4, 2);"
            )

        def insert1(self):
            tdSql.execute(f"insert into t1 values(1648791213000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791213002, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791213009, 0, 3, 3, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223000, 0, 1, 1, 1.0);")
            tdSql.execute(f"insert into t1 values(1648791223001, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223002, 9, 2, 2, 1.1);")
            tdSql.execute(f"insert into t1 values(1648791223009, 0, 3, 3, 1.0);")

        def check1(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 4
                and tdSql.getData(3, 1) == 2,
            )

        def insert2(self):
            tdSql.execute(f"delete from t1 where ts = 1648791213000;")

        def check2(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 4
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 3
                and tdSql.getData(3, 1) == 1,
            )

        def insert3(self):
            tdSql.execute(f"delete from t1 where ts = 1648791223002;")

        def check3(self):
            tdSql.checkResultsByFunc(
                f"select * from streamt;",
                lambda: tdSql.getRows() == 3
                and tdSql.getData(0, 1) == 4
                and tdSql.getData(1, 1) == 4
                and tdSql.getData(2, 1) == 2,
            )
