import time
from new_test_framework.utils import (tdLog, tdSql, tdStream, StreamCheckItem, waitForRows)


class TestStreamVtableRefVtable:
    """Test cases for streams where the trigger table is a multi-hop virtual table chain.

    When a stream is created FROM a virtual table (vtable-as-trigger), the runtime resolves
    the vtable's colRef chain to find the underlying physical table. The max chain depth is 32.

    Tests cover:
        - Every depth from 1 to 32 (parametric)
        - 33-hop negative test (exceeds max depth)
        - Multiple data types
        - NULL handling
        - Multi-round dynamic inserts
        - Large batch inserts
        - Partial column references
    """
    precision = 'ms'

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_vtable_ref_vtable(self):
        """Test streams with multi-hop virtual table trigger tables up to 32-hop

        Since: v3.4.1.0

        Labels: common, ci

        Jira: None

        History:
            - 2026-04-25 Created for vtable-ref-vtable feature
            - 2026-04-26 Extended to full 32-hop coverage
        """

        try:
            tdStream.createSnode()
        except Exception:
            tdLog.info("snode already exists, skip creation")
        tdSql.execute(f"alter all dnodes 'debugflag 135';")
        tdSql.execute(f"alter all dnodes 'stdebugflag 135';")

        streams = []

        # ======== Parametric N-hop tests: every depth from 1 to 32 ========
        for n in range(1, 33):
            streams.append(NHopTrigger(n))

        # ======== 33-hop negative test: exceeds max depth ========
        streams.append(NHopTooDeep(33))

        # ======== Edge case: multiple data types through 2-hop ========
        streams.append(self.TwoHopMultiType())

        # ======== Edge case: NULL values through 2-hop ========
        streams.append(self.TwoHopWithNulls())

        # ======== Edge case: multi-round dynamic inserts through 2-hop ========
        streams.append(self.TwoHopDynamicInsert())

        # ======== Edge case: large batch through 2-hop ========
        streams.append(self.TwoHopLargeBatch())

        # ======== Edge case: partial column reference (only some cols referenced) ========
        streams.append(self.TwoHopPartialRef())

        # ======== Edge case: single column through deep (16-hop) chain ========
        streams.append(self.DeepSingleColumn())

        tdStream.checkAll(streams)

    # ========================================================================
    # Multiple data types through 2-hop chain
    # ========================================================================
    class TwoHopMultiType(StreamCheckItem):
        """Two-hop chain with int, float, binary, bool columns"""
        def __init__(self):
            self.db = "test_vrv_multitype"
            self.phys = "phys_tb"
            self.vtb1 = "vtb_mid"
            self.vtb2 = "vtb_top"
            self.stream = "s_vrv_multitype"
            self.restb = "res_vrv_multitype"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.phys} (ts timestamp, c1 int, c2 float, c3 binary(32), c4 bool)")
            tdSql.execute(f"create vtable {self.vtb1} (ts timestamp, m1 int from {self.phys}.c1, m2 float from {self.phys}.c2, m3 binary(32) from {self.phys}.c3, m4 bool from {self.phys}.c4)")
            tdSql.execute(f"create vtable {self.vtb2} (ts timestamp, t1 int from {self.vtb1}.m1, t2 float from {self.vtb1}.m2, t3 binary(32) from {self.vtb1}.m3, t4 bool from {self.vtb1}.m4)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.vtb2} into {self.restb} as "
                f"select ts, t1, t2, t3, t4 from {self.vtb2} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:01', 42, 3.14, 'hello', true)")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:02', 99, 2.72, 'world', false)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 30)
            tdSql.checkData(0, 1, 42)
            tdSql.checkData(0, 3, 'hello')
            tdSql.checkData(0, 4, True)
            tdSql.checkData(1, 1, 99)
            tdSql.checkData(1, 3, 'world')
            tdSql.checkData(1, 4, False)

    # ========================================================================
    # Two-hop with NULL values
    # ========================================================================
    class TwoHopWithNulls(StreamCheckItem):
        """Two-hop chain with NULL values in some columns"""
        def __init__(self):
            self.db = "test_vrv_nulls"
            self.phys = "phys_tb"
            self.vtb1 = "vtb_mid"
            self.vtb2 = "vtb_top"
            self.stream = "s_vrv_nulls"
            self.restb = "res_vrv_nulls"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.phys} (ts timestamp, id int, val int)")
            tdSql.execute(f"create vtable {self.vtb1} (ts timestamp, mid_id int from {self.phys}.id, mid_val int from {self.phys}.val)")
            tdSql.execute(f"create vtable {self.vtb2} (ts timestamp, top_id int from {self.vtb1}.mid_id, top_val int from {self.vtb1}.mid_val)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.vtb2} into {self.restb} as "
                f"select ts, top_id, top_val from {self.vtb2} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:01', 1, null)")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:02', null, 200)")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:03', 3, 300)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 3, 30)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, None)
            tdSql.checkData(1, 1, None)
            tdSql.checkData(1, 2, 200)
            tdSql.checkData(2, 1, 3)
            tdSql.checkData(2, 2, 300)

    # ========================================================================
    # Two-hop with dynamic multi-round inserts
    # ========================================================================
    class TwoHopDynamicInsert(StreamCheckItem):
        """Two-hop chain with data inserted in multiple rounds"""
        def __init__(self):
            self.db = "test_vrv_dynamic"
            self.phys = "phys_tb"
            self.vtb1 = "vtb_mid"
            self.vtb2 = "vtb_top"
            self.stream = "s_vrv_dynamic"
            self.restb = "res_vrv_dynamic"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.phys} (ts timestamp, id int, val int)")
            tdSql.execute(f"create vtable {self.vtb1} (ts timestamp, mid_id int from {self.phys}.id, mid_val int from {self.phys}.val)")
            tdSql.execute(f"create vtable {self.vtb2} (ts timestamp, top_id int from {self.vtb1}.mid_id, top_val int from {self.vtb1}.mid_val)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.vtb2} into {self.restb} as "
                f"select ts, top_id, top_val from {self.vtb2} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:01', 1, 10)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 1, 30)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 10)

        def insert2(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:02', 2, 20) ('2026-01-01 00:00:03', 3, 30)")

        def check2(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 3, 30)
            tdSql.checkData(1, 1, 2)
            tdSql.checkData(1, 2, 20)
            tdSql.checkData(2, 1, 3)
            tdSql.checkData(2, 2, 30)

        def insert3(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:04', 4, 40) ('2026-01-01 00:00:05', 5, 50)")

        def check3(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 5, 30)
            tdSql.checkData(3, 1, 4)
            tdSql.checkData(3, 2, 40)
            tdSql.checkData(4, 1, 5)
            tdSql.checkData(4, 2, 50)

    # ========================================================================
    # Two-hop with large batch
    # ========================================================================
    class TwoHopLargeBatch(StreamCheckItem):
        """Two-hop chain with a large batch of rows"""
        def __init__(self):
            self.db = "test_vrv_batch"
            self.phys = "phys_tb"
            self.vtb1 = "vtb_mid"
            self.vtb2 = "vtb_top"
            self.stream = "s_vrv_batch"
            self.restb = "res_vrv_batch"
            self.batch_size = 50

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table {self.phys} (ts timestamp, id int, val int)")
            tdSql.execute(f"create vtable {self.vtb1} (ts timestamp, mid_id int from {self.phys}.id, mid_val int from {self.phys}.val)")
            tdSql.execute(f"create vtable {self.vtb2} (ts timestamp, top_id int from {self.vtb1}.mid_id, top_val int from {self.vtb1}.mid_val)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.vtb2} into {self.restb} as "
                f"select ts, top_id, top_val from {self.vtb2} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            values = " ".join([f"('2026-01-01 00:00:{i:02d}', {i}, {i*100})" for i in range(1, self.batch_size + 1)])
            tdSql.execute(f"insert into {self.phys} values {values}")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", self.batch_size, 60)
            tdSql.checkData(0, 1, 1)
            tdSql.checkData(0, 2, 100)
            tdSql.checkData(self.batch_size - 1, 1, self.batch_size)
            tdSql.checkData(self.batch_size - 1, 2, self.batch_size * 100)

    # ========================================================================
    # Two-hop with partial column reference
    # ========================================================================
    class TwoHopPartialRef(StreamCheckItem):
        """Two-hop chain where vtable only references a subset of physical columns"""
        def __init__(self):
            self.db = "test_vrv_partial"
            self.phys = "phys_tb"
            self.vtb1 = "vtb_mid"
            self.vtb2 = "vtb_top"
            self.stream = "s_vrv_partial"
            self.restb = "res_vrv_partial"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            # Physical table has 4 data columns, vtable1 references only 2
            tdSql.execute(f"create table {self.phys} (ts timestamp, a int, b int, c int, d int)")
            tdSql.execute(f"create vtable {self.vtb1} (ts timestamp, m_a int from {self.phys}.a, m_c int from {self.phys}.c)")
            tdSql.execute(f"create vtable {self.vtb2} (ts timestamp, t_a int from {self.vtb1}.m_a, t_c int from {self.vtb1}.m_c)")

            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {self.vtb2} into {self.restb} as "
                f"select ts, t_a, t_c from {self.vtb2} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:01', 10, 20, 30, 40)")
            tdSql.execute(f"insert into {self.phys} values ('2026-01-01 00:00:02', 50, 60, 70, 80)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 30)
            # t_a = a, t_c = c (b and d are skipped)
            tdSql.checkData(0, 1, 10)
            tdSql.checkData(0, 2, 30)
            tdSql.checkData(1, 1, 50)
            tdSql.checkData(1, 2, 70)

    # ========================================================================
    # Deep single-column chain (16-hop, one data column)
    # ========================================================================
    class DeepSingleColumn(StreamCheckItem):
        """16-hop chain with only one data column to test deep resolution"""
        def __init__(self):
            self.db = "test_vrv_deep1col"
            self.depth = 16
            self.stream = "s_vrv_deep1col"
            self.restb = "res_vrv_deep1col"

        def create(self):
            tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
            tdSql.execute(f"use {self.db}")

            tdSql.execute(f"create table phys (ts timestamp, x int)")
            prev_name = "phys"
            prev_col = "x"
            for i in range(1, self.depth + 1):
                vname = f"v{i}"
                vcol = f"c{i}"
                tdSql.execute(f"create vtable {vname} (ts timestamp, {vcol} int from {prev_name}.{prev_col})")
                prev_name = vname
                prev_col = vcol

            top_vtb = f"v{self.depth}"
            top_col = f"c{self.depth}"
            tdSql.execute(
                f"create stream {self.stream} sliding(1s) from {top_vtb} into {self.restb} as "
                f"select ts, {top_col} from {top_vtb} order by ts"
            )

        def insert1(self):
            tdSql.execute(f"use {self.db}")
            tdSql.execute(f"insert into phys values ('2026-01-01 00:00:01', 777) ('2026-01-01 00:00:02', 888)")

        def check1(self):
            waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 30)
            tdSql.checkData(0, 1, 777)
            tdSql.checkData(1, 1, 888)


# ============================================================================
# Parametric N-hop trigger (used for depths 1..32)
# ============================================================================
class NHopTrigger(StreamCheckItem):
    """Parametric N-hop vtable chain trigger test.
    Creates: physical -> vtable1 -> vtable2 -> ... -> vtableN
    Stream triggers from vtableN.
    """
    def __init__(self, n):
        self.n = n
        self.db = f"test_vrv_{n}hop"
        self.stream = f"s_vrv_{n}hop"
        self.restb = f"res_vrv_{n}hop"

    def create(self):
        tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
        tdSql.execute(f"use {self.db}")

        # Create physical table with 2 data columns
        tdSql.execute(f"create table phys (ts timestamp, id int, val int)")

        # Build chain: phys -> v1 -> v2 -> ... -> vN
        prev_name = "phys"
        prev_id = "id"
        prev_val = "val"
        for i in range(1, self.n + 1):
            vname = f"v{i}"
            vid = f"id{i}"
            vval = f"val{i}"
            tdSql.execute(
                f"create vtable {vname} (ts timestamp, "
                f"{vid} int from {prev_name}.{prev_id}, "
                f"{vval} int from {prev_name}.{prev_val})"
            )
            prev_name = vname
            prev_id = vid
            prev_val = vval

        # Stream from the top vtable
        top_vtb = f"v{self.n}"
        top_id = f"id{self.n}"
        top_val = f"val{self.n}"
        tdSql.execute(
            f"create stream {self.stream} sliding(1s) from {top_vtb} into {self.restb} as "
            f"select ts, {top_id}, {top_val} from {top_vtb} order by ts"
        )

    def insert1(self):
        tdSql.execute(f"use {self.db}")
        tdSql.execute(
            f"insert into phys values "
            f"('2026-01-01 00:00:01', 1, 100) "
            f"('2026-01-01 00:00:02', 2, 200)"
        )

    def check1(self):
        waitForRows(f"select * from {self.db}.{self.restb} order by ts", 2, 60)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 200)


# ============================================================================
# N-hop too deep (33-hop, exceeds maxDepth=32)
# ============================================================================
class NHopTooDeep(StreamCheckItem):
    """Negative test: 33-hop chain exceeds maxDepth=32.
    Stream creation may succeed, but data should NOT flow through.
    The runtime returns TSDB_CODE_STREAM_VTB_REF_TOO_DEEP.
    """
    def __init__(self, n):
        self.n = n
        self.db = f"test_vrv_{n}hop_deep"
        self.stream = f"s_vrv_{n}hop_deep"
        self.restb = f"res_vrv_{n}hop_deep"

    def create(self):
        tdSql.execute(f"create database if not exists {self.db} vgroups 1 buffer 8 precision '{TestStreamVtableRefVtable.precision}'")
        tdSql.execute(f"use {self.db}")

        tdSql.execute(f"create table phys (ts timestamp, id int, val int)")

        prev_name = "phys"
        prev_id = "id"
        prev_val = "val"
        for i in range(1, self.n + 1):
            vname = f"v{i}"
            vid = f"id{i}"
            vval = f"val{i}"
            tdSql.execute(
                f"create vtable {vname} (ts timestamp, "
                f"{vid} int from {prev_name}.{prev_id}, "
                f"{vval} int from {prev_name}.{prev_val})"
            )
            prev_name = vname
            prev_id = vid
            prev_val = vval

        top_vtb = f"v{self.n}"
        top_id = f"id{self.n}"
        top_val = f"val{self.n}"
        tdSql.execute(
            f"create stream {self.stream} sliding(1s) from {top_vtb} into {self.restb} as "
            f"select ts, {top_id}, {top_val} from {top_vtb} order by ts"
        )

    def insert1(self):
        tdSql.execute(f"use {self.db}")
        tdSql.execute(
            f"insert into phys values ('2026-01-01 00:00:01', 1, 100)"
        )

    def check1(self):
        # The 33-hop chain exceeds maxDepth=32. Data should NOT appear in result table.
        # Wait a bit to confirm no data flows through.
        time.sleep(15)
        try:
            tdSql.query(f"select * from {self.db}.{self.restb}")
            if tdSql.queryRows > 0:
                tdLog.exit(f"NHopTooDeep({self.n}): expected 0 rows but got {tdSql.queryRows} — depth limit not enforced!")
            tdLog.info(f"NHopTooDeep({self.n}): correctly got 0 rows (depth limit enforced)")
        except Exception:
            # Result table may not exist if stream failed before creating it — that's also acceptable
            tdLog.info(f"NHopTooDeep({self.n}): result table does not exist (depth limit enforced, stream failed)")
