import time
import threading

from taos.tmq import Consumer

from new_test_framework.utils import tdLog, tdSql, tdCom


class TestCase:
    """ Regression: tmq query-topic over a wide STABLE returns corrupted blocks for
    PARTIAL-column inserts consumed on the live/WAL path.

    Root cause
    ----------
    A child table of a wide stable is written with stmt2 *partial* inserts (only a
    subset of columns, e.g. ``(ts, ge020005``). A query topic projects far more
    columns than the partial insert set (``ts`` + all 42 ``GE02`` columns, incl. the
    NCHAR ``ge020004``). On the incremental WAL scan path the columns that were NOT
    present in the submit are serialized from uninitialized/freed column buffers
    instead of NULL:

      * NCHAR ``ge020004`` -> ``offset=0, colLength=0`` (the ``-1`` NULL marker is
        never written), i.e. a *headerless* varstr. WS/JDBC consumbers derive a bogus
        length from the next column and overrun the buffer
        (``IndexOutOfBoundsException: readerIndex(N)+4 exceeds writerIndex(N+2)``);
        the native consumer read garbage instead of NULL.
      * DOUBLE columns -> stale heap pointers / garbage instead of NULL.

    Genuine full-row inserts are always correct; only the partial-insert rows are
    corrupted, and only on the live/WAL path (the snapshot/TSDB path NULL-fills
    correctly). The defect in memory-state dependent, so the test drives a wide
    stable + many heterogeneous-shape child tables + a busy interleaved writer to
    churn the allocator while consuming live.

    The consumer here is the native ``taos.tmq`` consumer: with the bug, the projected
    ``ge020004`` comes back as something other than the inserted ``'1.0'`` / ``NULL``
    (or the poll raises), and un-set DOUBLE columns come back as denomal garbage
    instead of ``None``.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    # ---- schema -----------------------------------------------
    def _build_groups(self):
        # one column group per child table; together they make a wide stable.
        # the target child table 'e011_2' carries the GE02 group whole ge020004 is NCHAR.
        groups = {
            "e011_2": [f"ge02{n:04d}" for n in range(1, 43)],        # target, 42 cols
            "e011_1": [f"ge01{n:04d}" for n in range(1, 43)],
            "e011_3": [f"ge03{n:04d}" for n in range(1, 16)],
            "e011":   ["ge_hfo", "ge_mgo", "ge_sfoc"],
            "e012":   ["bfs000001"] + [f"bo{n:04d}" for n in range(1, 14)],
            "e013":   ["sg_fsp", "sg_cfsr"] + [f"sg{n:04d}" for n in range(1, 8)],
            "e014":   [f"sp{n:04d}" for n in range(1, 10)],
            "e009":   [f"me{n:04d}" for n in range(1, 12)],
            "e022":   [f"alert{n:04d}" for n in range(1, 13)],
            "e016":   [f"tank{n:04d}" for n in range(1, 49)],
            "e018":   [f"sad{n:04d}" for n in range(1, 28)],
        }
        return groups

    def prepare(self):
        self.db = "db_part_nchar"
        self.topic = "tp_part_nchar"
        # self.nchar_col = "ge020004"   # NCHAR(500) member of the GE02 group -> "ge02" + "0004"
        # note: group build formats as ge02%04d, so col #4 == "ge020004"
        tdSql.execute(f"drop topic if exists {self.topic}")
        time.sleep(1)
        tdSql.execute(f"drop database if exists {self.db}")
        tdSql.execute(f"create database {self.db} vgroups 1 precision 'ms'")
        tdSql.execute(f"use {self.db}")

        self.groups = self._build_groups()
        self.target_cols = self.groups["e011_2"]  # ts excluded
        self.nchar_col = self.target_cols[3]      # 4th GE02 column is the NCHAR one

        allc, seen = [], set()
        for cols in self.groups.values():
            for n in cols:
                if n not in seen:
                    seen.add(n); allc.append(n)

        def ctype(n):
            return "NCHAR(500)" if n == self.nchar_col else "DOUBLE"

        coldef = ", ".join(f"`{n}` {ctype(n)}" for n in allc)
        tdSql.execute(f"create stable st (ts timestamp, {coldef}) tags (eqid binary(32))")
        for t in self.groups:
            tdSql.execute(f"create table ct_{t} using st tags('{t}')")

        proj = "cast(ts as bigint) as ts, " + ", ".join(f"`{c}`" for c in self.target_cols)
        tdSql.execute(f"create topic {self.topic} as select {proj} from {self.db}.st where eqid='e011_2'")
        # projection column index of the nchar (ts=0, then target_cols in order)
        self.nchar_idx = 1 + self.target_cols.index(self.nchar_col)
        tdLog.info(f"wide stable {len(allc)} cols, {len(self.groups)} child tables; "
                   f"nchar '{self.nchar_col}' at projection index {self.nchar_idx}")
        self.base_ts = 1782356517400
        self.num_ts = 1000

    # ----- backgroud writer --------------------------------------------
    def writer(self, done):
        conn = tdCom.newcon(database=self.db)
        ncv = self.nchar_col

        def val(cn, i):
            return "1.0" if cn == ncv else round(10.0 + i * 0.01, 3)

        stmts = {}
        for t, cols in self.groups.items():
            cl = ["ts"] + cols
            stmts[t] = conn.statement2(
                f"insert into {self.db}.ct_{t} ({','.join('`'+x+'`' for x in cl)}) "
                f"values ({','.join('?'*len(cl))})")
        # partial insert into the target table: only (ts, ge020005-equivalent = th GE02 col)
        partial_col = self.target_cols[4]
        spar = conn.statement2(
            f"insert into {self.db}.ct_e011_2 (`ts`,`{partial_col}`) values (?,?)")
        try:
            for i in range(self.num_ts):
                ts = self.base_ts + i
                for t, cols in self.groups.items():
                    if t == "e011_2":
                        continue
                    cl = ["ts"] + cols
                    data = [[[ts] if cn == "ts" else [val(cn, i)] for cn in cl]]
                    stmts[t].bind_param([f"ct_{t}"], None, data)
                    stmts[t].execute()
                if i % 2 == 0:         # full row
                    cl = ["ts"] + self.target_cols
                    data = [[[ts] if cn == "ts" else [val(cn, i)] for cn in cl]]
                    stmts["e011_2"].bind_param(["ct_e011_2"], None, data)
                    stmts["e011_2"].execute()
                else:                   # partial row -> triggers the bug
                    spar.bind_param(["ct_e011_2"], None, [[[ts], [round(2.5 + i * 0.001, 3)]]])
                    spar.execute()
        finally:
            for s in list(stmts.values()) + [spar]:
                s.close()
            conn.close()
            done.set()

    # ----- test ------------------------------------------------
    def test_tmq_partial_insert_wide_stable(self):
        """summary: tmq query topic must NULL-fill columns absent from a partial insert.

        Since: 3.0

        Labels: tmq,nchar,partial,wide-stable

        Catalog:
        - tmq:bug

        History:
        - reproduce: partial-column insert on wide stable -> corrupted tmq block (bug 7021746946)
        """
        self.prepare()

        done = threading.Event()
        wt = threading.Thread(target=self.writer, args=(done,), daemon=True)
        wt.start()

        consumer = Consumer({
            "group.id": "g_part_nchar",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": "true",
        })
        consumer.subscribe([self.topic])

        bad_nchar = []         # ge020004 values that are neither '1.0 nor None
        garbage_dbl = []       # double cells that look like freed-memory (denormal/huge)
        total_rows = 0
        idx = self.nchar_idx
        try:
            empty = 0
            while empty < 8:
                res = consumer.poll(1)
                if not res:
                    if done.is_set():
                        empty += 1
                    continue
                empty = 0
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    for row in block.fetchall():    # raises if the block is unparseable
                        total_rows += 1
                        nc = row[idx]
                        if nc not in ("1.0", None, ""):
                            bad_nchar.append((row[0], repr(nc)))
                        for j, cell in enumerate(row):
                            if j in (0, idx):
                                continue
                            if isinstance(cell, float) and cell != 0.0 and \
                               (abs(cell) < 1e-100 or abs(cell) > 1e100):
                                garbage_dbl.append((row[0], j, cell))
        except Exception as e:     # a crash during decode IS the bug manifesting
            consumer.close()
            tdLog.exit(f"consumer raised while decoding tmq block (the bug): {e}")
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            wt.join(timeout=30)

        tdLog.info(f"consumed {total_rows} rows; "
                   f"bad_nchar={len(bad_nchar)} garbage_double_cells={len(garbage_dbl)}")
        if bad_nchar:
            tdLog.exit(f"ge020004 corrupted on partial-insert rows (expected '1.0 or NULL): "
                       f"{bad_nchar[:8]}")
        if garbage_dbl:
            tdLog.exit(f"DOUBLE columns hold freed-memory garbage on partial-insert rows: "
                       f"{garbage_dbl[:8]}")
        if total_rows == 0:
            tdLog.exit("consumed 0 rows - test did not exercise the path")

        tdLog.success(f"{__file__} successfully executed")
