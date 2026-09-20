import time
from new_test_framework.utils import tdLog, tdSql


class TestStableQueryTbnameEqFastpath:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    NUM_OF_TABLES = 2000
    BATCH_SIZE = 50

    def test_stable_query_tbname_eq_fastpath(self):
        """Query: tbname equal-condition fast path

        `tbname = 'const'` on a super table used to fall back to the generic tag-filter
        path, which fetches and scalar-filters the tags of every child table, while
        `tbname IN (...)` and indexed-tag conditions resolve candidate uids directly.
        This case covers the correctness of the tbname equal-condition fast path and a
        loose performance smoke check against an indexed-tag condition.

        Catalog:
            - SuperTable:Query

        Since: v3.3.7.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2026-09-18 Deng Yihao created for the tbname equal-condition fast path

        """

        tdLog.info(f"======== prepare {self.NUM_OF_TABLES} child tables in 2 vgroups")
        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database db1 vgroups 2")
        tdSql.execute("use db1;")
        tdSql.execute("create stable st1 (ts timestamp, f1 int) tags(tg1 int, tg2 binary(16));")
        tdSql.execute("create index idx1 on st1 (tg2);")

        for start in range(0, self.NUM_OF_TABLES, self.BATCH_SIZE):
            sql = "insert into "
            for i in range(start, min(start + self.BATCH_SIZE, self.NUM_OF_TABLES)):
                sql += " t%04d using st1 tags(%d, 'tag%04d') values ('2022-07-10 16:31:01', %d) " % (i, i, i, i)
            tdSql.execute(sql)

        tdLog.info("======== step1: tbname equal condition, results match the direct child-table query")
        tdSql.query("select * from t0500;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)

        tdSql.query("select * from st1 where tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)
        tdSql.checkData(0, 2, 500)
        tdSql.checkData(0, 3, "tag0500")

        tdSql.query("select count(*) from st1 where tbname = 't0500';")
        tdSql.checkData(0, 0, 1)

        # cold client catalog: the child-table rewrite cannot fire, so the equal condition
        # must be resolved by the vnode-side name lookup and still return the same rows
        tdSql.execute("reset query cache")
        tdSql.query("select * from st1 where tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)
        tdSql.checkData(0, 3, "tag0500")

        tdLog.info("======== step2: non-existent table name, no match")
        tdSql.query("select * from st1 where tbname = 'tb_not_exist';")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st1 where tbname = 'tb_not_exist';")
        tdSql.checkData(0, 0, 0)

        tdLog.info("======== step3: reversed operand order")
        tdSql.query("select * from st1 where 't0500' = tbname;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)

        tdLog.info("======== step4: tbname equal condition combined with AND")
        tdSql.query("select * from st1 where tbname = 't0700' and tg1 > 600;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 700)

        tdSql.query("select * from st1 where tbname = 't0500' and tg1 > 600;")
        tdSql.checkRows(0)

        tdSql.query("select * from st1 where tbname = 't0500' and f1 > 400;")
        tdSql.checkRows(1)

        tdLog.info("======== step5: OR combinations of tbname equal conditions")
        tdSql.query("select * from st1 where tbname = 't0500' or tbname = 't0700' order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 500)
        tdSql.checkData(1, 1, 700)

        tdSql.query("select * from st1 where tbname = 't0500' or tbname = 't0700' or tbname = 't0900';")
        tdSql.checkRows(3)

        tdSql.query("select * from st1 where tbname = 't0500' or tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)

        tdSql.query("select * from st1 where tbname = 'tb_not_exist' or tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)

        tdSql.query("select * from st1 where tbname = 'tb_not_exist' or tbname = 'tb_not_exist2';")
        tdSql.checkRows(0)

        tdSql.query("select count(*) from st1 where tbname = 'tb_not_exist' or tbname = 'tb_not_exist2';")
        tdSql.checkData(0, 0, 0)

        # reversed operand order inside the OR
        tdSql.query("select * from st1 where 't0500' = tbname or tbname = 't0700' order by tbname;")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 500)

        # OR nested in AND: only t0700 satisfies tg1 > 600
        tdSql.query("select * from st1 where (tbname = 't0500' or tbname = 't0700') and tg1 > 600;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 700)

        # mixed OR with a tag condition keeps the generic path and stays correct
        tdSql.query("select * from st1 where tbname = 't0500' or tg1 > 1998;")
        tdSql.checkRows(2)

        tdLog.info("======== step6: tag scan and cache-last scan go through the same table-list build")
        tdSql.query("select tbname, tg1 from st1 where tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "t0500")
        tdSql.checkData(0, 1, 500)

        tdSql.query("select last_row(*) from st1 where tbname = 't0500';")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 500)

        tdLog.info("======== step7: performance smoke, tbname eq should not be slower than an indexed tag lookup")
        # non-existent names on both sides: no client-side child-table rewrite can fire,
        # so both queries pay their vnode-side table-list resolution cost only
        for _ in range(3):
            tdSql.query("select count(*) from st1 where tbname = 'tb_not_exist';")
            tdSql.query("select count(*) from st1 where tg2 = 'tag_not_exist';")

        def min_cost(sql):
            costs = []
            for _ in range(5):
                start = time.time()
                tdSql.query(sql)
                costs.append(time.time() - start)
            return min(costs)

        cost_tbname_eq = min_cost("select count(*) from st1 where tbname = 'tb_not_exist';")
        cost_tbname_or = min_cost("select count(*) from st1 where tbname = 'tb_not_exist' or tbname = 'tb_not_exist2';")
        cost_tag_index = min_cost("select count(*) from st1 where tg2 = 'tag_not_exist';")
        tdLog.info(f"tbname equal-condition cost:{cost_tbname_eq:.4f}s, tbname OR cost:{cost_tbname_or:.4f}s, "
                   f"indexed-tag cost:{cost_tag_index:.4f}s")

        # wall-clock costs are logged for visibility only, never asserted: on loaded CI
        # machines any absolute or relative bound flakes. Perf regression detection belongs
        # to dedicated benchmarks; the measured before/after numbers live in the MR description.

        tdSql.execute("drop database if exists db1")
