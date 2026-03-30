from new_test_framework.utils import tdLog, tdSql


class TestScalarInvalidType:
    """Regression test for scalar engine crash when IN expression has
    invalid type propagated from unhandled node type in sclGetNodeType.

    Related: defect #6925396284
    Crash SQL: ifnull(b not between vb and a, n in ('s_199', ...)) in (today(), ...)
    Root cause: sclGetNodeType fallthrough set type=-1 silently, then
                scalarGenerateSetFromList used (uint32_t)-1 = 4294967295 as
                index into tDataTypes[] → OOB crash.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_invalid_type_no_crash(self):
        """Verify that semantically invalid IN expressions do not crash taosd.

        The expressions compare boolean/non-timestamp results against
        timestamp lists, which should return an error, not a crash.

        Catalog:
            - Query:Filter

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TD-6925396284

        History:
            - 2026-3-30 fix: prevent taosd crash on invalid type in scalar IN expression
        """
        
        tdSql.execute("drop database if exists tdsqlsmith_shared")
        tdSql.execute("create database tdsqlsmith_shared")
        tdSql.execute("use tdsqlsmith_shared")
        tdSql.execute(
            "create table t1(ts timestamp, id int, v int, c1 int, c2 int, "
            "u1 int unsigned, bi bigint, ubi bigint unsigned, f float, d double, "
            "si smallint, usi smallint unsigned, ti tinyint, uti tinyint unsigned, "
            "ok bool, a binary(32), b varchar(64), n nchar(32), "
            "vb varbinary(64), geo geometry(100), de decimal(18,6))"
        )
        tdSql.execute(
            "create table t2(ts timestamp, id int, v int, c1 int, c2 int, "
            "u1 int unsigned, bi bigint, ubi bigint unsigned, f float, d double, "
            "si smallint, usi smallint unsigned, ti tinyint, uti tinyint unsigned, "
            "ok bool, a binary(32), b varchar(64), n nchar(32), "
            "vb varbinary(64), geo geometry(100), de decimal(18,6))"
        )
        tdSql.execute(
            "create table t3(ts timestamp, id int, v int, c1 int, c2 int, "
            "u1 int unsigned, bi bigint, ubi bigint unsigned, f float, d double, "
            "si smallint, usi smallint unsigned, ti tinyint, uti tinyint unsigned, "
            "ok bool, a binary(32), b varchar(64), n nchar(32), "
            "vb varbinary(64), geo geometry(100), de decimal(18,6))"
        )
        tdSql.execute(
            "insert into t1 values(now,1,10,1,2,11,111111111,222222222,1.25,2.5,"
            "12,34,7,9,true,'alpha','beta','gamma','\\x010203','POINT(1 2)',123.456789)"
        )
        tdSql.execute(
            "insert into t2 values(now,2,20,3,4,21,211111111,322222222,3.5,4.75,"
            "-12,44,-7,19,false,'left','right','delta','\\x0A0B0C','POINT(2 3)',223.000001)"
        )
        tdSql.execute(
            "insert into t3 values(now,3,30,5,6,31,311111111,422222222,5.75,6.125,"
            "22,54,17,29,true,'foo','bar','omega','\\x112233','POINT(3 4)',323.5)"
        )

        # Previously caused taosd crash (OOB access on tDataTypes[4294967295])
        # After fix: should return error or empty result, not crash
        crash_sqls = [
            "select 's_770' from t3 as y where ifnull(b not between vb and a, n in ('s_199', 's_445', 's_683')) in (today(), today(), now()) order by abs(uti) & c2 desc nulls last",
            "select a, sum(c1), sum(usi) from t2 where (c2 between (+v) and ceil(ubi)) and (nullif(bi, si) not in ('s_436', 's_131', 's_542')) group by a",
            "select vb, f, max(u1) from t2 as x where not (ifnull(floor(d) & c2, ok is null) not in (false, true, false)) group by vb, f limit 21",
        ]

        for sql in crash_sqls:
            tdLog.debug(f"testing: {sql}")
            try:
                # The query may succeed (return 0 rows) or fail with error
                # Either is acceptable — what's NOT acceptable is a crash
                tdSql.query(sql, queryTimes=1)
                tdLog.debug(f"query returned {tdSql.queryRows} rows (no crash)")
            except Exception as e:
                tdLog.debug(f"query returned error (expected): {e}")

        tdSql.execute("drop database tdsqlsmith_shared")
        tdLog.success(f"{__file__} successfully executed")
