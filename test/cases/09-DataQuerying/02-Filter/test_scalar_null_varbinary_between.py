from new_test_framework.utils import tdLog, tdSql


class TestScalarNullVarbinaryBetween:
    """Regression test for scalar engine crash when BETWEEN expression has a
    NULL VARBINARY operand from cast(null as varbinary).

    Related: defect #6948526778
    Crash SQL: ... not (cast(null as varbinary) between now() and ts) ...
    Root cause: cast(null as varbinary) produces SValueNode{isNull=true, datum.p=NULL}.
                SCL_IS_VAR_VALUE_NODE matches because VARBINARY is a string type,
                but does not check isNull. sclConvertToTsValueNode then passes
                datum.p=NULL to convertStringToTimestamp, which calls
                varDataLen(NULL) -> SIGSEGV.
    Fix: in sclConvertToTsValueNode, update resType to TIMESTAMP for null nodes
         but skip datum conversion (datum.p is NULL). Remove the !isNull guards
         added to scalarConvertOpValueNodeTs so that null nodes still enter
         sclConvertToTsValueNode and get their type updated correctly.
    """

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_scalar_null_varbinary_between_no_crash(self):
        """Verify that BETWEEN with cast(null as varbinary) does not crash taosd.

        The expression 'cast(null as varbinary) between now() and ts' compares
        a NULL varbinary constant against timestamp bounds. After the fix the
        query should return a result (or a semantic error), never a crash.

        Catalog:
            - Query:Filter

        Since: v3.4.1.0

        Labels: common,ci

        Jira: TD-6948526778

        History:
            - 2026-4-13 fix: prevent taosd crash on NULL varbinary in BETWEEN timestamp expression
        """

        tdSql.execute("drop database if exists tdsqlsmith_shared")
        tdSql.execute("create database tdsqlsmith_shared")
        tdSql.execute("use tdsqlsmith_shared")

        create_cols = (
            "ts timestamp, id int, v int, c1 int, c2 int, "
            "u1 int unsigned, bi bigint, ubi bigint unsigned, f float, d double, "
            "si smallint, usi smallint unsigned, ti tinyint, uti tinyint unsigned, "
            "ok bool, a binary(32), b varchar(64), n nchar(32), "
            "vb varbinary(64), geo geometry(100), de decimal(18,6)"
        )
        tdSql.execute(f"create table t1({create_cols})")
        tdSql.execute(f"create table t2({create_cols})")
        tdSql.execute(f"create table t3({create_cols})")

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

        # --- original fuzzing reproduction (cast(null as varbinary) in ORDER BY) ---
        # de > 47 matches t3's single row (de=323.5). The ORDER BY uses nvl2(usi, ...,
        # not(...)) where usi=54 is non-NULL, so nvl2 picks the non-null branch and
        # the null varbinary expression is never evaluated at runtime — but it must
        # still survive constant folding in the planner without crashing.
        tdSql.query(
            "select de, u1, ti from t3 where de > 47 "
            "order by tbname asc nulls last, "
            "nvl2(usi, case server_version() when round(id) | 6 "
            "then ok not between ok and cast(de as timestamp) else d end, "
            "not (cast(null as varbinary) between now() and ts)) "
            "desc nulls last limit 33;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "323.500000")
        tdSql.checkData(0, 1, 31)
        tdSql.checkData(0, 2, 17)

        # --- minimal ORDER BY reproducer ---
        tdSql.query(
            "select de, u1, ti from t3 "
            "order by nvl2(usi, 1.0, not (cast(null as varbinary) between now() and ts)) "
            "limit 5;"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "323.500000")
        tdSql.checkData(0, 1, 31)
        tdSql.checkData(0, 2, 17)

        # --- WHERE clause variants: null string type in BETWEEN timestamp range ---
        # NOT (NULL BETWEEN now() AND ts) evaluates to true in TDengine when the
        # left operand is a null constant (treated as false in BETWEEN, negated to
        # true), so all rows pass the filter.
        tdSql.query(
            "select id from t1 where not (cast(null as binary(16)) between now() and ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)

        tdSql.query(
            "select id from t2 where not (cast(null as varchar(16)) between now() and ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 2)

        tdSql.query(
            "select id from t3 where not (cast(null as nchar(16)) between now() and ts);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.execute("drop database tdsqlsmith_shared")
        tdLog.success(f"{__file__} successfully executed")
