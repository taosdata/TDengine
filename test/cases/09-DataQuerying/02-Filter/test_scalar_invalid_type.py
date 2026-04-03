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
            "select 's_770' from t3 as y where ifnull(b not between vb and a, n in ('s_199', 's_445', 's_683')) in (today(), today(), now()) order by abs(uti) & c2 desc nulls last;",
            "select a, sum(c1), sum(usi) from t2 where (c2 between (+v) and ceil(ubi)) and (nullif(bi, si) not in ('s_436', 's_131', 's_542')) group by a;",
            "select vb, f, max(u1) from t2 as x where not (ifnull(floor(d) & c2, ok is null) not in (false, true, false)) group by vb, f limit 21;",
            "select usi, c1, avg(uti) from t2 as b where (not (trim(n) between position(n in b) and c2)) or (not (nullif(ti, ubi) not in ('s_857', 's_955', 's_284'))) group by usi, c1 having max(de) is not null;",
            "select c2 as z, avg(bi) from t3 as a where nullif(39, si) in (now(), now(), today()) group by c2 having max(si) is not null limit 49 offset 32;",
            "select b, a, avg(bi) from t2 as b where not (ifnull(c1, 5.59) in (9.15, 19, 30)) group by b, a having count(*) > 8.28 limit 9 offset 38;",
            "select ts != ts as y, id from t2 as y where ((case when v then ubi else bi end) in ('{\"k\":1}', '{\"k\":1}', '{\"k\":1}')) and (not isnull(c2)) order by cast(ti as binary(16)) asc nulls last, trim(a) desc nulls last;",
            "select rand() is null, *, * from t1 where not ((case when de / pow(c1, 15) then 16 else if(bi, uti, 45) end) in (now(), now(), today())) limit 28 offset 20;",
            "select n, sum(c2), sum(c1) from t2 where not (coalesce(u1, tbname) in ('s_587', 's_377', 's_211')) group by n having count(*) > 3 limit 34;",
            "select c2, count(*), max(c2) from t2 as y where not (ifnull(-ti, ceil(d) is null) in ('{\"k\":1}', '{\"k\":1}', '{\"k\":1}')) group by c2 limit 36;",
            "select case when cast(c1 as bigint) then 's_243' else b end, * from t2 where (case bi when abs(5.0) * uti then -usi else a end) in ('s_445', 's_561', 's_853') limit 7;",
            "select n, c2, max(ti) from t1 as a where if(ok, d, v) in ('s_277', 's_966', 's_361') group by n, c2 having sum(ti) > 5 limit 9;",
            "select v, count(*), sum(f) from t1 where not ((case c2 when nullif(trim(leading n from a), usi) then bi else c2 end) not in (false, false, false)) group by v having count(*) > 41 limit 6;",
            "select * from t1 where (case when si then si else 39 end) in ('s_949', 's_952', 's_701');",
            "select * from t1 as x where (case ubi is null when cast(u1 as integer) then replace(b, a, b) else abs(v) / 37 end) in (true, false, false);",
            "select 5, * from t2 as z where (case when usi then ok between ok and ok else de end) not in (26, 6.68, 13) order by trim(upper(n) from a) asc nulls last, +(+ifnull(c2, b)) asc nulls last limit 18 offset 49;",
            "select f, max(ti) from t2 as x where not (nvl2(c1, de not in (null, 's_720', '{\"k\":1}'), c2) not in (now(), today(), today())) group by f having count(*) > 1.82;",
            "select v, count(*), sum(v) from t3 as a where not (nvl2(cast(de as binary), uti, d) in (16, 9.66, 10)) group by v limit 38;",
            "select *, c2, u1, uti from t1 as b where isnotnull(id) or (coalesce(ubi, a) not in ('{\"k\":1}', '{\"k\":1}', '{\"k\":1}'));"
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
