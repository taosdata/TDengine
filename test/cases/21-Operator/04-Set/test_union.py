from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestUnion:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_ts6660(self):
        """union with order by

        test for ORDER BY column check in UNION

        Catalog:
            - Query:Union

        Since: v3.0.0.0

        Labels: common,ci

        Jira: https://jira.taosdata.com:18080/browse/TS-6660

        History:
            - 2025-8-11 Jinqing Kuang add test

        """

        db = "ts6660"
        stb = "stb"

        tdSql.execute(f"create database if not exists {db} keep 36500;")
        tdSql.execute(f"use {db};")

        tdLog.info("====== start create table and insert data")
        tdSql.execute(
            f"create table {stb} (attr_time timestamp, BAY_fwdwh_main double) tags (attr_id nchar(256));"
        )
        tdSql.execute(
            f"create table t1 using {stb} (attr_id) tags ('CN_53_N0007_S0001_BAY0001');"
        )
        tdSql.execute(
            f"create table t2 using {stb} (attr_id) tags ('CN_53_N0007_S0001_BAY0002');"
        )
        tdSql.execute(
            f"insert into t1 values ('2025-06-13 00:00:00.0', 130000), ('2025-06-13 07:30:00.0', 130730), ('2025-06-13 07:31:00.0', 130731);"
        )
        tdSql.execute(
            f"insert into t2 values ('2025-06-13 00:00:00.0', 130000), ('2025-06-13 07:30:00.0', 130730), ('2025-06-13 07:31:00.0', 130731);"
        )

        tdSql.error(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY _wstart;"""
        )

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart as attr_time, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY attr_time;"""
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    select attr_id, _wstart as attr_time, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY 2;"""
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"""
                    select attr_id, attr_time, BAY_fwdwh_main num
                    from {stb}
                    where attr_time = '2025-06-13 00:00:00.0'
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    UNION
                    (select attr_id, _wstart, first(BAY_fwdwh_main) num
                    from {stb}
                    where (attr_time = '2025-06-13 07:30:00.0' or attr_time = '2025-06-13 07:31:00.0')
                    and attr_id in ('CN_53_N0007_S0001_BAY0001', 'CN_53_N0007_S0001_BAY0002')
                    partition by attr_id interval(1m) fill(null)
                    ORDER BY _wstart);"""
        )
        tdSql.checkRows(6)
