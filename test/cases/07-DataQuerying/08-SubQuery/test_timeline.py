from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestTimeline:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_timeline(self):
        """timeline

        1.

        Catalog:
            - DataTypes

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/timeline.sim

        """

        tdSql.execute(f"create database test;")
        tdSql.execute(f"use test;")

        tdSql.execute(
            f"CREATE STABLE `demo` (`_ts` TIMESTAMP, `faev` DOUBLE) TAGS (`deviceid` VARCHAR(256));"
        )
        tdSql.execute(f"CREATE TABLE demo_1 USING demo (deviceid) TAGS ('1');")
        tdSql.execute(f"CREATE TABLE demo_2 USING demo (deviceid) TAGS ('2');")
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-11-30 00:00:00.000', 1.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-04 01:00:00.001', 2.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-04 02:00:00.002', 3.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_1 (_ts,faev) VALUES ('2023-12-05 03:00:00.003', 4.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-11-30 00:00:00.000', 5.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-28 01:00:00.001', 6.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-28 02:00:00.002', 7.0);"
        )
        tdSql.execute(
            f"INSERT INTO demo_2 (_ts,faev) VALUES ('2023-12-29 03:00:00.003', 8.0);"
        )

        tdSql.error(
            f"select diff(faev) from ((select ts, faev from demo union all select ts, faev from demo));"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts, faev from demo order by faev, _ts);"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev from demo union all select _ts + 1s, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by deviceid, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev, deviceid from demo union all select _ts + 1s, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev;"
        )

        tdSql.error(f"select diff(faev) from (select _ts, faev from demo);")
        tdSql.error(
            f"select diff(faev) from (select _ts, faev from demo order by faev, _ts);"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev from demo order by faev, _ts) partition by faev;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by faev, _ts) partition by deviceid;"
        )
        tdSql.error(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by deviceid, _ts) partition by faev;"
        )
        tdSql.query(
            f"select diff(faev) from (select _ts, faev, deviceid from demo order by faev, _ts, deviceid) partition by faev;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n))) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM (SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )
        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM (SELECT deviceid, ts, faev FROM (SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0)UNION ALL(SELECT deviceid, _wstart AS ts, LAST(faev) AS faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-11-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid INTERVAL(1n)) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0) ORDER BY deviceid, ts) PARTITION by deviceid;"
        )

        tdSql.query(
            f"select deviceid, ts, diff(faev) as diff_faev FROM ((SELECT deviceid, ts, faev FROM (SELECT deviceid, _ts AS ts, faev, DIFF(ROUND(faev*1000)/1000) AS diff_faev FROM demo WHERE deviceid in ('201000008','K201000258') AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' PARTITION BY deviceid) WHERE diff_faev < 0)UNION ALL(SELECT deviceid, ts, faev FROM (SELECT deviceid, ts, faev, DIFF(ROUND(faev*1000)/1000) as diff_faev FROM (SELECT deviceid, _ts as ts , faev FROM demo WHERE deviceid in ('201000008','K201000258')AND _ts >= '2023-12-01 00:00:00' AND _ts < '2024-01-01 00:00:00' ORDER BY ts desc) PARTITION BY deviceid) WHERE diff_faev > 0) ORDER BY ts, deviceid) PARTITION by deviceid;"
        )
