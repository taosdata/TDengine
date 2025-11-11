from new_test_framework.utils import tdLog, tdSql, etool
import random
import string
import os


class TestHavingBugs:
    """Add test case to verify the complicated query accuracy"""

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def test_ts4806(self):
        """Having bug TS-4806

        test event_windows + case when + having query fix

        Catalog:
            - Query:Having

        Since: v3.3.0.0

        Labels: common,ci

        Jira: TS-4806

        History:
            - 2024-6-5 Alex Duan Created
            - 2025-5-08 Huo Hong Migrated from uncatalog/army/query/accuracy/test_query_accuracy.py

        """
        # database for case TS-4806
        tdSql.execute("create database db_ts4806;")
        tdSql.execute("use db_ts4806;")
        # super table
        tdSql.execute(
            "create table st (ts timestamp, adl float, bdl float, cdl float, ady float, bdy float, cdy float) \
            tags(pt_radio float, ct_ratio float, rated_cap float, ta_id varchar(128), id varchar(128), area_code \
            varchar(128), zdy_flag int, elec_cust_name bigint,bureau_code bigint, fl_name varchar(32), classify_id \
                varchar(128));"
        )
        # child table
        tdSql.execute(
            "create table ct_1 using st tags(1.2, 1.3, 3.4, '271000276', '30000001', '10001', 1, 10001, 2000001, 'beijing', '13169');"
        )
        tdSql.execute(
            "create table ct_2 using st tags(2.1, 1.2, 3.3, '271000277', '30000002', '10002', 1, 10002, 2000002, 'shanghai', '13141');"
        )
        tdSql.execute(
            "create table ct_3 using st tags(3.1, 4.2, 5.3, '271000278', '30000003', '10003', 0, 10003, 2000003, 'guangzhou', '13151');"
        )
        # insert data for ts4806
        start_ts = 1705783972000
        data = [
            (1.1, 2.2, 3.3, 1.1, 2.2, 3.3),
            (1.2, 2.3, 3.4, 1.2, 2.3, 3.4),
            (1.3, 2.4, 3.5, 1.3, 2.4, 3.5),
            (1.4, 2.5, 3.6, 1.4, 2.5, 3.6),
            (1.5, 2.6, 3.7, 1.5, 2.6, 3.7),
            (1.6, 2.7, 3.8, 1.6, 2.7, 3.8),
            (1.7, 2.8, 3.9, 1.7, 2.8, 3.9),
            (1.8, 2.9, 4.0, 1.8, 2.9, 4.0),
            (1.9, 4.2, 4.1, 1.9, 3.0, 4.1),
            (1.2, 3.1, 4.2, 2.0, 3.1, 4.2),
        ]
        index = [1, 2, 5, 0, 7, 3, 8, 4, 6, 9]
        for ct in ["ct_1", "ct_2"]:
            for i in range(10):
                sql = f"insert into {ct} values"
                for j in range(1000):
                    sql += f"({start_ts + i * 1000 * 1000 + j * 1000}, {','.join([str(item) for item in data[index[i]]])}),"
                sql += ";"
                tdSql.execute(sql)

        tdSql.execute("use db_ts4806;")
        tdSql.query(
            "select _wstart, cj.id, count(*) from st cj where cj.ts >= '2024-01-21 04:52:52.000' and cj.ts <= '2024-01-21 07:39:31.000' \
            and cj.zdy_flag = 1 and cj.id in ('30000001', '30000002') partition by cj.id event_window start with \
            (CASE WHEN cj.adl >= cj.bdl AND cj.adl >= cj.cdl THEN cj.adl WHEN cj.bdl >= cj.adl AND cj.bdl >= cj.cdl \
            THEN cj.bdl ELSE cj.cdl END) * cj.ct_ratio * 0.4 * 1.732 / cj.rated_cap > 1 end with (CASE WHEN cj.adl >= \
            cj.bdl AND cj.adl >= cj.cdl THEN cj.adl WHEN cj.bdl >= cj.adl AND cj.bdl >= cj.cdl THEN cj.bdl ELSE cj.cdl \
            END) * cj.ct_ratio * 0.4 * 1.732 / cj.rated_cap <= 1 HAVING count(*) >= 4 order by _wstart, cj.id;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(4, 1, "30000002")
        tdSql.checkData(4, 2, 1001)

    def test_td31880(self):
        """Having bug TD-31880

        test last_row(ts) query fix

        Since: v3.3.0.0

        Labels: last_row

        Jira: TD-31880

        History:
            - 2024-9-19 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated from uncatalog/army/query/accuracy/test_query_accuracy.py

        """

        # database for case TD-31880
        tdSql.execute("create database db_td31880;")
        tdSql.execute("use db_td31880;")
        # super table
        # tdSql.execute("create table st (ts timestamp, c1 int) tags(t1 int);")
        tdSql.execute(
            "create stable if not exists db_td31880.stb (ts timestamp, c1 timestamp, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 tinyint unsigned, c7 smallint unsigned, c8 int unsigned, c9 bigint unsigned, c10 float, c11 double, c12 varchar(64), c13 varbinary(64), c14 nchar(64), c15 geometry(64), c16 bool) tags (t1 timestamp, t2 tinyint, t3 smallint, t4 int, t5 bigint, t6 tinyint unsigned, t7 smallint unsigned, t8 int unsigned, t9 bigint unsigned, t10 float, t11 double, t12 varchar(64), t13 varbinary(64), t14 nchar(64), t15 geometry(64), t16 bool);"
        )
        csv_file = os.sep.join(
            [os.path.dirname(__file__), "create_table_by_csv_0627_5.csv"]
        ).replace("\\", "\\\\")
        tdSql.execute(
            f'insert into db_td31880.stb (ts,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,tbname) file "{csv_file}";'
        )

        tdSql.execute("use db_td31880;")
        tdSql.query("select last_row(ts) from stb group by tbname;")
        tdSql.checkRows(5000)

    def test_td31966(self):
        """Having bug TD-31966

        test percentile() from window query fix

        Since: v3.3.0.0

        Labels: percentile, windows

        Jira: TD-31966

        History:
            - 2024-9-19 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated from uncatalog/army/query/accuracy/test_query_accuracy.py

        """

        # database for case TD-31966
        tdSql.execute("create database db_td31966;")
        tdSql.execute("use db_td31966;")
        tdSql.execute("create table tb1 (ts timestamp, c1 int, c2 int);")
        sql = "insert into tb1 values ('2024-09-09 11:41:00', 1, 1)('2024-09-09 11:41:01', 1, 100)('2024-09-09 11:41:02', 2, 1)('2024-09-09 11:41:11', 2, 2)('2024-09-09 11:41:12', 2, 100)"
        tdSql.execute(sql)

        tdSql.execute("use db_td31966;")
        tdSql.error("select percentile(c2,20) from tb1 count_window(2);")
        tdSql.error(
            "select percentile(c2,20) from tb1 event_window start with f1 = 1 end with f1 > 1;"
        )
        # percentile min for time window
        tdSql.query(
            "select _wstart, _wend, percentile(c2,0) from tb1 interval(5s) sliding(3s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 100)
        # percentile max for time window
        tdSql.query(
            "select _wstart, _wend, percentile(c2,100) from tb1 interval(5s) sliding(3s);"
        )
        tdSql.checkRows(4)
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(1, 2, 100)
        tdSql.checkData(2, 2, 100)
        tdSql.checkData(3, 2, 100)
        # percentile min for state window
        tdSql.query("select _wstart, _wend, percentile(c2,0) from tb1 state_window(1);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 1)
        # percentile max for state window
        tdSql.query(
            "select _wstart, _wend, percentile(c2,100) from tb1 state_window(1);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 100)
        # percentile min for session window
        tdSql.query("select _wstart, _wend, percentile(c2,0) from tb1 session(ts, 3s);")
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 2, 2)
        # percentile max for session window
        tdSql.query(
            "select _wstart, _wend, percentile(c2,100) from tb1 session(ts, 3s);"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 2, 100)
        tdSql.checkData(1, 2, 100)

    def test_td32059(self):
        """Having bug TD-32059

        test having with interval and fill

        Since: v3.3.0.0

        Labels: having

        Jira: TD-32059

        History:
            - 2024-9-19 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated from uncatalog/army/query/accuracy/test_having.py

        """
        # data for TD-32059
        tdSql.execute("create database td32059;")
        tdSql.execute("use td32059;")
        tdSql.execute(
            "create stable stb (ts timestamp, id int, gid int) tags (t1 int);"
        )
        tdSql.execute(
            "insert into tb1 using stb (t1) tags (1) values ('2024-09-11 09:53:13.999', 6, 6)('2024-09-11 09:53:15.005', 6, 6)('2024-09-11 09:53:15.402', 6, 6);"
        )
        tdSql.execute(
            "insert into tb2 using stb (t1) tags(2) values ('2024-09-11 09:54:59.790', 9, 9)('2024-09-11 09:55:58.978', 11, 11)('2024-09-11 09:56:22.755', 12, 12)('2024-09-11 09:56:23.276', 12, 12)('2024-09-11 09:56:23.783', 12, 12)('2024-09-11 09:56:26.783', 12, 12)('2024-09-11 09:56:29.783', 12, 12);"
        )

        tdSql.execute("use td32059;")
        tdSql.query(
            "SELECT _wstart, last_row(id) FROM stb WHERE ts BETWEEN '2024-09-11 09:50:13.999' AND '2024-09-11 09:59:13.999' INTERVAL(30s) FILL(PREV) HAVING(last_row(id) IS NOT NULL);"
        )
        tdSql.checkRows(13)
        tdSql.checkAssert("NULL" not in [item[1] for item in tdSql.queryResult])