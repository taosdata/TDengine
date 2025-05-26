from new_test_framework.utils import tdLog, tdSql


class TestHavingWithIntervalAndFill:
    """Add test case to cover having key word
    """
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        # data for TD-32059
        tdSql.execute("create database td32059;")
        tdSql.execute("use td32059;")
        tdSql.execute("create stable stb (ts timestamp, id int, gid int) tags (t1 int);")
        tdSql.execute("insert into tb1 using stb (t1) tags (1) values ('2024-09-11 09:53:13.999', 6, 6)('2024-09-11 09:53:15.005', 6, 6)('2024-09-11 09:53:15.402', 6, 6);")
        tdSql.execute("insert into tb2 using stb (t1) tags(2) values ('2024-09-11 09:54:59.790', 9, 9)('2024-09-11 09:55:58.978', 11, 11)('2024-09-11 09:56:22.755', 12, 12)('2024-09-11 09:56:23.276', 12, 12)('2024-09-11 09:56:23.783', 12, 12)('2024-09-11 09:56:26.783', 12, 12)('2024-09-11 09:56:29.783', 12, 12);")

    def test_td32059(self):
        """test having with interval and fill

        test having with interval and fill

        Since: v3.3.0.0

        Labels: having

        Jira: TD-32059

        History:
            - 2024-9-19 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        tdSql.execute("use td32059;")
        tdSql.query("SELECT _wstart, last_row(id) FROM stb WHERE ts BETWEEN '2024-09-11 09:50:13.999' AND '2024-09-11 09:59:13.999' INTERVAL(30s) FILL(PREV) HAVING(last_row(id) IS NOT NULL);")
        tdSql.checkRows(13)
        checkAssert ('NULL' not in [item[1] for item in tdSql.queryResult])



