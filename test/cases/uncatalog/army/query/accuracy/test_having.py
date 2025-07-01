
from new_test_framework.utils import tdLog, tdSql

class TestHaving:
    """Add test case to cover having key word
    """
    @classmethod
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def prepare_data(self):
        # data for TD-32059
        tdSql.execute("create database td32059;")
        tdSql.execute("use td32059;")
        tdSql.execute("create stable stb (ts timestamp, id int, gid int) tags (t1 int);")
        tdSql.execute("insert into tb1 using stb (t1) tags (1) values ('2024-09-11 09:53:13.999', 6, 6)('2024-09-11 09:53:15.005', 6, 6)('2024-09-11 09:53:15.402', 6, 6);")
        tdSql.execute("insert into tb2 using stb (t1) tags(2) values ('2024-09-11 09:54:59.790', 9, 9)('2024-09-11 09:55:58.978', 11, 11)('2024-09-11 09:56:22.755', 12, 12)('2024-09-11 09:56:23.276', 12, 12)('2024-09-11 09:56:23.783', 12, 12)('2024-09-11 09:56:26.783', 12, 12)('2024-09-11 09:56:29.783', 12, 12);")

    def test_having(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        self.prepare_data()
        tdSql.execute("use td32059;")
        tdSql.query("SELECT _wstart, last_row(id) FROM stb WHERE ts BETWEEN '2024-09-11 09:50:13.999' AND '2024-09-11 09:59:13.999' INTERVAL(30s) FILL(PREV) HAVING(last_row(id) IS NOT NULL);")
        tdSql.checkRows(13)
        assert ('NULL' not in [item[1] for item in tdSql.res])

        tdLog.success(f"{__file__} successfully executed")

