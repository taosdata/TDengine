from new_test_framework.utils import tdLog, tdSql

class TestOrderBySelectList:
    def setup(cls):
        tdLog.debug(f"start to execute {__file__}")
    
    def test_order_by_select_list(self):
        """Order By Select List

        Query the table with order by select list

        Catalog:
            - Query:OrderBy

        Since: v3.3.6.34

        Labels: common,ci

        Jira: TD-38284

        History:
            - 2025-11-26 Tony Zhang add this test case for TD-38284

        """

        tdLog.info(f"=============== Start test Order By Select List")

        tdSql.execute("create database if not exists db keep 3650;", show=1)
        tdSql.execute("use db;", show=1)
        tdSql.execute("create table tt (ts timestamp, v1 int, v2 int);", show=1)
        tdSql.execute("""
            insert into tt values 
            ('2025-01-01 09:00:00',  1, -3),
            ('2025-01-01 10:00:00', -2,  2),
            ('2025-01-01 11:00:00',  3,  1);
        """, show=1)

        # 'vv' only exists in select list
        tdSql.query("select ts, v1 as vv from tt order by vv asc", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 10:00:00')
        tdSql.checkData(0, 1, -2)
        tdSql.checkData(1, 0, '2025-01-01 09:00:00')
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, '2025-01-01 11:00:00')
        tdSql.checkData(2, 1, 3)

        # 'vv' only exists in select list
        tdSql.query("select ts, v1 as vv from tt order by abs(vv) asc", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 09:00:00')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, '2025-01-01 10:00:00')
        tdSql.checkData(1, 1, -2)
        tdSql.checkData(2, 0, '2025-01-01 11:00:00')
        tdSql.checkData(2, 1, 3)
        
        # 'v1' exists in both select list and table tt
        # prefer to use column from select list
        tdSql.query("select ts, v2 as v1 from tt order by v1 asc", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 09:00:00')
        tdSql.checkData(0, 1, -3)
        tdSql.checkData(1, 0, '2025-01-01 11:00:00')
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, '2025-01-01 10:00:00')
        tdSql.checkData(2, 1, 2)

        # 'v1' exists in both select list and table tt
        # prefer to use column from table tt
        tdSql.query("select ts, v2 as v1 from tt order by abs(v1) asc", show=1)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2025-01-01 09:00:00')
        tdSql.checkData(0, 1, -3)
        tdSql.checkData(1, 0, '2025-01-01 10:00:00')
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, '2025-01-01 11:00:00')
        tdSql.checkData(2, 1, 1)

        tdLog.info(f"=============== End test Order By Select List")