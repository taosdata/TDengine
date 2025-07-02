from new_test_framework.utils import tdLog, tdSql

class TestTs4348Td27939:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def test_ts_4348_td_27939(self):
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

        # TS-4348
        tdSql.query(f'select i8 from ts_4338.t;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where 1 = 1;')
        tdSql.checkRows(2)

        tdSql.query(f'select i8 from ts_4338.t where i8 = 1;')
        tdSql.checkRows(1)

        tdSql.query(f'select * from (select * from ts_4338.t where i8 = 3);')
        tdSql.checkRows(0)

        # TD-27939
        tdSql.query(f'select * from (select * from ts_4338.t where 1 = 100);')
        tdSql.checkRows(0)

        tdSql.query(f'select * from (select * from (select * from ts_4338.t where 1 = 200));')
        tdSql.checkRows(0)

        tdSql.execute("drop database if exists ts_4338;")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
