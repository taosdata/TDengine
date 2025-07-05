from new_test_framework.utils import tdLog, tdSql

class TestSystableFunc:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def test_systable_func(self):
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

        tdSql.prepare()

        tdSql.query(f"select count(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select sum(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select min(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select max(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select stddev(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select avg(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select apercentile(`columns`, 50) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select top(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select bottom(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select spread(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select histogram(`columns`, 'user_input', '[1, 3, 5]', 0) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select hyperloglog(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select sample(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.query(f"select mode(`columns`) from `information_schema`.`ins_tables`;")

        tdSql.error(f"select unique(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select tail(`columns`, 3) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select leastsquares(`columns`, 1, 1) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select elapsed(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select interp(`columns`) from `information_schema`.`ins_tables` range(0, 1) every(1s) fill(null);")
        tdSql.error(f"select percentile(`columns`, 50) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select derivative(`columns`, 1s, 0) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select irate(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select last_row(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select last(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select first(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select twa(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select diff(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select statecount(`columns`, 'GE', 0) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select stateduration(`columns`, 'GE', 0, 1s) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select csum(`columns`) from `information_schema`.`ins_tables`;")
        tdSql.error(f"select mavg(`columns`, 1) from `information_schema`.`ins_tables`;")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
