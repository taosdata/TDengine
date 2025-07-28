from new_test_framework.utils import tdLog, tdSql
import time


class TestTimezoneConf:
    updatecfgDict = {"clientCfg" : {'timezone': 'UTC', 'charset': 'gbk'}, 'timezone': 'UTC-8', 'charset': 'gbk'}
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)

    def timezone_check(self, timezone, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "timezone" :
                if tdSql.getData(i, 1).find(timezone) == -1:
                    tdLog.exit("show timezone:%s != %s"%(tdSql.getData(i, 1), timezone))

    def charset_check(self, charset, sql):
        tdSql.query(sql)
        rows = tdSql.getRows()
        for i in range(rows):
            if tdSql.getData(i, 0) == "charset" :
                if tdSql.getData(i, 1).find(charset) == -1:
                    tdLog.exit("show charset:%s != %s"%(tdSql.getData(i, 1), charset))

    def test_timezone_conf(self):
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

        self.charset_check('gbk', "show local variables")
        self.charset_check('gbk', "show dnode 1 variables")
        self.timezone_check('UTC', "show local variables")
        # time.sleep(50)
        self.timezone_check('UTC-8', "show dnode 1 variables")

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
