from new_test_framework.utils.log import tdLog
from new_test_framework.utils.sql import tdSql
from new_test_framework.utils.common import tdCom


class TestTS_5130:
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")
        cls.passwd = {'root':'taosdata',
                       'test':'test123@#$'}
    def prepare_user(self):
        tdSql.execute(f"create user test pass 'test123@#$' sysinfo 1")

    def check_connect_user(self, uname):
        try:
            for db in ['information_schema', 'performance_schema']:
                new_tdsql = tdCom.newTdSql(user=uname, password=self.passwd[uname], database=db)
                new_tdsql.query('show databases')
                new_tdsql.checkData(0, 0, 'information_schema')
                new_tdsql.checkData(1, 0, 'performance_schema')
                tdLog.success(f"Test User {uname} for {db} .......[OK]")
        except:
            tdLog.exit(f'{__file__} failed')
    
    def test_ts_5130(self):
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
        self.prepare_user()
        self.check_connect_user('root')
        self.check_connect_user('test')
    
        # Cleanup from original stop method
        tdLog.success(f"{__file__} successfully executed")



            
    

    