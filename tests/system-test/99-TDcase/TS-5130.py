from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
import taos



class TDTestCase:
    def init(self, conn, logSQl, replicaVal=1):
        self.replicaVar = int(replicaVal)
        tdLog.debug(f"start to excute {__file__}")
        self.conn = conn
        tdSql.init(conn.cursor(), False)
        self.passwd = {'root':'taosdata',
                       'test':'test'}
    def prepare_user(self):
        tdSql.execute(f"create user test pass 'test' sysinfo 1")

    def test_connect_user(self, uname):
        try:
            for db in ['information_schema', 'performance_schema']:
                new_tdsql = tdCom.newTdSql(user=uname, password=self.passwd[uname], database=db)
                new_tdsql.query('show databases')
                new_tdsql.checkData(0, 0, 'information_schema')
                new_tdsql.checkData(1, 0, 'performance_schema')
                tdLog.success(f"Test User {uname} for {db} .......[OK]")
        except:
            tdLog.exit(f'{__file__} failed')
    
    def run(self):
        self.prepare_user()
        self.test_connect_user('root')
        self.test_connect_user('test')
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


            
    
