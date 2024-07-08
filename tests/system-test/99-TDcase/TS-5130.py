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
                new_conn = taos.connect(host = self.conn._host, config = self.conn._config, 
                database =db, user=uname, password=self.passwd[uname])
                cursor = new_conn.cursor()
                cursor.execute(f'show databases')
                result = cursor.fetchall()
                dbname = [i for i in result]
                assert result == [('information_schema',), ('performance_schema',)]
                tdLog.success(f"Test User {uname} for {db} .......[OK]")
                new_conn.close()

        except:
            tdLog.debug(f"{__file__}  Failed!")
            exit(-1)
    
    def run(self):
        self.prepare_user()
        self.test_connect_user('root')
        self.test_connect_user('test')
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successful executed")

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


            
    
