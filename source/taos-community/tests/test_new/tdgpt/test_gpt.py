from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
import taos



class TDTestCase:
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {
        "debugFlag"        : "135",
        "queryBufferSize"  : 10240,
        'clientCfg'        : clientCfgDict
    }

    def init(self, conn, logSql, replicaVal=1):
        self.replicaVar = int(replicaVal)
        tdLog.debug(f"start to excute {__file__}")
        self.conn = conn
        tdSql.init(conn.cursor(), False)
        self.passwd = {'root':'taosdata',
                       'test':'test'}

    def prepare_anode_data(self):
        tdSql.execute(f"create anode '127.0.0.1:6090'")
        tdSql.execute(f"create database db_gpt")
        tdSql.execute(f"create table if not exists db_gpt.stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned);")
        tdSql.execute(f"create table db_gpt.ct1 using db_gpt.stb tags(1000);")
        tdSql.execute(f"insert into db_gpt.ct1(ts, c1) values(now-1a, 5)(now+1a, 14)(now+2a, 15)(now+3a, 15)(now+4a, 14);")
        tdSql.execute(f"insert into db_gpt.ct1(ts, c1) values(now+5a, 19)(now+6a, 17)(now+7a, 16)(now+8a, 20)(now+9a, 22);")
        tdSql.execute(f"insert into db_gpt.ct1(ts, c1) values(now+10a, 8)(now+11a, 21)(now+12a, 28)(now+13a, 11)(now+14a, 9);")
        tdSql.execute(f"insert into db_gpt.ct1(ts, c1) values(now+15a, 29)(now+16a, 40);")


    def test_forecast(self):
        """
        Test forecast
        """
        tdLog.info(f"Test forecast")
        tdSql.query(f"SELECT _frowts, FORECAST(c1, \"algo=arima,alpha=95,period=10,start_p=1,max_p=5,start_q=1,max_q=5,d=1\") from db_gpt.ct1 ;")
        tdSql.checkRows(10)

    def test_anomaly_window(self):
        """
        Test anomaly window
        """
        tdLog.info(f"Test anomaly window")
        tdSql.query(f"SELECT _wstart, _wend, SUM(c1)  FROM db_gpt.ct1  ANOMALY_WINDOW(c1, \"algo=iqr\");")
        tdSql.checkData(0,2,40)

    
    def run(self):
        self.prepare_anode_data()
        self.test_forecast()
        self.test_anomaly_window()
    
    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())


            
    
