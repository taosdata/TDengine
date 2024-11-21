from util.sql import *
from util.common import *
import taos
taos.taos_connect
class TDTestCase:
    def init(self, conn, logSql, replicaVar = 1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        self.conn = conn
        tdSql.init(conn.cursor(), logSql)
    def initdb(self):
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database d0")
        tdSql.execute("use d0")
        tdSql.execute("create stable stb0 (ts timestamp, w_ts timestamp, opc nchar(100), quality int) tags(t0 int)")
        tdSql.execute("create table t0 using stb0 tags(1)")
        tdSql.execute("create table t1 using stb0 tags(2)")
    def multi_insert(self):
        for i in range(5):
            tdSql.execute(f"insert into t1 values(1721265436000, now() + {i + 1}s, '0', 12) t1(opc, quality, ts) values ('opc2', 192, now()+ {i + 2}s) t1(ts, opc, quality) values(now() + {i + 3}s, 'opc4', 10) t1 values(1721265436000, now() + {i + 4}s, '1', 191) t1(opc, quality, ts) values('opc5', 192, now() + {i + 5}s) t1 values(now(), now() + {i + 6}s, '2', 192)")
            tdSql.execute("insert into t0 values(1721265436000,now(),'0',192) t0(quality,w_ts,ts) values(192,now(),1721265326000) t0(quality,w_t\
s,ts) values(190,now()+1s,1721265326000) t0 values(1721265436000,now()+2s,'1',191) t0(quality,w_ts,ts) values(192,now()+3s,\
1721265326002) t0(ts,w_ts,opc,quality) values(1721265436003,now()+4s,'3',193) t0 values(now(), now() + 4s , '2', 192)")
    def run(self):
        self.initdb()
        self.multi_insert()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")
tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())