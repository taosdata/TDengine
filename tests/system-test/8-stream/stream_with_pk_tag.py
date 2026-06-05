import threading
from util.log import *
from util.sql import *
from util.cases import *
from util.common import *

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self.tdCom = tdCom
        self.dbname = "stream_test"

    def stream_with_pk_tag(self):
        tdSql.execute(f"create database {self.dbname} vgroups 4;")
        tdSql.execute(f"use {self.dbname};")
        tdSql.execute("create table st(ts timestamp, a int primary key, b int , c int, d double) tags(ta varchar(100),tb int,tc int);")
        tdSql.execute('create table t1 using st tags("aa", 1, 2);')
        tdSql.execute('create stream streams3_2 trigger at_once ignore expired 0 ignore update 0 into streamt3_2 as select _wstart, a, max(b), count(*), ta from st partition by ta, a session(ts, 10s);;')
        sql_list = ["insert into stream_test.t1 values(1648791210001,1,2,3,4.1);", "insert into stream_test.t1 values(1648791210002,2,2,3,1.1);", "insert into stream_test.t1 values(1648791220000,3,2,3,2.1);", "insert into stream_test.t1 values(1648791220001,4,2,3,3.1);"]
        for i in range(5):
            for sql in sql_list:
                tdSql.execute(sql)
                time.sleep(2)

    def run(self):
        self.stream_with_pk_tag()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

event = threading.Event()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
