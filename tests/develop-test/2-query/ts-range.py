import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TS-4088] timestamp range support operator
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use ts_range")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists ts_range")
        tdSql.execute("create database if not exists ts_range")
        tdSql.execute('use ts_range')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tb1 using stb1 tags(1,'1',1.0);")

        tdSql.execute("create table tb2 using stb1 tags(2,'2',2.0);")

        tdSql.execute("create table tb3 using stb1 tags(3,'3',3.0);")

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute("insert into tb1 values ('2021-11-11 09:00:01',true,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);")

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:02\',true,2,NULL,2,NULL,2,NULL,"234",NULL,2,NULL,2,NULL);')

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:03\',false,NULL,3,NULL,3,NULL,3,NULL,"3456",NULL,3,NULL,3);')

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:04\',true,4,4,4,4,4,4,"456","4567",4,4,4,4);')

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:05\',true,127,32767,2147483647,9223372036854775807,3.402823466e+38,1.79769e+308,"567","5678",254,65534,4294967294,9223372036854775807);')

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:06\',true,-127,-32767,-2147483647,-9223372036854775807,-3.402823466e+38,-1.79769e+308,"678","6789",0,0,0,0);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:01\',true,2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:02\',true,3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:04\',true,5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:05\',true,6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:06\',true,7,7,7,7,7,7,"777","7777",7,7,7,7);')
        
        	
        tdSql.query('select count(*) from stb1 where ts < 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)        	
        tdSql.query('select count(*) from stb1 where ts >= 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 14)  
        	
        tdSql.query('select count(*) from stb1 where ts > 1000000000000 - 10s and ts <= 1000000000000 + 10s')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)   
	 
        tdSql.query('select count(*) from stb1 where ts > 1636592400000 + 3s');
        tdSql.checkData(0, 0, 6)
        #tdSql.execute('drop database ts_range')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
