import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-21890] table count scan test case
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use tbl_count")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tbl_count")
        tdSql.execute("create database if not exists tbl_count")
        tdSql.execute('use tbl_count')
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

        tdSql.query('select count(*),db_name, stable_name from information_schema.ins_tables group by db_name, stable_name;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 29)
        tdSql.checkData(0, 1, 'information_schema')
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'tbl_count')
        tdSql.checkData(1, 2, 'stb1')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, 'performance_schema')
        tdSql.checkData(2, 2, None)

        tdSql.query('select count(1) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v desc;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 29)
        tdSql.checkData(0, 1, 'information_schema')
        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(1, 2, None)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, 'tbl_count')
        tdSql.checkData(2, 2, 'stb1')

        tdSql.query('select count(1) v,db_name from information_schema.ins_tables group by db_name order by v asc')
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(2, 0, 29)
        tdSql.checkData(2, 1, 'information_schema')

        tdSql.query("select count(*) from information_schema.ins_tables where db_name='tbl_count'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables where db_name=\'tbl_count\' and stable_name="stb1";')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 37)


        tdSql.execute('create table stba (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tba1 using stba tags(1,'1',1.0);")

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:00\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:01\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:02\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:04\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:05\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:06\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:07\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:08\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:09\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        self.restartTaosd(1, dbname='tbl_count')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:10\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:11\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:12\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:13\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:14\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:15\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:16\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:17\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:18\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:19\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        self.restartTaosd(1, dbname='tbl_count')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:20\',true, 1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:21\',true, 2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:22\',true, 3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:23\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:24\',true, 5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:25\',true, 6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:26\',true, 7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:27\',true, 8,8,8,8,8,8,"888","8888",8,8,8,8);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:28\',true, 9,9,9,9,9,9,"999","9999",9,9,9,9);')

        tdSql.execute('insert into tba1 values (\'2021-11-11 09:00:29\',true, 0,0,0,0,0,0,"000","0000",0,0,0,0);')

        tdSql.query('select count(*) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(0, 2, 'stba')

        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'tbl_count')
        tdSql.checkData(1, 2, 'stb1')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, 'performance_schema')
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, 29)
        tdSql.checkData(3, 1, 'information_schema')
        tdSql.checkData(3, 2, None)

        tdSql.query('select count(1) v,db_name, stable_name from information_schema.ins_tables group by db_name, stable_name order by v;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(0, 2, 'stba')
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 'tbl_count')
        tdSql.checkData(1, 2, 'stb1')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, 'performance_schema')
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 0, 29)
        tdSql.checkData(3, 1, 'information_schema')
        tdSql.checkData(3, 2, None)

        tdSql.query('select count(1) v,db_name from information_schema.ins_tables group by db_name order by v')
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 4)
        tdSql.checkData(0, 1, 'tbl_count')
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(1, 1, 'performance_schema')
        tdSql.checkData(2, 0, 29)
        tdSql.checkData(2, 1, 'information_schema')

        tdSql.query("select count(*) from information_schema.ins_tables where db_name='tbl_count'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query('select count(*) from information_schema.ins_tables where db_name=\'tbl_count\' and stable_name="stb1";')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 3)

        tdSql.query('select count(*) from information_schema.ins_tables')
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 38)


        tdSql.execute('drop database tbl_count')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
