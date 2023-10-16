import sys 
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf

class TDTestCase:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TD-11204]Difference improvement that can ignore negative 
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use tagscan")


    def runSingleVgroup(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tagscan2")
        tdSql.execute("create database if not exists tagscan2 vgroups 1")
        tdSql.execute('use tagscan2')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tb1 using stb1 tags(1,'1',1.0);")

        tdSql.execute("create table tb2 using stb1 tags(2,'2',2.0);")

        tdSql.execute("create table tb3 using stb1 tags(3,'3',3.0);")

        tdSql.execute("create table tb4 using stb1 tags(4,'4',4.0);")

        tdSql.execute("create table tb5 using stb1 tags(5,'5',5.0);")

        tdSql.execute("create table tb6 using stb1 tags(5,'5',5.0);")

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb3 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb4 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb5 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb6 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.query('select tags t1,t2 from stb1 order by t1,t2;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, '3')
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, '4')
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, '5')
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(5, 1, '5')

        tdSql.query('select * from (select tags t1,t2 from stb1 group by t1,t2 slimit 2,3) order by t1,t2;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn from stb1 group by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname order by tbname limit 2,3) order by tn;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'tb3')

        tdSql.query('select * from (select distinct tbname tn from stb1 limit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select distinct tbname tn, t1,t2 from stb1 limit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags t1,t2 from stb1 order by t1, t2 limit 2,3) order by t1, t2;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, '3')
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, '4')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, '5')

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 partition by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn;')
        tdSql.checkRows(3)


        tdSql.execute('drop database tagscan2')
    def runMultiVgroups(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists tagscan")
        tdSql.execute("create database if not exists tagscan")
        tdSql.execute('use tagscan')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute("create table tb1 using stb1 tags(1,'1',1.0);")

        tdSql.execute("create table tb2 using stb1 tags(2,'2',2.0);")

        tdSql.execute("create table tb3 using stb1 tags(3,'3',3.0);")

        tdSql.execute("create table tb4 using stb1 tags(4,'4',4.0);")

        tdSql.execute("create table tb5 using stb1 tags(5,'5',5.0);")

        tdSql.execute("create table tb6 using stb1 tags(5,'5',5.0);")

        tdSql.execute('insert into tb1 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb2 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb3 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb4 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb5 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.execute('insert into tb6 values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1);')

        tdSql.query('select tags t1,t2 from stb1 order by t1,t2;')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, '1')
        tdSql.checkData(1, 0, 2)
        tdSql.checkData(1, 1, '2')
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(2, 1, '3')
        tdSql.checkData(3, 0, 4)
        tdSql.checkData(3, 1, '4')
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(4, 1, '5')
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(5, 1, '5')

        tdSql.query('select * from (select tags t1,t2 from stb1 group by t1,t2 slimit 2,3) order by t1,t2;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn from stb1 group by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn from stb1 group by tbname order by tbname limit 2,3) order by tn;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 'tb3')

        tdSql.query('select * from (select distinct tbname tn from stb1 limit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select distinct tbname tn, t1,t2 from stb1 limit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags t1,t2 from stb1 order by t1, t2 limit 2,3) order by t1, t2;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(0, 1, '3')
        tdSql.checkData(1, 0, 4)
        tdSql.checkData(1, 1, '4')
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(2, 1, '5')

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 partition by tbname slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn;')
        tdSql.checkRows(3)

        tdSql.query('select * from (select tags tbname tn, t1,t2 from stb1 group by tbname, t1,t2 slimit 2,3) order by tn;')
        tdSql.checkRows(3)


        tdSql.execute('drop database tagscan')

    def run(self):
        self.runMultiVgroups()
        self.runSingleVgroup()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
