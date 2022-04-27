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
    
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use diffneg")

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists diffneg")
        tdSql.execute("create database if not exists diffneg")
        tdSql.execute('use diffneg')
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

        tdSql.execute('create table tbn (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"111","1111",1,1,1,1);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:01\',true,2,2,2,2,2,2,"222","2222",2,2,2,2);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:02\',true,3,3,2,3,3,3,"333","3333",3,3,3,3);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:03\',false,4,4,4,4,4,4,"444","4444",4,4,4,4);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:04\',true,5,5,5,5,5,5,"555","5555",5,5,5,5);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:05\',true,6,6,6,6,6,6,"666","6666",6,6,6,6);')

        tdSql.execute('insert into tbn values (\'2021-11-11 09:00:06\',true,7,7,7,7,7,7,"777","7777",7,7,7,7);')

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c7,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c7,0) as a from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(c7) + 1 as a from tb1;')

        tdSql.error('select diff(tb1.*) + 1 as a from tb1;')

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c4,1) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.error('select diff(c1 + c2) from tb1;')

        tdSql.error('select diff(13) from tb1;')

        tdSql.query('select diff(c4,1) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c2) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 123)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c3) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 32763)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c5) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 9223372036854775803)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c6) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 3.4028234663852886e+38)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(c8) from tb1;')

        tdSql.error('select diff(c9) from tb1;')

        tdSql.error('select diff(c10) from tb1;')

        tdSql.error('select diff(c11) from tb1;')

        tdSql.error('select diff(c12) from tb1;')

        tdSql.error('select diff(c13) from tb1;')

        tdSql.error('select diff(12345678900000000000000000) from tb1;')

        tdSql.error('select distinct diff(c4,1) from tb1;')

        tdSql.error('select diff(t1) from stb1;')

        tdSql.error('select diff(c4,1),avg(c3) from tb1;')

        tdSql.error('select diff(c4,1),top(c3,1) from tb1;')

        tdSql.error('select diff(c4,1) from tb1 session(ts, 1s);')

        tdSql.error('select diff(c4,1) from tb1 STATE_WINDOW(c4,1);')

        tdSql.error('select diff(c4,1) from tb1 interval(1s) sliding(1s) fill(NULL);')

        tdSql.error('select diff(c4,1) from stb1 group by t1;')

        tdSql.error('select diff(c4,1) from stb1 group by ts;')

        tdSql.error('select diff(c4,1) from stb1 group by c1;')

        tdSql.query('select diff(c4,1) from stb1 group by tbname;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 'tb1')
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 'tb1')
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(2, 2, 'tb1')
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 'tb1')
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 'tb2')
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(5, 1, 0)
        tdSql.checkData(5, 2, 'tb2')
        tdSql.checkData(6, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(6, 1, 2)
        tdSql.checkData(6, 2, 'tb2')
        tdSql.checkData(7, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(7, 2, 'tb2')
        tdSql.checkData(8, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(8, 2, 'tb2')
        tdSql.checkData(9, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(9, 1, 1)
        tdSql.checkData(9, 2, 'tb2')

        tdSql.error('select diff(c4,1) from tb1 order by c2;')

        tdSql.error('select diff(c8),diff(c9) from tbn;')

        tdSql.error('select diff(ts) from (select avg(c2) as a from stb1 interval(1s));')

        tdSql.query('select diff(a) from (select diff(c2) as a from tb1);')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 121)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, -121)

        tdSql.error('select diff("abc") from tb1;')

        tdSql.query('select diff(c4,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c6) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 3.4028234663852886e+38)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(11)+c2 from tb1;')

        tdSql.error('select diff(c4,1)+c2 from tb1;')

        tdSql.error('select diff(c2)+11 from tb1;')

        tdSql.error('select diff(c4,1),c1,c2 from tb1;')

        tdSql.query('select diff(c4,1),t1,ts,tbname,_C0,_c0 from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 4, 'tb1')
        tdSql.checkData(0, 5, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 6, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 4, 'tb1')
        tdSql.checkData(1, 5, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 6, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 4, 'tb1')
        tdSql.checkData(2, 5, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 6, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 4, 'tb1')
        tdSql.checkData(3, 5, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 6, datetime.datetime(2021, 11, 11, 9, 0, 6))

        tdSql.error('select diff(c4,1),floor(c3) from tb1;')

        tdSql.error('select diff(c4,1),diff(c4,1) from tb1;')

        tdSql.query('select diff(c4,1) from tb1 where c2 is not null and c3 is not null;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 2147483643)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, 2)

        tdSql.query('select diff(c2) from tb1 order by ts desc;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(0, 1, -2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, -123)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(2, 1, -2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0))
        tdSql.checkData(3, 1, -1)

        tdSql.query('select diff(c4,1) from tb1 order by ts desc;')
        tdSql.checkRows(0)

        tdSql.query('select diff(c4,1) from tb1 order by ts desc limit 3 offset 2;')
        tdSql.checkRows(0)

        tdSql.error('select diff(c2) from stb1;')

        tdSql.error('select diff(c2) from stb1 order by ts desc;')

        tdSql.error('select diff(c4),t1 from stb1 order by ts desc;')

        tdSql.error('select diff(c3),tbname from stb1;')

        tdSql.error('select diff(c3),tbname from stb1 where t1 > 1;')

        tdSql.error('select diff(c8),diff(c9) from tbn;')

        tdSql.error('select diff(c8),diff(c9) from tbn order by ts desc;')

        tdSql.error('select diff(diff(c8)) from tbn;')

        tdSql.query('select diff(a) from (select avg(c2) as a from stb1 interval(1s));')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.error('select diff(c2) from (select * from stb1);')

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null));")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by ts;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by ts desc;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(0, 1, 126.5)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, -62.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, -0.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(3, 1, -1.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, -0.5)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0))
        tdSql.checkData(5, 1, -1.0)

        tdSql.error("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by a desc;")

        tdSql.error("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by a;")

        tdSql.query('select diff(a) from (select diff(c2) as a from tb1);')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 121)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, -121)

        tdSql.error('select diff(tb1.c3),diff(tb2.c3) from tb1,tb2 where tb1.ts=tb2.ts;')

        tdSql.query('select diff(c3) from tb1 union all select diff(c3) from tb2;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 32763)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(8, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(9, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(9, 1, 1)


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

        self.restartTaosd(1, dbname='diffneg')

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

        self.restartTaosd(1, dbname='diffneg')

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

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c7,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c7,0) as a from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(c7) + 1 as a from tb1;')

        tdSql.error('select diff(tb1.*) + 1 as a from tb1;')

        tdSql.query('select diff(c7,1) from tb1;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)

        tdSql.query('select diff(c4,1) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.error('select diff(c1 + c2) from tb1;')

        tdSql.error('select diff(13) from tb1;')

        tdSql.query('select diff(c4,1) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c2) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 123)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c3) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 32763)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c5) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 9223372036854775803)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c6) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 3.4028234663852886e+38)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.query('select diff(c7) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 1.79769e+308)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(c8) from tb1;')

        tdSql.error('select diff(c9) from tb1;')

        tdSql.error('select diff(c10) from tb1;')

        tdSql.error('select diff(c11) from tb1;')

        tdSql.error('select diff(c12) from tb1;')

        tdSql.error('select diff(c13) from tb1;')

        tdSql.error('select diff(12345678900000000000000000) from tb1;')

        tdSql.error('select distinct diff(c4,1) from tb1;')

        tdSql.error('select diff(t1) from stb1;')

        tdSql.error('select diff(c4,1),avg(c3) from tb1;')

        tdSql.error('select diff(c4,1),top(c3,1) from tb1;')

        tdSql.error('select diff(c4,1) from tb1 session(ts, 1s);')

        tdSql.error('select diff(c4,1) from tb1 STATE_WINDOW(c4,1);')

        tdSql.error('select diff(c4,1) from tb1 interval(1s) sliding(1s) fill(NULL);')

        tdSql.error('select diff(c4,1) from stb1 group by t1;')

        tdSql.error('select diff(c4,1) from stb1 group by ts;')

        tdSql.error('select diff(c4,1) from stb1 group by c1;')

        tdSql.query('select diff(c4,1) from stb1 group by tbname;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 'tb1')
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 'tb1')
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(2, 2, 'tb1')
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 'tb1')
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 'tb2')
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(5, 1, 0)
        tdSql.checkData(5, 2, 'tb2')
        tdSql.checkData(6, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(6, 1, 2)
        tdSql.checkData(6, 2, 'tb2')
        tdSql.checkData(7, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(7, 2, 'tb2')
        tdSql.checkData(8, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(8, 2, 'tb2')
        tdSql.checkData(9, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(9, 1, 1)
        tdSql.checkData(9, 2, 'tb2')

        tdSql.error('select diff(c4,1) from tb1 order by c2;')

        tdSql.error('select diff(c8),diff(c9) from tbn;')

        tdSql.error('select diff(ts) from (select avg(c2) as a from stb1 interval(1s));')

        tdSql.query('select diff(a) from (select diff(c2) as a from tb1);')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 121)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, -121)

        tdSql.error('select diff("abc") from tb1;')

        tdSql.query('select diff(c4,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c4,0) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)

        tdSql.query('select diff(c6) from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 3.4028234663852886e+38)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, -inf)

        tdSql.error('select diff(11)+c2 from tb1;')

        tdSql.error('select diff(c4,1)+c2 from tb1;')

        tdSql.error('select diff(c2)+11 from tb1;')

        tdSql.error('select diff(c4,1),c1,c2 from tb1;')

        tdSql.query('select diff(c4,1),t1,ts,tbname,_C0,_c0 from tb1;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 4, 'tb1')
        tdSql.checkData(0, 5, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(0, 6, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 1)
        tdSql.checkData(1, 3, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 4, 'tb1')
        tdSql.checkData(1, 5, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 6, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 2147483643)
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 3, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 4, 'tb1')
        tdSql.checkData(2, 5, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 6, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(3, 2, 1)
        tdSql.checkData(3, 3, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 4, 'tb1')
        tdSql.checkData(3, 5, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 6, datetime.datetime(2021, 11, 11, 9, 0, 6))

        tdSql.error('select diff(c4,1),floor(c3) from tb1;')

        tdSql.error('select diff(c4,1),diff(c4,1) from tb1;')

        tdSql.query('select diff(c4,1) from tb1 where c2 is not null and c3 is not null;')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 2147483643)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, 2)

        tdSql.query('select diff(c2) from tb1 order by ts desc;')
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(0, 1, -2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, -123)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(2, 1, -2)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0))
        tdSql.checkData(3, 1, -1)

        tdSql.query('select diff(c4,1) from tb1 order by ts desc;')
        tdSql.checkRows(0)

        tdSql.query('select diff(c4,1) from tb1 order by ts desc limit 3 offset 2;')
        tdSql.checkRows(0)

        tdSql.error('select diff(c2) from stb1;')

        tdSql.error('select diff(c2) from stb1 order by ts desc;')

        tdSql.error('select diff(c4),t1 from stb1 order by ts desc;')

        tdSql.error('select diff(c3),tbname from stb1;')

        tdSql.error('select diff(c3),tbname from stb1 where t1 > 1;')

        tdSql.error('select diff(c8),diff(c9) from tbn;')

        tdSql.error('select diff(c8),diff(c9) from tbn order by ts desc;')

        tdSql.error('select diff(diff(c8)) from tbn;')

        tdSql.query('select diff(a) from (select avg(c2) as a from stb1 interval(1s));')
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.error('select diff(c2) from (select * from stb1);')

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null));")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by ts;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(0, 1, 1.0)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(1, 1, 0.5)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, 1.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(3, 1, 0.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(4, 1, 62.0)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(5, 1, -126.5)

        tdSql.query("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by ts desc;")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(0, 1, 126.5)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, -62.0)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(2, 1, -0.5)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(3, 1, -1.5)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, -0.5)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0))
        tdSql.checkData(5, 1, -1.0)

        tdSql.error("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by a desc;")

        tdSql.error("select diff(a) from (select avg(c2) as a from stb1 where ts >= '2021-11-11 09:00:00.000' and ts <= '2021-11-11 09:00:09.000' interval(1s) fill(null)) order by a;")

        tdSql.query('select diff(a) from (select diff(c2) as a from tb1);')
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(1, 1, 121)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(2, 1, -121)

        tdSql.error('select diff(tb1.c3),diff(tb2.c3) from tb1,tb2 where tb1.ts=tb2.ts;')

        tdSql.query('select diff(c3) from tb1 union all select diff(c3) from tb2;')
        tdSql.checkRows(10)
        tdSql.checkData(0, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(2, 1, 32763)
        tdSql.checkData(3, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(3, 1, 2)
        tdSql.checkData(4, 0, datetime.datetime(2021, 11, 11, 9, 0, 1))
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, datetime.datetime(2021, 11, 11, 9, 0, 2))
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 0, datetime.datetime(2021, 11, 11, 9, 0, 3))
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 0, datetime.datetime(2021, 11, 11, 9, 0, 4))
        tdSql.checkData(7, 1, 1)
        tdSql.checkData(8, 0, datetime.datetime(2021, 11, 11, 9, 0, 5))
        tdSql.checkData(8, 1, 1)
        tdSql.checkData(9, 0, datetime.datetime(2021, 11, 11, 9, 0, 6))
        tdSql.checkData(9, 1, 1)

        tdSql.error('select diff(stb1.c4),diff(stba.c5) from stb1,stba where stb1.t1=stba.t1 and stb1.ts=stba.ts;')

        tdSql.error('select diff(c4) as a from stb1 union all select diff(c5) as a from stba;')

        tdSql.error('select diff(c2) from stba;')

        tdSql.error('select diff(min(c2)) from tba1;')

        tdSql.error('select diff(max(c2)) from tba1;')

        tdSql.error('select diff(count(c2)) from tba1;')

        tdSql.error('select diff(sum(c2)) from tba1;')

        tdSql.error('select diff(avg(c2)) from tba1;')

        tdSql.error('select diff(percentile(c2, 10)) from tba1;')

        tdSql.error('select diff(apercentile(c2, 10)) from tba1;')

        tdSql.error('select diff(stddev(c2)) from tba1;')

        tdSql.error('select diff(spread(c2)) from tba1;')

        tdSql.error('select diff(twa(c2)) from tba1;')

        tdSql.error('select diff(leastsquares(c2, 1, 1)) from tba1;')

        tdSql.error('select diff(interp(c2)) from tba1 every(1s)')

        tdSql.error('select diff(interp(c2)) from stba every(1s) group by tbname;')

        tdSql.error('select diff(elapsed(ts)) from tba1;')

        tdSql.error('select diff(rate(c2)) from tba1;')

        tdSql.error('select diff(irate(c2)) from tba1;')

        tdSql.error('select diff(first(c2)) from tba1;')

        tdSql.error('select diff(last(c2)) from tba1;')

        tdSql.error('select diff(last_row(c2)) from tba1;')

        tdSql.error('select diff(top(c2, 1)) from tba1;')

        tdSql.error('select diff(bottom(c2, 1)) from tba1;')

        tdSql.error('select diff(leastsquares(c2, 1, 1)) from tba1;')

        tdSql.error('select diff(derivative(c2, 1s, 0)) from tba1;')

        tdSql.error('select diff(diff(c2)) from tba1;')

        tdSql.error('select diff(csum(c2)) from tba1;')

        tdSql.error('select diff(mavg(c2,2)) from tba1;')

        tdSql.error('select diff(sample(c2,2)) from tba1;')

        tdSql.error('select diff(_block_dist()) from tba1;')


        tdSql.execute('drop database diffneg')
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
