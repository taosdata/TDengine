import sys 

sys.path.append("../tests/pytest")
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf
import taos


class TestInsertStb:
    def caseDescription(self):
        '''
        case1<shenglian zhou>: [TS-3932] insert into stb 
        ''' 
        return
    
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.conn = conn
        
        
    def restartTaosd(self, index=1, dbname="db"):
        tdDnodes.stop(index)
        tdDnodes.startWithoutSleep(index)
        tdSql.execute(f"use insert_stb")


    def test_normal(self):
        """测试超级表基本插入操作

        插入语句使用using、dbname.stable、tbname等关键字插入数据，插入单表数据，单表多条数据，多表多条数据，带标签插入等语句，执行成功

        Since: v3.3.0.0

        Labels: stable

        Jira: TD-12345,TS-1234

        History:
            - 2024-2-6 Feng Chao Created

        """

        print("running {}".format("normal"))
        tdSql.execute("drop database if exists insert_stb")
        tdSql.execute("create database if not exists insert_stb")
        tdSql.execute('use insert_stb')
        tdSql.execute('create database d1')

        tdSql.execute('create database d2')

        tdSql.execute('use d1;')

        tdSql.execute('create table st(ts timestamp, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute("insert into ct1 values('2021-04-19 00:00:02', 2);")

        tdSql.execute('use d2;')

        tdSql.execute('create table st(ts timestamp, f int) tags(t int);')

        tdSql.execute("insert into ct1 using st tags(1) values('2021-04-19 00:00:00', 1);")

        tdSql.execute("insert into ct2 using st tags(2) values('2021-04-19 00:00:01', 2);")

        tdSql.execute('create database db1 vgroups 1;')

        tdSql.execute('create table db1.stb (ts timestamp, c1 int, c2 int) tags(t1 int, t2 int);')

        tdSql.execute('use d1;')

        tdSql.execute("insert into st (tbname, ts, f, t) values('ct3', '2021-04-19 08:00:03', 3, 3);")

        tdSql.execute("insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:04', 6);")

        tdSql.execute("insert into d1.st (tbname, ts, f) values('ct6', '2021-04-19 08:00:05', 7)('ct8', '2021-04-19 08:00:06', 8);")

        tdSql.execute("insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:07', 9, 9)('ct8', '2021-04-19 08:00:08', 10, 10);")

        tdSql.execute("insert into d1.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:09', 9, 9)('ct8', '2021-04-19 08:00:10', 10, 10) d2.st (tbname, ts, f, t) values('ct6', '2021-04-19 08:00:11', 9, 9)('ct8', '2021-04-19 08:00:12', 10, 10);")

        tdSql.query('select * from d1.st order by ts;')
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0))

    
    def test_insert_stb(self):
        """测试超级表插入各种数据类型

        使用多种数据类型创建超级表，向超级表插入数据，包括常规数据，空数据，边界值等，插入均执行成功

        Since: v3.3.0.0

        Labels: stable, dataType

        Jira: TD-12345,TS-1234

        History:
            2024-2-6 Feng Chao Created

        """

        print("running {}".format('insert_stb'))
        self.conn.select_db('insert_stb')
        tdSql.execute('create table stb1 (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(10), c9 nchar(10), c10 tinyint unsigned, c11 smallint unsigned, c12 int unsigned, c13 bigint unsigned) TAGS(t1 int, t2 binary(10), t3 double);')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:00\',true,1,1,1,1,1,1,"123","1234",1,1,1,1, 1, \'1\', 1.0, \'tb1\');')

        tdSql.execute("insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values ('2021-11-11 09:00:01',true,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL, 2, '2', 2.0, 'tb1');")

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:02\',true,2,NULL,2,NULL,2,NULL,"234",NULL,2,NULL,2,NULL, 2, \'2\', 2.0, \'tb2\');')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:03\',false,NULL,3,NULL,3,NULL,3,NULL,"3456",NULL,3,NULL,3, 3, \'3\', 3.0, \'tb3\');')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:04\',true,4,4,4,4,4,4,"456","4567",4,4,4,4, 4, \'4.0\', 4.0, \'tb4\');')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:05\',true,127,32767,2147483647,9223372036854775807,3.402823466e+38,1.79769e+308,"567","5678",254,65534,4294967294,9223372036854775807, 5, \'5\', 5, \'max\' );')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,t1,t2,t3,tbname) values (\'2021-11-11 09:00:06\',true,-127,-32767,-2147483647,-9223372036854775807,-3.402823466e+38,-1.79769e+308,"678","6789",0,0,0,0, 6, \'6\', 6, \'min\');')

        tdSql.execute('insert into stb1(ts,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,tbname,t1,t2,t3) values (\'2021-11-11 09:00:07\',true,-127,-32767,-2147483647,-9223372036854775807,-3.402823466e+38,-1.79769e+308,"678","6789",0,0,0,0, \'min\', 6, \'6\', 6);')

        tdSql.query('select tbname,* from stb1 order by ts;')
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, 'tb1')
        tdSql.checkData(0, 1, datetime.datetime(2021, 11, 11, 9, 0))
        tdSql.checkData(0, 2, True)

    def test_stmt_error(self):
        """测试超级表插入stmt数据失败

        创建参数绑定对象，tag设置为填充对象，绑定参数后插入预期失败

        Since: v3.3.0.0

        Labels: stable, negative

        Jira: TD-12345,TS-1234

        History:
            - 2024-2-6 Feng Chao Created

        """
        conn = self.conn
        conn.select_db('insert_stb')
        conn.execute('create table stb9(ts timestamp, f int) tags (t int)')
        try:
            stmt = conn.statement("insert into stb9(tbname, f, t) values('ctb91', 1, ?)")
            params = taos.new_bind_params(1)
            params[0].int(1)
            stmt.bind_param(params)
            stmt.execute()
            result = stmt.use_result()
        except Exception as err:
            print(str(err))    
        
    def test_consecutive_seq(self):
        """测试超级表连续插入

        向超级表连续插入多条数据，插入均执行成功

        Since: v3.3.0.0

        Labels: stable

        Jira: TD-12345,TS-1234

        History:
            - 2024-2-6 Feng Chao Created

        """
        print("running {}".format("consecutive_seq"))
        tdSql.execute("drop database if exists insert_stb3")
        tdSql.execute("create database if not exists insert_stb3")
        tdSql.execute('use insert_stb3')
        tdSql.execute('create table st (ts timestamp, ti tinyint, si smallint, i int, bi bigint, f float, d double, b binary(10)) tags(t1 int, t2 float, t3 binary(10))')

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct0', 0, 0.000000, 'childtable', 1546300800000, 0, 0, 0, 0, 0.000000, 0.000000, 'hello') ('ct0', 0, 0.000000, 'childtable', 1546300800001, 1, 1, 1, 1, 1.000000, 2.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct1', 1, 1.000000, 'childtable', 1546301800000, 64, 16960, 1000000, 1000000, 1000000.000000, 2000000.000000, 'hello') ('ct1', 1, 1.000000, 'childtable', 1546301800001, 65, 16961, 1000001, 1000001, 1000001.000000, 2000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct2', 2, 2.000000, 'childtable', 1546302800000, -128, -31616, 2000000, 2000000, 2000000.000000, 4000000.000000, 'hello') ('ct2', 2, 2.000000, 'childtable', 1546302800001, -127, -31615, 2000001, 2000001, 2000001.000000, 4000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct3', 3, 3.000000, 'childtable', 1546303800000, -64, -14656, 3000000, 3000000, 3000000.000000, 6000000.000000, 'hello') ('ct3', 3, 3.000000, 'childtable', 1546303800001, -63, -14655, 3000001, 3000001, 3000001.000000, 6000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct4', 4, 4.000000, 'childtable', 1546304800000, 0, 2304, 4000000, 4000000, 4000000.000000, 8000000.000000, 'hello') ('ct4', 4, 4.000000, 'childtable', 1546304800001, 1, 2305, 4000001, 4000001, 4000001.000000, 8000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct5', 5, 5.000000, 'childtable', 1546305800000, 64, 19264, 5000000, 5000000, 5000000.000000, 10000000.000000, 'hello') ('ct5', 5, 5.000000, 'childtable', 1546305800001, 65, 19265, 5000001, 5000001, 5000001.000000, 10000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct6', 6, 6.000000, 'childtable', 1546306800000, -128, -29312, 6000000, 6000000, 6000000.000000, 12000000.000000, 'hello') ('ct6', 6, 6.000000, 'childtable', 1546306800001, -127, -29311, 6000001, 6000001, 6000001.000000, 12000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct7', 7, 7.000000, 'childtable', 1546307800000, -64, -12352, 7000000, 7000000, 7000000.000000, 14000000.000000, 'hello') ('ct7', 7, 7.000000, 'childtable', 1546307800001, -63, -12351, 7000001, 7000001, 7000001.000000, 14000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct8', 8, 8.000000, 'childtable', 1546308800000, 0, 4608, 8000000, 8000000, 8000000.000000, 16000000.000000, 'hello') ('ct8', 8, 8.000000, 'childtable', 1546308800001, 1, 4609, 8000001, 8000001, 8000001.000000, 16000002.000000, 'hello')")

        tdSql.execute("insert into st(tbname, t1, t2, t3, ts, ti, si, i, bi, f, d, b) values ('ct9', 9, 9.000000, 'childtable', 1546309800000, 64, 21568, 9000000, 9000000, 9000000.000000, 18000000.000000, 'hello') ('ct9', 9, 9.000000, 'childtable', 1546309800001, 65, 21569, 9000001, 9000001, 9000001.000000, 18000002.000000, 'hello')")

        tdSql.query('select * from st order by ts')
        tdSql.checkRows(20)
        tdSql.checkData(0, 0, datetime.datetime(2019, 1, 1, 8, 0))

        tdSql.execute('drop database insert_stb3')
        
    def run(self):
        self.test_normal()
        self.test_insert_stb()
        self.test_stmt_error()
        self.test_consecutive_seq()
        
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TestInsertStb())
tdCases.addLinux(__file__, TestInsertStb())
