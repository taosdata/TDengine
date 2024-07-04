from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
import taos

class TDTestCase(TBase):
    """Add test case to cover the basic privilege test
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def prepare_data(self):
        # database
        tdSql.execute("create database db;")
        tdSql.execute("use db;")
        
        # create super table
        tdSql.execute("create table stb(ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        
        # create child table
        tdSql.execute("create table ct1 using stb tags(1, 'beijing');")
        # insert data to child table
        tdSql.execute("insert into ct1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        
        # create reguler table
        tdSql.execute("create table rt1(ts timestamp, col1 float, col2 int, col3 varchar(16));")
        # insert data to reguler table
        tdSql.execute("insert into rt1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")

    def create_user(self, user_name, passwd):
        tdSql.execute(f"create user {user_name} pass '{passwd}';")

    def delete_user(self, user_name):
        tdSql.execute(f"drop user {user_name};")

    def test_common_user_privileges(self):
        self.prepare_data()
        # create user
        self.create_user("test", "test")
        # check user 'test' privileges
        testconn = taos.connect(user='test', password='test')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        # create db privilege
        testSql.error("create database db1;", expectErrInfo="Insufficient privilege for operation")
        # modify db、super table、child table、reguler table privilege
        testSql.error("alter database db buffer 512;")
        testSql.error("alter stable db.stb add column col4 float;")
        testSql.error("alter stable db.stb drop column col3;")
        testSql.error("alter stable db.stb modify column col3 varchar(32);")
        testSql.error("alter stable db.stb add tag t3 int;")
        testSql.error("alter stable db.stb drop tag t2;")
        testSql.error("alter stable db.stb rename tag t2 t21;")
        testSql.error("alter table ct1 add column col4 double;")
        testSql.error("alter table ct1 drop column col3;")
        testSql.error("alter table ct1 modify column col3 varchar(32);")
        testSql.error("alter table ct1 rename column col3 col4;")
        testSql.error("alter table ct1 set tag t1=11;")
        testSql.error("alter table rt1 add column col4 double;")
        testSql.error("alter table rt1 drop column col3;")
        testSql.error("alter table rt1 modify column col3 varchar(32);")
        testSql.error("alter table rt1 rename column col3 col4;")
        # create super table、child table、reguler table privilege
        testSql.error("create table stb2 (ts timestamp, col1 int) tags(t1 int, t2 varchar(16));")
        testSql.error("create table ct2 using db.stb tags(2, 'shanghai');")
        testSql.error("create table rt2 (ts timestamp, col1 int);")
        # query super table、child table、reguler table privilege
        testSql.error("select * from db.stb;")
        testSql.error("select * from db.ct1;")
        testSql.error("select * from db.rt1;")
        # query system table privilege
        testSql.query("select * from information_schema.ins_users;")
        testSql.checkRows(2)
        # drop db privilege
        testSql.error("drop database db;", expectErrInfo="Insufficient privilege for operation")
        # delete user
        self.delete_user("test")
        # drop db
        tdSql.execute("drop database db;")
        tdLog.info("test_common_user_privileges successfully executed")

    def test_common_user_with_createdb_privileges(self):
        self.prepare_data()
        # create user
        self.create_user("test", "test")
        # check user 'test' privileges
        testconn = taos.connect(user='test', password='test')
        cursor = testconn.cursor()
        testSql = TDSql()
        testSql.init(cursor)
        # grant create db privilege
        tdSql.execute("alter user test createdb 1;")
        # check user 'test' create db、super table、child table、reguler table、view privileges
        testSql.execute("create database db1;")
        testSql.execute("use db1;")
        testSql.execute("create stable stb1 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        testSql.execute("create table ct1 using stb1 tags(1, 'beijing');")
        testSql.execute("insert into ct1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from stb1;")
        testSql.checkRows(2)
        testSql.query("select * from ct1;")
        testSql.checkRows(2)
        testSql.execute("create table rt1(ts timestamp, col1 float, col2 int, col3 varchar(16));")
        testSql.execute("insert into rt1 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from rt1;")
        testSql.checkRows(2)
        testSql.execute("create view v1 as select * from stb1;")
        testSql.query("select * from v1;")
        testSql.checkRows(2)
        # check user 'test' alter db、super table、child table、reguler table privileges
        testSql.execute("alter database db1 buffer 512;")
        testSql.execute("alter stable db1.stb1 add column col4 varchar(16);")
        testSql.execute("alter stable db1.stb1 drop column col3;")
        testSql.execute("alter stable db1.stb1 modify column col4 varchar(32);")
        testSql.execute("alter stable db1.stb1 add tag t3 binary(16);")
        testSql.execute("alter stable db1.stb1 drop tag t2;")
        testSql.execute("alter table ct1 set tag t1=11;")
        testSql.execute("alter table rt1 add column col4 varchar(16);")
        testSql.execute("alter table rt1 drop column col3;")
        testSql.execute("alter table rt1 modify column col4 varchar(32);")
        testSql.execute("alter table rt1 rename column col4 col5;")
        # check user 'test' query privileges
        testSql.query("select * from db1.stb1;")
        testSql.checkRows(2)
        testSql.query("select * from db1.ct1;")
        testSql.checkRows(2)
        testSql.query("select * from db1.rt1;")
        testSql.checkRows(2)

        # create another user 'test1'
        self.create_user("test1", "test1")
        test1conn = taos.connect(user='test1', password='test1')
        cursor1 = test1conn.cursor()
        test1Sql = TDSql()
        test1Sql.init(cursor1)
        # check user 'test' doesn't privilege to grant create db privilege to another user
        testSql.error("alter user test1 createdb 1;", expectErrInfo="Insufficient privilege for operation")
        testSql.error("grant all on db1.stb1 to test1;", expectErrInfo="Insufficient privilege for operation")

        # grant read、write privilege to user 'test' and check user 'test' privileges
        tdSql.execute("grant all on db.* to test;")
        testSql.query("select * from db.stb;")
        testSql.checkRows(2)
        testSql.query("select * from db.ct1;")
        testSql.checkRows(2)
        testSql.query("select * from db.rt1;")
        testSql.checkRows(2)
        testSql.execute("create stable db.stb2 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        testSql.execute("create table db.ct2 using db.stb2 tags(2, 'shenzhen');")
        testSql.execute("insert into db.ct2 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        testSql.query("select * from db.stb2;")
        testSql.checkRows(2)
        testSql.query("select * from db.ct2;")
        testSql.checkRows(2)
        # grant all privilege to user 'test1' and check user 'test1' privileges
        tdSql.execute("grant all on db1.* to test1;")
        test1Sql.query("select * from db1.stb1;")
        test1Sql.checkRows(2)
        test1Sql.query("select * from db1.ct1;")
        test1Sql.checkRows(2)
        test1Sql.query("select * from db1.rt1;")
        test1Sql.checkRows(2)
        test1Sql.execute("create stable db1.stb2 (ts timestamp, col1 float, col2 int, col3 varchar(16)) tags(t1 int, t2 binary(20));")
        test1Sql.execute("create table db1.ct2 using db1.stb2 tags(3, 'guangzhou');")
        test1Sql.execute("insert into db1.ct2 values(now, 9.1, 200, 'aa')(now+1s, 8.9, 198, 'bb');")
        test1Sql.query("select * from db1.stb2;")
        test1Sql.checkRows(2)
        
        # revoke create db privilege from user 'test'
        tdSql.execute("alter user test createdb 0;")
        testSql.error("create database db2;", expectErrInfo="Insufficient privilege for operation")
        # check other privileges of user 'test'
        testSql.query("select * from db.stb;")
        testSql.checkRows(2)
        testSql.query("select * from db1.stb1;")
        testSql.checkRows(2)
        tdSql.execute("alter user test createdb 1;")
        testSql.execute("create database db2;")
        testSql.query("show databases;")
        tdLog.info(testSql.res)
        testSql.checkRows(5)
        testSql.execute("drop view v1;")
        testSql.execute("drop table db1.rt1;")
        testSql.execute("drop table db1.ct1;")
        testSql.execute("drop table db1.stb1;")
        testSql.execute("drop database db1;")
        testSql.execute("drop database db2;")
        tdSql.execute("drop database db;")
        self.delete_user("test")
        self.delete_user("test1")

    def run(self):
        self.test_common_user_privileges()
        self.test_common_user_with_createdb_privileges()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
