from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *


class TDTestCase(TBase):
    """Add test case to verify TD-30816 (last/last_row accuracy)
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

    def prepare_data(self):
        tdSql.execute("create database db_td30816 cachemodel 'both';")
        tdSql.execute("use db_td30816;")
        # create regular table
        tdSql.execute("create table rt_int (ts timestamp, c1 int primary key, c2 int);")
        tdSql.execute("create table rt_str (ts timestamp, c1 varchar(16) primary key, c2 varchar(16));")

        # create stable
        tdSql.execute("create table st_pk_int (ts timestamp, c1 int primary key, c2 int) tags (t1 int);")
        tdSql.execute("create table st_pk_str (ts timestamp, c1 varchar(16) primary key, c2 varchar(16)) tags (t1 int);")

        # create child table
        tdSql.execute("create table ct1 using st_pk_int tags(1);")
        tdSql.execute("create table ct2 using st_pk_int tags(2);")

        tdSql.execute("create table ct3 using st_pk_str tags(3);")
        tdSql.execute("create table ct4 using st_pk_str tags(4);")

        # insert data to regular table
        tdSql.execute("insert into rt_int values ('2021-01-01 00:00:00', 1, NULL);")
        tdSql.execute("insert into rt_int values ('2021-01-01 00:00:01', 2, 1);")
        tdSql.execute("insert into rt_str values ('2021-01-01 00:00:00', 'a', NULL);")
        tdSql.execute("insert into rt_str values ('2021-01-01 00:00:01', 'b', '1');")

        # insert data to child table
        tdSql.execute("insert into ct1 values ('2021-01-01 00:00:00', 1, 1);")
        tdSql.execute("insert into ct1 values ('2021-01-01 00:00:01', 2, NULL);")
        tdSql.execute("insert into ct2 values ('2021-01-01 00:00:00', 3, 3);")
        tdSql.execute("insert into ct2 values ('2021-01-01 00:00:01', 4, NULL);")

        tdSql.execute("insert into ct3 values ('2021-01-01 00:00:00', 'a', '1');")
        tdSql.execute("insert into ct3 values ('2021-01-01 00:00:01', 'b', NULL);")
        tdSql.execute("insert into ct4 values ('2021-01-01 00:00:00', 'c', '3');")
        tdSql.execute("insert into ct4 values ('2021-01-01 00:00:01', 'd', NULL);")

    def test_last_with_primarykey_int_ct(self):
        tdSql.execute("use db_td30816;")
        tdSql.query("select last(*) from st_pk_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 3)
        
        tdSql.query("select last_row(*) from st_pk_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, None)

        # delete and insert data
        tdSql.execute("delete from ct1 where ts='2021-01-01 00:00:01';")
        tdSql.execute("delete from ct2 where ts='2021-01-01 00:00:01';")
        tdSql.execute("insert into ct1 values ('2021-01-01 00:00:00', 0, 5);")
        tdSql.execute("insert into ct2 values ('2021-01-01 00:00:00', -1, 6);")
        tdSql.query("select last(*) from st_pk_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        
        tdSql.query("select last_row(*) from st_pk_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)
        tdLog.info("Finish test_last_with_primarykey_int_ct")
        
    def test_last_with_primarykey_str_ct(self):
        tdSql.execute("use db_td30816;")
        tdSql.query("select last(*) from st_pk_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 'd')
        tdSql.checkData(0, 2, '3')
        
        tdSql.query("select last_row(*) from st_pk_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 'd')
        tdSql.checkData(0, 2, None)
        
        # delete and insert data
        tdSql.execute("delete from ct3 where ts='2021-01-01 00:00:01';")
        tdSql.execute("delete from ct4 where ts='2021-01-01 00:00:01';")
        tdSql.execute("insert into ct3 values ('2021-01-01 00:00:00', '6', '5');")
        tdSql.execute("insert into ct4 values ('2021-01-01 00:00:00', '7', '6');")
        
        tdSql.query("select last(*) from st_pk_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 'c')
        tdSql.checkData(0, 2, '3')

        tdSql.query("select last_row(*) from st_pk_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 'c')
        tdSql.checkData(0, 2, 3)
        tdLog.info("Finish test_last_with_primarykey_str_ct")

    def test_last_with_primarykey_int_rt(self):
        tdSql.execute("use db_td30816;")
        tdSql.query("select last(*) from rt_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select last_row(*) from rt_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 1)

        tdSql.execute("delete from rt_int where ts='2021-01-01 00:00:01';")
        tdSql.execute("insert into rt_int values ('2021-01-01 00:00:00', 0, 5);")

        tdSql.query("select last(*) from rt_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 5)

        tdSql.query("select last_row(*) from rt_int;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, None)
        tdLog.info("Finish test_last_with_primarykey_int_rt")

    def test_last_with_primarykey_str_rt(self):
        tdSql.execute("use db_td30816;")
        tdSql.query("select last(*) from rt_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 'b')
        tdSql.checkData(0, 2, '1')
        
        tdSql.query("select last_row(*) from rt_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:01')
        tdSql.checkData(0, 1, 'b')
        tdSql.checkData(0, 2, '1')
        
        tdSql.execute("delete from rt_str where ts='2021-01-01 00:00:01';")
        tdSql.execute("insert into rt_str values ('2021-01-01 00:00:00', '2', '5');")

        tdSql.query("select last(*) from rt_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, '5')

        tdSql.query("select last_row(*) from rt_str;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2021-01-01 00:00:00')
        tdSql.checkData(0, 1, 'a')
        tdSql.checkData(0, 2, None)
        tdLog.info("Finish test_last_with_primarykey_str_rt")

    def run(self):
        self.prepare_data()
        # regular table
        self.test_last_with_primarykey_int_rt()
        self.test_last_with_primarykey_str_rt()
        # child tables
        self.test_last_with_primarykey_int_ct()
        self.test_last_with_primarykey_str_ct()

    def stop(self):
        tdSql.execute("drop database db_td30816;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
