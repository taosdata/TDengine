# author : bobliu
from util.log import *
from util.sql import *
from util.cases import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.row_nums = 10
        self.tb_nums = 10
        self.ts = 1537146000000
        self.dbname = "db"
        self.stable = "stb"

    def prepare_db(self):
        tdSql.execute(f" use {self.dbname} ")
        tdSql.execute(f" create stable {self.dbname}.{self.stable} (ts timestamp , c1 int , c2 bigint , c3 float , c4 double , c5 smallint , c6 tinyint , c7 bool , c8 binary(36) , c9 nchar(36) , uc1 int unsigned,\
            uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags(t1 timestamp , t2 int , t3 bigint , t4 float , t5 double , t6 smallint , t7 tinyint , t8 bool , t9 binary(36)\
                , t10 nchar(36) , t11 int unsigned , t12 bigint unsigned ,t13 smallint unsigned , t14 tinyint unsigned ) ")

        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {self.dbname}.{self.stable} tags ({ts} , {i} , {i}*10 ,{i}*1.0,{i}*1.0 , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )")

    def insert_db(self, tb_nums, row_nums):           
        for i in range(tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts_base = self.ts + i*10000
            for row in range(row_nums):
                ts = ts_base + row*1000
                tdSql.execute(f"insert into {tbname} values({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )")


    def test_groupby(self, check_num, real_num):
        # tbname
        tdSql.query(f"select count(*) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select count(*), sum(1) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} group by tbname order by count(*)")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} group by tbname having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} group by tbname having count(*) <= 0")
        tdSql.checkRows(check_num - real_num)

        # tag
        tdSql.query(f"select count(*) from {self.dbname}.{self.stable} group by t2 ")
        tdSql.checkRows(check_num)
    
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 ")
        tdSql.checkRows(check_num)

        # having
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 having count(*) <= 0")
        tdSql.checkRows(check_num - real_num)

        # col where filter nothing
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now group by t2 ")
        tdSql.checkRows(check_num)

        # col where filter all
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 group by t2 ")
        tdSql.checkRows(check_num)

        # col where filter part
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 group by t2 ")
        tdSql.checkRows(check_num)

        # col
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(check_num)

        #inner select
        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} group by tbname) ")
        tdSql.checkRows(check_num)

        # count + sum(col)
        tdSql.query(f"select count(*), sum(c1) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(check_num)

        ############### same with old ###############
        tdSql.query(f"select c1, count(*) from {self.dbname}.{self.stable} group by c1 ")
        num = 0
        if real_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)
    
        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} group by ts ")
        tdSql.checkRows(real_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} group by t2, c1 ")
        tdSql.checkRows(real_num * self.row_nums)

    
    def test_partitionby(self, check_num, real_num): 
        tdSql.query(f"select tbname , count(*) from {self.dbname}.{self.stable} partition by tbname ")
        tdSql.checkRows(check_num)
      
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} partition by tbname order by count(*)")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} partition by tbname having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname having count(*) <= 0")
        tdSql.checkRows(check_num - real_num)

        #tag
        tdSql.query(f"select count(*) from {self.dbname}.{self.stable} partition by t2 ")
        tdSql.checkRows(check_num)
    
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} partition by t2 ")
        tdSql.checkRows(check_num)

        # having
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} partition by t2 having count(*) <= 0")
        tdSql.checkRows(check_num - real_num)

        # col where filter nothing
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now partition by t2 ")
        tdSql.checkRows(check_num)

        # col where filter all
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 partition by t2 ")
        tdSql.checkRows(check_num)

        # col where filter part
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 partition by t2 ")
        tdSql.checkRows(check_num)

        #col
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname ")
        tdSql.checkRows(check_num)

        #inner select
        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} partition by tbname) ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname interval(1d)")
        tdSql.checkRows(real_num)

        ############### same with old ###############
        tdSql.query(f"select c1, count(*) from {self.dbname}.{self.stable} partition by c1 ")
        num = 0
        if real_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)
    
        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} partition by ts ")
        tdSql.checkRows(real_num * self.row_nums)

        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} partition by t2, c1 ")
        tdSql.checkRows(real_num * self.row_nums)

    def test_error(self):
        tdSql.error(f"select * from {self.dbname}.{self.stable} group by t2")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 where t2 = 1")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 interval(1d)")

  
    def run(self):
        tdSql.prepare()
        self.prepare_db()
        check_num = self.tb_nums
        self.test_groupby(check_num, 0)
        self.test_partitionby(check_num, 0)
        # insert into half of tables
        real_num = 5
        self.insert_db(real_num, self.row_nums)
        self.test_groupby(check_num, real_num)
        self.test_partitionby(check_num, real_num)

        # test old version before changed
        # self.test_groupby(0, 0)
        # self.test_partitionby(0, 0)
        # self.insert_db(5, self.row_nums)
        # self.test_groupby(5, 5)
        # self.test_partitionby(5, 5)

        self.test_error()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
