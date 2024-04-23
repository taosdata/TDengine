# author : wenzhouwww
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

    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f" use {dbname} ")
        tdSql.execute(f" create stable {dbname}.{stb_name} (ts timestamp , c1 int , c2 bigint , c3 float , c4 double , c5 smallint , c6 tinyint , c7 bool , c8 binary(36) , c9 nchar(36) , uc1 int unsigned,\
            uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags(t1 timestamp , t2 int , t3 bigint , t4 float , t5 double , t6 smallint , t7 tinyint , t8 bool , t9 binary(36)\
                , t10 nchar(36) , t11 int unsigned , t12 bigint unsigned ,t13 smallint unsigned , t14 tinyint unsigned ) ")

        for i in range(tb_nums):
            tbname = f"{dbname}.sub_{stb_name}_{i}"
            ts = self.ts + i*10000
            tdSql.execute(f"create table {tbname} using {dbname}.{stb_name} tags ({ts} , {i} , {i}*10 ,{i}*1.0,{i}*1.0 , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )")

            for row in range(row_nums):
                ts = self.ts + row*1000
                tdSql.execute(f"insert into {tbname} values({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )")

            for null in range(5):
                ts =  self.ts + row_nums*1000 + null*1000
                tdSql.execute(f"insert into {tbname} values({ts} , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL )")

    def basic_query(self, dbname="db"):
        tdSql.query(f"select count(*) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums + 5 )*self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums )*self.tb_nums)
        tdSql.query(f"select tbname , count(*) from {dbname}.stb partition by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb group by t1 order by t1 ")
        tdSql.checkRows(self.tb_nums)
        tdSql.error(f"select count(c1) from {dbname}.stb group by c1 order by t1 ")
        tdSql.error(f"select count(t1) from {dbname}.stb group by c1 order by t1 ")
        tdSql.query(f"select count(c1) from {dbname}.stb group by tbname order by tbname ")
        tdSql.checkRows(self.tb_nums)
        # bug need fix
        # tdSql.query(f"select count(t1) from {dbname}.stb group by t2 order by t2 ")
        # tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select count(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select c1 , count(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select abs(c1+c3), count(c1+c3) from {dbname}.stb group by abs(c1+c3) order by abs(c1+c3)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select count(c1+c3)+max(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) ,abs(t1) from {dbname}.stb group by abs(c1) order by abs(t1)+c2")
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)+c2")
        tdSql.query(f"select abs(c1+c3)+abs(c2) , count(c1+c3)+count(c2) from {dbname}.stb group by abs(c1+c3)+abs(c2) order by abs(c1+c3)+abs(c2)")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) , count(t2) from {dbname}.stb where abs(c1+t2)=1 partition by tbname")
        tdSql.checkRows(10)
        tdSql.query(f"select count(c1) from {dbname}.stb where abs(c1+t2)=1 partition by tbname")
        tdSql.checkRows(10)

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums)

        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by t1 order by t1")
        tdSql.error(f"select tbname , count(t1) from {dbname}.stb partition by t1 order by t1")
        tdSql.error(f"select tbname , count(t1) from {dbname}.stb partition by t2 order by t2")

        # # bug need fix
        # tdSql.query(f"select t2 , count(t1) from {dbname}.stb partition by t2 order by t2")
        # tdSql.checkRows(self.tb_nums)

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums)


        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by t2 order by t2")

        tdSql.query(f"select c2, count(c1) from {dbname}.stb partition by c2 order by c2 desc")
        tdSql.checkRows(self.tb_nums+1)
        tdSql.checkData(0,1,self.tb_nums)

        tdSql.error(f"select tbname , count(c1) from {dbname}.stb partition by c1 order by c2")


        tdSql.query(f"select tbname , abs(t2) from {dbname}.stb partition by c2 order by t2")
        tdSql.checkRows(self.tb_nums*(self.row_nums+5))

        tdSql.query(f"select count(c1) , count(t2) from {dbname}.stb partition by c2 ")
        tdSql.checkRows(self.row_nums+1)
        tdSql.checkData(0,1,self.row_nums)

        tdSql.query(f"select count(c1) , count(t2) ,c2 from {dbname}.stb partition by c2 order by c2")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) , count(t1) ,max(c2) ,tbname  from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkCols(4)

        tdSql.query(f"select count(c1) , count(t2) ,t1  from {dbname}.stb partition by t1 order by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,self.row_nums)

        # bug need fix
        # tdSql.query(f"select count(c1) , count(t1) ,abs(c1) from {dbname}.stb partition by abs(c1) order by abs(c1)")
        # tdSql.checkRows(self.row_nums+1)


        tdSql.query(f"select count(ceil(c2)) , count(floor(t2)) ,count(floor(c2)) from {dbname}.stb partition by abs(c2) order by abs(c2)")
        tdSql.checkRows(self.row_nums+1)


        tdSql.query(f"select count(ceil(c1-2)) , count(floor(t2+1)) ,max(c2-c1) from {dbname}.stb partition by abs(floor(c1)) order by abs(floor(c1))")
        tdSql.checkRows(self.row_nums+1)


        # interval
        tdSql.query(f"select count(c1) from {dbname}.stb interval(2s) sliding(1s)")

        # bug need fix

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>="2022-07-06 16:00:00.000 " and ts < "2022-07-06 17:00:00.000 " interval(50s) sliding(30s) fill(NULL)')

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname interval(10s) slimit 5 soffset 1 ")

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname interval(10s)")

        tdSql.query(f"select tbname , count(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s)")
        tdSql.checkData(0,0,'sub_stb_1')
        tdSql.checkData(0,1,self.row_nums)

        # tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname slimit 5 soffset 0 ")
        # tdSql.checkRows(5)

        # tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname order by tbname slimit 5 soffset 1 ")
        # tdSql.checkRows(5)

        tdSql.query(f"select tbname , count(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s) sliding(5s) ")

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 partition by tbname interval(50s) sliding(30s)')
        tdSql.query(f'select max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 interval(50s) sliding(30s)')
        tdSql.query(f'select tbname , count(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+10000 partition by tbname interval(50s) sliding(30s)')


    def run(self):
        tdSql.prepare()
        self.prepare_datas("stb",self.tb_nums,self.row_nums)
        self.basic_query()
        dbname="db"

        # # coverage case for taosd crash about bug fix
        tdSql.query(f"select sum(c1) from {dbname}.stb where t2+10 >1 ")
        tdSql.query(f"select count(c1),count(t1) from {dbname}.stb where -t2<1 ")
        tdSql.query(f"select tbname ,max(ceil(c1)) from {dbname}.stb group by tbname ")
        tdSql.query(f"select avg(abs(c1)) , tbname from {dbname}.stb group by tbname ")
        tdSql.query(f"select t1,c1 from {dbname}.stb where abs(t2+c1)=1 ")


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())