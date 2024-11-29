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
            tdSql.execute(f"create table {tbname} using {self.dbname}.{self.stable} tags ({ts} , {i} , %d ,%d , %f , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )"%(i*10,i*1.0,i*1.0))

    def insert_db(self, tb_nums, row_nums):           
        for i in range(tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts_base = self.ts + i*10000
            for row in range(row_nums):
                ts = ts_base + row*1000
                tdSql.execute(f"insert into {tbname} values({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )")


    def test_groupby(self, keyword, check_num, nonempty_tb_num):
        ####### by tbname
        tdSql.query(f"select count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by tbname ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by tbname order by count(*)")
        tdSql.checkRows(check_num)

        # last
        tdSql.query(f"select last(ts), count(*) from {self.dbname}.{self.stable} {keyword} by tbname order by last(ts)")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by tbname having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by tbname having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} {keyword} by t2 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by t2 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by t2 ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1 ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1 having c1 >= 0")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)
    
        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} {keyword} by ts ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, t3, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

    def test_groupby_position(self, keyword, check_num, nonempty_tb_num):
        ####### by tbname
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by 1 order by count(*)")
        tdSql.checkRows(check_num)

        # last
        tdSql.query(f"select tbname from {self.dbname}.{self.stable} {keyword} by 1 having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} {keyword} by 1 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} {keyword} by 1 having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by 1 ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by 1 ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by 1 ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select ts, count(*) from {self.dbname}.{self.stable} {keyword} by 1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2, t3, c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1, t3, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select sum(t0.sumc2) from (select c1, sum(c2) as sumc2 from {self.dbname}.{self.stable} {keyword} by 1) t0")
        num = 0
        if nonempty_tb_num > 0:
            num = 1
        tdSql.checkRows(num)

        tdSql.query(f"select abs(c1), count(*) from {self.dbname}.{self.stable} {keyword} by 1")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        ####### error case
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 10")
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 0")
        tdSql.error(f"select c1, c2, count(*) from {self.dbname}.{self.stable} {keyword} by 0, 1")
        tdSql.error(f"select c1, count(*) from {self.dbname}.{self.stable} {keyword} by 1.2")
        tdSql.error(f"select c1, c2, c3, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2.2, 3")
        tdSql.error(f"select c1, c2, count(*) from {self.dbname}.{self.stable} {keyword} by 1")
        tdSql.error(f"select c1, avg(c2), count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2")

    def test_groupby_alias(self, keyword, check_num, nonempty_tb_num):
        tdSql.query(f"select t1 as t1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t1_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t1 as t1_alias from {self.dbname}.{self.stable} {keyword} by t1_alias order by count(*)")
        tdSql.checkRows(check_num)

        # last
        tdSql.query(f"select t1 as t1_alias from {self.dbname}.{self.stable} {keyword} by t1_alias having count(*)>=0")
        tdSql.checkRows(check_num)

        # having filter out empty
        tdSql.query(f"select t1 as t1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t1_alias having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        ####### by tag
        tdSql.query(f"select t2 as t2_alias, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias having count(*) <= 0")
        tdSql.checkRows(check_num - nonempty_tb_num)

        # where
        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where ts < now {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where ts > 1737146000000 {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2 as t2_alias, count(*) from {self.dbname}.{self.stable} where c1 = 1 {keyword} by t2_alias ")
        tdSql.checkRows(check_num)

        ####### by col
        tdSql.query(f"select c1 as c1_alias, count(*), count(1), count(c1) from {self.dbname}.{self.stable} {keyword} by c1_alias ")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        tdSql.query(f"select ts as ts_alias, count(*) from {self.dbname}.{self.stable} {keyword} by ts_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # col + tag
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2, 2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias, t3_alias, 3 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2, t3_alias, c1_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        tdSql.query(f"select t2 as t2_alias, t3 as t3_alias, c1 as c1_alias, count(*) from {self.dbname}.{self.stable} {keyword} by t2_alias, t3, c1_alias ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        tdSql.query(f"select sum(t0.sumc2) from (select c1 as c1_alias, sum(c2) as sumc2 from {self.dbname}.{self.stable} {keyword} by c1_alias) t0")
        num = 0
        if nonempty_tb_num > 0:
            num = 1
        tdSql.checkRows(num)

        tdSql.query(f"select abs(c1) as abs_alias, count(*) from {self.dbname}.{self.stable} {keyword} by abs_alias")
        num = 0
        if nonempty_tb_num > 0:
            num = self.row_nums
        tdSql.checkRows(num)

        ####### error case
        tdSql.error(f"select c1, avg(c2) as avg_alias, count(*) from {self.dbname}.{self.stable} {keyword} by 1, avg_alias")

    def test_groupby_sub_table(self):
        for i in range(self.tb_nums):
            tbname = f"{self.dbname}.sub_{self.stable}_{i}"
            ts = self.ts + i*10000
            tdSql.query(f"select t1, t2, t3,count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select cast(t2 as binary(12)),count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i)

            tdSql.query(f"select t2 + 1, count(*) from {tbname}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, i + 1)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} group by tbname, c1, t4")
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} partition by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

            tdSql.query(f"select t1, t2, t3, count(*) from {tbname} partition by c1, tbname")
            tdSql.checkData(0, 1, i)
            tdSql.checkData(0, 2, i*10)

        tdSql.query(f"select t1, t2, t3, count(*) from {self.dbname}.{self.stable} partition by c1, tbname order by tbname desc")
        tdSql.checkRows(50)
        tdSql.checkData(0, 1, 4)
        tdSql.checkData(0, 2, 40)


    def test_multi_group_key(self, check_num, nonempty_tb_num):
        # multi tag/tbname
        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} group by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select cast(t2 as binary(12)), count(*) from {self.dbname}.{self.stable} group by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} partition by t2, t3, tbname")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} group by tbname order by tbname asc")
        tdSql.checkRows(check_num)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(3, 1, 30)

        tdSql.query(f"select t2, t3, tbname, count(*) from {self.dbname}.{self.stable} partition by tbname order by tbname asc")
        tdSql.checkRows(check_num)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(2, 1, 20)
        tdSql.checkData(3, 1, 30)

        # multi tag + col
        tdSql.query(f"select t1, t2, c1, count(*) from {self.dbname}.{self.stable} partition by t1, t2, c1 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)

        # tag + multi col
        tdSql.query(f"select t2, c1, c2, count(*) from {self.dbname}.{self.stable} partition by t2, c1, c2 ")
        tdSql.checkRows(nonempty_tb_num * self.row_nums)


    def test_multi_agg(self, all_tb_num, nonempty_tb_num):
        tdSql.query(f"select count(*), sum(c1) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f"select count(1), sum(1), avg(c1), apercentile(c1, 50), spread(ts) from {self.dbname}.{self.stable} group by tbname ")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f"select count(c1), sum(c1), min(c1), mode(c1), stddev(c1), spread(c1) from {self.dbname}.{self.stable} partition by tbname ")
        tdSql.checkRows(all_tb_num)

        # elapsed: continuous duration in a statistical period, table merge scan
        tdSql.query(f" select count(c1), max(c5), last_row(c5), elapsed(ts), spread(c1) from {self.dbname}.{self.stable} group by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select count(c1), min(c1), avg(c1), elapsed(ts), mode(c1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select count(c1), elapsed(ts), twa(c1), irate(c1), leastsquares(c1,1,1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(all_tb_num)

        tdSql.query(f" select avg(c1), elapsed(ts), twa(c1), irate(c1) from {self.dbname}.{self.stable} partition by tbname")
        tdSql.checkRows(nonempty_tb_num)

        # if nonempty_tb_num > 0:
        #   tdSql.query(f" select avg(c1), percentile(c1, 50) from {self.dbname}.sub_{self.stable}_1")
        #   tdSql.checkRows(1)

    def test_innerSelect(self, check_num):
        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} group by tbname) ")
        tdSql.checkRows(check_num)   

        tdSql.query(f"select * from (select count(c1) from {self.dbname}.{self.stable} partition by tbname) ")
        tdSql.checkRows(check_num)

        tdSql.query(f"select t1, c from (select t1, sum(c1) as s, count(*) as c from {self.dbname}.{self.stable} partition by t1)")
        tdSql.checkRows(check_num)


    def test_window(self, nonempty_tb_num):
        # empty group optimization condition is not met
        # time window
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname interval(1d)")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select _wstart, _wend, count(c1), max(c1), apercentile(c1, 50) from {self.dbname}.{self.stable} partition by tbname interval(1d)")
        tdSql.checkRows(nonempty_tb_num)

        # state window
        sql1 = f"select tbname, c1, count(*) from {self.dbname}.{self.stable} partition by tbname state_window(c1) order by tbname, c1"
        sql2 = f"select tbname, c1 from {self.dbname}.{self.stable} partition by tbname state_window(c1) order by tbname, c1"
        tdSql.query(sql1)
        tdSql.checkRows(nonempty_tb_num * self.row_nums)
        
        tdSql.check_query_col_data(sql1, sql2, 0)
        tdSql.check_query_col_data(sql1, sql2, 1)

        # session window
        tdSql.query(f"select count(c1) from {self.dbname}.{self.stable} partition by tbname session(ts, 5s)")
        tdSql.checkRows(nonempty_tb_num)
        sql1 = f"select tbname, _wstart, count(c1) from {self.dbname}.{self.stable} partition by tbname session(ts, 5s) order by tbname, _wstart"
        sql2 = f"select tbname, _wstart from {self.dbname}.{self.stable} partition by tbname session(ts, 5s) order by tbname, _wstart"
        tdSql.check_query_col_data(sql1, sql2, 0)
        tdSql.check_query_col_data(sql1, sql2, 1)

        # event window
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9;")
        tdSql.checkRows(nonempty_tb_num)
        tdSql.query(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9;")
        tdSql.checkRows(nonempty_tb_num)
        sql1 = f"select tbname, _wstart, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 order by tbname, _wstart"
        sql2 = f"select tbname, _wstart  from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 order by tbname, _wstart"
        tdSql.check_query_col_data(sql1, sql2, 0)
        tdSql.check_query_col_data(sql1, sql2, 1)

    def test_event_window(self, nonempty_tb_num):
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and 1=1;")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and 1=0;")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and tbname='sub_{self.stable}_0';")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and t2=0;")
        tdSql.checkRows(1)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and t2=0 having count(*) > 10;")
        tdSql.checkRows(0)
        tdSql.query(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and t2=0 having count(*) > 10;")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _rowts>0;")
        tdSql.checkRows(nonempty_tb_num)

        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart>0;")
        tdSql.checkRows(0)
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart<0;")
        tdSql.checkRows(0)
        tdSql.query(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _qstart<_qend;")
        tdSql.checkRows(0)

        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart<q_start;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart - q_start > 0;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _irowts>0;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 and _wduration > 5s end with c2 = 9;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart > 1299845454;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wduration + 1s > 5s;")
        tdSql.error(f"select tbname, count(*) from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and count(*) > 10;")
        
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart<q_start;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart - q_start > 0;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _irowts>0;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 and _wduration > 5s end with c2 = 9;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wstart > 1299845454;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and _wduration + 1s > 5s;")
        tdSql.error(f"select tbname from {self.dbname}.{self.stable} partition by tbname event_window start with c1 >= 0 end with c2 = 9 and count(*) > 10;")

    def test_error(self):
        tdSql.error(f"select * from {self.dbname}.{self.stable} group by t2")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 where t2 = 1")
        tdSql.error(f"select t2, count(*) from {self.dbname}.{self.stable} group by t2 interval(1d)")

    def test_TS5567(self):
        tdSql.query(f"select const_col from (select 1 as const_col from {self.dbname}.{self.stable}) t group by const_col")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col from {self.dbname}.{self.stable}) t partition by const_col")
        tdSql.checkRows(50)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) group by const_col")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) partition by const_col")
        tdSql.checkRows(10)
        tdSql.query(f"select const_col as c_c from (select 1 as const_col from {self.dbname}.{self.stable}) t group by c_c")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col as c_c from (select 1 as const_col from {self.dbname}.{self.stable}) t partition by c_c")
        tdSql.checkRows(50)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) group by 1")
        tdSql.checkRows(1)
        tdSql.query(f"select const_col from (select 1 as const_col, count(c1) from {self.dbname}.{self.stable} t group by c1) partition by 1")
        tdSql.checkRows(10)
    
    def test_TD_32883(self):
        sql = "select avg(c1), t9 from db.stb group by t9,t9, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), t10 from db.stb group by t10,t10, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), t10 from db.stb partition by t10,t10, tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)
        sql = "select avg(c1), concat(t9,t10) from db.stb group by concat(t9,t10), concat(t9,t10),tbname"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(5)

    def run(self):
        tdSql.prepare()
        self.prepare_db()
        # empty table only
        self.test_groupby('group', self.tb_nums, 0)
        self.test_groupby('partition', self.tb_nums, 0)
        self.test_groupby_position('group', self.tb_nums, 0)
        self.test_groupby_position('partition', self.tb_nums, 0)
        self.test_groupby_alias('group', self.tb_nums, 0)
        self.test_groupby_alias('partition', self.tb_nums, 0)
        self.test_innerSelect(self.tb_nums)
        self.test_multi_group_key(self.tb_nums, 0)
        self.test_multi_agg(self.tb_nums, 0)
        self.test_window(0)

        # insert data to 5 tables
        nonempty_tb_num = 5
        self.insert_db(nonempty_tb_num, self.row_nums)

        self.test_groupby('group', self.tb_nums, nonempty_tb_num)
        self.test_groupby('partition', self.tb_nums, nonempty_tb_num)
        self.test_groupby_position('group', self.tb_nums, nonempty_tb_num)
        self.test_groupby_position('partition', self.tb_nums, nonempty_tb_num)
        self.test_groupby_alias('group', self.tb_nums, nonempty_tb_num)
        self.test_groupby_alias('partition', self.tb_nums, nonempty_tb_num)
        self.test_groupby_sub_table()
        self.test_innerSelect(self.tb_nums)
        self.test_multi_group_key(self.tb_nums, nonempty_tb_num)
        self.test_multi_agg(self.tb_nums, nonempty_tb_num)
        self.test_window(nonempty_tb_num)
        self.test_event_window(nonempty_tb_num)

        self.test_TS5567()
        self.test_TD_32883()

        ## test old version before changed
        # self.test_groupby('group', 0, 0)
        # self.insert_db(5, self.row_nums)
        # self.test_groupby('group', 5, 5)

        self.test_error()


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
