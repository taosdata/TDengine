from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestPartitonBy:

    def setup_class(cls):
        cls.row_nums = 10
        cls.tb_nums = 10
        cls.ts = 1537146000000

    def do_partitionby(self):
        dbPrefix = "db"
        tbPrefix1 = "tba"
        tbPrefix2 = "tbb"
        mtPrefix = "stb"
        tbNum = 10
        rowNum = 2

        tdLog.info(f"=============== step1")
        i = 0
        db = dbPrefix + str(i)
        mt1 = mtPrefix + str(i)
        i = 1
        mt2 = mtPrefix + str(i)

        tdSql.execute(f"create database {db} vgroups 3")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt1} (ts timestamp, f1 int) TAGS(tag1 int, tag2 binary(500))"
        )
        sql = "create table "
        sql += f" tb0 using {mt1} tags(0, 'a')"
        sql += f" tb1 using {mt1} tags(1, 'b')"
        sql += f" tb2 using {mt1} tags(2, 'a')"
        sql += f" tb3 using {mt1} tags(3, 'a')"
        sql += f" tb4 using {mt1} tags(4, 'b')"
        sql += f" tb5 using {mt1} tags(5, 'a')"
        sql += f" tb6 using {mt1} tags(6, 'b')"
        sql += f" tb7 using {mt1} tags(7, 'b')"
        tdSql.execute(sql)

        tdSql.query(f"select * from {mt1} partition by tag1,tag2 limit 1")
        tdSql.checkRows(0)

        sql = "insert into "
        sql += " tb0 values ('2022-04-26 15:15:08', 1)"
        sql += " tb1 values ('2022-04-26 15:15:07', 2)"
        sql += " tb2 values ('2022-04-26 15:15:06', 3)"
        sql += " tb3 values ('2022-04-26 15:15:05', 4)"
        sql += " tb4 values ('2022-04-26 15:15:04', 5)"
        sql += " tb5 values ('2022-04-26 15:15:03', 6)"
        sql += " tb6 values ('2022-04-26 15:15:02', 7)"
        sql += " tb7 values ('2022-04-26 15:15:01', 8)"
        tdSql.execute(sql)

        tdSql.query(
            f"select _wstart as ts, count(*) from {mt1} partition by tag1 interval(1s) order by _wstart;"
        )
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "2022-04-26 15:15:01.000")
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 0, "2022-04-26 15:15:02.000")
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 0, "2022-04-26 15:15:03.000")
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(3, 0, "2022-04-26 15:15:04.000")
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(4, 0, "2022-04-26 15:15:05.000")
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(5, 0, "2022-04-26 15:15:06.000")
        tdSql.checkData(5, 1, 1)
        tdSql.checkData(6, 0, "2022-04-26 15:15:07.000")
        tdSql.checkData(6, 1, 1)
        tdSql.checkData(7, 0, "2022-04-26 15:15:08.000")
        tdSql.checkData(7, 1, 1)

        tdSql.query(
            f"select * from (select _wstart as ts, count(*) from {mt1} partition by tag1 interval(1s) order by _wstart) order by ts;"
        )
        tdSql.query(
            f"select _wstart as ts, count(*) from {mt1} interval(1s) order by _wstart;"
        )
        tdSql.query(
            f"select * from (select _wstart as ts, count(*) from {mt1} interval(1s) order by _wstart) order by ts;"
        )
        tdSql.query(
            f"select diff(a) from (select _wstart as ts, count(*) a from {mt1} interval(1s) order by _wstart);"
        )
        tdSql.query(
            f"select diff(a) from (select _wstart as ts, count(*) a from {mt1} partition by tag1 interval(1s) order by _wstart);"
        )

        tdSql.execute(f"insert into tb0 values (now, 0);")
        tdSql.execute(f"insert into tb1 values (now, 1);")
        tdSql.execute(f"insert into tb2 values (now, 2);")
        tdSql.execute(f"insert into tb3 values (now, 3);")
        tdSql.execute(f"insert into tb4 values (now, 4);")
        tdSql.execute(f"insert into tb5 values (now, 5);")
        tdSql.execute(f"insert into tb6 values (now, 6);")
        tdSql.execute(f"insert into tb7 values (now, 7);")

        tdSql.query(
            f"select * from (select 1 from {mt1} where ts is not null partition by tbname limit 1);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select count(*) from (select ts from {mt1} where ts is not null partition by tbname slimit 2);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

        tdSql.query(
            f"select count(*) from (select ts from {mt1} where ts is not null partition by tbname limit 2);"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 16)

        print("do 2-query case ....................... [passed]")

    #
    # ------------------- test_max_partition.py ----------------
    #
    def prepare_datas(self, stb_name , tb_nums , row_nums, dbname="db" ):
        tdSql.execute(f" create stable {dbname}.{stb_name} (ts timestamp , c1 int , c2 bigint , c3 float , c4 double , c5 smallint , c6 tinyint , c7 bool , c8 binary(36) , c9 nchar(36) , uc1 int unsigned,\
            uc2 bigint unsigned ,uc3 smallint unsigned , uc4 tinyint unsigned ) tags(t1 timestamp , t2 int , t3 bigint , t4 float , t5 double , t6 smallint , t7 tinyint , t8 bool , t9 binary(36)\
                , t10 nchar(36) , t11 int unsigned , t12 bigint unsigned ,t13 smallint unsigned , t14 tinyint unsigned ) ")

        sql1 = "create table "
        sql2 = "insert into "
        for i in range(tb_nums):
            tbname = f"{dbname}.sub_{stb_name}_{i}"
            ts = self.ts + i*1000*120
            sql1 += f" {tbname} using {dbname}.{stb_name} tags ({ts} , {i} , %d , %f , %f , 1 , 2, 'true', 'binary_{i}' ,'nchar_{i}',{i},{i},10,20 )"%(i*10,i*1.0,i*1.0)

            sql2 += f" {tbname} values"
            for row in range(row_nums):
                ts = ts + row*1000
                sql2 += f"({ts} , {row} , {row} , {row} , {row} , 1 , 2 , 'true' , 'binary_{row}' , 'nchar_{row}' , {row} , {row} , 1 ,2 )"

            for null in range(5):
                ts = ts + row_nums*1000 + null*1000
                sql2 += f"({ts} , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL , NULL )"

        tdSql.execute(sql1)
        tdSql.execute(sql2)

    def basic_query(self, dbname="db"):
        tdSql.query(f"select count(*) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums + 5 )*self.tb_nums)
        tdSql.query(f"select max(c1) from {dbname}.stb")
        tdSql.checkData(0,0,(self.row_nums -1))
        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select max(c1) from {dbname}.stb group by t1 order by t1 ")
        tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select max(c1) from {dbname}.stb group by c1 order by t1 ")
        tdSql.query(f"select max(t2) from {dbname}.stb group by c1 order by t1 ")
        tdSql.query(f"select max(c1) from {dbname}.stb group by tbname order by tbname ")
        tdSql.checkRows(self.tb_nums)
        # bug need fix
        tdSql.query(f"select max(t2) from {dbname}.stb group by t2 order by t2 ")
        tdSql.checkRows(self.tb_nums)
        tdSql.query(f"select max(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select c1 , max(c1) from {dbname}.stb group by c1 order by c1 ")
        tdSql.checkRows(self.row_nums+1)

        # support selective functions
        tdSql.query(f"select c1 ,c2 ,c3 , max(c1)  ,c4 ,c5 ,t11 from {dbname}.stb group by c1 order by c1 desc ")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select c1, tbname , max(c1)  ,c4 ,c5 ,t11 from {dbname}.stb group by c1 order by c1 desc ")
        tdSql.checkRows(self.row_nums+1)

        # bug need fix
        tdSql.query(f"select tbname , max(c1)  from {dbname}.sub_stb_1 where c1 is null group by c1 order by c1 desc ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,"sub_stb_1")
        tdSql.query(f"select tbname , max(c1)  from {dbname}.sub_stb_1 group by c1 order by c1 desc ")
        rows = tdSql.queryRows
        tdSql.query(f"select tbname , max(c1)  from {dbname}.sub_stb_1 where c1 is null or (1<2) group by c1 order by c1 desc ")
        tdSql.checkRows(rows)

        tdSql.query(f"select max(c1) ,c2 ,t2,tbname from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select abs(c1+c3), count(c1+c3) ,max(c1+t2) from {dbname}.stb group by abs(c1+c3) order by abs(c1+c3)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.query(f"select max(c1+c3)+min(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) ,abs(t1) from {dbname}.stb group by abs(c1) order by abs(t1)+c2")
        tdSql.error(f"select count(c1+c3)+max(c2) ,abs(c1) from {dbname}.stb group by abs(c1) order by abs(c1)+c2")
        tdSql.query(f"select abs(c1+c3)+abs(c2) , count(c1+c3)+max(c2) from {dbname}.stb group by abs(c1+c3)+abs(c2) order by abs(c1+c3)+abs(c2)")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select max(c1) , max(t2) from {dbname}.stb where abs(c1+t2)=1 partition by tbname ")
        tdSql.checkRows(2)
        tdSql.query(f"select max(c1) from {dbname}.stb where abs(c1+t2)=1 partition by tbname ")
        tdSql.checkRows(2)

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by tbname order by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums-1)

        tdSql.query(f"select tbname , max(c2) from {dbname}.stb partition by t1 order by t1")
        tdSql.query(f"select tbname , max(t2) from {dbname}.stb partition by t1 order by t1")
        tdSql.query(f"select tbname , max(t2) from {dbname}.stb partition by t2 order by t2")

        # # bug need fix
        tdSql.query(f"select t2 , max(t2) from {dbname}.stb partition by t2 order by t2")
        tdSql.checkRows(self.tb_nums)

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,1,self.row_nums-1)

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by t2 order by t2")

        tdSql.query(f"select c2, max(c1) from {dbname}.stb partition by c2 order by c2 desc")
        tdSql.checkRows(self.tb_nums+1)
        tdSql.checkData(0,1,self.row_nums-1)

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by c1 order by c2")

        tdSql.query(f"select tbname , abs(t2) from {dbname}.stb partition by c2 order by t2")
        tdSql.checkRows(self.tb_nums*(self.row_nums+5))

        tdSql.query(f"select max(c1) , count(t2) from {dbname}.stb partition by c2 ")
        tdSql.checkRows(self.row_nums+1)
        tdSql.checkData(0,1,self.row_nums)

        tdSql.query(f"select count(c1) , max(t2) ,c2 from {dbname}.stb partition by c2 order by c2")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select count(c1) , count(t1) ,max(c2) ,tbname  from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkCols(4)

        tdSql.query(f"select count(c1) , max(t2) ,t1  from {dbname}.stb partition by t1 order by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,self.row_nums)

        # bug need fix
        tdSql.query(f"select count(c1) , max(t2) ,abs(c1) from {dbname}.stb partition by abs(c1) order by abs(c1)")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select max(ceil(c2)) , max(floor(t2)) ,max(floor(c2)) from {dbname}.stb partition by abs(c2) order by abs(c2)")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select max(ceil(c1-2)) , max(floor(t2+1)) ,max(c2-c1) from {dbname}.stb partition by abs(floor(c1)) order by abs(floor(c1))")
        tdSql.checkRows(self.row_nums+1)

        tdSql.query(f"select tbname , max(c1) ,c1 from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,'sub_stb_0')
        tdSql.checkData(0,1,9)
        tdSql.checkData(0,2,9)

        tdSql.query(f"select tbname ,top(c1,1) ,c1 from {dbname}.stb partition by tbname order by tbname")
        tdSql.checkRows(self.tb_nums)

        tdSql.query(f"select c1 , sample(c1,2) from {dbname}.stb partition by tbname order by tbname ")
        tdSql.checkRows(self.tb_nums*2)

        # interval
        tdSql.query(f"select max(c1) from {dbname}.stb interval(2s) sliding(1s)")

        # bug need fix

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>="2022-07-06 16:00:00.000 " and ts < "2022-07-06 17:00:00.000 " interval(50s) sliding(30s) fill(NULL)')

        tdSql.query(f"select tbname , count(c1) from {dbname}.stb partition by tbname interval(10s) slimit 5 soffset 1 ")

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by tbname interval(10s)")
        tdSql.checkRows(self.row_nums*10)

        tdSql.query(f"select unique(c1) from {dbname}.stb partition  by tbname order by tbname")

        tdSql.query(f"select tbname , count(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s)")
        tdSql.checkData(0,0,'sub_stb_1')
        tdSql.checkData(0,1, 4)

        tdSql.query(f"select c1 , mavg(c1 ,2 ) from {dbname}.stb partition by c1")
        tdSql.checkRows(90)

        tdSql.query(f"select c1 , diff(c1 , 0) from {dbname}.stb partition by c1")
        tdSql.checkRows(139)

        tdSql.query(f"select c1 , csum(c1) from {dbname}.stb partition by c1")
        tdSql.checkRows(100)

        tdSql.query(f"select c1 , sample(c1,2) from {dbname}.stb partition by c1 order by c1")
        tdSql.checkRows(21)
        # bug need fix
        tdSql.checkData(0,1,None)

        tdSql.query(f"select c1 , twa(c1) from {dbname}.sub_stb_1 partition by c1 order by c1")
        tdSql.checkRows(11)
        tdSql.checkData(0,1,None)

        tdSql.query(f"select c1 , irate(c1) from {dbname}.stb partition by c1 order by c1")
        tdSql.checkRows(11)
        tdSql.checkData(0,1,None)

        tdSql.query(f"select c1 , DERIVATIVE(c1,2,1) from {dbname}.stb partition by c1 order by c1")
        tdSql.checkRows(90)
        # bug need fix
        tdSql.checkData(0,1,0.0)

        tdSql.query(f"select tbname , max(c1) from {dbname}.stb partition by tbname order by tbname slimit 5 soffset 0 ")
        tdSql.checkRows(10)

        tdSql.query(f"select tbname , max(c1) from {dbname}.sub_stb_1 partition by tbname interval(10s) sliding(5s) ")

        tdSql.query(f'select max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+1000 interval(50s) sliding(30s)')
        tdSql.query(f'select tbname , max(c1) from {dbname}.stb where ts>={self.ts} and ts < {self.ts}+1000 interval(50s) sliding(30s)')

    def do_max_partition(self):
        dbname = "db"
        tdSql.prepare()
        self.prepare_datas("stb",self.tb_nums,self.row_nums)
        self.basic_query()

        # # coverage case for taosd crash about bug fix
        tdSql.query(f"select sum(c1) from {dbname}.stb where t2+10 >1 ")
        tdSql.query(f"select count(c1),count(t1) from {dbname}.stb where -t2<1 ")
        tdSql.query(f"select tbname ,max(ceil(c1)) from {dbname}.stb group by tbname ")
        tdSql.query(f"select avg(abs(c1)) , tbname from {dbname}.stb group by tbname ")
        tdSql.query(f"select t1,c1 from {dbname}.stb where abs(t2+c1)=1 ")
    
        print("do max partiton by .................... [passed]")

    #
    # ------------------- main ----------------
    #
    def test_ts_partitionby(self):
        """Time series partition by
        
        1. Create stable with multiple tags
        2. Create multiple subtables using the stable with different tags
        3. Insert data into the subtables
        4. Query with partition by on different tags and verify the results
        5. Query various aggregate functions with partition by clause
        6. Query interval and sliding window with partition by clause
        7. Query edge cases and error handling for partition by queries

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-12-20 Alex Duan Migrated from uncatalog/system-test/2-query/test_max_partition.py

        """
        self.do_partitionby()
        self.do_max_partition()
 