from new_test_framework.utils import tdLog, tdSql

class TestDistinct:

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        pass

    def test_distinct(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        dbname = "db"

        tdLog.printNoPrefix("==========step1:create table")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int, c2 int) tags(t0 tinyint, t1 int, t2 int)")
        tdSql.execute("create stable db.stb2 (ts timestamp, c2 int, c3 binary(16)) tags(t2 binary(16), t3 binary(16), t4 int)")
        maxRemainderNum=7
        tbnum=101
        for i in range(tbnum-1):
            sql  = f"create table db.t{i} using db.stb1 tags({i%maxRemainderNum}, {(i-1)%maxRemainderNum}, {i%2})"
            tdSql.execute(sql)
            tdSql.execute(f"insert into db.t{i} values (now-10d, {i}, {i%3})")
            tdSql.execute(f"insert into db.t{i} values (now-9d, {i}, {(i-1)%3})")
            tdSql.execute(f"insert into db.t{i} values (now-8d, {i}, {(i-2)%3})")
            tdSql.execute(f"insert into db.t{i} (ts )values (now-7d)")

            tdSql.execute(f"create table db.t0{i} using db.stb2 tags('{i%maxRemainderNum}', '{(i-1)%maxRemainderNum}', {i%3})")
            tdSql.execute(f"insert into db.t0{i} values (now-10d, {i}, '{(i+1)%3}')")
            tdSql.execute(f"insert into db.t0{i} values (now-9d, {i}, '{(i+2)%3}')")
            tdSql.execute(f"insert into db.t0{i} values (now-8d, {i}, '{(i)%3}')")
            tdSql.execute(f"insert into db.t0{i} (ts )values (now-7d)")
        tdSql.execute("create table db.t100num using db.stb1 tags(null, null, null)")
        tdSql.execute("create table db.t0100num using db.stb2 tags(null, null, null)")
        tdSql.execute(f"insert into db.t100num values (now-10d, {tbnum-1}, 1)")
        tdSql.execute(f"insert into db.t100num values (now-9d, {tbnum-1}, 0)")
        tdSql.execute(f"insert into db.t100num values (now-8d, {tbnum-1}, 2)")
        tdSql.execute(f"insert into db.t100num (ts )values (now-7d)")
        tdSql.execute(f"insert into db.t0100num values (now-10d, {tbnum-1}, 1)")
        tdSql.execute(f"insert into db.t0100num values (now-9d, {tbnum-1}, 0)")
        tdSql.execute(f"insert into db.t0100num values (now-8d, {tbnum-1}, 2)")
        tdSql.execute(f"insert into db.t0100num (ts )values (now-7d)")

        # #========== distinct multi-data-coloumn ==========
        tdSql.query(f"select distinct  c1 from {dbname}.stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum)
        tdSql.query(f"select distinct  c2 from {dbname}.stb1")
        tdSql.checkRows(4)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum*3)
        tdSql.query(f"select distinct  c1,c1 from {dbname}.stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.stb1 where c1 <{tbnum} limit 3")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.stb1 where c1 <{tbnum} limit 3 offset {tbnum*3-2}")
        tdSql.checkRows(2)

        tdSql.query(f"select distinct  c1 from {dbname}.t1 where c1 <{tbnum}")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  c2 from {dbname}.t1")
        tdSql.checkRows(4)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.t1 where c1 <{tbnum}")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c1 from {dbname}.t1 ")
        tdSql.checkRows(2)
        tdSql.query(f"select distinct  c1,c1 from {dbname}.t1 where c1 <{tbnum}")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.t1 where c1 <{tbnum} limit 3")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from {dbname}.t1 where c1 <{tbnum} limit 3 offset 2")
        tdSql.checkRows(1)

        # tdSql.query(f"select distinct  c3 from {dbname}.stb2 where c2 <{tbnum} ")
        # tdSql.checkRows(3)
        # tdSql.query(f"select distinct  c3, c2 from {dbname}.stb2 where c2 <{tbnum} limit 2")
        # tdSql.checkRows(2)

        # tdSql.error(f"select distinct c5 from {dbname}.stb1")
        tdSql.error(f"select distinct c5 from {dbname}.t1")
        tdSql.error(f"select distinct c1 from db.*")
        tdSql.error(f"select c2, distinct c1 from {dbname}.stb1")
        tdSql.error(f"select c2, distinct c1 from {dbname}.t1")
        tdSql.error(f"select distinct c2 from ")
        tdSql.error("distinct c2 from {dbname}.stb1")
        tdSql.error("distinct c2 from {dbname}.t1")
        tdSql.error(f"select distinct  c1, c2, c3 from {dbname}.stb1")
        tdSql.error(f"select distinct  c1, c2, c3 from {dbname}.t1")
        tdSql.error(f"select distinct  stb1.c1, stb1.c2, stb2.c2, stb2.c3 from {dbname}.stb1")
        tdSql.error(f"select distinct  stb1.c1, stb1.c2, stb2.c2, stb2.c3 from {dbname}.t1")
        tdSql.error(f"select distinct  t1.c1, t1.c2, t2.c1, t2.c2 from {dbname}.t1")
        tdSql.query(f"select distinct  c1 c2, c2 c3 from {dbname}.stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum*3)
        tdSql.query(f"select distinct  c1 c2, c2 c3 from {dbname}.t1 where c1 <{tbnum}")
        tdSql.checkRows(3)
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 order by ts")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 order by ts")
        tdSql.error(f"select distinct  c1, ts from {dbname}.stb1 group by c2")
        tdSql.error(f"select distinct  c1, ts from {dbname}.t1 group by c2")
        tdSql.query(f"select distinct  c1, max(c2) from {dbname}.stb1 ")
        tdSql.query(f"select distinct  c1, max(c2) from {dbname}.t1 ")
        tdSql.error(f"select max(c2), distinct  c1 from {dbname}.stb1 ")
        tdSql.error(f"select max(c2), distinct  c1 from {dbname}.t1 ")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 where c1 > 3 group by t0")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 where c1 > 3 group by t0")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 where c1 > 3 interval(1d) ")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 where c1 > 3 interval(1d) ")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 where c1 > 3 interval(1d) fill(next)")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 where c1 > 3 interval(1d) fill(next)")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 where ts > now-10d and ts < now interval(1d) fill(next)")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 where ts > now-10d and ts < now interval(1d) fill(next)")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.stb1 where c1 > 3 slimit 1")
        tdSql.error(f"select distinct  c1, c2 from {dbname}.t1 where c1 > 3 slimit 1")
        tdSql.query(f"select distinct  c1, c2 from {dbname}.stb1 where c1 between {tbnum-2} and {tbnum} ")
        tdSql.checkRows(6)
        tdSql.query(f"select distinct  c1, c2 from {dbname}.t1 where c1 between {tbnum-2} and {tbnum} ")
        # tdSql.checkRows(1)
        tdSql.query(f"select distinct  c1, c2 from {dbname}.stb1 where c1 in (1,2,3,4,5)")
        tdSql.checkRows(15)
        tdSql.query(f"select distinct  c1, c2 from {dbname}.t1 where c1 in (1,2,3,4,5)")
        # tdSql.checkRows(1)
        tdSql.query(f"select distinct  c1, c2 from {dbname}.stb1 where c1 in (100,1000,10000)")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1, c2 from {dbname}.t1 where c1 in (100,1000,10000)")
        tdSql.checkRows(0)

        tdSql.query(f"select distinct  c1,c2 from (select * from {dbname}.stb1 where c1 > {tbnum-2}) ")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from (select * from {dbname}.t1 where c1 < {tbnum}) ")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from (select * from {dbname}.stb1 where t2 !=0 and t2 != 1) ")
        tdSql.checkRows(0)
        tdSql.query(f"select distinct  c1, c2 from (select distinct c1, c2 from {dbname}.stb1 where t0 > 2 and t1 < 3) ")
        tdSql.query(f"select  c1, c2 from (select distinct c1, c2 from {dbname}.stb1 where t0 > 2 and t1 < 3) ")
        tdSql.query(f"select distinct  c1, c2 from (select c2, c1 from {dbname}.stb1 where c1 > 2 ) where  c1 < 4")
        tdSql.checkRows(3)
        tdSql.error(f"select distinct  c1, c2 from (select c1 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.query(f"select distinct  c1, c2 from (select c2, c1 from {dbname}.stb1 where c1 > 2 order by ts)")
        tdSql.query(f"select distinct  c1, c2 from (select c2, c1 from {dbname}.t1 where c1 > 2 order by ts)")
        tdSql.error(f"select distinct  c1, c2 from (select c2, c1 from {dbname}.stb1 where c1 > 2 group by c1)")
        tdSql.query(f"select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from {dbname}.stb1 group by stb1.c1)")
        tdSql.query(f"select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from {dbname}.t1 group by t1.c1)")
        tdSql.query(f"select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from {dbname}.stb1 )")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from {dbname}.t1 )")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  stb1.c1, stb1.c2 from {dbname}.stb1, {dbname}.stb2 where stb1.ts=stb2.ts and stb1.t2=stb2.t4")
        tdSql.query(f"select distinct  t1.c1, t1.c2 from {dbname}.t1, {dbname}.t2 where t1.ts=t2.ts ")

        tdSql.query(f"select distinct  c1, c2 from (select count(c1) c1, count(c2) c2 from {dbname}.stb1 group by ts)")
        tdSql.query(f"select distinct  c1, c2 from (select count(c1) c1, count(c2) c2 from {dbname}.t1 group by ts)")



        #========== suport distinct multi-tags-coloumn ==========
        tdSql.query(f"select distinct  t1 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t1, t0 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t1, t2 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum*2+1)
        tdSql.query(f"select distinct  t0, t1, t2 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum*2+1)
        tdSql.query(f"select distinct  t0 t1, t1 t2 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t0, t0, t0 from {dbname}.stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t0, t1 from {dbname}.t1")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  t0, t1 from {dbname}.t100num")
        tdSql.checkRows(1)

        tdSql.query(f"select distinct  t3 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t2, t3 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t3, t2 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t4, t2 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum*3+1)
        tdSql.query(f"select distinct  t2, t3, t4 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum*3+1)
        tdSql.query(f"select distinct  t2 t1, t3 t2 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t3, t3, t3 from {dbname}.stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query(f"select distinct  t2, t3 from {dbname}.t01")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  t3, t4 from {dbname}.t0100num")
        tdSql.checkRows(1)


        ########## should be error #########
        tdSql.error(f"select distinct  from {dbname}.stb1")
        tdSql.error(f"select distinct t3 from {dbname}.stb1")
        tdSql.error(f"select distinct t1 from db.*")
        tdSql.error(f"select distinct t2 from ")
        tdSql.error(f"distinct t2 from {dbname}.stb1")
        tdSql.error(f"select distinct stb1")
        tdSql.error(f"select distinct  t0, t1, t2, t3 from {dbname}.stb1")
        tdSql.error(f"select distinct  stb1.t0, stb1.t1, stb2.t2, stb2.t3 from {dbname}.stb1")

        tdSql.error(f"select dist t0 from {dbname}.stb1")
        tdSql.error(f"select distinct  stb2.t2, stb2.t3 from {dbname}.stb1")
        tdSql.error(f"select distinct  stb2.t2 t1, stb2.t3 t2 from {dbname}.stb1")

        tdSql.query(f"select distinct  t0, t1 from {dbname}.t1 where t0 < 7")

        ########## add where condition ##########
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 limit 2")
        tdSql.checkRows(2)
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 limit 2 offset 2")
        tdSql.checkRows(1)
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 slimit 2")
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1 where c1 > 2")
        tdSql.query(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 and t1 < 5")
        tdSql.checkRows(1)
        tdSql.error(f"select distinct  stb1.t0, stb1.t1 from {dbname}.stb1, {dbname}.stb2 where stb1.t2=stb2.t4")
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where stb2.t4 > 2")
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 group by t0")
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 interval(1d) ")
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where t1 > 3 interval(1d) fill(next)")
        tdSql.error(f"select distinct  t0, t1 from {dbname}.stb1 where ts > now-10d and ts < now interval(1d) fill(next)")

        tdSql.error(f"select max(c1), distinct t0 from {dbname}.stb1 where t0 > 2")
        tdSql.query(f"select distinct t0, max(c1) from {dbname}.stb1 where t0 > 2")
        tdSql.error(f"select distinct  t0  from {dbname}.stb1 where t0 in (select t0 from {dbname}.stb1 where t0 > 2)")
        tdSql.query(f"select distinct  t0, t1  from {dbname}.stb1 where t0 in (1,2,3,4,5)")
        tdSql.checkRows(5)
        tdSql.query(f"select distinct  t1 from (select t0, t1 from {dbname}.stb1 where t0 > 2) ")
        tdSql.checkRows(4)
        tdSql.query(f"select distinct  t1 from (select distinct t0, t1 from {dbname}.stb1 where t0 > 2 and t1 < 3) ")
        # TODO: BUG of TD-17561
        # tdSql.query(f"select distinct  t1 from (select distinct t0, t1 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.query(f"select distinct  t1 from (select t0, t1 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.checkRows(1)
        tdSql.error(f"select distinct  t1, t0 from (select t1 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.query(f"select distinct  t1, t0 from (select max(t1) t1, max(t0) t0 from {dbname}.stb1 group by stb1.t1)")
        tdSql.query(f"select distinct  t1, t0 from (select max(t1) t1, max(t0) t0 from {dbname}.stb1)")
        tdSql.query(f"select distinct  t1, t0 from (select t1,t0 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  t1, t0 from (select t1,t0 from {dbname}.stb1 where t0 > 2 order by ts) where  t1 < 3")
        # TODO: BUG of TD-17561
        # tdSql.error(f"select t1, t0 from (select distinct t1,t0 from {dbname}.stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.error(f"select distinct  t1, t0 from (select t1,t0 from {dbname}.stb1 where t0 > 2  group by ts) where  t1 < 3")
        tdSql.query(f"select distinct  stb1.t1, stb1.t2 from {dbname}.stb1, {dbname}.stb2 where stb1.ts=stb2.ts and stb1.t2=stb2.t4")
        tdSql.query(f"select distinct  t1.t1, t1.t2 from {dbname}.t1, {dbname}.t2 where t1.ts=t2.ts ")
        
        self.ts5971()

        #tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

    def ts5971(self):
        dbname = "db"
                
        tdSql.execute(f"DROP TABLE IF EXISTS {dbname}.t5971")
        tdSql.execute(f"create table {dbname}.t5971 (time TIMESTAMP, c1 INT)")
        tdSql.execute(f"INSERT INTO {dbname}.t5971(time, c1) VALUES (1641024000000, 1), (1641024005000, 2)")
        tdSql.query(f"SELECT DISTINCT CSUM(c1), time FROM {dbname}.t5971 ORDER BY time")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1641024000000)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 1641024005000)
        
        tdSql.query(f"SELECT DISTINCT CSUM(c1), time AS ref FROM {dbname}.t5971 ORDER BY ref")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 1, 1641024000000)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(1, 1, 1641024005000)
        
        tdSql.query(f"SELECT DISTINCT CSUM(c1), time FROM {dbname}.t5971")
        tdSql.checkRows(2)
