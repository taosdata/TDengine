from new_test_framework.utils import tdLog, tdSql, tdStream, sc, clusterComCheck


class TestFilterOperator:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_operator(self):
        """Filter with operators

        1. Filtering with logical operators
        2. Filtering with comparison operators
        3. Combined with GROUP BY, ORDER BY, and LIMIT OFFSET clauses
        4. Including IS NULL operations

        Catalog:
            - - Query:Filter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-19 Simon Guan Migrated from tsim/parser/condition.sim
            - 2025-8-19 Simon Guan Migrated from tsim/query/complex_where.sim
            - 2025-8-19 Simon Guan Migrated from tsim/parser/where.sim

        """

        self.Condition()
        tdStream.dropAllStreamsAndDbs()
        self.ComplexWhere()
        tdStream.dropAllStreamsAndDbs()
        self.Where()
        tdStream.dropAllStreamsAndDbs()

    def Condition(self):
        tdSql.execute(f"drop database if exists cdb")
        tdSql.execute(f"create database if not exists cdb")
        tdSql.execute(f"use cdb")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(10), t3 double)"
        )
        tdSql.execute(f"create table tb1 using stb1 tags(1,'1',1.0)")
        tdSql.execute(f"create table tb2 using stb1 tags(2,'2',2.0)")
        tdSql.execute(f"create table tb3 using stb1 tags(3,'3',3.0)")
        tdSql.execute(f"create table tb4 using stb1 tags(4,'4',4.0)")
        tdSql.execute(f"create table tb5 using stb1 tags(5,'5',5.0)")
        tdSql.execute(f"create table tb6 using stb1 tags(6,'6',6.0)")

        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:00',1,1.0,1,1,1,1.0,true ,'1','1')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:01',2,2.0,2,2,2,2.0,true ,'2','2')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:02',3,3.0,3,3,3,3.0,false,'3','3')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:03',4,4.0,4,4,4,4.0,false,'4','4')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:04',11,11.0,11,11,11,11.0,true ,'11','11')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:05',12,12.0,12,12,12,12.0,true ,'12','12')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:06',13,13.0,13,13,13,13.0,false,'13','13')"
        )
        tdSql.execute(
            f"insert into tb1 values ('2021-05-05 18:19:07',14,14.0,14,14,14,14.0,false,'14','14')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2021-05-05 18:19:08',21,21.0,21,21,21,21.0,true ,'21','21')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2021-05-05 18:19:09',22,22.0,22,22,22,22.0,true ,'22','22')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2021-05-05 18:19:10',23,23.0,23,23,23,23.0,false,'23','23')"
        )
        tdSql.execute(
            f"insert into tb2 values ('2021-05-05 18:19:11',24,24.0,24,24,24,24.0,false,'24','24')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2021-05-05 18:19:12',31,31.0,31,31,31,31.0,true ,'31','31')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2021-05-05 18:19:13',32,32.0,32,32,32,32.0,true ,'32','32')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2021-05-05 18:19:14',33,33.0,33,33,33,33.0,false,'33','33')"
        )
        tdSql.execute(
            f"insert into tb3 values ('2021-05-05 18:19:15',34,34.0,34,34,34,34.0,false,'34','34')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2021-05-05 18:19:16',41,41.0,41,41,41,41.0,true ,'41','41')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2021-05-05 18:19:17',42,42.0,42,42,42,42.0,true ,'42','42')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2021-05-05 18:19:18',43,43.0,43,43,43,43.0,false,'43','43')"
        )
        tdSql.execute(
            f"insert into tb4 values ('2021-05-05 18:19:19',44,44.0,44,44,44,44.0,false,'44','44')"
        )
        tdSql.execute(
            f"insert into tb5 values ('2021-05-05 18:19:20',51,51.0,51,51,51,51.0,true ,'51','51')"
        )
        tdSql.execute(
            f"insert into tb5 values ('2021-05-05 18:19:21',52,52.0,52,52,52,52.0,true ,'52','52')"
        )
        tdSql.execute(
            f"insert into tb5 values ('2021-05-05 18:19:22',53,53.0,53,53,53,53.0,false,'53','53')"
        )
        tdSql.execute(
            f"insert into tb5 values ('2021-05-05 18:19:23',54,54.0,54,54,54,54.0,false,'54','54')"
        )
        tdSql.execute(
            f"insert into tb6 values ('2021-05-05 18:19:24',61,61.0,61,61,61,61.0,true ,'61','61')"
        )
        tdSql.execute(
            f"insert into tb6 values ('2021-05-05 18:19:25',62,62.0,62,62,62,62.0,true ,'62','62')"
        )
        tdSql.execute(
            f"insert into tb6 values ('2021-05-05 18:19:26',63,63.0,63,63,63,63.0,false,'63','63')"
        )
        tdSql.execute(
            f"insert into tb6 values ('2021-05-05 18:19:27',64,64.0,64,64,64,64.0,false,'64','64')"
        )
        tdSql.execute(
            f"insert into tb6 values ('2021-05-05 18:19:28',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
        )

        tdSql.execute(
            f"create table stb2 (ts timestamp, u1 int unsigned, u2 bigint unsigned, u3 smallint unsigned, u4 tinyint unsigned, ts2 timestamp) TAGS(t1 int unsigned, t2 bigint unsigned, t3 timestamp, t4 int)"
        )
        tdSql.execute(
            f"create table tb2_1 using stb2 tags(1,1,'2021-05-05 18:38:38',1)"
        )
        tdSql.execute(
            f"create table tb2_2 using stb2 tags(2,2,'2021-05-05 18:58:58',2)"
        )

        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:00',1,2,3,4,'2021-05-05 18:28:01')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:01',5,6,7,8,'2021-05-05 18:28:02')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:02',2,2,3,4,'2021-05-05 18:28:03')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:03',5,6,7,8,'2021-05-05 18:28:04')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:04',3,2,3,4,'2021-05-05 18:28:05')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:05',5,6,7,8,'2021-05-05 18:28:06')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:06',4,2,3,4,'2021-05-05 18:28:07')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:07',5,6,7,8,'2021-05-05 18:28:08')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:08',5,2,3,4,'2021-05-05 18:28:09')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:09',5,6,7,8,'2021-05-05 18:28:10')"
        )
        tdSql.execute(
            f"insert into tb2_1 values ('2021-05-05 18:19:10',6,2,3,4,'2021-05-05 18:28:11')"
        )
        tdSql.execute(
            f"insert into tb2_2 values ('2021-05-05 18:19:11',5,6,7,8,'2021-05-05 18:28:12')"
        )
        tdSql.execute(
            f"insert into tb2_2 values ('2021-05-05 18:19:12',7,2,3,4,'2021-05-05 18:28:13')"
        )
        tdSql.execute(
            f"insert into tb2_2 values ('2021-05-05 18:19:13',5,6,7,8,'2021-05-05 18:28:14')"
        )
        tdSql.execute(
            f"insert into tb2_2 values ('2021-05-05 18:19:14',8,2,3,4,'2021-05-05 18:28:15')"
        )
        tdSql.execute(
            f"insert into tb2_2 values ('2021-05-05 18:19:15',5,6,7,8,'2021-05-05 18:28:16')"
        )

        tdSql.execute(
            f"create table stb3 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int, t2 binary(10), t3 double)"
        )
        tdSql.execute(f"create table tb3_1 using stb3 tags(1,'1',1.0)")
        tdSql.execute(f"create table tb3_2 using stb3 tags(2,'2',2.0)")

        tdSql.execute(
            f"insert into tb3_1 values ('2021-01-05 18:19:00',1,1.0,1,1,1,1.0,true ,'1','1')"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-02-05 18:19:01',2,2.0,2,2,2,2.0,true ,'2','2')"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-03-05 18:19:02',3,3.0,3,3,3,3.0,false,'3','3')"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-04-05 18:19:03',4,4.0,4,4,4,4.0,false,'4','4')"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-05-05 18:19:28',5,NULL,5,NULL,5,NULL,true,NULL,'5')"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-06-05 18:19:28',NULL,6.0,NULL,6,NULL,6.0,NULL,'6',NULL)"
        )
        tdSql.execute(
            f"insert into tb3_1 values ('2021-07-05 18:19:28',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-01-06 18:19:00',11,11.0,11,11,11,11.0,true ,'11','11')"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-02-06 18:19:01',12,12.0,12,12,12,12.0,true ,'12','12')"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-03-06 18:19:02',13,13.0,13,13,13,13.0,false,'13','13')"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-04-06 18:19:03',14,14.0,14,14,14,14.0,false,'14','14')"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-05-06 18:19:28',15,NULL,15,NULL,15,NULL,true,NULL,'15')"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-06-06 18:19:28',NULL,16.0,NULL,16,NULL,16.0,NULL,'16',NULL)"
        )
        tdSql.execute(
            f"insert into tb3_2 values ('2021-07-06 18:19:28',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
        )

        tdSql.execute(
            f"create table stb4 (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9),c10 binary(16300)) TAGS(t1 int, t2 binary(10), t3 double)"
        )
        tdSql.execute(f"create table tb4_0 using stb4 tags(0,'0',0.0)")
        tdSql.execute(f"create table tb4_1 using stb4 tags(1,'1',1.0)")
        tdSql.execute(f"create table tb4_2 using stb4 tags(2,'2',2.0)")
        tdSql.execute(f"create table tb4_3 using stb4 tags(3,'3',3.0)")
        tdSql.execute(f"create table tb4_4 using stb4 tags(4,'4',4.0)")

        i = 0
        ts0 = 1625850000000
        blockNum = 5
        delta = 0
        tbname0 = "tb4_"
        a = 0
        b = 200
        c = 400
        while i < blockNum:
            x = 0
            rowNum = 1200
            while x < rowNum:
                ts = ts0 + x
                a = a + 1
                b = b + 1
                c = c + 1
                d = float(x) / 10
                tin = rowNum
                binary = "'binary" + str(int(c)) + "'"
                nchar = "'nchar" + str(int(c)) + "'"
                tbname = "'tb4_" + str(i) + "'"
                tdSql.execute(
                    f"insert into {tbname} values ( {ts} , {a} , {b} , {c} , {d} , {d} , {c} , true, {binary} , {nchar} , {binary} )"
                )
                x = x + 1
            i = i + 1
            ts0 = ts0 + 259200000

        self.condition_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")
        self.condition_query()

    def condition_query(self):
        tdSql.execute(f"use cdb;")

        tdLog.info(f'"column test"')
        tdSql.query(f"select * from stb1")
        tdSql.checkRows(29)

        tdSql.query(f"select * from stb1 where c1 > 0")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c8 > 0")
        tdSql.query(f"select * from stb1 where c7 in (0,2,3,1);")
        tdSql.query(f"select * from stb1 where c8 in (true);")
        tdSql.query(f"select * from stb1 where c8 in (1,2);")
        tdSql.query(f"select * from stb1 where t2 in (3.0);")
        tdSql.query(f"select ts,c1,c7 from stb1 where c7 > false")
        tdSql.query(f"select * from stb1 where c1 > NULL;")
        tdSql.query(f"select * from stb1 where c1 = NULL;")
        tdSql.error(f"select * from stb1 where c1 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c2 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c3 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c4 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c5 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c6 LIKE '%1';")
        tdSql.error(f"select * from stb1 where c7 LIKE '%1';")
        tdSql.query(f"select * from stb1 where c1 = 'NULL';")
        tdSql.query(f"select * from stb1 where c2 > 'NULL';")
        tdSql.query(f"select * from stb1 where c3 <> 'NULL';")
        tdSql.query(f"select * from stb1 where c4 != 'null';")
        tdSql.query(f"select * from stb1 where c5 >= 'null';")
        tdSql.query(f"select * from stb1 where c6 <= 'null';")
        tdSql.query(f"select * from stb1 where c7 < 'nuLl';")
        tdSql.query(f"select * from stb1 where c8 < 'nuLl';")
        tdSql.query(f"select * from stb1 where c9 > 'nuLl';")
        tdSql.error(
            f"select * from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b;"
        )
        tdSql.error(
            f"select a.ts,a.c1,a.c8 from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50 or b.c1 < 60;"
        )
        tdSql.query(
            f"select a.ts,a.c1,a.c8 from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and ((a.c1 > 50 and a.c1 < 60) or (b.c2 > 60));"
        )
        tdSql.query(f"select * from stb1 where 'c2' is null;")
        tdSql.query(f"select * from stb1 where 'c2' is not null;")

        tdSql.query(f"select * from stb1 where c2 > 3.0 or c2 < 60;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c2 > 3.0 or c2 < 60 and c2 > 50;")
        tdSql.checkRows(25)

        tdSql.query(f"select * from stb1 where (c2 > 3.0 or c2 < 60) and c2 > 50;")
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c2 > 3.0 or c2 < 60) and c2 > 50 and (c2 != 53 and c2 != 63);"
        )
        tdSql.checkRows(6)

        tdSql.query(
            f"select * from stb1 where (c2 > 3.0 or c2 < 60) and c2 > 50 and (c2 != 53 or c2 != 63);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c3 > 3.0 or c3 < 60) and c3 > 50 and (c3 != 53 or c3 != 63);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c4 > 3.0 or c4 < 60) and c4 > 50 and (c4 != 53 or c4 != 63);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c5 > 3.0 or c5 < 60) and c5 > 50 and (c5 != 53 or c5 != 63);"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c6 > 3.0 or c6 < 60) and c6 > 50 and (c6 != 53 or c6 != 63);"
        )
        tdSql.checkRows(8)

        tdSql.query(f"select * from stb1 where c8 = '51';")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.query(f"select * from stb1 where c8 != '51';")
        tdSql.checkRows(27)

        # xxx
        tdSql.query(f"select * from stb1 where c8 = '51' and c8 != '51';")
        tdSql.checkRows(0)

        # xxx
        tdSql.query(f"select * from stb1 where c8 = '51' or c8 != '51';")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c9 = '51';")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.query(f"select * from stb1 where c9 != '51';")
        tdSql.checkRows(27)

        tdSql.query(f"select * from stb1 where c9 = '51' and c9 != '51';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c9 = '51' or c9 != '51';")
        tdSql.checkRows(28)

        tdSql.query(f"select ts,c1,c7 from stb1 where c7 = false order by ts")
        tdSql.checkRows(14)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 0)

        tdSql.checkData(1, 0, "2021-05-05 18:19:03")

        tdSql.checkData(1, 1, 4)

        tdSql.checkData(1, 2, 0)

        tdSql.checkData(2, 0, "2021-05-05 18:19:06")

        tdSql.checkData(2, 1, 13)

        tdSql.checkData(2, 2, 0)

        tdSql.checkData(3, 0, "2021-05-05 18:19:07")

        tdSql.checkData(3, 1, 14)

        tdSql.checkData(3, 2, 0)

        tdSql.query(f"select ts,c1,c7 from stb1 where c7 = true order by ts")
        tdSql.checkRows(14)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(0, 2, 1)

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(1, 2, 1)

        tdSql.checkData(2, 0, "2021-05-05 18:19:04")

        tdSql.checkData(2, 1, 11)

        tdSql.checkData(2, 2, 1)

        tdSql.checkData(3, 0, "2021-05-05 18:19:05")

        tdSql.checkData(3, 1, 12)

        tdSql.checkData(3, 2, 1)

        tdSql.query(f"select * from stb1 where c8 = '51' or c8 = '4' order by ts")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:03")

        tdSql.checkData(0, 1, 4)

        tdSql.checkData(1, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 1, 51)

        tdSql.query(f"select * from stb1 where c1 > 50 and c1 > 53")
        tdSql.checkRows(5)

        tdSql.query(f"select * from stb1 where c1 > 50 or c1 > 53")
        tdSql.checkRows(8)

        tdSql.query(f"select * from stb1 where c1 > 50 and c1 > 53 and c1 < 52")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 > 50 or c1 > 53 or c1 < 51")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 > 50 and c1 > 53 or c1 < 51")
        tdSql.checkRows(25)

        tdSql.query(f"select * from stb1 where c1 > 50 or c1 > 53 and c1 < 51")
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 and c1 > 51 and c1 > 54"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 and c1 > 51 or c1 > 54"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 and c1 < 51 or c1 > 54"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 or c1 < 51 and c1 > 54"
        )
        tdSql.checkRows(5)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 or c1 > 51 and c1 < 54"
        )
        tdSql.checkRows(7)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or c1 > 53 and c1 < 51 and c1 > 54"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and c1 > 53 or c1 < 51 or c1 > 54"
        )
        tdSql.checkRows(25)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or c1 > 53 and c1 < 51 or c1 > 54"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or c1 > 53 or c1 < 51 and c1 > 54"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or c1 > 53 or c1 > 51 and c1 > 54"
        )
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or c1 > 53 or c1 < 51 or c1 > 54"
        )
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where (c1 > 50 and c1 > 53) and c1 < 52")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 > 50 and (c1 > 53 and c1 < 52)")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where (c1 > 50 or c1 > 53) or c1 < 51")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 > 50 or (c1 > 53 or c1 < 51)")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where (c1 > 50 and c1 > 53) or c1 < 51")
        tdSql.checkRows(25)

        tdSql.query(f"select * from stb1 where c1 > 50 and (c1 > 53 or c1 < 51)")
        tdSql.checkRows(5)

        tdSql.query(f"select * from stb1 where (c1 > 50 or c1 > 53) and c1 < 51")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 > 50 or (c1 > 53 and c1 < 51)")
        tdSql.checkRows(8)

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53) and (c1 < 51 and c1 > 54)"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53 and c1 < 51) and c1 > 54"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 and c1 < 51) and c1 > 54"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 and c1 < 51 or c1 > 54)"
        )
        tdSql.checkRows(4)

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53) or (c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:23")

        tdSql.checkData(1, 0, "2021-05-05 18:19:24")

        tdSql.checkData(2, 0, "2021-05-05 18:19:25")

        tdSql.checkData(3, 0, "2021-05-05 18:19:26")

        tdSql.checkData(4, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 or c1 < 51) and c1 > 54"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53 or c1 < 51) and c1 > 54"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 or c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:23")

        tdSql.checkData(1, 0, "2021-05-05 18:19:24")

        tdSql.checkData(2, 0, "2021-05-05 18:19:25")

        tdSql.checkData(3, 0, "2021-05-05 18:19:26")

        tdSql.checkData(4, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53) and (c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 and c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53 and c1 < 51) and c1 > 54 order by ts"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 and c1 < 51) and c1 > 54 order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53) or (c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(25)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 or c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:23")

        tdSql.checkData(1, 0, "2021-05-05 18:19:24")

        tdSql.checkData(2, 0, "2021-05-05 18:19:25")

        tdSql.checkData(3, 0, "2021-05-05 18:19:26")

        tdSql.checkData(4, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 and c1 > 53 or c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(25)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where c1 > 50 and (c1 > 53 or c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:23")

        tdSql.checkData(1, 0, "2021-05-05 18:19:24")

        tdSql.checkData(2, 0, "2021-05-05 18:19:25")

        tdSql.checkData(3, 0, "2021-05-05 18:19:26")

        tdSql.checkData(4, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53) and (c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 and c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53 and c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 and c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53) or (c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53 or c1 < 51) and c1 > 54 order by ts"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 or c1 < 51 and c1 > 54) order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 or c1 < 51) and c1 > 54 order by ts"
        )
        tdSql.checkRows(8)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where c1 > 62 or (c1 > 53 or c1 < 51) and c1 > 54 order by ts"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53) or (c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 or c1 < 51 or c1 > 54) order by ts"
        )
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where (c1 > 50 or c1 > 53 or c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where c1 > 50 or (c1 > 53 or c1 < 51) or c1 > 54 order by ts"
        )
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select ts,c1 from stb1 where (c1 > 60 or c1 < 10 or (c1 > 20 and c1 < 30)) and ts > '2021-05-05 18:19:00.000' and ts < '2021-05-05 18:19:25.000' and c1 != 21 and c1 != 22 order by ts"
        )
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 1, 4)

        tdSql.checkData(3, 0, "2021-05-05 18:19:10")

        tdSql.checkData(3, 1, 23)

        tdSql.checkData(4, 0, "2021-05-05 18:19:11")

        tdSql.checkData(4, 1, 24)

        tdSql.checkData(5, 0, "2021-05-05 18:19:24")

        tdSql.checkData(5, 1, 61)

        tdSql.query(
            f"select * from stb1 where (c1 > 40 or c1 < 20) and (c2 < 53 or c2 >= 63) and c3 > 1 and c3 < 5 order by ts"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 1, 4)

        tdSql.query(
            f"select * from stb1 where (c1 > 52 or c1 < 10) and (c2 > 1 and c2 < 61) order by ts"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 1, 4)

        tdSql.checkData(3, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 1, 53)

        tdSql.checkData(4, 0, "2021-05-05 18:19:23")

        tdSql.checkData(4, 1, 54)

        tdSql.query(
            f"select * from stb1 where (c3 > 52 or c3 < 10) and (c4 > 1 and c4 < 61) and (c5 = 2 or c6 = 3.0 or c6 = 4.0 or c6 = 53) order by ts"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.checkData(0, 1, 2)

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 1, 3)

        tdSql.checkData(2, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 1, 4)

        tdSql.checkData(3, 0, "2021-05-05 18:19:22")

        tdSql.checkData(3, 1, 53)

        tdSql.query(f"select * from stb1 where c1 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c2 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c3 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c4 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c5 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c6 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c7 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c8 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        # xxx
        tdSql.query(f"select * from stb1 where c8 like '1' order by ts;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        # xxx
        tdSql.query(
            f"select * from stb1 where c8 like '1%' and c8 like '%1' order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:04")

        # xxx
        tdSql.query(
            f"select * from stb1 where c8 like '1' and c8 like '2' order by ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c9 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(f"select * from stb1 where c1 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c2 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c3 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c4 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c5 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c6 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c7 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c8 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c9 is not null order by ts;")
        tdSql.checkRows(28)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from stb1 where c1 > 63 or c1 is null order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:27")

        tdSql.checkData(0, 1, 64)

        tdSql.checkData(1, 0, "2021-05-05 18:19:28")

        tdSql.checkData(1, 1, None)

        tdSql.query(f"select * from stb1 where c1 is null and c2 is null order by ts;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.checkData(0, 1, None)

        tdSql.query(
            f"select * from stb1 where c1 is null and c2 is null and c3 is not null order by ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c1 is null and c2 is null and ts > '2021-05-05 18:19:00.000' and ts < '2021-05-05 18:19:28.000' order by ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 is null and c1 > 0;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 is null or c1 is not null or c1 > 1;")
        tdSql.checkRows(29)

        tdSql.query(
            f"select * from stb1 where (c1 is null or c1 > 40) and c1 < 44 order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:16")

        tdSql.checkData(1, 0, "2021-05-05 18:19:17")

        tdSql.checkData(2, 0, "2021-05-05 18:19:18")

        tdSql.query(
            f"select * from stb1 where c1 in (11,21,31,41) and c1 in (11,42) order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where c8 in ('11','21','31','41') and c8 in ('11','42') order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb1 where (c1 > 60 and c2 > 40) or (c1 > 62 and c2 > 50) order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 = 3 or c1 = 5 or c1 >= 44 and c1 <= 52 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:19")

        tdSql.checkData(2, 0, "2021-05-05 18:19:20")

        tdSql.checkData(3, 0, "2021-05-05 18:19:21")

        tdSql.query(f"select * from stb1 where c8 LIKE '%1' order by ts;")
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:04")

        tdSql.checkData(2, 0, "2021-05-05 18:19:08")

        tdSql.checkData(3, 0, "2021-05-05 18:19:12")

        tdSql.checkData(4, 0, "2021-05-05 18:19:16")

        tdSql.checkData(5, 0, "2021-05-05 18:19:20")

        tdSql.checkData(6, 0, "2021-05-05 18:19:24")

        tdSql.query(f"select * from stb1 where c9 LIKE '%1' order by ts;")
        tdSql.checkRows(7)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:04")

        tdSql.checkData(2, 0, "2021-05-05 18:19:08")

        tdSql.checkData(3, 0, "2021-05-05 18:19:12")

        tdSql.checkData(4, 0, "2021-05-05 18:19:16")

        tdSql.checkData(5, 0, "2021-05-05 18:19:20")

        tdSql.checkData(6, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where (c8 LIKE '%1' or c9 like '_2') and (c5 > 50 or c6 > 30) and ( c8 like '3_' or c9 like '4_') and (c4 <= 31 or c4 >= 42) order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:12")

        tdSql.checkData(1, 0, "2021-05-05 18:19:17")

        tdSql.query(f"select * from stb1 where c1 in (1,3) order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.query(f"select * from stb1 where c3 in (11,22) order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:09")

        tdSql.query(f"select * from stb1 where c4 in (3,33) order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:14")

        tdSql.query(f"select * from stb1 where c5 in (3,33) and c8 in ('22','55');")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c5 in (3,33) and c8 in ('33','54') order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:14")

        tdSql.query(
            f"select * from stb1 where c5 in (3,33) or c8 in ('22','54') order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:09")

        tdSql.checkData(2, 0, "2021-05-05 18:19:14")

        tdSql.checkData(3, 0, "2021-05-05 18:19:23")

        tdSql.query(
            f"select * from stb1 where (c9 in ('3','1','2','4','5') or c9 in ('33','11','22','44','55')) and c9 in ('1','3','11','13') order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(2, 0, "2021-05-05 18:19:04")

        tdSql.query(
            f"select * from stb2 where (u1 in (1) or u2 in (5,6)) and (u3 in (3,6) or u4 in (7,8)) and ts2 in ('2021-05-05 18:28:02.000','2021-05-05 18:28:15.000','2021-05-05 18:28:01.000') order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.query(
            f"select * from stb2 where u2 in (2) and u3 in (1,2,3) and u4 in (1,2,4,5) and u1 > 3 and u1 < 6 and u1 != 4 order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:08")

        tdSql.query(
            f"select avg(c1) from tb1 where (c1 > 12 or c2 > 10) and (c3 < 12 or c3 > 13) ;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 12.500000000)

        tdSql.query(
            f"select count(c1),sum(c3) from tb1 where ((c7 = true and c6 > 2) or (c1 > 10 or c3 < 3)) and ((c8 like '1%') or (c9 like '%2' or c9 like '%3')) interval(5s);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 3)

        tdSql.checkData(0, 1, 14)

        tdSql.checkData(1, 0, 3)

        tdSql.checkData(1, 1, 39)

        tdSql.query(f"select * from stb1 where c8 = 'null';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c8 = 'NULL';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c9 = 'null';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c9 = 'NULL';")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c2 in (0,1) order by ts;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.query(f"select * from stb1 where c6 in (0,2,3,1) order by ts;")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.query(
            f"select ts,c1 from (select * from stb1 where (c1 > 60 or c1 < 10) and (c7 = true or c5 > 2 and c5 < 63)) where (c3 > 61 or c3 < 3) order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:25")

        tdSql.query(
            f"select a.* from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50 order by ts;"
        )
        tdSql.query(
            f"select a.* from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.checkData(2, 0, "2021-05-05 18:19:24")

        tdSql.checkData(3, 0, "2021-05-05 18:19:25")

        tdSql.query(
            f"select a.ts,a.c1,a.c8,a.c9 from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50 and b.c1 < 60 order by ts;"
        )
        tdSql.query(
            f"select a.ts,a.c1,a.c8 from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50 and b.c1 < 60 order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:20")

        tdSql.checkData(1, 0, "2021-05-05 18:19:21")

        tdSql.query(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and (a.c1 < 10 or a.c1 > 30) and (b.u1 < 5 or b.u1 > 5) order by a.ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(2, 0, "2021-05-05 18:19:12")

        tdSql.checkData(3, 0, "2021-05-05 18:19:14")

        tdSql.error(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and (a.c1 < 10 or a.c1 > 30) and (b.u1 < 5 or b.u1 > 5) order by ts;"
        )

        tdSql.query(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and a.c1 < 30 and b.u1 > 1 and a.c1 > 10 and b.u1 < 8 and b.u1<>5 order by a.ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:06")

        tdSql.checkData(2, 0, "2021-05-05 18:19:10")

        tdSql.error(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and a.c1 < 30 and b.u1 > 1 and a.c1 > 10 and b.u1 < 8 and b.u1<>5 order by ts;"
        )
        tdSql.query(
            f"select a.ts,a.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and a.c1 < 30 and b.u1 > 1 and a.c1 > 10 and b.u1 < 8 and b.u1<>5 order by ts;"
        )

        tdSql.query(f"select * from stb1 where c1 is null and c1 is not null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 is null or c1 is not null;")
        tdSql.checkRows(29)

        tdSql.query(f"select * from stb1 where c1 is null or c1 > 20 or c1 < 25;")
        tdSql.checkRows(29)

        tdSql.query(f"select * from stb1 where (c1 > 20 or c1 < 25) and c1 is null;")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where (c1 > 20 or c1 < 25) and (c1 > 62 or c1 < 3) order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where c1 > 11 and c1 != 11 and c1 != 14 and c1 < 14 order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:05")

        tdSql.checkData(1, 0, "2021-05-05 18:19:06")

        tdSql.query(
            f"select * from stb1 where (c1 > 60 or c1 < 4 or c1 > 10 and c1 < 20 and c1 != 13 or c1 < 2 or c1 > 50) order by ts"
        )
        tdSql.checkRows(14)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:04")

        tdSql.query(f"select * from stb1 where c1 > 62 or c1 >= 62 order by ts;")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:25")

        tdSql.checkData(1, 0, "2021-05-05 18:19:26")

        tdSql.checkData(2, 0, "2021-05-05 18:19:27")

        tdSql.query(f"select * from stb1 where c1 > 62 and c1 >= 62 order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.checkData(1, 0, "2021-05-05 18:19:27")

        tdSql.query(f"select * from stb1 where c1 >= 62 and c1 != 62 order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.checkData(1, 0, "2021-05-05 18:19:27")

        tdSql.query(f"select * from stb1 where c1 >= 62 or c1 != 62;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 >= 62 and c1 = 62;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:25")

        tdSql.query(f"select * from stb1 where c1 > 62 and c1 != 62 order by ts;")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.checkData(1, 0, "2021-05-05 18:19:27")

        tdSql.query(f"select * from stb1 where c1 > 62 and c1 = 62;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 is not null and c1 is not null;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 is not null or c1 is not null;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 is null and c1 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.query(f"select * from stb1 where c1 is null or c1 is null;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:28")

        tdSql.query(f"select * from stb1 where c2 > 3 and c2 < 3;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c2 = 3;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.query(f"select * from stb1 where c2 > 3 and c2 <= 3;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c2 >= 3 and c2 <= 3 order by ts;")
        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.query(
            f"select * from stb1 where (c2 in (1,2,3,4) or c2 in (11,12,13,14)) and c2 != 11 and c2 >2 and c2 != 14 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 0, "2021-05-05 18:19:05")

        tdSql.checkData(3, 0, "2021-05-05 18:19:06")

        tdSql.query(
            f"select * from stb1 where (c1 > 60 or c1 < 4 or c1 > 10 and c1 < 20 and c1 != 13 or c1 < 2 or c1 > 50) and (c1 != 51 and c1 <= 54 and c1 != 54 and c1 >=1 and c1 != 1) and (c1 >= 11 and c1 <=52 and c1 != 52 and c1 != 11);"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:05")

        tdSql.checkData(1, 0, "2021-05-05 18:19:07")

        tdSql.query(f"select * from stb1 where c1 > 1 and c1 is not null and c1 < 5;")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.checkData(1, 0, "2021-05-05 18:19:02")

        tdSql.checkData(2, 0, "2021-05-05 18:19:03")

        tdSql.query(
            f"select * from (select * from stb1 where c2 > 10 and c6 < 40) where c9 in ('11','21','31') order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:08")

        tdSql.checkData(2, 0, "2021-05-05 18:19:12")

        tdSql.query(
            f"select * from stb1 where c1 > 40 and c2 > 50 and c3 > 62 or c1 < 2 and c2 < 3 order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:26")

        tdSql.checkData(2, 0, "2021-05-05 18:19:27")

        tdSql.query(
            f"select * from stb1 where (c1 > 3 and c2 > 4) or (c1 < 60 and c2 < 30);"
        )
        tdSql.checkRows(28)

        tdSql.query(
            f"select * from stb1 where (c1 > 3 and c2 > 4) or (c1 < 60 and c2 < 30) or (c1 is null and c2 is null);"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select * from stb1 where (c1 > 3 and c2 > 4) or (c1 < 60 and c3 < 30) or (c1 is null and c2 is null);"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select * from stb1 where (c1 > 60 and c2 < 63) or (c1 >62 and c3 < 30) or (c1 is null and c2 is null) order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:28")

        tdSql.query(
            f"select * from stb1 where c1 between 60 and 9999999999 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.query(f"select * from stb1 where c1 > 9999999999;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 < 9999999999;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c1 = 9999999999;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c1 <> 9999999999;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c4 < -9999999999;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c4 > -9999999999;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c4 = -9999999999;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c4 <> -9999999999;")
        tdSql.checkRows(28)

        tdSql.query(f"select * from stb1 where c5 in (-9999999999);")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where c5 in (9999999999);")
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where c5 in (-9999999999,3,4,9999999999) order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:03")

        tdSql.query(f"select * from stb3 where c1 > 3 and c1 < 2 order by ts;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb3 where c1 is null order by ts;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-06-05 18:19:28.000")

        tdSql.checkData(1, 0, "2021-06-06 18:19:28.000")

        tdSql.checkData(2, 0, "2021-07-05 18:19:28.000")

        tdSql.checkData(3, 0, "2021-07-06 18:19:28.000")

        tdSql.query(f"select * from stb3 where c1 is not null order by ts;")
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2021-01-05 18:19:00")

        tdSql.checkData(1, 0, "2021-01-06 18:19:00")

        tdSql.checkData(2, 0, "2021-02-05 18:19:01")

        tdSql.checkData(3, 0, "2021-02-06 18:19:01")

        tdSql.checkData(4, 0, "2021-03-05 18:19:02")

        tdSql.checkData(5, 0, "2021-03-06 18:19:02")

        tdSql.checkData(6, 0, "2021-04-05 18:19:03")

        tdSql.checkData(7, 0, "2021-04-06 18:19:03")

        tdSql.query(f"select * from stb3 where c1 > 11 order by ts;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-02-06 18:19:01")

        tdSql.checkData(1, 0, "2021-03-06 18:19:02")

        tdSql.checkData(2, 0, "2021-04-06 18:19:03")

        tdSql.checkData(3, 0, "2021-05-06 18:19:28")

        tdSql.query(
            f"select * from stb3 where c1 is not null or c1 is null order by ts;"
        )
        tdSql.checkRows(14)

        tdSql.query(f"select ts,c1 from stb4 where c1 = 200 order by ts;")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.199")

        tdSql.query(f"select ts,c1 from stb4 where c1 != 200;")
        tdSql.checkRows(5999)

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 >= 200 and c2 > 500 and c3 < 800 and c4 between 33 and 37 and c4 != 35 and c2 < 555 and c1 < 339 and c1 in (331,333,335) order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.330")

        tdSql.checkData(1, 0, "2021-07-10 01:00:00.332")

        tdSql.checkData(2, 0, "2021-07-10 01:00:00.334")

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 > -3 and c1 < 5 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.000")

        tdSql.checkData(1, 0, "2021-07-10 01:00:00.001")

        tdSql.checkData(2, 0, "2021-07-10 01:00:00.002")

        tdSql.checkData(3, 0, "2021-07-10 01:00:00.003")

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 >= 2 and c1 < 5 order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.001")

        tdSql.checkData(1, 0, "2021-07-10 01:00:00.002")

        tdSql.checkData(2, 0, "2021-07-10 01:00:00.003")

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 >= -3 and c1 < 1300 order by ts;"
        )
        tdSql.checkRows(1299)

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 >= 1298 and c1 < 1300 or c2 > 210 and c2 < 213 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.010")

        tdSql.checkData(1, 0, "2021-07-10 01:00:00.011")

        tdSql.checkData(2, 0, "2021-07-13 01:00:00.097")

        tdSql.checkData(3, 0, "2021-07-13 01:00:00.098")

        tdSql.query(f"select ts,c1,c2,c3,c4 from stb4 where c1 >= -3;")
        tdSql.checkRows(6000)

        tdSql.query(f"select ts,c1,c2,c3,c4 from stb4 where c1 < 1400;")
        tdSql.checkRows(1399)

        tdSql.query(f"select ts,c1,c2,c3,c4 from stb4 where c1 < 1100;")
        tdSql.checkRows(1099)

        tdSql.query(
            f"select ts,c1,c2,c3,c4 from stb4 where c1 in(10,100, 1100,3300) and c1 != 10 order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-07-10 01:00:00.099")

        tdSql.checkData(1, 0, "2021-07-10 01:00:01.099")

        tdSql.checkData(2, 0, "2021-07-16 01:00:00.899")

        tdLog.info(f'"ts test"')

        tdSql.query(f"select ts,c1,c7 from stb1 where ts != '2021-05-05 18:19:27'")
        tdSql.query(
            f"select ts,c1,c7 from stb1 where ts > '2021-05-05 18:19:03.000' or ts < '2021-05-05 18:19:02.000';"
        )
        tdSql.query(
            f"select ts,c1,c7 from stb1 where ts > '2021-05-05 18:19:03.000' and ts > '2021-05-05 18:19:20.000' and ts != '2021-05-05 18:19:22.000';"
        )
        tdSql.error(f"select * from stb1 where ts2 like '2021-05-05%';")
        tdSql.query(
            f"select ts,c1,c2 from stb1 where (ts > '2021-05-05 18:19:25.000' or ts < '2021-05-05 18:19:05.000') and ts > '2021-05-05 18:19:01.000' and ts < '2021-05-05 18:19:27.000';"
        )
        tdSql.query(
            f"select ts,c1,c2 from stb1 where (ts > '2021-05-05 18:19:20.000' or ts < '2021-05-05 18:19:05.000') and ts != '2021-05-05 18:19:25.000';"
        )
        tdSql.query(
            f"select ts,c1,c2 from stb1 where ((ts >= '2021-05-05 18:19:05.000' and ts <= '2021-05-05 18:19:10.000') or (ts >= '2021-05-05 18:19:15.000' and ts <= '2021-05-05 18:19:20.000') or (ts >= '2021-05-05 18:19:11.000' and ts <= '2021-05-05 18:19:14.000'));"
        )
        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' or ts < '2021-05-05 18:19:24.000';"
        )
        tdSql.query(f"select * from stb1 where ts is null;")
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where ts is not null and ts is null;")
        tdSql.checkRows(0)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' and ts < '2021-05-05 18:19:10.000';"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:03.000' and ts < '2021-05-05 18:19:02';"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from stb1 where ts is not null;")
        tdSql.checkRows(29)

        tdSql.query(f"select * from stb1 where ts is not null or ts is null;")
        tdSql.checkRows(29)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' or ts < '2021-05-05 18:19:25.000';"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' and ts < '2021-05-05 18:19:26.000';"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:25")

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' or ts < '2021-05-05 18:19:28.000';"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' or ts > '2021-05-05 18:19:27.000' order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:25")

        tdSql.checkData(1, 0, "2021-05-05 18:19:26")

        tdSql.checkData(2, 0, "2021-05-05 18:19:27")

        tdSql.checkData(3, 0, "2021-05-05 18:19:28")

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts > '2021-05-05 18:19:20.000' or ts < '2021-05-05 18:19:05.000' or ts != '2021-05-05 18:19:25.000';"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ts >= '2021-05-05 18:19:25.000' or ts <> '2021-05-05 18:19:25.000';"
        )
        tdSql.checkRows(29)

        tdSql.query(
            f"select ts,c1,c2 from stb1 where ((ts >= '2021-05-05 18:19:05.000' and ts <= '2021-05-05 18:19:10.999') or (ts >= '2021-05-05 18:19:15.000' and ts <= '2021-05-05 18:19:20.000') or (ts >= '2021-05-05 18:19:11.000' and ts <= '2021-05-05 18:19:14.999')) order by ts;"
        )
        tdSql.checkRows(16)

        tdSql.checkData(0, 0, "2021-05-05 18:19:05")

        tdSql.query(
            f"select ts,c1,c2 from stb1 where (ts >= '2021-05-05 18:19:05.000' and ts <= '2021-05-05 18:19:10.000') or (ts >= '2021-05-05 18:19:12.000' and ts <= '2021-05-05 18:19:14.000') or (ts >= '2021-05-05 18:19:08.000' and ts <= '2021-05-05 18:19:17.000') order by ts;"
        )
        tdSql.checkRows(13)

        tdSql.checkData(0, 0, "2021-05-05 18:19:05")

        tdSql.query(
            f" select ts,c1,c2 from stb1 where (ts >= '2021-05-05 18:19:05.000' and ts <= '2021-05-05 18:19:10.000') or (ts >= '2021-05-05 18:19:02.000' and ts <= '2021-05-05 18:19:03.000') or (ts >= '2021-05-05 18:19:01.000' and ts <= '2021-05-05 18:19:08.000') order by ts;"
        )
        tdSql.checkRows(10)

        tdSql.checkData(0, 0, "2021-05-05 18:19:01")

        tdSql.query(
            f" select ts,c1,c2 from stb1 where ((ts >= '2021-05-05 18:19:08.000' and ts <= '2021-05-05 18:19:10.000') or (ts >= '2021-05-05 18:19:02.000' and ts <= '2021-05-05 18:19:03.000') or (ts >= '2021-05-05 18:19:05.000' and ts <= '2021-05-05 18:19:06.000') or (ts >= '2021-05-05 18:19:03.000' and ts <= '2021-05-05 18:19:12.000')) and (ts >= '2021-05-05 18:19:10.000') order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:10")

        tdSql.checkData(1, 0, "2021-05-05 18:19:11")

        tdSql.checkData(2, 0, "2021-05-05 18:19:12")

        tdSql.query(
            f"select ts,c1,c7 from stb1 where ts > '2021-05-05 18:19:25.000' and ts != '2021-05-05 18:19:18' order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.checkData(1, 0, "2021-05-05 18:19:27")

        tdSql.checkData(2, 0, "2021-05-05 18:19:28")

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:03.000' and ts > '2021-05-05 18:19:25' order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.checkData(1, 0, "2021-05-05 18:19:27")

        tdSql.checkData(2, 0, "2021-05-05 18:19:28")

        tdSql.query(
            f"select * from stb1 where ts < '2021-05-05 18:19:03.000' and ts < '2021-05-05 18:19:25' order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:23.000' and ts < '2021-05-05 18:19:25';"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:03.000' or ts > '2021-05-05 18:19:25' order by ts;"
        )
        tdSql.checkRows(25)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:05")

        tdSql.checkData(2, 0, "2021-05-05 18:19:06")

        tdSql.query(
            f"select * from stb1 where ts < '2021-05-05 18:19:03.000' or ts < '2021-05-05 18:19:25' order by ts;"
        )
        tdSql.checkRows(25)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:23.000' or ts < '2021-05-05 18:19:25' order by ts;"
        )
        tdSql.checkRows(29)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.query(
            f"select * from stb1 where (ts > '2021-05-05 18:19:23.000' or ts < '2021-05-05 18:19:25') and (ts > '2021-05-05 18:19:23.000' and ts < '2021-05-05 18:19:26') order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.query(
            f"select * from stb1 where (ts > '2021-05-05 18:19:23.000' or ts < '2021-05-05 18:19:25') and (ts > '2021-05-05 18:19:23.000' or ts > '2021-05-05 18:19:26') order by ts;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.checkData(2, 0, "2021-05-05 18:19:26")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.checkData(4, 0, "2021-05-05 18:19:28")

        tdSql.query(
            f"select * from stb2 where ts2 in ('2021-05-05 18:28:03','2021-05-05 18:28:05','2021-05-05 18:28:08') order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:04")

        tdSql.checkData(2, 0, "2021-05-05 18:19:07")

        tdSql.query(
            f"select * from stb2 where t3 in ('2021-05-05 18:38:38','2021-05-05 18:38:28','2021-05-05 18:38:08') and ts2 in ('2021-05-05 18:28:04','2021-05-05 18:28:04','2021-05-05 18:28:03') order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:03")

        # sql select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and (a.ts < '2021-05-05 18:19:03.000' or a.ts >= '2021-05-05 18:19:13.000') and (b.ts >= '2021-05-05 18:19:01.000' and b.ts <= '2021-05-05 18:19:14.000') order by a.ts;
        # if $rows != 4 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != @21-05-05 18:19:01.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(1,0) != @21-05-05 18:19:02.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != @21-05-05 18:19:13.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(3,0) != @21-05-05 18:19:14.000@ then
        #  return -1
        # endi
        #
        # sql select a.ts,c.ts,b.c1,c.u1,c.u2 from (select * from stb1) a, (select * from stb1) b, (select * from stb2) c where a.ts=b.ts and b.ts=c.ts and a.ts <= '2021-05-05 18:19:12.000' and b.ts >= '2021-05-05 18:19:06.000' and c.ts >= '2021-05-05 18:19:08.000' and c.ts <= '2021-05-05 18:19:11.000' and a.ts != '2021-05-05 18:19:10.000';
        # if $rows != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0) != @21-05-05 18:19:08.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(1,0) != @21-05-05 18:19:09.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(2,0) != @21-05-05 18:19:11.000@ then
        #  return -1
        # endi

        tdSql.query(
            f"select ts,c1,c2,c8 from (select * from stb1) where (ts <= '2021-05-05 18:19:06.000' or ts >= '2021-05-05 18:19:13.000') and (ts >= '2021-05-05 18:19:02.000' and ts <= '2021-05-05 18:19:14.000') and ts != '2021-05-05 18:19:04.000' order by ts;"
        )
        tdSql.checkRows(6)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:03")

        tdSql.checkData(2, 0, "2021-05-05 18:19:05")

        tdSql.checkData(3, 0, "2021-05-05 18:19:06")

        tdSql.checkData(4, 0, "2021-05-05 18:19:13")

        tdSql.checkData(5, 0, "2021-05-05 18:19:14")

        tdSql.query(
            f"select ts,c1,c2,c8 from (select * from stb1) where (ts <= '2021-05-05 18:19:03.000' or ts > '2021-05-05 18:19:26.000' or ts = '2021-05-05 18:19:26.000') and ts != '2021-05-05 18:19:03.000' and ts != '2021-05-05 18:19:26.000' order by ts;"
        )
        tdSql.checkRows(5)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:27")

        tdSql.checkData(4, 0, "2021-05-05 18:19:28")

        tdLog.info(f'"tbname test"')
        tdSql.query(f"select * from stb1 where tbname like '%3' and tbname like '%4';")

        tdSql.query(f"select * from stb1 where tbname like 'tb%';")
        tdSql.checkRows(29)

        tdSql.query(f"select * from stb1 where tbname like '%2' order by ts;")
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:08")

        tdSql.checkData(1, 0, "2021-05-05 18:19:09")

        tdSql.checkData(2, 0, "2021-05-05 18:19:10")

        tdSql.checkData(3, 0, "2021-05-05 18:19:11")

        tdLog.info(f'"tag test"')
        tdSql.query(
            f"select * from stb1 where t1 in (1,2) and t1 in (2,3) order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:08")

        tdSql.checkData(1, 0, "2021-05-05 18:19:09")

        tdSql.checkData(2, 0, "2021-05-05 18:19:10")

        tdSql.checkData(3, 0, "2021-05-05 18:19:11")

        tdSql.query(
            f"select * from stb2 where t1 in (1,2) and t2 in (2) and t3 in ('2021-05-05 18:58:57.000');"
        )
        tdSql.checkRows(0)

        tdLog.info(f'"join test"')
        tdSql.error(
            f"select * from tb1, tb2_1 where tb1.ts=tb2_1.ts or tb1.ts =tb2_1.ts;"
        )
        tdSql.query(
            f"select tb1.ts from tb1, tb2_1 where tb1.ts=tb2_1.ts and tb1.ts > '2021-05-05 18:19:03.000' and tb2_1.ts < '2021-05-05 18:19:06.000' order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:05")

        tdSql.query(
            f"select tb1.ts,tb1.*,tb2_1.* from tb1, tb2_1 where tb1.ts=tb2_1.ts and tb1.ts > '2021-05-05 18:19:03.000' and tb2_1.u1 < 5 order by tb1.ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:04")

        tdSql.checkData(1, 0, "2021-05-05 18:19:06")

        tdSql.query(
            f"select tb1.ts,tb1.*,tb2_1.* from tb1, tb2_1 where tb1.ts=tb2_1.ts and tb1.ts >= '2021-05-05 18:19:03.000' and tb1.c7=false and tb2_1.u3>4 order by tb1.ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:03")

        tdSql.checkData(1, 0, "2021-05-05 18:19:07")

        tdSql.query(
            f"select stb1.ts,stb1.c1,stb1.t1,stb2.ts,stb2.u1,stb2.t4 from stb1, stb2 where stb1.ts=stb2.ts and stb1.t1 = stb2.t4 order by stb1.ts;"
        )
        tdSql.checkRows(9)

        tdSql.checkData(0, 0, "2021-05-05 18:19:00")

        tdSql.checkData(1, 0, "2021-05-05 18:19:01")

        tdSql.checkData(2, 0, "2021-05-05 18:19:02")

        tdSql.checkData(3, 0, "2021-05-05 18:19:03")

        tdSql.checkData(4, 0, "2021-05-05 18:19:04")

        tdSql.checkData(5, 0, "2021-05-05 18:19:05")

        tdSql.checkData(6, 0, "2021-05-05 18:19:06")

        tdSql.checkData(7, 0, "2021-05-05 18:19:07")

        tdSql.checkData(8, 0, "2021-05-05 18:19:11")

        tdSql.query(
            f"select stb1.ts,stb1.c1,stb1.t1,stb2.ts,stb2.u1,stb2.t4 from stb1, stb2 where stb1.ts=stb2.ts and stb1.t1 = stb2.t4 and stb1.c1 > 2 and stb2.u1 <=4 order by stb1.ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:02")

        tdSql.checkData(1, 0, "2021-05-05 18:19:04")

        tdSql.checkData(2, 0, "2021-05-05 18:19:06")

        tdLog.info(f'"column&ts test"')
        tdSql.query(f"select count(*) from stb1 where ts > 0 or c1 > 0;")
        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:03.000' and ts < '2021-05-05 18:19:20.000' and (c1 > 23 or c1 < 14) and c7 in (true) and c8 like '%2' order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:05")

        tdSql.checkData(1, 0, "2021-05-05 18:19:13")

        tdSql.checkData(2, 0, "2021-05-05 18:19:17")

        tdLog.info(f'"column&tbname test"')
        tdSql.query(f" select count(*) from stb1 where tbname like 'tb%' or c1 > 0;")
        tdSql.query(
            f"select * from stb1 where tbname like '%3' and c6 < 34 and c5 != 33 and c4 > 31;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:13")

        tdLog.info(f'"column&tag test"')
        tdSql.query(f"select * from stb1 where t1 > 0 or c1 > 0")
        tdSql.query(f"select * from stb1 where c1 > 0 or t1 > 0")
        tdSql.query(f"select * from stb1 where t1 > 0 or c1 > 0 or t1 > 1")
        tdSql.query(f"select * from stb1 where c1 > 0 or t1 > 0 or c1 > 1")
        tdSql.query(f"select * from stb1 where t1 > 0 and c1 > 0 or t1 > 1")
        tdSql.query(f"select * from stb1 where c1 > 0 or t1 > 0 and c1 > 1")
        tdSql.query(f"select * from stb1 where c1 > 0 or t1 > 0 and c1 > 1")
        tdSql.query(f"select * from stb1 where t1 > 0 or t1 > 0 and c1 > 1")
        tdSql.query(
            f"select * from stb1 where (c1 > 0 and t1 > 0 ) or (t1 > 1 and c1 > 3)"
        )
        tdSql.query(f"select * from stb1 where (c1 > 0 and t1 > 0 ) or t1 > 1")
        tdSql.query(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and a.t1=b.t1;"
        )

        tdSql.query(f"select * from stb1 where c1 < 63 and t1 > 5")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:24")

        tdSql.checkData(1, 0, "2021-05-05 18:19:25")

        tdSql.query(
            f"select * from stb1 where t1 > 3 and t1 < 5 and c1 != 42 and c1 != 44 order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:16")

        tdSql.checkData(1, 0, "2021-05-05 18:19:18")

        tdSql.query(
            f"select * from stb1 where t1 > 1 and c1 > 21 and t1 < 3 and c1 < 24 and t1 != 3 and c1 != 23;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:09")

        tdSql.query(
            f"select * from stb1 where c1 > 1 and (t1 > 3 or t1 < 2) and (c2 > 2 and c2 < 62 and t1 != 4) and (t1 > 2 and t1 < 6) and c7 = true and c8 like '%2' order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:21")

        tdSql.query(
            f"select * from stb1 where c1!=31 and c1 !=32 and c1 <> 63 and c1 <>1 and c1 <> 21 and c1 <> 2 and c7 <> true and c8 <> '3' and c9 <> '4' and c2<>13 and c3 <> 23 and c4 <> 33 and c5 <> 34 and c6 <> 43 and c2 <> 53 and t1 <> 5 and t2 <>4 order by ts;"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, "2021-05-05 18:19:07")

        tdSql.checkData(1, 0, "2021-05-05 18:19:11")

        tdSql.checkData(2, 0, "2021-05-05 18:19:27")

        tdLog.info(f'"column&join test"')
        tdSql.error(
            f"select tb1.ts,tb1.c1,tb2_1.u1 from tb1, tb2_1 where tb1.ts=tb2_1.ts or tb1.c1 > 0;"
        )

        tdLog.info(f'"ts&tbname test"')
        tdSql.query(f"select count(*) from stb1 where ts > 0 or tbname like 'tb%';")

        tdLog.info(f'"ts&tag test"')
        tdSql.query(f"select count(*) from stb1 where ts > 0 or t1 > 0;")

        tdSql.query(
            f"select * from stb2 where t1!=1 and t2=2 and t3 in ('2021-05-05 18:58:58.000') and ts < '2021-05-05 18:19:13.000' order by ts;"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2021-05-05 18:19:11")

        tdSql.checkData(1, 0, "2021-05-05 18:19:12")

        tdLog.info(f'"ts&join test"')
        tdSql.error(
            f"select tb1.ts,tb1.c1,tb2_1.u1 from tb1, tb2_1 where tb1.ts=tb2_1.ts or tb1.ts > 0;"
        )
        tdSql.query(
            f"select tb1.ts,tb1.c1,tb2_1.u1 from tb1, tb2_1 where tb1.ts=tb2_1.ts and (tb1.ts > '2021-05-05 18:19:05.000' or tb1.ts < '2021-05-05 18:19:03.000' or tb1.ts > 0);"
        )

        tdLog.info(f'"tbname&tag test"')
        tdSql.query(
            f"select * from stb1 where tbname like 'tb%' and (t1=1 or t2=2 or t3=3) and t1 > 2 order by ts;"
        )
        tdSql.checkRows(4)

        tdSql.checkData(0, 0, "2021-05-05 18:19:12")

        tdSql.checkData(1, 0, "2021-05-05 18:19:13")

        tdSql.checkData(2, 0, "2021-05-05 18:19:14")

        tdSql.checkData(3, 0, "2021-05-05 18:19:15")

        tdLog.info(f'"tbname&join test"')

        tdLog.info(f'"tag&join test"')

        tdLog.info(f'"column&ts&tbname test"')
        tdSql.query(
            f"select count(*) from stb1 where tbname like 'tb%' or c1 > 0 or ts > 0;"
        )

        tdLog.info(f'"column&ts&tag test"')
        tdSql.query(f"select count(*) from stb1 where t1 > 0 or c1 > 0 or ts > 0;")
        tdSql.query(f"select count(*) from stb1 where c1 > 0 or t1 > 0 or ts > 0;")

        tdSql.query(
            f"select * from stb1 where (t1 > 0 or t1 > 2 ) and ts > '2021-05-05 18:19:10.000' and (c1 > 1 or c1 > 3) and (c6 > 40 or c6 < 30) and (c8 like '%3' or c8 like '_4') and (c9 like '1%' or c9 like '6%' or (c9 like '%3' and c9 != '23')) and ts > '2021-05-05 18:19:22.000' and ts <= '2021-05-05 18:19:26.000' order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:26")

        tdSql.query(
            f"select * from stb1 where ts > '2021-05-05 18:19:00.000' and c1 > 2 and t1 != 1 and c2 >= 23 and t2 >= 3 and c3 < 63 and c7 = false and t3 > 3 and t3 < 6 and c8 like '4%' and ts < '2021-05-05 18:19:19.000' and c2 > 40 and c3 != 42 order by ts;"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2021-05-05 18:19:18")

        tdLog.info(f'"column&ts&join test"')

        tdLog.info(f'"column&tbname&tag test"')
        tdSql.query(
            f"select count(*) from stb1 where c1 > 0 or tbname in ('tb1') or t1 > 0;"
        )

        tdLog.info(f'"column&tbname&join test"')
        tdLog.info(f'"column&tag&join test"')
        tdLog.info(f'"ts&tbname&tag test"')
        tdSql.query(
            f"select count(*) from stb1 where ts > 0 or tbname in ('tb1') or t1 > 0;"
        )

        tdLog.info(f'"ts&tbname&join test"')
        tdLog.info(f'"ts&tag&join test"')
        tdLog.info(f'"tbname&tag&join test"')

        tdLog.info(f'"column&ts&tbname&tag test"')
        tdSql.query(
            f"select * from stb1 where (tbname like 'tb%' or ts > '2021-05-05 18:19:01.000') and (t1 > 5 or t1 < 4) and c1 > 0;"
        )
        tdSql.query(
            f"select * from stb1 where (ts > '2021-05-05 18:19:01.000') and (ts > '2021-05-05 18:19:02.000' or t1 > 3) and (t1 > 5 or t1 < 4) and c1 > 0;"
        )
        tdSql.query(
            f"select ts,c1,c7 from stb1 where ts > '2021-05-05 18:19:03.000' or ts > '2021-05-05 18:19:20.000' and c1 > 0 and t1 > 0;"
        )

        tdLog.info(f'"column&ts&tbname&join test"')
        tdLog.info(f'"column&ts&tag&join test"')
        tdLog.info(f'"column&tbname&tag&join test"')
        tdLog.info(f'"ts&tbname&tag&join test"')

        tdLog.info(f'"column&ts&tbname&tag&join test"')

    def ComplexWhere(self):
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database db")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.execute(f"use db")

        tdLog.info(f"=============== create super table and child table")
        tdSql.execute(
            f"create table stb1 (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp) tags (t1 int)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.execute(f"create table ct1 using stb1 tags ( 1 )")
        tdSql.execute(f"create table ct2 using stb1 tags ( 2 )")
        tdSql.execute(f"create table ct3 using stb1 tags ( 3 )")
        tdSql.execute(f"create table ct4 using stb1 tags ( 4 )")
        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== insert data into child table ct1 (s)")
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+7a )'
        )
        tdSql.execute(
            f'insert into ct1 values ( \'2022-01-01 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+8a )'
        )

        tdLog.info(f"=============== insert data into child table ct2 (d)")
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 01:00:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 10:00:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-01 20:00:01.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 10:00:01.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-02 20:00:01.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 10:00:01.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", now+6a )'
        )
        tdSql.execute(
            f'insert into ct2 values ( \'2022-01-03 20:00:01.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", now+7a )'
        )

        tdLog.info(f"=============== insert data into child table ct3 (n)")
        tdSql.execute(
            f"insert into ct3 values ( '2021-12-21 01:01:01.000', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL )"
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2021-12-31 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-01 01:01:06.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-07 01:01:10.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-01-31 01:01:16.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-01 01:01:20.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-02-28 01:01:26.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-01 01:01:30.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct3 values ( \'2022-03-08 01:01:36.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )

        tdLog.info(f"=============== insert data into child table ct4 (y)")
        tdSql.execute(
            f'insert into ct4 values ( \'2020-10-21 01:01:01.000\', 1, 11111, 111, 11, 1.11, 11.11, 1, "binary1", "nchar1", now+1a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2020-12-31 01:01:01.000\', 2, 22222, 222, 22, 2.22, 22.22, 0, "binary2", "nchar2", now+2a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-01-01 01:01:06.000\', 3, 33333, 333, 33, 3.33, 33.33, 0, "binary3", "nchar3", now+3a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-05-07 01:01:10.000\', 4, 44444, 444, 44, 4.44, 44.44, 1, "binary4", "nchar4", now+4a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2021-09-30 01:01:16.000\', 5, 55555, 555, 55, 5.55, 55.55, 0, "binary5", "nchar5", now+5a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-02-01 01:01:20.000\', 6, 66666, 666, 66, 6.66, 66.66, 1, "binary6", "nchar6", now+6a )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-10-28 01:01:26.000\', 7, 00000, 000, 00, 0.00, 00.00, 1, "binary7", "nchar7", "1970-01-01 08:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-01 01:01:30.000\', 8, -88888, -888, -88, -8.88, -88.88, 0, "binary8", "nchar8", "1969-01-01 01:00:00.000" )'
        )
        tdSql.execute(
            f'insert into ct4 values ( \'2022-12-31 01:01:36.000\', 9, -99999999999999999, -999, -99, -9.99, -999999999999999999999.99, 1, "binary9", "nchar9", "1900-01-01 00:00:00.000" )'
        )

        tdLog.info(f"================ start query ======================")
        tdLog.info(f"================ query 1 where condition")
        tdSql.query(f"select * from ct3 where c1 < 5")
        tdLog.info(f"====> sql : select * from ct3 where c1 < 5")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 6)

        tdSql.query(f"select * from ct3 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 5)

        tdSql.query(f"select ts from ct3 where ts != 0")
        tdSql.query(f"select * from ct3 where ts <> 0")
        tdLog.info(f"====> sql : select * from ct3 where ts <> 0")
        # tdSql.checkRows(8)

        tdSql.query(f"select * from ct3 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select * from ct3 where c1 between 1 and 3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c7 between false and true")

        tdSql.query(f"select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c1 in ('true','false')")
        # tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c9 like "_char_"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c8 like "bi%"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(8)

        tdSql.query(f"select c1 from stb1 where c1 < 5")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 < 5")
        tdSql.checkRows(16)

        # tdSql.checkData(0, 1, 1)

        tdSql.query(f"select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 6)

        tdSql.query(
            f"select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdLog.info(
            f"====> sql : select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdSql.checkRows(32)

        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        # tdSql.checkRows(17)

        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select ts from stb1 where ts != 0")
        tdSql.query(f"select c1 from stb1 where ts <> 0")
        tdLog.info(f"====> sql : select c1 from stb1 where ts <> 0")
        # tdSql.checkRows(32)

        tdSql.query(f"select c1 from stb1 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 between 1 and 3")
        tdSql.checkRows(12)

        tdSql.query(f"select c1 from stb1 where c7 between false and true")

        tdSql.query(f"select c1 from stb1 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 in (1,2,3)")
        tdSql.checkRows(12)

        tdSql.query(f"select c1 from stb1 where c1 in ('true','false')")
        # tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c9 like "_char_"')
        tdLog.info(f'====> sql : select c1 from stb1 where c9 like "_char_"')
        tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c8 like "bi%"')
        tdLog.info(f'====> sql : select c1 from stb1 where c8 like "bi%"')
        tdSql.checkRows(32)

        tdLog.info(f"================ query 2 complex with where")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select abs(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select acos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select asin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select atan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select ceil(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select cos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select floor(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select log(c1,10) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select round(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select sin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select tan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        # =================================================
        tdLog.info(f"=============== stop and restart taosd")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=================== count all rows")
        tdSql.query(f"select count(c1) from stb1")
        tdLog.info(f"====> sql : select count(c1) from stb1")
        tdLog.info(f"====> rows: {tdSql.getData(0,0)}")
        # tdSql.checkData(0, 0, 33)

        tdLog.info(f"================ query 1 where condition")
        tdSql.query(f"select * from ct3 where c1 < 5")
        tdLog.info(f"====> sql : select * from ct3 where c1 < 5")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(1)

        tdSql.checkData(0, 1, 6)

        tdSql.query(f"select * from ct3 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2")
        tdLog.info(f"====> sql : select * from ct3 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(8)

        tdSql.checkData(0, 1, 1)

        tdSql.query(f"select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select * from ct3 where c1 >= 5 and c1 is not NULL")
        tdSql.checkRows(4)

        tdSql.checkData(0, 1, 5)

        tdSql.query(f"select ts from ct3 where ts != 0")
        tdSql.query(f"select * from ct3 where ts <> 0")
        tdLog.info(f"====> sql : select * from ct3 where ts <> 0")
        # tdSql.checkRows(8)

        tdSql.query(f"select * from ct3 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select * from ct3 where c1 between 1 and 3")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c7 between false and true")

        tdSql.query(f"select * from ct3 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(3)

        tdSql.query(f"select * from ct3 where c1 in ('true','false')")
        # tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c9 like "_char_"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(8)

        tdSql.query(f'select * from ct3 where c8 like "bi%"')
        tdLog.info(f"====> sql : select * from ct3 where c1 in (1,2,3)")
        tdSql.checkRows(8)

        tdSql.query(f"select c1 from stb1 where c1 < 5")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 < 5")
        tdSql.checkRows(16)

        # tdSql.checkData(0, 1, 1)

        tdSql.query(f"select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 > 5 and c1 <= 6")
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, 6)

        tdSql.query(
            f"select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdLog.info(
            f"====> sql : select c1 from stb1 where c1 >= 5 or c1 != 4 or c1 <> 3 or c1 = 2"
        )
        tdSql.checkRows(32)

        tdSql.checkData(0, 0, 1)

        tdSql.query(f"select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 >= 5 and c1 is not NULL")
        # tdSql.checkRows(17)

        tdSql.checkData(0, 0, 5)

        tdSql.query(f"select ts from stb1 where ts != 0")
        tdSql.query(f"select c1 from stb1 where ts <> 0")
        tdLog.info(f"====> sql : select c1 from stb1 where ts <> 0")
        # tdSql.checkRows(32)

        tdSql.query(f"select c1 from stb1 where c1 between 1 and 3")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 between 1 and 3")
        tdSql.checkRows(12)

        tdSql.query(f"select c1 from stb1 where c7 between false and true")

        tdSql.query(f"select c1 from stb1 where c1 in (1,2,3)")
        tdLog.info(f"====> sql : select c1 from stb1 where c1 in (1,2,3)")
        tdSql.checkRows(12)

        tdSql.query(f"select c1 from stb1 where c1 in ('true','false')")
        # tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c9 like "_char_"')
        tdLog.info(f'====> sql : select c1 from stb1 where c9 like "_char_"')
        tdSql.checkRows(32)

        tdSql.query(f'select c1 from stb1 where c8 like "bi%"')
        tdLog.info(f'====> sql : select c1 from stb1 where c8 like "bi%"')
        tdSql.checkRows(32)

        tdLog.info(f"================ query 2 complex with where")
        tdSql.query(
            f"select count(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select count(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select abs(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select abs(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select acos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select acos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select asin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select asin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select atan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select atan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select ceil(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select ceil(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select cos(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select cos(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select floor(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select floor(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select log(c1,10) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select log(c1,10) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select pow(c1,3) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select pow(c1,3) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select round(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select round(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select sqrt(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sqrt(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select sin(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select sin(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

        tdSql.query(
            f"select tan(c1) from ct3 where c1 > 2 group by c1 limit 1 offset 1"
        )
        tdLog.info(
            f"====> sql : select tan(c1) from ct3 where c1 > 2 group by c7 limit 1 offset 2"
        )
        # tdSql.checkRows(1)

    def Where(self):
        dbPrefix = "wh_db"
        tbPrefix = "wh_tb"
        mtPrefix = "wh_mt"
        tbNum = 10
        rowNum = 1000
        totalNum = tbNum * rowNum

        tdLog.info(f"=============== where.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"create database if not exists {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)"
        )

        half = tbNum / 2

        i = 0
        while i < half:
            tb = tbPrefix + str(i)
            nextSuffix = i + int(half)
            tb1 = tbPrefix + str(nextSuffix)

            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {mt} tags( {nextSuffix} )")

            x = 0
            while x < rowNum:
                y = x * 60000
                ms = 1600099200000 + y
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )  {tb1} values ({ms} , {c} , {c} , {c} , {c} , {c} , {c} , {c} , {binary} , {nchar} )"
                )
                x = x + 1

            i = i + 1

        i = 1
        tb = tbPrefix + str(i)

        ##
        tdSql.query(f"select * from {tb} where c7")

        # TBASE-654 : invalid filter expression cause server crashed
        tdSql.query(f"select count(*) from {tb} where c1<10 and c1<>2")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 90)

        tdSql.query(f"select * from {tb} where c7 = false")
        val = rowNum / 100
        tdSql.checkRows(val)

        tdSql.query(f"select * from {mt} where c7 = false")
        val = totalNum / 100
        tdSql.checkRows(val)

        tdSql.query(f"select distinct tbname from {mt}")
        tdSql.checkRows(tbNum)

        tdSql.query(f"select distinct tbname from {mt} where t1 < 2")
        tdSql.checkRows(2)

        # print $tbPrefix
        # $tb = $tbPrefix . 0
        # if $tdSql.getData(0,0,wh_tb1 then
        #  print expect wh_tb1, actual:$tdSql.getData(0,0)
        #  return -1
        # endi
        # $tb = $tbPrefix . 1
        # if $tdSql.getData(1,0,wh_tb0 then
        #  print expect wh_tb0, actual:$tdSql.getData(0,0)
        #  return -1
        # endi

        ## select specified columns

        tdLog.info(f"select c1 from {mt}")
        tdSql.query(f"select c1 from {mt}")

        tdLog.info(f"rows {tdSql.getRows()})")
        tdLog.info(f"totalNum {totalNum}")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select count(c1) from {mt}")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select c1 from {mt} where c1 >= 0")
        tdSql.checkRows(totalNum)

        tdSql.query(f"select count(c1) from {mt} where c1 <> -1")
        tdSql.checkData(0, 0, totalNum)

        tdSql.query(f"select count(c1) from {mt} where c1 <> -1 group by t1")
        tdSql.checkRows(tbNum)

        tdSql.checkData(0, 0, rowNum)

        ## like
        tdSql.error(f"select * from {mt} where c1 like 1")
        # sql_error select * from $mt where t1 like 1

        ## [TBASE-593]
        tdSql.execute(
            f"create table wh_mt1 (ts timestamp, c1 smallint, c2 int, c3 bigint, c4 float, c5 double, c6 tinyint, c7 binary(10), c8 nchar(10), c9 bool, c10 timestamp) tags (t1 binary(10), t2 smallint, t3 int, t4 bigint, t5 float, t6 double)"
        )
        tdSql.execute(
            f"create table wh_mt1_tb1 using wh_mt1 tags ('tb11', 1, 1, 1, 1, 1)"
        )
        tdSql.execute(
            f"insert into wh_mt1_tb1 values (now, 1, 1, 1, 1, 1, 1, 'binary', 'nchar', true, '2019-01-01 00:00:00.000')"
        )
        # sql_error select last(*) from wh_mt1 where c1 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c1 in ('1')
        # sql_error select last(*) from wh_mt1 where c2 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c2 in ('1')
        # sql_error select last(*) from wh_mt1 where c3 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c3 in ('1')
        # sql_error select last(*) from wh_mt1 where c4 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c4 in ('1')
        # sql_error select last(*) from wh_mt1 where c5 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c5 in ('1')
        # sql_error select last(*) from wh_mt1 where c6 in ('1')
        # sql_error select last(*) from wh_mt1_tb1 where c6 in ('1')
        # sql_error select last(*) from wh_mt1 where c7 in ('binary')
        # sql_error select last(*) from wh_mt1_tb1 where c7 in ('binary')
        # sql_error select last(*) from wh_mt1 where c8 in ('nchar')
        # sql_error select last(*) from wh_mt1_tb1 where c9 in (true, false)
        # sql_error select last(*) from wh_mt1 where c10 in ('2019-01-01 00:00:00.000')
        # sql_error select last(*) from wh_mt1_tb1 where c10 in ('2019-01-01 00:00:00.000')
        tdSql.query(f"select last(*) from wh_mt1 where c1 = 1")
        tdSql.checkRows(1)

        ## [TBASE-597]
        tdSql.execute(
            f"create table wh_mt2 (ts timestamp, c1 timestamp, c2 binary(10), c3 nchar(10)) tags (t1 binary(10))"
        )
        tdSql.execute(f"create table wh_mt2_tb1 using wh_mt2 tags ('wh_mt2_tb1')")

        # 2019-01-01 00:00:00.000     1546272000000
        # 2019-01-01 00:10:00.000     1546272600000
        # 2019-01-01 09:00:00.000     1546304400000
        # 2019-01-01 09:10:00.000     1546305000000
        tdSql.execute(
            f"insert into wh_mt2_tb1 values ('2019-01-01 00:00:00.000', '2019-01-01 09:00:00.000', 'binary10', 'nchar10')"
        )
        tdSql.execute(
            f"insert into wh_mt2_tb1 values ('2019-01-01 00:10:00.000', '2019-01-01 09:10:00.000', 'binary10', 'nchar10')"
        )

        tdSql.query(f"select * from wh_mt2_tb1 where c1 > 1546304400000")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:10:00.000")

        tdSql.query(f"select * from wh_mt2_tb1 where c1 > '2019-01-01 09:00:00.000'")
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:10:00.000")

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 > 1546304400000 and ts < '2019-01-01 00:10:00.000'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 > '2019-01-01 09:00:00.000' and ts < '2019-01-01 00:10:00.000'"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 >= 1546304400000 and c1 <= '2019-01-01 09:10:00.000'"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2019-01-01 00:00:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:00:00.000")

        tdSql.checkData(1, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(1, 1, "2019-01-01 09:10:00.000")

        tdSql.query(
            f"select * from wh_mt2_tb1 where c1 >= '2019-01-01 09:00:00.000' and c1 <= '2019-01-01 09:10:00.000'"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, "2019-01-01 00:00:00.000")

        tdSql.checkData(0, 1, "2019-01-01 09:00:00.000")

        tdSql.checkData(1, 0, "2019-01-01 00:10:00.000")

        tdSql.checkData(1, 1, "2019-01-01 09:10:00.000")

        tdSql.execute(
            f"create table tb_where_NULL (ts timestamp, c1 float, c2 binary(10))"
        )

        tdLog.info(f"===================>td-1604")
        tdSql.error(f"insert into tb_where_NULL values(?, ?, ?)")
        tdSql.error(f"insert into tb_where_NULL values(now, 1, ?)")
        tdSql.error(f"insert into tb_where_NULL values(?, 1, '')")
        tdSql.error(f"insert into tb_where_NULL values(now, ?, '12')")

        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:00.000', 1, 'val1')"
        )
        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:01.000', NULL, NULL)"
        )
        tdSql.execute(
            f"insert into tb_where_NULL values ('2019-01-01 09:00:02.000', 2, 'val2')"
        )
        tdSql.query(f"select * from tb_where_NULL where c1 = NULL")
        tdSql.query(f"select * from tb_where_NULL where c1 <> NULL")
        tdSql.query(f"select * from tb_where_NULL where c1 < NULL")
        tdSql.query(f'select * from tb_where_NULL where c1 = "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 <> "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 <> "nulL"')
        tdSql.query(f'select * from tb_where_NULL where c1 > "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c1 >= "NULL"')
        tdSql.query(f'select * from tb_where_NULL where c2 = "NULL"')
        tdSql.checkRows(0)

        tdSql.query(f'select * from tb_where_NULL where c2 <> "NULL"')
        tdSql.checkRows(2)

        tdSql.query(f'select * from tb_where_NULL where c2 <> "nUll"')
        tdSql.checkRows(2)

        tdLog.info(f"==========tbase-1363")
        # sql create table $mt (ts timestamp, c1 int, c2 float, c3 bigint, c4 smallint, c5 tinyint, c6 double, c7 bool, c8 binary(10), c9 nchar(9)) TAGS(t1 int)

        i = 0
        while i < 1:
            tb = "test_null_filter"
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            while x < 10000:
                y = x * 60000
                ms = 1601481600000 + y
                c = x % 100
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tdSql.execute(
                    f"insert into {tb} values ({ms} , null , null , null , null , null , null , null , null , null )"
                )
                x = x + 1

            i = i + 1

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(
            f"select * from wh_mt0 where c3 = 'abc' and tbname in ('test_null_filter');"
        )

        tdSql.query(
            f"select * from wh_mt0 where c3 = '1' and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(0)

        tdSql.query(f"select * from wh_mt0 where c3 = 1;")
        tdLog.info(f"{tdSql.getRows()}) -> 100")
        tdLog.info(f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}")
        tdSql.checkRows(100)

        tdSql.query(
            f"select * from wh_mt0 where c3 is null and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(10000)

        tdSql.query(
            f"select * from wh_mt0 where c3 is not null and tbname in ('test_null_filter');"
        )
        tdSql.checkRows(0)

        tdLog.info(f"==========================>td-3318")
        tdSql.execute(f"create table tu(ts timestamp,  k int, b binary(12))")
        tdSql.execute(f"insert into tu values(now, 1, 'abc')")
        tdSql.query(f"select stddev(k) from tu where b <>'abc' interval(1s)")
        tdSql.checkRows(0)

        tdLog.info(f"==========================> td-4783,td-4792")
        tdSql.execute(f"create table where_ts(ts timestamp, f int)")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:22:00', 1);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:23:00', 2);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:24:00', 3);")
        tdSql.execute(f"insert into where_ts values('2021-06-19 16:25:00', 1);")
        tdSql.query(
            f"select * from (select * from where_ts) where ts<'2021-06-19 16:25:00' and ts>'2021-06-19 16:22:00' order by ts;"
        )
        tdSql.checkRows(2)

        tdLog.info(f"{tdSql.getData(0,0)}, {tdSql.getData(0,1)}")
        tdSql.checkData(0, 1, 2)

        tdSql.execute(f"insert into where_ts values(now, 5);")
        tdSql.query(f"select * from (select * from where_ts) where ts<now;")
        tdSql.checkRows(5)
