import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError8:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error8(self):
        """valgrind check error 8

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError8.sim

        """

        clusterComCheck.checkDnodes(1)

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
            rowNum = 5
            while x < rowNum:
                ts = ts0 + x
                a = a + 1
                b = b + 1
                c = c + 1
                d = x / 10
                tin = rowNum
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                tbname = "'tb4_" + str(i) + "'"
                tdSql.execute(
                    f"insert into {tbname} values ( {ts} , {a} , {b} , {c} , {d} , {d} , {c} , true, {binary} , {nchar} , {binary} )"
                )
                x = x + 1

            i = i + 1
            ts0 = ts0 + 259200000

        tdLog.info(f"============== query")
        tdSql.query(
            f"select a.ts,a.c1,a.c8 from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and ((a.c1 > 50 and a.c1 < 60) or (b.c2 > 60));;"
        )
        tdSql.query(
            f"select * from stb1 where (c6 > 3.0 or c6 < 60) and c6 > 50 and (c6 != 53 or c6 != 63);;"
        )
        tdSql.query(
            f"select ts,c1 from stb1 where (c1 > 60 or c1 < 10 or (c1 > 20 and c1 < 30)) and ts > '2021-05-05 18:19:00.000' and ts < '2021-05-05 18:19:25.000' and c1 != 21 and c1 != 22 order by ts;"
        )
        tdSql.query(
            f"select a.* from (select * from stb1 where c7=true) a, (select * from stb1 where c1 > 30) b where a.ts=b.ts and a.c1 > 50 order by ts;;"
        )
        tdSql.query(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and (a.c1 < 10 or a.c1 > 30) and (b.u1 < 5 or b.u1 > 5) order by a.ts;;"
        )
        tdSql.query(
            f"select a.ts,b.ts,a.c1,b.u1,b.u2 from (select * from stb1) a, (select * from stb2) b where a.ts=b.ts and a.c1 < 30 and b.u1 > 1 and a.c1 > 10 and b.u1 < 8 and b.u1<>5 order by a.ts;;"
        )
        tdSql.query(
            f"select tb1.ts,tb1.*,tb2_1.* from tb1, tb2_1 where tb1.ts=tb2_1.ts and tb1.ts >= '2021-05-05 18:19:03.000' and tb1.c7=false and tb2_1.u3>4 order by tb1.ts;;"
        )
        tdSql.query(
            f"select stb1.ts,stb1.c1,stb1.t1,stb2.ts,stb2.u1,stb2.t4 from stb1, stb2 where stb1.ts=stb2.ts and stb1.t1 = stb2.t4 order by stb1.ts;;"
        )
        tdSql.query(f"select count(*) from stb1 where tbname like 'tb%' or c1 > 0;;")
        tdSql.query(
            f"select * from stb1 where tbname like 'tb%' and (t1=1 or t2=2 or t3=3) and t1 > 2 order by ts;;"
        )
