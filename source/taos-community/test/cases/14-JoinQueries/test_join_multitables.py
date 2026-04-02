from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestJoinMultitables:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_join_multitables(self):
        """Join multi-tables

        1.Create super table st0-stb with same schema but different tag numbers
        2.Create child tables from super tables with different tag values
        3.Insert data into child tables with same timestamps
        4.Join tables on timestamps and tag columns from different super tables 
        5. Check the result of join correctly

        Catalog:
            - Query:Join

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/parser/join_multitables.sim

        """

        tdLog.info(f"======================== dnode1 start")
        db = "testdb"
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.execute(
            f"create stable st0 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st1 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st2 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st3 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st4 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st5 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st6 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st7 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st8 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable st9 (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable sta (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )
        tdSql.execute(
            f"create stable stb (ts timestamp, f1 int, f2 double, f3 binary(10)) tags(id1 int, id2 smallint, id3 double, id4 bool, id5 binary(5));"
        )

        tdSql.execute(f"create table tb0_1 using st0 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb0_2 using st0 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb0_3 using st0 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb0_4 using st0 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb0_5 using st0 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb1_1 using st1 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb1_2 using st1 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb1_3 using st1 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb1_4 using st1 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb1_5 using st1 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb2_1 using st2 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb2_2 using st2 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb2_3 using st2 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb2_4 using st2 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb2_5 using st2 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb3_1 using st3 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb3_2 using st3 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb3_3 using st3 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb3_4 using st3 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb3_5 using st3 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb4_1 using st4 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb4_2 using st4 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb4_3 using st4 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb4_4 using st4 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb4_5 using st4 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb5_1 using st5 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb5_2 using st5 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb5_3 using st5 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb5_4 using st5 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb5_5 using st5 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb6_1 using st6 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb6_2 using st6 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb6_3 using st6 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb6_4 using st6 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb6_5 using st6 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb7_1 using st7 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb7_2 using st7 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb7_3 using st7 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb7_4 using st7 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb7_5 using st7 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb8_1 using st8 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb8_2 using st8 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb8_3 using st8 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb8_4 using st8 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb8_5 using st8 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tb9_1 using st9 tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tb9_2 using st9 tags(1,2,3.0,false,'4');")
        tdSql.execute(f"create table tb9_3 using st9 tags(2,3,4.0,true,'5');")
        tdSql.execute(f"create table tb9_4 using st9 tags(3,4,5.0,false,'6');")
        tdSql.execute(f"create table tb9_5 using st9 tags(4,5,6.0,true,'7');")

        tdSql.execute(f"create table tba_1 using sta tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tba_2 using sta tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tba_3 using sta tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tba_4 using sta tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tba_5 using sta tags(0,1,2.0,true,'3');")

        tdSql.execute(f"create table tbb_1 using stb tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tbb_2 using stb tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tbb_3 using stb tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tbb_4 using stb tags(0,1,2.0,true,'3');")
        tdSql.execute(f"create table tbb_5 using stb tags(0,1,2.0,true,'3');")

        tdSql.execute(
            f"insert into tb0_1 values('2021-03-01 01:00:00.000', 9901,9901.0,'01');"
        )
        tdSql.execute(
            f"insert into tb0_1 values('2021-03-02 01:00:00.000', 9901,9901.0,'01');"
        )
        tdSql.execute(
            f"insert into tb0_1 values('2021-03-03 01:00:00.000', 9901,9901.0,'01');"
        )
        tdSql.execute(
            f"insert into tb0_1 values('2021-03-04 01:00:00.000', 9901,9901.0,'01');"
        )
        tdSql.execute(
            f"insert into tb0_1 values('2021-03-05 01:00:00.000', 9901,9901.0,'01');"
        )
        tdSql.execute(
            f"insert into tb0_2 values('2021-03-01 02:00:00.000', 9902,9902.0,'02');"
        )
        tdSql.execute(
            f"insert into tb0_2 values('2021-03-02 02:00:00.000', 9902,9902.0,'02');"
        )
        tdSql.execute(
            f"insert into tb0_2 values('2021-03-03 02:00:00.000', 9902,9902.0,'02');"
        )
        tdSql.execute(
            f"insert into tb0_2 values('2021-03-04 02:00:00.000', 9902,9902.0,'02');"
        )
        tdSql.execute(
            f"insert into tb0_2 values('2021-03-05 02:00:00.000', 9902,9902.0,'02');"
        )
        tdSql.execute(
            f"insert into tb0_3 values('2021-03-01 03:00:00.000', 9903,9903.0,'03');"
        )
        tdSql.execute(
            f"insert into tb0_3 values('2021-03-02 03:00:00.000', 9903,9903.0,'03');"
        )
        tdSql.execute(
            f"insert into tb0_3 values('2021-03-03 03:00:00.000', 9903,9903.0,'03');"
        )
        tdSql.execute(
            f"insert into tb0_3 values('2021-03-04 03:00:00.000', 9903,9903.0,'03');"
        )
        tdSql.execute(
            f"insert into tb0_3 values('2021-03-05 03:00:00.000', 9903,9903.0,'03');"
        )
        tdSql.execute(
            f"insert into tb0_4 values('2021-03-01 04:00:00.000', 9904,9904.0,'04');"
        )
        tdSql.execute(
            f"insert into tb0_4 values('2021-03-02 04:00:00.000', 9904,9904.0,'04');"
        )
        tdSql.execute(
            f"insert into tb0_4 values('2021-03-03 04:00:00.000', 9904,9904.0,'04');"
        )
        tdSql.execute(
            f"insert into tb0_4 values('2021-03-04 04:00:00.000', 9904,9904.0,'04');"
        )
        tdSql.execute(
            f"insert into tb0_4 values('2021-03-05 04:00:00.000', 9904,9904.0,'04');"
        )
        tdSql.execute(
            f"insert into tb0_5 values('2021-03-01 05:00:00.000', 9905,9905.0,'05');"
        )
        tdSql.execute(
            f"insert into tb0_5 values('2021-03-02 05:00:00.000', 9905,9905.0,'05');"
        )
        tdSql.execute(
            f"insert into tb0_5 values('2021-03-03 05:00:00.000', 9905,9905.0,'05');"
        )
        tdSql.execute(
            f"insert into tb0_5 values('2021-03-04 05:00:00.000', 9905,9905.0,'05');"
        )
        tdSql.execute(
            f"insert into tb0_5 values('2021-03-05 05:00:00.000', 9905,9905.0,'05');"
        )

        tdSql.execute(
            f"insert into tb1_1 values('2021-03-01 01:00:00.000', 9911,9911.0,'11');"
        )
        tdSql.execute(
            f"insert into tb1_1 values('2021-03-02 01:00:00.000', 9911,9911.0,'11');"
        )
        tdSql.execute(
            f"insert into tb1_1 values('2021-03-03 01:00:00.000', 9911,9911.0,'11');"
        )
        tdSql.execute(
            f"insert into tb1_1 values('2021-03-04 01:00:00.000', 9911,9911.0,'11');"
        )
        tdSql.execute(
            f"insert into tb1_1 values('2021-03-05 01:00:00.000', 9911,9911.0,'11');"
        )
        tdSql.execute(
            f"insert into tb1_2 values('2021-03-01 02:00:00.000', 9912,9912.0,'12');"
        )
        tdSql.execute(
            f"insert into tb1_2 values('2021-03-02 02:00:00.000', 9912,9912.0,'12');"
        )
        tdSql.execute(
            f"insert into tb1_2 values('2021-03-03 02:00:00.000', 9912,9912.0,'12');"
        )
        tdSql.execute(
            f"insert into tb1_2 values('2021-03-04 02:00:00.000', 9912,9912.0,'12');"
        )
        tdSql.execute(
            f"insert into tb1_2 values('2021-03-05 02:00:00.000', 9912,9912.0,'12');"
        )
        tdSql.execute(
            f"insert into tb1_3 values('2021-03-01 03:00:00.000', 9913,9913.0,'13');"
        )
        tdSql.execute(
            f"insert into tb1_3 values('2021-03-02 03:00:00.000', 9913,9913.0,'13');"
        )
        tdSql.execute(
            f"insert into tb1_3 values('2021-03-03 03:00:00.000', 9913,9913.0,'13');"
        )
        tdSql.execute(
            f"insert into tb1_3 values('2021-03-04 03:00:00.000', 9913,9913.0,'13');"
        )
        tdSql.execute(
            f"insert into tb1_3 values('2021-03-05 03:00:00.000', 9913,9913.0,'13');"
        )
        tdSql.execute(
            f"insert into tb1_4 values('2021-03-01 04:00:00.000', 9914,9914.0,'14');"
        )
        tdSql.execute(
            f"insert into tb1_4 values('2021-03-02 04:00:00.000', 9914,9914.0,'14');"
        )
        tdSql.execute(
            f"insert into tb1_4 values('2021-03-03 04:00:00.000', 9914,9914.0,'14');"
        )
        tdSql.execute(
            f"insert into tb1_4 values('2021-03-04 04:00:00.000', 9914,9914.0,'14');"
        )
        tdSql.execute(
            f"insert into tb1_4 values('2021-03-05 04:00:00.000', 9914,9914.0,'14');"
        )
        tdSql.execute(
            f"insert into tb1_5 values('2021-03-01 05:00:00.000', 9915,9915.0,'15');"
        )
        tdSql.execute(
            f"insert into tb1_5 values('2021-03-02 05:00:00.000', 9915,9915.0,'15');"
        )
        tdSql.execute(
            f"insert into tb1_5 values('2021-03-03 05:00:00.000', 9915,9915.0,'15');"
        )
        tdSql.execute(
            f"insert into tb1_5 values('2021-03-04 05:00:00.000', 9915,9915.0,'15');"
        )
        tdSql.execute(
            f"insert into tb1_5 values('2021-03-05 05:00:00.000', 9915,9915.0,'15');"
        )

        tdSql.execute(
            f"insert into tb2_1 values('2021-03-01 01:00:00.000', 9921,9921.0,'21');"
        )
        tdSql.execute(
            f"insert into tb2_1 values('2021-03-02 01:00:00.000', 9921,9921.0,'21');"
        )
        tdSql.execute(
            f"insert into tb2_1 values('2021-03-03 01:00:00.000', 9921,9921.0,'21');"
        )
        tdSql.execute(
            f"insert into tb2_1 values('2021-03-04 01:00:00.000', 9921,9921.0,'21');"
        )
        tdSql.execute(
            f"insert into tb2_1 values('2021-03-05 01:00:00.000', 9921,9921.0,'21');"
        )
        tdSql.execute(
            f"insert into tb2_2 values('2021-03-01 02:00:00.000', 9922,9922.0,'22');"
        )
        tdSql.execute(
            f"insert into tb2_2 values('2021-03-02 02:00:00.000', 9922,9922.0,'22');"
        )
        tdSql.execute(
            f"insert into tb2_2 values('2021-03-03 02:00:00.000', 9922,9922.0,'22');"
        )
        tdSql.execute(
            f"insert into tb2_2 values('2021-03-04 02:00:00.000', 9922,9922.0,'22');"
        )
        tdSql.execute(
            f"insert into tb2_2 values('2021-03-05 02:00:00.000', 9922,9922.0,'22');"
        )
        tdSql.execute(
            f"insert into tb2_3 values('2021-03-01 03:00:00.000', 9923,9923.0,'23');"
        )
        tdSql.execute(
            f"insert into tb2_3 values('2021-03-02 03:00:00.000', 9923,9923.0,'23');"
        )
        tdSql.execute(
            f"insert into tb2_3 values('2021-03-03 03:00:00.000', 9923,9923.0,'23');"
        )
        tdSql.execute(
            f"insert into tb2_3 values('2021-03-04 03:00:00.000', 9923,9923.0,'23');"
        )
        tdSql.execute(
            f"insert into tb2_3 values('2021-03-05 03:00:00.000', 9923,9923.0,'23');"
        )
        tdSql.execute(
            f"insert into tb2_4 values('2021-03-01 04:00:00.000', 9924,9924.0,'24');"
        )
        tdSql.execute(
            f"insert into tb2_4 values('2021-03-02 04:00:00.000', 9924,9924.0,'24');"
        )
        tdSql.execute(
            f"insert into tb2_4 values('2021-03-03 04:00:00.000', 9924,9924.0,'24');"
        )
        tdSql.execute(
            f"insert into tb2_4 values('2021-03-04 04:00:00.000', 9924,9924.0,'24');"
        )
        tdSql.execute(
            f"insert into tb2_4 values('2021-03-05 04:00:00.000', 9924,9924.0,'24');"
        )
        tdSql.execute(
            f"insert into tb2_5 values('2021-03-01 05:00:00.000', 9925,9925.0,'25');"
        )
        tdSql.execute(
            f"insert into tb2_5 values('2021-03-02 05:00:00.000', 9925,9925.0,'25');"
        )
        tdSql.execute(
            f"insert into tb2_5 values('2021-03-03 05:00:00.000', 9925,9925.0,'25');"
        )
        tdSql.execute(
            f"insert into tb2_5 values('2021-03-04 05:00:00.000', 9925,9925.0,'25');"
        )
        tdSql.execute(
            f"insert into tb2_5 values('2021-03-05 05:00:00.000', 9925,9925.0,'25');"
        )

        tdSql.execute(
            f"insert into tb3_1 values('2021-03-01 01:00:00.000', 9931,9931.0,'31');"
        )
        tdSql.execute(
            f"insert into tb3_1 values('2021-03-02 01:00:00.000', 9931,9931.0,'31');"
        )
        tdSql.execute(
            f"insert into tb3_1 values('2021-03-03 01:00:00.000', 9931,9931.0,'31');"
        )
        tdSql.execute(
            f"insert into tb3_1 values('2021-03-04 01:00:00.000', 9931,9931.0,'31');"
        )
        tdSql.execute(
            f"insert into tb3_1 values('2021-03-05 01:00:00.000', 9931,9931.0,'31');"
        )
        tdSql.execute(
            f"insert into tb3_2 values('2021-03-01 02:00:00.000', 9932,9932.0,'32');"
        )
        tdSql.execute(
            f"insert into tb3_2 values('2021-03-02 02:00:00.000', 9932,9932.0,'32');"
        )
        tdSql.execute(
            f"insert into tb3_2 values('2021-03-03 02:00:00.000', 9932,9932.0,'32');"
        )
        tdSql.execute(
            f"insert into tb3_2 values('2021-03-04 02:00:00.000', 9932,9932.0,'32');"
        )
        tdSql.execute(
            f"insert into tb3_2 values('2021-03-05 02:00:00.000', 9932,9932.0,'32');"
        )
        tdSql.execute(
            f"insert into tb3_3 values('2021-03-01 03:00:00.000', 9933,9933.0,'33');"
        )
        tdSql.execute(
            f"insert into tb3_3 values('2021-03-02 03:00:00.000', 9933,9933.0,'33');"
        )
        tdSql.execute(
            f"insert into tb3_3 values('2021-03-03 03:00:00.000', 9933,9933.0,'33');"
        )
        tdSql.execute(
            f"insert into tb3_3 values('2021-03-04 03:00:00.000', 9933,9933.0,'33');"
        )
        tdSql.execute(
            f"insert into tb3_3 values('2021-03-05 03:00:00.000', 9933,9933.0,'33');"
        )
        tdSql.execute(
            f"insert into tb3_4 values('2021-03-01 04:00:00.000', 9934,9934.0,'34');"
        )
        tdSql.execute(
            f"insert into tb3_4 values('2021-03-02 04:00:00.000', 9934,9934.0,'34');"
        )
        tdSql.execute(
            f"insert into tb3_4 values('2021-03-03 04:00:00.000', 9934,9934.0,'34');"
        )
        tdSql.execute(
            f"insert into tb3_4 values('2021-03-04 04:00:00.000', 9934,9934.0,'34');"
        )
        tdSql.execute(
            f"insert into tb3_4 values('2021-03-05 04:00:00.000', 9934,9934.0,'34');"
        )
        tdSql.execute(
            f"insert into tb3_5 values('2021-03-01 05:00:00.000', 9935,9935.0,'35');"
        )
        tdSql.execute(
            f"insert into tb3_5 values('2021-03-02 05:00:00.000', 9935,9935.0,'35');"
        )
        tdSql.execute(
            f"insert into tb3_5 values('2021-03-03 05:00:00.000', 9935,9935.0,'35');"
        )
        tdSql.execute(
            f"insert into tb3_5 values('2021-03-04 05:00:00.000', 9935,9935.0,'35');"
        )
        tdSql.execute(
            f"insert into tb3_5 values('2021-03-05 05:00:00.000', 9935,9935.0,'35');"
        )

        tdSql.execute(
            f"insert into tb4_1 values('2021-03-01 01:00:00.000', 9941,9941.0,'41');"
        )
        tdSql.execute(
            f"insert into tb4_1 values('2021-03-02 01:00:00.000', 9941,9941.0,'41');"
        )
        tdSql.execute(
            f"insert into tb4_1 values('2021-03-03 01:00:00.000', 9941,9941.0,'41');"
        )
        tdSql.execute(
            f"insert into tb4_1 values('2021-03-04 01:00:00.000', 9941,9941.0,'41');"
        )
        tdSql.execute(
            f"insert into tb4_1 values('2021-03-05 01:00:00.000', 9941,9941.0,'41');"
        )
        tdSql.execute(
            f"insert into tb4_2 values('2021-03-01 02:00:00.000', 9942,9942.0,'42');"
        )
        tdSql.execute(
            f"insert into tb4_2 values('2021-03-02 02:00:00.000', 9942,9942.0,'42');"
        )
        tdSql.execute(
            f"insert into tb4_2 values('2021-03-03 02:00:00.000', 9942,9942.0,'42');"
        )
        tdSql.execute(
            f"insert into tb4_2 values('2021-03-04 02:00:00.000', 9942,9942.0,'42');"
        )
        tdSql.execute(
            f"insert into tb4_2 values('2021-03-05 02:00:00.000', 9942,9942.0,'42');"
        )
        tdSql.execute(
            f"insert into tb4_3 values('2021-03-01 03:00:00.000', 9943,9943.0,'43');"
        )
        tdSql.execute(
            f"insert into tb4_3 values('2021-03-02 03:00:00.000', 9943,9943.0,'43');"
        )
        tdSql.execute(
            f"insert into tb4_3 values('2021-03-03 03:00:00.000', 9943,9943.0,'43');"
        )
        tdSql.execute(
            f"insert into tb4_3 values('2021-03-04 03:00:00.000', 9943,9943.0,'43');"
        )
        tdSql.execute(
            f"insert into tb4_3 values('2021-03-05 03:00:00.000', 9943,9943.0,'43');"
        )
        tdSql.execute(
            f"insert into tb4_4 values('2021-03-01 04:00:00.000', 9944,9944.0,'44');"
        )
        tdSql.execute(
            f"insert into tb4_4 values('2021-03-02 04:00:00.000', 9944,9944.0,'44');"
        )
        tdSql.execute(
            f"insert into tb4_4 values('2021-03-03 04:00:00.000', 9944,9944.0,'44');"
        )
        tdSql.execute(
            f"insert into tb4_4 values('2021-03-04 04:00:00.000', 9944,9944.0,'44');"
        )
        tdSql.execute(
            f"insert into tb4_4 values('2021-03-05 04:00:00.000', 9944,9944.0,'44');"
        )
        tdSql.execute(
            f"insert into tb4_5 values('2021-03-01 05:00:00.000', 9945,9945.0,'45');"
        )
        tdSql.execute(
            f"insert into tb4_5 values('2021-03-02 05:00:00.000', 9945,9945.0,'45');"
        )
        tdSql.execute(
            f"insert into tb4_5 values('2021-03-03 05:00:00.000', 9945,9945.0,'45');"
        )
        tdSql.execute(
            f"insert into tb4_5 values('2021-03-04 05:00:00.000', 9945,9945.0,'45');"
        )
        tdSql.execute(
            f"insert into tb4_5 values('2021-03-05 05:00:00.000', 9945,9945.0,'45');"
        )

        tdSql.execute(
            f"insert into tb5_1 values('2021-03-01 01:00:00.000', 9951,9951.0,'51');"
        )
        tdSql.execute(
            f"insert into tb5_1 values('2021-03-02 01:00:00.000', 9951,9951.0,'51');"
        )
        tdSql.execute(
            f"insert into tb5_1 values('2021-03-03 01:00:00.000', 9951,9951.0,'51');"
        )
        tdSql.execute(
            f"insert into tb5_1 values('2021-03-04 01:00:00.000', 9951,9951.0,'51');"
        )
        tdSql.execute(
            f"insert into tb5_1 values('2021-03-05 01:00:00.000', 9951,9951.0,'51');"
        )
        tdSql.execute(
            f"insert into tb5_2 values('2021-03-01 02:00:00.000', 9952,9952.0,'52');"
        )
        tdSql.execute(
            f"insert into tb5_2 values('2021-03-02 02:00:00.000', 9952,9952.0,'52');"
        )
        tdSql.execute(
            f"insert into tb5_2 values('2021-03-03 02:00:00.000', 9952,9952.0,'52');"
        )
        tdSql.execute(
            f"insert into tb5_2 values('2021-03-04 02:00:00.000', 9952,9952.0,'52');"
        )
        tdSql.execute(
            f"insert into tb5_2 values('2021-03-05 02:00:00.000', 9952,9952.0,'52');"
        )
        tdSql.execute(
            f"insert into tb5_3 values('2021-03-01 03:00:00.000', 9953,9953.0,'53');"
        )
        tdSql.execute(
            f"insert into tb5_3 values('2021-03-02 03:00:00.000', 9953,9953.0,'53');"
        )
        tdSql.execute(
            f"insert into tb5_3 values('2021-03-03 03:00:00.000', 9953,9953.0,'53');"
        )
        tdSql.execute(
            f"insert into tb5_3 values('2021-03-04 03:00:00.000', 9953,9953.0,'53');"
        )
        tdSql.execute(
            f"insert into tb5_3 values('2021-03-05 03:00:00.000', 9953,9953.0,'53');"
        )
        tdSql.execute(
            f"insert into tb5_4 values('2021-03-01 04:00:00.000', 9954,9954.0,'54');"
        )
        tdSql.execute(
            f"insert into tb5_4 values('2021-03-02 04:00:00.000', 9954,9954.0,'54');"
        )
        tdSql.execute(
            f"insert into tb5_4 values('2021-03-03 04:00:00.000', 9954,9954.0,'54');"
        )
        tdSql.execute(
            f"insert into tb5_4 values('2021-03-04 04:00:00.000', 9954,9954.0,'54');"
        )
        tdSql.execute(
            f"insert into tb5_4 values('2021-03-05 04:00:00.000', 9954,9954.0,'54');"
        )
        tdSql.execute(
            f"insert into tb5_5 values('2021-03-01 05:00:00.000', 9955,9955.0,'55');"
        )
        tdSql.execute(
            f"insert into tb5_5 values('2021-03-02 05:00:00.000', 9955,9955.0,'55');"
        )
        tdSql.execute(
            f"insert into tb5_5 values('2021-03-03 05:00:00.000', 9955,9955.0,'55');"
        )
        tdSql.execute(
            f"insert into tb5_5 values('2021-03-04 05:00:00.000', 9955,9955.0,'55');"
        )
        tdSql.execute(
            f"insert into tb5_5 values('2021-03-05 05:00:00.000', 9955,9955.0,'55');"
        )

        tdSql.execute(
            f"insert into tb6_1 values('2021-03-01 01:00:00.000', 9961,9961.0,'61');"
        )
        tdSql.execute(
            f"insert into tb6_1 values('2021-03-02 01:00:00.000', 9961,9961.0,'61');"
        )
        tdSql.execute(
            f"insert into tb6_1 values('2021-03-03 01:00:00.000', 9961,9961.0,'61');"
        )
        tdSql.execute(
            f"insert into tb6_1 values('2021-03-04 01:00:00.000', 9961,9961.0,'61');"
        )
        tdSql.execute(
            f"insert into tb6_1 values('2021-03-05 01:00:00.000', 9961,9961.0,'61');"
        )
        tdSql.execute(
            f"insert into tb6_2 values('2021-03-01 02:00:00.000', 9962,9962.0,'62');"
        )
        tdSql.execute(
            f"insert into tb6_2 values('2021-03-02 02:00:00.000', 9962,9962.0,'62');"
        )
        tdSql.execute(
            f"insert into tb6_2 values('2021-03-03 02:00:00.000', 9962,9962.0,'62');"
        )
        tdSql.execute(
            f"insert into tb6_2 values('2021-03-04 02:00:00.000', 9962,9962.0,'62');"
        )
        tdSql.execute(
            f"insert into tb6_2 values('2021-03-05 02:00:00.000', 9962,9962.0,'62');"
        )
        tdSql.execute(
            f"insert into tb6_3 values('2021-03-01 03:00:00.000', 9963,9963.0,'63');"
        )
        tdSql.execute(
            f"insert into tb6_3 values('2021-03-02 03:00:00.000', 9963,9963.0,'63');"
        )
        tdSql.execute(
            f"insert into tb6_3 values('2021-03-03 03:00:00.000', 9963,9963.0,'63');"
        )
        tdSql.execute(
            f"insert into tb6_3 values('2021-03-04 03:00:00.000', 9963,9963.0,'63');"
        )
        tdSql.execute(
            f"insert into tb6_3 values('2021-03-05 03:00:00.000', 9963,9963.0,'63');"
        )
        tdSql.execute(
            f"insert into tb6_4 values('2021-03-01 04:00:00.000', 9964,9964.0,'64');"
        )
        tdSql.execute(
            f"insert into tb6_4 values('2021-03-02 04:00:00.000', 9964,9964.0,'64');"
        )
        tdSql.execute(
            f"insert into tb6_4 values('2021-03-03 04:00:00.000', 9964,9964.0,'64');"
        )
        tdSql.execute(
            f"insert into tb6_4 values('2021-03-04 04:00:00.000', 9964,9964.0,'64');"
        )
        tdSql.execute(
            f"insert into tb6_4 values('2021-03-05 04:00:00.000', 9964,9964.0,'64');"
        )
        tdSql.execute(
            f"insert into tb6_5 values('2021-03-01 05:00:00.000', 9965,9965.0,'65');"
        )
        tdSql.execute(
            f"insert into tb6_5 values('2021-03-02 05:00:00.000', 9965,9965.0,'65');"
        )
        tdSql.execute(
            f"insert into tb6_5 values('2021-03-03 05:00:00.000', 9965,9965.0,'65');"
        )
        tdSql.execute(
            f"insert into tb6_5 values('2021-03-04 05:00:00.000', 9965,9965.0,'65');"
        )
        tdSql.execute(
            f"insert into tb6_5 values('2021-03-05 05:00:00.000', 9965,9965.0,'65');"
        )

        tdSql.execute(
            f"insert into tb7_1 values('2021-03-01 01:00:00.000', 9971,9971.0,'71');"
        )
        tdSql.execute(
            f"insert into tb7_1 values('2021-03-02 01:00:00.000', 9971,9971.0,'71');"
        )
        tdSql.execute(
            f"insert into tb7_1 values('2021-03-03 01:00:00.000', 9971,9971.0,'71');"
        )
        tdSql.execute(
            f"insert into tb7_1 values('2021-03-04 01:00:00.000', 9971,9971.0,'71');"
        )
        tdSql.execute(
            f"insert into tb7_1 values('2021-03-05 01:00:00.000', 9971,9971.0,'71');"
        )
        tdSql.execute(
            f"insert into tb7_2 values('2021-03-01 02:00:00.000', 9972,9972.0,'72');"
        )
        tdSql.execute(
            f"insert into tb7_2 values('2021-03-02 02:00:00.000', 9972,9972.0,'72');"
        )
        tdSql.execute(
            f"insert into tb7_2 values('2021-03-03 02:00:00.000', 9972,9972.0,'72');"
        )
        tdSql.execute(
            f"insert into tb7_2 values('2021-03-04 02:00:00.000', 9972,9972.0,'72');"
        )
        tdSql.execute(
            f"insert into tb7_2 values('2021-03-05 02:00:00.000', 9972,9972.0,'72');"
        )
        tdSql.execute(
            f"insert into tb7_3 values('2021-03-01 03:00:00.000', 9973,9973.0,'73');"
        )
        tdSql.execute(
            f"insert into tb7_3 values('2021-03-02 03:00:00.000', 9973,9973.0,'73');"
        )
        tdSql.execute(
            f"insert into tb7_3 values('2021-03-03 03:00:00.000', 9973,9973.0,'73');"
        )
        tdSql.execute(
            f"insert into tb7_3 values('2021-03-04 03:00:00.000', 9973,9973.0,'73');"
        )
        tdSql.execute(
            f"insert into tb7_3 values('2021-03-05 03:00:00.000', 9973,9973.0,'73');"
        )
        tdSql.execute(
            f"insert into tb7_4 values('2021-03-01 04:00:00.000', 9974,9974.0,'74');"
        )
        tdSql.execute(
            f"insert into tb7_4 values('2021-03-02 04:00:00.000', 9974,9974.0,'74');"
        )
        tdSql.execute(
            f"insert into tb7_4 values('2021-03-03 04:00:00.000', 9974,9974.0,'74');"
        )
        tdSql.execute(
            f"insert into tb7_4 values('2021-03-04 04:00:00.000', 9974,9974.0,'74');"
        )
        tdSql.execute(
            f"insert into tb7_4 values('2021-03-05 04:00:00.000', 9974,9974.0,'74');"
        )
        tdSql.execute(
            f"insert into tb7_5 values('2021-03-01 05:00:00.000', 9975,9975.0,'75');"
        )
        tdSql.execute(
            f"insert into tb7_5 values('2021-03-02 05:00:00.000', 9975,9975.0,'75');"
        )
        tdSql.execute(
            f"insert into tb7_5 values('2021-03-03 05:00:00.000', 9975,9975.0,'75');"
        )
        tdSql.execute(
            f"insert into tb7_5 values('2021-03-04 05:00:00.000', 9975,9975.0,'75');"
        )
        tdSql.execute(
            f"insert into tb7_5 values('2021-03-05 05:00:00.000', 9975,9975.0,'75');"
        )

        tdSql.execute(
            f"insert into tb8_1 values('2021-03-01 01:00:00.000', 9981,9981.0,'81');"
        )
        tdSql.execute(
            f"insert into tb8_1 values('2021-03-02 01:00:00.000', 9981,9981.0,'81');"
        )
        tdSql.execute(
            f"insert into tb8_1 values('2021-03-03 01:00:00.000', 9981,9981.0,'81');"
        )
        tdSql.execute(
            f"insert into tb8_1 values('2021-03-04 01:00:00.000', 9981,9981.0,'81');"
        )
        tdSql.execute(
            f"insert into tb8_1 values('2021-03-05 01:00:00.000', 9981,9981.0,'81');"
        )
        tdSql.execute(
            f"insert into tb8_2 values('2021-03-01 02:00:00.000', 9982,9982.0,'82');"
        )
        tdSql.execute(
            f"insert into tb8_2 values('2021-03-02 02:00:00.000', 9982,9982.0,'82');"
        )
        tdSql.execute(
            f"insert into tb8_2 values('2021-03-03 02:00:00.000', 9982,9982.0,'82');"
        )
        tdSql.execute(
            f"insert into tb8_2 values('2021-03-04 02:00:00.000', 9982,9982.0,'82');"
        )
        tdSql.execute(
            f"insert into tb8_2 values('2021-03-05 02:00:00.000', 9982,9982.0,'82');"
        )
        tdSql.execute(
            f"insert into tb8_3 values('2021-03-01 03:00:00.000', 9983,9983.0,'83');"
        )
        tdSql.execute(
            f"insert into tb8_3 values('2021-03-02 03:00:00.000', 9983,9983.0,'83');"
        )
        tdSql.execute(
            f"insert into tb8_3 values('2021-03-03 03:00:00.000', 9983,9983.0,'83');"
        )
        tdSql.execute(
            f"insert into tb8_3 values('2021-03-04 03:00:00.000', 9983,9983.0,'83');"
        )
        tdSql.execute(
            f"insert into tb8_3 values('2021-03-05 03:00:00.000', 9983,9983.0,'83');"
        )
        tdSql.execute(
            f"insert into tb8_4 values('2021-03-01 04:00:00.000', 9984,9984.0,'84');"
        )
        tdSql.execute(
            f"insert into tb8_4 values('2021-03-02 04:00:00.000', 9984,9984.0,'84');"
        )
        tdSql.execute(
            f"insert into tb8_4 values('2021-03-03 04:00:00.000', 9984,9984.0,'84');"
        )
        tdSql.execute(
            f"insert into tb8_4 values('2021-03-04 04:00:00.000', 9984,9984.0,'84');"
        )
        tdSql.execute(
            f"insert into tb8_4 values('2021-03-05 04:00:00.000', 9984,9984.0,'84');"
        )
        tdSql.execute(
            f"insert into tb8_5 values('2021-03-01 05:00:00.000', 9985,9985.0,'85');"
        )
        tdSql.execute(
            f"insert into tb8_5 values('2021-03-02 05:00:00.000', 9985,9985.0,'85');"
        )
        tdSql.execute(
            f"insert into tb8_5 values('2021-03-03 05:00:00.000', 9985,9985.0,'85');"
        )
        tdSql.execute(
            f"insert into tb8_5 values('2021-03-04 05:00:00.000', 9985,9985.0,'85');"
        )
        tdSql.execute(
            f"insert into tb8_5 values('2021-03-05 05:00:00.000', 9985,9985.0,'85');"
        )

        tdSql.execute(
            f"insert into tb9_1 values('2021-03-01 01:00:00.000', 9991,9991.0,'91');"
        )
        tdSql.execute(
            f"insert into tb9_1 values('2021-03-02 01:00:00.000', 9991,9991.0,'91');"
        )
        tdSql.execute(
            f"insert into tb9_1 values('2021-03-03 01:00:00.000', 9991,9991.0,'91');"
        )
        tdSql.execute(
            f"insert into tb9_1 values('2021-03-04 01:00:00.000', 9991,9991.0,'91');"
        )
        tdSql.execute(
            f"insert into tb9_1 values('2021-03-05 01:00:00.000', 9991,9991.0,'91');"
        )
        tdSql.execute(
            f"insert into tb9_2 values('2021-03-01 02:00:00.000', 9992,9992.0,'92');"
        )
        tdSql.execute(
            f"insert into tb9_2 values('2021-03-02 02:00:00.000', 9992,9992.0,'92');"
        )
        tdSql.execute(
            f"insert into tb9_2 values('2021-03-03 02:00:00.000', 9992,9992.0,'92');"
        )
        tdSql.execute(
            f"insert into tb9_2 values('2021-03-04 02:00:00.000', 9992,9992.0,'92');"
        )
        tdSql.execute(
            f"insert into tb9_2 values('2021-03-05 02:00:00.000', 9992,9992.0,'92');"
        )
        tdSql.execute(
            f"insert into tb9_3 values('2021-03-01 03:00:00.000', 9993,9993.0,'93');"
        )
        tdSql.execute(
            f"insert into tb9_3 values('2021-03-02 03:00:00.000', 9993,9993.0,'93');"
        )
        tdSql.execute(
            f"insert into tb9_3 values('2021-03-03 03:00:00.000', 9993,9993.0,'93');"
        )
        tdSql.execute(
            f"insert into tb9_3 values('2021-03-04 03:00:00.000', 9993,9993.0,'93');"
        )
        tdSql.execute(
            f"insert into tb9_3 values('2021-03-05 03:00:00.000', 9993,9993.0,'93');"
        )
        tdSql.execute(
            f"insert into tb9_4 values('2021-03-01 04:00:00.000', 9994,9994.0,'94');"
        )
        tdSql.execute(
            f"insert into tb9_4 values('2021-03-02 04:00:00.000', 9994,9994.0,'94');"
        )
        tdSql.execute(
            f"insert into tb9_4 values('2021-03-03 04:00:00.000', 9994,9994.0,'94');"
        )
        tdSql.execute(
            f"insert into tb9_4 values('2021-03-04 04:00:00.000', 9994,9994.0,'94');"
        )
        tdSql.execute(
            f"insert into tb9_4 values('2021-03-05 04:00:00.000', 9994,9994.0,'94');"
        )
        tdSql.execute(
            f"insert into tb9_5 values('2021-03-01 05:00:00.000', 9995,9995.0,'95');"
        )
        tdSql.execute(
            f"insert into tb9_5 values('2021-03-02 05:00:00.000', 9995,9995.0,'95');"
        )
        tdSql.execute(
            f"insert into tb9_5 values('2021-03-03 05:00:00.000', 9995,9995.0,'95');"
        )
        tdSql.execute(
            f"insert into tb9_5 values('2021-03-04 05:00:00.000', 9995,9995.0,'95');"
        )
        tdSql.execute(
            f"insert into tb9_5 values('2021-03-05 05:00:00.000', 9995,9995.0,'95');"
        )

        tdSql.execute(
            f"insert into tba_1 values('2021-03-01 01:00:00.000', 99101,99101.0,'a1');"
        )
        tdSql.execute(
            f"insert into tba_1 values('2021-03-02 01:00:00.000', 99101,99101.0,'a1');"
        )
        tdSql.execute(
            f"insert into tba_1 values('2021-03-03 01:00:00.000', 99101,99101.0,'a1');"
        )
        tdSql.execute(
            f"insert into tba_1 values('2021-03-04 01:00:00.000', 99101,99101.0,'a1');"
        )
        tdSql.execute(
            f"insert into tba_1 values('2021-03-05 01:00:00.000', 99101,99101.0,'a1');"
        )
        tdSql.execute(
            f"insert into tba_2 values('2021-03-01 02:00:00.000', 99102,99102.0,'a2');"
        )
        tdSql.execute(
            f"insert into tba_2 values('2021-03-02 02:00:00.000', 99102,99102.0,'a2');"
        )
        tdSql.execute(
            f"insert into tba_2 values('2021-03-03 02:00:00.000', 99102,99102.0,'a2');"
        )
        tdSql.execute(
            f"insert into tba_2 values('2021-03-04 02:00:00.000', 99102,99102.0,'a2');"
        )
        tdSql.execute(
            f"insert into tba_2 values('2021-03-05 02:00:00.000', 99102,99102.0,'a2');"
        )
        tdSql.execute(
            f"insert into tba_3 values('2021-03-01 03:00:00.000', 99103,99103.0,'a3');"
        )
        tdSql.execute(
            f"insert into tba_3 values('2021-03-02 03:00:00.000', 99103,99103.0,'a3');"
        )
        tdSql.execute(
            f"insert into tba_3 values('2021-03-03 03:00:00.000', 99103,99103.0,'a3');"
        )
        tdSql.execute(
            f"insert into tba_3 values('2021-03-04 03:00:00.000', 99103,99103.0,'a3');"
        )
        tdSql.execute(
            f"insert into tba_3 values('2021-03-05 03:00:00.000', 99103,99103.0,'a3');"
        )
        tdSql.execute(
            f"insert into tba_4 values('2021-03-01 04:00:00.000', 99104,99104.0,'a4');"
        )
        tdSql.execute(
            f"insert into tba_4 values('2021-03-02 04:00:00.000', 99104,99104.0,'a4');"
        )
        tdSql.execute(
            f"insert into tba_4 values('2021-03-03 04:00:00.000', 99104,99104.0,'a4');"
        )
        tdSql.execute(
            f"insert into tba_4 values('2021-03-04 04:00:00.000', 99104,99104.0,'a4');"
        )
        tdSql.execute(
            f"insert into tba_4 values('2021-03-05 04:00:00.000', 99104,99104.0,'a4');"
        )
        tdSql.execute(
            f"insert into tba_5 values('2021-03-01 05:00:00.000', 99105,99105.0,'a5');"
        )
        tdSql.execute(
            f"insert into tba_5 values('2021-03-02 05:00:00.000', 99105,99105.0,'a5');"
        )
        tdSql.execute(
            f"insert into tba_5 values('2021-03-03 05:00:00.000', 99105,99105.0,'a5');"
        )
        tdSql.execute(
            f"insert into tba_5 values('2021-03-04 05:00:00.000', 99105,99105.0,'a5');"
        )
        tdSql.execute(
            f"insert into tba_5 values('2021-03-05 05:00:00.000', 99105,99105.0,'a5');"
        )

        tdSql.execute(
            f"insert into tbb_1 values('2021-03-01 01:00:00.000', 99111,99111.0,'b1');"
        )
        tdSql.execute(
            f"insert into tbb_1 values('2021-03-02 01:00:00.000', 99111,99111.0,'b1');"
        )
        tdSql.execute(
            f"insert into tbb_1 values('2021-03-03 01:00:00.000', 99111,99111.0,'b1');"
        )
        tdSql.execute(
            f"insert into tbb_1 values('2021-03-04 01:00:00.000', 99111,99111.0,'b1');"
        )
        tdSql.execute(
            f"insert into tbb_1 values('2021-03-05 01:00:00.000', 99111,99111.0,'b1');"
        )
        tdSql.execute(
            f"insert into tbb_2 values('2021-03-01 02:00:00.000', 99112,99112.0,'b2');"
        )
        tdSql.execute(
            f"insert into tbb_2 values('2021-03-02 02:00:00.000', 99112,99112.0,'b2');"
        )
        tdSql.execute(
            f"insert into tbb_2 values('2021-03-03 02:00:00.000', 99112,99112.0,'b2');"
        )
        tdSql.execute(
            f"insert into tbb_2 values('2021-03-04 02:00:00.000', 99112,99112.0,'b2');"
        )
        tdSql.execute(
            f"insert into tbb_2 values('2021-03-05 02:00:00.000', 99112,99112.0,'b2');"
        )
        tdSql.execute(
            f"insert into tbb_3 values('2021-03-01 03:00:00.000', 99113,99113.0,'b3');"
        )
        tdSql.execute(
            f"insert into tbb_3 values('2021-03-02 03:00:00.000', 99113,99113.0,'b3');"
        )
        tdSql.execute(
            f"insert into tbb_3 values('2021-03-03 03:00:00.000', 99113,99113.0,'b3');"
        )
        tdSql.execute(
            f"insert into tbb_3 values('2021-03-04 03:00:00.000', 99113,99113.0,'b3');"
        )
        tdSql.execute(
            f"insert into tbb_3 values('2021-03-05 03:00:00.000', 99113,99113.0,'b3');"
        )
        tdSql.execute(
            f"insert into tbb_4 values('2021-03-01 04:00:00.000', 99114,99114.0,'b4');"
        )
        tdSql.execute(
            f"insert into tbb_4 values('2021-03-02 04:00:00.000', 99114,99114.0,'b4');"
        )
        tdSql.execute(
            f"insert into tbb_4 values('2021-03-03 04:00:00.000', 99114,99114.0,'b4');"
        )
        tdSql.execute(
            f"insert into tbb_4 values('2021-03-04 04:00:00.000', 99114,99114.0,'b4');"
        )
        tdSql.execute(
            f"insert into tbb_4 values('2021-03-05 04:00:00.000', 99114,99114.0,'b4');"
        )
        tdSql.execute(
            f"insert into tbb_5 values('2021-03-01 05:00:00.000', 99115,99115.0,'b5');"
        )
        tdSql.execute(
            f"insert into tbb_5 values('2021-03-02 05:00:00.000', 99115,99115.0,'b5');"
        )
        tdSql.execute(
            f"insert into tbb_5 values('2021-03-03 05:00:00.000', 99115,99115.0,'b5');"
        )
        tdSql.execute(
            f"insert into tbb_5 values('2021-03-04 05:00:00.000', 99115,99115.0,'b5');"
        )
        tdSql.execute(
            f"insert into tbb_5 values('2021-03-05 05:00:00.000', 99115,99115.0,'b5');"
        )

        tdSql.query(
            f"select * from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select * from st0, st1 where st0.ts=st1.ts and st0.id2=st1.id2 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select * from st0, st1 where st0.id3=st1.id3 and st1.ts=st0.ts order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select * from st0, st1 where st1.id5=st0.id5 and st0.ts=st1.ts order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select st0.* from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)

        tdSql.query(
            f"select st0.* from st0, st1 where st0.ts=st1.ts and st1.id2=st0.id2 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)

        tdSql.query(
            f"select st0.* from st0, st1 where st0.id3=st1.id3 and st1.ts=st0.ts order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)

        tdSql.query(
            f"select st1.* from st0, st1 where st1.id5=st0.id5 and st0.ts=st1.ts order by st1.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9911)
        tdSql.checkData(0, 2, 9911.000000000)
        tdSql.checkData(0, 3, 11)
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)

        tdSql.query(
            f"select st0.f1,st1.f1 from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 order by st0.f1;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, 9901)
        tdSql.checkData(0, 1, 9911)
        tdSql.checkData(1, 0, 9901)
        tdSql.checkData(1, 1, 9911)
        tdSql.checkData(2, 0, 9901)
        tdSql.checkData(2, 1, 9911)
        tdSql.checkData(3, 0, 9901)
        tdSql.checkData(3, 1, 9911)

        tdSql.query(
            f"select st0.ts,st1.ts from st0, st1 where st0.ts=st1.ts and st1.id2=st0.id2 order by st1.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, "2021-03-01 01:00:00")
        tdSql.checkData(5, 0, "2021-03-02 01:00:00")
        tdSql.checkData(5, 1, "2021-03-02 01:00:00")

        tdSql.query(
            f"select st1.ts,st0.ts,st0.id3,st1.id3,st0.f3,st1.f3 from st0, st1 where st0.id3=st1.id3 and st1.ts=st0.ts order by st1.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, "2021-03-01 01:00:00")
        tdSql.checkData(0, 2, 2.000000000)
        tdSql.checkData(0, 3, 2.000000000)
        tdSql.checkData(0, 4, "01")
        tdSql.checkData(0, 5, 11)

        tdSql.query(
            f"select st0.ts,st0.f2,st1.f3,st1.f2,st0.f3 from st0, st1 where st1.id5=st0.id5 and st0.ts=st1.ts order by st1.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901.000000000)
        tdSql.checkData(0, 2, 11)
        tdSql.checkData(0, 3, 9911.000000000)
        tdSql.checkData(0, 4, "01")
        tdSql.checkData(5, 0, "2021-03-02 01:00:00")
        tdSql.checkData(5, 1, 9901.000000000)
        tdSql.checkData(5, 2, 11)
        tdSql.checkData(5, 3, 9911.000000000)
        tdSql.checkData(5, 4, "01")

        tdSql.query(
            f"select _wstart, last(*) from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 interval(10a);"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, "2021-03-01 01:00:00")
        tdSql.checkData(0, 2, 9901)
        tdSql.checkData(0, 3, 9901.000000000)
        tdSql.checkData(0, 4, "01")
        tdSql.checkData(0, 5, "2021-03-01 01:00:00")
        tdSql.checkData(0, 6, 9911)
        tdSql.checkData(0, 7, 9911.000000000)
        tdSql.checkData(0, 8, 11)

        tdSql.query(
            f"select _wstart, last(*) from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 interval(1d) sliding(1d);"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 00:00:00")
        tdSql.checkData(0, 1, "2021-03-01 05:00:00")
        tdSql.checkData(0, 2, 9905)
        tdSql.checkData(0, 3, 9905.000000000)
        tdSql.checkData(0, 4, "05")
        tdSql.checkData(0, 5, "2021-03-01 05:00:00")
        tdSql.checkData(0, 6, 9915)
        tdSql.checkData(0, 7, 9915.000000000)
        tdSql.checkData(0, 8, 15)

        tdSql.query(
            f"select st0.*,st1.* from st0, st1 where st1.id1=st0.id1 and st0.ts=st1.ts and st1.ts=st0.ts and st0.id1=st1.id1 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select st0.ts,* from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 order by st0.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, "2021-03-01 01:00:00")
        tdSql.checkData(0, 2, 9901)
        tdSql.checkData(0, 3, 9901.000000000)
        tdSql.checkData(0, 4, "01")
        tdSql.checkData(0, 5, 0)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 2.000000000)
        tdSql.checkData(0, 8, 1)
        tdSql.checkData(0, 9, 3)

        tdSql.query(
            f"select st0.*,st1.* from st0, st1 where st1.id1=st0.id1 and st0.ts=st1.ts and st1.ts=st0.ts and st0.id1=st1.id1 order by st0.ts limit 5 offset 5"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-02 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-02 01:00:00")

        tdSql.query(
            f"select top(st1.f1, 5) from st0, st1 where st1.id1=st0.id1 and st0.ts=st1.ts and st1.ts=st0.ts and st0.id1=st1.id1;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 9915)
        tdSql.checkData(1, 0, 9915)
        tdSql.checkData(2, 0, 9915)
        tdSql.checkData(3, 0, 9915)
        tdSql.checkData(4, 0, 9915)

        tdSql.query(
            f"select st0.ts, top(st0.f1,5) from st0, st1 where st1.id1=st0.id1 and st0.ts=st1.ts and st1.ts=st0.ts and st0.id1=st1.id1 order by st0.ts;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 05:00:00")
        tdSql.checkData(0, 1, 9905)
        tdSql.checkData(1, 0, "2021-03-02 05:00:00")
        tdSql.checkData(1, 1, 9905)
        tdSql.checkData(2, 0, "2021-03-03 05:00:00")
        tdSql.checkData(2, 1, 9905)
        tdSql.checkData(3, 0, "2021-03-04 05:00:00")
        tdSql.checkData(3, 1, 9905)
        tdSql.checkData(4, 0, "2021-03-05 05:00:00")
        tdSql.checkData(4, 1, 9905)

        # sql select st0.*,st1.*,st2.*,st3.* from st3,st2,st1,st0 where st0.id1=st3.id1 and st3.ts=st2.ts and st2.id1=st1.id1 and st1.ts=st0.ts;
        # sql select st0.*,st1.*,st2.*,st3.* from st3,st2,st1,st0 where st0.id1=st3.id1 and st3.ts=st2.ts and st2.id1=st1.id1 and st1.ts=st0.ts and st0.id1=st2.id1 and st1.ts=st2.ts;
        # if $rows != 25 then
        #  print $rows
        #  return -1
        # endi
        # if $tdSql.getData(0,0, "2021-03-01 01:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 9901 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2) != 9901.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,3) != 01 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 0 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6) != 2.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,8) != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,9, "2021-03-01 01:00:00.000@ then
        #  return -1
        # endi

        # sql select st0.*,st1.*,st2.*,st3.* from st3,st2,st1,st0 where st0.id1=st1.id1 and st1.ts=st0.ts and st2.id1=st3.id1 and st3.ts=st2.ts;
        # sql select st0.*,st1.*,st2.*,st3.* from st3,st2,st1,st0 where st0.id1=st1.id1 and st1.ts=st0.ts and st2.id1=st3.id1 and st3.ts=st2.ts and st0.id1=st2.id1 and st0.ts=st2.ts;
        # if $rows != 25 then
        #  return -1
        # endi
        # if $tdSql.getData(0,0, "2021-03-01 01:00:00.000@ then
        #  return -1
        # endi
        # if $tdSql.getData(0,1) != 9901 then
        #  return -1
        # endi
        # if $tdSql.getData(0,2) != 9901.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,3) != 01 then
        #  return -1
        # endi
        # if $tdSql.getData(0,4) != 0 then
        #  return -1
        # endi
        # if $tdSql.getData(0,5) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,6) != 2.000000000 then
        #  return -1
        # endi
        # if $tdSql.getData(0,7) != 1 then
        #  return -1
        # endi
        # if $tdSql.getData(0,8) != 3 then
        #  return -1
        # endi
        # if $tdSql.getData(0,9, "2021-03-01 01:00:00.000@ then
        #  return -1
        # endi

        tdSql.query(
            f"select st0.*,st1.*,st2.*,st3.*,st4.*,st5.*,st6.*,st7.*,st8.*,st9.* from st0,st1,st2,st3,st4,st5,st6,st7,st8,st9 where st0.ts=st2.ts and st0.ts=st4.ts and st0.ts=st6.ts and st0.ts=st8.ts and st1.ts=st3.ts and st3.ts=st5.ts and st5.ts=st7.ts and st7.ts=st9.ts and st0.ts=st1.ts and st0.id1=st2.id1 and st0.id1=st4.id1 and st0.id1=st6.id1 and st0.id1=st8.id1 and st1.id1=st3.id1 and st3.id1=st5.id1 and st5.id1=st7.id1 and st7.id1=st9.id1 and st0.id1=st1.id1 order by st1.ts;"
        )
        tdSql.checkRows(25)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, 0)
        tdSql.checkData(0, 5, 1)
        tdSql.checkData(0, 6, 2.000000000)
        tdSql.checkData(0, 7, 1)
        tdSql.checkData(0, 8, 3)
        tdSql.checkData(0, 9, "2021-03-01 01:00:00")

        tdSql.query(
            f"select tb0_1.*, tb1_1.* from tb0_1, tb1_1 where tb0_1.ts=tb1_1.ts;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, "2021-03-01 01:00:00")
        tdSql.checkData(0, 5, 9911)
        tdSql.checkData(0, 6, 9911.000000000)
        tdSql.checkData(0, 7, 11)
        tdSql.checkData(1, 0, "2021-03-02 01:00:00")
        tdSql.checkData(1, 1, 9901)
        tdSql.checkData(1, 2, 9901.000000000)
        tdSql.checkData(1, 3, "01")
        tdSql.checkData(1, 4, "2021-03-02 01:00:00")
        tdSql.checkData(1, 5, 9911)
        tdSql.checkData(1, 6, 9911.000000000)
        tdSql.checkData(1, 7, 11)

        tdSql.query(
            f"select tb0_1.*, tb1_1.* from tb0_1, tb1_1 where tb0_1.ts=tb1_1.ts and tb0_1.id1=tb1_1.id1;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, "2021-03-01 01:00:00")
        tdSql.checkData(0, 5, 9911)
        tdSql.checkData(0, 6, 9911.000000000)
        tdSql.checkData(0, 7, 11)
        tdSql.checkData(1, 0, "2021-03-02 01:00:00")
        tdSql.checkData(1, 1, 9901)
        tdSql.checkData(1, 2, 9901.000000000)
        tdSql.checkData(1, 3, "01")
        tdSql.checkData(1, 4, "2021-03-02 01:00:00")
        tdSql.checkData(1, 5, 9911)
        tdSql.checkData(1, 6, 9911.000000000)
        tdSql.checkData(1, 7, 11)

        tdSql.query(
            f"select tb0_1.*, tb1_2.*,tb2_3.*,tb3_4.*,tb4_5.* from tb0_1, tb1_2, tb2_3, tb3_4, tb4_5 where tb0_1.ts=tb1_2.ts and tb0_1.ts=tb2_3.ts and tb0_1.ts=tb3_4.ts and tb0_1.ts=tb4_5.ts;"
        )
        tdSql.checkRows(0)

        tdSql.query(
            f"select tb0_1.*, tb1_1.*,tb2_1.*,tb3_1.*,tb4_1.* from tb0_1, tb1_1, tb2_1, tb3_1, tb4_1 where tb0_1.ts=tb1_1.ts and tb0_1.ts=tb2_1.ts and tb0_1.ts=tb3_1.ts and tb0_1.ts=tb4_1.ts;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 01:00:00")
        tdSql.checkData(0, 1, 9901)
        tdSql.checkData(0, 2, 9901.000000000)
        tdSql.checkData(0, 3, "01")
        tdSql.checkData(0, 4, "2021-03-01 01:00:00")
        tdSql.checkData(0, 5, 9911)
        tdSql.checkData(0, 6, 9911.000000000)
        tdSql.checkData(0, 7, 11)
        tdSql.checkData(0, 8, "2021-03-01 01:00:00")
        tdSql.checkData(0, 9, 9921)

        tdSql.query(
            f"select tb0_5.*, tb1_5.*,tb2_5.*,tb3_5.*,tb4_5.*,tb5_5.*, tb6_5.*,tb7_5.*,tb8_5.*,tb9_5.* from tb0_5, tb1_5, tb2_5, tb3_5, tb4_5,tb5_5, tb6_5, tb7_5, tb8_5, tb9_5 where tb9_5.ts=tb8_5.ts and tb8_5.ts=tb7_5.ts and tb7_5.ts=tb6_5.ts and tb6_5.ts=tb5_5.ts and tb5_5.ts=tb4_5.ts and tb4_5.ts=tb3_5.ts and tb3_5.ts=tb2_5.ts and tb2_5.ts=tb1_5.ts and tb1_5.ts=tb0_5.ts;"
        )
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, "2021-03-01 05:00:00")
        tdSql.checkData(0, 1, 9905)
        tdSql.checkData(0, 2, 9905.000000000)
        tdSql.checkData(0, 3, "05")
        tdSql.checkData(0, 4, "2021-03-01 05:00:00")
        tdSql.checkData(0, 5, 9915)
        tdSql.checkData(0, 6, 9915.000000000)
        tdSql.checkData(0, 7, 15)
        tdSql.checkData(0, 8, "2021-03-01 05:00:00")
        tdSql.checkData(0, 9, 9925)

        tdSql.error(
            f"select tb0_1.*, tb1_1.* from tb0_1, tb1_1 where tb0_1.f1=tb1_1.f1;"
        )
        tdSql.query(
            f"select tb0_1.*, tb1_1.* from tb0_1, tb1_1 where tb0_1.ts=tb1_1.ts and tb0_1.id1=tb1_1.id2;"
        )
        tdSql.query(
            f"select tb0_5.*, tb1_5.*,tb2_5.*,tb3_5.*,tb4_5.*,tb5_5.*, tb6_5.*,tb7_5.*,tb8_5.*,tb9_5.*,tba_5.* from tb0_5, tb1_5, tb2_5, tb3_5, tb4_5,tb5_5, tb6_5, tb7_5, tb8_5, tb9_5, tba_5 where tb9_5.ts=tb8_5.ts and tb8_5.ts=tb7_5.ts and tb7_5.ts=tb6_5.ts and tb6_5.ts=tb5_5.ts and tb5_5.ts=tb4_5.ts and tb4_5.ts=tb3_5.ts and tb3_5.ts=tb2_5.ts and tb2_5.ts=tb1_5.ts and tb1_5.ts=tb0_5.ts and tb0_5.ts=tba_5.ts;"
        )

        tdSql.query(f"select * from st0, st1 where st0.ts=st1.ts;")
        tdSql.error(f"select * from st0, st1 where st0.id1=st1.id1;")
        tdSql.error(f"select * from st0, st1 where st0.f1=st1.f1 and st0.id1=st1.id1;")
        tdSql.query(
            f"select * from st0, st1, st2, st3 where st0.id1=st1.id1 and st2.id1=st3.id1 and st0.ts=st1.ts and st1.ts=st2.ts and st2.ts=st3.ts;"
        )
        tdSql.error(f"select * from st0, st1, st2 where st0.id1=st1.id1;")
        tdSql.error(
            f"select * from st0, st1 where st0.id1=st1.id1 and st0.id2=st1.id3;"
        )
        tdSql.error(f"select * from st0, st1 where st0.id1=st1.id1 or st0.ts=st1.ts;")
        tdSql.error(
            f"select * from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 or st0.id2=st1.id2;"
        )
        tdSql.error(
            f"select * from st0, st1, st2 where st0.ts=st1.ts and st0.id1=st1.id1;"
        )
        tdSql.error(f"select * from st0, st1 where st0.id1=st1.ts and st0.ts=st1.id1;")
        tdSql.query(f"select * from st0, st1 where st0.id1=st1.id2 and st0.ts=st1.ts;")
        tdSql.query(f"select * from st0, st1 where st1.id4=st0.id4 and st1.ts=st0.ts;")
        tdSql.query(f"select * from st0, st1 where st0.id1=st1.id2 and st1.ts=st0.ts;")
        tdSql.error(
            f"select * from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 interval 10a;"
        )
        tdSql.error(
            f"select last(*) from st0, st1 where st0.ts=st1.ts and st0.id1=st1.id1 group by f1;"
        )
        tdSql.error(
            f"select st0.*,st1.*,st2.*,st3.*,st4.*,st5.*,st6.*,st7.*,st8.*,st9.* from st0,st1,st2,st3,st4,st5,st6,st7,st8,st9 where st0.ts=st2.ts and st0.ts=st4.ts and st0.ts=st6.ts and st0.ts=st8.ts and st1.ts=st3.ts and st3.ts=st5.ts and st5.ts=st7.ts and st7.ts=st9.ts and st0.id1=st2.id1 and st0.id1=st4.id1 and st0.id1=st6.id1 and st0.id1=st8.id1 and st1.id1=st3.id1 and st3.id1=st5.id1 and st5.id1=st7.id1 and st7.id1=st9.id1;"
        )
        tdSql.query(
            f"select st0.*,st1.*,st2.*,st3.*,st4.*,st5.*,st6.*,st7.*,st8.*,st9.* from st0,st1,st2,st3,st4,st5,st6,st7,st8,st9,sta where st0.ts=st2.ts and st0.ts=st4.ts and st0.ts=st6.ts and st0.ts=st8.ts and st1.ts=st3.ts and st3.ts=st5.ts and st5.ts=st7.ts and st7.ts=st9.ts and st0.ts=st1.ts and st0.id1=st2.id1 and st0.id1=st4.id1 and st0.id1=st6.id1 and st0.id1=st8.id1 and st1.id1=st3.id1 and st3.id1=st5.id1 and st5.id1=st7.id1 and st7.id1=st9.id1 and st0.id1=st1.id1 and st0.id1=sta.id1 and st0.ts=sta.ts;"
        )
