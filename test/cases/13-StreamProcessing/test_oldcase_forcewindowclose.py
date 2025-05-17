import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStreamOldCaseForceWindowClose:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_oldcase_force_window_close(self):
        """Stream force window close

        1. basic test
        2. out of order data

        Catalog:
            - Streams:OldCase
        Since: v3.0.0.0
        Labels: common,ci
        Jira: None
        History:
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/forcewindowclose.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamFwcIntervalFill.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpForceWindowClose1.sim
            ## - 2025-5-15 Simon Guan Migrated from tsim/stream/streamInterpFwcError.sim
        """

        self.forcewindowclose()
        # self.streamFwcIntervalFill()
        # self.streamInterpForceWindowClose()
        # self.streamInterpForceWindowClose1()
        # self.streamInterpFwcError()

    def forcewindowclose(self):
        tdLog.info(f"forcewindowclose")
        clusterComCheck.drop_all_streams_and_dbs()

        tdLog.info(f'===================================== force window close with sliding test')
        tdLog.info(f'============ create db')
        tdSql.execute(f"create database test1 vgroups 2 precision 'us';")

        tdSql.execute(f"use test1")
        tdSql.execute(f"create stable st1(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu11 using st1 tags(1);")

        tdSql.error(f"create stream stream11 trigger force_window_close into str_dst1 as select  _wstart, count(*) from st1 partition by tbname interval(5s) sliding(6s);")
        tdSql.error(f"create stream stream11 trigger force_window_close into str_dst1 as select  _wstart, count(*) from st1 partition by tbname interval(5s) sliding(9a);")
        tdSql.error(f"create stream stream11 trigger force_window_close into str_dst1 as select  _wstart, count(*) from st1 partition by tbname interval(5s) sliding(1.1s);")
        tdSql.execute(f"create stream stream11 trigger force_window_close into str_dst1 as select  _wstart, _wend, count(*) from st1 partition by tbname interval(5s) sliding(1s);")
        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into tu11 values(now, 1);")
        for  i in range(20):
            tdSql.execute(f"insert into tu11 values(now + f{5500+500}a, 1);")


        tdSql.query(f"select sum(`count(*)`) from (select * from str_dst1)")

        tdSql.checkData(0, 0, 100)
          tdLog.info(f'expect 100, actual: {tdSql.getData(0,0)}')
        #return -1

        tdLog.info(f'========================================== create database')
        tdSql.execute(f"create database test vgroups 2;")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)}')

        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu1 using st tags(1);")

        tdSql.execute(f"create stream stream1 trigger force_window_close into str_dst as select  _wstart, count(*) from st partition by tbname interval(5s);")
        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into tu1 values(now, 1);")
sleep 5500

sql pause stream stream1

        loop_count = 0

loop1:
sleep 500
        loop_count = loop_count + 1
        if loop_count == 20 "":
  goto end_loop1

        tdSql.execute(f"insert into tu1 values(now, 1);")
goto loop1

end_loop1:
sql resume stream stream1
sleep 5000

        tdSql.query(f"select sum(`count(*)`) from (select * from str_dst)")

        tdSql.checkData(0, 0, 20)
          tdLog.info(f'expect 20, actual: {tdSql.getData(0,0)}')
        #return -1

        tdSql.execute(f"drop database test")

        tdLog.info(f'===================================== micro precision db test')
        tdLog.info(f'============ create db')
        tdSql.execute(f"create database test vgroups 2 precision 'us';")

        tdSql.execute(f"use test")
        tdSql.execute(f"create stable st(ts timestamp, a int) tags(t int);")
        tdSql.execute(f"create table tu1 using st tags(1);")

        tdSql.execute(f"create stream stream1 trigger force_window_close into str_dst as select _wstart, count(*) from st partition by tbname interval(5s);")
        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into tu1 values(now, 1);")
sleep 5500

sql pause stream stream1

        loop_count = 0

loop0:
sleep 500
        loop_count = loop_count + 1
        if loop_count == 20 "":
  goto end_loop

        tdSql.execute(f"insert into tu1 values(now, 1);")
goto loop0

end_loop:

sql resume stream stream1
sleep 5000

        tdSql.query(f"select sum(`count(*)`) from (select * from str_dst)")

        tdSql.checkData(0, 0, 20)
          tdLog.info(f'expect 20, actual: {tdSql.getData(0,0)}')
        #return -1

        tdSql.execute(f"drop stream stream1")
        tdSql.execute(f"drop table str_dst")

        tdLog.info(f'============================= too long watermark test')
        tdSql.execute(f"drop table tu1;")
        tdSql.execute(f"create table tu1 using st tags(1);")
        tdSql.execute(f"create stream stream2 trigger force_window_close watermark 30s into str_dst as select _wstart, count(*), now() from st partition by tbname interval(5s);")
        clusterComCheck.check_stream_status()

        loop_count = 0

loop2:
sleep 500
        loop_count = loop_count + 1
        if loop_count == 20 "":
  goto end_loop3

        tdSql.execute(f"insert into tu1 values(now, 1);")
goto loop2

end_loop3:

        tdSql.query(f"select count(*) from str_dst")
        tdLog.info(f'=================rows: {tdSql.getData(0,0)}')

        tdSql.checkData(0, 0, 0)
          tdLog.info(f'expect 0, actual {tdSql.getData(0,0)}')
        #return -1

sleep 35000

        tdSql.query(f"select sum(`count(*)`) from (select * from str_dst)")
        tdSql.checkData(0, 0, 19)
         tdLog.info(f'expect 19, actual: {tdSql.getData(0,0)}')
        #return -1

        tdSql.query(f"select round(timediff(`now()`, `_wstart`)/1000000) from str_dst;")
        tdSql.checkData(0, 0, 35.000000000)
          tdLog.info(f'expect 35.000000000 , actual {tdSql.getData(0,0)}')
        #return -1

#system sh/exec.sh -n dnode1 -s stop -x SIGINT

    def streamFwcIntervalFill(self):
        tdLog.info(f"streamFwcIntervalFill")
        clusterComCheck.drop_all_streams_and_dbs()
        
        #system sh/stop_dnodes.sh
#system sh/deploy.sh -n dnode1 -i 1
#system sh/exec.sh -n dnode1 -s start
sleep 50
        tdSql.connect('root')

        tdLog.info(f'step1')
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database test vgroups 4;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);")
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone()  from st partition by tbname,ta interval(2s) fill(value, 100, 200);")

        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,5,10,10) (now +  3200a,5,10,10)  (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);")
        tdSql.execute(f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,2,10,10) (now +  3200a,30,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);")

        tdLog.info(f'sql select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone()  from t1 partition by tbname,ta interval(2s)')
        tdSql.query(f"select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone()  from t1 partition by tbname,ta interval(2s);")

        query1_data01 = tdSql.getData(0,1)
        query1_data11 = tdSql.getData(1,1)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

        tdLog.info(f'sql select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone()  from t2 partition by tbname,ta interval(2s);')
        tdSql.query(f"select _wstart, count(a) as ca, now, ta, sum(b) as cb, timezone()  from t2 partition by tbname,ta interval(2s);")

        query2_data01 = tdSql.getData(0,1)
        query2_data11 = tdSql.getData(1,1)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

        loop_count = 0
loop0:

sleep 2000

        loop_count = loop_count + 1
        if loop_count == 20 "":

        tdLog.info(f'2 sql select * from streamt where ta == 1 order by 1;')
        tdSql.query(f"select * from streamt where ta == 1 order by 1;")

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# row 0
        if rows < 2 "":
          tdLog.info(f'======rows={tdSql.getRows()})')
  goto loop0

        if tdSql.getData(0,1) != query1_data01 "":
          tdLog.info(f'======tdSql.getData(0,1)========{tdSql.getData(0,1)}')
          tdLog.info(f'======query1_data01={query1_data01}')
        #return -1

        if tdSql.getData(1,1) != query1_data11 "":
          tdLog.info(f'======tdSql.getData(1,1)========{tdSql.getData(1,1)}')
          tdLog.info(f'======query1_data11={query1_data11}')
  goto loop0

        loop_count = 0
loop1:

sleep 2000

        loop_count = loop_count + 1
        if loop_count == 20 "":

        tdLog.info(f'2 sql select * from streamt where ta == 2 order by 1;')
        tdSql.query(f"select * from streamt where ta == 2 order by 1;")

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# row 0
        if rows < 2 "":
          tdLog.info(f'======rows={tdSql.getRows()})')
  goto loop1

        if tdSql.getData(0,1) != query2_data01 "":
          tdLog.info(f'======tdSql.getData(0,1)======{tdSql.getData(0,1)}')
          tdLog.info(f'====query2_data01={query2_data01}')
        #return -1

        if tdSql.getData(1,1) != query2_data11 "":
          tdLog.info(f'======tdSql.getData(1,1)======{tdSql.getData(1,1)}')
          tdLog.info(f'====query2_data11={query2_data11}')
  goto loop1

        loop_count = 0
loop2:

sleep 2000

        loop_count = loop_count + 1
        if loop_count == 20 "":

        tdLog.info(f'2 sql select * from streamt;')
        tdSql.query(f"select * from streamt;")

        if rows < 6 "":
          tdLog.info(f'======rows={tdSql.getRows()})')
  goto loop2

        tdLog.info(f'step2')
        tdLog.info(f'=============== create database')
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(f"create stable st(ts timestamp, a int, b int , c int)tags(ta int,tb int,tc int);")
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _wstart, count(*), ta  from st partition by tbname,ta interval(2s) fill(NULL);")

        clusterComCheck.check_stream_status()

        tdSql.execute(f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);")
        tdSql.execute(f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);")

        tdLog.info(f'sql select _wstart, count(*)  from t1 interval(2s) order by 1;')
        tdSql.query(f"select _wstart, count(*)  from t1 interval(2s) order by 1;")

        query1_data01 = tdSql.getData(0,1)
        query1_data11 = tdSql.getData(1,1)

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

        loop_count = 0
loop3:

sleep 2000

        loop_count = loop_count + 1
        if loop_count == 20 "":

        tdLog.info(f'2 sql select * from streamt where ta == 1 order by 1;')
        tdSql.query(f"select * from streamt where ta == 1 order by 1;")

        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

        if tdSql.getData(0,1) != query1_data01 "":
          tdLog.info(f'======tdSql.getData(0,1)======{tdSql.getData(0,1)}')
          tdLog.info(f'====query1_data01={query1_data01}')
  goto loop3

        tdSql.execute(f"insert into t1 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,20,1,1) (now + 5200a,30,10,10) (now + 5300a,40,10,10);")
        tdSql.execute(f"insert into t2 values(now +  3000a,1,1,1) (now +  3100a,3,10,10) (now +  3200a,5,10,10) (now + 5100a,10,1,1) (now + 5200a,40,10,10) (now + 5300a,7,10,10);")

        loop_count = 0
loop4:

sleep 2000

        loop_count = loop_count + 1
        if loop_count == 20 "":

        tdLog.info(f'2 sql select * from streamt;')
        tdSql.query(f"select * from streamt;")

        tdLog.info(f'======step2=rows={tdSql.getRows()})')
        tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
        tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
        tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
        tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
        tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
        tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

        if rows < 10 "":
          tdLog.info(f'======rows={tdSql.getRows()})')
  goto loop4

        tdLog.info(f'end')

#system sh/exec.sh -n dnode1 -s stop -x SIGINT

#     def streamInterpForceWindowClose(self):
#         tdLog.info(f"streamInterpForceWindowClose")
#         clusterComCheck.drop_all_streams_and_dbs()
# #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdLog.info(f'step1')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 1;")
#         tdSql.execute(f"use test;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, interp(b) as b, now  from t1 every(2s) fill(prev);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.1) (now + 10s,2,2,2,2.1) (now + 20s,3,3,3,3.1);")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop0:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 1;')
#         tdSql.query(f"select * from streamt where a == 1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

#         loop_count = 0
# loop1:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 2;')
#         tdSql.query(f"select * from streamt where a == 2;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

#         loop_count = 0
# loop2:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 3;')
#         tdSql.query(f"select * from streamt where a == 3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# sleep 4000

#         loop_count = 0
# loop3:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 3;')
#         tdSql.query(f"select * from streamt where a == 3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

#         tdLog.info(f'step2')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, interp(b) as b, now  from t1 every(2s) fill(NULL);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.1) (now + 10s,2,2,2,2.1) (now + 20s,3,3,3,3.1);")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop4:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a is null;')
#         tdSql.query(f"select * from streamt where a is null;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop4

#         tdLog.info(f'step3')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test3 vgroups 1;")
#         tdSql.execute(f"use test3;")

#         tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
#         tdSql.execute(f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, interp(b) as b, now  from t1 every(2s) fill(value,100,200);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.1) (now + 10s,2,2,2,2.1) (now + 20s,3,3,3,3.1);")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop5:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 100;')
#         tdSql.query(f"select * from streamt where a == 100;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop5

#         tdLog.info(f'end')

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT

#     def streamInterpForceWindowClose1(self):
#         tdLog.info(f"streamInterpForceWindowClose1")
#         clusterComCheck.drop_all_streams_and_dbs()

# #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdLog.info(f'step prev')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test vgroups 3;")
#         tdSql.execute(f"use test;")
#         tdSql.execute(f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);")

#         tdSql.execute(f"create table t1 using st tags(1,1,1);")
#         tdSql.execute(f"create table t2 using st tags(2,2,2);")
#         tdSql.execute(f"create table t3 using st tags(2,2,2);")

#         tdSql.execute(f"create stream streams1 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b,c every(5s) fill(prev);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.0) (now + 10s,2,1,1,2.0)(now + 20s,3,1,1,3.0)")
#         tdSql.execute(f"insert into t2 values(now,21,1,1,1.0) (now + 10s,22,1,1,2.0)(now + 20s,23,1,1,3.0)")
#         tdSql.execute(f"insert into t3 values(now,31,1,1,1.0) (now + 10s,32,1,1,2.0)(now + 20s,33,1,1,3.0)")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t2;')
#         tdSql.query(f"select * from t2;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t3;')
#         tdSql.query(f"select * from t3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop0:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 1;')
#         tdSql.query(f"select * from streamt where a == 1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

#         tdLog.info(f'2 sql select * from streamt where a == 21;')
#         tdSql.query(f"select * from streamt where a == 21;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

#         loop_count = 0
# loop1:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 31;')
#         tdSql.query(f"select * from streamt where a == 31;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop0

#         loop_count = 0
# loop1:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'sql select * from streamt where a == 2;')
#         tdSql.query(f"select * from streamt where a == 2;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

#         tdLog.info(f'3 sql select * from streamt where a == 22;')
#         tdSql.query(f"select * from streamt where a == 22;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

#         tdLog.info(f'3 sql select * from streamt where a == 32;')
#         tdSql.query(f"select * from streamt where a == 32;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop1

#         loop_count = 0
# loop2:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'4 sql select * from streamt where a == 3;')
#         tdSql.query(f"select * from streamt where a == 3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

#         tdLog.info(f'4 sql select * from streamt where a == 23;')
#         tdSql.query(f"select * from streamt where a == 23;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

#         tdLog.info(f'4 sql select * from streamt where a == 33;')
#         tdSql.query(f"select * from streamt where a == 33;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 2 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop2

# sleep 4000

#         loop_count = 0
# loop3:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'5 sql select * from streamt where a == 3;')
#         tdSql.query(f"select * from streamt where a == 3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

#         tdLog.info(f'5 sql select * from streamt where a == 23;')
#         tdSql.query(f"select * from streamt where a == 23;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

#         tdLog.info(f'5 sql select * from streamt where a == 33;')
#         tdSql.query(f"select * from streamt where a == 33;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

#         tdLog.info(f'2 sql select * from streamt where a == 3;')
#         tdSql.query(f"select * from streamt where a == 3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop3

#         tdLog.info(f'step2')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);")

#         tdSql.execute(f"create table t1 using st tags(1,1,1);")
#         tdSql.execute(f"create table t2 using st tags(2,2,2);")
#         tdSql.execute(f"create table t3 using st tags(2,2,2);")

#         tdSql.execute(f"create stream streams2 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b,c every(2s) fill(NULL);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.0) (now + 10s,2,1,1,2.0)(now + 20s,3,1,1,3.0)")
#         tdSql.execute(f"insert into t2 values(now,21,1,1,1.0) (now + 10s,22,1,1,2.0)(now + 20s,23,1,1,3.0)")
#         tdSql.execute(f"insert into t3 values(now,31,1,1,1.0) (now + 10s,32,1,1,2.0)(now + 20s,33,1,1,3.0)")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t2;')
#         tdSql.query(f"select * from t2;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t3;')
#         tdSql.query(f"select * from t3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop4:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a is null;')
#         tdSql.query(f"select * from streamt where a is null;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 5 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop4

#         tdLog.info(f'step3')
#         tdLog.info(f'=============== create database')
#         tdSql.execute(f"create database test3 vgroups 1;")
#         tdSql.execute(f"use test3;")

#         tdSql.execute(f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);")

#         tdSql.execute(f"create table t1 using st tags(1,1,1);")
#         tdSql.execute(f"create table t2 using st tags(2,2,2);")
#         tdSql.execute(f"create table t3 using st tags(2,2,2);")

#         tdSql.execute(f"create stream streams3 trigger force_window_close IGNORE EXPIRED 1 IGNORE UPDATE 1 into  streamt as select _irowts, interp(a) as a, _isfilled, tbname, b, c from st partition by tbname, b,c every(2s) fill(value,100);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"insert into t1 values(now,1,1,1,1.0) (now + 10s,2,1,1,2.0)(now + 20s,3,1,1,3.0)")
#         tdSql.execute(f"insert into t2 values(now,21,1,1,1.0) (now + 10s,22,1,1,2.0)(now + 20s,23,1,1,3.0)")
#         tdSql.execute(f"insert into t3 values(now,31,1,1,1.0) (now + 10s,32,1,1,2.0)(now + 20s,33,1,1,3.0)")

#         tdLog.info(f'sql select * from t1;')
#         tdSql.query(f"select * from t1;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t2;')
#         tdSql.query(f"select * from t2;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         tdLog.info(f'sql select * from t3;')
#         tdSql.query(f"select * from t3;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

#         loop_count = 0
# loop5:

# sleep 2000

#         loop_count = loop_count + 1
#         if loop_count == 20 "":

#         tdLog.info(f'2 sql select * from streamt where a == 100;')
#         tdSql.query(f"select * from streamt where a == 100;")

#         tdLog.info(f'{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)}')
#         tdLog.info(f'{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)}')
#         tdLog.info(f'{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)} {tdSql.getData(2,4)}')
#         tdLog.info(f'{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)} {tdSql.getData(3,4)}')
#         tdLog.info(f'{tdSql.getData(4,0)} {tdSql.getData(4,1)} {tdSql.getData(4,2)} {tdSql.getData(4,3)} {tdSql.getData(4,4)}')
#         tdLog.info(f'{tdSql.getData(5,0)} {tdSql.getData(5,1)} {tdSql.getData(5,2)} {tdSql.getData(5,3)} {tdSql.getData(5,4)}')

# # row 0
#         if rows < 10 "":
#           tdLog.info(f'======rows={tdSql.getRows()})')
#   goto loop5

#         tdLog.info(f'end')

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT

#     def streamInterpFwcError(self):
#         tdLog.info(f"streamInterpFwcError")
#         clusterComCheck.drop_all_streams_and_dbs()

# #system sh/stop_dnodes.sh
# #system sh/deploy.sh -n dnode1 -i 1
# #system sh/exec.sh -n dnode1 -s start
# sleep 50
#         tdSql.connect('root')

#         tdLog.info(f'step2')

#         tdSql.execute(f"create database test2 vgroups 1;")
#         tdSql.execute(f"use test2;")

#         tdSql.execute(f"create stable st(ts timestamp,a int,b int,c int, d double) tags(ta int,tb int,tc int);")
#         tdSql.execute(f"create table t1 using st tags(1,1,1);")
#         tdSql.execute(f"create table t2 using st tags(2,2,2);")

#         tdSql.execute(f"create stream streams1 trigger force_window_close into  streamt1 as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);")

#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"create stream streams2 trigger force_window_close IGNORE EXPIRED 0 into  streamt2 as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);")
#         tdSql.execute(f"create stream streams3 trigger force_window_close IGNORE UPDATE 0 into  streamt3 as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);")

#         tdSql.execute(f"create stream streams4 trigger force_window_close IGNORE EXPIRED 1 into  streamt4 as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);")
#         clusterComCheck.check_stream_status()

#         tdSql.execute(f"create stream streams5 trigger force_window_close IGNORE UPDATE 1 into  streamt5 as select _irowts, _isfilled as a1, interp(a) as a2 from st partition by tbname every(1s) fill(prev);")
#         clusterComCheck.check_stream_status()

# #system sh/exec.sh -n dnode1 -s stop -x SIGINT


