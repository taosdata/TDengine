from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck,tdDnodes
import datetime
from math import inf

class TestTbname:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
    #
    # ------------------- sim ----------------
    # 
    def do_sim_tbname(self):
        tdLog.info(f"======================== dnode1 start")

        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 nchar(20), t3 binary(20), t4 bigint, t5 smallint, t6 double)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            i1 = i + int(halfNum)
            tb = tbPrefix + str(i)
            tb1 = tbPrefix + str(i1)
            tgstr = "'tb" + str(i) + "'"
            tgstr1 = "'tb" + str(i1) + "'"
            tdSql.execute(
                f"create table {tb} using {stb} tags( {i} , {tgstr} , {tgstr} , {i} , {i} , {i} )"
            )
            tdSql.execute(
                f"create table {tb1} using {stb} tags( {i1} , {tgstr1} , {tgstr1} , {i} , {i} , {i} )"
            )

            sql = "insert into  "
            x = 0
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x / 10
                c = c * 10
                c = x - c
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                                
                sql += f" {tb} values ({ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                x = x + 1
            tdSql.execute(sql)

            i = i + 1

        tdLog.info(f"====== tables created")

        self.tbnameIn_query()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.tbnameIn_query()
        
        print("\ndo sim case ........................... [passed]")

    def tbnameIn_query(self):
        dbPrefix = "ti_db"
        tbPrefix = "ti_tb"
        stbPrefix = "ti_stb"
        tbNum = 2000
        rowNum = 10
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"========== tbnameIn_query.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tb = tbPrefix + "0"
        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        #### tbname in + other tag filtering
        ## [TBASE-362]
        # tbname in + tag filtering is allowed now!!!
        tdSql.query(
            f"select count(*) from {stb} where tbname in ('ti_tb1', 'ti_tb300') and t1 > 2"
        )

        # tbname in used on meter
        tdSql.query(f"select count(*) from {tb} where tbname in ('ti_tb1', 'ti_tb300')")

        ## tbname in + group by tag
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 300)

        ## duplicated tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb1', 'ti_tb1', 'ti_tb2', 'ti_tb2', 'ti_tb3') group by t1 order by t1 asc"
        )
        tdSql.checkRows(3)

        tdSql.checkData(0, 0, rowNum)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, rowNum)

        tdSql.checkData(1, 1, 2)

        tdSql.checkData(2, 0, rowNum)

        tdSql.checkData(2, 1, 3)

        ## wrong tbnames
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        ## tbname in + colummn filtering
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('tbname in', 'ti_tb1', 'ti_stb0', 'ti_tb2') and c8 like 'binary%' group by t1 order by t1 asc"
        )
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 1)

        tdSql.checkData(1, 0, 10)

        tdSql.checkData(1, 1, 2)

        ## tbname in can accpet Upper case table name
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb0', 'TI_tb1', 'TI_TB2') group by t1 order by t1"
        )
        tdSql.checkRows(1)

        tdSql.checkData(0, 0, 10)

        tdSql.checkData(0, 1, 0)

        # multiple tbname in is not allowed NOW
        tdSql.query(
            f"select count(*), t1 from {stb} where tbname in ('ti_tb1', 'ti_tb300') and tbname in ('ti_tb5', 'ti_tb1000') group by t1 order by t1 asc"
        )    

    #
    # ------------------- test_tbname_vgroup.py ----------------
    # 
    def do_tbname_vgroup(self):
        tdSql.execute("drop database if exists tbname_vgroup")
        tdSql.execute("create database if not exists tbname_vgroup")
        tdSql.execute('use tbname_vgroup')
        tdSql.execute('drop database if exists dbvg')
        tdSql.execute('create database dbvg vgroups 8;')

        tdSql.execute('use dbvg;')

        tdSql.execute('create table st(ts timestamp, f int) tags (t int);')

        sql = "insert into  "
        sql += " ct1 using st tags(1) values('2021-04-19 00:00:01', 1)"
        sql += " ct2 using st tags(2) values('2021-04-19 00:00:02', 2)"
        sql += " ct3 using st tags(3) values('2021-04-19 00:00:03', 3)"
        sql += " ct4 using st tags(4) values('2021-04-19 00:00:04', 4)"
        tdSql.execute(sql)

        tdSql.query("select * from st where tbname='ct1'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from st where tbname='ct3'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(0, 2, 3)

        tdSql.query("select * from st where tbname='ct3' and f=2")
        tdSql.checkRows(0)

        tdSql.query("select * from st where tbname='ct1' and tbname='ct4'")
        tdSql.checkRows(0)

        tdSql.query("select * from st where tbname='ct1' or tbname='ct4' order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 4)

        tdSql.query("select * from st where tbname='ct2' or tbname='ct3' order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(0, 1, 2)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(1, 1, 3)
        tdSql.checkData(1, 2, 3)

        tdSql.query("select * from st where tbname='ct1' or tbname='ct4' or tbname='ct3' or tbname='ct2' order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.query("select * from st where tbname='ct4' or 1=1 order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.query("select * from st where tbname in ('ct1') order by ts")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)

        tdSql.query("select * from st where tbname in ('ct1', 'ct2') order by ts")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)

        tdSql.query("select * from st where tbname in ('ct1', 'ct2') or tbname in ('ct3', 'ct4') order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.query("select * from st where tbname in ('ct1', 'ct2') or tbname='ct3' order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)

        tdSql.query("select * from st where tbname in ('ct1', 'ct2') and tbname='ct3' order by ts")
        tdSql.checkRows(0)

        tdSql.query("select * from st where tbname in ('ct1') or 1=1 order by ts")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(1, 0, datetime.datetime(2021, 4, 19, 0, 0, 2))
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(2, 0, datetime.datetime(2021, 4, 19, 0, 0, 3))
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 3)
        tdSql.checkData(3, 0, datetime.datetime(2021, 4, 19, 0, 0, 4))
        tdSql.checkData(3, 1, 4)
        tdSql.checkData(3, 2, 4)

        tdSql.query("explain select * from st where tbname='ct1'")
        tdSql.checkRows(1)
	
        tdSql.query("select table_name, vgroup_id from information_schema.ins_tables where db_name='dbvg' and type='CHILD_TABLE'")
        print(tdSql.queryResult)
        
        tdSql.query("explain select * from st where tbname in ('ct1', 'ct2')")
        if tdSql.queryResult[0][0].count("Data Exchange 2:1") == 0:
           tdLog.exit("failed, not two vgroups")
        else:
           tdLog.info("select * from st where tbname in ('ct1', 'ct2') involves two vgroups")	

        tdSql.execute('create table st2(ts timestamp, f int) tags (t int);')

        sql = "insert into  "
        sql += " ct21 using st2 tags(1) values('2021-04-19 00:00:01', 1)"
        sql += " ct22 using st2 tags(2) values('2021-04-19 00:00:02', 2)"
        sql += " ct23 using st2 tags(3) values('2021-04-19 00:00:03', 3)"
        sql += " ct24 using st2 tags(4) values('2021-04-19 00:00:04', 4)"
        tdSql.execute(sql)
        
        tdSql.query("select * from st, st2 where st.ts=st2.ts and st.tbname in ('ct1', 'ct2') and st2.tbname in ('ct21', 'ct23')")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, datetime.datetime(2021, 4, 19, 0, 0, 1))
        tdSql.checkData(0, 4, 1)
        tdSql.checkData(0, 5, 1)
        tdSql.execute('drop database tbname_vgroup')
    
        print("do tbname vgroup ...................... [passed]")


    #
    # ------------------- test_tbname_vgroup.py ----------------
    # 
    def td29092(self, dbname="db"):
        tdSql.execute("alter local \'showFullCreateTableColumn\' \'1\'")
        tdSql.execute(f'use {dbname}')
        tdSql.execute('CREATE STABLE `st` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute('CREATE STABLE `st2` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute('CREATE TABLE `t1` USING `st` (`t1`) TAGS (1);')
        tdSql.execute('CREATE TABLE `t2` USING `st` (`t1`) TAGS (2);')
        tdSql.execute('CREATE TABLE `t21` USING `st2` (`t1`) TAGS (21);')
        tdSql.execute('CREATE TABLE `nt` (`ts` TIMESTAMP, `v1` INT);')
        
        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(3):
            tdSql.execute(f"insert into {dbname}.t1 values ( { now_time + i * 1000 }, {i} )")
            tdSql.execute(f"insert into {dbname}.t2 values ( { now_time + i * 1000 }, {i} )")
            tdSql.execute(f"insert into {dbname}.nt values ( { now_time + i * 1000 }, {i} )")
        
        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query("select tbname, count(*) from nt;")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query("select nt.tbname, count(*) from nt;")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query("select tbname, count(*) from nt group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query("select nt.tbname, count(*) from nt group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 

        tdSql.query("select nt.tbname, count(*) from nt group by nt.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "nt")
        tdSql.checkData(0, 1, 3) 

        tdLog.debug(f"--------------  step2:  system table test   ------------------")
        tdSql.query("select tbname, count(*) from information_schema.ins_dnodes")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)

        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select tbname, count(*) from information_schema.ins_dnodes group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdSql.query("select ins_dnodes.tbname, count(*) from information_schema.ins_dnodes group by ins_dnodes.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        
        tdLog.debug(f"--------------  step3:  subtable test   ------------------")
        tdSql.query("select tbname, count(*) from t1")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select t1.tbname, count(*) from t1")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select tbname, count(*) from t1 group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select t1.tbname, count(*) from t1 group by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select t1.tbname, count(*) from t1 group by t1.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.error("select t1.tbname, count(*) from t2 group by t1.tbname")
        tdSql.error("select t1.tbname, count(*) from t1 group by t2.tbname")
        tdSql.error("select t2.tbname, count(*) from t1 group by t1.tbname")
        
        tdLog.debug(f"--------------  step4:  super table test   ------------------")
        tdSql.query("select tbname, count(*) from st group by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select tbname, count(*) from st partition by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select ts, t1 from st where st.tbname=\"t1\"")  
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\"")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\" order by tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, ts from st where tbname=\"t2\" order by st.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select tbname, count(*) from st where tbname=\"t2\" group by tbname order by tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        
        tdSql.query("select tbname, count(*) from st group by tbname order by tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        
        tdSql.query("select tbname, count(*) from st group by st.tbname order by st.tbname")  
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

        tdLog.debug(f"--------------  step4:  join test   ------------------")
        tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select t1.tbname, t2.tbname from t1, t2 where t1.ts=t2.ts and t1.tbname!=t2.tbname order by t1.tbname")  
        tdSql.checkRows(3)
        
        tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        tdSql.checkRows(0)
        
        tdSql.execute(f"insert into t21 values ( { now_time + 1000 }, 1 )")
        tdSql.query("select st.tbname, st2.tbname from st, st2 where st.ts=st2.ts and st.tbname!=st2.tbname order by st.tbname")  
        tdSql.checkRows(2)
        
        tdSql.query("select t1.tbname, st2.tbname from t1, st2 where t1.ts=st2.ts and t1.tbname!=st2.tbname order by t1.tbname")  
        tdSql.checkRows(1)
        
        tdSql.query("select nt.ts, st.tbname from nt, st where nt.ts=st.ts order by st.tbname")  
        tdSql.checkRows(6)
        
        tdSql.query("select nt.ts, t1.tbname from nt, t1 where nt.ts=t1.ts order by t1.tbname")  
        tdSql.checkRows(3)

    def ts6532(self, dbname="db"):
        tdSql.execute("alter local \'showFullCreateTableColumn\' \'1\'")
        tdSql.execute(f'use {dbname}')
        tdSql.execute('CREATE STABLE ```s``t``` (`ts` TIMESTAMP, ```v1``` INT) TAGS (```t``1``` INT);')
        tdSql.execute('CREATE STABLE ```s``t2``` (`ts` TIMESTAMP, ```v1``` INT) TAGS (```t``1``` INT);')
        tdSql.execute('CREATE TABLE ```t1``` USING db.```s``t``` TAGS (1);')
        tdSql.execute('CREATE TABLE `t2``` USING ```s``t``` (```t``1```) TAGS (2);')
        tdSql.execute('CREATE TABLE ```t21` USING ```s``t2``` (```t``1```) TAGS (21);')
        tdSql.execute('CREATE TABLE ```n``t``` (```t``s``` TIMESTAMP, ```v1` INT);')

        now_time = int(datetime.datetime.timestamp(datetime.datetime.now()) * 1000)
        for i in range(3):
            tdSql.execute(f"insert into {dbname}.```t1``` values({now_time + i * 1000}, {i})")
            tdSql.execute(f"insert into {dbname}.`t2``` values({now_time + i * 1000}, {i})")
            tdSql.execute(f"insert into {dbname}.```n``t``` values ({now_time + i * 1000}, {i})")

        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query("select tbname, count(*) from ```n``t```;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```n``t```.tbname, count(*) from ```n``t```;")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```n``t```.tbname, count(*) from ```n``t``` group by ```n``t```.tbname")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, 3)

        tdLog.debug(f"--------------  step3:  child table test   ------------------")
        tdSql.query("select tbname, count(*) from ```t1```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select tbname, count(*) from ```t1``` group by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1``` group by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)

        tdSql.query("select ```t1```.tbname, count(*) from ```t1``` group by ```t1```.tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        tdSql.error("select ```t1```.tbname, count(*) from `t2``` group by ```t1```.tbname")
        tdSql.error("select ```t1```.tbname, count(*) from ```t1``` group by `t2```.tbname")
        tdSql.error("select `t2```.tbname, count(*) from ```t1``` group by ```t1```.tbname")
        tdLog.debug(f"--------------  step4:  super table test   ------------------")
        tdSql.query("select tbname, count(*) from ```s``t``` group by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select ts, ```t``1``` from ```s``t``` where ```s``t```.tbname=\"`t1`\"")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(2, 1, 1)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\"")
        tdSql.checkRows(3)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\" order by tbname")
        tdSql.checkRows(3)
        tdSql.query("select tbname, ts from ```s``t``` where tbname=\"t2`\" order by ```s``t```.tbname")
        tdSql.checkRows(3)
        tdSql.query("select tbname, count(*) from ```s``t``` where tbname=\"t2`\" group by tbname order by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` group by tbname order by tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)
        tdSql.query("select tbname, count(*) from ```s``t``` group by ```s``t```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 3)
        tdSql.checkData(1, 1, 3)

        tdLog.debug(f"--------------  step4:  join test   ------------------")
        tdSql.query("select ```t1```.tbname, `t2```.tbname from ```t1```, `t2``` where ```t1```.ts=`t2```.ts and ```t1```.tbname!=`t2```.tbname")
        tdSql.checkRows(3)

        tdSql.query("select ```t1```.tbname, `t2```.tbname from ```t1```, `t2``` where ```t1```.ts=`t2```.ts and ```t1```.tbname!=`t2```.tbname order by ```t1```.tbname")
        tdSql.checkRows(3)

        tdSql.query("select ```s``t```.tbname, ```s``t2```.tbname from ```s``t```, ```s``t2``` where ```s``t```.ts=```s``t2```.ts and ```s``t```.tbname!=```s``t2```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(0)

        tdSql.execute(f"insert into ```t21` values ( { now_time + 1000 }, 1 )")
        tdSql.query("select ```s``t```.tbname, ```s``t2```.tbname from ```s``t```, ```s``t2``` where ```s``t```.ts=```s``t2```.ts and ```s``t```.tbname!=```s``t2```.tbname order by ```s``t```.tbname")
        tdSql.checkRows(2)
        
        tdSql.query("select ```t1```.tbname, ```s``t2```.tbname from ```t1```, ```s``t2``` where ```t1```.ts=```s``t2```.ts and ```t1```.tbname!=```s``t2```.tbname order by ```t1```.tbname")
        tdSql.checkRows(1)
        
        tdSql.query("select ```n``t```.```t``s```, ```s``t```.tbname from ```n``t```, ```s``t``` where ```n``t```.```t``s```=```s``t```.`ts` order by ```s``t```.tbname")
        tdSql.checkRows(6)
        
        tdSql.query("select ```n``t```.```t``s```, ```t1```.tbname from ```n``t```, ```t1``` where ```n``t```.```t``s```=```t1```.`ts` order by ```t1```.tbname")
        tdSql.checkRows(3)

        tdLog.debug(f"--------------  step5:  auto create table/show create table/drop/recreate with show result  ------------------")
        sql = "insert into  "
        tdSql.execute('INSERT INTO `t30``` USING ```s``t``` (```t``1```) TAGS (30) VALUES(now+30s,30);')
        tdSql.execute('INSERT INTO ```t31` USING ```s``t``` (```t``1```) TAGS (31) (`ts`,```v1```) VALUES(now+31s,31);')
        tdSql.execute('INSERT INTO `t3``2` USING ```s``t``` (```t``1```) TAGS (32) (```v1```,`ts`) VALUES(32,now+32s);')
        tdSql.execute('INSERT INTO ```t3``3``` USING ```s``t``` (```t``1```) TAGS (33) (```v1```,ts) VALUES(33,now+33s);')
        tdSql.execute('INSERT INTO `````t3````4````` USING ```s``t``` (```t``1```) TAGS (34) (ts,```v1```) VALUES(now+34s,34);')
        
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")  
        tdSql.checkRows(7)
        self.column_dict = {'`t30```':'t30`','```t31`':'`t31','`t3``2`':'t3`2','```t3``3```':'`t3`3`','`````t3````4`````':'``t3``4``'}
        self.show_create_result = []
        for key, value in self.column_dict.items():
            tdSql.query(f"select tbname, count(*) from {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select {key}.tbname, count(*) from {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select tbname, count(*) from {key} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"select {key}.tbname, count(*) from {key} group by tbname")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, 1)
            tdSql.query(f"show create table {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            self.show_create_result.append(tdSql.getData(0, 1))
            tdSql.query(f"drop table {key}")

        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(2)
        for i in range(len(self.show_create_result)):
            tdLog.info(f"{self.show_create_result[i]}")
            tdSql.query(f"{self.show_create_result[i]}")
        tdSql.query("select tbname, count(*) from ```s``t``` partition by tbname")
        tdSql.checkRows(7)

        i=0
        for key, value in self.column_dict.items():
            tdSql.query(f"show create table {key}")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, value)
            tdSql.checkData(0, 1, self.show_create_result[i])
            i+=1

        tdLog.debug(f"--------------  step6:  show create normal table/drop/recreate with show result ------------------")
        tdSql.query("show create table ```n``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, "CREATE TABLE ```n``t``` (```t``s``` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```v1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium')")
        showCreateResult = tdSql.getData(0, 1)
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(1)
        tdSql.query("drop table ```n``t```")
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateResult}")
        tdSql.query("show create table ```n``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")
        tdSql.checkData(0, 1, f"{showCreateResult}")
        tdSql.query("show tables like '`n`t`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`n`t`")

        tdLog.debug(f"--------------  step7:  show create super table/drop/recreate with show result ------------------")
        tdSql.query("show create table ```s``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")
        tdSql.checkData(0, 1, "CREATE STABLE ```s``t``` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```v1``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (```t``1``` INT)")
        showCreateResult = tdSql.getData(0, 1)
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(1)
        tdSql.query("drop table ```s``t```")
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateResult}")
        tdSql.query("show create table ```s``t```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")
        tdSql.checkData(0, 1, f"{showCreateResult}")
        tdSql.query("show stables like '`s`t`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`s`t`")

        tdLog.debug(f"--------------  step8:  show create virtual table/drop/recreate with show result ------------------")
        tdSql.execute("create vtable db.```vntb``100```(```ts``` timestamp, ```v0_``0` int from db.```n``t```.```v1`, ```v0_``1` int from db.```n``t```.```v1`)")
        tdSql.execute("create stable db.```vstb``100```(```ts``` timestamp, ```c``0``` int, ```c``1``` int) tags(```t0``` int, `t``1``` varchar(20)) virtual 1")
        tdSql.execute("create vtable db.```vctb``100```(```c``0``` from db.```n``t```.```v1`, ```c``1``` from db.```n``t```.```v1`) using db.```vstb``100``` tags(0, \"0\")")
        # normal table
        tdSql.query("show create vtable db.```vntb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        tdSql.checkData(0, 1, "CREATE VTABLE ```vntb``100``` (```ts``` TIMESTAMP, ```v0_``0` INT FROM `db`.```n``t```.```v1`, ```v0_``1` INT FROM `db`.```n``t```.```v1`)")
        showCreateVntb = tdSql.getData(0, 1)
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop vtable db.```vntb``100```")
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVntb}")
        tdSql.query("show create table db.```vntb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        tdSql.checkData(0, 1, f"{showCreateVntb}")
        tdSql.query("show vtables like '`vntb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vntb`100`")
        # child tble
        tdSql.query("show create vtable db.```vctb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        tdSql.checkData(0, 1, "CREATE VTABLE ```vctb``100``` (```c``0``` FROM `db`.```n``t```.```v1`, ```c``1``` FROM `db`.```n``t```.```v1`) USING ```vstb``100``` (```t0```, `t``1```) TAGS (0, \"0\")")
        showCreateVctb = tdSql.getData(0, 1)
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop vtable db.```vctb``100```")
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVctb}")
        tdSql.query("show create table db.```vctb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        tdSql.checkData(0, 1, f"{showCreateVctb}")
        tdSql.query("show vtables like '`vctb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vctb`100`")
        #super table
        tdSql.query("show create vtable db.```vstb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")
        tdSql.checkData(0, 1, "CREATE STABLE ```vstb``100``` (```ts``` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', ```c``0``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', ```c``1``` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (```t0``` INT, `t``1``` VARCHAR(20)) VIRTUAL 1")
        showCreateVstb = tdSql.getData(0, 1)
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(1)
        tdSql.query("drop table db.```vstb``100```")
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(0)
        tdSql.query(f"{showCreateVstb}")
        tdSql.query("show create table db.```vstb``100```")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")
        tdSql.checkData(0, 1, f"{showCreateVstb}")
        tdSql.query("show stables like '`vstb`100`'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "`vstb`100`")

    def do_tbname(self):
        tdSql.prepare()
        self.ts6532()
        self.td29092()    

        print("do tbname ............................. [passed]")


    #
    # ------------------- tbname in ----------------
    # 
    def do_tbnamein(self, dbname="db"):
        tdSql.execute(f'drop database if exists {dbname}')
        tdSql.execute(f'create database {dbname}')
        tdSql.execute(f'use {dbname}')
        tdSql.execute(f'CREATE STABLE {dbname}.`st1` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute(f'CREATE STABLE {dbname}.`st2` (`ts` TIMESTAMP, `v1` INT) TAGS (`t1` INT);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t11` USING {dbname}.`st1` (`t1`) TAGS (11);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t12` USING {dbname}.`st1` (`t1`) TAGS (12);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t21` USING {dbname}.`st2` (`t1`) TAGS (21);')
        tdSql.execute(f'CREATE TABLE {dbname}.`t22` USING {dbname}.`st2` (`t1`) TAGS (22);')
        tdSql.execute(f'CREATE TABLE {dbname}.`ta` (`ts` TIMESTAMP, `v1` INT);')
        
        sql = "insert into "
        sql += f" {dbname}.t11 values ( '2025-01-21 00:11:01', 111 )"
        sql += f" {dbname}.t11 values ( '2025-01-21 00:11:02', 112 )"
        sql += f" {dbname}.t11 values ( '2025-01-21 00:11:03', 113 )"
        sql += f" {dbname}.t12 values ( '2025-01-21 00:12:01', 121 )"
        sql += f" {dbname}.t12 values ( '2025-01-21 00:12:02', 122 )"
        sql += f" {dbname}.t12 values ( '2025-01-21 00:12:03', 123 )"

        sql += f" {dbname}.t21 values ( '2025-01-21 00:21:01', 211 )"
        sql += f" {dbname}.t21 values ( '2025-01-21 00:21:02', 212 )"
        sql += f" {dbname}.t21 values ( '2025-01-21 00:21:03', 213 )"
        sql += f" {dbname}.t22 values ( '2025-01-21 00:22:01', 221 )"
        sql += f" {dbname}.t22 values ( '2025-01-21 00:22:02', 222 )"
        sql += f" {dbname}.t22 values ( '2025-01-21 00:22:03', 223 )"

        sql += f" {dbname}.ta values ( '2025-01-21 00:00:01', 1 )"
        sql += f" {dbname}.ta values ( '2025-01-21 00:00:02', 2 )"
        sql += f" {dbname}.ta values ( '2025-01-21 00:00:03', 3 )"
        tdSql.execute(sql)
        
        tdLog.debug(f"--------------  step1:  normal table test   ------------------")
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('t21');")  
        tdSql.checkRows(0)
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') and tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 
  
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') or tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta') or tbname in ('tb');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21') and tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('ta', 't21') and tbname in ('t21');")  
        tdSql.checkRows(0)
        
        tdSql.query(f"select last(*) from {dbname}.ta where tbname in ('t21') or tbname in ('ta');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:00:03')
        tdSql.checkData(0, 1, 3) 

        tdLog.debug(f"--------------  step2:  super table test   ------------------")
        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t11');")  
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:11:03')
        tdSql.checkData(0, 1, 113)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t21');")  
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta', 't21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t21', 't12');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:12:03')
        tdSql.checkData(0, 1, 123)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta') and tbname in ('t12');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12') or tbname in ('t11');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:12:03')
        tdSql.checkData(0, 1, 123)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('ta') or tbname in ('t21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12', 't21') and tbname in ('t21');")
        tdSql.checkRows(0)

        tdSql.query(f"select last(*) from {dbname}.st1 where tbname in ('t12', 't11') and tbname in ('t11');")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2025-01-21 00:11:03')
        tdSql.checkData(0, 1, 113)    

        print("do tbname in .......................... [passed]")

    #
    # ------------------- main ----------------
    # 
    def test_query_pseudo_tbname(self):
        """Pseudo column tbname
        
        1. Create 1 database 1 stable 2000 child tables
        2. Insert 10 rows for each child table
        3. Select tbname from stable with tbname in (...) 
        4. Select tbname from child table with tbname in (...) 
        5. Select tbname from stable with tbname in (...) and other tag filtering 
        6. Select tbname from stable with tbname in (...) and group by tag 
        7. Select tbname from stable with duplicated tbnames in (...) 
        8. Select tbname from stable with wrong tbnames in (...) 
        9. Select tbname from stable with tbname in (...) and column filtering 
        10. Select tbname from stable with tbname in (...) with Upper case table name
        11. Restart dnode and check tbname in query again
        12. Query tbname in where clause
        13. Query tbname in join condition
        14. Query tbname with special characters: ` (backquote)
        15. Query tbname with special characters in virtual table
        16. Query tbname in virtual table
        17. Show create table tbname with special characters: ` (backquote)
        18. Show create table tbname in virtual table
        19. Drop and recreate tables with special characters: ` (backquote)
        20. Drop and recreate virtual table with special characters: ` (backquote)
        21. Join query with tbname in virtual table
        22. Tbname in clause with special characters in virtual table

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/tbnameIn.sim
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_tbname_vgroup.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_tbnameIn.py
            - 2025-12-09 Alex Duan Migrated from uncatalog/system-test/2-query/test_tbname.py

        """
        self.do_sim_tbname()
        self.do_tbname_vgroup()
        self.do_tbname()
        self.do_tbnamein()
        
        
        