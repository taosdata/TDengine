import random
import queue
import secrets
import time
import threading

from datetime import timezone
from random import randrange
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck
from new_test_framework.utils.common import TDCom, tdCom
from tzlocal import get_localzone


ROUND: int = 500

class TestInterp:
    updatecfgDict = {'asynclog': 0, 'ttlUnit': 1, 'ttlPushInterval': 5, 'ratioOfVnodeStreamThrea': 4, 'debugFlag': 143}
    check_failed: bool = False

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        
    #
    # ------------------ test_interp.py ------------------
    #
    def interp_on_empty_table(self):
        dbname = "db"
        tbname = "t"        

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(f'''create table if not exists {dbname}.{tbname} (ts timestamp, k int)''')

        tdLog.printNoPrefix("==========step2:interp query on empty table")

        tdSql.query(f"select _irowts, interp(k),k from {dbname}.{tbname} partition by k range(now()-1h, now()) every(1m) fill(prev)")
        tdSql.checkRows(0)
        
        tdSql.query(f"select _irowts, interp(k),k from {dbname}.{tbname} partition by k range(now()-1h, now()) every(1m) fill(next)")
        tdSql.checkRows(0)
        
        tdSql.query(f"select _irowts, interp(k),k from {dbname}.{tbname} partition by k range(now()-1h, now()) every(1m) fill(linear)")
        tdSql.checkRows(0)
        
        tdSql.query(f"select _irowts, interp(k),k from {dbname}.{tbname} partition by k range(now()-1h, now()) every(1m) fill(value, 2)")        
        tdSql.checkRows(0)
        
    def ts5181(self):
        tdSql.execute("create database db1 keep 36500")
        tdSql.execute("use db1")

        tdSql.execute("CREATE STABLE db1.`stb1` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `v1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`t1` INT, t2 nchar(20))")

        sql = "insert into"
        sql += "  db1.ttt_10000     using db1.stb1   tags(44400, '_ttt_10000')  values('2024-02-19 16:05:17.649', 22300  )"
        sql += "  db1.ttt_10000     using db1.stb1   tags(44400, '_ttt_10000')  values('2024-02-19 16:05:48.818',  22300 )"
        sql += "  db1.ttt_10       using db1.stb1    tags( 40  , '_ttt_10')  values('2024-02-19 16:25:36.013',  20    )"
        sql += "  db1.ttt_11       using db1.stb1    tags( 11  , '_ttt_11')   values('2024-02-19 16:39:50.385' , 20     )"  
        sql += "  db1.ttt_11       using db1.stb1    tags( 11  , '_ttt_11')   values('2024-02-19 16:43:51.742' , 20     )"  
        sql += "  db1.ttt_11       using db1.stb1    tags( 11  , '_ttt_11')   values('2024-02-20 08:35:13.518' , 20     )"  
        sql += "  db1.ttt_11       using db1.stb1    tags( 11  , '_ttt_11')   values('2024-02-20 08:58:42.255' , 20     )"  
        sql += "  db1.ttt_11       using db1.stb1    tags( 11  , '_ttt_11')   values('2024-02-21 09:57:49.477' , 20     )"  
        sql += "  db1.`ttt_2024-2-21` using db1.stb1 tags( 11  , '_ttt_2024-2-21') values('2024-02-21 09:58:21.882' , 20     )"  
        sql += "  db1.`ttt_2024-2-21` using db1.stb1 tags( 11  , '_ttt_2024-2-21') values('2024-02-26 16:08:31.675' , 20     )"  
        sql += "  db1.`ttt_2024-2-21` using db1.stb1 tags( 11  , '_ttt_2024-2-21') values('2024-02-26 16:11:43.445' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:12:30.276' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:07.188' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:07.653' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:07.879' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.083' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.273' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.429' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.599' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.775' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:08.940' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:09.110' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:09.254' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:09.409' , NULL   )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:34.750' , 12     )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:49.820' , 12     )"  
        sql += "  db1.`ttt_2024-2-33` using db1.stb1 tags( 11  , '_ttt_2024-2-33') values('2024-02-26 16:25:59.551' , NULL   )"  
        sql += "  db1.ttt_2        using db1.stb1    tags( 2   , '_ttt_2')   values('2024-02-19 15:26:39.644' , 2      )" 
        sql += "  db1.ttt_2        using db1.stb1    tags( 2   , '_ttt_2')   values('2024-02-19 15:26:40.433' , 2      )" 
        sql += "  db1.ttt_3        using db1.stb1    tags( 3   , '_ttt_3')   values('2024-02-19 15:27:22.613' , 1      )" 
        sql += "  db1.ttt_13       using db1.stb1    tags( 3   , '_ttt_13')   values('2024-02-19 15:27:39.719' , 1      )"  
        sql += "  db1.ttt_14       using db1.stb1    tags( 3   , '_ttt_14')   values('2024-02-19 15:28:36.235' , 222    )"  
        sql += "  db1.ttt_14       using db1.stb1    tags( 3   , '_ttt_14')   values('2024-02-19 15:28:59.310' , 222    )"  
        sql += "  db1.ttt_14       using db1.stb1    tags( 3   , '_ttt_14')   values('2024-02-19 15:29:18.897' , 222    )"  
        sql += "  db1.ttt_14       using db1.stb1    tags( 3   , '_ttt_14')   values('2024-02-19 15:50:24.682' , 223    )"  
        sql += "  db1.ttt_4        using db1.stb1    tags( 3   , '_ttt_4')   values('2024-02-19 15:31:19.945' , 222    )" 
        sql += "  db1.ttt_a        using db1.stb1    tags( 3   , '_ttt_a')   values('2024-02-19 15:31:37.915' , 4      )" 
        sql += "  db1.ttt_axxxx    using db1.stb1    tags( NULL, '_ttt_axxxx')   values('2024-02-19 15:31:58.953' , 4      )" 
        sql += "  db1.ttt_axxx     using db1.stb1    tags( 56  , '_ttt_axxx')   values('2024-02-19 15:32:22.323' , NULL   )"  
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-19 15:36:44.625' , 5444   )" 
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-19 15:38:41.479' , 5444   )" 
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-19 15:57:23.249' , 5444   )" 
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-19 16:04:20.465' , 5444   )" 
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-26 16:11:29.364' , 5444   )" 
        sql += "  db1.ttt_123      using db1.stb1    tags( 0   , '_ttt_123')   values('2024-02-19 15:44:52.136' , 223    )" 
        sql += "  db1.ttt_145      using db1.stb1    tags( 0   , '_ttt_145')   values('2024-02-19 15:50:28.580' , 223    )" 
        sql += "  db1.ttt_1465     using db1.stb1    tags( 0   , '_ttt_1465')   values('2024-02-19 15:50:32.493' , 223    )"  
        sql += "  db1.ttt_1465     using db1.stb1    tags( 0   , '_ttt_1465')   values('2024-02-19 15:57:36.866' , 223    )"  
        sql += "  db1.ttt_1465     using db1.stb1    tags( 0   , '_ttt_1465')   values('2024-02-19 16:04:52.794' , 221113 )"  
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-27 08:47:11.366' , 5444   )" 
        sql += "  db1.ttt_444      using db1.stb1    tags( 5633, '_ttt_444')   values('2024-02-28 09:35:46.474' , 5444   )"
        tdSql.execute(sql)

        tdSql.query("select *,tbname from db1.stb1 ;")
        tdSql.checkRows(51)

        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) fill(prev)")
        tdSql.checkRows(4)
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) fill(prev) order by tbname")
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(0, 3, "ttt_13")
        tdSql.checkData(1, 3, "ttt_14")
        tdSql.checkData(2, 3, "ttt_2")
        tdSql.checkData(3, 3, "ttt_3")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(value, 0) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, "ttt_13")
        
        tdSql.query("select _irowts as ts,interp(v1),tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(value, 0) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, "ttt_123")
        tdSql.checkData(1, 2, "ttt_13")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NULL) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, "ttt_13")
        
        tdSql.query("select _irowts as ts,interp(v1),t1 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NULL) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(1, 2, 3)
        
        tdSql.query("select _irowts as ts,interp(v1), tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NULL) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, "ttt_123")
        tdSql.checkData(1, 2, "ttt_13")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NULL) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, "ttt_13")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(LINEAR) order by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, "ttt_14")
        
        tdSql.query("select _irowts as ts,interp(v1), tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(LINEAR) order by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, "ttt_14")
        
        tdSql.query("select _irowts as ts,interp(v1),t1 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(LINEAR) order by tbname")
        tdSql.checkRows(1)
        tdSql.checkData(0, 2, 3)
        
        tdSql.query("select _irowts as ts,interp(v1),t1, tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NEXT) order by tbname")
        tdSql.checkRows(9)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        
        tdSql.query("select _irowts as ts,interp(v1),t1 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NEXT) order by tbname")
        tdSql.checkRows(9)
        tdSql.checkData(0, 2, 0)
        
        tdSql.query("select _irowts as ts,interp(v1),tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NEXT) order by tbname")
        tdSql.checkRows(9)
        tdSql.checkData(0, 2, "ttt_123")
        
        tdSql.query("select _irowts as ts,interp(v1),t1, tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(NULL_F) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        
        tdSql.query("select _irowts as ts,interp(v1),t1, tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(VALUE_F, 5) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "ttt_123")
        
        tdSql.query("select _irowts as ts,interp(v1),t1, t2, tbname from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(VALUE_F, 5) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "_ttt_123")
        tdSql.checkData(0, 4, "ttt_123")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname, t2 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) fill(prev)")
        tdSql.checkRows(4)
        
        tdSql.query("select _irowts as ts,interp(v1),t1,tbname, t2 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) fill(prev) order by tbname")
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(3, 2, 3)
        tdSql.checkData(0, 3, "ttt_13")
        tdSql.checkData(1, 3, "ttt_14")
        tdSql.checkData(2, 3, "ttt_2")
        tdSql.checkData(3, 3, "ttt_3")
        tdSql.checkData(0, 4, "_ttt_13")
        tdSql.checkData(1, 4, "_ttt_14")
        tdSql.checkData(2, 4, "_ttt_2")
        tdSql.checkData(3, 4, "_ttt_3")
        
        tdSql.query("select _irowts as ts,interp(v1),t1,t2 from db1.stb1 \
            where ts>'2024-02-19T15:25:00+08:00' and ts<'2024-02-19T16:05:00+08:00' \
            partition by tbname range('2024-02-19T15:30:00+08:00','2024-02-19T15:30:00+08:00') every(1m) \
            fill(value, 0) order by tbname")
        tdSql.checkRows(12)
        tdSql.checkData(0, 2, 0)
        tdSql.checkData(0, 3, "_ttt_123")
        tdSql.checkData(1, 2, 3)
        tdSql.checkData(1, 3, "_ttt_13")
    
    def do_interp(self):
        dbname = "db"
        tbname = "tb"
        tbname1 = "tb1"
        tbname2 = "tb2"
        tbname3 = "tb3"
        stbname = "stb"
        ctbname1 = "ctb1"
        ctbname2 = "ctb2"
        ctbname3 = "ctb3"
        num_of_ctables = 3

        tbname_null = "tb_null"
        ctbname1_null = "ctb1_null"
        ctbname2_null = "ctb2_null"
        ctbname3_null = "ctb3_null"
        stbname_null = "stb_null"

        tbname_single = "tb_single"
        ctbname1_single = "ctb1_single"
        ctbname2_single = "ctb2_single"
        ctbname3_single = "ctb3_single"
        stbname_single = "stb_single"

        tdSql.prepare()

        tdLog.printNoPrefix("==========step1:create table")

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname}
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10), c9 tinyint unsigned, c10 smallint unsigned, c11 int unsigned, c12 bigint unsigned)
            '''
        )

        tdLog.printNoPrefix("==========step2:insert data")

        tdSql.execute(f"use db")

        sql = "insert into"
        sql += f" {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar', 5, 5, 5, 5)"
        sql += f" {dbname}.{tbname} values ('2020-02-01 00:00:10', 10, 10, 10, 10, 10.0, 10.0, true, 'varchar', 'nchar', 10, 10, 10, 10)"
        sql += f" {dbname}.{tbname} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar', 15, 15, 15, 15)"
        tdSql.execute(sql)

        tdLog.printNoPrefix("==========step3:fill null")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15)
        tdSql.checkData(12, 0, None)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(null)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdLog.printNoPrefix("==========step4:fill value")

        ## {. . .}
        col_list = {'c0', 'c1', 'c2', 'c3', 'c9', 'c10', 'c11', 'c12'}
        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
            tdSql.checkRows(13)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(2, 0, 1)
            tdSql.checkData(3, 0, 1)
            tdSql.checkData(4, 0, 1)
            tdSql.checkData(5, 0, 1)
            tdSql.checkData(6, 0, 10)
            tdSql.checkData(7, 0, 1)
            tdSql.checkData(8, 0, 1)
            tdSql.checkData(9, 0, 1)
            tdSql.checkData(10, 0, 1)
            tdSql.checkData(11, 0, 15)
            tdSql.checkData(12, 0, 1)

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
            tdSql.checkRows(13)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(2, 0, 1)
            tdSql.checkData(3, 0, 1)
            tdSql.checkData(4, 0, 1)
            tdSql.checkData(5, 0, 1)
            tdSql.checkData(6, 0, 10)
            tdSql.checkData(7, 0, 1)
            tdSql.checkData(8, 0, 1)
            tdSql.checkData(9, 0, 1)
            tdSql.checkData(10, 0, 1)
            tdSql.checkData(11, 0, 15)
            tdSql.checkData(12, 0, 1)

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
            tdSql.checkRows(13)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(2, 0, 1)
            tdSql.checkData(3, 0, 1)
            tdSql.checkData(4, 0, 1)
            tdSql.checkData(5, 0, 1)
            tdSql.checkData(6, 0, 10)
            tdSql.checkData(7, 0, 1)
            tdSql.checkData(8, 0, 1)
            tdSql.checkData(9, 0, 1)
            tdSql.checkData(10, 0, 1)
            tdSql.checkData(11, 0, 15)
            tdSql.checkData(12, 0, 1)

        for col in col_list:
            tdSql.query(f"select interp({col}) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
            tdSql.checkRows(13)
            tdSql.checkData(0, 0, None)
            tdSql.checkData(1, 0, 5)
            tdSql.checkData(2, 0, None)
            tdSql.checkData(3, 0, None)
            tdSql.checkData(4, 0, None)
            tdSql.checkData(5, 0, None)
            tdSql.checkData(6, 0, 10)
            tdSql.checkData(7, 0, None)
            tdSql.checkData(8, 0, None)
            tdSql.checkData(9, 0, None)
            tdSql.checkData(10, 0, None)
            tdSql.checkData(11, 0, 15)
            tdSql.checkData(12, 0, None)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, None)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, 1.0)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, 1.0)
        tdSql.checkData(3, 0, 1.0)
        tdSql.checkData(4, 0, 1.0)
        tdSql.checkData(5, 0, 1.0)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, 1.0)
        tdSql.checkData(8, 0, 1.0)
        tdSql.checkData(9, 0, 1.0)
        tdSql.checkData(10, 0, 1.0)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, 1.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5.0)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, 10.0)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, 15.0)
        tdSql.checkData(12, 0, None)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1.0)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, true)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)
        tdSql.checkData(4, 0, True)
        tdSql.checkData(5, 0, True)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, True)
        tdSql.checkData(8, 0, True)
        tdSql.checkData(9, 0, True)
        tdSql.checkData(10, 0, True)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, True)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, false)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, False)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, False)
        tdSql.checkData(3, 0, False)
        tdSql.checkData(4, 0, False)
        tdSql.checkData(5, 0, False)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, False)
        tdSql.checkData(8, 0, False)
        tdSql.checkData(9, 0, False)
        tdSql.checkData(10, 0, False)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, False)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, NULL)")
        tdSql.checkRows(13)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, True)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, True)
        tdSql.checkData(12, 0, None)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)

        ## test fill value with string
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 'abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, 0)
        tdSql.checkData(2, 0, 0)
        tdSql.checkData(3, 0, 0)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '123')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 123)
        tdSql.checkData(1, 0, 123)
        tdSql.checkData(2, 0, 123)
        tdSql.checkData(3, 0, 123)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '123.123')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 123)
        tdSql.checkData(1, 0, 123)
        tdSql.checkData(2, 0, 123)
        tdSql.checkData(3, 0, 123)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '12abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 12)
        tdSql.checkData(1, 0, 12)
        tdSql.checkData(2, 0, 12)
        tdSql.checkData(3, 0, 12)

        ## test fill value with scalar expression
        # data types
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c2) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c3) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c4) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3.0)
        tdSql.checkData(1, 0, 3.0)
        tdSql.checkData(2, 0, 3.0)
        tdSql.checkData(3, 0, 3.0)

        tdSql.query(f"select interp(c5) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3.0)
        tdSql.checkData(1, 0, 3.0)
        tdSql.checkData(2, 0, 3.0)
        tdSql.checkData(3, 0, 3.0)

        tdSql.query(f"select interp(c6) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, True)
        tdSql.checkData(1, 0, True)
        tdSql.checkData(2, 0, True)
        tdSql.checkData(3, 0, True)

        # expr types
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1.0 + 2.0)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + 2.5)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + '2')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, 1 + '2.0')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '3' + 'abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(value, '2' + '1abc')")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 3)
        tdSql.checkData(1, 0, 3)
        tdSql.checkData(2, 0, 3)
        tdSql.checkData(3, 0, 3)

        tdLog.printNoPrefix("==========step5:fill prev")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(prev)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, 10)
        tdSql.checkData(8, 0, 10)
        tdSql.checkData(9, 0, 10)
        tdSql.checkData(10, 0, 15)
        tdSql.checkData(11, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(0)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(prev)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(prev)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)

        tdLog.printNoPrefix("==========step6:fill next")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next)")
        tdSql.checkRows(12)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 10)
        tdSql.checkData(7, 0, 15)
        tdSql.checkData(8, 0, 15)
        tdSql.checkData(9, 0, 15)
        tdSql.checkData(10, 0, 15)
        tdSql.checkData(11, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 10)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(next)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 10)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(next)")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("==========step7:fill linear")

        ## {. . .}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 11)
        tdSql.checkData(7, 0, 12)
        tdSql.checkData(8, 0, 13)
        tdSql.checkData(9, 0, 14)
        tdSql.checkData(10, 0, 15)

        ## {} ...
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:01', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(0)

        ## {.}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:03', '2020-02-01 00:00:07') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)

        ## .{}..
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:06', '2020-02-01 00:00:09') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(2, 0, 8)
        tdSql.checkData(3, 0, 9)

        ## .{.}.
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(1s) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 8)
        tdSql.checkData(1, 0, 9)
        tdSql.checkData(2, 0, 10)
        tdSql.checkData(3, 0, 11)
        tdSql.checkData(4, 0, 12)

        ## ..{.}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:13', '2020-02-01 00:00:17') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 13)
        tdSql.checkData(1, 0, 14)
        tdSql.checkData(2, 0, 15)

        ## ... {}
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:16', '2020-02-01 00:00:19') every(1s) fill(linear)")
        tdSql.checkRows(0)

        tdLog.printNoPrefix("==========step8:test _irowts,_isfilled with interp")

        # fill null
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(null)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1, True)
        tdSql.checkData(1, 1, True)
        tdSql.checkData(2, 1, True)
        tdSql.checkData(3, 1, True)
        tdSql.checkData(4, 1, False)
        tdSql.checkData(5, 1, True)
        tdSql.checkData(6, 1, True)
        tdSql.checkData(7, 1, True)
        tdSql.checkData(8, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(null)")
        tdSql.checkRows(13)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(12, 0, '2020-02-01 00:00:16.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)
        tdSql.checkData(12, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        # fill value
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(value, 1)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(value, 1)")
        tdSql.checkRows(13)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(12, 0, '2020-02-01 00:00:16.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)
        tdSql.checkData(12, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # fill prev
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(prev)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(prev)")
        tdSql.checkRows(12)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:15.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:16.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, False)
        tdSql.checkData(11, 1, True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(prev)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        # fill next
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(next)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(next)")
        tdSql.checkRows(12)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(11, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  False)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  False)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, True)
        tdSql.checkData(11, 1, False)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(next)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # fill linear
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:08', '2020-02-01 00:00:12') every(500a) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:08.500')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:09.500')
        tdSql.checkData(4, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.500')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:11.500')
        tdSql.checkData(8, 0, '2020-02-01 00:00:12.000')

        tdSql.checkData(0, 1,  True)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  False)
        tdSql.checkData(5, 1,  True)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:08.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:10.000')
        tdSql.checkData(6, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(7, 0, '2020-02-01 00:00:12.000')
        tdSql.checkData(8, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(9, 0, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)
        tdSql.checkData(6, 1,  True)
        tdSql.checkData(7, 1,  True)
        tdSql.checkData(8, 1,  True)
        tdSql.checkData(9, 1,  True)
        tdSql.checkData(10, 1, False)

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-01 00:00:15') every(2s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1, 0, '2020-02-01 00:00:07.000')
        tdSql.checkData(2, 0, '2020-02-01 00:00:09.000')
        tdSql.checkData(3, 0, '2020-02-01 00:00:11.000')
        tdSql.checkData(4, 0, '2020-02-01 00:00:13.000')
        tdSql.checkData(5, 0, '2020-02-01 00:00:15.000')

        tdSql.checkData(0, 1,  False)
        tdSql.checkData(1, 1,  True)
        tdSql.checkData(2, 1,  True)
        tdSql.checkData(3, 1,  True)
        tdSql.checkData(4, 1,  True)
        tdSql.checkData(5, 1,  False)

        # multiple _irowts,_isfilled
        tdSql.query(f"select interp(c0),_irowts,_isfilled from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(3)

        tdSql.checkData(0,  1, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, '2020-02-01 00:00:07.000')
        tdSql.checkData(3,  1, '2020-02-01 00:00:08.000')
        tdSql.checkData(4,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(5,  1, '2020-02-01 00:00:10.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:11.000')
        tdSql.checkData(7,  1, '2020-02-01 00:00:12.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:13.000')
        tdSql.checkData(9,  1, '2020-02-01 00:00:14.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(0,  2,  False)
        tdSql.checkData(1,  2,  True)
        tdSql.checkData(2,  2,  True)
        tdSql.checkData(3,  2,  True)
        tdSql.checkData(4,  2,  True)
        tdSql.checkData(5,  2,  False)
        tdSql.checkData(6,  2,  True)
        tdSql.checkData(7,  2,  True)
        tdSql.checkData(8,  2,  True)
        tdSql.checkData(9,  2,  True)
        tdSql.checkData(10, 2,  False)

        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c0), _isfilled, _irowts from {dbname}.{tbname} range('2020-02-01 00:00:04', '2020-02-01 00:00:16') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkCols(6)

        cols = (0, 5)
        for i in cols:
          tdSql.checkData(0, i, '2020-02-01 00:00:05.000')
          tdSql.checkData(1, i, '2020-02-01 00:00:06.000')
          tdSql.checkData(2, i, '2020-02-01 00:00:07.000')
          tdSql.checkData(3, i, '2020-02-01 00:00:08.000')
          tdSql.checkData(4, i, '2020-02-01 00:00:09.000')
          tdSql.checkData(5, i, '2020-02-01 00:00:10.000')
          tdSql.checkData(6, i, '2020-02-01 00:00:11.000')
          tdSql.checkData(7, i, '2020-02-01 00:00:12.000')
          tdSql.checkData(8, i, '2020-02-01 00:00:13.000')
          tdSql.checkData(9, i, '2020-02-01 00:00:14.000')
          tdSql.checkData(10, i, '2020-02-01 00:00:15.000')

        cols = (1, 4)
        for i in cols:
          tdSql.checkData(0,  i,  False)
          tdSql.checkData(1,  i,  True)
          tdSql.checkData(2,  i,  True)
          tdSql.checkData(3,  i,  True)
          tdSql.checkData(4,  i,  True)
          tdSql.checkData(5,  i,  False)
          tdSql.checkData(6,  i,  True)
          tdSql.checkData(7,  i,  True)
          tdSql.checkData(8,  i,  True)
          tdSql.checkData(9,  i,  True)
          tdSql.checkData(10, i,  False)

        tdLog.printNoPrefix("==========step9:test intra block interpolation")
        tdSql.execute(f"drop database {dbname}");

        tdSql.prepare()

        tdSql.execute(f"create table if not exists {dbname}.{tbname} (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10))")

        # set two data point has 10 days interval will be stored in different datablocks
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')")
        tdSql.execute(f"insert into {dbname}.{tbname} values ('2020-02-11 00:00:05', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.execute(
            f'''create stable if not exists {dbname}.{stbname}
            (ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 double, c5 float, c6 bool, c7 varchar(10), c8 nchar(10)) tags(t1 int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1} using {dbname}.{stbname} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2} using {dbname}.{stbname} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3} using {dbname}.{stbname} tags(3)
            '''
        )


        sql = "insert into"
        sql += f" {dbname}.{ctbname1} values ('2020-02-01 00:00:01', 1, 1, 1, 1, 1.0, 1.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname1} values ('2020-02-01 00:00:07', 7, 7, 7, 7, 7.0, 7.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname1} values ('2020-02-01 00:00:13', 13, 13, 13, 13, 13.0, 13.0, true, 'varchar', 'nchar')"

        sql += f" {dbname}.{ctbname2} values ('2020-02-01 00:00:03', 3, 3, 3, 3, 3.0, 3.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname2} values ('2020-02-01 00:00:09', 9, 9, 9, 9, 9.0, 9.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname2} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')"

        sql += f" {dbname}.{ctbname3} values ('2020-02-01 00:00:05', 5, 5, 5, 5, 5.0, 5.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname3} values ('2020-02-01 00:00:11', 11, 11, 11, 11, 11.0, 11.0, true, 'varchar', 'nchar')"
        sql += f" {dbname}.{ctbname3} values ('2020-02-01 00:00:17', 17, 17, 17, 17, 17.0, 17.0, true, 'varchar', 'nchar')"
        tdSql.execute(sql)

        tdSql.execute(f"flush database {dbname}");

        # test fill null

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(null)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)
        tdSql.checkData(9, 0, None)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        # test fill value

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(value, 1)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)
        tdSql.checkData(6, 0, 1)
        tdSql.checkData(7, 0, 1)
        tdSql.checkData(8, 0, 1)
        tdSql.checkData(9, 0, 1)
        tdSql.checkData(10, 0, 15)

        # | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 1)
        tdSql.checkData(3, 0, 1)
        tdSql.checkData(4, 0, 1)
        tdSql.checkData(5, 0, 1)

        # test fill prev

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(prev)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)
        tdSql.checkData(5, 0, 5)
        tdSql.checkData(6, 0, 5)
        tdSql.checkData(7, 0, 5)
        tdSql.checkData(8, 0, 5)
        tdSql.checkData(9, 0, 5)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 5)
        tdSql.checkData(3, 0, 5)
        tdSql.checkData(4, 0, 5)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)

        # test fill next

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(next)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)
        tdSql.checkData(6, 0, 15)
        tdSql.checkData(7, 0, 15)
        tdSql.checkData(8, 0, 15)
        tdSql.checkData(9, 0, 15)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 5)
        tdSql.checkData(2, 0, 15)
        tdSql.checkData(3, 0, 15)
        tdSql.checkData(4, 0, 15)
        tdSql.checkData(5, 0, 15)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 15)
        tdSql.checkData(1, 0, 15)

        # test fill linear

        ## | {. | | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-01 00:00:05', '2020-02-11 00:00:06') every(1d) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)
        tdSql.checkData(5, 0, 10)
        tdSql.checkData(6, 0, 11)
        tdSql.checkData(7, 0, 12)
        tdSql.checkData(8, 0, 13)
        tdSql.checkData(9, 0, 14)
        tdSql.checkData(10, 0, 15)

        ## | . | {} | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-03 00:00:05', '2020-02-07 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 7)
        tdSql.checkData(1, 0, 8)
        tdSql.checkData(2, 0, 9)
        tdSql.checkData(3, 0, 10)
        tdSql.checkData(4, 0, 11)

        ## | {. | } | . |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-01-31 00:00:05', '2020-02-05 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(5)
        tdSql.checkData(0, 0, 5)
        tdSql.checkData(1, 0, 6)
        tdSql.checkData(2, 0, 7)
        tdSql.checkData(3, 0, 8)
        tdSql.checkData(4, 0, 9)

        ## | . | { | .} |
        tdSql.query(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 14)
        tdSql.checkData(1, 0, 15)

        tdLog.printNoPrefix("==========step10:test interp with null data")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname1}
            (ts timestamp, c0 int, c1 int)
            '''
        )

        sql = "insert into"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:00', 0,    NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:05', NULL, NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:10', 10,   10)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:15', NULL, NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:20', 20,   NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:25', NULL, NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:30', 30,   30)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:35', 35,   NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:40', 40,   40)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:45', NULL, 45)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:50', 50,   NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:00:55', NULL, NULL)"
        sql += f" {dbname}.{tbname1} values ('2020-02-02 00:01:00', 55,   60)"
        tdSql.execute(sql)

        # test fill linear

        # check c0
        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)
        tdSql.checkData(6, 0, None)
        tdSql.checkData(7, 0, None)
        tdSql.checkData(8, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:01', '2020-02-02 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:03', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:10') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, 10)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(14)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, None)
        tdSql.checkData(12, 0, None)
        tdSql.checkData(13, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:05', '2020-02-02 00:00:20') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, 10)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, None)
        tdSql.checkData(11, 0, None)
        tdSql.checkData(12, 0, None)
        tdSql.checkData(13, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, 20)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:09', '2020-02-02 00:00:11') every(1s) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:10', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 10)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)
        tdSql.checkData(4, 0, None)
        tdSql.checkData(5, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:13') every(1s) fill(linear)")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(1, 0, None)
        tdSql.checkData(2, 0, None)
        tdSql.checkData(3, 0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:12', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(7)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(1,  0, 31)
        tdSql.checkData(2,  0, 32)
        tdSql.checkData(3,  0, 33)
        tdSql.checkData(4,  0, 34)
        tdSql.checkData(5,  0, 35)
        tdSql.checkData(6,  0, 36)
        tdSql.checkData(7,  0, 37)
        tdSql.checkData(8,  0, 38)
        tdSql.checkData(9,  0, 39)
        tdSql.checkData(10, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:25', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(5,  0, 30)
        tdSql.checkData(6,  0, 31)
        tdSql.checkData(7,  0, 32)
        tdSql.checkData(8,  0, 33)
        tdSql.checkData(9,  0, 34)
        tdSql.checkData(10, 0, 35)
        tdSql.checkData(11, 0, 36)
        tdSql.checkData(12, 0, 37)
        tdSql.checkData(13, 0, 38)
        tdSql.checkData(14, 0, 39)
        tdSql.checkData(15, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:20', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, 20)
        tdSql.checkData(10, 0, 30)
        tdSql.checkData(11, 0, 31)
        tdSql.checkData(12, 0, 32)
        tdSql.checkData(13, 0, 33)
        tdSql.checkData(14, 0, 34)
        tdSql.checkData(15, 0, 35)
        tdSql.checkData(16, 0, 36)
        tdSql.checkData(17, 0, 37)
        tdSql.checkData(18, 0, 38)
        tdSql.checkData(19, 0, 39)
        tdSql.checkData(20, 0, 40)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(1,  0, 31)
        tdSql.checkData(2,  0, 32)
        tdSql.checkData(3,  0, 33)
        tdSql.checkData(4,  0, 34)
        tdSql.checkData(5,  0, 35)
        tdSql.checkData(6,  0, 36)
        tdSql.checkData(7,  0, 37)
        tdSql.checkData(8,  0, 38)
        tdSql.checkData(9,  0, 39)
        tdSql.checkData(10, 0, 40)
        tdSql.checkData(20, 0, 50)

        tdSql.query(f"select interp(c0) from {dbname}.{tbname1} range('2020-02-02 00:00:20', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, 20)
        tdSql.checkData(10, 0, 30)
        tdSql.checkData(11, 0, 31)
        tdSql.checkData(12, 0, 32)
        tdSql.checkData(13, 0, 33)
        tdSql.checkData(14, 0, 34)
        tdSql.checkData(15, 0, 35)
        tdSql.checkData(16, 0, 36)
        tdSql.checkData(17, 0, 37)
        tdSql.checkData(18, 0, 38)
        tdSql.checkData(19, 0, 39)
        tdSql.checkData(20, 0, 40)
        tdSql.checkData(30, 0, 50)

        # check c1
        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-01 23:59:59', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:08') every(1s) fill(linear)")
        tdSql.checkRows(9)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:10') every(1s) fill(linear)")
        tdSql.checkRows(11)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:15') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:20') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:25') every(1s) fill(linear)")
        tdSql.checkRows(26)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)
        tdSql.checkData(21,  0, None)
        tdSql.checkData(22,  0, None)
        tdSql.checkData(23,  0, None)
        tdSql.checkData(24,  0, None)
        tdSql.checkData(25,  0, None)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:30') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, None)
        tdSql.checkData(1,  0, None)
        tdSql.checkData(2,  0, None)
        tdSql.checkData(3,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None)
        tdSql.checkData(6,  0, None)
        tdSql.checkData(7,  0, None)
        tdSql.checkData(8,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(11,  0, None)
        tdSql.checkData(12,  0, None)
        tdSql.checkData(13,  0, None)
        tdSql.checkData(14,  0, None)
        tdSql.checkData(15,  0, None)
        tdSql.checkData(16,  0, None)
        tdSql.checkData(17,  0, None)
        tdSql.checkData(18,  0, None)
        tdSql.checkData(19,  0, None)
        tdSql.checkData(20,  0, None)
        tdSql.checkData(21,  0, None)
        tdSql.checkData(22,  0, None)
        tdSql.checkData(23,  0, None)
        tdSql.checkData(24,  0, None)
        tdSql.checkData(25,  0, None)
        tdSql.checkData(26,  0, None)
        tdSql.checkData(27,  0, None)
        tdSql.checkData(28,  0, None)
        tdSql.checkData(29,  0, None)
        tdSql.checkData(30,  0, 30)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:35') every(1s) fill(linear)")
        tdSql.checkRows(36)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:40') every(1s) fill(linear)")
        tdSql.checkRows(41)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(46)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(51)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:00:55') every(1s) fill(linear)")
        tdSql.checkRows(56)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(61)
        tdSql.checkData(10, 0, 10)
        tdSql.checkData(30, 0, 30)
        tdSql.checkData(40, 0, 40)
        tdSql.checkData(41, 0, 41)
        tdSql.checkData(42, 0, 42)
        tdSql.checkData(43, 0, 43)
        tdSql.checkData(44, 0, 44)
        tdSql.checkData(45, 0, 45)
        tdSql.checkData(60, 0, 60)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:40', '2020-02-02 00:00:45') every(1s) fill(linear)")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, 40)
        tdSql.checkData(1, 0, 41)
        tdSql.checkData(2, 0, 42)
        tdSql.checkData(3, 0, 43)
        tdSql.checkData(4, 0, 44)
        tdSql.checkData(5, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:35', '2020-02-02 00:00:50') every(1s) fill(linear)")
        tdSql.checkRows(16)
        tdSql.checkData(5,  0, 40)
        tdSql.checkData(6,  0, 41)
        tdSql.checkData(7,  0, 42)
        tdSql.checkData(8,  0, 43)
        tdSql.checkData(9,  0, 44)
        tdSql.checkData(10, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:35', '2020-02-02 00:00:55') every(1s) fill(linear)")
        tdSql.checkRows(21)
        tdSql.checkData(5,  0, 40)
        tdSql.checkData(6,  0, 41)
        tdSql.checkData(7,  0, 42)
        tdSql.checkData(8,  0, 43)
        tdSql.checkData(9,  0, 44)
        tdSql.checkData(10, 0, 45)

        tdSql.query(f"select interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:30', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(31)
        tdSql.checkData(0,  0, 30)
        tdSql.checkData(10, 0, 40)
        tdSql.checkData(11, 0, 41)
        tdSql.checkData(12, 0, 42)
        tdSql.checkData(13, 0, 43)
        tdSql.checkData(14, 0, 44)
        tdSql.checkData(15, 0, 45)
        tdSql.checkData(30, 0, 60)

        # two interps
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(linear)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 31)
        tdSql.checkData(32, 0, 32)
        tdSql.checkData(33, 0, 33)
        tdSql.checkData(34, 0, 34)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 36)
        tdSql.checkData(37, 0, 37)
        tdSql.checkData(38, 0, 38)
        tdSql.checkData(39, 0, 39)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 41)
        tdSql.checkData(42, 1, 42)
        tdSql.checkData(43, 1, 43)
        tdSql.checkData(44, 1, 44)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill null
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(null)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, None)
        tdSql.checkData(34, 0, None)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, None)
        tdSql.checkData(39, 0, None)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, None)
        tdSql.checkData(44, 1, None)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill value
        tdSql.query(f"select _irowts, interp(c0), _irowts, interp(c1), _irowts from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(value, 123, 456)")
        tdSql.checkRows(61)
        tdSql.checkCols(5)
        tdSql.checkData(0,  1, 0)    #
        tdSql.checkData(1,  1, 123)
        tdSql.checkData(4,  1, 123)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 123)
        tdSql.checkData(9,  1, 123)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 123)
        tdSql.checkData(14, 1, 123)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, 123)
        tdSql.checkData(19, 1, 123)
        tdSql.checkData(20, 1, 20)   #
        tdSql.checkData(21, 1, 123)
        tdSql.checkData(24, 1, 123)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 123)
        tdSql.checkData(29, 1, 123)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 123)
        tdSql.checkData(34, 1, 123)
        tdSql.checkData(35, 1, 35)   #
        tdSql.checkData(36, 1, 123)
        tdSql.checkData(39, 1, 123)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 123)
        tdSql.checkData(44, 1, 123)
        tdSql.checkData(45, 1, None) #
        tdSql.checkData(46, 1, 123)
        tdSql.checkData(49, 1, 123)
        tdSql.checkData(50, 1, 50)   #
        tdSql.checkData(51, 1, 123)
        tdSql.checkData(54, 1, 123)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(59, 1, 123)
        tdSql.checkData(60, 1, 55)   #

        tdSql.checkData(0,  3, None) #
        tdSql.checkData(1,  3, 456)
        tdSql.checkData(4,  3, 456)
        tdSql.checkData(5,  3, None) #
        tdSql.checkData(6,  3, 456)
        tdSql.checkData(9,  3, 456)
        tdSql.checkData(10, 3, 10)   #
        tdSql.checkData(11, 3, 456)
        tdSql.checkData(14, 3, 456)
        tdSql.checkData(15, 3, None) #
        tdSql.checkData(16, 3, 456)
        tdSql.checkData(19, 3, 456)
        tdSql.checkData(20, 3, None) #
        tdSql.checkData(21, 3, 456)
        tdSql.checkData(24, 3, 456)
        tdSql.checkData(25, 3, None) #
        tdSql.checkData(26, 3, 456)
        tdSql.checkData(29, 3, 456)
        tdSql.checkData(30, 3, 30)   #
        tdSql.checkData(31, 3, 456)
        tdSql.checkData(34, 3, 456)
        tdSql.checkData(35, 3, None) #
        tdSql.checkData(36, 3, 456)
        tdSql.checkData(39, 3, 456)
        tdSql.checkData(40, 3, 40)   #
        tdSql.checkData(41, 3, 456)
        tdSql.checkData(44, 3, 456)
        tdSql.checkData(45, 3, 45)   #
        tdSql.checkData(46, 3, 456)
        tdSql.checkData(49, 3, 456)
        tdSql.checkData(50, 3, None) #
        tdSql.checkData(51, 3, 456)
        tdSql.checkData(54, 3, 456)
        tdSql.checkData(55, 3, None) #
        tdSql.checkData(56, 3, 456)
        tdSql.checkData(59, 3, 456)
        tdSql.checkData(60, 3, 60)   #

        tdSql.query(f"select _isfilled, interp(c0), _isfilled, interp(c1), _isfilled from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(value, 123 + 123, 234 + 234)")
        tdSql.checkRows(61)
        tdSql.checkCols(5)
        tdSql.checkData(0,  1, 0)    #
        tdSql.checkData(1,  1, 246)
        tdSql.checkData(4,  1, 246)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 246)
        tdSql.checkData(9,  1, 246)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 246)
        tdSql.checkData(14, 1, 246)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, 246)
        tdSql.checkData(19, 1, 246)
        tdSql.checkData(20, 1, 20)   #
        tdSql.checkData(21, 1, 246)
        tdSql.checkData(24, 1, 246)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 246)
        tdSql.checkData(29, 1, 246)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 246)
        tdSql.checkData(34, 1, 246)
        tdSql.checkData(35, 1, 35)   #
        tdSql.checkData(36, 1, 246)
        tdSql.checkData(39, 1, 246)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 246)
        tdSql.checkData(44, 1, 246)
        tdSql.checkData(45, 1, None) #
        tdSql.checkData(46, 1, 246)
        tdSql.checkData(49, 1, 246)
        tdSql.checkData(50, 1, 50)   #
        tdSql.checkData(51, 1, 246)
        tdSql.checkData(54, 1, 246)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(59, 1, 246)
        tdSql.checkData(60, 1, 55)   #

        tdSql.checkData(0,  3, None) #
        tdSql.checkData(1,  3, 468)
        tdSql.checkData(4,  3, 468)
        tdSql.checkData(5,  3, None) #
        tdSql.checkData(6,  3, 468)
        tdSql.checkData(9,  3, 468)
        tdSql.checkData(10, 3, 10)   #
        tdSql.checkData(11, 3, 468)
        tdSql.checkData(14, 3, 468)
        tdSql.checkData(15, 3, None) #
        tdSql.checkData(16, 3, 468)
        tdSql.checkData(19, 3, 468)
        tdSql.checkData(20, 3, None) #
        tdSql.checkData(21, 3, 468)
        tdSql.checkData(24, 3, 468)
        tdSql.checkData(25, 3, None) #
        tdSql.checkData(26, 3, 468)
        tdSql.checkData(29, 3, 468)
        tdSql.checkData(30, 3, 30)   #
        tdSql.checkData(31, 3, 468)
        tdSql.checkData(34, 3, 468)
        tdSql.checkData(35, 3, None) #
        tdSql.checkData(36, 3, 468)
        tdSql.checkData(39, 3, 468)
        tdSql.checkData(40, 3, 40)   #
        tdSql.checkData(41, 3, 468)
        tdSql.checkData(44, 3, 468)
        tdSql.checkData(45, 3, 45)   #
        tdSql.checkData(46, 3, 468)
        tdSql.checkData(49, 3, 468)
        tdSql.checkData(50, 3, None) #
        tdSql.checkData(51, 3, 468)
        tdSql.checkData(54, 3, 468)
        tdSql.checkData(55, 3, None) #
        tdSql.checkData(56, 3, 468)
        tdSql.checkData(59, 3, 468)
        tdSql.checkData(60, 3, 60)   #

        # test fill prev
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(prev)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, 0)
        tdSql.checkData(4,  0, 0)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, None)
        tdSql.checkData(9,  0, None)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, 10)
        tdSql.checkData(14, 0, 10)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, None)
        tdSql.checkData(19, 0, None)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, 20)
        tdSql.checkData(24, 0, 20)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, None)
        tdSql.checkData(29, 0, None)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 30)
        tdSql.checkData(34, 0, 30)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 35)
        tdSql.checkData(39, 0, 35)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, 40)
        tdSql.checkData(44, 0, 40)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, None)
        tdSql.checkData(49, 0, None)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, 50)
        tdSql.checkData(54, 0, 50)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, None)
        tdSql.checkData(59, 0, None)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, None)
        tdSql.checkData(9,  1, None)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, 10)
        tdSql.checkData(14, 1, 10)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, None)
        tdSql.checkData(29, 1, None)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, 30)
        tdSql.checkData(34, 1, 30)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, None)
        tdSql.checkData(39, 1, None)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 40)
        tdSql.checkData(44, 1, 40)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, 45)
        tdSql.checkData(49, 1, 45)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, None)
        tdSql.checkData(59, 1, None)
        tdSql.checkData(60, 1, 60)   #

        # test fill next
        tdSql.query(f"select interp(c0),interp(c1) from {dbname}.{tbname1} range('2020-02-02 00:00:00', '2020-02-02 00:01:00') every(1s) fill(next)")
        tdSql.checkRows(61)
        tdSql.checkCols(2)
        tdSql.checkData(0,  0, 0)    #
        tdSql.checkData(1,  0, None)
        tdSql.checkData(4,  0, None)
        tdSql.checkData(5,  0, None) #
        tdSql.checkData(6,  0, 10)
        tdSql.checkData(9,  0, 10)
        tdSql.checkData(10, 0, 10)   #
        tdSql.checkData(11, 0, None)
        tdSql.checkData(14, 0, None)
        tdSql.checkData(15, 0, None) #
        tdSql.checkData(16, 0, 20)
        tdSql.checkData(19, 0, 20)
        tdSql.checkData(20, 0, 20)   #
        tdSql.checkData(21, 0, None)
        tdSql.checkData(24, 0, None)
        tdSql.checkData(25, 0, None) #
        tdSql.checkData(26, 0, 30)
        tdSql.checkData(29, 0, 30)
        tdSql.checkData(30, 0, 30)   #
        tdSql.checkData(31, 0, 35)
        tdSql.checkData(34, 0, 35)
        tdSql.checkData(35, 0, 35)   #
        tdSql.checkData(36, 0, 40)
        tdSql.checkData(39, 0, 40)
        tdSql.checkData(40, 0, 40)   #
        tdSql.checkData(41, 0, None)
        tdSql.checkData(44, 0, None)
        tdSql.checkData(45, 0, None) #
        tdSql.checkData(46, 0, 50)
        tdSql.checkData(49, 0, 50)
        tdSql.checkData(50, 0, 50)   #
        tdSql.checkData(51, 0, None)
        tdSql.checkData(54, 0, None)
        tdSql.checkData(55, 0, None) #
        tdSql.checkData(56, 0, 55)
        tdSql.checkData(59, 0, 55)
        tdSql.checkData(60, 0, 55)   #

        tdSql.checkData(0,  1, None) #
        tdSql.checkData(1,  1, None)
        tdSql.checkData(4,  1, None)
        tdSql.checkData(5,  1, None) #
        tdSql.checkData(6,  1, 10)
        tdSql.checkData(9,  1, 10)
        tdSql.checkData(10, 1, 10)   #
        tdSql.checkData(11, 1, None)
        tdSql.checkData(14, 1, None)
        tdSql.checkData(15, 1, None) #
        tdSql.checkData(16, 1, None)
        tdSql.checkData(19, 1, None)
        tdSql.checkData(20, 1, None) #
        tdSql.checkData(21, 1, None)
        tdSql.checkData(24, 1, None)
        tdSql.checkData(25, 1, None) #
        tdSql.checkData(26, 1, 30)
        tdSql.checkData(29, 1, 30)
        tdSql.checkData(30, 1, 30)   #
        tdSql.checkData(31, 1, None)
        tdSql.checkData(34, 1, None)
        tdSql.checkData(35, 1, None) #
        tdSql.checkData(36, 1, 40)
        tdSql.checkData(39, 1, 40)
        tdSql.checkData(40, 1, 40)   #
        tdSql.checkData(41, 1, 45)
        tdSql.checkData(44, 1, 45)
        tdSql.checkData(45, 1, 45)   #
        tdSql.checkData(46, 1, None)
        tdSql.checkData(49, 1, None)
        tdSql.checkData(50, 1, None) #
        tdSql.checkData(51, 1, None)
        tdSql.checkData(54, 1, None)
        tdSql.checkData(55, 1, None) #
        tdSql.checkData(56, 1, 60)
        tdSql.checkData(59, 1, 60)
        tdSql.checkData(60, 1, 60)   #

        tdLog.printNoPrefix("==========step11:test multi-interp cases")
        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(null)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, None)
          tdSql.checkData(1, i, None)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, None)
          tdSql.checkData(4, i, None)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(value, 1, 1, 1, 1)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 1)
          tdSql.checkData(1, i, 1)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, 1)
          tdSql.checkData(4, i, 1)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(prev)")
        tdSql.checkRows(5)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 5)
          tdSql.checkData(1, i, 5)
          tdSql.checkData(2, i, 15)
          tdSql.checkData(3, i, 15)
          tdSql.checkData(4, i, 15)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkCols(4)

        for i in range (tdSql.queryCols):
          tdSql.checkData(0, i, 15)
          tdSql.checkData(1, i, 15)
          tdSql.checkData(2, i, 15)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkCols(4)

        tdSql.query(f"select interp(c0),interp(c1),interp(c2),interp(c3),interp(c4),interp(c5) from {dbname}.{tbname} range('2020-02-09 00:00:05', '2020-02-13 00:00:05') every(1d) fill(linear)")
        tdSql.checkRows(3)
        tdSql.checkCols(6)

        for i in range (tdSql.queryCols):
            tdSql.checkData(0, i, 13)

        tdLog.printNoPrefix("==========step12:test interp with boolean type")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname2}
            (ts timestamp, c0 bool)
            '''
        )


        sql = f"insert into {dbname}.{tbname2} values"
        sql += f"  ('2020-02-02 00:00:01', false)"
        sql += f"  ('2020-02-02 00:00:03', true)"
        sql += f"  ('2020-02-02 00:00:05', false)"
        sql += f"  ('2020-02-02 00:00:07', true)"
        sql += f"  ('2020-02-02 00:00:09', true)"
        sql += f"  ('2020-02-02 00:00:11', false)"
        sql += f"  ('2020-02-02 00:00:13', false)"
        sql += f"  ('2020-02-02 00:00:15', NULL)"
        sql += f"  ('2020-02-02 00:00:17', NULL)"
        tdSql.execute(sql)

        # test fill null
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(NULL)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, None)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        # test fill prev
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(18)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:01.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)

        tdSql.checkData(17, 0, '2020-02-02 00:00:18.000')

        # test fill next
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(18)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)

        tdSql.checkData(17, 0, '2020-02-02 00:00:17.000')

        # test fill value
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 1234)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, false)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, true)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, '0')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, '123')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, True)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, True)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, True)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, True)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, True)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, True)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, True)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, 'abc')")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, False)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, False)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, False)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, False)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, False)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, False)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(value, NULL)")
        tdSql.checkRows(19)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:00.000')

        tdSql.checkData(0, 2, None)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, None)
        tdSql.checkData(3, 2, True)
        tdSql.checkData(4, 2, None)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, None)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, None)
        tdSql.checkData(9, 2, True)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, False)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, None)
        tdSql.checkData(18, 2, None)

        tdSql.checkData(18, 0, '2020-02-02 00:00:18.000')

        # test fill linear
        tdSql.query(f"select _irowts,_isfilled,interp(c0) from {dbname}.{tbname2} range('2020-02-02 00:00:00', '2020-02-02 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)
        tdSql.checkCols(3)

        tdSql.checkData(0, 0, '2020-02-02 00:00:01.000')

        tdSql.checkData(0, 2, False)
        tdSql.checkData(1, 2, False)
        tdSql.checkData(2, 2, True)
        tdSql.checkData(3, 2, False)
        tdSql.checkData(4, 2, False)
        tdSql.checkData(5, 2, False)
        tdSql.checkData(6, 2, True)
        tdSql.checkData(7, 2, True)
        tdSql.checkData(8, 2, True)
        tdSql.checkData(9, 2, False)
        tdSql.checkData(10, 2, False)
        tdSql.checkData(11, 2, False)
        tdSql.checkData(12, 2, False)
        tdSql.checkData(13, 2, None)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, None)
        tdSql.checkData(16, 2, None)

        tdSql.checkData(16, 0, '2020-02-02 00:00:17.000')

        tdLog.printNoPrefix("==========step13:test error cases")

        tdSql.error(f"select interp(c0) from {dbname}.{tbname}")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05')")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} every(1s) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where ts >= '2020-02-10 00:00:05' and ts <= '2020-02-15 00:00:05' every(1s) fill(null)")

        # input can only be numerical types
        tdSql.error(f"select interp(ts) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        #tdSql.error(f"select interp(c6) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c7) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c8) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # input can only be columns
        tdSql.error(f"select interp(1) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(1.5) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(true) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(false) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('abcd') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp('中文字符') from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # invalid pseudo column usage
        tdSql.error(f"select interp(_irowts) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(_isfilled) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where _isfilled = true range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} where _irowts > 0 range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(null)")

        # fill value number mismatch
        tdSql.error(f"select interp(c0) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1, 2)")
        tdSql.error(f"select interp(c0), interp(c1) from {dbname}.{tbname} range('2020-02-10 00:00:05', '2020-02-15 00:00:05') every(1d) fill(value, 1)")

        tdLog.printNoPrefix("==========step13:test stable cases")

        # select interp from supertable
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(19)

        tdSql.checkData(0,  2, None)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, None)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, None)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, None)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, None)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, None)
        tdSql.checkData(17, 2, 17)
        tdSql.checkData(18, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(19)

        tdSql.checkData(0,  2, 0)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 0)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 0)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 0)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 0)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 0)
        tdSql.checkData(17, 2, 17)
        tdSql.checkData(18, 2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(18)

        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 17)
        tdSql.checkData(17, 2, 17)

        tdSql.checkData(17, 0, '2020-02-01 00:00:18.000')
        tdSql.checkData(17, 1, True)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(18)

        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 5)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 7)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 11)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 13)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 15)
        tdSql.checkData(16, 2, 17)
        tdSql.checkData(17, 2, 17)

        tdSql.checkData(17, 0, '2020-02-01 00:00:17.000')
        tdSql.checkData(17, 1, False)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 10)
        tdSql.checkData(10, 2, 11)
        tdSql.checkData(11, 2, 12)
        tdSql.checkData(12, 2, 13)
        tdSql.checkData(13, 2, 14)
        tdSql.checkData(14, 2, 15)
        tdSql.checkData(15, 2, 16)
        tdSql.checkData(16, 2, 17)

        # select interp from supertable partition by tbname

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")

        point_idx = {1, 7, 13, 22, 28, 34, 43, 49, 55}
        point_dict = {1:1, 7:7, 13:13, 22:3, 28:9, 34:15, 43:5, 49:11, 55:17}
        rows_per_partition = 19
        tdSql.checkRows(rows_per_partition * num_of_ctables)
        for i in range(num_of_ctables):
          for j in range(rows_per_partition):
            row = j + i * rows_per_partition
            tdSql.checkData(row,  0, f'ctb{i + 1}')
            tdSql.checkData(j,  1, f'2020-02-01 00:00:{j}.000')
            if row in point_idx:
                tdSql.checkData(row, 2, False)
            else:
                tdSql.checkData(row, 2, True)

            if row in point_idx:
                tdSql.checkData(row, 3, point_dict[row])
            else:
                tdSql.checkData(row, 3, None)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")

        point_idx = {1, 7, 13, 22, 28, 34, 43, 49, 55}
        point_dict = {1:1, 7:7, 13:13, 22:3, 28:9, 34:15, 43:5, 49:11, 55:17}
        rows_per_partition = 19
        tdSql.checkRows(rows_per_partition * num_of_ctables)
        for i in range(num_of_ctables):
          for j in range(rows_per_partition):
            row = j + i * rows_per_partition
            tdSql.checkData(row,  0, f'ctb{i + 1}')
            tdSql.checkData(j,  1, f'2020-02-01 00:00:{j}.000')
            if row in point_idx:
                tdSql.checkData(row, 2, False)
            else:
                tdSql.checkData(row, 2, True)

            if row in point_idx:
                tdSql.checkData(row, 3, point_dict[row])
            else:
                tdSql.checkData(row, 3, 0)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")

        tdSql.checkRows(48)
        for i in range(0, 18):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(18, 34):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(34, 48):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:18.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:03.000')
        tdSql.checkData(33, 1, '2020-02-01 00:00:18.000')

        tdSql.checkData(34, 1, '2020-02-01 00:00:05.000')
        tdSql.checkData(47, 1, '2020-02-01 00:00:18.000')

        for i in range(0, 6):
            tdSql.checkData(i, 3, 1)

        for i in range(6, 12):
            tdSql.checkData(i, 3, 7)

        for i in range(12, 18):
            tdSql.checkData(i, 3, 13)

        for i in range(18, 24):
            tdSql.checkData(i, 3, 3)

        for i in range(24, 30):
            tdSql.checkData(i, 3, 9)

        for i in range(30, 34):
            tdSql.checkData(i, 3, 15)

        for i in range(34, 40):
            tdSql.checkData(i, 3, 5)

        for i in range(40, 46):
            tdSql.checkData(i, 3, 11)

        for i in range(46, 48):
            tdSql.checkData(i, 3, 17)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")

        tdSql.checkRows(48)
        for i in range(0, 14):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(14, 30):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(30, 48):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:00.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:13.000')

        tdSql.checkData(14, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(29, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(30, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(47, 1, '2020-02-01 00:00:17.000')

        for i in range(0, 2):
            tdSql.checkData(i, 3, 1)

        for i in range(2, 8):
            tdSql.checkData(i, 3, 7)

        for i in range(8, 14):
            tdSql.checkData(i, 3, 13)

        for i in range(14, 18):
            tdSql.checkData(i, 3, 3)

        for i in range(18, 24):
            tdSql.checkData(i, 3, 9)

        for i in range(24, 30):
            tdSql.checkData(i, 3, 15)

        for i in range(30, 36):
            tdSql.checkData(i, 3, 5)

        for i in range(36, 42):
            tdSql.checkData(i, 3, 11)

        for i in range(42, 48):
            tdSql.checkData(i, 3, 17)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")

        tdSql.checkRows(39)
        for i in range(0, 13):
            tdSql.checkData(i, 0, 'ctb1')

        for i in range(13, 26):
            tdSql.checkData(i, 0, 'ctb2')

        for i in range(26, 39):
            tdSql.checkData(i, 0, 'ctb3')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(12, 1, '2020-02-01 00:00:13.000')

        tdSql.checkData(13, 1, '2020-02-01 00:00:03.000')
        tdSql.checkData(25, 1, '2020-02-01 00:00:15.000')

        tdSql.checkData(26, 1, '2020-02-01 00:00:05.000')
        tdSql.checkData(38, 1, '2020-02-01 00:00:17.000')

        for i in range(0, 13):
            tdSql.checkData(i, 3, i + 1)

        for i in range(13, 26):
            tdSql.checkData(i, 3, i - 10)

        for i in range(26, 39):
            tdSql.checkData(i, 3, i - 21)

        # select interp from supertable partition by column

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(171)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(171)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(90)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(90)

        tdSql.query(f"select c0, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by c0 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(9)

        # select interp from supertable partition by tag

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")
        tdSql.checkRows(57)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(value, 0)")
        tdSql.checkRows(57)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(prev)")
        tdSql.checkRows(48)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(next)")
        tdSql.checkRows(48)

        tdSql.query(f"select t1, _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by t1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(39)

        # select interp from supertable filter

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(27)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(27)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        # select interp from supertable filter limit

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 13")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 20")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 13")
        tdSql.checkRows(13)

        for i in range(13):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 40")
        tdSql.checkRows(39)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where ts between '2020-02-01 00:00:01.000' and '2020-02-01 00:00:13.000' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where c0 <= 13 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where t1 = 1 partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} where tbname = 'ctb1' partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear) limit 10")
        tdSql.checkRows(10)

        for i in range(10):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 1)

        # select interp from supertable with scalar expression

        tdSql.query(f"select _irowts, _isfilled, interp(1 + 1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, 2.0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 + 1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, i + 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 * 2) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, (i + 1) * 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0 + c1) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(linear)")
        tdSql.checkRows(17)

        for i in range(17):
          tdSql.checkData(i, 0, f'2020-02-01 00:00:{i + 1}.000')
          tdSql.checkData(i, 2, (i + 1) * 2)

        # check duplicate timestamp

        # add duplicate timestamp for different child tables
        tdSql.execute(f"insert into {dbname}.{ctbname1} values ('2020-02-01 00:00:15', 15, 15, 15, 15, 15.0, 15.0, true, 'varchar', 'nchar')")

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:18') every(1s) fill(null)")

        tdLog.printNoPrefix("======step 14: test interp ignore null values")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname_null}
            (ts timestamp, c0 int, c1 float, c2 bool)
            '''
        )

        sql = f"insert into {dbname}.{tbname_null} values"
        sql += " ('2020-02-02 00:00:01', 1,    1.0,  true)"
        sql += " ('2020-02-02 00:00:02', NULL, NULL, NULL)"
        sql += " ('2020-02-02 00:00:03', 3,    3.0,  false)"
        sql += " ('2020-02-02 00:00:04', NULL, NULL, NULL)"
        sql += " ('2020-02-02 00:00:05', NULL, NULL, NULL)"
        sql += " ('2020-02-02 00:00:06', 6,    6.0,  true)"
        sql += " ('2020-02-02 00:00:07', NULL, NULL, NULL)"
        sql += " ('2020-02-02 00:00:08', 8,    8.0,  false)"
        sql += " ('2020-02-02 00:00:09', 9,    9.0,  true)"
        sql += " ('2020-02-02 00:00:10', NULL, NULL, NULL)"
        sql += " ('2020-02-02 00:00:11', NULL, NULL, NULL)"
        tdSql.execute(sql)     

        tdSql.execute(
            f'''create table if not exists {dbname}.{stbname_null}
            (ts timestamp, c0 int, c1 float, c2 bool) tags (t0 int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1_null} using {dbname}.{stbname_null} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2_null} using {dbname}.{stbname_null} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3_null} using {dbname}.{stbname_null} tags(3)
            '''
        )

        sql = f"insert into"
        sql += f" {dbname}.{ctbname1_null} values ('2020-02-01 00:00:01', 1, 1.0, true)"
        sql += f" {dbname}.{ctbname1_null} values ('2020-02-01 00:00:07', NULL, NULL, NULL)"
        sql += f" {dbname}.{ctbname1_null} values ('2020-02-01 00:00:13', 13, 13.0, false)"

        sql += f" {dbname}.{ctbname2_null} values ('2020-02-01 00:00:03', NULL, NULL, NULL)"
        sql += f" {dbname}.{ctbname2_null} values ('2020-02-01 00:00:09', 9, 9.0, true)"
        sql += f" {dbname}.{ctbname2_null} values ('2020-02-01 00:00:15', 15, 15.0, false)"

        sql += f" {dbname}.{ctbname3_null} values ('2020-02-01 00:00:05', NULL, NULL, NULL)"
        sql += f" {dbname}.{ctbname3_null} values ('2020-02-01 00:00:11', NULL, NULL, NULL)"
        sql += f" {dbname}.{ctbname3_null} values ('2020-02-01 00:00:17', NULL, NULL, NULL)"
        tdSql.execute(sql)

        # fill null
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(NULL)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(27)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        for i in range(18, 27):
            tdSql.checkData(i, 0, 'ctb3_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:01.000')
        tdSql.checkData(26, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(NULL)")

        tdSql.checkRows(18)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 17):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        # fill value
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 0)
        tdSql.checkData(10, 2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(value, 0)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 0)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 0)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 0)
        tdSql.checkData(10, 2, 0)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 0)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 0)
        tdSql.checkData(2,  2, 0)
        tdSql.checkData(3,  2, 0)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 0)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 0)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(27)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        for i in range(18, 27):
            tdSql.checkData(i, 0, 'ctb3_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        tdSql.checkData(18, 1, '2020-02-01 00:00:01.000')
        tdSql.checkData(26, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(value, 0)")

        tdSql.checkRows(18)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 18):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(17, 1, '2020-02-01 00:00:17.000')

        # fill prev
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 3)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 6)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 9)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(prev)")

        tdSql.checkRows(11)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)
        tdSql.checkData(9,  1, True)
        tdSql.checkData(10, 1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 3)
        tdSql.checkData(4,  2, 3)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 6)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, 9)
        tdSql.checkData(10, 2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 1)
        tdSql.checkData(3,  2, 1)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 9)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 15)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, True)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 1)
        tdSql.checkData(2,  2, 1)
        tdSql.checkData(3,  2, 1)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 9)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(14)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 13):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:17.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(prev)")

        tdSql.checkRows(14)
        for i in range(0, 9):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(9, 13):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(8,  1, '2020-02-01 00:00:17.000')

        tdSql.checkData(9,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(13, 1, '2020-02-01 00:00:17.000')

        # fill next
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 6)
        tdSql.checkData(4,  2, 6)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 8)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(next)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 6)
        tdSql.checkData(4,  2, 6)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 8)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 9)
        tdSql.checkData(2,  2, 9)
        tdSql.checkData(3,  2, 9)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 13)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 9)
        tdSql.checkData(2,  2, 9)
        tdSql.checkData(3,  2, 9)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 13)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(15)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 15):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(14, 1, '2020-02-01 00:00:15.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(next)")

        tdSql.checkRows(15)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 15):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(14, 1, '2020-02-01 00:00:15.000')

        # fill linear
        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(11)
        for i in range (11):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, None)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, None)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)
        tdSql.checkData(9,  2, None)
        tdSql.checkData(10, 2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{tbname_null} range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_null} where c0 is not null range('2020-02-02 00:00:01', '2020-02-02 00:00:11') every(1s) fill(linear)")

        tdSql.checkRows(9)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, False)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, True)
        tdSql.checkData(5,  1, False)
        tdSql.checkData(6,  1, True)
        tdSql.checkData(7,  1, False)
        tdSql.checkData(8,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 2)
        tdSql.checkData(2,  2, 3)
        tdSql.checkData(3,  2, 4)
        tdSql.checkData(4,  2, 5)
        tdSql.checkData(5,  2, 6)
        tdSql.checkData(6,  2, 7)
        tdSql.checkData(7,  2, 8)
        tdSql.checkData(8,  2, 9)

        # super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(9)
        for i in range (9):
          tdSql.checkData(i, 1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, None)
        tdSql.checkData(2,  2, None)
        tdSql.checkData(3,  2, None)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, None)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)
        tdSql.checkData(8,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 5)
        tdSql.checkData(3,  2, 7)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 11)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(8)
        tdSql.checkData(0,  1, False)
        tdSql.checkData(1,  1, True)
        tdSql.checkData(2,  1, True)
        tdSql.checkData(3,  1, True)
        tdSql.checkData(4,  1, False)
        tdSql.checkData(5,  1, True)
        tdSql.checkData(6,  1, False)
        tdSql.checkData(7,  1, False)

        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  2, 5)
        tdSql.checkData(3,  2, 7)
        tdSql.checkData(4,  2, 9)
        tdSql.checkData(5,  2, 11)
        tdSql.checkData(6,  2, 13)
        tdSql.checkData(7,  2, 15)

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0, 1) from {dbname}.{stbname_null} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(11)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 11):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        tdSql.query(f"select tbname, _irowts, _isfilled, interp(c0) from {dbname}.{stbname_null} where c0 is not null partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.checkRows(11)
        for i in range(0, 7):
            tdSql.checkData(i, 0, 'ctb1_null')

        for i in range(7, 11):
            tdSql.checkData(i, 0, 'ctb2_null')

        tdSql.checkData(0,  1, '2020-02-01 00:00:01.000')
        tdSql.checkData(6,  1, '2020-02-01 00:00:13.000')

        tdSql.checkData(7,  1, '2020-02-01 00:00:09.000')
        tdSql.checkData(10, 1, '2020-02-01 00:00:15.000')

        # multiple column with ignoring null value is not allowed

        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c1), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0, 0), interp(c1, 0), interp(c2, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0), interp(c1, 0), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0), interp(c1, 1), interp(c2) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0, 1), interp(c1, 0), interp(c2, 0) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0), interp(c1, 0), interp(c2, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0, 1), interp(c1, 1), interp(c2, 1) from {dbname}.{stbname_null} range('2020-02-01 00:00:01', '2020-02-01 00:00:17') every(2s) fill(linear)")

        tdLog.printNoPrefix("======step 15: test interp pseudo columns")
        tdSql.error(f"select _irowts, c6 from {dbname}.{tbname}")

        tdLog.printNoPrefix("======step 16: test interp in nested query")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{stbname}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{stbname}) partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1}) partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.error(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2}) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select * from {dbname}.{ctbname1} union all select * from {dbname}.{ctbname2} order by ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from (select {ctbname1}.ts,{ctbname1}.c0 from {dbname}.{ctbname1}, {dbname}.{ctbname2} where {ctbname1}.ts = {ctbname2}.ts) range('2020-02-01 00:00:00', '2020-02-01 00:00:14') every(1s) fill(null)")

        tdLog.printNoPrefix("======step 17: test interp single point")
        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname_single}
            (ts timestamp, c0 int)
            '''
        )

        sql = f"insert into {dbname}.{tbname_single} values"
        sql += " ('2020-02-01 00:00:01', 1)"
        sql += " ('2020-02-01 00:00:03', 3)"
        sql += " ('2020-02-01 00:00:05', 5)"
        tdSql.execute(sql)

        tdSql.execute(
            f'''create table if not exists {dbname}.{stbname_single}
            (ts timestamp, c0 int, c1 float, c2 bool) tags (t0 int)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname1_single} using {dbname}.{stbname_single} tags(1)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname2_single} using {dbname}.{stbname_single} tags(2)
            '''
        )

        tdSql.execute(
            f'''create table if not exists {dbname}.{ctbname3_single} using {dbname}.{stbname_single} tags(3)
            '''
        )

        sql = f"insert into"
        sql += f" {dbname}.{ctbname1_single} values ('2020-02-01 00:00:01', 1, 1.0, true)"
        sql += f" {dbname}.{ctbname2_single} values ('2020-02-01 00:00:03', 3, 3.0, false)"
        sql += f" {dbname}.{ctbname3_single} values ('2020-02-01 00:00:05', 5, 5.0, true)"
        tdSql.execute(sql)

        # normal table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{tbname_single} range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        #super table
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(value, 0)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 2)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 4)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        # partition by tbname
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(null)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(null)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(null)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, None)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:00.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(value,0)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        for i in range(1, 3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:01.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:02.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(value,0)")
        tdSql.checkRows(3)
        for i in range(3):
          tdSql.checkData(i,  0, '2020-02-01 00:00:06.000')
          tdSql.checkData(i,  1, True)
          tdSql.checkData(i,  2, 0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(prev)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(prev)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(prev)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, False)
        tdSql.checkData(1,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(prev)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, False)
        tdSql.checkData(1,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(prev)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2,  1, False)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(2,  1, False)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(prev)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:06.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(next)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(next)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:04.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(next)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(next)")
        tdSql.checkRows(2)
        tdSql.checkData(0,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 3)
        tdSql.checkData(1,  0, '2020-02-01 00:00:02.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00', '2020-02-01 00:00:00') every(1s) fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(next)")
        tdSql.checkRows(3)
        tdSql.checkData(0,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(0,  1, True)
        tdSql.checkData(0,  2, 1)
        tdSql.checkData(1,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(1,  1, True)
        tdSql.checkData(1,  2, 3)
        tdSql.checkData(2,  0, '2020-02-01 00:00:00.000')
        tdSql.checkData(2,  1, True)
        tdSql.checkData(2,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:00') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01', '2020-02-01 00:00:01') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:01') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:01.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 1)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02', '2020-02-01 00:00:02') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:02') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03', '2020-02-01 00:00:03') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:03') fill(linear)")
        tdSql.checkData(0,  0, '2020-02-01 00:00:03.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 3)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04', '2020-02-01 00:00:04') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:04') fill(linear)")
        tdSql.checkRows(0)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05', '2020-02-01 00:00:05') every(1s) fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:05') fill(linear)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2020-02-01 00:00:05.000')
        tdSql.checkData(0,  1, False)
        tdSql.checkData(0,  2, 5)

        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06', '2020-02-01 00:00:06') every(1s) fill(linear)")
        tdSql.checkRows(0)
        tdSql.query(f"select _irowts, _isfilled, interp(c0) from {dbname}.{stbname_single} partition by tbname range('2020-02-01 00:00:06') fill(linear)")
        tdSql.checkRows(0)

        #### TS-3799 ####

        tdSql.execute(
            f'''create table if not exists {dbname}.{tbname3} (ts timestamp, c0 double)'''
        )

        sql = f"insert into {dbname}.{tbname3} values"
        sql += f" ('2023-08-06 23:59:51.000000000', 4.233947800000000)"
        sql += f" ('2023-08-06 23:59:52.000000000', 3.606781000000000)"
        sql += f" ('2023-08-06 23:59:52.500000000', 3.162353500000000)"
        sql += f" ('2023-08-06 23:59:53.000000000', 3.162292500000000)"
        sql += f" ('2023-08-06 23:59:53.500000000', 4.998230000000000)"
        sql += f" ('2023-08-06 23:59:54.400000000', 8.800414999999999)"
        sql += f" ('2023-08-06 23:59:54.900000000', 8.853271500000000)"
        sql += f" ('2023-08-06 23:59:55.900000000', 7.507751500000000)"
        sql += f" ('2023-08-06 23:59:56.400000000', 7.510681000000000)"
        sql += f" ('2023-08-06 23:59:56.900000000', 7.841614000000000)"
        sql += f" ('2023-08-06 23:59:57.900000000', 8.153809000000001)"
        sql += f" ('2023-08-06 23:59:58.500000000', 6.866455000000000)"
        sql += f" ('2023-08-06 23:59:59.000000000', 6.869140600000000)"
        sql += f" ('2023-08-07 00:00:00.000000000', 0.261475000000001)"
        tdSql.execute(sql)

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(next)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, 4.233947800000000)

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(value, 1)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, 1)

        tdSql.query(f"select _irowts, interp(c0) from {dbname}.{tbname3} range('2023-08-06 23:59:00','2023-08-06 23:59:59') every(1m) fill(null)")
        tdSql.checkRows(1)
        tdSql.checkData(0,  0, '2023-08-06 23:59:00')
        tdSql.checkData(0,  1, None)

        self.interp_on_empty_table()
        self.ts5181()

        print("\n")
        print("do_interp ............................. [passed]\n")

    #
    # ------------------ test_interp_extension.py ------------------
    #
  
    def create_database(self, tsql, dbName, dropFlag=1, vgroups=2, replica=1, duration: str = '1d'):
        if dropFlag == 1:
            tsql.execute("drop database if exists %s" % (dbName))

        tsql.execute("create database if not exists %s vgroups %d replica %d duration %s" % (
            dbName, vgroups, replica, duration))
        tdLog.debug("complete to create database %s" % (dbName))
        return

    def create_stable(self, tsql, paraDict):
        colString = tdCom.gen_column_type_str(
            colname_prefix=paraDict["colPrefix"], column_elm_list=paraDict["colSchema"])
        tagString = tdCom.gen_tag_type_str(
            tagname_prefix=paraDict["tagPrefix"], tag_elm_list=paraDict["tagSchema"])
        sqlString = f"create table if not exists %s.%s (%s) tags (%s)" % (
            paraDict["dbName"], paraDict["stbName"], colString, tagString)
        tdLog.debug("%s" % (sqlString))
        tsql.execute(sqlString)
        return

    def create_ctable(self, tsql=None, dbName='dbx', stbName='stb', ctbPrefix='ctb', ctbNum=1, ctbStartIdx=0):
        for i in range(ctbNum):
            sqlString = "create table %s.%s%d using %s.%s tags(%d, 'tb%d', 'tb%d', %d, %d, %d)" % (dbName, ctbPrefix, i+ctbStartIdx, dbName, stbName, (i+ctbStartIdx) % 5, i+ctbStartIdx + random.randint(
                1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100), i+ctbStartIdx + random.randint(1, 100))
            tsql.execute(sqlString)

        tdLog.debug("complete to create %d child tables by %s.%s" %
                    (ctbNum, dbName, stbName))
        return

    def init_normal_tb(self, tsql, db_name: str, tb_name: str, rows: int, start_ts: int, ts_step: int):
        sql = 'CREATE TABLE %s.%s (ts timestamp, c1 INT, c2 INT, c3 INT, c4 double, c5 VARCHAR(255))' % (
            db_name, tb_name)
        tsql.execute(sql)
        sql = 'INSERT INTO %s.%s values' % (db_name, tb_name)
        for j in range(rows):
            sql += f'(%d, %d,%d,%d,{random.random()},"varchar_%d"),' % (start_ts + j * ts_step + randrange(500), j %
                                                     10 + randrange(200), j % 10, j % 10, j % 10 + randrange(100))
        tsql.execute(sql)

    def insert_data(self, tsql, dbName, ctbPrefix, ctbNum, rowsPerTbl, batchNum, startTs, tsStep):
        tdLog.debug("start to insert data ............")
        tsql.execute("use %s" % dbName)
        pre_insert = "insert into "
        sql = pre_insert

        for i in range(ctbNum):
            rowsBatched = 0
            sql += " %s.%s%d values " % (dbName, ctbPrefix, i)
            for j in range(rowsPerTbl):
                if (i < ctbNum/2):
                    sql += "(%d, %d, %d, %d,%d,%d,%d,true,'binary%d', 'nchar%d') " % (startTs + j*tsStep + randrange(
                        500), j % 10 + randrange(100), j % 10 + randrange(200), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                else:
                    sql += "(%d, %d, NULL, %d,NULL,%d,%d,true,'binary%d', 'nchar%d') " % (
                        startTs + j*tsStep + randrange(500), j % 10, j % 10, j % 10, j % 10, j % 10, j % 10)
                rowsBatched += 1
                if ((rowsBatched == batchNum) or (j == rowsPerTbl - 1)):
                    tsql.execute(sql)
                    rowsBatched = 0
                    if j < rowsPerTbl - 1:
                        sql = "insert into %s.%s%d values " % (dbName, ctbPrefix, i)
                    else:
                        sql = "insert into "
        if sql != pre_insert:
            tsql.execute(sql)
        tdLog.debug("insert data ............ [OK]")
        return

    def init_data(self, db: str = 'test', ctb_num: int = 10, rows_per_ctb: int = 10000, start_ts: int = 1537146000000, ts_step: int = 500):
        # init
        self.vgroups = 4
        self.ctbNum = 10
        self.rowsPerTbl = 10000
        self.duraion = '1h'

        tdLog.printNoPrefix(
            "======== prepare test env include database, stable, ctables, and insert data: ")
        paraDict = {'dbName':     db,
                    'dropFlag':   1,
                    'vgroups':    4,
                    'stbName':    'meters',
                    'colPrefix':  'c',
                    'tagPrefix':  't',
                    'colSchema':   [{'type': 'INT', 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'FLOAT', 'count': 1}, {'type': 'DOUBLE', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'tinyint', 'count': 1}, {'type': 'bool', 'count': 1}, {'type': 'binary', 'len': 10, 'count': 1}, {'type': 'nchar', 'len': 10, 'count': 1}],
                    'tagSchema':   [{'type': 'INT', 'count': 1}, {'type': 'nchar', 'len': 20, 'count': 1}, {'type': 'binary', 'len': 20, 'count': 1}, {'type': 'BIGINT', 'count': 1}, {'type': 'smallint', 'count': 1}, {'type': 'DOUBLE', 'count': 1}],
                    'ctbPrefix':  't',
                    'ctbStartIdx': 0,
                    'ctbNum':     ctb_num,
                    'rowsPerTbl': rows_per_ctb,
                    'batchNum':   3000,
                    'startTs':    start_ts,
                    'tsStep':     ts_step}

        paraDict['vgroups'] = self.vgroups
        paraDict['ctbNum'] = ctb_num
        paraDict['rowsPerTbl'] = rows_per_ctb

        tdLog.info("create database")
        self.create_database(tsql=tdSql, dbName=paraDict["dbName"], dropFlag=paraDict["dropFlag"],
                             vgroups=paraDict["vgroups"], replica=self.replicaVar, duration=self.duraion)

        tdLog.info("create stb")
        self.create_stable(tsql=tdSql, paraDict=paraDict)

        tdLog.info("create child tables")
        self.create_ctable(tsql=tdSql, dbName=paraDict["dbName"],
                           stbName=paraDict["stbName"], ctbPrefix=paraDict["ctbPrefix"],
                           ctbNum=paraDict["ctbNum"], ctbStartIdx=paraDict["ctbStartIdx"])
        self.insert_data(tsql=tdSql, dbName=paraDict["dbName"],
                         ctbPrefix=paraDict["ctbPrefix"], ctbNum=paraDict["ctbNum"],
                         rowsPerTbl=paraDict["rowsPerTbl"], batchNum=paraDict["batchNum"],
                         startTs=paraDict["startTs"], tsStep=paraDict["tsStep"])
        self.init_normal_tb(tdSql, paraDict['dbName'], 'norm_tb',
                            paraDict['rowsPerTbl'], paraDict['startTs'], paraDict['tsStep'])

    def do_interp_extension(self):
        self.init_data()
        self.check_interp_extension()
        print("do_interp_extension ................... [passed]\n")


    def datetime_add_tz(self, dt):
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=get_localzone())
        return dt

    def binary_search_ts(self, select_results, ts):
        mid = 0
        try:
            found: bool = False
            start = 0
            end = len(select_results) - 1
            while start <= end:
                mid = (start + end) // 2
                if self.datetime_add_tz(select_results[mid][0]) == ts:
                    found = True
                    return mid
                elif self.datetime_add_tz(select_results[mid][0]) < ts:
                    start = mid + 1
                else:
                    end = mid - 1

            if not found:
                tdLog.exit(f"cannot find ts in select results {ts} {select_results}")
            return start
        except Exception as e:
            tdLog.debug(f"{select_results[mid][0]}, {ts}, {len(select_results)}, {select_results[mid]}")
            self.check_failed = True
            tdLog.exit(f"binary_search_ts error: {e}")

    def distance(self, ts1, ts2):
        return abs(self.datetime_add_tz(ts1) - self.datetime_add_tz(ts2))

    ## TODO pass last position to avoid search from the beginning
    def is_nearest(self, select_results, irowts_origin, irowts):
        if len(select_results) <= 1:
            return True
        try:
            #tdLog.debug(f"check is_nearest for: {irowts_origin} {irowts}")
            idx = self.binary_search_ts(select_results, irowts_origin)
            if idx == 0:
                #tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
                if not res:
                    tdLog.debug(f"prev row: null,cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            if idx == len(select_results) - 1:
                #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null")
                res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0])
                if not res:
                    tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: null, irowts_origin: {irowts_origin}, irowts: {irowts}")
                return res
            #tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}")
            res = self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx - 1][0]) and self.distance(irowts, select_results[idx][0]) <= self.distance(irowts, select_results[idx + 1][0])
            if not res:
                tdLog.debug(f"prev row: {select_results[idx - 1]},cur row: {select_results[idx]}, next row: {select_results[idx + 1]}, irowts_origin: {irowts_origin}, irowts: {irowts}")
            return res
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"is_nearest error: {e}")

    ## interp_results: _irowts_origin, _irowts, ..., _isfilled
    ## select_all_results must be sorted by ts in ascending order
    def check_result_for_near(self, interp_results, select_all_results, sql, sql_select_all):
        #tdLog.info(f"check_result_for_near for sql: {sql}, sql_select_all:{sql_select_all}")
        for row in interp_results:
            #tdLog.info(f"row: {row}")
            if row[0].tzinfo is None or row[0].tzinfo.utcoffset(row[0]) is None:
                irowts_origin = row[0].replace(tzinfo=get_localzone())
                irowts = row[1].replace(tzinfo=get_localzone())
            else:
                irowts_origin = row[0]
                irowts = row[1]
            if not self.is_nearest(select_all_results, irowts_origin, irowts):
                self.check_failed = True
                tdLog.exit(f"interp result is not the nearest for row: {row}, {sql}")

    def query_routine(self, sql_queue: queue.Queue, output_queue: queue.Queue):
        try:
            tdcom = TDCom()
            cli = tdcom.newTdSql()
            while True:
                item = sql_queue.get()
                if item is None or self.check_failed:
                    output_queue.put(None)
                    break
                (sql, sql_select_all, _) = item
                cli.query(sql, queryTimes=1)
                interp_results = cli.queryResult
                if sql_select_all is not None:
                    cli.query(sql_select_all, queryTimes=1)
                output_queue.put((sql, interp_results, cli.queryResult, sql_select_all))
            cli.close()
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"query_routine error: {e}")

    def interp_check_near_routine(self, select_all_results, output_queue: queue.Queue):
        try:
            while True:
                item = output_queue.get()
                if item is None:
                    break
                (sql, interp_results, all_results, sql_select_all) = item
                if all_results is not None:
                    self.check_result_for_near(interp_results, all_results, sql, sql_select_all)
                else:
                    self.check_result_for_near(interp_results, select_all_results, sql, None)
        except Exception as e:
            self.check_failed = True
            tdLog.exit(f"interp_check_near_routine error: {e}")

    def create_qt_threads(self, sql_queue: queue.Queue, output_queue: queue.Queue, num: int):
        qts = []
        for _ in range(0, num):
            qt = threading.Thread(target=self.query_routine, args=(sql_queue, output_queue))
            qt.start()
            qts.append(qt)
        return qts

    def wait_qt_threads(self, qts: list):
        for qt in qts:
            qt.join()

    ### first(ts)               | last(ts)
    ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
    def check_interp_fill_extension_near(self):
        sql = f"select last(ts), c1, c2 from test.t0"
        tdSql.query(sql, queryTimes=1)
        lastRow = tdSql.queryResult[0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(61)
        for i in range(0, 61):
            tdSql.checkData(i, 0, lastRow[0])
            tdSql.checkData(i, 2, lastRow[1])
            tdSql.checkData(i, 3, lastRow[2])
            tdSql.checkData(i, 4, True)

        sql = f"select ts, c1, c2 from test.t0 where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' order by ts asc"
        tdSql.query(sql, queryTimes=1)
        select_all_results = tdSql.queryResult
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range('2018-09-17 09:00:00', '2018-09-17 09:00:05') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(6)
        self.check_result_for_near(tdSql.queryResult, select_all_results, sql, None)

        start = 1537146000000
        end = 1537151000000

        tdSql.query("select ts, c1, c2 from test.t0 order by ts asc", queryTimes=1)
        select_all_results = tdSql.queryResult

        qt_threads_num = 4
        sql_queue = queue.Queue()
        output_queue = queue.Queue()
        qts = self.create_qt_threads(sql_queue, output_queue, qt_threads_num)
        ct = threading.Thread(target=self.interp_check_near_routine, args=(select_all_results, output_queue))
        ct.start()
        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            every = random.randint(1, 15)
            #tdLog.debug(f"range_start: {range_start}, range_end: {range_end}")
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_start}, {range_end}) every({every}s) fill(near)"
            sql_queue.put((sql, None, None))

        ### no prev only, no next only, no prev and no next, have prev and have next
        for i in range(0, ROUND):
            range_point = random.randint(start, end)
            ## all data points are can be filled by near
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 range({range_point}, 1h) fill(near, 1, 2)"
            sql_queue.put((sql, None, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            every = random.randint(1, 15)
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_start}, {range_end}) every({every}s) fill(near)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))

        for i in range(0, ROUND):
            range_start = random.randint(start, end)
            range_end = random.randint(range_start, end)
            range_where_start = random.randint(start, end)
            range_where_end = random.randint(range_where_start, end)
            range_point = random.randint(start, end)
            # 检查range_point与range_where_start或range_where_end的间隔是否小于等于1小时(3600000毫秒)
            one_hour_ms = 3600000
            if ((range_point < range_where_start and abs(range_point - range_where_start) > one_hour_ms) or 
                (range_point > range_where_end and abs(range_point - range_where_end) > one_hour_ms)):
                continue
            sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.t0 where ts between {range_where_start} and {range_where_end} range({range_point}, 1h) fill(near, 1, 2)"
            tdSql.query(f'select to_char(cast({range_where_start} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\'), to_char(cast({range_where_end} as timestamp), \'YYYY-MM-DD HH24:MI:SS.MS\')', queryTimes=1)
            where_start_str = tdSql.queryResult[0][0]
            where_end_str = tdSql.queryResult[0][1]
            sql_select_all = f"select ts, c1, c2 from test.t0 where ts between '{where_start_str}' and '{where_end_str}' order by ts asc"
            sql_queue.put((sql, sql_select_all, None))
        for i in range(0, qt_threads_num):
            sql_queue.put(None)
        self.wait_qt_threads(qts)
        ct.join()

        if self.check_failed:
            tdLog.exit("interp check near failed")

    def check_interp_extension_irowts_origin(self):
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near)"
        tdSql.query(sql, queryTimes=1)

        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(linear)"
        tdSql.error(sql, -2147473833)
        sql = f"select _irowts, _irowts_origin, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(NULL_F)"
        tdSql.error(sql, -2147473833)

    def check_interp_fill_extension(self):
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 0, 0)"
        tdSql.query(sql, queryTimes=1)

        ### must specify value
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near)"
        tdSql.error(sql, -2147473915)
        ### num of fill value mismatch
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(near, 1)"
        tdSql.error(sql, -2147473915)

        ### range with around interval cannot specify two timepoints, currently not supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00', 1h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473827) ## syntax error

        ### NULL/linear cannot specify other values
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:02:00') fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        ### cannot have every clause with range around
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) every(1s) fill(prev, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)

        ### cannot specify near/prev/next values when using range
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', '2020-02-01 00:01:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00') every(1s) fill(near, 1, 1)"
        tdSql.error(sql, -2147473915) ## cannot specify values

        ### when range around interval is set, only prev/next/near is supported
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(NULL)"
        tdSql.error(sql, -2147473861) ## TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear, 1, 1)"
        tdSql.error(sql, -2147473920)
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1h) fill(linear)"
        tdSql.error(sql, -2147473861) ## TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE

        ### range interval cannot be 0
        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 0h) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1y) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1n) fill(near, 1, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts, interp(c1), interp(c2), _isfilled from test.meters where ts between '2020-02-01 00:00:00' and '2020-02-01 00:00:00' range('2020-02-01 00:00:00', 1h) fill(near, 1, 1)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(0)

        ### first(ts)               | last(ts)
        ### 2018-09-17 09:00:00.047 | 2018-09-17 10:23:19.863
        sql = "select to_char(first(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        first_ts = tdSql.queryResult[0][0]
        sql = "select to_char(last(ts), 'YYYY-MM-DD HH24:MI:SS.MS') from test.meters"
        tdSql.query(sql, queryTimes=1)
        last_ts = tdSql.queryResult[0][0]
        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2020-02-01 00:00:00', 1d) fill(near, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2020-02-01 00:00:00.000')
        tdSql.checkData(0, 2, 1)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:25:00', 1d) fill(prev, 3, 4)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-18 10:25:00.000')
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 4)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 08:25:00', 1d) fill(next, 5, 6)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, None)
        tdSql.checkData(0, 1, '2018-09-16 08:25:00.000')
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 6)
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-16 09:00:01', 1d) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, first_ts)
        tdSql.checkData(0, 1, '2018-09-16 09:00:01')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('2018-09-18 10:23:19', 1d) fill(prev, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, '2018-09-18 10:23:19')
        tdSql.checkData(0, 4, True)

        sql = f"select _irowts_origin, _irowts, interp(c1), interp(c2), _isfilled from test.meters range('{last_ts}', 1a) fill(next, 1, 2)"
        tdSql.query(sql, queryTimes=1)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)
        tdSql.checkData(0, 1, last_ts)
        tdSql.checkData(0, 4, False)

    def check_interval_fill_extension(self):
        ## not allowed
        sql = f"select count(*) from test.meters interval(1s) fill(near)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select count(*) from test.meters interval(1s) fill(prev, 1)"
        tdSql.error(sql, -2147473920) ## syntax error
        sql = f"select count(*) from test.meters interval(1s) fill(next, 1)"
        tdSql.error(sql, -2147473920) ## syntax error

        sql = f"select _irowts_origin, count(*) from test.meters where ts between '2018-09-17 08:59:59' and '2018-09-17 09:00:06' interval(1s) fill(next)"
        tdSql.error(sql, -2147473918) ## invalid column name _irowts_origin

    def check_interp_fill_extension_stream(self):
        ## near is not supported
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(near);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        ## _irowts_origin is not support
        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts_origin, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(prev);"
        tdSql.error(sql, -2147473851) ## TSDB_CODE_PAR_INVALID_STREAM_QUERY

        sql = f"create stream s1 trigger force_window_close into test.s_res_tb as select _irowts, interp(c1), interp(c2)from test.meters partition by tbname every(1s) fill(next, 1, 1);"
        tdSql.error(sql, -2147473915) ## cannot specify values

    def check_interp_extension(self):
        self.check_interp_fill_extension_near()
        self.check_interp_extension_irowts_origin()
        self.check_interp_fill_extension()
        self.check_interval_fill_extension()
        #self.check_interp_fill_extension_stream()

    #
    # ------------------ test_interp_basic.py ------------------
    #
    def do_interp_basic(self):
        dbPrefix = "intp_db"
        tbPrefix = "intp_tb"
        stbPrefix = "intp_stb"
        tbNum = 4
        rowNum = 1000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== interp.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdLog.info(f"====== create tables")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {stb} (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int)"
        )

        i = 0
        ts = ts0
        halfNum = tbNum / 2
        while i < halfNum:
            tbId = int(i + halfNum)
            tb = tbPrefix + str(int(i))
            tb1 = tbPrefix + str(tbId)
            tdSql.execute(f"create table {tb} using {stb} tags( {i} )")
            tdSql.execute(f"create table {tb1} using {stb} tags( {tbId} )")

            x = 0
            sql = f"insert into {tb} values "
            while x < rowNum:
                xs = x * delta
                ts = ts0 + xs
                c = x % 10
                binary = "'binary" + str(c) + "'"
                nchar = "'nchar" + str(c) + "'"
                sql += f" ({ts} , {c} , {c} , {c} , {c} , {c} , {c} , true, {binary} , {nchar} )  {tb1} values ( {ts} , {c} , NULL , {c} , NULL , {c} , {c} , true, {binary} , {nchar} )"
                x = x + 1
            tdSql.execute(sql)
            i = i + 1

        tdLog.info(f"====== tables created")

        tdSql.execute(f"create table ap1 (ts timestamp, pav float);")
        tdSql.execute(
            f"INSERT INTO ap1 VALUES ('2021-07-25 02:19:54.100',1) ('2021-07-25 02:19:54.200',2) ('2021-07-25 02:19:54.300',3) ('2021-07-25 02:19:56.500',4) ('2021-07-25 02:19:57.500',5) ('2021-07-25 02:19:57.600',6) ('2021-07-25 02:19:57.900',7) ('2021-07-25 02:19:58.100',8) ('2021-07-25 02:19:58.300',9) ('2021-07-25 02:19:59.100',10) ('2021-07-25 02:19:59.300',11) ('2021-07-25 02:19:59.500',12) ('2021-07-25 02:19:59.700',13) ('2021-07-25 02:19:59.900',14) ('2021-07-25 02:20:05.000', 20) ('2021-07-25 02:25:00.000', 10000);"
        )

        self.interp_test()

        tdLog.info(f"================== restart server to commit data into disk")
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"================== server restart completed")

        self.interp_test()

        tdLog.info(f"================= TD-5931")
        tdSql.execute(f"create stable st5931(ts timestamp, f int) tags(t int)")
        tdSql.execute(f"create table ct5931 using st5931 tags(1)")
        tdSql.execute(f"create table nt5931(ts timestamp, f int)")
        tdSql.error(f"select interp(*) from nt5931 where ts=now")
        tdSql.error(f"select interp(*) from st5931 where ts=now")
        tdSql.error(f"select interp(*) from ct5931 where ts=now")

        tdSql.execute(
            f"create stable sta (ts timestamp, f1 double, f2 binary(200)) tags(t1 int);"
        )
        tdSql.execute(f"create table tba1 using sta tags(1);")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:01', -3.0, \"a\");")
        tdSql.execute(f"insert into tba1 values ('2022-04-26 15:15:05', 3.0, \"b\");")
        tdSql.query(
            f"select a from (select interp(f1) as a from tba1 where ts >= '2022-04-26 15:15:01' and ts <= '2022-04-26 15:15:05' range('2022-04-26 15:15:01','2022-04-26 15:15:05') every(1s) fill(linear)) where a > 0;"
        )
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 1.500000000)
        tdSql.checkData(1, 0, 3.000000000)

        tdSql.query(
            f"select a from (select interp(f1+1) as a from tba1 where ts >= '2022-04-26 15:15:01' and ts <= '2022-04-26 15:15:05' range('2022-04-26 15:15:01','2022-04-26 15:15:05') every(1s) fill(linear)) where a > 0;"
        )
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1.000000000)
        tdSql.checkData(1, 0, 2.500000000)
        tdSql.checkData(2, 0, 4.000000000)

        print("do_interp_basic ....................... [passed]\n")

    def interp_test(self):
        dbPrefix = "intp_db"
        tbPrefix = "intp_tb"
        stbPrefix = "intp_stb"
        tbNum = 4
        rowNum = 10000
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== intp_test.sim")
        i = 0
        db = dbPrefix + str(i)
        stb = stbPrefix + str(i)
        tsu = rowNum * delta
        tsu = tsu - delta
        tsu = tsu + ts0

        tdLog.info(f"====== use db")
        tdSql.execute(f"use {db}")

        ##### select interp from table
        tdLog.info(f"====== select interp from table")
        tb = tbPrefix + str(0)
        ## interp(*) from tb
        tdSql.error(f"select interp(*) from {tb} where ts = {ts0}")

        ## interp + limit offset
        tdSql.error(f"select interp(*) from {tb} where ts = {ts0} limit 5 offset 1")

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0}"
        )

        ## intp + aggregation functions
        t = ts0 + delta
        t = t + delta
        tdSql.error(
            f"select interp(ts), max(c1), min(c2), count(c3), sum(c4), avg(c5), stddev(c6), first(c7), last(c8), interp(c9) from {tb} where ts = {t}"
        )
        tdSql.error(f"select interp(ts) from {tb} where ts={ts0} interval(1s)")

        ### illegal queries on a table
        tdSql.error(f"select interp(ts), c1 from {tb} where ts = {ts0}")
        tdSql.error(f"select interp(ts) from {tb} where ts >= {ts0}")
        tdSql.error(
            f"select interp(ts), max(c1), min(c2), count(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(NULL)"
        )

        ### interp from tb + fill
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t}"
        )

        ## fill(none)
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        ## fill(NULL)
        t = tsu - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, NULL) order by ts asc"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(none)"
        )

        ## fill(prev)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(prev)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {t} fill(prev)"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(prev)"
        )

        ## fill(linear)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        # columns contain NULL values
        t = ts0 + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {t} fill(linear)"
        )

        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(linear)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(linear)"
        )

        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(linear)"
        )
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(linear)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        t = tsu + 1000
        tdLog.info(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(linear)"
        )

        ## fill(value)
        t = ts0 + 1000
        tdLog.info(f"91")
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {ts0} fill(value, -1, -2, -3)"
        )

        # table has NULL columns
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from intp_tb3 where ts = {ts0} fill(value, -1, -2, -3)"
        )

        t = ts0 - 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        t = tsu + 1000
        tdSql.error(
            f"select interp(ts), interp(c1), interp(c2), interp(c3), interp(c4), interp(c5), interp(c6), interp(c7), interp(c8), interp(c9) from {tb} where ts = {t} fill(value, -1, -2)"
        )

        ### select interp from stable
        ## interp(*) from stb
        tdLog.info(f"select interp(*) from {stb} where ts = {ts0}")
        tdSql.error(f"select interp(*) from {stb} where ts = {ts0}")

        tdSql.error(f"select interp(*) from {stb} where ts = {t}")

        ## interp(*) from stb + group by
        tdSql.error(
            f"select interp(ts, c1, c2, c3, c4, c5, c7, c9) from {stb} where ts = {ts0} group by tbname order by tbname asc"
        )
        tdLog.info(
            f"====== select interp(ts, c1, c2, c3, c4, c5, c7, c9) from {stb} where ts = {ts0} group by tbname order by tbname asc"
        )

        ## interp(*) from stb + group by + limit offset
        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} group by tbname limit 0"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} group by tbname limit 0 offset 1"
        )

        ## interp(*) from stb + group by + fill(none)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(none) group by tbname"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} fill(none) group by tbname"
        )

        ## interp(*) from stb + group by + fill(none)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(NULL) group by tbname"
        )

        tdSql.error(
            f"select interp(*) from {stb} where ts = {ts0} fill(NULL) group by tbname"
        )
        tdLog.info(f"{tdSql.getRows()})")

        ## interp(*) from stb + group by + fill(prev)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(prev) group by tbname"
        )

        ## interp(*) from stb + group by + fill(linear)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(linear) group by tbname"
        )

        ## interp(*) from stb + group by + fill(value)
        t = ts0 + 1000
        tdSql.error(
            f"select interp(*) from {stb} where ts = {t} fill(value, -1, -2) group by tbname"
        )

        tdSql.error(
            f"select interp(ts,c1) from intp_tb0 where ts>'2018-11-25 19:19:00' and ts<'2018-11-25 19:19:12';"
        )
        tdSql.error(
            f"select interp(ts,c1) from intp_tb0 where ts>'2018-11-25 19:19:00' and ts<'2018-11-25 19:19:12' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(c1) from intp_tb0 where ts>'2018-11-25 18:09:00' and ts<'2018-11-25 19:20:12' every(18m);"
        )

        tdSql.error(
            f"select interp(c1,c3,c4,ts) from intp_tb0 where ts>'2018-11-25 18:09:00' and ts<'2018-11-25 19:20:12' every(18m) fill(linear)"
        )

        tdSql.error(
            f"select interp(c1) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:42:00.000' every(1m) fill(linear);"
        )

        tdSql.error(
            f"select interp(c1) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:42:00.000' every(1m) fill(linear) order by ts desc;"
        )

        tdSql.error(
            f" select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(2m) fill(linear) order by ts;"
        )

        tdSql.error(
            f"select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(3m) fill(linear) order by ts;"
        )

        tdSql.error(
            f"select interp(c3) from intp_stb0 where ts >= '2018-09-17 20:35:00.000' and ts <= '2018-09-17 20:50:00.000' every(3m) fill(linear) order by ts desc;"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(value, 1);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(NULL);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:56' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:56' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:19:57' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:03' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:05' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:20:02' and ts<='2021-07-25 02:20:05' every(1s) fill(value, 1);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:20:02' and ts<='2021-07-25 02:20:05' every(1s) fill(null);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:20:25' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(linear);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 02:25:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 03:25:00' every(1s) fill(prev);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<='2021-07-25 03:25:00' every(1s) fill(next);"
        )

        tdSql.error(
            f"select interp(pav) from ap1 where ts> '2021-07-25 02:19:54' and ts<'2021-07-25 02:20:07' every(1s);"
        )

    #
    # ------------------ main ------------------
    #
    def test_func_ts_interp(self):
        """ Fun: interp()

        1. Basic query for different params
        2. Query on super/child/normal/empty table
        3. Support data types
        4. Error cases
        5. Query with where condition
        6. Query with partition/group/order by
        7. Query with sub query
        8. Query with union/join/fill/every/range/interval
        9. Select _irowts, _irowts_origin, _isfilled
        10. Check null value
        11. Single INTERP query covering multiple columns

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/interp.sim
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_interp.py
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_interp_extension.py
            - 2025-9-29 Alex Duan Migrated from 20-DataQuerying/12-Interp/test_interp_basic.py

        """
        self.do_interp()
        self.do_interp_extension()
        self.do_interp_basic()
