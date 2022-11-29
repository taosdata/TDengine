import taos
import sys
import datetime
import inspect
import math
from util.log import *
from util.sql import *
from util.cases import *


class TDTestCase:
    # updatecfgDict = {'debugFlag': 143 ,"cDebugFlag":143,"uDebugFlag":143 ,"rpcDebugFlag":143 , "tmrDebugFlag":143 ,
    # "jniDebugFlag":143 ,"simDebugFlag":143,"dDebugFlag":143, "dDebugFlag":143,"vDebugFlag":143,"mDebugFlag":143,"qDebugFlag":143,
    # "wDebugFlag":143,"sDebugFlag":143,"tsdbDebugFlag":143,"tqDebugFlag":143 ,"fsDebugFlag":143 ,"udfDebugFlag":143}
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def prepare_datas(self, dbname="db"):

        tdSql.execute(
            f''' CREATE TABLE ac_stb (TS TIMESTAMP, C1 INT, C2 BIGINT, C3 FLOAT, C4 DOUBLE, C5 BINARY(10), C6 BOOL, 
            C7 SMALLINT, C8 TINYINT, C9 NCHAR(10)) TAGS (T1 INT);
            '''            
        )
        
        tdSql.execute(
            f''' insert into ctb0 using ac_stb tags (1) values ( 1537146001000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146002000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f'''  insert into ntb0 using ac_stb tags (-1) values ( 1537146001000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146002000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f'''  insert into ntb0 using ac_stb tags (2) values ( 1537146003000 , 1,1,1,1,'bin',1,1,1,'________') 
            ( 1537146004000 , 2,2,2,2,'binar', 1,1,1,'nchar');
            '''       
        ) 

        tdSql.execute(
            f''' insert into ctb6 using ac_stb tags(1) values ( 1537146000000 , 1, 1, 1, 1, 'bin1', 1, 1, 1, '________1') 
            ctb6 using ac_stb tags(2) values ( 1537146000000 , 2, 2, 2, 2, 'bin2', 2, 2, 2, '________2') 
            ctb6 using ac_stb tags(3) values ( 1537146000000 , 3, 3, 3, 3, 'bin3', 3, 3, 3, '________3')
            '''       
        )
    

    def check_result(self, dbname="db"):
        tdSql.query("select c1,c1,c2,c3,c4,c5,c7,c8,c9 from ac_stb")
        tdSql.checkRows(7)

        tdSql.query("select t1, count(*), first(c9) from ac_stb partition by t1 order by t1 asc slimit 3")
        tdSql.checkRows(2)

        # TD-20582
        tdSql.query("explain analyze verbose true select count(*) from ac_stb where T1=1")
        tdSql.checkRows(16)

        # TD-20581
        tdSql.execute("insert into ntb0 select * from ntb0")
        tdSql.query("select * from ntb0")
        tdSql.checkRows(4)

        return
        # basic query
        

    def run(self):  # sourcery skip: extract-duplicate-method, remove-redundant-fstring
        tdSql.prepare()

        tdLog.printNoPrefix("========== step1: create table ==============")

        self.prepare_datas()

        tdLog.printNoPrefix("========== step2: check results ==============")

        self.check_result()

        


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
