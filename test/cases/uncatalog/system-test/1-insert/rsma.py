from datetime import datetime
import time

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

from util.cluster import *
sys.path.append("./6-cluster")
from clusterCommonCreate import *
from clusterCommonCheck import clusterComCheck


PRIMARY_COL = "ts"

INT_COL = "c_int"
BINT_COL = "c_bint"
SINT_COL = "c_sint"
TINT_COL = "c_tint"
FLOAT_COL = "c_float"
DOUBLE_COL = "c_double"
BOOL_COL = "c_bool"
TINT_UN_COL = "c_utint"
SINT_UN_COL = "c_usint"
BINT_UN_COL = "c_ubint"
INT_UN_COL = "c_uint"
BINARY_COL = "c_binary"
NCHAR_COL = "c_nchar"
TS_COL = "c_ts"

INT_TAG = "t_int"

TAG_COL = [INT_TAG]

## insert data args：
TIME_STEP = 10000
NOW = int(datetime.timestamp(datetime.now()) * 1000)

# init db/table
DBNAME  = "db"
DB1     = "db1"
DB2     = "db2"
DB3     = "db3"
DB4     = "db4"
STBNAME = "stb1"
CTBNAME = "ct1"
NTBNAME = "nt1"

class TDTestCase:

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor(), True)

    def create_ctable(self,tsql=None, dbName='dbx',stbName='stb',ctbPrefix='ctb',ctbNum=1):
        tsql.execute("use %s" %dbName)
        pre_create = "create table"
        sql = pre_create
        #tdLog.debug("doing create one  stable %s and %d  child table in %s  ..." %(stbname, count ,dbname))
        for i in range(ctbNum):
            tagValue = 'beijing'
            if (i % 2 == 0):
                tagValue = 'shanghai'
            sql += " %s%d using %s tags(%d, '%s')"%(ctbPrefix,i,stbName,i+1, tagValue)
            if (i > 0) and (i%100 == 0):
                tsql.execute(sql)
                sql = pre_create
        if sql != pre_create:
            tsql.execute(sql)

        tdLog.debug("complete to create %d child tables in %s.%s" %(ctbNum, dbName, stbName))
        return

    def getPath(self, tool="taosBenchmark"):
        if (platform.system().lower() == 'windows'):
            tool = tool + ".exe"
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        paths = []
        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    paths.append(os.path.join(root, tool))
                    break
        if (len(paths) == 0):
            tdLog.exit("taosBenchmark not found!")
            return
        else:
            tdLog.info("taosBenchmark found in %s" % paths[0])
            return paths[0]    
        

    def run(self):
        binPath = self.getPath()

        self.rows = 10
        tdLog.printNoPrefix("==========step0:all check")
        dbname='d0'
        tdSql.execute(f"create database {dbname} retentions -:363d,1m:364d,1h:365d  STT_TRIGGER 2  vgroups 6;")
        tdSql.execute(f"create stable if not exists {dbname}.st_min (ts timestamp, c1 int) tags (proid int,city binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;;")
        tdSql.execute(f"create stable if not exists {dbname}.st_avg (ts timestamp, c1 double) tags (city binary(20),district binary(20)) rollup(min) watermark 0s,1s max_delay 1m,180s;;")
        self.create_ctable(tdSql, dbname, 'st_min', 'ct_min', 1000)
        tdLog.printNoPrefix("==========step4:after wal, all check again ")
        datetime1 = datetime.now()
        print(datetime1)
        tdSql.execute(f"INSERT INTO  {dbname}.ct_min7 VALUES ('2023-11-07 10:01:00.001',797029643) ('2023-11-07 10:01:00.001',797029643);")
        tdSql.query(f"select * from {dbname}.st_min where ts>now-363d;")
        tdSql.checkData(0, 0, '2023-11-07 10:01:00.001')
        sleep(6)
        tdSql.query(f"select * from {dbname}.st_min where ts>now-364d;")
        tdSql.checkData(0, 0, '2023-11-07 10:01:00.000')
        tdSql.query(f"select * from {dbname}.st_min where ts>now-365d;")
        tdSql.checkData(0, 0, '2023-11-07 10:00:00.000')

        #bug
        os.system(f"{binPath} -f ./1-insert/benchmark-tbl-rsma-alter.json")
        tdSql.execute(f"alter database db_replica replica 3;")
        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=3,db_name="db_replica",count_number=240)        
        tdSql.execute(f"alter database db_replica replica 1;")
        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=1,db_name="db_replica",count_number=240)     
        tdSql.execute(f"alter database db_replica replica 3;")
        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=3,db_name="db_replica",count_number=240)        
        tdSql.execute(f"alter database db_replica replica 1;")
        clusterComCheck.check_vgroups_status(vgroup_numbers=2,db_replica=1,db_name="db_replica",count_number=240)   

        #bug
        # os.system(f"{binPath} -f ./1-insert/benchmark-tbl-rsma.json")
        # tdSql.query(f" select count(*) from db_update.stb1;")
        # tdSql.checkData(0, 0, 360000)


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
