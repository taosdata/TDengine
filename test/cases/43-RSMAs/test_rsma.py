# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, tdDnodes
from new_test_framework.utils.sqlset import TDSetSql
import os
import time
import shutil

class TestCase:
    """ Test case for rsma.

    1. Exceptional cases.
    2. Retention task monitor.
    3. Rollup automatically when execute: trim database <db_name>.
    4. Rollup manually when execute: rollup database <db_name>.

    Catalog:
        - Rollup SMA:Create/Drop/Show/Query/Trim/Rollup

    Since: v3.3.8.0

    Lables: common,ci,rsma

    Jira: TS-6113

    History:
        - 2025-09-25: Initial version from Kaili Xu.
    """

    path_parts = os.getcwd().split(os.sep)
    try:
        tdinternal_index = path_parts.index("TDinternal")
    except ValueError:
        raise ValueError("The specified directory 'TDinternal' was not found in the path.")
    TDinternal = os.sep.join(path_parts[:tdinternal_index + 1])
    dnode1Path = os.path.join(TDinternal, "sim", "dnode1")
    configFile = os.path.join(dnode1Path, "cfg", "taos.cfg")
    hostPath = os.path.join(dnode1Path, "multi")
    hostPrimary = os.path.join(hostPath, "taos01")
    mountPath = os.path.join(dnode1Path, "mnt")
    mountPrimary = os.path.join(mountPath, "taos01")
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict = {
        "debugFlag"        : 135,
        "forceReadConfig"  : 1,
        "dataDir"          : [  f"%s%staos00 0 0" % (hostPath, os.sep),
                                f"%s%staos01 0 1" % (hostPath, os.sep),
                                f"%s%staos02 0 0" % (hostPath, os.sep),
                                f"%s%staos10 1 0" % (hostPath, os.sep),
                                f"%s%staos11 1 0" % (hostPath, os.sep),
                                f"%s%staos12 1 0" % (hostPath, os.sep)],
        'clientCfg'        : clientCfgDict
    }

    def setup_cls(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

    def s0_reset_test_env(self):
        tdLog.info("reset test environment")
        self.s1_create_db_table()
        self.s2_create_rsma()

    def s1_create_db_table(self):
        tdSql.execute("drop database if exists d0")
        tdSql.execute("create database if not exists d0 replica 1 keep 36500d")
        tdSql.execute("drop database if exists d1")
        tdSql.execute("create database if not exists d1 replica 1 keep 36500d")
        tdSql.execute("use d0")
        tdSql.execute("create stable if not exists stb0 (ts timestamp, c0 int, c1 bigint, c2 float, c3 double, c4 bool, c5 varchar(10), c6 nchar(10)) tags(t0 int)")
        tdSql.execute("create stable if not exists stb1 (ts timestamp, c00 varchar(10), c0 int, c1 bigint, c2 float, c3 double, c4 bool, c5 varchar(10), c6 nchar(10)) tags(t0 int)")
        tdSql.execute("create table if not exists ntb0 (ts timestamp, c0 int, c1 bigint, c2 float, c3 double, c4 bool, c5 varchar(10), c6 nchar(10))")
        tdSql.execute("create table if not exists ctb0 using stb0 tags(0)")
        tdSql.execute("create table if not exists ctb1 using stb0 tags(1)")
        tdSql.execute("create table if not exists ctb11 using stb1 tags(11)")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:00:01.001',1,1,1,1,true, '1','1')")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:00:02.002',2,2,2,2,false, '2','2')")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:00:03.003',3,3,3,3,true, '3','3')")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:01:00.001',4,5,4,7,false, '44','4')")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:01:00.002',4,4,6,4,false, '4','444')")
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:02:00.002',5,5,5,5,true, '5','5')")
        tdSql.execute("insert  into ctb1 values('2024-10-01 08:00:01.001',111,11,1,1,true, '91','1')")
        tdSql.execute("insert  into ctb1 values('2024-10-01 08:00:02.002',22,2,2,2,false, '23','2')")
        tdSql.execute("insert  into ctb1 values('2024-10-01 08:00:03.003',33,333,3,3,true, '3','33')")
        tdSql.execute("insert  into ctb1 values('2024-10-01 08:01:00.001',14,4,4,4,false, '40','4')")
        tdSql.execute("insert  into ctb1 values('2024-10-01 08:02:00.002',5,555,5,5,true, '5','5')")
        tdSql.execute("insert  into ctb11 values('2024-10-01 08:00:01.001','111',111,11,1,1,true, '91','1')")
        tdSql.execute("insert  into ctb11 values('2024-10-01 08:00:02.002','22',22,2,2,2,false, '23','2')")
        tdSql.execute("insert  into ctb11 values('2024-10-01 08:00:03.003','33',33,333,3,3,true, '3','33')")
        tdSql.execute("insert  into ctb11 values('2024-10-01 08:01:00.001','14',14,4,4,4,false, '40','4')")
        tdSql.execute("insert  into ctb11 values('2024-10-01 08:02:00.002','55',5,555,5,5,true, '5','5')")
        tdSql.execute("flush database d0")
        tdSql.execute("select * from stb0")

    def s2_create_rsma(self):
        tdSql.execute("create rsma rsma1 on d0.stb0 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c5)) interval(1m,5m)")

        tdSql.error("create rsma rsma2 on d0.ntb0 interval(1m)", expectErrInfo=f"Rsma must be created on super table", fullMatched=False)
        tdSql.error("create rsma rsma3 on d0.ctb0 interval(1m)", expectErrInfo=f"Rsma must be created on super table", fullMatched=False)
        tdSql.error("create rsma rsma4 on information_schema.ins_users interval(1m)", expectErrInfo=f"Cannot create rsma on system table: `information_schema`.`ins_users`", fullMatched=False)
        tdSql.error("create rsma rsma6 on d0.stb0 interval(1m)", expectErrInfo=f"Rsma already exist in the table", fullMatched=False)

        tdSql.execute("create rsma rsma7 on d0.stb1 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c5),first(c6)) interval(1m,5m)")

        tdSql.error("create rsma rsma8 on d0.stb0 function(min(c0+1), max(c1), avg(c2), sum(c3),first(c4),last(c5)) interval(1m,5m)", expectErrInfo=f"Invalid func param for rsma, only one non-primary key column allowed: min", fullMatched=False)
        tdSql.error("create rsma rsma9 on d0.stb0 function(min(c100), max(c1), avg(c2), sum(c3),first(c4),last(c5)) interval(1m,5m)", expectErrInfo=f"Invalid func param for rsma since column not exist: min(c100)", fullMatched=False)
        tdSql.error("create rsma rsma10 on d0.stb0 function(count(c100), max(c1), avg(c2), sum(c3),first(c4),last(c5)) interval(1m,5m)", expectErrInfo=f"Invalid func for rsma: count", fullMatched=False)
        tdSql.error("create rsma rsma11 on d0.stb0 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c0)) interval(1m,5m)", expectErrInfo=f"Duplicated column not allowed for rsma: c0", fullMatched=False)
        tdSql.error("create rsma rsma12 on d0.stb0 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c0)) interval(1m,1m)", expectErrInfo=f"Second interval value for rsma should be greater than first interval: 60000,60000", fullMatched=False)
        tdSql.error("create rsma rsma13 on d0.stb0 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c0)) interval(1m,1a)", expectErrInfo=f"Second interval value for rsma should be greater than first interval: 60000,1", fullMatched=False)
        tdSql.error("create rsma rsma14 on d0.stb0 function(min(c0), max(c1), avg(c2), sum(c3),first(c4),last(c0)) interval(2m,3m)", expectErrInfo=f"Second interval value for rsma should be a multiple of first interval: 120000,180000", fullMatched=False)

    def s3_show_rsma(self):
        tdSql.query("show rsmas")
        tdSql.checkRows(2)
        tdSql.query("show d0.rsmas")
        tdSql.checkRows(2)
        tdSql.query("show d1.rsmas")
        tdSql.checkRows(0)
        tdSql.query("select * from information_schema.ins_rsmas")
        tdSql.checkRows(2)
        tdSql.query("select * from information_schema.ins_rsmas where db_name='d0'")
        tdSql.checkRows(2)

    def s4_drop_rsma(self):
        tdSql.query("show rsmas")
        tdSql.checkRows(2)
        tdSql.execute("drop rsma rsma1")
        tdSql.query("show rsmas")
        tdSql.checkRows(1)
        tdSql.execute("drop stable stb1")
        tdSql.query("show rsmas")
        tdSql.checkRows(0)
        self.s0_reset_test_env()
        tdSql.query("show rsmas")
        tdSql.checkRows(2)
        tdSql.execute("drop database d0")
        tdSql.query("show rsmas")
        tdSql.checkRows(0)

    def s5_0_check_rollup_result(self):
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(6)
        tdSql.query("select * from d0.ctb0 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 6)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 3)
        tdSql.checkData(0, 7, 3)
        tdSql.checkData(1, 0, '2024-10-01 08:01:00.000')
        tdSql.checkData(1, 1, 4)
        tdSql.checkData(1, 2, 5)
        tdSql.checkData(1, 3, 5)
        tdSql.checkData(1, 4, 11)
        tdSql.checkData(1, 5, False)
        tdSql.checkData(1, 6, 4)
        tdSql.checkData(1, 7, 444)
        tdSql.checkData(2, 0, '2024-10-01 08:02:00.000')
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 3, 5)
        tdSql.checkData(2, 4, 5)
        tdSql.checkData(2, 5, True)
        tdSql.checkData(2, 6, 5)
        tdSql.checkData(2, 7, 5)
        tdSql.query("select * from d0.ctb1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 22)
        tdSql.checkData(0, 2, 333)
        tdSql.checkData(0, 3, 2)
        tdSql.checkData(0, 4, 6)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 3)
        tdSql.checkData(0, 7, 33)
        tdSql.checkData(1, 0, '2024-10-01 08:01:00.000')
        tdSql.checkData(1, 1, 14)
        tdSql.checkData(1, 2, 4)
        tdSql.checkData(1, 3, 4)
        tdSql.checkData(1, 4, 4)
        tdSql.checkData(1, 5, False)
        tdSql.checkData(1, 6, 40)
        tdSql.checkData(1, 7, 4)
        tdSql.checkData(2, 0, '2024-10-01 08:02:00.000')
        tdSql.checkData(2, 1, 5)
        tdSql.checkData(2, 2, 555)
        tdSql.checkData(2, 3, 5)
        tdSql.checkData(2, 4, 5)
        tdSql.checkData(2, 5, True)
        tdSql.checkData(2, 6, 5)
        tdSql.checkData(2, 7, 5)
        tdSql.query("select * from d0.stb1 order by ts")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 33)
        tdSql.checkData(0, 2, 22)
        tdSql.checkData(0, 3, 333)
        tdSql.checkData(0, 4, 2)
        tdSql.checkData(0, 5, 6)
        tdSql.checkData(0, 6, True)
        tdSql.checkData(0, 7, 3)
        tdSql.checkData(0, 8, 1)
        tdSql.checkData(1, 0, '2024-10-01 08:01:00.000')
        tdSql.checkData(1, 1, 14)
        tdSql.checkData(1, 2, 14)
        tdSql.checkData(1, 3, 4)
        tdSql.checkData(1, 4, 4)
        tdSql.checkData(1, 5, 4)
        tdSql.checkData(1, 6, False)
        tdSql.checkData(1, 7, 40)
        tdSql.checkData(1, 8, 4)
        tdSql.checkData(2, 0, '2024-10-01 08:02:00.000')
        tdSql.checkData(2, 1, 55)
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(2, 3, 555)
        tdSql.checkData(2, 4, 5)
        tdSql.checkData(2, 5, 5)
        tdSql.checkData(2, 6, True)
        tdSql.checkData(2, 7, 5)
        tdSql.checkData(2, 8, 5)

    def s5_0_wait_trim_done(self):
        i = 0
        while True:
            tdSql.query("show retentions")
            if tdSql.getRows() == 0:
                break
            time.sleep(1)
            i += 1
            tdLog.info(f"wait for trim/rollup done, {i} second(s) elapsed")

    def s5_0_trim_db(self, tsdbOpType = 'trim'):
        self.s0_reset_test_env()
        tdSql.query("show rsmas")
        tdSql.checkRows(2)
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(11)
        tdSql.query("select * from d0.stb1")
        tdSql.checkRows(5)
        tdSql.execute("alter database d0 keep 30d,36500d")
        tdSql.execute("flush database d0")
        tdSql.execute(f"{tsdbOpType} database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        tdSql.query(f"show retention {tdSql.queryResult[0][0]}")
        tdSql.error("trim database d0", expectErrInfo=f"Trim or rollup already exist", fullMatched=False)
        tdSql.error("rollup database d0", expectErrInfo=f"Trim or rollup already exist", fullMatched=False)
        self.s5_0_wait_trim_done()
        self.s5_0_check_rollup_result()

    def s5_trim_db(self):
        tdLog.info("trim database")
        self.s5_0_trim_db('trim')
        # insert more data after trim
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:00:01.001',1,1,1,1,true, '1','1')")
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(7)
        tdSql.execute("flush database d0")
        # trim again
        time.sleep(5) # ensure commit is done
        tdSql.execute("trim database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        self.s5_0_wait_trim_done()
        # trim has no effect since no fset retention happen 
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(7)
        # 2nd level rollup has effect since expLevel changed
        tdLog.info("2nd level rollup by trim")
        tdSql.execute("alter database d0 keep 30d,60d,36500d")
        tdSql.execute("trim database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        self.s5_0_wait_trim_done()
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(2)
        tdSql.query("select * from d0.stb1")
        tdSql.checkRows(1)
        tdSql.execute("alter database d0 keep 30d,60d,36500d")
        tdSql.execute("rollup database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        self.s5_0_wait_trim_done()
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(2)
        tdSql.query("select * from d0.stb1")
        tdSql.checkRows(1)
        tdSql.query("select * from d0.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 3.25)
        tdSql.checkData(0, 4, 23)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 5)
        tdSql.query("select * from d0.ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 555)
        tdSql.checkData(0, 3, 3.66667)
        tdSql.checkData(0, 4, 15)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 5)
        tdSql.query("select * from d0.ctb11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 55)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 555)
        tdSql.checkData(0, 4, 3.66667)
        tdSql.checkData(0, 5, 15)
        tdSql.checkData(0, 6, True)
        tdSql.checkData(0, 7, 5)
        tdSql.checkData(0, 8, 1)


    def s6_rollup_db(self):
        tdLog.info("rollup database")
        self.s5_0_trim_db('rollup')
        # insert more data after rollup
        tdSql.execute("insert  into ctb0 values('2024-10-01 08:00:01.001',1,1,1,1,true, '1','1')")
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(7)
        tdSql.execute("flush database d0")
        # rollup again
        time.sleep(5) # ensure commit is done
        tdSql.execute("rollup database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        self.s5_0_wait_trim_done()
        # rollup has effect since new commit happen after last rollup
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(6)
        tdSql.query("select * from d0.ctb0")
        tdSql.checkRows(3)
        # check rollup result
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 3)
        tdSql.checkData(0, 3, 1.5)
        tdSql.checkData(0, 4, 7)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 1)
        tdSql.checkData(0, 7, 1)
        # 2nd level rollup has effect since expLevel changed
        tdLog.info("2nd level rollup")
        tdSql.execute("alter database d0 keep 30d,60d,36500d")
        tdSql.execute("rollup database d0")
        tdSql.query("show retentions")
        tdSql.checkRows(1)
        self.s5_0_wait_trim_done()
        tdSql.query("select * from d0.stb0")
        tdSql.checkRows(2)
        tdSql.query("select * from d0.stb1")
        tdSql.checkRows(1)
        tdSql.query("select * from d0.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 3.83333)
        tdSql.checkData(0, 4, 23)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 5)
        tdSql.query("select * from d0.ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(0, 2, 555)
        tdSql.checkData(0, 3, 3.66667)
        tdSql.checkData(0, 4, 15)
        tdSql.checkData(0, 5, True)
        tdSql.checkData(0, 6, 5)
        tdSql.checkData(0, 7, 5)
        tdSql.query("select * from d0.ctb11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '2024-10-01 08:00:00.000')
        tdSql.checkData(0, 1, 55)
        tdSql.checkData(0, 2, 5)
        tdSql.checkData(0, 3, 555)
        tdSql.checkData(0, 4, 3.66667)
        tdSql.checkData(0, 5, 15)
        tdSql.checkData(0, 6, True)
        tdSql.checkData(0, 7, 5)
        tdSql.checkData(0, 8, 1)

    def test_rsma(self):
        """ Test case for rsma.

        1. Exceptional cases.
        2. Retention task monitor.
        3. Rollup automatically when execute: trim database <db_name>.
        4. Rollup manually when execute: rollup database <db_name>.

        Catalog:
            - Rollup SMA:Create/Drop/Show/Query/Trim/Rollup

        Since: v3.3.8.0

        Lables: common,ci,rsma

        Jira: TS-6113

        History:
            - 2025-09-25: Initial version from Kaili Xu.
        """
        self.s1_create_db_table()
        self.s2_create_rsma()
        self.s3_show_rsma()
        self.s4_drop_rsma()
        self.s5_trim_db()
        self.s6_rollup_db()

        tdLog.success("%s successfully executed" % __file__)
