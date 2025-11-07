# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, tdDnodes
from new_test_framework.utils.sqlset import TDSetSql
import os
import time
import shutil

class TestMountBasic:
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

    def s0_prepare_mount_path(self):
        tdSql.execute("drop database if exists db0")
        tdSql.execute("create database if not exists db0 replica 1 stt_trigger 1")
        tdSql.execute("use db0")
        tdSql.execute("create table stb0(ts timestamp, c0 int, c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("create table ctb1 using stb0 tags(1)")
        tdSql.execute("create table stb1(ts timestamp, c0 int, c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb10 using stb1 tags(0)")
        tdSql.execute("create table ctb11 using stb1 tags(1)")
        tdSql.execute("create table ntb0(ts timestamp, c0 int)")
        tdSql.execute("create table ntb1(ts timestamp, c0 int)")
        tdSql.execute("create vtable db0.vntb0(ts timestamp, v0_0 int from db0.ntb0.c0, v0_1 int from db0.ntb1.c0)")
        tdSql.execute("create stable db0.vstb0(ts timestamp, c0 int, c1 int) tags(t0 int, t1 varchar(20)) virtual 1")
        tdSql.execute("create stable db0.vstb1(ts timestamp, c0 int, c1 int) tags(t0 int, t1 varchar(20)) virtual 1")
        tdSql.execute("create vtable db0.vctb0(c0 from db0.ctb0.c0, c1 from db0.ctb0.c2) using db0.vstb0 tags(0, '0')")
        tdSql.execute("create vtable db0.vctb1(c0 from db0.ctb0.c0, c1 from db0.ctb1.c0) using db0.vstb1 tags(0, '1')")
        for r in range(0, 5000, 50):
            tdSql.query(f"insert into db0.ctb0 values(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)  "%(r, r*2, r*4, r*3, float(r)/39, float(r)/23,r+1, (r+1)*2, (r+1)*4, (r+1)*3, float(r)/139, float(r)/123,r+2, (r+2)*2, (r+2)*4, (r+2)*3, float(r)/239, float(r)/223,r+3, (r+3)*2, (r+3)*4, (r+3)*3, float(r)/339, float(r)/323,r+4, (r+4)*2, (r+4)*4, (r+4)*3, float(r)/439, float(r)/423,r+5, r+5*2, r+5*4, r+5*3, float(r)/539, float(r)/523,r+6, r+6*2, r+6*4, r+6*3, float(r)/639, float(r)/623,r+7, r+7*2, r+7*4, r+7*3, float(r)/739, float(r)/723,r+8, r+8*2, r+8*4, r+8*3, float(r)/839, float(r)/823,r+9, r+9*2, r+9*4, r*3, float(r)/939, float(r)/923))
        for r in range(0, 5000, 50):
            tdSql.query(f"insert into db0.ctb1 values(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)(now + %ds, %d, %d, %d, %f, %f)  "%(r, r*2, r*4, r*3, float(r)/39, float(r)/23,r+1, (r+1)*2, (r+1)*4, (r+1)*3, float(r)/139, float(r)/123,r+2, (r+2)*2, (r+2)*4, (r+2)*3, float(r)/239, float(r)/223,r+3, (r+3)*2, (r+3)*4, (r+3)*3, float(r)/339, float(r)/323,r+4, (r+4)*2, (r+4)*4, (r+4)*3, float(r)/439, float(r)/423,r+5, r+5*2, r+5*4, r+5*3, float(r)/539, float(r)/523,r+6, r+6*2, r+6*4, r+6*3, float(r)/639, float(r)/623,r+7, r+7*2, r+7*4, r+7*3, float(r)/739, float(r)/723,r+8, r+8*2, r+8*4, r+8*3, float(r)/839, float(r)/823,r+9, r+9*2, r+9*4, r*3, float(r)/939, float(r)/923))
        tdSql.query("select last(ts) from db0.ctb0")
        tdSql.checkRows(1)
        tdLog.info("last ts in db0.ctb0: %s" % tdSql.queryResult[0][0])
        tdSql.execute(f"delete from db0.ctb0 where ts >= '{tdSql.queryResult[0][0]}'")
        tdSql.query("insert into db0.ntb0 values(now, 1)(now + 1s, 2)(now + 2s, 3)(now + 3s, 4)(now + 4s, 5)")
        tdSql.execute("flush database db0")
        tdSql.execute("create database if not exists db1 replica 1 stt_trigger 2")
        tdSql.execute("use db1")
        tdSql.execute("create table stb0(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("create table stb1(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb10 using stb1 tags(0)")
        tdSql.execute(f"insert into db1.ctb0 values(now, 1, 1, 1, 1.0, 1.0)(now + 1s, 2, 2, 2, 2.0, 2.0)(now + 2s, 3, 3, 3, 3.0, 3.0)(now + 3s, 4, 4, 4, 4.0, 4.0)(now + 4s, 5, 5, 5, 5.0, 5.0)")
        tdSql.query("select last(ts) from db1.ctb0")
        tdLog.info("last ts in db1.ctb0: %s" % tdSql.queryResult[0][0])
        tdSql.execute(f"delete from db1.stb0 where ts >= '{tdSql.queryResult[0][0]}'")
        tdSql.execute("flush database db1")
        tdDnodes.stop(1)
        time.sleep(1)
        try:
            if(os.path.exists(self.mountPath)):
                shutil.rmtree(self.mountPath)
            shutil.move(self.hostPath, self.mountPath)
        except Exception as e:
            raise Exception(repr(e))

    def s1_prepare_host_cluster(self):
        tdDnodes.start(1)
        tdSql.query("select * from information_schema.ins_dnodes;")
        tdSql.checkRows(1)
        tdSql.checkData(0,4,'ready')
        tdSql.execute("create database if not exists d0 replica 1")
        tdSql.execute(f"create user u1 PASS 'taosdata'")
        tdSql.query(f"select * from information_schema.ins_users")
        tdSql.checkRows(2)

    def replace_string_in_file(self, filename, origin, dest):
        with open(filename, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        modified = False
        new_lines = []
        for line in lines:
            if origin in line:
                modified_line = line.replace(origin, dest)
                new_lines.append(modified_line)
                modified = True
            else:
                new_lines.append(line)
        if modified:
            with open(filename, 'w', encoding='utf-8') as file:
                file.writelines(new_lines)

    def refact_mount_dataDir(self):
        localMountConf = os.path.join(self.mountPrimary, "dnode", "config", "local.json")
        try:
            self.replace_string_in_file(localMountConf, self.hostPath, self.mountPath)
        except Exception as e:
            raise Exception(f"failed to replace string in {localMountConf}: {repr(e)}")

    def corruptMntClusterId(self):
        mntDnodeConf = os.path.join(self.mountPrimary, "dnode", "dnode.json")
        try:
            self.replace_string_in_file(mntDnodeConf, '"clusterId":\t"', '"clusterId":\t"-')
        except Exception as e:
            raise Exception(f"failed to corrupt clusterId in {mntDnodeConf}: {repr(e)}")
    def recoverMntClusterId(self):
        mntDnodeConf = os.path.join(self.mountPrimary, "dnode", "dnode.json")
        try:
            self.replace_string_in_file(mntDnodeConf, '"clusterId":\t"-', '"clusterId":\t"')
        except Exception as e:
            raise Exception(f"failed to restore clusterId in {mntDnodeConf}: {repr(e)}")
    def refactConfBetweenHostAndMnt(self, toMnt = True):
        try:
            if toMnt:
                self.replace_string_in_file(self.configFile, 'multi', 'mnt')
            else:
                self.replace_string_in_file(self.configFile, 'mnt', 'multi')
        except Exception as e:
            raise Exception(f"failed to refact conf in {self.configFile}: {repr(e)}")

    def s2_check_mount_error(self):
        tdSql.error("create mount mnt_1 on dnode 1 from ''", expectErrInfo=f"The mount name cannot contain _", fullMatched=False)
        tdSql.error("create mount mnt1 on dnode 1 from ''", expectErrInfo=f"The mount path is invalid", fullMatched=False)
        tdSql.error("create mount mnt1 on dnode 1 from 'path_not_exist'", expectErrInfo="No such file or directory", fullMatched=False)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPath}'", expectErrInfo="No such file or directory", fullMatched=False)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.hostPrimary}'", expectErrInfo="Resource temporarily unavailable", fullMatched=False)
        tdSql.error(f"create mount d0 on dnode 1 from '{self.hostPrimary}'", expectErrInfo="Database with identical name already exists", fullMatched=False)
        self.refact_mount_dataDir()
        self.corruptMntClusterId()
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'", expectErrInfo="Cluster id not match", fullMatched=False)
        self.recoverMntClusterId()
        tdSql.error(f"drop mount mnt_not_exist", expectErrInfo="Mount not exist", fullMatched=False)
        tdSql.error(f"drop mount d0", expectErrInfo="Mount not exist", fullMatched=False)

    def check_mount_query(self):
        tdSql.query("show create database `mnt1_db0`")
        tdSql.checkRows(1)
        tdSql.query("show create database `mnt1_db1`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`stb0`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`ctb0`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`ctb1`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`stb1`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`ctb10`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db0`.`ctb11`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db1`.`stb0`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db1`.`ctb0`")
        tdSql.checkRows(1)
        tdSql.query("show create table `mnt1_db1`.`ctb10`")
        tdSql.checkRows(1)
        tdSql.query("show mnt1_db0.stables")
        tdSql.checkRows(4)
        tdSql.query("show mnt1_db0.tables")
        tdSql.checkRows(6)
        tdSql.query("show mnt1_db1.stables")
        tdSql.checkRows(2)
        tdSql.query("show mnt1_db1.tables")
        tdSql.checkRows(2)
        tdSql.query("desc mnt1_db0.stb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ctb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ctb1")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.stb1")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ctb10")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ctb11")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ntb0")
        tdSql.checkRows(2)
        tdSql.query("desc mnt1_db0.vstb0")
        tdSql.checkRows(5)
        tdSql.query("desc mnt1_db0.vstb1")
        tdSql.checkRows(5)
        tdSql.query("desc mnt1_db0.vctb0")
        tdSql.checkRows(5)
        tdSql.query("desc mnt1_db0.vntb0")
        tdSql.checkRows(3)
        tdSql.query("desc mnt1_db1.stb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db1.ctb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db1.ctb10")
        tdSql.checkRows(7)
        tdSql.query("select * from mnt1_db0.stb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db0.ctb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db0.ctb1 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db0.stb1 limit 1")
        tdSql.checkRows(0)
        tdSql.query("select * from mnt1_db0.ctb10 limit 1")
        tdSql.checkRows(0)
        tdSql.query("select * from mnt1_db0.ctb11 limit 1")
        tdSql.checkRows(0)
        tdSql.query("select * from mnt1_db1.stb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db1.ctb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from mnt1_db0.stb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1999)
        tdSql.query("select count(*) from mnt1_db0.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 999)
        tdSql.query("select count(*) from mnt1_db0.ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from mnt1_db0.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from mnt1_db0.ctb10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from mnt1_db0.ctb11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from mnt1_db1.stb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)
        tdSql.query("select count(*) from mnt1_db1.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)

    def s3_create_drop_show_mount(self):
        tdLog.info(" =============== step 3 create_drop_show_mount")
        tdSql.execute(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'")
        tdSql.query("show mounts", count_expected_res=1)
        tdLog.info(f"result: {tdSql.queryResult}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "mnt1")
        tdSql.checkData(0, 1, "1")
        tdSql.checkData(0, 3, self.mountPrimary)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'", expectErrInfo="Mount already exists", fullMatched=False)
        tdLog.info("check mount query")
        self.check_mount_query()
        tdLog.info("reboot and query from mount db")
        tdSql.execute(f"GRANT read ON mnt1_db0.* to u1;")
        tdSql.execute(f"GRANT write ON mnt1_db0.* to u1;")
        tdSql.execute(f"GRANT all ON mnt1_db1.* to u1;")
        tdSql.execute(f"GRANT read ON mnt1_db0.stb0 to u1")
        tdSql.execute(f"GRANT write ON mnt1_db0.stb0 to u1")
        tdSql.execute(f"GRANT write ON mnt1_db0.stb1 to u1")
        tdSql.execute(f"GRANT alter ON mnt1_db0.stb0 to u1")
        tdSql.execute(f"GRANT alter ON mnt1_db0.stb1 to u1")
        tdSql.execute(f"GRANT all ON mnt1_db1.stb0 to u1;")
        tdSql.execute(f"GRANT all ON mnt1_db1.stb1 to u1;")
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name = 'u1'")
        tdSql.checkRows(15)
        tdDnodes.stop(1)
        tdDnodes.start(1)
        self.check_mount_query()
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name = 'u1'")
        tdSql.checkRows(15)
        # check conflicts
        tdSql.error("insert into mnt1_db0.ctb0 values(now, 100, 100, 100, 100.0, 100.0)", expectErrInfo="Operation not supported", fullMatched=False)
        tdSql.error("create table mnt1_db0.ntb100 (ts timestamp, c0 int)", expectErrInfo="Operation not supported", fullMatched=False)
        tdSql.error("create table mnt1_db0.ctb100 using mnt1_db0.stb0 tags(100)", expectErrInfo="Operation not supported", fullMatched=False)
        tdSql.error("drop table mnt1_db0.ctb0", expectErrInfo="Operation not supported", fullMatched=False)
        tdSql.error("drop table mnt1_db0.ntb0", expectErrInfo="Operation not supported", fullMatched=False)
        tdSql.error("drop dnode 1", expectErrInfo="The replica of mnode cannot less than 1", fullMatched=False)
        tdSql.error("drop database mnt1_db0", expectErrInfo="Mount object not supported", fullMatched=False)
        tdSql.error("drop database mnt1_db1", expectErrInfo="Mount object not supported", fullMatched=False)
        tdSql.error("drop table mnt1_db0.stb0", expectErrInfo="Mount object not supported", fullMatched=False)
        tdSql.error("alter table mnt1_db0.stb0 add column c100 int", expectErrInfo="Mount object not supported", fullMatched=False)
        # drop mount
        tdSql.execute("drop mount mnt1")
        tdSql.query("show mounts")
        tdSql.checkRows(0)
        tdSql.query(f"select * from information_schema.ins_user_privileges where user_name = 'u1'")
        tdSql.checkRows(0)

    def s4_recheck_mount_path(self):
        tdLog.info(" =============== step 4 recheck mount path")
        tdDnodes.stop(1)
        self.refactConfBetweenHostAndMnt(toMnt=True)
        tdDnodes.start(1)
        # check mount path
        tdSql.execute("use db0")
        tdSql.query("desc db0.vstb0")
        tdSql.checkRows(5)
        tdSql.query("desc db0.vstb1")
        tdSql.checkRows(5)
        tdSql.query("desc db0.vctb0")
        tdSql.checkRows(5)
        tdSql.query("desc db0.vntb0")
        tdSql.checkRows(3)
        tdSql.query("select * from ctb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from ctb1 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from stb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from stb1 limit 1")
        tdSql.checkRows(0)
        tdSql.query("select * from ctb10 limit 1")
        tdSql.checkRows(0)
        tdSql.query("select * from ctb11 limit 1")
        tdSql.checkRows(0)
        tdSql.query("show stables")
        tdSql.checkRows(4)
        tdSql.query("show tables")
        tdSql.checkRows(6)
        tdSql.query("select count(*) from db0.stb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1999)
        tdSql.query("select count(*) from db0.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 999)
        tdSql.query("select count(*) from db0.ctb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1000)
        tdSql.query("select count(*) from db0.stb1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from db0.ctb10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from db0.ntb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from db0.vntb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from db0.ctb11")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)
        tdSql.query("select count(*) from db1.stb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)
        tdSql.query("select count(*) from db1.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 4)
        tdSql.query("select count(*) from db1.ctb10")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 0)

    def s5_check_remount(self):
        tdLog.info(" =============== step 5 check remount")
        tdDnodes.stop(1)
        self.refactConfBetweenHostAndMnt(toMnt=False)
        tdDnodes.start(1)
        self.s3_create_drop_show_mount()

    def test_mount_basic(self):
        """ Mount basic

        1. Prepare mount path with data.
        2. Prepare host cluster.
        3. Check mount error cases.
        4. Create, drop, and show mount.
        5. Check mount SDB object conflicts.

        Since: v3.3.7.0

        Lables: common,ci,mount

        Jira: TS-5868

        History:
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_mount_basic.py

        """
        self.s0_prepare_mount_path()
        self.s1_prepare_host_cluster()
        self.s2_check_mount_error()
        self.s3_create_drop_show_mount()
        self.s4_recheck_mount_path()
        self.s5_check_remount()
        self.s4_recheck_mount_path() # recheck
