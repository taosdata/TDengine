# -*- coding: utf-8 -*-

import time
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *

class TDTestCase:
    """ Test case for mount data path function.

    1. Prepare mount path with data.
    2. Prepare host cluster.
    3. Check mount error cases.
    4. Create, drop, and show mount.
    5. Check mount SDB object conflicts.

    Catalog:
        - Mounts:Create/Drop/Show/Query/Conflicts Detect/ReOpen

    Since: v3.3.7.0

    Lables: common,ci,mount

    Jira: TS-5868

    History:
        - 2025-07-08: Initial version from Kaili Xu.
    """

    path_parts = os.getcwd().split(os.sep)
    try:
        tdinternal_index = path_parts.index("TDinternal")
    except ValueError:
        raise ValueError("The specified directory 'TDinternal' was not found in the path.")
    TDinternal = os.sep.join(path_parts[:tdinternal_index + 1])
    dnode1Path = os.path.join(TDinternal, "sim", "dnode1")
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

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)
        self.setsql = TDSetSql()

    def s0_prepare_mount_path(self):
        tdSql.execute("drop database if exists db0")
        tdSql.execute("create database if not exists db0 replica 1")
        tdSql.execute("use db0")
        tdSql.execute("create table stb0(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("create table stb1(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb1 using stb1 tags(0)")
        tdSql.execute(f"insert into db0.ctb0 values(now, 1, 1, 1, 1.0, 1.0)(now + 1s, 2, 2, 2, 2.0, 2.0)(now + 2s, 3, 3, 3, 3.0, 3.0)(now + 3s, 4, 4, 4, 4.0, 4.0)(now + 4s, 5, 5, 5, 5.0, 5.0)")
        tdSql.execute("flush database db0")
        tdSql.execute("create database if not exists db1 replica 1")
        tdSql.execute("use db1")
        tdSql.execute("create table stb0(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb0 using stb0 tags(0)")
        tdSql.execute("create table stb1(ts timestamp, c0 int primary key,c1 bigint,c2 int,c3 float,c4 double) tags(t0 bigint unsigned)")
        tdSql.execute("create table ctb1 using stb1 tags(0)")
        tdSql.execute(f"insert into db1.ctb0 values(now, 1, 1, 1, 1.0, 1.0)(now + 1s, 2, 2, 2, 2.0, 2.0)(now + 2s, 3, 3, 3, 3.0, 3.0)(now + 3s, 4, 4, 4, 4.0, 4.0)(now + 4s, 5, 5, 5, 5.0, 5.0)")
        tdSql.execute("flush database db1")
        tdDnodes.stop(1)
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

    def s2_check_mount_error(self):
        tdSql.error("create mount mnt_1 on dnode 1 from ''", expectErrInfo=f"The mount name cannot contain _", fullMatched=False)
        tdSql.error("create mount mnt1 on dnode 1 from ''", expectErrInfo=f"The mount path is invalid", fullMatched=False)
        tdSql.error("create mount mnt1 on dnode 1 from 'path_not_exist'", expectErrInfo="No such file or directory", fullMatched=False)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPath}'", expectErrInfo="No such file or directory", fullMatched=False)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.hostPrimary}'", expectErrInfo="Cluster id identical to the host cluster id", fullMatched=False)
        tdSql.error(f"create mount d0 on dnode 1 from '{self.hostPrimary}'", expectErrInfo="Database with identical name already exists", fullMatched=False)
        self.refact_mount_dataDir()
        self.corruptMntClusterId()
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'", expectErrInfo="Cluster id not match", fullMatched=False)
        self.recoverMntClusterId()
        tdSql.error(f"drop mount mnt_not_exist", expectErrInfo="Mount not exist", fullMatched=False)
        tdSql.error(f"drop mount d0", expectErrInfo="Mount not exist", fullMatched=False)

    def s3_create_drop_show_mount(self):
        tdSql.execute(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'")
        tdSql.query("show mounts", count_expected_res=1)
        tdLog.info(f"result: {tdSql.queryResult}")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "mnt1")
        tdSql.checkData(0, 1, "1")
        tdSql.checkData(0, 3, self.mountPrimary)
        tdSql.error(f"create mount mnt1 on dnode 1 from '{self.mountPrimary}'", expectErrInfo="Mount already exists", fullMatched=False)
        # query from mount db
        tdSql.query("show create database `mnt1_db0`")
        tdSql.checkRows(1)
        tdSql.query("show create database `mnt1_db1`")
        tdSql.checkRows(1)
        tdSql.query("show mnt1_db0.stables")
        tdSql.checkRows(2)
        tdSql.query("show mnt1_db0.tables")
        tdSql.checkRows(2)
        tdSql.query("show mnt1_db1.stables")
        tdSql.checkRows(2)
        tdSql.query("show mnt1_db1.tables")
        tdSql.checkRows(2)
        tdSql.query("desc mnt1_db0.stb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db0.ctb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db1.stb0")
        tdSql.checkRows(7)
        tdSql.query("desc mnt1_db1.ctb0")
        tdSql.checkRows(7)
        tdSql.query("select * from mnt1_db0.stb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db0.ctb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db1.stb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select * from mnt1_db1.ctb0 limit 1")
        tdSql.checkRows(1)
        tdSql.query("select count(*) from mnt1_db1.stb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        tdSql.query("select count(*) from mnt1_db1.ctb0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 5)
        # check conflicts
        tdSql.error("drop dnode 1", expectErrInfo="The replica of mnode cannot less than 1", fullMatched=False)
        tdSql.error("drop database mnt1_db0", expectErrInfo="Mount object not supported", fullMatched=False)
        tdSql.error("drop database mnt1_db1", expectErrInfo="Mount object not supported", fullMatched=False)
        # drop mount
        tdSql.execute("drop mount mnt1")
        tdSql.query("show mounts")
        tdSql.checkRows(0)

    def run(self):
        self.s0_prepare_mount_path()
        self.s1_prepare_host_cluster()
        self.s2_check_mount_error()
        self.s3_create_drop_show_mount()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
