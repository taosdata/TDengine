###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import subprocess

from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/debug/build/bin")]
                    break
        return buildPath

    def getCfgDir(self) -> str:
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            cfgDir = self.getBuildPath() + "/community/sim/dnode1/cfg"
        else:
            cfgDir = self.getBuildPath() + "/sim/dnode1/cfg"
        return cfgDir

    def getCfgFile(self) -> str:
        return self.getCfgDir()+"/taos.cfg"

    def td3690(self):
        tdLog.printNoPrefix("==========TD-3690==========")
        tdSql.query("show variables")
        tdSql.checkData(51, 1, 864000)

    def td4082(self):
        tdLog.printNoPrefix("==========TD-4082==========")
        cfgfile = self.getCfgFile()
        max_compressMsgSize = 100000000

        tdSql.query("show variables")
        tdSql.checkData(26, 1, -1)

        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)

        tdDnodes.stop(index)
        cmd = f"sed -i '$a compressMSgSize {max_compressMsgSize}' {cfgfile} "
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)
        tdSql.query("show variables")
        tdSql.checkData(26, 1, 100000000)

        tdDnodes.stop(index)
        cmd = f"sed -i '$s/{max_compressMsgSize}/{max_compressMsgSize+10}/g' {cfgfile} "
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)
        tdSql.query("show variables")
        tdSql.checkData(26, 1, -1)

        tdDnodes.stop(index)
        cmd = f"sed -i '$d' {cfgfile}"
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)

    def td4097(self):
        tdLog.printNoPrefix("==========TD-4097==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db1")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("create database  if not exists db1 keep 3650")

        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create stable db.stb2 (ts timestamp, c1 int) tags(t1 int)")
        tdSql.execute("create stable db1.stb3 (ts timestamp, c1 int) tags(t1 int)")

        tdSql.execute("create table db.t10 using db.stb1 tags(1)")
        tdSql.execute("create table db.t11 using db.stb1 tags(2)")
        tdSql.execute("create table db.t20 using db.stb2 tags(3)")
        tdSql.execute("create table db1.t30 using db1.stb3 tags(4)")

        tdLog.printNoPrefix("==========TD-4097==========")
        # 插入数据，然后进行show create 操作

        # p1 不进入指定数据库
        tdSql.query("show create database db")
        tdSql.checkRows(1)
        tdSql.error("show create database ")
        tdSql.error("show create databases db ")
        tdSql.error("show create database db.stb1")
        tdSql.error("show create database db0")
        tdSql.error("show create database db db1")
        tdSql.error("show create database db, db1")
        tdSql.error("show create database stb1")
        tdSql.error("show create database * ")

        tdSql.query("show create stable db.stb1")
        tdSql.checkRows(1)
        tdSql.error("show create stable db.t10")
        tdSql.error("show create stable db.stb0")
        tdSql.error("show create stable stb1")
        tdSql.error("show create stable ")
        tdSql.error("show create stable *")
        tdSql.error("show create stable db.stb1 db.stb2")
        tdSql.error("show create stable db.stb1, db.stb2")

        tdSql.query("show create table db.stb1")
        tdSql.checkRows(1)
        tdSql.query("show create table db.t10")
        tdSql.checkRows(1)
        tdSql.error("show create table db.stb0")
        tdSql.error("show create table stb1")
        tdSql.error("show create table ")
        tdSql.error("show create table *")
        tdSql.error("show create table db.stb1 db.stb2")
        tdSql.error("show create table db.stb1, db.stb2")

        # p2 进入指定数据库
        tdSql.execute("use db")

        tdSql.query("show create database db")
        tdSql.checkRows(1)
        tdSql.query("show create database db1")
        tdSql.checkRows(1)
        tdSql.error("show create database ")
        tdSql.error("show create databases db ")
        tdSql.error("show create database db.stb1")
        tdSql.error("show create database db0")
        tdSql.error("show create database db db1")
        tdSql.error("show create database db, db1")
        tdSql.error("show create database stb1")
        tdSql.error("show create database * ")

        tdSql.query("show create stable db.stb1")
        tdSql.checkRows(1)
        tdSql.query("show create stable stb1")
        tdSql.checkRows(1)
        tdSql.query("show create stable db1.stb3")
        tdSql.checkRows(1)
        tdSql.error("show create stable db.t10")
        tdSql.error("show create stable db")
        tdSql.error("show create stable t10")
        tdSql.error("show create stable db.stb0")
        tdSql.error("show create stables stb1")
        tdSql.error("show create stable ")
        tdSql.error("show create stable *")
        tdSql.error("show create stable db.stb1 db.stb2")
        tdSql.error("show create stable stb1 stb2")
        tdSql.error("show create stable db.stb1, db.stb2")
        tdSql.error("show create stable stb1, stb2")

        tdSql.query("show create table db.stb1")
        tdSql.checkRows(1)
        tdSql.query("show create table stb1")
        tdSql.checkRows(1)
        tdSql.query("show create table db.t10")
        tdSql.checkRows(1)
        tdSql.query("show create table t10")
        tdSql.checkRows(1)
        tdSql.query("show create table db1.t30")
        tdSql.checkRows(1)
        tdSql.error("show create table t30")
        tdSql.error("show create table db.stb0")
        tdSql.error("show create table db.t0")
        tdSql.error("show create table db")
        tdSql.error("show create tables stb1")
        tdSql.error("show create tables t10")
        tdSql.error("show create table ")
        tdSql.error("show create table *")
        tdSql.error("show create table db.stb1 db.stb2")
        tdSql.error("show create table db.t11 db.t10")
        tdSql.error("show create table db.stb1, db.stb2")
        tdSql.error("show create table db.t11, db.t10")
        tdSql.error("show create table stb1 stb2")
        tdSql.error("show create table t11 t10")
        tdSql.error("show create table stb1, stb2")
        tdSql.error("show create table t11, t10")

        # p3 删库删表后进行查询
        tdSql.execute("drop table if exists t11")

        tdSql.error("show create table t11")
        tdSql.error("show create table db.t11")
        tdSql.query("show create stable stb1")
        tdSql.checkRows(1)
        tdSql.query("show create table t10")
        tdSql.checkRows(1)

        tdSql.execute("drop stable if exists stb2")

        tdSql.error("show create table stb2")
        tdSql.error("show create table db.stb2")
        tdSql.error("show create stable stb2")
        tdSql.error("show create stable db.stb2")
        tdSql.error("show create stable db.t20")
        tdSql.query("show create database db")
        tdSql.checkRows(1)
        tdSql.query("show create stable db.stb1")
        tdSql.checkRows(1)

        tdSql.execute("drop database if exists db1")
        tdSql.error("show create database db1")
        tdSql.error("show create stable db1.t31")
        tdSql.error("show create stable db1.stb3")
        tdSql.query("show create database db")
        tdSql.checkRows(1)
        tdSql.query("show create stable db.stb1")
        tdSql.checkRows(1)

    def td4153(self):
        tdLog.printNoPrefix("==========TD-4153==========")

        pass

    def td4288(self):
        tdLog.printNoPrefix("==========TD-4288==========")
        # keep ~ [days,365000]
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db")
        tdSql.query("show variables")
        tdSql.checkData(36, 1, 3650)
        tdSql.query("show databases")
        tdSql.checkData(0,7,"3650,3650,3650")

        days = tdSql.getData(0, 6)
        tdSql.error("alter database db keep 3650001")
        tdSql.error("alter database db keep 9")
        tdSql.error("alter database db keep 0b")
        tdSql.error("alter database db keep 3650,9,36500")
        tdSql.error("alter database db keep 3650,3650,365001")
        tdSql.error("alter database db keep 36500,a,36500")
        tdSql.error("alter database db keep (36500,3650,3650)")
        tdSql.error("alter database db keep [36500,3650,36500]")
        tdSql.error("alter database db keep 36500,0xff,3650")
        tdSql.error("alter database db keep 36500,0o365,3650")
        tdSql.error("alter database db keep 36500,0A3Ch,3650")
        tdSql.error("alter database db keep")
        tdSql.error("alter database db keep0 36500")

        tdSql.execute("alter database db keep 36500")
        tdSql.query("show databases")
        tdSql.checkData(0, 7, "3650,3650,36500")
        tdSql.execute("drop database if exists db")

        tdSql.execute("create database  if not exists db1")
        tdSql.query("show databases")
        tdSql.checkData(0, 7, "3650,3650,3650")
        tdSql.query("show variables")
        tdSql.checkData(36, 1, 3650)

        tdSql.execute("alter database db1 keep 365")
        tdSql.execute("drop database if exists db1")


        pass

    def td4724(self):
        tdLog.printNoPrefix("==========TD-4724==========")
        cfgfile = self.getCfgFile()
        minTablesPerVnode = 5
        maxTablesPerVnode = 10
        maxVgroupsPerDb = 100

        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)

        tdDnodes.stop(index)
        vnode_cmd = f"sed -i '$a maxVgroupsPerDb {maxVgroupsPerDb}' {cfgfile} "
        min_cmd = f"sed -i '$a minTablesPerVnode {minTablesPerVnode}' {cfgfile} "
        max_cmd = f"sed -i '$a maxTablesPerVnode {maxTablesPerVnode}' {cfgfile} "
        try:
            _ = subprocess.check_output(vnode_cmd, shell=True).decode("utf-8")
            _ = subprocess.check_output(min_cmd, shell=True).decode("utf-8")
            _ = subprocess.check_output(max_cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t1 int)")
        insert_sql = "insert into "
        for i in  range(100):
            tdSql.execute(f"create table db.t1{i} using db.stb1 tags({i})")
            insert_sql += f" t1{i} values({1604298064000 + i*1000}, {i})"
        tdSql.query("show dnodes")
        vnode_count = tdSql.getData(0, 2)
        if vnode_count <= 1:
            tdLog.exit("vnode is less than 2")

        tdSql.execute(insert_sql)
        tdDnodes.stop(index)
        cmd = f"sed -i '$d' {cfgfile}"
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)

        pass

    def run(self):

        # master branch
        # self.td3690()
        # self.td4082()
        # self.td4288()
        self.td4724()

        # develop branch
        # self.td4097()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())



