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
import random
import math

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
        tdSql.execute("create database  if not exists new keep 3650")
        tdSql.execute("create database  if not exists private keep 3650")
        tdSql.execute("create database  if not exists db2 keep 3650")

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
        tdSql.query("show create database db1")
        tdSql.checkRows(1)
        tdSql.query("show create database db2")
        tdSql.checkRows(1)
        tdSql.query("show create database new")
        tdSql.checkRows(1)
        tdSql.query("show create database private")
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

    def td4889(self):
        tdLog.printNoPrefix("==========TD-4889==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t1 int)")

        for i in range(1000):
            tdSql.execute(f"create table db.t1{i} using db.stb1 tags({i})")
            for j in range(100):
                tdSql.execute(f"insert into db.t1{i} values (now-100d, {i+j})")

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index})")
        for i in range(3):
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) == 1:
                tdLog.printNoPrefix("show vgroups row:0 col:6 data:0 == expect:1")
                break
            if i == 3:
                tdLog.exit("compacting not occured")
            time.sleep(0.5)

        pass

    def td5168insert(self):
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 float, c2 float, c3 double, c4 double) tags(t1 int)")
        tdSql.execute("create table db.t1 using db.stb1 tags(1)")

        for i in range(5):
            c1 = 1001.11 + i*0.1
            c2 = 1001.11 + i*0.1 + 1*0.01
            c3 = 1001.11 + i*0.1 + 2*0.01
            c4 = 1001.11 + i*0.1 + 3*0.01
            tdSql.execute(f"insert into db.t1 values ('2021-07-01 08:00:0{i}.000', {c1}, {c2}, {c3}, {c4})")

        # tdSql.execute("insert into db.t1 values ('2021-07-01 08:00:00.000', 1001.11, 1001.12, 1001.13, 1001.14)")
        # tdSql.execute("insert into db.t1 values ('2021-07-01 08:00:01.000', 1001.21, 1001.22, 1001.23, 1001.24)")
        # tdSql.execute("insert into db.t1 values ('2021-07-01 08:00:02.000', 1001.31, 1001.32, 1001.33, 1001.34)")
        # tdSql.execute("insert into db.t1 values ('2021-07-01 08:00:03.000', 1001.41, 1001.42, 1001.43, 1001.44)")
        # tdSql.execute("insert into db.t1 values ('2021-07-01 08:00:04.000', 1001.51, 1001.52, 1001.53, 1001.54)")

        # for i in range(1000000):
        for i in range(1000000):
            random1 = random.uniform(1000,1001)
            random2 = random.uniform(1000,1001)
            random3 = random.uniform(1000,1001)
            random4 = random.uniform(1000,1001)
            tdSql.execute(f"insert into db.t1 values (now+{i}a, {random1}, {random2},{random3}, {random4})")

        pass

    def td5168(self):
        tdLog.printNoPrefix("==========TD-4889==========")
        # 插入小范围内的随机数
        tdLog.printNoPrefix("=====step0: 默认情况下插入数据========")
        self.td5168insert()

        # 获取五个时间点的数据作为基准数值,未压缩情况下精准匹配
        for i in range(5):
            tdSql.query(f"select * from db.t1 where ts='2021-07-01 08:00:0{i}.000' ")
            # c1, c2, c3, c4 = tdSql.getData(0, 1), tdSql.getData(0, 2), tdSql.getData(0, 3), tdSql.getData(0, 4)
            for j in range(4):
                locals()["f" + str(j) + str(i)] = tdSql.getData(0, j+1)
                print(f"f{j}{i}:", locals()["f" + str(j) + str(i)])
                tdSql.checkData(0, j+1, locals()["f" + str(j) + str(i)])

        # tdSql.query("select * from db.t1 limit 100,1")
        # f10, f11, f12, f13 = tdSql.getData(0,1), tdSql.getData(0,2), tdSql.getData(0,3), tdSql.getData(0,4)
        #
        # tdSql.query("select * from db.t1 limit 1000,1")
        # f20, f21, f22, f23 = tdSql.getData(0,1), tdSql.getData(0,2), tdSql.getData(0,3), tdSql.getData(0,4)
        #
        # tdSql.query("select * from db.t1 limit 10000,1")
        # f30, f31, f32, f33 = tdSql.getData(0,1), tdSql.getData(0,2), tdSql.getData(0,3), tdSql.getData(0,4)
        #
        # tdSql.query("select * from db.t1 limit 100000,1")
        # f40, f41, f42, f43 = tdSql.getData(0,1), tdSql.getData(0,2), tdSql.getData(0,3), tdSql.getData(0,4)
        #
        # tdSql.query("select * from db.t1 limit 1000000,1")
        # f50, f51, f52, f53 = tdSql.getData(0,1), tdSql.getData(0,2), tdSql.getData(0,3), tdSql.getData(0,4)

        # 关闭服务并获取未开启压缩情况下的数据容量
        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)

        cfgdir = self.getCfgDir()
        cfgfile = self.getCfgFile()

        lossy_cfg_cmd=f"grep lossyColumns {cfgfile}|awk '{{print $2}}'"
        data_size_cmd = f"du -s {cfgdir}/../data/vnode/  | awk '{{print $1}}'"
        dsize_init = int(subprocess.check_output(data_size_cmd,shell=True).decode("utf-8"))
        lossy_args = subprocess.check_output(lossy_cfg_cmd, shell=True).decode("utf-8")
        tdLog.printNoPrefix(f"close the lossyColumns，data size is: {dsize_init};the lossyColumns line is: {lossy_args}")

        ###################################################
        float_lossy = "float"
        double_lossy = "double"
        float_double_lossy = "float|double"
        no_loosy = ""

        double_precision_cmd = f"sed -i '$a dPrecision 0.000001' {cfgfile}"
        _ = subprocess.check_output(double_precision_cmd, shell=True).decode("utf-8")

        lossy_float_cmd = f"sed -i '$a  lossyColumns {float_lossy}' {cfgfile} "
        lossy_double_cmd = f"sed -i '$d' {cfgfile} && sed -i '$a  lossyColumns {double_lossy}' {cfgfile} "
        lossy_float_double_cmd = f"sed -i '$d' {cfgfile} && sed -i '$a  lossyColumns {float_double_lossy}' {cfgfile} "
        lossy_no_cmd =  f"sed -i '$a  lossyColumns {no_loosy}' {cfgfile} "

        ###################################################

        # 开启有损压缩，参数float，并启动服务插入数据
        tdLog.printNoPrefix("=====step1: lossyColumns设置为float========")
        lossy_float =  subprocess.check_output(lossy_float_cmd, shell=True).decode("utf-8")
        tdDnodes.start(index)
        self.td5168insert()

        # 查询前面所述5个时间数据并与基准数值进行比较
        for i in range(5):
            tdSql.query(f"select * from db.t1 where ts='2021-07-01 08:00:0{i}.000' ")
            # c1, c2, c3, c4 = tdSql.getData(0, 1), tdSql.getData(0, 2), tdSql.getData(0, 3), tdSql.getData(0, 4)
            for j in range(4):
                # locals()["f" + str(j) + str(i)] = tdSql.getData(0, j+1)
                # print(f"f{j}{i}:", locals()["f" + str(j) + str(i)])
                tdSql.checkData(0, j+1, locals()["f" + str(j) + str(i)])

        # 关闭服务并获取压缩参数为float情况下的数据容量
        tdDnodes.stop(index)
        dsize_float = int(subprocess.check_output(data_size_cmd,shell=True).decode("utf-8"))
        lossy_args = subprocess.check_output(lossy_cfg_cmd, shell=True).decode("utf-8")
        tdLog.printNoPrefix(f"open the lossyColumns， data size is：{dsize_float};the lossyColumns line is: {lossy_args}")

        # 修改有损压缩，参数double，并启动服务
        tdLog.printNoPrefix("=====step2: lossyColumns设置为double========")
        lossy_double =  subprocess.check_output(lossy_double_cmd, shell=True).decode("utf-8")
        tdDnodes.start(index)
        self.td5168insert()

        # 查询前面所述5个时间数据并与基准数值进行比较
        for i in range(5):
            tdSql.query(f"select * from db.t1 where ts='2021-07-01 08:00:0{i}.000' ")
            for j in range(4):
                tdSql.checkData(0, j+1, locals()["f" + str(j) + str(i)])

        # 关闭服务并获取压缩参数为double情况下的数据容量
        tdDnodes.stop(index)
        dsize_double = int(subprocess.check_output(data_size_cmd, shell=True).decode("utf-8"))
        lossy_args = subprocess.check_output(lossy_cfg_cmd, shell=True).decode("utf-8")
        tdLog.printNoPrefix(f"open the lossyColumns, data size is：{dsize_double};the lossyColumns line is: {lossy_args}")

        # 修改有损压缩，参数 float&&double ，并启动服务
        tdLog.printNoPrefix("=====step3: lossyColumns设置为 float&&double ========")
        lossy_float_double =  subprocess.check_output(lossy_float_double_cmd, shell=True).decode("utf-8")
        tdDnodes.start(index)
        self.td5168insert()

        # 查询前面所述5个时间数据并与基准数值进行比较
        for i in range(5):
            tdSql.query(f"select * from db.t1 where ts='2021-07-01 08:00:0{i}.000' ")
            for j in range(4):
                tdSql.checkData(0, j+1, locals()["f" + str(j) + str(i)])

        # 关闭服务并获取压缩参数为 float&&double 情况下的数据容量
        tdDnodes.stop(index)
        dsize_float_double = int(subprocess.check_output(data_size_cmd, shell=True).decode("utf-8"))
        lossy_args = subprocess.check_output(lossy_cfg_cmd, shell=True).decode("utf-8")
        tdLog.printNoPrefix(f"open the lossyColumns， data size is：{dsize_float_double};the lossyColumns line is: {lossy_args}")

        if not ((dsize_float_double < dsize_init) and (dsize_double < dsize_init) and (dsize_float < dsize_init)) :
            tdLog.printNoPrefix(f"When lossyColumns value is float, data size is: {dsize_float}")
            tdLog.printNoPrefix(f"When lossyColumns value is double, data size is: {dsize_double}")
            tdLog.printNoPrefix(f"When lossyColumns value is float and double, data size is: {dsize_float_double}")
            tdLog.printNoPrefix(f"When lossyColumns is closed, data size is: {dsize_init}")
            tdLog.exit("压缩未生效")
        else:
            tdLog.printNoPrefix(f"When lossyColumns value is float, data size is: {dsize_float}")
            tdLog.printNoPrefix(f"When lossyColumns value is double, data size is: {dsize_double}")
            tdLog.printNoPrefix(f"When lossyColumns value is float and double, data size is: {dsize_float_double}")
            tdLog.printNoPrefix(f"When lossyColumns is closed, data size is: {dsize_init}")
            tdLog.printNoPrefix("压缩生效")
            
        pass

    def run(self):

        # master branch
        # self.td3690()
        # self.td4082()
        # self.td4288()
        # self.td4724()

        # develop branch
        # self.td4097()
        # self.td4889()
        self.td5168()


    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())



