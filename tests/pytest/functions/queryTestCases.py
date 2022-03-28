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
import numpy as np
import inspect

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

        tdSql.prepare()

        tdSql.execute("show variables")
        res_off = tdSql.cursor.fetchall()
        resList = np.array(res_off)
        index = np.where(resList == "offlineThreshold")
        index_value = np.dstack((index[0])).squeeze()
        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 864000)

    def td4082(self):
        tdLog.printNoPrefix("==========TD-4082==========")
        tdSql.prepare()

        cfgfile = self.getCfgFile()
        max_compressMsgSize = 100000000

        tdSql.execute("show variables")
        res_com = tdSql.cursor.fetchall()
        rescomlist = np.array(res_com)
        cpms_index = np.where(rescomlist == "compressMsgSize")
        index_value = np.dstack((cpms_index[0])).squeeze()

        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 524288)

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
        tdSql.checkData(index_value, 1, 100000000)

        tdDnodes.stop(index)
        cmd = f"sed -i '$s/{max_compressMsgSize}/{max_compressMsgSize+10}/g' {cfgfile} "
        try:
            _ = subprocess.check_output(cmd, shell=True).decode("utf-8")
        except Exception as e:
            raise e

        tdDnodes.start(index)
        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, -1)

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

        # tdLog.printNoPrefix("==========TD-4097==========")
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

        tdSql.execute("drop database if exists db")
        tdSql.execute("drop database if exists db1")
        tdSql.execute("drop database if exists new")
        tdSql.execute("drop database if exists db2")
        tdSql.execute("drop database if exists private")

    def td4153(self):
        tdLog.printNoPrefix("==========TD-4153==========")

        pass

    def td4288(self):
        tdLog.printNoPrefix("==========TD-4288==========")
        # keep ~ [days,365000]
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db")

        tdSql.execute("show variables")
        res_kp = tdSql.cursor.fetchall()
        resList = np.array(res_kp)
        keep_index = np.where(resList == "keep")
        index_value = np.dstack((keep_index[0])).squeeze()

        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 3650)

        tdSql.query("show databases")
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            tdSql.checkData(0, 7, "3650,3650,3650")
        else:
            tdSql.checkData(0, 7, 3650)

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
        if ("community" in selfPath):
            tdSql.checkData(0, 7, "36500,36500,36500")
        else:
            tdSql.checkData(0, 7, 36500)

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db1")
        tdSql.query("show databases")
        if ("community" in selfPath):
            tdSql.checkData(0, 7, "3650,3650,3650")
        else:
            tdSql.checkData(0, 7, 3650)

        tdSql.query("show variables")
        tdSql.checkData(index_value, 1, 3650)

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
        cfg = {
            'minRowsPerFileBlock': '10',
            'maxRowsPerFileBlock': '200',
            'minRows': '10',
            'maxRows': '200',
            'maxVgroupsPerDb': '100',
            'maxTablesPerVnode': '1200',
        }
        tdSql.query("show dnodes")
        dnode_index = tdSql.getData(0,0)
        tdDnodes.stop(dnode_index)
        tdDnodes.deploy(dnode_index, cfg)
        tdDnodes.start(dnode_index)

        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650 blocks 3 minrows 10 maxrows 200")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t1 int)")

        nowtime = int(round(time.time() * 1000))
        for i in range(1000):
            tdSql.execute(f"create table db.t1{i} using db.stb1 tags({i})")
            sql = f"insert into db.t1{i} values"
            for j in range(260):
                sql += f"({nowtime-1000*i-j}, {i+j})"
                # tdSql.execute(f"insert into db.t1{i} values (now-100d, {i+j})")
            tdSql.execute(sql)

        # tdDnodes.stop(dnode_index)
        # tdDnodes.start(dnode_index)

        tdSql.query("show vgroups")
        index = tdSql.getData(0,0)
        tdSql.checkData(0, 6, 0)
        tdSql.execute(f"compact vnodes in({index})")
        start_time = time.time()
        while True:
            tdSql.query("show vgroups")
            if tdSql.getData(0, 6) != 0:
                tdLog.printNoPrefix("show vgroups row:0 col:6 data:1 == expect:1")
                break
            run_time = time.time()-start_time
            if run_time > 3:
                tdLog.exit("compacting not occured")
            # time.sleep(0.1)

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
        for i in range(10000):
            random1 = random.uniform(1000,1001)
            random2 = random.uniform(1000,1001)
            random3 = random.uniform(1000,1001)
            random4 = random.uniform(1000,1001)
            tdSql.execute(f"insert into db.t1 values (now+{i}a, {random1}, {random2},{random3}, {random4})")

        pass

    def td5168(self):
        tdLog.printNoPrefix("==========TD-5168==========")
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

    def td5433(self):
        tdLog.printNoPrefix("==========TD-5433==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int) tags(t0 tinyint, t1 int)")
        tdSql.execute("create stable db.stb2 (ts timestamp, c1 int) tags(t0 binary(16), t1 binary(16))")
        numtab=20000
        for i in range(numtab):
            sql  = f"create table db.t{i} using db.stb1 tags({i%128}, {100+i})"
            tdSql.execute(sql)
            tdSql.execute(f"insert into db.t{i} values (now-10d, {i})")
            tdSql.execute(f"insert into db.t{i} values (now-9d, {i*2})")
            tdSql.execute(f"insert into db.t{i} values (now-8d, {i*3})")

        tdSql.execute("create table db.t01 using db.stb2 tags('1', '100')")
        tdSql.execute("create table db.t02 using db.stb2 tags('2', '200')")
        tdSql.execute("create table db.t03 using db.stb2 tags('3', '300')")
        tdSql.execute("create table db.t04 using db.stb2 tags('4', '400')")
        tdSql.execute("create table db.t05 using db.stb2 tags('5', '500')")

        tdSql.query("select distinct t1 from stb1 where t1 != '150'")
        tdSql.checkRows(numtab-1)
        tdSql.query("select distinct t1 from stb1 where t1 != 150")
        tdSql.checkRows(numtab-1)
        tdSql.query("select distinct t1 from stb1 where t1 = 150")
        tdSql.checkRows(1)
        tdSql.query("select distinct t1 from stb1 where t1 = '150'")
        tdSql.checkRows(1)
        tdSql.query("select distinct t1 from stb1")
        tdSql.checkRows(numtab)

        tdSql.query("select distinct t0 from stb1 where t0 != '2'")
        tdSql.checkRows(127)
        tdSql.query("select distinct t0 from stb1 where t0 != 2")
        tdSql.checkRows(127)
        tdSql.query("select distinct t0 from stb1 where t0 = 2")
        tdSql.checkRows(1)
        tdSql.query("select distinct t0 from stb1 where t0 = '2'")
        tdSql.checkRows(1)
        tdSql.query("select distinct t0 from stb1")
        tdSql.checkRows(128)

        tdSql.query("select distinct t1 from stb2 where t1 != '200'")
        tdSql.checkRows(4)
        tdSql.query("select distinct t1 from stb2 where t1 != 200")
        tdSql.checkRows(4)
        tdSql.query("select distinct t1 from stb2 where t1 = 200")
        tdSql.checkRows(1)
        tdSql.query("select distinct t1 from stb2 where t1 = '200'")
        tdSql.checkRows(1)
        tdSql.query("select distinct t1 from stb2")
        tdSql.checkRows(5)

        tdSql.query("select distinct t0 from stb2 where t0 != '2'")
        tdSql.checkRows(4)
        tdSql.query("select distinct t0 from stb2 where t0 != 2")
        tdSql.checkRows(4)
        tdSql.query("select distinct t0 from stb2 where t0 = 2")
        tdSql.checkRows(1)
        tdSql.query("select distinct t0 from stb2 where t0 = '2'")
        tdSql.checkRows(1)
        tdSql.query("select distinct t0 from stb2")
        tdSql.checkRows(5)

        pass

    def td5798(self):
        tdLog.printNoPrefix("==========TD-5798 + TD-5810==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int, c2 int) tags(t0 tinyint, t1 int, t2 int)")
        tdSql.execute("create stable db.stb2 (ts timestamp, c2 int, c3 binary(16)) tags(t2 binary(16), t3 binary(16), t4 int)")
        maxRemainderNum=7
        tbnum=101
        for i in range(tbnum-1):
            sql  = f"create table db.t{i} using db.stb1 tags({i%maxRemainderNum}, {(i-1)%maxRemainderNum}, {i%2})"
            tdSql.execute(sql)
            tdSql.execute(f"insert into db.t{i} values (now-10d, {i}, {i%3})")
            tdSql.execute(f"insert into db.t{i} values (now-9d, {i}, {(i-1)%3})")
            tdSql.execute(f"insert into db.t{i} values (now-8d, {i}, {(i-2)%3})")
            tdSql.execute(f"insert into db.t{i} (ts )values (now-7d)")

            tdSql.execute(f"create table db.t0{i} using db.stb2 tags('{i%maxRemainderNum}', '{(i-1)%maxRemainderNum}', {i%3})")
            tdSql.execute(f"insert into db.t0{i} values (now-10d, {i}, '{(i+1)%3}')")
            tdSql.execute(f"insert into db.t0{i} values (now-9d, {i}, '{(i+2)%3}')")
            tdSql.execute(f"insert into db.t0{i} values (now-8d, {i}, '{(i)%3}')")
            tdSql.execute(f"insert into db.t0{i} (ts )values (now-7d)")
        tdSql.execute("create table db.t100num using db.stb1 tags(null, null, null)")
        tdSql.execute("create table db.t0100num using db.stb2 tags(null, null, null)")
        tdSql.execute(f"insert into db.t100num values (now-10d, {tbnum-1}, 1)")
        tdSql.execute(f"insert into db.t100num values (now-9d, {tbnum-1}, 0)")
        tdSql.execute(f"insert into db.t100num values (now-8d, {tbnum-1}, 2)")
        tdSql.execute(f"insert into db.t100num (ts )values (now-7d)")
        tdSql.execute(f"insert into db.t0100num values (now-10d, {tbnum-1}, 1)")
        tdSql.execute(f"insert into db.t0100num values (now-9d, {tbnum-1}, 0)")
        tdSql.execute(f"insert into db.t0100num values (now-8d, {tbnum-1}, 2)")
        tdSql.execute(f"insert into db.t0100num (ts )values (now-7d)")

        #========== TD-5810 suport distinct multi-data-coloumn ==========
        tdSql.query(f"select distinct  c1 from stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum)
        tdSql.query(f"select distinct  c2 from stb1")
        tdSql.checkRows(4)
        tdSql.query(f"select distinct  c1,c2 from stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum*3)
        tdSql.query(f"select distinct  c1,c1 from stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum)
        tdSql.query(f"select distinct  c1,c2 from stb1 where c1 <{tbnum} limit 3")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from stb1 where c1 <{tbnum} limit 3 offset {tbnum*3-2}")
        tdSql.checkRows(2)

        tdSql.query(f"select distinct  c1 from t1 where c1 <{tbnum}")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  c2 from t1")
        tdSql.checkRows(4)
        tdSql.query(f"select distinct  c1,c2 from t1 where c1 <{tbnum}")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c1 from t1 ")
        tdSql.checkRows(2)
        tdSql.query(f"select distinct  c1,c1 from t1 where c1 <{tbnum}")
        tdSql.checkRows(1)
        tdSql.query(f"select distinct  c1,c2 from t1 where c1 <{tbnum} limit 3")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from t1 where c1 <{tbnum} limit 3 offset 2")
        tdSql.checkRows(1)

        tdSql.query(f"select distinct  c3 from stb2 where c2 <{tbnum} ")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c3, c2 from stb2 where c2 <{tbnum} limit 2")
        tdSql.checkRows(2)

        tdSql.error("select distinct c5 from stb1")
        tdSql.error("select distinct c5 from t1")
        tdSql.error("select distinct c1 from db.*")
        tdSql.error("select c2, distinct c1 from stb1")
        tdSql.error("select c2, distinct c1 from t1")
        tdSql.error("select distinct c2 from ")
        tdSql.error("distinct c2 from stb1")
        tdSql.error("distinct c2 from t1")
        tdSql.error("select distinct  c1, c2, c3 from stb1")
        tdSql.error("select distinct  c1, c2, c3 from t1")
        tdSql.error("select distinct  stb1.c1, stb1.c2, stb2.c2, stb2.c3 from stb1")
        tdSql.error("select distinct  stb1.c1, stb1.c2, stb2.c2, stb2.c3 from t1")
        tdSql.error("select distinct  t1.c1, t1.c2, t2.c1, t2.c2 from t1")
        tdSql.query(f"select distinct  c1 c2, c2 c3 from stb1 where c1 <{tbnum}")
        tdSql.checkRows(tbnum*3)
        tdSql.query(f"select distinct  c1 c2, c2 c3 from t1 where c1 <{tbnum}")
        tdSql.checkRows(3)
        tdSql.error("select distinct  c1, c2 from stb1 order by ts")
        tdSql.error("select distinct  c1, c2 from t1 order by ts")
        tdSql.error("select distinct  c1, ts from stb1 group by c2")
        tdSql.error("select distinct  c1, ts from t1 group by c2")
        tdSql.error("select distinct  c1, max(c2) from stb1 ")
        tdSql.error("select distinct  c1, max(c2) from t1 ")
        tdSql.error("select max(c2), distinct  c1 from stb1 ")
        tdSql.error("select max(c2), distinct  c1 from t1 ")
        tdSql.error("select distinct  c1, c2 from stb1 where c1 > 3 group by t0")
        tdSql.error("select distinct  c1, c2 from t1 where c1 > 3 group by t0")
        tdSql.error("select distinct  c1, c2 from stb1 where c1 > 3 interval(1d) ")
        tdSql.error("select distinct  c1, c2 from t1 where c1 > 3 interval(1d) ")
        tdSql.error("select distinct  c1, c2 from stb1 where c1 > 3 interval(1d) fill(next)")
        tdSql.error("select distinct  c1, c2 from t1 where c1 > 3 interval(1d) fill(next)")
        tdSql.error("select distinct  c1, c2 from stb1 where ts > now-10d and ts < now interval(1d) fill(next)")
        tdSql.error("select distinct  c1, c2 from t1 where ts > now-10d and ts < now interval(1d) fill(next)")
        tdSql.error("select distinct  c1, c2 from stb1 where c1 > 3 slimit 1")
        tdSql.error("select distinct  c1, c2 from t1 where c1 > 3 slimit 1")
        tdSql.query(f"select distinct  c1, c2 from stb1 where c1 between {tbnum-2} and {tbnum} ")
        tdSql.checkRows(6)
        tdSql.query("select distinct  c1, c2 from stb1 where c1 in (1,2,3,4,5)")
        tdSql.checkRows(15)
        tdSql.query("select distinct  c1, c2 from stb1 where c1 in (100,1000,10000)")
        tdSql.checkRows(3)

        tdSql.query(f"select distinct  c1,c2 from (select * from stb1 where c1 > {tbnum-2}) ")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from (select * from t1 where c1 < {tbnum}) ")
        tdSql.checkRows(3)
        tdSql.query(f"select distinct  c1,c2 from (select * from stb1 where t2 !=0 and t2 != 1) ")
        tdSql.checkRows(0)
        tdSql.error("select distinct  c1, c2 from (select distinct c1, c2 from stb1 where t0 > 2 and t1 < 3) ")
        tdSql.error("select  c1, c2 from (select distinct c1, c2 from stb1 where t0 > 2 and t1 < 3) ")
        tdSql.query("select distinct  c1, c2 from (select c2, c1 from stb1 where c1 > 2 ) where  c1 < 4")
        tdSql.checkRows(3)
        tdSql.error("select distinct  c1, c2 from (select c1 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.error("select distinct  c1, c2 from (select c2, c1 from stb1 where c1 > 2 order by ts)")
        # tdSql.error("select distinct  c1, c2 from (select c2, c1 from t1 where c1 > 2 order by ts)")
        tdSql.error("select distinct  c1, c2 from (select c2, c1 from stb1 where c1 > 2 group by c1)")
        # tdSql.error("select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from stb1 group by c1)")
        # tdSql.error("select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from t1 group by c1)")
        tdSql.query("select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from stb1 )")
        tdSql.checkRows(1)
        tdSql.query("select distinct  c1, c2 from (select max(c1) c1, max(c2) c2 from t1 )")
        tdSql.checkRows(1)
        tdSql.error("select distinct  stb1.c1, stb1.c2 from stb1 , stb2 where stb1.ts=stb2.ts and stb1.t2=stb2.t4")
        tdSql.error("select distinct  t1.c1, t1.c2 from t1 , t2 where t1.ts=t2.ts ")

        # tdSql.error("select distinct  c1, c2 from (select count(c1) c1, count(c2) c2 from stb1 group by ts)")
        # tdSql.error("select distinct  c1, c2 from (select count(c1) c1, count(c2) c2 from t1 group by ts)")



        #========== TD-5798 suport distinct multi-tags-coloumn ==========
        tdSql.query("select distinct  t1 from stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t0, t1 from stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t1, t0 from stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t1, t2 from stb1")
        tdSql.checkRows(maxRemainderNum*2+1)
        tdSql.query("select distinct  t0, t1, t2 from stb1")
        tdSql.checkRows(maxRemainderNum*2+1)
        tdSql.query("select distinct  t0 t1, t1 t2 from stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t0, t0, t0 from stb1")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t0, t1 from t1")
        tdSql.checkRows(1)
        tdSql.query("select distinct  t0, t1 from t100num")
        tdSql.checkRows(1)

        tdSql.query("select distinct  t3 from stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t2, t3 from stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t3, t2 from stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t4, t2 from stb2")
        tdSql.checkRows(maxRemainderNum*3+1)
        tdSql.query("select distinct  t2, t3, t4 from stb2")
        tdSql.checkRows(maxRemainderNum*3+1)
        tdSql.query("select distinct  t2 t1, t3 t2 from stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t3, t3, t3 from stb2")
        tdSql.checkRows(maxRemainderNum+1)
        tdSql.query("select distinct  t2, t3 from t01")
        tdSql.checkRows(1)
        tdSql.query("select distinct  t3, t4 from t0100num")
        tdSql.checkRows(1)


        ########## should be error #########
        tdSql.error("select distinct  from stb1")
        tdSql.error("select distinct t3 from stb1")
        tdSql.error("select distinct t1 from db.*")
        tdSql.error("select distinct t2 from ")
        tdSql.error("distinct t2 from stb1")
        tdSql.error("select distinct stb1")
        tdSql.error("select distinct  t0, t1, t2, t3 from stb1")
        tdSql.error("select distinct  stb1.t0, stb1.t1, stb2.t2, stb2.t3 from stb1")

        tdSql.error("select dist t0 from stb1")
        tdSql.error("select distinct  stb2.t2, stb2.t3 from stb1")
        tdSql.error("select distinct  stb2.t2 t1, stb2.t3 t2 from stb1")

        tdSql.error("select distinct  t0, t1 from t1 where t0 < 7")

        ########## add where condition ##########
        tdSql.query("select distinct  t0, t1 from stb1 where t1 > 3")
        tdSql.checkRows(3)
        tdSql.query("select distinct  t0, t1 from stb1 where t1 > 3 limit 2")
        tdSql.checkRows(2)
        tdSql.query("select distinct  t0, t1 from stb1 where t1 > 3 limit 2 offset 2")
        tdSql.checkRows(1)
        tdSql.query("select distinct  t0, t1 from stb1 where t1 > 3 slimit 2")
        tdSql.checkRows(3)
        tdSql.error("select distinct  t0, t1 from stb1 where c1 > 2")
        tdSql.query("select distinct  t0, t1 from stb1 where t1 > 3 and t1 < 5")
        tdSql.checkRows(1)
        tdSql.error("select distinct  stb1.t0, stb1.t1 from stb1, stb2 where stb1.t2=stb2.t4")
        tdSql.error("select distinct  t0, t1 from stb1 where stb2.t4 > 2")
        tdSql.error("select distinct  t0, t1 from stb1 where t1 > 3 group by t0")
        tdSql.error("select distinct  t0, t1 from stb1 where t1 > 3 interval(1d) ")
        tdSql.error("select distinct  t0, t1 from stb1 where t1 > 3 interval(1d) fill(next)")
        tdSql.error("select distinct  t0, t1 from stb1 where ts > now-10d and ts < now interval(1d) fill(next)")

        tdSql.error("select max(c1), distinct t0 from stb1 where t0 > 2")
        tdSql.error("select distinct t0, max(c1) from stb1 where t0 > 2")
        tdSql.error("select distinct  t0  from stb1 where t0 in (select t0 from stb1 where t0 > 2)")
        tdSql.query("select distinct  t0, t1  from stb1 where t0 in (1,2,3,4,5)")
        tdSql.checkRows(5)
        tdSql.query("select distinct  t1 from (select t0, t1 from stb1 where t0 > 2) ")
        tdSql.checkRows(4)
        tdSql.error("select distinct  t1 from (select distinct t0, t1 from stb1 where t0 > 2 and t1 < 3) ")
        tdSql.error("select distinct  t1 from (select distinct t0, t1 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.query("select distinct  t1 from (select t0, t1 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.checkRows(1)
        tdSql.error("select distinct  t1, t0 from (select t1 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.error("select distinct  t1, t0 from (select max(t1) t1, max(t0) t0 from stb1 group by t1)")
        tdSql.error("select distinct  t1, t0 from (select max(t1) t1, max(t0) t0 from stb1)")
        tdSql.query("select distinct  t1, t0 from (select t1,t0 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.checkRows(1)
        tdSql.error(" select distinct  t1, t0 from (select t1,t0 from stb1 where t0 > 2 order by ts) where  t1 < 3")
        tdSql.error("select t1, t0 from (select distinct t1,t0 from stb1 where t0 > 2 ) where  t1 < 3")
        tdSql.error(" select distinct  t1, t0 from (select t1,t0 from stb1 where t0 > 2  group by ts) where  t1 < 3")
        tdSql.error("select distinct  stb1.t1, stb1.t2 from stb1 , stb2 where stb1.ts=stb2.ts and stb1.t2=stb2.t4")
        tdSql.error("select distinct  t1.t1, t1.t2 from t1 , t2 where t1.ts=t2.ts ")

        pass

    def td5935(self):
        tdLog.printNoPrefix("==========TD-5935==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")

        tdSql.execute("use db")
        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int, c2 float) tags(t1 int, t2 int)")
        nowtime=int(round((time.time()*1000)))
        for i in range(100):
            sql = f"create table db.t{i} using db.stb1 tags({i % 7}, {i % 2})"
            tdSql.execute(sql)
            for j in range(1000):
                tdSql.execute(f"insert into db.t{i} values ({nowtime-j*10}, {1000-j}, {round(random.random()*j,3)})")
            tdSql.execute(f"insert into db.t{i} (ts) values ({nowtime-10000}) ")

        ########### TD-5933  verify the bug of "function stddev with interval return 0 rows" is fixed ##########
        stddevAndIntervalSql=f"select last(*) from t0 where ts>={nowtime-10000}  interval(10a) limit 10"
        tdSql.query(stddevAndIntervalSql)
        tdSql.checkRows(10)

        ########## TD-5978 verify the bug of "when start row is null, result by fill(next) is 0 " is fixed ##########
        fillsql=f"select last(*) from t0 where ts>={nowtime-10000} and ts<{nowtime}  interval(10a) fill(next) limit 10"
        tdSql.query(fillsql)
        fillResult=False
        if (tdSql.getData(0,2) != 0) and (tdSql.getData(0, 2)  is not None):
            fillResult=True
        if fillResult:
            tdLog.success(f"sql is :{fillsql}, fill(next) is correct")
        else:
            tdLog.exit("fill(next) is wrong")

        pass

    def td6068(self):
        tdLog.printNoPrefix("==========TD-6068==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute("create stable db.stb1 (ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool) tags(t1 int)")

        for i in range(100):
            sql  = f"create table db.t{i} using db.stb1 tags({i})"
            tdSql.execute(sql)
            tdSql.execute(f"insert into db.t{i} values (now-10h, {i}, {i+random.random()}, now-10h, 'a_{i}', '{i-random.random()}', True)")
            tdSql.execute(f"insert into db.t{i} values (now-9h, {i+random.randint(1,10)}, {i+random.random()}, now-9h, 'a_{i}', '{i-random.random()}', FALSE )")
            tdSql.execute(f"insert into db.t{i} values (now-8h, {i+random.randint(1,10)}, {i+random.random()}, now-8h, 'b_{i}', '{i-random.random()}', True)")
            tdSql.execute(f"insert into db.t{i} values (now-7h, {i+random.randint(1,10)}, {i+random.random()}, now-7h, 'b_{i}', '{i-random.random()}', FALSE )")
            tdSql.execute(f"insert into db.t{i} values (now-6h, {i+random.randint(1,10)}, {i+random.random()}, now-6h, 'c_{i}', '{i-random.random()}', True)")
            tdSql.execute(f"insert into db.t{i} values (now-5h, {i+random.randint(1,10)}, {i+random.random()}, now-5h, 'c_{i}', '{i-random.random()}', FALSE )")
            tdSql.execute(f"insert into db.t{i} (ts)values (now-4h)")
            tdSql.execute(f"insert into db.t{i} (ts)values (now-11h)")
            tdSql.execute(f"insert into db.t{i} (ts)values (now-450m)")

        tdSql.query("select ts as t,derivative(c1, 10m, 0) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, derivative(c1, 1h, 0) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.checkCols(4)
        tdSql.query("select ts as t, derivative(c1, 1s, 0) from t1")
        tdSql.query("select ts as t, derivative(c1, 1d, 0) from t1")
        tdSql.error("select ts as t, derivative(c1, 1h, 0) from stb1")
        tdSql.query("select ts as t, derivative(c2, 1h, 0) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, derivative(c3, 1h, 0) from t1")
        tdSql.error("select ts as t, derivative(c4, 1h, 0) from t1")
        tdSql.query("select ts as t, derivative(c5, 1h, 0) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, derivative(c6, 1h, 0) from t1")
        tdSql.error("select ts as t, derivative(t1, 1h, 0) from t1")

        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, diff(c1) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.checkCols(4)
        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.error("select ts as t, diff(c1) from stb1")
        tdSql.query("select ts as t, diff(c2) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, diff(c3) from t1")
        tdSql.error("select ts as t, diff(c4) from t1")
        tdSql.query("select ts as t, diff(c5) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, diff(c6) from t1")
        tdSql.error("select ts as t, diff(t1) from t1")
        tdSql.error("select ts as t, diff(c1, c2) from t1")

        tdSql.error("select ts as t, bottom(c1, 0) from t1")
        tdSql.query("select ts as t, bottom(c1, 5) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, bottom(c1, 5) from stb1")
        tdSql.checkRows(5)
        tdSql.query("select ts as t, bottom(c1, 5) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.query("select ts as t, bottom(c1, 8) from t1")
        tdSql.checkRows(6)
        tdSql.query("select ts as t, bottom(c2, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, bottom(c3, 5) from t1")
        tdSql.error("select ts as t, bottom(c4, 5) from t1")
        tdSql.query("select ts as t, bottom(c5, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, bottom(c6, 5) from t1")
        tdSql.error("select ts as t, bottom(c5, 8) as b from t1 order by b")
        tdSql.error("select ts as t, bottom(t1, 1) from t1")
        tdSql.error("select ts as t, bottom(t1, 1) from stb1")
        tdSql.error("select ts as t, bottom(t1, 3) from stb1 order by c3")
        tdSql.error("select ts as t, bottom(t1, 3) from t1 order by c3")


        tdSql.error("select ts as t, top(c1, 0) from t1")
        tdSql.query("select ts as t, top(c1, 5) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, top(c1, 5) from stb1")
        tdSql.checkRows(5)
        tdSql.query("select ts as t, top(c1, 5) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.query("select ts as t, top(c1, 8) from t1")
        tdSql.checkRows(6)
        tdSql.query("select ts as t, top(c2, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, top(c3, 5) from t1")
        tdSql.error("select ts as t, top(c4, 5) from t1")
        tdSql.query("select ts as t, top(c5, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, top(c6, 5) from t1")
        tdSql.error("select ts as t, top(c5, 8) as b from t1 order by b")
        tdSql.error("select ts as t, top(t1, 1) from t1")
        tdSql.error("select ts as t, top(t1, 1) from stb1")
        tdSql.error("select ts as t, top(t1, 3) from stb1 order by c3")
        tdSql.error("select ts as t, top(t1, 3) from t1 order by c3")

        tdDnodes.stop(1)
        tdDnodes.start(1)

        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, diff(c1) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.checkCols(4)
        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.query("select ts as t, diff(c1) from t1")
        tdSql.error("select ts as t, diff(c1) from stb1")
        tdSql.query("select ts as t, diff(c2) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, diff(c3) from t1")
        tdSql.error("select ts as t, diff(c4) from t1")
        tdSql.query("select ts as t, diff(c5) from t1")
        tdSql.checkRows(5)
        tdSql.error("select ts as t, diff(c6) from t1")
        tdSql.error("select ts as t, diff(t1) from t1")
        tdSql.error("select ts as t, diff(c1, c2) from t1")

        tdSql.error("select ts as t, bottom(c1, 0) from t1")
        tdSql.query("select ts as t, bottom(c1, 5) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, bottom(c1, 5) from stb1")
        tdSql.checkRows(5)
        tdSql.query("select ts as t, bottom(c1, 5) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.query("select ts as t, bottom(c1, 8) from t1")
        tdSql.checkRows(6)
        tdSql.query("select ts as t, bottom(c2, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, bottom(c3, 5) from t1")
        tdSql.error("select ts as t, bottom(c4, 5) from t1")
        tdSql.query("select ts as t, bottom(c5, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, bottom(c6, 5) from t1")
        tdSql.error("select ts as t, bottom(c5, 8) as b from t1 order by b")
        tdSql.error("select ts as t, bottom(t1, 1) from t1")
        tdSql.error("select ts as t, bottom(t1, 1) from stb1")
        tdSql.error("select ts as t, bottom(t1, 3) from stb1 order by c3")
        tdSql.error("select ts as t, bottom(t1, 3) from t1 order by c3")


        tdSql.error("select ts as t, top(c1, 0) from t1")
        tdSql.query("select ts as t, top(c1, 5) from t1")
        tdSql.checkRows(5)
        tdSql.checkCols(3)
        for i in range(5):
            data=tdSql.getData(i, 0)
            tdSql.checkData(i, 1, data)
        tdSql.query("select ts as t, top(c1, 5) from stb1")
        tdSql.checkRows(5)
        tdSql.query("select ts as t, top(c1, 5) from stb1 group by tbname")
        tdSql.checkRows(500)
        tdSql.query("select ts as t, top(c1, 8) from t1")
        tdSql.checkRows(6)
        tdSql.query("select ts as t, top(c2, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, top(c3, 5) from t1")
        tdSql.error("select ts as t, top(c4, 5) from t1")
        tdSql.query("select ts as t, top(c5, 8) from t1")
        tdSql.checkRows(6)
        tdSql.error("select ts as t, top(c6, 5) from t1")
        tdSql.error("select ts as t, top(c5, 8) as b from t1 order by b")
        tdSql.error("select ts as t, top(t1, 1) from t1")
        tdSql.error("select ts as t, top(t1, 1) from stb1")
        tdSql.error("select ts as t, top(t1, 3) from stb1 order by c3")
        tdSql.error("select ts as t, top(t1, 3) from t1 order by c3")
        pass

    def apercentile_query_form(self, col="c1", p=0, com=',', algo="'t-digest'", alias="", table_expr="t1", condition=""):

        '''
        apercentile function:
        :param col:         string, column name, required parameters;
        :param p:           float, percentile interval, [0,100], required parameters;
        :param algo:        string, alforithm, real form like: ', algorithm' , algorithm: {type:int, data:[0, 1]};
        :param alias:       string, result column another name;
        :param table_expr:  string or expression, data source（eg,table/stable name, result set）, required parameters;
        :param condition:   expression；
        :param args:        other funtions,like: ', last(col)'
        :return:            apercentile query statement,default: select apercentile(c1, 0, 1) from t1
        '''

        return f"select apercentile({col}, {p}{com} {algo})  {alias}  from {table_expr} {condition}"

    def checkapert(self,col="c1", p=0, com=',', algo='"t-digest"', alias="", table_expr="t1", condition="" ):

        tdSql.query(f"select count({col})  from {table_expr} {condition}")
        if tdSql.queryRows == 0:
            tdSql.query(self.apercentile_query_form(
                col=col, p=p, com=com, algo=algo, alias=alias, table_expr=table_expr, condition=condition
            ))
            tdSql.checkRows(0)
            return

        pset = [0, 40,  60, 100]
        if p not in pset:
            pset.append(p)

        if "stb" in table_expr:
            tdSql.query(f"select spread({col})  from stb1")
        else:
            tdSql.query(f"select avg(c1) from (select spread({col.split('.')[-1]}) c1 from stb1  group by tbname)")
        spread_num = tdSql.getData(0, 0)

        for pi in pset:

            if "group" in condition:
                tdSql.query(f"select last_row({col}) from {table_expr} {condition}")
                query_result = tdSql.queryResult
                query_rows = tdSql.queryRows
                for i in range(query_rows):
                    pre_condition = condition.replace("slimit",'limit').replace("group by tbname", "").split("soffset")[0]
                    tbname = query_result[i][-1]
                    tdSql.query(f"select percentile({col}, {pi}) {alias} from {tbname} {pre_condition}")
                    print(tdSql.sql)
                    pre_data = tdSql.getData(0, 0)
                    tdSql.query(self.apercentile_query_form(
                        col=col, p=pi, com=com, algo='"t-digest"', alias=alias, table_expr=table_expr, condition=condition
                    ))
                    if abs(tdSql.getData(i, 0)) >= (spread_num*0.02):
                        tdSql.checkDeviaRation(i, 0, pre_data, 0.1)
                    else:
                        devia = abs((tdSql.getData(i, 0) - pre_data) / (spread_num * 0.02))
                        if devia < 0.5:
                            tdLog.info(f"sql:{tdSql.sql}, result data:{tdSql.getData(i, 0)}, expect data:{pre_data}, "
                                           f"actual deviation:{devia} <= expect deviation: 0.01")
                        else:
                            tdLog.exit(
                                    f"[{inspect.getframeinfo(inspect.stack()[1][0]).lineno}],check failed:sql:{tdSql.sql}, "
                                    f"result data:{tdSql.getData(i, 0)}, expect data:{pre_data}, "
                                    f"actual deviation:{devia} > expect deviation: 0.01")

            # if "group" in condition:
            #     tdSql.query(self.apercentile_query_form(
            #         col=col, p=pi, com=com, algo='"default"', alias=alias, table_expr=table_expr, condition=condition
            #     ))
            #     query_result = tdSql.queryResult
            #     query_rows = tdSql.queryRows
            #     tdSql.query(self.apercentile_query_form(
            #         col=col, p=pi, com=com, algo='"t-digest"', alias=alias, table_expr=table_expr, condition=condition
            #     ))
            #     for i in range(query_rows):
            #         if abs(tdSql.getData(i, 0)) >= (spread_num*0.02):
            #             tdSql.checkDeviaRation(i, 0, query_result[i][0], 0.1)
            #         else:
            #             devia = abs((tdSql.getData(i, 0) - query_result[i][0]) / (spread_num * 0.02))
            #             if devia < 0.5:
            #                 tdLog.info(f"sql:{tdSql.sql}, result data:{tdSql.getData(i, 0)}, expect data:{tdSql.queryResult[i][0]}, "
            #                                f"actual deviation:{devia} <= expect deviation: 0.01")
            #             else:
            #                 tdLog.exit(
            #                         f"[{inspect.getframeinfo(inspect.stack()[1][0]).lineno}],check failed:sql:{tdSql.sql}, "
            #                         f"result data:{tdSql.getData(i, 0)}, expect data:{tdSql.queryResult[i][0]}, "
            #                         f"actual deviation:{devia} > expect deviation: 0.01")

            else:
                if ',' in alias or not alias:
                    tdSql.query(f"select {col} from {table_expr} {condition}")
                elif "stb" not in table_expr:
                    tdSql.query(f"select percentile({col}, {pi}) {alias} from {table_expr} {condition}")
                else:
                    tdSql.query(self.apercentile_query_form(
                            col=col, p=pi, com=com, algo='"default"', alias=alias, table_expr=table_expr, condition=condition
                        ))
                query_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
                tdSql.query(self.apercentile_query_form(
                    col=col, p=pi, com=com, algo=algo, alias=alias, table_expr=table_expr, condition=condition
                ))

                if abs(tdSql.getData(0, 0)) >= (spread_num * 0.02):
                    tdSql.checkDeviaRation(0, 0, np.percentile(query_result, pi), 0.1)
                else:
                    devia = abs((tdSql.getData(0, 0) - np.percentile(query_result, pi)) / (spread_num * 0.02))
                    if devia < 0.5:
                        tdLog.info(
                            f"sql:{tdSql.sql}, result data:{tdSql.getData(0, 0)}, expect data:{np.percentile(query_result, pi)}, "
                            f"actual deviation:{devia} <= expect deviation: 0.01")
                    else:
                        tdLog.exit(
                            f"[{inspect.getframeinfo(inspect.stack()[1][0]).lineno}],check failed:sql:{tdSql.sql}, "
                            f"result data:{tdSql.getData(0, 0)}, expect data:{np.percentile(query_result, pi)}, "
                            f"actual deviation:{devia} > expect deviation: 0.01")


    def apercentile_query(self):

        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)

        # case1： int col
        self.checkapert()
        # case2: float col
        case2 = {'col':'c2'}
        self.checkapert(**case2)
        # case3: double col
        case3 = {'col':'c5'}
        self.checkapert(**case3)
        # case4: bigint col
        case4 = {'col':'c7'}
        self.checkapert(**case4)
        # case5: smallint col
        case5 = {'col':'c8'}
        self.checkapert(**case5)
        # case6: tinyint col
        case6 = {'col':'c9'}
        self.checkapert(**case6)
        # case7: stable
        case7 = {'table_expr':'stb1'}
        self.checkapert(**case7)
        # case8: nest query, outquery
        case8 = {'table_expr':'(select c1 from t1)'}
        self.checkapert(**case8)
        # case9: nest query, inquery and out query
        case9 = {'table_expr':'(select apercentile(c1, 0) as c1 from t1)'}
        self.checkapert(**case9)

        # case10: nest query, inquery
        tdSql.query("select * from (select  c1 from stb1)")
        if tdSql.queryRows == 0:
            tdSql.query("select * from (select apercentile(c1,0) c1 from stb1)")
            tdSql.checkRows(0)
        else:
            query_result = np.array(tdSql.queryResult)[np.array(tdSql.queryResult) != None]
            tdSql.query("select * from (select apercentile(c1, 0) c1 from stb1)")
            tdSql.checkDeviaRation(0, 0, np.percentile(query_result, 0), 0.1)
            tdSql.query("select * from (select apercentile(c1,100) c1 from stb1)")
            tdSql.checkDeviaRation(0, 0, np.percentile(query_result, 100), 0.1)
            tdSql.query("select * from (select apercentile(c1,40) c1 from stb1)")
            tdSql.checkDeviaRation(0, 0, np.percentile(query_result, 40), 0.1)

        # case11: no algorithm = algo:0
        case11 = {'com':'', 'algo': ''}
        self.checkapert(**case11)

        # case12~14: p: bin/oct/hex
        case12 = {'p': 0b1100100}
        self.checkapert(**case12)
        case13 = {'algo':'"T-DIGEST"'}
        self.checkapert(**case13)
        case14 = {'p':0x32, 'algo':'"DEFAULT"'}
        self.checkapert(**case14)

        # case15~21: mix with aggregate function
        case15 = {'alias':', count(*)'}
        self.checkapert(**case15)
        case16 = {'alias':', avg(c1)'}
        self.checkapert(**case16)
        case17 = {'alias':', twa(c1)'}
        self.checkapert(**case17)
        case18 = {'alias':', irate(c1)'}
        self.checkapert(**case18)
        case19 = {'alias':', sum(c1)'}
        self.checkapert(**case19)
        case20 = {'alias':', stddev(c1)'}
        self.checkapert(**case20)
        case21 = {'alias':', leastsquares(c1, 1, 1)'}
        self.checkapert(**case21)

        # case22~27：mix with selector function
        case22 = {'alias':', min(c1)'}
        self.checkapert(**case22)
        case23 = {'alias':', max(c1)'}
        self.checkapert(**case23)
        case24 = {'alias':', first(c1)'}
        self.checkapert(**case24)
        case25 = {'alias':', last(c1)'}
        self.checkapert(**case25)
        case26 = {'alias':', percentile(c1, 0)'}
        self.checkapert(**case26)
        case27 = {'alias':', apercentile(c1, 0, "t-digest")'}
        self.checkapert(**case27)

        # case28~29: mix with computing function
        case28 = {'alias':', spread(c1)'}
        self.checkapert(**case28)
        # case29: mix with four operation
        case29 = {'alias':'+ spread(c1)'}
        self.checkapert(**case29)

        # case30~36: with condition
        case30 = {'condition':'where ts > now'}
        self.checkapert(**case30)
        case31 = {'condition':'where c1 between 1 and 200'}
        self.checkapert(**case31)
        case32 = {'condition':f'where c1 in {tuple(i for i in range(200))}'}
        self.checkapert(**case32)
        case33 = {'condition':'where c1>100 and c2<100'}
        self.checkapert(**case33)
        case34 = {'condition':'where c1 is not null'}
        self.checkapert(**case34)
        case35 = {'condition':'where c4 like "_inary%"'}
        self.checkapert(**case35)
        case36 = {'table_expr':'stb1' ,'condition':'where tbname like "t_"'}
        self.checkapert(**case36)

        # case37~38: with join
        case37 = {'col':'t1.c1','table_expr':'t1, t2 ','condition':'where t1.ts=t2.ts'}
        self.checkapert(**case37)
        case38 = {'col':'stb1.c1', 'table_expr':'stb1, stb2', 'condition':'where stb1.ts=stb2.ts and stb1.st1=stb2.st2'}
        self.checkapert(**case38)

        # case39: with group by
        case39 = {'table_expr':'stb1', 'condition':'group by tbname'}
        self.checkapert(**case39)

        # case40: with slimit
        case40 = {'table_expr':'stb1', 'condition':'group by tbname slimit 1'}
        self.checkapert(**case40)

        # case41: with soffset
        case41 = {'table_expr':'stb1', 'condition':'group by tbname slimit 1 soffset 1'}
        self.checkapert(**case41)

        # case42: with order by
        case42 = {'table_expr':'stb1' ,'condition':'order by ts'}
        self.checkapert(**case42)
        case43 = {'table_expr':'t1' ,'condition':'order by ts'}
        self.checkapert(**case43)

        # case44: with limit offset
        case44 = {'table_expr':'stb1', 'condition':'group by tbname limit 1'}
        self.checkapert(**case44)
        case45 = {'table_expr':'stb1', 'condition':'group by tbname limit 1 offset 1'}
        self.checkapert(**case45)

        pass

    def error_apercentile(self):

        # unusual test
        #
        # table schema :ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool
        #                 c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)
        #
        # form test
        tdSql.error(self.apercentile_query_form(col="",com='',algo='')) # no col , no algorithm
        tdSql.error(self.apercentile_query_form(col=""))                # no col , algorithm
        tdSql.error(self.apercentile_query_form(p='',com='',algo=''))   # no p , no algorithm
        tdSql.error(self.apercentile_query_form(p=''))                  # no p , algorithm
        tdSql.error("apercentile( c1, 100) from t1")                    # no select
        tdSql.error("select apercentile from t1")                       # no algorithm condition
        tdSql.error("select apercentile c1,0 from t1")                  # no brackets
        tdSql.error("select apercentile (c1,0)  t1")                    # no from
        tdSql.error(self.apercentile_query_form(col='(c1,0)',p='',com='',algo=''))   # no p , no algorithm
        tdSql.error("select apercentile( (c1,0) )  from t1")            # no table_expr
        tdSql.error("select apercentile{ (c1,0) } from t1")             # sql form error 1
        tdSql.error("select apercentile[ (c1,0) ] from t1")             # sql form error 2
        tdSql.error("select [apercentile(c1,0) ] from t1")              # sql form error 3
        tdSql.error("select apercentile((c1, 0), 'default')  from t1")          # sql form error 5
        tdSql.error("select apercentile(c1, (0, 'default'))  from t1")          # sql form error 6
        tdSql.error("select apercentile(c1, (0), 1)  from t1")          # sql form error 7
        tdSql.error("select apercentile([c1, 0], 'default')  from t1")          # sql form error 8
        tdSql.error("select apercentile(c1, [0, 'default'])  from t1")          # sql form error 9
        tdSql.error("select apercentile(c1, {0, 'default'})  from t1")          # sql form error 10
        tdSql.error("select apercentile([c1, 0])  from t1")             # sql form error 11
        tdSql.error("select apercentile({c1, 0})  from t1")             # sql form error 12
        tdSql.error("select apercentile(c1)  from t1")                  # agrs: 1
        tdSql.error("select apercentile(c1, 0, 'default', 0)  from t1")         # agrs: 4
        tdSql.error("select apercentile(c1, 0, 0, 'default')  from t1")         # agrs: 4
        tdSql.error("select apercentile()  from t1")                    # agrs: null 1
        tdSql.error("select apercentile  from t1")                      # agrs: null 2
        tdSql.error("select apercentile( , , )  from t1")               # agrs: null 3
        tdSql.error(self.apercentile_query_form(col='', p='', algo='')) # agrs: null 4
        tdSql.error(self.apercentile_query_form(col="st1"))              # col:tag column
        tdSql.error(self.apercentile_query_form(col=123))               # col:numerical
        tdSql.error(self.apercentile_query_form(col=True))              # col:bool
        tdSql.error(self.apercentile_query_form(col=''))                # col:''
        tdSql.error(self.apercentile_query_form(col="last(c1)"))        # col:expr
        tdSql.error(self.apercentile_query_form(col="t%"))              # col:non-numerical
        tdSql.error(self.apercentile_query_form(col="c3"))              # col-type: timestamp
        tdSql.error(self.apercentile_query_form(col="c4"))              # col-type: binary
        tdSql.error(self.apercentile_query_form(col="c6"))              # col-type: bool
        tdSql.error(self.apercentile_query_form(col="c10"))             # col-type: nchar
        tdSql.error(self.apercentile_query_form(p=True))                # p:bool
        tdSql.error(self.apercentile_query_form(p='a'))                 # p:str
        tdSql.error(self.apercentile_query_form(p='last(*)'))           # p:expr
        tdSql.error(self.apercentile_query_form(p="2021-08-01 00:00:00.000"))   # p:timestamp
        tdSql.error(self.apercentile_query_form(algo='t-digest'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='"t_digest"'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='"t-digest0"'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='"t-digest."'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='"t-digest%"'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='"t-digest*"'))    # algorithm:str
        tdSql.error(self.apercentile_query_form(algo='tdigest'))        # algorithm:str
        tdSql.error(self.apercentile_query_form(algo=2.0))              # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=1.9999))         # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=-0.9999))        # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=-1.0))             # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=0b1))            # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=0x1))            # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=0o1))            # algorithm:float
        tdSql.error(self.apercentile_query_form(algo=True))           # algorithm:bool
        tdSql.error(self.apercentile_query_form(algo="True"))         # algorithm:bool
        tdSql.error(self.apercentile_query_form(algo='2021-08-01 00:00:00.000'))     # algorithm:timestamp
        tdSql.error(self.apercentile_query_form(algo='last(c1)'))       # algorithm:expr

        # boundary test
        tdSql.error(self.apercentile_query_form(p=-1))                  # p left out of [0, 100]
        tdSql.error(self.apercentile_query_form(p=-9223372036854775809))    # p left out of bigint
        tdSql.error(self.apercentile_query_form(p=100.1))               # p right out of [0, 100]
        tdSql.error(self.apercentile_query_form(p=18446744073709551616))    # p right out of unsigned-bigint
        tdSql.error(self.apercentile_query_form(algo=-1))           # algorithm left out of [0, 1]
        tdSql.error(self.apercentile_query_form(algo=-9223372036854775809)) # algorithm left out of unsigned-bigint
        tdSql.error(self.apercentile_query_form(algo=2))            # algorithm right out of [0, 1]
        tdSql.error(self.apercentile_query_form(algo=18446744073709551616)) # algorithm right out of unsigned-bigint

        # mix function test
        tdSql.error(self.apercentile_query_form(alias=', top(c1,1)'))   # mix with top function
        tdSql.error(self.apercentile_query_form(alias=', top(c1,1)'))   # mix with bottom function
        tdSql.error(self.apercentile_query_form(alias=', last_row(c1)'))    # mix with last_row function
        tdSql.error(self.apercentile_query_form(alias=', distinct c1 '))    # mix with distinct function
        tdSql.error(self.apercentile_query_form(alias=', *'))           # mix with  *
        tdSql.error(self.apercentile_query_form(alias=', diff(c1)'))    # mix with  diff function
        tdSql.error(self.apercentile_query_form(alias=', interp(c1)', condition='ts="2021-10-10 00:00:00.000"'))    # mix with  interp function
        tdSql.error(self.apercentile_query_form(alias=', derivative(c1, 10m, 0)'))  # mix with derivative function
        tdSql.error(self.apercentile_query_form(alias=', diff(c1)'))    # mix with  diff function
        tdSql.error(self.apercentile_query_form(alias='+ c1)'))     # mix with  four operation

    def apercentile_data(self, tbnum, data_row, basetime):
        for i in range(tbnum):
            for j in range(data_row):
                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime + j*10}, {random.randint(-200, -1)}, {random.uniform(200, -1)}, {basetime + random.randint(-200, -1)}, "
                    f"'binary_{j}', {random.uniform(-200, -1)}, {random.choice([0,1])}, {random.randint(-200,-1)}, "
                    f"{random.randint(-200, -1)}, {random.randint(-127, -1)}, 'nchar_{j}' )"
                )

                tdSql.execute(
                    f"insert into t{i} values ("
                    f"{basetime - (j+1) * 10}, {random.randint(1, 200)}, {random.uniform(1, 200)}, {basetime - random.randint(1, 200)}, "
                    f"'binary_{j}_1', {random.uniform(1, 200)}, {random.choice([0, 1])}, {random.randint(1,200)}, "
                    f"{random.randint(1,200)}, {random.randint(1,127)}, 'nchar_{j}_1' )"
                )
                tdSql.execute(
                    f"insert into tt{i} values ( {basetime-(j+1) * 10}, {random.randint(1, 200)} )"
                )

        pass

    def td6108(self):
        tdLog.printNoPrefix("==========TD-6108==========")
        tdSql.execute("drop database if exists db")
        tdSql.execute("create database  if not exists db keep 3650")
        tdSql.execute("use db")

        tdSql.execute(
            "create stable db.stb1 (\
                ts timestamp, c1 int, c2 float, c3 timestamp, c4 binary(16), c5 double, c6 bool, \
                c7 bigint, c8 smallint, c9 tinyint, c10 nchar(16)\
                ) \
            tags(st1 int)"
        )
        tdSql.execute(
            "create stable db.stb2 (ts timestamp, c1 int) tags(st2 int)"
        )
        tbnum = 10
        for i in range(tbnum):
            tdSql.execute(f"create table t{i} using stb1 tags({i})")
            tdSql.execute(f"create table tt{i} using stb2 tags({i})")

        tdLog.printNoPrefix("######## no data test:")
        self.apercentile_query()
        self.error_apercentile()

        tdLog.printNoPrefix("######## insert data test:")
        nowtime = int(round(time.time() * 1000))
        per_table_rows = 1000
        self.apercentile_data(tbnum, per_table_rows, nowtime)
        self.apercentile_query()
        self.error_apercentile()

        tdLog.printNoPrefix("######## insert data with NULL test:")
        tdSql.execute(f"insert into t1(ts) values ({nowtime-5})")
        tdSql.execute(f"insert into t1(ts) values ({nowtime+5})")
        self.apercentile_query()
        self.error_apercentile()

        tdLog.printNoPrefix("######## check after WAL test:")
        tdSql.query("show dnodes")
        index = tdSql.getData(0, 0)
        tdDnodes.stop(index)
        tdDnodes.start(index)

        self.apercentile_query()
        self.error_apercentile()


    def run(self):

        self.td4097()

        # master branch
        self.td3690()
        # self.td4082()
        self.td4288()
        self.td4724()
        self.td5935()
        self.td6068()

        # self.td5168()
        # self.td5433()
        # self.td5798()

        # develop branch
        # self.td4889() In the scenario that with vnode/wal/wal* but without meta/data in vnode, the status is reset to 0 right now.
        self.td5798()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())



