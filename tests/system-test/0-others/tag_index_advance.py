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

import os
import random
import socket
import string
import subprocess
import sys
import time
from ssl import ALERT_DESCRIPTION_CERTIFICATE_UNOBTAINABLE

import taos
from util.cases import *
from util.common import *
from util.dnodes import *
from util.dnodes import TDDnode, TDDnodes
from util.log import *
from util.sql import *
from util.sqlset import *


#
# --------------    util   --------------------------
#
def pathSize(path):

    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for i in filenames:
            # use join to concatenate all the components of path
            f = os.path.join(dirpath, i)
            # use getsize to generate size in bytes and add it to the total size
            total_size += os.path.getsize(f)
            # print(dirpath)

    print(" %s  %.02f MB" % (path, total_size/1024/1024))
    return total_size

    '''
    total = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += pathSize(entry.path)
    
    print(" %s  %.02f MB"%(path, total/1024/1024))
    return total
    '''

#
# ---------------    cluster  ------------------------
#


class MyDnodes(TDDnodes):
    def __init__(self, dnodes_lists):
        super(MyDnodes, self).__init__()
        self.dnodes = dnodes_lists  # dnode must be TDDnode instance
        self.simDeployed = False


class TagCluster:
    noConn = True

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug(f"start to excute {__file__}")
        self.TDDnodes = None
        self.depoly_cluster(5)
        self.master_dnode = self.TDDnodes.dnodes[0]
        self.host = self.master_dnode.cfgDict["fqdn"]
        conn1 = taos.connect(
            self.master_dnode.cfgDict["fqdn"], config=self.master_dnode.cfgDir)
        tdSql.init(conn1.cursor())

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files or "taosd.exe" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def depoly_cluster(self, dnodes_nums):

        testCluster = False
        valgrind = 0
        hostname = socket.gethostname()
        dnodes = []
        start_port = 6030
        for num in range(1, dnodes_nums+1):
            dnode = TDDnode(num)
            dnode.addExtraCfg("firstEp", f"{hostname}:{start_port}")
            dnode.addExtraCfg("fqdn", f"{hostname}")
            dnode.addExtraCfg("serverPort", f"{start_port + (num-1)*100}")
            dnode.addExtraCfg("monitorFqdn", hostname)
            dnode.addExtraCfg("monitorPort", 7043)
            dnodes.append(dnode)

        self.TDDnodes = MyDnodes(dnodes)
        self.TDDnodes.init("")
        self.TDDnodes.setTestCluster(testCluster)
        self.TDDnodes.setValgrind(valgrind)

        self.TDDnodes.setAsan(tdDnodes.getAsan())
        self.TDDnodes.stopAll()
        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.deploy(dnode.index, {})

        for dnode in self.TDDnodes.dnodes:
            self.TDDnodes.starttaosd(dnode.index)

        # create cluster
        dnode_first_host = ""
        sql = ""
        for dnode in self.TDDnodes.dnodes[1:]:
            # print(dnode.cfgDict)
            dnode_id = dnode.cfgDict["fqdn"] + \
                ":" + dnode.cfgDict["serverPort"]
            if dnode_first_host == "":
                dnode_first_host = dnode.cfgDict["firstEp"].split(":")[0]
                dnode_first_port = dnode.cfgDict["firstEp"].split(":")[-1]
            sql += f"create dnode '{dnode_id}'; "

        cmd = f"{self.getBuildPath()}/build/bin/taos -h {dnode_first_host} -P {dnode_first_port} -s "
        cmd += f'"{sql}"'
        print(cmd)
        os.system(cmd)

        time.sleep(2)
        tdLog.info(" create cluster done! ")

    def getConnection(self, dnode):
        host = dnode.cfgDict["fqdn"]
        port = dnode.cfgDict["serverPort"]
        config_dir = dnode.cfgDir
        return taos.connect(host=host, port=int(port), config=config_dir)

    def run(self):
        tdLog.info(" create cluster ok.")

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")


class PerfDB:
    def __init__(self):
        self.sqls = []
        self.spends = []

    # execute
    def execute(self, sql):
        print(f"  perfdb execute {sql}")
        stime = time.time()
        ret = tdSql.execute(sql, 1)
        spend = time.time() - stime

        self.sqls.append(sql)
        self.spends.append(spend)
        return ret

    # query
    def query(self, sql):
        print(f"  perfdb query {sql}")
        start = time.time()
        ret = tdSql.query(sql, None, 1)
        spend = time.time() - start
        self.sqls.append(sql)
        self.spends.append(spend)
        return ret


#
#  -----------------  TDTestCase    ------------------
#
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        self.dbs = [PerfDB(), PerfDB()]
        self.cur = 0
        self.tagCluster = TagCluster()
        self.tagCluster.init(conn, logSql, replicaVar)
        self.lenBinary = 64
        self.lenNchar = 32

        # column
        self.column_dict = {
            'ts': 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': f'varchar({self.lenBinary})',
            'col13': f'nchar({self.lenNchar})'
        }
        # tag
        self.tag_dict = {
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': f'varchar({self.lenBinary})',
            't13': f'nchar({self.lenNchar})',
            't14': 'timestamp'
        }

    # generate specail wide random string
    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

    # execute
    def execute(self, sql):
        obj = self.dbs[self.cur]
        return obj.execute(sql)

    # query
    def query(self, sql):
        return self.dbs[self.cur].query(sql)

    def set_stb_sql(self, stbname, column_dict, tag_dict):
        column_sql = ''
        tag_sql = ''
        for k, v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k, v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql

    # create datbase
    def create_database(self, dbname, vgroups, replica):
        sql = f'create database {dbname} vgroups {vgroups} replica {replica}'
        tdSql.execute(sql)
        # tdSql.execute(sql)
        tdSql.execute(f'use {dbname}')

    # create stable and child tables
    def create_table(self, stbname, tbname, count):
        # create stable
        create_table_sql = self.set_stb_sql(
            stbname, self.column_dict, self.tag_dict)
        tdSql.execute(create_table_sql)

        # create child table
        tdLog.info(f" start create {count} child tables.")
        batchSql = ""
        batchSize = 5000
        for i in range(int(count/batchSize)):
            batchSql = "create table"
            for j in range(batchSize):
                ti = (i * batchSize + j) % 128
                binTxt = self.random_string(self.lenBinary)
                idx = i * batchSize + j
                tags = f'{ti},{ti},{idx},{idx},{ti},{ti},{idx},{idx},{idx}.000{idx},{idx}.000{idx},true,"{binTxt}","nch{idx}",now'
                sql = f'{tbname}{idx} using {stbname} tags({tags})'
                batchSql = batchSql + " " + sql
            tdSql.execute(batchSql)
            tdLog.info(f"  child table count = {i * batchSize}")

    # create stable and child tables

    def create_tagidx(self, stbname):
        cnt = -1
        for key in self.tag_dict.keys():
            # first tag have default index, so skip
            if cnt == -1:
                cnt = 0
                continue
            sql = f'create index idx_{key} on {stbname} ({key})'
            tdLog.info(f"  sql={sql}")
            tdSql.execute(sql)
            cnt += 1
        tdLog.info(f' create {cnt} tag indexs ok.')

     # insert to child table d1 data
    def insert_data(self, tbname):
        # d1 insert 3 rows
        for i in range(3):
            sql = f'insert into {tbname}1(ts,col1) values(now+{i}s,{i});'
            tdSql.execute(sql)
        # d20 insert 4
        for i in range(4):
            sql = f'insert into {tbname}20(ts,col1) values(now+{i}s,{i});'
            tdSql.execute(sql)

    # check show indexs
    def show_tagidx(self, dbname, stbname):
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="{dbname}"'
        tdSql.query(sql)
        rows = len(self.tag_dict.keys())-1
        tdSql.checkRows(rows)

        for i in range(rows):
            col_name = tdSql.getData(i, 1)
            idx_name = f'idx_{col_name}'
            tdSql.checkData(i, 0, idx_name)

        tdLog.info(f' show {rows} tag indexs ok.')

    # query with tag idx
    def query_tagidx(self, stbname):
        sql = f'select * from meters where t2=1'
        self.query(sql)
        tdSql.checkRows(3)

        sql = f'select * from meters where t2<10'
        self.query(sql)
        tdSql.checkRows(3)

        sql = f'select * from meters where t2>10'
        self.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where t3<30'
        self.query(sql)
        tdSql.checkRows(7)

        sql = f'select * from meters where t12="11"'
        tdSql.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where (t4 < 10 or t5 = 20) and t6= 30'
        self.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where (t7 < 20 and t8 = 20) or t4 = 20'
        self.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where t12 like "%ab%" '
        self.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where t13 = "d20" '
        self.query(sql)
        tdSql.checkRows(0)

        sql = f'select * from meters where t13 = "nch20" '
        self.query(sql)
        tdSql.checkRows(4)

        sql = f'select * from meters where tbname = "d20" '
        self.query(sql)
        tdSql.checkRows(4)

    # drop child table

    def drop_tables(self, tbname, count):
        # table d1 and d20 have verify data , so can not drop
        start = random.randint(21, count/2)
        end = random.randint(count/2 + 1, count - 1)
        for i in range(start, end):
            sql = f'drop table {tbname}{i}'
            tdSql.execute(sql)
        cnt = end - start + 1
        tdLog.info(f' drop table from {start} to {end} count={cnt}')

    # drop tag index
    def drop_tagidx(self, dbname, stbname):
        # drop index
        cnt = -1
        for key in self.tag_dict.keys():
            # first tag have default index, so skip
            if cnt == -1:
                cnt = 0
                continue
            sql = f'drop index idx_{key}'
            tdSql.execute(sql)
            cnt += 1

        # check idx result is 0
        sql = f'select index_name,column_name from information_schema.ins_indexes where db_name="{dbname}"'
        tdSql.query(sql)
        tdSql.checkRows(0)
        tdLog.info(f' drop {cnt} tag indexs ok.')

    # show performance
    def show_performance(self, count):
        db = self.dbs[0]
        db1 = self.dbs[1]
        cnt = len(db.sqls)
        cnt1 = len(db1.sqls)
        if cnt != len(db1.sqls):
            tdLog.info(
                f"  datebase sql count not equal. cnt={cnt}  cnt1={cnt1}\n")
            return False

        tdLog.info(f" database sql cnt ={cnt}")
        print(
            f"  ----------------- performance (child tables = {count})--------------------")
        print("   No  time(index) time(no-index) diff(col3-col2) rate(col2/col3)  sql")
        for i in range(cnt):
            key = db.sqls[i]
            value = db.spends[i]
            key1 = db1.sqls[i]
            value1 = db1.spends[i]
            diff = value1 - value
            rate = value/value1*100
            print("   %d   %.3fs      %.3fs          %.3fs         %d%%          %s" % (
                i+1, value, value1, diff, rate, key))
        print("  --------------------- end ------------------------")
        return True

    def show_diskspace(self):
        # calc
        selfPath = os.path.dirname(os.path.realpath(__file__))
        projPath = ""
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        # total
        vnode2_size = pathSize(projPath + "sim/dnode2/data/vnode/vnode2/")
        vnode3_size = pathSize(projPath + "sim/dnode3/data/vnode/vnode3/")
        vnode4_size = pathSize(projPath + "sim/dnode4/data/vnode/vnode4/")
        vnode5_size = pathSize(projPath + "sim/dnode5/data/vnode/vnode5/")

        # show
        print("  ----------------- disk space --------------------")
        idx_size = vnode2_size + vnode3_size
        noidx_size = vnode4_size + vnode5_size

        print("  index    = %.02f M" % (idx_size/1024/1024))
        print("  no-index = %.02f M" % (noidx_size/1024/1024))
        print("  index/no-index = %.2f multiple" % (idx_size/noidx_size))

        print("  -------------------- end ------------------------")

    # main

    def testdb(self, dbname, stable, tbname, count, createidx):
        # cur
        if createidx:
            self.cur = 0
        else:
            self.cur = 1

        # do
        self.create_database(dbname, 2, 1)
        self.create_table(stable, tbname, count)
        if (createidx):
            self.create_tagidx(stable)
        self.insert_data(tbname)
        if (createidx):
            self.show_tagidx(dbname, stable)
        self.query_tagidx(stable)
        # self.drop_tables(tbname, count)
        # if(createidx):
        #   self.drop_tagidx(dbname, stable)
        # query after delete , expect no crash
        # self.query_tagidx(stable)
        tdSql.execute(f'flush database {dbname}')

    # run
    def run(self):
        self.tagCluster.run()

        # var
        dbname = "tagindex"
        dbname1 = dbname + "1"
        stable = "meters"
        tbname = "d"
        count = 10000

        # test db
        tdLog.info(f" -------------  {dbname} ----------")
        self.testdb(dbname, stable, tbname, count, True)
        tdLog.info(f" -------------  {dbname1} ----------")
        self.testdb(dbname1, stable, tbname, count, False)

        # show test result
        self.show_performance(count)

        self.tagCluster.TDDnodes.stopAll()
        sec = 10
        print(f" sleep {sec}s wait taosd stopping ...")
        time.sleep(sec)

        self.show_diskspace()

    def stop(self):
        self.tagCluster.stop()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
