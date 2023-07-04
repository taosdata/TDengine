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
import random
import time
import copy
import string
import threading
import platform
import subprocess
import os

from taostest import TDCase, T, runner

needQuit = False

class dnode():
    def __init__(self, pid, path):
        self.pid  = pid
        self.path = path

# run exePath no wait finished
def runNoWait(exePath):
    if platform.system().lower() == 'windows':
        cmd = f"mintty -h never {exePath}"
    else:
        cmd = f"nohup {exePath} > /dev/null 2>&1 & "
    if os.system(cmd) != 0:
        return False
    else:
        return True

# get online dnodes
def getDnodes():
    cmd = "ps aux | grep taosd | awk '{{print $2,$11,$12,$13}}'"
    result = subprocess.check_output(cmd,shell=True)
    strout = result.decode('utf-8').split("\n")
    dnodes = []

    for line in strout:
        cols = line.split(' ')
        if len(cols) != 4:
            continue
        exepath = cols[1]
        if len(exepath) < 5 :
            continue
        if exepath[-5:] != 'taosd':
            continue

        # add to list
        path = cols[1] + " " + cols[2] + " " + cols[3]
        dnodes.append(dnode(cols[0], path))

    print(" show dnodes cnt=%d...\n"%(len(dnodes)))
    for dn in dnodes:
        print(f"  pid={dn.pid} path={dn.path}")

    return dnodes

def restartDnodes(dnodes, cnt, seconds):
    print(f"start dnode cnt={cnt} wait={seconds}s")
    selects = random.sample(dnodes, cnt)
    for select in selects:
        print(f" kill -9 {select.pid}")
        cmd = f"kill -9 {select.pid}"
        os.system(cmd)
        print(f" restart {select.path}")
        if runNoWait(select.path) == False:
            print(f"run {select.path} failed.")
            raise Exception("exe failed.")
        print(f" sleep {seconds}s ...")
        time.sleep(seconds)


# restart thread
def restartThread():
    print("start restart thread...")
    # kill seconds interval
    global needQuit
    killLoop = 5
    minKill = 1
    maxKill = 2
    i = 0
    while needQuit == False and i < killLoop :
        i += 1
        dnodes = getDnodes()
        killCnt = 0
        if len(dnodes) > 0:
            killCnt = random.randint(1, len(dnodes))
            restartDnodes(dnodes, killCnt, random.randint(1, 5))

        seconds = random.randint(minKill, maxKill)
        print(f"----------- kill loop i={i} killCnt={killCnt} done. do sleep {seconds}s ... \n")
        time.sleep(seconds)

def throwAndExit(strlog):
    global needQuit
    needQuit = True
    print(" throw error and exit(1)")
    raise AssertionError(strlog)
    exit(1)



class SplitVGroup(TDCase):
    # init
    def init(self):
        seed = time.clock_gettime(time.CLOCK_REALTIME)
        random.seed(seed)

    def cleanup(self) -> None:
        pass

    def desc(self) -> str:
        return "Split VGroup Case"

    def author(self) -> str:
        return "AlexDuan"

    def tags(self):
        return T.Management

    def get_report(self, start_time, stop_time) -> str:
        return ""

    # random string
    def random_string(self, count):
        letters = string.ascii_letters
        return ''.join(random.choice(letters) for i in range(count))

    # get col value and total max min ...
    def getColsValue(self, i, j):
        # c1 value
        if random.randint(1, 10) == 5:
            c1 = None
        else:
            c1 = 1

        # c2 value
        if j % 3200 == 0:
            c2 = 8764231
        elif random.randint(1, 10) == 5:
            c2 = None
        else:
            c2 = random.randint(-87654297, 98765321)    


        value = f"({self.ts}, "

        # c1
        if c1 is None:
            value += "null,"
        else:
            self.c1Cnt += 1
            value += f"{c1},"
        # c2
        if c2 is None:
            value += "null,"
        else:
            value += f"{c2},"
            # total count
            self.c2Cnt += 1
            # max
            if self.c2Max is None:
                self.c2Max = c2
            else:
                if c2 > self.c2Max:
                    self.c2Max = c2
            # min
            if self.c2Min is None:
                self.c2Min = c2
            else:
                if c2 < self.c2Min:
                    self.c2Min = c2
            # sum
            if self.c2Sum is None:
                self.c2Sum = c2
            else:
                self.c2Sum += c2

        # c3 same with ts
        value += f"{self.ts})"
        
        # move next
        self.ts += 1

        return value

    # insert data
    def insertData(self):
        self.logger.info("insert data ....")
        sqls = ""
        for i in range(self.childCnt):
            # insert child table
            values = ""
            pre_insert = f"insert into @db_name.t{i} values "
            for j in range(self.childRow):
                if values == "":
                    values = self.getColsValue(i, j)
                else:
                    values += "," + self.getColsValue(i, j)

                # batch insert    
                if j % self.batchSize == 0  and values != "":
                    sql = pre_insert + values
                    self.exeDouble(sql)
                    values = ""
            # append last
            if values != "":
                sql = pre_insert + values
                self.exeDouble(sql)
                values = ""

        # insert nomal talbe
        for i in range(20):
            self.ts += 1000
            name = self.random_string(20)
            sql = f"insert into @db_name.ta values({self.ts}, {i}, {self.ts%100000}, '{name}', false)"
            self.exeDouble(sql)

        # insert finished
        self.logger.info(f"insert data successfully.\n"
        f"                            inserted child table = {self.childCnt}\n"
        f"                            inserted child rows  = {self.childRow}\n"
        f"                            total inserted rows  = {self.childCnt*self.childRow}\n")
        return
    
    def exeDouble(self, sql):
        # dbname replace
        sql1 = sql.replace("@db_name", self.db1)

        if len(sql1) > 100:
            self.logger.info(sql1[:100])
        else:
            self.logger.info(sql1)
        self.tdSql.execute(sql1)

        sql2 = sql.replace("@db_name", self.db2)
        if len(sql1) > 100:
            self.logger.info(sql1[:100])
        else:
            self.logger.info(sql1)
        self.tdSql.execute(sql2)
        

    # prepareEnv
    def prepareEnv(self):
        # init                
        self.ts = 1680000000000
        self.childCnt = 10000000
        self.childRow = 10000
        self.batchSize = 5000
        self.vgroups1  = 20
        self.vgroups2  = 20
        self.db1 = "db1"
        self.db2 = "db2"
        
        # total
        self.c1Cnt = 0
        self.c2Cnt = 0
        self.c2Max = None
        self.c2Min = None
        self.c2Sum = None

        # drop database
        sql = "drop database if exists @db_name"
        self.exeDouble(sql)


        # create database  db
        sql = f"create database @db_name vgroups {self.vgroups1} replica 3"
        self.exeDouble(sql)

        # create super talbe st
        sql = f"create table @db_name.st(ts timestamp, c1 int, c2 bigint, ts1 timestamp) tags(area int)"
        self.exeDouble(sql)

        # create child table
        for i in range(self.childCnt):
            sql = f"create table @db_name.t{i} using @db_name.st tags({i}) "
            self.exeDouble(sql)

        # create normal table
        sql = f"create table @db_name.ta(ts timestamp, c1 int, c2 bigint, c3 binary(32), c4 bool)"
        self.exeDouble(sql)

        # insert data
        self.insertData()

    # check query result same
    def queryDouble(self, sql):
        # sql
        sql1 = sql.replace('@db_name', self.db1)
        self.logger.info(sql1)
        start1 = time.time()
        rows1 = self.tdSql.query(sql1)
        spend1 = time.time() - start1
        res1 = copy.copy(self.tdSql.query_data)

        sql2 = sql.replace('@db_name', self.db2)
        self.logger.info(sql2)
        start2 = time.time()
        self.tdSql.query(sql2)
        spend2 = time.time() - start2
        res2 = self.tdSql.query_data

        rowlen1 = len(res1)
        rowlen2 = len(res2)

        if rowlen1 != rowlen2:
            throwAndExit(f"rowlen1={rowlen1} rowlen2={rowlen2} both not equal.")
        
        for i in range(rowlen1):
            row1 = res1[i]
            row2 = res2[i]
            collen1 = len(row1)
            collen2 = len(row2)
            if collen1 != collen2:
                throwAndExit(f"collen1={collen1} collen2={collen2} both not equal.")
            for j in range(collen1):
                if row1[j] != row2[j]:
                    throwAndExit(f"col={j} col1={row1[j]} col2={row2[j]} both col not equal.")
                    

        # warning performance
        diff = (spend2 - spend1)*100/spend1
        self.logger.info("spend1=%.6fs spend2=%.6fs diff=%.1f%%"%(spend1, spend2, diff))
        if spend2 > spend1 and diff > 20:
            self.logger.info("warning: the diff for performance after spliting is over 20%")

        return True


    # check result
    def checkResult(self):
        # check vgroupid
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{self.db2}'"
        self.tdSql.query(sql)
        self.tdSql.checkRow(self.vgroups2)

        # check child table count same
        sql = "select table_name from information_schema.ins_tables where db_name='@db_name' order by table_name"
        self.queryDouble(sql)

        # check row value is ok
        sql = "select * from @db_name.st order by ts limit 100000"
        self.queryDouble(sql)

        # check row value is ok
        sql = "select count(*) from @db_name.st"
        self.queryDouble(sql)

        # where
        sql = "select *,tbname from @db_name.st where c1 < 1000 order by ts limit 100000"
        self.queryDouble(sql)

        # max
        sql = "select max(c1) from @db_name.st"
        self.queryDouble(sql)

        # min
        sql = "select min(c2) from @db_name.st"
        self.queryDouble(sql)

        # sum
        sql = "select sum(c1) from @db_name.st"
        self.queryDouble(sql)

        # normal table

        # all rows
        sql = "select * from @db_name.ta"
        self.queryDouble(sql)

        # count
        sql = "select count(*) from @db_name.ta"
        self.queryDouble(sql)

        # sum
        sql = "select sum(c1) from @db_name.ta"
        self.queryDouble(sql)


    # get vgroup list
    def getVGroup(self, db_name):
        vgidList = []
        sql = f"select vgroup_id from information_schema.ins_vgroups where db_name='{db_name}'"
        self.tdSql.query(sql)
        res = self.tdSql.query_data
        rows = len(res)
        for i in range(rows):
            vgidList.append(res[i][0])

        return vgidList;        

    # split vgroup on db2
    def splitVGroup(self, db_name):
        vgids = self.getVGroup(db_name)
        selid = random.choice(vgids)
        sql = f"split vgroup {selid}"
        self.logger.info(sql)
        self.tdSql.execute(sql)

        # wait end
        for i in range(1000):
            sql ="show transactions;"
            self.tdSql.query(sql)
            if self.tdSql.query_row == 0:
                self.logger.info("split vgroup finished.")
                return True
            #self.logger.info(f"i={i} wait split vgroup ...")
            time.sleep(1)

        throwAndExit("split vgroup transaction is not finished after executing 50s")

    # split empty database
    def splitEmptyDB(self):
        
        dbName = "emptydb"
        vgNum = 2
        # create database
        sql = f"create database {dbName} vgroups {vgNum}"
        self.logger.info(sql)
        self.tdSql.execute(sql)

        # split vgroup
        self.splitVGroup(dbName)
        vgList = self.getVGroup(dbName)
        vgNum1 = len(vgList)
        vgNum2 = vgNum + 1
        if vgNum1 != vgNum2:
            throwAndExit(f" vglist len={vgNum1} is not same for expect {vgNum2}")

    # run
    def run(self):
    
        # prepare env
        self.prepareEnv()

        # restart thread to restart taosd
        restartT = threading.Thread(target=restartThread)
        restartT.start()
        self.logger.info("restart thread ")

        for i in range(10):
            # split vgroup on db2
            self.splitVGroup(self.db2)
            self.vgroups2 += 1

            # check two db query result same
            self.checkResult()

            self.logger.info(f"split vgroup i={i} passed.")

        # split empty db
        self.splitEmptyDB()
        global needQuit
        needQuit = True
        self.logger.info("all exit.")
