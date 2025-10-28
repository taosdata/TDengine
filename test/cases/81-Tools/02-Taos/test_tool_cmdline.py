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

from new_test_framework.utils import tdLog, tdSql, sc, etool, eos
import time
import platform
import os


class TestFullopt:
    updatecfgDict = {
        'queryMaxConcurrentTables': '2K',
        'totalMemoryKB': '32000000',
        'slowLogScope':"query"
    }

    def insertData(self):
        tdLog.info(f"insert data.")

        # set insert data information
        self.childtable_count = 10
        self.insert_rows      = 10000
        self.timestamp_step   = 1000

        # taosBenchmark run
        etool.benchMark(command = f"-d {self.db} -t {self.childtable_count} -n {self.insert_rows} -v 2 -y")

    def doTaos(self):
        tdLog.info(f"check taos command options...")

        # local command
        options = [
                     "DebugFlag 143",
                     "fqdn 127.0.0.1",
                     "firstEp 127.0.0.1",
                     "metaCacheMaxSize 10000",
                     "minimalTmpDirGB 5",
                     "minimalLogDirGB 1",
                     "secondEp 127.0.0.2",
                     "smlChildTableName smltbname",
                     "smlAutoChildTableNameDelimiter autochild",
                     "smlTagName tagname",
                     "smlTsDefaultName tsdef",
                     "serverPort 6030",
                  ]
        # exec
        for option in options:
            rlist = self.taos(f"-s \"alter local '{option}'\"")
            self.checkListString(rlist, "Query OK,")
        # error
        etool.runBinFile("taos", f"-s \"alter local 'nocmd check'\"")

        # help
        rets = etool.runBinFile("taos", "--help")
        self.checkListNotEmpty(rets)
        # b r w s
        sql = f"select * from {self.db}.{self.stb} limit 10"
        rets = etool.runBinFile("taos", f'-B -r -w 100 -s "{sql}" ')
        self.checkListNotEmpty(rets)
        # -C
        rets = etool.runBinFile("taos", "-C")
        self.checkListNotEmpty(rets)
        # -t
        rets = etool.runBinFile("taos", "-t")
        self.checkListNotEmpty(rets)
        # -v
        rets = etool.runBinFile("taos", "-V")
        self.checkListNotEmpty(rets)
        # -?
        rets = etool.runBinFile("taos", "-?")
        self.checkListNotEmpty(rets)

        # TSDB_FQDN_LEN = 128
        lname = "testhostnamelength"
        lname.rjust(230, 'a')

        # except test
        sql = f"show vgroups;"
        etool.exeBinFile("taos", f'-h {lname} -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'-u {lname} -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'-d {lname} -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'-a {lname} -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'-p{lname}  -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'-w -s "{sql}" ', wait=False)
        etool.exeBinFile("taos", f'abc', wait=False)
        etool.exeBinFile("taos", f'-V', wait=False)
        etool.exeBinFile("taos", f'-?', wait=False)

        # others
        etool.exeBinFile("taos", f'-N 200 -l 2048 -s "{sql}" ', wait=False)


    def doTaosd(self):
        tdLog.info(f"check taosd command options...")
        idx = 1 # dnode1
        cfg = sc.dnodeCfgPath(idx)

        # -s
        sdb = "./sdb.json"
        eos.delFile(sdb)
        etool.exeBinFile("taosd", f"-s -c {cfg}")


        # -C
        etool.exeBinFile("taosd", "-C")
        # -k
        etool.exeBinFile("taosd", "-k", False)
        # -V
        rets = etool.runBinFile("taosd", "-V")
        self.checkListNotEmpty(rets)
        # --help
        rets = etool.runBinFile("taosd", "--help")
        self.checkListNotEmpty(rets)

        # except input
        etool.exeBinFile("taosd", "-c")
        etool.exeBinFile("taosd", "-e")

        # stop taosd
        sc.dnodeStop(idx)
        # other
        etool.exeBinFile("taosd", f"-dm -c {cfg}", False)
        sc.dnodeStop(idx)
        etool.exeBinFile("taosd", "-a http://192.168.1.10")

        #exe
        etool.exeBinFile("taosd", f"-E abc -c {cfg}", False)
        sc.dnodeStop(idx)
        etool.exeBinFile("taosd", f"-e def -c {cfg}", False)

        # stop taosd test taos as server
        sc.dnodeStop(idx)
        etool.exeBinFile("taos", f'-n server', wait=False)
        time.sleep(3)
        rlist = self.taos("-n client")
        self.checkListString(rlist, "total succ:  100/100")
        if platform.system().lower() == 'windows':
           os.system("taskkill /f /im taos.exe")
        else:
            eos.exe("pkill -9 taos")

        # call enter password
        etool.exeBinFile("taos", f'-p', wait=False)
        time.sleep(1)
        if platform.system().lower() == 'windows':
           os.system("taskkill /f /im taos.exe")
        else:
            eos.exe("pkill -9 taos")

    # run
    def test_tools_cmdline(self):
        """taos-CLI command line test
        
        1. Insert data with taosBenchmark json format
        2. Check taos-CLI all command lines
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cmdline/test_fullopt.py

        """
        tdLog.debug(f"start to excute {__file__}")
        # insert data
        self.insertData()
        # do taos
        self.doTaos()
        # do action
        self.doTaosd()
        tdLog.success(f"{__file__} successfully executed")

    def test_tools_cmdline_taosd(self):
        """taosd command line test
        
        1. Insert data with taosBenchmark json format
        2. Check taosd all command lines
        
        Catalog:
            - Components:Taosd

        Since: v3.0.0.0

        Labels: common,ci,ignore

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from uncatalog/army/cmdline/test_fullopt.py

        """
        pass

