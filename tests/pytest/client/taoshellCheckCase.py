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

import sys, shutil
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import subprocess


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
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

    def execute_cmd(self,cmd):
        out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE).stderr.read().decode("utf-8")
        if out.find("error:") >=0:
            print(cmd)
            print(out)
            sys.exit()
        
            

    def run(self):
        tdSql.prepare()
        build_path = self.getBuildPath() + "/debug/build/bin"
        tdLog.info("====== check tables use taos -d  -k  ========")

        tdSql.execute("drop database if exists test")
        tdSql.execute("drop database if exists dumptest")
        tdSql.execute("create database if not exists test")
        tdLog.info("====== only create database test ==== ")
        self.execute_cmd(build_path + "/" + "taos -d test -k 1  > res.txt 2>&1")

        tdSql.execute("use test")
        tdSql.execute("create stable st (ts timestamp , id int , val double , str binary(20) ) tags (ind int)")
        tdSql.execute("create table tb1 using st tags(1)")
        tdLog.info("======= only create one table ==========")
        self.execute_cmd(build_path + "/" + "taos -d test -k 1  > res.txt 2>&1")

        tdSql.execute("create table tb2 using st tags(2)")
        tdSql.execute("create table tb3 using st tags(3)")
        tdLog.info("======= only create three table =======")
        self.execute_cmd(build_path + "/" + "taos -d test -k 1  > res.txt 2>&1")

        tdSql.execute("create table tb4 using st tags(4)")
        tdSql.execute("create table tb5 using st tags(5)")
        tdLog.info("======= only create five table =======")
        self.execute_cmd(build_path + "/" + "taos -d test -k 1  > res.txt 2>&1")

        start_time = 1604298064000
        rows = 10
        tb_nums = 5
        tdLog.info("====== start insert rows ========")

        for i in range(1, tb_nums + 1):
            for j in range(rows):
                start_time += 10
                tdSql.execute(
                    "insert into tb%d values(%d, %d,%f,%s) " % (i, start_time, j, float(j), "'str" + str(j) + "'"))
        tdSql.query("select count(*) from st")
        tdSql.checkData(0, 0, 50)

        for i in range(1, tb_nums + 1):
            tdSql.execute("select * from test.tb%s" % (str(i)))

        tdLog.info("====== check taos -D filedir  ========")

        if not os.path.exists("./dumpdata"):
            os.mkdir("./dumpdata")
        else:
            shutil.rmtree("./dumpdata")
            os.mkdir("./dumpdata")

        os.system(build_path + "/" + "taosdump -D test -o ./dumpdata")
        sleep(2)
        os.system("cd ./dumpdata && mv dbs.sql tables.sql")
        os.system('sed -i "s/test/dumptest/g" `grep test -rl ./dumpdata`')
        os.system(build_path + "/" + "taos -D ./dumpdata")
        tdSql.query("select count(*) from dumptest.st")
        tdSql.checkData(0, 0, 50)

        tdLog.info("========test other file name about tables.sql========")
        os.system("rm -rf ./dumpdata/*")
        os.system(build_path + "/" + "taosdump -D test -o ./dumpdata")
        sleep(2)
        os.system("cd ./dumpdata && mv dbs.sql table.sql")
        os.system('sed -i "s/test/tt/g" `grep test -rl ./dumpdata`')
        cmd = build_path + "/" + "taos -D ./dumpdata"
        out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE).stderr.read().decode("utf-8")
        if out.find("error:") >=0:
            print("===========expected error occured======")
        
            
        tdLog.info("====== check taos shell params  ========")

        tdLog.info("====== step 1 : insert data with some unicode ========")

        sqls = ["drop database if exists dbst",
                "create database dbst",
                "use dbst",
                "create stable dbst.st (ts timestamp , id int , val double , str binary(200) ,char nchar(200) ) tags (ind int)",
                "create table dbst.tb1 using dbst.st tags(1)",
                "create table dbst.tb2 using dbst.st tags(2)",
                "insert into dbst.tb1 values('2021-07-14T10:40:00.006+0800' , 1 , 1.0 , 'binary_1','中文-1') ",
                "insert into dbst.tb1 values('2021-07-14T10:40:00.006Z' , 1 , 1.0 , 'binary\\'1','中文?-1')",
                "insert into dbst.tb1 values('2021-07-14 10:40:00.000',1,1.0,'！@#¥%……&*', '中文12&%#@!*')",
                "insert into dbst.tb1 values(now ,1,1.0,'(){}[];./?&*\n', '中文&%#@!*34')",
                "insert into dbst.tb1 values(now ,1,1.0,'\\t\\0', '中文_\\t\\0')",
                # "insert into dbst.tb1 values(now ,1,1.0,'\t\"', '中文_\t\\')", 
                "CREATE STABLE dbst.stb (TS TIMESTAMP , ID INT , VAL DOUBLE , STR BINARY(200) ,CHAR NCHAR(200) ) TAGS (IND INT)",
                "CREATE TABLE dbst.tbb1 USING dbst.STB TAGS(1)",
                "CREATE TABLE dbst.tbb2 USING dbst.STB TAGS(2)",
                "INSERT INTO dbst.TBB1 VALUES('2021-07-14T10:40:00.006+0800' , 1 , 1.0 , 'BINARY_1','中文-1')",
                "INSERT INTO dbst.TBB1 VALUES('2021-07-14T10:40:00.006Z' , 1 , 1.0 , 'BINARY1','中文?-1')",
                "INSERT INTO dbst.TBB1 VALUES('2021-07-14 10:40:00.000',1,1.0,'！@#¥%……&*', '中文12&%#@!*');"]
        for sql in sqls:
            cmd = build_path + "/" + "taos -s \""+sql+"\"" 
            self.execute_cmd(cmd)        
            
        basic_code = ['!' ,'#', '$', '%', '&', '(', ')', '*', '+', ',', '-', '.', '/', '0', '1',
                      '2', '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A',
                     'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M','N', 'O', 'P', 'Q', 'R',
                     'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '[', '\\',']' ,'^', '_', '`', 'a', 'b', 'c',
                    'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r','s', 't', 'u',
                     'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~']
        for code in basic_code:
            # bug -> : this is a bug need be repaired to support '`'  and '\'
            if code=='\\':
                cmd =  build_path + "/" + "taos -s \" insert into dbst.tb2 values(now ,2,2.0," +r'"\\"'+",'中文"+r'\\'+ "')\""
                continue   
            elif code =='`': 
                cmd = build_path + "/" + "taos -s \" insert into dbst.tb2 values(now ,2,2.0,'"+code+"','汉字"+code+"\')\""
                continue
            else:
                cmd = build_path + "/" + "taos -s \" insert into dbst.tb2 values(now ,2,2.0,'"+code+"','汉字"+code+"\')\""
                
            self.execute_cmd(cmd)
            
            
        tdLog.info("====== step 2 : query result of results ========")

        querys = ["select count(*) from dbst.tb2",
        "show dbst.tables",
        "show dbst.tables like tb_",
        "show dbst.tables like 't%'",
        "select * from dbst.stb",
        "select avg(val),max(id),min(id) from dbst.st ",
        "select last_row(*) from dbst.st",
        "select * from dbst.st where ts >'2021-07-14T10:40:00.006+0800' and ind = 1 ",
        "select max(val) from dbst.st where ts >'2021-07-14T10:40:00.006+0800' group by tbname",
        "select count(*) from dbst.st interval(1s) group by tbname",
        "show queries ",
        "show connections",
        "show functions",
        "select * from dbst.tb2 where str like 'a'",
        "select bottom(id, 3) from dbst.st; ",
        "select _block_dist() from dbst.st;",
        "select 5 from dbst.tb1;",
        "select id , val from dbst.st",
        "describe dbst.st",
        "alter stable dbst.st modify column str binary(205);" ]

        for query in querys:
            cmd = build_path + "/" + "taos -s \""+query+"\""
            self.execute_cmd(cmd)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
