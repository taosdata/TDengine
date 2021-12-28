###################################################################
#           Copyright (c) 2021 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys,os
import time
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
from util.types import TDSmlProtocolType, TDSmlTimestampType

class TDTestCase():
    updatecfgDict = {"smlTagNullName","setname"}  # add extra client params

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)
        self._conn = conn

    def caseDescription(self):

        '''
        case1 <wenzhouwww>: [TD-11436] : this is an test case for line proto no tag insert into TDengine .
        '''
        return

    def getBuildPath(self, tool="taosd"):
        buildPath = ""
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ((tool) in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath

    def no_tag_single_line_insert(self,name):
        self.name = name 

        lines3 = [  "sti c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
            "sti c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"
        ]
 
        code = self._conn.schemaless_insert(lines3, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        tdSql.query('select * from sti')
        tdSql.checkRows(2)
        
        tdSql.query('select tbname from sti')
        tdSql.checkRows(1)

        col_names = tdSql.getResult("describe sti")
        if col_names[-1][0]==self.name:
            tdLog.info(" ====================get expected tag name ===============")
        else:
            tdLog.exit("======================error occured for null tag==================")
    
    def no_tag_mulit_line_insert(self,name):

        lines3 = [  "sti c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
            "sti c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000"
        ]
 
        code = self._conn.schemaless_insert(lines3, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        tdSql.query('select * from sti')
        tdSql.checkRows(2)
        
        tdSql.query('select tbname from sti')
        tdSql.checkRows(1)

        col_names = tdSql.getResult("describe sti")
        if col_names[-1][0]==self.name:
            tdLog.info(" ====================get expected tag name ===============")
        else:
            tdLog.exit("======================error occured for null tag==================")
        


    def part_tag_single_insert(self,name):
        lines5 = [  "sti,t3=1 c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639050000",
            "sti,t1=abc c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640050000",
            "sti,t2=abc c1=3i64,c3=L\"passitagin\",c4=5f64,c5=5f64,c6=true 1626006833640050000"
        ]
        code = self._conn.schemaless_insert([ lines5[0] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        code = self._conn.schemaless_insert([ lines5[1] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        code = self._conn.schemaless_insert([ lines5[2] ], TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        tdSql.query('select * from sti')
        tdSql.checkRows(5)
        tdSql.checkData(4,3,None)
        tdSql.checkData(4,6,True)
        tdSql.checkData(2,8,"1")
        tdSql.checkData(3,9,"abc")
        
        tdSql.query('select tbname from sti')
        tdSql.checkRows(4)

        col_names = tdSql.getResult("describe sti")
        
        if col_names[-4][0]==self.name and col_names[-3][0]=="t3" and col_names[-2][0]=="t1" and col_names[-1][0]=="t2": 
            tdLog.info(" ====================get expected tag name ===============")
        else:
            tdLog.exit("======================error occured for null tag==================")
        

    def part_tag_multi_insert(self,name):

        lines6 = [  "str c1=4i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
            "str,t1=abc c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
            "str,t2=abc c1=3i64,c3=L\"passitagin\",c4=5f64,c5=5f64,c6=true 1626006833640000000"                                                                                                                                                      
        ]
        code = self._conn.schemaless_insert(lines6, TDSmlProtocolType.LINE.value, TDSmlTimestampType.NANO_SECOND.value)
        print("schemaless_insert result {}".format(code))
        
        tdSql.query('select * from str')
        tdSql.checkRows(3)
        tdSql.checkData(0,3,True)
        tdSql.checkData(1,3,None)
        tdSql.checkData(1,6,True)
        tdSql.checkData(0,8,"abc")
        tdSql.checkData(1,9,"abc")
        
        tdSql.query('select tbname from str')
        tdSql.checkRows(3)

        col_names = tdSql.getResult("describe str")
        
        if col_names[-3][0]==self.name and col_names[-2][0]=="t1" and col_names[-1][0]=="t2" : 
            tdLog.info(" ====================get expected tag name ===============")
        else:
            tdLog.exit("======================error occured for null tag==================")
        

    def run(self):
        print("running {}".format(__file__))
        tdSql.execute("drop database if exists test")
        tdSql.execute("create database if not exists test precision 'us'")
        tdSql.execute('use test')
        self.no_tag_single_line_insert("_tag_null")
        self.no_tag_mulit_line_insert("_tag_null")
        self.part_tag_single_insert("_tag_null")
        self.part_tag_multi_insert("_tag_null")

       

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
