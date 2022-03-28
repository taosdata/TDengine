# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        
        tdLog.info('======================== dnode1 start')
        tbPrefix = "ta_cr_tb"
        mtPrefix = "ta_cr_mt"
        tbNum = 2 
        rowNum = 10 
        totalNum = 200
        tagCondsLimit = 1024
        tdLog.info('=============== step1: create tbl and prepare data')
        i = 0
        i = 2
        mt = "%s%d" % (mtPrefix, i)
        tb = "%s%d" % (tbPrefix, i)
         
        sql ='create table %s (ts timestamp, tbcol int) TAGS(tgcol int)'% (mt)
        tdLog.info(sql)
        tdSql.execute(sql)
        for i in range(0, tbNum): 
          tblName = "%s%d"%(tbPrefix, i)
          sql = 'create table %s using %s TAGS(%d)'%(tblName, mt, i) 
          tdSql.execute(sql)
          for j in range(0, rowNum):
            sql = "insert into %s values(now, %d)"%(tblName, j) 
            tdSql.execute(sql)

        sqlPrefix = "select * from %s where "%(mt)    
        for i in range(2, 2048, 1):
          conds = "tgcol=1 and "* (i - 1)
          conds = "%stgcol=1"%(conds)
          sql = "%s%s"%(sqlPrefix, conds)
          if i >= tagCondsLimit: 
            tdSql.error(sql)
          else:   
            tdSql.query(sql)
            #tdSql.checkRows(1)

        for i in range(2, 2048, 1):
          conds = "" 
          for j in range(0, i - 1): 
            conds = conds + "tgcol=%d or "%(j%tbNum)
          conds += "tgcol=%d"%(i%tbNum)
          sql = sqlPrefix + conds 
          if i >= tagCondsLimit:
            tdSql.error(sql)
          else:  
            tdSql.query(sql)
        

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
