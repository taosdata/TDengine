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
import datetime

from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        tdSql.prepare()
        # test case for https://jira.taosdata.com:18080/browse/TD-4541

        tdLog.info("=============== step1,check normal table")    

        tdLog.info("=============== step1.1,drop table && create table")    
        cmd1 = 'drop table if exists length11 ;'
        cmd2 = 'create table length11 (ts timestamp,lengthbia binary(10),lengthnchar nchar(20));'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)

        tdLog.info("=============== step1.2,insert table right data") 
        cmd1 = 'insert into length11 values(now,\'aaaaaaaaaa\',\'bbbbbbbbbbbbbbbbbbbb\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from length11 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')

        tdLog.info("=============== step1.3,insert table wrong data") 
        cmd1 = 'insert into length11 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("string data overflow")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")      
        tdSql.query('select * from length11 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')

        tdLog.info("=============== step1.4,modify columu length ") 
        cmd1 = 'alter table length11 modify column lengthbia binary(10) ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one")   

        cmd2 = 'alter table length11 modify column lengthnchar nchar(20);'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one") 

        cmd3 = 'alter table length11 modify column lengthbia binary(11) ;'
        cmd4 = 'describe length11 ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3) 
        tdLog.info(cmd4)
        tdSql.execute(cmd4) 
        tdSql.query('describe length11 ;')
        tdSql.checkData(1,2,11) 

        cmd5 = 'alter table length11 modify column lengthnchar nchar(21);'
        cmd6 = 'describe length11 ;'
        tdLog.info(cmd5)
        tdSql.execute(cmd5) 
        tdLog.info(cmd6)
        tdSql.execute(cmd6) 
        tdSql.query('describe length11 ;')
        tdSql.checkData(2,2,21) 

        tdSql.query('select * from length11 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')


        tdLog.info("=============== step1.5,insert table right data") 
        cmd1 = 'insert into length11 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd1)    
        tdSql.execute(cmd1)  
        tdSql.query('select * from length11 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')




        tdLog.info("=============== step2,check stable table and tag")    

        tdLog.info("=============== step2.1,drop table && create table")    
        cmd1 = 'drop table if exists length1 ;'
        cmd2 = 'drop table if exists length2 ;'
        cmd3 = 'drop table if exists length2 ;'
        cmd4 = 'drop table if exists lengthsta1 ;'
        cmd5 = 'create stable lengthsta1(ts timestamp,lengthbia binary(10),lengthnchar nchar(20)) tags (tlengthbia binary(15),tlengthnchar nchar(25)) ;'
        cmd6 = 'create table length1 using lengthsta1 tags(\'aaaaabbbbbaaaaa\',\'bbbbbaaaaabbbbbaaaaabbbbb\') ; '
        tdLog.info(cmd1)
        tdSql.execute(cmd1)
        tdLog.info(cmd2)
        tdSql.execute(cmd2)
        tdLog.info(cmd3)
        tdSql.execute(cmd3)
        tdLog.info(cmd4)
        tdSql.execute(cmd4)
        tdLog.info(cmd5)
        tdSql.execute(cmd5)
        tdLog.info(cmd6)
        tdSql.execute(cmd6)

        tdLog.info("=============== step2.2,insert table right data") 
        cmd1 = 'insert into length1 values(now,\'aaaaaaaaaa\',\'bbbbbbbbbbbbbbbbbbbb\') ;'
        tdLog.info(cmd1)
        tdSql.execute(cmd1)         
        tdSql.query('select * from length1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')

        tdLog.info("=============== step2.3,insert table wrong data") 
        cmd1 = 'insert into length1 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("string data overflow")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("insert wrong data error catched")      
        tdSql.query('select * from length1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')

        tdLog.info("=============== step2.4,modify columu length ") 
        cmd0 = 'alter table length1 modify column lengthbia binary(10) ;'
        tdLog.info(cmd0)
        tdSql.error(cmd0)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("invalid operation: column can only be modified by super table")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: column can only be modified by super table")  

        cmd1 = 'alter table lengthsta1 modify column lengthbia binary(10) ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one")   

        cmd2 = 'alter table lengthsta1 modify column lengthnchar nchar(20);'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one") 

        cmd3 = 'alter table lengthsta1 modify column lengthbia binary(11) ;'
        cmd4 = 'describe lengthsta1 ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3) 
        tdLog.info(cmd4)
        tdSql.execute(cmd4) 
        tdSql.query('describe length1 ;')
        tdSql.checkData(1,2,11) 

        cmd5 = 'alter table lengthsta1 modify column lengthnchar nchar(21);'
        cmd6 = 'describe lengthsta1 ;'
        tdLog.info(cmd5)
        tdSql.execute(cmd5) 
        tdLog.info(cmd6)
        tdSql.execute(cmd6) 
        tdSql.query('describe lengthsta1 ;')
        tdSql.checkData(2,2,21) 

        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb')


        tdLog.info("=============== step2.5,insert table right data") 
        cmd1 = 'insert into length1 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd1)    
        tdSql.execute(cmd1)  
        tdSql.query('select * from length1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')



        tdLog.info("=============== step2.6,create table wrong tag") 
        cmd1 = 'create table length2 using lengthsta1 tags(\'aaaaabbbbbaaaaa1\',\'bbbbbaaaaabbbbbaaaaabbbbb1\')  ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("invalid operation: tag value too long")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("invalid operation: tag value too long")      
        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')

        tdLog.info("=============== step2.7,modify tag columu length ")  
        cmd1 = 'alter table lengthsta1 modify tag tlengthbia binary(15) ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one")   

        cmd2 = 'alter table lengthsta1 modify tag tlengthnchar nchar(25);'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one") 

        cmd3 = 'alter table lengthsta1 modify tag tlengthbia binary(16) ;'
        cmd4 = 'describe lengthsta1 ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3) 
        tdLog.info(cmd4)
        tdSql.execute(cmd4) 
        tdSql.query('describe lengthsta1 ;')
        tdSql.checkData(3,2,16) 

        cmd5 = 'alter table lengthsta1 modify tag tlengthnchar nchar(26);'
        cmd6 = 'describe lengthsta1 ;'
        tdLog.info(cmd5)
        tdSql.execute(cmd5) 
        tdLog.info(cmd6)
        tdSql.execute(cmd6) 
        tdSql.query('describe lengthsta1 ;')
        tdSql.checkData(4,2,26) 

        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.checkData(0,3,'aaaaabbbbbaaaaa') 
        tdSql.checkData(0,4,'bbbbbaaaaabbbbbaaaaabbbbb')        


        tdLog.info("=============== step2.8,creat tag right data and insert data") 
        cmd1 = 'create table length2 using lengthsta1 tags(\'aaaaabbbbbaaaaa1\',\'bbbbbaaaaabbbbbaaaaabbbbb1\')  ;'
        tdLog.info(cmd1)    
        tdSql.execute(cmd1)  
        tdSql.query('describe length2 ;')
        tdSql.checkData(3,2,16) 
        tdSql.checkData(4,2,26) 

        cmd2 = 'insert into length2 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd2)    
        tdSql.execute(cmd2)  
        tdSql.query('select * from length2 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.checkData(0,3,'aaaaabbbbbaaaaa1') 
        tdSql.checkData(0,4,'bbbbbaaaaabbbbbaaaaabbbbb1')  


        tdLog.info("=============== step2.9,modify tag columu length again ")  
        cmd1 = 'alter table lengthsta1 modify tag tlengthbia binary(16) ;'
        tdLog.info(cmd1)
        tdSql.error(cmd1)
        try:
            tdSql.execute(cmd1)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one")   

        cmd2 = 'alter table lengthsta1 modify tag tlengthnchar nchar(26);'
        tdLog.info(cmd2)
        tdSql.error(cmd2)
        try:
            tdSql.execute(cmd2)
            tdLog.exit("new column length should be bigger than old one")
        except Exception as e:
            tdLog.info(repr(e))
            tdLog.info("new column length should be bigger than old one") 

        cmd3 = 'alter table lengthsta1 modify tag tlengthbia binary(20) ;'
        cmd4 = 'describe lengthsta1 ;'
        tdLog.info(cmd3)
        tdSql.execute(cmd3) 
        tdLog.info(cmd4)
        tdSql.execute(cmd4) 
        tdSql.query('describe lengthsta1 ;')
        tdSql.checkData(3,2,20) 

        cmd5 = 'alter table lengthsta1 modify tag tlengthnchar nchar(30);'
        cmd6 = 'describe lengthsta1 ;'
        tdLog.info(cmd5)
        tdSql.execute(cmd5) 
        tdLog.info(cmd6)
        tdSql.execute(cmd6) 
        tdSql.query('describe lengthsta1 ;')
        tdSql.checkData(4,2,30) 

        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.checkData(0,3,'aaaaabbbbbaaaaa1') 
        tdSql.checkData(0,4,'bbbbbaaaaabbbbbaaaaabbbbb1')        


        tdLog.info("=============== step2.10,creat tag right data and insert data again") 
        cmd1 = 'create table length3 using lengthsta1 tags(\'aaaaabbbbbaaaaabbbbb\',\'bbbbbaaaaabbbbbaaaaabbbbbaaaaa\')  ;'
        tdLog.info(cmd1)    
        tdSql.execute(cmd1)  
        tdSql.query('describe length3 ;')
        tdSql.checkData(3,2,20) 
        tdSql.checkData(4,2,30) 

        cmd2 = 'insert into length3 values(now,\'aaaaaaaaaa1\',\'bbbbbbbbbbbbbbbbbbbb1\') ;'
        tdLog.info(cmd2)    
        tdSql.execute(cmd2)  
        tdSql.query('select * from length3 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.query('select * from lengthsta1 order by ts desc')
        tdSql.checkData(0,1,'aaaaaaaaaa1') 
        tdSql.checkData(0,2,'bbbbbbbbbbbbbbbbbbbb1')
        tdSql.checkData(0,3,'aaaaabbbbbaaaaabbbbb') 
        tdSql.checkData(0,4,'bbbbbaaaaabbbbbaaaaabbbbbaaaaa')  




    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
