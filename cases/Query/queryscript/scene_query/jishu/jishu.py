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

import random
import os
import time
#import taos
from Query.queryutil.createdata import *
from Query.queryutil.where import *
from Query.queryutil.stable_func import *
#from itertools import product
from itertools import combinations
#import subprocess

from taostest import TDCase

class TDTestQuery(TDCase):
    def init(self):
        super(TDTestQuery, self).init()
        self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        self.firstEP = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.firstEP.append(
                    self.taosd_setting['spec']['config']['firstEP'])
        self.target_taosd = self.firstEP[-1].split(':')
        print(self.target_taosd[0])
        self.service_host = self.target_taosd[0]

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# 
        '''
        return case_description

    #basic_param
    db = "top_security"
    
    num_random = 10
    stable_child_num = 100

        
    def create_db(self,database):
        fake = Faker('zh_CN')
        self.tdSql.execute('''drop database if exists %s ;''' %database)
        self.tdSql.execute('''create database %s DURATION 1d KEEP 36500 PRECISION 'ns' CACHEMODEL 'both';'''%database)
        self.tdSql.execute('''use %s;'''%database)
        self.tdSql.execute('''CREATE STABLE `st_intranet_flow_net_card` (`ts` TIMESTAMP, `sts` NCHAR(32), `id` NCHAR(24), `source_mac` NCHAR(64), `former_source_mac` NCHAR(64), `source_ip_mask` NCHAR(64), `source_port` INT, `source_addr` NCHAR(64), `target_ip` NCHAR(64), `target_mac` NCHAR(64), `target_port` INT, `target_addr` NCHAR(64), `protocol` NCHAR(64), `operation` NCHAR(64), `event_id` NCHAR(64), `rule_type` NCHAR(32), `rule_set` NCHAR(32), `threat_level` TINYINT, `threat_level_name` NCHAR(8), `agent_ip` NCHAR(64), `create_time` TIMESTAMP) TAGS (`source_ip` NCHAR(64), `event_type` NCHAR(16), `st_hour` NCHAR(2), `group_id` BIGINT, `black_list` TINYINT, `white_list` TINYINT);''')
        self.tdSql.execute('''CREATE STABLE `st_intranet_flow_scan` (`ts` TIMESTAMP, `sts` NCHAR(32), `id` NCHAR(24), `source_mac` NCHAR(64), `former_source_mac` NCHAR(64), `source_ip_mask` NCHAR(64), `source_port` INT, `source_addr` NCHAR(64), `target_ip` NCHAR(64), `target_mac` NCHAR(64), `target_port` INT, `target_addr` NCHAR(64), `protocol` NCHAR(64), `operation` NCHAR(64), `event_id` NCHAR(64), `rule_type` NCHAR(32), `rule_set` NCHAR(32), `threat_level` TINYINT, `threat_level_name` NCHAR(8), `agent_ip` NCHAR(64), `create_time` TIMESTAMP) TAGS (`source_ip` NCHAR(64), `event_type` NCHAR(16), `st_hour` NCHAR(2), `group_id` BIGINT, `black_list` TINYINT, `white_list` TINYINT);''')
        self.tdSql.execute('''CREATE STABLE `st_intranet_flow_broadcast` (`ts` TIMESTAMP, `sts` NCHAR(32), `id` NCHAR(24), `source_mac` NCHAR(64), `former_source_mac` NCHAR(64), `source_ip_mask` NCHAR(64), `source_port` INT, `source_addr` NCHAR(64), `target_ip` NCHAR(64), `target_mac` NCHAR(64), `target_port` INT, `target_addr` NCHAR(64), `protocol` NCHAR(64), `operation` NCHAR(64), `event_id` NCHAR(64), `rule_type` NCHAR(32), `rule_set` NCHAR(32), `threat_level` TINYINT, `threat_level_name` NCHAR(8), `agent_ip` NCHAR(64), `create_time` TIMESTAMP) TAGS (`source_ip` NCHAR(64), `event_type` NCHAR(16), `st_hour` NCHAR(2), `group_id` BIGINT, `black_list` TINYINT, `white_list` TINYINT);''')
                   
        for i in range(self.stable_child_num):
            if i /2 == 0 :
                self.tdSql.execute('''create table st_intranet_flow_scan_%d using st_intranet_flow_scan tags('st_intranet_flow_scan_2_%d', 'scan' , '%d', '%d' , 2 , 0 ) ;''' 
                      %(i , i , fake.random_int(min=00, max=99, step=1),fake.random_int(min=-32767, max=32767, step=1))) 
            else :
                self.tdSql.execute('''create table st_intranet_flow_scan_%d using st_intranet_flow_scan tags('st_intranet_flow_scan_1_%d', 'scan' , '%d', '%d' , 1 , 0 ) ;''' 
                      %(i , i , fake.random_int(min=00, max=99, step=1),fake.random_int(min=-32767, max=32767, step=1))) 
                
        self.insert_data("%s" % self.db, 1)

    def insert_data(self,database,n):
        time.sleep(10)
        fake = Faker('zh_CN')
            
        for i in range(self.num_random*n): 
            for j in range(self.stable_child_num):       
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now + 1h, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now - 1h, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now - 2h, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now - 3h, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                       
            for j in range(int(self.stable_child_num/2)):                       
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now , %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                
                self.tdSql.execute('''insert into st_intranet_flow_scan_%d  (ts , sts , id , source_mac , source_ip_mask,target_ip,operation,event_id,create_time ) 
                                values(now - 1h, %s, %s, '%s:%s:%s:%s:%s:%s', '%s.%s.%s.%s/%s', '%s.%s.%s.%s', 'ARP Request', %s , now) ;''' 
                            % ( j, fake.random_int(min=0, max=2147483647, step=1), fake.random_int(min=0, max=2147483647, step=1),
                            fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=24, step=1), 
                            fake.random_int(min=10, max=250, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), fake.random_int(min=10, max=99, step=1), 
                            fake.random_int(min=0, max=2147483647, step=1)))
                                
        i = random.randint(0,1)
        if i ==0:
            self.logger.info("======this case test use flush database =========")
            self.tdSql.execute("flush database %s;" %database)       
        elif i ==1:
            self.logger.info("===!!!===this case test not use flush database =====!!!====")

        self.tdSql.query("select count(*) from st_intranet_flow_scan;")
        self.logger.info("\n==========================count(st_intranet_flow_scan)=%d=========================\n" %self.tdSql.getData(0,0))        
                  
    def case_1(self):
        fake = Faker('zh_CN')
        time.sleep(20)
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                stable_where = tdWhere.regular_where()
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:

                        sql = "SELECT sum(event_count) eventCount, source_ip FROM (SELECT count(event_id) event_count,event_id, last(agent_ip) last_agent_ip,source_ip FROM  st_intranet_flow_scan WHERE ts >= now-2h AND ts < now AND event_type='scan' AND (white_list IS NULL or white_list=0) GROUP BY source_ip, event_id) WHERE event_count <= 10 GROUP BY source_ip LIMIT 0, 20 " 
                        self.tdSql.query(sql)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))       
                        self.tdCreateData.explain_sql(sql)

                        sql = "SELECT sum(event_count) eventCount, source_ip FROM (SELECT count(event_id) event_count,event_id, last(agent_ip) last_agent_ip,source_ip FROM  st_intranet_flow_scan WHERE ts >= now-2h AND ts < now AND event_type='scan' AND (white_list IS NULL or white_list=0) GROUP BY source_ip, event_id) WHERE event_count <= %d GROUP BY source_ip LIMIT %d, %d " %(fake.random_int(min=1, max=10, step=1),fake.random_int(min=0, max=5, step=1),fake.random_int(min=10, max=20, step=1))
                        self.tdSql.query(sql)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))       
                        self.tdCreateData.explain_sql(sql)
                        
                        sql2 = "select * from (%s) " %(sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        
                        sql2 = "(%s) union (%s) " %(sql,sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        sql3 = "select * from (%s) " %(sql2)
                        self.tdSql.query(sql3)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql3)
                        
                        sql2 = "(%s) union all (%s) " %(sql,sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        sql3 = "select * from (%s) " %(sql2)
                        self.tdSql.query(sql3)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql3)

            except Exception as e:
                raise e   

                  
    def case_2(self):
        time.sleep(20)
        fake = Faker('zh_CN')
        for i in (211,):
            func = tdFunction.func_stable_tbname_all(i)
            try:
                stable_where = tdWhere.regular_where()
                for i in range(2,len(stable_where[2])+1):
                    qt_where = list(combinations(stable_where[2],i))
                    for qt_where in qt_where:

                        sql = "SELECT sum(event_count) eventCount, source_ip FROM (SELECT count(event_id) event_count,event_id, last(agent_ip) last_agent_ip,source_ip FROM  st_intranet_flow_scan WHERE ts >= now-2h AND ts < now AND event_type='scan' AND (white_list IS NULL or white_list=0) GROUP BY source_ip, event_id) WHERE event_count <= 10 GROUP BY source_ip LIMIT 0, 20" 
                        self.tdSql.query(sql)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))       
                        self.tdCreateData.explain_sql(sql)
                        
                        sql = "SELECT sum(event_count) eventCount, source_ip FROM (SELECT count(event_id) event_count,event_id, last(agent_ip) last_agent_ip,source_ip FROM  st_intranet_flow_scan WHERE ts >= now-2h AND ts < now AND event_type='scan' AND (white_list IS NULL or white_list=0) GROUP BY source_ip, event_id) WHERE event_count <= %d GROUP BY source_ip LIMIT %d, %d" %(fake.random_int(min=1, max=10, step=1),fake.random_int(min=0, max=5, step=1),fake.random_int(min=10, max=20, step=1))
                        self.tdSql.query(sql)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))       
                        self.tdCreateData.explain_sql(sql)
                        
                        sql2 = "select * from (%s) " %(sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        
                        sql2 = "(%s) union (%s) " %(sql,sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        sql3 = "select * from (%s) " %(sql2)
                        self.tdSql.query(sql3)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql3)
                        
                        sql2 = "(%s) union all (%s) " %(sql,sql)
                        self.tdSql.query(sql2)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql2)
                        sql3 = "select * from (%s) " %(sql2)
                        self.tdSql.query(sql3)
                        self.logger.info("\n==========================count(result)=%d=======\n" %self.tdSql.getData(0,0))  
                        self.tdCreateData.explain_sql(sql3)

            except Exception as e:
                raise e   
                

                                
    def run(self):
        startTime = time.time() 
        
        self.create_db("%s" % self.db)
        
        while 1: 
            i = random.randint(0,3)
            if i ==0:
                self.insert_data("%s" % self.db, 1)
            elif i ==1: 
                self.case_2()
            else:    
                self.case_1()

        endTime = time.time()
        self.logger.info("total time %ds" % (endTime - startTime))

