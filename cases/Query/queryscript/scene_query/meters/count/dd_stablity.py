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


from itertools import product
from itertools import combinations
from faker import Faker
import time, datetime
from taostest import TDCase
import subprocess
import os
import random
from taostest.util.common import TDCom
from taostest.util.remote import Remote
import threading

class TDTestQuery(TDCase):
    
    def init(self):
        super(TDTestQuery, self).init()
        #self.tdCreateData = TDCreateData(self.tdSql, self.logger)
        
        #basic_param
        self.testcasePath = os.path.split(__file__)[0]
        self.testcaseFilename = os.path.split(__file__)[-1]
        
        self.dd_if = []
        for env_setting in self.env_setting["settings"]:
            if env_setting["name"].lower() == "taosd":
                self.taosd_setting = env_setting
                self.dd_if.append(
                    self.taosd_setting['spec']['dnodes'][0]['config']['logDir'])
        self.dd_if = str(self.dd_if).replace('[','').replace(']','')
        print(self.dd_if)

    def tags(self) :
	
        return ""

    def author(self) -> str:

        return "Guo Xiangyang"

    def desc(self) -> str:
        case_description = '''
        case1:# dd stablity
        '''
        return case_description        


    def dd_common(self,logname): 
        i1,i2 = random.randint(0,100),random.randint(0,1000)
        self.logger.info("dd:===dd if=%s/%s of=/tmp/dd-%s.txt bs=%dM count=%d===; " %(self.dd_if,logname,logname,i1,i2))
        os.system("dd if=%s/%s of=/tmp/dd-%s.txt bs=%dM count=%d"%(self.dd_if,logname,logname,i1,i2)) 
        time.sleep(random.randint(0,100)) 
        
        i1,i2,i3,i4,i5 = random.randint(10,40),random.randint(10,40),random.randint(10,50),random.randint(10,50),random.randint(50,200)
        self.logger.info("stress test -c %d -m %d -d %d -i %d -t %d " %(i1,i2,i3,i4,i5))
        #os.system("stress -c %d -m %d -d %d -i %d -t %d"%(i1,i2,i3,i4,i5)) 
        #stress -c 10 -m 5 -d 4 -i 2 -t 10
        #-c：模拟CPU使用个数
        # -m：模拟N个进程频繁调用/释放内存
        # -d：模拟磁盘写(创建文件，写入内容，删除文件)
        # -i：模拟I/O，将内存缓冲区中的数据立即写入磁盘
        # --hdd-bytes N：指定写入文件大小，默认为1GB（与 -d 使用）
        # --vm-bytes  N：指定分配内存的大小（与 -m 使用）
        # --vm-keep    ：一直占用内存，而不是循环调用/释放（与 -m 使用）
        # --timeout：N秒后退出（同 -t）
        os.system("stress -c %d -t %d"%(i1,i5))
        time.sleep(random.randint(0,100)) 
        os.system("stress -m %d -t %d"%(i2,i5))
        time.sleep(random.randint(0,100)) 
        os.system("stress -d %d -t %d"%(i3,i5))
        time.sleep(random.randint(0,100)) 
        os.system("stress -i %d -t %d"%(i4,i5))

    def taosd_0(self):        
        self.dd_common('taosdlog.0')
        
    def taosd_1(self):
        self.dd_common('taosdlog.1')
                          
    def taos_0(self):        
        self.dd_common('taoslog.0')
        
    def taos_1(self):  
        self.dd_common('taoslog.1')
        
    def taos0_0(self):  
        self.dd_common('taoslog0.0')
            
    def taos1_0(self):  
        self.dd_common('taoslog1.0')
    
    def taos2_0(self):        
        self.dd_common('taoslog2.0')
        
    def taos3_0(self):  
        self.dd_common('taoslog3.0')
        
    def taos4_0(self):  
        self.dd_common('taoslog4.0')
            
    def taos5_0(self):  
        self.dd_common('taoslog5.0')
    
    def taos6_0(self):        
        self.dd_common('taoslog6.0')
        
    def taos7_0(self):  
        self.dd_common('taoslog7.0')
        
    def taos8_0(self):        
        self.dd_common('taoslog8.0')
        
    def taos9_0(self):  
        self.dd_common('taoslog9.0')
                                                                    
    def run(self):
        startTime = time.time() 
        
        self.taosd_0()
        self.taosd_1()
        self.taos_0()
        self.taos_1()
        self.taos0_0()
        self.taos1_0()
        self.taos2_0()
        self.taos3_0()
        self.taos4_0()
        self.taos5_0()
        self.taos6_0()
        self.taos7_0()
        self.taos8_0()
        self.taos9_0()
        
        while(1):   
            self.taosd_0()
            self.taosd_1()
            self.taos_0()
            self.taos_1()
            self.taos0_0()
            self.taos1_0()
            self.taos2_0()
            self.taos3_0()
            self.taos4_0()
            self.taos5_0()
            self.taos6_0()
            self.taos7_0()
            self.taos8_0()
            self.taos9_0()
        
          
        #     t1 = threading.Thread(target=self.taosd_0) 
        #     t2 = threading.Thread(target=self.taosd_1) 
        #     t3 = threading.Thread(target=self.taos_0) 
        #     t4 = threading.Thread(target=self.taos_1) 
        #     t5 = threading.Thread(target=self.taos0_0) 
        #     t6 = threading.Thread(target=self.taos1_0) 
        #     t7 = threading.Thread(target=self.taos2_0) 
        #     t8 = threading.Thread(target=self.taos3_0) 
        #     t9 = threading.Thread(target=self.taos4_0)
        #     t10 = threading.Thread(target=self.taos5_0) 
        #     t11 = threading.Thread(target=self.taos6_0) 
        #     t12 = threading.Thread(target=self.taos7_0) 
        #     t13 = threading.Thread(target=self.taos8_0)  
        #     t14 = threading.Thread(target=self.taos9_0) 
            
        #     t1.start() 
        #     t2.start() 
        #     t3.start()  
        #     t4.start() 
        #     t5.start() 
        #     t6.start()
        #     t7.start() 
        #     t8.start() 
        #     t9.start()
        #     t10.start() 
        #     t11.start() 
        #     t12.start() 
        #     t13.start() 
        #     t14.start() 
            
            
        #     t1.join()
        #     t2.join()
        #     t3.join()
        #     t4.join()
        #     t5.join()
        #     t6.join()
        #     t7.join()
        #     t8.join()
        #     t9.join()
        #     t10.join()
        #     t11.join()
        #     t12.join()
        #     t13.join()
        #     t14.join()
            
        endTime = time.time()
        
    
        self.logger.info("total time %ds" % (endTime - startTime))
    

