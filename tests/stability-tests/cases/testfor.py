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
import sys
import time
import subprocess
from xml.etree.ElementInclude import include

class TDFor:

    def run(self):
        n = 0
        while n < 1000:           
                
            # testcmd = os.system("taostest --setup=test_184env.yaml")                
            # print ("The setup is %d " % (testcmd))
            n += 1
            print("\n==========================\n")
            # os.system("taostest --use=test_184env.yaml --case=querycases/function/stable_func_right.py --keep  >/root/log/stable_func_right.txt")
            # f1=open('/root/log/stable_func_right.txt')
            # data1 = f1.readlines()
            # f1.close() 
            
            # ls =['failed']
            
            # if str(ls).replace("[","").replace("]","").replace("'","") in data3[-4]:
            #     print("第 %d stable_func_special_10.py stop stop" %n)
            #     print(time.asctime(time.localtime(time.time())))
            #     break
            # else:
            #     print("第 %d stable_func_special_10.py go go go" %n)
            #     print(time.asctime(time.localtime(time.time())))
                
            # os.system("taostest --use=test_184env.yaml --case=querycases/function/stable_func_special_2.py --keep  >/root/log/stable_func_special_12.txt")
            # f3=open('/root/log/stable_func_special_12.txt')
            # data3 = f3.readlines()
            # f3.close() 
            
            # ls =['failed']
            
            # if str(ls).replace("[","").replace("]","").replace("'","") in data3[-4]:
            #     print("第 %d stable_func_special_12.py stop stop" %n)
            #     print(time.asctime(time.localtime(time.time())))
            #     break
            # else:
            #     print("第 %d stable_func_special_12.py go go go" %n)
            #     print(time.asctime(time.localtime(time.time())))
            
            os.system(" taostest --use=common_insert.yaml --case=Query/queryscript/stable_function/str/stable_str_upper.py  --keep  >>/root/for.txt")
            f3=open('/root/for.txt')
            data3 = f3.readlines()
            f3.close() 
            
            #ls =['FAILED']
            ls =['SUCC']
            
            if str(ls).replace("[","").replace("]","").replace("'","") in data3[-7]:
                print("第 %d stable_str_upper.py stop stop" %n)
                print(time.asctime(time.localtime(time.time())))
                break
            else:
                print("第 %d stable_str_upper.py go go go" %n)
                print(time.asctime(time.localtime(time.time())))
                
            # os.system("taostest --use=test_184env.yaml --case=querycases/function/stable_func_special_5.py --keep  >/root/log/stable_func_special_4.txt")
            # f4=open('/root/log/stable_func_special_4.txt')
            # data4 = f4.readlines()
            # f4.close() 
            
            # ls =['failed']
            
            # if str(ls).replace("[","").replace("]","").replace("'","") in data4[-4]:
            #     print("第 %d stable_func_special_4.py stop stop" %n)
            #     print(time.asctime(time.localtime(time.time())))
            #     break
            # else:
            #     print("第 %d stable_func_special_4.py go go go" %n)
            #     print(time.asctime(time.localtime(time.time())))
          

TDFor().run()
