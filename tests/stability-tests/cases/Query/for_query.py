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

class TDFor:

    def run(self):

        n = 1
        while n < 1000:            
            print("\n========第 %d 次 程序运行==================\n" % (n))     
            n += 1
            
            subprocess.getoutput("sysctl -p /etc/sysctl.conf")
            os.system("./query_all.sh  >/root/log/query_all.txt")
            #os.system("./query_all.sh  >/root/log/query_all_%d.txt" %n)
                      

TDFor().run()

