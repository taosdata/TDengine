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
import random

class ClusterTestcase:

        
    def run(self):
        os.system("./buildClusterEnv.sh -n 3 -v 2.0.14.1")
        os.system("yes|taosdemo -h 172.27.0.7 -n 100 -t 100 -x")
        os.system("python3 ../../concurrent_inquiry.py -H 172.27.0.7 -T 4 -t 4 -l 10")

clusterTest = ClusterTestcase()
clusterTest.run()