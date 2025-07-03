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
from new_test_framework.utils import tdLog, tdSql, etool
import os

class TestCloudTest:
    def caseDescription(self):
        """
        [TD-22022] taosBenchmark cloud test cases
        """

    def test_cloud_test(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx
        History:            - xxx
            - xxx
        """
        binPath = etool.benchMarkFile()
        cmd = "%s -T 1 -t 2 -n 10 -y" % binPath
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
 
        tdLog.success("%s successfully executed" % __file__)


