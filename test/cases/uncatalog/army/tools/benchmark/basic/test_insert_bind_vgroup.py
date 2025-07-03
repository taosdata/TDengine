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

class TestInsertBindVgroup:
    def caseDescription(self):
        """
        taosBenchmark insert->BindVGroup test cases
        """

    # bugs ts
    def checkBasic(self):
        # thread equal vgroups
        self.insertBenchJson(f"./{os.path.dirname(__file__)}/json/insertBindVGroup.json", "-g", True)
        # thread is limited
        self.insertBenchJson(f"./{os.path.dirname(__file__)}/json/insertBindVGroup.json", "-T 2", True)

    def test_insert_bind_vgroup(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        # basic
        self.checkBasic()
        # advance

        tdLog.success("%s successfully executed" % __file__)


