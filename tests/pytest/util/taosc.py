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
from util.command import *


class TDClient:
    def __init__(self):
        self.buildPath = tdCom.getBuildPath()

    def execute_sql(self, sql: str):
        ret = tdCmd.run_command(path=self.buildPath, command=f"taos -s '{sql}'")
        return ret

tdClient = TDClient()
