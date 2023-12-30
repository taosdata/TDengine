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
import os
import time
import datetime

class srvCtl:
    def __init__(self):
        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

    # return dnode data files list
    def dnodeDataFiles(self, idx):
        files = []
        return files

sc = srvCtl()