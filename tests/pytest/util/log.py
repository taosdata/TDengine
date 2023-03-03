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
from distutils.log import warn as printf


class TDLog:
    def __init__(self):
        self.path = ""

    def info(self, info):
        print("%s %s\n" % (datetime.datetime.now(), info))

    def sleep(self, sec):
        print("%s sleep %d seconds" % (datetime.datetime.now(), sec))
        time.sleep(sec)

    def debug(self, err):
        print("\033[1;36m%s %s\033[0m" % (datetime.datetime.now(), err))

    def success(self, info):
        printf("\033[1;32m%s %s\033[0m" % (datetime.datetime.now(), info))

    def notice(self, err):
        print("\033[1;33m%s %s\033[0m" % (datetime.datetime.now(), err))

    def exit(self, err):
        print("\033[1;31m%s %s\033[0m" % (datetime.datetime.now(), err))
        sys.exit(1)

    def printNoPrefix(self, info):
        print("\033[1;36m%s\033[0m" % (info))


tdLog = TDLog()
