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
import logging
logger = logging.getLogger(__name__)

class TDLog:
    def __init__(self):
        self.path = ""

    def info(self, info):
        logger.info(info)

    def sleep(self, sec):
        logger.info(f"sleep {sec} seconds")
        time.sleep(sec)

    def debug(self, err):
        logger.debug(err)

    def success(self, info):
        logger.info(info)

    def notice(self, err):
        logger.info(err)

    def exit(self, err):
        logger.error(err)
        sys.exit(1)

    def printNoPrefix(self, info):
        logger.info(info)


tdLog = TDLog()
