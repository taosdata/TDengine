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
from fabric import Connection
import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from datetime import datetime
import subprocess

# class VMPnode

# ##TODO: auto test version is currently unsupported, need to come up with 
# #       an auto test version in the future
# class TDTestCase:
#     def init(self, conn, logSql):
#         tdLog.debug("start to execute %s" % __file__)
#         tdSql.init(conn.cursor(), logSql)

#     def run(self):
#         return



#     def stop(self):
#         tdSql.close()
#         tdLog.debug("%s alter block manual check finish" % __file__)


# tdCases.addWindows(__file__, TDTestCase())
# tdCases.addLinux(__file__, TDTestCase())

conn = conn = Connection("{}@{}".format('ubuntu', "lyq-q-1"), connect_kwargs={"password": "{}".format('tbase125!')})
conn.run('sudo timedatectl set-ntp off')
conn.run('sudo timedatectl set-time 2020-10-20')