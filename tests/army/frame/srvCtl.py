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

from frame.server.dnode  import *
from frame.server.dnodes import *
from frame.server.cluster import *


class srvCtl:
    def __init__(self):
        # record server information
        self.dnodeNum = 0
        self.mnodeNum = 0
        self.mLevel = 0
        self.mLevelDisk = 0

    #
    #  control server
    #

    # start idx base is 1 
    def dnodeStart(self, idx):
        if clusterDnodes.getModel() == 'cluster':
            return clusterDnodes.starttaosd(idx)

        return tdDnodes.starttaosd(idx)

    # stop idx base is 1 
    def dnodeStop(self, idx):
        if clusterDnodes.getModel() == 'cluster':
            return clusterDnodes.stoptaosd(idx)

        return tdDnodes.stoptaosd(idx)

    def dnodeStopAll(self):
        if clusterDnodes.getModel() == 'cluster':
            return clusterDnodes.stopAll()

        return tdDnodes.stopAll()
    #
    #  about path
    #

    # get cluster root path like /root/TDinternal/sim/ 
    def clusterRootPath(self):
        if clusterDnodes.getModel() == 'cluster':
            return clusterDnodes.getDnodesRootDir()

        return tdDnodes.getDnodesRootDir()

    # return dnode data files list
    def dnodeDataFiles(self, idx):
        files = []
        return files
    
    #
    # get dnodes information
    #

    # taos.cfg position
    def dnodeCfgPath(self, idx):
        if clusterDnodes.getModel() == 'cluster':
            return clusterDnodes.getDnodeCfgPath(idx)
        return tdDnodes.getDnodeCfgPath(idx)
    

sc = srvCtl()