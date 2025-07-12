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

from .server.dnode import *
from .server.dnodes import *
from .server.cluster import *


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
        """
        Starts a dnode.

        Args:
            idx (int): The index of the dnode to start.

        Returns:
            bool: True if the dnode was started successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.starttaosd(idx)

        return tdDnodes.starttaosd(idx)

    def dnodeStartAll(self):
        """
        Starts all dnodes.

        Returns:
            bool: True if all dnodes were started successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            for dnode in clusterDnodes.dnodes:
                clusterDnodes.starttaosd(dnode.index)
        else:
            return tdDnodes.starttaosd(1)

    # stop idx base is 1
    def dnodeStop(self, idx):
        """
        Stops a dnode.

        Args:
            idx (int): The index of the dnode to stop.

        Returns:
            bool: True if the dnode was stopped successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.stoptaosd(idx)

        return tdDnodes.stoptaosd(idx)

    def dnodeForceStop(self, idx):
        """
        Force Stops a dnode.

        Args:
            idx (int): The index of the dnode to stop.

        Returns:
            bool: True if the dnode was stopped successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.forcestop(idx)

        return tdDnodes.forcestop(idx)

    def dnodeClearData(self, idx):
        """
        Clear dnode's data (Remove all data files).

        Args:
            idx (int): The index of the dnode to clear.

        Returns:
            bool: True if the dnode was cleared successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.dnodeClearData(idx)

        return tdDnodes.dnodeClearData(idx)

    def dnodeStopAll(self):
        """
        Stops all dnodes.

        Returns:
            bool: True if all dnodes were stopped successfully, False otherwise.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.stopAll()

        return tdDnodes.stopAll()

    def dnodeRestartAll(self) :
        """
        Restarts all dnodes.

        Returns:
            bool: True if all dnodes were restarted successfully, False otherwise.
        """

        self.dnodeStopAll()
        self.dnodeStartAll()


    #
    #  about path
    #

    # get cluster root path like /root/TDinternal/sim/
    def clusterRootPath(self):
        """
        Gets the root path of the cluster.

        Returns:
            str: The root path of the cluster.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.getDnodesRootDir()

        return tdDnodes.getDnodesRootDir()

    # get taosd path
    def taosdFile(self, idx):
        """
        Gets the path to the taosd file for a specific dnode.

        Args:
            idx (int): The index of the dnode.

        Returns:
            str: The path to the taosd file.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.taosdFile(idx)

        return tdDnodes.taosdFile(idx)

    # return dnode data files list
    def dnodeDataFiles(self, idx):
        """
        Gets the data files for a specific dnode.

        Args:
            idx (int): The index of the dnode.

        Returns:
            list: A list of data files for the dnode.
        """
        files = []
        return files

    #
    # get dnodes information
    #

    # taos.cfg position
    def dnodeCfgPath(self, idx):
        """
        Gets the configuration path for a specific dnode.

        Args:
            idx (int): The index of the dnode.

        Returns:
            str: The configuration path for the dnode.
        """
        if clusterDnodes.getModel() == "cluster":
            return clusterDnodes.getDnodeCfgPath(idx)
        return tdDnodes.getDnodeCfgPath(idx)


sc = srvCtl()
