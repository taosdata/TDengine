#!/usr/bin/python
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

# wget https://bootstrap.pypa.io/get-pip.py -O /tmp/get-pip.py
# pip install driver/Python/python2/
# python2 ubuntu.sim
# python2 ubuntu.sim -f query/basic

# -*- coding: utf-8 -*-  
import sys
import getopt
from util.log import *
from util.dnodes import *
from util.cases import *


##from alter.alter_table import *
##from alter.alter_stable import *
##from alter.file_corrupt import *
##
##from importtest.basic import *
##from importtest.commit import *
##from importtest.large import *
##from importtest.replica1 import *
##from importtest.replica2 import *
##from importtest.replica3 import *
##from importtest.import_and_query import *
##from importtest.import_and_query_replica3 import *
##
##from insert.basic import *
##from insert.query_block1_memory import *
##from insert.query_block2_memory import *
##from insert.query_block1_file import *
##from insert.query_block2_file import *
##from insert.query_file_memory import *
##from insert.query_multi_file import *
##from insert.longTimespanWrite import *
##
##from dbmgmt.dropDB_memory_test import *
##from dbmgmt.createTableAndKillDnodes import *
##from dbmgmt.createTableAndDropDnodes import *
##
### These use cases have bugs.
##from affectrows.insertsql import * 
##from affectrows.insertsql_blocks import *
##from affectrows.importsql import * 
##from affectrows.importsql_blocks import *
##
### It might take a long time to run.
##from http.retrieve import *
##
##from sdb.mnode2 import *
##from sdb.mnode3 import *
##from sdb.mnode2_delete import *
##
##from multithread.query_table import *
##
##from multithread.query_stable import *
##
###from thread.insert_replica1 import *
##from multithread.insert_replica1 import *
##from multithread.insert_replica2 import *
##from multithread.insert_replica3 import *
##from multithread.import_replica1 import *
##from multithread.import_replica2 import *
##from multithread.import_replica3 import *
##from multithread.insertAndImport_dnode6_replica3 import *
##from multithread.insertAndImport_dnode1_replica1 import *
##
##from bug.ahx_query_from_stables import *
##from bug.async_query import *
##from bug.batch_import1 import *
##from bug.batch_import3 import *
##from bug.column64_replica1 import *
##from bug.column256_replica1 import *
##from bug.connect_repeat import *
##
from cluster.alter_replica import *
from cluster.create_droptb import *
from cluster.single_upgrade import *
##from cluster.monitor_upgrade import *
from cluster.sync_createtb import *
from cluster.sync_createtb2 import *
from cluster.sync_droptb import *
from cluster.sync_droptb2 import *
from cluster.sync_altertb import *
from cluster.sync_altertb2 import *
from cluster.sync_dropdb import *
from cluster.sync_dropdb2 import *
from cluster.offline_droptb_online import *
from cluster.offline_droptb_online2 import *
from cluster.offline_createtb_online import *
from cluster.offline_createtb_online2 import *
from cluster.offline_altertb_online import *
from cluster.offline_altertb_online2 import *
from cluster.offline_dropdb_online import *
from cluster.offline_dropdb_online2 import *
from cluster.balance_adddnode import *
from cluster.balance_adddnode2 import *
from cluster.balance_dropdnode import *
from cluster.balance_dropdnode2 import *
from cluster.full_dropdnode import *
from cluster.full_createtb import *
from cluster.kill_timeout import *
from cluster.kill_timeout_restart import *
from cluster.kill_timeout_restart2 import *
##from cluster.nw_disable_timeout import *
##from cluster.nw_disable_timeout_restart import *
##from cluster.nw_disable_able import *
from cluster.kill_most import *
from cluster.kill_most_restart import *
from cluster.kill_allvnode_restart import *
from cluster.kill_corruptfile_restart import *
from cluster.corruptfile_restore import *
from cluster.corruptfile_restore2 import *
from cluster.kill_restart import *
##from cluster.kill_restart2 import *
from cluster.query_speed import *
from cluster_mgmt.multi_alter import *
from cluster_mgmt.kill_alter_restart import *
##from cluster_mgmt.kill_most import *
##from cluster_mgmt.kill_most_restart import *
##from cluster_mgmt.kill_all_restart import *
##from cluster_mgmt.sync_altertb import *
##from cluster_mgmt.sync_createtb import *
##from cluster_mgmt.sync_droptb import *
##from cluster_mgmt.sync_resync_altertb import *
##from cluster_mgmt.sync_resync_createtb import *
##from cluster_mgmt.sync_resync_droptb import *
##from cluster_mgmt.kill_corruptmgmt_restart import *
##
##from import_merge.importHead import *
##from import_merge.importTail import *
##from import_merge.importHeadOverlap import *
##from import_merge.importHeadPartOverlap import *
##from import_merge.importTailOverlap import *
##from import_merge.importTailPartOverlap import *
##from import_merge.importSpan import *
##from import_merge.importHRestart import *
##from import_merge.importTRestart import *
##from import_merge.importHORestart import *
##from import_merge.importHPORestart import *
##from import_merge.importTORestart import *
##from import_merge.importTPORestart import *
##from import_merge.importSRestart import *
##from import_merge.importBlock1H import *
##from import_merge.importBlock1T import *
##from import_merge.importBlock1HO import *
##from import_merge.importBlock1HPO import *
##from import_merge.importBlock1TO import *
##from import_merge.importBlock1TPO import *
##from import_merge.importBlock1S import *
##from import_merge.importBlock2H import *
##from import_merge.importBlock2T import *
##from import_merge.importBlock2HO import *
##from import_merge.importBlock2HPO import *
##from import_merge.importBlock2TO import *
##from import_merge.importBlock2TPO import *
##from import_merge.importBlock2S import *
##from import_merge.importBlockbetween import *
##from import_merge.importLastH import *
##from import_merge.importLastHO import *
##from import_merge.importLastHPO import *
##from import_merge.importLastT import *
##from import_merge.importLastTO import *
##from import_merge.importLastTPO import *
##from import_merge.importLastS import *
##from import_merge.importDataH import *
##from import_merge.importDataHO import *
##from import_merge.importDataHPO import *
##from import_merge.importDataT import *
##from import_merge.importDataTO import *
##from import_merge.importDataTPO import *
##from import_merge.importDataS import *
##from import_merge.importDataLastH import *
##from import_merge.importDataLastHO import *
##from import_merge.importDataLastHPO import *
##from import_merge.importDataLastT import *
##from import_merge.importDataLastTO import *
##from import_merge.importDataLastTPO import *
##from import_merge.importDataLastS import *
##from import_merge.importCacheFileH import *
##from import_merge.importCacheFileT import *
##from import_merge.importCacheFileHO import *
##from import_merge.importCacheFileHPO import *
##from import_merge.importCacheFileTO import *
##from import_merge.importCacheFileTPO import *
##from import_merge.importCacheFileS import *

from multi_import_merge.create_droptbH import *
from multi_import_merge.create_droptbT import *
from multi_import_merge.create_droptbHO import *
from multi_import_merge.create_droptbTO import *
from multi_import_merge.create_droptbHPO import *
from multi_import_merge.create_droptbTPO import *
from multi_import_merge.create_droptbS import *
from multi_import_merge.LastH import *
from multi_import_merge.LastT import *
from multi_import_merge.LastHO import *
from multi_import_merge.LastTO import *
from multi_import_merge.LastHPO import *
from multi_import_merge.LastTPO import *
from multi_import_merge.LastS import *
from multi_import_merge.DataH import *
from multi_import_merge.DataT import *
from multi_import_merge.DataHO import *
from multi_import_merge.DataTO import *
from multi_import_merge.DataHPO import *
from multi_import_merge.DataTPO import *
from multi_import_merge.DataS import *
from multi_import_merge.DataLastH import *
from multi_import_merge.DataLastT import *
from multi_import_merge.DataLastHO import *
from multi_import_merge.DataLastTO import *
from multi_import_merge.DataLastHPO import *
from multi_import_merge.DataLastTPO import *
from multi_import_merge.DataLastS import *
from multi_import_merge.CacheFileH import *
from multi_import_merge.CacheFileT import *
from multi_import_merge.CacheFileHO import *
from multi_import_merge.CacheFileTO import *
from multi_import_merge.CacheFileHPO import *
from multi_import_merge.CacheFileTPO import *
from multi_import_merge.CacheFileS import *
from multi_import_merge.Block1H import *
from multi_import_merge.Block1T import *
from multi_import_merge.Block1HO import *
from multi_import_merge.Block1TO import *
from multi_import_merge.Block1HPO import *
from multi_import_merge.Block1TPO import *
from multi_import_merge.Block1S import *
from multi_import_merge.Block2H import *
from multi_import_merge.Block2T import *
from multi_import_merge.Block2HO import *
from multi_import_merge.Block2TO import *
from multi_import_merge.Block2HPO import *
from multi_import_merge.Block2TPO import *
from multi_import_merge.Block2S import *
from multi_import_merge.Blockbetween import *
from multi_import_merge.multithread import *  ## large data import


if __name__=="__main__":
	fileName = "all"
	deployPath = ""
	masterIp = ""
	testCluster = False
	opts, args = getopt.getopt(sys.argv[1:], 'f:p:m:sch', ['file=', 'path=', 'master', 'stop', 'cluster', 'help'])
	for key, value in opts:
		if key in ['-h', '--help']:
			tdLog.printNoPrefix('A collection of test cases written using Python')
			tdLog.printNoPrefix('-f Name of test case file written by Python')
			tdLog.printNoPrefix('-p Deploy Path for Simulator')
			tdLog.printNoPrefix('-m Master Ip for Simulator')
			tdLog.printNoPrefix('-c Test Cluster Flag')
			tdLog.printNoPrefix('-s stop All dnodes')
			sys.exit(0)
		if key in ['-f', '--file']:
			fileName = value
		if key in ['-p', '--path']:
			deployPath = value	
		if key in ['-m', '--master']:
			masterIp = value	
		if key in ['-c', '--cluster']:
			testCluster = True		
		if key in ['-s', '--stop']:
			cmd = "ps -ef|grep taosd | grep 'taosd' | grep -v grep | awk '{print $2}' | xargs kill -9" 
			os.system(cmd)	
			tdLog.exit('stop All dnodes')
	
	if masterIp == "":
		tdDnodes.init(deployPath)
		if testCluster:
			tdLog.notice("Procedures for testing cluster")
			if fileName == "all":
				tdCases.runAllCluster()
			else:
				tdCases.runOneCluster(fileName)
		else:
			tdLog.notice("Procedures for testing self-deployment")
			tdDnodes.stopAll()
			tdDnodes.deploy(1)
			tdDnodes.start(1)
			conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
			if fileName == "all":
				tdCases.runAllLinux(conn)
			else:
				tdCases.runOneLinux(conn, fileName)
			conn.close()
	else:
		tdLog.notice("Procedures for tdengine deployed in %s" % (masterIp))
		conn = taos.connect(host=masterIp, config=tdDnodes.getSimCfgPath())
		if fileName == "all":
			tdCases.runAllWindows(conn)
		else:
			tdCases.runOneWindows(conn, fileName)
		conn.close()

	
	
