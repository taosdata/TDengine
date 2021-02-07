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

import os
import taos
import random
import argparse

class BuildDockerCluser:

    def __init__(self, hostName, user, password, configDir, numOfNodes, clusterVersion):
        self.hostName = hostName
        self.user = user
        self.password = password
        self.configDir = configDir
        self.numOfNodes = numOfNodes
        self.clusterVersion = clusterVersion        

    def getConnection(self):
        self.conn = taos.connect(
            host = self.hostName,
            user = self.user,
            password = self.password,
            config = self.configDir)

    def createDondes(self):
        self.cursor = self.conn.cursor()        
        for i in range(2, self.numOfNodes + 1):            
            self.cursor.execute("create dnode tdnode%d" % i)
    
    def startArbitrator(self):
        print("start arbitrator")
        os.system("docker exec -d $(docker ps|grep tdnode1|awk '{print $1}') tarbitrator")

    def run(self):
        if self.numOfNodes < 2 or self.numOfNodes > 5:
            print("the number of nodes must be between 2 and 5")
            exit(0)        
        os.system("./buildClusterEnv.sh -n %d -v %s" % (self.numOfNodes, self.clusterVersion))
        self.getConnection()
        self.createDondes()
        self.startArbitrator() 

parser = argparse.ArgumentParser()
parser.add_argument(
    '-H',
    '--host',
    action='store',
    default='tdnode1',
    type=str,
    help='host name to be connected (default: tdnode1)')
parser.add_argument(
    '-u',
    '--user',
    action='store',
    default='root',
    type=str,
    help='user (default: root)')
parser.add_argument(
    '-p',
    '--password',
    action='store',
    default='taosdata',
    type=str,
    help='password (default: taosdata)')
parser.add_argument(
    '-c',
    '--config-dir',
    action='store',
    default='/etc/taos',
    type=str,
    help='configuration directory (default: /etc/taos)')
parser.add_argument(
    '-n',
    '--num-of-nodes',
    action='store',
    default=2,
    type=int,
    help='number of nodes in the cluster (default: 2, min: 2, max: 5)')
parser.add_argument(
    '-v',
    '--version',
    action='store',
    default='2.0.14.1',
    type=str,
    help='the version of the cluster to be build, Default is 2.0.14.1')

args = parser.parse_args()
cluster = BuildDockerCluser(args.host, args.user, args.password, args.config_dir, args.num_of_nodes, args.version)
cluster.run()