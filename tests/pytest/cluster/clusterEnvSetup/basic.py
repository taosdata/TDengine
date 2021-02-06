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
import random
import argparse

class ClusterTestcase:

        
    def run(self):
        os.system("./buildClusterEnv.sh -n 3 -v 2.0.14.1")
        os.system("yes|taosdemo -h 172.27.0.7 -n 100 -t 100 -x")
        os.system("python3 ../../concurrent_inquiry.py -H 172.27.0.7 -T 4 -t 4 -l 10")

parser = argparse.ArgumentParser()
parser.add_argument(
    '-H',
    '--host-name',
    action='store',
    default='127.0.0.1',
    type=str,
    help='host name to be connected (default: 127.0.0.1)')
parser.add_argument(
    '-S',
    '--ts',
    action='store',
    default=1500000000000,
    type=int,
    help='insert data from timestamp (default: 1500000000000)')
parser.add_argument(
    '-d',
    '--db-name',
    action='store',
    default='test',
    type=str,
    help='Database name to be created (default: test)')
parser.add_argument(
    '-t',
    '--number-of-native-threads',
    action='store',
    default=10,
    type=int,
    help='Number of native threads (default: 10)')
parser.add_argument(
    '-T',
    '--number-of-rest-threads',
    action='store',
    default=10,
    type=int,
    help='Number of rest threads (default: 10)')
parser.add_argument(
    '-r',
    '--number-of-records',
    action='store',
    default=100,
    type=int,
    help='Number of record to be created for each table  (default: 100)')
parser.add_argument(
    '-c',
    '--create-table',
    action='store',
    default='0',
    type=int,
    help='whether gen data (default: 0)')
parser.add_argument(
    '-p',
    '--subtb-name-prefix',
    action='store',
    default='t',
    type=str,
    help='subtable-name-prefix (default: t)')
parser.add_argument(
    '-P',
    '--stb-name-prefix',
    action='store',
    default='st',
    type=str,
    help='stable-name-prefix (default: st)')
parser.add_argument(
    '-b',
    '--probabilities',
    action='store',
    default='0.05',
    type=float,
    help='probabilities of join (default: 0.05)')
parser.add_argument(
    '-l',
    '--loop-per-thread',
    action='store',
    default='100',
    type=int,
    help='loop per thread (default: 100)')
parser.add_argument(
    '-u',
    '--user',
    action='store', 
    default='root',
    type=str,
    help='user name')
parser.add_argument(
    '-w',
    '--password',
    action='store', 
    default='root',
    type=str,
    help='user name')
parser.add_argument(
    '-n',
    '--number-of-tables',
    action='store',
    default=1000,
    type=int,
    help='Number of subtales per stable (default: 1000)')
parser.add_argument(
    '-N',
    '--number-of-stables',
    action='store',
    default=2,
    type=int,
    help='Number of stables  (default: 2)')
parser.add_argument(
    '-m',
    '--mix-stable-subtable',
    action='store',
    default=0,
    type=int,
    help='0:stable & substable ,1:subtable ,2:stable (default: 0)')

args = parser.parse_args()
q = ConcurrentInquiry(
    args.ts,args.host_name,args.user,args.password,args.db_name,
                args.stb_name_prefix,args.subtb_name_prefix,args.number_of_native_threads,args.number_of_rest_threads,
                args.probabilities,args.loop_per_thread,args.number_of_stables,args.number_of_tables ,args.number_of_records,
                args.mix_stable_subtable )

if args.create_table: 
    q.gen_data()
q.get_full()

#q.gen_query_sql()
q.run()