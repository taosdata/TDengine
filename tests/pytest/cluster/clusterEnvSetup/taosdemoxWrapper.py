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

class taosdemoxWrapper:

    def __init__(self, host, metadata, database, tables, threads, configDir, replica, 
        columnType, columnsPerTable, rowsPerTable, disorderRatio, disorderRange, charTypeLen):
        self.host = host
        self.metadata = metadata
        self.database = database
        self.tables = tables
        self.threads = threads
        self.configDir = configDir
        self.replica = replica
        self.columnType = columnType
        self.columnsPerTable = columnsPerTable
        self.rowsPerTable = rowsPerTable
        self.disorderRatio = disorderRatio
        self.disorderRange = disorderRange
        self.charTypeLen = charTypeLen
        
    def run(self):
        if self.metadata is None:
            os.system("taosdemox -h %s -d %s -t %d -T %d -c %s -a %d -b %s -n %d -t %d -O %d -R %d -w %d -x -y" 
            % (self.host, self.database, self.tables, self.threads, self.configDir, self.replica, self.columnType,
            self.rowsPerTable, self.disorderRatio, self.disorderRange, self.charTypeLen))
        else:
            os.system("taosdemox -f %s" % self.metadata)


parser = argparse.ArgumentParser()
parser.add_argument(
    '-H',
    '--host-name',
    action='store',
    default='tdnode1',
    type=str,
    help='host name to be connected (default: tdnode1)')
parser.add_argument(
    '-f',
    '--metadata',
    action='store',
    default=None,
    type=str,
    help='The meta data to execution procedure, if use -f, all other options invalid, Default is NULL')
parser.add_argument(
    '-d',
    '--db-name',
    action='store',
    default='test',
    type=str,
    help='Database name to be created (default: test)')
parser.add_argument(
    '-t',
    '--num-of-tables',
    action='store',
    default=10,
    type=int,
    help='Number of tables (default: 10000)')
parser.add_argument(
    '-T',
    '--num-of-threads',
    action='store',
    default=10,
    type=int,
    help='Number of rest threads (default: 10)')
parser.add_argument(
    '-c',
    '--config-dir',
    action='store',
    default='/etc/taos/',
    type=str,
    help='Configuration directory. (Default is /etc/taos/)')
parser.add_argument(
    '-a',
    '--replica',
    action='store',
    default=100,
    type=int,
    help='Set the replica parameters of the database  (default: 1, min: 1, max: 3)')
parser.add_argument(
    '-b',
    '--column-type',
    action='store',
    default='int',
    type=str,
    help='the data_type of columns (default: TINYINT,SMALLINT,INT,BIGINT,FLOAT,DOUBLE,BINARY,NCHAR,BOOL,TIMESTAMP)')
parser.add_argument(
    '-l',
    '--num-of-cols',
    action='store',
    default=10,
    type=int,
    help='The number of columns per record (default: 10)')
parser.add_argument(
    '-n',
    '--num-of-rows',
    action='store',
    default=1000,
    type=int,
    help='Number of subtales per stable (default: 1000)')
parser.add_argument(
    '-O',
    '--disorder-ratio',
    action='store',
    default=0,
    type=int,
    help=' (0: in order, > 0: disorder ratio, default: 0)')
parser.add_argument(
    '-R',
    '--disorder-range',
    action='store',
    default=0,
    type=int,
    help='Out of order datas range, ms (default: 1000)')
parser.add_argument(
    '-w',
    '--char-type-length',
    action='store',
    default=16,
    type=int,
    help='Out of order datas range, ms (default: 16)')
    
args = parser.parse_args()
taosdemox = taosdemoxWrapper(args.host_name, args.metadata, args.db_name, args.num_of_tables, 
        args.num_of_threads, args.config_dir, args.replica, args.column_type, args.num_of_cols, 
        args.num_of_rows, args.disorder_ratio, args.disorder_range, args.char_type_length)
taosdemox.run()