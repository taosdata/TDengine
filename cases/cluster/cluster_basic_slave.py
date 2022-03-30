###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
from http import client
import os
import random
from ssl import PROTOCOL_TLS_SERVER
import time
import taos
import copy
import datetime
from itertools import product
from itertools import combinations
import subprocess
import logging
from taostest import TDCase
from distutils.log import warn as printf
from queryutil.createdata import *
from queryutil.where import *
from itertools import product
from itertools import combinations
import subprocess
from taostest.util.file import dict2file
from taostest.util.remote import Remote
import subprocess
import threading


class TestCluster(TDCase):

    def init(self):
        self._ts = 1420041600000  # 2015-01-01 00:00:00  this is begin time for first record
        self._ts_step = 1
        self._row_nums = 1000000
        self._col_nums = 1000  # col types is float
        self._stables_nums = 100
        self._table_nums = 100
        self._per_tables_of_stables = 1000
        self._tags_nums = 10
        self._replica = 3
        self._db_nums = 100
        self._alter_times = 1000
        self._dbs = ["db_%d"%db_num for db_num in range(self._db_nums)]
        self._tags = ["tags_%d"%tag_num for tag_num in range(self._tags_nums)]
        self._stablenames = ["table_%d"%table_num for table_num in range(self._stables_nums)]
        print(self._dbs)

    def get_db_name(self):
        pass

    def get_table_name(self):
        pass

    def get_tag_name(self):
        pass

    def create_db_tables(self):
        pass

    def generate_records(self, batch=False):
        pass

    def insert_per_rows(self, db_name, table_name):
        pass

    def batch_insert_rows(self, db_name, table_name):
        pass

    def run(self):
        pass

    def cleanup(self):
        pass

    def author(self):
        '''
        abstract about author
        '''
        return "wenzhouwww"

    def tags(self):
        '''
        set tags
        '''
        return "cluster", ""

    def desc(self) -> str:
        case_description = '''
            [test]<wenzhouwww> test case for loop kill and start TDengine ;
        '''
        return case_description
