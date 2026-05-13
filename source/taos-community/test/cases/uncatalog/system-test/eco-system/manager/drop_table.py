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


from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import random
import time
import traceback


class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    # describe table   
    def describe_table(self, tbname):
        columns = []
        tags = []
        sql = f"describe {tbname}"
        row_cnt = tdSql.query(sql)
        for i in range(0, row_cnt):
            col_name  = tdSql.queryResult[i][0]
            type_name = tdSql.queryResult[i][3]
            if type_name == "TAG":
                tags.append(col_name)
            else:
                columns.append(col_name)

        return columns,tags
    
    # show tables
    def show_tables(self):
        sql = "show tables;"
        row_cnt = tdSql.query(sql)
        tables = []
        for i in range(0, row_cnt):
            tb_name  = tdSql.queryResult[i][0]
            tables.append(tb_name)
        
    
    # execute sql
    def execute(self, sql):
        try:
            tdSql.execute(sql, 3)
            tdLog.info(f" exec ok. {sql}")
        except:
            tdLog.info(f" exe failed. {sql}")
            traceback.print_exc()


     # query
    def query_table(self, columns, tags):
        if len(columns) < 5 :
            return 
        if len(tags) < 5:
            return 

        sel_cols = random.sample(columns, random.randint(1,int(len(columns)-1)))
        sel_tags = random.sample(tags, random.randint(1, int(len(tags)-1)))

        field_cols = ",".join(sel_cols)
        field_tags = ",".join(sel_tags)

        #sql = f"select {field_cols},{field_tags} from meters ;"
        sql = f"select {field_cols},{field_tags} from meters"
        try:
            tdLog.info( " query sql:" + sql)
            tdSql.query("select * from meters limit 1")
        except:
            tdLog.info( " query failed :" + sql)
            traceback.print_exc()        
    
    # change table schema
    def drop_table(self, change_cnt):
        # init

        tables = self.show_tables()


        for i in range(change_cnt):
            col_idx = random.randint(0, ncol - 1)
            tag_idx = random.randint(0, ntag - 1)

            cols = list(self.column_dict.keys())
            tags = list(self.tag_dict.keys())

            # column 
            key = cols[col_idx]
            value = self.column_dict[key]
            sql = f'alter table meters drop column {key}'
            self.execute(sql)
            sql = f'alter table meters add column {key} {value}'
            self.execute(sql)


            # column 
            key = tags[col_idx]
            value = self.tag_dict[key]
            sql = f'alter table meters drop tag {key}'
            self.execute(sql)
            sql = f'alter table meters add tag {key} {value}'
            self.execute(sql)

            # drop and rename
            if i % 5 == 0:
                # update columns
                #columns,tags = self.describe_table("meters")
                tdLog.info(f"  =======  describe table column count = {len(cols)} tags= {len(tags)}======")
                self.query_table(cols, tags)

    # run
    def run(self):
        # seed
        random.seed(int(time.time()))
        self.dbname = "schema_change"

        # switch db
        tdSql.execute(f"use {self.dbname};")

        # change meters
        self.drop_table(1000000)

        
    # stop
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())