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

from taostest import TDCase, T

class TestTs5364(TDCase):
    def init(self):
        pass

    def prepare_data(self):
        """
        Prepares the data for testing by executing SQL statements.
        Drops the 'test' database if it exists, creates a new 'test' database,
        and creates two tables 'test.stb' and 'test.ctb' with specified columns and tags.
        Inserts a row into the 'test.ctb' table.
        """
        self.tdSql.execute('drop database if exists test;')
        self.tdSql.execute('create database if not exists test;')
        self.tdSql.execute('use test;')
        self.tdSql.execute('CREATE TABLE test.stb (ts TIMESTAMP,c0 int) TAGS (t0 tinyint);')
        self.tdSql.execute('CREATE TABLE test.ctb using stb tags (1);')
        self.tdSql.execute('insert into ctb values (now, 1);')
    
    def false_check(self):
        """
        This method performs a series of SQL queries and error checks.
        It queries the 'test.stb' and 'test.ctb' tables, then intentionally
        raises errors by querying non-existent tables 'test.stb1' and 'test.ctb1'.
        """
        self.tdSql.query('select * from test.stb;')
        self.tdSql.query('select * from test.ctb;')
        self.tdSql.error('select * from test.stb1;')
        self.tdSql.error('select * from test.ctb1;')
        self.tdSql.execute('alter local "queryTableNotExistAsEmpty true"')
        self.tdSql.query('select * from test.stb;')
        self.tdSql.query('select * from test.ctb;')
        self.tdSql.query('select * from test.stb1;')
        self.tdSql.query('select * from test.ctb1;')
        
    def true_check(self):
        """
        This method performs a series of SQL queries and error checks.
        It queries data from multiple tables and executes an alter statement.
        It also checks for errors in specific queries.
        """
        self.tdSql.query('select * from test.stb;')
        self.tdSql.query('select * from test.ctb;')
        self.tdSql.query('select * from test.stb1;')
        self.tdSql.query('select * from test.ctb1;')
        self.tdSql.execute('alter local "queryTableNotExistAsEmpty false"')
        self.tdSql.query('select * from test.stb;')
        self.tdSql.query('select * from test.ctb;')
        self.tdSql.error('select * from test.stb1;')
        self.tdSql.error('select * from test.ctb1;')
    
    def run(self):
        self.prepare_data()
        self.false_check()
        self.true_check()
        
    def cleanup(self):
        """
        Performs cleanup operations after the test case execution.
        alter local "queryTableNotExistAsEmpty false" anyway.
        """
        self.tdSql.execute('alter local "queryTableNotExistAsEmpty false"')

    def desc(self) -> str:
        case_description = """
            bug-ts5364
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Query