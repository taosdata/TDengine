from new_test_framework.utils import tdLog, tdSql

class TestConcat:

    def test_concat(self):
        """test concat function

        test concat function

        Since: v3.3.0.0

        Labels: concat

        History:
            - 2024-8-8 Youhao Li Created
            - 2025-5-08 Huo Hong Migrated to new test framework

        """
        self.dbname = 'db'
        self.tbname = f'{self.dbname}.tbconcat'
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database {self.dbname}')
        tdSql.execute(f'use {self.dbname}')
        tdSql.execute(f'create table if not exists {self.tbname}(ts timestamp, name varchar(20), name2 nchar(20))')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefg","你好")')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefgh","我好")')
        tdSql.execute(f'insert into {self.tbname} values(now(),"abcdefg", "")')
        tdSql.execute(f'select concat("你好",name2) from {self.tbname}')
        tdSql.execute(f'select concat(name2,"你好") from {self.tbname}')
        tdSql.execute(f'select concat(name2,"") from {self.tbname}')
        tdSql.execute(f'select concat("", name2) from {self.tbname}')

