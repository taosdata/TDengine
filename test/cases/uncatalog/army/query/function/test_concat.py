from new_test_framework.utils import tdLog, tdSql


class TestConcat:
    """Verify the concat function
    """
    
    def setup_class(cls):
        cls.dbname = 'db'
        cls.tbname = f'{cls.dbname}.tbconcat'
    def checkConcat(self):
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
    def test_concat(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        tdLog.debug(f"start to excute {__file__}")
        # check concat function
        self.checkConcat()
        tdLog.success(f"{__file__} successfully executed")