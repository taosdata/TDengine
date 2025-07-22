from wsgiref.headers import tspecials
from new_test_framework.utils import tdLog, tdSql

DBNAME = "db"

class TestLogicalOperators:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.rowNum = 10
        cls.batchNum = 5
        cls.ts = 1537146000000

    def test_logical_operators(self, dbname= DBNAME):
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
        tdSql.prepare()

        tdSql.execute(f'''create table {dbname}.tb (ts timestamp, v int, f float, b varchar(8))''')
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:00:00', 1, 2.0, 't0')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:01:00', 11, 12.1, 't0')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:02:00', 21, 22.2, 't0')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:03:00', 31, 32.3, 't0')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:04:00', 41, 42.4, 't0')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:05:00', 51, 52.5, 't1')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:06:00', 61, 62.6, 't1')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:07:00', 71, 72.7, 't1')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:08:00', 81, 82.8, 't1')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:09:00', 91, 92.9, 't1')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:00:00',101,112.9, 't2')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:01:00',111,112.1, 't2')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:02:00',121,122.2, 't2')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:03:00',131,132.3, 't2')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:04:00',141,142.4, 't2')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:05:00',151,152.5, 't3')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:06:00',161,162.6, 't3')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:07:00',171,172.7, 't3')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:08:00',181,182.8, 't3')")
        tdSql.execute(f"insert into {dbname}.tb values('2024-07-04 10:09:00',191,192.9, 't3')")
        #test for operator and
        tdSql.query('''select 
`T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` > 0
  and `T_9048C6F41B2A45CE94FF3`.`f` > `T_9048C6F41B2A45CE94FF3`.`v`
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        #test for operator or
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` > 0
  or `T_9048C6F41B2A45CE94FF3`.`f` > `T_9048C6F41B2A45CE94FF3`.`v`
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        #test for operator in
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` in (1)
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000;''')
        tdSql.checkRows(10)
        #test for operator not
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  not `T_9048C6F41B2A45CE94FF3`.`v` > 0
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        #test for operator between and
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` between 1 and 200
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        #test for operator is null
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` is null
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        #test for operator is not null
        tdSql.query('''select 
  `T_9048C6F41B2A45CE94FF3`.`ts` as `__fcol_0`,
  `T_9048C6F41B2A45CE94FF3`.`v` as `__fcol_1`,
  `T_9048C6F41B2A45CE94FF3`.`f` as `__fcol_2`,
  `T_9048C6F41B2A45CE94FF3`.`b` as `__fcol_3`,
  `T_9048C6F41B2A45CE94FF3`.`v` is not null
from `db`.`tb` as `T_9048C6F41B2A45CE94FF3`
limit 5000''')
        tdSql.checkRows(10)
        tdLog.success(f"{__file__} successfully executed")
