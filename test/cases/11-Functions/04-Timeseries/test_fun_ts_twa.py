from new_test_framework.utils import tdLog, tdSql

import os
import platform
import subprocess

class TestTwa:
    updatecfgDict = {"maxTablesPerVnode":2 ,"minTablesPerVnode":2,"tableIncStepPerVnode":2 }

    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")
        #tdSql.init(conn.cursor(), logSql)
        cls.vnode_disbutes = None
        cls.ts = 1537146000000
        cls.tb_nums = 20
        cls.row_nums = 100
        cls.time_step = 1000

    def prepare_datas_of_distribute(self, dbname="testdb"):

        # prepate datas for  20 tables distributed at different vgroups
        tdSql.execute(f"create database if not exists {dbname} keep 3650 duration 100 vgroups 5")
        tdSql.execute(
            f'''create table {dbname}.stb1
            (ts timestamp, c1 int, c2 bigint, c3 smallint, c4 tinyint, c5 float, c6 double, c7 bool, c8 binary(16),c9 nchar(32), c10 timestamp,c11 int UNSIGNED, c12 bigint UNSIGNED,  c13 smallint UNSIGNED, c14 tinyint UNSIGNED)
            tags (t0 timestamp, t1 int, t2 bigint, t3 smallint, t4 tinyint, t5 float, t6 double, t7 bool, t8 binary(16),t9 nchar(32))
            '''
        )

        for i in range(self.tb_nums):
            tdSql.execute(f'create table {dbname}.ct{i+1} using {dbname}.stb1 tags ( now(), {1*i}, {11111*i}, {111*i}, {1*i}, {1.11*i}, {11.11*i}, {i%2}, "binary{i}", "nchar{i}" )')
            ts = self.ts
            sql = f"insert into {dbname}.ct{i+1} values"
            for j in range(self.row_nums):
                ts+=j*self.time_step
                sql += f" ({ts}, 1, 11111, 111, 1, 1.11, 11.11, 2, 'binary{j}', 'nchar{j}', now()+{1*j}a, 1, 11111, 111, 1 )"
            tdSql.execute(sql)

        tdSql.execute(f"insert into {dbname}.ct1 values (now()-810d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()-400d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ")
        tdSql.execute(f"insert into {dbname}.ct1 values (now()+90d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL , NULL, NULL, NULL, NULL ) ")

        tdLog.info(" prepare data for distributed_aggregate done! ")

    def twa_support_types(self, dbname="testdb"):
        tdSql.query(f"desc {dbname}.stb1 ")
        schema_list = tdSql.queryResult
        for col_type in schema_list:
            if col_type[1] in ["TINYINT" ,"SMALLINT","BIGINT" ,"INT","FLOAT","DOUBLE","TINYINT UNSIGNED" ,"SMALLINT UNSIGNED","BIGINT UNSIGNED" ,"INT UNSIGNED"]:
                tdSql.query(f"select twa({col_type[0]}) from {dbname}.stb1 partition by tbname ")
            else:
                tdSql.error(f"select twa({col_type[0]}) from {dbname}.stb1 partition by tbname ")

    def check_distribute_datas(self, dbname="testdb"):
        # get vgroup_ids of all
        tdSql.query(f"show {dbname}.vgroups ")
        vgroups = tdSql.queryResult

        vnode_tables={}

        for vgroup_id in vgroups:
            vnode_tables[vgroup_id[0]]=[]

        # check sub_table of per vnode ,make sure sub_table has been distributed
        tdSql.query(f"select * from information_schema.ins_tables where db_name = '{dbname}' and table_name like 'ct%'")
        table_names = tdSql.queryResult
        tablenames = []
        for table_name in table_names:
            vnode_tables[table_name[6]].append(table_name[0])
        self.vnode_disbutes = vnode_tables

        count = 0
        for k ,v in vnode_tables.items():
            if len(v)>=2:
                count+=1
        if count < 2:
            tdLog.exit(" the datas of all not satisfy sub_table has been distributed ")

    def distribute_twa_query(self, dbname="testdb"):
        # basic filter
        tdSql.query(f"select twa(c1) from {dbname}.ct1  ")
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c2) from {dbname}.stb1 group by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,11111.000000000)

        tdSql.query(f"select twa(c1+c2) from {dbname}.stb1 partition by tbname ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c11) from {dbname}.ct1  ")
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)

        tdSql.query(f"select twa(c12) from {dbname}.stb1 group by tbname ")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,11111.000000000)

        tdSql.query(f"select twa(c11+c12) from {dbname}.stb1 partition by tbname ")
        tdSql.checkData(0,0,11112.000000000)

        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by t1")
        tdSql.checkRows(self.tb_nums)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c13) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c13) from {dbname}.stb1 group by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c14) from {dbname}.stb1 partition by tbname  ")
        tdSql.checkRows(self.tb_nums)
        
        tdSql.query(f"select twa(c14) from {dbname}.stb1 group by tbname  ")
        tdSql.checkRows(self.tb_nums)

        # union all
        tdSql.query(f"select twa(c1) from {dbname}.stb1 partition by tbname union all select twa(c1) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.checkData(0,0,1.000000000)
        tdSql.query(f"select twa(c11) from {dbname}.stb1 partition by tbname union all select twa(c11) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.checkData(0,0,1.000000000)
        
        tdSql.query(f"select twa(c2) from {dbname}.stb1 partition by tbname union all select twa(c2) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c3) from {dbname}.stb1 partition by tbname union all select twa(c3) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c4) from {dbname}.stb1 partition by tbname union all select twa(c4) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c12) from {dbname}.stb1 partition by tbname union all select twa(c12) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c13) from {dbname}.stb1 partition by tbname union all select twa(c13) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)
        tdSql.query(f"select twa(c14) from {dbname}.stb1 partition by tbname union all select twa(c14) from {dbname}.stb1 partition by tbname ")
        tdSql.checkRows(40)

        # join

        tdSql.execute(" create database if not exists db ")
        tdSql.execute(" use db ")
        tdSql.execute(" create stable db.st (ts timestamp , c1 int ,c2 float) tags(t1 int) ")
        tdSql.execute(" create table db.tb1 using db.st tags(1) ")
        tdSql.execute(" create table db.tb2 using db.st tags(2) ")

        values = "values"
        for i in range(10):
            ts = i*10 + self.ts
            values += f" ({ts},{i},{i}.0)"
        tdSql.execute(f"insert into db.tb1 {values} db.tb2 {values}")

        tdSql.query(f"select twa(tb1.c1), twa(tb2.c2) from db.tb1 tb1, db.tb2 tb2 where tb1.ts=tb2.ts ")
        tdSql.checkRows(1)
        tdSql.checkData(0,0,4.500000000)
        tdSql.checkData(0,1,4.500000000)

        # mixup with other functions
        tdSql.query(f"select twa(c1),twa(c2),max(c1),elapsed(ts) from {dbname}.ct1 ")
        tdSql.checkData(0,0,1.000000000)
        tdSql.checkData(0,1,11111.000000000)
        tdSql.checkData(0,2,1)

    def check_week_sliding_interval_across_blocks(self, dbname="twa_week_sliding_blocks"):
        tdSql.execute("set first_day_of_week 0")
        tdSql.execute(f"drop database if exists {dbname}")
        tdSql.execute(f"create database {dbname}")
        tdSql.execute(f"create table {dbname}.t(ts timestamp, v int)")

        tdSql.execute(
            f"insert into {dbname}.t values "
            "('2021-08-15 01:00:00.000', 1) "
            "('2021-08-27 01:46:40.000', 10) "
            "('2021-08-28 01:46:40.000', 20) "
            "('2021-08-29 01:46:40.000', 30) "
            "('2021-08-30 01:46:40.000', 40) "
            "('2021-08-31 01:46:40.000', 50)"
        )
        tdSql.execute(f"flush database {dbname}")
        tdSql.execute(
            f"insert into {dbname}.t values "
            "('2021-08-31 09:56:40.000', 60) "
            "('2021-09-01 01:46:40.000', 70) "
            "('2021-09-05 01:46:40.000', 80) "
            "('2021-09-11 20:56:40.000', 90) "
            "('2021-09-13 06:16:40.000', 100)"
        )

        sql = f"select _wstart, _wend, twa(v) from {dbname}.t interval(3w) sliding(1w)"
        tdLog.info(sql)
        taos = os.path.join(self.taos_bin_path, "taos")
        result = subprocess.run(
            [taos, "-c", self.cfg_path, "-s", f"set first_day_of_week 0; {sql};"],
            text=True,
            capture_output=True,
            timeout=30,
        )
        output = result.stdout + result.stderr
        if "DB error" in output or "Invalid timeline" in output:
            raise Exception(output)
        if result.returncode != 0:
            raise Exception(output)

    #
    # ------------------ main ------------------
    #
    def test_func_ts_twa(self):
        """ Fun: twa()

        1. Basic query for different params
        2. Query on super/child table
        3. Support data types
        4. Error cases
        5. Query with where condition
        6. Query with partition/group by
        7. Query with sub query
        8. Query with union
        9. Check null value

        Since: v3.0.0.0

        Labels: common,ci,integration,functional
        Jira: None

        History:     
            - 2025-9-29 Alex Duan Migrated from uncatalog/system-test/2-query/test_twa.py

        """

        self.prepare_datas_of_distribute()
        self.check_distribute_datas()
        self.check_week_sliding_interval_across_blocks()
        self.twa_support_types()
        self.distribute_twa_query()

        #tdSql.close()
