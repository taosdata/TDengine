# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-
from new_test_framework.utils import tdLog, tdSql, tdDnodes, TDSetSql
import random
import os
import platform
import shutil
import subprocess
import tempfile
import time
import sys

import pytest

class PerfDB:
    def __init__(self):
        self.sqls = []
        self.spends = []

    # execute
    def execute(self, sql):
        print(f"  perfdb execute {sql}")
        stime = time.time()
        ret = tdSql.execute(sql, 1)
        spend = time.time() - stime

        self.sqls.append(sql)
        self.spends.append(spend)
        return ret

    # query
    def query(self, sql):
        print(f"  perfdb query {sql}")
        start = time.time()
        ret = tdSql.query(sql, None, 1)
        spend = time.time() - start
        self.sqls.append(sql)
        self.spends.append(spend)
        return ret
    

class TestUdfpyMain:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)
        cls.setsql = TDSetSql()

        # udf path
        cls.udf_path = os.path.dirname(os.path.realpath(__file__)) + "/udfpy"


        cls.column_dict = {
            'ts': 'timestamp',
            'col1': 'tinyint',
            'col2': 'smallint',
            'col3': 'int',
            'col4': 'bigint',
            'col5': 'tinyint unsigned',
            'col6': 'smallint unsigned',
            'col7': 'int unsigned',
            'col8': 'bigint unsigned',
            'col9': 'float',
            'col10': 'double',
            'col11': 'bool',
            'col12': 'varchar(120)',
            'col13': 'nchar(100)',
        }
        cls.tag_dict = {
            't1': 'tinyint',
            't2': 'smallint',
            't3': 'int',
            't4': 'bigint',
            't5': 'tinyint unsigned',
            't6': 'smallint unsigned',
            't7': 'int unsigned',
            't8': 'bigint unsigned',
            't9': 'float',
            't10': 'double',
            't11': 'bool',
            't12': 'varchar(120)',
            't13': 'nchar(100)',         
        }

    def set_stb_sql(self,stbname,column_dict,tag_dict):
        column_sql = ''
        tag_sql = ''
        for k,v in column_dict.items():
            column_sql += f"{k} {v}, "
        for k,v in tag_dict.items():
            tag_sql += f"{k} {v}, "
        create_stb_sql = f'create stable {stbname} ({column_sql[:-2]}) tags ({tag_sql[:-2]})'
        return create_stb_sql
    
    # create stable and child tables
    def create_table(self, stbname, tbname, count):
        tdSql.execute("create database db wal_retention_period 4")
        tdSql.execute('use db')
        self.child_count = count
        self.stbname = stbname
        self.tbname  = tbname
        
        # create stable
        create_table_sql = self.set_stb_sql(stbname, self.column_dict, self.tag_dict)
        tdSql.execute(create_table_sql)

        batch_size = 1000
        # create child table
        for i in range(count):
            ti = i % 128
            tags = f'{ti},{ti},{i},{i},{ti},{ti},{i},{i},{i}.000{i},{i}.000{i},true,"var{i}","nch{i}"'
            sql  = f'create table {tbname}{i} using {stbname} tags({tags});'
            tdSql.execute(sql)            
            if i % batch_size == 0:
               tdLog.info(f" create child table {i} ...")

        tdLog.info(f" create {count} child tables ok.")

    # create with dicts
    def create_sf_dicts(self, dicts, filename):
        for fun_name, out_type in dicts.items():
            sql = f' create function {fun_name} as "{self.udf_path}/{filename}" outputtype {out_type} language "Python" '
            tdSql.execute(sql)
            tdLog.info(sql)

    # create_udfpy_function
    def create_scalar_udfpy(self):
        # scalar funciton
        self.scalar_funs = {
            'sf0': 'timestamp',    
            'sf1': 'tinyint',
            'sf2': 'smallint',
            'sf3': 'int',
            'sf4': 'bigint',
            'sf5': 'tinyint unsigned',
            'sf6': 'smallint unsigned',
            'sf7': 'int unsigned',
            'sf8': 'bigint unsigned',
            'sf9': 'float',
            'sf10': 'double',
            'sf11': 'bool',
            'sf12': 'varchar(120)',
            'sf13': 'nchar(100)'
        }
        # agg function
        self.agg_funs = {
            'af1': 'tinyint',
            'af2': 'smallint',
            'af3': 'int',
            'af4': 'bigint',
            'af5': 'tinyint unsigned',
            'af6': 'smallint unsigned',
            'af7': 'int unsigned',
            'af8': 'bigint unsigned',
            'af9': 'float',
            'af10': 'double',
            'af11': 'bool',
            'af12': 'varchar(120)',
            'af13': 'nchar(100)',
            'af14': 'timestamp'            
        }

        # multi_args
        self.create_sf_dicts(self.scalar_funs, "sf_origin.py")
        fun_name = "sf_multi_args"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "binary(1024)")

        # all type check null
        for col_name, col_type in self.column_dict.items():
             self.create_udf_sf(f"sf_null_{col_name}", "sf_null.py", col_type)

        # concat
        fun_name = "sf_concat_var"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "varchar(1024)")
        fun_name = "sf_concat_nch"
        self.create_udf_sf(fun_name, f'{fun_name}.py', "nchar(1024)")
             

    # fun_name == fun_name.py
    def create_udf_sf(self, fun_name, file_name, out_type):
        sql = f'create function {fun_name} as "{self.udf_path}/{file_name}" outputtype {out_type} language "Python" '
        tdSql.execute(sql)
        tdLog.info(sql)

    def create_udf_af(self, fun_name, file_name, out_type, bufsize):
        sql = f'create aggregate function {fun_name} as "{self.udf_path}/{file_name}" outputtype {out_type} bufsize {bufsize} language "Python" '
        tdSql.execute(sql)
        tdLog.info(sql)


    # sql1 query result eual with sql2
    def verify_same_result(self, sql1, sql2):
        # query
        result1 = tdSql.getResult(sql1)
        tdSql.query(sql2)
        
        for i, row in enumerate(result1):
            for j , val in enumerate(row):
                tdSql.checkData(i, j, result1[i][j])

    # same value like select col1, udf_fun1(col1) from st
    def verify_same_value(self, sql, col=0):
        tdSql.query(sql)
        nrows = tdSql.getRows()
        for i in range(nrows):
            val = tdSql.getData(i, col)
            tdSql.checkData(i, col + 1, val)

    # verify multi values
    def verify_same_multi_values(self, sql):
        tdSql.query(sql)
        nrows = tdSql.getRows()
        for i in range(nrows):
            udf_val = tdSql.getData(i, 0)
            vals = udf_val.split(',')
            for j,val in enumerate(vals, 1):
                tdSql.checkData(i, j, val)
            
    # query multi-args
    def query_multi_args(self):   
        cols = list(self.column_dict.keys()) + list(self.tag_dict.keys())
        cols.remove("col13")
        cols.remove("t13")
        cols.remove("ts")
        ncols = len(cols)
        print(cols)
        for i in range(2, ncols):
            sample = random.sample(cols, i)
            print(sample)
            cols_name = ','.join(sample)
            sql = f'select sf_multi_args({cols_name}),{cols_name} from {self.stbname} limit 10'
            self.verify_same_multi_values(sql)
            tdLog.info(sql)

    
    # query_udfpy
    def query_scalar_udfpy(self):
        # col
        for col_name, col_type in self.column_dict.items():
           for fun_name, out_type in self.scalar_funs.items():
               if col_type == out_type :                   
                    sql = f'select {col_name}, {fun_name}({col_name}) from {self.stbname} limit 10'
                    tdLog.info(sql)
                    self.verify_same_value(sql)
                    sql = f'select * from (select {col_name} as a, {fun_name}({col_name}) as b from {self.stbname}  limit 100) order by b,a desc'
                    tdLog.info(sql)
                    self.verify_same_value(sql)

        # multi-args
        self.query_multi_args()       

        # all type check null
        for col_name, col_type in self.column_dict.items():             
             fun_name = f"sf_null_{col_name}"
             sql = f'select {fun_name}({col_name}) from {self.stbname}'
             tdSql.query(sql)
             if col_type != "timestamp":
                tdSql.checkData(0, 0, "None")
             else:
                val = tdSql.getData(0, 0)
                if val is not None:
                    tdLog.exit(f" check {sql} not expect None.")

        # concat - stable
        sql = f'select sf_concat_var(col12, t12), concat(col12, t12) from {self.stbname} limit 1000'
        self.verify_same_value(sql)
        sql = f'select sf_concat_nch(col13, t13), concat(col13, t13) from {self.stbname} limit 1000'
        self.verify_same_value(sql)

        # concat - child table
        sql = f'select sf_concat_var(col12, t12), concat(col12, t12) from {self.tbname}0 limit 1000'
        self.verify_same_value(sql)
        sql = f'select sf_concat_nch(col13, t13), concat(col13, t13) from {self.tbname}0 limit 1000'
        self.verify_same_value(sql)

        # single child
        sql = f'select sf_concat_nch(col13, t13) from {self.tbname}1'
        tdSql.query(sql)
        tdSql.checkRows(5000)


    # create aggregate 
    def create_aggr_udfpy(self):

        bufsize = 200 * 1024
        # all type check null
        for col_name, col_type in self.column_dict.items():
             self.create_udf_af(f"af_null_{col_name}", "af_null.py", col_type, bufsize)

        # min
        file_name = "af_min.py"
        fun_name = "af_min_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_min_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)

        # sum
        file_name = "af_sum.py"
        fun_name = "af_sum_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_sum_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)
        fun_name = "af_sum_bigint"
        self.create_udf_af(fun_name, file_name, f"bigint", bufsize)

        # count
        file_name = "af_count.py"
        fun_name = "af_count_float"
        self.create_udf_af(fun_name, file_name, f"float", bufsize)
        fun_name = "af_count_int"
        self.create_udf_af(fun_name, file_name, f"int", bufsize)
        fun_name = "af_count_bigint"
        self.create_udf_af(fun_name, file_name, f"bigint", bufsize)


    # query aggregate 
    def query_aggr_udfpy(self) :
        # all type check null
        for col_name, col_type in self.column_dict.items():
             fun_name = f"af_null_{col_name}"
             sql = f'select {fun_name}({col_name}) from {self.stbname}'
             tdSql.query(sql)
             if col_type != "timestamp":
                tdSql.checkData(0, 0, "None")
             else:
                val = tdSql.getData(0, 0)
                if val is not None:
                    tdLog.exit(f" check {sql} not expect None.")

        # min
        sql = f'select min(col3), af_min_int(col3) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select min(col7), af_min_int(col7) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select min(col9), af_min_float(col9) from {self.stbname}'
        self.verify_same_value(sql)

        # sum
        sql = f'select sum(col1), af_sum_int(col1) from d0'
        self.verify_same_value(sql)
        sql = f'select sum(col3), af_sum_bigint(col3) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select sum(col9), af_sum_float(col9) from {self.stbname}'
        self.verify_same_value(sql)

        # count
        sql = f'select count(col1), af_count_int(col1) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select count(col7), af_count_bigint(col7) from {self.stbname}'
        self.verify_same_value(sql)
        sql = f'select count(col8), af_count_float(col8) from {self.stbname}'
        self.verify_same_value(sql)
        tdSql.execute('create table t_empty (ts timestamp, v int)')
        tdSql.query('select count(*), af_count_int(v) from t_empty')
        tdSql.checkData(0, 0, 0)
        tdSql.checkData(0, 1, 0)
        # child
        sql = f'select count(col8), af_count_float(col8) from {self.tbname}0'
        self.verify_same_value(sql)
        sql = f'select af_count_bigint(col7) from {self.tbname}1'
        tdSql.query(sql)
        tdSql.checkData(0, 0, 5000)

        # nest
        sql = f'select a+1000,b+1000 from (select count(col8) as a, af_count_float(col8) as b from {self.stbname})'
        self.verify_same_value(sql)
        # group by
        sql = f'select a+1000,b+1000 from (select count(col8) as a, af_count_float(col8) as b from {self.stbname} group by tbname)'
        self.verify_same_value(sql)
        # two filed expr
        sql = f'select sum(col1+col2),af_sum_float(col1+col2) from {self.stbname};'
        self.verify_same_value(sql)
        # interval
        sql = f'select af_sum_float(col2+col3),sum(col3+col2) from {self.stbname} interval(1s)'
        self.verify_same_value(sql)

    
    # insert to child table d1 data
    def insert_data(self, tbname, rows):
        ts = 1670000000000
        values = ""
        batch_size = 500
        child_name = ""
        for i in range(self.child_count):
            for j in range(rows):
                tj = j % 128
                cols = f'{tj},{tj},{j},{j},{tj},{tj},{j},{j},{j}.000{j},{j}.000{j},true,"var{j}","nch{j}涛思数据codepage is utf_32_le"'
                value = f'({ts+j},{cols})' 
                if values == "":
                    values = value
                else:
                    values += f",{value}"
                if j % batch_size == 0 or j + 1 == rows:
                   sql = f'insert into {tbname}{i} values {values};' 
                   tdSql.execute(sql)
                   tdLog.info(f" child table={i} rows={j} insert data.")
                   values = ""

        # partial columns update
        sql = f'insert into {tbname}0(ts, col1, col9, col11) values(now, 100, 200, 0)'
        tdSql.execute(sql)
        sql = f'insert into {tbname}0(ts, col2, col5, col8) values(now, 100, 200, 300)'
        tdSql.execute(sql)
        sql = f'insert into {tbname}0(ts, col3, col7, col13) values(now, null, null, null)'
        tdSql.execute(sql)        
        sql = f'insert into {tbname}0(ts) values(now)'
        tdSql.execute(sql)
        tdLog.info(f" insert {rows} to child table {self.child_count} .")

   
    # create stream
    def create_stream(self):
        sql = f"create stream ma  into sta subtable(concat('sta_',tbname)) \
            as select _wstart,count(col1),af_count_bigint(col1) from {self.stbname} partition by tbname interval(1s);"
        tdSql.execute(sql)
        tdLog.info(sql)

    #  query stream
    def verify_stream(self):
        sql = f"select * from sta limit 10"
        self.verify_same_value(sql, 1)

    # create tmq
    def create_tmq(self):
        sql = f"create topic topa as select concat(col12,t12),sf_concat_var(col12,t12) from {self.stbname};"   
        tdSql.execute(sql)
        tdLog.info(sql)

    def install_taospy(self):
        tdLog.info("install taospyudf...")
        # packs = ["taospyudf"]
        # for pack in packs:
        #     subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-i', 'https://pypi.org/simple', '-U', pack])
        # tdLog.info("call ldconfig...")
        # os.system("ldconfig")
        tdLog.info("libtaospyudf now ships with TDengine, no pip install needed.")
        tdLog.info("install taospyudf successfully.")

    # run
    def test_udf_main(self):
        """Udf basic for python

        1. Use built-in taospyudf plugin from TDengine installation
        2. Create stable and child tables
        3. Create scalar udfpy functions concat
        4. Create aggregate udfpy functions min, sum, count
        5. Insert data to child tables
        6. Query scalar udfpy functions
        7. Query aggregate udfpy functions
        8. Verify all operations are successful

        Since: v3.0.0.0

        Labels: common,ci,integration,functional

        Jira: None

        History:
            - 2025-11-02 Alex Duan Migrated from uncatalog/system-test/0-others/test_udfpy_main.py
    
        """    
        self.install_taospy()

        # var
        stable = "meters"
        tbname = "d"
        count = 10
        rows =  5000
        # do 
        self.create_table(stable, tbname, count)

        # create
        self.create_scalar_udfpy()
        self.create_aggr_udfpy()

        # create stream
        #newstm self.create_stream()

        # create tmq
        self.create_tmq()

        # insert data
        self.insert_data(tbname, rows)

        # query
        self.query_scalar_udfpy()
        self.query_aggr_udfpy()

        # show performance

    # --- missing taosudf companion ---
    def _udfd_candidates(self):
        # taosLocateCompanionExec() looks in the taosd directory first and then
        # in the fixed install directories.
        is_win = platform.system().lower() == "windows"
        name = "taosudf.exe" if is_win else "taosudf"
        app_dir = os.path.dirname(tdDnodes.dnodes[0].binPath)
        dirs = [app_dir]
        dirs += ["C:\\TDengine"] if is_win else ["/usr/bin", "/usr/local/taos/bin"]
        candidates = []
        for d in dirs:
            path = os.path.join(d, name)
            if os.path.isfile(path) and path not in candidates:
                candidates.append(path)
        return candidates

    @staticmethod
    def _kill_leftover_udfd():
        # A udfd left behind by an earlier case still holds the binary open on
        # Windows and would make the rename below fail.
        if platform.system().lower() == "windows":
            os.system("taskkill /f /im taosudf.exe >nul 2>&1")
        else:
            os.system("pkill -9 taosudf 2>/dev/null || true")

    def _hide_udfd(self, candidates):
        hidden = []
        for path in candidates:
            backup = path + ".udfd_missing_bak"
            if os.path.exists(backup):
                os.remove(backup)
            os.replace(path, backup)
            hidden.append((path, backup))
        return hidden

    @staticmethod
    def _restore_udfd(hidden):
        for path, backup in hidden:
            if os.path.exists(backup):
                os.replace(backup, path)

    def prepare_missing_udfd(self):
        tdSql.execute("drop database if exists udf_missing_db")
        tdSql.execute("create database udf_missing_db")
        tdSql.execute("create table udf_missing_db.t1 (ts timestamp, c1 int)")
        tdSql.execute("insert into udf_missing_db.t1 values (1720000000000, 3)")

    @staticmethod
    def make_stub_udf_lib():
        # CVE-2023-38502 requires the UDF library to exist, carry the platform
        # suffix and start with a valid ELF/PE magic number. The library is
        # never loaded because udfd cannot start, so a 16-byte stub is enough.
        stub_dir = tempfile.mkdtemp(prefix="udf_missing_")
        if platform.system().lower() == "windows":
            path = os.path.join(stub_dir, "libudf_missing_companion.dll")
            magic = b"MZ"
        else:
            path = os.path.join(stub_dir, "libudf_missing_companion.so")
            magic = b"\x7fELF"
        with open(path, "wb") as f:
            f.write(magic + b"\x00" * (16 - len(magic)))
        return stub_dir, path

    def do_missing_udfd(self):
        if tdDnodes.dnodes[0].remoteIP != "":
            pytest.skip("case requires a local deployment to hide the taosudf binary")

        candidates = self._udfd_candidates()
        if not candidates:
            tdLog.info("taosudf is already absent from every search path")

        tdDnodes.stop(1)
        self._kill_leftover_udfd()

        hidden = []
        stub_dir, lib = self.make_stub_udf_lib()
        try:
            # --- make the companion binary unavailable ---
            try:
                hidden = self._hide_udfd(candidates)
            except OSError as err:
                self._restore_udfd(hidden)
                hidden = []
                tdDnodes.start(1)
                pytest.skip(f"cannot make taosudf unavailable in this environment: {err}")

            # --- taosd has to come online and stay alive without udfd ---
            tdDnodes.start(1)
            tdSql.query("select server_version()")
            tdSql.checkRows(1)
            tdLog.info("taosd stays online without taosudf")

            # --- a UDF call must report an error instead of crashing taosd ---
            tdSql.execute("drop function if exists udf_no_companion")
            tdSql.execute("create function udf_no_companion as '%s' outputtype int" % lib.replace("\\", "/"))
            tdSql.error("select udf_no_companion(c1) from udf_missing_db.t1")

            tdSql.query("select server_version()")
            tdSql.checkRows(1)
            tdLog.info("taosd is still alive after the failed UDF call")
        finally:
            # --- always restore the deployment and leave a running server ---
            self._restore_udfd(hidden)
            shutil.rmtree(stub_dir, ignore_errors=True)
            tdDnodes.stop(1)
            tdDnodes.start(1)

        tdSql.query("select server_version()")
        tdSql.checkRows(1)
        tdSql.execute("drop function if exists udf_no_companion")

        print("missing taosudf startup ....... [ passed ]")

    def test_udf_missing_udfd(self):
        """Start taosd without the taosudf companion binary

        1. Stop taosd and make taosudf unavailable in every search path
        2. Start taosd: it must come online and stay alive without udfd
        3. Call a UDF: it must report an error instead of crashing taosd
        4. Restore taosudf, restart taosd and verify the server is usable

        Catalog:
            - UDF

        Since: v3.4.3.0

        Labels: common,ci

        Jira: 7167952520

        History:
            - 2026-11-18 Alex Duan Add regression coverage for a missing taosudf
        """
        self.prepare_missing_udfd()
        self.do_missing_udfd()

