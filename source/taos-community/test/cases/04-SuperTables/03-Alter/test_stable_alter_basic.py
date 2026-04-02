import os
import time
import threading
import shutil

from new_test_framework.utils        import tdLog, tdSql, tdStream, sc, clusterComCheck, constant
from new_test_framework.utils.common import tdCom
from new_test_framework.utils.sqlset import TDSetSql


class TestStableAlterBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        # init
        cls.setsql = TDSetSql()
        cls.fname = __file__ + '.tmp.sql'
        cls.dbname = 'db1'
        cls.ntbname = 'ntb'
        cls.stbname = 'stb'
        cls.stbnum = 10
        cls.ntbnum = 10
        cls.colnum = 52
        cls.tagnum = 15
        cls.collen = 320
        cls.colnum_modify = 40
        cls.tagnum_modify = 40
        cls.collen_old_modify = 160
        cls.collen_new_modify = 455
        cls.taglen_old_modify = 80
        cls.taglen_new_modify = 155
        cls.binary_length = 20 # the length of binary for column_dict
        cls.nchar_length = 20  # the length of nchar for column_dict
        cls.threadnum = 2
        cls.column_dict = {
            'ts'  : 'timestamp',
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
            'col12': f'binary({cls.binary_length})',
            'col13': f'nchar({cls.nchar_length})'
        }
        cls.tag_dict = {
            'ts_tag'  : 'timestamp',
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
            't12': f'binary({cls.binary_length})',
            't13': f'nchar({cls.nchar_length})'
        }
        cls.tag_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        cls.tbnum = 1
        cls.values_list = [
            f'now,1,2,3,4,5,6,7,8,9.9,10.1,true,"abcd","涛思数据"'
        ]
        cls.column_add_dict = {
            'col_time'      : 'timestamp',
            'col_tinyint'   : 'tinyint',
            'col_smallint'  : 'smallint',
            'col_int'       : 'int',
            'col_bigint'    : 'bigint',
            'col_untinyint' : 'tinyint unsigned',
            'col_smallint'  : 'smallint unsigned',
            'col_int'       : 'int unsigned',
            'col_bigint'    : 'bigint unsigned',
            'col_bool'      : 'bool',
            'col_float'     : 'float',
            'col_double'    : 'double',
            'col_binary'    : f'binary({constant.BINARY_LENGTH_MAX})',
            'col_nchar'     : f'nchar({constant.NCAHR_LENGTH_MAX})'

        }

    def alter_stable_check(self):
        tdSql.prepare()
        tdSql.execute(self.setsql.set_create_stable_sql(self.stbname,self.column_dict,self.tag_dict))
        tdSql.execute(self.setsql.set_create_normaltable_sql(self.ntbname,self.column_dict))
        for i in self.values_list:
            tdSql.execute(f'insert into {self.ntbname} values({i})')
        for i in range(self.tbnum):
            tdSql.execute(f'create table {self.stbname}_{i} using {self.stbname} tags({self.tag_list[i]})')
            for j in self.values_list:
                tdSql.execute(f'insert into {self.stbname}_{i} values({j})')
        for key,values in self.column_add_dict.items():
            tdSql.execute(f'alter stable {self.stbname} add column {key} {values}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict)+1)
                tdSql.query(f'select {key} from {self.stbname}_{i}')
                tdSql.checkRows(len(self.values_list))
            for i in range(self.tbnum):
                tdSql.error(f'alter stable {self.stbname}_{i} add column {key} {values}')
                tdSql.error(f'alter stable {self.stbname}_{i} drop column {key}')
            #! bug TD-16921
            tdSql.error(f'alter stable {self.ntbname} add column {key} {values}')
            tdSql.error(f'alter stable {self.ntbname} drop column {key}')
            tdSql.execute(f'alter stable {self.stbname} drop column {key}')
            tdSql.query(f'describe {self.stbname}')
            tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            for i in range(self.tbnum):
                tdSql.query(f'describe {self.stbname}_{i}')
                tdSql.checkRows(len(self.column_dict)+len(self.tag_dict))
            tdSql.error(f'select {key} from {self.stbname} ')
        for key,values in self.column_dict.items():
            if 'binary' in values.lower():
                v = f'binary({self.binary_length+1})'
                v_error = f'binary({self.binary_length-1})'
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'VARCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'VARCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
               #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            elif 'nchar' in values.lower():
                v = f'nchar({self.binary_length+1})'
                v_error = f'nchar({self.binary_length-1})'
                tdSql.error(f'alter stable {self.stbname} modify column {key} {v_error}')
                tdSql.execute(f'alter stable {self.stbname} modify column {key} {v}')
                tdSql.query(f'describe {self.stbname}')
                result = tdCom.getOneRow(1,'NCHAR')
                tdSql.checkEqual(result[0][2],self.binary_length+1)
                for i in range(self.tbnum):
                    tdSql.query(f'describe {self.stbname}_{i}')
                    result = tdCom.getOneRow(1,'NCHAR')
                    tdSql.checkEqual(result[0][2],self.binary_length+1)
                    tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
                #! bug TD-16921
                tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
            else:
                for v in self.column_dict.values():
                    tdSql.error(f'alter stable {self.stbname} modify column {key} {v}')
                    tdSql.error(f'alter stable {self.ntbname} modify column {key} {v}')
                    for i in range(self.tbnum):
                        tdSql.error(f'alter stable {self.stbname}_{i} modify column {key} {v}')
    
    def alter_stable_column_varchar_39001(self):
        """Check alter stable column varchar 39001 from 39000(TS-3841)
        """
        stbname = "st1"
        column_dict = {
            'ts'  : 'timestamp',
            'col1': 'varchar(39000)',
            'col2': 'tinyint',
            'col3': 'timestamp',
            'col4': 'tinyint',
            'col5': 'timestamp',
            'col6': 'varchar(18)',
            'col7': 'varchar(17)'
        }
        tag_dict = {
            'id': 'int'
        }

        tdSql.execute(self.setsql.set_create_stable_sql(stbname, column_dict, tag_dict))
        res = tdSql.getResult(f'desc {stbname}')
        tdLog.info(res)
        assert(res[1][2] == 39000)
        tdSql.execute(f'alter stable {stbname} modify column col1 varchar(39001)')
        res = tdSql.getResult(f'desc {stbname}')
        tdLog.info(res)
        assert(res[1][2] == 39001)

    def prepareAlterEnv(self):
        tdSql.execute(f'drop database if exists {self.dbname}')
        tdSql.execute(f'create database if not exists {self.dbname} vgroups 2')
        tdSql.execute(f'use {self.dbname}')

    def destroyAlterEnv(self):
        tdSql.execute(f'drop database if exists {self.dbname}')

    def alterTableTask(self, i):
        os.system(f'taos -f {self.fname}.{i};')

    def executeAlterTable(self, opt):
        threads = []
        for i in range(self.threadnum):
            thread = threading.Thread(target=self.alterTableTask, args=(i,))
            threads.append(thread)
            thread.start()
        for i in range(self.threadnum):
            threads[i].join()

    def checkAlterTable(self, opt):
        if opt in ["stb_add_col", "stb_add_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'desc {self.stbname}_{i}')
        elif opt in ["stb_modify_col", "stb_modify_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'desc {self.stbname}_{i}')
        elif opt in ["ntb_add_col", "ntb_modify_col"]:
            for i in range(self.ntbnum):
                tdSql.execute(f'desc {self.ntbname}_{i}')

    def destroyAlterTable(self):
        for i in range(self.threadnum):
            if os.path.isfile(f'{self.fname}.{i}'):
                os.remove(f'{self.fname}.{i}')
    
    def prepareAlterTable(self, opt):
        self.destroyAlterTable()
        lines = [f'use {self.dbname};\n']
        if opt in ["stb_add_col", "stb_add_tag"]:
            for i in range(self.stbnum):
                tdSql.execute(f'create table if not exists {self.stbname}_{i} (ts timestamp, c_0 NCHAR({self.collen})) tags(t0 nchar({self.collen}));')
            for i in range(self.stbnum):
                if opt == 'stb_add_col': 
                    for c in range(1, self.colnum):
                        lines.append(f'alter table {self.stbname}_{i} add column c_{c} NCHAR({self.collen});\n')
                else:
                    for c in range(1, self.tagnum):
                        lines.append(f'alter table {self.stbname}_{i} add tag t_{c} NCHAR({self.collen});\n')
        elif opt in ["stb_modify_col", "stb_modify_tag"]:
            for i in range(self.stbnum):
                createTbSql = f'CREATE table if not exists {self.stbname}_{i} (ts timestamp'
                for j in range(self.colnum_modify):
                    createTbSql += f',c_{j} NCHAR({self.collen_old_modify})'
                createTbSql += f') tags(t_0 NCHAR({self.taglen_old_modify})'
                for k in range(1,self.tagnum_modify):
                    createTbSql += f',t_{k} NCHAR({self.taglen_old_modify})'
                createTbSql += f');'
                tdLog.info(createTbSql)
                tdSql.execute(createTbSql)
            for i in range(self.stbnum):
                if opt == 'stb_modify_col': 
                    for c in range(self.colnum_modify):
                        lines.append(f'alter table {self.stbname}_{i} modify column c_{c} NCHAR({self.collen_new_modify});\n')
                else:
                    for c in range(self.tagnum_modify):
                        lines.append(f'alter table {self.stbname}_{i} modify tag t_{c} NCHAR({self.taglen_new_modify});\n')
        elif opt in ['ntb_add_col']:
            for i in range(self.ntbnum):
                tdSql.execute(f'create table if not exists {self.ntbname}_{i} (ts timestamp, c_0 NCHAR({self.collen}));')
            for i in range(self.ntbnum):
                for c in range(1, self.colnum):
                    lines.append(f'alter table {self.ntbname}_{i} add column c_{c} NCHAR({self.collen});\n')
        elif opt in ['ntb_modify_col']:
            for i in range(self.ntbnum):
                createTbSql = f'CREATE table if not exists {self.ntbname}_{i} (ts timestamp'
                for j in range(self.colnum_modify):
                    createTbSql += f',c_{j} NCHAR({self.collen_old_modify})'
                createTbSql += f');'
                tdLog.info(createTbSql)
                tdSql.execute(createTbSql)
            for i in range(self.ntbnum):
                for c in range(self.colnum_modify):
                    lines.append(f'alter table {self.ntbname}_{i} modify column c_{c} NCHAR({self.collen_new_modify});\n')
        # generate sql file
        with open(f'{self.fname}.0', "a") as f:
            f.writelines(lines)
        # clone sql file in case of race condition
        for i in range(1, self.threadnum):
            shutil.copy(f'{self.fname}.0', f'{self.fname}.{i}')

    def alter_stable_multi_client_check(self):
        """Check alter stable/ntable var type column/tag(PI-23)
        """
        alter_table_check_type = ["stb_add_col", "stb_add_tag", "stb_modify_col", "stb_modify_tag", "ntb_add_col", "ntb_modify_col"]

        for opt in alter_table_check_type:
            self.prepareAlterEnv()
            self.prepareAlterTable(opt)
            self.executeAlterTable(opt)
            self.checkAlterTable(opt)
            self.destroyAlterTable()
        self.destroyAlterEnv()

    def AlterColumn(self):
        dbPrefix = "m_alt_db"
        tbPrefix = "m_alt_tb"
        mtPrefix = "m_alt_mt"
        tbNum = 10
        rowNum = 5
        totalNum = tbNum * rowNum
        ts0 = 1537146000000
        delta = 600000
        tdLog.info(f"========== alter_column.sim")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)

        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        ##### alter table test, simeplest case
        tdSql.execute(
            f"create table tb (ts timestamp, c1 int, c2 binary(10), c3 nchar(10))"
        )
        tdSql.execute(f'insert into tb values (now, 1, "1", "1")')
        tdSql.execute(f"alter table tb modify column c2 binary(20);")
        tdSql.execute(f"alter table tb modify column c3 nchar(20);")

        tdSql.execute(
            f"create stable stb (ts timestamp, c1 int, c2 binary(10), c3 nchar(10)) tags(id1 int, id2 binary(10), id3 nchar(10))"
        )
        tdSql.execute(f'create table tb1 using stb tags(1, "a", "b")')
        tdSql.execute(f'insert into tb1 values (now, 1, "1", "1")')
        tdSql.execute(f"alter stable stb modify column c2 binary(20);")
        tdSql.execute(f"alter table stb modify column c2 binary(30);")
        tdSql.execute(f"alter stable stb modify column c3 nchar(20);")
        tdSql.execute(f"alter table stb modify column c3 nchar(30);")
        tdSql.execute(f"alter table stb modify tag id2 binary(11);")
        tdSql.error(f"alter stable stb modify tag id2 binary(11);")
        tdSql.execute(f"alter table stb modify tag id3 nchar(11);")
        tdSql.error(f"alter stable stb modify tag id3 nchar(11);")

        ##### ILLEGAL OPERATIONS

        # try dropping columns that are defined in metric
        tdSql.error(f"alter table tb modify column c1 binary(10);")
        tdSql.error(f"alter table tb modify column c1 double;")
        tdSql.error(f"alter table tb modify column c2 int;")
        tdSql.error(f"alter table tb modify column c2 binary(10);")
        tdSql.error(f"alter table tb modify column c2 binary(9);")
        tdSql.error(f"alter table tb modify column c2 binary(-9);")
        tdSql.error(f"alter table tb modify column c2 binary(0);")
        tdSql.error(f"alter table tb modify column c2 binary(65436);")
        tdSql.error(f"alter table tb modify column c2 nchar(30);")
        tdSql.error(f"alter table tb modify column c3 double;")
        tdSql.error(f"alter table tb modify column c3 nchar(10);")
        tdSql.error(f"alter table tb modify column c3 nchar(0);")
        tdSql.error(f"alter table tb modify column c3 nchar(-1);")
        tdSql.error(f"alter table tb modify column c3 binary(80);")
        tdSql.error(f"alter table tb modify column c3 nchar(17000);")
        tdSql.error(f"alter table tb modify column c3 nchar(100), c2 binary(30);")
        tdSql.error(f"alter table tb modify column c1 nchar(100), c2 binary(30);")
        tdSql.error(f"alter stable tb modify column c2 binary(30);")
        tdSql.error(f"alter table tb modify tag c2 binary(30);")
        tdSql.error(f"alter table stb modify tag id2 binary(10);")
        tdSql.error(f"alter table stb modify tag id2 nchar(30);")
        tdSql.error(f"alter stable stb modify tag id2 binary(10);")
        tdSql.error(f"alter stable stb modify tag id2 nchar(30);")
        tdSql.error(f"alter table stb modify tag id3 nchar(10);")
        tdSql.error(f"alter table stb modify tag id3 binary(30);")
        tdSql.error(f"alter stable stb modify tag id3 nchar(10);")
        tdSql.error(f"alter stable stb modify tag id3 binary(30);")
        tdSql.error(f"alter stable stb modify tag id1 binary(30);")
        tdSql.error(f"alter stable stb modify tag c1 binary(30);")

        tdSql.error(f"alter table tb1 modify column c2 binary(30);")
        tdSql.error(f"alter table tb1 modify column c3 nchar(30);")
        tdSql.error(f"alter table tb1 modify tag id2 binary(30);")
        tdSql.error(f"alter table tb1 modify tag id3 nchar(30);")
        tdSql.error(f"alter stable tb1 modify tag id2 binary(30);")
        tdSql.error(f"alter stable tb1 modify tag id3 nchar(30);")
        tdSql.error(f"alter stable tb1 modify column c2 binary(30);")

    def AlterStable(self):
        tdLog.info(f"========== alter_stable.sim")

        db = "demodb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        ##### alter stable test : rename tag name
        # case-1  rename tag name: new name inclue old name
        tdSql.execute(f"create table mt1 (ts timestamp, c1 int) tags (a int)")
        tdSql.execute(f"alter table mt1 rename tag a abcd")
        tdSql.execute(f"alter table mt1 rename tag abcd a")
        tdSql.error(f"alter table mt1 rename tag a 1")

        tdSql.error(f"create table mtx1 (ts timestamp, c1 int) tags (123 int)")

        tdSql.error(
            f"create table mt2 (ts timestamp, c1 int) tags (abc012345678901234567890123456789012345678901234567890123456789def int)"
        )
        tdSql.execute(
            f"create table mt3 (ts timestamp, c1 int) tags (abc012345678901234567890123456789012345678901234567890123456789 int)"
        )
        tdSql.error(
            f"alter table mt3 rename tag abc012345678901234567890123456789012345678901234567890123456789 abcdefg012345678901234567890123456789012345678901234567890123456789"
        )
        tdSql.execute(
            f"alter table mt3 rename tag abc012345678901234567890123456789012345678901234567890123456789 abcdefg0123456789012345678901234567890123456789"
        )

        # case-2 set tag value
        tdSql.execute(
            f"create table mt4 (ts timestamp, c1 int) tags (name binary(16), len int)"
        )
        tdSql.execute(f'create table tb1 using mt4 tags ("beijing", 100)')
        tdSql.execute(f'alter table tb1 set tag name = "shanghai"')
        tdSql.execute(f'alter table tb1 set tag name = ""')
        tdSql.execute(f'alter table tb1 set tag name = "shenzhen"')
        tdSql.execute(f"alter table tb1 set tag len = 379")

        # case TD-5594
        tdSql.execute(
            f"create stable st5520(ts timestamp, f int) tags(t0 bool, t1 nchar(4093), t2 nchar(1))"
        )
        tdSql.error(f"alter stable st5520 modify tag t2 nchar(2);")
        # test end
        tdSql.execute(f"drop database {db}")

    def StableOp(self):
        tdLog.info(f"========== stableOp.sim")

        dbPrefix = "fi_in_db"
        tbPrefix = "fi_in_tb"
        stbPrefix = "fi_in_stb"
        mtPrefix = "fi_in_mt"
        tbNum = 10
        rowNum = 20
        totalNum = 200

        tdLog.info(f"create_tb test")
        tdLog.info(f"=============== set up")
        i = 0
        db = dbPrefix + str(i)
        mt = mtPrefix + str(i)
        tb = tbPrefix + str(i)
        stb = stbPrefix + str(i)

        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        # case1: create stable test
        tdLog.info(f"=========== stableOp.sim case1: create/alter/drop stable test")
        tdSql.execute(f"CREATE STABLE {stb} (TS TIMESTAMP, COL1 INT) TAGS (ID INT);")
        tdSql.query(f"show stables")

        tdSql.checkRows(1)

        tdLog.info(f"tdSql.getData(0,0) = {tdSql.getData(0,0)}")
        tdSql.checkData(0, 0, stb)

        tdSql.error(f"CREATE STABLE {tb} using {stb} tags (1);")

        tdSql.execute(f"create table {tb} using {stb} tags (2);")
        tdSql.query(f"show tables")

        tdSql.checkRows(1)

        tdSql.execute(f"alter stable {stb} add column COL2 DOUBLE;")

        tdSql.execute(f"insert into {tb} values (now, 1, 2.0);")

        tdSql.query(f"select * from {tb} ;")

        tdSql.checkRows(1)

        tdSql.execute(f"alter stable {stb} drop column COL2;")

        tdSql.error(f"insert into {tb} values (now, 1, 2.0);")

        tdSql.execute(f"alter stable {stb} add tag tag2 int;")

        tdSql.execute(f"alter stable {stb} rename tag tag2 tag3;")

        tdSql.error(f"drop stable {tb}")

        tdSql.execute(f"drop table {tb} ;")

        tdSql.query(f"show tables")

        tdSql.checkRows(0)

        tdSql.execute(f"DROP STABLE {stb}")
        tdSql.query(f"show stables")

        tdSql.checkRows(0)

        tdLog.info(f"create/alter/drop stable test passed")

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

    def ColumnAdd(self):
        tdLog.info(f"========== tsim/stable/column_add.sim")

        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, "stb")
        tdSql.checkData(0, 6, 8)

        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 101)

        tdSql.error(f"alter table db.stb add column ts int")
        tdSql.error(f"alter table db.stb add column t1 int")
        tdSql.error(f"alter table db.stb add column t2 int")
        tdSql.error(f"alter table db.stb add column t3 int")
        tdSql.error(f"alter table db.stb add column c1 int")

        tdLog.info(f"========== step1 add column c3")
        tdSql.execute(f"alter table db.stb add column c3 int")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 4)

        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 101)

        tdSql.execute(f"insert into db.ctb values(now+1s, 1, 2, 3)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, 101)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 3)
        tdSql.checkData(1, 4, 101)

        tdLog.info(f"========== step2 add column c4")
        tdSql.execute(f"alter table db.stb add column c4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.execute(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(3)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, None)
        tdSql.checkData(0, 4, None)
        tdSql.checkData(0, 5, 101)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 2)
        tdSql.checkData(1, 3, 3)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(1, 5, 101)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, 101)

        tdLog.info(f"========== step3 add column c5")
        tdSql.execute(f"alter table db.stb add column c5 int")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3, 4, 5)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(4)
        tdSql.checkData(2, 1, 1)
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 3)
        tdSql.checkData(2, 4, 4)
        tdSql.checkData(2, 5, None)
        tdSql.checkData(2, 6, 101)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 4)
        tdSql.checkData(3, 5, 5)
        tdSql.checkData(3, 6, 101)

        tdLog.info(f"========== step4 add column c6")
        tdSql.execute(f"alter table db.stb add column c6 int")
        tdSql.execute(f"insert into db.ctb values(now+4s, 1, 2, 3, 4, 5, 6)")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(5)
        tdSql.checkData(3, 1, 1)
        tdSql.checkData(3, 2, 2)
        tdSql.checkData(3, 3, 3)
        tdSql.checkData(3, 4, 4)
        tdSql.checkData(3, 5, 5)
        tdSql.checkData(3, 6, None)
        tdSql.checkData(3, 7, 101)
        tdSql.checkData(4, 1, 1)
        tdSql.checkData(4, 2, 2)
        tdSql.checkData(4, 3, 3)
        tdSql.checkData(4, 4, 4)
        tdSql.checkData(4, 5, 5)
        tdSql.checkData(4, 6, 6)
        tdSql.checkData(4, 7, 101)

        tdLog.info(f"========== step5 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(10)

    def ColumnDrop(self):
        tdLog.info(f"========== tsim/stable/column_drop.sim")
        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4), c3 int, c4 bigint, c5 int, c6 int) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2", 3, 4, 5, 6)')

        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "stb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 7)
        tdSql.checkData(0, 4, 3)
        tdSql.checkData(0, 6, "abd")

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "ctb")
        tdSql.checkData(0, 1, "db")
        tdSql.checkData(0, 3, 7)
        tdSql.checkData(0, 4, "stb")
        tdSql.checkData(0, 6, 9)

        tdSql.checkData(0, 9, "CHILD_TABLE")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 6)
        tdSql.checkData(0, 7, 101)

        tdSql.error(f"alter table db.stb drop column ts")
        tdSql.error(f"alter table db.stb drop column t1")
        tdSql.error(f"alter table db.stb drop column t2")
        tdSql.error(f"alter table db.stb drop column t3")
        tdSql.error(f"alter table db.stb drop column c9")

        tdLog.info(f"========== step1 drop column c6")
        tdSql.execute(f"alter table db.stb drop column c6")
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.checkData(0, 3, 6)

        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.checkData(0, 3, 6)

        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")

        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 2)
        tdSql.checkData(0, 3, 3)
        tdSql.checkData(0, 4, 4)
        tdSql.checkData(0, 5, 5)
        tdSql.checkData(0, 6, 101)

        tdSql.execute(f"insert into db.ctb values(now+1s, 1, 2, 3, 4, 5)")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(2)

        tdLog.info(f"========== step2 drop column c5")
        tdSql.execute(f"alter table db.stb drop column c5")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")
        tdSql.execute(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3, 4)")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(4)

        tdLog.info(f"========== step3 drop column c4")
        tdSql.execute(f"alter table db.stb drop column c4")
        tdSql.query(f"select * from db.stb")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4, 5)")
        tdSql.error(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")
        tdSql.execute(f"insert into db.ctb values(now+3s, 1, 2, 3)")

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(5)

        tdLog.info(f"========== step4 add column c4")
        tdSql.execute(f"alter table db.stb add column c4 binary(13)")
        tdSql.execute(f"insert into db.ctb values(now+4s, 1, 2, 3, '4')")
        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(6)
        tdSql.checkData(1, 4, None)
        tdSql.checkData(2, 4, None)
        tdSql.checkData(3, 4, None)
        tdSql.checkData(5, 4, 4)

        tdLog.info(f"========== step5 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(8)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(3, 0, "c3")
        tdSql.checkData(4, 0, "c4")
        tdSql.checkData(4, 1, "VARCHAR")
        tdSql.checkData(4, 2, 13)
        tdSql.checkData(5, 0, "t1")
        tdSql.checkData(6, 0, "t2")
        tdSql.checkData(7, 0, "t3")

    def ColumnModify(self):
        tdLog.info(f"========== tsim/stable/column_modify.sim")
        tdSql.prepare("db", drop=True, vgroups=1)
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "1234")')

        tdSql.error(f"alter table db.stb MODIFY column c2 binary(3)")
        tdSql.error(f"alter table db.stb MODIFY column c2 int")
        tdSql.error(f"alter table db.stb MODIFY column c1 int")
        tdSql.error(f"alter table db.stb MODIFY column ts int")
        tdSql.error(f'insert into db.ctb values(now, 1, "12345")')

        tdLog.info(f"========== step1 modify column")
        tdSql.execute(f"alter table db.stb MODIFY column c2 binary(5)")
        tdSql.execute(f'insert into db.ctb values(now, 1, "12345")')

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 12345)
        tdSql.checkData(1, 3, 101)

        tdLog.info(f"========== step2 describe")
        tdSql.query(f"describe db.ctb")
        tdSql.checkRows(6)
        tdSql.checkData(0, 0, "ts")
        tdSql.checkData(1, 0, "c1")
        tdSql.checkData(2, 0, "c2")
        tdSql.checkData(2, 1, "VARCHAR")
        tdSql.checkData(2, 2, 5)
        tdSql.checkData(3, 0, "t1")
        tdSql.checkData(4, 0, "t2")
        tdSql.checkData(5, 0, "t3")

        sc.dnodeStop(1)
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from db.stb")
        tdSql.checkRows(2)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1234)
        tdSql.checkData(0, 3, 101)
        tdSql.checkData(1, 1, 1)
        tdSql.checkData(1, 2, 12345)
        tdSql.checkData(1, 3, 101)

    def test_stable_alter_basic(self):
        """Alter basic

        1. Add Modify Drop Column
        2. Add Modify Drop Rename Tag
        3. Alter Comment
        4. Alter add/modify/drop columns for all datatype
        5. Alter modify varchar column to 390001 length
        6. Alter add/modify tag/column with multi threads

        Catalog:
            - SuperTable:Alter

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-11 Simon Guan Migrated from tsim/parser/alter_column.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/alter_stable.sim
            - 2025-8-11 Simon Guan Migrated from tsim/parser/stableOp.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/column_add.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/column_drop.sim
            - 2025-8-11 Simon Guan Migrated from tsim/stable/column_modify.sim
            - 2025-9-15 Alex  Duan Migrated from uncatalog/system-test/1-insert/test_alter_stable.py

        """

        # sim
        self.AlterColumn()
        tdStream.dropAllStreamsAndDbs()
        self.AlterStable()
        tdStream.dropAllStreamsAndDbs()
        self.StableOp()
        tdStream.dropAllStreamsAndDbs()
        self.ColumnAdd()
        tdStream.dropAllStreamsAndDbs()
        self.ColumnDrop()
        tdStream.dropAllStreamsAndDbs()
        self.ColumnModify()
        tdStream.dropAllStreamsAndDbs()

        # Migrated from test_alter_stable.py
        self.alter_stable_check()
        self.alter_stable_column_varchar_39001()
        self.alter_stable_multi_client_check()