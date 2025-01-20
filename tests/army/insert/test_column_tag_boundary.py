from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
import random
import string

class TDTestCase(TBase):
    """Add test case to test column and tag boundary for task TD-28586
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # define the max properties of column and tag
        self.super_table_max_column_num = 4096
        self.max_tag_num = 128
        self.max_tag_length = 16382
        self.max_column_length = 65517
        self.child_table_num = 1
        self.insert_round_num = 300
        self.row_num_per_round = 15
        self.row_num_per_round_varbia_json = 8
        self.rows_all1 = self.insert_round_num * self.row_num_per_round
        self.rows_all2 = self.insert_round_num * self.row_num_per_round_varbia_json
        self.start_ts = 1704082431000

    def prepare_data(self):
        # database
        tdSql.execute("create database db;")
        tdSql.execute("use db;")

    def test_binary_boundary(self):
        # create tables
        tdSql.execute(f"create table st_binary (ts timestamp, c1 binary({self.max_column_length})) tags (t1 binary({self.max_tag_length}));")
        for i in range(self.child_table_num):
            # create child table with max column and tag length
            tag = ''.join(random.sample(string.ascii_lowercase, 1)) * self.max_tag_length
            tdSql.execute(f"create table ct_binary{i+1} using st_binary tags('{tag}');")
            # insert data
            for j in range(self.insert_round_num):
                sql = "insert into ct_binary%s values" % (i+1)
                for k in range(self.row_num_per_round):
                    sql += "(%s, '%s')," % (str(self.start_ts + (j * self.insert_round_num + k * self.row_num_per_round + 1)), 'a' * self.max_column_length)
                tdSql.execute(sql)
                tdLog.info(f"Insert {self.row_num_per_round} rows data into ct_binary{i+1} {j+1} times successfully")
        tdSql.execute("flush database db;")
        # check the data
        for i in range(self.child_table_num):
            tdSql.query(f"select * from ct_binary{i+1};")
            tdSql.checkRows(self.rows_all1)
            row_num = random.randint(0, self.rows_all1-1)
            tdSql.checkData(row_num, 1, 'a' * self.max_column_length)
            tdSql.query(f"show tags from ct_binary{i+1};")
            tdSql.checkData(0, 5, tag)                

    def test_varchar_boundary(self):
        # create tables
        tdSql.execute(f"create table st_varchar (ts timestamp, c1 varchar({self.max_column_length})) tags (t1 varchar({self.max_tag_length}));")
        for i in range(self.child_table_num):
            # create child table with max column and tag length
            tag = ''.join(random.sample(string.ascii_lowercase, 1)) * self.max_tag_length
            tdSql.execute(f"create table ct_varchar{i+1} using st_varchar tags('{tag}');")
            # insert data
            for j in range(self.insert_round_num):
                sql = "insert into ct_varchar%s values" % (i+1)
                for k in range(self.row_num_per_round):
                    sql += "(%s, '%s')," % (str(self.start_ts + (j * self.insert_round_num + k * self.row_num_per_round + 1)), 'b' * self.max_column_length)
                tdSql.execute(sql)
                tdLog.info(f"Insert {self.row_num_per_round} rows data into ct_varchar{i+1} {j+1} times successfully")
        tdSql.execute("flush database db;")
        # check the data
        for i in range(self.child_table_num):
            tdSql.query(f"select * from ct_varchar{i+1};")
            tdSql.checkRows(self.rows_all1)
            row_num = random.randint(0, self.rows_all1-1)
            tdSql.checkData(row_num, 1, 'b' * self.max_column_length)
            tdSql.query(f"show tags from ct_varchar{i+1};")
            tdSql.checkData(0, 5, tag)

    def gen_chinese_string(self, length):
        start = 0x4e00
        end = 0x9fa5
        chinese_string = ''
        for _ in range(length):
            chinese_string += chr(random.randint(start, end))
        return chinese_string

    def test_nchar_boundary(self):
        max_nchar_column_length = self.max_column_length // 4
        max_nchar_tag_length = self.max_tag_length // 4
        # create tables
        tdSql.execute(f"create table st_nchar (ts timestamp, c1 nchar({max_nchar_column_length})) tags (t1 nchar({max_nchar_tag_length}));")
        for i in range(self.child_table_num):
            # create child table with max column and tag length
            tag = self.gen_chinese_string(max_nchar_tag_length)
            column = self.gen_chinese_string(max_nchar_column_length)
            tdSql.execute(f"create table ct_nchar{i+1} using st_nchar tags('{tag}');")
            # insert data
            for j in range(self.insert_round_num):
                sql = "insert into ct_nchar%s values" % (i+1)
                for k in range(self.row_num_per_round):
                    sql += "(%s, '%s')," % (str(self.start_ts + (j * self.insert_round_num + k * self.row_num_per_round + 1)), column)
                tdSql.execute(sql)
                tdLog.info(f"Insert {self.row_num_per_round} rows data into ct_nchar{i+1} {j+1} times successfully")
        tdSql.execute("flush database db;")
        # check the data
        for i in range(self.child_table_num):
            tdSql.query(f"select * from ct_nchar{i+1};")
            tdSql.checkRows(self.rows_all1)
            row_num = random.randint(0, self.rows_all1-1)
            tdSql.checkData(row_num, 1, column)
            tdSql.query(f"show tags from ct_nchar{i+1};")
            tdSql.checkData(0, 5, tag)

    def test_varbinary_boundary(self):

        # create tables
        tdSql.execute(f"create table st_varbinary (ts timestamp, c1 varbinary({self.max_column_length})) tags (t1 varbinary({self.max_tag_length}));")
        for i in range(self.child_table_num):
            # create child table with max column and tag length
            tag = (''.join(random.sample(string.ascii_lowercase, 1)) * self.max_tag_length).encode().hex()
            column = (''.join(random.sample(string.ascii_lowercase, 1)) * self.max_column_length).encode().hex()
            tdSql.execute("create table ct_varbinary%s using st_varbinary tags('%s');" % (str(i+1), '\\x' + tag))
            # insert data
            for j in range(self.insert_round_num):
                sql = "insert into ct_varbinary%s values" % (i+1)
                for k in range(self.row_num_per_round_varbia_json):
                    sql += "(%s, '%s')," % (str(self.start_ts + (j * self.insert_round_num + k * self.row_num_per_round + 1)), '\\x' + column)
                tdSql.execute(sql)
                tdLog.info(f"Insert {self.row_num_per_round_varbia_json} rows data into ct_varbinary{i+1} {j+1} times successfully")
        tdSql.execute("flush database db;")
        # check the data
        for i in range(self.child_table_num):
            tdSql.query(f"select * from ct_varbinary{i+1};")
            tdSql.checkRows(self.rows_all2)
            row_num = random.randint(0, self.rows_all2-1)
            tdSql.checkData(row_num, 1, bytes.fromhex(column))
            tdSql.query(f"show tags from ct_varbinary{i+1};")
            tdSql.checkData(0, 5, '\\x' + tag.upper())

    def test_json_tag_boundary(self):
        max_json_tag_length = 4095
        max_json_tag_key_length = 256
        # create tables
        tdSql.execute(f"create table st_json_tag (ts timestamp, c1 varbinary({self.max_column_length})) tags (t1 json);")
        for i in range(self.child_table_num):
            # create child table with max column and tag length
            tag_key = ''.join(random.sample(string.ascii_lowercase, 1)) * max_json_tag_key_length
            tag_value = ''.join(random.sample(string.ascii_lowercase, 1)) * (max_json_tag_length - max_json_tag_key_length - 7)
            column = (''.join(random.sample(string.ascii_lowercase, 1)) * self.max_column_length).encode().hex()
            tdSql.execute("create table ct_json_tag%s using st_json_tag tags('%s');" % (str(i+1), f'{{"{tag_key}":"{tag_value}"}}'))
            # insert data
            for j in range(self.insert_round_num):
                sql = "insert into ct_json_tag%s values" % (i+1)
                for k in range(self.row_num_per_round_varbia_json):
                    sql += "(%s, '%s')," % (str(self.start_ts + (j * self.insert_round_num + k * self.row_num_per_round + 1)), '\\x' + column)
                tdSql.execute(sql)
                tdLog.info(f"Insert {self.row_num_per_round_varbia_json} rows data into ct_json_tag{i+1} {j+1} times successfully")
        tdSql.execute("flush database db;")
        # check the data
        for i in range(self.child_table_num):
            tdSql.query(f"select * from ct_json_tag{i+1} where t1->'{tag_key}' = '{tag_value}';")
            tdSql.checkRows(self.rows_all2)
            row_num = random.randint(0, self.rows_all2-1)
            tdSql.checkData(row_num, 1, bytes.fromhex(column))

    def run(self):
        self.prepare_data()
        self.test_binary_boundary()
        self.test_varchar_boundary()
        self.test_nchar_boundary()
        self.test_varbinary_boundary()
        self.test_json_tag_boundary()

    def stop(self):
        tdSql.execute("drop database db;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
