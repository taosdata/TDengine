from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
from datetime import datetime, timedelta

class TDTestCase(TBase):
    """Verify the join function
    """
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        # super table common name, will use db name as prefix, "_" as connector with table name as the whole table name
        self.super_table_names = ["st1", "st2", "st_empty"]
        # child table common name, will use db name as prefix, "_" as connector with super table name and child table name as the whole table name
        self.child_table_names = ["ct1", "ct2", "ct_empty"]
        # regular table common name, will use db name as prefix, "_" as connector with table name as the whole table name
        self.regular_table_names = ["t1", "t2", "t_empty"]
        self.total_sql_num = 0

    def column_fields(self, pk_int32=False, pk_int64=False, pk_str=False):
        """Column schema definition for all tables
        """
        if pk_int32:
            return "ts timestamp, v_int int primary key, v_ts timestamp, v_int_empty int, v_bigint bigint, v_bigint_empty bigint, \
                v_double double, v_double_empty double, v_binary binary(32), v_binary_empty binary(32), v_bool bool, v_bool_empty bool"
        elif pk_int64:
            return "ts timestamp, v_bigint bigint primary key, v_ts timestamp, v_int int, v_int_empty int, v_bigint_empty bigint, \
                v_double double, v_double_empty double, v_binary binary(32), v_binary_empty binary(32), v_bool bool, v_bool_empty bool"
        elif pk_str:
            return "ts timestamp, v_binary binary(32) primary key, v_ts timestamp, v_int int, v_int_empty int, v_bigint bigint, v_bigint_empty bigint, \
                v_double double, v_double_empty double, v_binary_empty binary(32), v_bool bool, v_bool_empty bool"
        else:
            return "ts timestamp, v_ts timestamp, v_int int, v_int_empty int, v_bigint bigint, v_bigint_empty bigint, \
                v_double double, v_double_empty double, v_binary binary(32), v_binary_empty binary(32), v_bool bool, v_bool_empty bool"

    def tag_fields(self):
        """Tag schema definition for super table
        """
        return "t_ts timestamp, t_int int, t_int_empty int, t_bigint bigint, t_bigint_empty bigint, \
            t_double double, t_double_empty double, t_binary binary(32), t_binary_empty binary(32), t_bool bool, t_bool_empty bool"

    def tag_values(self, mark_name):
        """Tag values for 3 super tables
        :param mark_name: tag values are marked with name for different test scenarios
        """
        v_dic = {
            "common": [
                "'2024-01-01 12:00:00', 1, NULL, 123456789, NULL, 1234.56789, NULL, 'test message', NULL, true, NULL",
                "'2024-01-01 13:00:00', 2, NULL, 123456789, NULL, 2234.56789, NULL, 'test message', NULL, false, NULL",
                "'2024-01-01 14:00:00', 2, NULL, 223456789, NULL, 2234.56789, NULL, 'test message', NULL, false, NULL"
            ]
        }
        return v_dic[mark_name]

    def create_tables(self, db_list, precision_list, tag_mark="common"):
        """Init the database and tables, the database number should keep same with table number
        :param db_list: the database name list
        :param precision_list: the precision each of the database, the precision can be "ms", "us", "ns"
        :param tag_mark: the tag values mark, default is "common"
        """
        for name, precision in zip(db_list, precision_list):
            tdSql.execute(f"create database {name} precision '{precision}';")
            tdSql.execute(f"use {name}")
            if precision == "ms":
                if name == "db_pk":
                    for st in self.super_table_names:
                        # primary key for int32
                        tdSql.execute(f"create table {'_'.join([name, st, 'int32'])} ({self.column_fields(pk_int32=True)}) tags({self.tag_fields()});")
                        # primary key for int64
                        tdSql.execute(f"create table {'_'.join([name, st, 'int64'])} ({self.column_fields(pk_int64=True)}) tags({self.tag_fields()});")
                        # primary key for binary
                        tdSql.execute(f"create table {'_'.join([name, st, 'str'])} ({self.column_fields(pk_str=True)}) tags({self.tag_fields()});")
                        if st != "st_empty":
                            tags = self.tag_values(tag_mark)
                            for index in range(len(self.child_table_names)):
                                tdSql.execute(f"create table {'_'.join([name, st, 'int32', self.child_table_names[index]])} using {'_'.join([name, st, 'int32'])} tags({tags[index]});")
                                tdSql.execute(f"create table {'_'.join([name, st, 'int64', self.child_table_names[index]])} using {'_'.join([name, st, 'int64'])} tags({tags[index]});")
                                tdSql.execute(f"create table {'_'.join([name, st, 'str', self.child_table_names[index]])} using {'_'.join([name, st, 'str'])} tags({tags[index]});")
                else:
                    # only for scenarios without primary key
                    for st in self.super_table_names:
                        tdSql.execute(f"create table {'_'.join([name, st])} ({self.column_fields()}) tags({self.tag_fields()});")
                        if st != "st_empty":
                            tags = self.tag_values(tag_mark)
                            for index in range(len(self.child_table_names)):
                                tdSql.execute(f"create table {'_'.join([name, st, self.child_table_names[index]])} using {'_'.join([name, st])} tags({tags[index]});")
                    for rt in self.regular_table_names:
                        tdSql.execute(f"create table {'_'.join([name, rt])} ({self.column_fields()});")
            else:
                # only create regular tables for 'us' and 'ns' precision
                tdSql.execute(f"create table {name}_t ({self.column_fields()});")

    def insert_data(self, db_name, table_name, data):
        """Insert data into table of database
        :param db_name: the database name
        :param table_name: the table name
        :param data: the data list
        """
        tdSql.execute(f"use {db_name}")
        sql = f"insert into {table_name} values"
        for d in data:
            sql += f"({d})"
        sql += ";"
        tdSql.execute(sql)

    def data(self, db_name_list, data_mark):
        """Data generator for test cases, is marked by data_mark
        :param db_name_list: the database name list, the data is same for different databases
        :param data_mark: the data mark for different test scenarios
        """
        data = {
            "common_ms": {
                "st1_ct1": [
                    "'2024-01-01 12:00:00.000', '2024-01-01 12:00:00.000', -2, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.200', '2024-01-01 12:00:00.200', 0, NULL, 123456792, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.400', '2024-01-01 12:00:00.400', 2, NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', '2024-01-01 12:00:00.600', 4, NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.800', '2024-01-01 12:00:00.800', 16, NULL, 123456798, NULL, 1234.56798, NULL, 'abce', NULL, true, NULL"
                ],
                "st1_ct2": [
                    "'2024-01-01 12:00:01.000', '2024-01-01 12:00:01.000', 8, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.200', '2024-01-01 12:00:01.200', 10, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', '2024-01-01 12:00:01.400', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.600', '2024-01-01 12:00:01.600', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.800', '2024-01-01 12:00:01.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL"
                ],
                "st2_ct1": [
                    "'2024-01-01 12:00:00.000', '2024-01-01 12:00:00.000', -3, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, false, NULL",
                    "'2024-01-01 12:00:00.100', '2024-01-01 12:00:00.100', -1, NULL, 123456791, NULL, 1234.56791, NULL, 'abcf', NULL, true, NULL",
                    "'2024-01-01 12:00:00.200', '2024-01-01 12:00:00.200', 1, NULL, 123456793, NULL, 1234.56793, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.300', '2024-01-01 12:00:00.300', 4, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', '2024-01-01 12:00:00.400', 6, NULL, 123456797, NULL, 1234.56797, NULL, 'abcg', NULL, false, NULL"
                ],
                "st2_ct2": [
                    "'2024-01-01 12:00:00.500', '2024-01-01 12:00:00.500', 7, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:01.600', '2024-01-01 12:00:00.600', 9, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.700', '2024-01-01 12:00:00.700', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.800', '2024-01-01 12:00:00.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:01.900', '2024-01-01 12:00:00.900', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL"
                ],
                "t1": [
                    "'2024-01-01 12:00:00.000', '2024-01-01 12:00:00.000', -2, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.200', '2024-01-01 12:00:00.200', 0, NULL, 123456792, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.400', '2024-01-01 12:00:00.400', 2, NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', '2024-01-01 12:00:00.600', 4, NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.800', '2024-01-01 12:00:00.800', 16, NULL, 123456798, NULL, 1234.56798, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.000', '2024-01-01 12:00:01.000', 8, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.200', '2024-01-01 12:00:01.200', 10, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', '2024-01-01 12:00:01.400', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.600', '2024-01-01 12:00:01.600', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.800', '2024-01-01 12:00:01.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                ],
                "t2": [
                    "'2024-01-01 12:00:00.000', '2024-01-01 12:00:00.000', -3, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, false, NULL",
                    "'2024-01-01 12:00:00.100', '2024-01-01 12:00:00.100', -1, NULL, 123456791, NULL, 1234.56791, NULL, 'abcf', NULL, true, NULL",
                    "'2024-01-01 12:00:00.200', '2024-01-01 12:00:00.200', 1, NULL, 123456793, NULL, 1234.56793, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.300', '2024-01-01 12:00:00.300', 4, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', '2024-01-01 12:00:00.400', 6, NULL, 123456797, NULL, 1234.56797, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.500', '2024-01-01 12:00:00.500', 7, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:01.600', '2024-01-01 12:00:00.600', 9, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.700', '2024-01-01 12:00:00.700', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.800', '2024-01-01 12:00:00.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:01.900', '2024-01-01 12:00:00.900', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                ]
            },
            "common_us": {
                "t": [
                    "'2024-01-01 12:00:00.000000', '2024-01-01 12:00:00.000', -2, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000200', '2024-01-01 12:00:00.200', 0, NULL, 123456792, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000400', '2024-01-01 12:00:00.400', 2, NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000600', '2024-01-01 12:00:00.600', 4, NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000800', '2024-01-01 12:00:00.800', 16, NULL, 123456798, NULL, 1234.56798, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.000000', '2024-01-01 12:00:01.000', 8, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000200', '2024-01-01 12:00:01.200', 10, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.000400', '2024-01-01 12:00:01.400', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000600', '2024-01-01 12:00:01.600', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.000800', '2024-01-01 12:00:01.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                ]
            },
            "common_ns": {
                "t": [
                    "'2024-01-01 12:00:00.000000000', '2024-01-01 12:00:00.000', -3, NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000000100', '2024-01-01 12:00:00.100', -1, NULL, 123456791, NULL, 1234.56791, NULL, 'abcf', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000000200', '2024-01-01 12:00:00.200', 1, NULL, 123456793, NULL, 1234.56793, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000000300', '2024-01-01 12:00:00.300', 4, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000000400', '2024-01-01 12:00:00.400', 6, NULL, 123456797, NULL, 1234.56797, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000000500', '2024-01-01 12:00:00.500', 7, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:01.000000600', '2024-01-01 12:00:00.600', 9, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000000700', '2024-01-01 12:00:00.700', 12, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000000800', '2024-01-01 12:00:00.800', 16, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000000900', '2024-01-01 12:00:00.900', 14, NULL, 123456795, NULL, 1234.56795, NULL, 'abcg', NULL, true, NULL",
                ]
            },
            "pk_int32_ms": {
                "st1_int32_ct1": [
                    "'2024-01-01 12:00:00.000', 0, '2024-01-01 12:00:00.000', NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', -2147483648, '2024-01-01 12:00:00.100', NULL, 123456781, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 2147483647, '2024-01-01 12:00:00.200', NULL, 123456781, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 0, '2024-01-01 12:00:00.200', NULL, 123456792, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 2147483647, '2024-01-01 12:00:00.200', NULL, 123456793, NULL, 1234.56793, NULL, 'abcde', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 2147483647,'2024-01-01 12:00:00.400', NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', -2147483648,'2024-01-01 12:00:00.500', NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 0,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', -2147483648,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 2147483647,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL"
                ],
                "st1_int32_ct2": [
                    "'2024-01-01 12:00:01.000', 2147483647, '2024-01-01 12:00:01.000', NULL, 123456780, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', -2147483648, '2024-01-01 12:00:01.100', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 0, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 2147483647, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56795, NULL, 'abcde', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', -2147483648, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56796, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 2147483647, '2024-01-01 12:00:01.400', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', -2147483648, '2024-01-01 12:00:01.600', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 2147483647, '2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 0, '2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', -2147483648,'2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL"
                ],
                "st2_int32_ct1": [
                    "'2024-01-01 12:00:00.000', 0, '2024-01-01 12:00:00.000', NULL, 123456780, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', -2147483648, '2024-01-01 12:00:00.100', NULL, 123456781, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 2147483647, '2024-01-01 12:00:00.200', NULL, 123456781, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 0, '2024-01-01 12:00:00.200', NULL, 123456792, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 2147483647, '2024-01-01 12:00:00.200', NULL, 123456793, NULL, 1234.56793, NULL, 'abcde', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 2147483647,'2024-01-01 12:00:00.400', NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', -2147483648,'2024-01-01 12:00:00.500', NULL, 123456794, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 0,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', -2147483648,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 2147483647,'2024-01-01 12:00:00.600', NULL, 123456796, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL"
                ],
                "st2_int32_ct2": [
                    "'2024-01-01 12:00:01.000', 2147483647, '2024-01-01 12:00:01.000', NULL, 123456780, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', -2147483648, '2024-01-01 12:00:01.100', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 0, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 2147483647, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56795, NULL, 'abcde', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', -2147483648, '2024-01-01 12:00:01.200', NULL, 123456795, NULL, 1234.56796, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 2147483647, '2024-01-01 12:00:01.400', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', -2147483648, '2024-01-01 12:00:01.600', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 2147483647, '2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 0, '2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', -2147483648,'2024-01-01 12:00:01.800', NULL, 123456795, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL"
                ]
            },
            "pk_int64_ms": {
                "st1_int64_ct1": [
                    "'2024-01-01 12:00:00.000', -9.2233720e+18, '2024-01-01 12:00:00.000', 2147483647, NULL, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', 0, '2024-01-01 12:00:00.100', 0, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 9.2233720e+18, '2024-01-01 12:00:00.200', 2147483647, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 0, '2024-01-01 12:00:00.200', 0, NULL, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 9.2233720e+18, '2024-01-01 12:00:00.200', 2147483647, NULL, NULL, 1234.56793, NULL, 'abcde', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 9.2233720e+18,'2024-01-01 12:00:00.400', 2147483647, NULL, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', -9.2233720e+18,'2024-01-01 12:00:00.500', -2147483648, NULL, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 0,'2024-01-01 12:00:00.600', 2147483647, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', -9.2233720e+18,'2024-01-01 12:00:00.600', -2147483648, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 9.2233720e+18,'2024-01-01 12:00:00.600', 2147483647, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL"
                ],
                "st1_int64_ct2": [
                    "'2024-01-01 12:00:01.000', 9.2233720e+18, '2024-01-01 12:00:01.000', -2147483648, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', -9.2233720e+18, '2024-01-01 12:00:01.100', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 0, '2024-01-01 12:00:01.200', 0, NULL, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 9.2233720e+18, '2024-01-01 12:00:01.200', 2147483647, NULL, NULL, 1234.56795, NULL, 'abcde', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', -9.2233720e+18, '2024-01-01 12:00:01.200', -2147483648, NULL, NULL, 1234.56796, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 9.2233720e+18, '2024-01-01 12:00:01.400', 2147483647, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', -9.2233720e+18, '2024-01-01 12:00:01.600', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 9.2233720e+18, '2024-01-01 12:00:01.800', 2147483647, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 0, '2024-01-01 12:00:01.800', 0, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', -9.2233720e+18,'2024-01-01 12:00:01.800', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL"
                ],
                "st2_int64_ct1": [
                    "'2024-01-01 12:00:00.000', -9.2233720e+18, '2024-01-01 12:00:00.000', 2147483647, NULL, NULL, 1234.56780, NULL, 'abc', NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', 0, '2024-01-01 12:00:00.100', 0, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 9.2233720e+18, '2024-01-01 12:00:00.200', 2147483647, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 0, '2024-01-01 12:00:00.200', 0, NULL, NULL, 1234.56792, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 9.2233720e+18, '2024-01-01 12:00:00.200', 2147483647, NULL, NULL, 1234.56793, NULL, 'abcde', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 9.2233720e+18,'2024-01-01 12:00:00.400', 2147483647, NULL, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', -9.2233720e+18,'2024-01-01 12:00:00.500', -2147483648, NULL, NULL, 1234.56794, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 0,'2024-01-01 12:00:00.600', 2147483647, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', -9.2233720e+18,'2024-01-01 12:00:00.600', -2147483648, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 9.2233720e+18,'2024-01-01 12:00:00.600', 2147483647, NULL, NULL, 1234.56796, NULL, 'abce', NULL, false, NULL"
                ],
                "st2_int64_ct2": [
                    "'2024-01-01 12:00:01.000', 9.2233720e+18, '2024-01-01 12:00:01.000', -2147483648, NULL, NULL, 1234.56781, NULL, 'abcd', NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', -9.2233720e+18, '2024-01-01 12:00:01.100', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 0, '2024-01-01 12:00:01.200', 0, NULL, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 9.2233720e+18, '2024-01-01 12:00:01.200', 2147483647, NULL, NULL, 1234.56795, NULL, 'abcde', NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', -9.2233720e+18, '2024-01-01 12:00:01.200', -2147483648, NULL, NULL, 1234.56796, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 9.2233720e+18, '2024-01-01 12:00:01.400', 2147483647, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', -9.2233720e+18, '2024-01-01 12:00:01.600', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 9.2233720e+18, '2024-01-01 12:00:01.800', 2147483647, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 0, '2024-01-01 12:00:01.800', 0, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', -9.2233720e+18,'2024-01-01 12:00:01.800', -2147483648, NULL, NULL, 1234.56795, NULL, 'abce', NULL, false, NULL"
                ]
            },
            "pk_str_ms": {
                "st1_str_ct1": [
                    "'2024-01-01 12:00:00.000', 'abc', '2024-01-01 12:00:00.000', 2147483647, NULL, -9.2233720e+18, NULL, 1234.56780, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', 'abcd', '2024-01-01 12:00:00.100', 0, NULL, 0, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 'abcde', '2024-01-01 12:00:00.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 'abcd', '2024-01-01 12:00:00.200', 0, NULL, 0, NULL, 1234.56792, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 'abcde', '2024-01-01 12:00:00.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56793, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 'abcd','2024-01-01 12:00:00.400', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56794, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 'abcde','2024-01-01 12:00:00.500', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56794, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 'abc','2024-01-01 12:00:00.600', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 'abcd','2024-01-01 12:00:00.600', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 'abcde','2024-01-01 12:00:00.600', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL"
                ],
                "st1_str_ct2": [
                    "'2024-01-01 12:00:01.000', 'abcd', '2024-01-01 12:00:01.000', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', 'abc', '2024-01-01 12:00:01.100', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 'abcd', '2024-01-01 12:00:01.200', 0, NULL, 0, NULL, 1234.56795, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 'abc', '2024-01-01 12:00:01.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 'abcde', '2024-01-01 12:00:01.200', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56796, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 'abc', '2024-01-01 12:00:01.400', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', 'abcd', '2024-01-01 12:00:01.600', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 'abcde', '2024-01-01 12:00:01.800', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 'abc', '2024-01-01 12:00:01.800', 0, NULL, 0, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 'abcd','2024-01-01 12:00:01.800', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL"
                ],
                "st2_str_ct1": [
                    "'2024-01-01 12:00:00.000', 'abc', '2024-01-01 12:00:00.000', 2147483647, NULL, -9.2233720e+18, NULL, 1234.56780, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.000', 'abcd', '2024-01-01 12:00:00.100', 0, NULL, 0, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.000', 'abcde', '2024-01-01 12:00:00.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 'abcd', '2024-01-01 12:00:00.200', 0, NULL, 0, NULL, 1234.56792, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.200', 'abcde', '2024-01-01 12:00:00.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56793, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 'abcd','2024-01-01 12:00:00.400', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56794, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.400', 'abcde','2024-01-01 12:00:00.500', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56794, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:00.600', 'abc','2024-01-01 12:00:00.600', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 'abcd','2024-01-01 12:00:00.600', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:00.600', 'abcde','2024-01-01 12:00:00.600', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56796, NULL, NULL, false, NULL"
                ],
                "st2_str_ct2": [
                    "'2024-01-01 12:00:01.000', 'abcd', '2024-01-01 12:00:01.000', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56781, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.000', 'abc', '2024-01-01 12:00:01.100', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 'abcd', '2024-01-01 12:00:01.200', 0, NULL, 0, NULL, 1234.56795, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.100', 'abc', '2024-01-01 12:00:01.200', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.100', 'abcde', '2024-01-01 12:00:01.200', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56796, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.400', 'abc', '2024-01-01 12:00:01.400', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.400', 'abcd', '2024-01-01 12:00:01.600', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, true, NULL",
                    "'2024-01-01 12:00:01.500', 'abcde', '2024-01-01 12:00:01.800', 2147483647, NULL, 9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 'abc', '2024-01-01 12:00:01.800', 0, NULL, 0, NULL, 1234.56795, NULL, NULL, false, NULL",
                    "'2024-01-01 12:00:01.800', 'abcd','2024-01-01 12:00:01.800', -2147483648, NULL, -9.2233720e+18, NULL, 1234.56795, NULL, NULL, false, NULL"
                ]
            },
        }
        for db in db_name_list:
            tdSql.execute(f"use {db}")
            for k in data[data_mark].keys():
                self.insert_data(db, "_".join([db, k]), data[data_mark][k])

    def sql_generator(self, join_type):
        """Sql sets for different join types, for one check point, maybe have multiple sqls, note the query
        result should be same with the expected result
        :param join_type: the join type, can be "inner", "left-outer", "right-outer", "full",
        "left-semi", "right-semi", "left-anti", "right-anti", "left-asof", "right-asof", “left-window", "right-window”
        """
        # id indicates the join type, case number and check points
        sql = {
            "inner": [
                {
                    "id": "ij_c1_1",
                    "desc": "inner join for super table with master and other connection conditions",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int >= t2.v_int or t1.v_int <= t2.v_int) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int and t1.v_int_empty is null and t2.v_int_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint = t2.v_bigint or t1.v_bigint != t2.v_bigint) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint <= t2.v_bigint) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint = t2.v_bigint or t1.v_bigint != t2.v_bigint and t1.v_bigint_empty is null and t2.v_bigint_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double and t1.v_double_empty is null and t2.v_double_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary >= t2.v_binary or t1.v_binary != t2.v_binary) and t1.v_binary_empty is null and t2.v_binary_empty is null order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool >= t2.v_bool or t1.v_bool <= t2.v_bool) and t1.v_bool_empty is null and t2.v_bool_empty is null order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts <= t2.t_ts) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int >= t2.t_int or t1.t_int <= t2.t_int) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int and t1.t_int_empty is null and t2.t_int_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint <= t2.t_bigint) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint and t1.t_bigint_empty is null and t2.t_bigint_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double >= t2.t_double or t1.t_double <= t2.t_double) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double and t1.t_double_empty is null and t2.t_double_empty is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[test]' or t1.t_binary nmatch '[test]') order by t1.v_int, t2.v_int;"
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary >= t2.t_binary or t1.t_binary != t2.t_binary) and t1.t_binary_empty is null and t2.t_binary_empty is null order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool >= t2.t_bool or t1.t_bool <= t2.t_bool) and t1.t_bool_empty is null and t2.t_bool_empty is null order by t1.v_int, t2.v_int;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,3)], [('2024-01-01 12:00:00.000'), (16)]]
                        }
                    }
                },
                {
                    "id": "ij_c1_2_1",
                    "desc": "inner join for child table with contant and scalar function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and t1.ts >='2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.v_int, t2.v_int;", 
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.v_int, t2.v_int;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts, t1.v_binary;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts=t2.ts and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts, t1.v_binary;",],
                    "res": {
                        "total_rows": 3,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (2,3)], [('2024-01-01 12:00:00.000'), (6)]]
                        }
                    }
                },
                {
                    "id": "ij_c1_2_2",
                    "desc": "inner join for super table with contant and scalar function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_int < 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_bigint <= 123456780 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_double <= 1234.56780 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary = 'abc' order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary = 'abc' and t1.v_bool = true and t2.v_bool = false order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and sin(t1.v_int) < 0 and ceil(t2.v_int) < 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and atan(t1.v_int) < 0 and round(t2.v_int) < 0 order by t1.ts;"],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0),(0,3)], [('2024-01-01 12:00:00.000'),(-3)]]
                        }
                    }
                },
                {
                    "id": "ij_c1_3",
                    "desc": "inner join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct1 t1 join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and  t2.v_int_empty is not null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and  t2.t_bigint_empty is not null and t1.v_binary_empty is null and t2.v_bool_empty is not null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st1 where ts >= now) t1, (select * from db1_st2 where ts >= now) t2 where t1.v_int = t2.v_int and t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t1, (select * from db1_st2 where ts >= now order by ts) t2 where t1.v_int = t2.v_int and t1.ts=t2.ts order by t1.ts;",],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "ij_c1_4",
                    "desc": "inner join for subquery, especially for group scenario by timestamp",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from (select * from db1_st1) t1 join (select * from db1_st2) t2 on t1.ts=t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 order by ts desc) t1 join (select * from db1_st2) t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts between '2024-01-01 12:00:00.000' and '2024-01-01 12:00:02') t1 join (select * from db1_st2 where ts >= '2024-01-01 12:00:00.000') t2 on t1.ts=t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where v_int > 0 or v_int <= 0) t1 join (select * from db1_st2) t2 on t1.ts=t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 join (select * from db1_st2) t2 on t1.ts=t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where v_int > 0 or v_int <= 0) t1 join db1_st2 t2 on t1.ts=t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0),(4,1)], [('2024-01-01 12:00:00.000'),('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ij_c2_1",
                    "desc": "inner join with filter",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000';",
                            "select t1.ts, t2.ts from db1.db1_st1 t1, db2.db2_st2 t2 where t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts < now order by t2.ts;"
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts < now order by t2.ts limit 10;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts < '2024-01-01 12:00:02.600' and (t1.v_int is null or t2.t_int_empty is null) order by t2.ts, t1.v_binary_empty;",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts < '2024-01-01 12:00:02.600' order by t2.ts, t1.v_binary_empty;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and (t1.v_int > 0 or t1.v_int <= 0);",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_bigint >= 123456780 or t2.v_bigint < 123456780);",
                            "select t1.ts, t2.ts from db1.db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and (t1.v_int > 0 or t1.v_int <= 0) and t1.v_bool in (true, false);",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_binary match '[abc]' or t2.v_binary nmatch '[abc]');"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(4,0)], [('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ij_c3_1",
                    "desc": "inner join with timetruncated function for ts",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where timetruncate(t1.ts, 1a)=t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts < '2024-01-01 12:00:02.600';",
                            "select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) and (t1.v_int > 0 or t1.v_int <= 0) and (t1.v_bool in (true, false));",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where ts <= now) t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) and (t1.v_bigint>t2.v_bigint or t1.v_bigint <= t2.v_bigint) order by t1.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where ts <= now) t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) and (t1.v_bigint=t2.v_bigint or t1.v_bigint != t2.v_bigint) order by t1.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where ts <= now) t2 where timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) and (t1.v_bool in (true, false)) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) order by t1.ts;",],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0)], [('2024-01-01 12:00:00.000'),('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ij_c4_1",
                    "desc": "inner join with nested query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select count(*) from (select t1.ts, t2.ts from db1_st1 t1, db1_st2 t2 where t1.ts=t2.ts);",
                            "select count(*) from (select t1.ts, t2.ts from (select * from db1_st1) t1, (select * from db1_st2) t2 where t1.ts=t2.ts);",
                            "select count(*) from (select t1.ts, t2.ts from (select * from db1_st1) t1, (select * from db1_st2) t2 where t1.ts=t2.ts and (t1.v_int + t2.v_bigint > t2.v_int or t1.v_int + t2.v_bigint <= t2.v_int));",
                            "select count(*) from (select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where v_int > 0 or v_int <= 0) t2 where t1.ts = t2.ts and (t1.v_bool in (true, false)));",
                            "select count(*) from (select t1.ts, avg(t2.v_int) from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where v_int > 0 or v_int <= 0) t2 where t1.ts = t2.ts and (t1.v_bool in (true, false)) group by t1.ts);",
                            "select count(*) from (select t1.ts, avg(t2.v_int) from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where v_int > 0 or v_int <= 0) t2 where t1.ts = t2.ts and (t1.v_bool in (true, false)) group by t1.ts having avg(t2.v_int) >0 or sum(t2.v_bigint) > 0);",
                            "select count(*) from (select t1.ts, avg(t2.v_int) from (select * from db1_st1 where ts <= now) t1, (select * from db1_st2 where v_int > 0 or v_int <= 0) t2 where t1.ts = t2.ts and (t1.v_bool in (true, false)) group by t1.ts having avg(t2.v_int) >0 or sum(t2.v_bigint) > 0 order by t1.ts);"],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(5)]]
                        }
                    }
                },
                {
                    "id": "ij_c5",
                    "desc": "inner join exception",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select diff(t1.v_int) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_int = t2.t_int;",
                            "select csum(t1.v_int) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_bigint = t2.t_bigint;",
                            "select derivative(t1.v_double, 1s, 0) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_binary = t2.t_binary;",
                            "select irate(t1.v_bigint) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_bool = t2.t_bool;",
                            "select mavg(t1.v_int, 2) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_ts = t2.t_ts;",
                            "select statecount(t1.v_int, 'eq', 1) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_int = t2.t_int;",
                            "select stateduration(t1.v_int, 'gt', 100, 1m) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_bigint = t2.t_bigint;",
                            "select twa(t1.v_int) from db1_st1 t1 join db1_st2 t2 on t1.ts = t2.ts and t1.t_double = t2.t_double;",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 join db1_st2 on t1.ts = t2.ts;"],
                }
            ],
            "left-outer": [
                {
                    "id": "loj_c1_1",
                    "desc": "left outer join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts order by t1.ts",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts = t2.v_ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_ts >= t2.v_ts or t1.v_ts < t1.v_ts) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t1.v_int_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint = t2.v_bigint or t1.v_bigint != t2.v_bigint) and t2.v_bigint_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t1.v_binary_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_bool_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int >= t2.t_int or t1.t_int < t2.t_int) and t1.t_int_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double >= t2.t_double or t1.t_double <= t2.t_double) and t1.t_double_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t1.t_binary_empty is null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.t_bool_empty is null order by t1.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,3), (9,2)], [('2024-01-01 12:00:00.000'), (16), (16)]]
                        }
                    }
                },
                {
                    "id": "loj_c1_2",
                    "desc": "left outer join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1.db1_st1 t1 left join db2.db2_st2 t2 on t1.ts=t2.ts and t1.ts >='2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts;", 
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and t2.v_int > 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1.db1_st1 t1 left join db2.db2_st2 t2 on t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and t2.v_int = 9 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1.db1_st1 t1 left join db2.db2_st2 t2 on t1.ts=t2.ts and t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1.db1_st1 t1 left join db2.db2_st2 t2 on t1.ts=t2.ts and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1.db1_st1 t1 left join db2.db2_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts;"
                            ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (8,3)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (9)]]
                        }
                    }
                },
                {
                    "id": "loj_c1_3",
                    "desc": "left outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st_empty t2 on t1.ts=t2.ts and t1.ts <= now order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and  t2.v_int_empty is not null order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and  t2.t_bigint_empty is not null and t1.v_binary_empty is null and t2.v_bool_empty is not null order by t1.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (1,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "loj_c1_4",
                    "desc": "left outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 left join db1_st2 t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 left join db1_st1 t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 left join db1_st2 t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 left join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st1 where ts >= now) t1 left join (select * from db1_st2 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t1 left join (select * from db1_st2 where ts >= now order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t1.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "loj_c2_1",
                    "desc": "left outer join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400';",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t2.ts < now or t2.ts is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t2.ts < now or t2.ts is null) order by t1.ts limit 8;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) order by t1.ts;",
                            "select t1.ts, t2.ts, t1.v_bool, t2.v_bool from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_bool in (true, false) or t2.v_bool is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t2.v_double is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.v_binary is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t2.t_bool is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.400' and (t1.t_int * t2.v_int_empty is null or t1.t_int * t2.v_int_empty is not null) order by t1.ts;"],
                    "res": {
                        "total_rows": 8,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (7,0), (6,1), (7,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600'), (None)]]
                        }
                    }
                },
                {
                    "id": "loj_c3_1",
                    "desc": "left outer join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where t1.ts >= '2024-01-01 12:00:00.000' and (t2.ts < '2024-01-01 12:00:02.600' or t2.ts is null) order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t1.v_int > 0 or t1.v_int <= 0) and (t1.v_bool in (true, false)) order by t1.ts limit 11;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t1.v_bigint>t2.v_bigint or t1.v_bigint <= t2.v_bigint or t2.v_bigint is null) order by t1.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t1.v_bigint=t2.v_bigint or t1.v_bigint != t2.v_bigint or t2.v_bigint is null) order by t1.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t1.v_bool in (true, false)) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t2.v_binary is null) order by t1.ts;",],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (8,1),(9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600'), (None)]]
                        }
                    }
                },
                {
                    "id": "loj_c4_1",
                    "desc": "left outer join for super table with nested query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0)) group by tbname order by tbname limit 2;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.v_double > 0 or t1.v_double <= 0 or t2.v_double is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%')) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t2.v_double is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is null)) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now) group by tbname having(sum(v_int)) > 0 order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t1.ts <= now) group by tbname order by tbname limit 2;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') where t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t2.v_double is null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t2.v_double is null) group by tbname order by tbname;",
                            "select first(ts1), tbname, avg(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t1.tbname from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (1,0), (0,2), (1,2)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.000'), (5), (9)]]
                        }
                    }
                },
                {
                    "id": "loj_c5_1",
                    "desc": "left outer join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 left join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 left join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 left join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 left join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 left join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 left join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 left join db1_st2 on t1.ts = t2.ts;"],
                }
            ],
            "right-outer": [
                {
                    "id": "roj_c1_1",
                    "desc": "right outer join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_ts = t2.v_ts or t1.v_ts != t2.v_ts) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1.db1_st1 t1 right join db2.db2_st2 t2 on t1.ts=t2.ts and (t1.v_ts >= t2.v_ts or t1.v_ts < t1.v_ts) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1.db1_st1 t1 right join db2.db2_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1.db1_st1 t1 right join db2.db2_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t1.v_int_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint = t2.v_bigint or t1.v_bigint != t2.v_bigint) and t2.v_bigint_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1.db1_st1 t1 right join db2.db2_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t1.v_binary_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_bool_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int >= t2.t_int or t1.t_int < t2.t_int) and t1.t_int_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double >= t2.t_double or t1.t_double <= t2.t_double) and t1.t_double_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t1.t_binary_empty is null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.t_bool_empty is null order by t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (9,0), (9,3)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:00.900')]]
                        }
                    }
                },
                {
                    "id": "roj_c1_2",
                    "desc": "right outer join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts >='2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' order by t2.ts;", 
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and t2.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t2.ts;"
                            ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (8,2)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "roj_c1_3",
                    "desc": "right outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right join db1_st1 t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right join db1_st1 t2 on t1.ts=t2.ts and t2.ts <= now order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right join db1_st1 t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is null and  t2.v_int_empty is not null order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right join db1_st1 t2 on t1.ts=t2.ts and t1.t_int_empty is null and  t2.t_bigint_empty is not null and t1.v_binary_empty is null and t2.v_bool_empty is not null order by t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "roj_c1_4",
                    "desc": "right outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st2 t1 right join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 right join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st2 t1 right join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 right join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 right join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 right join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "roj_c2_1",
                    "desc": "right outer join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400';",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.ts < now or t1.ts is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.ts < now or t1.ts is null) order by t2.ts limit 6;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_bool, t2.v_bool from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t2.v_bool in (true, false) or t1.v_bool is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t1.v_double is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.v_binary is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t1.t_bool is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.400' and (t1.t_int * t2.v_int_empty is null or t1.t_int * t2.v_int_empty is not null) order by t2.ts;"],
                    "res": {
                        "total_rows": 6,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (5,0), (5,1)], [('2024-01-01 12:00:00.400'), (None), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "roj_c3_1",
                    "desc": "right outer join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where t2.ts >= '2024-01-01 12:00:00.000' and (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0) and (t2.v_bool in (true, false)) order by t2.ts limit 11;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false)) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null) order by t2.ts;",],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (8,1),(9,1)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.600'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "roj_c4_1",
                    "desc": "right outer join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_int > 0 or t2.v_int <= 0)) group by tbname order by tbname limit 2;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0 or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_double > 0 or t2.v_double <= 0 or t1.v_double is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%')) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null)) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now) group by tbname having(sum(v_int)) > 0 order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.ts <= now) group by tbname order by tbname limit 2;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t1.v_double is null) group by tbname order by tbname;",
                            "select first(ts2), tbname, sum(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (1,0), (0,2), (1,2)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.500'), (7), (58)]]
                        }
                    }
                },
                {
                    "id": "roj_c5_1",
                    "desc": "right outer join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 right join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 right join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 right join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 right join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 right join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 right join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 right join db1_st2 on t1.ts = t2.ts;"],
                }
            ],
            "full": [
                {
                    "id": "fj_c1_1",
                    "desc": "full join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts order by t1.ts, t2.ts",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_ts = t2.v_ts or t1.v_ts != t2.v_ts or t1.v_ts is null or t2.v_ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_ts >= t2.v_ts or t1.v_ts < t1.v_ts) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t1.v_int_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint = t2.v_bigint or t1.v_bigint != t2.v_bigint) and t2.v_bigint_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t1.v_binary_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_bool_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int >= t2.t_int or t1.t_int < t2.t_int) and t1.t_int_empty is null order by t1.ts, t2.ts",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint >= t2.t_bigint or t1.t_bigint < t2.t_bigint) and t2.t_bigint_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double >= t2.t_double or t1.t_double <= t2.t_double) and t1.t_double_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t1.t_binary_empty is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.t_bool_empty is null order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 15,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (14,0), (13,1), (4,3), (14,3)], [(None), ('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600'), (14), (None)]]
                        }
                    }
                },
                {
                    "id": "fj_c1_2",
                    "desc": "full outer join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' and t2.ts > '2024-01-01 12:00:00.300' and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts >='2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts, t2.ts;", 
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 17,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (16,0), (15,1), (7,3), (15,3)], [(None), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600'), (None), ('2024-01-01 12:00:00.600')]]
                        }
                    }
                },
                {
                    "id": "fj_c1_3",
                    "desc": "full outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 full join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 full join db1_st1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 full join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 full join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 full join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 full join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 full join db1_st1 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t2.ts is not null order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "fj_c1_4",
                    "desc": "full outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st_empty t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "fj_c1_5",
                    "desc": "full outer join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 full join db1_st_empty t2 on t1.ts=t2.ts and t1.ts > now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct_empty t1 full join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 full join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 full join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 full join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 full join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "fj_c2_1",
                    "desc": "full outer join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.ts < now where t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-01-02 12:00:00.400' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_int) >= 0 where t1.ts between '2023-12-31 12:00:00.400' and now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.v_bigint >= 123456780 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts limit 5;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_bigint - t2.v_bigint) >= 0 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and abs(ceil(t1.v_double)) >= 0 where (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool is null or t1.v_bool is not null) where (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t1.v_int in (-2, 0, 2, 14, 16) where (t2.v_bool in (true, false) or t1.v_bool is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') and t1.v_binary is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t1.v_double is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.v_binary is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t1.t_bool is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_int * t2.v_int_empty is null and t1.t_int is not null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "fj_c3_1",
                    "desc": "full outer join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.ts >= '2024-01-01 12:00:00.000' or t2.ts is null) or (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 full join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0 or t2.v_int is null) and (t2.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts limit 15;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 full join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 full join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 full join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false) or t2.v_bool is null) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null or t2.v_binary is null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 15,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (14,0),(14,1)], [(None), ('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "fj_c4_1",
                    "desc": "full outer join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t1.v_int != t2.v_int) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.v_int > t2.v_int or t1.v_int < t2.v_int)) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0 or t1.v_bigint is null or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_double > 0 or t2.v_double <= 0 or t1.v_double is null or t2.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%')) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now) group by tbname having(sum(v_bigint)) > 0 order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.ts <= now) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is not null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is not null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t1.v_double is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is not null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (1,1)], [(17), (0)]]
                        }
                    }
                },
                {
                    "id": "fj_c4_2",
                    "desc": "full outer join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 full join db1_st2_ct1 t2 on t1.ts = t2.ts) union all (select t2.ts from db1_st1_ct2 t1 full join db1_st2_ct2 t2 on t1.ts = t2.ts));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts = t2.ts) union (select t2.ts from db1_st1 t1 full join db1_st2 t2 on t1.ts = t2.ts));",],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(16)]]
                        }
                    }
                },
                {
                    "id": "fj_c5_1",
                    "desc": "full outer join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 full join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 full join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 full join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 full join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 full join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select first(ts2), tbname, count(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts) group by tbname order by tbname;",
                            "select last(ts2), tbname, count(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts) group by tbname order by tbname;",
                            "select diff(ts2), tbname, count(v_int) from (select t1.ts ts1, t2.ts ts2, t2.v_int, t2.tbname from db1_st1 t1 full join db1_st2 t2 on t1.ts=t2.ts) group by tbname order by tbname;",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 full join db1_st2 on t1.ts = t2.ts;"],
                }
            ],
            "left-semi": [
                {
                    "id": "ls_c1_1",
                    "desc": "left semi join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts > '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:01.600' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts >= t2.v_ts and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_double_empty is null and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is null and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_bool_empty is null and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) and t2.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and t2.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.t_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_double_empty is null and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_binary_empty is null and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_bool_empty is null and t1.v_bigint > 123456780 order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 4,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (3,0)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ls_c1_2",
                    "desc": "left semi join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' and t2.ts > '2024-01-01 12:00:00.300' and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts >='2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts, t2.ts;", 
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 3,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (1,0), (2,0)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ls_c1_3",
                    "desc": "left semi join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left semi join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left semi join db1_st1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left semi join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 left semi join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left semi join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left semi join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 left semi join db1_st1 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_st_empty t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 left semi join db1_st_empty t2 on t1.ts=t2.ts and t1.ts > now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct_empty t1 left semi join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 left semi join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 left semi join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 left semi join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 left semi join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "ls_c2_1",
                    "desc": "left semi join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts < now where t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-01-02 12:00:00.400' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_int) >= 0 where t1.ts between '2023-12-31 12:00:00.400' and now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_bigint >= 123456780 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts limit 5;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_bigint - t2.v_bigint) >= 0 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and abs(ceil(t1.v_double)) >= 0 where (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool is null or t1.v_bool is not null) where (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int in (-2, 0, 2, 14, 16) where (t2.v_bool in (true, false) or t1.v_bool is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') and t1.v_binary is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t1.v_double is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.v_binary is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t1.t_bool is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_int * t2.v_int_empty is null and t1.t_int is not null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ls_c3_1",
                    "desc": "left semi join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.ts >= '2024-01-01 12:00:00.000' or t2.ts is null) or (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0 or t2.v_int is null) and (t2.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts limit 15;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false) or t2.v_bool is null) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null or t2.v_binary is null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ls_c4_1",
                    "desc": "left semi join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t1.v_int != t2.v_int) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.v_int > t2.v_int or t1.v_int < t2.v_int)) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0 or t1.v_bigint is null or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_double > 0 or t2.v_double <= 0 or t1.v_double is null or t2.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%')) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now) group by tbname having(sum(v_bigint)) > 0 order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.ts <= now) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is not null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is not null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t1.v_double is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is not null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (1,1)], [(17), (0)]]
                        }
                    }
                },
                {
                    "id": "ls_c4_2",
                    "desc": "left semi join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 left semi join db1_st2_ct1 t2 on t1.ts = t2.ts) union all (select t2.ts from db1_st1_ct2 t1 left semi join db1_st2_ct2 t2 on t1.ts = t2.ts));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts = t2.ts where t1.ts != '2024-01-01 12:00:00.800') union (select t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts = t2.ts where t2.ts != '2024-01-01 12:00:00.800'));",],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(4)]]
                        }
                    }
                },
                {
                    "id": "ls_c5_1",
                    "desc": "left semi join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 left semi join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 left semi join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 left semi join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 left semi join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 left semi join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 left semi join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 left semi join db1_st2 on t1.ts = t2.ts;"]
                }
            ],
            "right-semi": [
                {
                    "id": "rs_c1_1",
                    "desc": "right semi join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts > '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:01.600' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts >= t2.v_ts and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_double_empty is null and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is null and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_bool_empty is null and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) and t2.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and t2.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.t_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_double_empty is null and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_binary_empty is null and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_bool_empty is null and t1.v_bigint > 123456780 order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 4,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (3,0)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "rs_c1_2",
                    "desc": "right semi join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' and t2.ts > '2024-01-01 12:00:00.300' and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts >='2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts, t2.ts;", 
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 3,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (1,0), (2,0)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "rs_c1_3",
                    "desc": "right semi join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right semi join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right semi join db1_st1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right semi join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right semi join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right semi join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 right semi join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right semi join db1_st1 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_st_empty t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 right semi join db1_st_empty t2 on t1.ts=t2.ts and t1.ts > now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct_empty t1 right semi join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 right semi join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 right semi join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 right semi join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 right semi join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "rs_c2_1",
                    "desc": "right semi join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts < now where t1.ts >= '2024-01-01 12:00:00.000' and t2.ts <= '2024-01-02 12:00:00.400' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_int) >= 0 where t1.ts between '2023-12-31 12:00:00.400' and now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_bigint >= 123456780 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts limit 5;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_bigint - t2.v_bigint) >= 0 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and abs(ceil(t1.v_double)) >= 0 where (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool is null or t1.v_bool is not null) where (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int in (-2, 0, 2, 14, 16) where (t2.v_bool in (true, false) or t1.v_bool is not null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') and t1.v_binary is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t1.v_double is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.v_binary is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t1.t_bool is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_int * t2.v_int_empty is null and t1.t_int is not null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "rs_c3_1",
                    "desc": "right semi join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.ts >= '2024-01-01 12:00:00.000' or t2.ts is null) or (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0 or t2.v_int is null) and (t2.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts limit 15;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right semi join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false) or t2.v_bool is null) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null or t2.v_binary is null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "rs_c4_1",
                    "desc": "right semi join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t1.v_int != t2.v_int) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.v_int > t2.v_int or t1.v_int < t2.v_int)) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0 or t1.v_bigint is null or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_double > 0 or t2.v_double <= 0 or t1.v_double is null or t2.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%')) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null)) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now) group by tbname having(sum(v_bigint)) > 0 order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.ts <= now) group by tbname order by tbname limit 2;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is not null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is not null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t1.v_double is null) group by tbname order by tbname;",
                            "select tbname, spread(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is not null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (1,1)], [(17), (0)]]
                        }
                    }
                },
                {
                    "id": "rs_c4_2",
                    "desc": "right semi join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 right semi join db1_st2_ct1 t2 on t1.ts = t2.ts) union all (select t2.ts from db1_st1_ct2 t1 left semi join db1_st2_ct2 t2 on t1.ts = t2.ts));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts = t2.ts where t1.ts != '2024-01-01 12:00:00.800') union (select t2.ts from db1_st1 t1 left semi join db1_st2 t2 on t1.ts = t2.ts where t2.ts != '2024-01-01 12:00:00.800'));",],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(4)]]
                        }
                    }
                },
                {
                    "id": "rs_c5_1",
                    "desc": "right semi join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 right semi join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 right semi join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 right semi join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 right semi join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 right semi join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 right semi join db1_st2 on t1.ts = t2.ts;"]
                }
            ],
            "left-anti": [
                {
                    "id": "la_c1_1",
                    "desc": "left anti join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts > '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:01.600' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts >= t2.v_ts and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint_empty is null and t1.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_double_empty is null and t1.ts > '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t1.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is null and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_bool_empty is null and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) and t2.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and t2.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.t_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.t_double_empty is null and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.t_binary_empty is null and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.t_bool_empty is null and t1.v_bigint > 123456780 order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 6,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (5,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "la_c1_2",
                    "desc": "left anti join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' and t2.ts > '2024-01-01 12:00:00.300' and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts >='2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts, t2.ts;", 
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 7,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (6,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "la_c1_3",
                    "desc": "left anti join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left anti join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left anti join db1_st1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left anti join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 left anti join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left anti join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_t_empty t1 left anti join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 left anti join db1_st1 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where ts > now) t1 left anti join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where ts > '2024-02-01 01:00:00.000') t1 left anti join db1_st1 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where v_int > 738437) t1 left anti join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where v_bigint = 99999) t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where v_bool is null) t1 left anti join db1_st1 t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where v_binary = 'test') t1 left anti join db1_st1 t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from (select * from db1_st1 where v_int_empty is not null) t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 left anti join db1_st_empty t2 on t1.ts=t2.ts and t1.ts > now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct_empty t1 left anti join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 left anti join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 left anti join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 left anti join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 left anti join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "la_c2_1",
                    "desc": "left anti join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now where t1.ts >= '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_int) >= 0 where t1.ts between '2023-12-31 12:00:00.400' and now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_bigint >= 123456780 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts limit 5;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_bigint - t2.v_bigint) >= 0 where (t2.ts < now or t1.ts is not  null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or t1.v_int + t2.v_int is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and abs(ceil(t1.v_double)) >= 0 where (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2024-01-01 12:00:00.000' and now where (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool is null or t1.v_bool is not null) where (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.v_int in (-2, 0, 2, 14, 16) where (t1.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t2.v_binary is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t2.v_double is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.v_binary is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t1.t_bool in (true, false) or t1.t_bool is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts >= '2024-01-01 12:00:00.000' and (t1.t_int * t2.v_int_empty is null and t1.t_int is not null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0)], [('2024-01-01 12:00:00.600'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "la_c3_1",
                    "desc": "left anti join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.ts >= '2024-01-01 12:00:00.000' or t2.ts is null) or (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0 or t2.v_int is null) and (t2.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts limit 15;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 left anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false) or t2.v_bool is null) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null or t2.v_binary is null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0)], [('2024-01-01 12:00:00.600'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "la_c4_1",
                    "desc": "left anti join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.v_int > 0) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0 or t1.v_bigint is null or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t2.v_double > 0 or t2.v_double <= 0 or t1.v_double is null or t2.v_double is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.v_int > t2.v_int or t1.v_int < t2.v_int or t2.v_int is null)) group by tbname order by tbname limit 2;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%' or t2.v_binary is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t2.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t2.v_double is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t2.t_binary is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts where t1.ts <= now) group by tbname having(sum(v_bigint)) > 0 order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now where t1.t_ts <= now) group by tbname order by tbname limit 2;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0 or t2.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t2.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t2.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%' or t2.v_binary is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t2.v_bigint is null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t2.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t2.v_double is null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t2.v_double is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint, t1.tbname from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t2.t_binary is null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t2.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (1,1)], [(123456796), (493827180)]]
                        }
                    }
                },
                {
                    "id": "la_c4_2",
                    "desc": "left anti join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 left anti join db1_st2_ct1 t2 on t1.ts = t2.ts) union all (select t1.ts from db1_st1_ct2 t1 left anti join db1_st2_ct2 t2 on t1.ts = t2.ts));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts = t2.ts where t1.ts >= '2024-01-01 12:00:00.000') union (select t2.ts from db1_st1 t1 left anti join db1_st2 t2 on t1.ts = t2.ts where t1.ts between '2024-01-01 12:00:00.000' and '2024-01-01 12:00:01.000'));",],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(6)]]
                        }
                    }
                },
                {
                    "id": "la_c5_1",
                    "desc": "left anti join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 left anti join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 left anti join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 left anti join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 left anti join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 left anti join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 left anti join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 left anti join db1_st2 on t1.ts = t2.ts;"]
                }
            ],
            "right-anti": [
                {
                    "id": "ra_c1_1",
                    "desc": "right anti join for super table with master and other connection condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.v_ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:01.600' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_ts >= t2.v_ts and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int = t2.v_int or t1.v_int != t2.v_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_int > t2.v_int or t1.v_int <= t2.v_int) and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_int_empty is null and t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bigint >= t2.v_bigint or t1.v_bigint < t2.v_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double = t2.v_double or t1.v_double != t2.v_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_double >= t2.v_double or t1.v_double <= t2.v_double) and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_double_empty is null and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_binary_empty is null and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.v_bool = t2.v_bool or t1.v_bool != t2.v_bool) and t1.v_binary != t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.v_bool_empty is null and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts = t2.t_ts or t1.t_ts != t2.t_ts) and t1.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_ts >= t2.t_ts or t1.t_ts < t2.t_ts) and t2.ts != '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_int = t2.t_int or t1.t_int != t2.t_int) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_int_empty is null and t2.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t2.t_bigint_empty is null and t2.v_bigint != 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_double = t2.t_double or t1.t_double != t2.t_double) and t1.v_double > 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_double_empty is null and t2.v_double != 1234.567800000000034 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary) and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_binary match '[abc]' or t1.t_binary nmatch '[abc]') and t2.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_binary_empty is null and t1.v_binary != 'abc' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool) and t1.v_bigint > 123456780 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right semi join db1_st2 t2 on t1.ts=t2.ts and t1.t_bool_empty is null and t1.v_bigint > 123456780 order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 4,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (3,0)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ra_c1_2",
                    "desc": "right anti join for super table with contionn of scalar function and constant",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts between '2023-01-01 12:00:00.000' and '2024-03-01 12:00:00.300' and t2.ts > '2024-01-01 12:00:00.300' and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts >='2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' order by t1.ts, t2.ts;", 
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t2.ts <= '2024-03-01 12:00:00.300' and (t2.v_int = 9 or t2.v_int != 9) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= now and (t1.v_int > 0 or t1.v_int <= 0) and (t2.v_int >= 0 or t2.v_int < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and t1.ts <= '2024-03-01 12:00:00.300' and abs(t1.v_int) >= 0 and (acos(t2.v_int) > 0 or acos(t2.v_int) <= 0 or acos(t2.v_int) is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (asin(t1.v_bigint) is null or asin(t1.v_int) >= 0 or asin(t1.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (atan(t1.v_double) is null or atan(t1.v_double) > 0 or atan(t1.v_double) <= 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and length(t1.v_binary) >= 0 and char_length(t2.v_binary) >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and (t1.v_binary like '%abc%' or t1.v_binary not like '%abc%') order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts >= '2024-01-01 12:00:00.300' and cast(t1.t_ts as timestamp) <= now and (t2.v_int > 0 or t2.v_int <= 0) order by t1.ts, t2.ts;"
                            ],
                    "res": {
                        "total_rows": 7,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (6,1)], [(None), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ra_c1_3",
                    "desc": "right anti join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join db1_st_empty t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join db1_t_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right anti join db1_st1 t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join db1_t_empty t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join db1_t_empty t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right anti join db1_st1 t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join (select * from db1_st1 where ts > now) t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join (select * from db1_st1 where ts > '2024-02-01 01:00:00.000') t2 on t1.ts=t2.ts and t1.ts <= now and t2.ts <= now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join (select * from db1_st1 where v_int > 738437) t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right anti join (select * from db1_st1 where v_bigint = 99999) t2 on t1.ts=t2.ts and t1.v_int_empty is not null and t2.v_int_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join (select * from db1_st1 where v_bool is null) t2 on t1.ts=t2.ts and abs(t1.v_int+t2.v_int) = 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right anti join (select * from db1_st1 where v_binary = 'test') t2 on t1.ts=t2.ts and tan(t1.v_int_empty) = 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st2 t1 right anti join (select * from db1_st1 where v_int_empty is not null) t2 on t1.ts=t2.ts and t1.v_binary_empty is not null and t2.v_bool_empty is not null where t1.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st_empty t1 right anti join db1_st_empty t2 on t1.ts=t2.ts and t1.ts > now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_st1_ct_empty t1 right anti join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 right anti join db1_st_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from db1_t_empty t1 right anti join db1_t_empty t2 on t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now) t1 right anti join (select * from db1_st1 where ts >= now) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;",
                            "select t1.ts, t2.ts, t1.v_int, t2.v_int from (select * from db1_st2 where ts >= now order by ts) t1 right anti join (select _wstart ts, sum(v_int) v_int from db1_st1 where ts >= now partition by tbname interval(1m) order by ts) t2 on t1.v_int = t2.v_int and t1.ts=t2.ts order by t2.ts;"],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "ra_c2_1",
                    "desc": "right anti join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.ts >= '2024-01-01 12:00:00.000' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and abs(t2.v_int) >= 0 where t2.ts between '2023-12-31 12:00:00.400' and now order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.v_bigint >= 123456780 where (t1.ts < now or t2.ts is not  null) order by t1.ts, t2.ts limit 5;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and abs(t1.v_bigint - t2.v_bigint) >= 0 where (t1.ts < now or t2.ts is not  null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts between '2024-01-01 12:00:00.000' and now where (t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or t1.v_int + t2.v_int is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and abs(ceil(t2.v_double)) >= 0 where (t1.v_int * t2.v_int > 0 or t1.v_int * t2.v_int is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts between '2024-01-01 12:00:00.000' and now where (t1.v_bigint / t2.v_bigint > 0 or t1.v_bigint / t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and (t2.v_bool is null or t2.v_bool is not null) where (t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.v_int not in (-1, 4, 7, 12, 14) where (t2.v_bool in (true, false) or t1.v_bool is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') and t1.v_binary is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t2.v_int is not null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t1.t_bigint > t2.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t1.t_double > t2.t_double or t1.t_double <= t2.t_double or t1.v_double is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.v_binary is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_bool = t2.t_bool or t1.t_bool != t2.t_bool or t2.t_bool in (true, false) or t2.t_bool is null) order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts >= '2024-01-01 12:00:00.000' and (t1.t_int * t2.v_int_empty is null and t2.t_int is not null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (4,1)], [('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ra_c3_1",
                    "desc": "right anti join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.ts >= '2024-01-01 12:00:00.000' or t2.ts is null) or (t1.ts < '2024-01-01 12:00:02.600' or t1.ts is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right anti join db1_st2 t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_int > 0 or t2.v_int <= 0 or t2.v_int is null) and (t2.v_bool in (true, false) or t2.v_bool is null) order by t1.ts, t2.ts limit 15;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint>t1.v_bigint or t2.v_bigint <= t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bigint=t1.v_bigint or t2.v_bigint != t1.v_bigint or t1.v_bigint is null or t2.v_bigint is null) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from (select * from db1_st1 where ts <= now) t1 right anti join (select * from db1_st2 where ts <= now) t2 on timetruncate(t1.ts, 1a)=timetruncate(t2.ts,1a) where (t2.v_bool in (true, false) or t2.v_bool is null) and (t1.v_binary = t2.v_binary or t1.v_binary != t2.v_binary or t1.v_binary is null or t2.v_binary is null) order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (4,1)], [('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ra_c4_1",
                    "desc": "right anti join for super table with nested query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.v_int > 0 or t2.v_int <= 0) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_bigint > 0 or t2.v_bigint <= 0)) group by tbname order by tbname;", 
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.v_double > 0 or t1.v_double <= 0 or t2.v_double is null or t1.v_double is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.v_int > t2.v_int or t1.v_int < t2.v_int or t1.v_int is null)) group by tbname order by tbname limit 2;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%' or t1.v_binary is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null)) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts where t2.ts <= now) group by tbname having(sum(v_bigint)) > 0 order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now where t2.t_ts <= now) group by tbname order by tbname limit 2;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.v_int > 0 or t1.v_int <= 0 or t1.v_int is null) where t1.v_int + t2.v_int > 0 or t1.v_int + t2.v_int <= 0 or (t1.v_int + t2.v_int) is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_bigint > 0 or t1.v_bigint <= 0 or t1.v_bigint is null) where t1.v_bigint * t2.v_bigint > 0 or t1.v_bigint * t2.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_double > 0 or t1.v_double <= 0 or t1.v_double is null) where t1.v_double - t2.v_double != 0 or t1.v_double - t2.v_double = 0 or t1.v_double - t2.v_double is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t2.v_binary like '%abc%' or t2.v_binary not like '%abc%') where t2.v_binary like '%abc%' or t2.v_binary not like '%abc%' or t1.v_binary is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) where t1.t_int = t2.t_int or t1.t_int != t2.t_int or t1.v_int is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t2.t_bigint <= t1.t_bigint or t1.v_bigint is null) where t1.t_bigint = t2.t_bigint or t1.t_bigint != t2.t_bigint or t2.t_bigint > t1.t_bigint or t1.t_bigint <= t2.t_bigint or t1.v_bigint is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t2.t_double <= t1.t_double or t1.v_double is null) where t1.t_double = t2.t_double or t1.t_double != t2.t_double or t2.t_double > t1.t_double or t1.t_double <= t2.t_double or t1.v_double is null) group by tbname order by tbname;",
                            "select tbname, sum(v_bigint) from (select t1.ts ts1, t2.ts ts2, t2.v_bigint, t2.tbname from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts and t2.ts <= now and (t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t1.t_binary like '%abc%' or t1.t_binary not like '%abc%' or t1.t_binary is null) where t1.t_binary = t2.t_binary or t1.t_binary != t2.t_binary or t2.t_binary like '%abc%' or t2.t_binary not like '%abc%' or t1.t_binary is null) group by tbname order by tbname;",
                            ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (1,1)], [(246913586), (370370385)]]
                        }
                    }
                },
                {
                    "id": "ra_c4_2",
                    "desc": "right anti join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t2.ts from db1_st1_ct1 t1 right anti join db1_st2_ct1 t2 on t1.ts = t2.ts) union all (select t2.ts from db1_st1_ct2 t1 right anti join db1_st2_ct2 t2 on t1.ts = t2.ts));",
                            "select count(*) from (select ts from (select t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts = t2.ts where t2.ts >= '2024-01-01 12:00:00.000') union (select t2.ts from db1_st1 t1 right anti join db1_st2 t2 on t1.ts = t2.ts and t1.ts != '2024-01-01 12:00:00.800' where t2.ts between '2024-01-01 12:00:00.000' and '2024-01-01 12:00:01.000'));",],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(6)]]
                        }
                    }
                },
                {
                    "id": "ra_c5_1",
                    "desc": "right anti join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 right anti join db1_st2 t2 on t1.v_ts=t2.v_ts;",
                            "select * from db1_st1_ct1 t1 right anti join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 right anti join db1_st2 t2 on t1.ts < t2.ts;",
                            "select * from db1_st1 t1 right anti join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 right anti join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 right anti join db1_st2 t2 on timetruncate(t1.ts, 1sa)=timetruncate(t2.ts, 1sa);",
                            "select t1.ts, total from (select _wstart as ts, sum(v_int) total from db1_st1 partition by tbname interval(2s)) t1 right anti join db1_st2 on t1.ts = t2.ts;"]
                }
            ],
            "left-asof": [
                {
                    "id": "las_c1_1",
                    "desc": "left asof join for super table with master connection condition by >=、<=",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= t2.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts <= t1.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts <= t1.ts jlimit 1 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "las_c1_2",
                    "desc": "left asof join for super table with master connection condition >、<",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts > t2.ts order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts > t2.ts jlimit 1 order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts < t1.ts order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts < t1.ts jlimit 1 order by t1.ts, t2.ts;",
                    ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "las_c1_3",
                    "desc": "left asof join for super table with master connection condition =",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts = t1.ts order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts = t1.ts jlimit 1 order by t1.ts, t2.ts;",
                    ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (3,0), (3,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.600'), (None), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "las_c1_4",
                    "desc": "left asof join for super table with master and other connection conditions",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_int = t2.v_int order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_bigint = t2.v_bigint order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_ts = t2.v_ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_double = t2.v_double order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_binary = t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_bool = t2.v_bool order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_ts = t2.t_ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_int = t2.t_int order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_bigint = t2.t_bigint order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_double = t2.t_double order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_binary = t2.t_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.t_bool = t2.t_bool order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st1 t2 on t1.v_ts = t2.v_ts and t1.v_int = t2.v_int and t1.v_bigint = t2.v_bigint and t1.v_double = t2.v_double and t1.v_bool = t2.v_bool and t1.v_binary = t2.v_binary and t1.t_ts = t2.t_ts and t1.t_int = t2.t_int and t1.t_bigint = t2.t_bigint and t1.t_double = t2.t_double and t1.t_bool = t2.t_bool and t1.t_binary = t2.t_binary order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "las_c1_5",
                    "desc": "left asof join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts>=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts>t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts<=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts<t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts=t2.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts>=t2.ts jlimit 2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts>t2.ts jlimit 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts<=t2.ts jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.ts<t2.ts jlimit 1023 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.v_int_empty = t2.v_int_empty jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.v_bigint_empty = t2.v_bigint_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.v_double_empty = t2.v_double_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.v_binary_empty = t2.v_binary_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.v_bool_empty = t2.v_bool_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.t_int_empty = t2.t_int_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.t_bigint_empty = t2.t_bigint_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.t_double_empty = t2.t_double_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.t_binary_empty = t2.t_binary_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 left asof join db1_st1 t2 on t1.t_bool_empty = t2.t_bool_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 jlimit 0;"
                        ],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "las_c1_6",
                    "desc": "left asof join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts>=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts>t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts<=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts<t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts=t2.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts>=t2.ts jlimit 2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts>t2.ts jlimit 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts<=t2.ts jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.ts<t2.ts jlimit 1023 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.v_int_empty = t2.v_int_empty jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.v_bigint_empty = t2.v_bigint_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.v_double_empty = t2.v_double_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.v_binary_empty = t2.v_binary_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.v_bool_empty = t2.v_bool_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.t_int_empty = t2.t_int_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.t_bigint_empty = t2.t_bigint_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.t_double_empty = t2.t_double_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.t_binary_empty = t2.t_binary_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 left asof join db1_st_empty t2 on t1.t_bool_empty = t2.t_bool_empty jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "las_c2_1",
                    "desc": "left asof join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and t1.ts >= t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_bigint > 123456792 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.v_bigint > 123456793 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_binary like '%abce%' order by t1.ts, t2.ts limit 8;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_bigint > 123456792 and t2.v_binary like '%abcg%' order by t1.ts, t2.ts limit 9;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_binary like '%abce%' and t1.ts between '2024-01-01 00:00:00.000' and now order by t1.ts, t2.ts limit 8;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_bool in (true, false) and t1.v_int > 0 and (t2.v_bool = true or t2.v_bool = false) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_double in (1234.567939999999908, 1234.567960000000085, 1234.567980000000034, 1234.567950000000110) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.v_double in (1234.567970000000059, 1234.567950000000110) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.t_int >= 1 and t2.ts > '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.t_bigint >= 123456789 and t2.ts > '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.t_double >= 1234.567890000000034 and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.t_binary = 'test message' and t1.t_bool in (true, false) and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.t_bool in (true, false) and t1.t_int >= 1 and t1.t_bigint >= 123456789 and t1.t_double >= 1234.567890000000034 and t1.t_binary = 'test message' and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.t_int >= 1 and t2.ts > '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.t_bigint >= 123456789 and t2.ts > '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.t_double >= 1234.567890000000034 and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.t_binary = 'test message' and t1.t_bool in (true, false) and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.t_bool in (true, false) and t1.t_int >= 1 and t1.t_bigint >= 123456789 and t1.t_double >= 1234.567890000000034 and t1.t_binary = 'test message' and t1.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and length(t1.v_binary) > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (sin(t2.v_bigint) >= 0 or sin(t2.v_bigint) < 0)order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (cos(t2.v_double) >= 0 or cos(t2.v_double) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (tan(t2.v_int) >= 0 or tan(t2.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and length(t2.t_binary) > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (sin(t2.t_bigint) >= 0 or sin(t2.t_bigint) < 0)order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (cos(t2.t_double) >= 0 or cos(t2.t_double) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and (tan(t2.t_int) >= 0 or tan(t2.t_int) < 0) order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 8,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,0), (7,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "las_c3_1",
                    "desc": "left asof join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= timetruncate(t2.ts,1a) where t1.ts < '2024-01-01 12:00:02.600' or t2.ts is null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= timetruncate(t1.ts, 1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t2.ts <= timetruncate(t1.ts, 1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= t1.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= timetruncate(t1.ts, 1a) where t1.ts < '2024-01-01 12:00:02.600' or t2.ts is null order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "las_c4_1",
                    "desc": "left asof join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 on t1.ts > t2.ts order by ts1 desc) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= t2.ts order by ts1) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 on t2.ts < t1.ts order by ts2) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 on t2.ts <= t1.ts order by ts2 desc) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.ts <= now and t2.ts <= now) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int > 0 and t2.v_bigint > 0) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.v_binary like '%abc%' and t2.v_bool in (true, false)) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.t_ts <= now and t2.t_ts <= now) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.t_int > 0 and t2.t_bigint > 0) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left asof join db1_st2 t2 where t1.t_binary like '%test message%' and t2.t_bool in (true, false)) order by ts1 desc limit 1;"
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1)], [('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "las_c4_2",
                    "desc": "left asof join for aggregate and window query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select _wstart ts, count(ts2) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 left asof join db1_st2 t2) partition by tbname interval(1s) order by ts;",
                            "select first(ts1) ts, count(v_int2) from (select t1.ts ts1, t2.v_int v_int2, t1.tbname from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= t2.ts jlimit 1) group by tbname order by ts;",
                            "select _wstart ts, count(t_bool2) from (select t1.ts ts1, t2.t_bool t_bool2, t1.tbname from db1_st1 t1 left asof join db1_st2 t2 on t2.ts <= t1.ts) partition by tbname interval(1s) order by ts;",
                            "select first(ts1) ts, count(v_binary2) from (select t1.ts ts1, t2.v_binary v_binary2, t1.tbname from db1_st1 t1 left asof join db1_st2 t2 where t1.v_int = t2.v_int or t1.v_int != t2.v_int order by ts1 desc) group by tbname order by ts;",
                        ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (1,0), (1,1)], [('2024-01-01 12:00:00.000'), (5), ('2024-01-01 12:00:01.000'), (5)]]
                        }
                    }
                },
                {
                    "id": "las_c4_3",
                    "desc": "left asof join for union or union all operator",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 left asof join db1_st2_ct1 t2) union all (select t1.ts from db1_st1_ct2 t1 left asof join db1_st2_ct2 t2));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 left asof join db1_st2 t2) union (select t2.ts from db1_st1 t1 left asof join db1_st2 t2 where t2.ts != '2024-01-01 12:00:00.500'));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts <= t2.ts where t1.ts > '2024-01-01 12:00:00.600') union (select t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts > t2.ts where t2.ts is not null));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts >= t2.ts where t1.ts < '2024-01-01 12:00:00.700') union all (select t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.ts <= t2.ts where t2.ts is not null and t2.ts != '2024-01-01 12:00:01.600'));",
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(10)]]
                        }
                    }
                },
                {
                    "id": "las_c5_1",
                    "desc": "left asof join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 left asof join db1_st2 t2 on t1.ts = t2.ts and t1.v_int > 0;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 on t1.ts = t2.ts and t1.ts <= now;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 on t1.v_ts > t2.v_ts;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 jlimit -1;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 jlimit 1025;",
                            "select * from db1_st1_ct1 t1 left asof join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left asof join db1_st2 t2 on t1.t_int > t2.t_int jlimit 2 order by t1.ts, t2.ts;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 left asof join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 left asof join (select * from db1_st2) t2;",
                        ]
                }
            ],
            "right-asof": [
                {
                    "id": "ras_c1_1",
                    "desc": "right asof join for super table with master connection condition by >=、<=",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts >= t1.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts >= t1.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts <= t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts <= t2.ts jlimit 1 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ras_c1_2",
                    "desc": "right asof join for super table with master connection condition >、<",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts > t1.ts order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts > t1.ts jlimit 1 order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts < t2.ts order by t1.ts, t2.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts < t2.ts jlimit 1 order by t1.ts, t2.ts;",
                    ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [(None), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ras_c1_3",
                    "desc": "right asof join for super table with master connection condition =",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts = t1.ts order by t2.ts,t1.ts;",
                        "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts = t1.ts jlimit 1 order by t2.ts,t1.ts;",
                    ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (3,0), (3,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:00.300'), (None), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ras_c1_4",
                    "desc": "right asof join for super table with master and other connection conditions",
                    "is_ci": True,
                    "exception": False,
                    "sql": [
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_int = t2.v_int order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_bigint = t2.v_bigint order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_ts = t2.v_ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_double = t2.v_double order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_binary = t2.v_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_bool = t2.v_bool order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_ts = t2.t_ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_int = t2.t_int order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_bigint = t2.t_bigint order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_double = t2.t_double order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_binary = t2.t_binary order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.t_bool = t2.t_bool order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st1 t2 on t1.v_ts = t2.v_ts and t1.v_int = t2.v_int and t1.v_bigint = t2.v_bigint and t1.v_double = t2.v_double and t1.v_bool = t2.v_bool and t1.v_binary = t2.v_binary and t1.t_ts = t2.t_ts and t1.t_int = t2.t_int and t1.t_bigint = t2.t_bigint and t1.t_double = t2.t_double and t1.t_bool = t2.t_bool and t1.t_binary = t2.t_binary order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "ras_c1_5",
                    "desc": "right asof join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts>=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts>t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts<=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts<t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts=t2.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts>=t2.ts jlimit 2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts>t2.ts jlimit 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts<=t2.ts jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.ts<t2.ts jlimit 1023 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.v_int_empty = t2.v_int_empty jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.v_bigint_empty = t2.v_bigint_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.v_double_empty = t2.v_double_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.v_binary_empty = t2.v_binary_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.v_bool_empty = t2.v_bool_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.t_int_empty = t2.t_int_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.t_bigint_empty = t2.t_bigint_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.t_double_empty = t2.t_double_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.t_binary_empty = t2.t_binary_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st1 t1 right asof join db1_st_empty t2 on t1.t_bool_empty = t2.t_bool_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 jlimit 0;"
                        ],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "ras_c1_6",
                    "desc": "right asof join for blank table with condition of blank table、column and tag",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts>=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts>t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts<=t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts<t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts=t2.ts jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts>=t2.ts jlimit 2 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts>t2.ts jlimit 100 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts<=t2.ts jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.ts<t2.ts jlimit 1023 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.v_int_empty = t2.v_int_empty jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.v_bigint_empty = t2.v_bigint_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.v_double_empty = t2.v_double_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.v_binary_empty = t2.v_binary_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.v_bool_empty = t2.v_bool_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.t_int_empty = t2.t_int_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.t_bigint_empty = t2.t_bigint_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.t_double_empty = t2.t_double_empty jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.t_binary_empty = t2.t_binary_empty jlimit 1  order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts, t1.v_ts, t2.v_ts from db1_st_empty t1 right asof join db1_st1 t2 on t1.t_bool_empty = t2.t_bool_empty jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [(None), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800')]]
                        }
                    }
                },
                {
                    "id": "ras_c2_1",
                    "desc": "right asof join for super table with filter condition",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_int > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_int > 0 and t2.ts >= t1.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_bigint > 123456792 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_bigint >= 123456792 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_binary like '%abcg%' order by t1.ts, t2.ts limit 8;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_bigint >= 123456792 and t2.v_binary like '%abcg%' order by t1.ts, t2.ts limit 9;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_binary like '%abcg%' and t1.ts between '2024-01-01 00:00:00.000' and now order by t1.ts, t2.ts limit 8;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_bool in (true, false) and t2.v_int > 0 and (t1.v_bool = true or t1.v_bool = false) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.v_double in (1234.567929999999933, 1234.567970000000059, 1234.567950000000110) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_double in ( 1234.567919999999958, 1234.567939999999908, 1234.567960000000085, 1234.567980000000034, 1234.567950000000110) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_int >= 1 and t1.ts >= '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.t_bigint >= 123456789 and t2.ts >= '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.t_double >= 1234.567890000000034 and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.t_binary = 'test message' and t1.t_bool in (true, false) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_bool in (true, false) and t2.t_int >= 0 and t2.t_bigint >= 123456789 and t2.t_double >= 1234.567890000000034 and t2.t_binary = 'test message' and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_int >= 1 and t2.ts >= '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_bigint >= 123456789 and t2.ts >= '2024-01-01 12:00:00.200' order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_double >= 1234.567890000000034 and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_binary = 'test message' and t1.t_bool in (true, false) and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.t_bool in (true, false) and t1.t_int >= 1 and t1.t_bigint >= 123456789 and t1.t_double >= 1234.567890000000034 and t1.t_binary = 'test message' and t1.v_int >= 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and length(t1.v_binary) > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (sin(t2.v_bigint) >= 0 or sin(t2.v_bigint) < 0)order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (cos(t2.v_double) >= 0 or cos(t2.v_double) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (tan(t2.v_int) >= 0 or tan(t2.v_int) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and length(t2.t_binary) > 0 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (sin(t2.t_bigint) >= 0 or sin(t2.t_bigint) < 0)order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (cos(t2.t_double) >= 0 or cos(t2.t_double) < 0) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int >= 0 and (tan(t2.t_int) >= 0 or tan(t2.t_int) < 0) order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 8,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,0), (7,1)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ras_c3_1",
                    "desc": "right asof join for super table with timetruncate function",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= t2.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts >= timetruncate(t2.ts,1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t1.ts, 1a) >= timetruncate(t2.ts,1a) where t1.ts < '2024-01-01 12:00:02.600' or t2.ts is not null order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= timetruncate(t1.ts, 1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts <= timetruncate(t1.ts, 1a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= t1.ts order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on timetruncate(t2.ts,1a) <= timetruncate(t1.ts, 1a) where t2.ts < '2024-01-01 12:00:02.600' or t1.ts is null order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [(None), ('2024-01-01 12:00:01.900'), ('2024-01-01 12:00:01.600'), ('2024-01-01 12:00:01.600')]]
                        }
                    }
                },
                {
                    "id": "ras_c4_1",
                    "desc": "right asof join for super table with nested query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 on t2.ts > t1.ts order by ts1 desc) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 on t2.ts >= t1.ts order by ts1) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 on t2.ts > t1.ts order by ts2) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 on t2.ts >= t1.ts order by ts2 desc) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.ts <= now and t2.ts <= now) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.v_int > 0 and t2.v_bigint > 0) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.v_binary like '%abc%' and t2.v_bool in (true, false)) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.t_ts <= now and t2.t_ts <= now) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.t_int > 0 and t2.t_bigint > 0) order by ts1 desc limit 1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right asof join db1_st2 t2 where t1.t_binary like '%test message%' and t2.t_bool in (true, false)) order by ts1 desc limit 1;"
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1)], [('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "ras_c4_2",
                    "desc": "right asof join for aggregate and window query",
                    "is_ci": True, 
                    "exception": False,
                    "sql": ["select _wstart ts, count(ts1) from (select t1.ts ts1, t2.ts ts2, t2.tbname from db1_st1 t1 right asof join db1_st2 t2) partition by tbname interval(1s) order by ts;",
                            "select _wstart ts, count(t_bool1) from (select t2.ts ts2, t1.t_bool t_bool1, t2.tbname from db1_st1 t1 right asof join db1_st2 t2 on t1.ts <= t2.ts) partition by tbname interval(1s) order by ts;",
                        ],
                    "res": {
                        "total_rows": 3,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (1,0), (1,1), (2,0), (2,1)], [('2024-01-01 12:00:00.000'), (5), ('2024-01-01 12:00:00.000'), (3), ('2024-01-01 12:00:01.000'), (2)]]
                        }
                    }
                },
                {
                    "id": "ras_c4_3",
                    "desc": "right asof join for union or union all operator",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select count(*) from (select ts from (select t1.ts from db1_st1_ct1 t1 right asof join db1_st2_ct1 t2) union all (select t1.ts from db1_st1_ct2 t1 right asof join db1_st2_ct2 t2));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 right asof join db1_st2 t2) union (select t2.ts from db1_st1 t1 right asof join db1_st2 t2 where t2.ts not in ('2024-01-01 12:00:00.500', '2024-01-01 12:00:00.700')));",
                            "select count(*) from (select ts from (select t1.ts from db1_st1 t1 right asof join db1_st2 t2 on t2.ts <= t1.ts where t1.ts > '2024-01-01 12:00:00.600') union (select t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.ts > t2.ts where t2.ts is not null));",
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(10)]]
                        }
                    }
                },
                {
                    "id": "ras_c5_1",
                    "desc": "right asof join exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 right asof join db1_st2 t2 on t1.ts = t2.ts and t1.v_int > 0;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 on t1.ts = t2.ts and t1.ts <= now;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 on t1.ts != t2.ts;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 on t1.v_ts > t2.v_ts;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 jlimit -1;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 jlimit 1025;",
                            "select * from db1_st1_ct1 t1 right asof join db1_st2_ct1 t2 on t1.ts != t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right asof join db1_st2 t2 on t1.t_int > t2.t_int jlimit 2 order by t1.ts, t2.ts;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int;",
                            "select * from db1_st1 t1 right asof join db1_st2 t2 on timediff(now, t1.ts) = timediff(now, t2.ts);",
                            "select * from db1_st1 t1 right asof join (select * from db1_st2) t2;",
                            "select _wstart ts, count(t_bool1) from (select t1.ts ts1, t1.t_bool t_bool1, t2.tbname from db1_st1 t1 right asof join db1_st2 t2 on t1.ts <= t2.ts) partition by tbname interval(1s) order by ts;"
                        ]
                }
            ],
            "left-window": [
                {
                    "id": "lw_c1_1",
                    "desc": "left window join for super table with window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 left window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts;",
                        ],
                    "res": {
                        "total_rows": 17,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (14,0), (14,1), (16,0), (16,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.400'), (None), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_2",
                    "desc": "left window join for super table with window by different time unit",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100w, 100w) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100d, 100d) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100h, 100h) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100m, 100m) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100s, 100s) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100000a, 100000a) jlimit 1024 order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 100,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (99,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_3",
                    "desc": "left window join for super table with int column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) jlimit 1024 order by t2.ts desc limit 1;"],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1)], [('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:00.800')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_4",
                    "desc": "left window join for super table with bigint, double column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_5",
                    "desc": "left window join for super table with binary column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), (None)]]
                        }
                    }
                },
                {
                    "id": "lw_c1_6",
                    "desc": "left window join for super table with bool column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 11,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,1), (5,0), (5,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.100'), (None), ('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:00.700')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_7",
                    "desc": "left window join for super table with timestamp column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (3,1), (4,0), (4,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:00.800')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_8",
                    "desc": "left window join for super table with timestamp, int, bigint, double, binary, bool tag connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_bigint = t2.t_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts limit 14;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_binary = t2.t_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts limit 14;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 14,
                        "value_check": {
                            "type": "contain",
                            "values": [[(2,0), (2,1), (3,1), (4,1)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:00.300')]]
                        }
                    }
                },
                {
                    "id": "lw_c1_9",
                    "desc": "left window join for empty master table",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_int = t2.v_int window_offset(-1000a, 1000a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_bigint = t2.t_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_binary = t2.t_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 left window join db1_st2 t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "lw_c1_10",
                    "desc": "left window join for empty table-drive",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 10 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_int = t2.v_int window_offset(-1000a, 1000a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 102 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 104 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_bigint = t2.t_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_binary = t2.t_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st_empty t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (0,1), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (None), (None)]]
                        }
                    }
                },
                {
                    "id": "lw_c1_11",
                    "desc": "left window join for window query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 having(count(t2.v_int) >= 0) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty and t1.v_binary_empty = t2.v_binary_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bigint_empty is null order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty and t1.v_binary_empty = t2.v_binary_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bigint_empty is null having(count(t2.v_int) >= 0) order by t1.ts;",
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (7,1), (0,1), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (0), (2), (1)]]
                        }
                    }
                },
                {
                    "id": "lw_c2_1",
                    "desc": "left window join for window query with scalar filter",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' order by t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 left window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 10 where length(t2.v_binary) > 0 and t1.v_binary like '%abc%' and t1.ts >= '2024-01-01 12:00:00.400' and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 left window join db2.db2_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bool in (true, false) and t2.ts is not null and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 left window join db2.db2_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.t_ts >= '2024-01-01 12:00:00.000' and t1.t_int + t2.t_int > 1 and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts;",
                        ],
                    "res": {
                        "total_rows": 8,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (7,0), (7,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "lw_c2_2",
                    "desc": "left window join for window query with aggregate filter",
                    "is_ci": True,
                    "exception": False, 
                    "sql": ["select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' having(sum(t1.v_int + t2.v_int) >= 0) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' having(avg(abs(t1.v_double + t2.v_double)) > 0) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where length(t2.v_binary) > 0 and t1.v_binary like '%abc%' and t1.ts >= '2024-01-01 12:00:00.400' and t2.ts >= '2024-01-01 12:00:00.400' having(avg(length(t2.v_binary) + length(t2.v_binary)) > 1) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bool in (true, false) and t2.ts is not null and t2.ts >= '2024-01-01 12:00:00.400' having(apercentile((t1.v_double + t2.v_double), 100) > 0) order by t1.ts;",
                            "select t1.ts, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.t_ts >= '2024-01-01 12:00:00.000' and t1.t_int + t2.t_int > 1 and t2.ts >= '2024-01-01 12:00:00.400' having(hyperloglog(t2.v_bigint) >= 0) order by t1.ts;",
                        ],
                    "res": {
                        "total_rows": 5,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0), (4,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.800'), (1)]]
                        }
                    }
                },
                {
                    "id": "lw_c3_1",
                    "desc": "left window join for window query with nested query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t1.ts desc) partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' order by t1.ts desc) partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts desc) where ts2 is not null partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_int v_int1, t2.v_int v_int2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_int1 > 0 and v_int2 > 4 partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint v_bigint1, t2.v_bigint v_bigint2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bigint1 + v_bigint2 > 0 and ts1 between '2024-01-01 12:00:00.400' and now and ts2 != '2024-01-01 12:00:00.300' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_double v_double1, t2.v_double v_double2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_double1 * v_double2 > 0 and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, length(t2.v_binary) len, t1.v_binary v_binary1, t1.tbname from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where len > 0 and v_binary1 like '%abc%' and ts1 >= '2024-01-01 12:00:00.400' and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bool v_bool1, t2.v_bool v_bool2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bool1 in (true, false) and ts2 is not null and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.t_ts t_ts1, t2.t_ts t_ts2, t1.t_int t_int1, t2.t_int t_int2, t1.tbname from db1_st1 t1 left window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 order by t1.ts) where t_ts1 >= '2024-01-01 12:00:00.000' and t_int1 + t_int2 > 1 and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                        ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (1,0), (1,1)], [('2024-01-01 12:00:01.000'), (6), ('2024-01-01 12:00:02.000'), (2)]]
                        }
                    }
                },
                {
                    "id": "lw_c3_2",
                    "desc": "left window join for union query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct1') union all (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct2') order by ts1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct1') union all (select t1.ts ts1, t2.ts ts2 from db1.db1_st1 t1 left window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct2') order by ts1;"
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) union (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) order by ts1;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 left window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) union (select t1.ts ts1, t2.ts ts2 from db1.db1_st1 t1 left window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 1) order by ts1;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (6,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "lw_c4_1",
                    "desc": "left window join for super table with exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 t1 left window join db1_st2 t2 on t1.v_int > 0 window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 left window join db1_st2 t2 on t1.ts <= now window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 left window join db1_st2 t2 on t1.ts != t2.ts window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 left window join db1_st2 t2 on t1.v_ts > t2.v_ts window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 left window join db1_st2 t2 window_offset(-1a, 1a) jlimit -1;",
                            "select * from db1_st1 t1 left window join db1_st2 t2 window_offset(-1a, 1a) jlimit 1025;",
                            "select * from db1_st1_ct1 t1 left window join db1_st2_ct1 t2 on t1.v_int = t2.v_int or t1.v_bigint = t2.v_bigint window_offset(-1a, 1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 left window join db1_st2 t2 on abs(t1.t_int) > t2.t_int window_offset(-1a, 1a) jlimit 2 order by t1.ts, t2.ts;",
                            "select * from db1_st1 t1 left window join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 left window join (select * from db1_st2) t2 window_offset(-1a, 1a);",
                            "select first(t1.ts), count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-1a, 1a) group by t1.tbname;",
                            "select _wstart, count(t2.*) from db1_st1 t1 left window join db1_st2 t2 window_offset(-1a, 1a) partition by t1.tbname;",
                            "select first(t1.ts), count(t2.*) from db1_st1 t1 left window join db1_st2 t2 on t1.v_int = t2.v_int window_offset(-1a, 1a) slimit 1;",
                            "select * from db1_st1 t1 left window join db1_st2 t2 window_offset(-1y, 1y);",
                            "select * from db1.db1_st1 t1 left window join db_us.db_us_t t2 window_offset(-100a, 100a);",
                            "select * from db1.db1_st1 t1 left window join db_ns.db_ns_t t2 window_offset(-100a, 100a);",
                        ]
                }
            ],
            "right-window": [
                {
                    "id": "rw_c1_1",
                    "desc": "right window join for super table with window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 14 order by t1.ts, t2.ts limit 14;"
                        ],
                    "res": {
                        "total_rows": 14,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (13,0), (13,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_2",
                    "desc": "right window join for super table with window by different time unit",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100w, 100w) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100d, 100d) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100h, 100h) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100m, 100m) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100s, 100s) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100000a, 100000a) jlimit 1024 order by t1.ts, t2.ts;",
                        ],
                    "res": {
                        "total_rows": 100,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (99,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_3",
                    "desc": "right window join for super table with int column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts desc limit 1;"],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1)], [('2024-01-01 12:00:00.800'), ('2024-01-01 12:00:00.800')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_4",
                    "desc": "right window join for super table with bigint, double column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,0), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_5",
                    "desc": "right window join for super table with binary column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (7,0), (9,1), (9,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:01.900'), (None)]]
                        }
                    }
                },
                {
                    "id": "rw_c1_6",
                    "desc": "right window join for super table with bool column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(1,0), (1,1), (7,0), (5,0), (5,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.100'), (None), ('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:00.500')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_7",
                    "desc": "right window join for super table with timestamp column connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;"],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (3,0), (4,0), (4,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), (None), ('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:00.400')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_8",
                    "desc": "right window join for super table with timestamp, int, bigint, double, binary, bool tag connection condition window",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_bigint = t2.t_bigint and t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts limit 12;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_binary = t2.t_binary and t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts limit 12;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t2.ts, t1.ts;",
                        ],
                    "res": {
                        "total_rows": 12,
                        "value_check": {
                            "type": "contain",
                            "values": [[(2,0), (2,1), (3,1), (4,1)], [('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:00.100'), ('2024-01-01 12:00:00.200'), ('2024-01-01 12:00:00.300')]]
                        }
                    }
                },
                {
                    "id": "rw_c1_9",
                    "desc": "right window join for empty master table",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_int = t2.v_int window_offset(-1000a, 1000a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_bigint = t2.t_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_binary = t2.t_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st2 t1 right window join db1_st_empty t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 0
                    }
                },
                {
                    "id": "rw_c1_10",
                    "desc": "right window join for empty table-drive",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_ts = t2.v_ts window_offset(-100a, 100a) jlimit 10 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_int = t2.v_int window_offset(-1000a, 1000a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_bigint = t2.v_bigint window_offset(-100a, 100a) jlimit 1 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_double = t2.v_double window_offset(-100a, 100a) jlimit 102 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_binary = t2.v_binary window_offset(-100a, 100a) jlimit 104 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.v_bool = t2.v_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_ts = t2.t_ts window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_int = t2.t_int window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_bigint = t2.t_bigint window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_double = t2.t_double window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_binary = t2.t_binary window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;",
                            "select t1.ts, t2.ts from db1_st_empty t1 right window join db1_st1 t2 on t1.t_bool = t2.t_bool window_offset(-100a, 100a) jlimit 1024 order by t1.ts, t2.ts;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,1), (9,1), (0,0), (9,0)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), (None), (None)]]
                        }
                    }
                },
                {
                    "id": "rw_c1_11",
                    "desc": "right window join for window query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 having(count(t1.v_int) >= 0) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty and t1.v_binary_empty = t2.v_binary_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bigint_empty is null order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty and t1.v_binary_empty = t2.v_binary_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bigint_empty is null having(count(t1.v_int) >= 0) order by t2.ts;",
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (9,0), (7,1), (0,1), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.900'), (1), (1), (1)]]
                        }
                    }
                },
                {
                    "id": "rw_c2_1",
                    "desc": "right window join for window query with scalar filter",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' order by t2.ts;",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' order by t2.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 right window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 10 where length(t2.v_binary) > 0 and t1.v_binary like '%abc%' and t1.ts >= '2024-01-01 12:00:00.400' and t2.ts >= '2024-01-01 12:00:00.400' order by t2.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 right window join db2.db2_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bool in (true, false) and t2.ts is not null and t2.ts >= '2024-01-01 12:00:00.400' order by t2.ts;",
                            "select t1.ts, t2.ts from db1.db1_st1 t1 right window join db2.db2_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.t_ts >= '2024-01-01 12:00:00.000' and t1.t_int + t2.t_int > 1 and t2.ts >= '2024-01-01 12:00:00.400' order by t2.ts;",
                        ],
                    "res": {
                        "total_rows": 8,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (7,0), (7,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "rw_c2_2",
                    "desc": "right window join for window query with aggregate filter",
                    "is_ci": True,
                    "exception": False, 
                    "sql": ["select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' having(sum(t1.v_int + t2.v_int) >= 0) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' having(avg(abs(t1.v_double + t2.v_double)) > 0) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where length(t2.v_binary) > 0 and t1.v_binary like '%abc%' and t1.ts >= '2024-01-01 12:00:00.400' and t2.ts >= '2024-01-01 12:00:00.400' having(avg(length(t2.v_binary) + length(t2.v_binary)) > 1) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.v_bool in (true, false) and t2.ts is not null and t2.ts >= '2024-01-01 12:00:00.400' having(apercentile((t1.v_double + t2.v_double), 100) > 0) order by t2.ts;",
                            "select t2.ts, count(t1.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 where t1.t_ts >= '2024-01-01 12:00:00.000' and t1.t_int + t2.t_int > 1 and t2.ts >= '2024-01-01 12:00:00.400' having(hyperloglog(t2.v_bigint) >= 0) order by t2.ts;",
                        ],
                    "res": {
                        "total_rows": 6,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (4,0), (4,1)], [('2024-01-01 12:00:00.400'), ('2024-01-01 12:00:01.600'), (1)]]
                        }
                    }
                },
                {
                    "id": "rw_c3_1",
                    "desc": "right window join for window query with nested query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_int > 0 and t2.v_int > 4 order by t1.ts desc) partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_bigint + t2.v_bigint > 0 and t1.ts between '2024-01-01 12:00:00.400' and now and t2.ts != '2024-01-01 12:00:00.300' order by t1.ts desc) partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 where t1.v_double * t2.v_double > 0 and t2.ts >= '2024-01-01 12:00:00.400' order by t1.ts desc) where ts2 is not null partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_int v_int1, t2.v_int v_int2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_int1 > 0 and v_int2 > 4 partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bigint v_bigint1, t2.v_bigint v_bigint2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bigint1 + v_bigint2 > 0 and ts1 between '2024-01-01 12:00:00.400' and now and ts2 != '2024-01-01 12:00:00.300' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_double v_double1, t2.v_double v_double2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_double1 * v_double2 > 0 and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, length(t2.v_binary) len, t1.v_binary v_binary1, t1.tbname from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 10 order by t1.ts) where len > 0 and v_binary1 like '%abc%' and ts1 >= '2024-01-01 12:00:00.400' and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.v_bool v_bool1, t2.v_bool v_bool2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 order by t1.ts) where v_bool1 in (true, false) and ts2 is not null and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                            "select _wend, count(*) from (select t1.ts ts1, t2.ts ts2, t1.t_ts t_ts1, t2.t_ts t_ts2, t1.t_int t_int1, t2.t_int t_int2, t1.tbname from db1_st1 t1 right window join db1_st2 t2 on t1.v_int_empty = t2.v_int_empty window_offset(-100a, 100a) jlimit 10 order by t1.ts) where t_ts1 >= '2024-01-01 12:00:00.000' and t_int1 + t_int2 > 1 and ts2 >= '2024-01-01 12:00:00.400' partition by tbname interval(1s) order by _wend;",
                        ],
                    "res": {
                        "total_rows": 2,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (1,0), (1,1)], [('2024-01-01 12:00:01.000'), (6), ('2024-01-01 12:00:02.000'), (2)]]
                        }
                    }
                },
                {
                    "id": "rw_c3_2",
                    "desc": "right window join for union query",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct1') union all (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct2') order by ts1, ts2;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct1') union all (select t1.ts ts1, t2.ts ts2 from db1.db1_st1 t1 right window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 1 where t1.tbname = 'db1_st1_ct2') order by ts1, ts2;"
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) union (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) order by ts1, ts2;",
                            "select ts1, ts2 from (select t1.ts ts1, t2.ts ts2 from db1_st1 t1 right window join db1_st2 t2 window_offset(-100a, 100a) jlimit 1) union (select t1.ts ts1, t2.ts ts2 from db1.db1_st1 t1 right window join db2.db2_st2 t2 window_offset(-100a, 100a) jlimit 1) order by ts1, ts2;"
                        ],
                    "res": {
                        "total_rows": 10,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0), (0,1), (9,0), (9,1)], [('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:00.000'), ('2024-01-01 12:00:01.800'), ('2024-01-01 12:00:01.900')]]
                        }
                    }
                },
                {
                    "id": "rw_c4_1",
                    "desc": "right window join for super table with exceptions",
                    "is_ci": True,
                    "exception": True,
                    "sql": ["select * from db1_st1 right window join db1_st2 t2 on t1.v_int > 0 window_offset(-1a, 1a);",
                            "select * from db1_st1 right window join db1_st2 t2 on t1.ts <= now window_offset(-1a, 1a);",
                            "select * from db1_st1 right window join db1_st2 t2 on t1.ts != t2.ts window_offset(-1a, 1a);",
                            "select * from db1_st1 right window join db1_st2 t2 on t1.v_ts > t2.v_ts window_offset(-1a, 1a);",
                            "select * from db1_st1 right window join db1_st2 t2 window_offset(-1a, 1a) jlimit -1;",
                            "select * from db1_st1 right window join db1_st2 t2 window_offset(-1a, 1a) jlimit 1025;",
                            "select * from db1_st1_ct1 t1 right window join db1_st2_ct1 t2 on t1.v_int = t2.v_int or t1.v_bigint = t2.v_bigint window_offset(-1a, 1a);",
                            "select t1.ts, t2.ts from db1_st1 t1 right window join db1_st2 t2 on abs(t1.t_int) > t2.t_int window_offset(-1a, 1a) jlimit 2 order by t1.ts, t2.ts;",
                            "select * from db1_st1 t1 right window join db1_st2 t2 on t1.ts=t2.ts or t2.v_int=t1.v_int window_offset(-1a, 1a);",
                            "select * from db1_st1 t1 right window join (select * from db1_st2) t2 window_offset(-1a, 1a);",
                            "select first(t1.ts), count(t2.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-1a, 1a) group by t1.tbname;",
                            "select _wstart, count(t2.*) from db1_st1 t1 right window join db1_st2 t2 window_offset(-1a, 1a) partition by t1.tbname;",
                            "select first(t1.ts), count(t2.*) from db1_st1 t1 right window join db1_st2 t2 on t1.v_int = t2.v_int window_offset(-1a, 1a) slimit 1;",
                            "select * from db1_st1 t1 right window join db1_st2 t2 window_offset(-1y, 1y);",
                            "select * from db1.db1_st1 t1 right window join db_us.db_us_t t2 window_offset(-100a, 100a);",
                            "select * from db1.db1_st1 t1 right window join db_ns.db_ns_t t2 window_offset(-100a, 100a);",
                        ]
                }
            ],
            "pk_int32": [
                {
                    "id": "pk32_c1",
                    "desc": "all join functions with int32 primary key",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32 t1 inner join db_pk_st2_int32 t2 on t1.ts = t2.ts where t1.v_int = 2147483647 and t2.v_int = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32 t1 left join db_pk_st2_int32 t2 on t1.ts = t2.ts where t1.v_int <= -2147483648 and t2.v_int = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32 t1 right join db_pk_st2_int32 t2 on t1.ts = t2.ts where t1.v_int >= 2147483647 and t2.v_int = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32 t1 full join db_pk_st2_int32 t2 on t1.ts = t2.ts where t1.v_int < 0 and t2.v_int = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32 t1 left semi join db_pk_st2_int32 t2 on t1.ts = t2.ts where t1.v_int = 0 and t2.v_int != 0);",
                            "select count(ts2) from (select t2.ts ts2 from db_pk_st1_int32 t1 right semi join db_pk_st2_int32 t2 on t1.ts = t2.ts where t2.v_int = 0 and t1.v_int != 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int32_ct1 t1 left anti join db_pk_st1_int32_ct2 t2 on t1.ts = t2.ts where t1.ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:00.500');",
                            "select count(ts2) from (select t2.ts ts2 from db1.db1_st1 t1 right anti join db_pk_st1_int32 t2 on t1.ts = t2.ts);",
                            "select count(ts1) from (select t1.ts ts1, t2.ts from db_pk_st1_int32 t1 left asof join db_pk_st2_int32 t2 on t1.ts >= t2.ts jlimit 2 where t1.v_int >=0 and t2.v_int > 0 and t1.ts >= '2024-01-01 12:00:01.000');",
                            "select count(ts2) from (select t1.ts, t2.ts ts2 from db_pk_st1_int32 t1 right asof join db_pk_st2_int32 t2 on t1.ts >= t2.ts and t1.v_int = t2.v_int jlimit 2 where t2.v_int = 0 and t2.ts <= '2024-01-01 12:00:00.200');",
                            "select count(ts1) from (select t1.ts ts1, t2.ts, t1.v_int, t2.v_int from db_pk_st1_int32 t1 left window join db_pk_st2_int32 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) jlimit 1 where t1.v_int < 0 and t1.ts >= '2024-01-01 12:00:01.000');",
                            "select count(ts2) from (select t2.ts ts2, t1.ts, t1.v_int, t2.v_int from db_pk_st1_int32 t1 right window join db_pk_st2_int32 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) where t2.v_int = 0 and t2.ts > '2024-01-01 12:00:00.000');",
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(4)]]
                        }
                    }
                }
            ],
            "pk_int64": [
                {
                    "id": "pk64_c1",
                    "desc": "all join functions with int64 primary key",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64 t1 inner join db_pk_st2_int64 t2 on t1.ts = t2.ts where t1.v_bigint = 9223372000000000000 and t2.v_bigint = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64 t1 left join db_pk_st2_int64 t2 on t1.ts = t2.ts where t1.v_bigint > 0 and t2.v_bigint = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64 t1 right join db_pk_st2_int64 t2 on t1.ts = t2.ts where t1.v_bigint = 9223372000000000000 and t2.v_bigint = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64 t1 full join db_pk_st2_int64 t2 on t1.ts = t2.ts where t1.v_bigint < 0 and t2.v_bigint = 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64 t1 left semi join db_pk_st2_int64 t2 on t1.ts = t2.ts where t1.v_bigint = 0 and t2.v_bigint != 0);",
                            "select count(ts2) from (select t2.ts ts2 from db_pk_st1_int64 t1 right semi join db_pk_st2_int64 t2 on t1.ts = t2.ts where t2.v_bigint = 0 and t1.v_bigint != 0);",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_int64_ct1 t1 left anti join db_pk_st1_int64_ct2 t2 on t1.ts = t2.ts where t1.ts between '2024-01-01 12:00:00.100' and '2024-01-01 12:00:00.500');",
                            "select count(ts2) from (select t2.ts ts2 from db1.db1_st1 t1 right anti join db_pk_st1_int64 t2 on t1.ts = t2.ts);",
                            "select count(ts1) from (select t1.ts ts1, t2.ts from db_pk_st1_int64 t1 left asof join db_pk_st2_int64 t2 on t1.ts >= t2.ts jlimit 2 where t1.v_bigint >=0 and t2.v_bigint > 0 and t1.ts >= '2024-01-01 12:00:01.000');",
                            "select count(ts2) from (select t1.ts, t2.ts ts2 from db_pk_st1_int64 t1 right asof join db_pk_st2_int64 t2 on t1.ts >= t2.ts and t1.v_int = t2.v_int jlimit 2 where t2.v_bigint = 0 and t2.ts <= '2024-01-01 12:00:00.200');",
                            "select count(ts1) from (select t1.ts ts1, t2.ts, t1.v_int, t2.v_int from db_pk_st1_int64 t1 left window join db_pk_st2_int64 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) jlimit 1 where t1.v_bigint < 0 and t1.ts >= '2024-01-01 12:00:01.000');",
                            "select count(ts2) from (select t2.ts ts2, t1.ts, t1.v_int, t2.v_int from db_pk_st1_int64 t1 right window join db_pk_st2_int64 t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) where t2.v_bigint = 0 and t2.ts > '2024-01-01 12:00:00.200');",
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(4)]]
                        }
                    }
                },
            ],
            "pk_str": [
                {
                    "id": "pkstr_c1",
                    "desc": "all join functions with str primary key",
                    "is_ci": True,
                    "exception": False,
                    "sql": ["select count(ts1) from (select t1.ts ts1 from db_pk_st1_str t1 inner join db_pk_st2_str t2 on t1.ts = t2.ts where t1.v_binary like '%abcd%' and t1.ts >= '2024-01-01 12:00:01.000' and t2.v_binary != 'abcde');",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_str t1 left join db_pk_st2_str t2 on t1.ts = t2.ts where t1.v_binary like '%abcd%e' and t2.v_binary not in ('abc') and t1.ts < '2024-01-01 12:00:01.500');",
                            "select count(ts2) from (select t2.ts ts2 from db_pk_st1_str t1 right join db_pk_st2_str t2 on t1.ts = t2.ts where t2.v_binary match '[e]' and t2.ts between '2024-01-01 12:00:00.200' and now and t1.ts < '2024-01-01 12:00:01.500');",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_str t1 full join db_pk_st2_str t2 on t1.ts = t2.ts where length(t2.v_binary) > 4 and length(t1.v_binary) > 3 and t2.ts < '2024-01-01 12:00:01.500');",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_str t1 left semi join db_pk_st2_str t2 on t1.ts = t2.ts where t1.v_binary > t2.v_binary and t2.ts < '2024-01-01 12:00:01.500');",
                            "select count(ts2) from (select t2.ts ts2 from db_pk_st1_str t1 right semi join db_pk_st2_str t2 on t1.ts = t2.ts where t2.v_bigint >= 0 and t2.ts > '2024-01-01 12:00:00.000');",
                            "select count(ts1) from (select t1.ts ts1 from db_pk_st1_str_ct1 t1 left anti join db_pk_st1_str_ct2 t2 on t1.ts = t2.ts);",
                            "select count(ts2) from (select t2.ts ts2 from db_pk_st1_str_ct1 t1 right anti join db_pk_st1_str t2 on t1.ts = t2.ts);",
                            "select count(ts1) from (select t1.ts ts1, t2.ts from db_pk_st1_str t1 left asof join db_pk_st2_str t2 on t1.ts >= t2.ts jlimit 2 where t1.v_bigint >=0 and t2.v_bigint <= 0 and t2.ts <= '2024-01-01 12:00:01.300' and t1.ts != '2024-01-01 12:00:00.400');",
                            "select count(ts2) from (select t1.ts, t2.ts ts2, t2.v_bigint from db_pk_st1_str t1 right asof join db_pk_st2_str t2 on t1.ts >= t2.ts and t1.v_int = t2.v_int jlimit 2 where t2.v_bigint > 0 and t2.ts < '2024-01-01 12:00:01.100');",
                            "select count(ts1) from (select t1.ts ts1, t2.ts,t1.v_binary, t2.v_binary, t1.v_int, t2.v_int from db_pk_st1_str t1 left window join db_pk_st2_str t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) jlimit 1 where t1.v_int >= 0 and t1.ts >= '2024-01-01 12:00:00.100');",
                            "select count(ts2) from (select t2.ts ts2, t1.ts, t1.v_int, t2.v_int from db_pk_st1_str t1 right window join db_pk_st2_str t2 on t1.v_int = t2.v_int window_offset(-100a, 100a) where t2.v_binary match '[e]' and t1.ts < '2024-01-01 12:00:01.500');",
                        ],
                    "res": {
                        "total_rows": 1,
                        "value_check": {
                            "type": "contain",
                            "values": [[(0,0)], [(10)]]
                        }
                    }
                }
            ]
        }
        return sql[join_type]

    def result_validator(self, query_result, expected_result):
        """Query results validator
        :param query_result: the query result
        :param expected_result: the expected result defined in sql_generator
        """
        if expected_result:
            tdSql.checkEqual(len(query_result), expected_result["total_rows"])
            if "value_check" in expected_result.keys():
                if expected_result["value_check"]["type"] == "equal":
                    tdSql.checkEqual(tdSql.res, expected_result)
                elif expected_result["value_check"]["type"] == "contain":
                    for index in range(len(expected_result["value_check"]["values"][0])):
                        item = expected_result["value_check"]["values"][0][index]
                        tdLog.debug(item)
                        value = expected_result["value_check"]["values"][1][index]
                        if type(tdSql.res[item[0]][item[1]]) is datetime and value:
                            # millisecond
                            if len(value.split(".")[1]) == 3:
                                timestamp_res = tdSql.res[item[0]][item[1]].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            # microsecond
                            elif len(value.split(".")[1]) == 6:
                                timestamp_res = tdSql.res[item[0]][item[1]].strftime('%Y-%m-%d %H:%M:%S.%f')
                            # nanosecond
                            elif len(value.split(".")[1]) == 9:
                                timestamp_res = tdSql.res[item[0]][item[1]].strftime('%Y-%m-%d %H:%M:%S.%3N')
                            tdSql.checkEqual(timestamp_res, value)
                        else:
                            tdSql.checkEqual(tdSql.res[item[0]][item[1]], value)
                else:
                    tdLog.error("Unsupported value check type: %s" % expected_result["value_check"]["type"])         

    def test_join(self, join_type, pk=False):
        """Verify the join operation
        :param join_type: the join type, can be "inner", "left-outer", "right-outer", "full"
        """
        # common check points
        tdSql.query("show databases;")
        if 'db1' not in [item[0] for item in tdSql.res]:
            self.create_tables(["db1", "db2", "db_us", 'db_ns', 'db_pk'], ['ms', 'ms', 'us', 'ns', 'ms'])
            self.data(["db1", "db2"], "common_ms")
            # precision 'us'
            self.data(["db_us"], "common_us")
            # precision 'ns'
            self.data(["db_ns"], "common_ns")
            # primary key
            self.data(["db_pk"], "pk_int32_ms")
            self.data(["db_pk"], "pk_int64_ms")
            self.data(["db_pk"], "pk_str_ms")
        if pk:
            tdSql.execute("use db_pk;")
        else:
            tdSql.execute("use db1;")
        sql_list = self.sql_generator(join_type)
        sql_num = 0
        for sql in sql_list:
            tdLog.debug("Start the check point: %s" % sql["id"])
            tdLog.debug("Check point description: %s" % sql["desc"])
            if sql["is_ci"]:
                if sql["exception"]:
                    for q in sql["sql"]:
                        tdSql.error(q)
                        sql_num += 1
                else:
                    # make sure the query result is same with the expected result
                    for q in sql["sql"]:
                        tdSql.query(q)
                        tdLog.debug(tdSql.res)
                        self.result_validator(tdSql.res, sql["res"])
                        sql_num += 1
        tdLog.debug("Execute %d SQLs for %s join" % (sql_num, join_type))  
        self.total_sql_num += sql_num

    def run(self):
        # common check points for all join types with common data
        self.test_join("inner")
        self.test_join("left-outer")
        self.test_join("right-outer")
        self.test_join("full")
        self.test_join("left-semi")
        self.test_join("right-semi")
        self.test_join("left-anti")
        self.test_join("right-anti")
        self.test_join("left-asof")
        self.test_join("right-asof")
        self.test_join("left-window")
        self.test_join("right-window")

        # composite primary key
        self.test_join("pk_int32", pk=True)
        self.test_join("pk_int64", pk=True)
        self.test_join("pk_str", pk=True)
        
        tdLog.success(f"{self.total_sql_num} for join verification SQLs are executed successfully")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
