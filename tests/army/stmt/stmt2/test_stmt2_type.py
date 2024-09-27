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

import taos
from taos import *
from stmt.common import StmtCommon
from ctypes import *
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
import itertools


class TDTestCase:
    def init(self, conn, log_sql, replica_var=1):
        self.replica_var = int(replica_var)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.dbname = "stmt_type_test_cases"
        self.stmt_common = StmtCommon()
        self.connectstmt = None

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

    def newcon(self, host, cfg):
        user = "root"
        password = "taosdata"
        port = 6030
        con = taos.connect(host=host, user=user, password=password, config=cfg, port=port)
        tdLog.debug(con)
        return con

    def test_stmt_data_type(self, test_case_name, data_type, tags, invalid_tags):
        stablename = "stmt_td31428"
        self.connectstmt.execute(f"drop database if exists {self.dbname}")
        self.connectstmt.execute(f"create database if not exists {self.dbname} precision 'us'")
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute(f"create table if not exists {stablename} (ts timestamp, a {data_type}) tags (b {data_type})")

        stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags(?) values(?, ?)")

        flattened_tags = list(itertools.chain.from_iterable(itertools.chain.from_iterable(tags)))
        tss = [1626861392589111 + i for i in range(len(flattened_tags))]
        datas = [[tss, flattened_tags]]

        for i in range(len(tags)):
            tbnames = [f'd{i}']
            try:
                stmt2.bind_param(tbnames, tags[i], datas)
                stmt2.execute()
                self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags[i], datas)
            except Exception as err:
                raise err

        for i in range(len(invalid_tags)):
            tbnames = [f'd{i+100}']
            try:
                stmt2.bind_param(tbnames, invalid_tags[i], datas)
                stmt2.execute()
            except Exception:
                pass
            else:
                tdLog.exit(f"[{test_case_name}] No expected error occurred, invalid_tags[{i}]: {invalid_tags[i]}")

        flattened_invalid_tags = list(itertools.chain.from_iterable(itertools.chain.from_iterable(invalid_tags)))
        invalid_datas = [[[[1626861392589111], [tag]]] for tag in flattened_invalid_tags]

        if len(tags) > 0:
            for i in range(len(invalid_datas)):
                tbnames = [f'd{i+200}']
                try:
                    stmt2.bind_param(tbnames, tags[0], invalid_datas[i])
                    stmt2.execute()
                except Exception:
                    pass
                else:
                    tdLog.exit(f"[{test_case_name}] No expected error occurred, invalid_datas[{i}]: {invalid_datas[i]}")

    def test_stmt_timestamp_type(self):
        tags = [
            # normal
            [[1626861392589111]],
            [[1626861392590111]],
            [[1695645296185376]],
            [[1704067201685436]],
            [[1682942496546787]],
            # boundary
            [[None]],
            [[0]],
            [[1000000]],
            [[214748364700000]],
            [[214591679900000]],
            [[214591680000000]],
        ]

        invalid_tags = [
            [['hello']],
            # [[3.14]],
            # [[-3.14]],
            # [[100000000000000000000000]],
            # [[-100000000000000000000000]],
        ]

        self.test_stmt_data_type('test_stmt_timestamp_type', 'timestamp', tags, invalid_tags)

    def test_stmt_int_type(self):
        tags = [
            # normal
            [[12345]],
            [[-12345]],
            [[999999]],
            [[-999999]],
            [[2147483646]],
            # boundary
            [[None]],
            [[-2147483648]],
            [[2147483647]],
            [[0]],
            [[1]],
            [[-1]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[2147483648]],
            # [[2147483648000788]],
            # [[-2147483649]],
            # [[-214748364923]],
        ]

        self.test_stmt_data_type('test_stmt_int_type', 'int', tags, invalid_tags)

    def test_stmt_int_unsigned_type(self):
        tags = [
            # normal
            [[123456]],
            [[999999]],
            [[1000000]],
            [[4294967290]],
            [[4294967294]],
            # boundary
            [[None]],
            [[0]],
            [[1]],
            [[2]],
            [[4294967295]],
            [[2147483647]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[-1]],
            # [[4294967296]],
            # [[-4294967296]],
        ]

        self.test_stmt_data_type('test_stmt_int_unsigned_type', 'int unsigned', tags, invalid_tags)

    def test_stmt_bigint_type(self):
        tags = [
            # normal
            [[12345]],
            [[-12345]],
            [[999999]],
            [[-999999]],
            [[2147483646]],
            # boundary
            [[None]],
            [[1]],
            [[0]],
            [[-1]],
            [[9223372036854775807]],
            [[-9223372036854775808]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[9223372036854775808]],
            # [[92233720368547758082]],
            # [[-9223372036854775809]],
            # [[-92233720368547758091]],
        ]

        self.test_stmt_data_type('test_stmt_bigint_type', 'bigint', tags, invalid_tags)

    def test_stmt_bigint_unsigned_type(self):
        tags = [
            # normal
            [[123456789012345]],
            [[999999999999999]],
            [[1000000000000000]],
            [[18446744073709551614]],
            [[18446744073709551600]],
            # boundary
            [[None]],
            [[0]],
            [[1]],
            [[9223372036854775807]],
            [[18446744073709551615]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[-1]],
            # [[18446744073709551616]],
            # [[184467440737095516169090]],
        ]

        self.test_stmt_data_type('test_stmt_bigint_unsigned_type', 'bigint unsigned', tags, invalid_tags)

    def test_stmt_float_type(self):
        tags = [
            # normal
            [[123.456]],
            [[-123.456]],
            [[1.23456789]],
            [[-1.23456789]],
            [[3.402823466e38 - 0.1]],
            [[100]],
            # boundary
            [[None]],
            [[1.175494351e-38]],
            [[3.402823466e38]],
            [[0.0]],
            [[-1.0]],
            [[3.402823466e38 - 1.0]],
        ]

        invalid_tags = [
            [['hello']],
            # [[3.402823466e39]],
            # [[3.402823466e39 + 1.0]],
            # [[-3.402823466e39]],
            # [[-3.402823466e39 - 1.0]],
        ]

        self.test_stmt_data_type('test_stmt_float_type', 'float', tags, invalid_tags)

    def test_stmt_double_type(self):
        tags = [
            # normal
            [[123456789012.3456789]],
            [[-123456789012.3456789]],
            [[1.2345678901234567]],
            [[-1.2345678901234567]],
            [[1.7976931348623157e+308 - 0.1]],
            [[123456789]],
            # boundary
            [[None]],
            [[2.2250738585072014e-308]],
            [[1.7976931348623157e+308]],
            [[0.0]],
            [[-1.0]],
            [[1.7976931348623157e+308 - 1.0]],
        ]

        invalid_tags = [
            [['hello']],
            # [[1.7976931348623157e+309]],
            # [[1.7976931348623157e+309 + 1.0]],
            # [[-1.7976931348623157e+309]],
            # [[-1.7976931348623157e+309 - 1.0]],
        ]

        self.test_stmt_data_type('test_stmt_double_type', 'double', tags, invalid_tags)

    def test_stmt_binary_type(self):
        tags = [
            # normal
            [['hello world']],
            [['1234567890abcdef']],
            [[' \x7F\x80\x81\xFE']],
            [['!@#$%^&*()_+{}|:"<>?']],
            [['abc1234567890xyz']],
            [['\x00\x01\x02\x03abc']],
            # boundary
            [[None]],
            [['']],
            [['@@@@@!!!!!$$$$$#####']],
            [['abcdefghijklmnopqrst']],
        ]

        invalid_tags = [
            [[100]],
            [[-100]],
            [[3.14]],
            [[-3.14]],
            # [['\x00' * 20]],
            [['\xFF' * 20]],
            [['1234567890abcdefghijkl']],
            [['\xe4\xb8\xad\xe6\x96\x87']],
        ]

        self.test_stmt_data_type('test_stmt_binary_type', 'binary(20)', tags, invalid_tags)

    def test_stmt_smallint_type(self):
        tags = [
            # normal
            [[12345]],
            [[-12345]],
            [[30000]],
            [[-30000]],
            [[100]],
            # boundary
            [[None]],
            [[-32768]],
            [[32767]],
            [[-32767]],
            [[32766]],
            [[0]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[32768]],
            # [[-32769]],
        ]

        self.test_stmt_data_type('test_stmt_smallint_type', 'smallint', tags, invalid_tags)

    def test_stmt_smallint_unsigned_type(self):
        tags = [
            # normal
            [[12345]],
            [[60000]],
            [[500]],
            [[65530]],
            [[65534]],
            # boundary
            [[None]],
            [[0]],
            [[1]],
            [[65534]],
            [[65535]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[-1]],
            # [[65536]],
        ]

        self.test_stmt_data_type('test_stmt_smallint_unsigned_type', 'smallint unsigned', tags, invalid_tags)

    def test_stmt_tinyint_type(self):
        tags = [
            # normal
            [[50]],
            [[-50]],
            [[100]],
            [[-100]],
            [[10]],
            # boundary
            [[None]],
            [[-128]],
            [[127]],
            [[-127]],
            [[126]],
            [[0]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[-129]],
            # [[128]],
        ]

        self.test_stmt_data_type('test_stmt_tinyint_type', 'tinyint', tags, invalid_tags)

    def test_stmt_tinyint_unsigned_type(self):
        tags = [
            # normal
            [[5]],
            [[50]],
            [[200]],
            [[250]],
            [[254]],
            # boundary
            [[None]],
            [[0]],
            [[1]],
            [[255]],
        ]

        invalid_tags = [
            [['hello']],
            [[3.14]],
            [[-3.14]],
            # [[-1]],
            # [[256]],
        ]

        self.test_stmt_data_type('test_stmt_tinyint_unsigned_type', 'tinyint unsigned', tags, invalid_tags)

    def test_stmt_bool_type(self):
        tags = [
            # normal
            [[True]],
            [[False]],
            [[0]],
            [[1]],
            [[-1]],
            [[5]],
            [[-5]],
            # [[3.14]],
            # [[-3.14]],
            # boundary
            [[None]],
        ]

        invalid_tags = [
            [['']],
            [['hello']],
        ]

        self.test_stmt_data_type('test_stmt_bool_type', 'bool', tags, invalid_tags)

    def test_stmt_nchar_type(self):
        tags = [
            # normal
            [['测试字符串']],
            [['测试abc123']],
            [['!@# 测试']],
            [['a ' * 10]],
            [['testvalue'* 2]],
            [['\'\'a\'\'']],
            # boundary
            [[None]],
            [['']],
            [['a']],
            [['a' * 20]],
            [['a' * 19]],
            [['中' * 20]],
        ]

        invalid_tags = [
            [[5]],
            [[-5]],
            [[3.14]],
            [[-3.14]],
            # [['a' * 21]],
            # [['中' * 21]],
        ]

        self.test_stmt_data_type('test_stmt_nchar_type', 'nchar(20)', tags, invalid_tags)

    def test_stmt_json_type(self):
        stablename = "stmt_td31428"
        self.connectstmt.execute(f"drop database if exists {self.dbname}")
        self.connectstmt.execute(f"create database if not exists {self.dbname} precision 'us'")
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute(f"create table if not exists {stablename} (ts timestamp, i int) tags(j json)")

        stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags(?) values(?, ?)")

        tags = [
            # normal
            [['{"name": "test", "age": 30}']],
            [['{"is_valid": true, "count": 100, "score": 98.76}']],
            # boundary
            [[None]],
            [['']],
            [['{}']],
            [['\t']],
            [['{"": ""}']],
            [['{"name": null}']],
            [['{"name": "test", "name": "abc"}']],
        ]

        datas = [[[1626861392589111], [0]]]

        for i in range(len(tags)):
            tbnames = [f'd{i}']
            try:
                stmt2.bind_param(tbnames, tags[i], datas)
                stmt2.execute()
                self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags[i], datas)
            except Exception as err:
                raise err

        invalid_tags = [
            [['hello']],
            [[True]],
            [[False]],
            [[5]],
            [[3.14]],
            [['{"name": "test"']],
            [['[1, 2, 3]']],
            [['[1, 2, 3']],
            [["{'key': 'value'}"]],
            [[r'{key: value}']],
            [['[{"name": "test1"}, {"name": "test2"}]']],
            [[r'{"user": {"id": 1, "info": {"name": "test", "age": 30}}']],
        ]

        for i in range(len(invalid_tags)):
            tbnames = [f'd{i+100}']
            try:
                stmt2.bind_param(tbnames, invalid_tags[i], datas)
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit(f"[test_stmt_json_type] No expected error occurred, invalid_tags[{i}]: {invalid_tags[i]}")

    def test_stmt_varchar_type(self):
        tags = [
            # normal
            [['hello world']],
            [['1234567890abcdef']],
            [[' \x7F\x80\x81\xFE']],
            [['!@#$%^&*()_+{}|:"<>?']],
            [['abc1234567890xyz']],
            [['\x00\x01\x02\x03abc']],
            # boundary
            [[None]],
            [['']],
            [['@@@@@!!!!!$$$$$#####']],
            [['abcdefghijklmnopqrst']],
        ]

        invalid_tags = [
            [[100]],
            [[-100]],
            [[3.14]],
            [[-3.14]],
            # [['\x00' * 20]],
            [['\xFF' * 20]],
            [['1234567890abcdefghijkl']],
            [['\xe4\xb8\xad\xe6\x96\x87']],
        ]

        self.test_stmt_data_type('test_stmt_varchar_type', 'varchar(20)', tags, invalid_tags)

    def test_stmt_geometry_type(self):
        tags = [
            # normal
            # [['POINT(1.0 1.0)']],
            # [['POINT(123.456 789.012)']],
            # [['LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0)']],
            # [['POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))']],
            # [['LINESTRING(0 0, 100 100, 200 200, 300 300)']],
            # boundary
            [[None]],
            # [['POINT(1.0 1.0)']],
            # [['POINT EMPTY']],
            # [['LINESTRING(1.0 1.0, 2.0 2.0)']],
            # [['LINESTRING EMPTY']],
            # [['POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))']],
        ]

        invalid_tags = [
            # [['hello']],
            # [[5]],
            # [[-5]],
            # [[3.14]],
            # [[-3.14]],
            # [['POINT(1.0)']],
            # [['LINESTRING(1.0 1.0)']],
            # [['POLYGON((1.0 1.0, 2.0 2.0, 3.0 3.0))']],
            # [['POLYGON((0 0, 4 0, 4 4))']],
            # [['POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))']],
        ]

        self.test_stmt_data_type('test_stmt_geometry_type', 'geometry(100)', tags, invalid_tags)

    def test_stmt_varbinary_type(self):
        tags = [
            # normal
            [[b'abc']],
            [[b'\x01\x02\x03\x04']],
            [[b'\xFA\xFE\xFD\xFC\xFB']],
            [[b'\x00' * 20]],
            [[b'\x0A\x0B\x0C']],
            [[b'\x01' + b'string']],
            # boundary
            [[None]],
            [[b'']],
            [[b'\x00']],
            [[b'\x01' * 20]],
            [[b'\x01' * 19]],
        ]

        invalid_tags = [
            [['hello']],
            # [[0]],
            # [[1]],
            # [[-1]],
            # [[1000]],
            # [[-1000]],
            [[3.14]],
            [[-3.14]],
            [[b'\x01' * 21]],
            [[b'\x01' * 30]],
        ]

        self.test_stmt_data_type('test_stmt_varbinary_type', 'varbinary(20)', tags, invalid_tags)

    def run(self):
        build_path = self.stmt_common.getBuildPath()
        config = build_path + "../sim/dnode1/cfg/"
        host = "localhost"
        self.connectstmt = self.newcon(host, config)

        self.test_stmt_timestamp_type()
        self.test_stmt_int_type()
        self.test_stmt_int_unsigned_type()
        self.test_stmt_bigint_type()
        self.test_stmt_bigint_unsigned_type()
        self.test_stmt_float_type()
        self.test_stmt_double_type()
        self.test_stmt_binary_type()
        self.test_stmt_smallint_type()
        self.test_stmt_smallint_unsigned_type()
        self.test_stmt_tinyint_type()
        self.test_stmt_tinyint_unsigned_type()
        self.test_stmt_bool_type()
        self.test_stmt_nchar_type()
        self.test_stmt_json_type()
        self.test_stmt_varchar_type()
        self.test_stmt_geometry_type()
        self.test_stmt_varbinary_type()

        self.connectstmt.close()
        return


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
