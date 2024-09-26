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

    def test_stmt_timestamp_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, t timestamp)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        # primary key
        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    1626861392589111,
                    1626861392590111,
                    1695645296185376,
                    1704067201685436,
                    1682942496546787,
                    None,
                ]
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    1000000,
                    0,
                    214748364700000,
                    214591679900000,
                    214591680000000,
                ]
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err
        
        invalid_datas = [
            [[[1626861392589111], ["hello"]]],
            # [[[1626861392589111], [True]]],
            # [[[1626861392589111], [3.14]]],
            # [[[1626861392589111], [-1000000]]],
            # [[[1626861392589111], [100000000000000000000000]]],
            # [[[1626861392589111], [123456789]]],
            # [[[1626861392589111], [2147483648000001]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_timestamp_type_error] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_int_type(self):
        stablename = "stmt_td31428"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, i1 int) tags(i2 int)" % stablename)

        stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags(?) values(?, ?)")

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

        datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                    1626861392589117,
                    1626861392589118,
                    1626861392589119,
                    1626861392589120,
                    1626861392589121,
                ],
                [
                    # normal
                    12345,
                    -12345,
                    999999,
                    -999999,
                    2147483646,
                    # boundary
                    None,
                    -2147483648,
                    2147483647,
                    0,
                    1,
                    -1,
                ]
            ]
        ]

        for i in range(len(tags)):
            tbnames = [f'd{i}']
            try:
                stmt2.bind_param(tbnames, tags[i], datas)
                stmt2.execute()
                self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)
            except Exception as err:
                raise err

        invalid_tags = [
            [["hello"]],
            [[3.14]],
            [[-3.14]],
            # [[2147483648]],
            # [[2147483648000788]],
            # [[-2147483649]],
            # [[-214748364923]],
        ]

        for i in range(len(invalid_tags)):
            tbnames = [f'd{i+100}']
            try:
                stmt2.bind_param(tbnames, invalid_tags[i], datas)
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_int_type_error] No expected error occurred, invalid_tags[%s]: %s"
                    % (i, invalid_tags[i]))

        invalid_datas = [
            [[[1626861392589111], ["hello"]]],
            [[[1626861392589111], [3.14]]],
            [[[1626861392589111], [-3.14]]],
            # [[[1626861392589111], [2147483648]]],
            # [[[1626861392589111], [2147483648000788]]],
            # [[[1626861392589111], [-2147483649]]],
            # [[[1626861392589111], [-214748364923]]],
        ]

        for i in range(len(invalid_datas)):
            tbnames = [f'd{i+200}']
            try:
                stmt2.bind_param(tbnames, [[0]], invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_int_type_error] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_int_unsigned_type(self):
        stablename = "stmt_td31428"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, iu1 int unsigned) tags(iu2 int unsigned)" % stablename)

        stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags(?) values(?, ?)")

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

        invalid_tags = [
            [["hello"]],
            [[3.14]],
            [[-3.14]],
            # [[-1]],
            # [[4294967296]],
            # [[-4294967296]],
        ]

        for i in range(len(invalid_tags)):
            tbnames = [f'd{i+100}']
            try:
                stmt2.bind_param(tbnames, invalid_tags[i], datas)
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_int_type_error] No expected error occurred, invalid_tags[%s]: %s"
                    % (i, invalid_tags[i]))

        flattened_invalid_tags = list(itertools.chain.from_iterable(itertools.chain.from_iterable(invalid_tags)))
        invalid_datas = [[[[1626861392589111], [tag]]] for tag in flattened_invalid_tags]

        if len(tags) > 0:
            for i in range(len(invalid_datas)):
                tbnames = [f'd{i+200}']
                try:
                    stmt2.bind_param(tbnames, tags[0], invalid_datas[i])
                    stmt2.execute()
                except Exception as err:
                    pass
                else:
                    tdLog.exit("[test_stmt_int_type_error] No expected error occurred, invalid_datas[%s]: %s"
                        % (i, invalid_datas[i]))

    def test_stmt_bigint_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, b bigint)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    12345,
                    -12345,
                    999999,
                    -999999,
                    2147483646,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    1,
                    0,
                    -1,
                    9223372036854775807,
                    -9223372036854775808,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589111], ["hello"]]],
            # [[[1626861392589111], [True]]],
            [[[1626861392589111], [3.14]]],
            # [[[1626861392589111], [9223372036854775808]]],
            # [[[1626861392589111], [-9223372036854775809]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_bigint_type_error] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_bigint_unsigned_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, bu bigint unsigned)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    123456789012345,
                    999999999999999,
                    1000000000000000,
                    18446744073709551614,
                    18446744073709551600,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    0,
                    18446744073709551615,
                    1,
                    9223372036854775807,
                    18446744073709551600,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589111], ["hello"]]],
            # [[[1626861392589111], [True]]],
            [[[1626861392589111], [3.14]]],
            # [[[1626861392589111], [18446744073709551616]]],
            # [[[1626861392589111], [184467440737095516169090]]],
            # [[[1626861392589111], [-1]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_bigint_unsigned_type_error] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_float_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, f float)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                    1626861392589117,
                ],
                [
                    123.456,
                    -123.456,
                    1.23456789,
                    -1.23456789,
                    3.402823466e38 - 0.1,
                    100,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    1.175494351e-38,
                    3.402823466e38,
                    0.0,
                    -1.0,
                    3.402823466e38 - 1.0,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            # [[[1626861392589133], [3.402823466e39]]],
            # [[[1626861392589134], [-3.402823466e39]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_float_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_double_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, d double)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                    1626861392589117,
                ],
                [
                    123456789012.3456789,
                    -123456789012.3456789,
                    1.2345678901234567,
                    -1.2345678901234567,
                    1.7976931348623157e+308 - 0.1,
                    123456789,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    2.2250738585072014e-308,
                    1.7976931348623157e+308,
                    0.0,
                    -1.0,
                    1.7976931348623157e+308 - 1.0,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            # [[[1626861392589133], [1.7976931348623157e+309]]],
            # [[[1626861392589134], [-1.7976931348623157e+309]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_double_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_binary_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, b binary(20))" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    "hello world",
                    "1234567890abcdef",
                    " \x7F\x80\x81\xFE",
                    '!@#$%^&*()_+{}|:"<>?',
                    "abc1234567890xyz",
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    "",
                    "@@@@@!!!!!$$$$$#####",
                    "abcdefghijklmnopqrst",
                    "'\x00' * 20",
                    "'\xFF' * 20",
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], [True]]],
            [[[1626861392589132], [100]]],
            [[[1626861392589133], [3.14]]],
            [[[1626861392589134], ["1234567890abcdefghijkl"]]],
            [[[1626861392589135], ["\xe4\xb8\xad\xe6\x96\x87"]]],
            [[[1626861392589136], ["\x00\x01\x02\x03abc"]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_binary_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_smallint_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, s smallint)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    12345,
                    -12345,
                    30000,
                    -30000,
                    100,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    -32768,
                    32767,
                    -32767,
                    32766,
                    0,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            [[[1626861392589133], [3.14]]],
            # [[[1626861392589134], [32768]]],
            # [[[1626861392589135], [-32769]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_smallint_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_smallint_unsigned_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, su smallint unsigned)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    12345,
                    60000,
                    500,
                    65530,
                    65534,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                ],
                [
                    0,
                    1,
                    65534,
                    65535,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            [[[1626861392589133], [3.14]]],
            # [[[1626861392589134], [-1]]],
            # [[[1626861392589135], [65536]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_smallint_unsigned_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_tinyint_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, t tinyint)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    50,
                    -50,
                    100,
                    -100,
                    10,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    -128,
                    127,
                    -127,
                    126,
                    0,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            [[[1626861392589133], [3.14]]],
            # [[[1626861392589134], [-129]]],
            # [[[1626861392589135], [128]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_tinyint_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_tinyint_unsigned_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, tu tinyint unsigned)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    5,
                    50,
                    200,
                    250,
                    254,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                ],
                [
                    0,
                    1,
                    255,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], ["hello"]]],
            # [[[1626861392589132], [True]]],
            [[[1626861392589133], [3.14]]],
            # [[[1626861392589134], [-1]]],
            # [[[1626861392589135], [256]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_tinyint_unsigned_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_bool_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, b bool)" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                ],
                [
                    True,
                    False,
                    0,
                    1,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], [""]]],
            [[[1626861392589132], ["hello"]]],
            # [[[1626861392589133], [5]]],
            # [[[1626861392589134], [-5]]],
            [[[1626861392589135], [3.14]]],
            [[[1626861392589136], [-3.14]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_bool_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_nchar_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, n nchar(20))" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    "测试字符串",
                    "测试abc123",
                    "!@# 测试",
                    "a " * 10,
                    "testvalue"* 2,
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    "",
                    "a",
                    "a" * 20,
                    "a" * 19,
                    "中" * 20,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589132], [True]]],
            [[[1626861392589133], [3]]],
            [[[1626861392589133], [3.14]]],
            [[[1626861392589134], ["a" * 21]]],
            [[[1626861392589135], ["中" * 21]]],
            [[[1626861392589135], ['''a''']]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_nchar_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

    def test_stmt_json_type(self):
        stablename = "stmt_td31428"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, i int) tags(j json)" % stablename)

        stmt2 = self.connectstmt.statement2(f"insert into ? using {stablename} tags(?) values(?, ?)")

        tags = [
            # normal
            [['{"name": "test", "age": 30}']],
            [['{"is_valid": true, "count": 100, "score": 98.76}']],
            # boundary
            [['']],
            [['{}']],
            [['\t']],
            [[None]],
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
                self.stmt_common.checkResultCorrects(self.connectstmt, self.dbname, stablename, tbnames, tags, datas)
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
            invalid_tbnames = [f'd{i+100}']
            try:
                stmt2.bind_param(invalid_tbnames, invalid_tags[i], datas)
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_json_type] No expected error occurred, invalid_tags[%s]: %s"
                    % (i, invalid_tags[i]))

    def test_stmt_varchar_type(self):
        ctablename = "common_table"
        self.connectstmt.execute("drop database if exists %s" % self.dbname)
        self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
        self.connectstmt.select_db(self.dbname)
        self.connectstmt.execute("create table if not exists %s (ts timestamp, v varchar(20))" % ctablename)

        stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")

        tbnames = [ctablename]
        tags = None

        normal_datas = [
            [
                [
                    1626861392589111,
                    1626861392589112,
                    1626861392589113,
                    1626861392589114,
                    1626861392589115,
                    1626861392589116,
                ],
                [
                    "hello world",
                    "1234567890abcdef",
                    " \x7F\x80\x81\xFE",
                    '!@#$%^&*()_+{}|:"<>?',
                    "abc1234567890xyz",
                    None,
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, normal_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        boundary_datas = [
            [
                [
                    1626861392589121,
                    1626861392589122,
                    1626861392589123,
                    1626861392589124,
                    1626861392589125,
                ],
                [
                    "",
                    "@@@@@!!!!!$$$$$#####",
                    "abcdefghijklmnopqrst",
                    "'\x00' * 20",
                    "'\xFF' * 20",
                ],
            ]
        ]

        try:
            stmt2.bind_param(tbnames, tags, boundary_datas)
            stmt2.execute()
        except Exception as err:
            raise err

        invalid_datas = [
            [[[1626861392589131], [True]]],
            [[[1626861392589132], [100]]],
            [[[1626861392589133], [3.14]]],
            [[[1626861392589134], ["1234567890abcdefghijkl"]]],
            [[[1626861392589135], ["\xe4\xb8\xad\xe6\x96\x87"]]],
            [[[1626861392589136], ["\x00\x01\x02\x03abc"]]],
        ]

        for i in range(len(invalid_datas)):
            try:
                stmt2.bind_param(tbnames, tags, invalid_datas[i])
                stmt2.execute()
            except Exception as err:
                pass
            else:
                tdLog.exit("[test_stmt_varchar_type] No expected error occurred, invalid_datas[%s]: %s"
                    % (i, invalid_datas[i]))

#     def test_stmt_geometry_type(self):
#         ctablename = "common_table"
#         self.connectstmt.execute("drop database if exists %s" % self.dbname)
#         self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
#         self.connectstmt.select_db(self.dbname)
#         self.connectstmt.execute("create table if not exists %s (ts timestamp, g geometry)" % ctablename)
# 
#         stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")
# 
#         tbnames = [ctablename]
#         tags = None
# 
#         normal_datas = [
#             [
#                 [
#                     1626861392589111,
#                     1626861392589112,
#                     1626861392589113,
#                     1626861392589114,
#                     1626861392589115,
#                     1626861392589116,
#                 ],
#                 [
#                     POINT(123.456 789.012),
#                     LINESTRING(1.0 1.0, 2.0 2.0, 3.0 3.0),
#                     POLYGON((0 0, 4 0, 4 4, 0 4, 0 0)),
#                     POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2)),
#                     LINESTRING(0 0, 100 100, 200 200, 300 300),
#                     None,
#                 ],
#             ]
#         ]
# 
#         try:
#             stmt2.bind_param(tbnames, tags, normal_datas)
#             stmt2.execute()
#         except Exception as err:
#             raise err
# 
#         boundary_datas = [
#             [
#                 [
#                     1626861392589121,
#                     1626861392589122,
#                     1626861392589123,
#                     1626861392589124,
#                     1626861392589125,
#                 ],
#                 [
#                     POINT(1.0 1.0),
#                     POINT EMPTY,
#                     LINESTRING(1.0 1.0, 2.0 2.0),
#                     LINESTRING EMPTY,
#                     POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0)),
#                 ],
#             ]
#         ]
# 
#         try:
#             stmt2.bind_param(tbnames, tags, boundary_datas)
#             stmt2.execute()
#         except Exception as err:
#             raise err
# 
#         invalid_datas = [
#             [[[1626861392589131], ["hello"]]],
#             [[[1626861392589132], [True]]],
#             [[[1626861392589133], [100]]],
#             [[[1626861392589134], [3.14]]],
#             [[[1626861392589135], [POINT(1.0)]]],
#             [[[1626861392589136], [LINESTRING(1.0 1.0)]]],
#             [[[1626861392589137], [POLYGON((1.0 1.0, 2.0 2.0, 3.0 3.0))]]],
#             [[[1626861392589138], [POLYGON((0 0, 4 0, 4 4))]]],
#         ]
# 
#         for i in range(len(invalid_datas)):
#             try:
#                 stmt2.bind_param(tbnames, tags, invalid_datas[i])
#                 stmt2.execute()
#             except Exception as err:
#                 pass
#             else:
#                 tdLog.exit("[test_stmt_geometry_type] No expected error occurred, invalid_datas[%s]: %s"
#                     % (i, invalid_datas[i]))

#     def test_stmt_varbinary_type(self):
#         ctablename = "common_table"
#         self.connectstmt.execute("drop database if exists %s" % self.dbname)
#         self.connectstmt.execute("create database if not exists %s precision 'us'" % self.dbname)
#         self.connectstmt.select_db(self.dbname)
#         self.connectstmt.execute("create table if not exists %s (ts timestamp, v varbinary)" % ctablename)
# 
#         stmt2 = self.connectstmt.statement2(f"insert into {ctablename} values(?, ?)")
# 
#         tbnames = [ctablename]
#         tags = None
# 
#         normal_datas = [
#             [
#                 [
#                     1626861392589111,
#                     1626861392589112,
#                     1626861392589113,
#                     1626861392589114,
#                     1626861392589115,
#                     1626861392589116,
#                 ],
#                 [
#                     b'abc',
#                     b'\x01\x02\x03\x04',
#                     b'\xFA\xFE\xFD\xFC\xFB',
#                     b'\x00' * 20,
#                     b'\x0A\x0B\x0C',
#                     None,
#                 ],
#             ]
#         ]
# 
#         try:
#             stmt2.bind_param(tbnames, tags, normal_datas)
#             stmt2.execute()
#         except Exception as err:
#             raise err
# 
#         boundary_datas = [
#             [
#                 [
#                     1626861392589121,
#                     1626861392589122,
#                     1626861392589123,
#                     1626861392589124,
#                 ],
#                 [
#                     b'',
#                     b'\x00',
#                     b'\x01' * 20,
#                     b'\x01' * 19,
#                 ],
#             ]
#         ]
# 
#         try:
#             stmt2.bind_param(tbnames, tags, boundary_datas)
#             stmt2.execute()
#         except Exception as err:
#             raise err
# 
#         invalid_datas = [
#             [[[1626861392589131], ["hello"]]],
#             [[[1626861392589132], [True]]],
#             [[[1626861392589133], [3]]],
#             [[[1626861392589134], [3.14]]],
#             [[[1626861392589135], [b'\x01' * 21]]],
#             [[[1626861392589136], [b'\x01' + b'string']]],
#         ]
# 
#         for i in range(len(invalid_datas)):
#             try:
#                 stmt2.bind_param(tbnames, tags, invalid_datas[i])
#                 stmt2.execute()
#             except Exception as err:
#                 pass
#             else:
#                 tdLog.exit("[test_stmt_varbinary_type] No expected error occurred, invalid_datas[%s]: %s"
#                     % (i, invalid_datas[i]))

    def run(self):
        build_path = self.stmt_common.getBuildPath()
        config = build_path + "../sim/dnode1/cfg/"
        host = "localhost"
        self.connectstmt = self.newcon(host, config)

        # self.test_stmt_timestamp_type()
        # self.test_stmt_int_type()
        self.test_stmt_int_unsigned_type()
        # self.test_stmt_bigint_type()
        # self.test_stmt_bigint_unsigned_type()
        # self.test_stmt_float_type()
        # self.test_stmt_double_type()
        # self.test_stmt_binary_type()
        # self.test_stmt_smallint_type()
        # self.test_stmt_smallint_unsigned_type()
        # self.test_stmt_tinyint_type()
        # self.test_stmt_tinyint_unsigned_type()
        # self.test_stmt_bool_type()
        # self.test_stmt_nchar_type()
        # self.test_stmt_json_type()
        # self.test_stmt_varchar_type()
        # self.test_stmt_geometry_type()
        # self.test_stmt_varbinary_type()

        self.connectstmt.close()
        return


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
