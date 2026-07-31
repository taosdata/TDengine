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

from new_test_framework.utils import tdLog, tdSql


class TestCompressBugs:
    updatecfgDict = {
        "compressMsgSize": "1",
        "fqdn": "localhost",
        "clientCfg": {
            "fqdn": "127.0.0.1",
        },
    }

    def _xorshift_varbinary_literal(self, seed, size):
        state = seed & 0xFFFFFFFF
        payload = bytearray()
        for _ in range(size):
            state ^= (state << 13) & 0xFFFFFFFF
            state ^= state >> 17
            state ^= (state << 5) & 0xFFFFFFFF
            state &= 0xFFFFFFFF
            payload.append(state & 0xFF)
        return "\\x" + bytes(payload).hex().upper()

    def _exercise_lz4_boundary_block(self):
        # This payload makes the encoded two-column block length 2111 bytes and LZ4 writes 2114 bytes.
        size = 2049
        value = self._xorshift_varbinary_literal(1, size)
        tdSql.execute(f"create table s_overflow (ts timestamp, c1 varbinary({size}))")
        tdSql.execute(f"insert into s_overflow values(1700001002049, '{value}')")

        for _ in range(200):
            tdSql.query("select * from s_overflow")
            tdSql.checkRows(1)

    def test_compress_result_dispatcher_overflow(self):
        """Data dispatcher compressed-result buffer boundary

        1. Set compressMsgSize to 1 so large query result blocks enter result compression.
        2. Use different server/client FQDN strings so scheduler sets qMsg.compress=1.
        3. Insert a deterministic varbinary payload whose encoded block makes LZ4 output
           three bytes more than the raw block size.
        4. Repeatedly query the row so the vnode-query thread compresses the result block.

        Before the dataDispatcher buffer-size fix, ASAN builds can report heap-buffer-overflow in
        tsCompressStringImp()/LZ4_compress_default because the old code passed the total
        SDataCacheEntry allocation size as pEntry->data capacity.

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-06-25 Added regression for dataDispatcher compressed-result overflow
        """
        tdLog.debug(f"start to execute {__file__}")

        tdSql.execute("drop database if exists dd_compress_bugs")
        tdSql.execute("create database dd_compress_bugs vgroups 3")
        tdSql.execute("use dd_compress_bugs")
        self._exercise_lz4_boundary_block()

        tdSql.execute("drop database if exists dd_compress_bugs")
