###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies,
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
"""Shared helpers for virtual-child ref virtual-child performance benchmarks."""

from new_test_framework.utils import tdSql
from perf_test_framework import insert_rows

DEFAULT_TAG_GID = 11
DEFAULT_REGION = "alpha"


def build_vchild_chain(db, depths, rows_per_child,
                       data_cols=5, vgroups=1,
                       tag_gid=DEFAULT_TAG_GID, region=DEFAULT_REGION):
    """Build a vchild→vchild chain in a single database.

    Returns dict mapping depth → table name (depth 0 = physical source).
    """
    depths = sorted(set(depths))
    max_depth = max(depths) if depths else 0

    tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
    tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")
    tdSql.execute(f"USE {db};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))
    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) "
                  "TAGS (gid INT, region NCHAR(16));")
    tdSql.execute(f"CREATE TABLE src_ctb USING src_stb TAGS ({tag_gid}, '{region}');")
    insert_rows("src_ctb", rows_per_child, n_cols=data_cols)

    tdSql.execute(f"CREATE STABLE vstb_chain (ts TIMESTAMP, {col_defs}) "
                  "TAGS (gid INT, region NCHAR(16)) VIRTUAL 1;")

    chain = {0: f"{db}.src_ctb"}
    prev_table = "src_ctb"
    prev_cols = [f"c{i}" for i in range(data_cols)]
    for depth in range(1, max_depth + 1):
        tbl = f"vctb_l{depth}"
        col_refs = ", ".join(f"c{i} FROM {prev_table}.{prev_cols[i]}" for i in range(data_cols))
        tdSql.execute(f"CREATE VTABLE {tbl} ({col_refs}) "
                      f"USING vstb_chain TAGS ({tag_gid}, '{region}');")
        prev_table = tbl
        prev_cols = [f"c{i}" for i in range(data_cols)]
        if depth in depths:
            chain[depth] = f"{db}.{tbl}"

    return chain


def build_vchild_chain_cross_db(dbs, depths, rows_per_child, data_cols=5, vgroups=1):
    """Build a cross-database vchild chain.

    Args:
        dbs: list of database names, one per layer. dbs[0] = source, dbs[1] = L1, etc.
        depths: which depths to include in the returned chain dict
        rows_per_child: rows to insert
        data_cols: number of data columns

    Returns:
        dict mapping depth → fully qualified table name (db.table)
    """
    depths = sorted(set(depths))
    max_depth = max(depths) if depths else 0
    assert len(dbs) >= max_depth + 1, f"Need {max_depth + 1} dbs, got {len(dbs)}"

    # Ensure enough dbs
    while len(dbs) <= max_depth:
        dbs.append(f"pf_vchild_xdb_{len(dbs)}")

    # Create all databases
    for db in set(dbs):
        tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))

    # Source
    tdSql.execute(f"USE {dbs[0]};")
    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) "
                  "TAGS (gid INT, region NCHAR(16));")
    tdSql.execute("CREATE TABLE src_ctb USING src_stb TAGS (11, 'alpha');")
    insert_rows("src_ctb", rows_per_child, n_cols=data_cols)

    chain = {0: f"{dbs[0]}.src_ctb"}

    prev_table = f"{dbs[0]}.src_ctb"
    prev_cols = [f"c{i}" for i in range(data_cols)]

    for depth in range(1, max_depth + 1):
        db = dbs[depth]
        tdSql.execute(f"USE {db};")
        tdSql.execute(f"CREATE STABLE vstb (ts TIMESTAMP, {col_defs}) "
                      "TAGS (gid INT, region NCHAR(16)) VIRTUAL 1;")
        tbl_short = f"vctb_l{depth}"
        col_refs = ", ".join(f"c{i} FROM {prev_table}.{prev_cols[i]}" for i in range(data_cols))
        tdSql.execute(f"CREATE VTABLE {tbl_short} ({col_refs}) "
                      "USING vstb TAGS (11, 'alpha');")
        full_name = f"{db}.{tbl_short}"
        prev_table = full_name
        prev_cols = [f"c{i}" for i in range(data_cols)]
        if depth in depths:
            chain[depth] = full_name

    return chain


def half_filter_value(rows_per_child):
    return rows_per_child // 2
