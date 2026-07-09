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
"""Shared helpers for vstable-ref-vstable performance benchmarks."""

from new_test_framework.utils import tdSql
from perf_test_framework import insert_rows

BASE_TS = 1700000000000


def _literal_tag_values(child_idx, tag_cols):
    return ", ".join(str(child_idx * 10 + i) for i in range(tag_cols))


def build_vstable_chain(db, depths, children, rows_per_child,
                        data_cols=5, tag_cols=3, vgroups=2,
                        use_tag_ref=False):
    """Build a vstable→vstable chain in a single database.

    Args:
        use_tag_ref: if True, virtual children use tag-ref instead of literal tags.

    Returns dict mapping depth → stable table name (depth 0 = src_stb).
    """
    depths = sorted(set(depths))
    max_depth = max(depths) if depths else 0

    tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
    tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")
    tdSql.execute(f"USE {db};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))
    tag_defs = ", ".join(f"t{i} INT" for i in range(tag_cols))

    # Tag source (if using tag-ref)
    if use_tag_ref:
        tdSql.execute(f"CREATE STABLE tag_src (ts TIMESTAMP, dummy INT) TAGS ({tag_defs});")
        for c in range(children):
            tag_vals = _literal_tag_values(c, tag_cols)
            tdSql.execute(f"CREATE TABLE tag_c{c} USING tag_src TAGS ({tag_vals});")
            tdSql.execute(f"INSERT INTO tag_c{c} VALUES ({BASE_TS}, 0);")

    # Source stable + children
    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, tag_cols)
        tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
        insert_rows(f"src_c{c}", rows_per_child, n_cols=data_cols)

    chain = {0: f"{db}.src_stb"}
    for depth in range(1, max_depth + 1):
        stb = f"vstb_l{depth}"
        tdSql.execute(f"CREATE STABLE {stb} (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(children):
            prev_child = f"src_c{c}" if depth == 1 else f"vc_l{depth - 1}_c{c}"
            vt = f"vc_l{depth}_c{c}"
            col_refs = ", ".join(f"c{i} FROM {prev_child}.c{i}" for i in range(data_cols))
            if use_tag_ref:
                tag_refs = ", ".join(f"t{i} FROM tag_c{c}.t{i}" for i in range(tag_cols))
                tdSql.execute(f"CREATE VTABLE {vt} ({col_refs}) "
                              f"USING {stb} TAGS ({tag_refs});")
            else:
                tag_vals = _literal_tag_values(c, tag_cols)
                tdSql.execute(f"CREATE VTABLE {vt} ({col_refs}) "
                              f"USING {stb} TAGS ({tag_vals});")
        if depth in depths:
            chain[depth] = f"{db}.{stb}"

    return chain


def build_vstable_chain_cross_db(dbs, depths, children, rows_per_child,
                                 data_cols=5, tag_cols=3, vgroups=2):
    """Build cross-database vstable chain.

    Args:
        dbs: list of database names. dbs[0]=source, dbs[1]=L1, etc.

    Returns dict mapping depth → fully qualified stable name (db.stb).
    """
    depths = sorted(set(depths))
    max_depth = max(depths) if depths else 0
    assert len(dbs) >= max_depth + 1, f"Need {max_depth + 1} dbs, got {len(dbs)}"
    while len(dbs) <= max_depth:
        dbs.append(f"pf_vstable_xdb_{len(dbs)}")

    for db in set(dbs):
        tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
        tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))
    tag_defs = ", ".join(f"t{i} INT" for i in range(tag_cols))

    # Source
    tdSql.execute(f"USE {dbs[0]};")
    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, tag_cols)
        tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
        insert_rows(f"src_c{c}", rows_per_child, n_cols=data_cols)

    chain = {0: f"{dbs[0]}.src_stb"}

    for depth in range(1, max_depth + 1):
        db = dbs[depth]
        tdSql.execute(f"USE {db};")
        stb_short = f"vstb_l{depth}"
        tdSql.execute(f"CREATE STABLE {stb_short} (ts TIMESTAMP, {col_defs}) "
                      f"TAGS ({tag_defs}) VIRTUAL 1;")
        for c in range(children):
            if depth == 1:
                prev_child = f"{dbs[0]}.src_c{c}"
            else:
                prev_child = f"{dbs[depth - 1]}.vc_l{depth - 1}_c{c}"
            vt = f"vc_l{depth}_c{c}"
            col_refs = ", ".join(f"c{i} FROM {prev_child}.c{i}" for i in range(data_cols))
            tag_vals = _literal_tag_values(c, tag_cols)
            tdSql.execute(f"CREATE VTABLE {vt} ({col_refs}) "
                          f"USING {stb_short} TAGS ({tag_vals});")
        if depth in depths:
            chain[depth] = f"{db}.{stb_short}"

    return chain
