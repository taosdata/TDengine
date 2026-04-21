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
"""Shared helpers for tag-ref performance benchmarks."""

from new_test_framework.utils import tdSql
from perf_test_framework import insert_rows


def _literal_tag_values(child_idx, tag_cols):
    return ", ".join(str(child_idx * 10 + i) for i in range(tag_cols))


def build_tag_ref_env(db, children, rows_per_child,
                      data_cols=5, tag_cols=3, vgroups=2,
                      tag_db=None):
    """Build source tables + literal-tag vstable + tag-ref vstable.

    Args:
        db: database name for all tables
        children: number of child tables
        rows_per_child: rows per child
        data_cols: number of data columns (c0..c{data_cols-1})
        tag_cols: number of tag columns (t0..t{tag_cols-1})
        vgroups: vgroups for the database
        tag_db: if set, put tag source tables in a separate database (cross-db)

    Returns:
        dict with keys: src_stb, vstb_literal, vstb_tagref, db, tag_db
    """
    actual_tag_db = tag_db or db

    # Create databases
    tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
    tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")
    if tag_db and tag_db != db:
        tdSql.execute(f"DROP DATABASE IF EXISTS {tag_db};")
        tdSql.execute(f"CREATE DATABASE {tag_db} VGROUPS {vgroups};")

    tdSql.execute(f"USE {db};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))
    tag_defs = ", ".join(f"t{i} INT" for i in range(tag_cols))

    # Physical source stable + children
    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, tag_cols)
        tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
        insert_rows(f"src_c{c}", rows_per_child, n_cols=data_cols)

    # Tag source stable + children (for tag-ref)
    tag_src_prefix = f"{actual_tag_db}" if tag_db and tag_db != db else ""
    tdSql.execute(f"USE {actual_tag_db};")
    tdSql.execute(f"CREATE STABLE IF NOT EXISTS tag_src (ts TIMESTAMP, dummy INT) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, tag_cols)
        tdSql.execute(f"CREATE TABLE tag_c{c} USING tag_src TAGS ({tag_vals});")
        tdSql.execute(f"INSERT INTO tag_c{c} VALUES ({1700000000000}, 0);")

    # Literal-tag vstable
    tdSql.execute(f"USE {db};")
    tdSql.execute(f"CREATE STABLE vstb_literal (ts TIMESTAMP, {col_defs}) "
                  f"TAGS ({tag_defs}) VIRTUAL 1;")
    for c in range(children):
        col_refs = ", ".join(f"c{i} FROM src_c{c}.c{i}" for i in range(data_cols))
        tag_vals = _literal_tag_values(c, tag_cols)
        tdSql.execute(f"CREATE VTABLE vc_lit_c{c} ({col_refs}) "
                      f"USING vstb_literal TAGS ({tag_vals});")

    # Tag-ref vstable
    tag_src_prefix = f"{actual_tag_db}." if tag_db and tag_db != db else ""
    tdSql.execute(f"CREATE STABLE vstb_tagref (ts TIMESTAMP, {col_defs}) "
                  f"TAGS ({tag_defs}) VIRTUAL 1;")
    for c in range(children):
        col_refs = ", ".join(f"c{i} FROM src_c{c}.c{i}" for i in range(data_cols))
        tag_refs = ", ".join(f"t{i} FROM {tag_src_prefix}tag_c{c}.t{i}" for i in range(tag_cols))
        tdSql.execute(f"CREATE VTABLE vc_tref_c{c} ({col_refs}) "
                      f"USING vstb_tagref TAGS ({tag_refs});")

    return {
        "src_stb": "src_stb",
        "vstb_literal": "vstb_literal",
        "vstb_tagref": "vstb_tagref",
        "db": db,
        "tag_db": actual_tag_db,
    }


def build_mixed_tag_env(db, children, rows_per_child,
                        data_cols=5, total_tags=5, n_tag_refs=3, vgroups=2):
    """Build vstable with mixed literal + tag-ref tags.

    First n_tag_refs tags are tag-ref, rest are literal.
    """
    tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
    tdSql.execute(f"CREATE DATABASE {db} VGROUPS {vgroups};")
    tdSql.execute(f"USE {db};")

    col_defs = ", ".join(f"c{i} INT" for i in range(data_cols))
    tag_defs = ", ".join(f"t{i} INT" for i in range(total_tags))

    tdSql.execute(f"CREATE STABLE src_stb (ts TIMESTAMP, {col_defs}) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, total_tags)
        tdSql.execute(f"CREATE TABLE src_c{c} USING src_stb TAGS ({tag_vals});")
        insert_rows(f"src_c{c}", rows_per_child, n_cols=data_cols)

    tdSql.execute(f"CREATE STABLE tag_src (ts TIMESTAMP, dummy INT) TAGS ({tag_defs});")
    for c in range(children):
        tag_vals = _literal_tag_values(c, total_tags)
        tdSql.execute(f"CREATE TABLE tag_c{c} USING tag_src TAGS ({tag_vals});")
        tdSql.execute(f"INSERT INTO tag_c{c} VALUES ({1700000000000}, 0);")

    tdSql.execute(f"CREATE STABLE vstb_mixed (ts TIMESTAMP, {col_defs}) "
                  f"TAGS ({tag_defs}) VIRTUAL 1;")
    for c in range(children):
        col_refs = ", ".join(f"c{i} FROM src_c{c}.c{i}" for i in range(data_cols))
        # First n_tag_refs are tag-ref, rest are literal
        tag_parts = []
        for i in range(n_tag_refs):
            tag_parts.append(f"t{i} FROM tag_c{c}.t{i}")
        for i in range(n_tag_refs, total_tags):
            tag_parts.append(str(c * 10 + i))
        tag_str = ", ".join(tag_parts)
        tdSql.execute(f"CREATE VTABLE vc_mix_c{c} ({col_refs}) "
                      f"USING vstb_mixed TAGS ({tag_str});")

    return {
        "src_stb": "src_stb",
        "vstb_mixed": "vstb_mixed",
        "db": db,
        "n_tag_refs": n_tag_refs,
        "total_tags": total_tags,
    }
