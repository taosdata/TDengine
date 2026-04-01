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
"""Comprehensive tag-ref validation: 70+ tables, 100+ value checks.

Covers DDL syntax, error cases, metadata, and exhaustive query validation
with exact value assertions across same-DB, cross-DB, multi-DB, dup-ref,
mixed-literal, local-ref, and hybrid scenarios.
"""

from new_test_framework.utils import tdLog, tdSql, etool, tdCom

# ============================================================
# Source data definitions
# ============================================================

# ts_a (VGROUPS 2): 8 tables, TAGS(city NCHAR(20), pop INT)
SRC_A = {
    'a_bj': {'city': 'beijing',   'pop': 2154, 'data': [1, 2, 3]},
    'a_sh': {'city': 'shanghai',  'pop': 2487, 'data': [10, 20, 30, 40]},
    'a_gz': {'city': 'guangzhou', 'pop': 1530, 'data': [100]},
    'a_sz': {'city': 'shenzhen',  'pop': 1756, 'data': [200, 201]},
    'a_cd': {'city': 'chengdu',   'pop': 2094, 'data': [300, 301, 302, 303, 304]},
    'a_wh': {'city': 'wuhan',     'pop': 1232, 'data': [400]},
    'a_hz': {'city': 'hangzhou',  'pop': 1193, 'data': [500, 501, 502]},
    'a_nj': {'city': 'nanjing',   'pop': 931,  'data': [600, 601]},
}

# ts_b (VGROUPS 3): 6 tables, TAGS(name NCHAR(20), code INT)
SRC_B = {
    'b_alpha':   {'name': 'alpha',   'code': 101, 'data': [1, 2, 3]},
    'b_beta':    {'name': 'beta',    'code': 202, 'data': [11, 12]},
    'b_gamma':   {'name': 'gamma',   'code': 303, 'data': [21]},
    'b_delta':   {'name': 'delta',   'code': 404, 'data': [31, 32, 33, 34]},
    'b_epsilon': {'name': 'epsilon', 'code': 505, 'data': [41, 42]},
    'b_zeta':    {'name': 'zeta',    'code': 606, 'data': [51, 52, 53]},
}

# ts_c (VGROUPS 1): 4 tables, TAGS(label NCHAR(20), idx INT)
SRC_C = {
    'c_x': {'label': 'x-ray',   'idx': 10, 'data': [1000, 1001, 1002]},
    'c_y': {'label': 'yankee',  'idx': 20, 'data': [2000]},
    'c_z': {'label': 'zulu',    'idx': 30, 'data': [3000, 3001]},
    'c_w': {'label': 'whiskey', 'idx': 40, 'data': [4000, 4001, 4002, 4003]},
}

# td_aux local: TAGS(color NCHAR(16), pri INT)
SRC_AUX = {
    'aux_red':   {'color': 'red',   'pri': 1, 'data': [10, 11, 12]},
    'aux_green': {'color': 'green', 'pri': 2, 'data': [20, 21]},
    'aux_blue':  {'color': 'blue',  'pri': 3, 'data': [30]},
}

# ============================================================
# Virtual table mappings: (vtname, data_src, tag_src_1, tag_src_2, ...)
# ============================================================

# td_main.vst_scale: 20 children, tags from ts_a
# (name, data_src, city_src, pop_src) all keys into SRC_A
SCALE = [
    ('vs01', 'a_bj', 'a_bj', 'a_bj'), ('vs02', 'a_sh', 'a_sh', 'a_sh'),
    ('vs03', 'a_gz', 'a_gz', 'a_gz'), ('vs04', 'a_sz', 'a_sz', 'a_sz'),
    ('vs05', 'a_cd', 'a_cd', 'a_cd'), ('vs06', 'a_wh', 'a_wh', 'a_wh'),
    ('vs07', 'a_hz', 'a_hz', 'a_hz'), ('vs08', 'a_nj', 'a_nj', 'a_nj'),
    # cross-source tags:
    ('vs09', 'a_bj', 'a_sh', 'a_gz'), ('vs10', 'a_sh', 'a_cd', 'a_wh'),
    ('vs11', 'a_gz', 'a_nj', 'a_hz'), ('vs12', 'a_cd', 'a_bj', 'a_cd'),
    ('vs13', 'a_wh', 'a_gz', 'a_sh'), ('vs14', 'a_hz', 'a_sz', 'a_bj'),
    ('vs15', 'a_nj', 'a_wh', 'a_nj'), ('vs16', 'a_sz', 'a_hz', 'a_cd'),
    ('vs17', 'a_cd', 'a_wh', 'a_sz'), ('vs18', 'a_bj', 'a_nj', 'a_sh'),
    ('vs19', 'a_sh', 'a_bj', 'a_cd'), ('vs20', 'a_hz', 'a_cd', 'a_gz'),
]

# td_main.vst_3db: 10 children, tags from all 3 DBs
# (name, data_db, data_tbl, city_src(A), name_src(B), label_src(C))
TRIDB = [
    ('v3_01', 'ts_a', 'a_bj', 'a_bj', 'b_alpha', 'c_x'),
    ('v3_02', 'ts_a', 'a_sh', 'a_sh', 'b_beta', 'c_y'),
    ('v3_03', 'ts_a', 'a_gz', 'a_gz', 'b_gamma', 'c_z'),
    ('v3_04', 'ts_a', 'a_sz', 'a_sz', 'b_delta', 'c_w'),
    ('v3_05', 'ts_a', 'a_cd', 'a_cd', 'b_epsilon', 'c_x'),
    ('v3_06', 'ts_b', 'b_alpha', 'a_wh', 'b_zeta', 'c_y'),
    ('v3_07', 'ts_b', 'b_beta', 'a_hz', 'b_alpha', 'c_z'),
    ('v3_08', 'ts_c', 'c_x', 'a_nj', 'b_beta', 'c_w'),
    ('v3_09', 'ts_b', 'b_delta', 'a_bj', 'b_gamma', 'c_x'),
    ('v3_10', 'ts_c', 'c_w', 'a_cd', 'b_delta', 'c_z'),
]

# td_main.vst_dup: 5 children, overlapping tag refs
# (name, data_src(A), t1_db,t1_tbl,t1_tag, t2_db,t2_tbl,t2_tag, t3_db,t3_tbl,t3_tag)
DUP = [
    ('vd_01', 'a_bj', 'ts_a', 'a_bj', 'city', 'ts_a', 'a_bj', 'city', 'ts_a', 'a_bj', 'pop'),
    ('vd_02', 'a_sh', 'ts_a', 'a_sh', 'city', 'ts_a', 'a_gz', 'city', 'ts_a', 'a_gz', 'pop'),
    ('vd_03', 'a_cd', 'ts_b', 'b_alpha', 'name', 'ts_b', 'b_alpha', 'name', 'ts_b', 'b_alpha', 'code'),
    ('vd_04', 'a_wh', 'ts_a', 'a_wh', 'city', 'ts_c', 'c_x', 'label', 'ts_c', 'c_x', 'idx'),
    ('vd_05', 'a_gz', 'ts_a', 'a_gz', 'city', 'ts_b', 'b_zeta', 'name', 'ts_c', 'c_w', 'idx'),
]

# td_main.vst_mixed: 8 children, literal + cross-DB
# (name, data_src(A), lit_id, city_src(A), code_src(B))
MIXED = [
    ('vmx01', 'a_bj', 100, 'a_bj', 'b_alpha'), ('vmx02', 'a_sh', 200, 'a_sh', 'b_beta'),
    ('vmx03', 'a_gz', 300, 'a_gz', 'b_gamma'), ('vmx04', 'a_sz', 400, 'a_sz', 'b_delta'),
    ('vmx05', 'a_cd', 500, 'a_cd', 'b_epsilon'), ('vmx06', 'a_wh', 600, 'a_wh', 'b_zeta'),
    ('vmx07', 'a_hz', 700, 'a_hz', 'b_alpha'), ('vmx08', 'a_nj', 800, 'a_nj', 'b_beta'),
]

# td_aux.vst_local: 5 children, same-DB
# (name, data_src, color_src, pri_src)
LOCAL = [
    ('vl_01', 'aux_red', 'aux_red', 'aux_red'),
    ('vl_02', 'aux_green', 'aux_green', 'aux_green'),
    ('vl_03', 'aux_blue', 'aux_blue', 'aux_blue'),
    ('vl_04', 'aux_red', 'aux_green', 'aux_blue'),
    ('vl_05', 'aux_green', 'aux_blue', 'aux_red'),
]

# td_aux.vst_hybrid: 4 children, local + cross-DB
# (name, data_local?, data_tbl, color_src(AUX), city_db, city_tbl, city_tag)
HYBRID = [
    ('vh_01', True, 'aux_red', 'aux_red', 'ts_a', 'a_bj', 'city'),
    ('vh_02', False, 'a_sh', 'aux_green', 'ts_a', 'a_sh', 'city'),
    ('vh_03', False, 'b_gamma', 'aux_blue', 'ts_b', 'b_gamma', 'name'),
    ('vh_04', True, 'aux_blue', 'aux_red', 'ts_c', 'c_z', 'label'),
]

# ts_a.vst_same_a: 6 children, same-DB within ts_a
# (name, data_src, city_src, pop_src)
SAME_A = [
    ('va_01', 'a_bj', 'a_bj', 'a_bj'), ('va_02', 'a_sh', 'a_sh', 'a_sh'),
    ('va_03', 'a_gz', 'a_bj', 'a_sh'), ('va_04', 'a_sz', 'a_gz', 'a_cd'),
    ('va_05', 'a_cd', 'a_hz', 'a_nj'), ('va_06', 'a_hz', 'a_nj', 'a_wh'),
]

# td_main.vst_combo: 15 children, literal + multi-DB refs combined
# VSTB: (ts, val INT) TAGS (lit_score INT, ref_city NCHAR(20), ref_code INT, ref_label NCHAR(20))
# (name, data_db, data_tbl, lit_score, city_src(A), code_src(B), label_src(C))
COMBO = [
    ('vc01', 'ts_a', 'a_bj',  10, 'a_bj', 'b_alpha',   'c_x'),
    ('vc02', 'ts_a', 'a_sh',  10, 'a_sh', 'b_beta',    'c_y'),
    ('vc03', 'ts_a', 'a_gz',  20, 'a_gz', 'b_gamma',   'c_z'),
    ('vc04', 'ts_a', 'a_sz',  20, 'a_sz', 'b_delta',   'c_w'),
    ('vc05', 'ts_a', 'a_cd',  30, 'a_cd', 'b_epsilon',  'c_x'),
    ('vc06', 'ts_a', 'a_wh',  30, 'a_wh', 'b_zeta',    'c_y'),
    ('vc07', 'ts_a', 'a_hz',  40, 'a_hz', 'b_alpha',   'c_z'),
    ('vc08', 'ts_a', 'a_nj',  40, 'a_nj', 'b_beta',    'c_w'),
    ('vc09', 'ts_b', 'b_alpha', 50, 'a_bj', 'b_gamma',  'c_x'),
    ('vc10', 'ts_b', 'b_delta', 50, 'a_sh', 'b_delta',  'c_y'),
    ('vc11', 'ts_c', 'c_x',   60, 'a_gz', 'b_epsilon',  'c_z'),
    ('vc12', 'ts_c', 'c_w',   60, 'a_cd', 'b_zeta',    'c_w'),
    ('vc13', 'ts_a', 'a_bj',  10, 'a_cd', 'b_delta',   'c_z'),   # same data as vc01, different tags
    ('vc14', 'ts_b', 'b_beta', 70, 'a_sz', 'b_epsilon',  'c_x'),
    ('vc15', 'ts_c', 'c_z',   70, 'a_wh', 'b_zeta',    'c_y'),
]


def _combo_expected():
    """Pre-compute all expected rows for vst_combo STB queries."""
    rows = []
    for vt, ddb, dtbl, lit, csrc, bsrc, lsrc in COMBO:
        city = SRC_A[csrc]['city']
        code = SRC_B[bsrc]['code']
        label = SRC_C[lsrc]['label']
        for v in _data_rows(ddb, dtbl):
            rows.append((vt, lit, city, code, label, v))
    return rows


def _tag_val(db, tbl, tag):
    """Look up a tag value from any source DB."""
    if db == 'ts_a':
        return SRC_A[tbl][tag]
    if db == 'ts_b':
        return SRC_B[tbl][tag]
    if db == 'ts_c':
        return SRC_C[tbl][tag]
    return SRC_AUX[tbl][tag]


def _data_rows(db, tbl):
    """Get data values for a source table."""
    if db == 'ts_a':
        return SRC_A[tbl]['data']
    if db == 'ts_b':
        return SRC_B[tbl]['data']
    if db == 'ts_c':
        return SRC_C[tbl]['data']
    return SRC_AUX[tbl]['data']


# ============================================================
# Test class
# ============================================================

DB_DDL = "test_vtag_ref"
DB_DDL_CROSS = "test_vtag_ref_cross"


class TestVtableTagRef:
    """Comprehensive tests for virtual table tag column references.

    70+ tables across 7 databases, 100+ exact-value query assertions.
    """

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    def setup_class(cls):
        tdLog.info("=== setup: creating databases and tables ===")

        all_dbs = ["ts_a", "ts_b", "ts_c", "td_main", "td_aux", DB_DDL, DB_DDL_CROSS]
        for db in all_dbs:
            tdSql.execute(f"DROP DATABASE IF EXISTS {db};")
        tdSql.execute("CREATE DATABASE ts_a VGROUPS 2;")
        tdSql.execute("CREATE DATABASE ts_b VGROUPS 3;")
        tdSql.execute("CREATE DATABASE ts_c VGROUPS 1;")
        tdSql.execute("CREATE DATABASE td_main VGROUPS 4;")
        tdSql.execute("CREATE DATABASE td_aux VGROUPS 2;")
        tdSql.execute(f"CREATE DATABASE {DB_DDL};")
        tdSql.execute(f"CREATE DATABASE {DB_DDL_CROSS};")

        # --- ts_a source tables ---
        tdSql.execute("USE ts_a;")
        tdSql.execute("CREATE STABLE st_a (ts TIMESTAMP, v INT) TAGS (city NCHAR(20), pop INT);")
        for tbl, info in SRC_A.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING st_a TAGS ('{info['city']}', {info['pop']});")
        for tbl, info in SRC_A.items():
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({1700000000000 + j * 1000}, {v});")

        # --- ts_b source tables ---
        tdSql.execute("USE ts_b;")
        tdSql.execute("CREATE STABLE st_b (ts TIMESTAMP, v INT) TAGS (name NCHAR(20), code INT);")
        for tbl, info in SRC_B.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING st_b TAGS ('{info['name']}', {info['code']});")
        for tbl, info in SRC_B.items():
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({1700000000000 + j * 1000}, {v});")

        # --- ts_c source tables ---
        tdSql.execute("USE ts_c;")
        tdSql.execute("CREATE STABLE st_c (ts TIMESTAMP, v INT) TAGS (label NCHAR(20), idx INT);")
        for tbl, info in SRC_C.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING st_c TAGS ('{info['label']}', {info['idx']});")
        for tbl, info in SRC_C.items():
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({1700000000000 + j * 1000}, {v});")

        # --- td_aux local source tables ---
        tdSql.execute("USE td_aux;")
        tdSql.execute("CREATE STABLE aux_stb (ts TIMESTAMP, v INT) TAGS (color NCHAR(16), pri INT);")
        for tbl, info in SRC_AUX.items():
            tdSql.execute(f"CREATE TABLE {tbl} USING aux_stb TAGS ('{info['color']}', {info['pri']});")
        for tbl, info in SRC_AUX.items():
            for j, v in enumerate(info['data']):
                tdSql.execute(f"INSERT INTO {tbl} VALUES ({1700000000000 + j * 1000}, {v});")

        tdLog.info("  source tables created")

        # --- Virtual super tables ---
        tdSql.execute("USE td_main;")
        tdSql.execute("CREATE STABLE vst_scale (ts TIMESTAMP, val INT) TAGS (t_city NCHAR(20), t_pop INT) VIRTUAL 1;")
        tdSql.execute("CREATE STABLE vst_3db (ts TIMESTAMP, val INT) TAGS (t_city NCHAR(20), t_name NCHAR(20), t_label NCHAR(20)) VIRTUAL 1;")
        tdSql.execute("CREATE STABLE vst_dup (ts TIMESTAMP, val INT) TAGS (t1 NCHAR(20), t2 NCHAR(20), t3 INT) VIRTUAL 1;")
        tdSql.execute("CREATE STABLE vst_mixed (ts TIMESTAMP, val INT) TAGS (lit_id INT, t_city NCHAR(20), t_code INT) VIRTUAL 1;")
        tdSql.execute("CREATE STABLE vst_combo (ts TIMESTAMP, val INT) "
                      "TAGS (lit_score INT, ref_city NCHAR(20), ref_code INT, ref_label NCHAR(20)) VIRTUAL 1;")

        tdSql.execute("USE td_aux;")
        tdSql.execute("CREATE STABLE vst_local (ts TIMESTAMP, val INT) TAGS (t_color NCHAR(16), t_pri INT) VIRTUAL 1;")
        tdSql.execute("CREATE STABLE vst_hybrid (ts TIMESTAMP, val INT) TAGS (t_color NCHAR(16), t_city NCHAR(20)) VIRTUAL 1;")

        tdSql.execute("USE ts_a;")
        tdSql.execute("CREATE STABLE vst_same_a (ts TIMESTAMP, val INT) TAGS (t_city NCHAR(20), t_pop INT) VIRTUAL 1;")

        tdLog.info("  virtual super tables created")

        # --- vst_scale: 20 children ---
        tdSql.execute("USE td_main;")
        for vt, d, c, p in SCALE:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM ts_a.{d}.v) USING vst_scale "
                          f"TAGS (t_city FROM ts_a.{c}.city, t_pop FROM ts_a.{p}.pop);")

        # --- vst_3db: 10 children ---
        for vt, ddb, dtbl, csrc, nsrc, lsrc in TRIDB:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM {ddb}.{dtbl}.v) USING vst_3db "
                          f"TAGS (t_city FROM ts_a.{csrc}.city, t_name FROM ts_b.{nsrc}.name, "
                          f"t_label FROM ts_c.{lsrc}.label);")

        # --- vst_dup: 5 children ---
        for vt, dsrc, t1d, t1t, t1c, t2d, t2t, t2c, t3d, t3t, t3c in DUP:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM ts_a.{dsrc}.v) USING vst_dup "
                          f"TAGS (t1 FROM {t1d}.{t1t}.{t1c}, t2 FROM {t2d}.{t2t}.{t2c}, "
                          f"t3 FROM {t3d}.{t3t}.{t3c});")

        # --- vst_mixed: 8 children ---
        for vt, dsrc, lit, csrc, bsrc in MIXED:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM ts_a.{dsrc}.v) USING vst_mixed "
                          f"TAGS ({lit}, t_city FROM ts_a.{csrc}.city, t_code FROM ts_b.{bsrc}.code);")

        # --- vst_combo: 15 children (literal + 3-DB refs) ---
        for vt, ddb, dtbl, lit, csrc, bsrc, lsrc in COMBO:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM {ddb}.{dtbl}.v) USING vst_combo "
                          f"TAGS ({lit}, ref_city FROM ts_a.{csrc}.city, "
                          f"ref_code FROM ts_b.{bsrc}.code, ref_label FROM ts_c.{lsrc}.label);")

        # --- vst_local: 5 children ---
        tdSql.execute("USE td_aux;")
        for vt, d, c, p in LOCAL:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM {d}.v) USING vst_local "
                          f"TAGS (t_color FROM {c}.color, t_pri FROM {p}.pri);")

        # --- vst_hybrid: 4 children ---
        for vt, local, dtbl, csrc, cdb, ctbl, ctag in HYBRID:
            data_ref = f"{dtbl}.v" if local else f"{'ts_a' if dtbl.startswith('a_') else 'ts_b'}.{dtbl}.v"
            city_ref = f"{cdb}.{ctbl}.{ctag}"
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM {data_ref}) USING vst_hybrid "
                          f"TAGS (t_color FROM {csrc}.color, t_city FROM {city_ref});")

        # --- vst_same_a: 6 children ---
        tdSql.execute("USE ts_a;")
        for vt, d, c, p in SAME_A:
            tdSql.execute(f"CREATE VTABLE {vt} (val FROM {d}.v) USING vst_same_a "
                          f"TAGS (t_city FROM {c}.city, t_pop FROM {p}.pop);")

        tdLog.info("  virtual child tables created (73 total)")

        # --- DDL test database (org_stb with rich types) ---
        tdSql.execute(f"USE {DB_DDL};")
        tdSql.execute("CREATE STABLE org_stb ("
                      "ts TIMESTAMP, int_col INT, bigint_col BIGINT, float_col FLOAT, "
                      "double_col DOUBLE, binary_32_col BINARY(32), nchar_32_col NCHAR(32)"
                      ") TAGS ("
                      "int_tag INT, bool_tag BOOL, float_tag FLOAT, double_tag DOUBLE, "
                      "nchar_32_tag NCHAR(32), binary_32_tag BINARY(32))")
        for i in range(5):
            bval = 'true' if i % 2 == 0 else 'false'
            tdSql.execute(f"CREATE TABLE org_child_{i} USING org_stb "
                          f"TAGS ({i}, {bval}, {i}.{i}, {i}.{i}{i}, "
                          f"'nchar_child{i}', 'bin_child{i}');")
        for i in range(3):
            tdSql.execute(f"CREATE TABLE org_normal_{i} ("
                          "ts TIMESTAMP, int_col INT, bigint_col BIGINT, float_col FLOAT, "
                          "double_col DOUBLE, binary_32_col BINARY(32), nchar_32_col NCHAR(32))")
        tdSql.execute("CREATE STABLE vstb ("
                      "ts TIMESTAMP, int_col INT, bigint_col BIGINT, float_col FLOAT, "
                      "double_col DOUBLE, binary_32_col BINARY(32), nchar_32_col NCHAR(32)"
                      ") TAGS ("
                      "int_tag INT, bool_tag BOOL, float_tag FLOAT, double_tag DOUBLE, "
                      "nchar_32_tag NCHAR(32), binary_32_tag BINARY(32)) VIRTUAL 1")
        for i in range(5):
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO org_child_{i} VALUES ({ts}, {j}, {j*100}, "
                              f"{j}.{j}, {j}.{j}{j}, 'bin_{i}_{j}', 'nchar_{i}_{j}');")
        for i in range(3):
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO org_normal_{i} VALUES ({ts}, {j}, {j*100}, "
                              f"{j}.{j}, {j}.{j}{j}, 'bin_{i}_{j}', 'nchar_{i}_{j}');")

        # --- Cross-DB DDL source ---
        tdSql.execute(f"USE {DB_DDL_CROSS};")
        tdSql.execute("CREATE STABLE cross_stb ("
                      "ts TIMESTAMP, int_col INT, bigint_col BIGINT"
                      ") TAGS ("
                      "int_tag INT, bool_tag BOOL, float_tag FLOAT, double_tag DOUBLE, "
                      "nchar_32_tag NCHAR(32), binary_32_tag BINARY(32))")
        for i in range(3):
            bval = 'true' if i % 2 == 0 else 'false'
            tdSql.execute(f"CREATE TABLE cross_child_{i} USING cross_stb "
                          f"TAGS ({i+10}, {bval}, {i+10}.{i}, {i+10}.{i}{i}, "
                          f"'cross_nchar{i}', 'cross_bin{i}');")
            for j in range(10):
                ts = 1700000000000 + j * 1000
                tdSql.execute(f"INSERT INTO cross_child_{i} VALUES ({ts}, {j}, {j*100});")

        tdLog.info("=== setup complete ===")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _check_values(self, sql, expected, desc=""):
        """Assert query results match expected rows (order-independent)."""
        tdSql.query(sql)
        nrows = tdSql.queryRows
        ncols = len(expected[0]) if expected else 0
        assert nrows == len(expected), (
            f"{desc}: expected {len(expected)} rows, got {nrows}")

        actual = sorted([
            tuple(str(tdSql.getData(i, j)) for j in range(ncols))
            for i in range(nrows)
        ])
        exp = sorted([tuple(str(v) for v in r) for r in expected])

        assert actual == exp, (
            f"{desc}: mismatch\n"
            f"  Expected: {exp[:5]}{'...' if len(exp) > 5 else ''}\n"
            f"  Actual:   {actual[:5]}{'...' if len(actual) > 5 else ''}")
        tdLog.info(f"  PASS: {desc} ({nrows} rows)")

    def _check_count(self, sql, expected_count, desc=""):
        """Assert a COUNT(*) query returns expected value."""
        tdSql.query(sql)
        actual = tdSql.getData(0, 0)
        assert int(actual) == expected_count, (
            f"{desc}: expected count={expected_count}, got {actual}")
        tdLog.info(f"  PASS: {desc} (count={expected_count})")

    # ==================================================================
    # DDL Syntax Tests
    # ==================================================================

    def test_create_old_syntax(self):
        """Create: old tag ref syntax (FROM table.tag)

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        tdSql.execute("CREATE VTABLE vctb_old_0 ("
                      "int_col FROM org_child_0.int_col, "
                      "bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS ("
                      "FROM org_child_0.int_tag, FROM org_child_0.bool_tag, "
                      "FROM org_child_0.float_tag, FROM org_child_0.double_tag, "
                      "FROM org_child_0.nchar_32_tag, FROM org_child_0.binary_32_tag)")
        tdSql.query(f"SELECT COUNT(*) FROM {DB_DDL}.vctb_old_0;")
        tdSql.checkRows(1)
        tdLog.info("old syntax test passed")

    def test_create_specific_syntax(self):
        """Create: specific syntax (tag_name FROM table.tag)

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        # All tags from same source
        tdSql.execute("CREATE VTABLE vctb_spec_0 ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_0.bool_tag, "
                      "float_tag FROM org_child_0.float_tag, "
                      "double_tag FROM org_child_0.double_tag, "
                      "nchar_32_tag FROM org_child_0.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")
        # Tags from different sources
        tdSql.execute("CREATE VTABLE vctb_spec_1 ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, "
                      "bool_tag FROM org_child_1.bool_tag, "
                      "float_tag FROM org_child_2.float_tag, "
                      "double_tag FROM org_child_3.double_tag, "
                      "nchar_32_tag FROM org_child_4.nchar_32_tag, "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")
        # Mixed ref + literal
        tdSql.execute("CREATE VTABLE vctb_spec_2 ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, false, "
                      "float_tag FROM org_child_2.float_tag, 3.14, "
                      "'literal_nchar', binary_32_tag FROM org_child_0.binary_32_tag)")
        tdLog.info("specific syntax test passed")

    def test_create_positional_syntax(self):
        """Create: positional syntax (table.tag / db.table.tag)

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        # 2-part: table.tag
        tdSql.execute("CREATE VTABLE vctb_pos_0 ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "org_child_0.int_tag, org_child_0.bool_tag, "
                      "org_child_0.float_tag, org_child_0.double_tag, "
                      "org_child_0.nchar_32_tag, org_child_0.binary_32_tag)")
        # 3-part: db.table.tag
        tdSql.execute(f"CREATE VTABLE vctb_pos_1 ("
                      "int_col FROM org_child_0.int_col"
                      f") USING vstb TAGS ("
                      f"{DB_DDL}.org_child_0.int_tag, {DB_DDL}.org_child_0.bool_tag, "
                      f"{DB_DDL}.org_child_0.float_tag, {DB_DDL}.org_child_0.double_tag, "
                      f"{DB_DDL}.org_child_0.nchar_32_tag, {DB_DDL}.org_child_0.binary_32_tag)")
        tdLog.info("positional syntax test passed")

    def test_create_mixed_syntax(self):
        """Create: mixed syntax (literal + old FROM + specific + positional)

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        tdSql.execute("CREATE VTABLE vctb_mix_0 ("
                      "int_col FROM org_child_0.int_col"
                      ") USING vstb TAGS ("
                      "100, "
                      "bool_tag FROM org_child_1.bool_tag, "
                      "org_child_2.float_tag, "
                      "FROM org_child_3.double_tag, "
                      "'mixed_nchar', "
                      "binary_32_tag FROM org_child_0.binary_32_tag)")
        tdSql.query(f"SHOW CREATE VTABLE {DB_DDL}.vctb_mix_0;")
        tdSql.checkRows(1)
        create_sql = str(tdSql.getData(0, 1))
        assert 'vctb_mix_0' in create_sql
        assert 'mixed_nchar' in create_sql
        tdLog.info("mixed syntax test passed")

    def test_create_cross_db(self):
        """Create: cross-database tag references

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        # Specific syntax cross-DB
        tdSql.execute(f"CREATE VTABLE vctb_xdb_0 ("
                      f"int_col FROM {DB_DDL_CROSS}.cross_child_0.int_col"
                      f") USING vstb TAGS ("
                      f"int_tag FROM {DB_DDL_CROSS}.cross_child_0.int_tag, "
                      f"bool_tag FROM {DB_DDL_CROSS}.cross_child_0.bool_tag, "
                      f"float_tag FROM {DB_DDL_CROSS}.cross_child_0.float_tag, "
                      f"double_tag FROM {DB_DDL_CROSS}.cross_child_0.double_tag, "
                      f"nchar_32_tag FROM {DB_DDL_CROSS}.cross_child_0.nchar_32_tag, "
                      f"binary_32_tag FROM {DB_DDL_CROSS}.cross_child_0.binary_32_tag)")
        # Positional syntax cross-DB
        tdSql.execute(f"CREATE VTABLE vctb_xdb_1 ("
                      f"int_col FROM {DB_DDL_CROSS}.cross_child_1.int_col"
                      f") USING vstb TAGS ("
                      f"{DB_DDL_CROSS}.cross_child_1.int_tag, "
                      f"{DB_DDL_CROSS}.cross_child_1.bool_tag, "
                      f"{DB_DDL_CROSS}.cross_child_1.float_tag, "
                      f"{DB_DDL_CROSS}.cross_child_1.double_tag, "
                      f"{DB_DDL_CROSS}.cross_child_1.nchar_32_tag, "
                      f"{DB_DDL_CROSS}.cross_child_1.binary_32_tag)")
        # Mixed: cross-DB ref + literal
        tdSql.execute(f"CREATE VTABLE vctb_xdb_2 ("
                      f"int_col FROM {DB_DDL_CROSS}.cross_child_2.int_col"
                      f") USING vstb TAGS ("
                      f"int_tag FROM {DB_DDL_CROSS}.cross_child_2.int_tag, "
                      f"true, "
                      f"float_tag FROM {DB_DDL_CROSS}.cross_child_2.float_tag, "
                      f"9.99, 'cross_nchar_val', 'cross_bin_val')")
        tdLog.info("cross-DB create test passed")

    # ==================================================================
    # Metadata Tests
    # ==================================================================

    def test_metadata_show_describe(self):
        """Metadata: SHOW CREATE VTABLE, DESCRIBE, SHOW TAGS

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, metadata, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")

        # SHOW CREATE VTABLE
        tdSql.query(f"SHOW CREATE VTABLE {DB_DDL}.vctb_spec_0;")
        tdSql.checkRows(1)
        create_sql = str(tdSql.getData(0, 1))
        assert 'vctb_spec_0' in create_sql

        # DESCRIBE: ts(1) + 6 data cols + 6 tags = 13 rows
        tdSql.query(f"DESCRIBE {DB_DDL}.vctb_spec_0;")
        tdSql.checkRows(13)

        # SHOW TAGS: 6 tags
        tdSql.query(f"SHOW TAGS FROM {DB_DDL}.vctb_old_0;")
        tdSql.checkRows(6)

        tdLog.info("metadata test passed")

    # ==================================================================
    # Error Tests
    # ==================================================================

    def test_error_cases(self):
        """Error: invalid tag ref patterns

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref, negative
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")

        # Non-existent tag column
        tdSql.error("CREATE VTABLE vctb_err_0 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "int_tag FROM org_child_0.not_exist_tag, false, 1.0, 2.0, 'n', 'b')")
        # Non-existent source table
        tdSql.error("CREATE VTABLE vctb_err_1 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "int_tag FROM not_exist_table.int_tag, false, 1.0, 2.0, 'n', 'b')")
        # Data column used as tag ref
        err = tdSql.error("CREATE VTABLE vctb_err_2 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                          "int_tag FROM org_child_0.int_col, false, 1.0, 2.0, 'n', 'b')")
        assert "not a tag column" in err, f"Expected 'not a tag column', got: {err}"
        # Positional: non-existent column
        tdSql.error("CREATE VTABLE vctb_err_3 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "org_child_0.not_exist_tag, false, 1.0, 2.0, 'n', 'b')")
        # Positional: non-existent table
        tdSql.error("CREATE VTABLE vctb_err_4 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "not_exist_table.int_tag, false, 1.0, 2.0, 'n', 'b')")
        # Type mismatch: int_tag -> bool_tag position
        tdSql.error("CREATE VTABLE vctb_err_5 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "0, bool_tag FROM org_child_0.int_tag, 1.0, 2.0, 'n', 'b')")
        # Non-existent DB in 3-part name
        tdSql.error("CREATE VTABLE vctb_err_6 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "nonexist_db.org_child_0.int_tag, false, 1.0, 2.0, 'n', 'b')")
        # Tag ref from normal table (no tags)
        tdSql.error("CREATE VTABLE vctb_err_7 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                    "int_tag FROM org_normal_0.int_col, false, 1.0, 2.0, 'n', 'b')")
        tdLog.info("error cases test passed")

    def test_must_reference_tag_column(self):
        """Error: tag ref must point to actual tag, not data column

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref, negative
        Jira: None
        History: - 2026-2-12 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        err = tdSql.error("CREATE VTABLE vctb_nt_0 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                          "int_tag FROM org_child_0.int_col, false, 1.0, 2.0, 'n', 'b')")
        assert "not a tag column" in err
        err = tdSql.error("CREATE VTABLE vctb_nt_1 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                          "int_tag FROM org_child_0.ts, false, 1.0, 2.0, 'n', 'b')")
        assert "not a tag column" in err
        err = tdSql.error("CREATE VTABLE vctb_nt_2 (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                          "org_child_0.bigint_col, false, 1.0, 2.0, 'n', 'b')")
        assert "not a tag column" in err
        tdLog.info("must-reference-tag test passed")

    # ==================================================================
    # Lifecycle Tests
    # ==================================================================

    def test_drop_recreate(self):
        """Lifecycle: drop and recreate vtable with tag refs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, drop, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        tdSql.execute("CREATE VTABLE vctb_temp (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                      "int_tag FROM org_child_0.int_tag, bool_tag FROM org_child_0.bool_tag, "
                      "float_tag FROM org_child_0.float_tag, double_tag FROM org_child_0.double_tag, "
                      "nchar_32_tag FROM org_child_0.nchar_32_tag, binary_32_tag FROM org_child_0.binary_32_tag)")
        tdSql.query(f"SHOW CREATE VTABLE {DB_DDL}.vctb_temp;")
        tdSql.checkRows(1)
        tdSql.execute(f"DROP VTABLE {DB_DDL}.vctb_temp;")
        tdSql.error(f"SHOW CREATE VTABLE {DB_DDL}.vctb_temp;")
        # Recreate with different refs
        tdSql.execute("CREATE VTABLE vctb_temp (int_col FROM org_child_0.int_col) USING vstb TAGS ("
                      "org_child_4.int_tag, org_child_4.bool_tag, org_child_4.float_tag, "
                      "org_child_4.double_tag, org_child_4.nchar_32_tag, org_child_4.binary_32_tag)")
        tdSql.query(f"SHOW CREATE VTABLE {DB_DDL}.vctb_temp;")
        tdSql.checkRows(1)
        tdSql.execute(f"DROP VTABLE {DB_DDL}.vctb_temp;")
        tdLog.info("drop/recreate test passed")

    def test_all_literal_compat(self):
        """Compat: all literal tags (no refs) backward compatibility

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, create, tag_ref
        Jira: None
        History: - 2026-2-11 Created
        """
        tdSql.execute(f"USE {DB_DDL};")
        tdSql.execute("CREATE VTABLE vctb_lit_0 ("
                      "int_col FROM org_child_0.int_col, bigint_col FROM org_child_1.bigint_col"
                      ") USING vstb TAGS (42, true, 3.14, 2.718, 'hello_nchar', 'hello_bin')")
        tdSql.query(f"SHOW CREATE VTABLE {DB_DDL}.vctb_lit_0;")
        tdSql.checkRows(1)
        tdSql.query(f"SHOW TAGS FROM {DB_DDL}.vctb_lit_0;")
        tdSql.checkRows(6)
        tdLog.info("all-literal compat test passed")

    # ==================================================================
    # Query Validation: Scale (20 cross-DB child tables)
    # ==================================================================

    def test_query_scale_child_20(self):
        """Query: 20 scale child tables with cross-DB tag refs

        Each child refs data and tags from ts_a tables via cross-DB.
        8 same-source + 12 cross-source tag combinations.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, cross_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        for vt, d, c, p in SCALE:
            exp = [(SRC_A[c]['city'], SRC_A[p]['pop'], v) for v in SRC_A[d]['data']]
            self._check_values(
                f"SELECT t_city, t_pop, val FROM {vt};", exp,
                f"scale {vt}")

    def test_query_scale_stb(self):
        """Query: scale virtual super table aggregation and filtering

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")

        # COUNT(*)
        total = sum(len(SRC_A[d]['data']) for _, d, _, _ in SCALE)
        self._check_count("SELECT COUNT(*) FROM vst_scale;", total, "scale stb count")

        # Full scan
        all_rows = []
        for vt, d, c, p in SCALE:
            for v in SRC_A[d]['data']:
                all_rows.append((vt, SRC_A[c]['city'], SRC_A[p]['pop'], v))
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale;", all_rows,
                           "scale stb full scan")

        # tbname filter
        exp = [r for r in all_rows if r[0] == 'vs09']
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale WHERE tbname='vs09';",
                           exp, "scale tbname=vs09")

        exp = [r for r in all_rows if r[0] == 'vs20']
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale WHERE tbname='vs20';",
                           exp, "scale tbname=vs20")

        # tag filter: pop >= 2000
        exp = [r for r in all_rows if int(r[2]) >= 2000]
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale WHERE t_pop >= 2000;",
                           exp, "scale pop>=2000")

        # tag filter: city = 'beijing'
        exp = [r for r in all_rows if r[1] == 'beijing']
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale WHERE t_city='beijing';",
                           exp, "scale city=beijing")

        # tag filter: city = 'chengdu'
        exp = [r for r in all_rows if r[1] == 'chengdu']
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale WHERE t_city='chengdu';",
                           exp, "scale city=chengdu")

        # combined data+tag filter
        exp = [r for r in all_rows if int(r[2]) < 1500 and int(r[3]) >= 100]
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_scale "
                           "WHERE t_pop < 1500 AND val >= 100;",
                           exp, "scale pop<1500 AND val>=100")

        # GROUP BY
        exp_gb = []
        for vt, d, c, p in SCALE:
            rows = SRC_A[d]['data']
            exp_gb.append((vt, SRC_A[c]['city'], len(rows), sum(rows)))
        self._check_values("SELECT tbname, t_city, COUNT(*), SUM(val) FROM vst_scale "
                           "GROUP BY tbname, t_city;", exp_gb, "scale group by")

        # COUNT with tag filter
        cnt = sum(len(SRC_A[d]['data']) for _, d, c, _ in SCALE if SRC_A[c]['city'] == 'beijing')
        self._check_count("SELECT COUNT(*) FROM vst_scale WHERE t_city='beijing';",
                          cnt, "scale count city=beijing")

        # tag-only query
        tag_exp = [(vt, SRC_A[c]['city'], SRC_A[p]['pop'])
                   for vt, d, c, p in SCALE for _ in SRC_A[d]['data']]
        self._check_values("SELECT tbname, t_city, t_pop FROM vst_scale;",
                           tag_exp, "scale tag-only")

        # data-only query
        data_exp = [(v,) for _, d, _, _ in SCALE for v in SRC_A[d]['data']]
        self._check_values("SELECT val FROM vst_scale;", data_exp, "scale data-only")

        tdLog.info("scale STB queries passed")

    # ==================================================================
    # Query Validation: 3-DB (10 child tables, tags from 3 DBs)
    # ==================================================================

    def test_query_3db_child_10(self):
        """Query: 10 child tables with tags from 3 source DBs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, cross_db, multi_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        for vt, ddb, dtbl, csrc, nsrc, lsrc in TRIDB:
            rows = _data_rows(ddb, dtbl)
            city = SRC_A[csrc]['city']
            name = SRC_B[nsrc]['name']
            label = SRC_C[lsrc]['label']
            exp = [(city, name, label, v) for v in rows]
            self._check_values(f"SELECT t_city, t_name, t_label, val FROM {vt};",
                               exp, f"3db {vt}")

    def test_query_3db_stb(self):
        """Query: 3-DB virtual super table queries

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, multi_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")

        total = sum(len(_data_rows(ddb, dtbl)) for _, ddb, dtbl, _, _, _ in TRIDB)
        self._check_count("SELECT COUNT(*) FROM vst_3db;", total, "3db count")

        all_rows = []
        for vt, ddb, dtbl, csrc, nsrc, lsrc in TRIDB:
            city = SRC_A[csrc]['city']
            name = SRC_B[nsrc]['name']
            label = SRC_C[lsrc]['label']
            for v in _data_rows(ddb, dtbl):
                all_rows.append((vt, city, name, label, v))
        self._check_values("SELECT tbname, t_city, t_name, t_label, val FROM vst_3db;",
                           all_rows, "3db full scan")

        exp = [r for r in all_rows if r[2] == 'alpha']
        self._check_values("SELECT tbname, t_city, t_name, t_label, val FROM vst_3db "
                           "WHERE t_name='alpha';", exp, "3db name=alpha")

        exp = [r for r in all_rows if r[3] == 'x-ray']
        self._check_values("SELECT tbname, t_city, t_name, t_label, val FROM vst_3db "
                           "WHERE t_label='x-ray';", exp, "3db label=x-ray")

        exp = [r for r in all_rows if r[1] == 'beijing']
        self._check_values("SELECT tbname, t_city, t_name, t_label, val FROM vst_3db "
                           "WHERE t_city='beijing';", exp, "3db city=beijing")

        exp_gb = []
        for vt, ddb, dtbl, csrc, nsrc, lsrc in TRIDB:
            rows = _data_rows(ddb, dtbl)
            exp_gb.append((vt, SRC_A[csrc]['city'], len(rows), sum(rows)))
        self._check_values("SELECT tbname, t_city, COUNT(*), SUM(val) FROM vst_3db "
                           "GROUP BY tbname, t_city;", exp_gb, "3db group by")

        exp = [r for r in all_rows if r[1] == 'shanghai' and int(str(r[4])) >= 20]
        self._check_values("SELECT tbname, t_city, t_name, t_label, val FROM vst_3db "
                           "WHERE t_city='shanghai' AND val >= 20;", exp,
                           "3db city=shanghai AND val>=20")

        tdLog.info("3db STB queries passed")

    # ==================================================================
    # Query Validation: Dup tag refs (5 child tables)
    # ==================================================================

    def test_query_dup_child_5(self):
        """Query: 5 child tables with duplicate/overlapping tag refs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, dup_ref
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        for vt, dsrc, t1d, t1t, t1c, t2d, t2t, t2c, t3d, t3t, t3c in DUP:
            rows = SRC_A[dsrc]['data']
            v1 = _tag_val(t1d, t1t, t1c)
            v2 = _tag_val(t2d, t2t, t2c)
            v3 = _tag_val(t3d, t3t, t3c)
            exp = [(v1, v2, v3, v) for v in rows]
            self._check_values(f"SELECT t1, t2, t3, val FROM {vt};", exp,
                               f"dup {vt} (t1={v1},t2={v2},t3={v3})")

    def test_query_dup_stb(self):
        """Query: dup tag ref virtual super table queries

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, dup_ref
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")

        total = sum(len(SRC_A[dsrc]['data']) for _, dsrc, *_ in DUP)
        self._check_count("SELECT COUNT(*) FROM vst_dup;", total, "dup count")

        all_rows = []
        for vt, dsrc, t1d, t1t, t1c, t2d, t2t, t2c, t3d, t3t, t3c in DUP:
            v1 = _tag_val(t1d, t1t, t1c)
            v2 = _tag_val(t2d, t2t, t2c)
            v3 = _tag_val(t3d, t3t, t3c)
            for v in SRC_A[dsrc]['data']:
                all_rows.append((vt, v1, v2, v3, v))
        self._check_values("SELECT tbname, t1, t2, t3, val FROM vst_dup;",
                           all_rows, "dup full scan")

        exp = [r for r in all_rows if r[1] == 'beijing']
        self._check_values("SELECT tbname, t1, t2, t3, val FROM vst_dup WHERE t1='beijing';",
                           exp, "dup t1=beijing")

        exp = [r for r in all_rows if r[1] == 'guangzhou']
        self._check_values("SELECT tbname, t1, t2, t3, val FROM vst_dup WHERE t1='guangzhou';",
                           exp, "dup t1=guangzhou")

        tdLog.info("dup STB queries passed")

    # ==================================================================
    # Query Validation: Mixed literal + cross-DB (8 child tables)
    # ==================================================================

    def test_query_mixed_child_8(self):
        """Query: 8 child tables with literal + cross-DB tag refs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, mixed, cross_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        for vt, dsrc, lit, csrc, bsrc in MIXED:
            rows = SRC_A[dsrc]['data']
            city = SRC_A[csrc]['city']
            code = SRC_B[bsrc]['code']
            exp = [(lit, city, code, v) for v in rows]
            self._check_values(f"SELECT lit_id, t_city, t_code, val FROM {vt};",
                               exp, f"mixed {vt}")

    def test_query_mixed_stb(self):
        """Query: mixed literal+ref virtual super table queries

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, mixed
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")

        total = sum(len(SRC_A[dsrc]['data']) for _, dsrc, *_ in MIXED)
        self._check_count("SELECT COUNT(*) FROM vst_mixed;", total, "mixed count")

        all_rows = []
        for vt, dsrc, lit, csrc, bsrc in MIXED:
            city = SRC_A[csrc]['city']
            code = SRC_B[bsrc]['code']
            for v in SRC_A[dsrc]['data']:
                all_rows.append((vt, lit, city, code, v))
        self._check_values("SELECT tbname, lit_id, t_city, t_code, val FROM vst_mixed;",
                           all_rows, "mixed full scan")

        exp = [r for r in all_rows if int(str(r[1])) >= 500]
        self._check_values("SELECT tbname, lit_id, t_city, t_code, val FROM vst_mixed "
                           "WHERE lit_id >= 500;", exp, "mixed lit_id>=500")

        exp = [r for r in all_rows if r[2] == 'beijing']
        self._check_values("SELECT tbname, lit_id, t_city, t_code, val FROM vst_mixed "
                           "WHERE t_city='beijing';", exp, "mixed city=beijing")

        exp_gb = []
        for vt, dsrc, lit, csrc, bsrc in MIXED:
            rows = SRC_A[dsrc]['data']
            exp_gb.append((vt, lit, len(rows), sum(rows)))
        self._check_values("SELECT tbname, lit_id, COUNT(*), SUM(val) FROM vst_mixed "
                           "GROUP BY tbname, lit_id;", exp_gb, "mixed group by")

        tdLog.info("mixed STB queries passed")

    # ==================================================================
    # Query Validation: Local refs (5 child tables in td_aux)
    # ==================================================================

    def test_query_local_child_5(self):
        """Query: 5 child tables with same-DB local tag refs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, same_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_aux;")
        for vt, d, c, p in LOCAL:
            rows = SRC_AUX[d]['data']
            color = SRC_AUX[c]['color']
            pri = SRC_AUX[p]['pri']
            exp = [(color, pri, v) for v in rows]
            self._check_values(f"SELECT t_color, t_pri, val FROM {vt};",
                               exp, f"local {vt}")

    def test_query_local_stb(self):
        """Query: local ref virtual super table queries

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, same_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_aux;")

        total = sum(len(SRC_AUX[d]['data']) for _, d, _, _ in LOCAL)
        self._check_count("SELECT COUNT(*) FROM vst_local;", total, "local count")

        all_rows = []
        for vt, d, c, p in LOCAL:
            color = SRC_AUX[c]['color']
            pri = SRC_AUX[p]['pri']
            for v in SRC_AUX[d]['data']:
                all_rows.append((vt, color, pri, v))
        self._check_values("SELECT tbname, t_color, t_pri, val FROM vst_local;",
                           all_rows, "local full scan")

        exp = [r for r in all_rows if r[1] == 'red']
        self._check_values("SELECT tbname, t_color, t_pri, val FROM vst_local "
                           "WHERE t_color='red';", exp, "local color=red")

        exp = [r for r in all_rows if r[1] == 'green']
        self._check_values("SELECT tbname, t_color, t_pri, val FROM vst_local "
                           "WHERE t_color='green';", exp, "local color=green")

        tdLog.info("local STB queries passed")

    # ==================================================================
    # Query Validation: Hybrid local + cross-DB (4 child tables)
    # ==================================================================

    def test_query_hybrid_child_4(self):
        """Query: 4 child tables mixing local and cross-DB tag refs

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, hybrid, cross_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_aux;")
        for vt, local, dtbl, csrc, cdb, ctbl, ctag in HYBRID:
            if local:
                rows = SRC_AUX[dtbl]['data']
            elif dtbl.startswith('a_'):
                rows = SRC_A[dtbl]['data']
            else:
                rows = SRC_B[dtbl]['data']
            color = SRC_AUX[csrc]['color']
            city = _tag_val(cdb, ctbl, ctag)
            exp = [(color, city, v) for v in rows]
            self._check_values(f"SELECT t_color, t_city, val FROM {vt};",
                               exp, f"hybrid {vt}")

    def test_query_hybrid_stb(self):
        """Query: hybrid virtual super table queries

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, hybrid
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_aux;")

        all_rows = []
        for vt, local, dtbl, csrc, cdb, ctbl, ctag in HYBRID:
            if local:
                rows = SRC_AUX[dtbl]['data']
            elif dtbl.startswith('a_'):
                rows = SRC_A[dtbl]['data']
            else:
                rows = SRC_B[dtbl]['data']
            color = SRC_AUX[csrc]['color']
            city = _tag_val(cdb, ctbl, ctag)
            for v in rows:
                all_rows.append((vt, color, city, v))

        self._check_count("SELECT COUNT(*) FROM vst_hybrid;", len(all_rows), "hybrid count")
        self._check_values("SELECT tbname, t_color, t_city, val FROM vst_hybrid;",
                           all_rows, "hybrid full scan")

        tdLog.info("hybrid STB queries passed")

    # ==================================================================
    # Query Validation: Same-DB within ts_a (6 child tables)
    # ==================================================================

    def test_query_same_db_child_6(self):
        """Query: 6 child tables with same-DB tag refs within ts_a

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, same_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE ts_a;")
        for vt, d, c, p in SAME_A:
            rows = SRC_A[d]['data']
            city = SRC_A[c]['city']
            pop = SRC_A[p]['pop']
            exp = [(city, pop, v) for v in rows]
            self._check_values(f"SELECT t_city, t_pop, val FROM {vt};",
                               exp, f"same_a {vt}")

    def test_query_same_db_stb(self):
        """Query: same-DB virtual super table queries in ts_a

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, same_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE ts_a;")

        total = sum(len(SRC_A[d]['data']) for _, d, _, _ in SAME_A)
        self._check_count("SELECT COUNT(*) FROM vst_same_a;", total, "same_a count")

        all_rows = []
        for vt, d, c, p in SAME_A:
            city = SRC_A[c]['city']
            pop = SRC_A[p]['pop']
            for v in SRC_A[d]['data']:
                all_rows.append((vt, city, pop, v))
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_same_a;",
                           all_rows, "same_a full scan")

        exp = [r for r in all_rows if r[1] == 'beijing']
        self._check_values("SELECT tbname, t_city, t_pop, val FROM vst_same_a "
                           "WHERE t_city='beijing';", exp, "same_a city=beijing")

        exp_gb = []
        for vt, d, c, p in SAME_A:
            rows = SRC_A[d]['data']
            exp_gb.append((vt, SRC_A[c]['city'], len(rows), sum(rows)))
        self._check_values("SELECT tbname, t_city, COUNT(*), SUM(val) FROM vst_same_a "
                           "GROUP BY tbname, t_city;", exp_gb, "same_a group by")

        tdLog.info("same-DB STB queries passed")

    # ==================================================================
    # Query Validation: Advanced aggregates
    # ==================================================================

    def test_query_advanced_aggregates(self):
        """Query: advanced aggregation across virtual super tables

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, aggregate
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")

        # SUM on scale
        total_sum = sum(v for _, d, _, _ in SCALE for v in SRC_A[d]['data'])
        self._check_values("SELECT SUM(val) FROM vst_scale;",
                           [(total_sum,)], "scale SUM")

        # MIN/MAX on scale
        all_vals = [v for _, d, _, _ in SCALE for v in SRC_A[d]['data']]
        self._check_values("SELECT MIN(val), MAX(val) FROM vst_scale;",
                           [(min(all_vals), max(all_vals))], "scale MIN/MAX")

        # COUNT per city on mixed
        for city_name in ['beijing', 'guangzhou', 'nanjing']:
            cnt = sum(len(SRC_A[dsrc]['data'])
                      for _, dsrc, _, csrc, _ in MIXED
                      if SRC_A[csrc]['city'] == city_name)
            if cnt > 0:
                self._check_count(
                    f"SELECT COUNT(*) FROM vst_mixed WHERE t_city='{city_name}';",
                    cnt, f"mixed count city={city_name}")

        # data-only from 3db
        data_3db = [(v,) for _, ddb, dtbl, _, _, _ in TRIDB for v in _data_rows(ddb, dtbl)]
        self._check_values("SELECT val FROM vst_3db;", data_3db, "3db data-only")

        # SUM on 3db
        total_3db = sum(v for _, ddb, dtbl, _, _, _ in TRIDB for v in _data_rows(ddb, dtbl))
        self._check_values("SELECT SUM(val) FROM vst_3db;",
                           [(total_3db,)], "3db SUM")

        tdLog.info("advanced aggregates passed")

    # ==================================================================
    # Query Validation: COMBO — full combination test
    # 15 children, literal + 3-DB refs, multi-vgroup, multi-source-child
    # ==================================================================

    def test_query_combo_child_15(self):
        """Query: 15 combo child tables (literal + 3-DB tag refs)

        Each child has 1 literal tag + 3 tag refs from ts_a/ts_b/ts_c.
        Data sources span all 3 DBs. Tests multi-child, multi-DB, multi-vgroup.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, combo, cross_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        for vt, ddb, dtbl, lit, csrc, bsrc, lsrc in COMBO:
            rows = _data_rows(ddb, dtbl)
            city = SRC_A[csrc]['city']
            code = SRC_B[bsrc]['code']
            label = SRC_C[lsrc]['label']
            exp = [(lit, city, code, label, v) for v in rows]
            self._check_values(f"SELECT lit_score, ref_city, ref_code, ref_label, val FROM {vt};",
                               exp, f"combo {vt}")

    def test_query_combo_stb_full(self):
        """Query: combo STB full scan + count

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        total = len(all_rows)
        self._check_count("SELECT COUNT(*) FROM vst_combo;", total, "combo count")

        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo;",
            all_rows, "combo full scan")
        tdLog.info("combo full scan passed")

    def test_query_combo_projection(self):
        """Query: combo STB various column projections

        Tests tag-only, data-only, partial tag+data, tbname+tag projections.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, projection
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # tag-only: tbname + all 4 tags
        tag_exp = [(r[0], r[1], r[2], r[3], r[4]) for r in all_rows]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label FROM vst_combo;",
            tag_exp, "combo tag-only projection")

        # data-only
        data_exp = [(r[5],) for r in all_rows]
        self._check_values("SELECT val FROM vst_combo;", data_exp,
                           "combo data-only projection")

        # literal tag + data
        ld_exp = [(r[1], r[5]) for r in all_rows]
        self._check_values("SELECT lit_score, val FROM vst_combo;", ld_exp,
                           "combo lit+data projection")

        # ref tag + data
        rd_exp = [(r[2], r[5]) for r in all_rows]
        self._check_values("SELECT ref_city, val FROM vst_combo;", rd_exp,
                           "combo ref_city+data projection")

        # tbname + one ref tag + one literal tag
        tr_exp = [(r[0], r[2], r[1]) for r in all_rows]
        self._check_values("SELECT tbname, ref_city, lit_score FROM vst_combo;", tr_exp,
                           "combo tbname+ref+lit projection")

        tdLog.info("combo projection passed")

    def test_query_combo_literal_tag_filter(self):
        """Query: combo STB WHERE on literal tag (lit_score)

        Tests equality, range, and IN-style conditions on the literal tag.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, filter, literal
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # lit_score = 10 (vc01, vc02, vc13)
        exp = [r for r in all_rows if r[1] == 10]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo "
            "WHERE lit_score = 10;", exp, "combo lit_score=10")

        # lit_score >= 50 (vc09-vc15)
        exp = [r for r in all_rows if r[1] >= 50]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo "
            "WHERE lit_score >= 50;", exp, "combo lit_score>=50")

        # lit_score < 30 (vc01-vc04, vc13)
        exp = [r for r in all_rows if r[1] < 30]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo "
            "WHERE lit_score < 30;", exp, "combo lit_score<30")

        # COUNT with literal filter
        cnt = len([r for r in all_rows if r[1] == 30])
        self._check_count("SELECT COUNT(*) FROM vst_combo WHERE lit_score = 30;",
                          cnt, "combo count lit=30")

        cnt = len([r for r in all_rows if r[1] >= 60])
        self._check_count("SELECT COUNT(*) FROM vst_combo WHERE lit_score >= 60;",
                          cnt, "combo count lit>=60")

        tdLog.info("combo literal tag filter passed")

    def test_query_combo_ref_tag_filter(self):
        """Query: combo STB WHERE on ref tags (ref_city, ref_code, ref_label)

        Tests conditions on each of the 3 cross-DB tag refs independently.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, filter, cross_db
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # ref_city = 'beijing' (from ts_a)
        exp = [r for r in all_rows if r[2] == 'beijing']
        self._check_values(
            "SELECT tbname, lit_score, ref_city, val FROM vst_combo "
            "WHERE ref_city = 'beijing';", [(r[0], r[1], r[2], r[5]) for r in exp],
            "combo ref_city=beijing")

        # ref_city = 'chengdu'
        exp = [r for r in all_rows if r[2] == 'chengdu']
        self._check_values(
            "SELECT tbname, lit_score, ref_city, val FROM vst_combo "
            "WHERE ref_city = 'chengdu';", [(r[0], r[1], r[2], r[5]) for r in exp],
            "combo ref_city=chengdu")

        # ref_code >= 500 (from ts_b)
        exp = [r for r in all_rows if r[3] >= 500]
        self._check_values(
            "SELECT tbname, lit_score, ref_code, val FROM vst_combo "
            "WHERE ref_code >= 500;", [(r[0], r[1], r[3], r[5]) for r in exp],
            "combo ref_code>=500")

        # ref_label = 'x-ray' (from ts_c)
        exp = [r for r in all_rows if r[4] == 'x-ray']
        self._check_values(
            "SELECT tbname, ref_label, val FROM vst_combo "
            "WHERE ref_label = 'x-ray';", [(r[0], r[4], r[5]) for r in exp],
            "combo ref_label=x-ray")

        # ref_label = 'whiskey'
        exp = [r for r in all_rows if r[4] == 'whiskey']
        self._check_values(
            "SELECT tbname, ref_label, val FROM vst_combo "
            "WHERE ref_label = 'whiskey';", [(r[0], r[4], r[5]) for r in exp],
            "combo ref_label=whiskey")

        # COUNT with ref tag filter
        cnt = len([r for r in all_rows if r[2] == 'beijing'])
        self._check_count("SELECT COUNT(*) FROM vst_combo WHERE ref_city='beijing';",
                          cnt, "combo count ref_city=beijing")

        tdLog.info("combo ref tag filter passed")

    def test_query_combo_mixed_tag_filter(self):
        """Query: combo STB WHERE on literal + ref tags combined

        Tests combined conditions across literal and cross-DB ref tags.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, filter, mixed
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # lit_score = 10 AND ref_city = 'beijing'
        exp = [r for r in all_rows if r[1] == 10 and r[2] == 'beijing']
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo "
            "WHERE lit_score = 10 AND ref_city = 'beijing';", exp,
            "combo lit=10 AND city=beijing")

        # lit_score >= 40 AND ref_label = 'x-ray'
        exp = [r for r in all_rows if r[1] >= 40 and r[4] == 'x-ray']
        self._check_values(
            "SELECT tbname, lit_score, ref_label, val FROM vst_combo "
            "WHERE lit_score >= 40 AND ref_label = 'x-ray';",
            [(r[0], r[1], r[4], r[5]) for r in exp],
            "combo lit>=40 AND label=x-ray")

        # lit_score = 50 AND ref_code >= 400
        exp = [r for r in all_rows if r[1] == 50 and r[3] >= 400]
        self._check_values(
            "SELECT tbname, ref_city, ref_code, val FROM vst_combo "
            "WHERE lit_score = 50 AND ref_code >= 400;",
            [(r[0], r[2], r[3], r[5]) for r in exp],
            "combo lit=50 AND code>=400")

        # ref_city = 'shanghai' AND ref_code >= 300
        exp = [r for r in all_rows if r[2] == 'shanghai' and r[3] >= 300]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, val FROM vst_combo "
            "WHERE ref_city = 'shanghai' AND ref_code >= 300;",
            [(r[0], r[1], r[2], r[3], r[5]) for r in exp],
            "combo city=shanghai AND code>=300")

        tdLog.info("combo mixed tag filter passed")

    def test_query_combo_data_tag_filter(self):
        """Query: combo STB WHERE on data column + tag conditions combined

        Tests combined conditions on val (data) + literal tag + ref tags.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, filter, data
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # val >= 100 AND lit_score <= 20
        exp = [r for r in all_rows if r[5] >= 100 and r[1] <= 20]
        self._check_values(
            "SELECT tbname, lit_score, ref_city, val FROM vst_combo "
            "WHERE val >= 100 AND lit_score <= 20;",
            [(r[0], r[1], r[2], r[5]) for r in exp],
            "combo val>=100 AND lit<=20")

        # val >= 500 AND ref_city = 'hangzhou'
        exp = [r for r in all_rows if r[5] >= 500 and r[2] == 'hangzhou']
        self._check_values(
            "SELECT tbname, ref_city, val FROM vst_combo "
            "WHERE val >= 500 AND ref_city = 'hangzhou';",
            [(r[0], r[2], r[5]) for r in exp],
            "combo val>=500 AND city=hangzhou")

        # val < 50 AND lit_score >= 40 AND ref_label = 'x-ray'
        exp = [r for r in all_rows if r[5] < 50 and r[1] >= 40 and r[4] == 'x-ray']
        self._check_values(
            "SELECT tbname, lit_score, ref_label, val FROM vst_combo "
            "WHERE val < 50 AND lit_score >= 40 AND ref_label = 'x-ray';",
            [(r[0], r[1], r[4], r[5]) for r in exp],
            "combo val<50 AND lit>=40 AND label=x-ray")

        # tbname filter + tag condition
        exp = [r for r in all_rows if r[0] == 'vc13']
        self._check_values(
            "SELECT tbname, lit_score, ref_city, ref_code, ref_label, val FROM vst_combo "
            "WHERE tbname = 'vc13';", exp,
            "combo tbname=vc13")

        tdLog.info("combo data+tag filter passed")

    def test_query_combo_aggregate(self):
        """Query: combo STB aggregations with literal/ref tag filters

        Tests COUNT, SUM, AVG, MIN, MAX with various tag filter combinations.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, aggregate
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()
        all_vals = [r[5] for r in all_rows]

        # Global aggregates
        self._check_values("SELECT SUM(val) FROM vst_combo;",
                           [(sum(all_vals),)], "combo SUM(val)")
        self._check_values("SELECT MIN(val), MAX(val) FROM vst_combo;",
                           [(min(all_vals), max(all_vals))], "combo MIN/MAX")

        # COUNT with literal tag filter
        for score in [10, 20, 30, 40, 50, 60, 70]:
            sub = [r for r in all_rows if r[1] == score]
            if sub:
                self._check_count(
                    f"SELECT COUNT(*) FROM vst_combo WHERE lit_score = {score};",
                    len(sub), f"combo count lit={score}")

        # COUNT(*) with ref tag filter
        for city_name in ['beijing', 'chengdu', 'hangzhou']:
            cnt = len([r for r in all_rows if r[2] == city_name])
            if cnt > 0:
                self._check_count(
                    f"SELECT COUNT(*) FROM vst_combo WHERE ref_city = '{city_name}';",
                    cnt, f"combo count city={city_name}")

        # SUM with ref tag filter
        sub = [r[5] for r in all_rows if r[2] == 'beijing']
        if sub:
            self._check_values(
                "SELECT SUM(val) FROM vst_combo WHERE ref_city = 'beijing';",
                [(sum(sub),)], "combo SUM city=beijing")

        # SUM with literal + ref combined
        sub = [r[5] for r in all_rows if r[1] >= 50 and r[4] == 'x-ray']
        if sub:
            self._check_values(
                "SELECT SUM(val) FROM vst_combo WHERE lit_score >= 50 AND ref_label = 'x-ray';",
                [(sum(sub),)], "combo SUM lit>=50 AND label=x-ray")

        # COUNT with data + tag filter
        cnt = len([r for r in all_rows if r[5] >= 100 and r[1] <= 30])
        self._check_count(
            "SELECT COUNT(*) FROM vst_combo WHERE val >= 100 AND lit_score <= 30;",
            cnt, "combo count val>=100 AND lit<=30")

        tdLog.info("combo aggregates passed")

    def test_query_combo_group_by(self):
        """Query: combo STB GROUP BY literal tag, ref tag, and mixed

        Tests GROUP BY on literal tag, cross-DB ref tags, and combinations.

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref, stb, combo, group_by
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute("USE td_main;")
        all_rows = _combo_expected()

        # GROUP BY tbname (per-child summary)
        exp_gb = []
        for vt, ddb, dtbl, lit, csrc, bsrc, lsrc in COMBO:
            rows = _data_rows(ddb, dtbl)
            exp_gb.append((vt, lit, SRC_A[csrc]['city'], len(rows), sum(rows)))
        self._check_values(
            "SELECT tbname, lit_score, ref_city, COUNT(*), SUM(val) FROM vst_combo "
            "GROUP BY tbname, lit_score, ref_city;", exp_gb,
            "combo group by tbname")

        # GROUP BY lit_score
        from collections import defaultdict
        by_lit = defaultdict(lambda: [0, 0])
        for r in all_rows:
            by_lit[r[1]][0] += 1
            by_lit[r[1]][1] += r[5]
        exp_lit = [(score, cnt_sum[0], cnt_sum[1]) for score, cnt_sum in by_lit.items()]
        self._check_values(
            "SELECT lit_score, COUNT(*), SUM(val) FROM vst_combo GROUP BY lit_score;",
            exp_lit, "combo group by lit_score")

        # GROUP BY ref_city
        by_city = defaultdict(lambda: [0, 0])
        for r in all_rows:
            by_city[r[2]][0] += 1
            by_city[r[2]][1] += r[5]
        exp_city = [(city, cnt_sum[0], cnt_sum[1]) for city, cnt_sum in by_city.items()]
        self._check_values(
            "SELECT ref_city, COUNT(*), SUM(val) FROM vst_combo GROUP BY ref_city;",
            exp_city, "combo group by ref_city")

        # GROUP BY ref_label
        by_label = defaultdict(lambda: [0, 0])
        for r in all_rows:
            by_label[r[4]][0] += 1
            by_label[r[4]][1] += r[5]
        exp_label = [(label, cnt_sum[0], cnt_sum[1]) for label, cnt_sum in by_label.items()]
        self._check_values(
            "SELECT ref_label, COUNT(*), SUM(val) FROM vst_combo GROUP BY ref_label;",
            exp_label, "combo group by ref_label")

        # GROUP BY lit_score with tag filter on ref
        sub = [r for r in all_rows if r[4] == 'x-ray']
        by_lit2 = defaultdict(lambda: [0, 0])
        for r in sub:
            by_lit2[r[1]][0] += 1
            by_lit2[r[1]][1] += r[5]
        exp_lit2 = [(score, cnt_sum[0], cnt_sum[1]) for score, cnt_sum in by_lit2.items()]
        self._check_values(
            "SELECT lit_score, COUNT(*), SUM(val) FROM vst_combo "
            "WHERE ref_label = 'x-ray' GROUP BY lit_score;",
            exp_lit2, "combo group by lit WHERE label=x-ray")

        tdLog.info("combo group by passed")

    # ==================================================================
    # Query Validation: DDL database query (org_stb child tables)
    # ==================================================================

    def test_query_ddl_db_data(self):
        """Query: verify data access on DDL test database vtables

        Catalog: - VirtualTable
        Since: v3.3.6.0
        Labels: virtual, query, tag_ref
        Jira: None
        History: - 2026-3-31 Created
        """
        tdSql.execute(f"USE {DB_DDL};")

        # vctb_spec_0: all tags from org_child_0, data from org_child_0
        tdSql.query(f"SELECT int_col FROM {DB_DDL}.vctb_spec_0;")
        tdSql.checkRows(10)
        for i in range(10):
            assert tdSql.getData(i, 0) is not None

        # vctb_xdb_0: data from cross_child_0
        tdSql.query(f"SELECT int_col FROM {DB_DDL}.vctb_xdb_0;")
        tdSql.checkRows(10)

        # vstb super table count
        tdSql.query(f"SELECT COUNT(*) FROM {DB_DDL}.vstb;")
        count = tdSql.getData(0, 0)
        tdLog.info(f"  DDL vstb total count: {count}")
        assert int(count) > 0

        tdLog.info("DDL db query test passed")
