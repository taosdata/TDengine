###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""DDL tests for DROP VTABLE / DROP STABLE … VIRTUAL referencing ext sources.

Section 5 of the DDL test plan.
"""

# -*- coding: utf-8 -*-
import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402
from ext_source_helpers import (  # noqa: E402
    create_ext_source, create_remote_db, create_pg_table, create_mysql_table)


_LOCAL_DB = "vdrop_local"
_PG_DB    = "vdrop_pg"
_MY_DB    = "vdrop_my"
_PG_SRC   = "vdrop_pg_src"
_MY_SRC   = "vdrop_my_src"


def _ensure_env():
    create_remote_db("postgresql", _PG_DB)
    create_remote_db("mysql", _MY_DB)
    create_pg_table(_PG_DB, "r",
                    "ts TIMESTAMP PRIMARY KEY, v INTEGER",
                    ["('2024-01-01 00:00:00', 1)"])
    create_mysql_table(_MY_DB, "r",
                       "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT",
                       ["('2024-01-01 00:00:00', 100)"])
    create_ext_source(_PG_SRC, "postgresql", _PG_DB)
    create_ext_source(_MY_SRC, "mysql", _MY_DB)


# ===========================================================================

class TestVtableDropExtSource:

    @classmethod
    def setup_class(cls):
        ExtSrcEnv.ensure_env()
        ExtSrcEnv.ensure_qnode()
        _ensure_env()
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        tdSql.execute(f"CREATE DATABASE {_LOCAL_DB} PRECISION 'ms'")
        tdSql.execute(f"USE {_LOCAL_DB}")

    @classmethod
    def teardown_class(cls):
        tdSql.execute(f"DROP DATABASE IF EXISTS {_LOCAL_DB}")
        for n in (_PG_SRC, _MY_SRC):
            tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {n}")

    def setup_method(self, method):
        tdSql.execute(f"USE {_LOCAL_DB}")

    # -------------------------------------------------------------------

    def test_drop_normal_vtable(self):
        tdSql.execute(
            f"CREATE VTABLE v_drop_n (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v)")
        tdSql.execute("DROP VTABLE v_drop_n")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND table_name='v_drop_n'")
        tdSql.checkData(0, 0, 0)

    def test_drop_if_exists_missing(self):
        tdSql.execute("DROP VTABLE IF EXISTS v_drop_missing")

    def test_drop_missing_without_if_exists_fails(self):
        tdSql.error("DROP VTABLE v_drop_missing2")

    def test_drop_child_vtable(self):
        tdSql.execute(
            "CREATE STABLE stb_drop (ts timestamp, v int) "
            "TAGS (s nchar(8)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_drop_one ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) USING stb_drop TAGS ('a')")
        tdSql.execute("DROP VTABLE vctb_drop_one")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{_LOCAL_DB}' AND stable_name='stb_drop'")
        tdSql.checkData(0, 0, 0)
        tdSql.execute("DROP STABLE stb_drop")

    def test_drop_vstable_with_children(self):
        # DROP STABLE removes the stable AND all of its child vtables.
        # If the product enforces "must be empty", flip the expectation —
        # the test then asserts the error.
        tdSql.execute(
            "CREATE STABLE stb_drop_c (ts timestamp, v int) "
            "TAGS (s nchar(8)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_dc1 ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) USING stb_drop_c TAGS ('a')")
        tdSql.execute(
            f"CREATE VTABLE vctb_dc2 ("
            f"v FROM {_MY_SRC}.{_MY_DB}.r.v) USING stb_drop_c TAGS ('b')")
        tdSql.execute("DROP STABLE stb_drop_c")
        tdSql.query(
            "SELECT count(*) FROM information_schema.ins_tables "
            f"WHERE db_name='{_LOCAL_DB}' AND stable_name='stb_drop_c'")
        tdSql.checkData(0, 0, 0)

    def test_drop_vstable_heterogeneous_children(self):
        # Same as above but the children come from different ext sources —
        # ensures the drop cleans up references in both source bookkeepings.
        tdSql.execute(
            "CREATE STABLE stb_drop_het (ts timestamp, v int) "
            "TAGS (s nchar(8)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_dh_pg ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) USING stb_drop_het TAGS ('pg')")
        tdSql.execute(
            f"CREATE VTABLE vctb_dh_my ("
            f"v FROM {_MY_SRC}.{_MY_DB}.r.v) USING stb_drop_het TAGS ('my')")
        tdSql.execute("DROP STABLE stb_drop_het")
        # After drop, both ext sources are still droppable — proves the
        # references were released.
        tdSql.execute(f"DROP EXTERNAL SOURCE {_PG_SRC}")
        tdSql.execute(f"DROP EXTERNAL SOURCE {_MY_SRC}")
        _ensure_env()
        tdSql.execute(f"USE {_LOCAL_DB}")

    def test_drop_recreate_same_name(self):
        tdSql.execute(
            f"CREATE VTABLE v_drop_rc (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v)")
        tdSql.execute("DROP VTABLE v_drop_rc")
        # Recreate same name with a different non-ext column type.
        tdSql.execute(
            f"CREATE VTABLE v_drop_rc (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v, extra double)")
        tdSql.query("DESCRIBE v_drop_rc")
        tdSql.checkRows(3)
        tdSql.checkData(1, 0, "v")
        tdSql.checkData(2, 0, "extra")
        tdSql.execute("DROP VTABLE v_drop_rc")

    def test_drop_database_cascade(self):
        # Create a fresh DB with vtable/vstb on it and drop the whole DB.
        db = "vdrop_cascade"
        tdSql.execute(f"DROP DATABASE IF EXISTS {db}")
        tdSql.execute(f"CREATE DATABASE {db} PRECISION 'ms'")
        tdSql.execute(f"USE {db}")
        tdSql.execute(
            f"CREATE VTABLE v_casc (ts timestamp, "
            f"v int FROM {_PG_SRC}.{_PG_DB}.r.v)")
        tdSql.execute(
            "CREATE STABLE stb_casc (ts timestamp, v int) "
            "TAGS (s nchar(8)) VIRTUAL 1")
        tdSql.execute(
            f"CREATE VTABLE vctb_casc ("
            f"v FROM {_PG_SRC}.{_PG_DB}.r.v) USING stb_casc TAGS ('x')")
        tdSql.execute(f"DROP DATABASE {db}")
        tdSql.query("SELECT count(*) FROM information_schema.ins_tables "
                    f"WHERE db_name='{db}'")
        tdSql.checkData(0, 0, 0)
        tdSql.execute(f"USE {_LOCAL_DB}")
