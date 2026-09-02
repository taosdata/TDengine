###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
###################################################################

"""Shared helpers for ext-source DDL tests.

Provides unified interfaces to provision remote databases/tables and
create EXTERNAL SOURCE objects, avoiding duplication across test files.
"""

import os
import sys

from new_test_framework.utils import tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402


# ---------------------------------------------------------------------------
# External source creation
# ---------------------------------------------------------------------------

def create_ext_source(name, src_type, database, schema="public"):
    """Create an EXTERNAL SOURCE in TDengine.

    Args:
        name:     source name in TDengine
        src_type: 'postgresql' | 'mysql' | 'influxdb'
        database: remote database/bucket name
        schema:   PG schema (only for postgresql, default 'public')
    """
    tdSql.execute(f"DROP EXTERNAL SOURCE IF EXISTS {name}")
    if src_type == "postgresql":
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {name} TYPE='{src_type}' "
            f"HOST='{ExtSrcEnv.PG_HOST}' PORT={ExtSrcEnv.PG_PORT} "
            f"USER='{ExtSrcEnv.PG_USER}' PASSWORD='{ExtSrcEnv.PG_PASS}' "
            f"DATABASE={database} SCHEMA={schema}")
    elif src_type == "mysql":
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {name} TYPE='{src_type}' "
            f"HOST='{ExtSrcEnv.MYSQL_HOST}' PORT={ExtSrcEnv.MYSQL_PORT} "
            f"USER='{ExtSrcEnv.MYSQL_USER}' PASSWORD='{ExtSrcEnv.MYSQL_PASS}' "
            f"DATABASE={database}")
    elif src_type == "influxdb":
        token = ExtSrcEnv._get_influx_token(ExtSrcEnv.INFLUX_VERSIONS[0])
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {name} TYPE='{src_type}' "
            f"HOST='{ExtSrcEnv.INFLUX_HOST}' PORT={ExtSrcEnv.INFLUX_PORT} "
            f"API_TOKEN='{token}' "
            f"DATABASE={database} OPTIONS('protocol'='flight_sql')")
    else:
        raise ValueError(f"Unknown source type: {src_type}")


# ---------------------------------------------------------------------------
# Remote database provisioning
# ---------------------------------------------------------------------------

def create_remote_db(src_type, database):
    """Create a remote database/bucket if it doesn't exist.

    Args:
        src_type: 'postgresql' | 'mysql' | 'influxdb'
        database: database/bucket name to create
    """
    if src_type == "postgresql":
        ExtSrcEnv.pg_create_db(database)
    elif src_type == "mysql":
        ExtSrcEnv.mysql_create_db(database)
    elif src_type == "influxdb":
        ExtSrcEnv.influx_create_db(database)
    else:
        raise ValueError(f"Unknown source type: {src_type}")


# ---------------------------------------------------------------------------
# Remote table creation
# ---------------------------------------------------------------------------

def create_pg_table(database, table, col_defs, rows=None):
    """Create (or replace) a PostgreSQL table and optionally insert rows.

    Args:
        database: PG database name
        table:    table name
        col_defs: column definition string, e.g.
                  "ts TIMESTAMP PRIMARY KEY, v INTEGER, w INTEGER"
        rows:     optional list of row value strings, e.g.
                  ["('2024-01-01', 1, 10)", "('2024-01-02', 2, 20)"]
    """
    stmts = [
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} ({col_defs})",
    ]
    if rows:
        stmts.append(f"INSERT INTO {table} VALUES {', '.join(rows)}")
    ExtSrcEnv.pg_exec(database, stmts)


def create_mysql_table(database, table, col_defs, rows=None):
    """Create (or replace) a MySQL table and optionally insert rows.

    Args:
        database: MySQL database name
        table:    table name
        col_defs: column definition string, e.g.
                  "ts DATETIME(3) NOT NULL PRIMARY KEY, v INT"
        rows:     optional list of row value strings, e.g.
                  ["('2024-01-01', 100)"]
    """
    stmts = [
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} ({col_defs})",
    ]
    if rows:
        stmts.append(f"INSERT INTO {table} VALUES {', '.join(rows)}")
    ExtSrcEnv.mysql_exec(database, stmts)


def create_pg_view(database, view, select_sql, schema="public"):
    """Create (or replace) a PostgreSQL view."""
    stmts = [
        f"DROP VIEW IF EXISTS {schema}.{view}",
        f"CREATE VIEW {schema}.{view} AS {select_sql}",
    ]
    ExtSrcEnv.pg_exec(database, stmts)


def create_mysql_view(database, view, select_sql):
    """Create (or replace) a MySQL view."""
    stmts = [
        f"DROP VIEW IF EXISTS `{view}`",
        f"CREATE VIEW `{view}` AS {select_sql}",
    ]
    ExtSrcEnv.mysql_exec(database, stmts)


def create_influx_measurement(database, lines):
    """Write line-protocol data to InfluxDB (schema-on-write).

    Args:
        database: InfluxDB bucket name
        lines:    list of line-protocol strings
    """
    ExtSrcEnv.influx_write(database, lines)
