# Copyright (c) 2016 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Shared helpers for ext-source DDL tests.

Provides unified interfaces to provision remote databases/tables and
create EXTERNAL SOURCE objects, avoiding duplication across test files.
"""

import os
import sys

from new_test_framework.utils import tdLog, tdSql

_FQ_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 "..", "..", "..", "09-DataQuerying", "19-FederatedQuery"))
if _FQ_DIR not in sys.path:
    sys.path.insert(0, _FQ_DIR)
from federated_query_common import ExtSrcEnv  # noqa: E402


def extract_show_create_sql(sql):
    tdSql.query(sql)
    tdSql.checkRows(1)
    for col in range(tdSql.queryCols):
        value = tdSql.getData(0, col)
        if isinstance(value, str) and "CREATE" in value.upper():
            return value
    tdLog.exit(f"no CREATE statement in SHOW output: {sql}")


# ---------------------------------------------------------------------------
# External source creation
# ---------------------------------------------------------------------------

def create_ext_source(name, src_type, database, schema="public",
                      user=None, password=None):
    """Create an EXTERNAL SOURCE in TDengine.

    Args:
        name:     source name in TDengine
        src_type: 'postgresql' | 'mysql' | 'influxdb' | 'sqlserver' | 'oracle'
        database: remote database/bucket name; for Oracle this is the Easy
                  Connect service name (Oracle has no per-source database)
        schema:   PG schema (default 'public'); SQL Server schema (default
                  'dbo'); Oracle schema (defaults to the login user, upper
                  case, because unquoted Oracle identifiers fold to upper case)
        user:     Oracle login override (the fixture logs in as the schema
                  owner, since another schema's tables are not granted)
        password: Oracle password override
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
    elif src_type == "sqlserver":
        # The shared default schema is PG's "public"; SQL Server's default is "dbo".
        sqlserver_schema = "dbo" if schema in ("", None, "public") else schema
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {name} TYPE='{src_type}' "
            f"HOST='{ExtSrcEnv.SQLSERVER_HOST}' PORT={ExtSrcEnv.SQLSERVER_PORT} "
            f"USER='{ExtSrcEnv.SQLSERVER_USER}' PASSWORD='{ExtSrcEnv.SQLSERVER_PASS}' "
            f"DATABASE={database} SCHEMA={sqlserver_schema}")
    elif src_type == "oracle":
        # Oracle has one namespace level (the schema) and no CREATE DATABASE:
        # DATABASE carries the Easy Connect service name and SCHEMA carries the
        # schema.  The schema is the login user, so a non-default one needs
        # that user's credentials too.
        oracle_user = user if user is not None else ExtSrcEnv.ORACLE_USER
        oracle_pass = password if password is not None else ExtSrcEnv.ORACLE_PASS
        oracle_schema = (oracle_user.upper()
                         if schema in ("", None, "public") else schema)
        tdSql.execute(
            f"CREATE EXTERNAL SOURCE {name} TYPE='{src_type}' "
            f"HOST='{ExtSrcEnv.ORACLE_HOST}' PORT={ExtSrcEnv.ORACLE_PORT} "
            f"USER='{oracle_user}' PASSWORD='{oracle_pass}' "
            f"DATABASE={database} SCHEMA={oracle_schema}")
    else:
        raise ValueError(f"Unknown source type: {src_type}")


# ---------------------------------------------------------------------------
# Remote database provisioning
# ---------------------------------------------------------------------------

def create_remote_db(src_type, database):
    """Create a remote database/bucket if it doesn't exist.

    Args:
        src_type: 'postgresql' | 'mysql' | 'influxdb' | 'sqlserver' | 'oracle'
        database: database/bucket name to create; ignored for Oracle
    """
    if src_type == "postgresql":
        ExtSrcEnv.pg_create_db(database)
    elif src_type == "mysql":
        ExtSrcEnv.mysql_create_db(database)
    elif src_type == "influxdb":
        ExtSrcEnv.influx_create_db(database)
    elif src_type == "sqlserver":
        ExtSrcEnv.sqlserver_create_db(database)
    elif src_type == "oracle":
        # Oracle has no CREATE DATABASE in this model: the namespace is the
        # login user's schema, provisioned by ensure_ext_env.sh, and the
        # database argument above is the service name of the running instance.
        return
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


def create_sqlserver_table(database, table, col_defs, rows=None, schema="dbo"):
    """Create (or replace) a SQL Server table and optionally insert rows.

    Args:
        database: SQL Server database name
        table:    table name
        col_defs: column definition string, e.g.
                  "ts datetime2(3) NOT NULL PRIMARY KEY, v INT"
        rows:     optional list of row value strings, e.g.
                  ["('2024-01-01 00:00:00.000', 11)"]
        schema:   SQL Server schema (default 'dbo')
    """
    stmts = []
    if schema != "dbo":
        # A non-default schema must exist before the table can be created in it.
        stmts.append(
            f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA {schema}')")
    stmts.extend([
        f"IF OBJECT_ID('{schema}.{table}') IS NOT NULL DROP TABLE {schema}.{table}",
        f"CREATE TABLE {schema}.{table} ({col_defs})",
    ])
    if rows:
        stmts.append(
            f"INSERT INTO {schema}.{table} VALUES {', '.join(rows)}")
    ExtSrcEnv.sqlserver_exec(database, stmts)


def create_oracle_table(table, col_defs, rows=None, schema=None,
                        user=None, password=None):
    """Create (or replace) an Oracle table and optionally insert rows.

    Oracle has no CREATE OR REPLACE TABLE and no IF EXISTS for DROP TABLE, so
    an existing object is dropped first (ORA-00942 means there was none).

    Args:
        table:    table name (unquoted, so Oracle folds it to upper case)
        col_defs: column definition string, e.g.
                  "ts TIMESTAMP(3) NOT NULL PRIMARY KEY, v NUMBER(10,0)"
        rows:     optional list of row value strings, e.g.
                  ["(TIMESTAMP '2024-01-01 00:00:00.000', 11)"]
        schema:   owning schema; must be the login user, because the fixture
                  accounts hold no cross-schema grants
        user:     login used to run the DDL (defaults to the test user)
        password: password for `user`
    """
    cfg = next(ExtSrcEnv.oracle_version_configs())
    owner = (user if user is not None else cfg.user).upper()
    if schema not in ("", None) and schema.upper() != owner:
        raise ValueError(
            f"Oracle schema {schema!r} differs from the login user {owner!r}; "
            "the fixture accounts own no cross-schema grants")
    _user = user if user is not None else cfg.user
    _pass = password if password is not None else cfg.password
    try:
        ExtSrcEnv._oracle_sql(
            cfg, f'DROP TABLE "{table.upper()}" CASCADE CONSTRAINTS PURGE',
            user=_user, password=_pass)
    except RuntimeError as e:
        if "ORA-00942" not in str(e):
            raise
    stmts = [f"CREATE TABLE {table} ({col_defs})"]
    if rows:
        if len(rows) == 1:
            stmts.append(f"INSERT INTO {table} VALUES {rows[0]}")
        else:
            # Oracle has no multi-row VALUES clause; INSERT ALL is the
            # supported form for inserting several rows in one statement.
            into = " ".join(f"INTO {table} VALUES {row}" for row in rows)
            stmts.append(f"INSERT ALL {into} SELECT 1 FROM dual")
    stmts.append("COMMIT")
    ExtSrcEnv._oracle_sql(cfg, stmts, user=_user, password=_pass)


def create_oracle_view(view, select_sql, schema=None,
                       user=None, password=None):
    """Create (or replace) an Oracle view.

    Args:
        view:       view name (unquoted, so it folds to upper case)
        select_sql: the view body, fully qualified if it reads another schema
        schema/user/password: as for create_oracle_table
    """
    cfg = next(ExtSrcEnv.oracle_version_configs())
    owner = (user if user is not None else cfg.user).upper()
    if schema not in ("", None) and schema.upper() != owner:
        raise ValueError(
            f"Oracle schema {schema!r} differs from the login user {owner!r}; "
            "the fixture accounts own no cross-schema grants")
    _user = user if user is not None else cfg.user
    _pass = password if password is not None else cfg.password
    try:
        ExtSrcEnv._oracle_sql(cfg, f'DROP VIEW "{view.upper()}"',
                              user=_user, password=_pass)
    except RuntimeError as e:
        if "ORA-00942" not in str(e):
            raise
    ExtSrcEnv._oracle_sql(cfg, f"CREATE VIEW {view} AS {select_sql}",
                          user=_user, password=_pass)


def create_oracle_materialized_view(view, select_sql, schema=None):
    """Create (or replace) an Oracle materialized view.

    The fixture accounts are granted CREATE TABLE/VIEW/SEQUENCE but not
    CREATE MATERIALIZED VIEW, so the object is created by the instance's
    admin account inside the schema owner's namespace; the owner can then read
    it like any other object of its own schema.

    Args:
        view:       materialized view name (unquoted, folds to upper case)
        select_sql: the materialized view body
        schema:     owning schema (defaults to the test user)
    """
    cfg = next(ExtSrcEnv.oracle_version_configs())
    owner = (schema or ExtSrcEnv.ORACLE_USER).upper()
    qualified = f'"{owner}"."{view.upper()}"'
    try:
        ExtSrcEnv._oracle_sql(cfg, f"DROP MATERIALIZED VIEW {qualified}")
    except RuntimeError as e:
        # ORA-00942: no such table; ORA-12003: the materialized view itself
        # does not exist (a missing MV is not reported as a missing table).
        if "ORA-00942" not in str(e) and "ORA-12003" not in str(e):
            raise
    ExtSrcEnv.oracle_admin_exec_cfg(cfg, [
        f"CREATE MATERIALIZED VIEW {qualified} AS {select_sql}"])


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
