/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_QSQLCMD_H
#define TDENGINE_QSQLCMD_H

#ifdef __cplusplus
extern "C" {
#endif

enum _sql_type {
  TSDB_SQL_SELECT = 1,
  TSDB_SQL_FETCH,
  TSDB_SQL_INSERT,

  TSDB_SQL_MGMT,  // the SQL below is for mgmt node
  TSDB_SQL_CREATE_DB,
  TSDB_SQL_CREATE_TABLE,
  TSDB_SQL_DROP_DB,
  TSDB_SQL_DROP_TABLE,
  TSDB_SQL_CREATE_ACCT,
  TSDB_SQL_CREATE_USER,  // 10
  TSDB_SQL_DROP_ACCT,
  TSDB_SQL_DROP_USER,
  TSDB_SQL_ALTER_USER,
  TSDB_SQL_ALTER_ACCT,
  TSDB_SQL_ALTER_TABLE,
  TSDB_SQL_ALTER_DB,
  TSDB_SQL_CREATE_MNODE,
  TSDB_SQL_DROP_MNODE,
  TSDB_SQL_CREATE_DNODE,
  TSDB_SQL_DROP_DNODE,  // 20
  TSDB_SQL_CFG_DNODE,
  TSDB_SQL_CFG_MNODE,
  TSDB_SQL_SHOW,
  TSDB_SQL_RETRIEVE,
  TSDB_SQL_KILL_QUERY,
  TSDB_SQL_KILL_STREAM,
  TSDB_SQL_KILL_CONNECTION,

  TSDB_SQL_READ,  // SQL below is for read operation
  TSDB_SQL_CONNECT,
  TSDB_SQL_USE_DB,  // 30
  TSDB_SQL_META,
  TSDB_SQL_STABLEVGROUP,
  TSDB_SQL_MULTI_META,
  TSDB_SQL_HB,

  TSDB_SQL_LOCAL,  // SQL below for client local
  TSDB_SQL_DESCRIBE_TABLE,
  TSDB_SQL_RETRIEVE_LOCALMERGE,
  TSDB_SQL_METRIC_JOIN_RETRIEVE,

  /*
   * build empty result instead of accessing dnode to fetch result
   * reset the client cache
   */
  TSDB_SQL_RETRIEVE_EMPTY_RESULT,

  TSDB_SQL_RESET_CACHE,    // 40
  TSDB_SQL_SERV_STATUS,
  TSDB_SQL_CURRENT_DB,
  TSDB_SQL_SERV_VERSION,
  TSDB_SQL_CLI_VERSION,
  TSDB_SQL_CURRENT_USER,
  TSDB_SQL_CFG_LOCAL,

  TSDB_SQL_MAX  // 47
};


// create table operation type
enum TSQL_TYPE {
  TSQL_CREATE_TABLE = 0x1,
  TSQL_CREATE_STABLE = 0x2,
  TSQL_CREATE_TABLE_FROM_STABLE = 0x3,
  TSQL_CREATE_STREAM = 0x4,
};

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QSQLCMD_H
