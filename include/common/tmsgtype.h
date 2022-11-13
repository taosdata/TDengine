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

#ifndef _TD_COMMON_SQLMSGTYPE_H_
#define _TD_COMMON_SQLMSGTYPE_H_

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off

// sql type

#ifdef TSDB_SQL_C
#define TSDB_DEFINE_SQL_TYPE( name, msg ) msg, 
char *sqlMsgType[] = {
  "null",
#else
#define TSDB_DEFINE_SQL_TYPE( name, msg ) name,
enum {
  TSDB_SQL_NULL = 0,
#endif

  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SELECT, "select" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_FETCH, "fetch" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_INSERT, "insert" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_UPDATE_TAG_VAL, "update-tag-val" )

  // the SQL below is for mgmt node
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_MGMT, "mgmt" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_DB, "create-db" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_STABLE, "create-stable" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_TABLE, "create-table" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_FUNCTION, "create-function" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_DB, "drop-db" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_TABLE, "drop-table" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_FUNCTION, "drop-function" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_ACCT, "create-acct" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_USER, "create-user" ) 
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_ACCT, "drop-acct" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_USER, "drop-user" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_ALTER_USER, "alter-user" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_ALTER_ACCT, "alter-acct" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_ALTER_TABLE, "alter-table" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_ALTER_DB, "alter-db" )
 
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SYNC_DB_REPLICA, "sync db-replica")
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_MNODE, "create-mnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_MNODE, "drop-mnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CREATE_DNODE, "create-dnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DROP_DNODE, "drop-dnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CFG_DNODE, "cfg-dnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CFG_MNODE, "cfg-mnode" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SHOW, "show" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_RETRIEVE_MNODE, "retrieve" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_KILL_QUERY, "kill-query" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_KILL_STREAM, "kill-stream" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_KILL_CONNECTION, "kill-connection" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_COMPACT_VNODE, "compact-vnode" )

  // SQL below is for read operation
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_READ, "read" )  
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CONNECT, "connect" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_USE_DB, "use-db" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_META, "meta" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_STABLEVGROUP, "stable-vgroup" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_MULTI_META, "multi-meta" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_HB, "heart-beat" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_RETRIEVE_FUNC, "retrieve-function" )

  // SQL below for client local 
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_LOCAL, "local" ) 
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_DESCRIBE_TABLE, "describe-table" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_RETRIEVE_GLOBALMERGE, "retrieve-globalmerge" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_TABLE_JOIN_RETRIEVE, "join-retrieve" )

  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SHOW_CREATE_TABLE, "show-create-table")
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SHOW_CREATE_STABLE, "show-create-stable")
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SHOW_CREATE_DATABASE, "show-create-database")

  // build empty result instead of accessing dnode to fetch result reset the client cache
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_RETRIEVE_EMPTY_RESULT, "retrieve-empty-result" )

  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_RESET_CACHE,  "reset-cache" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SERV_STATUS,  "serv-status" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CURRENT_DB,   "current-db" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_SERV_VERSION, "serv-version" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CLI_VERSION,  "cli-version" )
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CURRENT_USER, "current-user ")
  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_CFG_LOCAL,    "cfg-local" )

  TSDB_DEFINE_SQL_TYPE( TSDB_SQL_MAX, "max" )
};

// clang-format on

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_SQLMSGTYPE_H_*/
