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

#define _DEFAULT_SOURCE
#include "mndInfoSchema.h"
#include "mndInt.h"

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_COL_NAME_LEN   ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

static const SInfosTableSchema dnodesSchema[] = {
    {.name = "id", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "max_vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "note", .bytes = 256 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema mnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "role", .bytes = 12 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "role_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema modulesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = 134 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "module", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema qnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema snodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema bnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema clusterSchema[] = {
    {.name = "id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "name", .bytes = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema userDBSchema[] = {
    {.name = "name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "vgroups", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "ntables", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "replica", .bytes = 2, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "strict", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "days", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "keep", .bytes = 24 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "cache", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "blocks", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "minrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "maxrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "wal", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "fsync", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "comp", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "cachelast", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "precision", .bytes = 2 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "ttl", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "single_stable", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "stream_mode", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    // {.name = "update", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},  // disable update
};

static const SInfosTableSchema userFuncSchema[] = {
    {.name = "name", .bytes = TSDB_FUNC_NAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "comment", .bytes = PATH_MAX - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "aggregate", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "comment", .bytes = TSDB_TYPE_STR_MAX_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "code_len", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "bufsize", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SInfosTableSchema userIdxSchema[] = {
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_database", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "column_name", .bytes = SYSTABLE_SCH_COL_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_type", .bytes = 10, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_extensions", .bytes = 256, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema userStbsSchema[] = {
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "tags", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "last_update", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "table_comment", .bytes = 1024 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema userStreamsSchema[] = {
    {.name = "stream_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "user_name", .bytes = 23, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "dest_table", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = 1024, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema userTblsSchema[] = {
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "uid", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "ttl", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "table_comment", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SInfosTableSchema userTblDistSchema[] = {
    {.name = "db_name", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "table_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "distributed_histogram", .bytes = 500 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "min_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "max_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "avg_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "stddev_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "rows", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "blocks", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "storage_size", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "compression_ratio", .bytes = 8, .type = TSDB_DATA_TYPE_DOUBLE},
    {.name = "rows_in_mem", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "seek_header_time", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SInfosTableSchema userUsersSchema[] = {
    {.name = "name", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "privilege", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "account", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema grantsSchema[] = {
    {.name = "version", .bytes = 8 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "expire time", .bytes = 19 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "expired", .bytes = 5 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "storage(GB)", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "timeseries", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "databases", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "users", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "accounts", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "dnodes", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "connections", .bytes = 11 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "streams", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "cpu cores", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "speed(PPS)", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "querytime", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema vgroupsSchema[] = {
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "tables", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "v1_dnode", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "v1_status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "v2_dnode", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "v2_status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "v3_dnode", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "v3_status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "status", .bytes = 12 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "nfiles", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "file_size", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

// TODO put into perf schema
static const SInfosTableSchema topicSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = 1024 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "row_len", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SInfosTableSchema consumerSchema[] = {
    {.name = "client_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "status", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    // ep
    // up time
    // topics
};

static const SInfosTableSchema subscribeSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "client_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema smaSchema[] = {
    {.name = "sma_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema transSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "created_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "stage", .bytes = TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "type", .bytes = TSDB_TRANS_TYPE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "last_exec_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "last_error", .bytes = (TSDB_TRANS_ERROR_LEN - 1) + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema configSchema[] = {
    {.name = "name", .bytes = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "value", .bytes = TSDB_CONIIG_VALUE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableSchema connSchema[] = {
    {.name = "connId", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "program", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "ip:port", .bytes = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "login_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "last_access", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SInfosTableSchema querySchema[] = {
    {.name = "queryId", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "connId", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "ip:port", .bytes = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "qid", .bytes = 22 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "created_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "time", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "sql_obj_id", .bytes = QUERY_OBJ_ID_SIZE + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "ep", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "stable_query", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL},
    {.name = "sub_queries", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "sub_query_info", .bytes = TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SInfosTableMeta infosMeta[] = {
    {TSDB_INS_TABLE_DNODES, dnodesSchema, tListLen(dnodesSchema)},
    {TSDB_INS_TABLE_MNODES, mnodesSchema, tListLen(mnodesSchema)},
    {TSDB_INS_TABLE_MODULES, modulesSchema, tListLen(modulesSchema)},
    {TSDB_INS_TABLE_QNODES, qnodesSchema, tListLen(qnodesSchema)},
    {TSDB_INS_TABLE_SNODES, snodesSchema, tListLen(snodesSchema)},
    {TSDB_INS_TABLE_BNODES, bnodesSchema, tListLen(bnodesSchema)},
    {TSDB_INS_TABLE_CLUSTER, clusterSchema, tListLen(clusterSchema)},
    {TSDB_INS_TABLE_USER_DATABASES, userDBSchema, tListLen(userDBSchema)},
    {TSDB_INS_TABLE_USER_FUNCTIONS, userFuncSchema, tListLen(userFuncSchema)},
    {TSDB_INS_TABLE_USER_INDEXES, userIdxSchema, tListLen(userIdxSchema)},
    {TSDB_INS_TABLE_USER_STABLES, userStbsSchema, tListLen(userStbsSchema)},
    {TSDB_INS_TABLE_USER_STREAMS, userStreamsSchema, tListLen(userStreamsSchema)},
    {TSDB_INS_TABLE_USER_TABLES, userTblsSchema, tListLen(userTblsSchema)},
    {TSDB_INS_TABLE_USER_TABLE_DISTRIBUTED, userTblDistSchema, tListLen(userTblDistSchema)},
    {TSDB_INS_TABLE_USER_USERS, userUsersSchema, tListLen(userUsersSchema)},
    {TSDB_INS_TABLE_LICENCES, grantsSchema, tListLen(grantsSchema)},
    {TSDB_INS_TABLE_VGROUPS, vgroupsSchema, tListLen(vgroupsSchema)},
    {TSDB_INS_TABLE_TOPICS, topicSchema, tListLen(topicSchema)},
    {TSDB_INS_TABLE_CONSUMERS, consumerSchema, tListLen(consumerSchema)},
    {TSDB_INS_TABLE_SUBSCRIBES, subscribeSchema, tListLen(subscribeSchema)},
    {TSDB_INS_TABLE_TRANS, transSchema, tListLen(transSchema)},
    {TSDB_INS_TABLE_SMAS, smaSchema, tListLen(smaSchema)},
    {TSDB_INS_TABLE_CONFIGS, configSchema, tListLen(configSchema)},
    {TSDB_INS_TABLE_CONNS, connSchema, tListLen(connSchema)},
    {TSDB_INS_TABLE_QUERIES, querySchema, tListLen(querySchema)},
};

static int32_t mndInitInfosTableSchema(const SInfosTableSchema *pSrc, int32_t colNum, SSchema **pDst) {
  SSchema *schema = taosMemoryCalloc(colNum, sizeof(SSchema));
  if (NULL == schema) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < colNum; ++i) {
    tstrncpy(schema[i].name, pSrc[i].name, sizeof(schema[i].name));
    schema[i].type = pSrc[i].type;
    schema[i].colId = i + 1;
    schema[i].bytes = pSrc[i].bytes;
  }

  *pDst = schema;
  return 0;
}

static int32_t mndInsInitMeta(SHashObj *hash) {
  STableMetaRsp meta = {0};

  tstrncpy(meta.dbFName, TSDB_INFORMATION_SCHEMA_DB, sizeof(meta.dbFName));
  meta.tableType = TSDB_SYSTEM_TABLE;
  meta.sversion = 1;
  meta.tversion = 1;

  for (int32_t i = 0; i < tListLen(infosMeta); ++i) {
    tstrncpy(meta.tbName, infosMeta[i].name, sizeof(meta.tbName));
    meta.numOfColumns = infosMeta[i].colNum;

    if (mndInitInfosTableSchema(infosMeta[i].schema, infosMeta[i].colNum, &meta.pSchemas)) {
      return -1;
    }

    if (taosHashPut(hash, meta.tbName, strlen(meta.tbName) + 1, &meta, sizeof(meta))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

int32_t mndBuildInsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  if (NULL == pMnode->infosMeta) {
    terrno = TSDB_CODE_MND_NOT_READY;
    return -1;
  }

  STableMetaRsp *pMeta = taosHashGet(pMnode->infosMeta, tbName, strlen(tbName) + 1);
  if (NULL == pMeta) {
    mError("invalid information schema table name:%s", tbName);
    terrno = TSDB_CODE_MND_INVALID_INFOS_TBL;
    return -1;
  }

  *pRsp = *pMeta;

  pRsp->pSchemas = taosMemoryCalloc(pMeta->numOfColumns, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    pRsp->pSchemas = NULL;
    return -1;
  }

  memcpy(pRsp->pSchemas, pMeta->pSchemas, pMeta->numOfColumns * sizeof(SSchema));
  return 0;
}

int32_t mndInitInfos(SMnode *pMnode) {
  pMnode->infosMeta = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), false, HASH_NO_LOCK);
  if (pMnode->infosMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return mndInsInitMeta(pMnode->infosMeta);
}

void mndCleanupInfos(SMnode *pMnode) {
  if (NULL == pMnode->infosMeta) {
    return;
  }

  STableMetaRsp *pMeta = taosHashIterate(pMnode->infosMeta, NULL);
  while (pMeta) {
    taosMemoryFreeClear(pMeta->pSchemas);
    pMeta = taosHashIterate(pMnode->infosMeta, pMeta);
  }

  taosHashCleanup(pMnode->infosMeta);
  pMnode->infosMeta = NULL;
}
