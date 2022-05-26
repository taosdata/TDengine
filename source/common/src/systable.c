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

#include "systable.h"
#include "taos.h"
#include "tdef.h"
#include "types.h"

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_COL_NAME_LEN   ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

static const SSysDbTableSchema dnodesSchema[] = {
    {.name = "id", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "support_vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "note", .bytes = 256 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema mnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "role", .bytes = 12 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema modulesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = 134 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "module", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema qnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema snodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema bnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema clusterSchema[] = {
    {.name = "id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "name", .bytes = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema userDBSchema[] = {
    {.name = "name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "vgroups", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT},
    {.name = "ntables", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "replica", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "strict", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "duration", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "keep", .bytes = 24 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "buffer", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "pagesize", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "pages", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "minrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "maxrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "wal", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "fsync", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "comp", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "cachelast", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "precision", .bytes = 2 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "single_stable", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    // {.name = "update", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},  // disable update
};

static const SSysDbTableSchema userFuncSchema[] = {
    {.name = "name", .bytes = TSDB_FUNC_NAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "comment", .bytes = PATH_MAX - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "aggregate", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "output_type", .bytes = TSDB_TYPE_STR_MAX_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "code_len", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "bufsize", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SSysDbTableSchema userIdxSchema[] = {
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_database", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "column_name", .bytes = SYSTABLE_SCH_COL_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_type", .bytes = 10, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "index_extensions", .bytes = 256, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema userStbsSchema[] = {
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "tags", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "last_update", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "table_comment", .bytes = 1024 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema userStreamsSchema[] = {
    {.name = "stream_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "user_name", .bytes = 23, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "dest_table", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = 1024, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema userTblsSchema[] = {
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "uid", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "ttl", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "table_comment", .bytes = 512 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "type", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema userTblDistSchema[] = {
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

static const SSysDbTableSchema userUsersSchema[] = {
    {.name = "name", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "privilege", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema grantsSchema[] = {
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

static const SSysDbTableSchema vgroupsSchema[] = {
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
    {.name = "tsma", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT},
};

static const SSysDbTableSchema smaSchema[] = {
    {.name = "sma_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SSysDbTableSchema transSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "stage", .bytes = TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "type", .bytes = TSDB_TRANS_TYPE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "failed_times", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "last_exec_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "last_error", .bytes = (TSDB_TRANS_ERROR_LEN - 1) + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema configSchema[] = {
    {.name = "name", .bytes = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "value", .bytes = TSDB_CONIIG_VALUE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysTableMeta infosMeta[] = {
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
    {TSDB_INS_TABLE_CONFIGS, configSchema, tListLen(configSchema)},
};

static const SSysDbTableSchema connectionsSchema[] = {
    {.name = "conn_id", .bytes = 4, .type = TSDB_DATA_TYPE_UINT},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "program", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "end_point", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "login_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "last_access", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema topicSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    // TODO config
};

static const SSysDbTableSchema consumerSchema[] = {
    {.name = "consumer_id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "consumer_group", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "client_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "status", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "topics", .bytes = TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "end_point", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "up_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "subscribe_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "rebalance_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SSysDbTableSchema subscriptionSchema[] = {
    {.name = "topic_name", .bytes = TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "consumer_group", .bytes = TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "consumer_id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
};

static const SSysDbTableSchema offsetSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "committed_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "current_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "skip_log_cnt", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
};

static const SSysDbTableSchema querySchema[] = {
    {.name = "query_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "connId", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "end_point", .bytes = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "qid", .bytes = 22 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "time", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "sql_obj_id", .bytes = QUERY_OBJ_ID_SIZE + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "ep", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "stable_query", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL},
    {.name = "sub_queries", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "sub_query_info", .bytes = TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema streamSchema[] = {
    {.name = "stream_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "status", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "source_db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "target_db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "target_table", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR},
    {.name = "watermark", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "trigger", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
};

static const SSysTableMeta perfsMeta[] = {
    {TSDB_PERFS_TABLE_CONNECTIONS, connectionsSchema, tListLen(connectionsSchema)},
    {TSDB_PERFS_TABLE_QUERIES, querySchema, tListLen(querySchema)},
    {TSDB_PERFS_TABLE_TOPICS, topicSchema, tListLen(topicSchema)},
    {TSDB_PERFS_TABLE_CONSUMERS, consumerSchema, tListLen(consumerSchema)},
    {TSDB_PERFS_TABLE_SUBSCRIPTIONS, subscriptionSchema, tListLen(subscriptionSchema)},
    {TSDB_PERFS_TABLE_OFFSETS, offsetSchema, tListLen(offsetSchema)},
    {TSDB_PERFS_TABLE_TRANS, transSchema, tListLen(transSchema)},
    {TSDB_PERFS_TABLE_SMAS, smaSchema, tListLen(smaSchema)},
    {TSDB_PERFS_TABLE_STREAMS, streamSchema, tListLen(streamSchema)},
};

void getInfosDbMeta(const SSysTableMeta** pInfosTableMeta, size_t* size) {
  *pInfosTableMeta = infosMeta;
  *size = tListLen(infosMeta);
}

void getPerfDbMeta(const SSysTableMeta** pPerfsTableMeta, size_t* size) {
  *pPerfsTableMeta = perfsMeta;
  *size = tListLen(perfsMeta);
}
