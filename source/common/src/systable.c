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
#include "taosdef.h"
#include "tdef.h"
#include "tgrant.h"
#include "tmsg.h"
#include "types.h"

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_COL_NAME_LEN   ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_VIEW_NAME_LEN ((TSDB_VIEW_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

// clang-format off
static const SSysDbTableSchema dnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "support_vnodes", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "reboot_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "note", .bytes = 256 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
#ifdef TD_ENTERPRISE
    {.name = "machine_id", .bytes = TSDB_MACHINE_ID_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
#endif
};

static const SSysDbTableSchema mnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "role", .bytes = 12 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "role_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
};

static const SSysDbTableSchema modulesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "endpoint", .bytes = 134 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "module", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysDbTableSchema qnodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
};

static const SSysDbTableSchema snodesSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "endpoint", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
};

static const SSysDbTableSchema clusterSchema[] = {
    {.name = "id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = true},
    {.name = "name", .bytes = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "uptime", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "version", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "expire_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
};

static const SSysDbTableSchema userDBSchema[] = {
    {.name = "name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "vgroups", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "ntables", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "replica", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "strict", .bytes = TSDB_DB_STRICT_STR_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "duration", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "keep", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "buffer", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "pagesize", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "pages", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "minrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "maxrows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "comp", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "precision", .bytes = 2 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "status", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "retentions", .bytes = 60 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "single_stable", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL, .sysInfo = true},
    {.name = "cachemodel", .bytes = TSDB_CACHE_MODEL_STR_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "cachesize", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "wal_level", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "wal_fsync_period", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "wal_retention_period", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "wal_retention_size", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = true},
    {.name = "stt_trigger", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "table_prefix", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "table_suffix", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "tsdb_pagesize", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "keep_time_offset", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
};

static const SSysDbTableSchema userFuncSchema[] = {
    {.name = "name", .bytes = TSDB_FUNC_NAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "comment", .bytes = PATH_MAX - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "aggregate", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "output_type", .bytes = TSDB_TYPE_STR_MAX_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "code_len", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "bufsize", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "func_language", .bytes = TSDB_TYPE_STR_MAX_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "func_body", .bytes = TSDB_MAX_BINARY_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "func_version", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
};

static const SSysDbTableSchema userIdxSchema[] = {
    {.name = "index_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "column_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "index_type", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema userStbsSchema[] = {
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "tags", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "last_update", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "table_comment", .bytes = TSDB_TB_COMMENT_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "watermark", .bytes = 64 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "max_delay", .bytes = 64 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "rollup", .bytes = 128 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema streamSchema[] = {
    {.name = "stream_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "status", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "source_db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "target_db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "target_table", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "watermark", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "trigger", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "sink_quota", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "history_scan_idle", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema streamTaskSchema[] = {
    {.name = "stream_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "task_id", .bytes = 16 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "node_type", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "node_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "level", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "status", .bytes = 12 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "stage", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "in_queue", .bytes = 20, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
//    {.name = "out_queue", .bytes = 20, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "info", .bytes = 25, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema userTblsSchema[] = {
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "columns", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "uid", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "ttl", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "table_comment", .bytes = TSDB_TB_COMMENT_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "type", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema userTagsSchema[] = {
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "tag_name", .bytes = TSDB_COL_NAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "tag_type", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "tag_value", .bytes = TSDB_MAX_TAGS_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema userColsSchema[] = {
    {.name = "table_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "table_type", .bytes = 21 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "col_name", .bytes = TSDB_COL_NAME_LEN - 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "col_type", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "col_length", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "col_precision", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "col_scale", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "col_nullable", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false}
};

static const SSysDbTableSchema userTblDistSchema[] = {
    {.name = "db_name", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "table_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "distributed_histogram", .bytes = 500 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "min_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "max_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "avg_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "stddev_of_rows", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "rows", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = true},
    {.name = "blocks", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "storage_size", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = true},
    {.name = "compression_ratio", .bytes = 8, .type = TSDB_DATA_TYPE_DOUBLE, .sysInfo = true},
    {.name = "rows_in_mem", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "seek_header_time", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
};

static const SSysDbTableSchema userUsersSchema[] = {
    {.name = "name", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "super", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "enable", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "sysinfo", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "allowed_host", .bytes = TSDB_PRIVILEDGE_HOST_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

GRANTS_SCHEMA;

static const SSysDbTableSchema vgroupsSchema[] = {
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "tables", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "v1_dnode", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "v1_status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "v2_dnode", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "v2_status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "v3_dnode", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "v3_status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "v4_dnode", .bytes = 2, .type = TSDB_DATA_TYPE_SMALLINT, .sysInfo = true},
    {.name = "v4_status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "cacheload", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "cacheelements", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "tsma", .bytes = 1, .type = TSDB_DATA_TYPE_TINYINT, .sysInfo = true},
    // {.name = "compact_start_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
};

static const SSysDbTableSchema smaSchema[] = {
    {.name = "sma_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "stable_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
};

static const SSysDbTableSchema transSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "stage", .bytes = TSDB_TRANS_STAGE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "oper", .bytes = TSDB_TRANS_OPER_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "stable", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "failed_times", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "last_exec_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "last_action_info", .bytes = (TSDB_TRANS_ERROR_LEN - 1) + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR},
};

static const SSysDbTableSchema configSchema[] = {
    {.name = "name", .bytes = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "value", .bytes = TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema variablesSchema[] = {
    {.name = "dnode_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "name", .bytes = TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "value", .bytes = TSDB_CONFIG_VALUE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "scope", .bytes = TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysDbTableSchema topicSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "schema", .bytes = TSDB_MAX_BINARY_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "meta", .bytes = 4 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "type", .bytes = 8 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
};

static const SSysDbTableSchema subscriptionSchema[] = {
    {.name = "topic_name", .bytes = TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "consumer_group", .bytes = TSDB_CGROUP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "consumer_id", .bytes = 32, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "offset", .bytes = TSDB_OFFSET_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "rows", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
};

static const SSysDbTableSchema vnodesSchema[] = {
    {.name = "dnode_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = true},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = true},
    {.name = "status", .bytes = 9 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "role_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "start_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = true},
    {.name = "restored", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL, .sysInfo = true},
};

static const SSysDbTableSchema userUserPrivilegesSchema[] = {
    {.name = "user_name", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "privilege", .bytes = 10 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "db_name", .bytes = TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "table_name", .bytes = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "condition", .bytes = TSDB_PRIVILEDGE_CONDITION_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "notes", .bytes = 64 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysDbTableSchema userViewsSchema[] = {
    {.name = "view_name", .bytes = SYSTABLE_SCH_VIEW_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "effective_user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "type", .bytes = 128 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "query_sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "parameters", .bytes = 2048 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "default_values", .bytes = 2048 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "target_table", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
//    {.name = "column_list", .bytes = 2048 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema userCompactsSchema[] = {
    {.name = "compact_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "start_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
};

static const SSysDbTableSchema userCompactsDetailSchema[] = {
    {.name = "compact_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "dnode_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "number_fileset", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "finished", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "start_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
};

static const SSysDbTableSchema useGrantsFullSchema[] = {
    {.name = "grant_name", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "display_name", .bytes = 256 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "expire", .bytes = 32 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "limits", .bytes = 512 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysDbTableSchema useGrantsLogsSchema[] = {
    {.name = "state", .bytes = 1536 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "active", .bytes = 512 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "machine", .bytes = TSDB_GRANT_LOG_COL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysDbTableSchema useMachinesSchema[] = {
    {.name = "id", .bytes = TSDB_CLUSTER_ID_LEN + 1 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
    {.name = "machine", .bytes = 7552 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = true},
};

static const SSysTableMeta infosMeta[] = {
    {TSDB_INS_TABLE_DNODES, dnodesSchema, tListLen(dnodesSchema), true},
    {TSDB_INS_TABLE_MNODES, mnodesSchema, tListLen(mnodesSchema), true},
    // {TSDB_INS_TABLE_MODULES, modulesSchema, tListLen(modulesSchema), true},
    {TSDB_INS_TABLE_QNODES, qnodesSchema, tListLen(qnodesSchema), true},
    {TSDB_INS_TABLE_SNODES, snodesSchema, tListLen(snodesSchema), true},
    {TSDB_INS_TABLE_CLUSTER, clusterSchema, tListLen(clusterSchema), true},
    {TSDB_INS_TABLE_DATABASES, userDBSchema, tListLen(userDBSchema), false},
    {TSDB_INS_TABLE_FUNCTIONS, userFuncSchema, tListLen(userFuncSchema), false},
    {TSDB_INS_TABLE_INDEXES, userIdxSchema, tListLen(userIdxSchema), false},
    {TSDB_INS_TABLE_STABLES, userStbsSchema, tListLen(userStbsSchema), false},
    {TSDB_INS_TABLE_TABLES, userTblsSchema, tListLen(userTblsSchema), false},
    {TSDB_INS_TABLE_TAGS, userTagsSchema, tListLen(userTagsSchema), false},
    {TSDB_INS_TABLE_COLS, userColsSchema, tListLen(userColsSchema), false},
    // {TSDB_INS_TABLE_TABLE_DISTRIBUTED, userTblDistSchema, tListLen(userTblDistSchema)},
    {TSDB_INS_TABLE_USERS, userUsersSchema, tListLen(userUsersSchema), true},
    {TSDB_INS_TABLE_LICENCES, grantsSchema, tListLen(grantsSchema), true},
    {TSDB_INS_TABLE_VGROUPS, vgroupsSchema, tListLen(vgroupsSchema), true},
    {TSDB_INS_TABLE_CONFIGS, configSchema, tListLen(configSchema), false},
    {TSDB_INS_TABLE_DNODE_VARIABLES, variablesSchema, tListLen(variablesSchema), true},
    {TSDB_INS_TABLE_TOPICS, topicSchema, tListLen(topicSchema), false},
    {TSDB_INS_TABLE_SUBSCRIPTIONS, subscriptionSchema, tListLen(subscriptionSchema), false},
    {TSDB_INS_TABLE_STREAMS, streamSchema, tListLen(streamSchema), false},
    {TSDB_INS_TABLE_STREAM_TASKS, streamTaskSchema, tListLen(streamTaskSchema), false},
    {TSDB_INS_TABLE_VNODES, vnodesSchema, tListLen(vnodesSchema), true},
    {TSDB_INS_TABLE_USER_PRIVILEGES, userUserPrivilegesSchema, tListLen(userUserPrivilegesSchema), true},
    {TSDB_INS_TABLE_VIEWS, userViewsSchema, tListLen(userViewsSchema), false},
    {TSDB_INS_TABLE_COMPACTS, userCompactsSchema, tListLen(userCompactsSchema), false},
    {TSDB_INS_TABLE_COMPACT_DETAILS, userCompactsDetailSchema, tListLen(userCompactsDetailSchema), false},
    {TSDB_INS_TABLE_GRANTS_FULL, useGrantsFullSchema, tListLen(useGrantsFullSchema), true},
    {TSDB_INS_TABLE_GRANTS_LOGS, useGrantsLogsSchema, tListLen(useGrantsLogsSchema), true},
    {TSDB_INS_TABLE_MACHINES, useMachinesSchema, tListLen(useMachinesSchema), true},
};

static const SSysDbTableSchema connectionsSchema[] = {
    {.name = "conn_id", .bytes = 4, .type = TSDB_DATA_TYPE_UINT, .sysInfo = false},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "app", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_UINT, .sysInfo = false},
    {.name = "end_point", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "login_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "last_access", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
};


static const SSysDbTableSchema consumerSchema[] = {
    {.name = "consumer_id", .bytes = 32, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "consumer_group", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "client_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "status", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "topics", .bytes = TSDB_TOPIC_FNAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    /*{.name = "end_point", .bytes = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},*/
    {.name = "up_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "subscribe_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "rebalance_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "parameters", .bytes = 64 + TSDB_OFFSET_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
};

static const SSysDbTableSchema offsetSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY, .sysInfo = false},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "committed_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "current_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "skip_log_cnt", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
};

static const SSysDbTableSchema querySchema[] = {
    {.name = "kill_id", .bytes = TSDB_QUERY_ID_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "query_id", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "conn_id", .bytes = 4, .type = TSDB_DATA_TYPE_UINT, .sysInfo = false},
    {.name = "app", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "end_point", .bytes = TSDB_IPv4ADDR_LEN + 6 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "exec_usec", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT, .sysInfo = false},
    {.name = "stable_query", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL, .sysInfo = false},
    {.name = "sub_query", .bytes = 1, .type = TSDB_DATA_TYPE_BOOL, .sysInfo = false},
    {.name = "sub_num", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "sub_status", .bytes = TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};

static const SSysDbTableSchema appSchema[] = {
    {.name = "app_id", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "ip", .bytes = TSDB_IPv4ADDR_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "name", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "start_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
    {.name = "insert_req", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "insert_row", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "insert_time", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "insert_bytes", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "fetch_bytes", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "query_time", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "slow_query", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "total_req", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "current_req", .bytes = 8, .type = TSDB_DATA_TYPE_UBIGINT, .sysInfo = false},
    {.name = "last_access", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP, .sysInfo = false},
};

static const SSysTableMeta perfsMeta[] = {
    {TSDB_PERFS_TABLE_CONNECTIONS, connectionsSchema, tListLen(connectionsSchema), false},
    {TSDB_PERFS_TABLE_QUERIES, querySchema, tListLen(querySchema), false},
    {TSDB_PERFS_TABLE_CONSUMERS, consumerSchema, tListLen(consumerSchema), false},
    // {TSDB_PERFS_TABLE_OFFSETS, offsetSchema, tListLen(offsetSchema)},
    {TSDB_PERFS_TABLE_TRANS, transSchema, tListLen(transSchema), false},
    // {TSDB_PERFS_TABLE_SMAS, smaSchema, tListLen(smaSchema), false},
    {TSDB_PERFS_TABLE_APPS, appSchema, tListLen(appSchema), false}};
// clang-format on

void getInfosDbMeta(const SSysTableMeta** pInfosTableMeta, size_t* size) {
  if (pInfosTableMeta) {
    *pInfosTableMeta = infosMeta;
  }
  if (size) {
    *size = tListLen(infosMeta);
  }
}

void getPerfDbMeta(const SSysTableMeta** pPerfsTableMeta, size_t* size) {
  if (pPerfsTableMeta) {
    *pPerfsTableMeta = perfsMeta;
  }
  if (size) {
    *size = tListLen(perfsMeta);
  }
}

void getVisibleInfosTablesNum(bool sysInfo, size_t* size) {
  if (sysInfo) {
    getInfosDbMeta(NULL, size);
    return;
  }
  *size = 0;
  const SSysTableMeta* pMeta = NULL;
  size_t               totalNum = 0;
  getInfosDbMeta(&pMeta, &totalNum);
  for (size_t i = 0; i < totalNum; ++i) {
    if (!pMeta[i].sysInfo) {
      ++(*size);
    }
  }
}

bool invisibleColumn(bool sysInfo, int8_t tableType, int8_t flags) {
  if (sysInfo || TSDB_SYSTEM_TABLE != tableType) {
    return false;
  }
  return 0 != (flags & COL_IS_SYSINFO);
}
