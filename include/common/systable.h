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

#ifndef TDENGINE_SYSTABLE_H
#define TDENGINE_SYSTABLE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#define TSDB_INFORMATION_SCHEMA_DB       "information_schema"
#define TSDB_INS_TABLE_DNODES            "ins_dnodes"
#define TSDB_INS_TABLE_MNODES            "ins_mnodes"
#define TSDB_INS_TABLE_MODULES           "ins_modules"
#define TSDB_INS_TABLE_QNODES            "ins_qnodes"
#define TSDB_INS_TABLE_BNODES            "ins_bnodes"  // no longer used
#define TSDB_INS_TABLE_SNODES            "ins_snodes"
#define TSDB_INS_TABLE_CLUSTER           "ins_cluster"
#define TSDB_INS_TABLE_DATABASES         "ins_databases"
#define TSDB_INS_TABLE_FUNCTIONS         "ins_functions"
#define TSDB_INS_TABLE_INDEXES           "ins_indexes"
#define TSDB_INS_TABLE_STABLES           "ins_stables"
#define TSDB_INS_TABLE_TABLES            "ins_tables"
#define TSDB_INS_TABLE_TAGS              "ins_tags"
#define TSDB_INS_TABLE_COLS              "ins_columns"
#define TSDB_INS_TABLE_TABLE_DISTRIBUTED "ins_table_distributed"
#define TSDB_INS_TABLE_USERS             "ins_users"
#define TSDB_INS_TABLE_LICENCES          "ins_grants"
#define TSDB_INS_TABLE_VGROUPS           "ins_vgroups"
#define TSDB_INS_TABLE_VNODES            "ins_vnodes"
#define TSDB_INS_TABLE_CONFIGS           "ins_configs"
#define TSDB_INS_TABLE_DNODE_VARIABLES   "ins_dnode_variables"
#define TSDB_INS_TABLE_SUBSCRIPTIONS     "ins_subscriptions"
#define TSDB_INS_TABLE_TOPICS            "ins_topics"
#define TSDB_INS_TABLE_STREAMS           "ins_streams"
#define TSDB_INS_TABLE_STREAM_TASKS      "ins_stream_tasks"
#define TSDB_INS_TABLE_USER_PRIVILEGES   "ins_user_privileges"
#define TSDB_INS_TABLE_VIEWS             "ins_views"
#define TSDB_INS_TABLE_COMPACTS          "ins_compacts"
#define TSDB_INS_TABLE_COMPACT_DETAILS   "ins_compact_details"
#define TSDB_INS_TABLE_GRANTS_FULL       "ins_grants_full"
#define TSDB_INS_TABLE_GRANTS_LOGS       "ins_grants_logs"
#define TSDB_INS_TABLE_MACHINES          "ins_machines"

#define TSDB_PERFORMANCE_SCHEMA_DB   "performance_schema"
#define TSDB_PERFS_TABLE_SMAS        "perf_smas"
#define TSDB_PERFS_TABLE_CONNECTIONS "perf_connections"
#define TSDB_PERFS_TABLE_QUERIES     "perf_queries"
#define TSDB_PERFS_TABLE_CONSUMERS   "perf_consumers"
#define TSDB_PERFS_TABLE_OFFSETS     "perf_offsets"
#define TSDB_PERFS_TABLE_TRANS       "perf_trans"
#define TSDB_PERFS_TABLE_APPS        "perf_apps"

#define TSDB_AUDIT_DB                "audit"
#define TSDB_AUDIT_STB_OPERATION     "operations"
#define TSDB_AUDIT_CTB_OPERATION     "t_operations_"
#define TSDB_AUDIT_CTB_OPERATION_LEN 13

typedef struct SSysDbTableSchema {
  const char*   name;
  const int32_t type;
  const int32_t bytes;
  const bool    sysInfo;
} SSysDbTableSchema;

typedef struct SSysTableMeta {
  const char*              name;
  const SSysDbTableSchema* schema;
  const int32_t            colNum;
  const bool               sysInfo;
} SSysTableMeta;

void getInfosDbMeta(const SSysTableMeta** pInfosTableMeta, size_t* size);
void getPerfDbMeta(const SSysTableMeta** pPerfsTableMeta, size_t* size);
void getVisibleInfosTablesNum(bool sysInfo, size_t* size);
bool invisibleColumn(bool sysInfo, int8_t tableType, int8_t flags);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SYSTABLE_H
