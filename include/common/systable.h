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

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#ifndef TDENGINE_SYSTABLE_H
#define TDENGINE_SYSTABLE_H

#define TSDB_INFORMATION_SCHEMA_DB            "information_schema"
#define TSDB_INS_TABLE_DNODES                 "dnodes"
#define TSDB_INS_TABLE_MNODES                 "mnodes"
#define TSDB_INS_TABLE_MODULES                "modules"
#define TSDB_INS_TABLE_QNODES                 "qnodes"
#define TSDB_INS_TABLE_BNODES                 "bnodes"
#define TSDB_INS_TABLE_SNODES                 "snodes"
#define TSDB_INS_TABLE_CLUSTER                "cluster"
#define TSDB_INS_TABLE_USER_DATABASES         "user_databases"
#define TSDB_INS_TABLE_USER_FUNCTIONS         "user_functions"
#define TSDB_INS_TABLE_USER_INDEXES           "user_indexes"
#define TSDB_INS_TABLE_USER_STABLES           "user_stables"
#define TSDB_INS_TABLE_USER_TABLES            "user_tables"
#define TSDB_INS_TABLE_USER_TABLE_DISTRIBUTED "user_table_distributed"
#define TSDB_INS_TABLE_USER_USERS             "user_users"
#define TSDB_INS_TABLE_LICENCES               "grants"
#define TSDB_INS_TABLE_VGROUPS                "vgroups"
#define TSDB_INS_TABLE_VNODES                 "vnodes"
#define TSDB_INS_TABLE_CONFIGS                "configs"

#define TSDB_PERFORMANCE_SCHEMA_DB     "performance_schema"
#define TSDB_PERFS_TABLE_SMAS          "smas"
#define TSDB_PERFS_TABLE_CONNECTIONS   "connections"
#define TSDB_PERFS_TABLE_QUERIES       "queries"
#define TSDB_PERFS_TABLE_TOPICS        "topics"
#define TSDB_PERFS_TABLE_CONSUMERS     "consumers"
#define TSDB_PERFS_TABLE_SUBSCRIPTIONS "subscriptions"
#define TSDB_PERFS_TABLE_OFFSETS       "offsets"
#define TSDB_PERFS_TABLE_TRANS         "trans"
#define TSDB_PERFS_TABLE_STREAMS       "streams"

typedef struct SSysDbTableSchema {
  const char*   name;
  const int32_t type;
  const int32_t bytes;
} SSysDbTableSchema;

typedef struct SSysTableMeta {
  const char*              name;
  const SSysDbTableSchema* schema;
  const int32_t            colNum;
} SSysTableMeta;

void getInfosDbMeta(const SSysTableMeta** pInfosTableMeta, size_t* size);
void getPerfDbMeta(const SSysTableMeta** pPerfsTableMeta, size_t* size);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SYSTABLE_H
