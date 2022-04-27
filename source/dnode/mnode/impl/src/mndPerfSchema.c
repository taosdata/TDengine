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
#include "mndPerfSchema.h"
#include "mndInt.h"

//!!!! Note: only APPEND columns in below tables, NO insert !!!!
static const SPerfsTableSchema connectionsSchema[] = {
    {.name = "conn_id", .bytes = 4, .type = TSDB_DATA_TYPE_UINT},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "program", .bytes = TSDB_APP_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "end_point", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "login_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "last_access", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};
static const SPerfsTableSchema queriesSchema[] = {
    {.name = "query_id", .bytes = 4, .type = TSDB_DATA_TYPE_UBIGINT},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "user", .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "fqdn", .bytes = TSDB_FQDN_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "exec_time", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sub_queries", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "sub_query_info", .bytes = TSDB_SHOW_SUBQUERY_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
};

static const SPerfsTableSchema topicSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "db_name", .bytes = SYSTABLE_SCH_DB_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "create_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "sql", .bytes = TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    // TODO config
};

static const SPerfsTableSchema consumerSchema[] = {
    {.name = "consumer_id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "app_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "status", .bytes = 20 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "topics", .bytes = TSDB_SHOW_LIST_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "pid", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "end_point", .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "up_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "subscribe_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
    {.name = "rebalance_time", .bytes = 8, .type = TSDB_DATA_TYPE_TIMESTAMP},
};

static const SPerfsTableSchema subscriptionSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "consumer_id", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
};

static const SPerfsTableSchema offsetSchema[] = {
    {.name = "topic_name", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "group_id", .bytes = SYSTABLE_SCH_TABLE_NAME_LEN, .type = TSDB_DATA_TYPE_BINARY},
    {.name = "vgroup_id", .bytes = 4, .type = TSDB_DATA_TYPE_INT},
    {.name = "committed_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "current_offset", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
    {.name = "skip_log_cnt", .bytes = 8, .type = TSDB_DATA_TYPE_BIGINT},
};

static const SPerfsTableMeta perfsMeta[] = {
    {TSDB_PERFS_TABLE_CONNECTIONS, connectionsSchema, tListLen(connectionsSchema)},
    {TSDB_PERFS_TABLE_QUERIES, queriesSchema, tListLen(queriesSchema)},
    {TSDB_PERFS_TABLE_TOPICS, topicSchema, tListLen(topicSchema)},
    {TSDB_PERFS_TABLE_CONSUMERS, consumerSchema, tListLen(consumerSchema)},
    {TSDB_PERFS_TABLE_SUBSCRIPTIONS, subscriptionSchema, tListLen(subscriptionSchema)},
    {TSDB_PERFS_TABLE_OFFSETS, offsetSchema, tListLen(offsetSchema)},
};

// connection/application/
int32_t mndInitPerfsTableSchema(const SPerfsTableSchema *pSrc, int32_t colNum, SSchema **pDst) {
  SSchema *schema = taosMemoryCalloc(colNum, sizeof(SSchema));
  if (NULL == schema) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < colNum; ++i) {
    strcpy(schema[i].name, pSrc[i].name);

    schema[i].type = pSrc[i].type;
    schema[i].colId = i + 1;
    schema[i].bytes = pSrc[i].bytes;
  }

  *pDst = schema;
  return TSDB_CODE_SUCCESS;
}

int32_t mndPerfsInitMeta(SHashObj *hash) {
  STableMetaRsp meta = {0};

  strcpy(meta.dbFName, TSDB_INFORMATION_SCHEMA_DB);
  meta.tableType = TSDB_SYSTEM_TABLE;
  meta.sversion = 1;
  meta.tversion = 1;

  for (int32_t i = 0; i < tListLen(perfsMeta); ++i) {
    strcpy(meta.tbName, perfsMeta[i].name);
    meta.numOfColumns = perfsMeta[i].colNum;

    if (mndInitPerfsTableSchema(perfsMeta[i].schema, perfsMeta[i].colNum, &meta.pSchemas)) {
      return -1;
    }

    if (taosHashPut(hash, meta.tbName, strlen(meta.tbName), &meta, sizeof(meta))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mndBuildPerfsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  if (NULL == pMnode->perfsMeta) {
    terrno = TSDB_CODE_MND_NOT_READY;
    return -1;
  }

  STableMetaRsp *meta = (STableMetaRsp *)taosHashGet(pMnode->perfsMeta, tbName, strlen(tbName));
  if (NULL == meta) {
    mError("invalid performance schema table name:%s", tbName);
    terrno = TSDB_CODE_MND_INVALID_SYS_TABLENAME;
    return -1;
  }

  *pRsp = *meta;

  pRsp->pSchemas = taosMemoryCalloc(meta->numOfColumns, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    pRsp->pSchemas = NULL;
    return -1;
  }

  memcpy(pRsp->pSchemas, meta->pSchemas, meta->numOfColumns * sizeof(SSchema));

  return 0;
}

int32_t mndInitPerfs(SMnode *pMnode) {
  pMnode->perfsMeta = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pMnode->perfsMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return mndPerfsInitMeta(pMnode->perfsMeta);
}

void mndCleanupPerfs(SMnode *pMnode) {
  if (NULL == pMnode->perfsMeta) {
    return;
  }

  void *pIter = taosHashIterate(pMnode->perfsMeta, NULL);
  while (pIter) {
    STableMetaRsp *meta = (STableMetaRsp *)pIter;

    taosMemoryFreeClear(meta->pSchemas);

    pIter = taosHashIterate(pMnode->perfsMeta, pIter);
  }

  taosHashCleanup(pMnode->perfsMeta);
  pMnode->perfsMeta = NULL;
}

