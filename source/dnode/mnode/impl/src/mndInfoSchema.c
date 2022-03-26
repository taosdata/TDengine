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

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

static const SInfosTableSchema dnodesSchema[] = {{.name = "id",             .bytes = 2,   .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "endpoint",       .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "vnodes",         .bytes = 2,   .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "max_vnodes",     .bytes = 2,   .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "status",         .bytes = 10 + VARSTR_HEADER_SIZE,  .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "create_time",    .bytes = 8,   .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                 {.name = "note",           .bytes = 256 + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
                                                };
static const SInfosTableSchema mnodesSchema[] = {{.name = "id",             .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "endpoint",       .bytes = TSDB_EP_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "role",           .bytes = 12 + VARSTR_HEADER_SIZE,  .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "role_time",      .bytes = 8,   .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                 {.name = "create_time",    .bytes = 8,   .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                };
static const SInfosTableSchema modulesSchema[] = {{.name = "id",             .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "endpoint",        .bytes = 134, .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "module",          .bytes = 10,  .type = TSDB_DATA_TYPE_BINARY},
                                                };
static const SInfosTableSchema qnodesSchema[] = {{.name = "id",             .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "endpoint",       .bytes = 134, .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "create_time",    .bytes = 8,   .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                };
static const SInfosTableSchema userDBSchema[] = {{.name = "name",             .bytes = SYSTABLE_SCH_DB_NAME_LEN,   .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "create_time",      .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                 {.name = "vgroups",          .bytes = 2,    .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "ntables",          .bytes = 8,    .type = TSDB_DATA_TYPE_BIGINT},
                                                 {.name = "replica",          .bytes = 2,    .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "quorum",           .bytes = 2,    .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "days",             .bytes = 2,    .type = TSDB_DATA_TYPE_SMALLINT},
                                                 {.name = "keep",             .bytes = 24 + VARSTR_HEADER_SIZE,    .type = TSDB_DATA_TYPE_BINARY},
                                                 {.name = "cache",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "blocks",           .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "minrows",          .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "maxrows",          .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "wallevel",         .bytes = 1,    .type = TSDB_DATA_TYPE_TINYINT},
                                                 {.name = "fsync",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                 {.name = "comp",             .bytes = 1,    .type = TSDB_DATA_TYPE_TINYINT},
                                                 {.name = "cachelast",        .bytes = 1,    .type = TSDB_DATA_TYPE_TINYINT},
                                                 {.name = "precision",        .bytes = 3 + VARSTR_HEADER_SIZE,    .type = TSDB_DATA_TYPE_BINARY},
//                                                 {.name = "update",           .bytes = 1,   .type = TSDB_DATA_TYPE_TINYINT},  // disable update
                                                };
static const SInfosTableSchema userFuncSchema[] = {{.name = "name",           .bytes = 32,   .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "create_time",    .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                   {.name = "ntables",        .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "precision",      .bytes = 2,    .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "status",         .bytes = 10,   .type = TSDB_DATA_TYPE_BINARY},
                                                  };
static const SInfosTableSchema userIdxSchema[] = {{.name = "db_name",          .bytes = 32,   .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "table_name",       .bytes = SYSTABLE_SCH_DB_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "index_database",   .bytes = 32,   .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "index_name",       .bytes = SYSTABLE_SCH_DB_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "column_name",      .bytes = 64,   .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "index_type",       .bytes = 10,   .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "index_extensions", .bytes = 256,  .type = TSDB_DATA_TYPE_BINARY},
                                                 };
static const SInfosTableSchema userStbsSchema[] = {{.name = "stable_name",     .bytes = SYSTABLE_SCH_TABLE_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "db_name",         .bytes = SYSTABLE_SCH_DB_NAME_LEN,   .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "create_time",     .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                   {.name = "columns",         .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "tags",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "tables",          .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "last_update",     .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                   {.name = "table_comment",   .bytes = 1024 + VARSTR_HEADER_SIZE,    .type = TSDB_DATA_TYPE_INT},
                                                  };
static const SInfosTableSchema userStreamsSchema[] = {{.name = "stream_name",  .bytes = SYSTABLE_SCH_DB_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "user_name",    .bytes = 23,   .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "dest_table",   .bytes = SYSTABLE_SCH_DB_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "create_time",  .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                      {.name = "sql",          .bytes = 1024, .type = TSDB_DATA_TYPE_BINARY},
                                                     };
static const SInfosTableSchema userTblsSchema[] = {
                                                   {.name = "table_name",      .bytes = SYSTABLE_SCH_TABLE_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "db_name",         .bytes = SYSTABLE_SCH_DB_NAME_LEN,   .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "create_time",     .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                   {.name = "columns",         .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "stable_name",     .bytes = SYSTABLE_SCH_TABLE_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                   {.name = "uid",             .bytes = 8,    .type = TSDB_DATA_TYPE_BIGINT},
                                                   {.name = "vgroup_id",       .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "ttl",             .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                   {.name = "table_comment",    .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                  };
static const SInfosTableSchema userTblDistSchema[] = {{.name = "db_name",                .bytes = 32,   .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "table_name",             .bytes = SYSTABLE_SCH_DB_NAME_LEN,  .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "distributed_histogram",  .bytes = 500,  .type = TSDB_DATA_TYPE_BINARY},
                                                      {.name = "min_of_rows",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "max_of_rows",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "avg_of_rows",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "stddev_of_rows",         .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "rows",                   .bytes = 8,    .type = TSDB_DATA_TYPE_BIGINT},
                                                      {.name = "blocks",                 .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "storage_size",           .bytes = 8,    .type = TSDB_DATA_TYPE_BIGINT},
                                                      {.name = "compression_ratio",      .bytes = 8,    .type = TSDB_DATA_TYPE_DOUBLE},
                                                      {.name = "rows_in_mem",            .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                      {.name = "seek_header_time",       .bytes = 4,    .type = TSDB_DATA_TYPE_INT},
                                                     };
static const SInfosTableSchema userUsersSchema[] = {{.name = "name",           .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE,   .type = TSDB_DATA_TYPE_BINARY},
                                                    {.name = "privilege",      .bytes = 10 + VARSTR_HEADER_SIZE,  .type = TSDB_DATA_TYPE_BINARY},
                                                    {.name = "create_time",    .bytes = 8,    .type = TSDB_DATA_TYPE_TIMESTAMP},
                                                    {.name = "account",        .bytes = TSDB_USER_LEN + VARSTR_HEADER_SIZE,    .type = TSDB_DATA_TYPE_BINARY},
                                                   };
static const SInfosTableSchema vgroupsSchema[] = {{.name = "vgroup_id",        .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "db_name",          .bytes = SYSTABLE_SCH_DB_NAME_LEN,   .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "tables",           .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "status",           .bytes = 12 + VARSTR_HEADER_SIZE,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "onlines",          .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "v1_dnode",         .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "v1_status",        .bytes = 10,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "v2_dnode",         .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "v2_status",        .bytes = 10,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "v3_dnode",         .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "v3_status",        .bytes = 10,  .type = TSDB_DATA_TYPE_BINARY},
                                                  {.name = "compacting",       .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "nfiles",           .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                  {.name = "file_size",        .bytes = 4,   .type = TSDB_DATA_TYPE_INT},
                                                 };

static const SInfosTableMeta infosMeta[] = {{TSDB_INS_TABLE_DNODES, dnodesSchema, tListLen(dnodesSchema)},
                                            {TSDB_INS_TABLE_MNODES, mnodesSchema, tListLen(mnodesSchema)},
                                            {TSDB_INS_TABLE_MODULES, modulesSchema, tListLen(modulesSchema)},
                                            {TSDB_INS_TABLE_QNODES, qnodesSchema, tListLen(qnodesSchema)},
                                            {TSDB_INS_TABLE_USER_DATABASES, userDBSchema, tListLen(userDBSchema)},
                                            {TSDB_INS_TABLE_USER_FUNCTIONS, userFuncSchema, tListLen(userFuncSchema)},
                                            {TSDB_INS_TABLE_USER_INDEXES, userIdxSchema, tListLen(userIdxSchema)},
                                            {TSDB_INS_TABLE_USER_STABLES, userStbsSchema, tListLen(userStbsSchema)},
                                            {TSDB_INS_TABLE_USER_STREAMS, userStreamsSchema, tListLen(userStreamsSchema)},
                                            {TSDB_INS_TABLE_USER_TABLES, userTblsSchema, tListLen(userTblsSchema)},
                                            {TSDB_INS_TABLE_USER_TABLE_DISTRIBUTED, userTblDistSchema, tListLen(userTblDistSchema)},
                                            {TSDB_INS_TABLE_USER_USERS, userUsersSchema, tListLen(userUsersSchema)},
                                            {TSDB_INS_TABLE_VGROUPS, vgroupsSchema, tListLen(vgroupsSchema)},
                                 };

//connection/application/
int32_t mndInitInfosTableSchema(const SInfosTableSchema *pSrc, int32_t colNum, SSchema **pDst) {
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

int32_t mndInsInitMeta(SHashObj *hash) {
  STableMetaRsp meta = {0};

  strcpy(meta.dbFName, TSDB_INFORMATION_SCHEMA_DB);
  meta.tableType = TSDB_SYSTEM_TABLE;
  meta.sversion = 1;
  meta.tversion = 1;

  for (int32_t i = 0; i < tListLen(infosMeta); ++i) {
    strcpy(meta.tbName, infosMeta[i].name);
    meta.numOfColumns = infosMeta[i].colNum;
    
    if (mndInitInfosTableSchema(infosMeta[i].schema, infosMeta[i].colNum, &meta.pSchemas)) {
      return -1;
    }
    
    if (taosHashPut(hash, meta.tbName, strlen(meta.tbName), &meta, sizeof(meta))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mndBuildInsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  if (NULL == pMnode->infosMeta) {
    terrno = TSDB_CODE_MND_NOT_READY;
    return -1;
  }

  STableMetaRsp *meta = (STableMetaRsp *)taosHashGet(pMnode->infosMeta, tbName, strlen(tbName));
  if (NULL == meta) {
    mError("invalid information schema table name:%s", tbName);
    terrno = TSDB_CODE_MND_INVALID_INFOS_TBL;
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

int32_t mndInitInfos(SMnode *pMnode) {
  pMnode->infosMeta = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
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
  
  void *pIter = taosHashIterate(pMnode->infosMeta, NULL);
  while (pIter) {
    STableMetaRsp *meta = (STableMetaRsp *)pIter;

    taosMemoryFreeClear(meta->pSchemas);
    
    pIter = taosHashIterate(pMnode->infosMeta, pIter);
  }

  taosHashCleanup(pMnode->infosMeta);
  pMnode->infosMeta = NULL;
}



