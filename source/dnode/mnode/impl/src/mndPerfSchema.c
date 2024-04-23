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
#include "mndInt.h"
#include "systable.h"

// connection/application/
int32_t mndInitPerfsTableSchema(const SSysDbTableSchema *pSrc, int32_t colNum, SSchema **pDst) {
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
  return TSDB_CODE_SUCCESS;
}

int32_t mndPerfsInitMeta(SHashObj *hash) {
  STableMetaRsp meta = {0};

  tstrncpy(meta.dbFName, TSDB_INFORMATION_SCHEMA_DB, sizeof(meta.dbFName));
  meta.tableType = TSDB_SYSTEM_TABLE;
  meta.sversion = 1;
  meta.tversion = 1;

  size_t               size = 0;
  const SSysTableMeta *pSysDbTableMeta = NULL;
  getPerfDbMeta(&pSysDbTableMeta, &size);

  for (int32_t i = 0; i < size; ++i) {
    tstrncpy(meta.tbName, pSysDbTableMeta[i].name, sizeof(meta.tbName));
    meta.numOfColumns = pSysDbTableMeta[i].colNum;

    if (mndInitPerfsTableSchema(pSysDbTableMeta[i].schema, pSysDbTableMeta[i].colNum, &meta.pSchemas)) {
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
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  STableMetaRsp *meta = (STableMetaRsp *)taosHashGet(pMnode->perfsMeta, tbName, strlen(tbName));
  if (NULL == meta) {
    mError("invalid performance schema table name:%s", tbName);
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
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

int32_t mndBuildPerfsTableCfg(SMnode *pMnode, const char *dbFName, const char *tbName, STableCfgRsp *pRsp) {
  if (NULL == pMnode->perfsMeta) {
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  STableMetaRsp *pMeta = taosHashGet(pMnode->perfsMeta, tbName, strlen(tbName));
  if (NULL == pMeta) {
    mError("invalid performance schema table name:%s", tbName);
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  strcpy(pRsp->tbName, pMeta->tbName);
  strcpy(pRsp->stbName, pMeta->stbName);
  strcpy(pRsp->dbFName, pMeta->dbFName);
  pRsp->numOfTags = pMeta->numOfTags;
  pRsp->numOfColumns = pMeta->numOfColumns;
  pRsp->tableType = pMeta->tableType;

  pRsp->pSchemas = taosMemoryCalloc(pMeta->numOfColumns, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    pRsp->pSchemas = NULL;
    return -1;
  }

  memcpy(pRsp->pSchemas, pMeta->pSchemas, pMeta->numOfColumns * sizeof(SSchema));
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
