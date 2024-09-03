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
  int32_t  code = 0;
  SSchema *schema = taosMemoryCalloc(colNum, sizeof(SSchema));
  if (NULL == schema) {
    code = terrno;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < colNum; ++i) {
    tstrncpy(schema[i].name, pSrc[i].name, sizeof(schema[i].name));

    schema[i].type = pSrc[i].type;
    schema[i].colId = i + 1;
    schema[i].bytes = pSrc[i].bytes;
  }

  *pDst = schema;
  TAOS_RETURN(code);
}

int32_t mndPerfsInitMeta(SHashObj *hash) {
  int32_t       code = 0;
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

    TAOS_CHECK_RETURN(mndInitPerfsTableSchema(pSysDbTableMeta[i].schema, pSysDbTableMeta[i].colNum, &meta.pSchemas));

    if (taosHashPut(hash, meta.tbName, strlen(meta.tbName), &meta, sizeof(meta))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      TAOS_RETURN(code);
    }
  }

  TAOS_RETURN(code);
}

int32_t mndBuildPerfsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  int32_t code = 0;
  if (NULL == pMnode->perfsMeta) {
    code = TSDB_CODE_APP_ERROR;
    TAOS_RETURN(code);
  }

  STableMetaRsp *meta = (STableMetaRsp *)taosHashGet(pMnode->perfsMeta, tbName, strlen(tbName));
  if (NULL == meta) {
    mError("invalid performance schema table name:%s", tbName);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }

  *pRsp = *meta;

  pRsp->pSchemas = taosMemoryCalloc(meta->numOfColumns, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    code = terrno;
    pRsp->pSchemas = NULL;
    TAOS_RETURN(code);
  }

  memcpy(pRsp->pSchemas, meta->pSchemas, meta->numOfColumns * sizeof(SSchema));
  TAOS_RETURN(code);
}

int32_t mndBuildPerfsTableCfg(SMnode *pMnode, const char *dbFName, const char *tbName, STableCfgRsp *pRsp) {
  int32_t code = 0;
  if (NULL == pMnode->perfsMeta) {
    code = TSDB_CODE_APP_ERROR;
    TAOS_RETURN(code);
  }

  STableMetaRsp *pMeta = taosHashGet(pMnode->perfsMeta, tbName, strlen(tbName));
  if (NULL == pMeta) {
    mError("invalid performance schema table name:%s", tbName);
    code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    TAOS_RETURN(code);
  }

  strcpy(pRsp->tbName, pMeta->tbName);
  strcpy(pRsp->stbName, pMeta->stbName);
  strcpy(pRsp->dbFName, pMeta->dbFName);
  pRsp->numOfTags = pMeta->numOfTags;
  pRsp->numOfColumns = pMeta->numOfColumns;
  pRsp->tableType = pMeta->tableType;

  pRsp->pSchemas = taosMemoryCalloc(pMeta->numOfColumns, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    code = terrno;
    pRsp->pSchemas = NULL;
    TAOS_RETURN(code);
  }

  memcpy(pRsp->pSchemas, pMeta->pSchemas, pMeta->numOfColumns * sizeof(SSchema));
  TAOS_RETURN(code);
}

int32_t mndInitPerfs(SMnode *pMnode) {
  int32_t code = 0;
  pMnode->perfsMeta = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  if (pMnode->perfsMeta == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_RETURN(code);
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
