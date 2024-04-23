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

static int32_t mndInitInfosTableSchema(const SSysDbTableSchema *pSrc, int32_t colNum, SSchema **pDst) {
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
    if (pSrc[i].sysInfo) {
      schema[i].flags |= COL_IS_SYSINFO;
    }
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

  size_t               size = 0;
  const SSysTableMeta *pInfosTableMeta = NULL;
  getInfosDbMeta(&pInfosTableMeta, &size);

  for (int32_t i = 0; i < size; ++i) {
    tstrncpy(meta.tbName, pInfosTableMeta[i].name, sizeof(meta.tbName));
    meta.numOfColumns = pInfosTableMeta[i].colNum;
    meta.sysInfo = pInfosTableMeta[i].sysInfo;

    if (mndInitInfosTableSchema(pInfosTableMeta[i].schema, pInfosTableMeta[i].colNum, &meta.pSchemas)) {
      return -1;
    }

    if (taosHashPut(hash, meta.tbName, strlen(meta.tbName), &meta, sizeof(meta))) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

int32_t mndBuildInsTableSchema(SMnode *pMnode, const char *dbFName, const char *tbName, bool sysinfo,
                               STableMetaRsp *pRsp) {
  if (NULL == pMnode->infosMeta) {
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  STableMetaRsp *pMeta = taosHashGet(pMnode->infosMeta, tbName, strlen(tbName));
  if (NULL == pMeta) {
    mError("invalid information schema table name:%s", tbName);
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    return -1;
  }

  if (!sysinfo && pMeta->sysInfo) {
    mError("no permission to get schema of table name:%s", tbName);
    terrno = TSDB_CODE_PAR_PERMISSION_DENIED;
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

int32_t mndBuildInsTableCfg(SMnode *pMnode, const char *dbFName, const char *tbName, STableCfgRsp *pRsp) {
  if (NULL == pMnode->infosMeta) {
    terrno = TSDB_CODE_APP_ERROR;
    return -1;
  }

  STableMetaRsp *pMeta = taosHashGet(pMnode->infosMeta, tbName, strlen(tbName));
  if (NULL == pMeta) {
    mError("invalid information schema table name:%s", tbName);
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
