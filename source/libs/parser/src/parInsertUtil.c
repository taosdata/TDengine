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

#include "parInsertUtil.h"

#include "catalog.h"
#include "parInt.h"
#include "parUtil.h"
#include "querynodes.h"
#include "tRealloc.h"
#include "tdatablock.h"

void qDestroyBoundColInfo(void* pInfo) {
  if (NULL == pInfo) {
    return;
  }

  SBoundColInfo* pBoundInfo = (SBoundColInfo*)pInfo;

  taosMemoryFreeClear(pBoundInfo->pColIndex);
}

static char* tableNameGetPosition(SToken* pToken, char target) {
  bool inEscape = false;
  bool inQuote = false;
  char quotaStr = 0;

  for (uint32_t i = 0; i < pToken->n; ++i) {
    if (*(pToken->z + i) == target && (!inEscape) && (!inQuote)) {
      return pToken->z + i;
    }

    if (*(pToken->z + i) == TS_ESCAPE_CHAR) {
      if (!inQuote) {
        inEscape = !inEscape;
      }
    }

    if (*(pToken->z + i) == '\'' || *(pToken->z + i) == '"') {
      if (!inEscape) {
        if (!inQuote) {
          quotaStr = *(pToken->z + i);
          inQuote = !inQuote;
        } else if (quotaStr == *(pToken->z + i)) {
          inQuote = !inQuote;
        }
      }
    }
  }

  return NULL;
}

int32_t insCreateSName(SName* pName, SToken* pTableName, int32_t acctId, const char* dbName, SMsgBuf* pMsgBuf) {
  const char* msg1 = "name too long";
  const char* msg2 = "invalid database name";
  const char* msg3 = "db is not specified";
  const char* msg4 = "invalid table name";

  int32_t code = TSDB_CODE_SUCCESS;
  char*   p = tableNameGetPosition(pTableName, TS_PATH_DELIMITER[0]);

  if (p != NULL) {  // db has been specified in sql string so we ignore current db path
    assert(*p == TS_PATH_DELIMITER[0]);

    int32_t dbLen = p - pTableName->z;
    if (dbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg2);
    }
    char name[TSDB_DB_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, dbLen);
    int32_t actualDbLen = strdequote(name);

    code = tNameSetDbName(pName, acctId, name, actualDbLen);
    if (code != TSDB_CODE_SUCCESS) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    int32_t tbLen = pTableName->n - dbLen - 1;
    if (tbLen <= 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg4);
    }

    char tbname[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(tbname, p + 1, tbLen);
    /*tbLen = */ strdequote(tbname);

    code = tNameFromString(pName, tbname, T_NAME_TABLE);
    if (code != 0) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  } else {  // get current DB name first, and then set it into path
    if (pTableName->n >= TSDB_TABLE_NAME_LEN) {
      return buildInvalidOperationMsg(pMsgBuf, msg1);
    }

    assert(pTableName->n < TSDB_TABLE_FNAME_LEN);

    char name[TSDB_TABLE_FNAME_LEN] = {0};
    strncpy(name, pTableName->z, pTableName->n);
    strdequote(name);

    if (dbName == NULL) {
      return buildInvalidOperationMsg(pMsgBuf, msg3);
    }

    code = tNameSetDbName(pName, acctId, dbName, strlen(dbName));
    if (code != TSDB_CODE_SUCCESS) {
      code = buildInvalidOperationMsg(pMsgBuf, msg2);
      return code;
    }

    code = tNameFromString(pName, name, T_NAME_TABLE);
    if (code != 0) {
      code = buildInvalidOperationMsg(pMsgBuf, msg1);
    }
  }

  if (NULL != strchr(pName->tname, '.')) {
    code = generateSyntaxErrMsgExt(pMsgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, "The table name cannot contain '.'");
  }

  return code;
}

int16_t insFindCol(SToken* pColname, int16_t start, int16_t end, SSchema* pSchema) {
  while (start < end) {
    if (strlen(pSchema[start].name) == pColname->n && strncmp(pColname->z, pSchema[start].name, pColname->n) == 0) {
      return start;
    }
    ++start;
  }
  return -1;
}

void insBuildCreateTbReq(SVCreateTbReq* pTbReq, const char* tname, STag* pTag, int64_t suid, const char* sname,
                         SArray* tagName, uint8_t tagNum, int32_t ttl) {
  pTbReq->type = TD_CHILD_TABLE;
  pTbReq->name = strdup(tname);
  pTbReq->ctb.suid = suid;
  pTbReq->ctb.tagNum = tagNum;
  if (sname) pTbReq->ctb.stbName = strdup(sname);
  pTbReq->ctb.pTag = (uint8_t*)pTag;
  pTbReq->ctb.tagName = taosArrayDup(tagName, NULL);
  pTbReq->ttl = ttl;
  pTbReq->commentLen = -1;

  return;
}

static void initBoundCols(int32_t ncols, int16_t* pBoundCols) {
  for (int32_t i = 0; i < ncols; ++i) {
    pBoundCols[i] = i;
  }
}

static void initColValues(STableMeta* pTableMeta, SArray* pValues) {
  SSchema* pSchemas = getTableColumnSchema(pTableMeta);
  for (int32_t i = 0; i < pTableMeta->tableInfo.numOfColumns; ++i) {
    SColVal val = COL_VAL_NONE(pSchemas[i].colId, pSchemas[i].type);
    taosArrayPush(pValues, &val);
  }
}

int32_t insInitBoundColsInfo(int32_t numOfBound, SBoundColInfo* pInfo) {
  pInfo->numOfCols = numOfBound;
  pInfo->numOfBound = numOfBound;
  pInfo->pColIndex = taosMemoryCalloc(numOfBound, sizeof(int16_t));
  if (NULL == pInfo->pColIndex) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  initBoundCols(numOfBound, pInfo->pColIndex);
  return TSDB_CODE_SUCCESS;
}

void insCheckTableDataOrder(STableDataCxt* pTableCxt, TSKEY tsKey) {
  // once the data block is disordered, we do NOT keep last timestamp any more
  if (!pTableCxt->ordered) {
    return;
  }

  if (tsKey < pTableCxt->lastTs) {
    pTableCxt->ordered = false;
  }

  if (tsKey == pTableCxt->lastTs) {
    pTableCxt->duplicateTs = true;
  }

  pTableCxt->lastTs = tsKey;
  return;
}

void insDestroyBoundColInfo(SBoundColInfo* pInfo) { taosMemoryFreeClear(pInfo->pColIndex); }

static int32_t createTableDataCxt(STableMeta* pTableMeta, SVCreateTbReq** pCreateTbReq, STableDataCxt** pOutput,
                                  bool colMode) {
  STableDataCxt* pTableCxt = taosMemoryCalloc(1, sizeof(STableDataCxt));
  if (NULL == pTableCxt) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  pTableCxt->lastTs = 0;
  pTableCxt->ordered = true;
  pTableCxt->duplicateTs = false;

  pTableCxt->pMeta = tableMetaDup(pTableMeta);
  if (NULL == pTableCxt->pMeta) {
    code = TSDB_CODE_OUT_OF_MEMORY;
  }
  if (TSDB_CODE_SUCCESS == code) {
    pTableCxt->pSchema =
        tBuildTSchema(getTableColumnSchema(pTableMeta), pTableMeta->tableInfo.numOfColumns, pTableMeta->sversion);
    if (NULL == pTableCxt->pSchema) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = insInitBoundColsInfo(pTableMeta->tableInfo.numOfColumns, &pTableCxt->boundColsInfo);
  }
  if (TSDB_CODE_SUCCESS == code) {
    pTableCxt->pValues = taosArrayInit(pTableMeta->tableInfo.numOfColumns, sizeof(SColVal));
    if (NULL == pTableCxt->pValues) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      initColValues(pTableMeta, pTableCxt->pValues);
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    pTableCxt->pData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
    if (NULL == pTableCxt->pData) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    } else {
      pTableCxt->pData->flags = NULL != *pCreateTbReq ? SUBMIT_REQ_AUTO_CREATE_TABLE : 0;
      pTableCxt->pData->flags |= colMode ? SUBMIT_REQ_COLUMN_DATA_FORMAT : 0;
      pTableCxt->pData->suid = pTableMeta->suid;
      pTableCxt->pData->uid = pTableMeta->uid;
      pTableCxt->pData->sver = pTableMeta->sversion;
      pTableCxt->pData->pCreateTbReq = *pCreateTbReq;
      *pCreateTbReq = NULL;
      if (pTableCxt->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
        pTableCxt->pData->aCol = taosArrayInit(128, sizeof(SColData));
        if (NULL == pTableCxt->pData->aCol) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      } else {
        pTableCxt->pData->aRowP = taosArrayInit(128, POINTER_BYTES);
        if (NULL == pTableCxt->pData->aRowP) {
          code = TSDB_CODE_OUT_OF_MEMORY;
        }
      }
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pOutput = pTableCxt;
    qDebug("tableDataCxt created, uid:%" PRId64 ", vgId:%d", pTableMeta->uid, pTableMeta->vgId);
  } else {
    taosMemoryFree(pTableCxt);
  }

  return code;
}

static void resetColValues(SArray* pValues) {
  int32_t num = taosArrayGetSize(pValues);
  for (int32_t i = 0; i < num; ++i) {
    SColVal* pVal = taosArrayGet(pValues, i);
    pVal->flag = CV_FLAG_NONE;
  }
}

int32_t insGetTableDataCxt(SHashObj* pHash, void* id, int32_t idLen, STableMeta* pTableMeta,
                           SVCreateTbReq** pCreateTbReq, STableDataCxt** pTableCxt, bool colMode) {
  STableDataCxt** tmp = (STableDataCxt**)taosHashGet(pHash, id, idLen);
  if (NULL != tmp) {
    *pTableCxt = *tmp;
    resetColValues((*pTableCxt)->pValues);
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = createTableDataCxt(pTableMeta, pCreateTbReq, pTableCxt, colMode);
  if (TSDB_CODE_SUCCESS == code) {
    code = taosHashPut(pHash, id, idLen, pTableCxt, POINTER_BYTES);
  }
  return code;
}

static void destroyColVal(void* p) {
  SColVal* pVal = p;
  if (TSDB_DATA_TYPE_NCHAR == pVal->type) {
    taosMemoryFree(pVal->value.pData);
  }
}

void insDestroyTableDataCxt(STableDataCxt* pTableCxt) {
  if (NULL == pTableCxt) {
    return;
  }

  taosMemoryFreeClear(pTableCxt->pMeta);
  tDestroyTSchema(pTableCxt->pSchema);
  insDestroyBoundColInfo(&pTableCxt->boundColsInfo);
  taosArrayDestroyEx(pTableCxt->pValues, destroyColVal);
  if (pTableCxt->pData) {
    tDestroySSubmitTbData(pTableCxt->pData, TSDB_MSG_FLG_ENCODE);
    taosMemoryFree(pTableCxt->pData);
  }
  taosMemoryFree(pTableCxt);
}

void insDestroyVgroupDataCxt(SVgroupDataCxt* pVgCxt) {
  if (NULL == pVgCxt) {
    return;
  }

  tDestroySSubmitReq2(pVgCxt->pData, TSDB_MSG_FLG_ENCODE);
  taosMemoryFree(pVgCxt->pData);
  taosMemoryFree(pVgCxt);
}

void insDestroyVgroupDataCxtList(SArray* pVgCxtList) {
  if (NULL == pVgCxtList) {
    return;
  }

  size_t size = taosArrayGetSize(pVgCxtList);
  for (int32_t i = 0; i < size; i++) {
    void* p = taosArrayGetP(pVgCxtList, i);
    insDestroyVgroupDataCxt(p);
  }

  taosArrayDestroy(pVgCxtList);
}

void insDestroyVgroupDataCxtHashMap(SHashObj* pVgCxtHash) {
  if (NULL == pVgCxtHash) {
    return;
  }

  void** p = taosHashIterate(pVgCxtHash, NULL);
  while (p) {
    insDestroyVgroupDataCxt(*(SVgroupDataCxt**)p);

    p = taosHashIterate(pVgCxtHash, p);
  }

  taosHashCleanup(pVgCxtHash);
}

void insDestroyTableDataCxtHashMap(SHashObj* pTableCxtHash) {
  if (NULL == pTableCxtHash) {
    return;
  }

  void** p = taosHashIterate(pTableCxtHash, NULL);
  while (p) {
    insDestroyTableDataCxt(*(STableDataCxt**)p);

    p = taosHashIterate(pTableCxtHash, p);
  }

  taosHashCleanup(pTableCxtHash);
}

static int32_t fillVgroupDataCxt(STableDataCxt* pTableCxt, SVgroupDataCxt* pVgCxt) {
  if (NULL == pVgCxt->pData->aSubmitTbData) {
    pVgCxt->pData->aSubmitTbData = taosArrayInit(128, sizeof(SSubmitTbData));
    if (NULL == pVgCxt->pData->aSubmitTbData) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  taosArrayPush(pVgCxt->pData->aSubmitTbData, pTableCxt->pData);
  taosMemoryFreeClear(pTableCxt->pData);

  qDebug("add tableDataCxt uid:%" PRId64 " to vgId:%d", pTableCxt->pMeta->uid, pVgCxt->vgId);

  return TSDB_CODE_SUCCESS;
}

static int32_t createVgroupDataCxt(STableDataCxt* pTableCxt, SHashObj* pVgroupHash, SArray* pVgroupList,
                                   SVgroupDataCxt** pOutput) {
  SVgroupDataCxt* pVgCxt = taosMemoryCalloc(1, sizeof(SVgroupDataCxt));
  if (NULL == pVgCxt) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pVgCxt->pData = taosMemoryCalloc(1, sizeof(SSubmitReq2));
  if (NULL == pVgCxt->pData) {
    insDestroyVgroupDataCxt(pVgCxt);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pVgCxt->vgId = pTableCxt->pMeta->vgId;
  int32_t code = taosHashPut(pVgroupHash, &pVgCxt->vgId, sizeof(pVgCxt->vgId), &pVgCxt, POINTER_BYTES);
  if (TSDB_CODE_SUCCESS == code) {
    taosArrayPush(pVgroupList, &pVgCxt);
    *pOutput = pVgCxt;
  } else {
    insDestroyVgroupDataCxt(pVgCxt);
  }
  return code;
}

int insColDataComp(const void* lp, const void* rp) {
  SColData* pLeft = (SColData*)lp;
  SColData* pRight = (SColData*)rp;
  if (pLeft->cid < pRight->cid) {
    return -1;
  } else if (pLeft->cid > pRight->cid) {
    return 1;
  }

  return 0;
}

int32_t insMergeTableDataCxt(SHashObj* pTableHash, SArray** pVgDataBlocks) {
  SHashObj* pVgroupHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  SArray*   pVgroupList = taosArrayInit(8, POINTER_BYTES);
  if (NULL == pVgroupHash || NULL == pVgroupList) {
    taosHashCleanup(pVgroupHash);
    taosArrayDestroy(pVgroupList);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  bool    colFormat = false;

  void* p = taosHashIterate(pTableHash, NULL);
  if (p) {
    STableDataCxt* pTableCxt = *(STableDataCxt**)p;
    colFormat = (0 != (pTableCxt->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT));
  }

  while (TSDB_CODE_SUCCESS == code && NULL != p) {
    STableDataCxt* pTableCxt = *(STableDataCxt**)p;
    if (colFormat) {
      SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, 0);
      if (pCol->nVal <= 0) {
        p = taosHashIterate(pTableHash, p);
        continue;
      }

      if (pTableCxt->pData->pCreateTbReq) {
        pTableCxt->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
      }

      taosArraySort(pTableCxt->pData->aCol, insColDataComp);

      tColDataSortMerge(pTableCxt->pData->aCol);
    } else {
      if (!pTableCxt->ordered) {
        tRowSort(pTableCxt->pData->aRowP);
      }
      if (!pTableCxt->ordered || pTableCxt->duplicateTs) {
        code = tRowMerge(pTableCxt->pData->aRowP, pTableCxt->pSchema, 0);
      }
    }

    if (TSDB_CODE_SUCCESS == code) {
      SVgroupDataCxt* pVgCxt = NULL;
      int32_t         vgId = pTableCxt->pMeta->vgId;
      void**          p = taosHashGet(pVgroupHash, &vgId, sizeof(vgId));
      if (NULL == p) {
        code = createVgroupDataCxt(pTableCxt, pVgroupHash, pVgroupList, &pVgCxt);
      } else {
        pVgCxt = *(SVgroupDataCxt**)p;
      }
      if (TSDB_CODE_SUCCESS == code) {
        code = fillVgroupDataCxt(pTableCxt, pVgCxt);
      }
    }
    if (TSDB_CODE_SUCCESS == code) {
      p = taosHashIterate(pTableHash, p);
    }
  }

  taosHashCleanup(pVgroupHash);
  if (TSDB_CODE_SUCCESS == code) {
    *pVgDataBlocks = pVgroupList;
  } else {
    insDestroyVgroupDataCxtList(pVgroupList);
  }

  return code;
}

static int32_t buildSubmitReq(int32_t vgId, SSubmitReq2* pReq, void** pData, uint32_t* pLen) {
  int32_t  code = TSDB_CODE_SUCCESS;
  uint32_t len = 0;
  void*    pBuf = NULL;
  tEncodeSize(tEncodeSSubmitReq2, pReq, len, code);
  if (TSDB_CODE_SUCCESS == code) {
    SEncoder encoder;
    len += sizeof(SMsgHead);
    pBuf = taosMemoryMalloc(len);
    if (NULL == pBuf) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    ((SMsgHead*)pBuf)->vgId = htonl(vgId);
    ((SMsgHead*)pBuf)->contLen = htonl(len);
    tEncoderInit(&encoder, POINTER_SHIFT(pBuf, sizeof(SMsgHead)), len - sizeof(SMsgHead));
    code = tEncodeSSubmitReq2(&encoder, pReq);
    tEncoderClear(&encoder);
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pData = pBuf;
    *pLen = len;
  } else {
    taosMemoryFree(pBuf);
  }
  return code;
}

static void destroyVgDataBlocks(void* p) {
  SVgDataBlocks* pVg = p;
  taosMemoryFree(pVg->pData);
  taosMemoryFree(pVg);
}

int32_t insBuildVgDataBlocks(SHashObj* pVgroupsHashObj, SArray* pVgDataCxtList, SArray** pVgDataBlocks) {
  size_t  numOfVg = taosArrayGetSize(pVgDataCxtList);
  SArray* pDataBlocks = taosArrayInit(numOfVg, POINTER_BYTES);
  if (NULL == pDataBlocks) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (size_t i = 0; TSDB_CODE_SUCCESS == code && i < numOfVg; ++i) {
    SVgroupDataCxt* src = taosArrayGetP(pVgDataCxtList, i);
    SVgDataBlocks*  dst = taosMemoryCalloc(1, sizeof(SVgDataBlocks));
    if (NULL == dst) {
      code = TSDB_CODE_OUT_OF_MEMORY;
    }
    if (TSDB_CODE_SUCCESS == code) {
      dst->numOfTables = taosArrayGetSize(src->pData->aSubmitTbData);
      code = taosHashGetDup(pVgroupsHashObj, (const char*)&src->vgId, sizeof(src->vgId), &dst->vg);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = buildSubmitReq(src->vgId, src->pData, &dst->pData, &dst->size);
    }
    if (TSDB_CODE_SUCCESS == code) {
      code = (NULL == taosArrayPush(pDataBlocks, &dst) ? TSDB_CODE_OUT_OF_MEMORY : TSDB_CODE_SUCCESS);
    }
  }

  if (TSDB_CODE_SUCCESS == code) {
    *pVgDataBlocks = pDataBlocks;
  } else {
    taosArrayDestroyP(pDataBlocks, destroyVgDataBlocks);
  }

  return code;
}

static int bindFileds(SBoundColInfo* pBoundInfo, SSchema* pSchema, TAOS_FIELD* fields, int numFields) {
  bool* pUseCols = taosMemoryCalloc(pBoundInfo->numOfCols, sizeof(bool));
  if (NULL == pUseCols) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBoundInfo->numOfBound = 0;

  int16_t lastColIdx = -1;  // last column found
  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < numFields; i++) {
    SToken token;
    token.z = fields[i].name;
    token.n = strlen(fields[i].name);

    int16_t t = lastColIdx + 1;
    int16_t index = insFindCol(&token, t, pBoundInfo->numOfCols, pSchema);
    if (index < 0 && t > 0) {
      index = insFindCol(&token, 0, t, pSchema);
    }
    if (index < 0) {
      uError("can not find column name:%s", token.z);
      code = TSDB_CODE_PAR_INVALID_COLUMN;
      break;
    } else if (pUseCols[index]) {
      code = TSDB_CODE_PAR_INVALID_COLUMN;
      uError("duplicated column name:%s", token.z);
      break;
    } else {
      lastColIdx = index;
      pUseCols[index] = true;
      pBoundInfo->pColIndex[pBoundInfo->numOfBound] = index;
      ++pBoundInfo->numOfBound;
    }
  }

  if (TSDB_CODE_SUCCESS == code && !pUseCols[0]) {
    uError("primary timestamp column can not be null:");
    code = TSDB_CODE_PAR_INVALID_COLUMN;
  }

  taosMemoryFree(pUseCols);
  return code;
}

int rawBlockBindData(SQuery* query, STableMeta* pTableMeta, void* data, SVCreateTbReq* pCreateTb, TAOS_FIELD* tFields,
                     int numFields) {
  STableDataCxt* pTableCxt = NULL;
  int            ret = insGetTableDataCxt(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid,
                                          sizeof(pTableMeta->uid), pTableMeta, &pCreateTb, &pTableCxt, true);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("insGetTableDataCxt error");
    goto end;
  }
  if (tFields != NULL) {
    ret = bindFileds(&pTableCxt->boundColsInfo, getTableColumnSchema(pTableMeta), tFields, numFields);
    if (ret != TSDB_CODE_SUCCESS) {
      uError("bindFileds error");
      goto end;
    }
  }
  // no need to bind, because select * get all fields
  ret = initTableColSubmitData(pTableCxt);
  if (ret != TSDB_CODE_SUCCESS) {
    uError("initTableColSubmitData error");
    goto end;
  }

  char* p = (char*)data;
  // | version | total length | total rows | total columns | flag seg| block group id | column schema | each column
  // length |
  p += sizeof(int32_t);
  p += sizeof(int32_t);

  int32_t numOfRows = *(int32_t*)p;
  p += sizeof(int32_t);

  int32_t numOfCols = *(int32_t*)p;
  p += sizeof(int32_t);

  p += sizeof(int32_t);
  p += sizeof(uint64_t);

  int8_t* fields = p;
  p += numOfCols * (sizeof(int8_t) + sizeof(int32_t));

  int32_t* colLength = (int32_t*)p;
  p += sizeof(int32_t) * numOfCols;

  char* pStart = p;

  SSchema*       pSchema = getTableColumnSchema(pTableCxt->pMeta);
  SBoundColInfo* boundInfo = &pTableCxt->boundColsInfo;

  if (boundInfo->numOfBound != numOfCols) {
    uError("boundInfo->numOfBound:%d != numOfCols:%d", boundInfo->numOfBound, numOfCols);
    ret = TSDB_CODE_INVALID_PARA;
    goto end;
  }
  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema*  pColSchema = &pSchema[boundInfo->pColIndex[c]];
    SColData* pCol = taosArrayGet(pTableCxt->pData->aCol, c);

    if (*fields != pColSchema->type && *(int32_t*)(fields + sizeof(int8_t)) != pColSchema->bytes) {
      uError("type or bytes not equal");
      ret = TSDB_CODE_INVALID_PARA;
      goto end;
    }

    colLength[c] = htonl(colLength[c]);
    int8_t* offset = pStart;
    if (IS_VAR_DATA_TYPE(pColSchema->type)) {
      pStart += numOfRows * sizeof(int32_t);
    } else {
      pStart += BitmapLen(numOfRows);
    }
    char* pData = pStart;

    tColDataAddValueByDataBlock(pCol, pColSchema->type, pColSchema->bytes, numOfRows, offset, pData);
    fields += sizeof(int8_t) + sizeof(int32_t);
    pStart += colLength[c];
  }

end:
  return ret;
}