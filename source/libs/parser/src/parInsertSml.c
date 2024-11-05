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
#include "parInt.h"
#include "parToken.h"
#include "ttime.h"

int32_t qCreateSName(SName* pName, const char* pTableName, int32_t acctId, char* dbName, char* msgBuf,
                     int32_t msgBufLen) {
  SMsgBuf msg = {.buf = msgBuf, .len = msgBufLen};
  SToken  sToken;
  int32_t code = 0;
  char*   tbName = NULL;

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n == 0) {
    return buildInvalidOperationMsg(&msg, "empty table name");
  }

  code = insCreateSName(pName, &sToken, acctId, dbName, &msg);
  if (code) {
    return code;
  }

  NEXT_TOKEN(pTableName, sToken);

  if (sToken.n > 0) {
    return buildInvalidOperationMsg(&msg, "table name format is wrong");
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t smlBoundColumnData(SArray* cols, SBoundColInfo* pBoundInfo, SSchema* pSchema, bool isTag) {
  bool* pUseCols = taosMemoryCalloc(pBoundInfo->numOfCols, sizeof(bool));
  if (NULL == pUseCols) {
    return terrno;
  }

  pBoundInfo->numOfBound = 0;
  int16_t lastColIdx = -1;  // last column found
  int32_t code = TSDB_CODE_SUCCESS;

  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv*  kv = taosArrayGet(cols, i);
    SToken   sToken = {.n = kv->keyLen, .z = (char*)kv->key};
    col_id_t t = lastColIdx + 1;
    col_id_t index = ((t == 0 && !isTag) ? 0 : insFindCol(&sToken, t, pBoundInfo->numOfCols, pSchema));
    uTrace("SML, index:%d, t:%d, ncols:%d", index, t, pBoundInfo->numOfCols);
    if (index < 0 && t > 0) {
      index = insFindCol(&sToken, 0, t, pSchema);
    }

    if (index < 0) {
      uError("smlBoundColumnData. index:%d", index);
      code = TSDB_CODE_SML_INVALID_DATA;
      goto end;
    }
    if (pUseCols[index]) {
      uError("smlBoundColumnData. already set. index:%d", index);
      code = TSDB_CODE_SML_INVALID_DATA;
      goto end;
    }
    lastColIdx = index;
    pUseCols[index] = true;
    pBoundInfo->pColIndex[pBoundInfo->numOfBound] = index;
    ++pBoundInfo->numOfBound;
  }

end:
  taosMemoryFree(pUseCols);

  return code;
}

/**
 * @brief No json tag for schemaless
 *
 * @param cols
 * @param tags
 * @param pSchema
 * @param ppTag
 * @param msg
 * @return int32_t
 */
static int32_t smlBuildTagRow(SArray* cols, SBoundColInfo* tags, SSchema* pSchema, STag** ppTag, SArray** tagName,
                              SMsgBuf* msg) {
  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  if (!pTagArray) {
    return terrno;
  }
  *tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (!*tagName) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int i = 0; i < tags->numOfBound; ++i) {
    SSchema* pTagSchema = &pSchema[tags->pColIndex[i]];
    SSmlKv*  kv = taosArrayGet(cols, i);
    if (kv == NULL){
      code = terrno;
      uError("SML smlBuildTagRow error kv is null");
      goto end;
    }
    if (kv->keyLen != strlen(pTagSchema->name) || memcmp(kv->key, pTagSchema->name, kv->keyLen) != 0 ||
        kv->type != pTagSchema->type) {
      code = TSDB_CODE_SML_INVALID_DATA;
      uError("SML smlBuildTagRow error col not same %s", pTagSchema->name);
      goto end;
    }

    if (taosArrayPush(*tagName, pTagSchema->name) == NULL){
      code = terrno;
      uError("SML smlBuildTagRow error push tag name");
      goto end;
    }
    STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
    //    strcpy(val.colName, pTagSchema->name);
    if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_VARBINARY ||
        pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
      val.pData = (uint8_t*)kv->value;
      val.nData = kv->length;
    } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
      int32_t output = 0;
      void*   p = taosMemoryCalloc(1, kv->length * TSDB_NCHAR_SIZE);
      if (p == NULL) {
        code = terrno;
        goto end;
      }
      if (!taosMbsToUcs4(kv->value, kv->length, (TdUcs4*)(p), kv->length * TSDB_NCHAR_SIZE, &output)) {
        if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
          taosMemoryFree(p);
          code = generateSyntaxErrMsg(msg, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
          goto end;
        }
        char buf[512] = {0};
        (void)snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(terrno));
        taosMemoryFree(p);
        code = buildSyntaxErrMsg(msg, buf, kv->value);
        goto end;
      }
      val.pData = p;
      val.nData = output;
    } else {
      (void)memcpy(&val.i64, &(kv->value), kv->length);
    }
    if (taosArrayPush(pTagArray, &val) == NULL){
      code = terrno;
      uError("SML smlBuildTagRow error push tag array");
      goto end;
    }
  }

  code = tTagNew(pTagArray, 1, false, ppTag);
end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFree(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  return code;
}

int32_t smlInitTableDataCtx(SQuery* query, STableMeta* pTableMeta, STableDataCxt** cxt) {
  SVCreateTbReq* pCreateTbReq = NULL;
  int            ret = insGetTableDataCxt(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid,
                                          sizeof(pTableMeta->uid), pTableMeta, &pCreateTbReq, cxt, false, false);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }

  ret = initTableColSubmitData(*cxt);
  if (ret != TSDB_CODE_SUCCESS) {
    return ret;
  }
  return TSDB_CODE_SUCCESS;
}

void clearColValArraySml(SArray* pCols) {
  int32_t num = taosArrayGetSize(pCols);
  for (int32_t i = 0; i < num; ++i) {
    SColVal* pCol = taosArrayGet(pCols, i);
    if (TSDB_DATA_TYPE_NCHAR == pCol->value.type || TSDB_DATA_TYPE_GEOMETRY == pCol->value.type ||
        TSDB_DATA_TYPE_VARBINARY == pCol->value.type) {
      taosMemoryFreeClear(pCol->value.pData);
    }
    pCol->flag = CV_FLAG_NONE;
    pCol->value.val = 0;
  }
}

int32_t smlBuildRow(STableDataCxt* pTableCxt) {
  SRow** pRow = taosArrayReserve(pTableCxt->pData->aRowP, 1);
  if (pRow == NULL){
    return terrno;
  }
  int    ret = tRowBuild(pTableCxt->pValues, pTableCxt->pSchema, pRow);
  if (TSDB_CODE_SUCCESS != ret) {
    return ret;
  }
  SRowKey key;
  tRowGetKey(*pRow, &key);
  insCheckTableDataOrder(pTableCxt, &key);
  return TSDB_CODE_SUCCESS;
}

int32_t smlBuildCol(STableDataCxt* pTableCxt, SSchema* schema, void* data, int32_t index) {
  int      ret = TSDB_CODE_SUCCESS;
  SSchema* pColSchema = schema + index;
  SColVal* pVal = taosArrayGet(pTableCxt->pValues, index);
  if (pVal == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  SSmlKv*  kv = (SSmlKv*)data;
  if (kv->keyLen != strlen(pColSchema->name) || memcmp(kv->key, pColSchema->name, kv->keyLen) != 0 ||
      kv->type != pColSchema->type) {
    ret = TSDB_CODE_SML_INVALID_DATA;
    char* tmp = taosMemoryCalloc(kv->keyLen + 1, 1);
    if (tmp) {
      (void)memcpy(tmp, kv->key, kv->keyLen);
      uInfo("SML data(name:%s type:%s) is not same like the db data(name:%s type:%s)", tmp, tDataTypes[kv->type].name,
            pColSchema->name, tDataTypes[pColSchema->type].name);
      taosMemoryFree(tmp);
    } else {
      uError("SML smlBuildCol out of memory");
      ret = terrno;
    }
    goto end;
  }
  if (kv->type == TSDB_DATA_TYPE_NCHAR) {
    int32_t len = 0;
    int64_t size = pColSchema->bytes - VARSTR_HEADER_SIZE;
    if (size <= 0) {
      ret = TSDB_CODE_SML_INVALID_DATA;
      goto end;
    }
    char* pUcs4 = taosMemoryCalloc(1, size);
    if (NULL == pUcs4) {
      ret = terrno;
      goto end;
    }
    if (!taosMbsToUcs4(kv->value, kv->length, (TdUcs4*)pUcs4, size, &len)) {
      if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
        taosMemoryFree(pUcs4);
        ret = TSDB_CODE_PAR_VALUE_TOO_LONG;
        goto end;
      }
      taosMemoryFree(pUcs4);
      ret = TSDB_CODE_TSC_INVALID_VALUE;
      goto end;
    }
    pVal->value.pData = pUcs4;
    pVal->value.nData = len;
  } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
    pVal->value.nData = kv->length;
    pVal->value.pData = (uint8_t*)kv->value;
  } else if (kv->type == TSDB_DATA_TYPE_GEOMETRY || kv->type == TSDB_DATA_TYPE_VARBINARY) {
    pVal->value.nData = kv->length;
    pVal->value.pData = taosMemoryMalloc(kv->length);
    if (!pVal->value.pData) {
      ret = terrno;
      uError("SML smlBuildCol malloc failed %s:%d, err: %s", __func__, __LINE__, tstrerror(ret));
      goto end;
    }
    (void)memcpy(pVal->value.pData, (uint8_t*)kv->value, kv->length);
  } else {
    (void)memcpy(&pVal->value.val, &(kv->value), kv->length);
  }
  pVal->flag = CV_FLAG_VALUE;

end:
  return ret;
}

int32_t smlBindData(SQuery* query, bool dataFormat, SArray* tags, SArray* colsSchema, SArray* cols,
                    STableMeta* pTableMeta, char* tableName, const char* sTableName, int32_t sTableNameLen, int32_t ttl,
                    char* msgBuf, int32_t msgBufLen) {
  SMsgBuf pBuf = {.buf = msgBuf, .len = msgBufLen};

  SSchema*       pTagsSchema = getTableTagSchema(pTableMeta);
  SBoundColInfo  bindTags = {0};
  SVCreateTbReq* pCreateTblReq = NULL;
  SArray*        tagName = NULL;

  int ret = insInitBoundColsInfo(getNumOfTags(pTableMeta), &bindTags);
  if (ret != TSDB_CODE_SUCCESS) {
    ret = buildInvalidOperationMsg(&pBuf, "init bound cols error");
    goto end;
  }

  ret = smlBoundColumnData(tags, &bindTags, pTagsSchema, true);
  if (ret != TSDB_CODE_SUCCESS) {
    ret = buildInvalidOperationMsg(&pBuf, "bound tags error");
    goto end;
  }

  STag* pTag = NULL;

  ret = smlBuildTagRow(tags, &bindTags, pTagsSchema, &pTag, &tagName, &pBuf);
  if (ret != TSDB_CODE_SUCCESS) {
    goto end;
  }

  pCreateTblReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  if (NULL == pCreateTblReq) {
    ret = terrno;
    goto end;
  }
  ret = insBuildCreateTbReq(pCreateTblReq, tableName, pTag, pTableMeta->suid, NULL, tagName,
                            pTableMeta->tableInfo.numOfTags, ttl);
  if (TSDB_CODE_SUCCESS != ret) {
    goto end;
  }

  pCreateTblReq->ctb.stbName = taosMemoryCalloc(1, sTableNameLen + 1);
  if (pCreateTblReq->ctb.stbName == NULL){
    ret = terrno;
    goto end;
  }
  (void)memcpy(pCreateTblReq->ctb.stbName, sTableName, sTableNameLen);

  if (dataFormat) {
    STableDataCxt** pTableCxt = (STableDataCxt**)taosHashGet(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj,
                                                             &pTableMeta->uid, sizeof(pTableMeta->uid));
    if (NULL == pTableCxt) {
      ret = buildInvalidOperationMsg(&pBuf, "dataformat true. get tableDataCtx error");
      goto end;
    }
    (*pTableCxt)->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
    (*pTableCxt)->pData->pCreateTbReq = pCreateTblReq;
    (*pTableCxt)->pMeta->uid = pTableMeta->uid;
    (*pTableCxt)->pMeta->vgId = pTableMeta->vgId;
    pCreateTblReq = NULL;
    goto end;
  }

  STableDataCxt* pTableCxt = NULL;
  ret = insGetTableDataCxt(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid,
                           sizeof(pTableMeta->uid), pTableMeta, &pCreateTblReq, &pTableCxt, false, false);
  if (ret != TSDB_CODE_SUCCESS) {
    ret = buildInvalidOperationMsg(&pBuf, "insGetTableDataCxt error");
    goto end;
  }

  SSchema* pSchema = getTableColumnSchema(pTableMeta);
  ret = smlBoundColumnData(colsSchema, &pTableCxt->boundColsInfo, pSchema, false);
  if (ret != TSDB_CODE_SUCCESS) {
    ret = buildInvalidOperationMsg(&pBuf, "bound cols error");
    goto end;
  }

  ret = initTableColSubmitData(pTableCxt);
  if (ret != TSDB_CODE_SUCCESS) {
    ret = buildInvalidOperationMsg(&pBuf, "initTableColSubmitData error");
    goto end;
  }

  int32_t rowNum = taosArrayGetSize(cols);
  if (rowNum <= 0) {
    ret = buildInvalidOperationMsg(&pBuf, "cols size <= 0");
    goto end;
  }

  for (int32_t r = 0; r < rowNum; ++r) {
    void* rowData = taosArrayGetP(cols, r);
    if (rowData == NULL) {
      ret = terrno;
      goto end;
    }
    // 1. set the parsed value from sql string
    for (int c = 0; c < pTableCxt->boundColsInfo.numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[pTableCxt->boundColsInfo.pColIndex[c]];
      SColVal* pVal = taosArrayGet(pTableCxt->pValues, pTableCxt->boundColsInfo.pColIndex[c]);
      if (pVal == NULL) {
        ret = terrno;
        goto end;
      }
      void**   p = taosHashGet(rowData, pColSchema->name, strlen(pColSchema->name));
      if (p == NULL) {
        continue;
      }
      SSmlKv* kv = *(SSmlKv**)p;
      if (kv->type != pColSchema->type) {
        ret = buildInvalidOperationMsg(&pBuf, "kv type not equal to col type");
        goto end;
      }
      if (pColSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
        kv->i = convertTimePrecision(kv->i, TSDB_TIME_PRECISION_NANO, pTableMeta->tableInfo.precision);
      }
      if (kv->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t len = 0;
        char*   pUcs4 = taosMemoryCalloc(1, pColSchema->bytes - VARSTR_HEADER_SIZE);
        if (NULL == pUcs4) {
          ret = terrno;
          goto end;
        }
        if (!taosMbsToUcs4(kv->value, kv->length, (TdUcs4*)pUcs4, pColSchema->bytes - VARSTR_HEADER_SIZE, &len)) {
          if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
            uError("sml bind taosMbsToUcs4 error, kv length:%d, bytes:%d, kv->value:%s", (int)kv->length,
                   pColSchema->bytes, kv->value);
            (void)buildInvalidOperationMsg(&pBuf, "value too long");
            ret = TSDB_CODE_PAR_VALUE_TOO_LONG;
            goto end;
          }
          ret = buildInvalidOperationMsg(&pBuf, strerror(terrno));
          goto end;
        }
        pVal->value.pData = pUcs4;
        pVal->value.nData = len;
      } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
        pVal->value.nData = kv->length;
        pVal->value.pData = (uint8_t*)kv->value;
      } else if (kv->type == TSDB_DATA_TYPE_GEOMETRY || kv->type == TSDB_DATA_TYPE_VARBINARY) {
        pVal->value.nData = kv->length;
        pVal->value.pData = taosMemoryMalloc(kv->length);
        if (NULL == pVal->value.pData) {
          ret = terrno;
          goto end;
        }
        (void)memcpy(pVal->value.pData, (uint8_t*)kv->value, kv->length);
      } else {
        (void)memcpy(&pVal->value.val, &(kv->value), kv->length);
      }
      pVal->flag = CV_FLAG_VALUE;
    }

    SRow** pRow = taosArrayReserve(pTableCxt->pData->aRowP, 1);
    if (NULL == pRow) {
      ret = terrno;
      goto end;
    }
    ret = tRowBuild(pTableCxt->pValues, pTableCxt->pSchema, pRow);
    if (TSDB_CODE_SUCCESS != ret) {
      ret = buildInvalidOperationMsg(&pBuf, "tRowBuild error");
      goto end;
    }
    SRowKey key;
    tRowGetKey(*pRow, &key);
    insCheckTableDataOrder(pTableCxt, &key);
    clearColValArraySml(pTableCxt->pValues);
  }

end:
  insDestroyBoundColInfo(&bindTags);
  tdDestroySVCreateTbReq(pCreateTblReq);
  taosMemoryFree(pCreateTblReq);
  taosArrayDestroy(tagName);
  return ret;
}

int32_t smlInitHandle(SQuery** query) {
  *query = NULL;
  SQuery* pQuery = NULL;
  SVnodeModifyOpStmt* stmt = NULL;

  int32_t code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery);
  if (code != 0) {
    uError("SML create pQuery error");
    goto END;
  }
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&stmt);
  if (code != 0) {
    uError("SML create SVnodeModifyOpStmt error");
    goto END;
  }
  stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (stmt->pTableBlockHashObj == NULL){
    uError("SML create pTableBlockHashObj error");
    code = terrno;
    goto END;
  }
  stmt->freeHashFunc = insDestroyTableDataCxtHashMap;
  stmt->freeArrayFunc = insDestroyVgroupDataCxtList;

  pQuery->pRoot = (SNode*)stmt;
  *query = pQuery;
  return code;

END:
  nodesDestroyNode((SNode*)stmt);
  qDestroyQuery(pQuery);
  return code;
}

int32_t smlBuildOutput(SQuery* handle, SHashObj* pVgHash) {
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)(handle)->pRoot;
  // merge according to vgId
  int32_t code = insMergeTableDataCxt(pStmt->pTableBlockHashObj, &pStmt->pVgDataBlocks, true);
  if (code != TSDB_CODE_SUCCESS) {
    uError("insMergeTableDataCxt failed");
    return code;
  }
  code = insBuildVgDataBlocks(pVgHash, pStmt->pVgDataBlocks, &pStmt->pDataBlocks, false);
  if (code != TSDB_CODE_SUCCESS) {
    uError("insBuildVgDataBlocks failed");
    return code;
  }
  return code;
}
