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
  SToken  sToken = {0};
  int     code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  NEXT_TOKEN(pTableName, sToken);
  TSDB_CHECK_CONDITION(sToken.n != 0, code, lino, end, TSDB_CODE_TSC_INVALID_OPERATION);
  code = insCreateSName(pName, &sToken, acctId, dbName, &msg);
  TSDB_CHECK_CODE(code, lino, end);
  NEXT_TOKEN(pTableName, sToken);
  TSDB_CHECK_CONDITION(sToken.n <= 0, code, lino, end, TSDB_CODE_TSC_INVALID_OPERATION);

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t smlBoundColumnData(SArray* cols, SBoundColInfo* pBoundInfo, SSchema* pSchema, bool isTag) {
  int     code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool*   pUseCols = taosMemoryCalloc(pBoundInfo->numOfCols, sizeof(bool));
  TSDB_CHECK_NULL(pUseCols, code, lino, end, terrno);
  pBoundInfo->numOfBound = 0;
  int16_t lastColIdx = -1;  // last column found

  for (int i = 0; i < taosArrayGetSize(cols); ++i) {
    SSmlKv*  kv = taosArrayGet(cols, i);
    SToken   sToken = {.n = kv->keyLen, .z = (char*)kv->key};
    col_id_t t = lastColIdx + 1;
    col_id_t index = ((t == 0 && !isTag) ? 0 : insFindCol(&sToken, t, pBoundInfo->numOfCols, pSchema));
    uTrace("SML, index:%d, t:%d, ncols:%d", index, t, pBoundInfo->numOfCols);
    if (index < 0 && t > 0) {
      index = insFindCol(&sToken, 0, t, pSchema);
    }

    TSDB_CHECK_CONDITION(index >= 0, code, lino, end, TSDB_CODE_SML_INVALID_DATA);
    TSDB_CHECK_CONDITION(!pUseCols[index], code, lino, end, TSDB_CODE_SML_INVALID_DATA);

    lastColIdx = index;
    pUseCols[index] = true;
    pBoundInfo->pColIndex[pBoundInfo->numOfBound] = index;
    ++pBoundInfo->numOfBound;
  }

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(pUseCols);
  return code;
}

static int32_t smlMbsToUcs4(const char* mbs, size_t mbsLen, void** result, int32_t* resultLen, int32_t maxLen,
                            void* charsetCxt) {
  int     code = TSDB_CODE_SUCCESS;
  void*   pUcs4 = NULL;
  int32_t lino = 0;
  pUcs4 = taosMemoryCalloc(1, maxLen);
  TSDB_CHECK_NULL(pUcs4, code, lino, end, terrno);
  TSDB_CHECK_CONDITION(taosMbsToUcs4(mbs, mbsLen, (TdUcs4*)pUcs4, maxLen, resultLen, charsetCxt), code, lino, end,
                       terrno);
  *result = pUcs4;
  pUcs4 = NULL;

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(pUcs4);
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
                              SMsgBuf* msg, void* charsetCxt) {
  int     code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  TSDB_CHECK_NULL(pTagArray, code, lino, end, terrno);
  *tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  TSDB_CHECK_NULL(*tagName, code, lino, end, terrno);

  for (int i = 0; i < tags->numOfBound; ++i) {
    SSchema* pTagSchema = &pSchema[tags->pColIndex[i]];
    SSmlKv*  kv = taosArrayGet(cols, i);
    TSDB_CHECK_NULL(kv, code, lino, end, terrno);
    bool cond = (kv->keyLen == strlen(pTagSchema->name) && memcmp(kv->key, pTagSchema->name, kv->keyLen) == 0 &&
                 kv->type == pTagSchema->type);
    TSDB_CHECK_CONDITION(cond, code, lino, end, TSDB_CODE_SML_INVALID_DATA);
    TSDB_CHECK_NULL(taosArrayPush(*tagName, pTagSchema->name), code, lino, end, terrno);
    STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
    if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_VARBINARY ||
        pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
      val.pData = (uint8_t*)kv->value;
      val.nData = kv->length;
    } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
      code = smlMbsToUcs4(kv->value, kv->length, (void**)&val.pData, (int32_t*)&val.nData, kv->length * TSDB_NCHAR_SIZE,
                          charsetCxt);
      TSDB_CHECK_CODE(code, lino, end);
    } else {
      (void)memcpy(&val.i64, &(kv->value), kv->length);
    }
    TSDB_CHECK_NULL(taosArrayPush(pTagArray, &val), code, lino, end, terrno);
  }
  code = tTagNew(pTagArray, 1, false, ppTag);

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
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
  int            ret = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SVCreateTbReq* pCreateTbReq = NULL;
  ret = insGetTableDataCxt(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj, &pTableMeta->uid,
                           sizeof(pTableMeta->uid), pTableMeta, &pCreateTbReq, cxt, false, false);
  TSDB_CHECK_CODE(ret, lino, end);
  ret = initTableColSubmitData(*cxt);
  TSDB_CHECK_CODE(ret, lino, end);

end:
  if (ret != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(ret));
  }
  return ret;
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
  int     ret = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SRow**  pRow = taosArrayReserve(pTableCxt->pData->aRowP, 1);
  TSDB_CHECK_NULL(pRow, ret, lino, end, terrno);

  SRowBuildScanInfo sinfo = {0};
  ret = tRowBuild(pTableCxt->pValues, pTableCxt->pSchema, pRow, &sinfo);
  TSDB_CHECK_CODE(ret, lino, end);
  SRowKey key;
  tRowGetKey(*pRow, &key);
  insCheckTableDataOrder(pTableCxt, &key);
end:
  if (ret != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(ret));
  }
  return ret;
}

int32_t smlBuildCol(STableDataCxt* pTableCxt, SSchema* schema, void* data, int32_t index, void* charsetCxt) {
  int      ret = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SSchema* pColSchema = schema + index;
  SColVal* pVal = taosArrayGet(pTableCxt->pValues, index);
  TSDB_CHECK_NULL(pVal, ret, lino, end, TSDB_CODE_SUCCESS);
  SSmlKv* kv = (SSmlKv*)data;
  if (kv->keyLen != strlen(pColSchema->name) || memcmp(kv->key, pColSchema->name, kv->keyLen) != 0 ||
      kv->type != pColSchema->type) {
    ret = TSDB_CODE_SML_INVALID_DATA;
    char* tmp = taosMemoryCalloc(kv->keyLen + 1, 1);
    TSDB_CHECK_NULL(tmp, ret, lino, end, terrno);
    (void)memcpy(tmp, kv->key, kv->keyLen);
    uInfo("SML data(name:%s type:%s) is not same like the db data(name:%s type:%s)", tmp, tDataTypes[kv->type].name,
          pColSchema->name, tDataTypes[pColSchema->type].name);
    taosMemoryFree(tmp);
    goto end;
  }
  if (kv->type == TSDB_DATA_TYPE_NCHAR) {
    ret = smlMbsToUcs4(kv->value, kv->length, (void**)&pVal->value.pData, &pVal->value.nData,
                       pColSchema->bytes - VARSTR_HEADER_SIZE, charsetCxt);
    TSDB_CHECK_CODE(ret, lino, end);
  } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
    pVal->value.nData = kv->length;
    pVal->value.pData = (uint8_t*)kv->value;
  } else if (kv->type == TSDB_DATA_TYPE_GEOMETRY || kv->type == TSDB_DATA_TYPE_VARBINARY) {
    pVal->value.nData = kv->length;
    pVal->value.pData = taosMemoryMalloc(kv->length);
    TSDB_CHECK_NULL(pVal->value.pData, ret, lino, end, terrno);

    (void)memcpy(pVal->value.pData, (uint8_t*)kv->value, kv->length);
  } else {
    valueSetDatum(&pVal->value, kv->type, &(kv->value), kv->length);
  }
  pVal->flag = CV_FLAG_VALUE;

end:
  if (ret != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(ret));
  }
  return ret;
}

int32_t smlBindData(SQuery* query, bool dataFormat, SArray* tags, SArray* colsSchema, SArray* cols,
                    STableMeta* pTableMeta, char* tableName, const char* sTableName, int32_t sTableNameLen, int32_t ttl,
                    char* msgBuf, int32_t msgBufLen, void* charsetCxt) {
  int32_t lino = 0;
  int32_t ret = 0;
  SMsgBuf pBuf = {.buf = msgBuf, .len = msgBufLen};

  SSchema*       pTagsSchema = getTableTagSchema(pTableMeta);
  SBoundColInfo  bindTags = {0};
  SVCreateTbReq* pCreateTblReq = NULL;
  SArray*        tagName = NULL;

  ret = insInitBoundColsInfo(getNumOfTags(pTableMeta), &bindTags);
  TSDB_CHECK_CODE(ret, lino, end);

  ret = smlBoundColumnData(tags, &bindTags, pTagsSchema, true);
  TSDB_CHECK_CODE(ret, lino, end);

  STag* pTag = NULL;
  ret = smlBuildTagRow(tags, &bindTags, pTagsSchema, &pTag, &tagName, &pBuf, charsetCxt);
  TSDB_CHECK_CODE(ret, lino, end);

  pCreateTblReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
  TSDB_CHECK_NULL(pCreateTblReq, ret, lino, end, terrno);

  ret = insBuildCreateTbReq(pCreateTblReq, tableName, pTag, pTableMeta->suid, NULL, tagName,
                            pTableMeta->tableInfo.numOfTags, ttl);
  TSDB_CHECK_CODE(ret, lino, end);

  pCreateTblReq->ctb.stbName = taosMemoryCalloc(1, sTableNameLen + 1);
  TSDB_CHECK_NULL(pCreateTblReq->ctb.stbName, ret, lino, end, terrno);

  (void)memcpy(pCreateTblReq->ctb.stbName, sTableName, sTableNameLen);

  if (dataFormat) {
    STableDataCxt** pTableCxt = (STableDataCxt**)taosHashGet(((SVnodeModifyOpStmt*)(query->pRoot))->pTableBlockHashObj,
                                                             &pTableMeta->uid, sizeof(pTableMeta->uid));
    TSDB_CHECK_NULL(pTableCxt, ret, lino, end, TSDB_CODE_TSC_INVALID_OPERATION);
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
  TSDB_CHECK_CODE(ret, lino, end);

  SSchema* pSchema = getTableColumnSchema(pTableMeta);
  ret = smlBoundColumnData(colsSchema, &pTableCxt->boundColsInfo, pSchema, false);
  TSDB_CHECK_CODE(ret, lino, end);

  ret = initTableColSubmitData(pTableCxt);
  TSDB_CHECK_CODE(ret, lino, end);

  int32_t rowNum = taosArrayGetSize(cols);
  TSDB_CHECK_CONDITION(rowNum > 0, ret, lino, end, TSDB_CODE_TSC_INVALID_OPERATION);

  for (int32_t r = 0; r < rowNum; ++r) {
    void* rowData = taosArrayGetP(cols, r);
    TSDB_CHECK_NULL(rowData, ret, lino, end, terrno);

    // 1. set the parsed value from sql string
    for (int c = 0; c < pTableCxt->boundColsInfo.numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[pTableCxt->boundColsInfo.pColIndex[c]];
      SColVal* pVal = taosArrayGet(pTableCxt->pValues, pTableCxt->boundColsInfo.pColIndex[c]);
      TSDB_CHECK_NULL(pVal, ret, lino, end, terrno);
      void** p = taosHashGet(rowData, pColSchema->name, strlen(pColSchema->name));
      if (p == NULL) {
        continue;
      }
      SSmlKv* kv = *(SSmlKv**)p;
      TSDB_CHECK_CONDITION(kv->type == pColSchema->type, ret, lino, end, TSDB_CODE_TSC_INVALID_OPERATION);

      if (pColSchema->type == TSDB_DATA_TYPE_TIMESTAMP) {
        kv->i = convertTimePrecision(kv->i, TSDB_TIME_PRECISION_NANO, pTableMeta->tableInfo.precision);
      }
      if (kv->type == TSDB_DATA_TYPE_NCHAR) {
        ret = smlMbsToUcs4(kv->value, kv->length, (void**)&pVal->value.pData, (int32_t*)&pVal->value.nData,
                           pColSchema->bytes - VARSTR_HEADER_SIZE, charsetCxt);
        TSDB_CHECK_CODE(ret, lino, end);
      } else if (kv->type == TSDB_DATA_TYPE_BINARY) {
        pVal->value.nData = kv->length;
        pVal->value.pData = (uint8_t*)kv->value;
      } else if (kv->type == TSDB_DATA_TYPE_GEOMETRY || kv->type == TSDB_DATA_TYPE_VARBINARY) {
        pVal->value.nData = kv->length;
        pVal->value.pData = taosMemoryMalloc(kv->length);
        TSDB_CHECK_NULL(pVal->value.pData, ret, lino, end, terrno);
        (void)memcpy(pVal->value.pData, (uint8_t*)kv->value, kv->length);
      } else {
        valueSetDatum(&pVal->value, kv->type, &(kv->value), kv->length);
      }
      pVal->flag = CV_FLAG_VALUE;
    }

    SRow** pRow = taosArrayReserve(pTableCxt->pData->aRowP, 1);
    TSDB_CHECK_NULL(pRow, ret, lino, end, terrno);
    SRowBuildScanInfo sinfo = {0};
    ret = tRowBuild(pTableCxt->pValues, pTableCxt->pSchema, pRow, &sinfo);
    TSDB_CHECK_CODE(ret, lino, end);
    SRowKey key = {0};
    tRowGetKey(*pRow, &key);
    insCheckTableDataOrder(pTableCxt, &key);
    clearColValArraySml(pTableCxt->pValues);
  }

end:
  if (ret != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(ret));
    ret = buildInvalidOperationMsg(&pBuf, tstrerror(ret));
  }
  insDestroyBoundColInfo(&bindTags);
  tdDestroySVCreateTbReq(pCreateTblReq);
  taosMemoryFree(pCreateTblReq);
  taosArrayDestroy(tagName);
  return ret;
}

int32_t smlInitHandle(SQuery** query) {
  int32_t             lino = 0;
  int32_t             code = 0;
  SQuery*             pQuery = NULL;
  SVnodeModifyOpStmt* stmt = NULL;
  TSDB_CHECK_NULL(query, code, lino, end, TSDB_CODE_INVALID_PARA);

  *query = NULL;
  code = nodesMakeNode(QUERY_NODE_QUERY, (SNode**)&pQuery);
  TSDB_CHECK_CODE(code, lino, end);
  pQuery->execMode = QUERY_EXEC_MODE_SCHEDULE;
  pQuery->haveResultSet = false;
  pQuery->msgType = TDMT_VND_SUBMIT;
  code = nodesMakeNode(QUERY_NODE_VNODE_MODIFY_STMT, (SNode**)&stmt);
  TSDB_CHECK_CODE(code, lino, end);
  stmt->pTableBlockHashObj = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  TSDB_CHECK_NULL(stmt->pTableBlockHashObj, code, lino, end, terrno);
  stmt->freeHashFunc = insDestroyTableDataCxtHashMap;
  stmt->freeArrayFunc = insDestroyVgroupDataCxtList;

  pQuery->pRoot = (SNode*)stmt;
  *query = pQuery;
  return code;

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  nodesDestroyNode((SNode*)stmt);
  qDestroyQuery(pQuery);
  return code;
}

int32_t smlBuildOutput(SQuery* handle, SHashObj* pVgHash) {
  int32_t lino = 0;
  int32_t code = 0;

  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)(handle)->pRoot;
  code = insMergeTableDataCxt(pStmt->pTableBlockHashObj, &pStmt->pVgDataBlocks, true);
  TSDB_CHECK_CODE(code, lino, end);
  code = insBuildVgDataBlocks(pVgHash, pStmt->pVgDataBlocks, &pStmt->pDataBlocks, false);
  TSDB_CHECK_CODE(code, lino, end);

end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t smlBuildOutputRaw(SQuery* handle, SHashObj* pVgHash) {
  int32_t lino = 0;
  int32_t code = 0;

  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)(handle)->pRoot;
  code = insBuildVgDataBlocks(pVgHash, pStmt->pVgDataBlocks, &pStmt->pDataBlocks, false);
  TSDB_CHECK_CODE(code, lino, end);

  end:
  if (code != 0) {
    uError("%s failed at %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
