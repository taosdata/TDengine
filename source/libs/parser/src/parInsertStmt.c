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

#include "os.h"
#include "parInsertUtil.h"
#include "parInt.h"
#include "parToken.h"
#include "query.h"
#include "tglobal.h"
#include "ttime.h"
#include "ttypes.h"

typedef struct SKvParam {
  int16_t  pos;
  SArray*  pTagVals;
  SSchema* schema;
  char     buf[TSDB_MAX_TAGS_LEN];
} SKvParam;

int32_t qCloneCurrentTbData(STableDataCxt* pDataBlock, SSubmitTbData** pData) {
  *pData = taosMemoryCalloc(1, sizeof(SSubmitTbData));
  if (NULL == *pData) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SSubmitTbData* pNew = *pData;

  *pNew = *pDataBlock->pData;

  cloneSVreateTbReq(pDataBlock->pData->pCreateTbReq, &pNew->pCreateTbReq);
  pNew->aCol = taosArrayDup(pDataBlock->pData->aCol, NULL);

  int32_t colNum = taosArrayGetSize(pNew->aCol);
  for (int32_t i = 0; i < colNum; ++i) {
    SColData* pCol = (SColData*)taosArrayGet(pNew->aCol, i);
    tColDataDeepClear(pCol);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SArray*             pVgDataBlocks = NULL;
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

  // merge according to vgId
  if (taosHashGetSize(pBlockHash) > 0) {
    code = insMergeTableDataCxt(pBlockHash, &pVgDataBlocks, true);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildVgDataBlocks(pVgHash, pVgDataBlocks, &pStmt->pDataBlocks);
  }

  if (pStmt->freeArrayFunc) {
    pStmt->freeArrayFunc(pVgDataBlocks);
  }
  return code;
}

int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SMsgBuf        pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t        code = TSDB_CODE_SUCCESS;
  SBoundColInfo* tags = (SBoundColInfo*)boundTags;
  if (NULL == tags) {
    return TSDB_CODE_APP_ERROR;
  }

  SArray* pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  if (!pTagArray) {
    return buildInvalidOperationMsg(&pBuf, "out of memory");
  }

  SArray* tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  if (!tagName) {
    code = buildInvalidOperationMsg(&pBuf, "out of memory");
    goto end;
  }

  SSchema* pSchema = getTableTagSchema(pDataBlock->pMeta);

  bool  isJson = false;
  STag* pTag = NULL;

  for (int c = 0; c < tags->numOfBound; ++c) {
    if (bind[c].is_null && bind[c].is_null[0]) {
      continue;
    }

    SSchema* pTagSchema = &pSchema[tags->pColIndex[c]];
    int32_t  colLen = pTagSchema->bytes;
    if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
      colLen = bind[c].length[0];
      if ((colLen + VARSTR_HEADER_SIZE) > pTagSchema->bytes) {
        code = buildInvalidOperationMsg(&pBuf, "tag length is too big");
        goto end;
      }
    }
    taosArrayPush(tagName, pTagSchema->name);
    if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
      if (colLen > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        code = buildSyntaxErrMsg(&pBuf, "json string too long than 4095", bind[c].buffer);
        goto end;
      }

      isJson = true;
      char* tmp = taosMemoryCalloc(1, colLen + 1);
      memcpy(tmp, bind[c].buffer, colLen);
      code = parseJsontoTagData(tmp, pTagArray, &pTag, &pBuf);
      taosMemoryFree(tmp);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    } else {
      STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
      //      strcpy(val.colName, pTagSchema->name);
      if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_VARBINARY ||
          pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
        val.pData = (uint8_t*)bind[c].buffer;
        val.nData = colLen;
      } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t output = 0;
        void*   p = taosMemoryCalloc(1, colLen * TSDB_NCHAR_SIZE);
        if (p == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto end;
        }
        if (!taosMbsToUcs4(bind[c].buffer, colLen, (TdUcs4*)(p), colLen * TSDB_NCHAR_SIZE, &output)) {
          if (errno == E2BIG) {
            taosMemoryFree(p);
            code = generateSyntaxErrMsg(&pBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
            goto end;
          }
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(errno));
          taosMemoryFree(p);
          code = buildSyntaxErrMsg(&pBuf, buf, bind[c].buffer);
          goto end;
        }
        val.pData = p;
        val.nData = output;
      } else {
        memcpy(&val.i64, bind[c].buffer, colLen);
      }
      taosArrayPush(pTagArray, &val);
    }
  }

  if (!isJson && (code = tTagNew(pTagArray, 1, false, &pTag)) != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (NULL == pDataBlock->pData->pCreateTbReq) {
    pDataBlock->pData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == pDataBlock->pData->pCreateTbReq) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto end;
    }
  }

  insBuildCreateTbReq(pDataBlock->pData->pCreateTbReq, tName, pTag, suid, sTableName, tagName,
                      pDataBlock->pMeta->tableInfo.numOfTags, TSDB_DEFAULT_TABLE_TTL);

end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  taosArrayDestroy(tagName);

  return code;
}

int32_t convertStmtNcharCol(SMsgBuf* pMsgBuf, SSchema* pSchema, TAOS_MULTI_BIND* src, TAOS_MULTI_BIND* dst) {
  int32_t output = 0;
  int32_t newBuflen = (pSchema->bytes - VARSTR_HEADER_SIZE) * src->num;
  if (dst->buffer_length < newBuflen) {
    dst->buffer = taosMemoryRealloc(dst->buffer, newBuflen);
    if (NULL == dst->buffer) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (NULL == dst->length) {
    dst->length = taosMemoryRealloc(dst->length, sizeof(int32_t) * src->num);
    if (NULL == dst->buffer) {
      taosMemoryFreeClear(dst->buffer);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  dst->buffer_length = pSchema->bytes - VARSTR_HEADER_SIZE;

  for (int32_t i = 0; i < src->num; ++i) {
    if (src->is_null && src->is_null[i]) {
      continue;
    }

    if (!taosMbsToUcs4(((char*)src->buffer) + src->buffer_length * i, src->length[i],
                       (TdUcs4*)(((char*)dst->buffer) + dst->buffer_length * i), dst->buffer_length, &output)) {
      if (errno == E2BIG) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(errno));
      return buildSyntaxErrMsg(pMsgBuf, buf, NULL);
    }

    dst->length[i] = output;
  }

  dst->buffer_type = src->buffer_type;
  dst->is_null = src->is_null;
  dst->num = src->num;

  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtColsValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t          rowNum = bind->num;
  TAOS_MULTI_BIND  ncharBind = {0};
  TAOS_MULTI_BIND* pBind = NULL;
  int32_t          code = 0;

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema*  pColSchema = &pSchema[boundInfo->pColIndex[c]];
    SColData* pCol = taosArrayGet(pDataBlock->pData->aCol, c);

    if (bind[c].num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bind[c].is_null && *bind[c].is_null)) && bind[c].buffer_type != pColSchema->type) { // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol(&pBuf, pColSchema, bind + c, &ncharBind);
      if (code) {
        goto _return;
      }
      pBind = &ncharBind;
    } else {
      pBind = bind + c;
    }

    code = tColDataAddValueByBind(pCol, pBind, IS_VAR_DATA_TYPE(pColSchema->type) ? pColSchema->bytes - VARSTR_HEADER_SIZE: -1);
    if (code) {
      goto _return;
    }
  }

  qDebug("stmt all %d columns bind %d rows data", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t qBindStmtSingleColValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, int32_t colIdx,
                                int32_t rowNum) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  SSchema*         pColSchema = &pSchema[boundInfo->pColIndex[colIdx]];
  SColData*        pCol = taosArrayGet(pDataBlock->pData->aCol, colIdx);
  TAOS_MULTI_BIND  ncharBind = {0};
  TAOS_MULTI_BIND* pBind = NULL;
  int32_t          code = 0;

  if (bind->num != rowNum) {
    return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
  }

  if (bind->buffer_type != pColSchema->type) {
    return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
  }

  if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
    code = convertStmtNcharCol(&pBuf, pColSchema, bind, &ncharBind);
    if (code) {
      goto _return;
    }
    pBind = &ncharBind;
  } else {
    pBind = bind;
  }

  tColDataAddValueByBind(pCol, pBind, IS_VAR_DATA_TYPE(pColSchema->type) ? pColSchema->bytes - VARSTR_HEADER_SIZE: -1);

  qDebug("stmt col %d bind %d rows data", colIdx, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t buildBoundFields(int32_t numOfBound, int16_t* boundColumns, SSchema* pSchema, int32_t* fieldNum,
                         TAOS_FIELD_E** fields, uint8_t timePrec) {
  if (fields) {
    *fields = taosMemoryCalloc(numOfBound, sizeof(TAOS_FIELD_E));
    if (NULL == *fields) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SSchema* schema = &pSchema[boundColumns[0]];
    if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
      (*fields)[0].precision = timePrec;
    }

    for (int32_t i = 0; i < numOfBound; ++i) {
      schema = &pSchema[boundColumns[i]];
      strcpy((*fields)[i].name, schema->name);
      (*fields)[i].type = schema->type;
      (*fields)[i].bytes = schema->bytes;
    }
  }

  *fieldNum = numOfBound;

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtTagFields(void* pBlock, void* boundTags, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SBoundColInfo* tags = (SBoundColInfo*)boundTags;
  if (NULL == tags) {
    return TSDB_CODE_APP_ERROR;
  }

  if (pDataBlock->pMeta->tableType != TSDB_SUPER_TABLE && pDataBlock->pMeta->tableType != TSDB_CHILD_TABLE) {
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }

  SSchema* pSchema = getTableTagSchema(pDataBlock->pMeta);
  if (tags->numOfBound <= 0) {
    *fieldNum = 0;
    *fields = NULL;

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(tags->numOfBound, tags->pColIndex, pSchema, fieldNum, fields, 0));

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtColFields(void* pBlock, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SSchema*       pSchema = getTableColumnSchema(pDataBlock->pMeta);
  if (pDataBlock->boundColsInfo.numOfBound <= 0) {
    *fieldNum = 0;
    if (fields) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(pDataBlock->boundColsInfo.numOfBound, pDataBlock->boundColsInfo.pColIndex, pSchema,
                              fieldNum, fields, pDataBlock->pMeta->tableInfo.precision));

  return TSDB_CODE_SUCCESS;
}

int32_t qResetStmtDataBlock(STableDataCxt* block, bool deepClear) {
  STableDataCxt* pBlock = (STableDataCxt*)block;
  int32_t        colNum = taosArrayGetSize(pBlock->pData->aCol);

  for (int32_t i = 0; i < colNum; ++i) {
    SColData* pCol = (SColData*)taosArrayGet(pBlock->pData->aCol, i);
    if (deepClear) {
      tColDataDeepClear(pCol);
    } else {
      tColDataClear(pCol);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qCloneStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, bool reset) {
  int32_t code = 0;

  *pDst = taosMemoryCalloc(1, sizeof(STableDataCxt));
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  STableDataCxt* pNewCxt = (STableDataCxt*)*pDst;
  STableDataCxt* pCxt = (STableDataCxt*)pSrc;
  pNewCxt->pSchema = NULL;
  pNewCxt->pValues = NULL;

  if (pCxt->pMeta) {
    void* pNewMeta = taosMemoryMalloc(TABLE_META_SIZE(pCxt->pMeta));
    if (NULL == pNewMeta) {
      insDestroyTableDataCxt(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(pNewMeta, pCxt->pMeta, TABLE_META_SIZE(pCxt->pMeta));
    pNewCxt->pMeta = pNewMeta;
  }

  memcpy(&pNewCxt->boundColsInfo, &pCxt->boundColsInfo, sizeof(pCxt->boundColsInfo));
  pNewCxt->boundColsInfo.pColIndex = NULL;

  if (pCxt->boundColsInfo.pColIndex) {
    void* pNewColIdx = taosMemoryMalloc(pCxt->boundColsInfo.numOfBound * sizeof(*pCxt->boundColsInfo.pColIndex));
    if (NULL == pNewColIdx) {
      insDestroyTableDataCxt(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(pNewColIdx, pCxt->boundColsInfo.pColIndex,
           pCxt->boundColsInfo.numOfBound * sizeof(*pCxt->boundColsInfo.pColIndex));
    pNewCxt->boundColsInfo.pColIndex = pNewColIdx;
  }

  if (pCxt->pData) {
    SSubmitTbData* pNewTb = (SSubmitTbData*)taosMemoryMalloc(sizeof(SSubmitTbData));
    if (NULL == pNewTb) {
      insDestroyTableDataCxt(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    memcpy(pNewTb, pCxt->pData, sizeof(*pCxt->pData));
    pNewTb->pCreateTbReq = NULL;

    pNewTb->aCol = taosArrayDup(pCxt->pData->aCol, NULL);
    if (NULL == pNewTb) {
      insDestroyTableDataCxt(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pNewCxt->pData = pNewTb;

    if (reset) {
      code = qResetStmtDataBlock(*pDst, true);
    }
  }

  return code;
}

int32_t qRebuildStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, uint64_t uid, uint64_t suid, int32_t vgId,
                              bool rebuildCreateTb) {
  int32_t code = qCloneStmtDataBlock(pDst, pSrc, false);
  if (code) {
    return code;
  }

  STableDataCxt* pBlock = (STableDataCxt*)*pDst;
  if (pBlock->pMeta) {
    pBlock->pMeta->uid = uid;
    pBlock->pMeta->vgId = vgId;
    pBlock->pMeta->suid = suid;
  }

  pBlock->pData->suid = suid;
  pBlock->pData->uid = uid;

  if (rebuildCreateTb && NULL == pBlock->pData->pCreateTbReq) {
    pBlock->pData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == pBlock->pData->pCreateTbReq) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

STableMeta* qGetTableMetaInDataBlock(STableDataCxt* pDataBlock) { return ((STableDataCxt*)pDataBlock)->pMeta; }

void qDestroyStmtDataBlock(STableDataCxt* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  insDestroyTableDataCxt(pDataBlock);
}
