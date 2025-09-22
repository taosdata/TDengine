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

#include "geosWrapper.h"
#include "os.h"
#include "parInsertUtil.h"
#include "parInt.h"
#include "parToken.h"
#include "query.h"
#include "tdataformat.h"
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
    return terrno;
  }

  int8_t         flag = 1;
  SSubmitTbData* pNew = *pData;

  *pNew = *pDataBlock->pData;
  pNew->pBlobSet = NULL;

  int32_t code = cloneSVreateTbReq(pDataBlock->pData->pCreateTbReq, &pNew->pCreateTbReq);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFreeClear(*pData);
    return code;
  }
  pNew->aCol = taosArrayDup(pDataBlock->pData->aCol, NULL);
  if (!pNew->aCol) {
    code = terrno;
    taosMemoryFreeClear(*pData);
    return code;
  }

  int32_t colNum = taosArrayGetSize(pNew->aCol);
  for (int32_t i = 0; i < colNum; ++i) {
    if (pDataBlock->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      SColData* pCol = (SColData*)taosArrayGet(pNew->aCol, i);
      tColDataDeepClear(pCol);
    } else {
      pNew->aCol = taosArrayInit(20, POINTER_BYTES);
      if (pNew->aCol == NULL) {
        code = terrno;
        taosMemoryFreeClear(*pData);
        return code;
      }
    }
  }

  return code;
}

int32_t qAppendStmtTableOutput(SQuery* pQuery, SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx,
                               SStbInterlaceInfo* pBuildInfo) {
  // merge according to vgId
  return insAppendStmtTableDataCxt(pAllVgHash, pTbData, pTbCtx, pBuildInfo);
}

int32_t qAppendStmt2TableOutput(SQuery* pQuery, SHashObj* pAllVgHash, STableColsData* pTbData, STableDataCxt* pTbCtx,
                                SStbInterlaceInfo* pBuildInfo, SVCreateTbReq* ctbReq) {
  // merge according to vgId
  return insAppendStmt2TableDataCxt(pAllVgHash, pTbData, pTbCtx, pBuildInfo, ctbReq);
}

int32_t qBuildStmtFinOutput(SQuery* pQuery, SHashObj* pAllVgHash, SArray* pVgDataBlocks) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildVgDataBlocks(pAllVgHash, pVgDataBlocks, &pStmt->pDataBlocks, true);
  }

  if (pStmt->freeArrayFunc) {
    pStmt->freeArrayFunc(pVgDataBlocks);
  }
  return code;
}

/*
int32_t qBuildStmtOutputFromTbList(SQuery* pQuery, SHashObj* pVgHash, SArray* pBlockList, STableDataCxt* pTbCtx, int32_t
tbNum) { int32_t             code = TSDB_CODE_SUCCESS; SArray*             pVgDataBlocks = NULL; SVnodeModifyOpStmt*
pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

  // merge according to vgId
  if (tbNum > 0) {
    code = insMergeStmtTableDataCxt(pTbCtx, pBlockList, &pVgDataBlocks, true, tbNum);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildVgDataBlocks(pVgHash, pVgDataBlocks, &pStmt->pDataBlocks);
  }

  if (pStmt->freeArrayFunc) {
    pStmt->freeArrayFunc(pVgDataBlocks);
  }
  return code;
}
*/

int32_t qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SArray*             pVgDataBlocks = NULL;
  SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pQuery->pRoot;

  // merge according to vgId
  if (taosHashGetSize(pBlockHash) > 0) {
    code = insMergeTableDataCxt(pBlockHash, &pVgDataBlocks, true);
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildVgDataBlocks(pVgHash, pVgDataBlocks, &pStmt->pDataBlocks, false);
  }

  if (pStmt->freeArrayFunc) {
    pStmt->freeArrayFunc(pVgDataBlocks);
  }
  return code;
}

int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, void* charsetCxt) {
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
      if (!bind[c].length) {
        code = buildInvalidOperationMsg(&pBuf, "var tag length is null");
        goto end;
      }
      colLen = bind[c].length[0];
      if ((colLen + VARSTR_HEADER_SIZE) > pTagSchema->bytes) {
        code = buildInvalidOperationMsg(&pBuf, "tag length is too big");
        goto end;
      }
    }
    if (NULL == taosArrayPush(tagName, pTagSchema->name)) {
      code = terrno;
      goto end;
    }
    if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
      if (colLen > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        code = buildSyntaxErrMsg(&pBuf, "json string too long than 4095", bind[c].buffer);
        goto end;
      }

      isJson = true;
      char* tmp = taosMemoryCalloc(1, colLen + 1);
      if (!tmp) {
        code = terrno;
        goto end;
      }
      memcpy(tmp, bind[c].buffer, colLen);
      code = parseJsontoTagData(tmp, pTagArray, &pTag, &pBuf, charsetCxt);
      taosMemoryFree(tmp);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    } else {
      STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
      //      strcpy(val.colName, pTagSchema->name);
      if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_VARBINARY ||
          pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
        if (pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
          code = initCtxAsText();
          if (code) {
            qError("geometry init failed:%s", tstrerror(code));
            goto end;
          }
          code = checkWKB(bind[c].buffer, colLen);
          if (code) {
            qError("stmt bind invalid geometry tag:%s, must be WKB format", (char*)bind[c].buffer);
            goto end;
          }
        }
        val.pData = (uint8_t*)bind[c].buffer;
        val.nData = colLen;
      } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t output = 0;
        void*   p = taosMemoryCalloc(1, colLen * TSDB_NCHAR_SIZE);
        if (p == NULL) {
          code = terrno;
          goto end;
        }
        if (!taosMbsToUcs4(bind[c].buffer, colLen, (TdUcs4*)(p), colLen * TSDB_NCHAR_SIZE, &output, charsetCxt)) {
          if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
            taosMemoryFree(p);
            code = generateSyntaxErrMsg(&pBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
            goto end;
          }
          char buf[512] = {0};
          snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(terrno));
          taosMemoryFree(p);
          code = buildSyntaxErrMsg(&pBuf, buf, bind[c].buffer);
          goto end;
        }
        val.pData = p;
        val.nData = output;
      } else {
        memcpy(&val.i64, bind[c].buffer, colLen);
      }
      if (IS_VAR_DATA_TYPE(pTagSchema->type) && val.nData > pTagSchema->bytes) {
        code = TSDB_CODE_PAR_VALUE_TOO_LONG;
        goto end;
      }
      if (NULL == taosArrayPush(pTagArray, &val)) {
        code = terrno;
        goto end;
      }
    }
  }

  if (!isJson && (code = tTagNew(pTagArray, 1, false, &pTag)) != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (NULL == pDataBlock->pData->pCreateTbReq) {
    pDataBlock->pData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == pDataBlock->pData->pCreateTbReq) {
      code = terrno;
      goto end;
    }
  } else {
    SVCreateTbReq* tmp = pDataBlock->pData->pCreateTbReq;
    taosMemoryFreeClear(tmp->name);
    taosMemoryFreeClear(tmp->ctb.pTag);
    taosMemoryFreeClear(tmp->ctb.stbName);
    taosArrayDestroy(tmp->ctb.tagName);
    tmp->ctb.tagName = NULL;
  }

  code = insBuildCreateTbReq(pDataBlock->pData->pCreateTbReq, tName, pTag, suid, sTableName, tagName,
                             pDataBlock->pMeta->tableInfo.numOfTags, TSDB_DEFAULT_TABLE_TTL);
  pTag = NULL;

end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  taosArrayDestroy(tagName);
  taosMemoryFree(pTag);

  return code;
}

int32_t convertStmtNcharCol(SMsgBuf* pMsgBuf, SSchema* pSchema, TAOS_MULTI_BIND* src, TAOS_MULTI_BIND* dst,
                            void* charsetCxt) {
  int32_t output = 0;
  int32_t newBuflen = (pSchema->bytes - VARSTR_HEADER_SIZE) * src->num;
  if (dst->buffer_length < newBuflen) {
    dst->buffer = taosMemoryRealloc(dst->buffer, newBuflen);
    if (NULL == dst->buffer) {
      return terrno;
    }
  }

  if (NULL == dst->length) {
    dst->length = taosMemoryRealloc(dst->length, sizeof(int32_t) * src->num);
    if (NULL == dst->length) {
      taosMemoryFreeClear(dst->buffer);
      return terrno;
    }
  }

  dst->buffer_length = pSchema->bytes - VARSTR_HEADER_SIZE;

  for (int32_t i = 0; i < src->num; ++i) {
    if (src->is_null && src->is_null[i]) {
      continue;
    }

    if (!taosMbsToUcs4(((char*)src->buffer) + src->buffer_length * i, src->length[i],
                       (TdUcs4*)(((char*)dst->buffer) + dst->buffer_length * i), dst->buffer_length, &output,
                       charsetCxt)) {
      if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(terrno));
      return buildSyntaxErrMsg(pMsgBuf, buf, NULL);
    }

    dst->length[i] = output;
  }

  dst->buffer_type = src->buffer_type;
  dst->is_null = src->is_null;
  dst->num = src->num;

  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtStbColsValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen,
                              STSchema** pTSchema, SBindInfo* pBindInfos, void* charsetCxt) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t          rowNum = bind->num;
  TAOS_MULTI_BIND  ncharBind = {0};
  TAOS_MULTI_BIND* pBind = NULL;
  int32_t          code = 0;
  int16_t          lastColId = -1;
  bool             colInOrder = true;

  if (NULL == pTSchema || NULL == *pTSchema) {
    *pTSchema = tBuildTSchema(pSchema, pDataBlock->pMeta->tableInfo.numOfColumns, pDataBlock->pMeta->sversion);
  }

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema* pColSchema = &pSchema[boundInfo->pColIndex[c]];
    if (pColSchema->colId <= lastColId) {
      colInOrder = false;
    } else {
      lastColId = pColSchema->colId;
    }
    // SColData* pCol = taosArrayGet(pCols, c);

    if (bind[c].num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bind[c].is_null && *bind[c].is_null)) &&
        bind[c].buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol(&pBuf, pColSchema, bind + c, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      pBind = &ncharBind;
    } else {
      pBind = bind + c;
    }

    pBindInfos[c].columnId = pColSchema->colId;
    pBindInfos[c].bind = pBind;
    pBindInfos[c].type = pColSchema->type;
  }

  code = tRowBuildFromBind(pBindInfos, boundInfo->numOfBound, colInOrder, *pTSchema, pCols, &pDataBlock->ordered,
                           &pDataBlock->duplicateTs);

  parserDebug("stmt all %d columns bind %d rows data", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t qBindStmtColsValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen,
                           void* charsetCxt) {
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
    SColData* pCol = taosArrayGet(pCols, c);

    if (bind[c].num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bind[c].is_null && *bind[c].is_null)) &&
        bind[c].buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol(&pBuf, pColSchema, bind + c, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      pBind = &ncharBind;
    } else {
      pBind = bind + c;
    }

    int32_t bytes = 0;
    if (IS_VAR_DATA_TYPE(pColSchema->type)) {
      if (IS_STR_DATA_BLOB(pColSchema->type)) {
        bytes = pColSchema->bytes - BLOBSTR_HEADER_SIZE;
      } else {
        bytes = pColSchema->bytes - VARSTR_HEADER_SIZE;
      }
    } else {
      bytes = -1;
    }
    code = tColDataAddValueByBind(pCol, pBind, bytes, initCtxAsText, checkWKB);
    if (code) {
      goto _return;
    }
  }

  parserDebug("stmt all %d columns bind %d rows data", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t qBindUpdateStmtColsValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen,
                                 void* charsetCxt, SSHashObj* parsedCols) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t          rowNum = bind->num;
  TAOS_MULTI_BIND  ncharBind = {0};
  TAOS_MULTI_BIND* pBind = NULL;
  int32_t          code = 0;
  int32_t          actualIndex = 0;
  int32_t          numOfBound = boundInfo->numOfBound + tSimpleHashGetSize(parsedCols);

  for (int c = 0; c < numOfBound; ++c) {
    if (tSimpleHashGet(parsedCols, &c, sizeof(c))) {
      continue;
    }
    SSchema*  pColSchema = &pSchema[boundInfo->pColIndex[actualIndex]];
    SColData* pCol = taosArrayGet(pCols, actualIndex);

    if (bind[c].num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bind[c].is_null && *bind[c].is_null)) &&
        bind[c].buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol(&pBuf, pColSchema, bind + c, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      pBind = &ncharBind;
    } else {
      pBind = bind + c;
    }

    int32_t bytes = 0;
    if (IS_VAR_DATA_TYPE(pColSchema->type)) {
      if (IS_STR_DATA_BLOB(pColSchema->type)) {
        bytes = pColSchema->bytes - BLOBSTR_HEADER_SIZE;
      } else {
        bytes = pColSchema->bytes - VARSTR_HEADER_SIZE;
      }
    } else {
      bytes = -1;
    }
    code = tColDataAddValueByBind(pCol, pBind, bytes, initCtxAsText, checkWKB);
    if (code) {
      goto _return;
    }
    actualIndex++;
  }

  parserDebug("stmt all %d columns bind %d rows data", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t qBindStmtSingleColValue(void* pBlock, SArray* pCols, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen,
                                int32_t colIdx, int32_t rowNum, void* charsetCxt) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  SSchema*         pColSchema = &pSchema[boundInfo->pColIndex[colIdx]];
  SColData*        pCol = taosArrayGet(pCols, colIdx);
  TAOS_MULTI_BIND  ncharBind = {0};
  TAOS_MULTI_BIND* pBind = NULL;
  int32_t          code = 0;

  if (bind->num != rowNum) {
    return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
  }

  // Column index exceeds the number of columns
  if (colIdx >= pCols->size && pCol == NULL) {
    return buildInvalidOperationMsg(&pBuf, "column index exceeds the number of columns");
  }

  if (bind->buffer_type != pColSchema->type) {
    return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
  }

  if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
    code = convertStmtNcharCol(&pBuf, pColSchema, bind, &ncharBind, charsetCxt);
    if (code) {
      goto _return;
    }
    pBind = &ncharBind;
  } else {
    pBind = bind;
  }
  
  code = tColDataAddValueByBind(pCol, pBind,
                                IS_VAR_DATA_TYPE(pColSchema->type) ? pColSchema->bytes - VARSTR_HEADER_SIZE : -1,
                                initCtxAsText, checkWKB);

  parserDebug("stmt col %d bind %d rows data", colIdx, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t qBindStmtTagsValue2(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                            TAOS_STMT2_BIND* bind, char* msgBuf, int32_t msgBufLen, void* charsetCxt,
                            SVCreateTbReq* pCreateTbReq) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SMsgBuf        pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t        code = TSDB_CODE_SUCCESS;
  SBoundColInfo* tags = (SBoundColInfo*)boundTags;
  if (NULL == tags) {
    return buildInvalidOperationMsg(&pBuf, "tags is null");
  }

  SArray* pTagArray;
  if (tags->parseredTags) {
    pTagArray = taosArrayDup(tags->parseredTags->pTagVals, NULL);
  } else {
    pTagArray = taosArrayInit(tags->numOfBound, sizeof(STagVal));
  }
  if (!pTagArray) {
    return buildInvalidOperationMsg(&pBuf, "out of memory");
  }

  SArray* tagName;
  if (tags->parseredTags) {
    tagName = taosArrayDup(tags->parseredTags->STagNames, NULL);
  } else {
    tagName = taosArrayInit(8, TSDB_COL_NAME_LEN);
  }

  if (!tagName) {
    code = buildInvalidOperationMsg(&pBuf, "out of memory");
    goto end;
  }

  SSchema* pSchema = getTableTagSchema(pDataBlock->pMeta);

  bool  isJson = false;
  STag* pTag = NULL;

  int bindIdx = 0;
  for (int c = 0; c < tags->numOfBound; ++c) {
    if (bind == NULL) {
      break;
    }
    if (tags->parseredTags) {
      bool found = false;
      for (int k = 0; k < tags->parseredTags->numOfTags; k++) {
        if (tags->parseredTags->pTagIndex[k] == tags->pColIndex[c]) {
          found = true;
          break;
        }
      }
      if (found) {
        continue;
      }
    }

    TAOS_STMT2_BIND bindData = bind[bindIdx++];

    if (bindData.is_null && bindData.is_null[0]) {
      continue;
    }

    SSchema* pTagSchema = &pSchema[tags->pColIndex[c]];
    int32_t  colLen = pTagSchema->bytes;
    if (IS_VAR_DATA_TYPE(pTagSchema->type)) {
      if (IS_STR_DATA_BLOB(pTagSchema->type)) {
        return TSDB_CODE_BLOB_NOT_SUPPORT_TAG;
      }
      if (!bindData.length) {
        code = buildInvalidOperationMsg(&pBuf, "var tag length is null");
        goto end;
      }
      colLen = bindData.length[0];
      if ((colLen + VARSTR_HEADER_SIZE) > pTagSchema->bytes) {
        code = buildInvalidOperationMsg(&pBuf, "tag length is too big");
        goto end;
      }
    }
    if (NULL == taosArrayPush(tagName, pTagSchema->name)) {
      code = terrno;
      goto end;
    }
    if (pTagSchema->type == TSDB_DATA_TYPE_JSON) {
      if (colLen > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        code = buildSyntaxErrMsg(&pBuf, "json string too long than 4095", bindData.buffer);
        goto end;
      }

      isJson = true;
      char* tmp = taosMemoryCalloc(1, colLen + 1);
      if (!tmp) {
        code = terrno;
        goto end;
      }
      memcpy(tmp, bindData.buffer, colLen);
      code = parseJsontoTagData(tmp, pTagArray, &pTag, &pBuf, charsetCxt);
      taosMemoryFree(tmp);
      if (code != TSDB_CODE_SUCCESS) {
        goto end;
      }
    } else {
      STagVal val = {.cid = pTagSchema->colId, .type = pTagSchema->type};
      //      strcpy(val.colName, pTagSchema->name);
      if (pTagSchema->type == TSDB_DATA_TYPE_BINARY || pTagSchema->type == TSDB_DATA_TYPE_VARBINARY ||
          pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
        if (pTagSchema->type == TSDB_DATA_TYPE_GEOMETRY) {
          code = initCtxAsText();
          if (code) {
            qError("geometry init failed:%s", tstrerror(code));
            goto end;
          }
          code = checkWKB(bindData.buffer, colLen);
          if (code) {
            qError("stmt2 bind invalid geometry tag:%s, must be WKB format", (char*)bindData.buffer);
            goto end;
          }
        }
        val.pData = (uint8_t*)bindData.buffer;
        val.nData = colLen;
      } else if (pTagSchema->type == TSDB_DATA_TYPE_NCHAR) {
        int32_t output = 0;
        void*   p = taosMemoryCalloc(1, colLen * TSDB_NCHAR_SIZE);
        if (p == NULL) {
          code = terrno;
          goto end;
        }
        if (colLen != 0) {
          if (!taosMbsToUcs4(bindData.buffer, colLen, (TdUcs4*)(p), colLen * TSDB_NCHAR_SIZE, &output, charsetCxt)) {
            if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
              taosMemoryFree(p);
              code = generateSyntaxErrMsg(&pBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pTagSchema->name);
              goto end;
            }
            char buf[512] = {0};
            snprintf(buf, tListLen(buf), " taosMbsToUcs4 error:%s", strerror(terrno));
            taosMemoryFree(p);
            code = buildSyntaxErrMsg(&pBuf, buf, bindData.buffer);
            goto end;
          }
        }
        val.pData = p;
        val.nData = output;

      } else {
        memcpy(&val.i64, bindData.buffer, colLen);
      }
      if (IS_VAR_DATA_TYPE(pTagSchema->type) && val.nData > pTagSchema->bytes) {
        code = TSDB_CODE_PAR_VALUE_TOO_LONG;
        goto end;
      }
      if (NULL == taosArrayPush(pTagArray, &val)) {
        code = terrno;
        goto end;
      }
    }
  }

  if (!isJson && (code = tTagNew(pTagArray, 1, false, &pTag)) != TSDB_CODE_SUCCESS) {
    goto end;
  }

  if (pCreateTbReq) {
    code = insBuildCreateTbReq(pCreateTbReq, tName, pTag, suid, sTableName, tagName,
                               pDataBlock->pMeta->tableInfo.numOfTags, TSDB_DEFAULT_TABLE_TTL);
    pTag = NULL;
    goto end;
  }

  if (NULL == pDataBlock->pData->pCreateTbReq) {
    pDataBlock->pData->pCreateTbReq = taosMemoryCalloc(1, sizeof(SVCreateTbReq));
    if (NULL == pDataBlock->pData->pCreateTbReq) {
      code = terrno;
      goto end;
    }
  } else {
    SVCreateTbReq* tmp = pDataBlock->pData->pCreateTbReq;
    taosMemoryFreeClear(tmp->name);
    taosMemoryFreeClear(tmp->ctb.pTag);
    taosMemoryFreeClear(tmp->ctb.stbName);
    taosArrayDestroy(tmp->ctb.tagName);
    tmp->ctb.tagName = NULL;
  }

  code = insBuildCreateTbReq(pDataBlock->pData->pCreateTbReq, tName, pTag, suid, sTableName, tagName,
                             pDataBlock->pMeta->tableInfo.numOfTags, TSDB_DEFAULT_TABLE_TTL);
  pTag = NULL;

end:
  for (int i = 0; i < taosArrayGetSize(pTagArray); ++i) {
    STagVal* p = (STagVal*)taosArrayGet(pTagArray, i);
    if (p->type == TSDB_DATA_TYPE_NCHAR) {
      taosMemoryFreeClear(p->pData);
    }
  }
  taosArrayDestroy(pTagArray);
  taosArrayDestroy(tagName);
  taosMemoryFree(pTag);

  return code;
}

static int32_t convertStmtStbNcharCol2(SMsgBuf* pMsgBuf, SSchema* pSchema, TAOS_STMT2_BIND* src, TAOS_STMT2_BIND* dst,
                                       void* charsetCxt) {
  int32_t       output = 0;
  const int32_t max_buf_len = pSchema->bytes - VARSTR_HEADER_SIZE;

  dst->buffer = taosMemoryCalloc(src->num, max_buf_len);
  if (NULL == dst->buffer) {
    return terrno;
  }

  dst->length = taosMemoryCalloc(src->num, sizeof(int32_t));
  if (NULL == dst->length) {
    taosMemoryFreeClear(dst->buffer);
    return terrno;
  }

  char* src_buf = src->buffer;
  char* dst_buf = dst->buffer;
  for (int32_t i = 0; i < src->num; ++i) {
    if (src->is_null && src->is_null[i]) {
      continue;
    }

    if (src->length[i] == 0) {
      dst->length[i] = 0;
      continue;
    }

    if (!taosMbsToUcs4(src_buf, src->length[i], (TdUcs4*)dst_buf, max_buf_len, &output, charsetCxt)) {
      if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(terrno));
      return buildSyntaxErrMsg(pMsgBuf, buf, NULL);
    }

    dst->length[i] = output;
    src_buf += src->length[i];
    dst_buf += output;
  }

  dst->buffer_type = src->buffer_type;
  dst->is_null = src->is_null;
  dst->num = src->num;

  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtStbColsValue2(void* pBlock, SArray* pCols, SSHashObj* parsedCols, TAOS_STMT2_BIND* bind, char* msgBuf,
                               int32_t msgBufLen, STSchema** pTSchema, SBindInfo2* pBindInfos, void* charsetCxt,
                               SBlobSet** ppBlob) {
  STableDataCxt*  pDataBlock = (STableDataCxt*)pBlock;
  SSchema*        pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*  boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf         pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t         rowNum = bind->num;
  SArray*         ncharBinds = NULL;
  TAOS_STMT2_BIND ncharBind = {0};
  int32_t         code = 0;
  int16_t         lastColId = -1;
  bool            colInOrder = true;
  int             ncharColNums = 0;
  int32_t         bindIdx = 0;
  int8_t          hasBlob = 0;
  int32_t         lino = 0;
  if (NULL == pTSchema || NULL == *pTSchema) {
    *pTSchema = tBuildTSchema(pSchema, pDataBlock->pMeta->tableInfo.numOfColumns, pDataBlock->pMeta->sversion);
  }

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    if (TSDB_DATA_TYPE_NCHAR == pSchema[boundInfo->pColIndex[c]].type) {
      ncharColNums++;
    }
  }
  if (ncharColNums > 0) {
    ncharBinds = taosArrayInit(ncharColNums, sizeof(ncharBind));
    if (!ncharBinds) {
      code = terrno;
      goto _return;
    }
  }

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema* pColSchema = &pSchema[boundInfo->pColIndex[c]];
    if (pColSchema->colId <= lastColId) {
      colInOrder = false;
    } else {
      lastColId = pColSchema->colId;
    }

    if (parsedCols) {
      SColVal* pParsedVal = tSimpleHashGet(parsedCols, &pColSchema->colId, sizeof(int16_t));
      if (pParsedVal) {
        pBindInfos[c].columnId = pColSchema->colId;
        pBindInfos[c].type = pColSchema->type;
        pBindInfos[c].bytes = pColSchema->bytes;
        continue;
      }
    }

    TAOS_STMT2_BIND bindData = bind[bindIdx];

    if (bindData.num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bindData.is_null && *bindData.is_null)) &&
        bindData.buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtStbNcharCol2(&pBuf, pColSchema, bind + bindIdx, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      if (!taosArrayPush(ncharBinds, &ncharBind)) {
        code = terrno;
        goto _return;
      }
      pBindInfos[c].bind = taosArrayGetLast(ncharBinds);
    } else if (TSDB_DATA_TYPE_GEOMETRY == pColSchema->type) {
      code = initCtxAsText();
      if (code) {
        qError("geometry init failed:%s", tstrerror(code));
        goto _return;
      }
      uint8_t* buf = bindData.buffer;
      for (int j = 0; j < bindData.num; j++) {
        if (bindData.is_null && bindData.is_null[j]) {
          continue;
        }
        code = checkWKB(buf, bindData.length[j]);
        if (code) {
          qError("stmt2 interlace mode geometry data[%d]:{%s},length:%d must be in WKB format", c, buf,
                 bindData.length[j]);
          goto _return;
        }
        buf += bindData.length[j];
      }
      pBindInfos[c].bind = bind + bindIdx;
    } else {
      if (IS_STR_DATA_BLOB(pColSchema->type)) hasBlob = 1;
      pBindInfos[c].bind = bind + bindIdx;
    }

    pBindInfos[c].columnId = pColSchema->colId;
    pBindInfos[c].type = pColSchema->type;
    pBindInfos[c].bytes = pColSchema->bytes;

    bindIdx++;
  }

  if (hasBlob == 0) {
    code = tRowBuildFromBind2(pBindInfos, boundInfo->numOfBound, parsedCols, colInOrder, *pTSchema, pCols,
                              &pDataBlock->ordered, &pDataBlock->duplicateTs);
  } else {
    code = tBlobSetCreate(1024, 1, ppBlob);
    TAOS_CHECK_GOTO(code, &lino, _return);

    code = tRowBuildFromBind2WithBlob(pBindInfos, boundInfo->numOfBound, colInOrder, *pTSchema, pCols,
                                      &pDataBlock->ordered, &pDataBlock->duplicateTs, *ppBlob);
    TAOS_CHECK_GOTO(code, &lino, _return);
  }

  parserDebug("stmt all %d columns bind %d rows data", boundInfo->numOfBound, rowNum);

_return:
  if (ncharBinds) {
    for (int i = 0; i < TARRAY_SIZE(ncharBinds); ++i) {
      TAOS_STMT2_BIND* ncBind = TARRAY_DATA(ncharBinds);
      taosMemoryFree(ncBind[i].buffer);
      taosMemoryFree(ncBind[i].length);
    }
    taosArrayDestroy(ncharBinds);
  }
  if (code != 0) {
    parserDebug("stmt2 failed to bind at lino %d since %s", lino, tstrerror(code));
  }

  return code;
}

static int32_t convertStmtNcharCol2(SMsgBuf* pMsgBuf, SSchema* pSchema, TAOS_STMT2_BIND* src, TAOS_STMT2_BIND* dst,
                                    void* charsetCxt) {
  int32_t       output = 0;
  const int32_t max_buf_len = pSchema->bytes - VARSTR_HEADER_SIZE;

  int32_t newBuflen = (pSchema->bytes - VARSTR_HEADER_SIZE) * src->num;
  // if (dst->buffer_length < newBuflen) {
  dst->buffer = taosMemoryRealloc(dst->buffer, newBuflen);
  if (NULL == dst->buffer) {
    return terrno;
  }
  //}

  if (NULL == dst->length) {
    dst->length = taosMemoryRealloc(dst->length, sizeof(int32_t) * src->num);
    if (NULL == dst->length) {
      taosMemoryFreeClear(dst->buffer);
      return terrno;
    }
  }

  // dst->buffer_length = pSchema->bytes - VARSTR_HEADER_SIZE;
  char* src_buf = src->buffer;
  char* dst_buf = dst->buffer;
  for (int32_t i = 0; i < src->num; ++i) {
    if (src->is_null && src->is_null[i]) {
      continue;
    }

    if (src->length[i] == 0) {
      dst->length[i] = 0;
      continue;
    }

    /*if (!taosMbsToUcs4(((char*)src->buffer) + src->buffer_length * i, src->length[i],
      (TdUcs4*)(((char*)dst->buffer) + dst->buffer_length * i), dst->buffer_length, &output)) {*/
    if (!taosMbsToUcs4(src_buf, src->length[i], (TdUcs4*)dst_buf, max_buf_len, &output, charsetCxt)) {
      if (terrno == TAOS_SYSTEM_ERROR(E2BIG)) {
        return generateSyntaxErrMsg(pMsgBuf, TSDB_CODE_PAR_VALUE_TOO_LONG, pSchema->name);
      }
      char buf[512] = {0};
      snprintf(buf, tListLen(buf), "%s", strerror(terrno));
      return buildSyntaxErrMsg(pMsgBuf, buf, NULL);
    }

    dst->length[i] = output;
    src_buf += src->length[i];
    dst_buf += output;
  }

  dst->buffer_type = src->buffer_type;
  dst->is_null = src->is_null;
  dst->num = src->num;

  return TSDB_CODE_SUCCESS;
}

int32_t qBindStmtColsValue2(void* pBlock, SArray* pCols, SSHashObj* parsedCols, TAOS_STMT2_BIND* bind, char* msgBuf,
                            int32_t msgBufLen, void* charsetCxt) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t          rowNum = bind->num;
  TAOS_STMT2_BIND  ncharBind = {0};
  TAOS_STMT2_BIND* pBind = NULL;
  int32_t          code = 0;
  int32_t          lino = 0;
  int32_t          bindIdx = 0;

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema*  pColSchema = &pSchema[boundInfo->pColIndex[c]];
    SColData* pCol = taosArrayGet(pCols, c);
    if (pCol == NULL || pColSchema == NULL) {
      code = buildInvalidOperationMsg(&pBuf, "get column schema or column data failed");
      goto _return;
    }

    if (boundInfo->pColIndex[c] == 0) {
      pCol->cflag |= COL_IS_KEY;
    }

    if (parsedCols) {
      SColVal* pParsedVal = tSimpleHashGet(parsedCols, &pColSchema->colId, sizeof(int16_t));
      if (pParsedVal) {
        for (int row = 0; row < rowNum; row++) {
          code = tColDataAppendValue(pCol, pParsedVal);
          if (code) {
            goto _return;
          }
        }
        continue;
      }
    }
    TAOS_STMT2_BIND bindData = bind[bindIdx];
    if (bindData.num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bindData.is_null && *bindData.is_null)) &&
        bindData.buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol2(&pBuf, pColSchema, bind + bindIdx, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      pBind = &ncharBind;
    } else {
      pBind = bind + bindIdx;
    }
    int8_t  isBlob = 0;
    int32_t bytes = -1;
    if (IS_VAR_DATA_TYPE(pColSchema->type)) {
      if (IS_STR_DATA_BLOB(pColSchema->type)) {
        isBlob = 1;
        bytes = TSDB_MAX_BLOB_LEN;
      } else {
        bytes = pColSchema->bytes - VARSTR_HEADER_SIZE;
      }
    }
    if (isBlob == 0) {
      code = tColDataAddValueByBind2(pCol, pBind, bytes, initCtxAsText, checkWKB);
    } else {
      if (pDataBlock->pData->pBlobSet == NULL) {
        code = tBlobSetCreate(1024, 1, &pDataBlock->pData->pBlobSet);
        TAOS_CHECK_GOTO(code, &lino, _return);
      }
      code = tColDataAddValueByBind2WithBlob(pCol, pBind, bytes, pDataBlock->pData->pBlobSet);
    }

    if (code) {
      goto _return;
    }
    bindIdx++;
  }

  parserDebug("stmt2 all %d columns bind %d rows data as col format", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);
  if (code != 0) {
    parserDebug("stmt2 failed to bind col at lino %d since %s", lino, tstrerror(code));
  }

  return code;
}

int32_t qBindStmtSingleColValue2(void* pBlock, SArray* pCols, TAOS_STMT2_BIND* bind, char* msgBuf, int32_t msgBufLen,
                                 int32_t colIdx, int32_t rowNum, void* charsetCxt) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  SSchema*         pColSchema = &pSchema[boundInfo->pColIndex[colIdx]];
  SColData*        pCol = taosArrayGet(pCols, colIdx);
  TAOS_STMT2_BIND  ncharBind = {0};
  TAOS_STMT2_BIND* pBind = NULL;
  int32_t          code = 0;
  int32_t          lino = 0;

  if (bind->num != rowNum) {
    return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
  }

  // Column index exceeds the number of columns
  if (colIdx >= pCols->size && pCol == NULL) {
    return buildInvalidOperationMsg(&pBuf, "column index exceeds the number of columns");
  }

  if (bind->buffer_type != pColSchema->type) {
    return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
  }

  if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
    code = convertStmtNcharCol2(&pBuf, pColSchema, bind, &ncharBind, charsetCxt);
    if (code) {
      goto _return;
    }
    pBind = &ncharBind;
  } else {
    pBind = bind;
  }

  int32_t bytes = -1;
  int8_t  hasBlob = 0;
  if (IS_VAR_DATA_TYPE(pColSchema->type)) {
    if (IS_STR_DATA_BLOB(pColSchema->type)) {
      bytes = TSDB_MAX_BLOB_LEN;
      hasBlob = 1;
    } else {
      bytes = pColSchema->bytes - VARSTR_HEADER_SIZE;
    }
  } else {
    bytes = -1;
  }

  if (hasBlob) {
    if (pDataBlock->pData->pBlobSet == NULL) {
      code = tBlobSetCreate(1024, 1, &pDataBlock->pData->pBlobSet);
      TAOS_CHECK_GOTO(code, &lino, _return);
    }
    code = tColDataAddValueByBind2WithBlob(pCol, pBind, bytes, pDataBlock->pData->pBlobSet);
  } else {
    code = tColDataAddValueByBind2(pCol, pBind, bytes, initCtxAsText, checkWKB);
  }

  parserDebug("stmt col %d bind %d rows data", colIdx, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);
  if (code != 0) {
    parserDebug("stmt failed to parse at lino %d since %s", lino, tstrerror(code));
  }

  return code;
}

int32_t qBindStmt2RowValue(void* pBlock, SArray* pCols, SSHashObj* parsedCols, TAOS_STMT2_BIND* bind, char* msgBuf,
                           int32_t msgBufLen, STSchema** pTSchema, SBindInfo2* pBindInfos, void* charsetCxt) {
  STableDataCxt*   pDataBlock = (STableDataCxt*)pBlock;
  SSchema*         pSchema = getTableColumnSchema(pDataBlock->pMeta);
  SBoundColInfo*   boundInfo = &pDataBlock->boundColsInfo;
  SMsgBuf          pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t          rowNum = bind->num;
  TAOS_STMT2_BIND  ncharBind = {0};
  TAOS_STMT2_BIND* pBind = NULL;
  int32_t          code = 0;
  int16_t          lastColId = -1;
  bool             colInOrder = true;
  int8_t           hasBlob = 0;

  if (NULL == pTSchema || NULL == *pTSchema) {
    *pTSchema = tBuildTSchema(pSchema, pDataBlock->pMeta->tableInfo.numOfColumns, pDataBlock->pMeta->sversion);
  }

  for (int c = 0; c < boundInfo->numOfBound; ++c) {
    SSchema* pColSchema = &pSchema[boundInfo->pColIndex[c]];
    if (pColSchema->colId <= lastColId) {
      colInOrder = false;
    } else {
      lastColId = pColSchema->colId;
    }

    if (bind[c].num != rowNum) {
      code = buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      goto _return;
    }

    if ((!(rowNum == 1 && bind[c].is_null && *bind[c].is_null)) &&
        bind[c].buffer_type != pColSchema->type) {  // for rowNum ==1 , connector may not set buffer_type
      code = buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      goto _return;
    }

    if (TSDB_DATA_TYPE_NCHAR == pColSchema->type) {
      code = convertStmtNcharCol2(&pBuf, pColSchema, bind + c, &ncharBind, charsetCxt);
      if (code) {
        goto _return;
      }
      pBindInfos[c].bind = &ncharBind;
    } else if (TSDB_DATA_TYPE_GEOMETRY == pColSchema->type) {
      code = initCtxAsText();
      if (code) {
        qError("geometry init failed:%s", tstrerror(code));
        goto _return;
      }
      uint8_t* buf = bind[c].buffer;
      for (int j = 0; j < bind[c].num; j++) {
        if (bind[c].is_null && bind[c].is_null[j]) {
          continue;
        }
        code = checkWKB(buf, bind[c].length[j]);
        if (code) {
          qError("stmt2 row bind geometry data[%d]:{%s},length:%d must be in WKB format", c, buf, bind[c].length[j]);
          goto _return;
        }
        buf += bind[c].length[j];
      }
      pBindInfos[c].bind = bind + c;
    } else {
      if (IS_STR_DATA_BLOB(pColSchema->type)) {
        hasBlob = 1;
      }
      pBindInfos[c].bind = bind + c;
    }

    pBindInfos[c].columnId = pColSchema->colId;
    pBindInfos[c].type = pColSchema->type;
    pBindInfos[c].bytes = pColSchema->bytes;

    if (code) {
      goto _return;
    }
  }

  pDataBlock->pData->flags &= ~SUBMIT_REQ_COLUMN_DATA_FORMAT;
  if (pDataBlock->pData->pCreateTbReq != NULL) {
    pDataBlock->pData->flags |= SUBMIT_REQ_AUTO_CREATE_TABLE;
  }

  if (hasBlob == 0) {
    code = tRowBuildFromBind2(pBindInfos, boundInfo->numOfBound, parsedCols, colInOrder, *pTSchema, pCols,
                            &pDataBlock->ordered, &pDataBlock->duplicateTs);
  } else {
    code = TSDB_CODE_BLOB_NOT_SUPPORT;
  }
  qDebug("stmt2 all %d columns bind %d rows data as row format", boundInfo->numOfBound, rowNum);

_return:

  taosMemoryFree(ncharBind.buffer);
  taosMemoryFree(ncharBind.length);

  return code;
}

int32_t buildBoundFields(int32_t numOfBound, int16_t* boundColumns, SSchema* pSchema, int32_t* fieldNum,
                         TAOS_FIELD_E** fields, uint8_t timePrec) {
  if (fields != NULL) {
    *fields = taosMemoryCalloc(numOfBound, sizeof(TAOS_FIELD_E));
    if (NULL == *fields) {
      return terrno;
    }

    SSchema* schema = &pSchema[boundColumns[0]];
    if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
      (*fields)[0].precision = timePrec;
    }

    for (int32_t i = 0; i < numOfBound; ++i) {
      schema = &pSchema[boundColumns[i]];
      tstrncpy((*fields)[i].name, schema->name, 65);
      (*fields)[i].type = schema->type;
      (*fields)[i].bytes = calcTypeBytesFromSchemaBytes(schema->type, schema->bytes, true);
    }
  }

  *fieldNum = numOfBound;

  return TSDB_CODE_SUCCESS;
}

int32_t buildStbBoundFields(SBoundColInfo boundColsInfo, SSchema* pSchema, int32_t* fieldNum, TAOS_FIELD_ALL** fields,
                            STableMeta* pMeta, void* boundTags, SSHashObj* parsedCols, uint8_t tbNameFlag) {
  SBoundColInfo* tags = (SBoundColInfo*)boundTags;
  bool           hastag = (tags != NULL) && !(tbNameFlag & IS_FIXED_TAG);
  bool           hasPreBindTbname =
      (tbNameFlag & IS_FIXED_VALUE) == 0 && ((tbNameFlag & USING_CLAUSE) != 0 || pMeta->tableType == TSDB_NORMAL_TABLE);
  int32_t numOfBound = boundColsInfo.numOfBound + (hasPreBindTbname ? 1 : 0);
  if (hastag) {
    numOfBound += tags->mixTagsCols ? 0 : tags->numOfBound;
  }

  // Adjust the number of bound fields if there are parsed tags or parsed columns
  if (tags->parseredTags) {
    numOfBound -= tags->parseredTags->numOfTags;
  }
  if (parsedCols) {
    numOfBound -= tSimpleHashGetSize(parsedCols);
  }

  int32_t idx = 0;
  if (fields != NULL) {
    *fields = taosMemoryCalloc(numOfBound, sizeof(TAOS_FIELD_ALL));
    if (NULL == *fields) {
      return terrno;
    }

    if (hasPreBindTbname) {
      (*fields)[idx].field_type = TAOS_FIELD_TBNAME;
      tstrncpy((*fields)[idx].name, "tbname", sizeof((*fields)[idx].name));
      (*fields)[idx].type = TSDB_DATA_TYPE_BINARY;
      (*fields)[idx].bytes = TSDB_TABLE_FNAME_LEN;
      idx++;
    }

    if (hastag && tags->numOfBound > 0 && !tags->mixTagsCols) {
      SSchema* tagSchema = getTableTagSchema(pMeta);

      for (int32_t i = 0; i < tags->numOfBound; ++i) {
        SSchema* schema = &tagSchema[tags->pColIndex[i]];

        if (tags->parseredTags && tags->parseredTags->numOfTags > 0) {
          int32_t tag_idx = schema->colId - 1 - pMeta->tableInfo.numOfColumns;
          bool    found = false;
          for (int k = 0; k < tags->parseredTags->numOfTags; k++) {
            if (tags->parseredTags->pTagIndex[k] == tag_idx) {
              found = true;
              break;
            }
          }
          if (found) {
            continue;
          }
        }

        (*fields)[idx].field_type = TAOS_FIELD_TAG;

        tstrncpy((*fields)[idx].name, schema->name, sizeof((*fields)[i].name));
        (*fields)[idx].type = schema->type;
        (*fields)[idx].bytes = schema->bytes;
        if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
          (*fields)[idx].precision = pMeta->tableInfo.precision;
        }
        idx++;
      }
    }

    if (boundColsInfo.numOfBound > 0) {
      SSchema* schema = &pSchema[boundColsInfo.pColIndex[0]];

      for (int32_t i = 0; i < boundColsInfo.numOfBound; ++i) {
        int16_t idxCol = boundColsInfo.pColIndex[i];

        if (idxCol == pMeta->tableInfo.numOfColumns + pMeta->tableInfo.numOfTags) {
          (*fields)[idx].field_type = TAOS_FIELD_TBNAME;
          tstrncpy((*fields)[idx].name, "tbname", sizeof((*fields)[idx].name));
          (*fields)[idx].type = TSDB_DATA_TYPE_BINARY;
          (*fields)[idx].bytes = TSDB_TABLE_FNAME_LEN;

          idx++;
          continue;
        } else if (idxCol < pMeta->tableInfo.numOfColumns) {
          if (parsedCols && tSimpleHashGet(parsedCols, &pSchema[idxCol].colId, sizeof(int16_t))) {
            continue;
          }
          (*fields)[idx].field_type = TAOS_FIELD_COL;
        } else {
          if (tags->parseredTags && tags->parseredTags->numOfTags > 0) {
            int32_t tag_idx = idxCol - pMeta->tableInfo.numOfColumns;
            bool    found = false;
            for (int k = 0; k < tags->parseredTags->numOfTags; k++) {
              if (tags->parseredTags->pTagIndex[k] == tag_idx) {
                found = true;
                break;
              }
            }
            if (found) {
              continue;
            }
          }
          (*fields)[idx].field_type = TAOS_FIELD_TAG;
        }

        schema = &pSchema[idxCol];
        tstrncpy((*fields)[idx].name, schema->name, sizeof((*fields)[idx].name));
        (*fields)[idx].type = schema->type;
        (*fields)[idx].bytes = schema->bytes;
        if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
          (*fields)[idx].precision = pMeta->tableInfo.precision;
        }
        idx++;
      }
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
    if (fields != NULL) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(pDataBlock->boundColsInfo.numOfBound, pDataBlock->boundColsInfo.pColIndex, pSchema,
                              fieldNum, fields, pDataBlock->pMeta->tableInfo.precision));

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildUpdateStmtColFields(void* pBlock, int32_t* fieldNum, TAOS_FIELD_E** fields, SSHashObj* parsedCols) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SSchema*       pSchema = getTableColumnSchema(pDataBlock->pMeta);
  int32_t        numOfBound = pDataBlock->boundColsInfo.numOfBound + tSimpleHashGetSize(parsedCols);
  if (numOfBound <= 0) {
    *fieldNum = 0;
    if (fields != NULL) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  if (fields != NULL) {
    *fields = taosMemoryCalloc(numOfBound, sizeof(TAOS_FIELD_E));
    if (NULL == *fields) {
      return terrno;
    }

    int32_t actualIdx = 0;
    for (int32_t i = 0; i < numOfBound; ++i) {
      SSchema* schema;
      int32_t* idx = (int32_t*)tSimpleHashGet(parsedCols, &i, sizeof(int32_t));
      if (idx) {
        schema = &pSchema[*idx];
      } else {
        schema = &pSchema[pDataBlock->boundColsInfo.pColIndex[actualIdx++]];
      }
      tstrncpy((*fields)[i].name, schema->name, 65);
      (*fields)[i].type = schema->type;
      (*fields)[i].bytes = calcTypeBytesFromSchemaBytes(schema->type, schema->bytes, true);
      if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
        (*fields)[i].precision = pDataBlock->pMeta->tableInfo.precision;
      }
    }
  }

  *fieldNum = numOfBound;

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtStbColFields(void* pBlock, void* boundTags, SSHashObj* parsedCols, uint8_t tbNameFlag,
                               int32_t* fieldNum, TAOS_FIELD_ALL** fields) {
  STableDataCxt* pDataBlock = (STableDataCxt*)pBlock;
  SSchema*       pSchema = getTableColumnSchema(pDataBlock->pMeta);
  if (pDataBlock->boundColsInfo.numOfBound <= 0) {
    *fieldNum = 0;
    if (fields != NULL) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildStbBoundFields(pDataBlock->boundColsInfo, pSchema, fieldNum, fields, pDataBlock->pMeta, boundTags,
                                 parsedCols, tbNameFlag));

  return TSDB_CODE_SUCCESS;
}

int32_t qResetStmtColumns(SArray* pCols, bool deepClear) {
  int32_t colNum = taosArrayGetSize(pCols);

  for (int32_t i = 0; i < colNum; ++i) {
    SColData* pCol = (SColData*)taosArrayGet(pCols, i);
    if (pCol == NULL) {
      parserError("qResetStmtColumns column:%d is NULL", i);
      return terrno;
    }
    if (deepClear) {
      tColDataDeepClear(pCol);
    } else {
      tColDataClear(pCol);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qResetStmtDataBlock(STableDataCxt* block, bool deepClear) {
  int32_t        code = 0;
  STableDataCxt* pBlock = (STableDataCxt*)block;
  int32_t        colNum = taosArrayGetSize(pBlock->pData->aCol);

  int8_t flag = 0;
  for (int32_t i = 0; i < colNum; ++i) {
    flag = pBlock->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT;
    if (pBlock->pData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
      SColData* pCol = (SColData*)taosArrayGet(pBlock->pData->aCol, i);
      if (pCol == NULL) {
        parserError("qResetStmtDataBlock column:%d is NULL", i);
        return terrno;
      }
      if (deepClear) {
        tColDataDeepClear(pCol);

      } else {
        tColDataClear(pCol);
      }

    } else {
      pBlock->pData->aRowP = taosArrayInit(20, POINTER_BYTES);
    }
  }

  tBlobSetDestroy(pBlock->pData->pBlobSet);
  pBlock->pData->pBlobSet = NULL;
  return code;
}

int32_t qCloneStmtDataBlock(STableDataCxt** pDst, STableDataCxt* pSrc, bool reset) {
  int32_t code = 0;

  *pDst = taosMemoryCalloc(1, sizeof(STableDataCxt));
  if (NULL == *pDst) {
    return terrno;
  }

  STableDataCxt* pNewCxt = (STableDataCxt*)*pDst;
  STableDataCxt* pCxt = (STableDataCxt*)pSrc;
  pNewCxt->hasBlob = pSrc->hasBlob;
  pNewCxt->pSchema = NULL;
  pNewCxt->pValues = NULL;

  if (pCxt->pMeta) {
    void* pNewMeta = taosMemoryMalloc(TABLE_META_SIZE(pCxt->pMeta));
    if (NULL == pNewMeta) {
      insDestroyTableDataCxt(*pDst);
      return terrno;
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
      return terrno;
    }
    memcpy(pNewColIdx, pCxt->boundColsInfo.pColIndex,
           pCxt->boundColsInfo.numOfBound * sizeof(*pCxt->boundColsInfo.pColIndex));
    pNewCxt->boundColsInfo.pColIndex = pNewColIdx;
  }

  if (pCxt->pData) {
    int8_t         flag = 1;
    SSubmitTbData* pNewTb = (SSubmitTbData*)taosMemoryMalloc(sizeof(SSubmitTbData));
    if (NULL == pNewTb) {
      insDestroyTableDataCxt(*pDst);
      return terrno;
    }

    memcpy(pNewTb, pCxt->pData, sizeof(*pCxt->pData));
    pNewTb->pCreateTbReq = NULL;
    if (pNewTb->pBlobSet != NULL) {
      flag = pNewTb->pBlobSet->type;
    }
    pNewTb->pBlobSet = NULL;

    pNewTb->aCol = taosArrayDup(pCxt->pData->aCol, NULL);
    if (NULL == pNewTb->aCol) {
      insDestroyTableDataCxt(*pDst);
      return terrno;
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
      return terrno;
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
