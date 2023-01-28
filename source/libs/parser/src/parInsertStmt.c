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

int32_t qBuildStmtOutput(SQuery* pQuery, SHashObj* pVgHash, SHashObj* pBlockHash) {
  int32_t code = TSDB_CODE_SUCCESS;
  SArray* pVgDataBlocks = NULL;
  // merge according to vgId
  if (taosHashGetSize(pBlockHash) > 0) {
    code = insMergeTableDataBlocks(pBlockHash, &pVgDataBlocks);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = insBuildOutput(pVgHash, pVgDataBlocks, &((SVnodeModifOpStmt*)pQuery->pRoot)->pDataBlocks);
  }
  insDestroyBlockArrayList(pVgDataBlocks);
  return code;
}

int32_t qBindStmtTagsValue(void* pBlock, void* boundTags, int64_t suid, const char* sTableName, char* tName,
                           TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t             code = TSDB_CODE_SUCCESS;
  SParsedDataColInfo* tags = (SParsedDataColInfo*)boundTags;
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

  SSchema* pSchema = getTableTagSchema(pDataBlock->pTableMeta);

  bool  isJson = false;
  STag* pTag = NULL;

  for (int c = 0; c < tags->numOfBound; ++c) {
    if (bind[c].is_null && bind[c].is_null[0]) {
      continue;
    }

    SSchema* pTagSchema = &pSchema[tags->boundColumns[c]];
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
      if (pTagSchema->type == TSDB_DATA_TYPE_BINARY) {
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

  SVCreateTbReq tbReq = {0};
  insBuildCreateTbReq(&tbReq, tName, pTag, suid, sTableName, tagName, pDataBlock->pTableMeta->tableInfo.numOfTags,
                      TSDB_DEFAULT_TABLE_TTL);
  code = insBuildCreateTbMsg(pDataBlock, &tbReq);
  tdDestroySVCreateTbReq(&tbReq);

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

int32_t qBindStmtColsValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*            pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  int32_t             extendedRowSize = insGetExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  int32_t             rowNum = bind->num;

  CHECK_CODE(
      insInitRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));

  CHECK_CODE(insAllocateMemForSize(pDataBlock, extendedRowSize * bind->num));

  for (int32_t r = 0; r < bind->num; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size);  // skip the SSubmitBlk header
    tdSRowResetBuf(pBuilder, row);

    for (int c = 0; c < spd->numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[spd->boundColumns[c]];

      if (bind[c].num != rowNum) {
        return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
      }

      param.schema = pColSchema;
      insGetSTSRowAppendInfo(pBuilder->rowType, spd, c, &param.toffset, &param.colIdx);

      if (bind[c].is_null && bind[c].is_null[r]) {
        if (pColSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
          return buildInvalidOperationMsg(&pBuf, "primary timestamp should not be NULL");
        }

        CHECK_CODE(insMemRowAppend(&pBuf, NULL, 0, &param));
      } else {
        if (bind[c].buffer_type != pColSchema->type) {
          return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
        }

        int32_t colLen = pColSchema->bytes;
        if (IS_VAR_DATA_TYPE(pColSchema->type)) {
          colLen = bind[c].length[r];
        }

        CHECK_CODE(insMemRowAppend(&pBuf, (char*)bind[c].buffer + bind[c].buffer_length * r, colLen, &param));
      }

      if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
        TSKEY tsKey = TD_ROW_KEY(row);
        insCheckTimestamp(pDataBlock, (const char*)&tsKey);
      }
    }
    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }
    tdSRowEnd(pBuilder);
#ifdef TD_DEBUG_PRINT_ROW
    STSchema* pSTSchema = tdGetSTSChemaFromSSChema(pSchema, spd->numOfCols, 1);
    tdSRowPrint(row, pSTSchema, __func__);
    taosMemoryFree(pSTSchema);
#endif
    pDataBlock->size += extendedRowSize;
  }

  SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
  return insSetBlockInfo(pBlocks, pDataBlock, bind->num, &pBuf);
}

int32_t qBindStmtSingleColValue(void* pBlock, TAOS_MULTI_BIND* bind, char* msgBuf, int32_t msgBufLen, int32_t colIdx,
                                int32_t rowNum) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*            pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  int32_t             extendedRowSize = insGetExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam           param = {.rb = pBuilder};
  SMsgBuf             pBuf = {.buf = msgBuf, .len = msgBufLen};
  bool                rowStart = (0 == colIdx);
  bool                rowEnd = ((colIdx + 1) == spd->numOfBound);

  if (rowStart) {
    CHECK_CODE(
        insInitRowBuilder(&pDataBlock->rowBuilder, pDataBlock->pTableMeta->sversion, &pDataBlock->boundColumnInfo));
    CHECK_CODE(insAllocateMemForSize(pDataBlock, extendedRowSize * bind->num));
  }

  for (int32_t r = 0; r < bind->num; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size + extendedRowSize * r);  // skip the SSubmitBlk header
    if (rowStart) {
      tdSRowResetBuf(pBuilder, row);
    } else {
      tdSRowGetBuf(pBuilder, row);
    }

    SSchema* pColSchema = &pSchema[spd->boundColumns[colIdx]];

    if (bind->num != rowNum) {
      return buildInvalidOperationMsg(&pBuf, "row number in each bind param should be the same");
    }

    param.schema = pColSchema;
    insGetSTSRowAppendInfo(pBuilder->rowType, spd, colIdx, &param.toffset, &param.colIdx);

    if (bind->is_null && bind->is_null[r]) {
      if (pColSchema->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        return buildInvalidOperationMsg(&pBuf, "primary timestamp should not be NULL");
      }

      CHECK_CODE(insMemRowAppend(&pBuf, NULL, 0, &param));
    } else {
      if (bind->buffer_type != pColSchema->type) {
        return buildInvalidOperationMsg(&pBuf, "column type mis-match with buffer type");
      }

      int32_t colLen = pColSchema->bytes;
      if (IS_VAR_DATA_TYPE(pColSchema->type)) {
        colLen = bind->length[r];
      }

      CHECK_CODE(insMemRowAppend(&pBuf, (char*)bind->buffer + bind->buffer_length * r, colLen, &param));
    }

    if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
      TSKEY tsKey = TD_ROW_KEY(row);
      insCheckTimestamp(pDataBlock, (const char*)&tsKey);
    }

    // set the null value for the columns that do not assign values
    if (rowEnd && (spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      pBuilder->hasNone = true;
    }
    if (rowEnd) {
      tdSRowEnd(pBuilder);
    }
#ifdef TD_DEBUG_PRINT_ROW
    if (rowEnd) {
      STSchema* pSTSchema = tdGetSTSChemaFromSSChema(pSchema, spd->numOfCols, 1);
      tdSRowPrint(row, pSTSchema, __func__);
      taosMemoryFree(pSTSchema);
    }
#endif
  }

  if (rowEnd) {
    pDataBlock->size += extendedRowSize * bind->num;

    SSubmitBlk* pBlocks = (SSubmitBlk*)(pDataBlock->pData);
    CHECK_CODE(insSetBlockInfo(pBlocks, pDataBlock, bind->num, &pBuf));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t buildBoundFields(SParsedDataColInfo* boundInfo, SSchema* pSchema, int32_t* fieldNum, TAOS_FIELD_E** fields,
                         uint8_t timePrec) {
  if (fields) {
    *fields = taosMemoryCalloc(boundInfo->numOfBound, sizeof(TAOS_FIELD));
    if (NULL == *fields) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SSchema* schema = &pSchema[boundInfo->boundColumns[0]];
    if (TSDB_DATA_TYPE_TIMESTAMP == schema->type) {
      (*fields)[0].precision = timePrec;
    }

    for (int32_t i = 0; i < boundInfo->numOfBound; ++i) {
      schema = &pSchema[boundInfo->boundColumns[i]];
      strcpy((*fields)[i].name, schema->name);
      (*fields)[i].type = schema->type;
      (*fields)[i].bytes = schema->bytes;
    }
  }

  *fieldNum = boundInfo->numOfBound;

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtTagFields(void* pBlock, void* boundTags, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataBlocks*   pDataBlock = (STableDataBlocks*)pBlock;
  SParsedDataColInfo* tags = (SParsedDataColInfo*)boundTags;
  if (NULL == tags) {
    return TSDB_CODE_APP_ERROR;
  }

  if (pDataBlock->pTableMeta->tableType != TSDB_SUPER_TABLE && pDataBlock->pTableMeta->tableType != TSDB_CHILD_TABLE) {
    return TSDB_CODE_TSC_STMT_API_ERROR;
  }

  SSchema* pSchema = getTableTagSchema(pDataBlock->pTableMeta);
  if (tags->numOfBound <= 0) {
    *fieldNum = 0;
    *fields = NULL;

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(tags, pSchema, fieldNum, fields, 0));

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtColFields(void* pBlock, int32_t* fieldNum, TAOS_FIELD_E** fields) {
  STableDataBlocks* pDataBlock = (STableDataBlocks*)pBlock;
  SSchema*          pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  if (pDataBlock->boundColumnInfo.numOfBound <= 0) {
    *fieldNum = 0;
    if (fields) {
      *fields = NULL;
    }

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(&pDataBlock->boundColumnInfo, pSchema, fieldNum, fields,
                              pDataBlock->pTableMeta->tableInfo.precision));

  return TSDB_CODE_SUCCESS;
}

int32_t qResetStmtDataBlock(void* block, bool keepBuf) {
  STableDataBlocks* pBlock = (STableDataBlocks*)block;

  if (keepBuf) {
    taosMemoryFreeClear(pBlock->pData);
    pBlock->pData = taosMemoryMalloc(TSDB_PAYLOAD_SIZE);
    if (NULL == pBlock->pData) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memset(pBlock->pData, 0, sizeof(SSubmitBlk));
  } else {
    pBlock->pData = NULL;
  }

  pBlock->ordered = true;
  pBlock->prevTS = INT64_MIN;
  pBlock->size = sizeof(SSubmitBlk);
  pBlock->tsSource = -1;
  pBlock->numOfTables = 1;
  pBlock->nAllocSize = TSDB_PAYLOAD_SIZE;
  pBlock->headerSize = pBlock->size;
  pBlock->createTbReqLen = 0;

  memset(&pBlock->rowBuilder, 0, sizeof(pBlock->rowBuilder));

  return TSDB_CODE_SUCCESS;
}

int32_t qCloneStmtDataBlock(void** pDst, void* pSrc) {
  *pDst = taosMemoryMalloc(sizeof(STableDataBlocks));
  if (NULL == *pDst) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  memcpy(*pDst, pSrc, sizeof(STableDataBlocks));
  ((STableDataBlocks*)(*pDst))->cloned = true;

  STableDataBlocks* pBlock = (STableDataBlocks*)(*pDst);
  if (pBlock->pTableMeta) {
    void* pNewMeta = taosMemoryMalloc(TABLE_META_SIZE(pBlock->pTableMeta));
    if (NULL == pNewMeta) {
      taosMemoryFreeClear(*pDst);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(pNewMeta, pBlock->pTableMeta, TABLE_META_SIZE(pBlock->pTableMeta));
    pBlock->pTableMeta = pNewMeta;
  }

  if (pBlock->boundColumnInfo.boundColumns) {
    int32_t size = pBlock->boundColumnInfo.numOfCols * sizeof(col_id_t);
    void* tmp = taosMemoryMalloc(size);
    memcpy(tmp, pBlock->boundColumnInfo.boundColumns, size);
    pBlock->boundColumnInfo.boundColumns = tmp;
  }

  if (pBlock->boundColumnInfo.cols) {
    int32_t size = pBlock->boundColumnInfo.numOfCols * sizeof(SBoundColumn);
    void* tmp = taosMemoryMalloc(size);
    memcpy(tmp, pBlock->boundColumnInfo.cols, size);
    pBlock->boundColumnInfo.cols = tmp;
  }

  if (pBlock->boundColumnInfo.colIdxInfo) {
    int32_t size = pBlock->boundColumnInfo.numOfBound * sizeof(SBoundIdxInfo);
    void* tmp = taosMemoryMalloc(size);
    memcpy(tmp, pBlock->boundColumnInfo.colIdxInfo, size);
    pBlock->boundColumnInfo.colIdxInfo = tmp;
  }

  return qResetStmtDataBlock(*pDst, false);
}

int32_t qRebuildStmtDataBlock(void** pDst, void* pSrc, uint64_t uid, int32_t vgId) {
  int32_t code = qCloneStmtDataBlock(pDst, pSrc);
  if (code) {
    return code;
  }

  STableDataBlocks* pBlock = (STableDataBlocks*)*pDst;
  pBlock->pData = taosMemoryMalloc(pBlock->nAllocSize);
  if (NULL == pBlock->pData) {
    qDestroyStmtDataBlock(pBlock);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBlock->vgId = vgId;

  if (pBlock->pTableMeta) {
    pBlock->pTableMeta->uid = uid;
    pBlock->pTableMeta->vgId = vgId;
  }

  memset(pBlock->pData, 0, sizeof(SSubmitBlk));

  return TSDB_CODE_SUCCESS;
}

STableMeta* qGetTableMetaInDataBlock(void* pDataBlock) { return ((STableDataBlocks*)pDataBlock)->pTableMeta; }

void qFreeStmtDataBlock(void* pDataBlock) {
  if (pDataBlock == NULL) {
    return;
  }

  taosMemoryFreeClear(((STableDataBlocks*)pDataBlock)->pTableMeta);
  taosMemoryFreeClear(((STableDataBlocks*)pDataBlock)->pData);
  taosMemoryFreeClear(pDataBlock);
}

void qDestroyStmtDataBlock(void* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  STableDataBlocks* pDataBlock = (STableDataBlocks*)pBlock;

  pDataBlock->cloned = false;
  insDestroyDataBlock(pDataBlock);
}
