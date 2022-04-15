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

#include "parser.h"

#include "parInt.h"
#include "parToken.h"

static bool isInsertSql(const char* pStr, size_t length) {
  int32_t index = 0;

  do {
    SToken t0 = tStrGetToken((char*) pStr, &index, false);
    if (t0.type != TK_NK_LP) {
      return t0.type == TK_INSERT || t0.type == TK_IMPORT;
    }
  } while (1);
}

static int32_t parseSqlIntoAst(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = parse(pCxt, pQuery);
  if (TSDB_CODE_SUCCESS == code) {
    code = translate(pCxt, *pQuery);
  }
  if (TSDB_CODE_SUCCESS == code) {
    code = calculateConstant(pCxt, *pQuery);
  }
  return code;
}

int32_t qParseQuerySql(SParseContext* pCxt, SQuery** pQuery) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isInsertSql(pCxt->pSql, pCxt->sqlLen)) {
    code = parseInsertSql(pCxt, pQuery);
  } else {
    code = parseSqlIntoAst(pCxt, pQuery);
  }
  terrno = code;
  return code;
}

int32_t qCreateSName(SName* pName, char* pTableName, int32_t acctId, char* dbName, char *msgBuf, int32_t msgBufLen) {
  SMsgBuf msg = {.buf = msgBuf, .len =msgBufLen};
  SToken sToken;
  int32_t code = 0;
  char *tbName = NULL;
  
  NEXT_TOKEN(pTableName, sToken);
  
  if (sToken.n == 0) {
    return buildInvalidOperationMsg(&msg, "empty table name");
  }

  code = createSName(pName, &sToken, acctId, dbName, &msg);
  if (code) {
    return code;
  }

  NEXT_TOKEN(pTableName, sToken);

  if (SToken.n > 0) {
    return buildInvalidOperationMsg(&msg, "table name format is wrong");
  }

  return TSDB_CODE_SUCCESS;
}


int32_t qBindStmtData(SQuery* pQuery, TAOS_MULTI_BIND *bind, char *msgBuf, int32_t msgBufLen) {
  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;
  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pCtx->pTableBlockHashObj, (const char*)&pCtx->tbUid, sizeof(pCtx->tbUid));
  if (NULL == pDataBlock) {
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  SSchema* pSchema = getTableColumnSchema(pDataBlock->pTableMeta);
  int32_t extendedRowSize = getExtendedRowSize(pDataBlock);
  SParsedDataColInfo* spd = &pDataBlock->boundColumnInfo;
  SRowBuilder*        pBuilder = &pDataBlock->rowBuilder;
  SMemParam param = {.rb = pBuilder};
  SMsgBuf pBuf = {.buf = msgBuf, .len = msgBufLen}; 

  CHECK_CODE(allocateMemForSize(pDataBlock, extendedRowSize * bind->num);
  
  for (int32_t r = 0; r < bind->num; ++r) {
    STSRow* row = (STSRow*)(pDataBlock->pData + pDataBlock->size);  // skip the SSubmitBlk header
    tdSRowResetBuf(pBuilder, row);
    
    // 1. set the parsed value from sql string
    for (int c = 0; c < spd->numOfBound; ++c) {
      SSchema* pColSchema = &pSchema[spd->boundColumns[c] - 1];
      
      param.schema = pColSchema;
      getSTSRowAppendInfo(pBuilder->rowType, spd, c, &param.toffset, &param.colIdx);

      if (bind[c].is_null && bind[c].is_null[r]) {
        CHECK_CODE(MemRowAppend(&pBuf, NULL, 0, &param));
      } else {
        int32_t colLen = pColSchema->bytes;
        if (IS_VAR_DATA_TYPE(pColSchema->type)) {
          colLen = bind[c].length[r];
        }
        
        CHECK_CODE(MemRowAppend(&pBuf, (char *)bind[c].buffer + bind[c].buffer_length * r, colLen, &param));
      }
    
      if (PRIMARYKEY_TIMESTAMP_COL_ID == pColSchema->colId) {
        TSKEY tsKey = TD_ROW_KEY(row);
        checkTimestamp(pDataBlock, (const char *)&tsKey);
      }
    }
    
    // set the null value for the columns that do not assign values
    if ((spd->numOfBound < spd->numOfCols) && TD_IS_TP_ROW(row)) {
      for (int32_t i = 0; i < spd->numOfCols; ++i) {
        if (spd->cols[i].valStat == VAL_STAT_NONE) {  // the primary TS key is not VAL_STAT_NONE
          tdAppendColValToTpRow(pBuilder, TD_VTYPE_NONE, getNullValue(pSchema[i].type), true, pSchema[i].type, i,
                                spd->cols[i].toffset);
        }
      }
    }
    
    pDataBlock->size += extendedRowSize;
  }
  
  SSubmitBlk *pBlocks = (SSubmitBlk *)(pDataBlock->pData);
  if (TSDB_CODE_SUCCESS != setBlockInfo(pBlocks, pDataBlock, bind->num)) {
    return buildInvalidOperationMsg(&pBuf, "too many rows in sql, total number of rows should be less than 32767");
  }

  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtOutput(SQuery* pQuery) {
  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;
  int32_t code = 0;
  SInsertParseContext insertCtx = {
    .pVgroupsHashObj = pCtx->pVgroupsHashObj,
    .pTableBlockHashObj = pCtx->pTableBlockHashObj,
  };
  
  // merge according to vgId
  if (taosHashGetSize(pCtx->pTableBlockHashObj) > 0) {
    CHECK_CODE_GOTO(mergeTableDataBlocks(pCtx->pTableBlockHashObj, modifyNode->payloadType, &insertCtx.pVgDataBlocks), _return);
  }
  
  code = buildOutput(&insertCtx);

_return:

  destroyInsertParseContext(&insertCtx);

  return code;
}

int32_t buildBoundFields(SParsedDataColInfo *boundInfo, SSchema *pSchema, int32_t *fieldNum, TAOS_FIELD** fields) {
  *fields = taosMemoryCalloc(boundInfo->numOfBound, sizeof(TAOS_FIELD));
  if (NULL == *fields) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < boundInfo->numOfBound; ++i) {
    SSchema* pTagSchema = &pSchema[boundInfo->boundColumns[i] - 1];
    strcpy((*fields)[i].name, pTagSchema->name);
    (*fields)[i].type = pTagSchema->type;
    (*fields)[i].bytes = pTagSchema->bytes;
  }

  *fieldNum = boundInfo->numOfBound;

  return TSDB_CODE_SUCCESS;
}


int32_t qBuildStmtTagFields(SQuery* pQuery, int32_t *fieldNum, TAOS_FIELD** fields) {
  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;
  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pCtx->pTableBlockHashObj, (const char*)&pCtx->tbUid, sizeof(pCtx->tbUid));
  if (NULL == pDataBlock) {
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  SSchema* pSchema = getTableTagSchema(pDataBlock->pTableMeta);  
  if (pCtx->tags.numOfBound <= 0) {
    *fieldNum = 0;
    *fields = NULL;

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(&pCtx->tags, pSchema, fieldNum, fields));
  
  return TSDB_CODE_SUCCESS;
}

int32_t qBuildStmtColFields(SQuery* pQuery, int32_t *fieldNum, TAOS_FIELD** fields) {
  SVnodeModifOpStmt *modifyNode = (SVnodeModifOpStmt *)pQuery->pRoot;
  SStmtDataCtx *pCtx = &modifyNode->stmtCtx;
  STableDataBlocks *pDataBlock = (STableDataBlocks**)taosHashGet(pCtx->pTableBlockHashObj, (const char*)&pCtx->tbUid, sizeof(pCtx->tbUid));
  if (NULL == pDataBlock) {
    return TSDB_CODE_QRY_APP_ERROR;
  }
  
  SSchema* pSchema = getTableColumnSchema(pDataBlock->pTableMeta);  
  if (pCtx->tags.numOfBound <= 0) {
    *fieldNum = 0;
    *fields = NULL;

    return TSDB_CODE_SUCCESS;
  }

  CHECK_CODE(buildBoundFields(&pDataBlock->boundColumnInfo, pSchema, fieldNum, fields));
  
  return TSDB_CODE_SUCCESS;
}

void qDestroyQuery(SQuery* pQueryNode) {
  if (NULL == pQueryNode) {
    return;
  }
  nodesDestroyNode(pQueryNode->pRoot);
  taosMemoryFreeClear(pQueryNode->pResSchema);
  if (NULL != pQueryNode->pCmdMsg) {
    taosMemoryFreeClear(pQueryNode->pCmdMsg->pMsg);
    taosMemoryFreeClear(pQueryNode->pCmdMsg);
  }
  taosArrayDestroy(pQueryNode->pDbList);
  taosArrayDestroy(pQueryNode->pTableList);
  taosMemoryFreeClear(pQueryNode);
}

int32_t qExtractResultSchema(const SNode* pRoot, int32_t* numOfCols, SSchema** pSchema) {
  return extractResultSchema(pRoot, numOfCols, pSchema);
}
