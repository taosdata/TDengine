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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "index.h"
#include "nodes.h"
#include "operator.h"
#include "os.h"
#include "query.h"
#include "querynodes.h"
#include "querytask.h"
#include "tcompare.h"
#include "tdatablock.h"
#include "tfill.h"
#include "tglobal.h"
#include "thash.h"
#include "tname.h"
#include "ttypes.h"

static int32_t createDataBlockForSchema(SRSchema *pRSchema, SSDataBlock **ppDataBlock, int32_t maxBufRows) {
  int32_t      code = 0, lino = 0;
  STSchema    *pTSchema = pRSchema->tSchema;
  SExtSchema  *pExtSchema = (SExtSchema *)pRSchema->extSchema;
  int32_t      numOfCols = pTSchema->numOfCols;
  SSDataBlock *pBlock = NULL;

  if (numOfCols < 1) {
    TAOS_CHECK_EXIT(TSDB_CODE_RSMA_INVALID_SCHEMA);
  }
  TAOS_CHECK_EXIT(createDataBlock(&pBlock));

  // the primary timestamp column is included since it's needed by funcs like first/last.
  for (int32_t i = 0; i < numOfCols; ++i) {
    STColumn       *pCol = pTSchema->columns + i;
    SColumnInfoData colInfoData = createColumnInfoData(pCol->type, pCol->bytes, pCol->colId);
    if (pExtSchema && IS_DECIMAL_TYPE(pCol->type)) {
      decimalFromTypeMod(pExtSchema[i].typeMod, &colInfoData.info.precision, &colInfoData.info.scale);
    }
    TAOS_CHECK_EXIT(blockDataAppendColInfo(pBlock, &colInfoData));
  }
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, maxBufRows));

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *ppDataBlock = pBlock;
  return code;
}

static int32_t createDataBlockForTargets(SRSchema *pRSchema, SNodeList *pTargets, SSDataBlock **ppDataBlock,
                                         int32_t maxBufRows) {
  int32_t      code = 0, lino = 0;
  int32_t      numOfCols = LIST_LENGTH(pTargets);
  SExtSchema  *pExtSchema = (SExtSchema *)pRSchema->extSchema;
  SSDataBlock *pBlock = NULL;

  if (numOfCols < 1) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }
  TAOS_CHECK_EXIT(createDataBlock(&pBlock));
  if (pExtSchema) ++pExtSchema;
  for (int32_t i = 0; i < numOfCols; ++i) {  // the first timestamp column is not included in targets
    STargetNode *pTargetNode = (STargetNode *)nodesListGetNode(pTargets, i);
    if (!pTargetNode) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
    }
    SFunctionNode *pFuncNode = (SFunctionNode *)pTargetNode->pExpr;
    if (!pFuncNode || pFuncNode->node.type != QUERY_NODE_FUNCTION) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
    }
    if (LIST_LENGTH(pFuncNode->pParameterList) < 1 || LIST_LENGTH(pFuncNode->pParameterList) > 3) { // col[,pk[,composite key]]
      TAOS_CHECK_EXIT(TSDB_CODE_RSMA_INVALID_FUNC_PARAM);
    }
    SColumnNode *pColNode = (SColumnNode *)nodesListGetNode(pFuncNode->pParameterList, 0);
    if (!pColNode || pColNode->node.type != QUERY_NODE_COLUMN) {
      TAOS_CHECK_EXIT(TSDB_CODE_RSMA_INVALID_FUNC_PARAM);
    }

    SColumnInfoData colInfoData =
        createColumnInfoData(pFuncNode->node.resType.type, pFuncNode->node.resType.bytes, pColNode->colId);
    if (pExtSchema && IS_DECIMAL_TYPE(pColNode->node.resType.type)) {
      decimalFromTypeMod(pExtSchema[i].typeMod, &colInfoData.info.precision, &colInfoData.info.scale);
    }
    TAOS_CHECK_EXIT(blockDataAppendColInfo(pBlock, &colInfoData));
  }
  TAOS_CHECK_EXIT(blockDataEnsureCapacity(pBlock, maxBufRows));

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pBlock);
    pBlock = NULL;
  }
  *ppDataBlock = pBlock;
  return code;
}

int32_t tdRollupCtxInit(SRollupCtx *pCtx, SRSchema *pRSchema, int8_t precision, const char *dbName) {
  int32_t         code = 0, lino = 0;
  STSchema       *pTSchema = pRSchema->tSchema;
  SExtSchema     *pExtSchema = pRSchema->extSchema;
  SSDataBlock    *pInputBlock = NULL;
  SSDataBlock    *pResBlock = NULL;
  SExprSupp      *pExprSup = NULL;
  SAggSupporter  *pAggSup = NULL;
  SGroupResInfo  *pGroupResInfo = NULL;
  SResultRowInfo *pResultRowInfo = NULL;
  SExecTaskInfo  *pTaskInfo = NULL;
  SResultRow     *pResultRow = NULL;
  STargetNode    *pTargetNode = NULL;
  SFunctionNode  *pFuncNode = NULL;
  SColumnNode    *pColNode = NULL;
  SColumnNode    *pPrimaryKeyColNode = NULL;
  SColumnNode    *pCompositeKeyColNode = NULL;
  int32_t         nCols = pTSchema->numOfCols;
  int32_t         exprNum = 0;
  bool            hasPk = false;
  int32_t         pkBytes = 0;
  SExprInfo      *pExprInfo = NULL;
  char            buf[512] = "\0";

  if (nCols < 2) {
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }

  if (!(pExprSup = pCtx->exprSup)) {
    if (!(pExprSup = taosMemoryCalloc(1, sizeof(SExprSupp)))) {
      TAOS_CHECK_EXIT(terrno);
    }
    pCtx->exprSup = pExprSup;
  }

  if (!(pAggSup = pCtx->aggSup)) {
    if (!(pAggSup = taosMemoryCalloc(1, sizeof(SAggSupporter)))) {
      TAOS_CHECK_EXIT(terrno);
    }
    pCtx->aggSup = pAggSup;
  }

  if (!(pResultRowInfo = pCtx->resultRowInfo)) {
    if (!(pResultRowInfo = taosMemoryCalloc(1, sizeof(SResultRowInfo)))) {
      TAOS_CHECK_EXIT(terrno);
    }
    pCtx->resultRowInfo = pResultRowInfo;
  }

  if (!(pGroupResInfo = pCtx->pGroupResInfo)) {
    if (!(pGroupResInfo = taosMemoryCalloc(1, sizeof(SGroupResInfo)))) {
      TAOS_CHECK_EXIT(terrno);
    }
    pCtx->pGroupResInfo = pGroupResInfo;
  }

  if (!(pTaskInfo = pCtx->pTaskInfo)) {
    if (!(pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo)))) {
      TAOS_CHECK_EXIT(terrno);
    }
    pCtx->pTaskInfo = pTaskInfo;
    pTaskInfo->id.str = taosStrdup("rollup");
    if (!pTaskInfo->id.str) {
      TAOS_CHECK_EXIT(terrno);
    }
  }

  if (!pCtx->pColValArr && !(pCtx->pColValArr = taosArrayInit(nCols, sizeof(SColVal)))) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (!pCtx->pBuf && !(pCtx->pBuf = taosMemoryMalloc(TSDB_MAX_BYTES_PER_ROW + VARSTR_HEADER_SIZE))) {
    TAOS_CHECK_EXIT(terrno);
  }

  int32_t inputRowSize = sizeof(int64_t);  // for the timestamp column
  if (pTSchema->columns[1].flags & COL_IS_KEY) {
    hasPk = true;
    pkBytes = pTSchema->columns[1].bytes;
  }
  for (int32_t i = 1; i < nCols; i++) {    // skip the first timestamp column
    STColumn *pCol = pTSchema->columns + i;
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&pColNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode **)&pFuncNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_TARGET, (SNode **)&pTargetNode));

    // build the column node
    pColNode->node.resType.type = pCol->type;
    pColNode->node.resType.bytes = pCol->bytes;
    pColNode->node.resType.precision = 0;
    pColNode->node.resType.scale = 0;
    if (pExtSchema && IS_DECIMAL_TYPE(pCol->type)) {
      decimalFromTypeMod(pExtSchema[i].typeMod, &pColNode->node.resType.precision, &pColNode->node.resType.scale);
    }

    pColNode->colId = pCol->colId;
    pColNode->colType = COLUMN_TYPE_COLUMN;

    pColNode->slotId = i;

    pColNode->tableId = pRSchema->tbUid;
    pColNode->tableType = pRSchema->tbType;
    // (void)snprintf(pColNode->dbName, TSDB_DB_NAME_LEN, "%s", dbName);
    // (void)snprintf(pColNode->tableName, TSDB_TABLE_NAME_LEN, "%s", pRSchema->tbName);
    // (void)snprintf(pColNode->tableAlias, TSDB_TABLE_NAME_LEN, "%s", pRSchema->tbName);
    // snprintf(pColNode->colName, TSDB_COL_NAME_LEN, "c%d", i); // TODO: fill if necessary

    // build the function node
    // pFuncNode->node.resType = pColNode->node.resType;
    // pFuncNode->funcId = pRSchema->funcIds[i];
    // pFuncNode->funcType = fmGetFuncTypeById(pFuncNode->funcId);
    (void)snprintf(pFuncNode->functionName, TSDB_FUNC_NAME_LEN, "%s", fmGetFuncName(pRSchema->funcIds[i]));
    TAOS_CHECK_EXIT(nodesListMakeAppend(&pFuncNode->pParameterList, (SNode *)pColNode));
    pColNode = NULL;
    TAOS_CHECK_EXIT(fmGetFuncInfo(pFuncNode, buf, sizeof(buf)));
    pFuncNode->hasPk = hasPk;
    pFuncNode->pkBytes = pkBytes;

    if (fmIsImplicitTsFunc(pFuncNode->funcId)) {
      TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&pPrimaryKeyColNode));
      pPrimaryKeyColNode->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
      pPrimaryKeyColNode->node.resType.bytes = sizeof(int64_t);
      pPrimaryKeyColNode->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
      pPrimaryKeyColNode->colType = COLUMN_TYPE_COLUMN;
      pPrimaryKeyColNode->slotId = 0;  // put the timestamp column at the 1st position
      // (void)snprintf(pPrimaryKeyColNode->tableName, TSDB_TABLE_NAME_LEN, "%s", pRSchema->tbName);
      (void)snprintf(pFuncNode->functionName, TSDB_FUNC_NAME_LEN, "%s", pFuncNode->functionName);
      TAOS_CHECK_EXIT(nodesListMakeAppend(&pFuncNode->pParameterList, (SNode *)pPrimaryKeyColNode));
      pPrimaryKeyColNode = NULL;
      if (hasPk) {
        TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&pCompositeKeyColNode));
        pCompositeKeyColNode->node.resType.type = pTSchema->columns[1].type;
        pCompositeKeyColNode->node.resType.bytes = pTSchema->columns[1].bytes;
        pCompositeKeyColNode->colId = pTSchema->columns[1].colId;
        pCompositeKeyColNode->colType = COLUMN_TYPE_COLUMN;
        pCompositeKeyColNode->slotId = 1;
        TAOS_CHECK_EXIT(nodesListMakeAppend(&pFuncNode->pParameterList, (SNode *)pCompositeKeyColNode));
        pCompositeKeyColNode = NULL;
      }
    }

    // build the target node
    pTargetNode->slotId = (int16_t)(i - 1);
    pTargetNode->dataBlockId = 0;  // only one data block for rollup
    pTargetNode->pExpr = (SNode *)pFuncNode;
    pFuncNode = NULL;

    TAOS_CHECK_EXIT(nodesListMakeAppend((SNodeList **)&pCtx->pTargets, (SNode *)pTargetNode));
    pTargetNode = NULL;

    inputRowSize += pCol->bytes;
  }

  pCtx->rowSize = inputRowSize;
  pCtx->maxBufRows = (64LL * 1024 * 1024) / inputRowSize;
  if (pCtx->maxBufRows > 4096) {
    pCtx->maxBufRows = 4096;
  } else if (pCtx->maxBufRows < 1024) {
    pCtx->maxBufRows = 1024;
  }

  TAOS_CHECK_EXIT(createDataBlockForSchema(pRSchema, &pCtx->pInputBlock, pCtx->maxBufRows));
  TAOS_CHECK_EXIT(createDataBlockForTargets(pRSchema, pCtx->pTargets, &pCtx->pResBlock, pCtx->maxBufRows));

  initResultRowInfo(pResultRowInfo);

  TAOS_CHECK_EXIT(createExprInfo((SNodeList *)pCtx->pTargets, NULL, &pExprInfo, &exprNum));
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  TAOS_CHECK_EXIT(initAggSup(pExprSup, pAggSup, pExprInfo, exprNum, keyBufSize, "rollup", NULL, NULL));

  int32_t groupId = 0;
  pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char *)&groupId, sizeof(groupId), true,
                                      groupId, pTaskInfo, false, pAggSup, true);
  if (pResultRow == NULL || pTaskInfo->code != 0) {
    TAOS_CHECK_EXIT(pTaskInfo->code);
  }
  pCtx->resultRow = pResultRow;
  int32_t rowSize = pAggSup->resultRowSize;
  if (pResultRow->pageId == -1) {
    TAOS_CHECK_EXIT(addNewResultRowBuf(pResultRow, pAggSup->pResultBuf, rowSize));
  }

  TAOS_CHECK_EXIT(setResultRowInitCtx(pResultRow, pExprSup->pCtx, exprNum, pExprSup->rowEntryInfoOffset));

  TAOS_CHECK_EXIT(initGroupedResultInfo(pCtx->pGroupResInfo, pCtx->aggSup->pResultRowHashTable, 0));

_exit:
  nodesDestroyNode((SNode *)pCompositeKeyColNode);
  nodesDestroyNode((SNode *)pPrimaryKeyColNode);
  nodesDestroyNode((SNode *)pColNode);
  nodesDestroyNode((SNode *)pFuncNode);
  nodesDestroyNode((SNode *)pTargetNode);
  if (code != 0) {
    if (buf[0] != 0) {
      qError("%s failed at line %d since %s(%s)", __func__, lino, tstrerror(code), buf);
    } else {
      qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    }
  }

  return code;
}

int32_t tdRollupDoAggregate(SRollupCtx *pCtx) {
  int32_t         code = 0, lino = 0;
  SqlFunctionCtx *pFuncCtx = pCtx->exprSup->pCtx;
  SExprSupp      *pExprSup = pCtx->exprSup;

  if ((pExprSup->numOfExprs > 0 && pFuncCtx == NULL)) {
    TAOS_CHECK_EXIT(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  TAOS_CHECK_EXIT(
      setInputDataBlock(pCtx->exprSup, pCtx->pInputBlock, ORDER_ASC, pCtx->pInputBlock->info.scanFlag, true));

  for (int32_t k = 0; k < pExprSup->numOfExprs; ++k) {
    if (functionNeedToExecute(&pFuncCtx[k])) {
      // todo add a dummy function to avoid process check
      if (pFuncCtx[k].fpSet.process == NULL) {
        continue;
      }

      if ((&pFuncCtx[k])->input.pData[0] == NULL) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      } else {
        code = pFuncCtx[k].fpSet.process(&pFuncCtx[k]);
      }

      if (code != TSDB_CODE_SUCCESS) {
        if (pFuncCtx[k].fpSet.cleanup != NULL) {
          pFuncCtx[k].fpSet.cleanup(&pFuncCtx[k]);
        }
        TAOS_CHECK_EXIT(code);
      }
    }
  }
_exit:
  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tdRollupFinalize(SRollupCtx *pCtx) {
  int32_t      code = 0, lino = 0;
  SSDataBlock *pResBlock = pCtx->pResBlock;

  blockDataCleanup(pResBlock);
  // pResBlock->info.id.groupId = 0;
  doCopyToSDataBlock(pCtx->pTaskInfo, pResBlock, pCtx->exprSup, pCtx->aggSup->pResultBuf, pCtx->pGroupResInfo,
                     pCtx->maxBufRows, true, NULL);
_exit:
  return code;
}

void tdRollupCtxCleanup(SRollupCtx *pCtx, bool deep) {
  if (pCtx) {
    if (pCtx->pTargets) {
      nodesDestroyList((SNodeList *)pCtx->pTargets);
      pCtx->pTargets = NULL;
    }
    if (pCtx->pBuf && deep) {
      taosMemFreeClear(pCtx->pBuf);
    }
    if (pCtx->exprSup) {
      cleanupExprSupp(pCtx->exprSup);
      if (deep) taosMemFreeClear(pCtx->exprSup);
    }
    if (pCtx->aggSup) {
      cleanupAggSup(pCtx->aggSup);
      if (deep) taosMemFreeClear(pCtx->aggSup);
    }
    if (pCtx->resultRowInfo) {
      pCtx->resultRowInfo->openWindow = tdListFree(pCtx->resultRowInfo->openWindow);
      if (deep) taosMemFreeClear(pCtx->resultRowInfo);
    }
    if (pCtx->pGroupResInfo) {
      cleanupGroupResInfo(pCtx->pGroupResInfo);
      if (deep) taosMemFreeClear(pCtx->pGroupResInfo);
    }
    if (pCtx->pTaskInfo) {
      if (deep) {
        taosMemFreeClear(pCtx->pTaskInfo->id.str);
        taosMemFreeClear(pCtx->pTaskInfo);
      }
    }
    if (pCtx->pColValArr) {
      if (deep) {
        taosArrayDestroy(pCtx->pColValArr);
        pCtx->pColValArr = NULL;
      } else {
        taosArrayClear(pCtx->pColValArr);
      }
    }
    blockDataDestroy(pCtx->pInputBlock);
    blockDataDestroy(pCtx->pResBlock);
  }
}

int32_t tdRollupCtxReset(SRollupCtx *pCtx) {
  int32_t code = 0, lino = 0;
  pCtx->winTotalRows = 0;
  pCtx->pGroupResInfo->index = 0;
  pCtx->pGroupResInfo->delIndex = 0;
  clearResultRowInitFlag(pCtx->exprSup->pCtx, pCtx->exprSup->numOfExprs);
  TAOS_CHECK_EXIT(setResultRowInitCtx(pCtx->resultRow, pCtx->exprSup->pCtx, pCtx->exprSup->numOfExprs,
                                      pCtx->exprSup->rowEntryInfoOffset));
_exit:
  return code;
}
