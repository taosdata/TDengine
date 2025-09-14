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

int32_t tdRollupCtxInit(SRollupCtx *pCtx, SRSchema *pRSchema, int8_t precision, const char *dbName) {
  int32_t         code = 0, lino = 0;
  STSchema       *pTSchema = pRSchema->tSchema;
  SSDataBlock    *pResBlock = NULL;
  SExprSupp      *pExprSup = NULL;
  SAggSupporter  *pAggSup = NULL;
  SResultRowInfo *pResultRowInfo = NULL;
  SExecTaskInfo  *pTaskInfo = NULL;
  SResultRow     *pResultRow = NULL;
  SNodeList      *pTargets = NULL;
  STargetNode    *pTargetNode = NULL;
  SFunctionNode  *pFuncNode = NULL;
  SColumnNode    *pColNode = NULL;
  int32_t         nCols = pTSchema->numOfCols;
  int32_t         exprNum = 0;
  SExprInfo      *pExprInfo = NULL;

  if (!(pExprSup = taosMemoryCalloc(1, sizeof(SExprSupp)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pCtx->exprSup = pExprSup;
  if (!(pAggSup = taosMemoryCalloc(1, sizeof(SAggSupporter)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pCtx->aggSup = pAggSup;
  if (!(pResultRowInfo = taosMemoryCalloc(1, sizeof(SResultRowInfo)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pCtx->resultRowInfo = pResultRowInfo;
  if (!(pTaskInfo = taosMemoryCalloc(1, sizeof(SExecTaskInfo)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pCtx->pTaskInfo = pTaskInfo;

  for (int32_t i = 1; i < nCols; i++) {  // skip the first timestamp column
    STColumn *pCol = pTSchema->columns + i;
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&pColNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode **)&pFuncNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_TARGET, (SNode **)&pTargetNode));

    // build the column node
    pColNode->node.resType.type = pCol->type;
    pColNode->node.resType.precision = precision;
    pColNode->node.resType.bytes = pCol->bytes;
    pColNode->node.resType.scale = 0;  // TODO: use the real scale for decimal

    pColNode->colId = pCol->colId;
    pColNode->colType = COLUMN_TYPE_COLUMN;

    pColNode->slotId = (int16_t)(i - 1);

    pColNode->tableId = pRSchema->tbUid;
    pColNode->tableType = pRSchema->tbType;
    (void)snprintf(pColNode->dbName, TSDB_DB_NAME_LEN, "%s", dbName);
    (void)snprintf(pColNode->tableName, TSDB_TABLE_NAME_LEN, "%s", pRSchema->tbName);
    (void)snprintf(pColNode->tableAlias, TSDB_TABLE_NAME_LEN, "%s", pRSchema->tbName);
    // snprintf(pColNode->colName, TSDB_COL_NAME_LEN, "c%d", i); // TODO: fill if necessary

    // build the function node
    pFuncNode->node.resType = pColNode->node.resType;
    pFuncNode->funcId = pRSchema->funcIds[i];
    pFuncNode->funcType = fmGetFuncTypeById(pFuncNode->funcId);
    (void)snprintf(pFuncNode->functionName, TSDB_FUNC_NAME_LEN, "%s", fmGetFuncName(pFuncNode->funcId));
    TAOS_CHECK_EXIT(nodesListMakeAppend(&pFuncNode->pParameterList, (SNode *)pColNode));
    pColNode = NULL;

    // build the target node
    pTargetNode->slotId = (int16_t)(i - 1);
    pTargetNode->dataBlockId = 0;  // only one data block for rollup
    pTargetNode->pExpr = (SNode *)pFuncNode;
    pFuncNode = NULL;

    TAOS_CHECK_EXIT(nodesListMakeAppend(&pTargets, (SNode *)pTargetNode));
    pTargetNode = NULL;
  }

  // SSDataBlock *pResBlock = createDataBlockFromDescNode(NULL); // pAggNode->node.pOutputDataBlockDesc);

  initResultRowInfo(pResultRowInfo);

  TAOS_CHECK_EXIT(createExprInfo(pTargets, NULL, &pExprInfo, &exprNum));
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  TAOS_CHECK_EXIT(initAggSup(pExprSup, pAggSup, pExprInfo, exprNum, keyBufSize, "rollup", NULL, NULL));

  int32_t groupId = 0;
  pResultRow = doSetResultOutBufByKey(pAggSup->pResultBuf, pResultRowInfo, (char *)&groupId, sizeof(groupId), true,
                                      groupId, pTaskInfo, false, pAggSup, true);
  if (pResultRow == NULL || pTaskInfo->code != 0) {
    TAOS_CHECK_EXIT(pTaskInfo->code);
  }
  // int32_t rowSize = pAggSup->resultRowSize;
  // if (pResultRow->pageId == -1) {
  //   TAOS_CHECK_EXIT(addNewResultRowBuf(pResultRow, pAggSup->pResultBuf, rowSize));
  // }

  TAOS_CHECK_EXIT(setResultRowInitCtx(pResultRow, pExprSup->pCtx, exprNum, pExprSup->rowEntryInfoOffset));

_exit:
  nodesDestroyNode((SNode *)pColNode);
  nodesDestroyNode((SNode *)pFuncNode);
  nodesDestroyNode((SNode *)pTargetNode);
  nodesDestroyList(pTargets);
  if (code != 0) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));

  }

  return code;
}

int32_t tdRollupAppendData(SRollupCtx *pCtx, void *pInput) {
  int32_t code = 0, lino = 0;
_exit:
  return code;
}

int32_t tdRollupDoAggregate(SRollupCtx *pCtx) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SqlFunctionCtx *pFuncCtx = pCtx->exprSup->pCtx;
  SExprSupp      *pExprSup = pCtx->exprSup;

  if ((pExprSup->numOfExprs > 0 && pFuncCtx == NULL)) {
    qError("%s failed at line %d since pFuncCtx is NULL.", __func__, __LINE__);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  for (int32_t k = 0; k < pExprSup->numOfExprs; ++k) {
    if (functionNeedToExecute(&pFuncCtx[k])) {
      // todo add a dummy function to avoid process check
      if (pFuncCtx[k].fpSet.process == NULL) {
        continue;
      }

      if ((&pFuncCtx[k])->input.pData[0] == NULL) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        qError("%s aggregate function error happens, input data is NULL.", GET_TASKID(pCtx->pTaskInfo));
      } else {
        code = pFuncCtx[k].fpSet.process(&pFuncCtx[k]);
      }

      if (code != TSDB_CODE_SUCCESS) {
        if (pFuncCtx[k].fpSet.cleanup != NULL) {
          pFuncCtx[k].fpSet.cleanup(&pFuncCtx[k]);
        }
        qError("%s aggregate function error happens, code:%s", GET_TASKID(pCtx->pTaskInfo), tstrerror(code));
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tdRollupFinalize(SRollupCtx *pCtx, void *pOutput) {
  int32_t code = 0, lino = 0;
_exit:
  return code;
}

void tdRollupCtxCleanup(SRollupCtx *pCtx, bool deep) {
  if (pCtx) {
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
    if (pCtx->pTaskInfo) {
      if (deep) taosMemFreeClear(pCtx->pTaskInfo);
    }
  }
}
