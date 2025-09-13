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

int32_t tdRollupCtxInit(SExprSupp **ppSup, SAggSupporter **ppAggSup, SRSchema *pRSchema, int8_t precision,
                        const char *dbName) {
  int32_t        code = 0, lino = 0;
  STSchema      *pTSchema = pRSchema->tSchema;
  SExprSupp     *pSup = NULL;
  SAggSupporter *pAggSup = NULL;
  SNodeList     *pFuncs = NULL;
  SFunctionNode *pFuncNode = NULL;
  SColumnNode   *pColNode = NULL;
  int32_t        nCols = pTSchema->numOfCols;
  int32_t        exprNum = 0;
  SExprInfo     *pExprInfo = NULL;

  if (!(*ppSup) && !(*ppSup = taosMemoryCalloc(1, sizeof(SExprSupp)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pSup = *ppSup;
  if (!(*ppAggSup) && !(*ppAggSup = taosMemoryCalloc(1, sizeof(SAggSupporter)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pAggSup = *ppAggSup;

  for (int32_t i = 1; i < nCols; i++) {  // skip the first timestamp column
    STColumn *pCol = pTSchema->columns + i;
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode **)&pColNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode **)&pFuncNode));

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
    TAOS_CHECK_EXIT(nodesListMakeAppend(&pFuncs, (SNode *)pFuncNode));
    pFuncNode = NULL;
  }
  TAOS_CHECK_EXIT(createExprInfo(pFuncs, NULL, &pExprInfo, &exprNum));
  size_t keyBufSize = sizeof(int64_t) + sizeof(int64_t) + POINTER_BYTES;
  TAOS_CHECK_EXIT(initAggSup(pSup, pAggSup, pExprInfo, exprNum, keyBufSize, "rollup", NULL, NULL));
_exit:
  nodesDestroyNode((SNode *)pFuncNode);
  nodesDestroyNode((SNode *)pColNode);
  nodesDestroyList(pFuncs);
  if (pExprInfo) {
    destroyExprInfo(pExprInfo, exprNum);
    taosMemoryFreeClear(pExprInfo);
  }

  return code;
}

void tdRollupCtxCleanup(SExprSupp *pSup, SAggSupporter *pAggSup) {
  if (pSup) cleanupExprSupp(pSup);
  if (pAggSup) cleanupAggSup(pAggSup);
}
