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

int32_t tdRollupCtxInit(SExprSupp **ppSup, STSchema *pTSchema, int8_t precision, const char* dbName, const char* tbName, int64_t tbUid, int8_t tbType) {
  int32_t        code = 0, lino = 0;
  int32_t        nCols = pTSchema->numOfCols;
  SExprSupp     *pSup = NULL;
  SFunctionNode *pFuncNode = NULL;
  SColumnNode   *pColNode = NULL;

  if (!(*ppSup) && !(*ppSup = taosMemoryCalloc(1, sizeof(SExprSupp)))) {
    TAOS_CHECK_EXIT(terrno);
  }
  pSup = *ppSup;

  for (int32_t i = 1; i < nCols; i++) {
    SSchema *pSchema = pTSchema->columns + i;
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode *)&pFuncNode));
    TAOS_CHECK_EXIT(nodesMakeNode(QUERY_NODE_COLUMN, (SNode *)&pColNode));

    pColNode->node.resType.type = pSchema->type;
    pColNode->node.resType.precision = precision;
    pColNode->node.resType.bytes = pSchema->bytes;
    pColNode->node.resType.scale = 0; // TODO: use the real scale for decimal

    pColNode->colId = pSchema->colId;
    pColNode->colType = COLUMN_TYPE_COLUMN;
  }

  // SNodeList *paramList = NULL;
  // nodesListMakeStrictAppend(&paramList, NULL);

_exit:
  nodesDestroyNode(pFuncNode);
  nodesDestroyNode(pColNode);
  return code;
}

void tdRollupCtxCleanup(SExprSupp *pSup) { cleanupExprSupp(pSup); }
