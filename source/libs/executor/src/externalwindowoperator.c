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
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

typedef struct SExternalWindowOperator {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SExprSupp      scalarSupp;
  int32_t        primaryTsIndex;
  bool           scalarMode;
  SArray*        pWins;
} SExternalWindowOperator;

int32_t createExternalWindowOperator(SOperatorInfo* pDownstream, SIntervalPhysiNode* pPhynode, SExecTaskInfo* pTaskInfo,
                                     SOperatorInfo** pOptrOut) {
  QRY_PARAM_CHECK(pOptrOut);
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExternalWindowOperator* pExtW = taosMemoryCalloc(1, sizeof(SExternalWindowOperator));
  SOperatorInfo*           pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (!pExtW || !pOperator) {
    code = terrno;
    lino = __LINE__;
    goto _error;
  }

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pPhynode->window.node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pResBlock, code, lino, _error, terrno);
  initBasicInfo(&pExtW->binfo, pResBlock);

  if (pExtW->scalarMode) {
  } else {
  }

_error:
  destroyOperatorAndDownstreams(pOperator, &pDownstream, 1);
  pTaskInfo->code = code;
  qError("error happens at %s %d, code:%s", __func__, lino, tstrerror(code));
  return code;
}

static bool hashExternalWindow(SOperatorInfo* pOperator, SResultRowInfo* pResutlRowInfo, SSDataBlock* pInputBlock,
                               int32_t scanFlag) {
  SExternalWindowOperator* pExtW = (SExternalWindowOperator*)pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SExprSupp*               pSup = &pOperator->exprSupp;

  // get timewindow to calc
  
  return true;
}

static int32_t doOpenExternalWindow(SOperatorInfo* pOperator) {
  if (OPTR_IS_OPENED(pOperator)) return TSDB_CODE_SUCCESS;
  int32_t                  code = 0;
  int32_t                  lino = 0;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SOperatorInfo*           pDownstream = pOperator->pDownstream[0];
  SExternalWindowOperator* pExtW = pOperator->info;
  SExprSupp*               pSup = &pOperator->exprSupp;

  int32_t scanFlag = MAIN_SCAN;
  int64_t st = taosGetTimestampUs();

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) break;

    if (!pExtW->scalarMode) {
      if (pExtW->scalarSupp.pExprInfo) {
        SExprSupp* pExprSup = &pExtW->scalarSupp;
        code = projectApplyFunctions(pExprSup->pExprInfo, pBlock, pBlock, pExprSup->pCtx, pExprSup->numOfExprs, NULL);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      code = setInputDataBlock(pSup, pBlock, pExtW->binfo.inputTsOrder, scanFlag, true);
      QUERY_CHECK_CODE(code, lino, _end);
      // do hash agg
    }
  }
_end:
  if (code != 0) {
    qError("%s failed at line %d since:%s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}
