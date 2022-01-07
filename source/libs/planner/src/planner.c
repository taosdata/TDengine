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
#include "plannerInt.h"

static void destroyDataSinkNode(SDataSink* pSinkNode) {
  if (pSinkNode == NULL) {
    return;
  }
  tfree(pSinkNode);
}

void qDestroySubplan(SSubplan* pSubplan) {
  if (pSubplan == NULL) {
    return;
  }

  taosArrayDestroy(pSubplan->pChildren);
  taosArrayDestroy(pSubplan->pParents);
  destroyDataSinkNode(pSubplan->pDataSink);
  // todo destroy pNode
  tfree(pSubplan);
}

void qDestroyQueryDag(struct SQueryDag* pDag) {
  if (pDag == NULL) {
    return;
  }

  size_t size = taosArrayGetSize(pDag->pSubplans);
  for(size_t i = 0; i < size; ++i) {
    SArray* pa = taosArrayGetP(pDag->pSubplans, i);

    size_t t = taosArrayGetSize(pa);
    for(int32_t j = 0; j < t; ++j) {
      SSubplan* pSubplan = taosArrayGetP(pa, j);
      qDestroySubplan(pSubplan);
    }
    taosArrayDestroy(pa);
  }

  taosArrayDestroy(pDag->pSubplans);
  tfree(pDag);
}

int32_t qCreateQueryDag(const struct SQueryNode* pNode, struct SQueryDag** pDag, uint64_t requestId) {
  SQueryPlanNode* logicPlan;
  int32_t code = createQueryPlan(pNode, &logicPlan);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(logicPlan);
    return code;
  }

  //
  if (logicPlan->info.type != QNODE_MODIFY) {
//    char* str = NULL;
//    queryPlanToString(logicPlan, &str);
//    printf("%s\n", str);
  }

  code = optimizeQueryPlan(logicPlan);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(logicPlan);
    return code;
  }

  code = createDag(logicPlan, NULL, pDag, requestId);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(logicPlan);
    qDestroyQueryDag(*pDag);
    return code;
  }

  destroyQueryPlan(logicPlan);
  return TSDB_CODE_SUCCESS;
}

int32_t qSetSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SQueryNodeAddr* ep) {
  return setSubplanExecutionNode(subplan, templateId, ep);
}

int32_t qSubPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  return subPlanToString(subplan, str, len);
}

int32_t qStringToSubplan(const char* str, SSubplan** subplan) {
  return stringToSubplan(str, subplan);
}
