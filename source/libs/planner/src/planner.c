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

static void extractResSchema(struct SQueryDag* const* pDag, SSchema** pResSchema, int32_t* numOfCols);

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

int32_t qCreateQueryDag(const struct SQueryNode* pNode, struct SQueryDag** pDag, SSchema** pResSchema, int32_t* numOfCols, SArray* pNodeList,
    uint64_t requestId) {
  SQueryPlanNode* pLogicPlan;
  int32_t code = createQueryPlan(pNode, &pLogicPlan);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(pLogicPlan);
    return code;
  }

  if (pLogicPlan->info.type != QNODE_MODIFY) {
    char* str = NULL;
    queryPlanToString(pLogicPlan, &str);
    qDebug("reqId:0x%"PRIx64": %s", requestId, str);
    tfree(str);
  }

  code = optimizeQueryPlan(pLogicPlan);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(pLogicPlan);
    return code;
  }

  code = createDag(pLogicPlan, NULL, pDag, pNodeList, requestId);
  if (TSDB_CODE_SUCCESS != code) {
    destroyQueryPlan(pLogicPlan);
    qDestroyQueryDag(*pDag);
    return code;
  }

  extractResSchema(pDag, pResSchema, numOfCols);

  destroyQueryPlan(pLogicPlan);
  return TSDB_CODE_SUCCESS;
}

// extract the final result schema
void extractResSchema(struct SQueryDag* const* pDag, SSchema** pResSchema, int32_t* numOfCols) {
  SArray* pTopSubplan = taosArrayGetP((*pDag)->pSubplans, 0);

  SSubplan*         pPlan = taosArrayGetP(pTopSubplan, 0);
  SDataBlockSchema* pDataBlockSchema = &(pPlan->pDataSink->schema);

  *numOfCols = pDataBlockSchema->numOfCols;
  if (*numOfCols > 0) {
    *pResSchema = calloc(pDataBlockSchema->numOfCols, sizeof(SSchema));
    memcpy((*pResSchema), pDataBlockSchema->pSchema, pDataBlockSchema->numOfCols * sizeof(SSchema));
  }
}

void qSetSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SDownstreamSource* pSource) {
  setSubplanExecutionNode(subplan, templateId, pSource);
}

int32_t qSubPlanToString(const SSubplan *subplan, char** str, int32_t* len) {
  return subPlanToString(subplan, str, len);
}

int32_t qStringToSubplan(const char* str, SSubplan** subplan) {
  return stringToSubplan(str, subplan);
}
