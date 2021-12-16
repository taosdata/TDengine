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

#include "plannerInt.h"
#include "parser.h"

static const char* gOpName[] = {
  "Unknown",
#define INCLUDE_AS_NAME
#include "plannerOp.h"
#undef INCLUDE_AS_NAME
};

typedef struct SPlanContext {
  struct SCatalog* pCatalog;
  struct SQueryDag* pDag;
  SSubplan* pCurrentSubplan;
  SSubplanId nextId;
} SPlanContext;

static void toDataBlockSchema(SQueryPlanNode* pPlanNode, SDataBlockSchema* dataBlockSchema) {
  SWAP(dataBlockSchema->pSchema, pPlanNode->pSchema, SSchema*);
  dataBlockSchema->numOfCols = pPlanNode->numOfCols;
}

static SPhyNode* initPhyNode(SQueryPlanNode* pPlanNode, int32_t type, int32_t size) {
  SPhyNode* node = (SPhyNode*)calloc(1, size);
  node->info.type = type;
  node->info.name = gOpName[type];
  SWAP(node->pTargets, pPlanNode->pExpr, SArray*);
  toDataBlockSchema(pPlanNode, &(node->targetSchema));
}

static SPhyNode* createTagScanNode(SQueryPlanNode* pPlanNode) {
  return initPhyNode(pPlanNode, OP_TagScan, sizeof(STagScanPhyNode));
}

static SSubplan* initSubplan(SPlanContext* pCxt, int32_t type) {
  SSubplan* subplan = calloc(1, sizeof(SSubplan));
  subplan->id = pCxt->nextId;
  ++(pCxt->nextId.subplanId);
  subplan->type = type;
  subplan->level = 0;
  if (NULL != pCxt->pCurrentSubplan) {
    subplan->level = pCxt->pCurrentSubplan->level + 1;
    if (NULL == pCxt->pCurrentSubplan->pChildern) {
      pCxt->pCurrentSubplan->pChildern = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    }
    taosArrayPush(pCxt->pCurrentSubplan->pChildern, subplan);
    subplan->pParents = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    taosArrayPush(subplan->pParents, pCxt->pCurrentSubplan);
  }
  pCxt->pCurrentSubplan = subplan;
  return subplan;
}

static uint8_t getScanFlag(SQueryPlanNode* pPlanNode) {
  // todo
  return MASTER_SCAN;
}

static SPhyNode* createTableScanNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  STableScanPhyNode* node = (STableScanPhyNode*)initPhyNode(pPlanNode, OP_TableScan, sizeof(STableScanPhyNode));
  node->scan.uid = pTable->pMeta->pTableMeta->uid;
  node->scan.tableType = pTable->pMeta->pTableMeta->tableType;
  node->scanFlag = getScanFlag(pPlanNode);
  node->window = pTable->window;
  // todo tag cond
}

static void vgroupToEpSet(const SVgroupMsg* vg, SEpSet* epSet) {
  // todo
}

static void splitSubplanBySTable(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  SVgroupsInfo* vgroupList = pTable->pMeta->vgroupList;
  for (int32_t i = 0; i < pTable->pMeta->vgroupList->numOfVgroups; ++i) {
    SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_SCAN);
    vgroupToEpSet(&(pTable->pMeta->vgroupList->vgroups[i]), &subplan->execEpSet);
    subplan->pNode = createTableScanNode(pCxt, pPlanNode, pTable);
    // todo reset pCxt->pCurrentSubplan
  }
}

static SPhyNode* createExchangeNode() {

}

static SPhyNode* createScanNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SQueryTableInfo* pTable = (SQueryTableInfo*)pPlanNode->pExtInfo;
  if (TSDB_SUPER_TABLE == pTable->pMeta->pTableMeta->tableType) {
    splitSubplanBySTable(pCxt, pPlanNode, pTable);
    return createExchangeNode(pCxt, pTable);
  }
  return createTableScanNode(pCxt, pPlanNode, pTable);
}

static SPhyNode* createPhyNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SPhyNode* node = NULL;
  switch (pPlanNode->info.type) {
    case QNODE_TAGSCAN:
      node = createTagScanNode(pPlanNode);
      break;
    case QNODE_TABLESCAN:
      node = createScanNode(pCxt, pPlanNode);
      break;
    default:
      assert(false);
  }
  if (pPlanNode->pChildren != NULL && taosArrayGetSize(pPlanNode->pChildren) > 0) {
    node->pChildren = taosArrayInit(4, POINTER_BYTES);
    size_t size = taosArrayGetSize(pPlanNode->pChildren);
    for(int32_t i = 0; i < size; ++i) {
      SPhyNode* child = createPhyNode(pCxt, taosArrayGet(pPlanNode->pChildren, i));
      child->pParent = node;
      taosArrayPush(node->pChildren, &child);
    }
  }
  return node;
}

static void createSubplanByLevel(SPlanContext* pCxt, SQueryPlanNode* pRoot) {
  SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_MERGE);
  subplan->pNode = createPhyNode(pCxt, pRoot);
  SArray* l0 = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  taosArrayPush(l0, &subplan);
  taosArrayPush(pCxt->pDag->pSubplans, &l0);
  // todo deal subquery
}

int32_t createDag(SQueryPlanNode* pQueryNode, struct SCatalog* pCatalog, SQueryDag** pDag) {
  SPlanContext context = {
    .pCatalog = pCatalog,
    .pDag = calloc(1, sizeof(SQueryDag)),
    .pCurrentSubplan = NULL
  };
  if (NULL == context.pDag) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  context.pDag->pSubplans = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  createSubplanByLevel(&context, pQueryNode);
  *pDag = context.pDag;
  return TSDB_CODE_SUCCESS;
}

int32_t subPlanToString(struct SSubplan *pPhyNode, char** str) {
  return TSDB_CODE_SUCCESS;
}
