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

#define STORE_CURRENT_SUBPLAN(cxt) SSubplan* _ = cxt->pCurrentSubplan
#define RECOVERY_CURRENT_SUBPLAN(cxt) cxt->pCurrentSubplan = _

typedef struct SPlanContext {
  struct SCatalog* pCatalog;
  struct SQueryDag* pDag;
  SSubplan* pCurrentSubplan;
  SSubplanId nextId;
} SPlanContext;

static const char* gOpName[] = {
  "Unknown",
#define INCLUDE_AS_NAME
#include "plannerOp.h"
#undef INCLUDE_AS_NAME
};

int32_t opNameToOpType(const char* name) {
  for (int32_t i = 1; i < sizeof(gOpName) / sizeof(gOpName[0]); ++i) {
    if (strcmp(name, gOpName[i])) {
      return i;
    }
  }
  return OP_Unknown;
}

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

static SPhyNode* initScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable, int32_t type, int32_t size) {
  SScanPhyNode* node = (SScanPhyNode*)initPhyNode(pPlanNode, type, size);
  node->uid = pTable->pMeta->pTableMeta->uid;
  node->tableType = pTable->pMeta->pTableMeta->tableType;
  return (SPhyNode*)node;
}

static SPhyNode* createPseudoScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable, int32_t op) {
  return initScanNode(pPlanNode, pTable, op, sizeof(SScanPhyNode));
}

static SPhyNode* createTagScanNode(SQueryPlanNode* pPlanNode) {
  SQueryTableInfo* pTable = (SQueryTableInfo*)pPlanNode->pExtInfo;
  return createPseudoScanNode(pPlanNode, pTable, OP_TagScan);
}

static uint8_t getScanFlag(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  // todo
  return MASTER_SCAN;
}

static SPhyNode* createUserTableScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable, int32_t op) {
  STableScanPhyNode* node = (STableScanPhyNode*)initScanNode(pPlanNode, pTable, op, sizeof(STableScanPhyNode));
  node->scanFlag = getScanFlag(pPlanNode, pTable);
  node->window = pTable->window;
  // todo tag cond
  return (SPhyNode*)node;
}

static SPhyNode* createSingleTableScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  return createUserTableScanNode(pPlanNode, pTable, OP_TableScan);
}

static bool isSystemTable(SQueryTableInfo* pTable) {
  // todo
  return false;
}

static bool needSeqScan(SQueryPlanNode* pPlanNode) {
  // todo
  return false;
}

static SPhyNode* createMultiTableScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  if (isSystemTable(pTable)) {
    return createPseudoScanNode(pPlanNode, pTable, OP_SystemTableScan);
  } else if (needSeqScan(pPlanNode)) {
    return createUserTableScanNode(pPlanNode, pTable, OP_TableSeqScan);
  }
  return createUserTableScanNode(pPlanNode, pTable, OP_DataBlocksOptScan);
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
    taosArrayPush(pCxt->pCurrentSubplan->pChildern, &subplan);
    subplan->pParents = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    taosArrayPush(subplan->pParents, &pCxt->pCurrentSubplan);
  }
  SArray* currentLevel;
  if (subplan->level >= taosArrayGetSize(pCxt->pDag->pSubplans)) {
    currentLevel = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    taosArrayPush(pCxt->pDag->pSubplans, &currentLevel);
  } else {
    currentLevel = taosArrayGetP(pCxt->pDag->pSubplans, subplan->level);
  }
  taosArrayPush(currentLevel, &subplan);
  pCxt->pCurrentSubplan = subplan;
  return subplan;
}

static void vgroupToEpSet(const SVgroupMsg* vg, SEpSet* epSet) {
  epSet->inUse = 0; // todo
  epSet->numOfEps = vg->numOfEps;
  for (int8_t i = 0; i < vg->numOfEps; ++i) {
    epSet->port[i] = vg->epAddr[i].port;
    strcpy(epSet->fqdn[i], vg->epAddr[i].fqdn);
  }
  return;
}

static uint64_t splitSubplanByTable(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  SVgroupsInfo* vgroupList = pTable->pMeta->vgroupList;
  for (int32_t i = 0; i < pTable->pMeta->vgroupList->numOfVgroups; ++i) {
    STORE_CURRENT_SUBPLAN(pCxt);
    SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_SCAN);
    vgroupToEpSet(&(pTable->pMeta->vgroupList->vgroups[i]), &subplan->execEpSet);
    subplan->pNode = createMultiTableScanNode(pPlanNode, pTable);
    RECOVERY_CURRENT_SUBPLAN(pCxt);
  }
  return pCxt->nextId.templateId++;
}

static SPhyNode* createExchangeNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, uint64_t srcTemplateId) {
  SExchangePhyNode* node = (SExchangePhyNode*)initPhyNode(pPlanNode, OP_Exchange, sizeof(SExchangePhyNode));
  node->srcTemplateId = srcTemplateId;
  return (SPhyNode*)node;
}

static bool needMultiNodeScan(SQueryTableInfo* pTable) {
  // todo system table, for instance, user_tables
  return (TSDB_SUPER_TABLE == pTable->pMeta->pTableMeta->tableType);
}

static SPhyNode* createTableScanNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SQueryTableInfo* pTable = (SQueryTableInfo*)pPlanNode->pExtInfo;
  if (needMultiNodeScan(pTable)) {
    return createExchangeNode(pCxt, pPlanNode, splitSubplanByTable(pCxt, pPlanNode, pTable));
  }
  return createSingleTableScanNode(pPlanNode, pTable);
}

static SPhyNode* createPhyNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SPhyNode* node = NULL;
  switch (pPlanNode->info.type) {
    case QNODE_TAGSCAN:
      node = createTagScanNode(pPlanNode);
      break;
    case QNODE_TABLESCAN:
      node = createTableScanNode(pCxt, pPlanNode);
      break;
    default:
      assert(false);
  }
  if (pPlanNode->pChildren != NULL && taosArrayGetSize(pPlanNode->pChildren) > 0) {
    node->pChildren = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
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
  ++(pCxt->nextId.templateId);
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
    .pCurrentSubplan = NULL,
    .nextId = {0} // todo queryid
  };
  if (NULL == context.pDag) {
    return TSDB_CODE_TSC_OUT_OF_MEMORY;
  }
  context.pDag->pSubplans = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
  createSubplanByLevel(&context, pQueryNode);
  *pDag = context.pDag;
  return TSDB_CODE_SUCCESS;
}

int32_t setSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SArray* eps) {
  //todo
}
