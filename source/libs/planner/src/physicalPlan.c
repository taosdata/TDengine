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
#include "exception.h"
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

static void* validPointer(void* p) {
  if (NULL == p) {
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return p;
}

const char* opTypeToOpName(int32_t type) {
  return gOpName[type];
}

int32_t opNameToOpType(const char* name) {
  for (int32_t i = 1; i < sizeof(gOpName) / sizeof(gOpName[0]); ++i) {
    if (0 == strcmp(name, gOpName[i])) {
      return i;
    }
  }
  return OP_Unknown;
}

const char* dsinkTypeToDsinkName(int32_t type) {
  switch (type) {
    case DSINK_Dispatch:
      return "Dispatch";
    case DSINK_Insert:
      return "Insert";
    default:
      break;
  }
  return "Unknown";
}

int32_t dsinkNameToDsinkType(const char* name) {
  if (0 == strcmp(name, "Dispatch")) {
    return DSINK_Dispatch;
  } else if (0 == strcmp(name, "Insert")) {
    return DSINK_Insert;
  }
  return DSINK_Unknown;
}

static SDataSink* initDataSink(int32_t type, int32_t size) {
  SDataSink* sink = (SDataSink*)validPointer(calloc(1, size));
  sink->info.type = type;
  sink->info.name = dsinkTypeToDsinkName(type);
  return sink;
}

static SDataSink* createDataDispatcher(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SDataDispatcher* dispatcher = (SDataDispatcher*)initDataSink(DSINK_Dispatch, sizeof(SDataDispatcher));
  return (SDataSink*)dispatcher;
}

static SDataSink* createDataInserter(SPlanContext* pCxt, SVgDataBlocks* pBlocks) {
  SDataInserter* inserter = (SDataInserter*)initDataSink(DSINK_Insert, sizeof(SDataInserter));
  inserter->numOfTables = pBlocks->numOfTables;
  inserter->size = pBlocks->size;
  SWAP(inserter->pData, pBlocks->pData, char*);
  return (SDataSink*)inserter;
}

static bool toDataBlockSchema(SQueryPlanNode* pPlanNode, SDataBlockSchema* dataBlockSchema) {
  dataBlockSchema->numOfCols = pPlanNode->numOfCols;
  dataBlockSchema->pSchema = malloc(sizeof(SSlotSchema) * pPlanNode->numOfCols);
  if (NULL == dataBlockSchema->pSchema) {
    return false;
  }
  memcpy(dataBlockSchema->pSchema, pPlanNode->pSchema, sizeof(SSlotSchema) * pPlanNode->numOfCols);
  return true;
}

static bool cloneExprArray(SArray** dst, SArray* src) {
  if (NULL == src) {
    return true;
  }
  size_t size = taosArrayGetSize(src);
  if (0 == size) {
    return true;
  }
  *dst = taosArrayInit(size, POINTER_BYTES);
  if (NULL == *dst) {
    return false;
  }
  return (TSDB_CODE_SUCCESS == copyAllExprInfo(*dst, src, true) ? true : false);
}

static SPhyNode* initPhyNode(SQueryPlanNode* pPlanNode, int32_t type, int32_t size) {
  SPhyNode* node = (SPhyNode*)validPointer(calloc(1, size));
  node->info.type = type;
  node->info.name = opTypeToOpName(type);
  if (!cloneExprArray(&node->pTargets, pPlanNode->pExpr) || !toDataBlockSchema(pPlanNode, &(node->targetSchema))) {
    free(node);
    return NULL;
  }
  return node;
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
  SSubplan* subplan = validPointer(calloc(1, sizeof(SSubplan)));
  subplan->id = pCxt->nextId;
  ++(pCxt->nextId.subplanId);

  subplan->type  = type;
  subplan->level = 0;
  if (NULL != pCxt->pCurrentSubplan) {
    subplan->level = pCxt->pCurrentSubplan->level + 1;
    if (NULL == pCxt->pCurrentSubplan->pChildren) {
      pCxt->pCurrentSubplan->pChildren = validPointer(taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES));
    }

    taosArrayPush(pCxt->pCurrentSubplan->pChildren, &subplan);
    subplan->pParents = validPointer(taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES));
    taosArrayPush(subplan->pParents, &pCxt->pCurrentSubplan);
  }

  SArray* currentLevel;
  if (subplan->level >= taosArrayGetSize(pCxt->pDag->pSubplans)) {
    currentLevel = validPointer(taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES));
    taosArrayPush(pCxt->pDag->pSubplans, &currentLevel);
  } else {
    currentLevel = taosArrayGetP(pCxt->pDag->pSubplans, subplan->level);
  }

  taosArrayPush(currentLevel, &subplan);
  pCxt->pCurrentSubplan = subplan;
  ++(pCxt->pDag->numOfSubplans);
  return subplan;
}

static void vgroupInfoToEpSet(const SVgroupInfo* vg, SQueryNodeAddr* execNode) {
  execNode->nodeId = vg->vgId;
  execNode->inUse = 0; // todo
  execNode->numOfEps = vg->numOfEps;
  for (int8_t i = 0; i < vg->numOfEps; ++i) {
    execNode->epAddr[i] = vg->epAddr[i];
  }
  return;
}

static void vgroupMsgToEpSet(const SVgroupMsg* vg, SQueryNodeAddr* execNode) {
  execNode->nodeId = vg->vgId;
  execNode->inUse = 0; // todo
  execNode->numOfEps = vg->numOfEps;
  for (int8_t i = 0; i < vg->numOfEps; ++i) {
    execNode->epAddr[i] = vg->epAddr[i];
  }
  return;
}

static uint64_t splitSubplanByTable(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable) {
  SVgroupsInfo* vgroupList = pTable->pMeta->vgroupList;
  for (int32_t i = 0; i < pTable->pMeta->vgroupList->numOfVgroups; ++i) {
    STORE_CURRENT_SUBPLAN(pCxt);
    SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_SCAN);
    vgroupMsgToEpSet(&(pTable->pMeta->vgroupList->vgroups[i]), &subplan->execNode);
    subplan->pNode = createMultiTableScanNode(pPlanNode, pTable);
    subplan->pDataSink = createDataDispatcher(pCxt, pPlanNode);
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
    case QNODE_PROJECT:
//      node = create
    case QNODE_MODIFY:
      // Insert is not an operator in a physical plan.
      break;
    default:
      assert(false);
  }

  if (pPlanNode->pChildren != NULL && taosArrayGetSize(pPlanNode->pChildren) > 0) {
    node->pChildren = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    size_t size = taosArrayGetSize(pPlanNode->pChildren);
    for(int32_t i = 0; i < size; ++i) {
      SPhyNode* child = createPhyNode(pCxt, taosArrayGetP(pPlanNode->pChildren, i));
      child->pParent = node;
      taosArrayPush(node->pChildren, &child);
    }
  }

  return node;
}

static void splitModificationOpSubPlan(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SDataPayloadInfo* pPayload = (SDataPayloadInfo*) pPlanNode->pExtInfo;

  size_t numOfVgroups = taosArrayGetSize(pPayload->payload);
  for (int32_t i = 0; i < numOfVgroups; ++i) {
    STORE_CURRENT_SUBPLAN(pCxt);
    SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_MODIFY);
    SVgDataBlocks* blocks = (SVgDataBlocks*)taosArrayGetP(pPayload->payload, i);

    vgroupInfoToEpSet(&blocks->vg, &subplan->execNode);
    subplan->pDataSink  = createDataInserter(pCxt, blocks);
    subplan->pNode      = NULL;
    subplan->type       = QUERY_TYPE_MODIFY;
    subplan->msgType    = pPayload->msgType;
    subplan->id.queryId = pCxt->pDag->queryId;

    RECOVERY_CURRENT_SUBPLAN(pCxt);
  }
}

static void createSubplanByLevel(SPlanContext* pCxt, SQueryPlanNode* pRoot) {
  if (QNODE_MODIFY == pRoot->info.type) {
    splitModificationOpSubPlan(pCxt, pRoot);
  } else {
    SSubplan* subplan  = initSubplan(pCxt, QUERY_TYPE_MERGE);
    ++(pCxt->nextId.templateId);

    subplan->msgType   = TDMT_VND_QUERY;
    subplan->pNode     = createPhyNode(pCxt, pRoot);
    subplan->pDataSink = createDataDispatcher(pCxt, pRoot);
  }
  // todo deal subquery
}

int32_t createDag(SQueryPlanNode* pQueryNode, struct SCatalog* pCatalog, SQueryDag** pDag, uint64_t requestId) {
  TRY(TSDB_MAX_TAG_CONDITIONS) {
    SPlanContext context = {
      .pCatalog = pCatalog,
      .pDag = validPointer(calloc(1, sizeof(SQueryDag))),
      .pCurrentSubplan = NULL,
      .nextId = {.queryId = requestId},
    };

    *pDag = context.pDag;
    context.pDag->queryId = requestId;

    context.pDag->pSubplans = validPointer(taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES));
    createSubplanByLevel(&context, pQueryNode);
  } CATCH(code) {
    CLEANUP_EXECUTE();
    terrno = code;
    return TSDB_CODE_FAILED;
  } END_TRY
  return TSDB_CODE_SUCCESS;
}

int32_t setSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SQueryNodeAddr* ep) {
  //todo
}
