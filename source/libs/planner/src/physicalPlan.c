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

#if 0

#include "plannerInt.h"
#include "texception.h"
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

static bool copySchema(SDataBlockSchema* dst, const SDataBlockSchema* src) {
  dst->pSchema = malloc(sizeof(SSlotSchema) * src->numOfCols);
  if (NULL == dst->pSchema) {
    return false;
  }
  memcpy(dst->pSchema, src->pSchema, sizeof(SSlotSchema) * src->numOfCols);
  dst->numOfCols = src->numOfCols;
  dst->resultRowSize = src->resultRowSize;
  dst->precision = src->precision;
  return true;
}

static bool toDataBlockSchema(SQueryPlanNode* pPlanNode, SDataBlockSchema* dataBlockSchema) {
  dataBlockSchema->numOfCols = pPlanNode->numOfExpr;
  dataBlockSchema->pSchema = malloc(sizeof(SSlotSchema) * pPlanNode->numOfExpr);
  if (NULL == dataBlockSchema->pSchema) {
    return false;
  }

  dataBlockSchema->resultRowSize = 0;
  for (int32_t i = 0; i < pPlanNode->numOfExpr; ++i) {
    SExprInfo* pExprInfo = taosArrayGetP(pPlanNode->pExpr, i);
    memcpy(&dataBlockSchema->pSchema[i], &pExprInfo->base.resSchema, sizeof(SSlotSchema));

    dataBlockSchema->resultRowSize += dataBlockSchema->pSchema[i].bytes;
  }

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

static SDataSink* initDataSink(int32_t type, int32_t size, const SPhyNode* pRoot) {
  SDataSink* sink = (SDataSink*)validPointer(calloc(1, size));
  sink->info.type = type;
  sink->info.name = dsinkTypeToDsinkName(type);
  if (NULL !=pRoot && !copySchema(&sink->schema, &pRoot->targetSchema)) {
    tfree(sink);
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return sink;
}

static SDataSink* createDataInserter(SPlanContext* pCxt, SVgDataBlocks* pBlocks, const SPhyNode* pRoot) {
  SDataInserter* inserter = (SDataInserter*)initDataSink(DSINK_Insert, sizeof(SDataInserter), pRoot);
  inserter->numOfTables = pBlocks->numOfTables;
  inserter->size = pBlocks->size;
  TSWAP(inserter->pData, pBlocks->pData, char*);
  return (SDataSink*)inserter;
}

static SDataSink* createDataDispatcher(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, const SPhyNode* pRoot) {
  SDataDispatcher* dispatcher = (SDataDispatcher*)initDataSink(DSINK_Dispatch, sizeof(SDataDispatcher), pRoot);
  return (SDataSink*)dispatcher;
}

static SPhyNode* initPhyNode(SQueryPlanNode* pPlanNode, int32_t type, int32_t size) {
  SPhyNode* node = (SPhyNode*)validPointer(calloc(1, size));
  node->info.type = type;
  node->info.name = opTypeToOpName(type);
  if (!cloneExprArray(&node->pTargets, pPlanNode->pExpr) || !toDataBlockSchema(pPlanNode, &(node->targetSchema))) {
    free(node);
    THROW(TSDB_CODE_TSC_OUT_OF_MEMORY);
  }
  return node;
}

static void cleanupPhyNode(SPhyNode* pPhyNode) {
  if (pPhyNode == NULL) {
    return;
  }

  dropOneLevelExprInfo(pPhyNode->pTargets);
  tfree(pPhyNode->targetSchema.pSchema);
  tfree(pPhyNode);
}

static SPhyNode* initScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTable, int32_t type, int32_t size) {
  SScanPhyNode* node = (SScanPhyNode*) initPhyNode(pPlanNode, type, size);

  STableMeta *pTableMeta = pTable->pMeta->pTableMeta;
  node->uid       = pTableMeta->uid;
  node->count     = 1;
  node->order     = TSDB_ORDER_ASC;
  node->tableType = pTableMeta->tableType;

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
  return MAIN_SCAN;
}

static SPhyNode* createUserTableScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pQueryTableInfo, int32_t op) {
  STableScanPhyNode* node = (STableScanPhyNode*)initScanNode(pPlanNode, pQueryTableInfo, op, sizeof(STableScanPhyNode));
  node->scanFlag = getScanFlag(pPlanNode, pQueryTableInfo);
  node->window = pQueryTableInfo->window;
  // todo tag cond
  return (SPhyNode*)node;
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
  int32_t type = (pPlanNode->info.type == QNODE_TABLESCAN)? OP_TableScan:OP_StreamScan;
  return createUserTableScanNode(pPlanNode, pTable, type);
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

static void vgroupInfoToNodeAddr(const SVgroupInfo* vg, SQueryNodeAddr* pNodeAddr) {
  pNodeAddr->nodeId = vg->vgId;
  pNodeAddr->epset  = vg->epset;
}

static uint64_t splitSubplanByTable(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, SQueryTableInfo* pTableInfo) {
  SVgroupsInfo* pVgroupList = pTableInfo->pMeta->vgroupList;
  for (int32_t i = 0; i < pVgroupList->numOfVgroups; ++i) {
    STORE_CURRENT_SUBPLAN(pCxt);
    SSubplan* subplan = initSubplan(pCxt, QUERY_TYPE_SCAN);
    subplan->msgType   = TDMT_VND_QUERY;

    vgroupInfoToNodeAddr(&(pTableInfo->pMeta->vgroupList->vgroups[i]), &subplan->execNode);
    subplan->pNode = createMultiTableScanNode(pPlanNode, pTableInfo);
    subplan->pDataSink = createDataDispatcher(pCxt, pPlanNode, subplan->pNode);
    RECOVERY_CURRENT_SUBPLAN(pCxt);
  }
  return pCxt->nextId.templateId++;
}

static SPhyNode* createExchangeNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode, uint64_t srcTemplateId) {
  SExchangePhyNode* node = (SExchangePhyNode*)initPhyNode(pPlanNode, OP_Exchange, sizeof(SExchangePhyNode));
  node->srcTemplateId = srcTemplateId;
  node->pSrcEndPoints = validPointer(taosArrayInit(TARRAY_MIN_SIZE, sizeof(SDownstreamSource)));
  return (SPhyNode*)node;
}

static bool needMultiNodeScan(SQueryTableInfo* pTable) {
  // todo system table, for instance, user_tables
  return (TSDB_SUPER_TABLE == pTable->pMeta->pTableMeta->tableType);
}

// TODO: the SVgroupInfo index
static SPhyNode* createSingleTableScanNode(SQueryPlanNode* pPlanNode, SQueryTableInfo* pTableInfo, SSubplan* subplan) {
  SVgroupsInfo* pVgroupsInfo = pTableInfo->pMeta->vgroupList;
  vgroupInfoToNodeAddr(&(pVgroupsInfo->vgroups[0]), &subplan->execNode);
  int32_t type = (pPlanNode->info.type == QNODE_TABLESCAN)? OP_TableScan:OP_StreamScan;
  return createUserTableScanNode(pPlanNode, pTableInfo, type);
}

static SPhyNode* createTableScanNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SQueryTableInfo* pTable = (SQueryTableInfo*)pPlanNode->pExtInfo;
  if (needMultiNodeScan(pTable)) {
    return createExchangeNode(pCxt, pPlanNode, splitSubplanByTable(pCxt, pPlanNode, pTable));
  }

  return createSingleTableScanNode(pPlanNode, pTable, pCxt->pCurrentSubplan);
}

static SPhyNode* createSingleTableAgg(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SAggPhyNode* node = (SAggPhyNode*)initPhyNode(pPlanNode, OP_Aggregate, sizeof(SAggPhyNode));
  SGroupbyExpr* pGroupBy = (SGroupbyExpr*)pPlanNode->pExtInfo;
  node->aggAlgo = AGG_ALGO_PLAIN;
  node->aggSplit = AGG_SPLIT_FINAL;
  if (NULL != pGroupBy) {
    node->aggAlgo = AGG_ALGO_HASHED;
    node->pGroupByList = validPointer(taosArrayDup(pGroupBy->columnInfo));
  }
  return (SPhyNode*)node;
}

static SPhyNode* createAggNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  // if (needMultiNodeAgg(pPlanNode)) {

  // }
  return createSingleTableAgg(pCxt, pPlanNode);
}

static SPhyNode* createPhyNode(SPlanContext* pCxt, SQueryPlanNode* pPlanNode) {
  SPhyNode* node = NULL;
  switch (pPlanNode->info.type) {
    case QNODE_TAGSCAN:
      node = createTagScanNode(pPlanNode);
      break;
    case QNODE_STREAMSCAN:
    case QNODE_TABLESCAN:
      node = createTableScanNode(pCxt, pPlanNode);
      break;
    case QNODE_AGGREGATE:
    case QNODE_GROUPBY:
      node = createAggNode(pCxt, pPlanNode);
      break;
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

    subplan->execNode.epset = blocks->vg.epset;
    subplan->pDataSink  = createDataInserter(pCxt, blocks, NULL);
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
    SSubplan* subplan  = initSubplan(pCxt, QUERY_TYPE_SCAN);
    ++(pCxt->nextId.templateId);

    subplan->msgType   = TDMT_VND_QUERY;
    subplan->pNode     = createPhyNode(pCxt, pRoot);
    subplan->pDataSink = createDataDispatcher(pCxt, pRoot, subplan->pNode);
  }
  // todo deal subquery
}

static void postCreateDag(SQueryPlanNode* pQueryNode, SQueryDag* pDag, SArray* pNodeList) {
  // The exchange operator is not necessary, in case of the stream scan.
  // Here we need to remove it from the DAG.
  if (pQueryNode->info.type == QNODE_STREAMSCAN) {
    SArray* pRootLevel = taosArrayGetP(pDag->pSubplans, 0);
    SSubplan *pSubplan = taosArrayGetP(pRootLevel, 0);

    if (pSubplan->pNode->info.type == OP_Exchange) {
      ASSERT(taosArrayGetSize(pRootLevel) == 1);

      taosArrayRemove(pDag->pSubplans, 0);
      // And then update the number of the subplans.
      pDag->numOfSubplans -= 1;
    }
  } else {
    // Traverse the dag again to acquire the execution node.
    if (pNodeList != NULL) {
      SArray** pSubLevel = taosArrayGetLast(pDag->pSubplans);
      size_t   num = taosArrayGetSize(*pSubLevel);
      for (int32_t j = 0; j < num; ++j) {
        SSubplan* pPlan = taosArrayGetP(*pSubLevel, j);
        taosArrayPush(pNodeList, &pPlan->execNode);
      }
    }
  }
}

int32_t createDag(SQueryPlanNode* pQueryNode, struct SCatalog* pCatalog, SQueryDag** pDag, SArray* pNodeList, uint64_t requestId) {
  TRY(TSDB_MAX_TAG_CONDITIONS) {
    SPlanContext context = {
      .pCatalog = pCatalog,
      .pDag     = validPointer(calloc(1, sizeof(SQueryDag))),
      .pCurrentSubplan = NULL,
       //The unsigned Id starting from 1 would be better
      .nextId   = {.queryId = requestId, .subplanId = 1, .templateId = 1},
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

  postCreateDag(pQueryNode, *pDag, pNodeList);
  return TSDB_CODE_SUCCESS;
}

void setExchangSourceNode(uint64_t templateId, SDownstreamSource *pSource, SPhyNode* pNode) {
  if (NULL == pNode) {
    return;
  }
  if (OP_Exchange == pNode->info.type) {
    SExchangePhyNode* pExchange = (SExchangePhyNode*)pNode;
    if (templateId == pExchange->srcTemplateId) {
      taosArrayPush(pExchange->pSrcEndPoints, pSource);
    }
  }

  if (pNode->pChildren != NULL) {
    size_t size = taosArrayGetSize(pNode->pChildren);
    for(int32_t i = 0; i < size; ++i) {
      setExchangSourceNode(templateId, pSource, taosArrayGetP(pNode->pChildren, i));
    }
  }
}

void setSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SDownstreamSource* pSource) {
  setExchangSourceNode(templateId, pSource, subplan->pNode);
}

static void destroyDataSinkNode(SDataSink* pSinkNode) {
  if (pSinkNode == NULL) {
    return;
  }

  if (queryNodeType(pSinkNode) == DSINK_Dispatch) {
    SDataDispatcher* pDdSink = (SDataDispatcher*)pSinkNode;
    tfree(pDdSink->sink.schema.pSchema);
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
  cleanupPhyNode(pSubplan->pNode);

  tfree(pSubplan);
}

#endif

#include "plannerInt.h"

#include "functionMgt.h"

typedef struct SSlotIndex {
  int16_t dataBlockId;
  int16_t slotId;
} SSlotIndex;

typedef struct SPhysiPlanContext {
  SPlanContext* pPlanCxt;
  int32_t errCode;
  int16_t nextDataBlockId;
  SArray* pLocationHelper;
} SPhysiPlanContext;

static int32_t getSlotKey(SNode* pNode, char* pKey) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if ('\0' == pCol->tableAlias[0]) {
      return sprintf(pKey, "%s", pCol->colName);
    }
    return sprintf(pKey, "%s.%s", pCol->tableAlias, pCol->colName);
  }
  return sprintf(pKey, "%s", ((SExprNode*)pNode)->aliasName);
}

static SNode* createSlotDesc(SPhysiPlanContext* pCxt, const SNode* pNode, int16_t slotId) {
  SSlotDescNode* pSlot = (SSlotDescNode*)nodesMakeNode(QUERY_NODE_SLOT_DESC);
  CHECK_ALLOC(pSlot, NULL);
  pSlot->slotId = slotId;
  pSlot->dataType = ((SExprNode*)pNode)->resType;
  pSlot->reserve = false;
  pSlot->output = false;
  return (SNode*)pSlot;
}

static SNode* createTarget(SNode* pNode, int16_t dataBlockId, int16_t slotId) {
  STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
  if (NULL == pTarget) {
    return NULL;
  }
  pTarget->dataBlockId = dataBlockId;
  pTarget->slotId = slotId;
  pTarget->pExpr = pNode;
  return (SNode*)pTarget;
}

static int32_t addDataBlockDesc(SPhysiPlanContext* pCxt, SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  SHashObj* pHash = NULL;
  if (NULL == pDataBlockDesc->pSlots) {
    pDataBlockDesc->pSlots = nodesMakeList();
    CHECK_ALLOC(pDataBlockDesc->pSlots, TSDB_CODE_OUT_OF_MEMORY);

    pHash = taosHashInit(LIST_LENGTH(pList), taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
    CHECK_ALLOC(pHash, TSDB_CODE_OUT_OF_MEMORY);
    if (NULL == taosArrayInsert(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId, &pHash)) {
      taosHashCleanup(pHash);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  }
  
  SNode* pNode = NULL;
  int16_t slotId = taosHashGetSize(pHash);
  FOREACH(pNode, pList) {
    SNode* pSlot = createSlotDesc(pCxt, pNode, slotId);
    CHECK_ALLOC(pSlot, TSDB_CODE_OUT_OF_MEMORY);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pDataBlockDesc->pSlots, (SNode*)pSlot)) {
      nodesDestroyNode(pSlot);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    SSlotIndex index = { .dataBlockId = pDataBlockDesc->dataBlockId, .slotId = slotId };
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
    int32_t len = getSlotKey(pNode, name);
    CHECK_CODE(taosHashPut(pHash, name, len, &index, sizeof(SSlotIndex)), TSDB_CODE_OUT_OF_MEMORY);

    SNode* pTarget = createTarget(pNode, pDataBlockDesc->dataBlockId, slotId);
    CHECK_ALLOC(pTarget, TSDB_CODE_OUT_OF_MEMORY);
    REPLACE_NODE(pTarget);
  
    ++slotId;
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct SSetSlotIdCxt {
  int32_t errCode;
  SHashObj* pLeftHash;
  SHashObj* pRightHash;
} SSetSlotIdCxt;

static EDealRes doSetSlotId(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 != strcmp(((SColumnNode*)pNode)->colName, "*")) {
    SSetSlotIdCxt* pCxt = (SSetSlotIdCxt*)pContext;
    char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
    int32_t len = getSlotKey(pNode, name);
    SSlotIndex* pIndex = taosHashGet(pCxt->pLeftHash, name, len);
    if (NULL == pIndex) {
      pIndex = taosHashGet(pCxt->pRightHash, name, len);
    }
    // pIndex is definitely not NULL, otherwise it is a bug
    ((SColumnNode*)pNode)->dataBlockId = pIndex->dataBlockId;
    ((SColumnNode*)pNode)->slotId = pIndex->slotId;
    CHECK_ALLOC(pNode, DEAL_RES_ERROR);
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static SNode* setNodeSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNode* pNode) {
  SNode* pRes = nodesCloneNode(pNode);
  CHECK_ALLOC(pRes, NULL);
  SSetSlotIdCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)) };
  nodesWalkNode(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyNode(pRes);
    return NULL;
  }
  return pRes;
}

static SNodeList* setListSlotId(SPhysiPlanContext* pCxt, int16_t leftDataBlockId, int16_t rightDataBlockId, SNodeList* pList) {
  SNodeList* pRes = nodesCloneList(pList);
  CHECK_ALLOC(pRes, NULL);
  SSetSlotIdCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pLeftHash = taosArrayGetP(pCxt->pLocationHelper, leftDataBlockId),
      .pRightHash = (rightDataBlockId < 0 ? NULL : taosArrayGetP(pCxt->pLocationHelper, rightDataBlockId)) };
  nodesWalkList(pRes, doSetSlotId, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(pRes);
    return NULL;
  }
  return pRes;
}

static SPhysiNode* makePhysiNode(SPhysiPlanContext* pCxt, ENodeType type) {
  SPhysiNode* pPhysiNode = (SPhysiNode*)nodesMakeNode(type);
  if (NULL == pPhysiNode) {
    return NULL;
  }
  pPhysiNode->outputDataBlockDesc.dataBlockId = pCxt->nextDataBlockId++;
  pPhysiNode->outputDataBlockDesc.type = QUERY_NODE_DATABLOCK_DESC;
  return pPhysiNode;
}

static int32_t setConditionsSlotId(SPhysiPlanContext* pCxt, const SLogicNode* pLogicNode, SPhysiNode* pPhysiNode) {
  if (NULL != pLogicNode->pConditions) {
    pPhysiNode->pConditions = setNodeSlotId(pCxt, pPhysiNode->outputDataBlockDesc.dataBlockId, -1, pLogicNode->pConditions);
    CHECK_ALLOC(pPhysiNode->pConditions, TSDB_CODE_OUT_OF_MEMORY);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setSlotOutput(SPhysiPlanContext* pCxt, SNodeList* pTargets, SDataBlockDescNode* pDataBlockDesc) {
  SHashObj* pHash = taosArrayGetP(pCxt->pLocationHelper, pDataBlockDesc->dataBlockId);
  char name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  SNode* pNode;
  FOREACH(pNode, pTargets) {
    int32_t len = getSlotKey(pNode, name);
    SSlotIndex* pIndex = taosHashGet(pHash, name, len);
    ((SSlotDescNode*)nodesListGetNode(pDataBlockDesc->pSlots, pIndex->slotId))->output = true;
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t initScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode, SScanPhysiNode* pScanPhysiNode) {
  if (NULL != pScanLogicNode->pScanCols) {
    pScanPhysiNode->pScanCols = nodesCloneList(pScanLogicNode->pScanCols);
    CHECK_ALLOC(pScanPhysiNode->pScanCols, TSDB_CODE_OUT_OF_MEMORY);
  }
  // Data block describe also needs to be set without scanning column, such as SELECT COUNT(*) FROM t
  CHECK_CODE(addDataBlockDesc(pCxt, pScanPhysiNode->pScanCols, &pScanPhysiNode->node.outputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pScanLogicNode, (SPhysiNode*)pScanPhysiNode), TSDB_CODE_OUT_OF_MEMORY);

  CHECK_CODE(setSlotOutput(pCxt, pScanLogicNode->node.pTargets, &pScanPhysiNode->node.outputDataBlockDesc), TSDB_CODE_OUT_OF_MEMORY);

  pScanPhysiNode->uid = pScanLogicNode->pMeta->uid;
  pScanPhysiNode->tableType = pScanLogicNode->pMeta->tableType;
  pScanPhysiNode->order = TSDB_ORDER_ASC;
  pScanPhysiNode->count = 1;
  pScanPhysiNode->reverse = 0;

  return TSDB_CODE_SUCCESS;
}

static SPhysiNode* createTagScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode) {
  STagScanPhysiNode* pTagScan = (STagScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN);
  CHECK_ALLOC(pTagScan, NULL);
  CHECK_CODE(initScanPhysiNode(pCxt, pScanLogicNode, (SScanPhysiNode*)pTagScan), (SPhysiNode*)pTagScan);
  return (SPhysiNode*)pTagScan;
}

static SPhysiNode* createTableScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode) {
  STableScanPhysiNode* pTableScan = (STableScanPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
  CHECK_ALLOC(pTableScan, NULL);
  CHECK_CODE(initScanPhysiNode(pCxt, pScanLogicNode, (SScanPhysiNode*)pTableScan), (SPhysiNode*)pTableScan);
  pTableScan->scanFlag = pScanLogicNode->scanFlag;
  pTableScan->scanRange = pScanLogicNode->scanRange;
  return (SPhysiNode*)pTableScan;
}

static SPhysiNode* createScanPhysiNode(SPhysiPlanContext* pCxt, SScanLogicNode* pScanLogicNode) {
  switch (pScanLogicNode->scanType) {
    case SCAN_TYPE_TAG:
      return createTagScanPhysiNode(pCxt, pScanLogicNode);
    case SCAN_TYPE_TABLE:
      return createTableScanPhysiNode(pCxt, pScanLogicNode);
    case SCAN_TYPE_STABLE:
    case SCAN_TYPE_STREAM:
      break;
    default:
      break;
  }
  return NULL;
}

static SNodeList* createJoinOutputCols(SPhysiPlanContext* pCxt, SDataBlockDescNode* pLeftDesc, SDataBlockDescNode* pRightDesc) {
  SNodeList* pCols = nodesMakeList();
  CHECK_ALLOC(pCols, NULL);
  SNode* pNode;
  FOREACH(pNode, pLeftDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      goto error;
    }
    pCol->node.resType = pSlot->dataType;
    pCol->dataBlockId = pLeftDesc->dataBlockId;
    pCol->slotId = pSlot->slotId;
    pCol->colId = -1;
    if (TSDB_CODE_SUCCESS != nodesListAppend(pCols, (SNode*)pCol)) {
      goto error;
    }
  }
  FOREACH(pNode, pRightDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    if (NULL == pCol) {
      goto error;
    }
    pCol->node.resType = pSlot->dataType;
    pCol->dataBlockId = pRightDesc->dataBlockId;
    pCol->slotId = pSlot->slotId;
    pCol->colId = -1;
    if (TSDB_CODE_SUCCESS != nodesListAppend(pCols, (SNode*)pCol)) {
      goto error;
    }
  }
  return pCols;
error:
  nodesDestroyList(pCols);
  return NULL;
}

static SPhysiNode* createJoinPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SJoinLogicNode* pJoinLogicNode) {
  SJoinPhysiNode* pJoin = (SJoinPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_JOIN);
  CHECK_ALLOC(pJoin, NULL);

  SDataBlockDescNode* pLeftDesc = &((SPhysiNode*)nodesListGetNode(pChildren, 0))->outputDataBlockDesc;
  SDataBlockDescNode* pRightDesc = &((SPhysiNode*)nodesListGetNode(pChildren, 1))->outputDataBlockDesc;
  pJoin->pOnConditions = setNodeSlotId(pCxt, pLeftDesc->dataBlockId, pRightDesc->dataBlockId, pJoinLogicNode->pOnConditions);
  CHECK_ALLOC(pJoin->pOnConditions, (SPhysiNode*)pJoin);

  pJoin->pTargets = createJoinOutputCols(pCxt, pLeftDesc, pRightDesc);
  CHECK_ALLOC(pJoin->pTargets, (SPhysiNode*)pJoin);
  CHECK_CODE(addDataBlockDesc(pCxt, pJoin->pTargets, &pJoin->node.outputDataBlockDesc), (SPhysiNode*)pJoin);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pJoinLogicNode, (SPhysiNode*)pJoin), (SPhysiNode*)pJoin);

  CHECK_CODE(setSlotOutput(pCxt, pJoinLogicNode->node.pTargets, &pJoin->node.outputDataBlockDesc), (SPhysiNode*)pJoin);

  return (SPhysiNode*)pJoin;
}

typedef struct SRewritePrecalcExprsCxt {
  int32_t errCode;
  int32_t planNodeId;
  int32_t rewriteId;
  SNodeList* pPrecalcExprs;
} SRewritePrecalcExprsCxt;

static EDealRes collectAndRewrite(SRewritePrecalcExprsCxt* pCxt, SNode** pNode) {
  SNode* pExpr = nodesCloneNode(*pNode);
  CHECK_ALLOC(pExpr, DEAL_RES_ERROR);
  if (nodesListAppend(pCxt->pPrecalcExprs, pExpr)) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == pCol) {
    nodesDestroyNode(pExpr);
    return DEAL_RES_ERROR;
  }
  SExprNode* pRewrittenExpr = (SExprNode*)pExpr;
  pCol->node.resType = pRewrittenExpr->resType;
  if ('\0' != pRewrittenExpr->aliasName[0]) {
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  } else {
    snprintf(pRewrittenExpr->aliasName, sizeof(pRewrittenExpr->aliasName), "#expr_%d_%d", pCxt->planNodeId, pCxt->rewriteId);
    strcpy(pCol->colName, pRewrittenExpr->aliasName);
  }
  nodesDestroyNode(*pNode);
  *pNode = (SNode*)pCol;
  return DEAL_RES_IGNORE_CHILD;
}

static EDealRes doRewritePrecalcExprs(SNode** pNode, void* pContext) {
  SRewritePrecalcExprsCxt* pCxt = (SRewritePrecalcExprsCxt*)pContext;
  switch (nodeType(*pNode)) {
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION: {
      return collectAndRewrite(pContext, pNode);
    }
    case QUERY_NODE_FUNCTION: {
      if (!fmIsAggFunc(((SFunctionNode*)(*pNode))->funcId)) {
        return collectAndRewrite(pContext, pNode);
      }
    }
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t rewritePrecalcExprs(SPhysiPlanContext* pCxt, SNodeList* pList, SNodeList** pPrecalcExprs, SNodeList** pRewrittenList) {
  if (NULL == pList) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == *pPrecalcExprs) {
    *pPrecalcExprs = nodesMakeList();
    CHECK_ALLOC(*pPrecalcExprs, TSDB_CODE_OUT_OF_MEMORY);
  }
  if (NULL == *pRewrittenList) {
    *pRewrittenList = nodesMakeList();
    CHECK_ALLOC(*pRewrittenList, TSDB_CODE_OUT_OF_MEMORY);
  }
  SNode* pNode = NULL;
  FOREACH(pNode, pList) {
    SNode* pNew = NULL;
    if (QUERY_NODE_GROUPING_SET == nodeType(pNode)) {
      pNew = nodesCloneNode(nodesListGetNode(((SGroupingSetNode*)pNode)->pParameterList, 0));
    } else {
      pNew = nodesCloneNode(pNode);
    }
    CHECK_ALLOC(pNew, TSDB_CODE_OUT_OF_MEMORY);
    CHECK_CODE(nodesListAppend(*pRewrittenList, pNew), TSDB_CODE_OUT_OF_MEMORY);
  }
  SRewritePrecalcExprsCxt cxt = { .errCode = TSDB_CODE_SUCCESS, .pPrecalcExprs = *pPrecalcExprs };
  nodesRewriteList(*pRewrittenList, doRewritePrecalcExprs, &cxt);
  if (0 == LIST_LENGTH(cxt.pPrecalcExprs)) {
    nodesDestroyList(cxt.pPrecalcExprs);
    *pPrecalcExprs = NULL;
  }
  return cxt.errCode;
}

static SPhysiNode* createAggPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SAggLogicNode* pAggLogicNode) {
  SAggPhysiNode* pAgg = (SAggPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_AGG);
  CHECK_ALLOC(pAgg, NULL);

  SNodeList* pPrecalcExprs = NULL;
  SNodeList* pGroupKeys = NULL;
  SNodeList* pAggFuncs = NULL;
  CHECK_CODE(rewritePrecalcExprs(pCxt, pAggLogicNode->pGroupKeys, &pPrecalcExprs, &pGroupKeys), (SPhysiNode*)pAgg);
  CHECK_CODE(rewritePrecalcExprs(pCxt, pAggLogicNode->pAggFuncs, &pPrecalcExprs, &pAggFuncs), (SPhysiNode*)pAgg);

  SDataBlockDescNode* pChildTupe = &(((SPhysiNode*)nodesListGetNode(pChildren, 0))->outputDataBlockDesc);
  // push down expression to outputDataBlockDesc of child node
  if (NULL != pPrecalcExprs) {
    pAgg->pExprs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pPrecalcExprs);
    CHECK_ALLOC(pAgg->pExprs, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pExprs, pChildTupe), (SPhysiNode*)pAgg);
  }

  if (NULL != pGroupKeys) {
    pAgg->pGroupKeys = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pGroupKeys);
    CHECK_ALLOC(pAgg->pGroupKeys, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pGroupKeys, &pAgg->node.outputDataBlockDesc), (SPhysiNode*)pAgg);
  }

  if (NULL != pAggFuncs) {
    pAgg->pAggFuncs = setListSlotId(pCxt, pChildTupe->dataBlockId, -1, pAggFuncs);
    CHECK_ALLOC(pAgg->pAggFuncs, (SPhysiNode*)pAgg);
    CHECK_CODE(addDataBlockDesc(pCxt, pAgg->pAggFuncs, &pAgg->node.outputDataBlockDesc), (SPhysiNode*)pAgg);
  }

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pAggLogicNode, (SPhysiNode*)pAgg), (SPhysiNode*)pAgg);

  CHECK_CODE(setSlotOutput(pCxt, pAggLogicNode->node.pTargets, &pAgg->node.outputDataBlockDesc), (SPhysiNode*)pAgg);

  return (SPhysiNode*)pAgg;
}

static SPhysiNode* createProjectPhysiNode(SPhysiPlanContext* pCxt, SNodeList* pChildren, SProjectLogicNode* pProjectLogicNode) {
  SProjectPhysiNode* pProject = (SProjectPhysiNode*)makePhysiNode(pCxt, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
  CHECK_ALLOC(pProject, NULL);

  pProject->pProjections = setListSlotId(pCxt, ((SPhysiNode*)nodesListGetNode(pChildren, 0))->outputDataBlockDesc.dataBlockId, -1, pProjectLogicNode->pProjections);
  CHECK_ALLOC(pProject->pProjections, (SPhysiNode*)pProject);
  CHECK_CODE(addDataBlockDesc(pCxt, pProject->pProjections, &pProject->node.outputDataBlockDesc), (SPhysiNode*)pProject);

  CHECK_CODE(setConditionsSlotId(pCxt, (const SLogicNode*)pProjectLogicNode, (SPhysiNode*)pProject), (SPhysiNode*)pProject);

  return (SPhysiNode*)pProject;
}

static SPhysiNode* createPhysiNode(SPhysiPlanContext* pCxt, SLogicNode* pLogicPlan) {
  SNodeList* pChildren = nodesMakeList();
  CHECK_ALLOC(pChildren, NULL);

  SNode* pLogicChild;
  FOREACH(pLogicChild, pLogicPlan->pChildren) {
    SNode* pChildPhyNode = (SNode*)createPhysiNode(pCxt, (SLogicNode*)pLogicChild);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pChildren, pChildPhyNode)) {
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;
      nodesDestroyList(pChildren);
      return NULL;
    }
  }

  SPhysiNode* pPhyNode = NULL;
  switch (nodeType(pLogicPlan)) {
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      pPhyNode = createScanPhysiNode(pCxt, (SScanLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      pPhyNode = createJoinPhysiNode(pCxt, pChildren, (SJoinLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      pPhyNode = createAggPhysiNode(pCxt, pChildren, (SAggLogicNode*)pLogicPlan);
      break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      pPhyNode = createProjectPhysiNode(pCxt, pChildren, (SProjectLogicNode*)pLogicPlan);
      break;
    default:
      break;
  }

  pPhyNode->pChildren = pChildren;
  SNode* pChild;
  FOREACH(pChild, pPhyNode->pChildren) {
    ((SPhysiNode*)pChild)->pParent = pPhyNode;
  }

  return pPhyNode;
}

static SSubplan* createPhysiSubplan(SPhysiPlanContext* pCxt, SSubLogicPlan* pLogicSubplan) {
  SSubplan* pSubplan = (SSubplan*)nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN);
  pSubplan->pNode = createPhysiNode(pCxt, pLogicSubplan->pNode);
  return pSubplan;
}

static SQueryLogicPlan* createRawQueryLogicPlan(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode) {
  SQueryLogicPlan* pLogicPlan = (SQueryLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN);
  CHECK_ALLOC(pLogicPlan, NULL);
  pLogicPlan->pSubplans = nodesMakeList();
  CHECK_ALLOC(pLogicPlan->pSubplans, pLogicPlan);
  SNodeListNode* pTopSubplans = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_ALLOC(pTopSubplans, pLogicPlan);
  if (TSDB_CODE_SUCCESS != nodesListAppend(pLogicPlan->pSubplans, (SNode*)pTopSubplans)) {
    nodesDestroyNode((SNode*)pTopSubplans);
    return pLogicPlan;
  }
  pTopSubplans->pNodeList = nodesMakeList();
  CHECK_ALLOC(pTopSubplans->pNodeList, pLogicPlan);
  SSubLogicPlan* pSubplan = (SSubLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  CHECK_ALLOC(pSubplan, pLogicPlan);
  if (TSDB_CODE_SUCCESS != nodesListAppend(pTopSubplans->pNodeList, (SNode*)pSubplan)) {
    nodesDestroyNode((SNode*)pSubplan);
    return pLogicPlan;
  }
  pSubplan->pNode = (SLogicNode*)nodesCloneNode((SNode*)pLogicNode);
  CHECK_ALLOC(pSubplan->pNode, pLogicPlan);
  return pLogicPlan;
}

static int32_t splitLogicPlan(SPhysiPlanContext* pCxt, SLogicNode* pLogicNode, SQueryLogicPlan** pLogicPlan) {
  SQueryLogicPlan* pPlan = createRawQueryLogicPlan(pCxt, pLogicNode);
  if (TSDB_CODE_SUCCESS != pCxt->errCode) {
    nodesDestroyNode((SNode*)pPlan);
    return pCxt->errCode;
  }
  // todo split
  *pLogicPlan = pPlan;
  return TSDB_CODE_SUCCESS;
}

static int32_t buildPhysiPlan(SPhysiPlanContext* pCxt, SQueryLogicPlan* pLogicPlan, SQueryPlan** pPlan) {
  SQueryPlan* pQueryPlan = (SQueryPlan*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN);
  CHECK_ALLOC(pQueryPlan, TSDB_CODE_OUT_OF_MEMORY);
  *pPlan = pQueryPlan;
  pQueryPlan->queryId = pCxt->pPlanCxt->queryId;

  pQueryPlan->pSubplans = nodesMakeList();
  CHECK_ALLOC(pQueryPlan->pSubplans, TSDB_CODE_OUT_OF_MEMORY);
  SNode* pNode;
  FOREACH(pNode, pLogicPlan->pSubplans) {
    SNodeListNode* pLevelSubplans = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
    CHECK_ALLOC(pLevelSubplans, TSDB_CODE_OUT_OF_MEMORY);
    if (TSDB_CODE_SUCCESS != nodesListAppend(pQueryPlan->pSubplans, (SNode*)pLevelSubplans)) {
      nodesDestroyNode((SNode*)pLevelSubplans);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    SNode* pLogicSubplan;
    FOREACH(pLogicSubplan, ((SNodeListNode*)pNode)->pNodeList) {
      SSubplan* pSubplan = createPhysiSubplan(pCxt, (SSubLogicPlan*)pLogicSubplan);
      CHECK_ALLOC(pSubplan, TSDB_CODE_OUT_OF_MEMORY);
      if (TSDB_CODE_SUCCESS != nodesListAppend(pLevelSubplans->pNodeList, (SNode*)pSubplan)) {
        nodesDestroyNode((SNode*)pSubplan);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      ++(pQueryPlan->numOfSubplans);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t createPhysiPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SQueryPlan** pPlan) {
  SPhysiPlanContext cxt = {
    .pPlanCxt = pCxt,
    .errCode = TSDB_CODE_SUCCESS,
    .nextDataBlockId = 0,
    .pLocationHelper = taosArrayInit(32, POINTER_BYTES)
  };
  if (NULL == cxt.pLocationHelper) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SQueryLogicPlan* pLogicPlan;
  int32_t code = splitLogicPlan(&cxt, pLogicNode, &pLogicPlan);
  // todo scale out
  // todo maping
  if (TSDB_CODE_SUCCESS == code) {
    code = buildPhysiPlan(&cxt, pLogicPlan, pPlan);
  }
  nodesDestroyNode((SNode*)pLogicPlan);
  return code;
}
