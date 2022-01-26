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

  // traverse the dag again to acquire the execution node.
  if (pNodeList != NULL) {
    SArray** pSubLevel = taosArrayGetLast((*pDag)->pSubplans);
    size_t  num = taosArrayGetSize(*pSubLevel);
    for (int32_t j = 0; j < num; ++j) {
      SSubplan* pPlan = taosArrayGetP(*pSubLevel, j);
      taosArrayPush(pNodeList, &pPlan->execNode);
    }
  }

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
