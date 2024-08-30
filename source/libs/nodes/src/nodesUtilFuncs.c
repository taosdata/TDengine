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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "tdatablock.h"
#include "thash.h"
#include "tref.h"
#include "functionMgt.h"

typedef struct SNodeMemChunk {
  int32_t               availableSize;
  int32_t               usedSize;
  char*                 pBuf;
  struct SNodeMemChunk* pNext;
} SNodeMemChunk;

struct SNodeAllocator {
  int64_t        self;
  int64_t        queryId;
  int32_t        chunkSize;
  int32_t        chunkNum;
  SNodeMemChunk* pCurrChunk;
  SNodeMemChunk* pChunks;
  TdThreadMutex  mutex;
};

static threadlocal SNodeAllocator* g_pNodeAllocator;
static int32_t                     g_allocatorReqRefPool = -1;

char* getJoinTypeString(EJoinType type) {
  static char* joinType[] = {"", "INNER", "LEFT", "RIGHT", "FULL"};
  return joinType[type];
}

char* getJoinSTypeString(EJoinSubType type) {
  static char* joinSType[] = {"", "", "OUTER", "SEMI", "ANTI", "ANY", "ASOF", "WINDOW"};
  return joinSType[type];
}

char* getFullJoinTypeString(EJoinType type, EJoinSubType stype) {
  static char* joinFullType[][8] = {
    {"INNER", "INNER", "INNER", "INNER", "INNER", "INNER ANY", "INNER", "INNER"},
    {"LEFT", "LEFT", "LEFT OUTER", "LEFT SEMI", "LEFT ANTI", "LEFT ANY", "LEFT ASOF", "LEFT WINDOW"},
    {"RIGHT", "RIGHT", "RIGHT OUTER", "RIGHT SEMI", "RIGHT ANTI", "RIGHT ANY", "RIGHT ASOF", "RIGHT WINDOW"},
    {"FULL", "FULL", "FULL OUTER", "FULL", "FULL", "FULL ANY", "FULL", "FULL"}
  };
  return joinFullType[type][stype];
}


int32_t mergeJoinConds(SNode** ppDst, SNode** ppSrc) {
  if (NULL == *ppSrc) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == *ppDst) {
    *ppDst = *ppSrc;
    *ppSrc = NULL;
    return TSDB_CODE_SUCCESS;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc) && ((SLogicConditionNode*)(*ppSrc))->condType == LOGIC_COND_TYPE_AND) {
    TSWAP(*ppDst, *ppSrc);
  }
  int32_t code = 0;
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppDst)) {
    SLogicConditionNode* pDst = (SLogicConditionNode*)*ppDst;
    if (pDst->condType == LOGIC_COND_TYPE_AND) {
      if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc) && ((SLogicConditionNode*)(*ppSrc))->condType == LOGIC_COND_TYPE_AND) {
        code = nodesListStrictAppendList(pDst->pParameterList, ((SLogicConditionNode*)(*ppSrc))->pParameterList);
        ((SLogicConditionNode*)(*ppSrc))->pParameterList = NULL;
      } else {
        code = nodesListStrictAppend(pDst->pParameterList, *ppSrc);
        *ppSrc = NULL;
      }
      nodesDestroyNode(*ppSrc);
      *ppSrc = NULL;

      return code;
    }
  }

  SLogicConditionNode* pLogicCond = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  pLogicCond->pParameterList = NULL;
  code = nodesListMakeStrictAppend(&pLogicCond->pParameterList, *ppSrc);
  if (TSDB_CODE_SUCCESS == code) {
    *ppSrc = NULL;
    code = nodesListMakeStrictAppend(&pLogicCond->pParameterList, *ppDst);
  }
  if (TSDB_CODE_SUCCESS  == code) {
    *ppDst = (SNode*)pLogicCond;
  }
  return code;
}


static int32_t callocNodeChunk(SNodeAllocator* pAllocator, SNodeMemChunk** pOutChunk) {
  SNodeMemChunk* pNewChunk = taosMemoryCalloc(1, sizeof(SNodeMemChunk) + pAllocator->chunkSize);
  if (NULL == pNewChunk) {
    if (pOutChunk) *pOutChunk = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pNewChunk->pBuf = (char*)(pNewChunk + 1);
  pNewChunk->availableSize = pAllocator->chunkSize;
  pNewChunk->usedSize = 0;
  pNewChunk->pNext = NULL;
  if (NULL != pAllocator->pCurrChunk) {
    pAllocator->pCurrChunk->pNext = pNewChunk;
  }
  pAllocator->pCurrChunk = pNewChunk;
  if (NULL == pAllocator->pChunks) {
    pAllocator->pChunks = pNewChunk;
  }
  ++(pAllocator->chunkNum);
  if (pOutChunk) *pOutChunk = pNewChunk;
  return TSDB_CODE_SUCCESS;
}

static int32_t nodesCallocImpl(int32_t size, void** pOut) {
  if (NULL == g_pNodeAllocator) {
    *pOut = taosMemoryCalloc(1, size);
    if (!*pOut) return TSDB_CODE_OUT_OF_MEMORY;
    return TSDB_CODE_SUCCESS;
  }

  if (g_pNodeAllocator->pCurrChunk->usedSize + size > g_pNodeAllocator->pCurrChunk->availableSize) {
    int32_t code = callocNodeChunk(g_pNodeAllocator, NULL);
    if (TSDB_CODE_SUCCESS != code) {
      *pOut = NULL;
      return code;
    }
  }
  void* p = g_pNodeAllocator->pCurrChunk->pBuf + g_pNodeAllocator->pCurrChunk->usedSize;
  g_pNodeAllocator->pCurrChunk->usedSize += size;
  *pOut = p;
  return TSDB_CODE_SUCCESS;;
}

static int32_t nodesCalloc(int32_t num, int32_t size, void** pOut) {
  void* p = NULL;
  int32_t code = nodesCallocImpl(num * size + 1, &p);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  *(char*)p = (NULL != g_pNodeAllocator) ? 1 : 0;
  *pOut = (char*)p + 1;
  return TSDB_CODE_SUCCESS;
}

void nodesFree(void* p) {
  char* ptr = (char*)p - 1;
  if (0 == *ptr) {
    taosMemoryFree(ptr);
  }
  return;
}

static int32_t createNodeAllocator(int32_t chunkSize, SNodeAllocator** pAllocator) {
  *pAllocator = taosMemoryCalloc(1, sizeof(SNodeAllocator));
  if (NULL == *pAllocator) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  (*pAllocator)->chunkSize = chunkSize;
  int32_t code = callocNodeChunk(*pAllocator, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFreeClear(*pAllocator);
    return code;
  }
  if (0 != taosThreadMutexInit(&(*pAllocator)->mutex, NULL)) {
    return TAOS_SYSTEM_ERROR(errno);
  }
  return TSDB_CODE_SUCCESS;
}

static void destroyNodeAllocator(void* p) {
  if (NULL == p) {
    return;
  }

  SNodeAllocator* pAllocator = p;

  nodesDebug("query id %" PRIx64 " allocator id %" PRIx64 " alloc chunkNum: %d, chunkTotakSize: %d",
             pAllocator->queryId, pAllocator->self, pAllocator->chunkNum, pAllocator->chunkNum * pAllocator->chunkSize);

  SNodeMemChunk* pChunk = pAllocator->pChunks;
  while (NULL != pChunk) {
    SNodeMemChunk* pTemp = pChunk->pNext;
    taosMemoryFree(pChunk);
    pChunk = pTemp;
  }
  (void)taosThreadMutexDestroy(&pAllocator->mutex);
  taosMemoryFree(pAllocator);
}

int32_t nodesInitAllocatorSet() {
  if (g_allocatorReqRefPool >= 0) {
    nodesWarn("nodes already initialized");
    return TSDB_CODE_SUCCESS;
  }

  g_allocatorReqRefPool = taosOpenRef(1024, destroyNodeAllocator);
  if (g_allocatorReqRefPool < 0) {
    nodesError("init nodes failed");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void nodesDestroyAllocatorSet() {
  if (g_allocatorReqRefPool >= 0) {
    SNodeAllocator* pAllocator = taosIterateRef(g_allocatorReqRefPool, 0);
    int64_t         refId = 0;
    while (NULL != pAllocator) {
      refId = pAllocator->self;
      (void)taosRemoveRef(g_allocatorReqRefPool, refId);
      pAllocator = taosIterateRef(g_allocatorReqRefPool, refId);
    }
    taosCloseRef(g_allocatorReqRefPool);
  }
}

int32_t nodesCreateAllocator(int64_t queryId, int32_t chunkSize, int64_t* pAllocatorId) {
  SNodeAllocator* pAllocator = NULL;
  int32_t         code = createNodeAllocator(chunkSize, &pAllocator);
  if (TSDB_CODE_SUCCESS == code) {
    pAllocator->self = taosAddRef(g_allocatorReqRefPool, pAllocator);
    if (pAllocator->self <= 0) {
      return terrno;
    }
    pAllocator->queryId = queryId;
    *pAllocatorId = pAllocator->self;
  }
  return code;
}

int32_t nodesSimAcquireAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeAllocator* pAllocator = taosAcquireRef(g_allocatorReqRefPool, allocatorId);
  if (NULL == pAllocator) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t nodesSimReleaseAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  return taosReleaseRef(g_allocatorReqRefPool, allocatorId);
}

int32_t nodesAcquireAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SNodeAllocator* pAllocator = taosAcquireRef(g_allocatorReqRefPool, allocatorId);
  if (NULL == pAllocator) {
    return terrno;
  }
  (void)taosThreadMutexLock(&pAllocator->mutex);
  g_pNodeAllocator = pAllocator;
  return TSDB_CODE_SUCCESS;
}

int32_t nodesReleaseAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (NULL == g_pNodeAllocator) {
    nodesError("allocator id %" PRIx64
               " release failed: The nodesReleaseAllocator function needs to be called after the nodesAcquireAllocator "
               "function is called!",
               allocatorId);
    return TSDB_CODE_FAILED;
  }
  SNodeAllocator* pAllocator = g_pNodeAllocator;
  g_pNodeAllocator = NULL;
  (void)taosThreadMutexUnlock(&pAllocator->mutex);
  return taosReleaseRef(g_allocatorReqRefPool, allocatorId);
}

int64_t nodesMakeAllocatorWeakRef(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return 0;
  }

  SNodeAllocator* pAllocator = taosAcquireRef(g_allocatorReqRefPool, allocatorId);
  if (NULL == pAllocator) {
    nodesError("allocator id %" PRIx64 " weak reference failed", allocatorId);
    return -1;
  }
  return pAllocator->self;
}

int64_t nodesReleaseAllocatorWeakRef(int64_t allocatorId) { return taosReleaseRef(g_allocatorReqRefPool, allocatorId); }

void nodesDestroyAllocator(int64_t allocatorId) {
  if (allocatorId <= 0) {
    return;
  }

  (void)taosRemoveRef(g_allocatorReqRefPool, allocatorId);
}

static int32_t makeNode(ENodeType type, int32_t size, SNode** ppNode) {
  SNode*  p = NULL;
  int32_t code = nodesCalloc(1, size, (void**)&p);
  if (TSDB_CODE_SUCCESS == code) {
    setNodeType(p, type);
    *ppNode = p;
  }
  return code;
}

int32_t nodesMakeNode(ENodeType type, SNode** ppNodeOut) {
  SNode* pNode = NULL;
  int32_t code = 0;
  switch (type) {
    case QUERY_NODE_COLUMN:
      code = makeNode(type, sizeof(SColumnNode), &pNode); break;
    case QUERY_NODE_VALUE:
      code = makeNode(type, sizeof(SValueNode), &pNode); break;
    case QUERY_NODE_OPERATOR:
      code = makeNode(type, sizeof(SOperatorNode), &pNode); break;
    case QUERY_NODE_LOGIC_CONDITION:
      code = makeNode(type, sizeof(SLogicConditionNode), &pNode); break;
    case QUERY_NODE_FUNCTION:
      code = makeNode(type, sizeof(SFunctionNode), &pNode); break;
    case QUERY_NODE_REAL_TABLE:
      code = makeNode(type, sizeof(SRealTableNode), &pNode); break;
    case QUERY_NODE_TEMP_TABLE:
      code = makeNode(type, sizeof(STempTableNode), &pNode); break;
    case QUERY_NODE_JOIN_TABLE:
      code = makeNode(type, sizeof(SJoinTableNode), &pNode); break;
    case QUERY_NODE_GROUPING_SET:
      code = makeNode(type, sizeof(SGroupingSetNode), &pNode); break;
    case QUERY_NODE_ORDER_BY_EXPR:
      code = makeNode(type, sizeof(SOrderByExprNode), &pNode); break;
    case QUERY_NODE_LIMIT:
      code = makeNode(type, sizeof(SLimitNode), &pNode); break;
    case QUERY_NODE_STATE_WINDOW:
      code = makeNode(type, sizeof(SStateWindowNode), &pNode); break;
    case QUERY_NODE_SESSION_WINDOW:
      code = makeNode(type, sizeof(SSessionWindowNode), &pNode); break;
    case QUERY_NODE_INTERVAL_WINDOW:
      code = makeNode(type, sizeof(SIntervalWindowNode), &pNode); break;
    case QUERY_NODE_NODE_LIST:
      code = makeNode(type, sizeof(SNodeListNode), &pNode); break;
    case QUERY_NODE_FILL:
      code = makeNode(type, sizeof(SFillNode), &pNode); break;
    case QUERY_NODE_RAW_EXPR:
      code = makeNode(type, sizeof(SRawExprNode), &pNode); break;
    case QUERY_NODE_TARGET:
      code = makeNode(type, sizeof(STargetNode), &pNode); break;
    case QUERY_NODE_DATABLOCK_DESC:
      code = makeNode(type, sizeof(SDataBlockDescNode), &pNode); break;
    case QUERY_NODE_SLOT_DESC:
      code = makeNode(type, sizeof(SSlotDescNode), &pNode); break;
    case QUERY_NODE_COLUMN_DEF:
      code = makeNode(type, sizeof(SColumnDefNode), &pNode); break;
    case QUERY_NODE_DOWNSTREAM_SOURCE:
      code = makeNode(type, sizeof(SDownstreamSourceNode), &pNode); break;
    case QUERY_NODE_DATABASE_OPTIONS:
      code = makeNode(type, sizeof(SDatabaseOptions), &pNode); break;
    case QUERY_NODE_TABLE_OPTIONS:
      code = makeNode(type, sizeof(STableOptions), &pNode); break;
    case QUERY_NODE_COLUMN_OPTIONS:
      code = makeNode(type, sizeof(SColumnOptions), &pNode); break;
    case QUERY_NODE_INDEX_OPTIONS:
      code = makeNode(type, sizeof(SIndexOptions), &pNode); break;
    case QUERY_NODE_EXPLAIN_OPTIONS:
      code = makeNode(type, sizeof(SExplainOptions), &pNode); break;
    case QUERY_NODE_STREAM_OPTIONS:
      code = makeNode(type, sizeof(SStreamOptions), &pNode); break;
    case QUERY_NODE_LEFT_VALUE:
      code = makeNode(type, sizeof(SLeftValueNode), &pNode); break;
    case QUERY_NODE_COLUMN_REF:
      code = makeNode(type, sizeof(SColumnRefNode), &pNode); break;
    case QUERY_NODE_WHEN_THEN:
      code = makeNode(type, sizeof(SWhenThenNode), &pNode); break;
    case QUERY_NODE_CASE_WHEN:
      code = makeNode(type, sizeof(SCaseWhenNode), &pNode); break;
    case QUERY_NODE_EVENT_WINDOW:
      code = makeNode(type, sizeof(SEventWindowNode), &pNode); break;
    case QUERY_NODE_COUNT_WINDOW:
      code = makeNode(type, sizeof(SCountWindowNode), &pNode); break;
    case QUERY_NODE_HINT:
      code = makeNode(type, sizeof(SHintNode), &pNode); break;
    case QUERY_NODE_VIEW:
      code = makeNode(type, sizeof(SViewNode), &pNode); break;
    case QUERY_NODE_WINDOW_OFFSET:
      code = makeNode(type, sizeof(SWindowOffsetNode), &pNode); break;
    case QUERY_NODE_SET_OPERATOR:
      code = makeNode(type, sizeof(SSetOperator), &pNode); break;
    case QUERY_NODE_SELECT_STMT:
      code = makeNode(type, sizeof(SSelectStmt), &pNode); break;
    case QUERY_NODE_VNODE_MODIFY_STMT:
      code = makeNode(type, sizeof(SVnodeModifyOpStmt), &pNode); break;
    case QUERY_NODE_CREATE_DATABASE_STMT:
      code = makeNode(type, sizeof(SCreateDatabaseStmt), &pNode); break;
    case QUERY_NODE_DROP_DATABASE_STMT:
      code = makeNode(type, sizeof(SDropDatabaseStmt), &pNode); break;
    case QUERY_NODE_ALTER_DATABASE_STMT:
      code = makeNode(type, sizeof(SAlterDatabaseStmt), &pNode); break;
    case QUERY_NODE_FLUSH_DATABASE_STMT:
      code = makeNode(type, sizeof(SFlushDatabaseStmt), &pNode); break;
    case QUERY_NODE_TRIM_DATABASE_STMT:
      code = makeNode(type, sizeof(STrimDatabaseStmt), &pNode); break;
    case QUERY_NODE_S3MIGRATE_DATABASE_STMT:
      code = makeNode(type, sizeof(SS3MigrateDatabaseStmt), &pNode); break;
    case QUERY_NODE_CREATE_TABLE_STMT:
      code = makeNode(type, sizeof(SCreateTableStmt), &pNode); break;
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE:
      code = makeNode(type, sizeof(SCreateSubTableClause), &pNode); break;
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE:
      code = makeNode(type, sizeof(SCreateSubTableFromFileClause), &pNode); break;
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      code = makeNode(type, sizeof(SCreateMultiTablesStmt), &pNode); break;
    case QUERY_NODE_DROP_TABLE_CLAUSE:
      code = makeNode(type, sizeof(SDropTableClause), &pNode); break;
    case QUERY_NODE_DROP_TABLE_STMT:
      code = makeNode(type, sizeof(SDropTableStmt), &pNode); break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:
      code = makeNode(type, sizeof(SDropSuperTableStmt), &pNode); break;
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT:
      code = makeNode(type, sizeof(SAlterTableStmt), &pNode); break;
    case QUERY_NODE_CREATE_USER_STMT:
      code = makeNode(type, sizeof(SCreateUserStmt), &pNode); break;
    case QUERY_NODE_ALTER_USER_STMT:
      code = makeNode(type, sizeof(SAlterUserStmt), &pNode); break;
    case QUERY_NODE_DROP_USER_STMT:
      code = makeNode(type, sizeof(SDropUserStmt), &pNode); break;
    case QUERY_NODE_USE_DATABASE_STMT:
      code = makeNode(type, sizeof(SUseDatabaseStmt), &pNode); break;
    case QUERY_NODE_CREATE_DNODE_STMT:
      code = makeNode(type, sizeof(SCreateDnodeStmt), &pNode); break;
    case QUERY_NODE_DROP_DNODE_STMT:
      code = makeNode(type, sizeof(SDropDnodeStmt), &pNode); break;
    case QUERY_NODE_ALTER_DNODE_STMT:
      code = makeNode(type, sizeof(SAlterDnodeStmt), &pNode); break;
    case QUERY_NODE_CREATE_INDEX_STMT:
      code = makeNode(type, sizeof(SCreateIndexStmt), &pNode); break;
    case QUERY_NODE_DROP_INDEX_STMT:
      code = makeNode(type, sizeof(SDropIndexStmt), &pNode); break;
    case QUERY_NODE_CREATE_QNODE_STMT:
    case QUERY_NODE_CREATE_BNODE_STMT:
    case QUERY_NODE_CREATE_SNODE_STMT:
    case QUERY_NODE_CREATE_MNODE_STMT:
      code = makeNode(type, sizeof(SCreateComponentNodeStmt), &pNode); break;
    case QUERY_NODE_DROP_QNODE_STMT:
    case QUERY_NODE_DROP_BNODE_STMT:
    case QUERY_NODE_DROP_SNODE_STMT:
    case QUERY_NODE_DROP_MNODE_STMT:
      code = makeNode(type, sizeof(SDropComponentNodeStmt), &pNode); break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      code = makeNode(type, sizeof(SCreateTopicStmt), &pNode); break;
    case QUERY_NODE_DROP_TOPIC_STMT:
      code = makeNode(type, sizeof(SDropTopicStmt), &pNode); break;
    case QUERY_NODE_DROP_CGROUP_STMT:
      code = makeNode(type, sizeof(SDropCGroupStmt), &pNode); break;
    case QUERY_NODE_ALTER_LOCAL_STMT:
      code = makeNode(type, sizeof(SAlterLocalStmt), &pNode); break;
    case QUERY_NODE_EXPLAIN_STMT:
      code = makeNode(type, sizeof(SExplainStmt), &pNode); break;
    case QUERY_NODE_DESCRIBE_STMT:
      code = makeNode(type, sizeof(SDescribeStmt), &pNode); break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:
      code = makeNode(type, sizeof(SNode), &pNode); break;
    case QUERY_NODE_COMPACT_DATABASE_STMT:
      code = makeNode(type, sizeof(SCompactDatabaseStmt), &pNode); break;
    case QUERY_NODE_CREATE_FUNCTION_STMT:
      code = makeNode(type, sizeof(SCreateFunctionStmt), &pNode); break;
    case QUERY_NODE_DROP_FUNCTION_STMT:
      code = makeNode(type, sizeof(SDropFunctionStmt), &pNode); break;
    case QUERY_NODE_CREATE_STREAM_STMT:
      code = makeNode(type, sizeof(SCreateStreamStmt), &pNode); break;
    case QUERY_NODE_DROP_STREAM_STMT:
      code = makeNode(type, sizeof(SDropStreamStmt), &pNode); break;
    case QUERY_NODE_PAUSE_STREAM_STMT:
      code = makeNode(type, sizeof(SPauseStreamStmt), &pNode); break;
    case QUERY_NODE_RESUME_STREAM_STMT:
      code = makeNode(type, sizeof(SResumeStreamStmt), &pNode); break;
    case QUERY_NODE_BALANCE_VGROUP_STMT:
      code = makeNode(type, sizeof(SBalanceVgroupStmt), &pNode); break;
    case QUERY_NODE_BALANCE_VGROUP_LEADER_STMT:
      code = makeNode(type, sizeof(SBalanceVgroupLeaderStmt), &pNode); break;
    case QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT:
      code = makeNode(type, sizeof(SBalanceVgroupLeaderStmt), &pNode); break;
    case QUERY_NODE_MERGE_VGROUP_STMT:
      code = makeNode(type, sizeof(SMergeVgroupStmt), &pNode); break;
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      code = makeNode(type, sizeof(SRedistributeVgroupStmt), &pNode); break;
    case QUERY_NODE_SPLIT_VGROUP_STMT:
      code = makeNode(type, sizeof(SSplitVgroupStmt), &pNode); break;
    case QUERY_NODE_SYNCDB_STMT:
      break;
    case QUERY_NODE_GRANT_STMT:
      code = makeNode(type, sizeof(SGrantStmt), &pNode); break;
    case QUERY_NODE_REVOKE_STMT:
      code = makeNode(type, sizeof(SRevokeStmt), &pNode); break;
    case QUERY_NODE_ALTER_CLUSTER_STMT:
      code = makeNode(type, sizeof(SAlterClusterStmt), &pNode); break;
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_ARBGROUPS_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_USERS_FULL_STMT:
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_VARIABLES_STMT:
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT:
    case QUERY_NODE_SHOW_TAGS_STMT:
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
    case QUERY_NODE_SHOW_VIEWS_STMT:
    case QUERY_NODE_SHOW_GRANTS_FULL_STMT:
    case QUERY_NODE_SHOW_GRANTS_LOGS_STMT:
    case QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT:
    case QUERY_NODE_SHOW_ENCRYPTIONS_STMT:
    case QUERY_NODE_SHOW_TSMAS_STMT:
      code = makeNode(type, sizeof(SShowStmt), &pNode); break;
    case QUERY_NODE_SHOW_TABLE_TAGS_STMT:
      code = makeNode(type, sizeof(SShowTableTagsStmt), &pNode); break;
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
      code = makeNode(type, sizeof(SShowDnodeVariablesStmt), &pNode); break;
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      code = makeNode(type, sizeof(SShowCreateDatabaseStmt), &pNode); break;
    case QUERY_NODE_SHOW_DB_ALIVE_STMT:
    case QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT:
      code = makeNode(type, sizeof(SShowAliveStmt), &pNode); break;
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      code = makeNode(type, sizeof(SShowCreateTableStmt), &pNode); break;
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:
      code = makeNode(type, sizeof(SShowCreateViewStmt), &pNode); break;
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:
      code = makeNode(type, sizeof(SShowTableDistributedStmt), &pNode); break;
    case QUERY_NODE_SHOW_COMPACTS_STMT:
      code = makeNode(type, sizeof(SShowCompactsStmt), &pNode); break;
    case QUERY_NODE_SHOW_COMPACT_DETAILS_STMT:
      code = makeNode(type, sizeof(SShowCompactDetailsStmt), &pNode); break;
    case QUERY_NODE_KILL_QUERY_STMT:
      code = makeNode(type, sizeof(SKillQueryStmt), &pNode); break;
    case QUERY_NODE_KILL_TRANSACTION_STMT:
    case QUERY_NODE_KILL_CONNECTION_STMT:
    case QUERY_NODE_KILL_COMPACT_STMT:
      code = makeNode(type, sizeof(SKillStmt), &pNode); break;
    case QUERY_NODE_DELETE_STMT:
      code = makeNode(type, sizeof(SDeleteStmt), &pNode); break;
    case QUERY_NODE_INSERT_STMT:
      code = makeNode(type, sizeof(SInsertStmt), &pNode); break;
    case QUERY_NODE_QUERY:
      code = makeNode(type, sizeof(SQuery), &pNode); break;
    case QUERY_NODE_RESTORE_DNODE_STMT:
    case QUERY_NODE_RESTORE_QNODE_STMT:
    case QUERY_NODE_RESTORE_MNODE_STMT:
    case QUERY_NODE_RESTORE_VNODE_STMT:
      code = makeNode(type, sizeof(SRestoreComponentNodeStmt), &pNode); break;
    case QUERY_NODE_CREATE_VIEW_STMT:
      code = makeNode(type, sizeof(SCreateViewStmt), &pNode); break;
    case QUERY_NODE_DROP_VIEW_STMT:
      code = makeNode(type, sizeof(SDropViewStmt), &pNode); break;
    case QUERY_NODE_CREATE_TSMA_STMT:
      code = makeNode(type, sizeof(SCreateTSMAStmt), &pNode); break;
    case QUERY_NODE_DROP_TSMA_STMT:
      code = makeNode(type, sizeof(SDropTSMAStmt), &pNode); break;
    case QUERY_NODE_TSMA_OPTIONS:
      code = makeNode(type, sizeof(STSMAOptions), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_SCAN:
      code = makeNode(type, sizeof(SScanLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_JOIN:
      code = makeNode(type, sizeof(SJoinLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_AGG:
      code = makeNode(type, sizeof(SAggLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_PROJECT:
      code = makeNode(type, sizeof(SProjectLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY:
      code = makeNode(type, sizeof(SVnodeModifyLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      code = makeNode(type, sizeof(SExchangeLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_MERGE:
      code = makeNode(type, sizeof(SMergeLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_WINDOW:
      code = makeNode(type, sizeof(SWindowLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_FILL:
      code = makeNode(type, sizeof(SFillLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_SORT:
      code = makeNode(type, sizeof(SSortLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_PARTITION:
      code = makeNode(type, sizeof(SPartitionLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC:
      code = makeNode(type, sizeof(SIndefRowsFuncLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC:
      code = makeNode(type, sizeof(SInterpFuncLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE:
      code = makeNode(type, sizeof(SGroupCacheLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL:
      code = makeNode(type, sizeof(SDynQueryCtrlLogicNode), &pNode); break;
    case QUERY_NODE_LOGIC_SUBPLAN:
      code = makeNode(type, sizeof(SLogicSubplan), &pNode); break;
    case QUERY_NODE_LOGIC_PLAN:
      code = makeNode(type, sizeof(SQueryLogicPlan), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      code = makeNode(type, sizeof(STagScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
      code = makeNode(type, sizeof(STableScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
      code = makeNode(type, sizeof(STableSeqScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
      code = makeNode(type, sizeof(STableMergeScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
      code = makeNode(type, sizeof(SStreamScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
      code = makeNode(type, sizeof(SSystemTableScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      code = makeNode(type, sizeof(SBlockDistScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
      code = makeNode(type, sizeof(SLastRowScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
      code = makeNode(type, sizeof(STableCountScanPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      code = makeNode(type, sizeof(SProjectPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      code = makeNode(type, sizeof(SSortMergeJoinPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:
      code = makeNode(type, sizeof(SHashJoinPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
      code = makeNode(type, sizeof(SAggPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
      code = makeNode(type, sizeof(SExchangePhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
      code = makeNode(type, sizeof(SMergePhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
      code = makeNode(type, sizeof(SSortPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT:
      code = makeNode(type, sizeof(SGroupSortPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
      code = makeNode(type, sizeof(SIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
      code = makeNode(type, sizeof(SMergeAlignedIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
      code = makeNode(type, sizeof(SStreamIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
      code = makeNode(type, sizeof(SStreamFinalIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
      code = makeNode(type, sizeof(SStreamSemiIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      code = makeNode(type, sizeof(SStreamMidIntervalPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
      code = makeNode(type, sizeof(SFillPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
      code = makeNode(type, sizeof(SSessionWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
      code = makeNode(type, sizeof(SStreamSessionWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
      code = makeNode(type, sizeof(SStreamSemiSessionWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      code = makeNode(type, sizeof(SStreamFinalSessionWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
      code = makeNode(type, sizeof(SStateWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
      code = makeNode(type, sizeof(SStreamStateWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
      code = makeNode(type, sizeof(SEventWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
      code = makeNode(type, sizeof(SStreamEventWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT:
      code = makeNode(type, sizeof(SCountWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT:
      code = makeNode(type, sizeof(SStreamCountWinodwPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
      code = makeNode(type, sizeof(SPartitionPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
      code = makeNode(type, sizeof(SStreamPartitionPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
      code = makeNode(type, sizeof(SIndefRowsFuncPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
      code = makeNode(type, sizeof(SInterpFuncLogicNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      code = makeNode(type, sizeof(SDataDispatcherNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
      code = makeNode(type, sizeof(SDataInserterNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT:
      code = makeNode(type, sizeof(SQueryInserterNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
      code = makeNode(type, sizeof(SDataDeleterNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
      code = makeNode(type, sizeof(SGroupCachePhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:
      code = makeNode(type, sizeof(SDynQueryCtrlPhysiNode), &pNode); break;
    case QUERY_NODE_PHYSICAL_SUBPLAN:
      code = makeNode(type, sizeof(SSubplan), &pNode); break;
    case QUERY_NODE_PHYSICAL_PLAN:
      code = makeNode(type, sizeof(SQueryPlan), &pNode); break;
    default:
      break;
  }
  if (TSDB_CODE_SUCCESS != code)
    nodesError("nodesMakeNode unknown node = %s", nodesNodeName(type));
  else
    *ppNodeOut = pNode;
  return code;
}

static void destroyVgDataBlockArray(SArray* pArray) {
  size_t size = taosArrayGetSize(pArray);
  for (size_t i = 0; i < size; ++i) {
    SVgDataBlocks* pVg = taosArrayGetP(pArray, i);
    taosMemoryFreeClear(pVg->pData);
    taosMemoryFreeClear(pVg);
  }
  taosArrayDestroy(pArray);
}

static void destroyLogicNode(SLogicNode* pNode) {
  nodesDestroyList(pNode->pTargets);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pLimit);
  nodesDestroyNode(pNode->pSlimit);
  nodesDestroyList(pNode->pHint);
}

static void destroyPhysiNode(SPhysiNode* pNode) {
  nodesDestroyList(pNode->pChildren);
  nodesDestroyNode(pNode->pConditions);
  nodesDestroyNode((SNode*)pNode->pOutputDataBlockDesc);
  nodesDestroyNode(pNode->pLimit);
  nodesDestroyNode(pNode->pSlimit);
}

static void destroyWinodwPhysiNode(SWindowPhysiNode* pNode) {
  destroyPhysiNode((SPhysiNode*)pNode);
  nodesDestroyList(pNode->pExprs);
  nodesDestroyList(pNode->pFuncs);
  nodesDestroyNode(pNode->pTspk);
  nodesDestroyNode(pNode->pTsEnd);
}

static void destroyPartitionPhysiNode(SPartitionPhysiNode* pNode) {
  destroyPhysiNode((SPhysiNode*)pNode);
  nodesDestroyList(pNode->pExprs);
  nodesDestroyList(pNode->pPartitionKeys);
  nodesDestroyList(pNode->pTargets);
}

static void destroyScanPhysiNode(SScanPhysiNode* pNode) {
  destroyPhysiNode((SPhysiNode*)pNode);
  nodesDestroyList(pNode->pScanCols);
  nodesDestroyList(pNode->pScanPseudoCols);
}

static void destroyDataSinkNode(SDataSinkNode* pNode) { nodesDestroyNode((SNode*)pNode->pInputDataBlockDesc); }

static void destroyExprNode(SExprNode* pExpr) { taosArrayDestroy(pExpr->pAssociation); }

static void destroyTableCfg(STableCfg* pCfg) {
  if (NULL == pCfg) {
    return;
  }
  taosArrayDestroy(pCfg->pFuncs);
  taosMemoryFree(pCfg->pComment);
  taosMemoryFree(pCfg->pSchemas);
  taosMemoryFree(pCfg->pSchemaExt);
  taosMemoryFree(pCfg->pTags);
  taosMemoryFree(pCfg);
}

static void destroySmaIndex(void* pIndex) { taosMemoryFree(((STableIndexInfo*)pIndex)->expr); }

static void destroyFuncParam(void* pValue) { taosMemoryFree(((SFunctParam*)pValue)->pCol); }

static void destroyHintValue(EHintOption option, void* value) {
  switch (option) {
    default:
      break;
  }

  taosMemoryFree(value);
}

void nodesDestroyNode(SNode* pNode) {
  if (NULL == pNode) {
    return;
  }

  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
      destroyExprNode((SExprNode*)pNode);
      break;
    case QUERY_NODE_VALUE: {
      SValueNode* pValue = (SValueNode*)pNode;
      destroyExprNode((SExprNode*)pNode);
      taosMemoryFreeClear(pValue->literal);
      if (IS_VAR_DATA_TYPE(pValue->node.resType.type)) {
        taosMemoryFreeClear(pValue->datum.p);
      }
      break;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOp = (SOperatorNode*)pNode;
      destroyExprNode((SExprNode*)pNode);
      nodesDestroyNode(pOp->pLeft);
      nodesDestroyNode(pOp->pRight);
      break;
    }
    case QUERY_NODE_LOGIC_CONDITION:
      destroyExprNode((SExprNode*)pNode);
      nodesDestroyList(((SLogicConditionNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_FUNCTION:
      destroyExprNode((SExprNode*)pNode);
      nodesDestroyList(((SFunctionNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_REAL_TABLE: {
      SRealTableNode* pReal = (SRealTableNode*)pNode;
      taosMemoryFreeClear(pReal->pMeta);
      taosMemoryFreeClear(pReal->pVgroupList);
      taosArrayDestroyEx(pReal->pSmaIndexes, destroySmaIndex);
      taosArrayDestroyP(pReal->tsmaTargetTbVgInfo, taosMemoryFree);
      taosArrayDestroy(pReal->tsmaTargetTbInfo);
      break;
    }
    case QUERY_NODE_TEMP_TABLE:
      nodesDestroyNode(((STempTableNode*)pNode)->pSubquery);
      break;
    case QUERY_NODE_JOIN_TABLE: {
      SJoinTableNode* pJoin = (SJoinTableNode*)pNode;
      nodesDestroyNode(pJoin->pWindowOffset);
      nodesDestroyNode(pJoin->pJLimit);
      nodesDestroyNode(pJoin->addPrimCond);
      nodesDestroyNode(pJoin->pLeft);
      nodesDestroyNode(pJoin->pRight);
      nodesDestroyNode(pJoin->pOnCond);
      break;
    }
    case QUERY_NODE_GROUPING_SET:
      nodesDestroyList(((SGroupingSetNode*)pNode)->pParameterList);
      break;
    case QUERY_NODE_ORDER_BY_EXPR:
      nodesDestroyNode(((SOrderByExprNode*)pNode)->pExpr);
      break;
    case QUERY_NODE_LIMIT:  // no pointer field
      break;
    case QUERY_NODE_STATE_WINDOW: {
      SStateWindowNode* pState = (SStateWindowNode*)pNode;
      nodesDestroyNode(pState->pCol);
      nodesDestroyNode(pState->pExpr);
      break;
    }
    case QUERY_NODE_SESSION_WINDOW: {
      SSessionWindowNode* pSession = (SSessionWindowNode*)pNode;
      nodesDestroyNode((SNode*)pSession->pCol);
      nodesDestroyNode((SNode*)pSession->pGap);
      break;
    }
    case QUERY_NODE_INTERVAL_WINDOW: {
      SIntervalWindowNode* pJoin = (SIntervalWindowNode*)pNode;
      nodesDestroyNode(pJoin->pCol);
      nodesDestroyNode(pJoin->pInterval);
      nodesDestroyNode(pJoin->pOffset);
      nodesDestroyNode(pJoin->pSliding);
      nodesDestroyNode(pJoin->pFill);
      break;
    }
    case QUERY_NODE_NODE_LIST:
      nodesDestroyList(((SNodeListNode*)pNode)->pNodeList);
      break;
    case QUERY_NODE_FILL: {
      SFillNode* pFill = (SFillNode*)pNode;
      nodesDestroyNode(pFill->pValues);
      nodesDestroyNode(pFill->pWStartTs);
      break;
    }
    case QUERY_NODE_RAW_EXPR:
      nodesDestroyNode(((SRawExprNode*)pNode)->pNode);
      break;
    case QUERY_NODE_TARGET:
      nodesDestroyNode(((STargetNode*)pNode)->pExpr);
      break;
    case QUERY_NODE_DATABLOCK_DESC:
      nodesDestroyList(((SDataBlockDescNode*)pNode)->pSlots);
      break;
    case QUERY_NODE_SLOT_DESC:          // no pointer field
      break;
    case QUERY_NODE_COLUMN_DEF:
      nodesDestroyNode(((SColumnDefNode*)pNode)->pOptions);
      break;
    case QUERY_NODE_DOWNSTREAM_SOURCE:  // no pointer field
      break;
    case QUERY_NODE_DATABASE_OPTIONS: {
      SDatabaseOptions* pOptions = (SDatabaseOptions*)pNode;
      nodesDestroyNode((SNode*)pOptions->pDaysPerFile);
      nodesDestroyNode((SNode*)pOptions->s3KeepLocalStr);
      nodesDestroyList(pOptions->pKeep);
      nodesDestroyList(pOptions->pRetentions);
      break;
    }
    case QUERY_NODE_TABLE_OPTIONS: {
      STableOptions* pOptions = (STableOptions*)pNode;
      nodesDestroyList(pOptions->pMaxDelay);
      nodesDestroyList(pOptions->pWatermark);
      nodesDestroyList(pOptions->pRollupFuncs);
      nodesDestroyList(pOptions->pSma);
      nodesDestroyList(pOptions->pDeleteMark);
      break;
    }
    case QUERY_NODE_COLUMN_OPTIONS: {
      SColumnOptions* pOptions = (SColumnOptions*)pNode;
      break;
    }
    case QUERY_NODE_INDEX_OPTIONS: {
      SIndexOptions* pOptions = (SIndexOptions*)pNode;
      nodesDestroyList(pOptions->pFuncs);
      nodesDestroyNode(pOptions->pInterval);
      nodesDestroyNode(pOptions->pOffset);
      nodesDestroyNode(pOptions->pSliding);
      nodesDestroyNode(pOptions->pStreamOptions);
      break;
    }
    case QUERY_NODE_EXPLAIN_OPTIONS:  // no pointer field
      break;
    case QUERY_NODE_STREAM_OPTIONS: {
      SStreamOptions* pOptions = (SStreamOptions*)pNode;
      nodesDestroyNode(pOptions->pDelay);
      nodesDestroyNode(pOptions->pWatermark);
      nodesDestroyNode(pOptions->pDeleteMark);
      break;
    }
    case QUERY_NODE_TSMA_OPTIONS: {
      STSMAOptions* pOptions = (STSMAOptions*)pNode;
      nodesDestroyList(pOptions->pFuncs);
      nodesDestroyNode(pOptions->pInterval);
      break;
    }
    case QUERY_NODE_LEFT_VALUE:  // no pointer field
    case QUERY_NODE_COLUMN_REF:  // no pointer field
      break;
    case QUERY_NODE_WHEN_THEN: {
      SWhenThenNode* pWhenThen = (SWhenThenNode*)pNode;
      destroyExprNode((SExprNode*)pNode);
      nodesDestroyNode(pWhenThen->pWhen);
      nodesDestroyNode(pWhenThen->pThen);
      break;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)pNode;
      destroyExprNode((SExprNode*)pNode);
      nodesDestroyNode(pCaseWhen->pCase);
      nodesDestroyNode(pCaseWhen->pElse);
      nodesDestroyList(pCaseWhen->pWhenThenList);
      break;
    }
    case QUERY_NODE_EVENT_WINDOW: {
      SEventWindowNode* pEvent = (SEventWindowNode*)pNode;
      nodesDestroyNode(pEvent->pCol);
      nodesDestroyNode(pEvent->pStartCond);
      nodesDestroyNode(pEvent->pEndCond);
      break;
    }
    case QUERY_NODE_COUNT_WINDOW: {
      SCountWindowNode* pEvent = (SCountWindowNode*)pNode;
      nodesDestroyNode(pEvent->pCol);
      break;
    }
    case QUERY_NODE_HINT: {
      SHintNode* pHint = (SHintNode*)pNode;
      destroyHintValue(pHint->option, pHint->value);
      break;
    }
    case QUERY_NODE_VIEW: {
      SViewNode* pView = (SViewNode*)pNode;
      taosMemoryFreeClear(pView->pMeta);
      taosMemoryFreeClear(pView->pVgroupList);
      taosArrayDestroyEx(pView->pSmaIndexes, destroySmaIndex);
      break;
    }
    case QUERY_NODE_WINDOW_OFFSET: {
      SWindowOffsetNode* pWin = (SWindowOffsetNode*)pNode;
      nodesDestroyNode(pWin->pStartOffset);
      nodesDestroyNode(pWin->pEndOffset);
      break;
    }
    case QUERY_NODE_SET_OPERATOR: {
      SSetOperator* pStmt = (SSetOperator*)pNode;
      nodesDestroyList(pStmt->pProjectionList);
      nodesDestroyNode(pStmt->pLeft);
      nodesDestroyNode(pStmt->pRight);
      nodesDestroyList(pStmt->pOrderByList);
      nodesDestroyNode(pStmt->pLimit);
      break;
    }
    case QUERY_NODE_SELECT_STMT: {
      SSelectStmt* pStmt = (SSelectStmt*)pNode;
      nodesDestroyList(pStmt->pProjectionList);
      nodesDestroyNode(pStmt->pFromTable);
      nodesDestroyNode(pStmt->pWhere);
      nodesDestroyList(pStmt->pPartitionByList);
      nodesDestroyList(pStmt->pTags);
      nodesDestroyNode(pStmt->pSubtable);
      nodesDestroyNode(pStmt->pWindow);
      nodesDestroyList(pStmt->pGroupByList);
      nodesDestroyNode(pStmt->pHaving);
      nodesDestroyNode(pStmt->pRange);
      nodesDestroyNode(pStmt->pEvery);
      nodesDestroyNode(pStmt->pFill);
      nodesDestroyList(pStmt->pOrderByList);
      nodesDestroyNode((SNode*)pStmt->pLimit);
      nodesDestroyNode((SNode*)pStmt->pSlimit);
      nodesDestroyList(pStmt->pHint);
      break;
    }
    case QUERY_NODE_VNODE_MODIFY_STMT: {
      SVnodeModifyOpStmt* pStmt = (SVnodeModifyOpStmt*)pNode;
      destroyVgDataBlockArray(pStmt->pDataBlocks);
      taosMemoryFreeClear(pStmt->pTableMeta);
      nodesDestroyNode(pStmt->pTagCond);
      taosArrayDestroy(pStmt->pTableTag);
      taosHashCleanup(pStmt->pVgroupsHashObj);
      taosHashCleanup(pStmt->pSubTableHashObj);
      taosHashCleanup(pStmt->pTableNameHashObj);
      taosHashCleanup(pStmt->pDbFNameHashObj);
      taosHashCleanup(pStmt->pTableCxtHashObj);
      if (pStmt->freeHashFunc) {
        pStmt->freeHashFunc(pStmt->pTableBlockHashObj);
      }
      if (pStmt->freeArrayFunc) {
        pStmt->freeArrayFunc(pStmt->pVgDataBlocks);
      }
      tdDestroySVCreateTbReq(pStmt->pCreateTblReq);
      taosMemoryFreeClear(pStmt->pCreateTblReq);
      if (pStmt->freeStbRowsCxtFunc) {
        pStmt->freeStbRowsCxtFunc(pStmt->pStbRowsCxt);
      }
      taosMemoryFreeClear(pStmt->pStbRowsCxt);

      taosMemoryFreeClear(pStmt->pCreateTbInfo);

      if (pStmt->destroyParseFileCxt) {
        pStmt->destroyParseFileCxt(&pStmt->pParFileCxt);
      }

      (void)taosCloseFile(&pStmt->fp);
      break;
    }
    case QUERY_NODE_CREATE_DATABASE_STMT:
      nodesDestroyNode((SNode*)((SCreateDatabaseStmt*)pNode)->pOptions);
      break;
    case QUERY_NODE_DROP_DATABASE_STMT:  // no pointer field
      break;
    case QUERY_NODE_ALTER_DATABASE_STMT:
      nodesDestroyNode((SNode*)((SAlterDatabaseStmt*)pNode)->pOptions);
      break;
    case QUERY_NODE_FLUSH_DATABASE_STMT:  // no pointer field
    case QUERY_NODE_TRIM_DATABASE_STMT:   // no pointer field
      break;
    case QUERY_NODE_S3MIGRATE_DATABASE_STMT:   // no pointer field
      break;
    case QUERY_NODE_CREATE_TABLE_STMT: {
      SCreateTableStmt* pStmt = (SCreateTableStmt*)pNode;
      nodesDestroyList(pStmt->pCols);
      nodesDestroyList(pStmt->pTags);
      nodesDestroyNode((SNode*)pStmt->pOptions);
      break;
    }
    case QUERY_NODE_CREATE_SUBTABLE_CLAUSE: {
      SCreateSubTableClause* pStmt = (SCreateSubTableClause*)pNode;
      nodesDestroyList(pStmt->pSpecificTags);
      nodesDestroyList(pStmt->pValsOfTags);
      nodesDestroyNode((SNode*)pStmt->pOptions);
      break;
    }
    case QUERY_NODE_CREATE_SUBTABLE_FROM_FILE_CLAUSE: {
      SCreateSubTableFromFileClause* pStmt = (SCreateSubTableFromFileClause*)pNode;
      nodesDestroyList(pStmt->pSpecificTags);
      break;
    }
    case QUERY_NODE_CREATE_MULTI_TABLES_STMT:
      nodesDestroyList(((SCreateMultiTablesStmt*)pNode)->pSubTables);
      break;
    case QUERY_NODE_DROP_TABLE_CLAUSE:  // no pointer field
      break;
    case QUERY_NODE_DROP_TABLE_STMT:
      nodesDestroyList(((SDropTableStmt*)pNode)->pTables);
      break;
    case QUERY_NODE_DROP_SUPER_TABLE_STMT:  // no pointer field
      break;
    case QUERY_NODE_ALTER_TABLE_STMT:
    case QUERY_NODE_ALTER_SUPER_TABLE_STMT: {
      SAlterTableStmt* pStmt = (SAlterTableStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode((SNode*)pStmt->pVal);
      break;
    }
    case QUERY_NODE_CREATE_USER_STMT: {
      SCreateUserStmt* pStmt = (SCreateUserStmt*)pNode;
      taosMemoryFree(pStmt->pIpRanges);
      nodesDestroyList(pStmt->pNodeListIpRanges);
      break;
    }
    case QUERY_NODE_ALTER_USER_STMT: {
      SAlterUserStmt* pStmt = (SAlterUserStmt*)pNode;
      taosMemoryFree(pStmt->pIpRanges);
      nodesDestroyList(pStmt->pNodeListIpRanges);
    }
    case QUERY_NODE_DROP_USER_STMT:     // no pointer field
    case QUERY_NODE_USE_DATABASE_STMT:  // no pointer field
    case QUERY_NODE_CREATE_DNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_DNODE_STMT:    // no pointer field
    case QUERY_NODE_ALTER_DNODE_STMT:   // no pointer field
      break;
    case QUERY_NODE_CREATE_INDEX_STMT: {
      SCreateIndexStmt* pStmt = (SCreateIndexStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyList(pStmt->pCols);
      if (pStmt->pReq) {
        tFreeSMCreateSmaReq(pStmt->pReq);
        taosMemoryFreeClear(pStmt->pReq);
      }
      break;
    }
    case QUERY_NODE_DROP_INDEX_STMT:    // no pointer field
    case QUERY_NODE_CREATE_QNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_QNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_BNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_BNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_SNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_SNODE_STMT:    // no pointer field
    case QUERY_NODE_CREATE_MNODE_STMT:  // no pointer field
    case QUERY_NODE_DROP_MNODE_STMT:    // no pointer field
      break;
    case QUERY_NODE_CREATE_TOPIC_STMT:
      nodesDestroyNode(((SCreateTopicStmt*)pNode)->pQuery);
      nodesDestroyNode(((SCreateTopicStmt*)pNode)->pWhere);
      break;
    case QUERY_NODE_DROP_TOPIC_STMT:   // no pointer field
    case QUERY_NODE_DROP_CGROUP_STMT:  // no pointer field
    case QUERY_NODE_ALTER_LOCAL_STMT:  // no pointer field
      break;
    case QUERY_NODE_EXPLAIN_STMT: {
      SExplainStmt* pStmt = (SExplainStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode(pStmt->pQuery);
      break;
    }
    case QUERY_NODE_DESCRIBE_STMT:
      taosMemoryFree(((SDescribeStmt*)pNode)->pMeta);
      break;
    case QUERY_NODE_RESET_QUERY_CACHE_STMT:  // no pointer field
      break;
    case QUERY_NODE_COMPACT_DATABASE_STMT: {
      SCompactDatabaseStmt* pStmt = (SCompactDatabaseStmt*)pNode;
      nodesDestroyNode(pStmt->pStart);
      nodesDestroyNode(pStmt->pEnd);
      break;
    }
    case QUERY_NODE_CREATE_FUNCTION_STMT:  // no pointer field
    case QUERY_NODE_DROP_FUNCTION_STMT:    // no pointer field
      break;
    case QUERY_NODE_CREATE_STREAM_STMT: {
      SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      nodesDestroyNode(pStmt->pQuery);
      nodesDestroyList(pStmt->pTags);
      nodesDestroyNode(pStmt->pSubtable);
      tFreeSCMCreateStreamReq(pStmt->pReq);
      taosMemoryFreeClear(pStmt->pReq);
      break;
    }
    case QUERY_NODE_DROP_STREAM_STMT:            // no pointer field
    case QUERY_NODE_PAUSE_STREAM_STMT:           // no pointer field
    case QUERY_NODE_RESUME_STREAM_STMT:          // no pointer field
    case QUERY_NODE_BALANCE_VGROUP_STMT:         // no pointer field
    case QUERY_NODE_BALANCE_VGROUP_LEADER_STMT:  // no pointer field
    case QUERY_NODE_BALANCE_VGROUP_LEADER_DATABASE_STMT:  // no pointer field
    case QUERY_NODE_MERGE_VGROUP_STMT:           // no pointer field
      break;
    case QUERY_NODE_REDISTRIBUTE_VGROUP_STMT:
      nodesDestroyList(((SRedistributeVgroupStmt*)pNode)->pDnodes);
      break;
    case QUERY_NODE_SPLIT_VGROUP_STMT:  // no pointer field
    case QUERY_NODE_SYNCDB_STMT:        // no pointer field
      break;
    case QUERY_NODE_GRANT_STMT:
      nodesDestroyNode(((SGrantStmt*)pNode)->pTagCond);
      break;
    case QUERY_NODE_REVOKE_STMT:
      nodesDestroyNode(((SRevokeStmt*)pNode)->pTagCond);
      break;
    case QUERY_NODE_ALTER_CLUSTER_STMT:           // no pointer field
      break;
    case QUERY_NODE_SHOW_DNODES_STMT:
    case QUERY_NODE_SHOW_MNODES_STMT:
    case QUERY_NODE_SHOW_MODULES_STMT:
    case QUERY_NODE_SHOW_QNODES_STMT:
    case QUERY_NODE_SHOW_SNODES_STMT:
    case QUERY_NODE_SHOW_BNODES_STMT:
    case QUERY_NODE_SHOW_ARBGROUPS_STMT:
    case QUERY_NODE_SHOW_CLUSTER_STMT:
    case QUERY_NODE_SHOW_DATABASES_STMT:
    case QUERY_NODE_SHOW_FUNCTIONS_STMT:
    case QUERY_NODE_SHOW_INDEXES_STMT:
    case QUERY_NODE_SHOW_STABLES_STMT:
    case QUERY_NODE_SHOW_STREAMS_STMT:
    case QUERY_NODE_SHOW_TABLES_STMT:
    case QUERY_NODE_SHOW_USERS_STMT:
    case QUERY_NODE_SHOW_USERS_FULL_STMT:
    case QUERY_NODE_SHOW_LICENCES_STMT:
    case QUERY_NODE_SHOW_VGROUPS_STMT:
    case QUERY_NODE_SHOW_TOPICS_STMT:
    case QUERY_NODE_SHOW_CONSUMERS_STMT:
    case QUERY_NODE_SHOW_CONNECTIONS_STMT:
    case QUERY_NODE_SHOW_QUERIES_STMT:
    case QUERY_NODE_SHOW_VNODES_STMT:
    case QUERY_NODE_SHOW_APPS_STMT:
    case QUERY_NODE_SHOW_SCORES_STMT:
    case QUERY_NODE_SHOW_VARIABLES_STMT:
    case QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT:
    case QUERY_NODE_SHOW_TRANSACTIONS_STMT:
    case QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT:
    case QUERY_NODE_SHOW_TAGS_STMT:
    case QUERY_NODE_SHOW_USER_PRIVILEGES_STMT:
    case QUERY_NODE_SHOW_VIEWS_STMT:
    case QUERY_NODE_SHOW_GRANTS_FULL_STMT:
    case QUERY_NODE_SHOW_GRANTS_LOGS_STMT:
    case QUERY_NODE_SHOW_CLUSTER_MACHINES_STMT:
    case QUERY_NODE_SHOW_ENCRYPTIONS_STMT:
    case QUERY_NODE_SHOW_TSMAS_STMT: {
      SShowStmt* pStmt = (SShowStmt*)pNode;
      nodesDestroyNode(pStmt->pDbName);
      nodesDestroyNode(pStmt->pTbName);
      break;
    }
    case QUERY_NODE_SHOW_TABLE_TAGS_STMT: {
      SShowTableTagsStmt* pStmt = (SShowTableTagsStmt*)pNode;
      nodesDestroyNode(pStmt->pDbName);
      nodesDestroyNode(pStmt->pTbName);
      nodesDestroyList(pStmt->pTags);
      break;
    }
    case QUERY_NODE_SHOW_DNODE_VARIABLES_STMT:
      nodesDestroyNode(((SShowDnodeVariablesStmt*)pNode)->pDnodeId);
      nodesDestroyNode(((SShowDnodeVariablesStmt*)pNode)->pLikePattern);
      break;
    case QUERY_NODE_SHOW_COMPACTS_STMT:
      break;
    case QUERY_NODE_SHOW_COMPACT_DETAILS_STMT: {
      SShowCompactDetailsStmt* pStmt = (SShowCompactDetailsStmt*)pNode;
      nodesDestroyNode(pStmt->pCompactId);
      break;
    }
    case QUERY_NODE_SHOW_CREATE_DATABASE_STMT:
      taosMemoryFreeClear(((SShowCreateDatabaseStmt*)pNode)->pCfg);
      break;
    case QUERY_NODE_SHOW_CREATE_TABLE_STMT:
    case QUERY_NODE_SHOW_CREATE_STABLE_STMT:
      taosMemoryFreeClear(((SShowCreateTableStmt*)pNode)->pDbCfg);
      destroyTableCfg((STableCfg*)(((SShowCreateTableStmt*)pNode)->pTableCfg));
      break;
    case QUERY_NODE_SHOW_CREATE_VIEW_STMT:        // no pointer field
    case QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT:  // no pointer field
    case QUERY_NODE_KILL_CONNECTION_STMT:         // no pointer field
    case QUERY_NODE_KILL_QUERY_STMT:              // no pointer field
    case QUERY_NODE_KILL_TRANSACTION_STMT:        // no pointer field
    case QUERY_NODE_KILL_COMPACT_STMT:            // no pointer field
      break;
    case QUERY_NODE_DELETE_STMT: {
      SDeleteStmt* pStmt = (SDeleteStmt*)pNode;
      nodesDestroyNode(pStmt->pFromTable);
      nodesDestroyNode(pStmt->pWhere);
      nodesDestroyNode(pStmt->pCountFunc);
      nodesDestroyNode(pStmt->pFirstFunc);
      nodesDestroyNode(pStmt->pLastFunc);
      nodesDestroyNode(pStmt->pTagCond);
      break;
    }
    case QUERY_NODE_INSERT_STMT: {
      SInsertStmt* pStmt = (SInsertStmt*)pNode;
      nodesDestroyNode(pStmt->pTable);
      nodesDestroyList(pStmt->pCols);
      nodesDestroyNode(pStmt->pQuery);
      break;
    }
    case QUERY_NODE_QUERY: {
      SQuery* pQuery = (SQuery*)pNode;
      nodesDestroyNode(pQuery->pPrevRoot);
      nodesDestroyNode(pQuery->pRoot);
      nodesDestroyNode(pQuery->pPostRoot);
      taosMemoryFreeClear(pQuery->pResSchema);
      if (NULL != pQuery->pCmdMsg) {
        taosMemoryFreeClear(pQuery->pCmdMsg->pMsg);
        taosMemoryFreeClear(pQuery->pCmdMsg);
      }
      taosArrayDestroy(pQuery->pDbList);
      taosArrayDestroy(pQuery->pTableList);
      taosArrayDestroy(pQuery->pTargetTableList);
      taosArrayDestroy(pQuery->pPlaceholderValues);
      nodesDestroyNode(pQuery->pPrepareRoot);
      break;
    }
    case QUERY_NODE_RESTORE_DNODE_STMT:   // no pointer field
    case QUERY_NODE_RESTORE_QNODE_STMT:   // no pointer field
    case QUERY_NODE_RESTORE_MNODE_STMT:   // no pointer field
    case QUERY_NODE_RESTORE_VNODE_STMT:   // no pointer field
      break;
    case QUERY_NODE_CREATE_VIEW_STMT:  {
      SCreateViewStmt* pStmt = (SCreateViewStmt*)pNode;
      taosMemoryFree(pStmt->pQuerySql);
      tFreeSCMCreateViewReq(&pStmt->createReq);
      nodesDestroyNode(pStmt->pQuery);
      break;
    }
    case QUERY_NODE_DROP_VIEW_STMT:
      break;
    case QUERY_NODE_CREATE_TSMA_STMT: {
      SCreateTSMAStmt* pStmt = (SCreateTSMAStmt*)pNode;
      nodesDestroyNode((SNode*)pStmt->pOptions);
      if (pStmt->pReq) {
        tFreeSMCreateSmaReq(pStmt->pReq);
        taosMemoryFreeClear(pStmt->pReq);
      }
      break;
                                      }
    case QUERY_NODE_LOGIC_PLAN_SCAN: {
      SScanLogicNode* pLogicNode = (SScanLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pScanCols);
      nodesDestroyList(pLogicNode->pScanPseudoCols);
      taosMemoryFreeClear(pLogicNode->pVgroupList);
      nodesDestroyList(pLogicNode->pDynamicScanFuncs);
      nodesDestroyNode(pLogicNode->pTagCond);
      nodesDestroyNode(pLogicNode->pTagIndexCond);
      taosArrayDestroyEx(pLogicNode->pSmaIndexes, destroySmaIndex);
      nodesDestroyList(pLogicNode->pGroupTags);
      nodesDestroyList(pLogicNode->pTags);
      nodesDestroyNode(pLogicNode->pSubtable);
      taosArrayDestroyEx(pLogicNode->pFuncTypes, destroyFuncParam);
      taosArrayDestroyP(pLogicNode->pTsmaTargetTbVgInfo, taosMemoryFree);
      taosArrayDestroy(pLogicNode->pTsmaTargetTbInfo);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_JOIN: {
      SJoinLogicNode* pLogicNode = (SJoinLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyNode(pLogicNode->pWindowOffset);
      nodesDestroyNode(pLogicNode->pJLimit);
      nodesDestroyNode(pLogicNode->addPrimEqCond);
      nodesDestroyNode(pLogicNode->pPrimKeyEqCond);
      nodesDestroyNode(pLogicNode->pColEqCond);
      nodesDestroyNode(pLogicNode->pColOnCond);
      nodesDestroyNode(pLogicNode->pTagEqCond);
      nodesDestroyNode(pLogicNode->pTagOnCond);
      nodesDestroyNode(pLogicNode->pFullOnCond);
      nodesDestroyList(pLogicNode->pLeftEqNodes);
      nodesDestroyList(pLogicNode->pRightEqNodes);
      nodesDestroyNode(pLogicNode->pLeftOnCond);
      nodesDestroyNode(pLogicNode->pRightOnCond);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_AGG: {
      SAggLogicNode* pLogicNode = (SAggLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pAggFuncs);
      nodesDestroyList(pLogicNode->pGroupKeys);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PROJECT: {
      SProjectLogicNode* pLogicNode = (SProjectLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pProjections);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY: {
      SVnodeModifyLogicNode* pLogicNode = (SVnodeModifyLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      destroyVgDataBlockArray(pLogicNode->pDataBlocks);
      // pVgDataBlocks is weak reference
      nodesDestroyNode(pLogicNode->pAffectedRows);
      nodesDestroyNode(pLogicNode->pStartTs);
      nodesDestroyNode(pLogicNode->pEndTs);
      taosMemoryFreeClear(pLogicNode->pVgroupList);
      nodesDestroyList(pLogicNode->pInsertCols);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_EXCHANGE:
      destroyLogicNode((SLogicNode*)pNode);
      break;
    case QUERY_NODE_LOGIC_PLAN_MERGE: {
      SMergeLogicNode* pLogicNode = (SMergeLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pMergeKeys);
      nodesDestroyList(pLogicNode->pInputs);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_WINDOW: {
      SWindowLogicNode* pLogicNode = (SWindowLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pFuncs);
      nodesDestroyNode(pLogicNode->pTspk);
      nodesDestroyNode(pLogicNode->pTsEnd);
      nodesDestroyNode(pLogicNode->pStateExpr);
      nodesDestroyNode(pLogicNode->pStartCond);
      nodesDestroyNode(pLogicNode->pEndCond);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_FILL: {
      SFillLogicNode* pLogicNode = (SFillLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyNode(pLogicNode->pWStartTs);
      nodesDestroyNode(pLogicNode->pValues);
      nodesDestroyList(pLogicNode->pFillExprs);
      nodesDestroyList(pLogicNode->pNotFillExprs);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_SORT: {
      SSortLogicNode* pLogicNode = (SSortLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pSortKeys);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_PARTITION: {
      SPartitionLogicNode* pLogicNode = (SPartitionLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pPartitionKeys);
      nodesDestroyList(pLogicNode->pTags);
      nodesDestroyNode(pLogicNode->pSubtable);
      nodesDestroyList(pLogicNode->pAggFuncs);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncLogicNode* pLogicNode = (SIndefRowsFuncLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pFuncs);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_INTERP_FUNC: {
      SInterpFuncLogicNode* pLogicNode = (SInterpFuncLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pFuncs);
      nodesDestroyNode(pLogicNode->pFillValues);
      nodesDestroyNode(pLogicNode->pTimeSeries);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_GROUP_CACHE: {
      SGroupCacheLogicNode* pLogicNode = (SGroupCacheLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      nodesDestroyList(pLogicNode->pGroupCols);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL: {
      SDynQueryCtrlLogicNode* pLogicNode = (SDynQueryCtrlLogicNode*)pNode;
      destroyLogicNode((SLogicNode*)pLogicNode);
      break;
    }
    case QUERY_NODE_LOGIC_SUBPLAN: {
      SLogicSubplan* pSubplan = (SLogicSubplan*)pNode;
      nodesDestroyList(pSubplan->pChildren);
      nodesDestroyNode((SNode*)pSubplan->pNode);
      nodesClearList(pSubplan->pParents);
      taosMemoryFreeClear(pSubplan->pVgroupList);
      break;
    }
    case QUERY_NODE_LOGIC_PLAN:
      nodesDestroyList(((SQueryLogicPlan*)pNode)->pTopSubplans);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
      destroyScanPhysiNode((SScanPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN: {
      SLastRowScanPhysiNode* pPhyNode = (SLastRowScanPhysiNode*)pNode;
      destroyScanPhysiNode((SScanPhysiNode*)pNode);
      nodesDestroyList(pPhyNode->pGroupTags);
      nodesDestroyList(pPhyNode->pTargets);
      taosArrayDestroy(pPhyNode->pFuncTypes);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN: {
      STableScanPhysiNode* pPhyNode = (STableScanPhysiNode*)pNode;
      destroyScanPhysiNode((SScanPhysiNode*)pNode);
      nodesDestroyList(pPhyNode->pDynamicScanFuncs);
      nodesDestroyList(pPhyNode->pGroupTags);
      nodesDestroyList(pPhyNode->pTags);
      nodesDestroyNode(pPhyNode->pSubtable);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
      SProjectPhysiNode* pPhyNode = (SProjectPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pProjections);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN: {
      SSortMergeJoinPhysiNode* pPhyNode = (SSortMergeJoinPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pWindowOffset);
      nodesDestroyNode(pPhyNode->pJLimit);
      nodesDestroyNode(pPhyNode->leftPrimExpr);
      nodesDestroyNode(pPhyNode->rightPrimExpr);
      nodesDestroyList(pPhyNode->pEqLeft);
      nodesDestroyList(pPhyNode->pEqRight);
      nodesDestroyNode(pPhyNode->pPrimKeyCond);
      nodesDestroyNode(pPhyNode->pFullOnCond);
      nodesDestroyList(pPhyNode->pTargets);
      nodesDestroyNode(pPhyNode->pColEqCond);
      nodesDestroyNode(pPhyNode->pColOnCond);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN: {
      SHashJoinPhysiNode* pPhyNode = (SHashJoinPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pOnLeft);
      nodesDestroyList(pPhyNode->pOnRight);
      nodesDestroyNode(pPhyNode->leftPrimExpr);
      nodesDestroyNode(pPhyNode->rightPrimExpr);
      nodesDestroyNode(pPhyNode->pFullOnCond);
      nodesDestroyList(pPhyNode->pTargets);

      nodesDestroyNode(pPhyNode->pPrimKeyCond);
      nodesDestroyNode(pPhyNode->pColEqCond);
      nodesDestroyNode(pPhyNode->pTagEqCond);

      nodesDestroyNode(pPhyNode->pLeftOnCond);
      nodesDestroyNode(pPhyNode->pRightOnCond);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG: {
      SAggPhysiNode* pPhyNode = (SAggPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pAggFuncs);
      nodesDestroyList(pPhyNode->pGroupKeys);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE: {
      SExchangePhysiNode* pPhyNode = (SExchangePhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pSrcEndPoints);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE: {
      SMergePhysiNode* pPhyNode = (SMergePhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pMergeKeys);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT: {
      SSortPhysiNode* pPhyNode = (SSortPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pSortKeys);
      nodesDestroyList(pPhyNode->pTargets);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
      destroyWinodwPhysiNode((SWindowPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL: {
      SFillPhysiNode* pPhyNode = (SFillPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pFillExprs);
      nodesDestroyList(pPhyNode->pNotFillExprs);
      nodesDestroyNode(pPhyNode->pWStartTs);
      nodesDestroyNode(pPhyNode->pValues);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
      destroyWinodwPhysiNode((SWindowPhysiNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE: {
      SStateWinodwPhysiNode* pPhyNode = (SStateWinodwPhysiNode*)pNode;
      destroyWinodwPhysiNode((SWindowPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pStateKey);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT: {
      SEventWinodwPhysiNode* pPhyNode = (SEventWinodwPhysiNode*)pNode;
      destroyWinodwPhysiNode((SWindowPhysiNode*)pPhyNode);
      nodesDestroyNode(pPhyNode->pStartCond);
      nodesDestroyNode(pPhyNode->pEndCond);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT: {
      SCountWinodwPhysiNode* pPhyNode = (SCountWinodwPhysiNode*)pNode;
      destroyWinodwPhysiNode((SWindowPhysiNode*)pPhyNode);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION: {
      destroyPartitionPhysiNode((SPartitionPhysiNode*)pNode);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION: {
      SStreamPartitionPhysiNode* pPhyNode = (SStreamPartitionPhysiNode*)pNode;
      destroyPartitionPhysiNode((SPartitionPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pTags);
      nodesDestroyNode(pPhyNode->pSubtable);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC: {
      SIndefRowsFuncPhysiNode* pPhyNode = (SIndefRowsFuncPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pFuncs);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC: {
      SInterpFuncPhysiNode* pPhyNode = (SInterpFuncPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pExprs);
      nodesDestroyList(pPhyNode->pFuncs);
      nodesDestroyNode(pPhyNode->pFillValues);
      nodesDestroyNode(pPhyNode->pTimeSeries);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
      destroyDataSinkNode((SDataSinkNode*)pNode);
      break;
    case QUERY_NODE_PHYSICAL_PLAN_INSERT: {
      SDataInserterNode* pSink = (SDataInserterNode*)pNode;
      destroyDataSinkNode((SDataSinkNode*)pSink);
      taosMemoryFreeClear(pSink->pData);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT: {
      SQueryInserterNode* pSink = (SQueryInserterNode*)pNode;
      destroyDataSinkNode((SDataSinkNode*)pSink);
      nodesDestroyList(pSink->pCols);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DELETE: {
      SDataDeleterNode* pSink = (SDataDeleterNode*)pNode;
      destroyDataSinkNode((SDataSinkNode*)pSink);
      nodesDestroyNode(pSink->pAffectedRows);
      nodesDestroyNode(pSink->pStartTs);
      nodesDestroyNode(pSink->pEndTs);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE: {
      SGroupCachePhysiNode* pPhyNode = (SGroupCachePhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      nodesDestroyList(pPhyNode->pGroupCols);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL: {
      SDynQueryCtrlPhysiNode* pPhyNode = (SDynQueryCtrlPhysiNode*)pNode;
      destroyPhysiNode((SPhysiNode*)pPhyNode);
      break;
    }
    case QUERY_NODE_PHYSICAL_SUBPLAN: {
      SSubplan* pSubplan = (SSubplan*)pNode;
      nodesClearList(pSubplan->pChildren);
      nodesDestroyNode((SNode*)pSubplan->pNode);
      nodesDestroyNode((SNode*)pSubplan->pDataSink);
      nodesDestroyNode((SNode*)pSubplan->pTagCond);
      nodesDestroyNode((SNode*)pSubplan->pTagIndexCond);
      nodesClearList(pSubplan->pParents);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN:
      nodesDestroyList(((SQueryPlan*)pNode)->pSubplans);
      break;
    default:
      break;
  }
  nodesFree(pNode);
  return;
}

int32_t nodesMakeList(SNodeList** ppListOut) {
  SNodeList* p = NULL;
  int32_t code = nodesCalloc(1, sizeof(SNodeList), (void**)&p);
  if (TSDB_CODE_SUCCESS == code) {
    *ppListOut = p;
  }
  return code;
}

int32_t nodesListAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = NULL;
  int32_t code = nodesCalloc(1, sizeof(SListCell), (void**)&p);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  p->pNode = pNode;
  if (NULL == pList->pHead) {
    pList->pHead = p;
  }
  if (NULL != pList->pTail) {
    pList->pTail->pNext = p;
  }
  p->pPrev = pList->pTail;
  pList->pTail = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppend(SNodeList* pList, SNode* pNode) {
  if (NULL == pNode) {
    return TSDB_CODE_INVALID_PARA;
  }
  int32_t code = nodesListAppend(pList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
}

int32_t nodesListMakeAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    int32_t code = nodesMakeList(pList);
    if (NULL == *pList) {
      return code;
    }
  }
  return nodesListAppend(*pList, pNode);
}

int32_t nodesListMakeStrictAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    int32_t code = nodesMakeList(pList);
    if (NULL == *pList) {
      return code;
    }
  }
  return nodesListStrictAppend(*pList, pNode);
}

int32_t nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pSrc) {
    return TSDB_CODE_FAILED;
  }

  if (NULL == pTarget->pHead) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pTarget->pTail->pNext = pSrc->pHead;
    if (NULL != pSrc->pHead) {
      pSrc->pHead->pPrev = pTarget->pTail;
    }
  }
  pTarget->pTail = pSrc->pTail;
  pTarget->length += pSrc->length;
  nodesFree(pSrc);

  return TSDB_CODE_SUCCESS;
}

int32_t nodesListStrictAppendList(SNodeList* pTarget, SNodeList* pSrc) {
  if (NULL == pSrc) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t code = nodesListAppendList(pTarget, pSrc);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pSrc);
  }
  return code;
}


int32_t nodesListMakeStrictAppendList(SNodeList** pTarget, SNodeList* pSrc) {
  if (NULL == *pTarget) {
    int32_t code = nodesMakeList(pTarget);
    if (NULL == *pTarget) {
      return code;
    }
  }
  return nodesListStrictAppendList(*pTarget, pSrc);
}

int32_t    nodesListMakePushFront(SNodeList** pList, SNode* pNode) {
  if (*pList == NULL) {
    int32_t code = nodesMakeList(pList);
    if (*pList == NULL) {
      return code;
    }
  }
  return nodesListPushFront(*pList, pNode);
}

int32_t nodesListPushFront(SNodeList* pList, SNode* pNode) {
  if (NULL == pList || NULL == pNode) {
    return TSDB_CODE_FAILED;
  }
  SListCell* p = NULL;
  int32_t code = nodesCalloc(1, sizeof(SListCell), (void**)&p);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  p->pNode = pNode;
  if (NULL != pList->pHead) {
    pList->pHead->pPrev = p;
    p->pNext = pList->pHead;
  }
  pList->pHead = p;
  pList->pTail = pList->pTail ? pList->pTail : p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}

SListCell* nodesListErase(SNodeList* pList, SListCell* pCell) {
  if (NULL == pCell->pPrev) {
    pList->pHead = pCell->pNext;
  } else {
    pCell->pPrev->pNext = pCell->pNext;
  }
  if (NULL == pCell->pNext) {
    pList->pTail = pCell->pPrev;
  } else {
    pCell->pNext->pPrev = pCell->pPrev;
  }
  SListCell* pNext = pCell->pNext;
  nodesDestroyNode(pCell->pNode);
  nodesFree(pCell);
  --(pList->length);
  return pNext;
}

void nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pPos || NULL == pSrc || NULL == pSrc->pHead) {
    return;
  }

  if (NULL == pPos->pPrev) {
    pTarget->pHead = pSrc->pHead;
  } else {
    pPos->pPrev->pNext = pSrc->pHead;
  }
  pSrc->pHead->pPrev = pPos->pPrev;
  pSrc->pTail->pNext = pPos;
  pPos->pPrev = pSrc->pTail;

  pTarget->length += pSrc->length;
  nodesFree(pSrc);
}

void nodesListInsertListAfterPos(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc) {
  if (NULL == pTarget || NULL == pPos || NULL == pSrc || NULL == pSrc->pHead) {
    return;
  }

  if (NULL == pPos->pNext) {
    pTarget->pTail = pSrc->pHead;
  } else {
    pPos->pNext->pPrev = pSrc->pHead;
  }

  pSrc->pHead->pPrev = pPos;
  pSrc->pTail->pNext = pPos->pNext;

  pPos->pNext = pSrc->pHead;

  pTarget->length += pSrc->length;
  nodesFree(pSrc);
}

SNode* nodesListGetNode(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return node;
    }
  }
  return NULL;
}

SListCell* nodesListGetCell(SNodeList* pList, int32_t index) {
  SNode* node;
  FOREACH(node, pList) {
    if (0 == index--) {
      return cell;
    }
  }
  return NULL;
}

void nodesDestroyList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    pNext = nodesListErase(pList, pNext);
  }
  nodesFree(pList);
}

void nodesClearList(SNodeList* pList) {
  if (NULL == pList) {
    return;
  }

  SListCell* pNext = pList->pHead;
  while (NULL != pNext) {
    SListCell* tmp = pNext;
    pNext = pNext->pNext;
    nodesFree(tmp);
  }
  nodesFree(pList);
}

void* nodesGetValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
      return (void*)&pNode->typeData;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      return (void*)pNode->datum.p;
    default:
      break;
  }

  return NULL;
}

int32_t nodesSetValueNodeValue(SValueNode* pNode, void* value) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL:
      pNode->datum.b = *(bool*)value;
      *(bool*)&pNode->typeData = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pNode->datum.i = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pNode->datum.i = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_INT:
      pNode->datum.i = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      pNode->datum.i = *(int64_t*)value;
      *(int64_t*)&pNode->typeData = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pNode->datum.u = *(int8_t*)value;
      *(int8_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pNode->datum.u = *(int16_t*)value;
      *(int16_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UINT:
      pNode->datum.u = *(int32_t*)value;
      *(int32_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pNode->datum.u = *(uint64_t*)value;
      *(uint64_t*)&pNode->typeData = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pNode->datum.d = *(float*)value;
      *(float*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pNode->datum.d = *(double*)value;
      *(double*)&pNode->typeData = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      pNode->datum.p = (char*)value;
      break;
    default:
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

char* nodesGetStrValueFromNode(SValueNode* pNode) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_BOOL: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%s", pNode->datum.b ? "true" : "false");
      return buf;
    }
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRId64, pNode->datum.i);
      return buf;
    }
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%" PRIu64, pNode->datum.u);
      return buf;
    }
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE: {
      void* buf = taosMemoryMalloc(MAX_NUM_STR_SIZE);
      if (NULL == buf) {
        return NULL;
      }

      sprintf(buf, "%e", pNode->datum.d);
      return buf;
    }
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY: {
      int32_t bufSize = varDataLen(pNode->datum.p) + 2 + 1;
      void*   buf = taosMemoryMalloc(bufSize);
      if (NULL == buf) {
        return NULL;
      }

      snprintf(buf, bufSize, "'%s'", varDataVal(pNode->datum.p));
      return buf;
    }
    default:
      break;
  }

  return NULL;
}

bool nodesIsExprNode(const SNode* pNode) {
  ENodeType type = nodeType(pNode);
  return (QUERY_NODE_COLUMN == type || QUERY_NODE_VALUE == type || QUERY_NODE_OPERATOR == type ||
          QUERY_NODE_FUNCTION == type || QUERY_NODE_LOGIC_CONDITION == type || QUERY_NODE_CASE_WHEN == type);
}

bool nodesIsUnaryOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_MINUS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsArithmeticOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_REM:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsComparisonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_JSON_CONTAINS:
    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsJsonOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsRegularOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsMatchRegularOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
      return true;
    default:
      break;
  }
  return false;
}

bool nodesIsBitwiseOp(const SOperatorNode* pOp) {
  switch (pOp->opType) {
    case OP_TYPE_BIT_AND:
    case OP_TYPE_BIT_OR:
      return true;
    default:
      break;
  }
  return false;
}

typedef struct SCollectColumnsCxt {
  int32_t         errCode;
  const char*     pTableAlias;
  SSHashObj*      pMultiTableAlias;
  ECollectColType collectType;
  SNodeList*      pCols;
  SHashObj*       pColHash;
} SCollectColumnsCxt;

static EDealRes doCollect(SCollectColumnsCxt* pCxt, SColumnNode* pCol, SNode* pNode) {
  char    name[TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN];
  int32_t len = 0;
  if ('\0' == pCol->tableAlias[0]) {
    len = snprintf(name, sizeof(name), "%s", pCol->colName);
  } else {
    len = snprintf(name, sizeof(name), "%s.%s", pCol->tableAlias, pCol->colName);
  }
  if (NULL == taosHashGet(pCxt->pColHash, name, len)) {
    pCxt->errCode = taosHashPut(pCxt->pColHash, name, len, NULL, 0);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      SNode* pNew = NULL;
      pCxt->errCode = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == pCxt->errCode) {
        pCxt->errCode = nodesListStrictAppend(pCxt->pCols, pNew);
      }
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static bool isCollectType(ECollectColType collectType, EColumnType colType) {
  return COLLECT_COL_TYPE_ALL == collectType
             ? true
             : (COLLECT_COL_TYPE_TAG == collectType ? COLUMN_TYPE_TAG == colType : (COLUMN_TYPE_TAG != colType && COLUMN_TYPE_TBNAME != colType));
}

static EDealRes collectColumns(SNode* pNode, void* pContext) {
  SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (isCollectType(pCxt->collectType, pCol->colType) && 0 != strcmp(pCol->colName, "*") &&
        (NULL == pCxt->pTableAlias || 0 == strcmp(pCxt->pTableAlias, pCol->tableAlias))) {
      return doCollect(pCxt, pCol, pNode);
    }
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes collectColumnsExt(SNode* pNode, void* pContext) {
  SCollectColumnsCxt* pCxt = (SCollectColumnsCxt*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    SColumnNode* pCol = (SColumnNode*)pNode;
    if (isCollectType(pCxt->collectType, pCol->colType) && 0 != strcmp(pCol->colName, "*") &&
        (NULL == pCxt->pMultiTableAlias || NULL != (pCxt->pTableAlias = tSimpleHashGet(pCxt->pMultiTableAlias, pCol->tableAlias, strlen(pCol->tableAlias))))) {
      return doCollect(pCxt, pCol, pNode);
    }
  }
  return DEAL_RES_CONTINUE;
}


int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, ECollectColType type,
                            SNodeList** pCols) {
  if (NULL == pSelect || NULL == pCols) {
    return TSDB_CODE_FAILED;
  }
  SNodeList * pList = NULL;
  if (!*pCols) {
    int32_t code = nodesMakeList(&pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = pTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? pList : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectColumns, &cxt);
  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t nodesCollectColumnsExt(SSelectStmt* pSelect, ESqlClause clause, SSHashObj* pMultiTableAlias, ECollectColType type,
                            SNodeList** pCols) {
  if (NULL == pSelect || NULL == pCols) {
    return TSDB_CODE_FAILED;
  }

  SNodeList * pList = NULL;
  if (!*pCols) {
    int32_t code = nodesMakeList(&pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = NULL,
      .pMultiTableAlias = pMultiTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? pList : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;
  nodesWalkSelectStmtImpl(pSelect, clause, collectColumnsExt, &cxt);
  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t nodesCollectColumnsFromNode(SNode* node, const char* pTableAlias, ECollectColType type, SNodeList** pCols) {
  if (NULL == pCols) {
    return TSDB_CODE_FAILED;
  }
  SNodeList * pList = NULL;
  if (!*pCols) {
    int32_t code = nodesMakeList(&pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  SCollectColumnsCxt cxt = {
      .errCode = TSDB_CODE_SUCCESS,
      .pTableAlias = pTableAlias,
      .collectType = type,
      .pCols = (NULL == *pCols ? pList : *pCols),
      .pColHash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK)};
  if (NULL == cxt.pCols || NULL == cxt.pColHash) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pCols = NULL;

  nodesWalkExpr(node, collectColumns, &cxt);

  taosHashCleanup(cxt.pColHash);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pCols);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pCols) > 0) {
    *pCols = cxt.pCols;
  } else {
    nodesDestroyList(cxt.pCols);
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct SCollectFuncsCxt {
  int32_t         errCode;
  char*           tableAlias;
  FFuncClassifier classifier;
  SNodeList*      pFuncs;
} SCollectFuncsCxt;

static EDealRes collectFuncs(SNode* pNode, void* pContext) {
  SCollectFuncsCxt* pCxt = (SCollectFuncsCxt*)pContext;
  if (QUERY_NODE_FUNCTION == nodeType(pNode) && pCxt->classifier(((SFunctionNode*)pNode)->funcId) &&
      !(((SExprNode*)pNode)->orderAlias)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (FUNCTION_TYPE_TBNAME == pFunc->funcType && pCxt->tableAlias) {
      SValueNode* pVal = (SValueNode*)nodesListGetNode(pFunc->pParameterList, 0);
      if (pVal && strcmp(pVal->literal, pCxt->tableAlias)) {
        return DEAL_RES_CONTINUE;
      }
    }

    bool bFound = false;
    SNode* pn = NULL;
    FOREACH(pn, pCxt->pFuncs) {
      if (nodesEqualNode(pn, pNode)) {
        bFound = true;
        break;
      }
    }
    if (!bFound) {
      SNode* pNew = NULL;
      pCxt->errCode = nodesCloneNode(pNode, &pNew);
      if (TSDB_CODE_SUCCESS == pCxt->errCode) {
        pCxt->errCode = nodesListStrictAppend(pCxt->pFuncs, pNew);
      }
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

static uint32_t funcNodeHash(const char* pKey, uint32_t len) {
  SExprNode* pExpr = *(SExprNode**)pKey;
  return MurmurHash3_32(pExpr->aliasName, strlen(pExpr->aliasName));
}

static int32_t funcNodeEqual(const void* pLeft, const void* pRight, size_t len) {
  // if (0 != strcmp((*(const SExprNode**)pLeft)->aliasName, (*(const SExprNode**)pRight)->aliasName)) {
  //   return 1;
  // }
  return nodesEqualNode(*(const SNode**)pLeft, *(const SNode**)pRight) ? 0 : 1;
}

int32_t nodesCollectSelectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier, SNodeList* pFuncs) {
  if (NULL == pSelect || NULL == pFuncs) {
    return TSDB_CODE_FAILED;
  }

  SCollectFuncsCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                          .classifier = classifier,
                          .tableAlias = tableAlias,
                          .pFuncs = pFuncs};

  nodesWalkSelectStmt(pSelect, clause, collectFuncs, &cxt);
  return cxt.errCode;
}

int32_t nodesCollectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier, SNodeList** pFuncs) {
  if (NULL == pSelect || NULL == pFuncs) {
    return TSDB_CODE_FAILED;
  }
  SNodeList* pList = NULL;
  if (!*pFuncs) {
    int32_t code = nodesMakeList(&pList);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  SCollectFuncsCxt cxt = {.errCode = TSDB_CODE_SUCCESS,
                          .classifier = classifier,
                          .tableAlias = tableAlias,
                          .pFuncs = (NULL == *pFuncs ? pList : *pFuncs)};
  if (NULL == cxt.pFuncs) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *pFuncs = NULL;
  nodesWalkSelectStmt(pSelect, clause, collectFuncs, &cxt);
  if (TSDB_CODE_SUCCESS == cxt.errCode) {
    if (LIST_LENGTH(cxt.pFuncs) > 0) {
      *pFuncs = cxt.pFuncs;
    } else {
      nodesDestroyList(cxt.pFuncs);
    }
  } else {
    nodesDestroyList(cxt.pFuncs);
  }

  return cxt.errCode;
}

typedef struct SCollectSpecialNodesCxt {
  int32_t    errCode;
  ENodeType  type;
  SNodeList* pNodes;
} SCollectSpecialNodesCxt;

static EDealRes collectSpecialNodes(SNode* pNode, void* pContext) {
  SCollectSpecialNodesCxt* pCxt = (SCollectSpecialNodesCxt*)pContext;
  if (pCxt->type == nodeType(pNode)) {
    SNode* pNew = NULL;
    pCxt->errCode = nodesCloneNode(pNode, &pNew);
    if (TSDB_CODE_SUCCESS == pCxt->errCode) {
      pCxt->errCode = nodesListStrictAppend(pCxt->pNodes, pNew);
    }
    return (TSDB_CODE_SUCCESS == pCxt->errCode ? DEAL_RES_IGNORE_CHILD : DEAL_RES_ERROR);
  }
  return DEAL_RES_CONTINUE;
}

int32_t nodesCollectSpecialNodes(SSelectStmt* pSelect, ESqlClause clause, ENodeType type, SNodeList** ppNodes) {
  if (NULL == pSelect || NULL == ppNodes) {
    return TSDB_CODE_FAILED;
  }
  SCollectSpecialNodesCxt cxt = {.errCode = TSDB_CODE_SUCCESS, .type = type, .pNodes = NULL};
  if (!*ppNodes) {
    cxt.errCode = nodesMakeList(&cxt.pNodes);
  } else {
    cxt.pNodes = *ppNodes;
  }
  if (NULL == cxt.pNodes) {
    return cxt.errCode;
  }
  *ppNodes = NULL;
  nodesWalkSelectStmt(pSelect, SQL_CLAUSE_GROUP_BY, collectSpecialNodes, &cxt);
  if (TSDB_CODE_SUCCESS != cxt.errCode) {
    nodesDestroyList(cxt.pNodes);
    return cxt.errCode;
  }
  if (LIST_LENGTH(cxt.pNodes) > 0) {
    *ppNodes = cxt.pNodes;
  } else {
    nodesDestroyList(cxt.pNodes);
  }

  return TSDB_CODE_SUCCESS;
}

static EDealRes hasColumn(SNode* pNode, void* pContext) {
  if (QUERY_NODE_COLUMN == nodeType(pNode)) {
    *(bool*)pContext = true;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

bool nodesExprHasColumn(SNode* pNode) {
  bool hasCol = false;
  nodesWalkExprPostOrder(pNode, hasColumn, &hasCol);
  return hasCol;
}

bool nodesExprsHasColumn(SNodeList* pList) {
  bool hasCol = false;
  nodesWalkExprsPostOrder(pList, hasColumn, &hasCol);
  return hasCol;
}

char* nodesGetFillModeString(EFillMode mode) {
  switch (mode) {
    case FILL_MODE_NONE:
      return "none";
    case FILL_MODE_VALUE:
      return "value";
    case FILL_MODE_VALUE_F:
      return "value_f";
    case FILL_MODE_PREV:
      return "prev";
    case FILL_MODE_NULL:
      return "null";
    case FILL_MODE_NULL_F:
      return "null_f";
    case FILL_MODE_LINEAR:
      return "linear";
    case FILL_MODE_NEXT:
      return "next";
    default:
      return "unknown";
  }
}

char* nodesGetNameFromColumnNode(SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_COLUMN != pNode->type) {
    return "NULL";
  }

  return ((SColumnNode*)pNode)->node.userAlias;
}

int32_t nodesGetOutputNumFromSlotList(SNodeList* pSlots) {
  if (NULL == pSlots || pSlots->length <= 0) {
    return 0;
  }

  SNode*  pNode = NULL;
  int32_t num = 0;
  FOREACH(pNode, pSlots) {
    if (QUERY_NODE_SLOT_DESC != pNode->type) {
      continue;
    }

    SSlotDescNode* descNode = (SSlotDescNode*)pNode;
    if (descNode->output) {
      ++num;
    }
  }

  return num;
}

int32_t nodesValueNodeToVariant(const SValueNode* pNode, SVariant* pVal) {
  int32_t code = 0;
  if (pNode->isNull) {
    pVal->nType = TSDB_DATA_TYPE_NULL;
    pVal->nLen = tDataTypes[TSDB_DATA_TYPE_NULL].bytes;
    return code;
  }
  pVal->nType = pNode->node.resType.type;
  pVal->nLen = pNode->node.resType.bytes;
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      break;
    case TSDB_DATA_TYPE_BOOL:
      pVal->i = pNode->datum.b;
      break;
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      pVal->i = pNode->datum.i;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
      pVal->u = pNode->datum.u;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pVal->f = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pVal->d = pNode->datum.d;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      pVal->pz = taosMemoryMalloc(pVal->nLen + 1);
      if (pVal->pz) {
        memcpy(pVal->pz, pNode->datum.p, pVal->nLen);
        pVal->pz[pVal->nLen] = 0;
      } else {
        code = terrno;
      }
      break;
    case TSDB_DATA_TYPE_JSON:
      pVal->nLen = getJsonValueLen(pNode->datum.p);
      pVal->pz = taosMemoryMalloc(pVal->nLen);
      if (pVal->pz) {
        memcpy(pVal->pz, pNode->datum.p, pVal->nLen);
      } else {
        code = terrno;
      }
      break;
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
      // todo
    default:
      break;
  }
  return code;
}

int32_t nodesMergeConds(SNode** pDst, SNodeList** pSrc) {
  if (NULL == *pSrc) {
    return TSDB_CODE_SUCCESS;
  }

  if (1 == LIST_LENGTH(*pSrc)) {
    *pDst = nodesListGetNode(*pSrc, 0);
    nodesClearList(*pSrc);
  } else {
    SLogicConditionNode* pLogicCond = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogicCond->condType = LOGIC_COND_TYPE_AND;
    pLogicCond->pParameterList = *pSrc;
    *pDst = (SNode*)pLogicCond;
  }
  *pSrc = NULL;

  return TSDB_CODE_SUCCESS;
}

const char* dataOrderStr(EDataOrderLevel order) {
  switch (order) {
    case DATA_ORDER_LEVEL_NONE:
      return "no order required";
    case DATA_ORDER_LEVEL_IN_BLOCK:
      return "in-datablock order";
    case DATA_ORDER_LEVEL_IN_GROUP:
      return "in-group order";
    case DATA_ORDER_LEVEL_GLOBAL:
      return "global order";
    default:
      break;
  }
  return "unknown";
}

int32_t nodesMakeValueNodeFromString(char* literal, SValueNode** ppValNode) {
  int32_t lenStr = strlen(literal);
  SValueNode* pValNode = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValNode);
  if (pValNode) {
    pValNode->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
    pValNode->node.resType.bytes = lenStr + VARSTR_HEADER_SIZE;
    char* p = taosMemoryMalloc(lenStr + 1  + VARSTR_HEADER_SIZE);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    varDataSetLen(p, lenStr);
    memcpy(varDataVal(p), literal, lenStr + 1);
    pValNode->datum.p = p;
    pValNode->literal = tstrdup(literal);
    pValNode->translate = true;
    pValNode->isNull = false;
    *ppValNode = pValNode;
  }
  return code;
}

int32_t nodesMakeValueNodeFromBool(bool b, SValueNode** ppValNode) {
  SValueNode* pValNode = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValNode);
  if (TSDB_CODE_SUCCESS == code) {
    pValNode->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pValNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    code = nodesSetValueNodeValue(pValNode, &b);
    if (TSDB_CODE_SUCCESS == code) {
      pValNode->translate = true;
      pValNode->isNull = false;
      *ppValNode = pValNode;
    } else {
      nodesDestroyNode((SNode*)pValNode);
    }
  }
  return code;
}

int32_t nodesMakeValueNodeFromInt32(int32_t value, SNode** ppNode) {
  SValueNode* pValNode = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValNode);
  if (TSDB_CODE_SUCCESS == code) {
    pValNode->node.resType.type = TSDB_DATA_TYPE_INT;
    pValNode->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
    code = nodesSetValueNodeValue(pValNode, &value);
    if (TSDB_CODE_SUCCESS == code) {
      pValNode->translate = true;
      pValNode->isNull = false;
      *ppNode = (SNode*)pValNode;
    } else {
      nodesDestroyNode((SNode*)pValNode);
    }
  }
  return code;
}

bool nodesIsStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' == ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

bool nodesIsTableStar(SNode* pNode) {
  return (QUERY_NODE_COLUMN == nodeType(pNode)) && ('\0' != ((SColumnNode*)pNode)->tableAlias[0]) &&
         (0 == strcmp(((SColumnNode*)pNode)->colName, "*"));
}

void nodesSortList(SNodeList** pList, int32_t (*comp)(SNode* pNode1, SNode* pNode2)) {
  if ((*pList)->length == 1) return;

  uint32_t inSize = 1;
  SListCell* pHead = (*pList)->pHead;
  while (1) {
    SListCell* p = pHead;
    pHead = NULL;
    SListCell* pTail = NULL;

    uint32_t nMerges = 0;
    while (p) {
      ++nMerges;
      SListCell* q = p;
      uint32_t pSize = 0;
      for (uint32_t i = 0; i < inSize; ++i) {
        ++pSize;
        q = q->pNext;
        if (!q) {
          break;
        }
      }

      uint32_t qSize = inSize;

      while (pSize > 0 || (qSize > 0 && q)) {
        SListCell* pCell;
        if (pSize == 0) {
          pCell = q;
          q = q->pNext;
          --qSize;
        } else if (qSize == 0 || !q) {
          pCell = p;
          p = p->pNext;
          --pSize;
        } else if (comp(q->pNode, p->pNode) >= 0) {
          pCell = p;
          p = p->pNext;
          --pSize;
        } else {
          pCell = q;
          q = q->pNext;
          --qSize;
        }

        if (pTail) {
          pTail->pNext = pCell;
          pCell->pPrev = pTail;
        } else {
          pHead = pCell;
          pHead->pPrev = NULL;
        }
        pTail = pCell;
      }
      p = q;
    }
    pTail->pNext = NULL;

    if (nMerges <= 1) {
      (*pList)->pHead = pHead;
      (*pList)->pTail = pTail;
      return;
    }
    inSize *= 2;
  }
}
