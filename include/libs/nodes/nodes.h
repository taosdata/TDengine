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

#ifndef _TD_NODES_H_
#define _TD_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tdef.h"

#define nodeType(nodeptr)              (((const SNode*)(nodeptr))->type)
#define setNodeType(nodeptr, nodetype) (((SNode*)(nodeptr))->type = (nodetype))

#define LIST_LENGTH(l) (NULL != (l) ? (l)->length : 0)

#define FOREACH(node, list)                                                                                   \
  for (SListCell* cell = (NULL != (list) ? (list)->pHead : NULL), *pNext;                                     \
       (NULL != cell ? (node = cell->pNode, pNext = cell->pNext, true) : (node = NULL, pNext = NULL, false)); \
       cell = pNext)

#define REPLACE_NODE(newNode) cell->pNode = (SNode*)(newNode)

#define INSERT_LIST(target, src) nodesListInsertList((target), cell, src)

#define WHERE_EACH(node, list)                               \
  SListCell* cell = (NULL != (list) ? (list)->pHead : NULL); \
  while (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false))

#define WHERE_NEXT cell = cell->pNext

// only be use in WHERE_EACH
#define ERASE_NODE(list) cell = nodesListErase((list), cell)

#define FORBOTH(node1, list1, node2, list2)                                               \
  for (SListCell* cell1 = (NULL != (list1) ? (list1)->pHead : NULL),                      \
                  *cell2 = (NULL != (list2) ? (list2)->pHead : NULL);                     \
       (NULL == cell1 ? (node1 = NULL, false) : (node1 = cell1->pNode, true)),            \
                  (NULL == cell2 ? (node2 = NULL, false) : (node2 = cell2->pNode, true)), \
                  (node1 != NULL && node2 != NULL);                                       \
       cell1 = cell1->pNext, cell2 = cell2->pNext)

#define REPLACE_LIST1_NODE(newNode) cell1->pNode = (SNode*)(newNode)
#define REPLACE_LIST2_NODE(newNode) cell2->pNode = (SNode*)(newNode)

#define FOREACH_FOR_REWRITE(node, list)                           \
  for (SListCell* cell = (NULL != (list) ? (list)->pHead : NULL); \
       (NULL != cell ? (node = &(cell->pNode), true) : (node = NULL, false)); cell = cell->pNext)

#define NODES_DESTORY_NODE(node) \
  do {                           \
    nodesDestroyNode((node));    \
    (node) = NULL;               \
  } while (0)

#define NODES_DESTORY_LIST(list) \
  do {                           \
    nodesDestroyList((list));    \
    (list) = NULL;               \
  } while (0)

#define NODES_CLEAR_LIST(list) \
  do {                         \
    nodesClearList((list));    \
    (list) = NULL;             \
  } while (0)

typedef enum ENodeType {
  // Syntax nodes are used in parser and planner module, and some are also used in executor module, such as COLUMN,
  // VALUE, OPERATOR, FUNCTION and so on.
  QUERY_NODE_COLUMN = 1,
  QUERY_NODE_VALUE,
  QUERY_NODE_OPERATOR,
  QUERY_NODE_LOGIC_CONDITION,
  QUERY_NODE_FUNCTION,
  QUERY_NODE_REAL_TABLE,
  QUERY_NODE_TEMP_TABLE,
  QUERY_NODE_JOIN_TABLE,
  QUERY_NODE_GROUPING_SET,
  QUERY_NODE_ORDER_BY_EXPR,
  QUERY_NODE_LIMIT,
  QUERY_NODE_STATE_WINDOW,
  QUERY_NODE_SESSION_WINDOW,
  QUERY_NODE_INTERVAL_WINDOW,
  QUERY_NODE_NODE_LIST,
  QUERY_NODE_FILL,
  QUERY_NODE_RAW_EXPR,  // Only be used in parser module.
  QUERY_NODE_TARGET,
  QUERY_NODE_DATABLOCK_DESC,
  QUERY_NODE_SLOT_DESC,
  QUERY_NODE_COLUMN_DEF,
  QUERY_NODE_DOWNSTREAM_SOURCE,
  QUERY_NODE_DATABASE_OPTIONS,
  QUERY_NODE_TABLE_OPTIONS,
  QUERY_NODE_INDEX_OPTIONS,
  QUERY_NODE_EXPLAIN_OPTIONS,
  QUERY_NODE_STREAM_OPTIONS,
  QUERY_NODE_LEFT_VALUE,
  QUERY_NODE_COLUMN_REF,
  QUERY_NODE_WHEN_THEN,
  QUERY_NODE_CASE_WHEN,
  QUERY_NODE_EVENT_WINDOW,

  // Statement nodes are used in parser and planner module.
  QUERY_NODE_SET_OPERATOR = 100,
  QUERY_NODE_SELECT_STMT,
  QUERY_NODE_VNODE_MODIFY_STMT,
  QUERY_NODE_CREATE_DATABASE_STMT,
  QUERY_NODE_DROP_DATABASE_STMT,
  QUERY_NODE_ALTER_DATABASE_STMT,
  QUERY_NODE_FLUSH_DATABASE_STMT,
  QUERY_NODE_TRIM_DATABASE_STMT,
  QUERY_NODE_CREATE_TABLE_STMT,
  QUERY_NODE_CREATE_SUBTABLE_CLAUSE,
  QUERY_NODE_CREATE_MULTI_TABLES_STMT,
  QUERY_NODE_DROP_TABLE_CLAUSE,
  QUERY_NODE_DROP_TABLE_STMT,
  QUERY_NODE_DROP_SUPER_TABLE_STMT,
  QUERY_NODE_ALTER_TABLE_STMT,
  QUERY_NODE_ALTER_SUPER_TABLE_STMT,
  QUERY_NODE_CREATE_USER_STMT,
  QUERY_NODE_ALTER_USER_STMT,
  QUERY_NODE_DROP_USER_STMT,
  QUERY_NODE_USE_DATABASE_STMT,
  QUERY_NODE_CREATE_DNODE_STMT,
  QUERY_NODE_DROP_DNODE_STMT,
  QUERY_NODE_ALTER_DNODE_STMT,
  QUERY_NODE_CREATE_INDEX_STMT,
  QUERY_NODE_DROP_INDEX_STMT,
  QUERY_NODE_CREATE_QNODE_STMT,
  QUERY_NODE_DROP_QNODE_STMT,
  QUERY_NODE_CREATE_BNODE_STMT,
  QUERY_NODE_DROP_BNODE_STMT,
  QUERY_NODE_CREATE_SNODE_STMT,
  QUERY_NODE_DROP_SNODE_STMT,
  QUERY_NODE_CREATE_MNODE_STMT,
  QUERY_NODE_DROP_MNODE_STMT,
  QUERY_NODE_CREATE_TOPIC_STMT,
  QUERY_NODE_DROP_TOPIC_STMT,
  QUERY_NODE_DROP_CGROUP_STMT,
  QUERY_NODE_ALTER_LOCAL_STMT,
  QUERY_NODE_EXPLAIN_STMT,
  QUERY_NODE_DESCRIBE_STMT,
  QUERY_NODE_RESET_QUERY_CACHE_STMT,
  QUERY_NODE_COMPACT_DATABASE_STMT,
  QUERY_NODE_CREATE_FUNCTION_STMT,
  QUERY_NODE_DROP_FUNCTION_STMT,
  QUERY_NODE_CREATE_STREAM_STMT,
  QUERY_NODE_DROP_STREAM_STMT,
  QUERY_NODE_BALANCE_VGROUP_STMT,
  QUERY_NODE_MERGE_VGROUP_STMT,
  QUERY_NODE_REDISTRIBUTE_VGROUP_STMT,
  QUERY_NODE_SPLIT_VGROUP_STMT,
  QUERY_NODE_SYNCDB_STMT,
  QUERY_NODE_GRANT_STMT,
  QUERY_NODE_REVOKE_STMT,
  QUERY_NODE_SHOW_DNODES_STMT,
  QUERY_NODE_SHOW_MNODES_STMT,
  QUERY_NODE_SHOW_MODULES_STMT,
  QUERY_NODE_SHOW_QNODES_STMT,
  QUERY_NODE_SHOW_SNODES_STMT,
  QUERY_NODE_SHOW_BNODES_STMT,
  QUERY_NODE_SHOW_CLUSTER_STMT,
  QUERY_NODE_SHOW_DATABASES_STMT,
  QUERY_NODE_SHOW_FUNCTIONS_STMT,
  QUERY_NODE_SHOW_INDEXES_STMT,
  QUERY_NODE_SHOW_STABLES_STMT,
  QUERY_NODE_SHOW_STREAMS_STMT,
  QUERY_NODE_SHOW_TABLES_STMT,
  QUERY_NODE_SHOW_TAGS_STMT,
  QUERY_NODE_SHOW_USERS_STMT,
  QUERY_NODE_SHOW_LICENCES_STMT,
  QUERY_NODE_SHOW_VGROUPS_STMT,
  QUERY_NODE_SHOW_TOPICS_STMT,
  QUERY_NODE_SHOW_CONSUMERS_STMT,
  QUERY_NODE_SHOW_CONNECTIONS_STMT,
  QUERY_NODE_SHOW_QUERIES_STMT,
  QUERY_NODE_SHOW_APPS_STMT,
  QUERY_NODE_SHOW_VARIABLES_STMT,
  QUERY_NODE_SHOW_DNODE_VARIABLES_STMT,
  QUERY_NODE_SHOW_TRANSACTIONS_STMT,
  QUERY_NODE_SHOW_SUBSCRIPTIONS_STMT,
  QUERY_NODE_SHOW_VNODES_STMT,
  QUERY_NODE_SHOW_USER_PRIVILEGES_STMT,
  QUERY_NODE_SHOW_CREATE_DATABASE_STMT,
  QUERY_NODE_SHOW_CREATE_TABLE_STMT,
  QUERY_NODE_SHOW_CREATE_STABLE_STMT,
  QUERY_NODE_SHOW_TABLE_DISTRIBUTED_STMT,
  QUERY_NODE_SHOW_LOCAL_VARIABLES_STMT,
  QUERY_NODE_SHOW_SCORES_STMT,
  QUERY_NODE_SHOW_TABLE_TAGS_STMT,
  QUERY_NODE_KILL_CONNECTION_STMT,
  QUERY_NODE_KILL_QUERY_STMT,
  QUERY_NODE_KILL_TRANSACTION_STMT,
  QUERY_NODE_DELETE_STMT,
  QUERY_NODE_INSERT_STMT,
  QUERY_NODE_QUERY,
  QUERY_NODE_SHOW_DB_ALIVE_STMT,
  QUERY_NODE_SHOW_CLUSTER_ALIVE_STMT,
  QUERY_NODE_BALANCE_VGROUP_LEADER_STMT,
  QUERY_NODE_RESTORE_DNODE_STMT,
  QUERY_NODE_RESTORE_QNODE_STMT,
  QUERY_NODE_RESTORE_MNODE_STMT,
  QUERY_NODE_RESTORE_VNODE_STMT,
  QUERY_NODE_PAUSE_STREAM_STMT,
  QUERY_NODE_RESUME_STREAM_STMT,

  // logic plan node
  QUERY_NODE_LOGIC_PLAN_SCAN = 1000,
  QUERY_NODE_LOGIC_PLAN_JOIN,
  QUERY_NODE_LOGIC_PLAN_AGG,
  QUERY_NODE_LOGIC_PLAN_PROJECT,
  QUERY_NODE_LOGIC_PLAN_VNODE_MODIFY,
  QUERY_NODE_LOGIC_PLAN_EXCHANGE,
  QUERY_NODE_LOGIC_PLAN_MERGE,
  QUERY_NODE_LOGIC_PLAN_WINDOW,
  QUERY_NODE_LOGIC_PLAN_FILL,
  QUERY_NODE_LOGIC_PLAN_SORT,
  QUERY_NODE_LOGIC_PLAN_PARTITION,
  QUERY_NODE_LOGIC_PLAN_INDEF_ROWS_FUNC,
  QUERY_NODE_LOGIC_PLAN_INTERP_FUNC,
  QUERY_NODE_LOGIC_SUBPLAN,
  QUERY_NODE_LOGIC_PLAN,

  // physical plan node
  QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN = 1100,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_PROJECT,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
  QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
  QUERY_NODE_PHYSICAL_PLAN_MERGE,
  QUERY_NODE_PHYSICAL_PLAN_SORT,
  QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT,
  QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_FILL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE,
  QUERY_NODE_PHYSICAL_PLAN_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_DISPATCH,
  QUERY_NODE_PHYSICAL_PLAN_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_DELETE,
  QUERY_NODE_PHYSICAL_SUBPLAN,
  QUERY_NODE_PHYSICAL_PLAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT
} ENodeType;

/**
 * The first field of a node of any type is guaranteed to be the ENodeType.
 * Hence the type of any node can be gotten by casting it to SNode.
 */
typedef struct SNode {
  ENodeType type;
} SNode;

typedef struct SListCell {
  struct SListCell* pPrev;
  struct SListCell* pNext;
  SNode*            pNode;
} SListCell;

typedef struct SNodeList {
  int32_t    length;
  SListCell* pHead;
  SListCell* pTail;
} SNodeList;

typedef struct SNodeAllocator SNodeAllocator;

int32_t nodesInitAllocatorSet();
void    nodesDestroyAllocatorSet();
int32_t nodesCreateAllocator(int64_t queryId, int32_t chunkSize, int64_t* pAllocatorId);
int32_t nodesAcquireAllocator(int64_t allocatorId);
int32_t nodesReleaseAllocator(int64_t allocatorId);
int64_t nodesMakeAllocatorWeakRef(int64_t allocatorId);
int64_t nodesReleaseAllocatorWeakRef(int64_t allocatorId);
void    nodesDestroyAllocator(int64_t allocatorId);

SNode* nodesMakeNode(ENodeType type);
void   nodesDestroyNode(SNode* pNode);
void   nodesFree(void* p);

SNodeList* nodesMakeList();
int32_t    nodesListAppend(SNodeList* pList, SNode* pNode);
int32_t    nodesListStrictAppend(SNodeList* pList, SNode* pNode);
int32_t    nodesListMakeAppend(SNodeList** pList, SNode* pNode);
int32_t    nodesListMakeStrictAppend(SNodeList** pList, SNode* pNode);
int32_t    nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc);
int32_t    nodesListStrictAppendList(SNodeList* pTarget, SNodeList* pSrc);
int32_t    nodesListPushFront(SNodeList* pList, SNode* pNode);
SListCell* nodesListErase(SNodeList* pList, SListCell* pCell);
void       nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc);
SNode*     nodesListGetNode(SNodeList* pList, int32_t index);
SListCell* nodesListGetCell(SNodeList* pList, int32_t index);
void       nodesDestroyList(SNodeList* pList);
// Only clear the linked list structure, without releasing the elements inside
void nodesClearList(SNodeList* pList);

typedef enum EDealRes { DEAL_RES_CONTINUE = 1, DEAL_RES_IGNORE_CHILD, DEAL_RES_ERROR, DEAL_RES_END } EDealRes;

typedef EDealRes (*FNodeWalker)(SNode* pNode, void* pContext);
void nodesWalkExpr(SNode* pNode, FNodeWalker walker, void* pContext);
void nodesWalkExprs(SNodeList* pList, FNodeWalker walker, void* pContext);
void nodesWalkExprPostOrder(SNode* pNode, FNodeWalker walker, void* pContext);
void nodesWalkExprsPostOrder(SNodeList* pList, FNodeWalker walker, void* pContext);

typedef EDealRes (*FNodeRewriter)(SNode** pNode, void* pContext);
void nodesRewriteExpr(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprs(SNodeList* pList, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprPostOrder(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprsPostOrder(SNodeList* pList, FNodeRewriter rewriter, void* pContext);

bool nodesEqualNode(const SNode* a, const SNode* b);

SNode*     nodesCloneNode(const SNode* pNode);
SNodeList* nodesCloneList(const SNodeList* pList);

const char* nodesNodeName(ENodeType type);
int32_t     nodesNodeToString(const SNode* pNode, bool format, char** pStr, int32_t* pLen);
int32_t     nodesStringToNode(const char* pStr, SNode** pNode);

int32_t nodesListToString(const SNodeList* pList, bool format, char** pStr, int32_t* pLen);
int32_t nodesStringToList(const char* pStr, SNodeList** pList);

int32_t nodesNodeToMsg(const SNode* pNode, char** pMsg, int32_t* pLen);
int32_t nodesMsgToNode(const char* pStr, int32_t len, SNode** pNode);

int32_t nodesNodeToSQL(SNode* pNode, char* buf, int32_t bufSize, int32_t* len);
char*   nodesGetNameFromColumnNode(SNode* pNode);
int32_t nodesGetOutputNumFromSlotList(SNodeList* pSlots);

#ifdef __cplusplus
}
#endif

#endif /*_TD_NODES_H_*/
