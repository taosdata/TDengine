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

#define nodeType(nodeptr) (((const SNode*)(nodeptr))->type)
#define setNodeType(nodeptr, type) (((SNode*)(nodeptr))->type = (type))

#define LIST_LENGTH(l) (NULL != (l) ? (l)->length : 0)

#define FOREACH(node, list)	\
  for (SListCell* cell = (NULL != (list) ? (list)->pHead : NULL); (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false)); cell = cell->pNext)

#define REPLACE_NODE(newNode) cell->pNode = (SNode*)(newNode)

#define INSERT_LIST(target, src) nodesListInsertList((target), cell, src)

#define WHERE_EACH(node, list) \
  SListCell* cell = (NULL != (list) ? (list)->pHead : NULL); \
  while (NULL != cell ? (node = cell->pNode, true) : (node = NULL, false))

#define WHERE_NEXT cell = cell->pNext

// only be use in WHERE_EACH
#define ERASE_NODE(list) cell = nodesListErase((list), cell)

#define FORBOTH(node1, list1, node2, list2) \
  for (SListCell* cell1 = (NULL != (list1) ? (list1)->pHead : NULL), *cell2 = (NULL != (list2) ? (list2)->pHead : NULL); \
    (NULL == cell1 ? (node1 = NULL, false) : (node1 = cell1->pNode, true)), (NULL == cell2 ? (node2 = NULL, false) : (node2 = cell2->pNode, true)), (node1 != NULL && node2 != NULL); \
    cell1 = cell1->pNext, cell2 = cell2->pNext)

#define FOREACH_FOR_REWRITE(node, list)	\
  for (SListCell* cell = (NULL != (list) ? (list)->pHead : NULL); (NULL != cell ? (node = &(cell->pNode), true) : (node = NULL, false)); cell = cell->pNext)

typedef enum ENodeType {
  // Syntax nodes are used in parser and planner module, and some are also used in executor module, such as COLUMN, VALUE, OPERATOR, FUNCTION and so on.
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
  QUERY_NODE_RAW_EXPR, // Only be used in parser module.
  QUERY_NODE_TARGET,
  QUERY_NODE_DATABLOCK_DESC,
  QUERY_NODE_SLOT_DESC,
  QUERY_NODE_COLUMN_DEF,
  QUERY_NODE_DOWNSTREAM_SOURCE,
  QUERY_NODE_DATABASE_OPTIONS,
  QUERY_NODE_TABLE_OPTIONS,
  QUERY_NODE_INDEX_OPTIONS,
  QUERY_NODE_EXPLAIN_OPTIONS,

  // Statement nodes are used in parser and planner module.
  QUERY_NODE_SET_OPERATOR,
  QUERY_NODE_SELECT_STMT,
  QUERY_NODE_VNODE_MODIF_STMT,
  QUERY_NODE_CREATE_DATABASE_STMT,
  QUERY_NODE_DROP_DATABASE_STMT,
  QUERY_NODE_ALTER_DATABASE_STMT,
  QUERY_NODE_CREATE_TABLE_STMT,
  QUERY_NODE_CREATE_SUBTABLE_CLAUSE,
  QUERY_NODE_CREATE_MULTI_TABLE_STMT,
  QUERY_NODE_DROP_TABLE_CLAUSE,
  QUERY_NODE_DROP_TABLE_STMT,
  QUERY_NODE_DROP_SUPER_TABLE_STMT,
  QUERY_NODE_ALTER_TABLE_STMT,
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
  QUERY_NODE_CREATE_TOPIC_STMT,
  QUERY_NODE_DROP_TOPIC_STMT,
  QUERY_NODE_ALTER_LOCAL_STMT,
  QUERY_NODE_EXPLAIN_STMT,
  QUERY_NODE_DESCRIBE_STMT,
  QUERY_NODE_RESET_QUERY_CACHE_STMT,
  QUERY_NODE_COMPACT_STMT,
  QUERY_NODE_CREATE_FUNCTION_STMT,
  QUERY_NODE_DROP_FUNCTION_STMT,
  QUERY_NODE_CREATE_STREAM_STMT,
  QUERY_NODE_DROP_STREAM_STMT,
  QUERY_NODE_MERGE_VGROUP_STMT,
  QUERY_NODE_REDISTRIBUTE_VGROUP_STMT,
  QUERY_NODE_SPLIT_VGROUP_STMT,
  QUERY_NODE_SYNCDB_STMT,
  QUERY_NODE_SHOW_DATABASES_STMT,
  QUERY_NODE_SHOW_TABLES_STMT,
  QUERY_NODE_SHOW_STABLES_STMT,
  QUERY_NODE_SHOW_USERS_STMT,
  QUERY_NODE_SHOW_DNODES_STMT,
  QUERY_NODE_SHOW_VGROUPS_STMT,
  QUERY_NODE_SHOW_MNODES_STMT,
  QUERY_NODE_SHOW_MODULES_STMT,
  QUERY_NODE_SHOW_QNODES_STMT,
  QUERY_NODE_SHOW_FUNCTIONS_STMT,
  QUERY_NODE_SHOW_INDEXES_STMT,
  QUERY_NODE_SHOW_STREAMS_STMT,
  QUERY_NODE_SHOW_APPS_STMT,
  QUERY_NODE_SHOW_CONNECTIONS_STMT,
  QUERY_NODE_SHOW_LICENCE_STMT,
  QUERY_NODE_SHOW_CREATE_DATABASE_STMT,
  QUERY_NODE_SHOW_CREATE_TABLE_STMT,
  QUERY_NODE_SHOW_CREATE_STABLE_STMT, 
  QUERY_NODE_SHOW_QUERIES_STMT,
  QUERY_NODE_SHOW_SCORES_STMT,
  QUERY_NODE_SHOW_TOPICS_STMT,
  QUERY_NODE_SHOW_VARIABLE_STMT,
  QUERY_NODE_KILL_CONNECTION_STMT,
  QUERY_NODE_KILL_QUERY_STMT,

  // logic plan node
  QUERY_NODE_LOGIC_PLAN_SCAN,
  QUERY_NODE_LOGIC_PLAN_JOIN,
  QUERY_NODE_LOGIC_PLAN_AGG,
  QUERY_NODE_LOGIC_PLAN_PROJECT,
  QUERY_NODE_LOGIC_PLAN_VNODE_MODIF,
  QUERY_NODE_LOGIC_PLAN_EXCHANGE,
  QUERY_NODE_LOGIC_PLAN_WINDOW,
  QUERY_NODE_LOGIC_PLAN_SORT,
  QUERY_NODE_LOGIC_PLAN_PARTITION,
  QUERY_NODE_LOGIC_SUBPLAN,
  QUERY_NODE_LOGIC_PLAN,

  // physical plan node
  QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_PROJECT,
  QUERY_NODE_PHYSICAL_PLAN_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_AGG,
  QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
  QUERY_NODE_PHYSICAL_PLAN_SORT,
  QUERY_NODE_PHYSICAL_PLAN_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_SESSION_WINDOW,
  QUERY_NODE_PHYSICAL_PLAN_STATE_WINDOW,
  QUERY_NODE_PHYSICAL_PLAN_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_DISPATCH,
  QUERY_NODE_PHYSICAL_PLAN_INSERT,
  QUERY_NODE_PHYSICAL_SUBPLAN,
  QUERY_NODE_PHYSICAL_PLAN
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
  SNode* pNode;
} SListCell;

typedef struct SNodeList {
  int32_t length;
  SListCell* pHead;
  SListCell* pTail;
} SNodeList;

#define SNodeptr void*  

SNodeptr nodesMakeNode(ENodeType type);
void nodesDestroyNode(SNodeptr pNode);

SNodeList* nodesMakeList();
int32_t nodesListAppend(SNodeList* pList, SNodeptr pNode);
int32_t nodesListStrictAppend(SNodeList* pList, SNodeptr pNode);
int32_t nodesListMakeAppend(SNodeList** pList, SNodeptr pNode);
int32_t nodesListAppendList(SNodeList* pTarget, SNodeList* pSrc);
int32_t nodesListStrictAppendList(SNodeList* pTarget, SNodeList* pSrc);
int32_t nodesListPushFront(SNodeList* pList, SNodeptr pNode);
SListCell* nodesListErase(SNodeList* pList, SListCell* pCell);
void nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc);
SNodeptr nodesListGetNode(SNodeList* pList, int32_t index);
void nodesDestroyList(SNodeList* pList);
// Only clear the linked list structure, without releasing the elements inside
void nodesClearList(SNodeList* pList);

typedef enum EDealRes {
  DEAL_RES_CONTINUE = 1,
  DEAL_RES_IGNORE_CHILD,
  DEAL_RES_ERROR,
} EDealRes;

typedef EDealRes (*FNodeWalker)(SNode* pNode, void* pContext);
void nodesWalkExpr(SNodeptr pNode, FNodeWalker walker, void* pContext);
void nodesWalkExprs(SNodeList* pList, FNodeWalker walker, void* pContext);
void nodesWalkExprPostOrder(SNodeptr pNode, FNodeWalker walker, void* pContext);
void nodesWalkExprsPostOrder(SNodeList* pList, FNodeWalker walker, void* pContext);

typedef EDealRes (*FNodeRewriter)(SNode** pNode, void* pContext);
void nodesRewriteExpr(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprs(SNodeList* pList, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprPostOrder(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteExprsPostOrder(SNodeList* pList, FNodeRewriter rewriter, void* pContext);

bool nodesEqualNode(const SNodeptr a, const SNodeptr b);

SNodeptr nodesCloneNode(const SNodeptr pNode);
SNodeList* nodesCloneList(const SNodeList* pList);

const char* nodesNodeName(ENodeType type);
int32_t nodesNodeToString(const SNodeptr pNode, bool format, char** pStr, int32_t* pLen);
int32_t nodesStringToNode(const char* pStr, SNode** pNode);

int32_t nodesListToString(const SNodeList* pList, bool format, char** pStr, int32_t* pLen);
int32_t nodesStringToList(const char* pStr, SNodeList** pList);

int32_t nodesNodeToSQL(SNode *pNode, char *buf, int32_t bufSize, int32_t *len);
char *nodesGetNameFromColumnNode(SNode *pNode);
int32_t nodesGetOutputNumFromSlotList(SNodeList* pSlots);

#ifdef __cplusplus
}
#endif

#endif /*_TD_NODES_H_*/
