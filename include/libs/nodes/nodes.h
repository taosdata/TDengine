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

// only be use in FOREACH
#define ERASE_NODE(list) cell = nodesListErase(list, cell);

#define REPLACE_NODE(newNode) cell->pNode = (SNode*)(newNode)

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
  QUERY_NODE_COLUMN_REF,

  // Only be used in parser module.
  QUERY_NODE_RAW_EXPR,

  // Statement nodes are used in parser and planner module.
  QUERY_NODE_SET_OPERATOR,
  QUERY_NODE_SELECT_STMT,
  QUERY_NODE_SHOW_STMT,

  QUERY_NODE_LOGIC_PLAN_SCAN,
  QUERY_NODE_LOGIC_PLAN_JOIN,
  QUERY_NODE_LOGIC_PLAN_FILTER,
  QUERY_NODE_LOGIC_PLAN_AGG,
  QUERY_NODE_LOGIC_PLAN_PROJECT
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
  int16_t length;
  SListCell* pHead;
  SListCell* pTail;
} SNodeList;

SNode* nodesMakeNode(ENodeType type);
void nodesDestroyNode(SNode* pNode);

SNodeList* nodesMakeList();
int32_t nodesListAppend(SNodeList* pList, SNode* pNode);
SListCell* nodesListErase(SNodeList* pList, SListCell* pCell);
SNode* nodesListGetNode(SNodeList* pList, int32_t index);
void nodesDestroyList(SNodeList* pList);

typedef enum EDealRes {
  DEAL_RES_CONTINUE = 1,
  DEAL_RES_IGNORE_CHILD,
  DEAL_RES_ERROR,
} EDealRes;

typedef EDealRes (*FNodeWalker)(SNode* pNode, void* pContext);
void nodesWalkNode(SNode* pNode, FNodeWalker walker, void* pContext);
void nodesWalkList(SNodeList* pList, FNodeWalker walker, void* pContext);
void nodesWalkNodePostOrder(SNode* pNode, FNodeWalker walker, void* pContext);
void nodesWalkListPostOrder(SNodeList* pList, FNodeWalker walker, void* pContext);

typedef EDealRes (*FNodeRewriter)(SNode** pNode, void* pContext);
void nodesRewriteNode(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteList(SNodeList* pList, FNodeRewriter rewriter, void* pContext);
void nodesRewriteNodePostOrder(SNode** pNode, FNodeRewriter rewriter, void* pContext);
void nodesRewriteListPostOrder(SNodeList* pList, FNodeRewriter rewriter, void* pContext);

bool nodesEqualNode(const SNode* a, const SNode* b);

SNode* nodesCloneNode(const SNode* pNode);
SNodeList* nodesCloneList(const SNodeList* pList);

int32_t nodesNodeToString(const SNode* pNode, char** pStr, int32_t* pLen);
int32_t nodesStringToNode(const char* pStr, SNode** pNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_NODES_H_*/
