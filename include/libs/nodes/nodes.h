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
#include "tmsg.h"

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
int32_t    nodesListMakeStrictAppendList(SNodeList** pTarget, SNodeList* pSrc);
int32_t    nodesListPushFront(SNodeList* pList, SNode* pNode);
SListCell* nodesListErase(SNodeList* pList, SListCell* pCell);
void       nodesListInsertList(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc);
void       nodesListInsertListAfterPos(SNodeList* pTarget, SListCell* pPos, SNodeList* pSrc);
SNode*     nodesListGetNode(SNodeList* pList, int32_t index);
SListCell* nodesListGetCell(SNodeList* pList, int32_t index);
void       nodesDestroyList(SNodeList* pList);
bool       nodesListMatch(const SNodeList* pList, const SNodeList* pSubList);

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
bool nodeListNodeEqual(const SNodeList* a, const SNode* b);

bool nodesMatchNode(const SNode* pSub, const SNode* pNode);

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
