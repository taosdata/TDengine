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

#define _DEFAULT_SOURCE
#include "tlist.h"

void tdListInit(SList *list, int32_t eleSize) {
  TD_DLIST_INIT(list);
  listEleSize(list) = eleSize;
}

SList *tdListNew(int32_t eleSize) {
  SList *list = (SList *)taosMemoryMalloc(sizeof(SList));
  if (list == NULL) return NULL;

  tdListInit(list, eleSize);
  return list;
}

void tdListEmpty(SList *list) {
  SListNode *node;
  while ((node = TD_DLIST_HEAD(list)) != NULL) {
    TD_DLIST_POP(list, node);
    taosMemoryFree(node);
  }
}

void *tdListFree(SList *list) {
  if (list) {
    tdListEmpty(list);
    taosMemoryFree(list);
  }

  return NULL;
}

void tdListEmptyP(SList *list, FDelete fp) {
  SListNode *node;
  while ((node = TD_DLIST_HEAD(list)) != NULL) {
    TD_DLIST_POP(list, node);
    fp(node->data);
    taosMemoryFree(node);
  }
}

void *tdListFreeP(SList *list, FDelete fp) {
  if (list) {
    tdListEmptyP(list, fp);
    taosMemoryFree(list);
  }

  return NULL;
}

void tdListPrependNode(SList *list, SListNode *node) { TD_DLIST_PREPEND(list, node); }

void tdListAppendNode(SList *list, SListNode *node) { TD_DLIST_APPEND(list, node); }

int32_t tdListPrepend(SList *list, void *data) {
  SListNode *node = (SListNode *)taosMemoryMalloc(sizeof(SListNode) + list->eleSize);
  if (node == NULL) return -1;

  memcpy((void *)(node->data), data, list->eleSize);
  TD_DLIST_PREPEND(list, node);

  return 0;
}

int32_t tdListAppend(SList *list, const void *data) {
  SListNode *node = (SListNode *)taosMemoryCalloc(1, sizeof(SListNode) + list->eleSize);
  if (node == NULL) return -1;

  memcpy((void *)(node->data), data, list->eleSize);
  TD_DLIST_APPEND(list, node);

  return 0;
}
// return the node pointer
SListNode *tdListAdd(SList *list, const void *data) {
  SListNode *node = (SListNode *)taosMemoryCalloc(1, sizeof(SListNode) + list->eleSize);
  if (node == NULL) return NULL;

  memcpy((void *)(node->data), data, list->eleSize);
  TD_DLIST_APPEND(list, node);
  return node;
}

SListNode *tdListPopHead(SList *list) {
  SListNode *node;

  node = TD_DLIST_HEAD(list);

  if (node) {
    TD_DLIST_POP(list, node);
  }

  return node;
}

SListNode *tdListPopTail(SList *list) {
  SListNode *node;

  node = TD_DLIST_TAIL(list);
  if (node) {
    TD_DLIST_POP(list, node);
  }

  return node;
}

SListNode *tdListGetHead(SList *list) { return TD_DLIST_HEAD(list); }

SListNode *tdListGetTail(SList *list) { return TD_DLIST_TAIL(list); }

SListNode *tdListPopNode(SList *list, SListNode *node) {
  TD_DLIST_POP(list, node);
  return node;
}

// Move all node elements from src to dst, the dst is assumed as an empty list
void tdListMove(SList *src, SList *dst) {
  SListNode *node = NULL;
  while ((node = tdListPopHead(src)) != NULL) {
    tdListAppendNode(dst, node);
  }
}

void tdListDiscard(SList *list) {
  if (list) {
    listHead(list) = listTail(list) = NULL;
    listNEles(list) = 0;
  }
}

void tdListNodeGetData(SList *list, SListNode *node, void *target) { memcpy(target, node->data, listEleSize(list)); }

void tdListInitIter(SList *list, SListIter *pIter, TD_LIST_DIRECTION_T direction) {
  pIter->direction = direction;
  if (direction == TD_LIST_FORWARD) {
    pIter->next = listHead(list);
  } else {
    pIter->next = listTail(list);
  }
}

SListNode *tdListNext(SListIter *pIter) {
  SListNode *node = pIter->next;
  if (node == NULL) return NULL;
  if (pIter->direction == TD_LIST_FORWARD) {
    pIter->next = TD_DLIST_NODE_NEXT(node);
  } else {
    pIter->next = TD_DLIST_NODE_PREV(node);
  }

  return node;
}