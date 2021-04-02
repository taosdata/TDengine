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
#include <stdlib.h>
#include <string.h>

#include "tlist.h"

SList *tdListNew(int eleSize) {
  SList *list = (SList *)malloc(sizeof(SList));
  if (list == NULL) return NULL;

  list->eleSize = eleSize;
  list->numOfEles = 0;
  list->head = list->tail = NULL;
  return list;
}

void tdListEmpty(SList *list) {
  SListNode *node = list->head;
  while (node) {
    list->head = node->next;
    free(node);
    node = list->head;
  }
  list->head = list->tail = 0;
  list->numOfEles = 0;
}

void *tdListFree(SList *list) {
  if (list) {
    tdListEmpty(list);
    free(list);
  }

  return NULL;
}

void tdListPrependNode(SList *list, SListNode *node) {
  if (list->head == NULL) {
    list->head = node;
    list->tail = node;
  } else {
    node->next = list->head;
    node->prev = NULL;
    list->head->prev = node;
    list->head = node;
  }
  list->numOfEles++;
}

void tdListAppendNode(SList *list, SListNode *node) {
  if (list->head == NULL) {
    list->head = node;
    list->tail = node;
  } else {
    node->prev = list->tail;
    node->next = NULL;
    list->tail->next = node;
    list->tail = node;
  }

  list->numOfEles++;
}

int tdListPrepend(SList *list, void *data) {
  SListNode *node = (SListNode *)malloc(sizeof(SListNode) + list->eleSize);
  if (node == NULL) return -1;

  node->next = node->prev = NULL;
  memcpy((void *)(node->data), data, list->eleSize);
  tdListPrependNode(list, node);

  return 0;
}

int tdListAppend(SList *list, void *data) {
  SListNode *node = (SListNode *)calloc(1, sizeof(SListNode) + list->eleSize);
  if (node == NULL) return -1;

  memcpy((void *)(node->data), data, list->eleSize);
  tdListAppendNode(list, node);

  return 0;
}

SListNode *tdListPopHead(SList *list) {
  if (list->head == NULL) return NULL;
  SListNode *node = list->head;
  if (node->next == NULL) {
    list->head = NULL;
    list->tail = NULL;
  } else {
    list->head = node->next;
  }
  list->numOfEles--;
  node->next = NULL;
  node->prev = NULL;
  return node;
}

SListNode *tdListPopTail(SList *list) {
  if (list->tail == NULL) return NULL;
  SListNode *node = list->tail;
  if (node->prev == NULL) {
    list->head = NULL;
    list->tail = NULL;
  } else {
    list->tail = node->prev;
  }
  list->numOfEles--;
  node->next = node->prev = NULL;
  return node;
}

SListNode *tdListGetHead(SList *list) {
  if (list == NULL || list->numOfEles == 0) {
    return NULL;
  }

  return list->head;
}

SListNode *tsListGetTail(SList *list) {
  if (list == NULL || list->numOfEles == 0) {
    return NULL;
  }

  return list->tail;
}

SListNode *tdListPopNode(SList *list, SListNode *node) {
  if (list->head == node) {
    list->head = node->next;
  }
  if (list->tail == node) {
    list->tail = node->prev;
  }

  if (node->prev != NULL) {
    node->prev->next = node->next;
  }
  if (node->next != NULL) {
    node->next->prev = node->prev;
  }
  list->numOfEles--;
  node->next = node->prev = NULL;

  return node;
}

// Move all node elements from src to dst, the dst is assumed as an empty list
void tdListMove(SList *src, SList *dst) {
  // assert(dst->eleSize == src->eleSize);
  SListNode *node = NULL;
  while ((node = tdListPopHead(src)) != NULL) {
    tdListAppendNode(dst, node);
  }
}

void tdListDiscard(SList *list) {
  if (list) {
    list->head = list->tail = NULL;
    list->numOfEles = 0;
  }
}

void tdListNodeGetData(SList *list, SListNode *node, void *target) { memcpy(target, node->data, list->eleSize); }

void tdListInitIter(SList *list, SListIter *pIter, TD_LIST_DIRECTION_T direction) {
  pIter->direction = direction;
  if (direction == TD_LIST_FORWARD) {
    pIter->next = list->head;
  } else {
    pIter->next = list->tail;
  }
}

SListNode *tdListNext(SListIter *pIter) {
  SListNode *node = pIter->next;
  if (node == NULL) return NULL;
  if (pIter->direction == TD_LIST_FORWARD) {
    pIter->next = node->next;
  } else {
    pIter->next = node->prev;
  }

  return node;
}