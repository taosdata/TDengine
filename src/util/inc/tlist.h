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
#ifndef _TD_LIST_
#define _TD_LIST_

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { TD_LIST_FORWARD, TD_LIST_BACKWARD } TD_LIST_DIRECTION_T;

typedef struct _list_node {
  struct _list_node *next;
  struct _list_node *prev;
  char               data[];
} SListNode;

typedef struct {
  struct _list_node *head;
  struct _list_node *tail;
  int                numOfEles;
  int                eleSize;
} SList;

typedef struct {
  SListNode *         next;
  TD_LIST_DIRECTION_T direction;
} SListIter;

#define listHead(l) (l)->head
#define listTail(l) (l)->tail
#define listNEles(l) (l)->numOfEles
#define listEleSize(l) (l)->eleSize
#define isListEmpty(l) ((l)->numOfEles == 0)
#define listNodeFree(n) free(n);

SList *    tdListNew(int eleSize);
void *     tdListFree(SList *list);
void       tdListEmpty(SList *list);
void       tdListPrependNode(SList *list, SListNode *node);
void       tdListAppendNode(SList *list, SListNode *node);
int        tdListPrepend(SList *list, void *data);
int        tdListAppend(SList *list, void *data);
SListNode *tdListPopHead(SList *list);
SListNode *tdListPopTail(SList *list);
SListNode *tdListGetHead(SList *list);
SListNode *tsListGetTail(SList *list);
SListNode *tdListPopNode(SList *list, SListNode *node);
void       tdListMove(SList *src, SList *dst);
void       tdListDiscard(SList *list);

void       tdListNodeGetData(SList *list, SListNode *node, void *target);
void       tdListInitIter(SList *list, SListIter *pIter, TD_LIST_DIRECTION_T direction);
SListNode *tdListNext(SListIter *pIter);

#ifdef __cplusplus
}
#endif

#endif