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

#ifndef _TD_UTIL_LIST_H_
#define _TD_UTIL_LIST_H_

#include "os.h"
#include "talgo.h"

#ifdef __cplusplus
extern "C" {
#endif

// Single linked list ================
#define TD_SLIST_NODE(TYPE) \
  struct {                  \
    struct TYPE *sl_next_;  \
  }

#define TD_SLIST(TYPE)      \
  struct {                  \
    struct TYPE *sl_head_;  \
    int32_t      sl_neles_; \
  }

#define TD_SLIST_HEAD(sl)                         ((sl)->sl_head_)
#define TD_SLIST_NELES(sl)                        ((sl)->sl_neles_)
#define TD_SLIST_NODE_NEXT(sln)                   ((sln)->sl_next_)
#define TD_SLIST_NODE_NEXT_WITH_FIELD(sln, field) ((sln)->field.sl_next_)

#define TD_SLIST_INIT(sl)  \
  do {                     \
    (sl)->sl_head_ = NULL; \
    (sl)->sl_neles_ = 0;   \
  } while (0)

#define TD_SLIST_PUSH(sl, sln)                   \
  do {                                           \
    TD_SLIST_NODE_NEXT(sln) = TD_SLIST_HEAD(sl); \
    TD_SLIST_HEAD(sl) = (sln);                   \
    TD_SLIST_NELES(sl) += 1;                     \
  } while (0)

#define TD_SLIST_PUSH_WITH_FIELD(sl, sln, field)                   \
  do {                                                             \
    TD_SLIST_NODE_NEXT_WITH_FIELD(sln, field) = TD_SLIST_HEAD(sl); \
    TD_SLIST_HEAD(sl) = (sln);                                     \
    TD_SLIST_NELES(sl) += 1;                                       \
  } while (0)

#define TD_SLIST_POP(sl)                                       \
  do {                                                         \
    TD_SLIST_HEAD(sl) = TD_SLIST_NODE_NEXT(TD_SLIST_HEAD(sl)); \
    TD_SLIST_NELES(sl) -= 1;                                   \
  } while (0)

#define TD_SLIST_POP_WITH_FIELD(sl, field)                                       \
  do {                                                                           \
    TD_SLIST_HEAD(sl) = TD_SLIST_NODE_NEXT_WITH_FIELD(TD_SLIST_HEAD(sl), field); \
    TD_SLIST_NELES(sl) -= 1;                                                     \
  } while (0)

// Double linked list ================
#define TD_DLIST_NODE(TYPE) \
  struct {                  \
    struct TYPE *dl_prev_;  \
    struct TYPE *dl_next_;  \
  }

#define TD_DLIST(TYPE)      \
  struct {                  \
    struct TYPE *dl_head_;  \
    struct TYPE *dl_tail_;  \
    int32_t      dl_neles_; \
  }

#define TD_DLIST_NODE_PREV(dln)                   ((dln)->dl_prev_)
#define TD_DLIST_NODE_NEXT(dln)                   ((dln)->dl_next_)
#define TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) ((dln)->field.dl_prev_)
#define TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) ((dln)->field.dl_next_)
#define TD_DLIST_HEAD(dl)                         ((dl)->dl_head_)
#define TD_DLIST_TAIL(dl)                         ((dl)->dl_tail_)
#define TD_DLIST_NELES(dl)                        ((dl)->dl_neles_)

#define TD_DLIST_INIT(dl)                         \
  do {                                            \
    TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = NULL; \
    TD_DLIST_NELES(dl) = 0;                       \
  } while (0)

#define TD_DLIST_APPEND(dl, dln)                                \
  do {                                                          \
    if (TD_DLIST_HEAD(dl) == NULL) {                            \
      TD_DLIST_NODE_PREV(dln) = TD_DLIST_NODE_NEXT(dln) = NULL; \
      TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = (dln);            \
    } else {                                                    \
      TD_DLIST_NODE_PREV(dln) = TD_DLIST_TAIL(dl);              \
      TD_DLIST_NODE_NEXT(dln) = NULL;                           \
      TD_DLIST_NODE_NEXT(TD_DLIST_TAIL(dl)) = (dln);            \
      TD_DLIST_TAIL(dl) = (dln);                                \
    }                                                           \
    TD_DLIST_NELES(dl) += 1;                                    \
  } while (0)

#define TD_DLIST_APPEND_WITH_FIELD(dl, dln, field)                                                  \
  do {                                                                                              \
    if (TD_DLIST_HEAD(dl) == NULL) {                                                                \
      TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) = TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) = NULL; \
      TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = (dln);                                                \
    } else {                                                                                        \
      TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) = TD_DLIST_TAIL(dl);                                \
      TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) = NULL;                                             \
      TD_DLIST_NODE_NEXT_WITH_FIELD(TD_DLIST_TAIL(dl), field) = (dln);                              \
      TD_DLIST_TAIL(dl) = (dln);                                                                    \
    }                                                                                               \
    TD_DLIST_NELES(dl) += 1;                                                                        \
  } while (0)

#define TD_DLIST_PREPEND(dl, dln)                               \
  do {                                                          \
    if (TD_DLIST_HEAD(dl) == NULL) {                            \
      TD_DLIST_NODE_PREV(dln) = TD_DLIST_NODE_NEXT(dln) = NULL; \
      TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = (dln);            \
    } else {                                                    \
      TD_DLIST_NODE_PREV(dln) = NULL;                           \
      TD_DLIST_NODE_NEXT(dln) = TD_DLIST_HEAD(dl);              \
      TD_DLIST_NODE_PREV(TD_DLIST_HEAD(dl)) = (dln);            \
      TD_DLIST_HEAD(dl) = (dln);                                \
    }                                                           \
    TD_DLIST_NELES(dl) += 1;                                    \
  } while (0)

#define TD_DLIST_PREPEND_WITH_FIELD(dl, dln, field)                                                 \
  do {                                                                                              \
    if (TD_DLIST_HEAD(dl) == NULL) {                                                                \
      TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) = TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) = NULL; \
      TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = (dln);                                                \
    } else {                                                                                        \
      TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) = NULL;                                             \
      TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) = TD_DLIST_HEAD(dl);                                \
      TD_DLIST_NODE_PREV_WITH_FIELD(TD_DLIST_HEAD(dl), field) = (dln);                              \
      TD_DLIST_HEAD(dl) = (dln);                                                                    \
    }                                                                                               \
    TD_DLIST_NELES(dl) += 1;                                                                        \
  } while (0)

#define TD_DLIST_POP(dl, dln)                                                \
  do {                                                                       \
    if (TD_DLIST_HEAD(dl) == (dln)) {                                        \
      TD_DLIST_HEAD(dl) = TD_DLIST_NODE_NEXT(dln);                           \
    }                                                                        \
    if (TD_DLIST_TAIL(dl) == (dln)) {                                        \
      TD_DLIST_TAIL(dl) = TD_DLIST_NODE_PREV(dln);                           \
    }                                                                        \
    if (TD_DLIST_NODE_PREV(dln) != NULL) {                                   \
      TD_DLIST_NODE_NEXT(TD_DLIST_NODE_PREV(dln)) = TD_DLIST_NODE_NEXT(dln); \
    }                                                                        \
    if (TD_DLIST_NODE_NEXT(dln) != NULL) {                                   \
      TD_DLIST_NODE_PREV(TD_DLIST_NODE_NEXT(dln)) = TD_DLIST_NODE_PREV(dln); \
    }                                                                        \
    TD_DLIST_NELES(dl) -= 1;                                                 \
    TD_DLIST_NODE_PREV(dln) = TD_DLIST_NODE_NEXT(dln) = NULL;                \
  } while (0)

#define TD_DLIST_POP_WITH_FIELD(dl, dln, field)                                                   \
  do {                                                                                            \
    if (TD_DLIST_HEAD(dl) == (dln)) {                                                             \
      TD_DLIST_HEAD(dl) = TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field);                              \
    }                                                                                             \
    if (TD_DLIST_TAIL(dl) == (dln)) {                                                             \
      TD_DLIST_TAIL(dl) = TD_DLIST_NODE_PREV_WITH_FIELD(dln, field);                              \
    }                                                                                             \
    if (TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) != NULL) {                                      \
      TD_DLIST_NODE_NEXT_WITH_FIELD(TD_DLIST_NODE_PREV_WITH_FIELD(dln, field), field) =           \
          TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field);                                              \
    }                                                                                             \
    if (TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) != NULL) {                                      \
      TD_DLIST_NODE_PREV_WITH_FIELD(TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field), field) =           \
          TD_DLIST_NODE_PREV_WITH_FIELD(dln, field);                                              \
    }                                                                                             \
    TD_DLIST_NELES(dl) -= 1;                                                                      \
    TD_DLIST_NODE_PREV_WITH_FIELD(dln, field) = TD_DLIST_NODE_NEXT_WITH_FIELD(dln, field) = NULL; \
  } while (0)

// General double linked list
typedef enum { TD_LIST_FORWARD, TD_LIST_BACKWARD } TD_LIST_DIRECTION_T;

typedef struct SListNode {
  TD_DLIST_NODE(SListNode);
  char data[];
} SListNode;

typedef struct {
  TD_DLIST(SListNode);
  int32_t eleSize;
} SList;

typedef struct {
  SListNode          *next;
  TD_LIST_DIRECTION_T direction;
} SListIter;

#define listHead(l)     TD_DLIST_HEAD(l)
#define listTail(l)     TD_DLIST_TAIL(l)
#define listNEles(l)    TD_DLIST_NELES(l)
#define listEleSize(l)  ((l)->eleSize)
#define isListEmpty(l)  (TD_DLIST_NELES(l) == 0)
#define listNodeFree(n) taosMemoryFree(n)

void       tdListInit(SList *list, int32_t eleSize);
void       tdListEmpty(SList *list);
SList     *tdListNew(int32_t eleSize);
void      *tdListFree(SList *list);
void      *tdListFreeP(SList *list, FDelete fp);
void       tdListPrependNode(SList *list, SListNode *node);
void       tdListAppendNode(SList *list, SListNode *node);
int32_t    tdListPrepend(SList *list, void *data);
int32_t    tdListAppend(SList *list, const void *data);
SListNode *tdListAdd(SList *list, const void *data);
SListNode *tdListPopHead(SList *list);
SListNode *tdListPopTail(SList *list);
SListNode *tdListGetHead(SList *list);
SListNode *tdListGetTail(SList *list);
SListNode *tdListPopNode(SList *list, SListNode *node);
void       tdListMove(SList *src, SList *dst);
void       tdListDiscard(SList *list);

void       tdListNodeGetData(SList *list, SListNode *node, void *target);
void       tdListInitIter(SList *list, SListIter *pIter, TD_LIST_DIRECTION_T direction);
SListNode *tdListNext(SListIter *pIter);

// macros ====================================================================================

// q: for queue
// n: for node
// m: for member

#define LISTD(TYPE)    \
  struct {             \
    TYPE *next, *prev; \
  }

#define LISTD_NEXT(n, m)      ((n)->m.next)
#define LISTD_PREV(n, m)      ((n)->m.prev)
#define LISTD_INIT(q, m)      (LISTD_NEXT(q, m) = LISTD_PREV(q, m) = (q))
#define LISTD_HEAD(q, m)      (LISTD_NEXT(q, m))
#define LISTD_TAIL(q, m)      (LISTD_PREV(q, m))
#define LISTD_PREV_NEXT(n, m) (LISTD_NEXT(LISTD_PREV(n, m), m))
#define LISTD_NEXT_PREV(n, m) (LISTD_PREV(LISTD_NEXT(n, m), m))

#define LISTD_INSERT_HEAD(q, n, m)       \
  do {                                   \
    LISTD_NEXT(n, m) = LISTD_NEXT(q, m); \
    LISTD_PREV(n, m) = (q);              \
    LISTD_NEXT_PREV(n, m) = (n);         \
    LISTD_NEXT(q, m) = (n);              \
  } while (0)

#define LISTD_INSERT_TAIL(q, n, m)       \
  do {                                   \
    LISTD_NEXT(n, m) = (q);              \
    LISTD_PREV(n, m) = LISTD_PREV(q, m); \
    LISTD_PREV_NEXT(n, m) = (n);         \
    LISTD_PREV(q, m) = (n);              \
  } while (0)

#define LISTD_REMOVE(n, m)                    \
  do {                                        \
    LISTD_PREV_NEXT(n, m) = LISTD_NEXT(n, m); \
    LISTD_NEXT_PREV(n, m) = LISTD_PREV(n, m); \
  } while (0)

#define LISTD_FOREACH(q, n, m)         for ((n) = LISTD_HEAD(q, m); (n) != (q); (n) = LISTD_NEXT(n, m))
#define LISTD_FOREACH_REVERSE(q, n, m) for ((n) = LISTD_TAIL(q, m); (n) != (q); (n) = LISTD_PREV(n, m))
#define LISTD_FOREACH_SAFE(q, n, t, m) \
  for ((n) = LISTD_HEAD(q, m), (t) = LISTD_NEXT(n, m); (n) != (q); (n) = (t), (t) = LISTD_NEXT(n, m))
#define LISTD_FOREACH_REVERSE_SAFE(q, n, t, m) \
  for ((n) = LISTD_TAIL(q, m), (t) = LISTD_PREV(n, m); (n) != (q); (n) = (t), (t) = LISTD_PREV(n, m))

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_LIST_H_*/