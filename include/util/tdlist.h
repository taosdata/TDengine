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

#ifndef _TD_UTIL_TDLIST_H_
#define _TD_UTIL_TDLIST_H_

#ifdef __cplusplus
extern "C" {
#endif

// Single linked list
#define TD_SLIST_NODE(TYPE) \
  struct {                  \
    struct TYPE *sl_next_;  \
  }

#define TD_SLIST(TYPE)      \
  struct {                  \
    struct TYPE *sl_head_;  \
    int          sl_neles_; \
  }

#define TD_SLIST_HEAD(sl) ((sl)->sl_head_)
#define TD_SLIST_NELES(sl) ((sl)->sl_neles_)
#define TD_SLIST_NODE_NEXT(sln) ((sln)->sl_next_)

#define tSListInit(sl)     \
  do {                     \
    (sl)->sl_head_ = NULL; \
    (sl)->sl_neles_ = 0;   \
  } while (0)

#define tSListPush(sl, sln)                      \
  do {                                           \
    TD_SLIST_NODE_NEXT(sln) = TD_SLIST_HEAD(sl); \
    TD_SLIST_HEAD(sl) = (sln);                   \
    TD_SLIST_NELES(sl) += 1;                     \
  } while (0)

#define tSListPop(sl)                                          \
  do {                                                         \
    TD_SLIST_HEAD(sl) = TD_SLIST_NODE_NEXT(TD_SLIST_HEAD(sl)); \
    TD_SLIST_NELES(sl) -= 1;                                   \
  } while (0)

// Double linked list
#define TD_DLIST_NODE(TYPE) \
  struct {                  \
    TYPE *dl_prev_;         \
    TYPE *dl_next_;         \
  }

#define TD_DLIST(TYPE)      \
  struct {                  \
    struct TYPE *dl_head_;  \
    struct TYPE *dl_tail_;  \
    int          dl_neles_; \
  }

#define TD_DLIST_NODE_PREV(dln) ((dln)->dl_prev_)
#define TD_DLIST_NODE_NEXT(dln) ((dln)->dl_next_)
#define TD_DLIST_HEAD(dl) ((dl)->dl_head_)
#define TD_DLIST_TAIL(dl) ((dl)->dl_tail_)
#define TD_DLIST_NELES(dl) ((dl)->dl_neles_)

#define tDListInit(dl)                            \
  do {                                            \
    TD_DLIST_HEAD(dl) = TD_DLIST_TAIL(dl) = NULL; \
    TD_DLIST_NELES(dl) = 0;                       \
  } while (0)

#define tDListAppend(dl, dln)                                   \
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

#define tDListPrepend(dl, dln)                                  \
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

#define tDListPop(dl, dln)                                                   \
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

#if 0
// List iterator
#define TD_LIST_FITER 0
#define TD_LIST_BITER 1
#define TD_LIST_ITER(S)     \
  struct {                  \
    int it_dir_;            \
    S * it_next_;           \
    S * it_ptr_;            \
    TD_DLIST(S) * it_list_; \
  }

#define tlistIterInit(it, l, dir)   \
  (it)->it_dir_ = (dir);            \
  (it)->it_list_ = l;               \
  if ((dir) == TD_LIST_FITER) {     \
    (it)->it_next_ = (l)->dl_head_; \
  } else {                          \
    (it)->it_next_ = (l)->dl_tail_; \
  }

#define tlistIterNext(it)                       \
  ({                                            \
    (it)->it_ptr_ = (it)->it_next_;             \
    if ((it)->it_next_ != NULL) {               \
      if ((it)->it_dir_ == TD_LIST_FITER) {     \
        (it)->it_next_ = (it)->it_next_->next_; \
      } else {                                  \
        (it)->it_next_ = (it)->it_next_->prev_; \
      }                                         \
    }                                           \
    (it)->it_ptr_;                              \
  })
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TDLIST_H_*/