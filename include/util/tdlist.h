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
    struct type *sl_next_;  \
  }

#define TD_SLIST(TYPE)     \
  struct {                 \
    struct TYPE *sl_head_; \
  }

#define TD_SLIST_NODE_NEXT(sln) (sln)->sl_next_

#define tSListInit(sl)     \
  do {                     \
    (sl)->sl_head_ = NULL; \
  } while (0)

// Double linked list
#define TD_DLIST_NODE(S) \
  struct {               \
    S *prev_;            \
    S *next_;            \
  }

#define TD_DLIST(S) \
  struct {          \
    S * head_;      \
    S * tail_;      \
    int neles_;     \
  }

#define tlistInit(l)              \
  (l)->head_ = (l)->tail_ = NULL; \
  (l)->neles_ = 0;

#define tlistHead(l) (l)->head_
#define tlistTail(l) (l)->tail_
#define tlistNEles(l) (l)->neles_

#define tlistAppend(l, n)           \
  if ((l)->head_ == NULL) {         \
    (n)->prev_ = (n)->next_ = NULL; \
    (l)->head_ = (l)->tail_ = (n);  \
  } else {                          \
    (n)->prev_ = (l)->tail_;        \
    (n)->next_ = NULL;              \
    (l)->tail_->next_ = (n);        \
    (l)->tail_ = (n);               \
  }                                 \
  (l)->neles_ += 1;

#define tlistPrepend(l, n)          \
  if ((l)->head_ == NULL) {         \
    (n)->prev_ = (n)->next_ = NULL; \
    (l)->head_ = (l)->tail_ = (n);  \
  } else {                          \
    (n)->prev_ = NULL;              \
    (n)->next_ = (l)->head_;        \
    (l)->head_->prev_ = (n);        \
    (l)->head_ = (n);               \
  }                                 \
  (l)->neles_ += 1;

#define tlistPop(l, n)              \
  if ((l)->head_ == (n)) {          \
    (l)->head_ = (n)->next_;        \
  }                                 \
  if ((l)->tail_ == (n)) {          \
    (l)->tail_ = (n)->prev_;        \
  }                                 \
  if ((n)->prev_ != NULL) {         \
    (n)->prev_->next_ = (n)->next_; \
  }                                 \
  if ((n)->next_ != NULL) {         \
    (n)->next_->prev_ = (n)->prev_; \
  }                                 \
  (l)->neles_ -= 1;                 \
  (n)->prev_ = (n)->next_ = NULL;

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

#define tlistIterInit(it, l, dir) \
  (it)->it_dir_ = (dir);          \
  (it)->it_list_ = l;             \
  if ((dir) == TD_LIST_FITER) {   \
    (it)->it_next_ = (l)->head_;  \
  } else {                        \
    (it)->it_next_ = (l)->tail_;  \
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

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TDLIST_H_*/