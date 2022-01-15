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

#ifndef _TD_TRANSPORT_INT_H_
#define _TD_TRANSPORT_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef USE_UV

#include <stddef.h>
typedef void *queue[2];

/* Private macros. */
#define QUEUE_NEXT(q) (*(queue **)&((*(q))[0]))
#define QUEUE_PREV(q) (*(queue **)&((*(q))[1]))

#define QUEUE_PREV_NEXT(q) (QUEUE_NEXT(QUEUE_PREV(q)))
#define QUEUE_NEXT_PREV(q) (QUEUE_PREV(QUEUE_NEXT(q)))

/* Initialize an empty queue. */
#define QUEUE_INIT(q)    \
  {                      \
    QUEUE_NEXT(q) = (q); \
    QUEUE_PREV(q) = (q); \
  }

/* Return true if the queue has no element. */
#define QUEUE_IS_EMPTY(q) ((const queue *)(q) == (const queue *)QUEUE_NEXT(q))

/* Insert an element at the back of a queue. */
#define QUEUE_PUSH(q, e)           \
  {                                \
    QUEUE_NEXT(e) = (q);           \
    QUEUE_PREV(e) = QUEUE_PREV(q); \
    QUEUE_PREV_NEXT(e) = (e);      \
    QUEUE_PREV(q) = (e);           \
  }

/* Remove the given element from the queue. Any element can be removed at any *
 * time. */
#define QUEUE_REMOVE(e)                 \
  {                                     \
    QUEUE_PREV_NEXT(e) = QUEUE_NEXT(e); \
    QUEUE_NEXT_PREV(e) = QUEUE_PREV(e); \
  }

/* Return the element at the front of the queue. */
#define QUEUE_HEAD(q) (QUEUE_NEXT(q))

/* Return the element at the back of the queue. */
#define QUEUE_TAIL(q) (QUEUE_PREV(q))

/* Iterate over the element of a queue. * Mutating the queue while iterating
 * results in undefined behavior. */
#define QUEUE_FOREACH(q, e) for ((q) = QUEUE_NEXT(e); (q) != (e); (q) = QUEUE_NEXT(q))

/* Return the structure holding the given element. */
#define QUEUE_DATA(e, type, field) ((type *)((void *)((char *)(e)-offsetof(type, field))))

#endif  // USE_LIBUV

#ifdef __cplusplus
}
#endif

#endif /*_TD_TRANSPORT_INT_H_*/
