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

#ifndef _TD_UTIL_RING_BUF_H
#define _TD_UTIL_RING_BUF_H

#ifdef __cplusplus
extern "C" {
#endif

#define TRINGBUF(TYPE) \
  struct {             \
    TYPE   *data;      \
    int32_t capacity;  \
    int32_t head;      \
    int32_t tail;      \
    int32_t size;      \
  }

#define TRINGBUF_CAPACITY(rbuf) ((rbuf)->capacity)
#define TRINGBUF_SIZE(rbuf)     ((rbuf)->size)
#define TRINGBUF_IS_EMPTY(rbuf) ((rbuf)->size == 0)
#define TRINGBUF_IS_FULL(rbuf)  ((rbuf)->size == (rbuf)->capacity)
#define TRINGBUF_HEAD(rbuf)     (&(rbuf)->data[(rbuf)->head])
#define TRINGBUF_TAIL(rbuf)     (&(rbuf)->data[(rbuf)->tail])
#define TRINGBUF_MOVE_NEXT(rbuf, ptr)               \
  do {                                              \
    (ptr)++;                                        \
    if ((ptr) == (rbuf)->data + (rbuf)->capacity) { \
      (ptr) = (rbuf)->data;                         \
    }                                               \
  } while (0)

static FORCE_INLINE int32_t tringbufExtend(void *rbuf, int32_t expSize, int32_t eleSize) {
  TRINGBUF(void) *rb = rbuf;

  int32_t capacity = rb->capacity;
  if (capacity == 0) {
    capacity = TMAX(64 / eleSize, 1);  // at least 1 element
  }
  while (capacity < expSize) {
    capacity <<= 1;
  }

  void *p = taosMemoryRealloc(rb->data, capacity * eleSize);
  if (p == NULL) return terrno;
  rb->data = p;

  if (rb->head > rb->tail || (rb->head == rb->tail && rb->size > 0)) {
    // move the data to the end of the buffer
    int32_t  nele = rb->capacity - rb->head;
    uint8_t *src = (uint8_t *)rb->data + rb->head * eleSize;
    uint8_t *dst = (uint8_t *)p + (capacity - nele) * eleSize;
    memmove(dst, src, nele * eleSize);
    rb->head = capacity - nele;
  }
  rb->capacity = capacity;
  return 0;
}

static FORCE_INLINE int32_t tringbufPushBatch(void *rbuf, const void *elePtr, int32_t numEle, int32_t eleSize) {
  TRINGBUF(void) *rb = rbuf;

  int32_t ret = 0;
  if (rb->size + numEle > rb->capacity) {
    ret = tringbufExtend(rbuf, rb->size + numEle, eleSize);
  }
  if (ret == 0) {
    if (rb->tail + numEle > rb->capacity) {
      int32_t  nele = rb->capacity - rb->tail;
      uint8_t *src = (uint8_t *)elePtr;
      uint8_t *dst = (uint8_t *)rb->data + rb->tail * eleSize;
      memmove(dst, src, nele * eleSize);
      src += nele * eleSize;
      dst = (uint8_t *)rb->data;
      memmove(dst, src, (numEle - nele) * eleSize);
      rb->tail = numEle - nele;
    } else {
      uint8_t *src = (uint8_t *)elePtr;
      uint8_t *dst = (uint8_t *)rb->data + rb->tail * eleSize;
      memmove(dst, src, numEle * eleSize);
      rb->tail += numEle;
      if (rb->tail == rb->capacity) {
        rb->tail = 0;
      }
    }
    rb->size += numEle;
  }
  return ret;
}

#define TRINGBUF_INIT(rbuf) \
  do {                      \
    (rbuf)->data = NULL;    \
    (rbuf)->capacity = 0;   \
    (rbuf)->head = 0;       \
    (rbuf)->tail = 0;       \
    (rbuf)->size = 0;       \
  } while (0)

#define TRINGBUF_RESERVE(rbuf, cap) tringbufExtend(rbuf, cap, sizeof((rbuf)->data[0]))

#define TRINGBUF_DESTROY(rbuf)           \
  do {                                   \
    if ((rbuf)->data) {                  \
      taosMemoryFreeClear((rbuf)->data); \
      (rbuf)->data = NULL;               \
    }                                    \
    (rbuf)->capacity = 0;                \
    (rbuf)->head = 0;                    \
    (rbuf)->tail = 0;                    \
    (rbuf)->size = 0;                    \
  } while (0)

#define TRINGBUF_APPEND(rbuf, ele) tringbufPushBatch(rbuf, &(ele), 1, sizeof((rbuf)->data[0]))
#define TRINGBUF_DEQUEUE(rbuf)                \
  do {                                        \
    if ((rbuf)->size > 0) {                   \
      (rbuf)->head += 1;                      \
      if ((rbuf)->head == (rbuf)->capacity) { \
        (rbuf)->head = 0;                     \
      }                                       \
      (rbuf)->size -= 1;                      \
    }                                         \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_RING_BUF_H_*/
