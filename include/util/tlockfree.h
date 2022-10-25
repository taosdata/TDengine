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

#ifndef _TD_UTIL_LOCK_FREE_H_
#define _TD_UTIL_LOCK_FREE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

// reference counting
typedef void (*_ref_fn_t)(const void *pObj);

#define T_REF_DECLARE()   \
  struct {                \
    volatile int32_t val; \
  } _ref;

#define T_REF_REGISTER_FUNC(s, e) \
  struct {                        \
    _ref_fn_t start;              \
    _ref_fn_t end;                \
  } _ref_func = {.begin = (s), .end = (e)};

// set the initial reference count value
#define T_REF_INIT_VAL(x, _v)                \
  do {                                       \
    assert(_v >= 0);                         \
    atomic_store_32(&((x)->_ref.val), (_v)); \
  } while (0)

// increase the reference count by 1
#define T_REF_INC(x) (atomic_add_fetch_32(&((x)->_ref.val), 1))

#define T_REF_INC_WITH_CB(x, p)                           \
  do {                                                    \
    int32_t v = atomic_add_fetch_32(&((x)->_ref.val), 1); \
    if (v == 1 && (p)->_ref_func.begin != NULL) {         \
      (p)->_ref_func.begin((x));                          \
    }                                                     \
  } while (0)

#define T_REF_DEC(x) (atomic_sub_fetch_32(&((x)->_ref.val), 1))

#define T_REF_DEC_WITH_CB(x, p)                           \
  do {                                                    \
    int32_t v = atomic_sub_fetch_32(&((x)->_ref.val), 1); \
    if (v == 0 && (p)->_ref_func.end != NULL) {           \
      (p)->_ref_func.end((x));                            \
    }                                                     \
  } while (0)

#define T_REF_VAL_CHECK(x) assert((x)->_ref.val >= 0);

#define T_REF_VAL_GET(x) (x)->_ref.val

// single writer multiple reader lock
typedef volatile int32_t SRWLatch;

void    taosInitRWLatch(SRWLatch *pLatch);
void    taosWLockLatch(SRWLatch *pLatch);
void    taosWUnLockLatch(SRWLatch *pLatch);
void    taosRLockLatch(SRWLatch *pLatch);
void    taosRUnLockLatch(SRWLatch *pLatch);
int32_t taosWTryLockLatch(SRWLatch *pLatch);

// copy on read
#define taosCorBeginRead(x)                     \
  for (uint32_t i_ = 1; 1; ++i_) {              \
    int32_t old_ = atomic_add_fetch_32((x), 0); \
    if (old_ & 0x00000001) {                    \
      if (i_ % 1000 == 0) {                     \
        sched_yield();                          \
      }                                         \
      continue;                                 \
    }

#define taosCorEndRead(x)                    \
  if (atomic_add_fetch_32((x), 0) == old_) { \
    break;                                   \
  }                                          \
  }

#define taosCorBeginWrite(x) \
  taosCorBeginRead(x) if (atomic_val_compare_exchange_32((x), old_, old_ + 1) != old_) { continue; }

#define taosCorEndWrite(x)     \
  atomic_add_fetch_32((x), 1); \
  break;                       \
  }

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_LOCK_FREE_H_*/
