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

#ifndef TDENGINE_EHASH_H
#define TDENGINE_EHASH_H

#include "hashfunc.h"
#include "tglobal.h"
#include "tlog.h"

#include <stdlib.h>


#ifdef __cplusplus
extern "C" {
#endif

#define DD(fmt, ...)                                                  \
  taosPrintLog("EHS ", tscEmbedded ? 255 : uDebugFlag,                \
               "%s[%d]:%s() " fmt "",                                 \
               basename((char*)__FILE__), __LINE__, __func__,         \
               ##__VA_ARGS__)

#define DA(fmt, ...)                 \
do {                                 \
  DD(fmt, ##__VA_ARGS__);            \
  abort();                           \
} while (0)

#define DASSERT(statement)           \
do {                                 \
  if (statement) break;              \
  DD("%s", #statement);              \
  abort();                           \
} while (0)

#define D() DD(".")
#define DILE() DA("internal logic error")

#define EHASH_MAX_CAPACITY (1024 * 1024 * 16)
#define EHASH_DEFAULT_LOAD_FACTOR (0.75)
#define EHASH_INDEX(v, c) ((v) & ((c)-1))

typedef struct ehash_node_s              ehash_node_t;
typedef struct ehash_obj_s               ehash_obj_t;
typedef struct ehash_iter_s              ehash_iter_t;
typedef struct ehash_conf_s              ehash_conf_t;
typedef struct ehash_node_val_s          ehash_node_val_t;
typedef struct ehash_node_kv_s           ehash_node_kv_t;

struct ehash_conf_s {
  size_t              capacity;
  _hash_fn_t          fn;
  int                 enableUpdate;
};

struct ehash_node_val_s {
  const char *buf;
  size_t      blen;
  // which is to be call'd when the node is about to free internally
  void (*free_cb)(ehash_obj_t *obj, const char *buf, size_t blen);
};

struct ehash_node_kv_s {
  const char *key;
  size_t      klen;
  const char *buf;
  size_t      blen;
};


const char*     ehash_node_get_key(ehash_node_t *node);
size_t          ehash_node_get_klen(ehash_node_t *node);
const char*     ehash_node_get_buf(ehash_node_t *node);
size_t          ehash_node_get_blen(ehash_node_t *node);

ehash_obj_t*    ehash_create(ehash_conf_t conf);
size_t          ehash_size(const ehash_obj_t *obj);
size_t          ehash_get_max_slot_length(ehash_obj_t *obj);

// ehash_put/ehash_get/ehash_node_acquire/ehash_query/ehash_iter_curr
// note1:   returns a currently-alive-node, it remains accessible until ehash_node_release
// note2:   does NOT guarantee its liveness, because node could be zombie'd by ehash_remove or ehash_put

// copy in, inc ref
// if key/klen exists, the old node becomes `zombie`
ehash_node_t*   ehash_put(ehash_obj_t *obj, const char *key, size_t klen, ehash_node_val_t val);
// inc ref, buf not copy
ehash_node_t*   ehash_get(ehash_obj_t *obj, const char *key, size_t klen);
// inc ref, fail if node's zombie'd
ehash_node_t*   ehash_node_acquire(ehash_node_t *node);
// inc ref if found
ehash_node_t*   ehash_query(ehash_obj_t *obj, int (*predict)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv), void *arg);
// inc ref if not end
ehash_node_t*   ehash_iter_curr(ehash_iter_t *iter);
// dec ref
// shall'd be paired with ehash_put/ehash_get/ehash_node_acquire/ehash_query/ehash_iter_curr, to make it balance
int             ehash_node_release(ehash_node_t *node);

// `arbitrary remove`
// if ref==0, remove
// if ref>0, set zombie flag, remove delayed
// otherwise, panic
int             ehash_remove(ehash_obj_t *obj, const char *key, size_t klen);

// panic if any of ehash_node_t is in use, or in other words, not released
void            ehash_destroy(ehash_obj_t *obj);

ehash_iter_t*   ehash_iter_create(ehash_obj_t *obj);
// return current node, which remains accessible until next call to ehash_iter_next or ehash_iter_destroy
// because ehash_node_t is held internally
int             ehash_iter_next(ehash_iter_t *iter, ehash_node_kv_t *kv);
void            ehash_iter_destroy(ehash_iter_t* iter);

// for `fp`
// key/buf not copy
// ref-inc'd before calling `fp`
// ref-dec'd after calling `fp`
// user can call any of above function except ehash_destroy
int ehash_traverse(ehash_obj_t *obj, void (*fp)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop), void *arg);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_EHASH_H

