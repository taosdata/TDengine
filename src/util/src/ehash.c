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

#include "ehash.h"

#include "tlockfree.h"

// #include "os.h"
// #include "taosdef.h"

#define EHASH_NEED_RESIZE(_h) ((_h)->size >= (_h)->capacity * EHASH_DEFAULT_LOAD_FACTOR)

#define GET_NODE_REF(node) atomic_load_64(&(node->refcount))
#define INC_NODE_REF(node) atomic_fetch_add_64(&(node->refcount), 1)
#define DEC_NODE_REF(node)                                            \
do {                                                                  \
  int64_t ref = atomic_sub_fetch_64(&(node->refcount), 1);            \
  if (ref>0) break;                                                   \
  if (ref<0) {                                                        \
    DILE();                                                           \
    break;                                                            \
  }                                                                   \
  if (!is_node_zombie(node)) {                                        \
    break;                                                            \
  }                                                                   \
  ehash_obj_t *obj = node->obj;                                       \
  if (!obj) {                                                         \
    DILE();                                                           \
    break;                                                            \
  }                                                                   \
  do_free_node(node);                                                 \
} while (0)

#define GET_OBJ_REF(obj) atomic_load_64(&(obj->refcount))
#define INC_OBJ_REF(obj) atomic_fetch_add_64(&(obj->refcount), 1)
#define DEC_OBJ_REF(obj) atomic_sub_fetch_64(&(obj->refcount), 1)
#define CHECK_OBJ(obj)                 \
do {                                   \
  if (GET_OBJ_REF(obj)>0) break;       \
  DILE();                              \
} while (0)

typedef struct ehash_slot_s              ehash_slot_t;

struct ehash_node_s {
  char             *key;
  size_t            klen;
  size_t            hashVal;
  char             *data;
  size_t            size;

  void (*free_cb)(ehash_obj_t *obj, const char *buf, size_t blen);

  ehash_obj_t        *obj;
  ehash_node_t       *prev;
  ehash_node_t       *next;

  ehash_slot_t       *slot;
  ehash_node_t       *prev_in_slot;
  ehash_node_t       *next_in_slot;

  int32_t             refcount;
  unsigned int        zombie:2;
  unsigned int        dying:2;
};

struct ehash_slot_s {
  ehash_node_t        *head;
  ehash_node_t        *tail;

  size_t               count;
  size_t               alives;
};

struct ehash_obj_s {
  ehash_slot_t     *slots;
  size_t          capacity;     // number of slots
  size_t          size;         // number of elements in hash table
  _hash_fn_t      hashFp;       // hash function

  SRWLatch        lock;         // read-write spin lock

  ehash_node_t      *head;
  ehash_node_t      *tail;

  int64_t         destroying_thread;
  int32_t         refcount;
  unsigned int    destroying:2;
  unsigned int    enableUpdate:2;
};

struct ehash_iter_s {
  ehash_obj_t       *obj;
  ehash_node_t      *curr;
  unsigned int    end:2;
};

static int  locking_obj(ehash_obj_t *obj);
static void unlocking_obj(ehash_obj_t *obj);
static int  lock_wr(ehash_obj_t *obj);
static void unlock_wr(ehash_obj_t *obj);
static void lock_rd(ehash_obj_t *obj);
static void unlock_rd(ehash_obj_t *obj);
static ehash_node_t* do_hash_get(ehash_obj_t *obj, uint32_t hashVal, const char *key, size_t klen);
static int  is_node_equal(ehash_obj_t *obj, ehash_node_t *node, const char *buf, size_t blen);
static ehash_node_t* do_hash_put(ehash_obj_t *obj, uint32_t hashVal, const char *key, size_t klen, ehash_node_val_t *val);
static void do_hash_push_to_slot(ehash_node_t *node);
static int  is_node_zombie(ehash_node_t *node);
static void set_node_zombie(ehash_node_t *node);
static void do_remove_from_slot(ehash_node_t *node);
static void do_remove_from_hash(ehash_node_t *node);
static void do_free_node(ehash_node_t *node);
static void do_hash_expand_space(ehash_obj_t *obj);

static int32_t do_round_capacity(int32_t length);

const char* ehash_node_get_key(ehash_node_t *node) {
  return node->key;
}

size_t ehash_node_get_klen(ehash_node_t *node) {
  return node->klen;
}

const char* ehash_node_get_buf(ehash_node_t *node) {
  return node->data;
}

size_t ehash_node_get_blen(ehash_node_t *node) {
  return node->size;
}

ehash_obj_t *ehash_create(ehash_conf_t conf) {
  size_t        capacity = conf.capacity;
  _hash_fn_t    fn       = conf.fn;

  if (capacity == 0 || fn == NULL) return NULL;

  ehash_obj_t *obj = (ehash_obj_t *)calloc(1, sizeof(*obj));
  if (obj == NULL) {
    DD("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // the max slots is not defined by user
  obj->capacity = do_round_capacity((int32_t)capacity);
  assert((obj->capacity & (obj->capacity - 1)) == 0);

  obj->hashFp = fn;
  obj->enableUpdate = conf.enableUpdate;

  do_hash_expand_space(obj);
  if (obj->slots == NULL) {
    free(obj);
    DD("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  return obj;
}

size_t ehash_size(const ehash_obj_t *obj) {
  if (!obj) return 0;
  return obj->size;
}

static size_t do_ehash_get_max_slot_length(ehash_obj_t *obj) {
  size_t longest = 0;
  if (obj->slots) {
    for (size_t i=0; i<obj->capacity; ++i) {
      ehash_slot_t *slot = obj->slots + i;
      size_t alives = slot->alives;
      if (longest < alives) {
        longest = alives;
      }
    }
  }

  return longest;
}

size_t ehash_get_max_slot_length(ehash_obj_t *obj) {
  if (!obj) return 0;
  if (!locking_obj(obj)) return 0;
  lock_rd(obj);
  size_t r = do_ehash_get_max_slot_length(obj);
  unlock_rd(obj);
  unlocking_obj(obj);
  return r;
}

static ehash_node_t* do_ehash_put(ehash_obj_t *obj, const char *key, size_t klen, ehash_node_val_t *val) {
  if (klen == 0 || key == NULL) return NULL;
  uint32_t hashVal = (*obj->hashFp)(key, (uint32_t)klen);

  ehash_node_t *node = do_hash_get(obj, hashVal, key, klen);
  do {
    if (node && !obj->enableUpdate) {
      DEC_NODE_REF(node);
      node = NULL;
      break;
    }
    if (node) {
      int equal = is_node_equal(obj, node, val->buf, val->blen);
      if (equal) break;
      set_node_zombie(node);
      DEC_NODE_REF(node);
      node = NULL;
    }
    if (EHASH_NEED_RESIZE(obj)) {
      int32_t size = (int32_t)(obj->capacity << 1u);
      if (size <= EHASH_MAX_CAPACITY && size > obj->capacity) {
        obj->capacity = size;
        do_hash_expand_space(obj);
      }
    }
    node = do_hash_put(obj, hashVal, key, klen, val);
  } while (0);

  if (node) INC_OBJ_REF(obj);
  return node;
}

ehash_node_t* ehash_put(ehash_obj_t *obj, const char *key, size_t klen, ehash_node_val_t val) {
  if (!obj) return NULL;
  if (!locking_obj(obj)) return NULL;
  if (!lock_wr(obj)) return NULL;
  ehash_node_t *node = do_ehash_put(obj, key, klen, &val);
  unlock_wr(obj);
  unlocking_obj(obj);
  return node;
}

static ehash_node_t* do_ehash_get(ehash_obj_t *obj, const char *key, size_t klen) {
  if (klen == 0 || key == NULL) return NULL;

  uint32_t hashVal = (*obj->hashFp)(key, (uint32_t)klen);

  ehash_node_t *node = NULL;

  do {
    node = do_hash_get(obj, hashVal, key, klen);
    if (!node) break;

    INC_OBJ_REF(obj);
  } while (0);

  return node;
}

ehash_node_t* ehash_get(ehash_obj_t *obj, const char *key, size_t klen) {
  if (!obj) return NULL;
  if (!locking_obj(obj)) return NULL;
  lock_rd(obj);
  ehash_node_t *node = do_ehash_get(obj, key, klen);
  unlock_rd(obj);
  unlocking_obj(obj);
  return node;
}

static ehash_node_t* do_ehash_node_acquire(ehash_node_t *node) {
  if (is_node_zombie(node)) return NULL;
  INC_NODE_REF(node);
  INC_OBJ_REF(node->obj);
  return node;
}

ehash_node_t*   ehash_node_acquire(ehash_node_t *node) {
  if (!node) return NULL;
  ehash_obj_t *obj = node->obj;
  if (!locking_obj(obj)) return NULL;
  lock_rd(obj);
  ehash_node_t *n = do_ehash_node_acquire(node);
  unlock_rd(obj);
  unlocking_obj(obj);
  return n;
}

static ehash_node_t* do_ehash_query(ehash_obj_t *obj, int (*predict)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv), void *arg) {
  ehash_iter_t *iter = ehash_iter_create(obj);
  if (!iter) return NULL;

  ehash_node_t *node = NULL;
  ehash_node_kv_t kv = {0};
  while (ehash_iter_next(iter, &kv)) {
    int found = predict(obj, arg, kv);
    if (!found) continue;
    node = iter->curr;
    INC_NODE_REF(node);
    INC_OBJ_REF(obj);
    break;
  }

  ehash_iter_destroy(iter);

  return node;
}

ehash_node_t* ehash_query(ehash_obj_t *obj, int (*predict)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv), void *arg) {
  if (!obj) return NULL;
  if (!predict) return NULL;
  ehash_node_t *node = do_ehash_query(obj, predict, arg);
  return node;
}

ehash_node_t* ehash_iter_curr(ehash_iter_t *iter) {
  if (!iter) return NULL;
  ehash_obj_t *obj = iter->obj;
  if (!locking_obj(obj)) {
    DILE();
    return NULL; // never reached here
  }
  if (!lock_wr(obj)) {
    DILE();
    return NULL; // never reached here
  }
  ehash_node_t *curr = iter->curr;
  if (curr) {
    INC_NODE_REF(curr);
    INC_OBJ_REF(obj);
  }
  unlock_wr(obj);
  unlocking_obj(obj);
  return curr;
}

static int do_ehash_node_release(ehash_node_t *node) {
  ehash_obj_t *obj = node->obj;

  do {
    if (GET_NODE_REF(node)<=0) {
      DILE();
      return -1; // never reached here
    }
    if (GET_OBJ_REF(obj)<2) {
      DILE();
      return -1; // never reached here
    }
    DEC_NODE_REF(node);
    DEC_OBJ_REF(obj);
  } while (0);

  return 0;
}

int ehash_node_release(ehash_node_t *node) {
  if (!node) return -1;
  ehash_obj_t *obj = node->obj;
  if (!locking_obj(obj)) {
    DILE();
    return -1; // never reached here
  }
  if (!lock_wr(obj)) {
    DILE();
    return -1; // never reached here
  }
  int r = do_ehash_node_release(node);
  unlock_wr(obj);
  unlocking_obj(obj);
  return r;
}

static int do_ehash_remove(ehash_obj_t *obj, const char *key, size_t klen) {
  if (klen == 0 || key == NULL) {
    DILE();
    return -1; // never reached here
  }

  uint32_t hashVal = (*obj->hashFp)(key, (uint32_t)klen);

  do {
    ehash_node_t *node = do_hash_get(obj, hashVal, key, klen);
    if (!node) break;

    set_node_zombie(node);

    DEC_NODE_REF(node);
  } while (0);

  return 0;
}

int ehash_remove(ehash_obj_t *obj, const char *key, size_t klen) {
  if (!obj) return -1;
  if (!locking_obj(obj)) return -1;
  if (!lock_wr(obj)) return -1;
  int r = do_ehash_remove(obj, key, klen);
  unlock_wr(obj);
  unlocking_obj(obj);
  return r;
}

static int do_ehash_traverse(ehash_obj_t *obj, void (*fp)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop), void *arg) {
  ehash_iter_t *iter = ehash_iter_create(obj);
  if (!iter) return -1;

  ehash_node_kv_t kv = {0};
  while (ehash_iter_next(iter, &kv)) {
    if (fp) {
      int keep = 1, stop = 0;
      fp(obj, arg, kv, &keep, &stop);
      if (!keep) {
        DASSERT(lock_wr(obj));
        set_node_zombie(iter->curr);
        unlock_wr(obj);
      }
      if (stop) break;
    }
  }

  ehash_iter_destroy(iter);

  return 0;
}

int ehash_traverse(ehash_obj_t *obj, void (*fp)(ehash_obj_t *obj, void *arg, ehash_node_kv_t kv, int *keep, int *stop), void *arg) {
  if (!obj) return -1;
  int r = do_ehash_traverse(obj, fp, arg);
  return r;
}

void ehash_destroy(ehash_obj_t *obj) {
  if (!obj) return;
  int64_t me = taosGetPthreadId();
  if (atomic_val_compare_exchange_64(&obj->destroying_thread, 0, me)) {
    DILE();
    return; // never reached here
  }

  if (!lock_wr(obj)) {
    DILE();
    return; // never reached here
  }

  if (obj->destroying) {
    DILE();
    return; // never reached here
  }

  obj->destroying = 1;

  if (GET_OBJ_REF(obj)!=0) {
    DILE();
    return; // never reached here
  }

  for (ehash_node_t *node = obj->head; node;) {
    DASSERT(GET_NODE_REF(node)==0);
    ehash_node_t *next = node->next;
    set_node_zombie(node);
    do_free_node(node);
    node = next;
  }
  free(obj->slots);
  obj->slots = NULL;
  unlock_wr(obj);

  free(obj);
}

static ehash_iter_t* do_ehash_iter_create(ehash_obj_t *obj) {
  ehash_iter_t *iter = NULL;

  do {
    iter = (ehash_iter_t*)calloc(1, sizeof(*iter));
    if (!iter) break;

    INC_OBJ_REF(obj);
    iter->obj   = obj;
  } while (0);

  return iter;
}

ehash_iter_t* ehash_iter_create(ehash_obj_t *obj) {
  if (!obj) return NULL;
  if (!locking_obj(obj)) return NULL;
  if (!lock_wr(obj)) return NULL;
  ehash_iter_t *iter = do_ehash_iter_create(obj);
  unlock_wr(obj);
  unlocking_obj(obj);
  return iter;
}

static int do_ehash_iter_next(ehash_iter_t *iter, ehash_node_kv_t *kv) {
  if (iter->end) return 0;

  ehash_obj_t *obj    = iter->obj;

  do {
    if (!iter->curr) {
      iter->curr = obj->head;
      while (iter->curr) {
        if (!is_node_zombie(iter->curr)) break;
        iter->curr = iter->curr->next;
      }
      if (!iter->curr) {
        iter->end = 1;
      } else {
        INC_NODE_REF(iter->curr);
      }
      break;
    }

    while (iter->curr && !iter->end) {
      ehash_node_t *next = iter->curr->next;
      if (next) INC_NODE_REF(next);
      DEC_NODE_REF(iter->curr);
      iter->curr = next;
      if (!iter->curr) break;
      if (!is_node_zombie(iter->curr)) break;
    }
  } while (0);

  if (!iter->curr) iter->end = 1;

  if (iter->end && iter->curr) {
    DEC_NODE_REF(iter->curr);
    iter->curr = NULL;
  }

  if (iter->curr && kv) {
    kv->key      = iter->curr->key;
    kv->klen     = iter->curr->klen;
    kv->buf      = iter->curr->data;
    kv->blen     = iter->curr->size;
  }

  return iter->end ? 0 : 1;
}

int ehash_iter_next(ehash_iter_t *iter, ehash_node_kv_t *kv) {
  if (!iter) return -1;
  ehash_obj_t *obj = iter->obj;
  if (!locking_obj(obj)) {
    DILE();
    return -1; // never reached here
  }
  if (!lock_wr(obj)) {
    DILE();
    return -1; // never reached here
  }
  int r = do_ehash_iter_next(iter, kv);
  unlock_wr(obj);
  unlocking_obj(obj);
  return r;
}

static void do_ehash_iter_destroy(ehash_iter_t* iter) {
  ehash_obj_t *obj   = iter->obj;
  ehash_node_t *curr = iter->curr;

  iter->obj  = NULL;
  iter->curr = NULL;

  if (curr) {
    if (!lock_wr(obj)) {
      DILE();
      return; // never reached here
    }
    DEC_NODE_REF(curr);
    unlock_wr(obj);
  }
  DEC_OBJ_REF(obj);

  free(iter);
}

void ehash_iter_destroy(ehash_iter_t* iter) {
  if (!iter) return;
  ehash_obj_t *obj = iter->obj;
  if (!locking_obj(obj)) return;
  do_ehash_iter_destroy(iter);
  unlocking_obj(obj);
}

static int lock_wr(ehash_obj_t *obj) {
  int64_t me = taosGetPthreadId();
  int64_t destroying_thread = atomic_val_compare_exchange_64(&obj->destroying_thread, me, me);
  if (destroying_thread && destroying_thread != me) {
    return 0;
  }
  taosWLockLatch(&obj->lock);
  return 1;
}

static void unlock_wr(ehash_obj_t *obj) {
  taosWUnLockLatch(&obj->lock);
}

static void lock_rd(ehash_obj_t *obj) {
  taosRLockLatch(&obj->lock);
}

static void unlock_rd(ehash_obj_t *obj) {
  taosRUnLockLatch(&obj->lock);
}

static ehash_node_t* do_hash_get(ehash_obj_t *obj, uint32_t hashVal, const char *key, size_t klen) {
  int32_t idx = EHASH_INDEX(hashVal, obj->capacity);
  ehash_slot_t *slot = obj->slots + idx;
  for (ehash_node_t *node = slot->head; node; node = node->next_in_slot) {
    if (is_node_zombie(node)) continue;
    if (node->klen != klen) continue;
    if (memcmp(node->key, key, klen)) continue;
    INC_NODE_REF(node);
    return node;
  }
  return NULL;
}

static int is_node_equal(ehash_obj_t *obj, ehash_node_t *node, const char *buf, size_t blen) {
  if (!node->data) return 0;
  if (node->size != blen) return 0;
  return (0==memcmp(node->data, buf, blen));
}

static ehash_node_t* do_hash_put(ehash_obj_t *obj, uint32_t hashVal, const char *key, size_t klen, ehash_node_val_t *val) {
  ehash_node_t *node = calloc(1, sizeof(*node));

  if (node == NULL) {
    DD("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  node->data = malloc(val->blen + klen + 2);
  memcpy(node->data, val->buf, val->blen);
  node->data[val->blen] = '\0';
  node->size = val->blen;

  node->key = node->data + val->blen + 1;
  memcpy(node->key, key, klen);
  node->key[klen] = '\0';
  node->klen = klen;

  node->hashVal = hashVal;
  node->free_cb = val->free_cb;

  node->prev = obj->tail;
  if (obj->tail) {
    obj->tail->next = node;
  } else {
    obj->head = node;
  }
  obj->tail = node;

  node->obj = obj;

  do_hash_push_to_slot(node);

  obj->size += 1;

  INC_NODE_REF(node);
  return node;
}

static void do_hash_push_to_slot(ehash_node_t *node) {
  ehash_obj_t *obj = node->obj;

  size_t idx = EHASH_INDEX(node->hashVal, obj->capacity);
  ehash_slot_t *slot = obj->slots + idx;

  node->slot = slot;
  node->prev_in_slot = slot->tail;
  node->next_in_slot = NULL;
  if (slot->tail) {
    slot->tail->next_in_slot = node;
  } else {
    slot->head = node;
  }
  slot->tail = node;

  slot->count  += 1;
  slot->alives += 1;
}

static int is_node_zombie(ehash_node_t *node) {
  return (node->zombie!=0);
}

static void set_node_zombie(ehash_node_t *node) {
  if (node->zombie) return;
  node->zombie  = 1;
  if (node->slot) {
    node->slot->alives -= 1;
    DASSERT(node->slot->alives>=0);
  }
}

static void do_remove_from_slot(ehash_node_t *node) {
  ehash_slot_t *slot = node->slot;
  if (!slot) return;

  ehash_node_t *prev = node->prev_in_slot;
  ehash_node_t *next = node->next_in_slot;

  if (prev) prev->next_in_slot = next;
  else      slot->head         = next;

  if (next) next->prev_in_slot = prev;
  else      slot->tail         = prev;

  slot->count -= 1;
  DASSERT(slot->count>=0);

  node->slot = NULL;
}

static void do_remove_from_hash(ehash_node_t *node) {
  ehash_obj_t *obj = node->obj;
  if (!obj) return;

  ehash_node_t *prev = node->prev;
  ehash_node_t *next = node->next;

  if (prev) prev->next = next;
  else      obj->head  = next;

  if (next) next->prev = prev;
  else      obj->tail  = prev;

  obj->size -= 1;

  node->obj = NULL;
}

static void do_free_node(ehash_node_t *node) {
  ehash_obj_t *obj = node->obj;
  if (GET_NODE_REF(node)!=0) {
    DILE();
    return; // never reached here
  }
  DASSERT(!node->dying);
  node->dying = 1;
  do_remove_from_slot(node);
  do_remove_from_hash(node);
  if (node->free_cb) {
    unlock_wr(obj);
    node->free_cb(obj, node->data, node->size);
    if (!lock_wr(obj)) {
      DILE();
      return; // never reached here
    }
  }
  free(node->data);
  node->data = NULL;
  node->key  = NULL;
  node->obj  = NULL;
  free(node);
}

static void do_hash_expand_space(ehash_obj_t *obj) {
  ehash_slot_t *slots = (ehash_slot_t*)realloc(obj->slots, obj->capacity * sizeof(*slots));
  if (!slots) return;

  for (size_t i=0; i<obj->capacity; ++i) {
    memset(slots+i, 0, sizeof(*slots));
  }

  obj->slots = slots;

  for (ehash_node_t *node = obj->head; node; node = node->next) {
    if (is_node_zombie(node)) continue;
    do_hash_push_to_slot(node);
  }
}

static int32_t do_round_capacity(int32_t length) {
  int32_t len = MIN(length, EHASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < len) i = (i << 1u);
  return i;
}

static int locking_obj(ehash_obj_t *obj) {
  if (obj->destroying) return 0;
  INC_OBJ_REF(obj);
  return 1;
}

static void unlocking_obj(ehash_obj_t *obj) {
  DEC_OBJ_REF(obj);
}

