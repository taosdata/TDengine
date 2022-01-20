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

#include "todbc_hash.h"

#include "todbc_list.h"
#include "todbc_log.h"

#include <stdlib.h>

typedef struct todbc_hash_slot_s            todbc_hash_slot_t;

struct todbc_hash_s {
  todbc_hash_conf_t            conf;

  todbc_hash_slot_t           *slots;
  size_t                       n_slots;
};

struct todbc_hash_slot_s {
  todbc_list_t                *list;
};

todbc_hash_t* todbc_hash_create(todbc_hash_conf_t conf) {
  if (!conf.key_hash) return NULL;
  if (!conf.key_comp) return NULL;
  if (!conf.val_free) return NULL;

  todbc_hash_t *hash = (todbc_hash_t*)calloc(1, sizeof(*hash));
  if (!hash) return NULL;

  hash->conf = conf;
  if (hash->conf.slots==0) {
    hash->conf.slots = 33;
  }

  hash->slots = (todbc_hash_slot_t*)calloc(hash->conf.slots, sizeof(*hash->slots));
  do {
    if (!hash->slots) break;
    hash->n_slots = hash->conf.slots;
    return hash;
  } while (0);

  todbc_hash_free(hash);
  return NULL;
}

void todbc_hash_free(todbc_hash_t *hash) {
  if (!hash) return;
  for (int i=0; i<hash->n_slots; ++i) {
    todbc_hash_slot_t *slot = hash->slots + i;
    if (!slot->list) continue;
    todbc_list_free(slot->list);
    slot->list = NULL;
  }
  free(hash->slots);
  hash->n_slots = 0;
}

typedef struct kv_s             kv_t;
struct kv_s {
  todbc_hash_t      *hash;
  void              *key;
  void              *val;
};

static void do_val_free(todbc_list_t *list, void *val, void *arg) {
  todbc_hash_t *hash = (todbc_hash_t*)arg;
  DASSERT(list);
  DASSERT(hash);
  DASSERT(hash->conf.val_free);
  hash->conf.val_free(hash, val, hash->conf.arg);
}

int todbc_hash_put(todbc_hash_t *hash, void *key, void *val) {
  if (!hash) return -1;
  if (!hash->slots) return -1;
  if (!key) return -1;
  if (!val) return -1;

  void *old = NULL;
  int found = 0;
  int r = todbc_hash_get(hash, key, &old, &found);
  if (r) return r;
  if (found) return -1;

  unsigned long hash_val = hash->conf.key_hash(hash, key);
  todbc_hash_slot_t *slot = hash->slots + (hash_val % hash->n_slots);
  if (!slot->list) {
    todbc_list_conf_t conf = {0};
    conf.arg      = hash;
    conf.val_free = do_val_free;

    slot->list = todbc_list_create(conf);
    if (!slot->list) return -1;
  }

  r = todbc_list_pushback(slot->list, val);

  return r;
}

static int do_comp(todbc_list_t *list, void *old, void *val, void *arg) {
  kv_t *kv = (kv_t*)arg;
  DASSERT(kv);
  DASSERT(kv->hash);
  DASSERT(kv->key);
  DASSERT(kv->hash->conf.key_comp);
  DASSERT(kv->key == val);
  int r = kv->hash->conf.key_comp(kv->hash, kv->key, old);
  if (r==0) {
    kv->val = old;
  }
  return r;
}

int todbc_hash_get(todbc_hash_t *hash, void *key, void **val, int *found) {
  if (!hash) return -1;
  if (!hash->slots) return -1;
  if (!key) return -1;
  if (!val) return -1;
  if (!found) return -1;

  *found = 0;

  unsigned long hash_val = hash->conf.key_hash(hash, key);
  todbc_hash_slot_t *slot = hash->slots + (hash_val % hash->n_slots);
  if (slot->list) {
    kv_t kv = {0};
    kv.hash = hash;
    kv.key  = key;
    kv.val  = NULL;
    int r = todbc_list_find(slot->list, key, found, do_comp, &kv);
    if (*found) {
      DASSERT(r==0);
      DASSERT(kv.val);
      *val = kv.val;
    }
    return r;
  }

  return 0;
}

int todbc_hash_del(todbc_hash_t *hash, void *key) {
  return -1;
}

typedef struct arg_s             arg_t;
struct arg_s {
  todbc_hash_t        *hash;
  void                *arg;
  int (*iterate)(todbc_hash_t *hash, void *val, void *arg);
};

static int do_iterate(todbc_list_t *list, void *val, void *arg);

int todbc_hash_traverse(todbc_hash_t *hash, int (*iterate)(todbc_hash_t *hash, void *val, void *arg), void *arg) {
  if (!hash) return -1;
  if (!iterate) return -1;

  for (int i=0; i<hash->n_slots; ++i) {
    todbc_hash_slot_t *slot = hash->slots + i;
    if (!slot->list) continue;
    arg_t targ = {0};
    targ.hash = hash;
    targ.arg  = arg;
    targ.iterate = iterate;
    int r = todbc_list_traverse(slot->list, do_iterate, &targ);
    if (r) return r;
  }

  return 0;
}

static int do_iterate(todbc_list_t *list, void *val, void *arg) {
  arg_t *targ = (arg_t*)arg;
  DASSERT(targ);
  DASSERT(targ->iterate);
  DASSERT(targ->hash);
  return targ->iterate(targ->hash, val, targ->arg);
}

