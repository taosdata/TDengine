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

#ifndef _todbc_hash_h_
#define _todbc_hash_h_


#include <stdint.h>
#include <stddef.h>

// non-thread-safe

typedef struct todbc_hash_conf_s      todbc_hash_conf_t;
typedef struct todbc_hash_s           todbc_hash_t;

typedef void (*hash_val_free_f)(todbc_hash_t *hash, void *val, void *arg);
typedef unsigned long (*hash_key_hash_f)(todbc_hash_t *hash, void *key);
typedef int  (*hash_key_comp_f)(todbc_hash_t *hash, void *key, void *val);

struct todbc_hash_conf_s {
  void              *arg;
  hash_val_free_f    val_free;

  hash_key_hash_f    key_hash;
  hash_key_comp_f    key_comp;

  size_t             slots;
};

todbc_hash_t* todbc_hash_create(todbc_hash_conf_t conf);
void          todbc_hash_free(todbc_hash_t *hash);

// fail if key exists
int todbc_hash_put(todbc_hash_t *hash, void *key, void *val);
int todbc_hash_get(todbc_hash_t *hash, void *key, void **val, int *found);
int todbc_hash_del(todbc_hash_t *hash, void *key);
typedef int (*hash_iterate_f)(todbc_hash_t *hash, void *val, void *arg);
int todbc_hash_traverse(todbc_hash_t *hash, hash_iterate_f iterate, void *arg);

#endif // _todbc_hash_h_

