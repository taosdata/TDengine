/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_ASSOC_H
#define TDENGINE_ASSOC_H

#include <stdbool.h>
#include <stdint.h>
#include "assoc.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*hash_func_t)(const void *key, size_t length);

struct hashtable_t {
  item_t** primary_hashtable;

  item_t** old_hashtable;

  hash_func_t hash;

  int   hashpower;
  bool    expanding;
};

cache_code_t hash_init(cache_context_t* context);
cache_code_t hash_put(cache_context_t* context, item_t* item);
item_t* hash_get(cache_context_t* context, item_t* item);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_ASSOC_H */
