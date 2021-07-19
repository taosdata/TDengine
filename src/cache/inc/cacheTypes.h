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

#ifndef TDENGINE_CACHE_TYPES_H
#define TDENGINE_CACHE_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

struct cache_context_t;
typedef struct cache_context_t cache_context_t;

enum cache_code_t;
typedef enum cache_code_t cache_code_t;

struct cache_option_t;
typedef struct cache_option_t cache_option_t;

struct hashtable_t;
typedef struct hashtable_t hashtable_t;

struct cache_item_t;
typedef struct cache_item_t cache_item_t;

struct cache_slab_t;
typedef struct cache_slab_t cache_slab_t;

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_TYPES_H */