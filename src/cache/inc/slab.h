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

#ifndef TDENGINE_SLAB_H
#define TDENGINE_SLAB_H

#include <stdlib.h> // for size_t
#include "slab.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

struct slab_t {
  unsigned int size;      /* sizes of items */
  unsigned int perslab;   /* how many items per slab */
} ;

cache_code_t slab_init(cache_context_t *);

unsigned int slabs_clsid(cache_context_t *context, size_t size);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SLAB_H