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

#include "cacheint.h"
#include "cacheItem.h"
#include "cacheDefine.h"
#include "cacheSlab.h"

cache_code_t slab_init(cache_context_t *context) {
  context->slabs = NULL;

  context->slabs = calloc(1, sizeof(cache_slab_t) * MAX_NUMBER_OF_SLAB_CLASSES);
  if (context->slabs == NULL) {
    goto error;
  }

  int i = 0;
  size_t size = sizeof(cache_item_t) + CHUNK_SIZE;
  while (i < MAX_NUMBER_OF_SLAB_CLASSES) {
    cache_slab_t *slab = calloc(1, sizeof(cache_slab_t));
    if (slab == NULL) {
      goto error;
    }

    if (size % CHUNK_ALIGN_BYTES) {
      size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
    }
    slab->size = size;
    slab->perslab = SLAB_PAGE_SIZE / size;

    context->slabs[i] = slab;

    size *= context->options.factor;
  }
  context->power_largest = i;

  return CACHE_OK;

error:
  if (context->slabs != NULL) {
    while (i < MAX_NUMBER_OF_SLAB_CLASSES) {
      if (context->slabs[i] == NULL) {
        continue;
      }
      free(context->slabs[i]);
    }
  }
  return CACHE_FAIL;
}

unsigned int slab_class_id(cache_context_t *context, size_t size) {
  int i = 0;
  while (size > context->slabs[i]->size) {
    if (i++ > context->power_largest) {
      return context->power_largest;
    }
  }

  return i;
}

cache_item_t* slab_alloc(cache_context_t *context, size_t ntotal) {
  /*
  unsigned int id = slab_class_id(context, ntotal);
  cache_item_t *item = NULL;

  */
  return NULL;
}
