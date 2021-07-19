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
#include "item.h"

size_t item_size(uint8_t nkey, int nbytes) {
  return sizeof(item_t) + sizeof(unsigned int) + (nkey + 1) + nbytes;
}

item_t* item_alloc(cache_context_t* context, size_t ntotal, int id) {
  return NULL;
}

void item_free(cache_context_t* context, item_t* item) {

}