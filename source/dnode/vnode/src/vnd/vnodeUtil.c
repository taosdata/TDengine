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

#include "vnd.h"

int32_t vnodeRealloc(void** pp, int32_t size) {
  uint8_t* p = NULL;
  int32_t  csize = 0;

  if (*pp) {
    p = (uint8_t*)(*pp) - sizeof(int32_t);
    csize = *(int32_t*)p;
  }

  if (csize >= size) {
    return 0;
  }

  p = (uint8_t*)taosMemoryRealloc(p, size);
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *(int32_t*)p = size;
  *pp = p + sizeof(int32_t);

  return 0;
}

void vnodeFree(void* p) {
  if (p) {
    taosMemoryFree(((uint8_t*)p) - sizeof(int32_t));
  }
}