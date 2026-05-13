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

#ifndef _TD_UTIL_IDPOOL_H_
#define _TD_UTIL_IDPOOL_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int32_t       maxId;
  int32_t       numOfFree;
  int32_t       freeSlot;
  bool         *freeList;
  TdThreadMutex mutex;
} id_pool_t;

void   *taosInitIdPool(int32_t maxId);
int32_t taosUpdateIdPool(id_pool_t *handle, int32_t maxId);
int32_t taosIdPoolMaxSize(id_pool_t *handle);
int32_t taosAllocateId(id_pool_t *handle);
void    taosFreeId(id_pool_t *handle, int32_t id);
void    taosIdPoolCleanUp(id_pool_t *handle);
int32_t taosIdPoolNumOfUsed(id_pool_t *handle);
bool    taosIdPoolMarkStatus(id_pool_t *handle, int32_t id);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_IDPOOL_H_*/
