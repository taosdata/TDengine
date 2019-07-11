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

#ifndef TDENGINE_VNODECACHE_H
#define TDENGINE_VNODECACHE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  short              notFree;
  short              numOfPoints;
  int                slot;
  int                index;
  int64_t            blockId;
  struct _meter_obj *pMeterObj;
  char *             offset[];
} SCacheBlock;

typedef struct {
  int64_t       blocks;
  int           maxBlocks;
  int           numOfBlocks;
  int           unCommittedBlocks;
  int32_t       currentSlot;
  int32_t       commitSlot;   // which slot is committed
  int32_t       commitPoint;  // starting point for next commit
  SCacheBlock **cacheBlocks;  // cache block list, circular list
} SCacheInfo;

typedef struct {
  int             vnode;
  char **         pMem;
  long            freeSlot;
  pthread_mutex_t vmutex;
  uint64_t        count;  // kind of transcation ID
  long            notFreeSlots;
  long            threshold;
  char            commitInProcess;
  int             cacheBlockSize;
  int             cacheNumOfBlocks;
} SCachePool;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_VNODECACHE_H
