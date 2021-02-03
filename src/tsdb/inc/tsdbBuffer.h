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

#ifndef _TD_TSDB_BUFFER_H_
#define _TD_TSDB_BUFFER_H_

typedef struct {
  int64_t blockId;
  int     offset;
  int     remain;
  char    data[];
} STsdbBufBlock;

typedef struct {
  pthread_cond_t poolNotEmpty;
  int            bufBlockSize;
  int            tBufBlocks;
  int            nBufBlocks;
  int64_t        index;
  SList*         bufBlockList;
} STsdbBufPool;

#define TSDB_BUFFER_RESERVE 1024  // Reseve 1K as commit threshold

STsdbBufPool* tsdbNewBufPool();
void          tsdbFreeBufPool(STsdbBufPool* pBufPool);
int           tsdbOpenBufPool(STsdbRepo* pRepo);
void          tsdbCloseBufPool(STsdbRepo* pRepo);
SListNode*    tsdbAllocBufBlockFromPool(STsdbRepo* pRepo);

#endif /* _TD_TSDB_BUFFER_H_ */