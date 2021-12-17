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

#ifndef _TD_TSDB_H_
#define _TD_TSDB_H_

#include "mallocator.h"

#ifdef __cplusplus
extern "C" {
#endif

// TYPES EXPOSED
typedef struct STsdb STsdb;

typedef struct STsdbCfg {
  uint64_t lruCacheSize;
  uint32_t keep0;
  uint32_t keep1;
  uint32_t keep2;
} STsdbCfg;

// STsdb
STsdb *tsdbOpen(const char *path, const STsdbCfg *pTsdbCfg, SMemAllocatorFactory *pMAF);
void   tsdbClose(STsdb *);
void   tsdbRemove(const char *path);
int    tsdbInsertData(STsdb *pTsdb, SSubmitMsg *pMsg);
int    tsdbPrepareCommit(STsdb *pTsdb);
int    tsdbCommit(STsdb *pTsdb);

// STsdbCfg
int  tsdbOptionsInit(STsdbCfg *);
void tsdbOptionsClear(STsdbCfg *);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_H_*/
