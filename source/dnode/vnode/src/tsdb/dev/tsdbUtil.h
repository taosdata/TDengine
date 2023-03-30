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

#ifndef _TSDB_UTIL_H
#define _TSDB_UTIL_H

#include "tsdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// SDelBlock ----------
typedef struct SDelBlock SDelBlock;
typedef struct SDelBlk   SDelBlk;

int32_t tDelBlockCreate(SDelBlock *pDelBlock, int32_t capacity);
int32_t tDelBlockDestroy(SDelBlock *pDelBlock);
int32_t tDelBlockClear(SDelBlock *pDelBlock);
int32_t tDelBlockAppend(SDelBlock *pDelBlock, const TABLEID *tbid, const SDelData *pDelData);

// STbStatisBlock ----------
typedef struct STbStatisBlock STbStatisBlock;
typedef struct STbStatisBlk   STbStatisBlk;

int32_t tTbStatisBlockCreate(STbStatisBlock *pTbStatisBlock, int32_t capacity);
int32_t tTbStatisBlockDestroy(STbStatisBlock *pTbStatisBlock);
int32_t tTbStatisBlockClear(STbStatisBlock *pTbStatisBlock);

// other apis
int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb);
int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow);

/* Exposed Structs */
// <suid, uid, version, skey, ekey>
struct SDelBlock {
  int32_t  capacity;
  int32_t  nRow;
  int64_t *aData[5];  // [suid, uid, version, skey, ekey
};

struct SDelBlk {
  int64_t suidMax;
  int64_t suidMin;
  int64_t uidMax;
  int64_t uidMin;
  int64_t verMax;
  int64_t verMin;
};
struct STbStatisBlock {
  int32_t  capacity;
  int32_t  nRow;
  int64_t *aData[7];  // [suid, uid, skey, sver, ekey, ever, count]
};
struct STbStatisBlk {
  int64_t suidMax;
  int64_t suidMin;
  int64_t uidMax;
  int64_t uidMin;
  int64_t offset;
  int64_t size;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/