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

#include "tsdbDef.h"

#ifdef __cplusplus
extern "C" {
#endif

// SDelBlock ----------

typedef union {
  int64_t aData[5];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t version;
    int64_t skey;
    int64_t ekey;
  };
} SDelRecord;

typedef union {
  TARRAY2(int64_t) aData[5];
  struct {
    TARRAY2(int64_t) aSuid[1];
    TARRAY2(int64_t) aUid[1];
    TARRAY2(int64_t) aVer[1];
    TARRAY2(int64_t) aSkey[1];
    TARRAY2(int64_t) aEkey[1];
  };
} SDelBlock;

typedef struct SDelBlk {
  int32_t   nRow;
  TABLEID   minTid;
  TABLEID   maxTid;
  int64_t   minVer;
  int64_t   maxVer;
  SFDataPtr dp;
} SDelBlk;

#define DEL_BLOCK_SIZE(db) TARRAY2_SIZE((db)->aSuid)

int32_t tDelBlockInit(SDelBlock *delBlock);
int32_t tDelBlockFree(SDelBlock *delBlock);
int32_t tDelBlockClear(SDelBlock *delBlock);
int32_t tDelBlockPut(SDelBlock *delBlock, const SDelRecord *delRecord);
int32_t tDelBlockEncode(SDelBlock *delBlock, void *buf, int32_t size);
int32_t tDelBlockDecode(const void *buf, SDelBlock *delBlock);

// STbStatisBlock ----------
typedef union {
  int64_t aData[9];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t firstKey;
    int64_t firstVer;
    int64_t lastKey;
    int64_t lastVer;
    int64_t minVer;
    int64_t maxVer;
    int64_t count;
  };
} STbStatisRecord;

typedef union {
  TARRAY2(int64_t) aData[9];
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) firstKey[1];
    TARRAY2(int64_t) firstVer[1];
    TARRAY2(int64_t) lastKey[1];
    TARRAY2(int64_t) lastVer[1];
    TARRAY2(int64_t) minVer[1];
    TARRAY2(int64_t) maxVer[1];
    TARRAY2(int64_t) aCount[1];
  };
} STbStatisBlock;

typedef struct STbStatisBlk {
  int32_t   numRec;
  TABLEID   minTid;
  TABLEID   maxTid;
  int64_t   minVer;
  int64_t   maxVer;
  SFDataPtr dp;
} STbStatisBlk;

#define STATIS_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tStatisBlockInit(STbStatisBlock *statisBlock);
int32_t tStatisBlockFree(STbStatisBlock *statisBlock);
int32_t tStatisBlockClear(STbStatisBlock *statisBlock);
int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *statisRecord);
int32_t tStatisBlockEncode(STbStatisBlock *statisBlock, void *buf, int32_t size);
int32_t tStatisBlockDecode(const void *buf, STbStatisBlock *statisBlock);

// other apis
int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb);
int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/