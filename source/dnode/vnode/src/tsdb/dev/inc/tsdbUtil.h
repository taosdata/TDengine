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

#define DEL_RECORD_NUM_ELEM 5
typedef union {
  int64_t aData[DEL_RECORD_NUM_ELEM];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t version;
    int64_t skey;
    int64_t ekey;
  };
} SDelRecord;

typedef union {
  TARRAY2(int64_t) dataArr[DEL_RECORD_NUM_ELEM];
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) version[1];
    TARRAY2(int64_t) skey[1];
    TARRAY2(int64_t) ekey[1];
  };
} SDelBlock;

typedef struct SDelBlk {
  int32_t   numRec;
  int32_t   size[DEL_RECORD_NUM_ELEM];
  TABLEID   minTid;
  TABLEID   maxTid;
  int64_t   minVer;
  int64_t   maxVer;
  SFDataPtr dp[1];
} SDelBlk;

#define DEL_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tDelBlockInit(SDelBlock *delBlock);
int32_t tDelBlockFree(SDelBlock *delBlock);
int32_t tDelBlockClear(SDelBlock *delBlock);
int32_t tDelBlockPut(SDelBlock *delBlock, const SDelRecord *delRecord);
int32_t tDelBlockEncode(SDelBlock *delBlock, void *buf, int32_t size);
int32_t tDelBlockDecode(const void *buf, SDelBlock *delBlock);

// STbStatisBlock ----------
#define STATIS_RECORD_NUM_ELEM 9
typedef union {
  int64_t aData[STATIS_RECORD_NUM_ELEM];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t firstKey;
    int64_t firstKeyVer;
    int64_t lastKey;
    int64_t lastKeyVer;
    int64_t minVer;
    int64_t maxVer;
    int64_t count;
  };
} STbStatisRecord;

typedef union {
  TARRAY2(int64_t) dataArr[STATIS_RECORD_NUM_ELEM];
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) firstKey[1];
    TARRAY2(int64_t) firstKeyVer[1];
    TARRAY2(int64_t) lastKey[1];
    TARRAY2(int64_t) lastKeyVer[1];
    TARRAY2(int64_t) minVer[1];
    TARRAY2(int64_t) maxVer[1];
    TARRAY2(int64_t) count[1];
  };
} STbStatisBlock;

typedef struct SStatisBlk {
  int32_t   numRec;
  int32_t   size[STATIS_RECORD_NUM_ELEM];
  TABLEID   minTbid;
  TABLEID   maxTbid;
  int64_t   minVer;
  int64_t   maxVer;
  SFDataPtr dp[1];
} SStatisBlk;

#define STATIS_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tStatisBlockInit(STbStatisBlock *statisBlock);
int32_t tStatisBlockFree(STbStatisBlock *statisBlock);
int32_t tStatisBlockClear(STbStatisBlock *statisBlock);
int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *statisRecord);

// other apis
int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb);
int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/