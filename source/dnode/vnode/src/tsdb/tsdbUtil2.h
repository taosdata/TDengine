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

// STombRecord ----------
#define TOMB_RECORD_ELEM_NUM 5
typedef union {
  int64_t dataArr[TOMB_RECORD_ELEM_NUM];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t version;
    int64_t skey;
    int64_t ekey;
  };
} STombRecord;

typedef union {
  TARRAY2(int64_t) dataArr[TOMB_RECORD_ELEM_NUM];
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) version[1];
    TARRAY2(int64_t) skey[1];
    TARRAY2(int64_t) ekey[1];
  };
} STombBlock;

typedef struct {
  SFDataPtr dp[1];
  TABLEID   minTbid;
  TABLEID   maxTbid;
  int64_t   minVer;
  int64_t   maxVer;
  int32_t   numRec;
  int32_t   size[TOMB_RECORD_ELEM_NUM];
  int8_t    cmprAlg;
  int8_t    rsvd[7];
} STombBlk;

typedef TARRAY2(STombBlk) TTombBlkArray;

#define TOMB_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tTombBlockInit(STombBlock *tombBlock);
int32_t tTombBlockDestroy(STombBlock *tombBlock);
int32_t tTombBlockClear(STombBlock *tombBlock);
int32_t tTombBlockPut(STombBlock *tombBlock, const STombRecord *record);
int32_t tTombBlockGet(STombBlock *tombBlock, int32_t idx, STombRecord *record);
int32_t tTombRecordCompare(const STombRecord *record1, const STombRecord *record2);

// STbStatisRecord ----------
#define STATIS_RECORD_NUM_ELEM 5
typedef union {
  int64_t dataArr[STATIS_RECORD_NUM_ELEM];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t firstKey;
    int64_t lastKey;
    int64_t count;
  };
} STbStatisRecord;

typedef union {
  TARRAY2(int64_t) dataArr[STATIS_RECORD_NUM_ELEM];
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) firstKey[1];
    TARRAY2(int64_t) lastKey[1];
    TARRAY2(int64_t) count[1];
  };
} STbStatisBlock;

typedef struct {
  SFDataPtr dp[1];
  TABLEID   minTbid;
  TABLEID   maxTbid;
  int32_t   numRec;
  int32_t   size[STATIS_RECORD_NUM_ELEM];
  int8_t    cmprAlg;
  int8_t    rsvd[7];
} SStatisBlk;

#define STATIS_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tStatisBlockInit(STbStatisBlock *statisBlock);
int32_t tStatisBlockDestroy(STbStatisBlock *statisBlock);
int32_t tStatisBlockClear(STbStatisBlock *statisBlock);
int32_t tStatisBlockPut(STbStatisBlock *statisBlock, const STbStatisRecord *record);
int32_t tStatisBlockGet(STbStatisBlock *statisBlock, int32_t idx, STbStatisRecord *record);

// SBrinRecord ----------
typedef union {
  struct {
    int64_t dataArr1[10];
    int32_t dataArr2[5];
  };
  struct {
    int64_t suid;
    int64_t uid;
    int64_t firstKey;
    int64_t firstKeyVer;
    int64_t lastKey;
    int64_t lastKeyVer;
    int64_t minVer;
    int64_t maxVer;
    int64_t blockOffset;
    int64_t smaOffset;
    int32_t blockSize;
    int32_t blockKeySize;
    int32_t smaSize;
    int32_t numRow;
    int32_t count;
  };
} SBrinRecord;

typedef union {
  struct {
    TARRAY2(int64_t) dataArr1[10];
    TARRAY2(int32_t) dataArr2[5];
  };
  struct {
    TARRAY2(int64_t) suid[1];
    TARRAY2(int64_t) uid[1];
    TARRAY2(int64_t) firstKey[1];
    TARRAY2(int64_t) firstKeyVer[1];
    TARRAY2(int64_t) lastKey[1];
    TARRAY2(int64_t) lastKeyVer[1];
    TARRAY2(int64_t) minVer[1];
    TARRAY2(int64_t) maxVer[1];
    TARRAY2(int64_t) blockOffset[1];
    TARRAY2(int64_t) smaOffset[1];
    TARRAY2(int32_t) blockSize[1];
    TARRAY2(int32_t) blockKeySize[1];
    TARRAY2(int32_t) smaSize[1];
    TARRAY2(int32_t) numRow[1];
    TARRAY2(int32_t) count[1];
  };
} SBrinBlock;

typedef struct {
  SFDataPtr dp[1];
  TABLEID   minTbid;
  TABLEID   maxTbid;
  int64_t   minVer;
  int64_t   maxVer;
  int32_t   numRec;
  int32_t   size[15];
  int8_t    cmprAlg;
  int8_t    rsvd[7];
} SBrinBlk;

typedef TARRAY2(SBrinBlk) TBrinBlkArray;

#define BRIN_BLOCK_SIZE(db) TARRAY2_SIZE((db)->suid)

int32_t tBrinBlockInit(SBrinBlock *brinBlock);
int32_t tBrinBlockDestroy(SBrinBlock *brinBlock);
int32_t tBrinBlockClear(SBrinBlock *brinBlock);
int32_t tBrinBlockPut(SBrinBlock *brinBlock, const SBrinRecord *record);
int32_t tBrinBlockGet(SBrinBlock *brinBlock, int32_t idx, SBrinRecord *record);

// other apis
int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb);
int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/