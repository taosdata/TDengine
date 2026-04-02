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
  int64_t data[TOMB_RECORD_ELEM_NUM];
  struct {
    int64_t suid;
    int64_t uid;
    int64_t version;
    int64_t skey;
    int64_t ekey;
  };
} STombRecord;

typedef struct {
  int32_t numOfRecords;
  union {
    SBuffer buffers[TOMB_RECORD_ELEM_NUM];
    struct {
      SBuffer suids;
      SBuffer uids;
      SBuffer versions;
      SBuffer skeys;
      SBuffer ekeys;
    };
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

#define TOMB_BLOCK_SIZE(db) ((db)->numOfRecords)

void    tTombBlockInit(STombBlock *tombBlock);
void    tTombBlockDestroy(STombBlock *tombBlock);
void    tTombBlockClear(STombBlock *tombBlock);
int32_t tTombBlockPut(STombBlock *tombBlock, const STombRecord *record);
int32_t tTombBlockGet(STombBlock *tombBlock, int32_t idx, STombRecord *record);
int32_t tTombRecordCompare(const STombRecord *record1, const STombRecord *record2);

// STbStatisRecord ----------
typedef struct {
  int64_t suid;
  int64_t uid;
  SRowKey firstKey;
  SRowKey lastKey;
  int64_t count;
} STbStatisRecord;

typedef struct {
  int8_t  numOfPKs;
  int32_t numOfRecords;
  union {
    SBuffer buffers[5];
    struct {
      SBuffer suids;               // int64_t
      SBuffer uids;                // int64_t
      SBuffer firstKeyTimestamps;  // int64_t
      SBuffer lastKeyTimestamps;   // int64_t
      SBuffer counts;              // int64_t
    };
  };
  SValueColumn firstKeyPKs[TD_MAX_PK_COLS];
  SValueColumn lastKeyPKs[TD_MAX_PK_COLS];
} STbStatisBlock;

typedef struct {
  SFDataPtr dp[1];
  TABLEID   minTbid;
  TABLEID   maxTbid;
  int32_t   numRec;
  int32_t   size[5];
  int8_t    cmprAlg;
  int8_t    numOfPKs;  // number of primary keys
  int8_t    rsvd[6];
} SStatisBlk;

#define STATIS_BLOCK_SIZE(db) ((db)->numOfRecords)

int32_t tStatisBlockInit(STbStatisBlock *statisBlock);
void    tStatisBlockDestroy(STbStatisBlock *statisBlock);
void    tStatisBlockClear(STbStatisBlock *statisBlock);
int32_t tStatisBlockPut(STbStatisBlock *statisBlock, SRowInfo *row, int32_t maxRecords);
int32_t tStatisBlockGet(STbStatisBlock *statisBlock, int32_t idx, STbStatisRecord *record);

// SBrinRecord ----------
typedef struct {
  int64_t     suid;
  int64_t     uid;
  STsdbRowKey firstKey;
  STsdbRowKey lastKey;
  int64_t     minVer;
  int64_t     maxVer;
  int64_t     blockOffset;
  int64_t     smaOffset;
  int32_t     blockSize;
  int32_t     blockKeySize;
  int32_t     smaSize;
  int32_t     numRow;
  int32_t     count;
} SBrinRecord;

typedef struct {
  int8_t  numOfPKs;
  int32_t numOfRecords;
  union {
    SBuffer buffers[15];
    struct {
      SBuffer suids;               // int64_t
      SBuffer uids;                // int64_t
      SBuffer firstKeyTimestamps;  // int64_t
      SBuffer firstKeyVersions;    // int64_t
      SBuffer lastKeyTimestamps;   // int64_t
      SBuffer lastKeyVersions;     // int64_t
      SBuffer minVers;             // int64_t
      SBuffer maxVers;             // int64_t
      SBuffer blockOffsets;        // int64_t
      SBuffer smaOffsets;          // int64_t
      SBuffer blockSizes;          // int32_t
      SBuffer blockKeySizes;       // int32_t
      SBuffer smaSizes;            // int32_t
      SBuffer numRows;             // int32_t
      SBuffer counts;              // int32_t
    };
  };
  SValueColumn firstKeyPKs[TD_MAX_PK_COLS];
  SValueColumn lastKeyPKs[TD_MAX_PK_COLS];
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
  int8_t    numOfPKs;  // number of primary keys
  int8_t    rsvd[6];
} SBrinBlk;

typedef TARRAY2(SBrinBlk) TBrinBlkArray;

#define BRIN_BLOCK_SIZE(db) ((db)->numOfRecords)

int32_t tBrinBlockInit(SBrinBlock *brinBlock);
void    tBrinBlockDestroy(SBrinBlock *brinBlock);
void    tBrinBlockClear(SBrinBlock *brinBlock);
int32_t tBrinBlockPut(SBrinBlock *brinBlock, const SBrinRecord *record);
int32_t tBrinBlockGet(SBrinBlock *brinBlock, int32_t idx, SBrinRecord *record);

// other apis
int32_t tsdbUpdateSkmTb(STsdb *pTsdb, const TABLEID *tbid, SSkmInfo *pSkmTb);
int32_t tsdbUpdateSkmRow(STsdb *pTsdb, const TABLEID *tbid, int32_t sver, SSkmInfo *pSkmRow);

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_UTIL_H*/