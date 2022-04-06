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

#ifndef _TD_TSDB_READ_IMPL_H_
#define _TD_TSDB_READ_IMPL_H_

#include "os.h"
#include "tfs.h"
#include "tsdb.h"
#include "tsdbFile.h"
#include "tskiplist.h"
#include "tsdbMemory.h"
#include "tcommon.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SReadH SReadH;

typedef struct {
  int32_t  tid;
  uint32_t len;
  uint32_t offset;
  uint32_t hasLast : 2;
  uint32_t numOfBlocks : 30;
  uint64_t uid;
  TSKEY    maxKey;
} SBlockIdx;

#ifdef TD_REFACTOR_3
typedef struct {
  int64_t last : 1;
  int64_t offset : 63;
  int32_t algorithm : 8;
  int32_t numOfRows : 24;
  int32_t len;
  int32_t keyLen;  // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  int16_t numOfSubBlocks;
  int16_t numOfCols;  // not including timestamp column
  TSKEY   keyFirst;
  TSKEY   keyLast;
} SBlock;

#else

typedef enum {
  TSDB_SBLK_VER_0 = 0,
  TSDB_SBLK_VER_MAX,
} ESBlockVer;

#define SBlockVerLatest TSDB_SBLK_VER_0

typedef struct {
  uint8_t  last : 1;
  uint8_t  blkVer : 7;
  uint8_t  numOfSubBlocks;
  col_id_t numOfCols;    // not including timestamp column
  uint32_t len;          // data block length
  uint32_t keyLen : 20;  // key column length, keyOffset = offset+sizeof(SBlockData)+sizeof(SBlockCol)*numOfCols
  uint32_t algorithm : 4;
  uint32_t reserve : 8;
  col_id_t numOfBSma;
  uint16_t numOfRows;
  int64_t  offset;
  uint64_t aggrStat : 1;
  uint64_t aggrOffset : 63;
  TSKEY    keyFirst;
  TSKEY    keyLast;
} SBlockV0;

#define SBlock          SBlockV0  // latest SBlock definition

#endif

typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  tid;
  uint64_t uid;
  SBlock   blocks[];
} SBlockInfo;

#ifdef TD_REFACTOR_3
typedef struct {
  int16_t  colId;
  uint16_t bitmap : 1;  // 0: has bitmap if has NULL/NORM rows, 1: no bitmap if all rows are NORM
  uint16_t reserve : 15;
  int32_t  len;
  uint32_t type : 8;
  uint32_t offset : 24;
  int64_t  sum;
  int64_t  max;
  int64_t  min;
  int16_t  maxIndex;
  int16_t  minIndex;
  int16_t  numOfNull;
  uint8_t  offsetH;
  char     padding[1];
} SBlockCol;
#else
typedef struct {
  int16_t  colId;
  uint16_t type : 6;
  uint16_t blen : 10;   // bitmap length(TODO: full UT for the bitmap compress of various data input)
  uint32_t bitmap : 1;  // 0: has bitmap if has NULL/NORM rows, 1: no bitmap if all rows are NORM
  uint32_t len : 31;    // data length + bitmap length
  uint32_t offset;
} SBlockColV0;

#define SBlockCol SBlockColV0  // latest SBlockCol definition

typedef struct {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SAggrBlkColV0;

#define SAggrBlkCol SAggrBlkColV0  // latest SAggrBlkCol definition

#endif

// Code here just for back-ward compatibility
static FORCE_INLINE void tsdbSetBlockColOffset(SBlockCol *pBlockCol, uint32_t offset) {
#ifdef TD_REFACTOR_3
  pBlockCol->offset = offset & ((((uint32_t)1) << 24) - 1);
  pBlockCol->offsetH = (uint8_t)(offset >> 24);
#else
  pBlockCol->offset = offset;
#endif
}

static FORCE_INLINE uint32_t tsdbGetBlockColOffset(SBlockCol *pBlockCol) {
#ifdef TD_REFACTOR_3
  uint32_t offset1 = pBlockCol->offset;
  uint32_t offset2 = pBlockCol->offsetH;
  return (offset1 | (offset2 << 24));
#else
  return pBlockCol->offset;
#endif
}

typedef struct {
  int32_t   delimiter;  // For recovery usage
  int32_t   numOfCols;  // For recovery usage
  uint64_t  uid;        // For recovery usage
  SBlockCol cols[];
} SBlockData;

typedef void SAggrBlkData;  // SBlockCol cols[];

struct SReadH {
  STsdb *       pRepo;
  SDFileSet     rSet;     // FSET to read
  SArray *      aBlkIdx;  // SBlockIdx array
  STable *      pTable;   // table to read
  SBlockIdx *   pBlkIdx;  // current reading table SBlockIdx
  int           cidx;
  SBlockInfo *  pBlkInfo;
  SBlockData *  pBlkData;      // Block info
  SAggrBlkData *pAggrBlkData;  // Aggregate Block info
  SDataCols *   pDCols[2];
  void *        pBuf;    // buffer
  void *        pCBuf;   // compression buffer
  void *        pExBuf;  // extra buffer
};

#define TSDB_READ_REPO(rh)      ((rh)->pRepo)
#define TSDB_READ_REPO_ID(rh)   REPO_ID(TSDB_READ_REPO(rh))
#define TSDB_READ_FSET(rh)      (&((rh)->rSet))
#define TSDB_READ_TABLE(rh)     ((rh)->pTable)
#define TSDB_READ_HEAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_HEAD)
#define TSDB_READ_DATA_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_DATA)
#define TSDB_READ_LAST_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_LAST)
#define TSDB_READ_SMAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_SMAD)
#define TSDB_READ_SMAL_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_SMAL)
#define TSDB_READ_BUF(rh)       ((rh)->pBuf)
#define TSDB_READ_COMP_BUF(rh)  ((rh)->pCBuf)
#define TSDB_READ_EXBUF(rh)     ((rh)->pExBuf)

#define TSDB_BLOCK_STATIS_SIZE(ncols, blkVer) \
  (sizeof(SBlockData) + sizeof(SBlockColV##blkVer) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockStatisSize(int nCols, uint32_t blkVer) {
  switch (blkVer) {
    case TSDB_SBLK_VER_0:
    default:
      return TSDB_BLOCK_STATIS_SIZE(nCols, 0);
  }
}

#define TSDB_BLOCK_AGGR_SIZE(ncols, blkVer) (sizeof(SAggrBlkColV##blkVer) * (ncols) + sizeof(TSCKSUM))

static FORCE_INLINE size_t tsdbBlockAggrSize(int nCols, uint32_t blkVer) {
  switch (blkVer) {
    case TSDB_SBLK_VER_0:
    default:
      return TSDB_BLOCK_AGGR_SIZE(nCols, 0);
  }
}

int   tsdbInitReadH(SReadH *pReadh, STsdb *pRepo);
void  tsdbDestroyReadH(SReadH *pReadh);
int   tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet);
void  tsdbCloseAndUnsetFSet(SReadH *pReadh);
int   tsdbLoadBlockIdx(SReadH *pReadh);
int   tsdbSetReadTable(SReadH *pReadh, STable *pTable);
int   tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget);
int   tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlockInfo);
int   tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo, const int16_t *colIds, int numOfColsIds);
int   tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock);
int   tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx);
void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx);
void  tsdbGetBlockStatis(SReadH *pReadh, SDataStatis *pStatis, int numOfCols, SBlock *pBlock);

static FORCE_INLINE int tsdbMakeRoom(void **ppBuf, size_t size) {
  void * pBuf = *ppBuf;
  size_t tsize = taosTSizeof(pBuf);

  if (tsize < size) {
    if (tsize == 0) tsize = 1024;

    while (tsize < size) {
      tsize *= 2;
    }

    *ppBuf = taosTRealloc(pBuf, tsize);
    if (*ppBuf == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_TSDB_READ_IMPL_H_*/
