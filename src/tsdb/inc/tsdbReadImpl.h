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

typedef struct {
  int32_t  delimiter;  // For recovery usage
  int32_t  tid;
  uint64_t uid;
  SBlock   blocks[];
} SBlockInfo;

typedef struct {
  int16_t  colId;
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

// Code here just for back-ward compatibility
static FORCE_INLINE void tsdbSetBlockColOffset(SBlockCol *pBlockCol, uint32_t offset) {
  pBlockCol->offset = offset & ((((uint32_t)1) << 24) - 1);
  pBlockCol->offsetH = (uint8_t)(offset >> 24);
}

static FORCE_INLINE uint32_t tsdbGetBlockColOffset(SBlockCol *pBlockCol) {
  uint32_t offset1 = pBlockCol->offset;
  uint32_t offset2 = pBlockCol->offsetH;
  return (offset1 | (offset2 << 24));
}

typedef struct {
  int32_t   delimiter;  // For recovery usage
  int32_t   numOfCols;  // For recovery usage
  uint64_t  uid;        // For recovery usage
  SBlockCol cols[];
} SBlockData;

struct SReadH {
  STsdbRepo * pRepo;
  SDFileSet   rSet;     // FSET to read
  SArray *    aBlkIdx;  // SBlockIdx array
  STable *    pTable;   // table to read
  SBlockIdx * pBlkIdx;  // current reading table SBlockIdx
  int         cidx;
  SBlockInfo *pBlkInfo;
  SBlockData *pBlkData;  // Block info
  SDataCols * pDCols[2];
  void *      pBuf;   // buffer
  void *      pCBuf;  // compression buffer
};

#define TSDB_READ_REPO(rh) ((rh)->pRepo)
#define TSDB_READ_REPO_ID(rh) REPO_ID(TSDB_READ_REPO(rh))
#define TSDB_READ_FSET(rh) (&((rh)->rSet))
#define TSDB_READ_TABLE(rh) ((rh)->pTable)
#define TSDB_READ_HEAD_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_HEAD)
#define TSDB_READ_DATA_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_DATA)
#define TSDB_READ_LAST_FILE(rh) TSDB_DFILE_IN_SET(TSDB_READ_FSET(rh), TSDB_FILE_LAST)
#define TSDB_READ_BUF(rh) ((rh)->pBuf)
#define TSDB_READ_COMP_BUF(rh) ((rh)->pCBuf)

#define TSDB_BLOCK_STATIS_SIZE(ncols) (sizeof(SBlockData) + sizeof(SBlockCol) * (ncols) + sizeof(TSCKSUM))

int   tsdbInitReadH(SReadH *pReadh, STsdbRepo *pRepo);
void  tsdbDestroyReadH(SReadH *pReadh);
int   tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet);
void  tsdbCloseAndUnsetFSet(SReadH *pReadh);
int   tsdbLoadBlockIdx(SReadH *pReadh);
int   tsdbSetReadTable(SReadH *pReadh, STable *pTable);
int   tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget);
int   tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlockInfo);
int   tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo, int16_t *colIds, int numOfColsIds);
int   tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock);
int   tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx);
void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx);
void  tsdbGetBlockStatis(SReadH *pReadh, SDataStatis *pStatis, int numOfCols);

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

#endif /*_TD_TSDB_READ_IMPL_H_*/