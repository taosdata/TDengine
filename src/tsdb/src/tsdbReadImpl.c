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

#include "tsdbint.h"

#define TSDB_KEY_COL_OFFSET 0

static void tsdbResetReadTable(SReadH *pReadh);
static void tsdbResetReadFile(SReadH *pReadh);
static int  tsdbLoadBlockDataImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols);
static int  tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, void *content, int32_t len, int8_t comp, int numOfRows,
                                         int maxPoints, char *buffer, int bufferSize);
static int  tsdbLoadBlockDataColsImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                      int numOfColIds);
static int  tsdbLoadColData(SReadH *pReadh, SDFile *pDFile, SBlock *pBlock, SBlockCol *pBlockCol, SDataCol *pDataCol);

int tsdbInitReadH(SReadH *pReadh, STsdbRepo *pRepo) {
  ASSERT(pReadh != NULL && pRepo != NULL);

  STsdbCfg *pCfg = REPO_CFG(pRepo);

  memset((void *)pReadh, 0, sizeof(*pReadh));
  pReadh->pRepo = pRepo;

  TSDB_FSET_SET_CLOSED(TSDB_READ_FSET(pReadh));

  pReadh->aBlkIdx = taosArrayInit(1024, sizeof(SBlockIdx));
  if (pReadh->aBlkIdx == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  pReadh->pDCols[0] = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pReadh->pDCols[0] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadH(pReadh);
    return -1;
  }

  pReadh->pDCols[1] = tdNewDataCols(0, 0, pCfg->maxRowsPerFileBlock);
  if (pReadh->pDCols[1] == NULL) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    tsdbDestroyReadH(pReadh);
    return -1;
  }

  return 0;
}

void tsdbDestroyReadH(SReadH *pReadh) {
  if (pReadh == NULL) return;

  pReadh->pCBuf = taosTZfree(pReadh->pCBuf);
  pReadh->pBuf = taosTZfree(pReadh->pBuf);
  pReadh->pDCols[0] = tdFreeDataCols(pReadh->pDCols[0]);
  pReadh->pDCols[1] = tdFreeDataCols(pReadh->pDCols[1]);
  pReadh->pBlkData = taosTZfree(pReadh->pBlkData);
  pReadh->pBlkInfo = taosTZfree(pReadh->pBlkInfo);
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
  pReadh->pTable = NULL;
  pReadh->aBlkIdx = taosArrayDestroy(pReadh->aBlkIdx);
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadh));
  pReadh->pRepo = NULL;
}

int tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet) {
  ASSERT(pSet != NULL);
  tsdbResetReadFile(pReadh);

  pReadh->rSet = *pSet;
  TSDB_FSET_SET_CLOSED(TSDB_READ_FSET(pReadh));
  if (tsdbOpenDFileSet(TSDB_READ_FSET(pReadh), O_RDONLY) < 0) return -1;

  return 0;
}

void tsdbCloseAndUnsetFSet(SReadH *pReadh) { tsdbResetReadFile(pReadh); }

int tsdbLoadBlockIdx(SReadH *pReadh) {
  SDFile *  pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx blkIdx;

  ASSERT(taosArrayGetSize(pReadh->aBlkIdx) == 0);

  // No data at all, just return
  if (pHeadf->info.offset <= 0) return 0;

  if (tsdbSeekDFile(pHeadf, pHeadf->info.offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while seek file %s sinces %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pHeadf->info.len) < 0) return -1;

  int64_t nread = tsdbReadDFile(pHeadf, TSDB_READ_BUF(pReadh), pHeadf->info.len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while read file %s sinces %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pHeadf->info.offset,
              pHeadf->info.len);
    return -1;
  }

  if (nread < pHeadf->info.len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockIdx part in file %s is corrupted, offset:%u expected bytes:%u read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), pHeadf->info.len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockIdx part in file %s is corrupted since wrong checksum, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pHeadf->info.offset, pHeadf->info.len);
    return -1;
  }

  void *ptr = TSDB_READ_BUF(pReadh);
  int   tsize = 0;
  while (POINTER_DISTANCE(ptr, TSDB_READ_BUF(pReadh)) < (pHeadf->info.len - sizeof(TSCKSUM))) {
    ptr = tsdbDecodeSBlockIdx(ptr, &blkIdx);
    ASSERT(ptr != NULL);

    if (taosArrayPush(pReadh->aBlkIdx, (void *)(&blkIdx)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }

    tsize++;
    ASSERT(tsize == 1 || ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 2))->tid <
                             ((SBlockIdx *)taosArrayGet(pReadh->aBlkIdx, tsize - 1))->tid);
  }

  return 0;
}

int tsdbSetReadTable(SReadH *pReadh, STable *pTable) {
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

  pReadh->pTable = pTable;

  if (tdInitDataCols(pReadh->pDCols[0], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  if (tdInitDataCols(pReadh->pDCols[1], pSchema) < 0) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  size_t size = taosArrayGetSize(pReadh->aBlkIdx);
  if (size > 0) {
    while (true) {
      if (pReadh->cidx >= size) {
        pReadh->pBlkIdx = NULL;
        break;
      }

      SBlockIdx *pBlkIdx = taosArrayGet(pReadh->aBlkIdx, pReadh->cidx);
      if (pBlkIdx->tid == TABLE_TID(pTable)) {
        if (pBlkIdx->uid == TABLE_UID(pTable)) {
          pReadh->pBlkIdx = pBlkIdx;
        } else {
          pReadh->pBlkIdx = NULL;
        }
        pReadh->cidx++;
        break;
      } else if (pBlkIdx->tid > TABLE_TID(pTable)) {
        pReadh->pBlkIdx = NULL;
        break;
      } else {
        pReadh->cidx++;
      }
    }
  } else {
    pReadh->pBlkIdx = NULL;
  }

  return 0;
}

int tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget) {
  ASSERT(pReadh->pBlkIdx != NULL);

  SDFile *   pHeadf = TSDB_READ_HEAD_FILE(pReadh);
  SBlockIdx *pBlkIdx = pReadh->pBlkIdx;

  if (tsdbSeekDFile(pHeadf, pBlkIdx->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockInfo part while seek file %s since %s, offset:%u len:%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (tsdbMakeRoom((void **)(&(pReadh->pBlkInfo)), pBlkIdx->len) < 0) return -1;

  int64_t nread = tsdbReadDFile(pHeadf, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load SBlockInfo part while read file %s sinces %s, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), tstrerror(terrno), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  if (nread < pBlkIdx->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockInfo part in file %s is corrupted, offset:%u expected bytes:%u read bytes:%" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)(pReadh->pBlkInfo), pBlkIdx->len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d SBlockInfo part in file %s is corrupted since wrong checksum, offset:%u len :%u",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pHeadf), pBlkIdx->offset, pBlkIdx->len);
    return -1;
  }

  ASSERT(pBlkIdx->tid == pReadh->pBlkInfo->tid && pBlkIdx->uid == pReadh->pBlkInfo->uid);

  if (pTarget) {
    memcpy(pTarget, (void *)(pReadh->pBlkInfo), pBlkIdx->len);
  }

  return 0;
}

int tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo) {
  ASSERT(pBlock->numOfSubBlocks > 0);

  SBlock *iBlock = pBlock;
  if (pBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = (SBlock *)POINTER_SHIFT(pBlkInfo, pBlock->offset);
    } else {
      iBlock = (SBlock *)POINTER_SHIFT(pReadh->pBlkInfo, pBlock->offset);
    }
  }

  if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[0]) < 0) return -1;
  for (int i = 1; i < pBlock->numOfSubBlocks; i++) {
    iBlock++;
    if (tsdbLoadBlockDataImpl(pReadh, iBlock, pReadh->pDCols[1]) < 0) return -1;
    if (tdMergeDataCols(pReadh->pDCols[0], pReadh->pDCols[1], pReadh->pDCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadh->pDCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadh->pDCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadh->pDCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pBlkInfo, int16_t *colIds, int numOfColsIds) {
  ASSERT(pBlock->numOfSubBlocks > 0);

  SBlock *iBlock = pBlock;
  if (pBlock->numOfSubBlocks > 1) {
    if (pBlkInfo) {
      iBlock = POINTER_SHIFT(pBlkInfo, pBlock->offset);
    } else {
      iBlock = POINTER_SHIFT(pReadh->pBlkInfo, pBlock->offset);
    }
  }

  if (tsdbLoadBlockDataColsImpl(pReadh, iBlock, pReadh->pDCols[0], colIds, numOfColsIds) < 0) return -1;
  for (int i = 1; i < pBlock->numOfSubBlocks; i++) {
    iBlock++;
    if (tsdbLoadBlockDataColsImpl(pReadh, iBlock, pReadh->pDCols[1], colIds, numOfColsIds) < 0) return -1;
    if (tdMergeDataCols(pReadh->pDCols[0], pReadh->pDCols[1], pReadh->pDCols[1]->numOfRows) < 0) return -1;
  }

  ASSERT(pReadh->pDCols[0]->numOfRows == pBlock->numOfRows);
  ASSERT(dataColsKeyFirst(pReadh->pDCols[0]) == pBlock->keyFirst);
  ASSERT(dataColsKeyLast(pReadh->pDCols[0]) == pBlock->keyLast);

  return 0;
}

int tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock) {
  ASSERT(pBlock->numOfSubBlocks <= 1);

  SDFile *pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);

  if (tsdbSeekDFile(pDFile, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block statis part while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tstrerror(terrno));
    return -1;
  }

  size_t size = TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols);
  if (tsdbMakeRoom((void **)(&(pReadh->pBlkData)), size) < 0) return -1;

  int64_t nread = tsdbReadDFile(pDFile, (void *)(pReadh->pBlkData), size);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block statis part while read file %s sinces %s, offset:%" PRId64 " len :%" PRIzu,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), (int64_t)pBlock->offset, size);
    return -1;
  }

  if (nread < size) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted, offset:%" PRId64 " expected bytes:%" PRIzu
              " read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, size, nread);
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)(pReadh->pBlkData), size)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted since wrong checksum, offset:%" PRId64 " len :%" PRIzu,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, size);
    return -1;
  }

  return 0;
}

int tsdbEncodeSBlockIdx(void **buf, SBlockIdx *pIdx) {
  int tlen = 0;

  tlen += taosEncodeVariantI32(buf, pIdx->tid);
  tlen += taosEncodeVariantU32(buf, pIdx->len);
  tlen += taosEncodeVariantU32(buf, pIdx->offset);
  tlen += taosEncodeFixedU8(buf, pIdx->hasLast);
  tlen += taosEncodeVariantU32(buf, pIdx->numOfBlocks);
  tlen += taosEncodeFixedU64(buf, pIdx->uid);
  tlen += taosEncodeFixedU64(buf, pIdx->maxKey);

  return tlen;
}

void *tsdbDecodeSBlockIdx(void *buf, SBlockIdx *pIdx) {
  uint8_t  hasLast = 0;
  uint32_t numOfBlocks = 0;
  uint64_t value = 0;

  if ((buf = taosDecodeVariantI32(buf, &(pIdx->tid))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->len))) == NULL) return NULL;
  if ((buf = taosDecodeVariantU32(buf, &(pIdx->offset))) == NULL) return NULL;
  if ((buf = taosDecodeFixedU8(buf, &(hasLast))) == NULL) return NULL;
  pIdx->hasLast = hasLast;
  if ((buf = taosDecodeVariantU32(buf, &(numOfBlocks))) == NULL) return NULL;
  pIdx->numOfBlocks = numOfBlocks;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->uid = (int64_t)value;
  if ((buf = taosDecodeFixedU64(buf, &value)) == NULL) return NULL;
  pIdx->maxKey = (TSKEY)value;

  return buf;
}

void tsdbGetBlockStatis(SReadH *pReadh, SDataStatis *pStatis, int numOfCols) {
  SBlockData *pBlockData = pReadh->pBlkData;

  for (int i = 0, j = 0; i < numOfCols;) {
    if (j >= pBlockData->numOfCols) {
      pStatis[i].numOfNull = -1;
      i++;
      continue;
    }

    if (pStatis[i].colId == pBlockData->cols[j].colId) {
      pStatis[i].sum = pBlockData->cols[j].sum;
      pStatis[i].max = pBlockData->cols[j].max;
      pStatis[i].min = pBlockData->cols[j].min;
      pStatis[i].maxIndex = pBlockData->cols[j].maxIndex;
      pStatis[i].minIndex = pBlockData->cols[j].minIndex;
      pStatis[i].numOfNull = pBlockData->cols[j].numOfNull;
      i++;
      j++;
    } else if (pStatis[i].colId < pBlockData->cols[j].colId) {
      pStatis[i].numOfNull = -1;
      i++;
    } else {
      j++;
    }
  }
}

static void tsdbResetReadTable(SReadH *pReadh) {
  tdResetDataCols(pReadh->pDCols[0]);
  tdResetDataCols(pReadh->pDCols[1]);
  pReadh->cidx = 0;
  pReadh->pBlkIdx = NULL;
  pReadh->pTable = NULL;
}

static void tsdbResetReadFile(SReadH *pReadh) {
  tsdbResetReadTable(pReadh);
  taosArrayClear(pReadh->aBlkIdx);
  tsdbCloseDFileSet(TSDB_READ_FSET(pReadh));
}

static int tsdbLoadBlockDataImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols) {
  ASSERT(pBlock->numOfSubBlocks >= 0 && pBlock->numOfSubBlocks <= 1);

  SDFile *pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);

  tdResetDataCols(pDataCols);
  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pBlock->len) < 0) return -1;

  SBlockData *pBlockData = (SBlockData *)TSDB_READ_BUF(pReadh);

  if (tsdbSeekDFile(pDFile, pBlock->offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block data part while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadh), pBlock->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block data part while read file %s sinces %s, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), (int64_t)pBlock->offset,
              pBlock->len);
    return -1;
  }

  if (nread < pBlock->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block data part in file %s is corrupted, offset:%" PRId64
              " expected bytes:%d read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, pBlock->len, nread);
    return -1;
  }

  int32_t tsize = TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols);
  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), tsize)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block statis part in file %s is corrupted since wrong checksum, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), (int64_t)pBlock->offset, tsize);
    return -1;
  }

  ASSERT(tsize < pBlock->len);
  ASSERT(pBlockData->numOfCols == pBlock->numOfCols);

  pDataCols->numOfRows = pBlock->numOfRows;

  // Recover the data
  int ccol = 0;  // loop iter for SBlockCol object
  int dcol = 0;  // loop iter for SDataCols object
  while (dcol < pDataCols->numOfCols) {
    SDataCol *pDataCol = &(pDataCols->cols[dcol]);
    if (dcol != 0 && ccol >= pBlockData->numOfCols) {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
      continue;
    }

    int16_t tcolId = 0;
    int32_t toffset = TSDB_KEY_COL_OFFSET;
    int32_t tlen = pBlock->keyLen;

    if (dcol != 0) {
      SBlockCol *pBlockCol = &(pBlockData->cols[ccol]);
      tcolId = pBlockCol->colId;
      toffset = pBlockCol->offset;
      tlen = pBlockCol->len;
    } else {
      ASSERT(pDataCol->colId == tcolId);
    }

    if (tcolId == pDataCol->colId) {
      if (pBlock->algorithm == TWO_STAGE_COMP) {
        int zsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;
        if (tsdbMakeRoom((void **)(&TSDB_READ_COMP_BUF(pReadh)), zsize) < 0) return -1;
      }

      if (tsdbCheckAndDecodeColumnData(pDataCol, POINTER_SHIFT(pBlockData, tsize + toffset), tlen, pBlock->algorithm,
                                       pBlock->numOfRows, pDataCols->maxPoints, TSDB_READ_COMP_BUF(pReadh),
                                       taosTSizeof(TSDB_READ_COMP_BUF(pReadh))) < 0) {
        tsdbError("vgId:%d file %s is broken at column %d block offset %" PRId64 " column offset %d",
                  TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tcolId, (int64_t)pBlock->offset, toffset);
        return -1;
      }

      if (dcol != 0) {
        ccol++;
      }
      dcol++;
    } else if (tcolId < pDataCol->colId) {
      ccol++;
    } else {
      // Set current column as NULL and forward
      dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
      dcol++;
    }
  }

  return 0;
}

static int tsdbCheckAndDecodeColumnData(SDataCol *pDataCol, void *content, int32_t len, int8_t comp, int numOfRows,
                                        int maxPoints, char *buffer, int bufferSize) {
  if (!taosCheckChecksumWhole((uint8_t *)content, len)) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    return -1;
  }

  // Decode the data
  if (comp) {
    // Need to decompress
    int tlen = (*(tDataTypeDesc[pDataCol->type].decompFunc))(content, len - sizeof(TSCKSUM), numOfRows, pDataCol->pData,
                                                             pDataCol->spaceSize, comp, buffer, bufferSize);
    if (tlen <= 0) {
      tsdbError("Failed to decompress column, file corrupted, len:%d comp:%d numOfRows:%d maxPoints:%d bufferSize:%d",
                len, comp, numOfRows, maxPoints, bufferSize);
      terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
      return -1;
    }
    pDataCol->len = tlen;
  } else {
    // No need to decompress, just memcpy it
    pDataCol->len = len - sizeof(TSCKSUM);
    memcpy(pDataCol->pData, content, pDataCol->len);
  }

  if (IS_VAR_DATA_TYPE(pDataCol->type)) {
    dataColSetOffset(pDataCol, numOfRows);
  }
  return 0;
}

static int tsdbLoadBlockDataColsImpl(SReadH *pReadh, SBlock *pBlock, SDataCols *pDataCols, int16_t *colIds,
                                     int numOfColIds) {
  ASSERT(pBlock->numOfSubBlocks <= 1);
  ASSERT(colIds[0] == 0);

  SDFile *  pDFile = (pBlock->last) ? TSDB_READ_LAST_FILE(pReadh) : TSDB_READ_DATA_FILE(pReadh);
  SBlockCol blockCol = {0};

  tdResetDataCols(pDataCols);

  // If only load timestamp column, no need to load SBlockData part
  if (numOfColIds > 1 && tsdbLoadBlockStatis(pReadh, pBlock) < 0) return -1;

  pDataCols->numOfRows = pBlock->numOfRows;

  int dcol = 0;
  int ccol = 0;
  for (int i = 0; i < numOfColIds; i++) {
    int16_t    colId = colIds[i];
    SDataCol * pDataCol = NULL;
    SBlockCol *pBlockCol = NULL;

    while (true) {
      if (dcol >= pDataCols->numOfCols) {
        pDataCol = NULL;
        break;
      }
      pDataCol = &pDataCols->cols[dcol];
      if (pDataCol->colId > colId) {
        pDataCol = NULL;
        break;
      } else {
        dcol++;
        if (pDataCol->colId == colId) break;
      }
    }

    if (pDataCol == NULL) continue;
    ASSERT(pDataCol->colId == colId);

    if (colId == 0) {  // load the key row
      blockCol.colId = colId;
      blockCol.len = pBlock->keyLen;
      blockCol.type = pDataCol->type;
      blockCol.offset = TSDB_KEY_COL_OFFSET;
      pBlockCol = &blockCol;
    } else {  // load non-key rows
      while (true) {
        if (ccol >= pBlock->numOfCols) {
          pBlockCol = NULL;
          break;
        }

        pBlockCol = &(pReadh->pBlkData->cols[ccol]);
        if (pBlockCol->colId > colId) {
          pBlockCol = NULL;
          break;
        } else {
          ccol++;
          if (pBlockCol->colId == colId) break;
        }
      }

      if (pBlockCol == NULL) {
        dataColSetNEleNull(pDataCol, pBlock->numOfRows, pDataCols->maxPoints);
        continue;
      }

      ASSERT(pBlockCol->colId == pDataCol->colId);
    }

    if (tsdbLoadColData(pReadh, pDFile, pBlock, pBlockCol, pDataCol) < 0) return -1;
  }

  return 0;
}

static int tsdbLoadColData(SReadH *pReadh, SDFile *pDFile, SBlock *pBlock, SBlockCol *pBlockCol, SDataCol *pDataCol) {
  ASSERT(pDataCol->colId == pBlockCol->colId);

  STsdbRepo *pRepo = TSDB_READ_REPO(pReadh);
  STsdbCfg * pCfg = REPO_CFG(pRepo);
  int        tsize = pDataCol->bytes * pBlock->numOfRows + COMP_OVERFLOW_BYTES;

  if (tsdbMakeRoom((void **)(&TSDB_READ_BUF(pReadh)), pBlockCol->len) < 0) return -1;
  if (tsdbMakeRoom((void **)(&TSDB_READ_COMP_BUF(pReadh)), tsize) < 0) return -1;

  int64_t offset = pBlock->offset + TSDB_BLOCK_STATIS_SIZE(pBlock->numOfCols) + pBlockCol->offset;
  if (tsdbSeekDFile(pDFile, offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load block column data while seek file %s to offset %" PRId64 " since %s",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), offset, tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadh), pBlockCol->len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load block column data while read file %s sinces %s, offset:%" PRId64 " len :%d",
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), tstrerror(terrno), offset, pBlockCol->len);
    return -1;
  }

  if (nread < pBlockCol->len) {
    terrno = TSDB_CODE_TDB_FILE_CORRUPTED;
    tsdbError("vgId:%d block column data in file %s is corrupted, offset:%" PRId64 " expected bytes:%d" PRIzu
              " read bytes: %" PRId64,
              TSDB_READ_REPO_ID(pReadh), TSDB_FILE_FULL_NAME(pDFile), offset, pBlockCol->len, nread);
    return -1;
  }

  if (tsdbCheckAndDecodeColumnData(pDataCol, pReadh->pBuf, pBlockCol->len, pBlock->algorithm, pBlock->numOfRows,
                                   pCfg->maxRowsPerFileBlock, pReadh->pCBuf, (int32_t)taosTSizeof(pReadh->pCBuf)) < 0) {
    tsdbError("vgId:%d file %s is broken at column %d offset %" PRId64, REPO_ID(pRepo), TSDB_FILE_FULL_NAME(pDFile),
              pBlockCol->colId, offset);
    return -1;
  }

  return 0;
}