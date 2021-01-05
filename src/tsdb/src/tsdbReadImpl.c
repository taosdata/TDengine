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

#include "tchecksum.h"
#include "tsdbMain.h"

int tsdbInitReadH(SReadH *pReadh, STsdbRepo *pRepo) {
  // TODO
  return 0;
}

void tsdbDestroyReadH(SReadH *pReadh) {
  // TODO
}

int tsdbSetAndOpenReadFSet(SReadH *pReadh, SDFileSet *pSet) {
  // TODO
  return 0;
}

void tsdbCloseAndUnsetFSet(SReadH *pReadh) {
  // TODO
}

int tsdbLoadBlockIdx(SReadH *pReadh) {
  SDFile *  pDFile = TSDB_DFILE_IN_SET(TSDB_READ_FSET(pReadh));
  SBlockIdx blkIdx;

  if (tsdbSeekDFile(pDFile, pDFile->info.offset, SEEK_SET) < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while seek file %s sinces %s", TSDB_READ_REPO_ID(pReadh), ,
              tstrerror(terrno));
    return -1;
  }

  int64_t nread = tsdbReadDFile(pDFile, TSDB_READ_BUF(pReadh), pDFile->info.len);
  if (nread < 0) {
    tsdbError("vgId:%d failed to load SBlockIdx part while seek file %s sinces %s", TSDB_READ_REPO_ID(pReadh), ,
              tstrerror(terrno));
    return -1;
  }

  if (nread < pDFile->info.len) {
    tsdbError("vgId:%d failed to load SBlockIdx part while seek file %s sinces %s", TSDB_READ_REPO_ID(pReadh), ,
              tstrerror(terrno));
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)TSDB_READ_BUF(pReadh), pDFile->info.len)) {
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return -1;
  }

  void *ptr = TSDB_READ_BUF(pReadh);
  while (POINTER_DISTANCE(ptr, TSDB_READ_BUF(pReadh)) < (pDFile->info.len - sizeof(TSCKSUM))) {
    ptr = tsdbDecodeSBlockIdx(ptr, &blkIdx);

    if (taosArrayPush(pReadh->aBlcIdx, (void *)(&blkIdx)) < 0) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return -1;
    }
  }

  return 0;
}

int tsdbSetReadTable(SReadH *pReadh, STable *pTable) {
  STSchema *pSchema = tsdbGetTableSchemaImpl(pTable, false, false, -1);

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
        pReadh->pBlockIdx = NULL;
        break;
      }

      SBlockIdx *pBlkIdx = taosArrayGet(pReadh->aBlkIdx, pReadh->cidx);
      if (pBlkIdx->tid == TABLE_TID(pTable)) {
        if (pBlkIdx->uid == TABLE_UID(pTable)) {
          pReadh->pBlockIdx = pBlkIdx;
        } else {
          pReadh->pBlockIdx = NULL;
        }
        pReadh->cidx++;
        break;
      } else if (pBlkIdx->tid > TABLE_TID(pTable)) {
        pReadh->pBlockIdx = NULL;
        break;
      } else {
        pReadh->cidx++;
      }
    }
  } else {
    pReadh->pBlockIdx = NULL;
  }

  return 0;
}

int tsdbLoadBlockInfo(SReadH *pReadh, void *pTarget) {
  // TODO
  return 0;
}

int tsdbLoadBlockData(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pInfo) {
  // TODO
  return 0;
}

int tsdbLoadBlockDataCols(SReadH *pReadh, SBlock *pBlock, SBlockInfo *pInfo, int16_t *colIds, int numOfColsIds) {
  // TODO
  return 0;
}

int tsdbLoadBlockStatis(SReadH *pReadh, SBlock *pBlock) {
  // TODO
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
