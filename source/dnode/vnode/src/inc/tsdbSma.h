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

#ifndef _TD_TSDB_SMA_H_
#define _TD_TSDB_SMA_H_

// insert/update interface
int32_t tsdbInsertTSmaDataImpl(STsdb *pTsdb, STSma *param, STSmaData *pData);
int32_t tsdbInsertRSmaDataImpl(STsdb *pTsdb, SRSma *param, STSmaData *pData);


// query interface
// TODO: This is the basic params, and should wrap the params to a queryHandle.
int32_t tsdbGetTSmaDataImpl(STsdb *pTsdb, STSma *param, STSmaData *pData, STimeWindow *queryWin, int32_t nMaxResult);

// management interface
int32_t tsdbGetTSmaStatus(STsdb *pTsdb, STSma *param, void* result);
int32_t tsdbRemoveTSmaData(STsdb *pTsdb, STSma *param, STimeWindow *pWin);




// internal func
static FORCE_INLINE int32_t tsdbEncodeTSmaKey(uint64_t tableUid, col_id_t colId, TSKEY tsKey, void **pData) {
  int32_t len = 0;
  len += taosEncodeFixedU64(pData, tableUid);
  len += taosEncodeFixedU16(pData, colId);
  len += taosEncodeFixedI64(pData, tsKey);
  return len;
}

#if 0

typedef struct {
  int   minFid;
  int   midFid;
  int   maxFid;
  TSKEY minKey;
} SRtn;

typedef struct {
  uint64_t uid;
  int64_t  offset;
  int64_t  size;
} SKVRecord;

void tsdbGetRtnSnap(STsdb *pRepo, SRtn *pRtn);

static FORCE_INLINE int TSDB_KEY_FID(TSKEY key, int32_t days, int8_t precision) {
  if (key < 0) {
    return (int)((key + 1) / tsTickPerDay[precision] / days - 1);
  } else {
    return (int)((key / tsTickPerDay[precision] / days));
  }
}

static FORCE_INLINE int tsdbGetFidLevel(int fid, SRtn *pRtn) {
  if (fid >= pRtn->maxFid) {
    return 0;
  } else if (fid >= pRtn->midFid) {
    return 1;
  } else if (fid >= pRtn->minFid) {
    return 2;
  } else {
    return -1;
  }
}

#define TSDB_DEFAULT_BLOCK_ROWS(maxRows) ((maxRows)*4 / 5)

int   tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord);
void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord);
void *tsdbCommitData(STsdbRepo *pRepo);
int   tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn);
int tsdbWriteBlockInfoImpl(SDFile *pHeadf, STable *pTable, SArray *pSupA, SArray *pSubA, void **ppBuf, SBlockIdx *pIdx);
int tsdbWriteBlockIdx(SDFile *pHeadf, SArray *pIdxA, void **ppBuf);
int tsdbWriteBlockImpl(STsdbRepo *pRepo, STable *pTable, SDFile *pDFile, SDataCols *pDataCols, SBlock *pBlock,
                       bool isLast, bool isSuper, void **ppBuf, void **ppCBuf);
int   tsdbApplyRtn(STsdbRepo *pRepo);

#endif

#endif /* _TD_TSDB_SMA_H_ */