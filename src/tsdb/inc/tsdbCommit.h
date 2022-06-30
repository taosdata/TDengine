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

#ifndef _TD_TSDB_COMMIT_H_
#define _TD_TSDB_COMMIT_H_

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

#define TSDB_DEFAULT_BLOCK_ROWS(maxRows) ((maxRows)*4 / 5)

void  tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn);
int   tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord);
void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord);
void *tsdbCommitData(STsdbRepo *pRepo, bool end);
int   tsdbApplyRtnOnFSet(STsdbRepo *pRepo, SDFileSet *pSet, SRtn *pRtn);
int tsdbWriteBlockInfoImpl(SDFile *pHeadf, STable *pTable, SArray *pSupA, SArray *pSubA, void **ppBuf, SBlockIdx *pIdx);
int tsdbWriteBlockIdx(SDFile *pHeadf, SArray *pIdxA, void **ppBuf);
int   tsdbWriteBlockImpl(STsdbRepo *pRepo, STable *pTable, SDFile *pDFile, SDFile *pDFileAggr, SDataCols *pDataCols,
                         SBlock *pBlock, bool isLast, bool isSuper, void **ppBuf, void **ppCBuf, void **ppExBuf);
int   tsdbApplyRtn(STsdbRepo *pRepo);

// commit control command 
int tsdbCommitControl(STsdbRepo* pRepo, SControlDataInfo* pCtlDataInfo);

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

static FORCE_INLINE int TSDB_KEY_FID(TSKEY key, int32_t days, int8_t precision) {
int64_t fid;
  if (key < 0) {
    fid = ((key + 1) / tsTickPerDay[precision] / days - 1);
  } else {
    fid = ((key / tsTickPerDay[precision] / days));
  }

  // check fid over int max or min, set with int max or min
  if (fid > INT32_MAX) {
    fid = INT32_MAX;
  } else if(fid < INT32_MIN){
    fid = INT32_MIN;
  }
  return (int)fid;
}

#endif /* _TD_TSDB_COMMIT_H_ */