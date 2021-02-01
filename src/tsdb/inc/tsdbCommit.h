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

void  tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn);
int   tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord);
void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord);
void *tsdbCommitData(STsdbRepo *pRepo);

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

#endif /* _TD_TSDB_COMMIT_H_ */