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

#ifndef _TD_TSDB_RECOVER_H_
#define _TD_TSDB_RECOVER_H_

// typedef struct {
//   int   minFid;
//   int   midFid;
//   int   maxFid;
//   TSKEY minKey;
// } SRtn;

// typedef struct {
//   //   uint64_t uid;
//   //   int64_t  offset;
//   //   int64_t  size;
// } SKVRecord;

// typedef struct {
//   SRtn rtn;  // retention snapshot
//   SFSIter      fsIter;  // tsdb file iterator
//   int          niters;  // memory iterators
//   SCommitIter *iters;
//   bool         isRFileSet;  // read and commit FSET
//   SReadH       readh;
//   SDFileSet    wSet;
//   bool         isDFileSame;
//   bool         isLFileSame;
//   TSKEY        minKey;
//   TSKEY        maxKey;
//   SArray *     aBlkIdx;  // SBlockIdx array
//   STable *     pTable;
//   SArray *     aSupBlk;  // Table super-block array
//   SArray *     aSubBlk;  // table sub-block array
//   SDataCols *  pDataCols;
// } SRecoverH;

// #define TSDB_COMMIT_WRITE_FSET(ch) (&((ch)->wSet))
// #define TSDB_COMMIT_HEAD_FILE(ch) TSDB_DFILE_IN_SET(TSDB_COMMIT_WRITE_FSET(ch), TSDB_FILE_HEAD)

// void  tsdbGetRtnSnap(STsdbRepo *pRepo, SRtn *pRtn);
// int   tsdbEncodeKVRecord(void **buf, SKVRecord *pRecord);
// void *tsdbDecodeKVRecord(void *buf, SKVRecord *pRecord);
// void *tsdbCommitData(STsdbRepo *pRepo);
// int   tsdbInitCommitH(SCommitH *pCommith, STsdbRepo *pRepo);
// void  tsdbDestroyCommitH(SCommitH *pCommith);
// int   tsdbCommitAddBlock(SCommitH *pCommith, const SBlock *pSupBlock, const SBlock *pSubBlocks, int nSubBlocks);
// int   tsdbWriteBlockInfo(SCommitH *pCommih);
// int   tsdbWriteBlockIdx(SCommitH *pCommih);

// static FORCE_INLINE int tsdbGetFidLevel(int fid, SRtn *pRtn) {
//   if (fid >= pRtn->maxFid) {
//     return 0;
//   } else if (fid >= pRtn->midFid) {
//     return 1;
//   } else if (fid >= pRtn->minFid) {
//     return 2;
//   } else {
//     return -1;
//   }
// }

/**
 * check and recover data
 */
int tsdbRecoverDataMain(STsdbRepo *pRepo);

#endif /* _TD_TSDB_RECOVER_H_ */