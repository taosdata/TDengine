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
#ifndef _BSE_INC_H_
#define _BSE_INC_H_
#include "bse.h"
#include "bseUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BSE_DEFAULT_BLOCK_SIZE (4 * 1024 * 1024)

struct SSeqRange {
  int64_t sseq;
  int64_t eseq;
};

struct SBlockItemInfo {
  int32_t size;
  int64_t seq;
};
struct SBseCommitInfo {
  int8_t  fmtVer;
  int32_t vgId;
  int64_t commitVer;
  int64_t lastVer;
  int64_t lastSeq;
  SArray *pFileList;
};

typedef struct {
  SArray  *pBatchList;
  bsequeue queue;
  void    *pBse;
} SBatchMgt;
struct SBse {
  char path[TSDB_FILENAME_LEN];

  int64_t        ver;
  uint64_t       seq;
  SBseCfg        cfg;
  TdThreadRwlock rwlock;
  TdThreadMutex  mutex;

  SBatchMgt      batchMgt[1];
  void          *pTableMgt;
  SBseCommitInfo commitInfo;

  int64_t latestSt;
  int64_t retention;
};

struct SBseBatch {
  int32_t  num;
  uint8_t *buf;
  int32_t  len;
  int32_t  cap;
  int64_t  seq;
  SArray  *pSeq;
  void    *pBse;
  int64_t  startSeq;
  int8_t   commited;
  bsequeue node;
};

int32_t bseGetAliveFileList(SBse *pBse, SArray **pFileList);

#ifdef __cplusplus
}
#endif
#endif
