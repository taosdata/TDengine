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

#define BSE_DEFAULT_BLOCK_SIZE (16 << 20)

struct SSeqRange {
  int64_t sseq;
  int64_t eseq;
};

int8_t seqRangeContains(struct SSeqRange *p, int64_t seq);
void   seqRangeReset(struct SSeqRange *p);
void   seqRangeUpdate(struct SSeqRange *dst, struct SSeqRange *src);
int8_t seqRangeIsGreater(struct SSeqRange *p, int64_t seq);

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
  // SRetention     retention;
  int64_t latestSt;
  // int32_t        keepDays;
  // int32_t        keeps;
  // int8_t         precision;
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

typedef struct {
  void   *data;
  int32_t cap;
  int8_t  type;
  int64_t size;
  int8_t  compressType;

  void    *pCachItem;
  uint8_t *kvBuffer;  // meta handle, used for table reader
  int32_t  kvSize;
  int32_t  kvCap;
} SBlockWrapper;

int32_t blockWrapperInit(SBlockWrapper *p, int32_t cap);
int32_t blockWrapperPushMeta(SBlockWrapper *p, int64_t seq, uint8_t *value, int32_t len);
void    blockWrapperClearMeta(SBlockWrapper *p);
void    blockWrapperCleanup(SBlockWrapper *p);
int32_t blockWrapperResize(SBlockWrapper *p, int32_t cap);
void    blockWrapperClear(SBlockWrapper *p);
void    blockWrapperTransfer(SBlockWrapper *dst, SBlockWrapper *src);
void    blockWrapperSetType(SBlockWrapper *p, int8_t type);

int32_t blockWrapperSize(SBlockWrapper *p, int32_t extra);
int32_t blockWrapperSeek(SBlockWrapper *p, int64_t tgt, uint8_t **pValue, int32_t *len);

typedef struct {
  SRWLatch         latch;
  int32_t          ref;
  struct SSeqRange range;
  struct SSeqRange tableRange;
  SArray          *pMetaHandle;
  SBlockWrapper    pBlockWrapper;
  void            *pBse;
  void            *pTableBuilder;
} STableMemTable;

int32_t bseMemTableCreate(STableMemTable **ppMemTable, int32_t cap);
void    bseMemTableDestroy(STableMemTable *pMemTable);

int32_t bseMemTableRef(STableMemTable *pMemTable);
void    bseMemTableUnRef(STableMemTable *pMemTable);

int32_t bseMemTablePush(STableMemTable *pMemTable, void *pHandle);

int32_t bseMemTablGetMetaBlock(STableMemTable *pMetaTable, SArray **pMetaBlock);

int32_t bseGetAliveFileList(SBse *pBse, SArray **pFileList, int8_t lock);
#ifdef __cplusplus
}
#endif
#endif
