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
#ifndef _TD_VNODE_BSE_H_
#define _TD_VNODE_BSE_H_

#include "os.h"
#include "tchecksum.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  kNoCompres = 0,
  kLZ4Compres = 1,
  kZSTDCompres = 2,
  kZLibCompres = 4,
  kZxCompress = 8,
} SBseCompress;

typedef struct {
  uint64_t offset;
  int32_t  size;
  int32_t  vlen;
  int32_t  seq;
} SBlockItemInfo;

typedef struct {
  int32_t vgId;
  int64_t commitVer;
  int64_t lastVer;
  int64_t lastSeq;
  SArray *pFileList;
} SBseCommitInfo;
typedef struct {
  int32_t vgId;
  int64_t committed;
  int32_t encryptAlgorithm;
  char    encryptKey[ENCRYPT_KEY_LEN + 1];
  int8_t  clearFiles;
  int8_t  compressType;
  int32_t blockSize;
} SBseCfg;

typedef struct {
  char path[TSDB_FILENAME_LEN];

  int64_t        ver;
  uint64_t       seq;
  SBseCfg        cfg;
  TdThreadRwlock rwlock;
  TdThreadMutex  mutex;

  SArray        *pBatchList;
  void          *pTableMgt;
  SBseCommitInfo commitInfo;
} SBse;

typedef struct {
  int32_t  num;
  uint8_t *buf;
  int32_t  len;
  int32_t  cap;
  int64_t  seq;
  SArray  *pSeq;
  void    *pBse;
} SBseBatch;

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse);
int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len);
int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len);
int32_t bseCommit(SBse *pBse);
int32_t bseRollback(SBse *pBse, int64_t ver);
int32_t bseBeginSnapshot(SBse *pBse, int64_t ver);
int32_t bseEndSnapshot(SBse *pBse);
int32_t bseStopSnapshot(SBse *pBse);
void    bseClose(SBse *pBse);

int32_t bseAppendBatch(SBse *pBse, SBseBatch *pBatch);
int32_t bseBatchInit(SBse *pBse, SBseBatch **pBatch, int32_t nKey);

int32_t bseBatchPut(SBseBatch *pBatch, int64_t *seq, uint8_t *value, int32_t len);
int32_t bseBatchGetSize(SBseBatch *pBatch, int32_t *size);
int32_t bseBatchDestroy(SBseBatch *pBatch);

#ifdef __cplusplus
}
#endif

#endif