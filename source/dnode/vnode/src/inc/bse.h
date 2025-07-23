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

enum {
  kNoCompres = 0,
  kLZ4Compres = 1,
  kZLibCompres = 2,
  kZSTDCompres = 3,
  kZxCompress = 4,
};

typedef struct {
  int32_t vgId;
  int32_t encryptAlgorithm;
  char    encryptKey[ENCRYPT_KEY_LEN + 1];
  int8_t  compressType;
  int32_t blockSize;
  int8_t  clearUncommittedFile;
  int64_t keepDays;
  int32_t    keeps;
  SRetention retention;  // retention in seconds, 0 means no retention
  int8_t  precision;  // precision in seconds, 0 means no precision
  int32_t tableCacheSize;
  int32_t blockCacheSize;
} SBseCfg;

typedef struct SBse              SBse;
typedef struct SBseBatch         SBseBatch;
typedef struct SSeqRange         SSeqRange;
typedef struct SBseRawFileWriter SBseRawFileWriter;
typedef struct SBseSnapWriter    SBseSnapWriter;
typedef struct SBseSnapReader    SBseSnapReader;
typedef struct SBseCommitInfo    SBseCommitInfo;
typedef struct SBlockItemInfo    SBlockItemInfo;

// batch func
int32_t bseBatchInit(SBse *pBse, SBseBatch **pBatch, int32_t nKey);
int32_t bseBatchPut(SBseBatch *pBatch, int64_t *seq, uint8_t *value, int32_t len);
int32_t bseBatchGetSize(SBseBatch *pBatch, int32_t *size);
int32_t bseBatchDestroy(SBseBatch *pBatch);
int32_t bseCommitBatch(SBse *pBse, SBseBatch *pBatch);

int32_t bseUpdateCfg(SBse *pBse, SBseCfg *pCfg);

int32_t bseSetCompressType(SBse *pBse, int8_t compressType);
int32_t bseSetBlockSize(SBse *pBse, int32_t blockSize);
int32_t bseSetBlockCacheSize(SBse *pBse, int32_t blockCacheSize);
int32_t bseSetTableCacheSize(SBse *pBse, int32_t blockCacheSize);
int32_t bseSetKeepDays(SBse *pBse, int32_t keepDays);

#define BSE_GET_BLOCK_SIZE(p)       ((p)->cfg.blockSize)
#define BSE_GET_COMPRESS_TYPE(p)    ((p)->cfg.compressType)
#define BSE_GET_KEEPS_DAYS(p)       ((p)->cfg.keepDays)
#define BSE_GET_TABLE_CACHE_SIZE(p) ((p)->cfg.tableCacheSize)
#define BSE_GET_BLOCK_CACHE_SIZE(p) ((p)->cfg.blockCacheSize)
#define BSE_GET_VGID(p)             ((p)->cfg.vgId)

int32_t bseSnapWriterOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapWriter **writer);
int32_t bseSnapWriterWrite(SBseSnapWriter *writer, uint8_t *data, int32_t len);
int32_t bseSnapWriterClose(SBseSnapWriter **writer, int8_t rollback);

int32_t bseSnapReaderOpen(SBse *pBse, int64_t sver, int64_t ever, SBseSnapReader **reader);
int32_t bseSnapReaderRead(SBseSnapReader *reader, uint8_t **data);
int32_t bseSnapReaderRead2(SBseSnapReader *reader, uint8_t **data, int32_t *len);
int32_t bseSnapReaderClose(SBseSnapReader **reader);

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse);
void    bseClose(SBse *pBse);

// int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len);
int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len);
int32_t bseCommit(SBse *pBse);
int32_t bseRollback(SBse *pBse, int64_t ver);
// int32_t bseBeginSnapshot(SBse *pBse, int64_t ver);
// int32_t bseEndSnapshot(SBse *pBse);
// int32_t bseStopSnapshot(SBse *pBse);
int32_t bseCompact(SBse *pBse);
int32_t bseDelete(SBse *pBse, SSeqRange range);
int32_t bseReload(SBse *pBse);
int32_t bseTrim(SBse *pBse);

#ifdef __cplusplus
}
#endif

#endif