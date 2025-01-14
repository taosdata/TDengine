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

#include "os.h"
#include "tlog.h"
#include "tmsg.h"

#define kMaxEncodeLen 20
#define kEncodeLen    (2 * (kMaxEncodeLen) + 8)

#define kMagicNumber 0xdb4775248b80fb57ull;
#define kMagicNum    0x123456

#define kBlockTrailerSize (1 + 1 + 4)  // complete flag + compress type + crc unmask

static int32_t kBlockCap = 1024 * 16;

typedef struct {
  uint64_t offset;
  int32_t  size;
} SValueInfo;

typedef struct {
  uint64_t offset;
  uint64_t size;
} SBlkHandle;

typedef struct {
  uint32_t head[4];
  int8_t   type;
  uint32_t len;
  uint32_t cap;
  char    *data;
} SBlkData;

typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;

typedef struct {
  TdFilePtr    pDataFile;
  TdFilePtr    pIdxFile;
  SBlkData     data;
  SBlkData     tdata;
  STableFooter footer;
} STableBuilder;

typedef struct {
  char    path[TSDB_FILENAME_LEN];
  int64_t ver;

  STableBuilder *pTableBuilder;
  SHashObj      *pTableCache;
  TdThreadMutex  mutex;
  uint64_t       seq;
  uint64_t       commitSeq;
  SHashObj      *pSeqOffsetCache;
} SBse;

typedef struct {
  int32_t vgId;
  int32_t fsyncPeriod;
  int32_t retentionPeriod;  // secs
  int32_t rollPeriod;       // secs
  int64_t retentionSize;
  int64_t segSize;
  int64_t committed;
  int32_t encryptAlgorithm;
  char    encryptKey[ENCRYPT_KEY_LEN + 1];
  int8_t  clearFiles;
} SBseCfg;

static void bseBuildDataFullName(SBse *pBse, char *name);
static void bseBuildIndexFullName(SBse *pBse, char *name);

static int32_t tableOpen(char *name, STableBuilder **pTable);
static int32_t tableClose(STableBuilder *pTable);
static int32_t tableAppend(STableBuilder *pTable, uint64_t key, uint8_t *value, int32_t len, uint64_t *offset);
static int32_t tableGet(STableBuilder *pTable, uint64_t key, uint8_t **pValue, int32_t *len);
static int32_t tableFlush(STableBuilder *pTable);
static int32_t tableCommit(STableBuilder *pTable);
static int32_t tableLoadBlk(STableBuilder *pTable, uint32_t blockId, SBlkData *blk);
typedef struct {
  uint8_t  type;
  uint32_t len;
  uint32_t num;
} SBlkHeader;

// data block func
static int32_t blockInit(int32_t cap, int8_t type, SBlkData *blk);
static int32_t blockCleanup(SBlkData *data);
static int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len);
static int64_t blockEstimateSize(SBlkData *data);
static int8_t  blockShouldFlush(SBlkData *data, int32_t len);
static int32_t blockReset(SBlkData *data);
static int32_t blockSeek(SBlkData *data, uint64_t key, uint32_t blockId, uint8_t *pValue, int32_t *len);

// block handle func
static int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf);
static int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf);

// table footer func
static int32_t footerEncode(STableFooter *pFooter, char *buf);
static int32_t footerDecode(STableFooter *pFooter, char *buf);

int32_t blkHandleEncode(SBlkHandle *pHandle, char *buf) {
  char   *p = buf;
  int32_t tlen = 0;
  tlen += taosEncodeVariantU64((void **)&p, pHandle->offset);
  tlen += taosEncodeVariantU64((void **)&p, pHandle->size);
  return tlen;
}
int32_t blkHandleDecode(SBlkHandle *pHandle, char *buf) {
  char *p = buf;
  p = taosDecodeVariantU64(p, &pHandle->offset);
  p = taosDecodeVariantU64(p, &pHandle->size);
  return p - buf;
}

int32_t footerEncode(STableFooter *pFooter, char *buf) {
  char   *p = buf;
  int32_t len = 0;
  len += blkHandleEncode(pFooter->metaHandle, p + len);
  len += blkHandleEncode(pFooter->indexHandle, p + len);
  p = p + len;
  taosEncodeFixedU32((void **)&p, kMagicNum);
  taosEncodeFixedU32((void **)&p, kMagicNum);
  return 0;
}
int32_t footerDecode(STableFooter *pFooter, char *buf) {
  int32_t  code = 0;
  char    *p = buf;
  char    *mp = p + strlen(p) - kEncodeLen - 8;
  uint32_t ml, mh;
  taosDecodeFixedU32(mp, &ml);
  taosDecodeFixedU32(mp + 4, &mh);

  if (ml != kMagicNum || mh != kMagicNum) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t len = blkHandleDecode(pFooter->metaHandle, buf);
  if (len < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  len = blkHandleDecode(pFooter->indexHandle, buf + len);
  if (len < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return code;
}

int32_t blockInit(int32_t cap, int8_t type, SBlkData *blk) {
  blk->type = type;
  blk->len = 0;
  blk->cap = cap;
  blk->data = taosMemoryMalloc(cap);
  if (blk->data == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len) {
  int32_t code = 0;
  char   *p = blk->data + blk->len;
  blk->len += taosEncodeVariantU64((void **)&p, key);
  blk->len += taosEncodeVariantU32((void **)&p, len);
  blk->len += taosEncodeBinary((void **)&p, value, len);
  return code;
}

int64_t blockEstimateSize(SBlkData *data) { return data->len + kBlockTrailerSize; }

int8_t blockShouldFlush(SBlkData *data, int32_t len) {
  // 0 not flush,1 flush
  return data->len + kBlockTrailerSize + len + 12 >= kBlockCap;
}

int32_t blockReset(SBlkData *data) {
  memset(data->head, 0, sizeof(data->head));
  data->type = 0;
  data->len = 0;
  return 0;
}
int32_t blockCleanup(SBlkData *data) {
  taosMemoryFree(data->data);
  return 0;
}

static int32_t blockSeek(SBlkData *data, uint64_t key, uint32_t blockId, uint8_t *pValue, int32_t *len) {
  int32_t code = 0;
  return code;
}

void bseBuildDataFullName(SBse *pBse, char *name) { snprintf(name, strlen(name), "%s/%s", pBse->path, "data"); }
void bseBuildIndexFullName(SBse *pBse, char *name) { snprintf(name, strlen(name), "%s/%s", pBse->path, "index"); }

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse) {
  int32_t lino = 0;
  int32_t code = 0;

  SBse *p = taosMemoryMalloc(sizeof(SBse));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }

  p->pTableBuilder = taosMemoryMalloc(sizeof(STableBuilder));
  if (p->pTableBuilder == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }

  p->pSeqOffsetCache =
      taosHashInit(4096 * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);

  taosThreadMutexInit(&p->mutex, NULL);
  tstrncpy(p->path, path, sizeof(p->path));
  *pBse = p;
_err:
  if (code != 0) {
    // failed to open bse sinc
  }
  return code;
}
void bseClose(SBse *pBse) {
  if (pBse == NULL) {
    return;
  }

  taosMemoryFree(pBse);
}

int32_t tableOpen(char *name, STableBuilder **pTable) {
  int32_t        code = 0;
  STableBuilder *pBuilder = taosMemoryMalloc(sizeof(STableBuilder));
  code = blockInit(kBlockCap, 0, &pBuilder->data);
  return code;
}

int32_t tableClose(STableBuilder *pTable) {
  int32_t code = 0;
  code = blockCleanup(&pTable->data);
  taosMemFree(pTable);
  return code;
}
int32_t tableAppend(STableBuilder *pTableBuilder, uint64_t key, uint8_t *value, int32_t len, uint64_t *offset) {
  int32_t code = 0;
  if (blockShouldFlush(&pTableBuilder->data, len)) {
    code = tableFlush(pTableBuilder);
  }
  code = blockAdd(&pTableBuilder->data, key, value, len);

  return code;
}
int32_t tableGet(STableBuilder *pTableBuilder, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;

  return code;
}
int32_t tableFlush(STableBuilder *pTableBuilder) {
  /// do write to file
  int32_t code = 0;

  return code;
}
int32_t tableCommit(STableBuilder *pTableBuilder) {
  // Generate static info and footer info;

  int32_t code = 0;
  return code;
}

int32_t tableLoadBlk(STableBuilder *pTable, uint32_t blockId, SBlkData *blk) {
  int32_t  code = 0;
  uint32_t offset = blockId * kBlockCap;
  code = taosLSeekFile(pTable->pDataFile, offset, SEEK_SET);
  if (code != 0) {
    return code;
  }

  code = taosReadFile(pTable->pDataFile, blk->data, blk->cap);
  return code;
}

int32_t tableLoadBySeq(STableBuilder *pTable, SValueInfo *pInfo, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  code = tableLoadBlk(pTable, pInfo->offset / kBlockCap, &pTable->data);
  return code;
}

int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t  line = 0;
  int32_t  code = 0;
  uint64_t offset = 0;
  uint64_t tseq = 0;

  SValueInfo info;

  taosThreadMutexLock(&pBse->mutex);
  tseq = ++pBse->seq;

  code = tableAppend(pBse->pTableBuilder, tseq, value, len, &offset);
  TAOS_CHECK_GOTO(code, &line, _err);

  info.offset = offset;
  info.size = len;

  code = taosHashPut(pBse->pSeqOffsetCache, &tseq, sizeof(tseq), &info, sizeof(SValueInfo));
  TAOS_CHECK_GOTO(code, &line, _err);

  *seq = tseq;
_err:
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;
  taosThreadMutexLock(&pBse->mutex);
  SValueInfo *pValueInfo = taosHashGet(pBse->pSeqOffsetCache, &seq, sizeof(uint64_t));

  if (pValueInfo == NULL) {
    code = TSDB_CODE_NOT_FOUND;
    TAOS_CHECK_GOTO(code, &line, _err);
  }

  *pValue = taosMemoryMalloc(pValueInfo->size + 10);

  *len = pValueInfo->size;
_err:
  taosThreadMutexUnlock(&pBse->mutex);
  if (code != 0) {
    // failed to get bse value since tstrerror(code)
  }
  return code;
}

int32_t bseCommit(SBse *pBse) {
  int32_t code = 0;
  return code;
}

int32_t bseRollback(SBse *pBse, int64_t ver) {
  // TODO
  return 0;
}

int32_t bseRollbackImpl(SBse *pBse) {
  int32_t code = 0;
  return code;
}

int32_t bseBeginSnapshot(SBse *pBse, int64_t ver) {
  // TODO
  return 0;
}

int32_t bseEndSnapshot(SBse *pBse) {
  // TODO
  return 0;
}

int32_t bseStopSnapshot(SBse *pBse) {
  // TODO
  return 0;
}