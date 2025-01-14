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

enum type {
  BSE_DATA_TYPE = 0x1,
  BSE_META_TYPE = 0x2,
  BSE_FOOTER_TYPE = 0x4,
};

#define kBlockTrailerSize (1 + 1 + 4)  // complete flag + compress type + crc unmask

static int32_t kBlockCap = 1024 * 16;

// clang-format off
#define bseFatal(...) do { if (bseDebugFlag & DEBUG_FATAL) { taosPrintLog("BSE FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define bseError(...) do { if (bseDebugFlag & DEBUG_ERROR) { taosPrintLog("BSE ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define bseWarn(...)  do { if (bseDebugFlag & DEBUG_WARN)  { taosPrintLog("BSE WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define bseInfo(...)  do { if (bseDebugFlag & DEBUG_INFO)  { taosPrintLog("BSE ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define bseDebug(...) do { if (bseDebugFlag & DEBUG_DEBUG) { taosPrintLog("BSE ", DEBUG_DEBUG, bseDebugFlag, __VA_ARGS__); }}    while(0)
#define bseTrace(...) do { if (bseDebugFlag & DEBUG_TRACE) { taosPrintLog("BSE ", DEBUG_TRACE, bseDebugFlag, __VA_ARGS__); }}    while(0)

#define bseGTrace(param, ...) do { if (bseDebugFlag & DEBUG_TRACE) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseTrace(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGFatal(param, ...) do { if (bseDebugFlag & DEBUG_FATAL) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseFatal(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGError(param, ...) do { if (bseDebugFlag & DEBUG_ERROR) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseError(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGWarn(param, ...)  do { if (bseDebugFlag & DEBUG_WARN)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseWarn(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGInfo(param, ...)  do { if (bseDebugFlag & DEBUG_INFO)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseInfo(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGDebug(param, ...) do { if (bseDebugFlag & DEBUG_DEBUG) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseDebug(param ",QID:%s", __VA_ARGS__, buf);}}    while(0)

// clang-format on

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
  uint32_t id;
  uint32_t len;
  uint32_t cap;
  char    *data;
} SBlkData;

typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  TdFilePtr    pIdxFile;
  SBlkData     data;
  SBlkData     tdata;
  STableFooter footer;
  SHashObj    *pCache;
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
static int32_t tableAppendData(STableBuilder *pTable, uint64_t key, uint8_t *value, int32_t len);
static int32_t tableGet(STableBuilder *pTable, uint64_t offset, uint64_t key, uint8_t **pValue, int32_t *len);
static int32_t tableFlush(STableBuilder *pTable);
static int32_t tableCommit(STableBuilder *pTable);
static int32_t tableLoadBlk(STableBuilder *pTable, uint32_t blockId, SBlkData *blk);
static int32_t tableRebuild(STableBuilder *pTable);
typedef struct {
  uint8_t  type;
  uint32_t len;
  uint32_t num;
} SBlkHeader;

// data block func
static int32_t blockInit(int32_t cap, int8_t type, SBlkData *blk);
static int32_t blockCleanup(SBlkData *data);
static int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset);
static int64_t blockEstimateSize(SBlkData *data);
static int8_t  blockShouldFlush(SBlkData *data, int32_t len);
static int32_t blockReset(SBlkData *data);
static int32_t blockSeek(SBlkData *data, uint64_t key, uint32_t blockId, uint8_t *pValue, int32_t *len);
static int32_t blockSeekOffset(SBlkData *data, uint32_t offset, uint64_t key, uint8_t **pValue, int32_t *len);

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
  blk->len = sizeof(blk->head);
  blk->cap = cap;
  blk->data = taosMemoryMalloc(cap);
  if (blk->data == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}

int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset) {
  int32_t code = 0;
  char   *p = blk->data + blk->len;

  *offset = blk->id * kBlockCap + blk->len;
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
  data->type = BSE_DATA_TYPE;
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
static int32_t blockSeekOffset(SBlkData *data, uint32_t offset, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t  code = 0;
  int32_t  tlen = 0;
  uint64_t tkey = 0;
  uint8_t *p = (uint8_t *)data->data + offset;
  p = taosDecodeVariantU64(p, &tkey);
  if (tkey != key) {
    return TSDB_CODE_NOT_FOUND;
  }
  p = taosDecodeVariantI32(p, &tlen);
  p = taosDecodeBinary(p, (void **)pValue, tlen);
  *len = tlen;

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
    bseError("failed to open bse since %s", tstrerror(code));
  }
  return code;
}
void bseClose(SBse *pBse) {
  if (pBse == NULL) {
    return;
  }

  taosMemFreeClear(pBse->pTableBuilder);
  taosHashCleanup(pBse->pSeqOffsetCache);
  taosMemoryFree(pBse);
}

int32_t tableOpen(char *name, STableBuilder **ppTable) {
  int32_t line = 0;
  int32_t code = 0;

  STableBuilder *pTable = taosMemoryMalloc(sizeof(STableBuilder));
  if (pTable == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  pTable->pCache = taosHashInit(4096 * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  if (pTable->pCache == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  code = blockInit(kBlockCap, 0, &pTable->data);
  TAOS_CHECK_GOTO(code, &line, _err);

  code = blockInit(kBlockCap, 0, &pTable->tdata);
  TAOS_CHECK_GOTO(code, &line, _err);

  *ppTable = pTable;
_err:
  if (code != 0) {
    bseError("failed to open table since %s", tstrerror(code));
    if (pTable != NULL) {
      tableClose(pTable);
    }
  } else {
    bseInfo("bse file %s succ to be opened", pTable->name);
  }

  return code;
}

int32_t tableClose(STableBuilder *pTable) {
  int32_t code = 0;
  code = blockCleanup(&pTable->data);
  code = blockCleanup(&pTable->tdata);
  taosHashCleanup(pTable->pCache);

  taosMemFree(pTable);
  return code;
}
int32_t tableAppendData(STableBuilder *pTable, uint64_t key, uint8_t *value, int32_t len) {
  int32_t    line = 0;
  int32_t    code = 0;
  uint32_t   offset = 0;
  SValueInfo info;

  if (blockShouldFlush(&pTable->data, len)) {
    TAOS_CHECK_GOTO(tableFlush(pTable), &line, _err);
    TAOS_CHECK_GOTO(blockReset(&pTable->data), &line, _err);
  }

  code = blockAdd(&pTable->data, key, value, len, &offset);
  TAOS_CHECK_GOTO(code, &line, _err);

  info.offset = offset;
  info.size = len;
  code = taosHashPut(pTable->pCache, &key, sizeof(key), &info, sizeof(SValueInfo));

  TAOS_CHECK_GOTO(code, &line, _err);

_err:
  if (code != 0) {
    bseError("bse file %s failed to append value at line since %s", pTable->name, tstrerror(code));
  }
  return code;
}

int32_t tableAppendMeta(STableBuilder *pTable) {
  int32_t line = 0;
  int32_t code = 0;
  TAOS_CHECK_GOTO(tableFlush(pTable), &line, _err);
  TAOS_CHECK_GOTO(blockReset(&pTable->data), &line, _err);

  SBlkData *pBlk = &(SBlkData){0};
  pBlk->type = BSE_META_TYPE;
  pBlk->len = taosHashGetSize(pTable->pCache);

  return code;
_err:
  if (code != 0) {
    bseError("bse file %s failed to append meta since %s", pTable->name, tstrerror(code));
  }
  return code;
}

int32_t tableGet(STableBuilder *pTable, uint64_t offset, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;
  int32_t blkId = offset / kBlockCap;
  int32_t blkOffset = offset % kBlockCap;

  if (pTable->data.id == blkId) {
    code = blockSeekOffset(&pTable->data, blkOffset, key, pValue, len);
    TAOS_CHECK_GOTO(code, &line, _err);
  } else {
    code = tableLoadBlk(pTable, blkId, &pTable->tdata);
    TAOS_CHECK_GOTO(code, &line, _err);

    code = blockSeekOffset(&pTable->tdata, blkOffset, key, pValue, len);
    TAOS_CHECK_GOTO(code, &line, _err);
  }
_err:
  if (code != 0) {
    bseError("failed to get value by key %" PRIu64 "since %s", key, tstrerror(code));
  }
  return code;
}
int32_t tableFlush(STableBuilder *pTable) {
  /// do write to file
  // Do CRC and write to table
  return taosWriteFile(pTable->pDataFile, pTable->data.data, pTable->data.len);
}

int32_t tableCommit(STableBuilder *pTable) {
  // Generate static info and footer info;
  int32_t   code = 0;
  SBlkData *pBlk = &(SBlkData){0};
  pBlk->type = BSE_META_TYPE;
  pBlk->id = pTable->data.id;

  pBlk->len = taosHashGetSize(pTable->pCache);

  return code;
}

int32_t tableLoadBlk(STableBuilder *pTable, uint32_t blockId, SBlkData *blk) {
  int32_t code = 0;
  int32_t offset = blockId * kBlockCap;

  taosLSeekFile(pTable->pDataFile, offset, SEEK_SET);
  code = taosReadFile(pTable->pDataFile, blk->data, kBlockCap);
  return code;
}

int32_t tableLoadBySeq(STableBuilder *pTable, uint64_t key, SValueInfo *pInfo, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  return tableGet(pTable, pInfo->offset, key, pValue, len);
}

int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t  line = 0;
  int32_t  code = 0;
  uint32_t offset = 0;
  uint64_t tseq = 0;

  taosThreadMutexLock(&pBse->mutex);
  tseq = ++pBse->seq;

  code = tableAppendData(pBse->pTableBuilder, tseq, value, len);
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
  if (*pValue == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    TAOS_CHECK_GOTO(code, &line, _err);
  }
  code = tableLoadBySeq(pBse->pTableBuilder, seq, pValueInfo, pValue, len);
  TAOS_CHECK_GOTO(code, &line, _err);

_err:
  taosThreadMutexUnlock(&pBse->mutex);
  if (code != 0) {
    bseError("failed to get value by seq %" PRIu64 "since %s", seq, tstrerror(code));
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