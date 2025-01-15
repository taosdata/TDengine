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

#include "bse.h"
#include "os.h"
#include "tchecksum.h"
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

static int32_t kBlockCap = 512;

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

static void bseBuildDataFullName(SBse *pBse, char *name);
static void bseBuildIndexFullName(SBse *pBse, char *name);

static int32_t tableOpen(const char *name, STable **pTable);
static int32_t tableClose(STable *pTable);
static int32_t tableAppendData(STable *pTable, uint64_t key, uint8_t *value, int32_t len);
static int32_t tableGet(STable *pTable, uint64_t offset, uint64_t key, uint8_t **pValue, int32_t *len);
static int32_t tableFlushBlock(STable *pTable);
static int32_t tableCommit(STable *pTable);
static int32_t tableLoadBlk(STable *pTable, uint32_t blockId, SBlkData *blk);
static int32_t tableRebuild(STable *pTable);

// data block func
static int32_t blockInit(int32_t blockId, int32_t cap, int8_t type, SBlkData *blk);
static int32_t blockCleanup(SBlkData *data);
static int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset);
static int8_t  blockShouldFlush(SBlkData *data, int32_t len);
static int32_t blockReset(SBlkData *data, uint8_t type, int32_t blkId);
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

int32_t blockInit(int32_t id, int32_t cap, int8_t type, SBlkData *blk) {
  blk->type = type;
  blk->len = 0;
  blk->cap = cap;
  blk->id = id;

  blk->pData = (SBlkData2 *)taosMemoryCalloc(1, kBlockCap);
  blk->pData->id = id;
  blk->pData->len = 0;
  blk->pData->head[3] = type;

  return 0;
}

int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;

  uint8_t *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;
  pBlk->len += taosEncodeVariantU64((void **)&p, key);
  pBlk->len += taosEncodeVariantI32((void **)&p, len);
  pBlk->len += taosEncodeBinary((void **)&p, value, len);
  return code;
}

int32_t blockAdd2(SBlkData *blk, uint64_t key, SValueInfo *pValue, uint64_t *offset) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;

  uint8_t *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;
  pBlk->len += taosEncodeVariantU64((void **)&p, key);
  pBlk->len += taosEncodeVariantU64((void **)&p, pValue->offset);
  pBlk->len += taosEncodeVariantI32((void **)&p, pValue->size);
  return code;
}

// 0 not flush,1 flush
int8_t blockShouldFlush(SBlkData *data, int32_t len) {
  //
  SBlkData2 *pBlkData = data->pData;
  int32_t    estimite = taosEncodeVariantU64(NULL, len);
  estimite += taosEncodeVariantI32(NULL, sizeof(int32_t));
  return (estimite + sizeof(SBlkData2) + pBlkData->len + sizeof(TSCKSUM)) > data->cap;
}

int32_t blockReset(SBlkData *data, uint8_t type, int32_t blockId) {
  SBlkData2 *pBlkData = data->pData;

  memset((uint8_t *)pBlkData, 0, kBlockCap);
  data->type = type;
  data->len = sizeof(SBlkData2);
  data->id = blockId;

  memset(pBlkData->head, 0, sizeof(pBlkData->head));
  pBlkData->id = blockId;
  pBlkData->len = 0;
  pBlkData->head[3] = type;
  return 0;
}
int32_t blockCleanup(SBlkData *data) {
  taosMemoryFree(data->pData);
  return 0;
}

static int32_t blockSeek(SBlkData *data, uint64_t key, uint32_t blockId, uint8_t *pValue, int32_t *len) {
  int32_t code = 0;
  return code;
}
static int32_t blockSeekOffset(SBlkData *data, uint32_t offset, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t    code = 0;
  int32_t    tlen = 0;
  uint64_t   tkey = 0;
  SBlkData2 *pBlkData = data->pData;
  uint8_t   *p = (uint8_t *)pBlkData->data + offset;
  p = taosDecodeVariantU64(p, &tkey);
  if (tkey != key) {
    return TSDB_CODE_NOT_FOUND;
  }
  p = taosDecodeVariantI32(p, &tlen);
  p = taosDecodeBinary(p, (void **)pValue, tlen);
  *len = tlen;

  return code;
}

static void bseBuildDataFullName(SBse *pBse, char *name) { snprintf(name, strlen(name), "%s/%s", pBse->path, "data"); }

static void bseBuildIndexFullName(SBse *pBse, char *name) {
  snprintf(name, strlen(name), "%s/%s", pBse->path, "index");
}

int32_t tableOpen(const char *name, STable **ppTable) {
  int32_t line = 0;
  int32_t code = 0;

  STable *pTable = taosMemoryMalloc(sizeof(STable));
  if (pTable == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  pTable->pCache = taosHashInit(4096 * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  if (pTable->pCache == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  pTable->blockId = 0;
  code = blockInit(pTable->blockId, kBlockCap, BSE_DATA_TYPE, &pTable->data);
  TAOS_CHECK_GOTO(code, &line, _err);

  code = blockInit(pTable->blockId, kBlockCap, BSE_DATA_TYPE, &pTable->bufBlk);
  TAOS_CHECK_GOTO(code, &line, _err);

  pTable->pDataFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  *ppTable = pTable;
_err:
  if (code != 0) {
    bseError("failed to open table %s at line %d since %s", pTable->name, line, tstrerror(code));
    if (pTable != NULL) {
      tableClose(pTable);
    }
  } else {
    bseInfo("bse table file %s succ to be opened", pTable->name);
  }

  return code;
}

int32_t tableClose(STable *pTable) {
  int32_t code = 0;
  code = blockCleanup(&pTable->data);
  code = blockCleanup(&pTable->bufBlk);
  taosHashCleanup(pTable->pCache);

  taosCloseFile(&pTable->pDataFile);

  taosMemFree(pTable);
  return code;
}
int32_t tableAppendData(STable *pTable, uint64_t key, uint8_t *value, int32_t len) {
  int32_t    line = 0;
  int32_t    code = 0;
  uint32_t   offset = 0;
  SValueInfo info;

  if (blockShouldFlush(&pTable->data, len)) {
    TAOS_CHECK_GOTO(tableFlushBlock(pTable), &line, _err);
    pTable->blockId++;
    TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_DATA_TYPE, pTable->blockId), &line, _err);
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

int32_t tableAppendMeta(STable *pTable) {
  int32_t  line = 0;
  int32_t  code = 0;
  uint64_t key, offset;

  pTable->blockId++;
  TAOS_CHECK_GOTO(tableFlushBlock(pTable), &line, _err);

  TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_META_TYPE, pTable->blockId), &line, _err);

  SBlkData2 *pBlk = pTable->data.pData;
  void      *pIter = taosHashIterate(pTable->pCache, NULL);
  while (pIter) {
    key = *(uint64_t *)taosHashGetKey(pIter, NULL);
    SValueInfo *value = (SValueInfo *)pIter;
    code = blockAdd2(&pTable->data, key, value, &offset);
    pIter = taosHashIterate(pTable->pCache, pIter);
  }

  tableFlushBlock(pTable);

  return code;
_err:
  if (code != 0) {
    bseError("bse file %s failed to append meta since %s", pTable->name, tstrerror(code));
  }
  return code;
}

int32_t tableGet(STable *pTable, uint64_t offset, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;
  int32_t blkId = offset / kBlockCap;
  int32_t blkOffset = offset % kBlockCap;

  if (pTable->data.id == blkId) {
    code = blockSeekOffset(&pTable->data, blkOffset, key, pValue, len);
    TAOS_CHECK_GOTO(code, &line, _err);
  } else {
    code = tableLoadBlk(pTable, blkId, &pTable->bufBlk);
    TAOS_CHECK_GOTO(code, &line, _err);

    code = blockSeekOffset(&pTable->bufBlk, blkOffset, key, pValue, len);
    TAOS_CHECK_GOTO(code, &line, _err);
  }
_err:
  if (code != 0) {
    bseError("failed to get value by key %" PRIu64 "since %s", key, tstrerror(code));
  }
  return code;
}

int32_t tableFlushBlock(STable *pTable) {
  /// do write to file
  // Do CRC and write to table
  int32_t code = 0;
  int32_t line = 0;

  SBlkData2 *pBlk = pTable->data.pData;
  if (pBlk->len == 0) {
    return 0;
  }

  code = taosCalcChecksumAppend(0, (uint8_t *)pBlk, kBlockCap);
  TAOS_CHECK_GOTO(code, &line, _err);

  code = taosLSeekFile(pTable->pDataFile, pBlk->id * kBlockCap, SEEK_SET);
  if (code < 0) {
    TAOS_CHECK_GOTO(code, &line, _err);
  }

  code = 0;
  int64_t nwrite = taosWriteFile(pTable->pDataFile, (uint8_t *)pBlk, (int64_t)kBlockCap);
  if (nwrite != kBlockCap) {
    code = terrno;
    TAOS_CHECK_GOTO(code, &line, _err);
  }
_err:
  if (code != 0) {
    bseError("table file %s failed to flush table since %s", pTable->name, tstrerror(code));
  }
  return code;
}

int32_t tableCommit(STable *pTable) {
  // Generate static info and footer info;
  return tableAppendMeta(pTable);
}

int32_t tableLoadBlk(STable *pTable, uint32_t blockId, SBlkData *blk) {
  int32_t code = 0;
  int32_t offset = blockId * kBlockCap;
  taosLSeekFile(pTable->pDataFile, offset, SEEK_SET);
  SBlkData2 *pBlk = blk->pData;

  int64_t len = taosReadFile(pTable->pDataFile, (uint8_t *)pBlk, kBlockCap);
  if (len != kBlockCap) {
    code = terrno;
    return code;
  }
  if (taosCheckChecksumWhole((uint8_t *)pBlk, kBlockCap) == 0) {
    code = TSDB_CODE_INVALID_MSG;
  }
  return code;
}

int32_t tableLoadBySeq(STable *pTable, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t     code = 0;
  SValueInfo *pInfo = taosHashGet(pTable->pCache, &key, sizeof(uint64_t));
  if (pInfo == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  return tableGet(pTable, pInfo->offset, key, pValue, len);
}

int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse) {
  int32_t lino = 0;
  int32_t code = 0;

  SBse *p = taosMemoryMalloc(sizeof(SBse));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }

  code = tableOpen(path, &p->pTable);
  TAOS_CHECK_GOTO(code, &lino, _err);

  // p->pSeqOffsetCache =
  //     taosHashInit(4096 * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  // if (p->pSeqOffsetCache == NULL) {
  //   TAOS_CHECK_GOTO(terrno, &lino, _err);
  // }

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
  tableClose(pBse->pTable);
  // taosHashCleanup(pBse->pSeqOffsetCache);
  taosMemoryFree(pBse);
}

int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t  line = 0;
  int32_t  code = 0;
  uint32_t offset = 0;
  uint64_t tseq = 0;

  taosThreadMutexLock(&pBse->mutex);
  tseq = ++pBse->seq;

  code = tableAppendData(pBse->pTable, tseq, value, len);
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
  code = tableLoadBySeq(pBse->pTable, seq, pValue, len);
  TAOS_CHECK_GOTO(code, &line, _err);

_err:
  taosThreadMutexUnlock(&pBse->mutex);
  if (code != 0) {
    bseError("failed to get value by seq %" PRIu64 " since %s", seq, tstrerror(code));
  }
  return code;
}

int32_t bseCommit(SBse *pBse) {
  // Generate static info and footer info;
  return tableCommit(pBse->pTable);
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