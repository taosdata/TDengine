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
#include "cJSON.h"
#include "os.h"
#include "tchecksum.h"
#include "tcompare.h"
#include "tlog.h"
#include "tmsg.h"
#include "tutil.h"

#define kMaxEncodeLen 20
#define kEncodeLen    (2 * (kMaxEncodeLen) + 8)

#define kMagicNumber 0xdb4775248b80fb57ull;
#define kMagicNum    0x123456

#define BSE_FILE_FULL_LEN TSDB_FILENAME_LEN

enum type {
  BSE_DATA_TYPE = 0x1,
  BSE_META_TYPE = 0x2,
  BSE_FOOTER_TYPE = 0x4,
};

typedef struct {
  int64_t firstVer;
  int64_t lastVer;
  int64_t createTs;
  int64_t closeTs;
  int64_t fileSize;
  int64_t syncedOffset;
} SBseFileInfo;
#define kBlockTrailerSize (1 + 1 + 4)  // complete flag + compress type + crc unmask

#define BSE_DATA_SUFFIX  "data"
#define BSE_LOG_SUFFIX   "log"
#define BSE_INDEX_SUFFIX "idx"

static char *bseFilexSuffix[] = {
    "idx",
    "data",
    "log",
};
static int32_t kBlockCap = 8 * 1024 * 1024;

static void bseBuildDataFullName(SBse *pBse, int64_t ver, char *name);
static void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *name);
static void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf);

static int32_t bseFindCurrMetaVer(SBse *pBse);

static int32_t bseRecover(SBse *pBse);

static int32_t tableOpen(const char *name, STable **pTable, uint8_t openFile);
static int32_t tableClose(STable *pTable, uint8_t clear);
static int32_t tableAppendData(STable *pTable, uint64_t key, uint8_t *value, int32_t len);
static int32_t tableGet(STable *pTable, uint64_t offset, uint64_t key, uint8_t **pValue, int32_t *len);
static int32_t tableFlushBlock(STable *pTable);
static int32_t tableCommit(STable *pTable);
static int32_t tableLoadBlk(STable *pTable, uint32_t blockId, SBlkData *blk);
static int32_t tableRebuild(STable *pTable);

static int32_t readerTableOpen(const char *name, uint64_t ver, SReaderTable **pTable);
static int32_t readerTableClose(SReaderTable *pTable);
// static int32_t tableAppendData(STable *pTable, uint64_t key, uint8_t *value, int32_t len);
static int32_t readerTableGet(SReaderTable *pTable, uint64_t key, uint8_t **pValue, int32_t *len);
// static int32_t readerTableLoadBlk(SReaderTable *pTable, uint32_t blockId, SBlkData *blk);
static int32_t readerTableBuild(SReaderTable *pTable);

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
  blk->dataNum = 0;

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
  blk->dataNum++;
  return code;
}
int32_t blockAdd2(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;
  uint8_t   *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;

  memcpy(p, value, len);
  pBlk->len += len;
  return 0;
}

int32_t blockAddMeta(SBlkData *blk, uint64_t key, SValueInfo *pValue, uint64_t *offset) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;

  uint8_t *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;
  pBlk->len += taosEncodeVariantU64((void **)&p, key);
  pBlk->len += taosEncodeVariantU64((void **)&p, pValue->offset);

  blk->dataNum++;
  return code;
}

// 0 not flush,1 flush
int8_t blockShouldFlush(SBlkData *data, int32_t len) {
  //
  SBlkData2 *pBlkData = data->pData;
  int32_t    estimite = taosEncodeVariantU64(NULL, sizeof(UINT64_MAX));
  estimite += taosEncodeVariantI32(NULL, len);
  estimite += len;
  return (estimite + sizeof(SBlkData2) + pBlkData->len + sizeof(TSCKSUM)) > data->cap;
}

int8_t blockShouldFlush2(SBlkData *data, int32_t len) {
  SBlkData2 *pBlkData = data->pData;
  return sizeof(SBlkData2) + pBlkData->len + len + sizeof(TSCKSUM) > data->cap;
}
int8_t blockMetaShouldFlush(SBlkData *data, int32_t len) {
  //
  SBlkData2 *pBlkData = data->pData;
  int32_t    estimite = taosEncodeVariantU64(NULL, sizeof(UINT64_MAX));
  estimite += taosEncodeVariantU64(NULL, len);
  return (estimite + sizeof(SBlkData2) + pBlkData->len + sizeof(TSCKSUM)) > data->cap;
}

int32_t blockReset(SBlkData *data, uint8_t type, int32_t blockId) {
  SBlkData2 *pBlkData = data->pData;

  memset((uint8_t *)pBlkData, 0, kBlockCap);
  data->type = type;
  data->len = 0;
  data->id = blockId;
  data->dataNum = 0;

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

static void bseBuildDataFullName(SBse *pBse, int64_t ver, char *buf) {
  // build data file name
  // snprintf(name, strlen(name), "%s/%s020"."BSE_DATA_SUFFIX, ver, pBse->path);
  // sprintf(name, strlen(name), "%s/%020"."BSE_DATA_SUFFIX, ver, pBse->path);
  TAOS_UNUSED(sprintf(buf, "%s/%d.%s", pBse->path, (int32_t)(ver), BSE_DATA_SUFFIX));
}

static void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *buf) {
  // build index file name
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_INDEX_SUFFIX, pBse->path, ver));
}
static void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf) {
  TAOS_UNUSED(sprintf(buf, "%s/%020" PRId64 "." BSE_LOG_SUFFIX, pBse->path, ver));
}

// 0 not flush,1 flush

int32_t tableOpen(const char *name, STable **ppTable, uint8_t openFile) {
  int32_t line = 0;
  int32_t code = 0;

  STable *pTable = taosMemoryCalloc(1, sizeof(STable));
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

  pTable->fileOpened = openFile;
  if (openFile) {
    pTable->pDataFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  }

  tstrncpy(pTable->name, name, TSDB_FILENAME_LEN);
  *ppTable = pTable;

_err:
  if (code != 0) {
    bseError("failed to open table %s at line %d since %s", pTable->name, line, tstrerror(code));
    if (pTable != NULL) {
      tableClose(pTable, 1);
    }
  } else {
    bseInfo("bse table file %s succ to be opened", pTable->name);
  }

  return code;
}

int32_t tableClear(STable *pTable) {
  int32_t code = 0;
  pTable->commited = 0;

  taosHashClear(pTable->pCache);
  memset((uint8_t *)pTable->bufBlk.pData, 0, kBlockCap);
  memset((uint8_t *)pTable->data.pData, 0, kBlockCap);
  pTable->blockId = 0;
  pTable->initSeq = 0;
  taosHashClear(pTable->pCache);

  pTable->pDataFile = NULL;
  pTable->pIdxFile = NULL;
  pTable->fileOpened = 0;
  return code;
}
int32_t tableOpenFile(STable *pTable, char *name) {
  int32_t code = 0;
  pTable->pDataFile = taosOpenFile(name, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  if (pTable->pDataFile == NULL) {
    return terrno;
  }

  memcpy(pTable->name, name, strlen(name));
  pTable->fileOpened = 1;
  return code;
}
int32_t tableClose(STable *pTable, uint8_t clear) {
  int32_t line = 0;
  int32_t code = 0;
  if (pTable == NULL) {
    return code;
  }

  // tableCommit(pTable);
  pTable->commited = 1;
  code = blockCleanup(&pTable->data);
  TAOS_CHECK_GOTO(code, &line, _err);
  code = blockCleanup(&pTable->bufBlk);
  TAOS_CHECK_GOTO(code, &line, _err);
  taosHashCleanup(pTable->pCache);

  code = taosCloseFile(&pTable->pDataFile);
  pTable->fileOpened = 0;
  TAOS_CHECK_GOTO(code, &line, _err);

  taosMemFree(pTable);
_err:
  if (code != 0) {
    bseError("bse file %s failed to close table since %s", pTable->name, tstrerror(code));
  }
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
  code = taosHashPut(pTable->pCache, &key, sizeof(key), &info, sizeof(info));

  TAOS_CHECK_GOTO(code, &line, _err);

_err:
  if (code != 0) {
    bseError("bse file %s failed to append value at line since %s", pTable->name, tstrerror(code));
  }
  return code;
}
int32_t tableAppendData2(STable *pTable, uint64_t key, uint8_t *value, int32_t len) {
  int32_t    line = 0;
  int32_t    code;
  uint32_t   offset;
  SValueInfo info;
  if (blockShouldFlush2(&pTable->data, len)) {
    code = tableFlushBlock(pTable);
    TAOS_CHECK_GOTO(code, &line, _error);
    pTable->blockId++;
    TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_DATA_TYPE, pTable->blockId), &line, _error);
  }
  code = blockAdd2(&pTable->data, key, value, len, &offset);
  info.offset = offset;
  info.size = len;
  // code = taosHashPut(pTable->pCache, &key, sizeof(key), &info, sizeof(info));
_error:
  return code;
}

int32_t tableAppendBatch(STable *pTable, SBseBatch *pBatch) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint32_t offset = 0;
  void    *pIter = taosHashIterate(pBatch->pOffset, NULL);
  while (pIter) {
    uint64_t   *seq = taosHashGetKey(pIter, NULL);
    SValueInfo *pInfo = (SValueInfo *)pIter;

    code = tableAppendData2(pTable, *seq, pBatch->buf + pInfo->offset, pInfo->size);
    TAOS_CHECK_GOTO(code, &lino, _error);

    pIter = taosHashIterate(pBatch->pOffset, pIter);
  }
_error:
  if (code != 0) {
    bseError("failed to append batch since %s at line %d", tstrerror(code), lino);
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

    if (blockMetaShouldFlush(&pTable->data, value->offset)) {
      code = tableFlushBlock(pTable);
      TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_META_TYPE, pTable->blockId), &line, _err);
    }

    code = blockAddMeta(&pTable->data, key, value, &offset);
    pIter = taosHashIterate(pTable->pCache, pIter);
  }

  code = tableFlushBlock(pTable);
  TAOS_CHECK_GOTO(code, &line, _err);

  TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_DATA_TYPE, pTable->blockId), &line, _err);

  taosHashClear(pTable->pCache);
  taosCloseFile(&pTable->pDataFile);
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
    bseError("failed to get value by key %" PRIu64 " since %s at line ", key, tstrerror(code), line);
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
  // serialize or not
  pBlk->len = pTable->data.dataNum;

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
  if (pTable->commited) {
    return 0;
  }
  pTable->commited = 1;
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

int32_t bseLoadMeta(SBse *pBse) {
  int32_t code = 0;
  return code;
}
static FORCE_INLINE int32_t bseBuildMetaName(SBse *pBse, int ver, char *name) {
  return snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver%d", pBse->path, TD_DIRSEP, ver);
}
static FORCE_INLINE int32_t bseBuildTempMetaName(SBse *pBse, char *name) {
  return snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver.tmp", pBse->path, TD_DIRSEP);
}

static int32_t bseMetaSerialize(SBse *pBse, char **pBuf, int32_t *len) {
  int32_t code = 0;
  int32_t line = 0;
  char    buf[128] = {0};

  int32_t sz = taosArrayGetSize(pBse->fileSet);
  cJSON  *pRoot = cJSON_CreateObject();
  cJSON  *pMeta = cJSON_CreateObject();
  cJSON  *pFiles = cJSON_CreateArray();
  cJSON  *pField;
  if (pRoot == NULL || pMeta == NULL || pFiles == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, &line, _err);
  }

  int64_t firstVer = -1;
  int64_t snapshotVer = -1;
  int64_t commitVer = -1;
  int64_t lastVer = -1;

  if (!cJSON_AddItemToObject(pRoot, "meta", pMeta)) goto _err;
  snprintf(buf, sizeof(buf), "%" PRId64, firstVer);
  if (cJSON_AddStringToObject(pMeta, "firstVer", buf) == NULL) goto _err;
  (void)snprintf(buf, sizeof(buf), "%" PRId64, snapshotVer);
  if (cJSON_AddStringToObject(pMeta, "snapshotVer", buf) == NULL) goto _err;
  (void)snprintf(buf, sizeof(buf), "%" PRId64, commitVer);
  if (cJSON_AddStringToObject(pMeta, "commitVer", buf) == NULL) goto _err;
  (void)snprintf(buf, sizeof(buf), "%" PRId64, lastVer);
  if (cJSON_AddStringToObject(pMeta, "lastVer", buf) == NULL) goto _err;
  for (int i = 0; i < taosArrayGetSize(pBse->fileSet); i++) {
    SBseFileInfo *pInfo = taosArrayGet(pBse->fileSet, i);
    if (!cJSON_AddItemToArray(pFiles, pField = cJSON_CreateObject())) {
      bseInfo("vgId:%d, failed to add field to files", pBse->cfg.vgId);
    }
    if (pField == NULL) {
      cJSON_Delete(pRoot);
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    // cjson only support int32_t or double
    // string are used to prohibit the loss of precision
    (void)snprintf(buf, sizeof(buf), "%" PRId64, pInfo->firstVer);
    if (cJSON_AddStringToObject(pField, "firstVer", buf) == NULL) goto _err;
    (void)snprintf(buf, sizeof(buf), "%" PRId64, pInfo->lastVer);
    if (cJSON_AddStringToObject(pField, "lastVer", buf) == NULL) goto _err;
    (void)snprintf(buf, sizeof(buf), "%" PRId64, pInfo->createTs);
    if (cJSON_AddStringToObject(pField, "createTs", buf) == NULL) goto _err;
    (void)snprintf(buf, sizeof(buf), "%" PRId64, pInfo->closeTs);
    if (cJSON_AddStringToObject(pField, "closeTs", buf) == NULL) goto _err;
    (void)snprintf(buf, sizeof(buf), "%" PRId64, pInfo->fileSize);
    if (cJSON_AddStringToObject(pField, "fileSize", buf) == NULL) goto _err;
  }
  char *pSerialized = cJSON_Print(pRoot);
  cJSON_Delete(pRoot);

  *pBuf = pSerialized;
  *len = strlen(pSerialized);
  return code;
_err:
  bseError("vgId:%d, %s failed at line %d since %s", pBse->cfg.vgId, __func__, line, tstrerror(code));
  cJSON_Delete(pRoot);
  cJSON_Delete(pMeta);
  cJSON_Delete(pFiles);
  return code;
}

int32_t bseSaveMeta(SBse *pBse) {
  int32_t code = 0, line = 0;
  char    fNameStr[BSE_FILE_FULL_LEN] = {0};
  char    tNameStr[BSE_FILE_FULL_LEN] = {0};
  int32_t nBytes = 0;
  int32_t ver = bseFindCurrMetaVer(pBse);

  int32_t n = bseBuildTempMetaName(pBse, tNameStr);
  if (n >= sizeof(tNameStr)) {
    return TSDB_CODE_INVALID_PARA;
  }

  TdFilePtr pMetaFile = taosOpenFile(tNameStr, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pMetaFile == NULL) {
    bseError("vgId:%d, failed to open file due to %s. file:%s", pBse->cfg.vgId, strerror(errno), tNameStr);
    TAOS_RETURN(terrno);
  }
  char   *buf = NULL;
  int32_t len = 0;
  code = bseMetaSerialize(pBse, &buf, &len);
  TAOS_CHECK_GOTO(code, &line, _err);

  nBytes = taosWriteFile(pMetaFile, buf, len);
  if (nBytes != len) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  code = taosFsyncFile(pMetaFile);
  TAOS_CHECK_GOTO(code, &line, _err);

  if (taosCloseFile(&pMetaFile) < 0) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  n = bseBuildMetaName(pBse, ver + 1, fNameStr);
  if (n >= sizeof(fNameStr)) {
    TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &line, _err);
  }

  if (taosRenameFile(tNameStr, fNameStr) < 0) {
    TAOS_CHECK_GOTO(TAOS_SYSTEM_ERROR(errno), &line, _err);
  }

  if (ver > -1) {
    n = bseBuildMetaName(pBse, ver, fNameStr);
    if (n >= sizeof(fNameStr)) {
      TAOS_CHECK_GOTO(TSDB_CODE_FAILED, &line, _err);
    }
    code = taosRemoveFile(fNameStr);
    if (code) {
      bseError("vgId:%d, failed to remove file due to %s. file:%s", pBse->cfg.vgId, strerror(errno), fNameStr);
    } else {
      bseInfo("vgId:%d, remove old meta file: %s", pBse->cfg.vgId, fNameStr);
    }
  }
  taosMemoryFree(buf);
  return code;
_err:
  bseError("vgId:%d, %s failed at line %d since %s", pBse->cfg.vgId, __func__, line, tstrerror(code));
  (void)taosCloseFile(&pMetaFile);
  taosMemoryFree(buf);
  return code;
}

static inline int32_t cmprBseFileInfo(const void *pLeft, const void *pRight) {
  SBseFileInfo *pInfoLeft = (SBseFileInfo *)pLeft;
  SBseFileInfo *pInfoRight = (SBseFileInfo *)pRight;
  return compareInt64Val(&pInfoLeft->firstVer, &pInfoRight->firstVer);
}

static inline int32_t recoverDataFile(SBse *pBse, int64_t fileVer, int8_t *next) {
  int32_t  code = 0;
  int32_t  line = 0;
  int64_t  size = 0;
  char     path[TSDB_FILENAME_LEN] = {0};
  uint8_t *blockBuf = NULL;
  bseBuildDataFullName(pBse, fileVer, path);

  code = taosStatFile(path, &size, NULL, NULL);
  if (size == 0) {
    *next = 0;
    return 0;
  }
  TAOS_CHECK_GOTO(code, &line, _err);

  TdFilePtr fd = taosOpenFile(path, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_READ | TD_FILE_APPEND);
  int64_t   offset = size > kBlockCap ? (size / kBlockCap - 1) * kBlockCap : 0;
  taosLSeekFile(fd, offset, SEEK_SET);

  blockBuf = taosMemCalloc(1, kBlockCap);
  if (taosReadFile(fd, blockBuf, kBlockCap) == kBlockCap) {
    if (taosCheckChecksumWhole(blockBuf, kBlockCap) == 0) {
      code = TSDB_CODE_INVALID_MSG;
      TAOS_CHECK_GOTO(code, &line, _err);
    }
    SBlkData2 *pBlk = (SBlkData2 *)blockBuf;
    if (pBlk->head[3] == BSE_META_TYPE) {
      *next = 1;
      // recover meta
    } else {
      *next = 0;
      // recover data
    }
    // check crc
  } else {
    TAOS_CHECK_GOTO(TSDB_CODE_INVALID_MSG, &line, _err);
  }
_err:
  if (code != 0) {
    bseError("bse table file %s failed to recover at line %d since %s", path, line, tstrerror(code));
  }
  taosCloseFile(&fd);
  taosMemFree(blockBuf);
  return code;
}
static int32_t bseRecover(SBse *pBse) {
  int32_t     code = 0;
  int32_t     line = 0;
  const char *logPattern = "^[0-9]+.log$";
  const char *idxPattern = "^[0-9]+.idx$";
  const char *dataPattern = "^[0-9]+.data$";

  regex_t  logRegPattern, idxRegPattern, dataRegPattern;
  TdDirPtr pDir = NULL;
  SArray  *logs = NULL;

  bseInfo("vgId:%d, begin to repair meta, bse path:%s, firstVer:%d, lastVer:%d, snapshotVer:%d", pBse->cfg.vgId,
          pBse->path, 0, 0, 0);

  TAOS_CHECK_GOTO(regcomp(&logRegPattern, logPattern, REG_EXTENDED), &line, _err);
  TAOS_CHECK_GOTO(regcomp(&idxRegPattern, idxPattern, REG_EXTENDED), &line, _err);
  TAOS_CHECK_GOTO(regcomp(&dataRegPattern, dataPattern, REG_EXTENDED), &line, _err);

  logs = taosArrayInit(4, sizeof(SBseFileInfo));
  if (logs == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }
  pBse->fileSet = logs;

  pDir = taosOpenDir(pBse->path);
  TAOS_CHECK_GOTO(code, &line, _err);

  TdDirEntryPtr pDirEntry;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *name = taosDirEntryBaseName(taosGetDirEntryName(pDirEntry));
    int   code = regexec(&dataRegPattern, name, 0, NULL, 0);
    if (code == 0) {
      SBseFileInfo fileInfo;
      (void)memset(&fileInfo, -1, sizeof(SBseFileInfo));
      (void)sscanf(name, "%" PRId64 ".data", &fileInfo.firstVer);
      if (taosArrayPush(logs, &fileInfo) == 0) {
        TAOS_CHECK_GOTO(terrno, &line, _err);
      }
    }
  }
  taosCloseDir(&pDir);

  char fNameStr[TSDB_FILENAME_LEN] = {0};
  taosArraySort(logs, cmprBseFileInfo);
  int32_t fileIdx = taosArrayGetSize(logs);
  if (fileIdx > 0) {
    while (--fileIdx) {
      int8_t        next = 0;
      SBseFileInfo *pFileInfo = taosArrayGet(pBse->fileSet, fileIdx);
      code = recoverDataFile(pBse, pFileInfo->firstVer, &next);
      if (code == 0) {
        if (next == 1) {
          SBseFileInfo info = {.firstVer = pFileInfo->firstVer + 1};
          if (taosArrayPush(pBse->fileSet, &info) == NULL) {
            TAOS_CHECK_GOTO(terrno, &line, _err);
          }
        } else {
          //
        }
        break;
      }
    }
  } else {
    SBseFileInfo info = {.firstVer = 0};
    bseBuildDataFullName(pBse, info.firstVer, fNameStr);
    if (taosArrayPush(pBse->fileSet, &info) == NULL) {
      TAOS_CHECK_GOTO(terrno, &line, _err);
    }
  }
_err:
  if (code != 0) {
    bseError("vgId:%d, failed to repair meta since %s", pBse->cfg.vgId, tstrerror(code));
  }
  // taosArrayDestroy(logs);
  regfree(&logRegPattern);
  regfree(&idxRegPattern);
  regfree(&dataRegPattern);
  return code;
}
static int32_t bseFindCurrMetaVer(SBse *pBse) {
  int32_t     code = 0;
  const char *pattern = "^bse-ver[0-9]+$";

  regex_t bseMetaRegexPattern;
  if (regcomp(&bseMetaRegexPattern, pattern, REG_EXTENDED) != 0) {
    bseError("failed to compile bse faile pattern, error %s", tstrerror(terrno));
    return terrno;
  }
  TdDirPtr pDir = taosOpenDir(pBse->path);
  if (pDir == NULL) {
    bseError("vgId:%d, path:%s, failed to open since %s", pBse->cfg.vgId, pBse->path, tstrerror(terrno));
    regfree(&bseMetaRegexPattern);
    return terrno;
  }

  TdDirEntryPtr pDirEntry;

  // find existing bse-ver[x].json
  int bseVer = -1;
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *name = taosDirEntryBaseName(taosGetDirEntryName(pDirEntry));
    int   code = regexec(&bseMetaRegexPattern, name, 0, NULL, 0);
    if (code == 0) {
      (void)sscanf(name, "meta-ver%d", &bseVer);
      bseDebug("vgId:%d, bse find current meta: %s is the meta file, ver %d", pBse->cfg.vgId, name, bseVer);
      break;
    }
    bseDebug("vgId:%d, bse find current meta: %s is not meta file", pBse->cfg.vgId, name);
  }
  if (taosCloseDir(&pDir) != 0) {
    bseError("failed to close dir, ret:%s", tstrerror(terrno));
    regfree(&bseMetaRegexPattern);
    return terrno;
  }
  regfree(&bseMetaRegexPattern);
  return bseVer;
}
int32_t bseInitLock(SBse *pBse) {
  TdThreadRwlockAttr attr;
  (void)taosThreadRwlockAttrInit(&attr);
  (void)taosThreadRwlockAttrSetKindNP(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  (void)taosThreadRwlockInit(&pBse->rwlock, &attr);
  (void)taosThreadRwlockAttrDestroy(&attr);
  return 0;
}
int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse) {
  int32_t lino = 0;
  int32_t code = 0;

  SBse *p = taosMemoryMalloc(sizeof(SBse));
  if (p == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }
  p->cfg = *pCfg;
  // init lock
  tstrncpy(p->path, path, sizeof(p->path));

  code = taosMkDir(p->path);
  TAOS_CHECK_GOTO(code, &lino, _err);

  code = bseInitLock(p);
  TAOS_CHECK_GOTO(code, &lino, _err);

  // init table
  code = bseRecover(p);
  TAOS_CHECK_GOTO(code, &lino, _err);

  p->inUse = 0;

  SBseFileInfo *pInfo = taosArrayGetLast(p->fileSet);
  p->seq = pInfo->firstVer + 1;

  char buf[TSDB_FILENAME_LEN] = {0};
  bseBuildDataFullName(p, p->seq, buf);

  code = tableOpen(buf, &p->pTable[p->inUse], 1);
  TAOS_CHECK_GOTO(code, &lino, _err);

  bseBuildDataFullName(p, p->seq + 1, buf);
  code = tableOpen(buf, &p->pTable[1 - p->inUse], 0);
  TAOS_CHECK_GOTO(code, &lino, _err);

  p->pTable[p->inUse]->initSeq = p->seq;
  p->pTable[1 - p->inUse]->initSeq = p->seq + 1;

  // init other mutex
  taosThreadMutexInit(&p->mutex, NULL);

  p->pTableCache = taosHashInit(4096 * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), true, HASH_ENTRY_LOCK);
  if (p->pTableCache == NULL) {
    TAOS_CHECK_GOTO(terrno, &lino, _err);
  }

  *pBse = p;

_err:
  if (code != 0) {
    bseClose(p);
    bseError("failed to open bse since %s", tstrerror(code));
  }
  return code;
}

void bseClose(SBse *pBse) {
  if (pBse == NULL) {
    return;
  }
  bseInfo("start to close bse");
  // bseCommit(pBse);

  tableClose(pBse->pTable[1 - pBse->inUse], 1);
  tableClose(pBse->pTable[pBse->inUse], 1);
  taosArrayDestroy(pBse->fileSet);
  taosThreadMutexDestroy(&pBse->mutex);
  taosThreadRwlockDestroy(&pBse->rwlock);

  void *pIter = taosHashIterate(pBse->pTableCache, NULL);
  while (pIter != NULL) {
    SReaderTableWrapper *p = pIter;
    readerTableClose(p->pTable);

    pIter = taosHashIterate(pBse->pTableCache, pIter);
  }
  taosHashCleanup(pBse->pTableCache);
  taosMemoryFree(pBse);
}

int32_t bseAppend(SBse *pBse, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t  line = 0;
  int32_t  code = 0;
  uint32_t offset = 0;
  uint64_t tseq = 0;

  taosThreadMutexLock(&pBse->mutex);
  tseq = ++pBse->seq;

  code = tableAppendData(pBse->pTable[pBse->inUse], tseq, value, len);
  TAOS_CHECK_GOTO(code, &line, _err);

  *seq = tseq;

  bseDebug("bse succ to append value by seq %" PRIu64, tseq);
_err:
  if (code != 0) {
    bseInfo("bse failed to append value by seq %" PRIu64 " since %s", tseq, tstrerror(code));
  }
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseAppendBatch(SBse *pBse, SBseBatch *pBatch) {
  int32_t code = 0;
  taosThreadMutexLock(&pBse->mutex);
  code = tableAppendBatch(pBse->pTable[pBse->inUse], pBatch);
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}

int32_t bseBatchInit(SBse *pBse, SBseBatch **pBatch, int32_t nKeys) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SBseBatch *p = taosMemoryMalloc(sizeof(SBseBatch));
  if (p == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  uint64_t sseq = 0;

  // atomic later
  taosThreadMutexLock(&pBse->mutex);
  sseq = pBse->seq;
  pBse->seq += nKeys;
  taosThreadMutexUnlock(&pBse->mutex);

  p->pBse = pBse;
  p->len = 0;
  p->seq = sseq++;
  p->cap = 1024;
  p->buf = taosMemCalloc(1, p->cap);
  if (p->buf == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->pOffset = taosHashInit(nKeys, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (p->pOffset == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  *pBatch = p;
_error:
  if (code != 0) {
    bseError("failed to build batch since %s", tstrerror(code));
    bseBatchDestroy(p);
  }
  return code;
}
int32_t bseBatchPut(SBseBatch *pBatch, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t offset = pBatch->len;

  int64_t aseq = pBatch->seq;

  pBatch->seq++;
  pBatch->num++;

  code = bseBatchMayResize(pBatch, pBatch->len + sizeof(uint64_t) + sizeof(uint32_t) + len);
  TSDB_CHECK_CODE(terrno, lino, _error);

  uint8_t *p = pBatch->buf + pBatch->len;
  offset = pBatch->len;
  offset += taosEncodeVariantU64((void **)&p, aseq);
  offset += taosEncodeVariantI32((void **)&p, len);
  offset += taosEncodeBinary((void **)&p, value, len);

  SValueInfo info = {.offset = pBatch->len, .size = offset - pBatch->len, .vlen = len};
  pBatch->len = offset;

  *seq = aseq;
  code = taosHashPut(pBatch->pOffset, &aseq, sizeof((aseq)), &info, sizeof(info));

_error:
  if (code != 0) {
    bseError("failed to put value by seq %" PRId64 " since %s at lino %d", aseq, tstrerror(code), lino);
  }
  return code;
}

int32_t bseBatchGet(SBseBatch *pBatch, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  return 0;
}

int32_t bseBatchCommit(SBseBatch *pBatch) { return bseAppendBatch(pBatch->pBse, pBatch); }

int32_t bseBatchDestroy(SBseBatch *pBatch) {
  if (pBatch == NULL) return 0;

  int32_t code = 0;
  taosMemoryFree(pBatch->buf);
  taosHashCleanup(pBatch->pOffset);

  taosMemFree(pBatch);
  return code;
}

int32_t bseBatchMayResize(SBseBatch *pBatch, int32_t alen) {
  int32_t lino = 0;
  int32_t code = 0;
  if (alen > pBatch->cap) {
    int32_t cap = (pBatch->cap == 0) ? 1 : pBatch->cap;
    if (cap > (INT32_MAX >> 1)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    cap--;
    cap |= cap >> 1;
    cap |= cap >> 2;
    cap |= cap >> 4;
    cap |= cap >> 8;
    cap |= cap >> 16;
    cap++;
    cap = (cap < alen) ? cap << 1 : cap;

    uint8_t *buf = taosMemRealloc(pBatch->buf, cap);
    if (buf == NULL) {
      TSDB_CHECK_CODE(terrno, lino, _error);
    }
    pBatch->cap = cap;
    pBatch->buf = buf;
  }
_error:
  if (code != 0) {
    bseError("failed to resize batch buffer since %s at line %d", tstrerror(code), lino);
  }
  return code;
}
int32_t bseFileSetCmprFn(const void *p1, const void *p2) {
  SBseFileInfo *k1 = (SBseFileInfo *)p1;
  SBseFileInfo *k2 = (SBseFileInfo *)p2;
  if (k1->firstVer < k2->firstVer) return -1;

  return 0;
}

int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;

  taosThreadMutexLock(&pBse->mutex);
  if (seq >= pBse->pTable[pBse->inUse]->initSeq) {
    code = tableLoadBySeq(pBse->pTable[pBse->inUse], seq, pValue, len);
    goto _err;
  } else {
    SBseFileInfo key = {.firstVer = seq};
    // SBseFileInfo *p = taosArraySearch(pBse->fileSet, &key, bseFileSetCmprFn, TD_GT);
    SBseFileInfo *p = NULL;
    for (int32_t i = 0; i < taosArrayGetSize(pBse->fileSet); i++) {
      SBseFileInfo *pInfo = taosArrayGet(pBse->fileSet, i);
      bseDebug("firstVer:%" PRId64, pInfo->firstVer);
      if (pInfo->firstVer >= seq) {
        if ((i + 1) >= taosArrayGetSize(pBse->fileSet)) {
          p = pInfo;
          break;
        } else {
          SBseFileInfo *pNext = taosArrayGet(pBse->fileSet, i + 1);
          if (pNext->firstVer > seq) {
            p = pInfo;
            break;
          }
        }
      }
    }

    if (p == NULL) {
      TAOS_CHECK_GOTO(TSDB_CODE_NOT_FOUND, &line, _err);
    } else {
      SReaderTableWrapper *pTableWraper = taosHashGet(pBse->pTableCache, &p->firstVer, sizeof(p->firstVer));
      if (pTableWraper == NULL) {
        char buf[TSDB_FILENAME_LEN] = {0};
        bseBuildDataFullName(pBse, p->firstVer, buf);

        SReaderTable *pTable = NULL;
        code = readerTableOpen(buf, p->firstVer, &pTable);
        TAOS_CHECK_GOTO(code, &line, _err);

        SReaderTableWrapper tableWraper = {.pTable = pTable};
        code = taosHashPut(pBse->pTableCache, &p->firstVer, sizeof(p->firstVer), &tableWraper,
                           sizeof(SReaderTableWrapper));
        TAOS_CHECK_GOTO(code, &line, _err);

        pTableWraper = taosHashGet(pBse->pTableCache, &p->firstVer, sizeof(p->firstVer));
      }
      code = readerTableGet(pTableWraper->pTable, seq, pValue, len);
    }
  }

_err:
  taosThreadMutexUnlock(&pBse->mutex);
  if (code != 0) {
    bseError("failed to get value by seq %" PRIu64 " since %s", seq, tstrerror(code));
  }
  return code;
}

int32_t seqComparFunc(const void *p1, const void *p2) {
  uint64_t pu1 = *(const uint64_t *)p1;
  uint64_t pu2 = *(const uint64_t *)p2;
  if (pu1 == pu2) {
    return 0;
  } else {
    return (pu1 < pu2) ? -1 : 1;
  }
}
int32_t bseMultiGet(SBse *pBse, SArray *pKey, SArray *ppValue) {
  int32_t code = 0;
  taosSort(pKey->pData, taosArrayGetSize(pKey), sizeof(int64_t), seqComparFunc);

  taosThreadMutexLock(&pBse->mutex);
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseCommit(SBse *pBse) {
  // Generate static info and footer info;
  int32_t code = 0;
  int32_t line = 0;
  int64_t start = taosGetTimestampMs();
  char    buf[TSDB_FILENAME_LEN] = {0};
  char    tbuf[TSDB_FILENAME_LEN] = {0};

  uint64_t oldSeq = 0, newSeq = 0;
  taosThreadMutexLock(&pBse->mutex);
  oldSeq = pBse->seq;
  newSeq = ++pBse->seq;
  pBse->inUse = 1 - pBse->inUse;
  pBse->pTable[pBse->inUse]->initSeq = pBse->seq;

  bseBuildDataFullName(pBse, newSeq, buf);
  if (pBse->pTable[pBse->inUse]->fileOpened == 0) {
    tableOpenFile(pBse->pTable[pBse->inUse], buf);
  }

  taosThreadMutexUnlock(&pBse->mutex);

  code = tableCommit(pBse->pTable[1 - pBse->inUse]);

  tableClear(pBse->pTable[1 - pBse->inUse]);

  SBseFileInfo info = {.firstVer = oldSeq};
  if (taosArrayPush(pBse->fileSet, &info) == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  taosArraySort(pBse->fileSet, cmprBseFileInfo);

  memset(buf, 0, sizeof(buf));
  bseBuildDataFullName(pBse, oldSeq, buf);
  taosRenameFile(pBse->pTable[1 - pBse->inUse]->name, buf);

_err:
  if (code != 0) {
    bseError("bse failed to commit at line %d since %s", line, tstrerror(code));
  } else {
    bseInfo("bse succ to commit, cost %" PRId64 " ms", taosGetTimestampMs() - start);
  }
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
static int32_t readerTableOpen(const char *name, uint64_t lastSeq, SReaderTable **ppTable) {
  int32_t line = 0;
  int32_t code = 0;

  SReaderTable *pTable = taosMemoryCalloc(1, sizeof(SReaderTable));
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

  tstrncpy(pTable->name, name, TSDB_FILENAME_LEN);
  pTable->lastSeq = lastSeq;
  *ppTable = pTable;

  code = readerTableBuild(pTable);
_err:
  if (code != 0) {
    bseError("failed to open table %s at line %d since %s", pTable->name, line, tstrerror(code));
    if (pTable != NULL) {
      readerTableClose(pTable);
    }
  } else {
    bseInfo("bse table file %s succ to be opened", pTable->name);
  }

  return code;
}
static int32_t readerTableClose(SReaderTable *pTable) {
  int32_t line = 0;
  int32_t code = 0;

  code = blockCleanup(&pTable->data);
  TAOS_CHECK_GOTO(code, &line, _err);

  code = blockCleanup(&pTable->bufBlk);
  TAOS_CHECK_GOTO(code, &line, _err);

  taosHashCleanup(pTable->pCache);

  code = taosCloseFile(&pTable->pDataFile);
  TAOS_CHECK_GOTO(code, &line, _err);

  taosMemFree(pTable);
_err:
  if (code != 0) {
    bseError("bse file %s failed to close table since %s", pTable->name, tstrerror(code));
  }
  return code;
}
static int32_t readerTableGet(SReaderTable *pTable, uint64_t key, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  int32_t line = 0;

  uint64_t *offset = taosHashGet(pTable->pCache, &key, sizeof(key));
  if (offset == NULL) {
    goto _err;
    // TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  int32_t blkId = (*offset) / kBlockCap;
  int32_t blkOffset = (*offset) % kBlockCap;

  code = tableLoadBlk((STable *)pTable, blkId, &pTable->bufBlk);
  TAOS_CHECK_GOTO(code, &line, _err);

  code = blockSeekOffset(&pTable->bufBlk, blkOffset, key, pValue, len);
  TAOS_CHECK_GOTO(code, &line, _err);

_err:
  return code;
}

static int32_t readerTableBuild(SReaderTable *pTable) {
  int32_t  line = 0;
  int32_t  code = 0;
  int64_t  size = 0;
  uint8_t *buf = NULL;

  code = taosStatFile(pTable->name, &size, NULL, NULL);
  if (size == 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  pTable->pDataFile = taosOpenFile(pTable->name, TD_FILE_READ);
  if (pTable->pDataFile == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  buf = taosMemoryCalloc(1, kBlockCap);
  if (buf == NULL) {
    TAOS_CHECK_GOTO(terrno, &line, _err);
  }

  do {
    memset(buf, 0, kBlockCap);
    int64_t offset = size > kBlockCap ? ((size / kBlockCap) - 1) * kBlockCap : 0;
    if (offset <= 0) {
      break;
    }
    taosLSeekFile(pTable->pDataFile, offset, SEEK_SET);
    if (taosReadFile(pTable->pDataFile, buf, kBlockCap) == kBlockCap) {
      if (taosCheckChecksumWhole(buf, kBlockCap) == 1) {
        SBlkData2 *pBlk = (SBlkData2 *)buf;
        if (pBlk->head[3] == BSE_META_TYPE) {
          size -= kBlockCap;
          uint8_t *data = pBlk->data;
          uint8_t *p = data;
          uint32_t count = 0;
          do {
            uint64_t seq, offset;
            p = taosDecodeVariantU64(p, &seq);
            p = taosDecodeVariantU64(p, &offset);
            count++;

            code = taosHashPut(pTable->pCache, &seq, sizeof(uint64_t), &offset, sizeof(uint64_t));
            // printf("seq:%" PRIu64 ", offset:%" PRIu64 "\n", seq, offset);
          } while (count < pBlk->len);
        } else {
          break;
        }
      } else {
        code = TSDB_CODE_INVALID_MSG;
        break;
      }
    }
  } while (1);
_err:
  if (code != 0) {
    bseError("table file %s failed to build reader table since %s", pTable->name, tstrerror(code));
    if (pTable->pDataFile != NULL) {
      taosCloseFile(&pTable->pDataFile);
    }
  }
  taosMemFree(buf);
  return code;
}
