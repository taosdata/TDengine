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
#include "tlog.h"
#include "tmsg.h"

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

#define kBlockTrailerSize (1 + 1 + 4)  // complete flag + compress type + crc unmask

static int32_t kBlockCap = 512;

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

int32_t blockAddMeta(SBlkData *blk, uint64_t key, SValueInfo *pValue, uint64_t *offset) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;

  uint8_t *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;
  pBlk->len += taosEncodeVariantU64((void **)&p, key);
  pBlk->len += taosEncodeVariantU64((void **)&p, pValue->offset);
  pBlk->len += taosEncodeVariantI32((void **)&p, pValue->size);

  blk->dataNum++;
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
  data->len = 0;
  data->id = blockId;
  data->dataNum = 0;

  // memset(pBlkData->head, 0, sizeof(pBlkData->head));
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

static void bseBuildDataFullName(SBse *pBse, char *name) {
  // build data file name
  snprintf(name, strlen(name), "%s%s%s", pBse->path, TD_DIRSEP, "data");
}

static void bseBuildIndexFullName(SBse *pBse, char *name) {
  // build index file name
  snprintf(name, strlen(name), "%s%s%s", pBse->path, TD_DIRSEP, "index");
}
static void bseBuildLogFullName(SBse *pBse, char *name) {
  // build log file name
  snprintf(name, strlen(name), "%s%s%s", pBse->path, TD_DIRSEP, "log");
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
    code = blockAddMeta(&pTable->data, key, value, &offset);
    pIter = taosHashIterate(pTable->pCache, pIter);
  }

  code = tableFlushBlock(pTable);
  TAOS_CHECK_GOTO(code, &line, _err);

  TAOS_CHECK_GOTO(blockReset(&pTable->data, BSE_FOOTER_TYPE, pTable->blockId), &line, _err);

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
static char *bseFilexSuffix[] = {
    "idx",
    "data",
    "log",
};

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

typedef struct {
  int64_t firstVer;
  int64_t lastVer;
  int64_t createTs;
  int64_t closeTs;
  int64_t fileSize;
  int64_t syncedOffset;
} SBseFileInfo;

static int32_t bseFindCurrMetaVer(SBse *pBse);

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

  code = bseInitLock(p);
  TAOS_CHECK_GOTO(code, &lino, _err);

  code = tableOpen(path, &p->pTable);
  TAOS_CHECK_GOTO(code, &lino, _err);

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
  taosThreadMutexDestroy(&pBse->mutex);
  taosThreadRwlockDestroy(&pBse->rwlock);
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