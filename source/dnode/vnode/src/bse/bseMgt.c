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
#include "bseTable.h"
#include "bseTableMgt.h"
#include "bseUtil.h"
#include "cJSON.h"
#include "lz4.h"
#include "os.h"
#include "tchecksum.h"
#include "tcompare.h"
#include "tlog.h"
#include "tmsg.h"
#include "tutil.h"

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
static int32_t kBlockCap = 16 * 1024 * 1024;

typedef struct {
  int64_t seq;
  int32_t len;
  int32_t blockId;
} BlockInfo;

static void bseBuildDataFullName(SBse *pBse, int64_t ver, char *name);
static void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *name);
static void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf);

static int32_t bseFindCurrMetaVer(SBse *pBse);

static int32_t bseRecover(SBse *pBse);
static int32_t bseGenCommitInfo(SBse *pBse, SArray *pInfo);

static int32_t bseBatchClear(SBseBatch *pBatch);
static int32_t bseRecycleBatch(SBse *pBse, SBseBatch *pBatch);
static int32_t bseBatchCreate(SBseBatch **pBatch, int32_t nKeys);
static int32_t bseBatchMayResize(SBseBatch *pBatch, int32_t alen);

// data block func
static int32_t blockInit(int32_t blockId, int32_t cap, int8_t type, SBlkData *blk);
static int32_t blockCleanup(SBlkData *data);
// static int32_t blockAdd(SBlkData *blk, uint64_t key, uint8_t *value, int32_t len, uint32_t *offset);
static int8_t  blockMayShouldFlush(SBlkData *data, int32_t len);
static int32_t blockReset(SBlkData *data, uint8_t type, int32_t blkId);
// static int32_t blockSeek(SBlkData *data, uint64_t key, uint32_t blockId, uint8_t *pValue, int32_t *len);
static int32_t blockSeekOffset(SBlkData *data, uint32_t offset, uint64_t key, uint8_t **pValue, int32_t *len);

/*
 vgId: 0,
 commitVer: 0,
 lastVer: 0,
 lastSeq: 0,
 fileSet: [{startSeq: 0, endSeq: 0, size:xx, level:xxx,name:xxx},...],
*/

static int32_t bseUpdateCommitInfo(SBseCommitInfo *pCommit, STableLiveFileInfo *pInfo) {
  if (taosArrayPush(pCommit->pFileList, pInfo) == NULL) {
    return terrno;
  }
  return 0;
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
  uint8_t   *p = pBlk->data + pBlk->len;
  *offset = pBlk->id * kBlockCap + pBlk->len;

  memcpy(p, value, len);
  pBlk->len += len;
  blk->dataNum += len;

  return 0;
}

int32_t blockAddMeta(SBlkData *blk, BlockInfo *pInfo) {
  int32_t    code = 0;
  SBlkData2 *pBlk = blk->pData;
  uint8_t   *p = pBlk->data + pBlk->len;

  pBlk->len += taosEncodeVariantI64((void **)&p, pInfo->seq);
  pBlk->len += taosEncodeVariantI32((void **)&p, pInfo->len);
  pBlk->len += taosEncodeVariantI32((void **)&p, pInfo->blockId);
  blk->dataNum++;
  return 0;
}

int8_t blockMayShouldFlush(SBlkData *data, int32_t len) {
  SBlkData2 *pBlkData = data->pData;
  return (sizeof(SBlkData2) + pBlkData->len + len + sizeof(TSCKSUM)) + sizeof(int32_t) > data->cap;
}

int8_t blockMetaShouldFlush(SBlkData *data, BlockInfo *pInfo) {
  SBlkData2 *pBlkData = data->pData;
  int32_t    len = taosEncodeVariantI64(NULL, pInfo->seq);
  len += taosEncodeVariantI32(NULL, pInfo->len);
  len += taosEncodeVariantI32(NULL, pInfo->blockId);
  return (sizeof(SBlkData2) + pBlkData->len + len + sizeof(TSCKSUM)) + sizeof(int32_t) > data->cap;
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
static void bseBuildCurrentMetaName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-current.meta", pBse->path, TD_DIRSEP);
}

static void bseBuildTempCurrentMetaName(SBse *pBse, char *name) {
  snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-current.meta.temp", pBse->path, TD_DIRSEP);
}

static FORCE_INLINE int32_t bseBuildMetaName(SBse *pBse, int ver, char *name) {
  return snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver%d", pBse->path, TD_DIRSEP, ver);
}
static FORCE_INLINE int32_t bseBuildTempMetaName(SBse *pBse, char *name) {
  return snprintf(name, BSE_FILE_FULL_LEN, "%s%sbse-ver.tmp", pBse->path, TD_DIRSEP);
}

static int32_t bseSerailCommitInfo(SBse *pBse, SArray *fileSet, char **pBuf, int32_t *len) {
  int32_t code = 0;
  // int32_t code = 0;
  int32_t line = 0;

  cJSON *pRoot = cJSON_CreateObject();
  cJSON *pFileSet = cJSON_CreateArray();
  if (pRoot == NULL || pFileSet == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, line, _err);
  }

  cJSON_AddNumberToObject(pRoot, "vgId", pBse->cfg.vgId);
  cJSON_AddNumberToObject(pRoot, "commitVer", pBse->commitInfo.commitVer);
  cJSON_AddNumberToObject(pRoot, "lastVer", pBse->commitInfo.lastVer);
  cJSON_AddNumberToObject(pRoot, "lastSeq", pBse->commitInfo.lastSeq);
  cJSON_AddItemToObject(pRoot, "fileSet", pFileSet);

  for (int32_t i = 0; i < taosArrayGetSize(fileSet); i++) {
    STableLiveFileInfo *pInfo = taosArrayGet(fileSet, i);
    cJSON              *pField = cJSON_CreateObject();
    cJSON_AddNumberToObject(pField, "startSeq", pInfo->sseq);
    cJSON_AddNumberToObject(pField, "endSeq", pInfo->eseq);
    cJSON_AddNumberToObject(pField, "size", pInfo->size);
    cJSON_AddNumberToObject(pField, "level", pInfo->level);
    cJSON_AddStringToObject(pField, "name", pInfo->name);
    cJSON_AddItemToArray(pFileSet, pField);
  }

  char   *pSerialized = cJSON_PrintUnformatted(pRoot);
  int32_t sz = strlen(pSerialized);
  cJSON_Delete(pRoot);

_err:
  bseError("vgId:%d, %s failed at line %d since %s", pBse->cfg.vgId, __func__, line, tstrerror(code));
  cJSON_Delete(pRoot);
  cJSON_Delete(pFileSet);
  return code;
}

static int32_t bseReadCurrent(SBse *pBse, char **p, int64_t *len) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr fd = NULL;
  int64_t   sz = 0;
  char      name[TSDB_FILENAME_LEN] = {0};

  char *pCurrent = NULL;

  bseBuildCurrentMetaName(pBse, name);
  if (taosCheckExistFile(name) == 0) {
    bseInfo("vgId:%d, no current meta file found, skip recover", pBse->cfg.vgId);
    return 0;
  }
  code = taosStatFile(name, &sz, NULL, NULL);
  TSDB_CHECK_CODE(code, lino, _error);

  fd = taosOpenFile(name, TD_FILE_READ);
  if (fd == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  pCurrent = (char *)taosMemoryCalloc(1, sz + 1);
  if (pCurrent == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int64_t nread = taosReadFile(fd, pCurrent, sz);
  if (nread != sz) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  *p = pCurrent;
  *len = sz;

_error:
  if (code != 0) {
    bseError("vgId:%d, failed to read current since %s at line %d", pBse->cfg.vgId, tstrerror(code), lino);
    taosCloseFile(&fd);
    taosMemFree(pCurrent);
  }
  return code;
}

static int32_t bseInitCommitInfo(SBse *pBse, char *pCurrent, SBseCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  cJSON  *pRoot = cJSON_Parse(pCurrent);
  if (pRoot == NULL) {
    bseError("vgId:%d, failed to parse current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _error);
  }
  cJSON *item = cJSON_GetObjectItem(pRoot, "vgId");
  if (item == NULL) {
    bseError("vgId:%d, failed to get vgId from current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _error;
  }
  pInfo->vgId = item->valuedouble;

  item = cJSON_GetObjectItem(pRoot, "commitVer");
  if (item == NULL) {
    bseError("vgId:%d, failed to get commitVer from current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _error;
  }
  pInfo->commitVer = item->valuedouble;

  item = cJSON_GetObjectItem(pRoot, "lastVer");
  if (item == NULL) {
    bseError("vgId:%d, failed to get lastVer from current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _error;
  }
  pInfo->lastVer = item->valuedouble;

  item = cJSON_GetObjectItem(pRoot, "lastSeq");
  if (item == NULL) {
    bseError("vgId:%d, failed to get lastSeq from current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _error;
  }
  pInfo->lastSeq = item->valuedouble;

  cJSON *pFiles = cJSON_GetObjectItem(pRoot, "fileSet");
  cJSON *pField = NULL;
  cJSON_ArrayForEach(pField, pFiles) {
    cJSON *pStartSeq = cJSON_GetObjectItem(pField, "startSeq");
    cJSON *pEndSeq = cJSON_GetObjectItem(pField, "endSeq");
    cJSON *pFileSize = cJSON_GetObjectItem(pField, "size");
    cJSON *pLevel = cJSON_GetObjectItem(pField, "level");
    cJSON *pName = cJSON_GetObjectItem(pField, "name");
    if (pStartSeq == NULL || pEndSeq == NULL || pFileSize == NULL || pLevel == NULL || pName == NULL) {
      bseError("vgId:%d, failed to get field from files", pBse->cfg.vgId);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _error;
    }
    STableLiveFileInfo info = {0};
    info.sseq = pStartSeq->valuedouble;
    info.eseq = pEndSeq->valuedouble;
    info.size = pFileSize->valuedouble;
    info.level = pLevel->valuedouble;
    strncpy(info.name, pName->valuestring, sizeof(info.name));

    if (taosArrayPush(pInfo->pFileList, &info) == NULL) {
      code = terrno;
      goto _error;
    }
  }
_error:
  if (code != 0) {
    bseError("vgId:%d failed to get commit info from current meta since %s", BSE_VGID(pBse), tstrerror(code));
  }
  return code;
}
static int32_t bseRecover(SBse *pBse) {
  int32_t        code = 0;
  int32_t        lino = 0;
  char          *pCurrent = NULL;
  int64_t        len = 0;
  SBseCommitInfo info = {0};

  code = bseReadCurrent(pBse, &pCurrent, &len);
  TSDB_CHECK_CODE(code, lino, _error);

  if (len == 0) {
    bseInfo("vgId:%d, no current meta file found, no need to recover", BSE_VGID(pBse));
    return 0;
  }
  code = bseInitCommitInfo(pBse, pCurrent, &info);

_error:
  if (code != 0) {
    bseError("vgId:%d, failed to recover since %s", pBse->cfg.vgId, tstrerror(code));
  }
  taosMemFree(pCurrent);
  return code;
}
int32_t bseInitLock(SBse *pBse) {
  TdThreadRwlockAttr attr;
  (void)taosThreadRwlockAttrInit(&attr);
  (void)taosThreadRwlockAttrSetKindNP(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  (void)taosThreadRwlockInit(&pBse->rwlock, &attr);
  (void)taosThreadRwlockAttrDestroy(&attr);
  return 0;
}

static int32_t bseInitEnv(SBse *p) {
  int32_t code = 0;
  int32_t lino = 0;

  code = bseInitLock(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = taosMkDir(p->path);
  TSDB_CHECK_CODE(code, lino, _err);
_err:
  if (code != 0) {
    bseError("failed to init bse env at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

static int32_t bseCreateTableManager(SBse *p) { return bseTableMgtInit(p, (void **)&p->pTableMgt); }

static int32_t bseCreateCommitInfo(SBse *pBse) {
  SBseCommitInfo *pCommit = &pBse->commitInfo;
  pCommit->pFileList = taosArrayInit(64, sizeof(STableLiveFileInfo));
  if (pCommit->pFileList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return 0;
}
int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse) {
  int32_t lino = 0;
  int32_t code = 0;

  SBse *p = taosMemoryMalloc(sizeof(SBse));
  if (p == NULL) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _err);
  }
  p->cfg = *pCfg;
  tstrncpy(p->path, path, sizeof(p->path));

  code = bseInitEnv(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseCreateTableManager(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseCreateCommitInfo(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseRecover(p);
  TSDB_CHECK_CODE(code, lino, _err);

_err:
  if (code != 0) {
    bseError("vgId:%d failed to open bse at line %d since %s", BSE_VGID(p), lino, tstrerror(code));
  }
  return code;
}

void bseClose(SBse *pBse) {
  int32_t code;
  if (pBse == NULL) {
    return;
  }

  return;
}
int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;
  return code;
}

int32_t bseAppendBatch(SBse *pBse, SBseBatch *pBatch) {
  int32_t code = 0;
  taosThreadMutexLock(&pBse->mutex);
  code = 0;  // tableAppendBatch(pBse->pTable[pBse->inUse], pBatch);
  code = bseRecycleBatch(pBse, pBatch);

  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
int32_t bseGetOrCreateBatch(SBse *pBse, SBseBatch **pBatch) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SBseBatch **p;

  if (taosArrayGetSize(pBse->pBatchList) > 0) {
    p = (SBseBatch **)taosArrayPop(pBse->pBatchList);
  } else {
    SBseBatch *b = NULL;
    code = bseBatchCreate(&b, 1024);
    TSDB_CHECK_CODE(code, lino, _error);

    if (taosArrayPush(pBse->pBatchList, &b) == NULL) {
      bseBatchDestroy(b);
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
    p = (SBseBatch **)taosArrayPop(pBse->pBatchList);
  }
  *pBatch = *p;

_error:
  if (code != 0) {
    bseInfo("vgId:%d failed to get bse batch since %s at line %d", BSE_VGID(pBse), tstrerror(code), lino);
  }
  return code;
}

int32_t bseRecycleBatch(SBse *pBse, SBseBatch *pBatch) {
  int32_t code = 0;

  bseBatchClear(pBatch);
  if (taosArrayPush(pBse->pBatchList, &pBatch) == NULL) {
    code = terrno;
  }
  return code;
}

int32_t bseBatchCreate(SBseBatch **pBatch, int32_t nKeys) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SBseBatch *p = taosMemoryCalloc(1, sizeof(SBseBatch));
  TSDB_CHECK_CODE(code = terrno, lino, _error);

  p->len = 0;
  p->seq = 0;
  p->cap = 1024;
  p->buf = taosMemCalloc(1, p->cap);
  if (p->buf == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->pSeq = taosArrayInit(nKeys, sizeof(SValueInfo));
  if (p->pSeq == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  *pBatch = p;

_error:
  if (code != 0) {
    bseError("failed to create bse batch since %s at line %d", tstrerror(code), lino);
    bseBatchDestroy(p);
  }
  return code;
}
int32_t bseBatchSetParam(SBseBatch *pBatch, int64_t seq, int32_t cap) {
  pBatch->seq = seq;
  taosArrayEnsureCap(pBatch->pSeq, cap);

  return 0;
}
int32_t bseBatchInit(SBse *pBse, SBseBatch **pBatch, int32_t nKeys) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SBseBatch *p = NULL;
  uint64_t   sseq = 0;

  // atomic later
  taosThreadMutexLock(&pBse->mutex);
  sseq = pBse->seq;
  pBse->seq += nKeys;
  code = bseGetOrCreateBatch(pBse, &p);
  taosThreadMutexUnlock(&pBse->mutex);

  TSDB_CHECK_CODE(code, lino, _error);

  code = bseBatchSetParam(p, sseq, nKeys);

  p->pBse = pBse;

  TSDB_CHECK_CODE(code, lino, _error);

  *pBatch = p;
_error:
  if (code != 0) {
    bseError("vgId:%d failed to build batch since %s", BSE_VGID((SBse *)p->pBse), tstrerror(code));
    bseBatchDestroy(p);
  }
  return code;
}
int32_t bseBatchPut(SBseBatch *pBatch, uint64_t *seq, uint8_t *value, int32_t len) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t offset = 0;

  int64_t lseq = pBatch->seq;

  code = bseBatchMayResize(pBatch, pBatch->len + sizeof(int64_t) + sizeof(int32_t) + len);
  TSDB_CHECK_CODE(code, lino, _error);

  uint8_t *p = pBatch->buf + pBatch->len;
  offset += taosEncodeVariantI64((void **)&p, lseq);
  offset += taosEncodeVariantI32((void **)&p, len);
  offset += taosEncodeBinary((void **)&p, value, len);

  SValueInfo info = {.offset = pBatch->len, .size = offset, .vlen = len, .seq = lseq};
  pBatch->len += offset;

  if (taosArrayPush(pBatch->pSeq, &info) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  pBatch->seq++;
  pBatch->num++;

  *seq = lseq;
  bseDebug("succ to put seq %" PRId64 " to batch", lseq);

_error:
  if (code != 0) {
    bseError("vgId:%d failed to put value by seq %" PRId64 " since %s at lino %d", BSE_VGID((SBse *)pBatch->pBse), lseq,
             tstrerror(code), lino);
  }
  return code;
}

int32_t bseBatchGetSize(SBseBatch *pBatch, int32_t *sz) {
  int32_t code = 0;

  if (pBatch == NULL) return TSDB_CODE_INVALID_MSG;
  *sz = pBatch->len;

  return code;
}

int32_t bseBatchGet(SBseBatch *pBatch, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t code = 0;
  return 0;
}
int32_t bseBatchClear(SBseBatch *pBatch) {
  pBatch->len = 0;
  pBatch->num = 0;
  pBatch->seq = 0;
  taosArrayClear(pBatch->pSeq);
  return 0;
}

int32_t bseBatchDestroy(SBseBatch *pBatch) {
  if (pBatch == NULL) return 0;

  int32_t code = 0;
  taosMemoryFree(pBatch->buf);
  taosArrayDestroy(pBatch->pSeq);

  taosMemFree(pBatch);
  return code;
}

int32_t bseBatchMayResize(SBseBatch *pBatch, int32_t alen) {
  int32_t lino = 0;
  int32_t code = 0;
  if (alen > pBatch->cap) {
    int32_t cap = pBatch->cap;
    while (cap < alen) {
      cap <<= 1;
    }

    uint8_t *buf = taosMemRealloc(pBatch->buf, cap);
    if (buf == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }

    pBatch->cap = cap;
    pBatch->buf = buf;
  } else {
    return code;
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
int32_t bseIterate(SBse *pBse, uint64_t start, uint64_t end, SArray *pValue) {
  int32_t code = 0;
  taosThreadMutexLock(&pBse->mutex);
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

static int32_t bseGenCommitInfo(SBse *pBse, SArray *pFileSet) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char      buf[TSDB_FILENAME_LEN] = {0};
  char     *pBuf = NULL;
  int32_t   len = 0;
  TdFilePtr fd = NULL;
  code = bseSerailCommitInfo(pBse, pFileSet, &pBuf, &len);
  TSDB_CHECK_CODE(code, lino, _error);

  bseBuildTempCurrentMetaName(pBse, buf);

  fd = taosOpenFile(buf, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (fd == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  int64_t nwrite = taosWriteFile(fd, pBuf, len);
  if (nwrite != len) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  code = taosFsyncFile(fd);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("vgId:%d failed to gen commit info since %s", BSE_VGID(pBse), tstrerror(code));
  }
  taosMemFree(pBuf);

  taosCloseFile(&fd);
  return code;
}

int32_t bseCommitFinish(SBse *pBse) {
  int32_t code = 0;
  return code;

  char buf[TSDB_FILENAME_LEN] = {0};
  char tbuf[TSDB_FILENAME_LEN] = {0};

  bseBuildCurrentMetaName(pBse, buf);
  bseBuildTempMetaName(pBse, tbuf);

  code = taosRenameFile(tbuf, buf);
  return code;
}
int32_t bseCommit(SBse *pBse) {
  // Generate static info and footer info;
  int64_t st = taosGetTimestampMs();
  int64_t cost = 0;
  int32_t code = 0;
  int32_t line = 0;

  code = bseTableMgtCommit(pBse->pTableMgt);
  TSDB_CHECK_CODE(code, line, _error);

  code = bseGenCommitInfo(pBse, ((STableMgt *)pBse->pTableMgt)->pFileList);
  TSDB_CHECK_CODE(code, line, _error);

  bseCommitFinish(pBse);

_error:
  cost = taosGetTimestampMs() - st;
  if (cost > 100) {
    bseWarn("vgId:%d bse commit cost %" PRId64 " ms", BSE_VGID(pBse), cost);
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
