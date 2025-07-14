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
#include "bseInc.h"
#include "bseSnapshot.h"
#include "bseTable.h"
#include "bseTableMgt.h"
#include "bseUtil.h"
#include "cJSON.h"

#define BSE_FMT_VER 0x1

static void bseCfgSetDefault(SBseCfg *pCfg);

static int32_t bseClear(SBse *pBse);
static int32_t bseInitEnv(SBse *p);
static int32_t bseInitStartSeq(SBse *pBse);
static int32_t bseRecover(SBse *pBse, int8_t rm);
static int32_t bseGenCommitInfo(SBse *pBse, SArray *pInfo);
static int32_t bseCreateTableManager(SBse *p);
static int32_t bseCommitDo(SBse *pBse, SArray *pFileSet);

static int32_t bseDeserialCommitInfo(SBse *pBse, char *pCurrent, SBseCommitInfo *pInfo);
static int32_t bseSerailCommitInfo(SBse *pBse, SArray *fileSet, char **pBuf, int32_t *len);
static int32_t bseReadCurrentFile(SBse *pBse, char **p, int64_t *len);
static int32_t bseListAllFiles(const char *path, SArray *pFiles);
static int32_t bseRemoveUnCommitFile(SBse *p);

static int32_t bseCreateBatchList(SBse *pBse);

static int32_t bseBatchMgtInit(SBatchMgt *pBatchMgt, SBse *pBse);
static int32_t bseBatchMgtGet(SBatchMgt *pBatchMgt, SBseBatch **pBatch);
static int32_t bseBatchMgtRecycle(SBatchMgt *pBatchMgt, SBseBatch *pBatch);
static void    bseBatchMgtCleanup(SBatchMgt *pBatchMgt);

static int32_t bseBatchCreate(SBseBatch **pBatch, int32_t nKeys);
static int32_t bseBatchClear(SBseBatch *pBatch);
static int32_t bseRecycleBatchImpl(SBatchMgt *pMgt, SBseBatch *pBatch);
static int32_t bseBatchMayResize(SBseBatch *pBatch, int32_t alen);

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

  cJSON_AddNumberToObject(pRoot, "fmtVer", pBse->commitInfo.fmtVer);
  cJSON_AddNumberToObject(pRoot, "vgId", pBse->cfg.vgId);
  cJSON_AddNumberToObject(pRoot, "commitVer", pBse->commitInfo.commitVer);
  cJSON_AddNumberToObject(pRoot, "commitSeq", pBse->commitInfo.lastSeq);
  cJSON_AddItemToObject(pRoot, "fileSet", pFileSet);

  for (int32_t i = 0; i < taosArrayGetSize(fileSet); i++) {
    SBseLiveFileInfo *pInfo = taosArrayGet(fileSet, i);
    cJSON            *pField = cJSON_CreateObject();
    cJSON_AddNumberToObject(pField, "startSeq", pInfo->range.sseq);
    cJSON_AddNumberToObject(pField, "endSeq", pInfo->range.eseq);
    cJSON_AddNumberToObject(pField, "size", pInfo->size);
    cJSON_AddNumberToObject(pField, "level", pInfo->level);
    cJSON_AddNumberToObject(pField, "retention", pInfo->retentionTs);
    cJSON_AddItemToArray(pFileSet, pField);
  }

  char   *pSerialized = cJSON_PrintUnformatted(pRoot);
  int32_t sz = strlen(pSerialized);

  *pBuf = pSerialized;
  *len = sz;

_err:
  if (code != 0) {
    bseError("vgId:%d failed at line %d since %s", pBse->cfg.vgId, line, tstrerror(code));
    cJSON_Delete(pFileSet);
  }
  cJSON_Delete(pRoot);
  pRoot = NULL;
  return code;
}
int32_t bseDeserialCommitInfo(SBse *pBse, char *pCurrent, SBseCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  cJSON  *pRoot = cJSON_Parse(pCurrent);
  if (pRoot == NULL) {
    bseError("vgId:%d, failed to parse current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    TSDB_CHECK_CODE(code, lino, _error);
  }

  cJSON *item = cJSON_GetObjectItem(pRoot, "fmtVer");
  if (item == NULL) {
    bseError("vgId:%d, failed to get fmtVer from current meta", pBse->cfg.vgId);
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _error;
  }
  pInfo->fmtVer = item->valuedouble;

  item = cJSON_GetObjectItem(pRoot, "vgId");
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

  item = cJSON_GetObjectItem(pRoot, "commitSeq");
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
    cJSON *pRetentionTs = cJSON_GetObjectItem(pField, "retention");
    if (pStartSeq == NULL || pEndSeq == NULL || pFileSize == NULL || pLevel == NULL || pRetentionTs == NULL) {
      bseError("vgId:%d, failed to get field from files", pBse->cfg.vgId);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _error;
    }

    SBseLiveFileInfo info = {0};
    info.range.sseq = pStartSeq->valuedouble;
    info.range.eseq = pEndSeq->valuedouble;
    info.size = pFileSize->valuedouble;
    info.level = pLevel->valuedouble;
    info.retentionTs = pRetentionTs->valuedouble;

    if (taosArrayPush(pInfo->pFileList, &info) == NULL) {
      code = terrno;
      goto _error;
    }
  }
_error:
  if (code != 0) {
    bseError("vgId:%d failed to get commit info from current meta since %s", BSE_GET_VGID(pBse), tstrerror(code));
  }
  cJSON_Delete(pRoot);
  return code;
}

int32_t bseReadCurrentFile(SBse *pBse, char **p, int64_t *len) {
  int32_t   code = 0;
  int32_t   lino = 0;
  TdFilePtr fd = NULL;
  int64_t   sz = 0;
  char      name[TSDB_FILENAME_LEN] = {0};

  char *pCurrent = NULL;

  bseBuildCurrentName(pBse, name);
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
  taosCloseFile(&fd);

  *p = pCurrent;
  *len = sz;

_error:
  if (code != 0) {
    bseError("vgId:%d, failed to read current at line %d since %s", pBse->cfg.vgId, lino, tstrerror(code));
    taosCloseFile(&fd);
    taosMemoryFree(pCurrent);
  }
  return code;
}

int32_t bseListAllFiles(const char *path, SArray *pFiles) {
  SBseLiveFileInfo info = {0};

  int32_t code = 0;
  int32_t lino = 0;

  TdDirPtr pDir = taosOpenDir(path);
  if (pDir == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  TdDirEntryPtr de = NULL;
  while ((de = taosReadDir(pDir)) != NULL) {
    char *name = taosGetDirEntryName(de);
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) continue;

    if (strstr(name, BSE_DATA_SUFFIX) == NULL) {
      continue;
    }
    SBseLiveFileInfo info = {0};
    memcpy(info.name, name, strlen(name));

    if (taosArrayPush(pFiles, &info) == NULL) {
      code = terrno;
      goto _error;
    }
  }

_error:
  if (code != 0) {
    bseError("failed to list files at line %d since %s", lino, tstrerror(code));
  }
  taosCloseDir(&pDir);
  return code;
}

int32_t removeUnCommitFile(SBse *p, SArray *pCommitedFiles, SArray *pAllFiles) {
  int32_t code = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pAllFiles); i++) {
    SBseLiveFileInfo *pInfo = taosArrayGet(pAllFiles, i);
    int32_t           found = 0;
    for (int32_t j = 0; j < taosArrayGetSize(pCommitedFiles); j++) {
      SBseLiveFileInfo *pCommited = taosArrayGet(pCommitedFiles, j);
      if (strcmp(pInfo->name, pCommited->name) == 0) {
        found = 1;
        break;
      }
    }
    if (found == 0) {
      char buf[TSDB_FILENAME_LEN] = {0};
      bseBuildFullName(p, pInfo->name, buf);

      code = taosRemoveFile(buf);
      if (code != 0) {
        bseError("vgId:%d failed to remove file %s since %s", p->cfg.vgId, pInfo->name, tstrerror(code));
      } else {
        bseInfo("vgId:%d remove file %s", p->cfg.vgId, pInfo->name);
      }
    }
  }

  return code;
}
int32_t bseRemoveUnCommitFile(SBse *p) {
  int32_t code = 0;

  SArray *pFiles = taosArrayInit(64, sizeof(SBseLiveFileInfo));
  if (pFiles == NULL) {
    return terrno;
  }

  code = bseListAllFiles(p->path, pFiles);
  if (code != 0) {
    taosArrayDestroy(pFiles);
    return code;
  }
  code = removeUnCommitFile(p, p->commitInfo.pFileList, pFiles);
  taosArrayDestroy(pFiles);
  return code;
}

int32_t bseInitStartSeq(SBse *pBse) {
  int32_t code = 0;
  int64_t lastSeq = 0;

  SBseLiveFileInfo *pLastFile = taosArrayGetLast(pBse->commitInfo.pFileList);
  if (pLastFile != NULL) {
    lastSeq = pLastFile->range.eseq;
  }
  pBse->seq = lastSeq + 1;
  return code;
}

int32_t bseRecover(SBse *pBse, int8_t rmUnCommited) {
  int32_t code = 0;
  int32_t lino = 0;
  char   *pCurrent = NULL;
  int64_t len = 0;

  code = bseReadCurrentFile(pBse, &pCurrent, &len);
  TSDB_CHECK_CODE(code, lino, _error);

  if (len == 0) {
    bseInfo("vgId:%d, no current meta file found, no need to recover", BSE_GET_VGID(pBse));
  } else {
    code = bseDeserialCommitInfo(pBse, pCurrent, &pBse->commitInfo);
    TSDB_CHECK_CODE(code, lino, _error);

    if (pBse->commitInfo.fmtVer != BSE_FMT_VER) {
      bseError("vgId:%d, current meta file version %d not match with %d", pBse->cfg.vgId,
               pBse->commitInfo.fmtVer, BSE_FMT_VER);
      code = TSDB_CODE_FILE_CORRUPTED;
      goto _error;
    }

    if (taosArrayGetSize(pBse->commitInfo.pFileList) > 0) {
      SBseLiveFileInfo *pLast = taosArrayGetLast(pBse->commitInfo.pFileList);
      code = bseTableMgtRecoverTable(pBse->pTableMgt, pLast);
      TSDB_CHECK_CODE(code, lino, _error);

      code = bseTableMgtSetLastRetentionTs(pBse->pTableMgt, pLast->retentionTs);
    }
  }

  code = bseInitStartSeq(pBse);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("vgId:%d, failed to recover since %s", pBse->cfg.vgId, tstrerror(code));
  }
  taosMemoryFree(pCurrent);
  return code;
}
int32_t bseInitLock(SBse *pBse) {
  TdThreadRwlockAttr attr;
  (void)taosThreadRwlockAttrInit(&attr);
  (void)taosThreadRwlockAttrSetKindNP(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  (void)taosThreadRwlockInit(&pBse->rwlock, &attr);
  (void)taosThreadRwlockAttrDestroy(&attr);

  taosThreadMutexInit(&pBse->mutex, NULL);
  return 0;
}

int32_t bseInitEnv(SBse *p) {
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

int32_t bseCreateTableManager(SBse *p) { return bseTableMgtCreate(p, (void **)&p->pTableMgt); }

int32_t bseCreateCommitInfo(SBse *pBse) {
  SBseCommitInfo *pCommit = &pBse->commitInfo;
  pCommit->pFileList = taosArrayInit(64, sizeof(SBseLiveFileInfo));
  if (pCommit->pFileList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pCommit->fmtVer = BSE_FMT_VER;
  return 0;
}

void bseCfgSetDefault(SBseCfg *pCfg) {
  if (pCfg == NULL) {
    return;
  }
  if (pCfg->compressType == 0) {
    pCfg->compressType = kLZ4Compres;
  }
  if (pCfg->blockSize == 0) {
    pCfg->blockSize = BSE_DEFAULT_BLOCK_SIZE;
  }

  if (pCfg->keepDays == 0) {
    pCfg->keepDays = 365;
  }
}
int32_t bseOpen(const char *path, SBseCfg *pCfg, SBse **pBse) {
  int32_t lino = 0;
  int32_t code = 0;

  SBse *p = taosMemoryCalloc(1, sizeof(SBse));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _err);
  }

  p->retention = pCfg->retention;
  if (p->retention <= 0) {
    p->retention = 10 * 24 * 3600;  // default to 1 year
  }

  p->keepDays = pCfg->keepDays;

  p->cfg = *pCfg;
  bseCfgSetDefault(&p->cfg);

  tstrncpy(p->path, path, sizeof(p->path));

  code = bseInitEnv(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseCreateTableManager(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseCreateCommitInfo(p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseBatchMgtInit(p->batchMgt, p);
  TSDB_CHECK_CODE(code, lino, _err);

  code = bseRecover(p, 1);
  TSDB_CHECK_CODE(code, lino, _err);

  *pBse = p;
_err:
  if (code != 0) {
    bseError("vgId:%d failed to open bse at line %d since %s", BSE_GET_VGID(p), lino, tstrerror(code));
  }
  return code;
}

static int32_t bseClear(SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  code = bseTableMgtClear(pBse->pTableMgt);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("vgId:%d failed to clear bse at line %d since %s", BSE_GET_VGID(pBse), lino, tstrerror(code));
  }
  return code;
}
void bseClose(SBse *pBse) {
  int32_t code;
  if (pBse == NULL) {
    return;
  }
  bseTableMgtCleanup(pBse->pTableMgt);
  bseBatchMgtCleanup(pBse->batchMgt);

  taosArrayDestroy(pBse->commitInfo.pFileList);
  taosThreadMutexDestroy(&pBse->mutex);
  taosThreadRwlockDestroy(&pBse->rwlock);

  taosMemoryFree(pBse);
  return;
}

int32_t bseGet(SBse *pBse, uint64_t seq, uint8_t **pValue, int32_t *len) {
  int32_t line = 0;
  int32_t code = 0;

  taosThreadRwlockRdlock(&pBse->rwlock);
  code = bseTableMgtGet(pBse->pTableMgt, seq, pValue, len);
  taosThreadRwlockUnlock(&pBse->rwlock);

  if (code != 0) {
    bseError("vgId:%d failed to get value from seq %" PRId64 " at line %d since %s", BSE_GET_VGID(pBse), seq, line,
             tstrerror(code));
  } else {
    bseDebug("vgId:%d get value from seq %" PRId64 " at line %d", BSE_GET_VGID(pBse), seq, line);
  }
  return code;
}

int32_t bseCommitBatch(SBse *pBse, SBseBatch *pBatch) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadMutexLock(&pBse->mutex);
  pBatch->commited = 1;

  while (!BSE_QUEUE_IS_EMPTY(&pBse->batchMgt->queue)) {
    bsequeue *h = BSE_QUEUE_HEAD(&pBse->batchMgt->queue);

    SBseBatch *p = BSE_QUEUE_DATA(h, SBseBatch, node);
    if (p->commited == 1) {
      BSE_QUEUE_REMOVE(&p->node);

      code = bseTableMgtAppend(pBse->pTableMgt, pBatch);
      TSDB_CHECK_CODE(code, lino, _error);

      code = bseRecycleBatchImpl(pBse->batchMgt, p);
      TSDB_CHECK_CODE(code, lino, _error);
    } else {
      break;
    }
  }
_error:
  if (code != 0) {
    bseError("vgId:%d failed to append batch at line %d since %s", BSE_GET_VGID(pBse), lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseReload(SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pBse->mutex);
  code = bseClear(pBse);
  TSDB_CHECK_CODE(code, lino, _error);

  code = bseRecover(pBse, 1);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("vgId:%d failed to reload bse at line %d since %s", BSE_GET_VGID(pBse), lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}
int32_t bseTrim(SBse *pBse) {
  int32_t code = 0;
  return code;
}

int32_t bseRecycleBatch(SBse *pBse, SBseBatch *pBatch) {
  int32_t code = 0;
  if (pBatch == NULL) return code;

  taosThreadMutexLock(&pBse->mutex);
  code = bseRecycleBatchImpl(pBse->batchMgt, pBatch);
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

static int32_t bseBatchMgtInit(SBatchMgt *pBatchMgt, SBse *pBse) {
  int32_t code = 0;
  int32_t lino = 0;

  pBatchMgt->pBse = pBse;

  pBatchMgt->pBatchList = taosArrayInit(2, sizeof(SBseBatch *));
  if (pBatchMgt->pBatchList == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  SBseBatch *b = NULL;
  code = bseBatchCreate(&b, 1024);
  TSDB_CHECK_CODE(code, lino, _error);

  if (taosArrayPush(pBatchMgt->pBatchList, &b) == NULL) {
    bseBatchDestroy(b);
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  BSE_QUEUE_INIT(&pBatchMgt->queue);
_error:
  if (code != 0) {
    if (pBatchMgt->pBatchList != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pBatchMgt->pBatchList); i++) {
        SBseBatch **p = taosArrayGet(pBatchMgt->pBatchList, i);
        bseBatchDestroy(*p);
      }
      taosArrayDestroy(pBatchMgt->pBatchList);
    }
    bseError("vgId:%d failed to init batch mgt at line %d since %s", BSE_GET_VGID(pBse), lino, tstrerror(code));
  }
  return code;
}

static void bseBatchMgtCleanup(SBatchMgt *pBatchMgt) {
  if (pBatchMgt == NULL) return;

  for (int32_t i = 0; i < taosArrayGetSize(pBatchMgt->pBatchList); i++) {
    SBseBatch **p = taosArrayGet(pBatchMgt->pBatchList, i);
    bseBatchDestroy(*p);
  }

  taosArrayDestroy(pBatchMgt->pBatchList);
}

static int32_t bseBatchMgtRecycle(SBatchMgt *pBatchMgt, SBseBatch *pBatch) {
  int32_t code = 0;
  if (pBatch == NULL) return code;

  bseBatchClear(pBatch);

  if (taosArrayPush(pBatchMgt->pBatchList, &pBatch) == NULL) {
    bseBatchDestroy(pBatch);
    code = terrno;
  }
  if (code != 0) {
    bseError("vgId:%d failed to recycle batch since %s", BSE_GET_VGID((SBse *)pBatchMgt->pBse), tstrerror(code));
  }
  return code;
}
static int32_t bseBatchMgtGet(SBatchMgt *pBatchMgt, SBseBatch **pBatch) {
  int32_t code = 0;
  int32_t lino = 0;

  SBseBatch **p;

  if (taosArrayGetSize(pBatchMgt->pBatchList) > 0) {
    p = (SBseBatch **)taosArrayPop(pBatchMgt->pBatchList);
  } else {
    SBseBatch *b = NULL;
    code = bseBatchCreate(&b, 1024);
    TSDB_CHECK_CODE(code, lino, _error);

    if (taosArrayPush(pBatchMgt->pBatchList, &b) == NULL) {
      bseBatchDestroy(b);
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
    p = (SBseBatch **)taosArrayPop(pBatchMgt->pBatchList);
  }

  BSE_QUEUE_PUSH(&pBatchMgt->queue, &((*p)->node));
  *pBatch = *p;

_error:
  if (code != 0) {
    bseInfo("vgId:%d failed to get bse batch at line %d since %s", BSE_GET_VGID((SBse *)pBatchMgt->pBse), lino,
            tstrerror(code));
  }
  return code;
}

int32_t bseRecycleBatchImpl(SBatchMgt *pMgt, SBseBatch *pBatch) {
  // code
  return bseBatchMgtRecycle(pMgt, pBatch);
}

int32_t bseBatchCreate(SBseBatch **pBatch, int32_t nKeys) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SBseBatch *p = taosMemoryCalloc(1, sizeof(SBseBatch));
  if (p == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->len = 0;
  p->seq = 0;
  p->cap = 1024;
  p->buf = taosMemCalloc(1, p->cap);
  if (p->buf == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  p->pSeq = taosArrayInit(nKeys, sizeof(SBlockItemInfo));

  if (p->pSeq == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }
  BSE_QUEUE_INIT(&p->node);

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
  return taosArrayEnsureCap(pBatch->pSeq, cap);
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

  code = bseBatchMgtGet(pBse->batchMgt, &p);
  taosThreadMutexUnlock(&pBse->mutex);

  bseDebug("vgId:%d bse seq start from: %" PRId64 " to %" PRId64 "", BSE_GET_VGID(pBse), sseq, sseq + nKeys - 1);
  TSDB_CHECK_CODE(code, lino, _error);

  code = bseBatchSetParam(p, sseq, nKeys);
  TSDB_CHECK_CODE(code, lino, _error);

  p->startSeq = sseq;
  p->pBse = pBse;
  *pBatch = p;
_error:
  if (code != 0) {
    bseError("vgId:%d failed to build batch since %s", BSE_GET_VGID((SBse *)p->pBse), tstrerror(code));
    bseBatchDestroy(p);
  }
  return code;
}
int32_t bseBatchPut(SBseBatch *pBatch, int64_t *seq, uint8_t *value, int32_t len) {
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

  SBlockItemInfo info = {.size = offset, .seq = lseq};
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
    bseError("vgId:%d failed to put value by seq %" PRId64 " at line %d since %s", BSE_GET_VGID((SBse *)pBatch->pBse),
             lseq, lino, tstrerror(code));
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
  pBatch->commited = 0;
  BSE_QUEUE_REMOVE(&pBatch->node);
  taosArrayClear(pBatch->pSeq);
  return 0;
}

int32_t bseBatchDestroy(SBseBatch *pBatch) {
  if (pBatch == NULL) return 0;

  int32_t code = 0;
  taosMemoryFree(pBatch->buf);
  taosArrayDestroy(pBatch->pSeq);
  BSE_QUEUE_REMOVE(&pBatch->node);

  taosMemoryFree(pBatch);
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

static int32_t seqComparFunc(const void *p1, const void *p2) {
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

int32_t bseGenCommitInfo(SBse *pBse, SArray *pFileSet) {
  int32_t   code = 0;
  int32_t   lino = 0;
  char      buf[TSDB_FILENAME_LEN] = {0};
  char     *pBuf = NULL;
  int32_t   len = 0;
  TdFilePtr fd = NULL;

  code = bseSerailCommitInfo(pBse, pFileSet, &pBuf, &len);
  TSDB_CHECK_CODE(code, lino, _error);

  bseBuildTempCurrentName(pBse, buf);

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
    bseError("vgId:%d failed to gen commit info since %s", BSE_GET_VGID(pBse), tstrerror(code));
  }
  taosMemoryFree(pBuf);

  taosCloseFile(&fd);
  return code;
}

int32_t bseCommitFinish(SBse *pBse) {
  int32_t code = 0;

  char buf[TSDB_FILENAME_LEN] = {0};
  char tbuf[TSDB_FILENAME_LEN] = {0};

  bseBuildCurrentName(pBse, buf);
  bseBuildTempCurrentName(pBse, tbuf);

  code = taosRenameFile(tbuf, buf);
  return code;
}
int32_t bseCommitDo(SBse *pBse, SArray *pFileSet) {
  int32_t code = 0;
  int32_t lino = 0;

  code = bseGenCommitInfo(pBse, pFileSet);
  TSDB_CHECK_CODE(code, lino, _error);

  code = bseCommitFinish(pBse);
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("vgId:%d failed to commit at line %d since %s", BSE_GET_VGID(pBse), lino, tstrerror(code));
  }
  return code;
}

int32_t bseUpdateCommitInfo(SBse *pBse, SBseLiveFileInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pBse->mutex);
  SBseCommitInfo *pCommit = &pBse->commitInfo;
  if (taosArrayGetSize(pCommit->pFileList) == 0) {
    if (taosArrayPush(pCommit->pFileList, pInfo) == NULL) {
      TSDB_CHECK_CODE(code = terrno, lino, _error);
    }
  } else {
    SBseLiveFileInfo *pLast = taosArrayGetLast(pCommit->pFileList);
    if (pLast->retentionTs == pInfo->retentionTs) {
      memcpy(pLast, pInfo, sizeof(SBseLiveFileInfo));
    }
  }
_error:
  if (code != 0) {
    bseError("vgId:%d failed to update commit info since %s", BSE_GET_VGID(pBse), tstrerror(code));
  }

  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseGetAliveFileList(SBse *pBse, SArray **pFileList) {
  int32_t code = 0;
  int32_t lino = 0;
  SArray *p = taosArrayInit(4, sizeof(SBseLiveFileInfo));
  taosThreadMutexLock(&pBse->mutex);
  if (taosArrayAddAll(p, pBse->commitInfo.pFileList) == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _error);
  }

  *pFileList = p;
_error:
  if (code != 0) {
    bseError("vgId:%d failed to get alive file list since %s", BSE_GET_VGID(pBse), tstrerror(code));
  }
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}
int32_t bseCommit(SBse *pBse) {
  // Generate static info and footer info;
  int64_t cost = 0;
  int32_t code = 0;
  int32_t line = 0;
  int64_t st = taosGetTimestampMs();
  SArray *pLiveFile = NULL;

  SBseLiveFileInfo info = {0};
  code = bseTableMgtCommit(pBse->pTableMgt, &info);
  TSDB_CHECK_CODE(code, line, _error);

  if (info.size == 0) {
    bseInfo("vgId:%d no data to commit", BSE_GET_VGID(pBse));
    return 0;
  }

  code = bseUpdateCommitInfo(pBse, &info);
  TSDB_CHECK_CODE(code, line, _error);

  code = bseGetAliveFileList(pBse, &pLiveFile);
  TSDB_CHECK_CODE(code, line, _error);

  if (taosArrayGetSize(pLiveFile) == 0) {
    bseInfo("vgId:%d no alive file to commit", BSE_GET_VGID(pBse));
    taosArrayDestroy(pLiveFile);
    return 0;
  }

  code = bseCommitDo(pBse, pLiveFile);
  TSDB_CHECK_CODE(code, line, _error);

_error:
  cost = taosGetTimestampMs() - st;
  bseWarn("vgId:%d bse commit cost %" PRId64 " ms", BSE_GET_VGID(pBse), cost);
  if (code != 0) {
    bseError("vgId:%d failed to commit at line %d since %s", BSE_GET_VGID(pBse), line, tstrerror(code));
  }
  taosArrayDestroy(pLiveFile);

  return code;
}

int32_t bseRollback(SBse *pBse, int64_t ver) {
  // TODO
  int32_t code = 0;
  return code;
}

int32_t bseRollbackImpl(SBse *pBse) {
  int32_t code = 0;
  return code;
}

int32_t bseCompact(SBse *pBse) {
  // impl later
  int32_t code = 0;
  return code;
}

int32_t bseDelete(SBse *pBse, SSeqRange range) {
  int32_t code = 0;
  return code;
}

int32_t bseUpdateCfg(SBse *pBse, SBseCfg *pCfg) {
  int32_t code = 0;
  if (pCfg == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }

  taosThreadMutexLock(&pBse->mutex);
  if (pCfg->blockSize > 0) {
    pBse->cfg.blockSize = pCfg->blockSize;
  }

  if (pCfg->keepDays > 0) {
    pBse->cfg.keepDays = pCfg->keepDays;
  }

  if (pCfg->compressType >= kNoCompres && pCfg->compressType <= kZxCompress) {
    pBse->cfg.compressType = pCfg->compressType;
  }

  if (pCfg->tableCacheSize >= 0) {
    pBse->cfg.tableCacheSize = pCfg->tableCacheSize;
  }

  if (pCfg->blockCacheSize >= 0) {
    pBse->cfg.blockCacheSize = pCfg->blockCacheSize;
  }
  taosThreadMutexUnlock(&pBse->mutex);
  return code;
}

int32_t bseUpdatCfgNoLock(SBse *pBse, SBseCfg *pCfg) {
  int32_t code = 0;
  if (pCfg == NULL) {
    return TSDB_CODE_INVALID_MSG;
  }
  if (pCfg->blockSize > 0) {
    pBse->cfg.blockSize = pCfg->blockSize;
  }

  if (pCfg->keepDays > 0) {
    pBse->cfg.keepDays = pCfg->keepDays;
  }

  if (pCfg->compressType >= kNoCompres && pCfg->compressType <= kZxCompress) {
    pBse->cfg.compressType = pCfg->compressType;
  }

  if (pCfg->tableCacheSize >= 0) {
    pBse->cfg.tableCacheSize = pCfg->tableCacheSize;
  }

  if (pCfg->blockCacheSize >= 0) {
    pBse->cfg.blockCacheSize = pCfg->blockCacheSize;
  }
  return code;
}
int32_t bseSetCompressType(SBse *pBse, int8_t compressType) {
  int32_t code = 0;
  if (compressType < kNoCompres || compressType > kZxCompress) {
    return TSDB_CODE_INVALID_MSG;
  }
  taosThreadMutexLock(&pBse->mutex);
  pBse->cfg.compressType = compressType;
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
int32_t bseSetBlockSize(SBse *pBse, int32_t blockSize) {
  int32_t code = 0;
  if (blockSize <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  taosThreadMutexLock(&pBse->mutex);
  pBse->cfg.blockSize = blockSize;
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
int32_t bseSetBlockCacheSize(SBse *pBse, int32_t blockCacheSize) {
  int32_t code = 0;
  if (blockCacheSize <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  taosThreadMutexLock(&pBse->mutex);
  pBse->cfg.blockCacheSize = blockCacheSize;

  code = bseTableMgtSetBlockCacheSize(pBse->pTableMgt, blockCacheSize);
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
int32_t bseSetTableCacheSize(SBse *pBse, int32_t tableCacheSize) {
  int32_t code = 0;
  if (tableCacheSize <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  taosThreadMutexLock(&pBse->mutex);

  pBse->cfg.tableCacheSize = tableCacheSize;
  code = bseTableMgtSetTableCacheSize(pBse->pTableMgt, tableCacheSize);
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
int32_t bseSetKeepDays(SBse *pBse, int32_t keepDays) {
  int32_t code = 0;
  if (keepDays <= 0) {
    return TSDB_CODE_INVALID_MSG;
  }
  taosThreadMutexLock(&pBse->mutex);
  pBse->cfg.keepDays = keepDays;
  taosThreadMutexUnlock(&pBse->mutex);

  return code;
}
