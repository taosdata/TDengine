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

#include "meta.h"
#include "sync.h"
#include "vnd.h"
#include "vnodeInt.h"

extern int32_t tsdbPreCommit(STsdb *pTsdb);
extern int32_t tsdbCommitBegin(STsdb *pTsdb, SCommitInfo *pInfo);
extern int32_t tsdbCommitCommit(STsdb *pTsdb);
extern int32_t tsdbCommitAbort(STsdb *pTsdb);

#define VND_INFO_FNAME_TMP "vnode_tmp.json"

static int vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData);
static int vnodeCommitImpl(SCommitInfo *pInfo);

#define WAIT_TIME_MILI_SEC 10  // miliseconds

static int32_t vnodeTryRecycleBufPool(SVnode *pVnode) {
  int32_t code = 0;

  if (pVnode->onRecycle == NULL) {
    if (pVnode->recycleHead == NULL) {
      vDebug("vgId:%d, no recyclable buffer pool", TD_VID(pVnode));
      goto _exit;
    } else {
      vDebug("vgId:%d, buffer pool %p of id %d on recycle queue, try to recycle", TD_VID(pVnode), pVnode->recycleHead,
             pVnode->recycleHead->id);

      pVnode->onRecycle = pVnode->recycleHead;
      if (pVnode->recycleHead == pVnode->recycleTail) {
        pVnode->recycleHead = pVnode->recycleTail = NULL;
      } else {
        pVnode->recycleHead = pVnode->recycleHead->recycleNext;
        pVnode->recycleHead->recyclePrev = NULL;
      }
      pVnode->onRecycle->recycleNext = pVnode->onRecycle->recyclePrev = NULL;
    }
  }

  code = vnodeBufPoolRecycle(pVnode->onRecycle);
  if (code) goto _exit;

_exit:
  if (code) {
    vError("vgId:%d, %s failed since %s", TD_VID(pVnode), __func__, tstrerror(code));
  }
  return code;
}
static int32_t vnodeGetBufPoolToUse(SVnode *pVnode) {
  int32_t code = 0;
  int32_t lino = 0;

  (void)taosThreadMutexLock(&pVnode->mutex);

  int32_t nTry = 0;
  for (;;) {
    ++nTry;

    if (pVnode->freeList) {
      vDebug("vgId:%d, allocate free buffer pool on %d try, pPool:%p id:%d", TD_VID(pVnode), nTry, pVnode->freeList,
             pVnode->freeList->id);

      pVnode->inUse = pVnode->freeList;
      pVnode->inUse->nRef = 1;
      pVnode->freeList = pVnode->inUse->freeNext;
      pVnode->inUse->freeNext = NULL;
      break;
    } else {
      vDebug("vgId:%d, no free buffer pool on %d try, try to recycle...", TD_VID(pVnode), nTry);

      code = vnodeTryRecycleBufPool(pVnode);
      TSDB_CHECK_CODE(code, lino, _exit);

      if (pVnode->freeList == NULL) {
        vDebug("vgId:%d, no free buffer pool on %d try, wait %d ms...", TD_VID(pVnode), nTry, WAIT_TIME_MILI_SEC);

        struct timeval  tv;
        struct timespec ts;
        (void)taosGetTimeOfDay(&tv);
        ts.tv_nsec = tv.tv_usec * 1000 + WAIT_TIME_MILI_SEC * 1000000;
        if (ts.tv_nsec > 999999999l) {
          ts.tv_sec = tv.tv_sec + 1;
          ts.tv_nsec -= 1000000000l;
        } else {
          ts.tv_sec = tv.tv_sec;
        }

        int32_t rc = taosThreadCondTimedWait(&pVnode->poolNotEmpty, &pVnode->mutex, &ts);
        if (rc && rc != ETIMEDOUT) {
          code = TAOS_SYSTEM_ERROR(rc);
          TSDB_CHECK_CODE(code, lino, _exit);
        }
      }
    }
  }

_exit:
  (void)taosThreadMutexUnlock(&pVnode->mutex);
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}
int vnodeBegin(SVnode *pVnode) {
  int32_t code = 0;
  int32_t lino = 0;

  // alloc buffer pool
  code = vnodeGetBufPoolToUse(pVnode);
  TSDB_CHECK_CODE(code, lino, _exit);

  // begin meta
  code = metaBegin(pVnode->pMeta, META_BEGIN_HEAP_BUFFERPOOL);
  TSDB_CHECK_CODE(code, lino, _exit);

  // begin tsdb
  code = tsdbBegin(pVnode->pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  // begin sma
  if (VND_IS_RSMA(pVnode)) {
    code = smaBegin(pVnode->pSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

_exit:
  if (code) {
    terrno = code;
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

int vnodeShouldCommit(SVnode *pVnode, bool atExit) {
  bool diskAvail = osDataSpaceAvailable();
  bool needCommit = false;

  (void)taosThreadMutexLock(&pVnode->mutex);
  if (pVnode->inUse && diskAvail) {
    needCommit = (pVnode->inUse->size > pVnode->inUse->node.size) ||
                 (atExit && (pVnode->inUse->size > 0 || pVnode->pMeta->changed ||
                             pVnode->state.applied - pVnode->state.committed > 4096));
  }
  vTrace("vgId:%d, should commit:%d, disk available:%d, buffer size:%" PRId64 ", node size:%" PRId64
         ", meta changed:%d"
         ", state:[%" PRId64 ",%" PRId64 "]",
         TD_VID(pVnode), needCommit, diskAvail, pVnode->inUse ? pVnode->inUse->size : 0,
         pVnode->inUse ? pVnode->inUse->node.size : 0, pVnode->pMeta->changed, pVnode->state.applied,
         pVnode->state.committed);
  (void)taosThreadMutexUnlock(&pVnode->mutex);
  return needCommit;
}

int vnodeSaveInfo(const char *dir, const SVnodeInfo *pInfo) {
  int32_t   code = 0;
  int32_t   lino;
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile = NULL;
  char     *data = NULL;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  code = vnodeEncodeInfo(pInfo, &data);
  TSDB_CHECK_CODE(code, lino, _exit);

  // save info to a vnode_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(errno), lino, _exit);
  }

  if (taosWriteFile(pFile, data, strlen(data)) < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  if (taosFsyncFile(pFile) < 0) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(errno), lino, _exit);
  }

_exit:
  if (code) {
    vError("vgId:%d %s failed at %s:%d since %s", pInfo->config.vgId, __func__, __FILE__, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d, vnode info is saved, fname:%s replica:%d selfIndex:%d changeVersion:%d", pInfo->config.vgId, fname,
          pInfo->config.syncCfg.replicaNum, pInfo->config.syncCfg.myIndex, pInfo->config.syncCfg.changeVersion);
  }
  (void)taosCloseFile(&pFile);
  taosMemoryFree(data);
  return code;
}

int vnodeCommitInfo(const char *dir) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  int32_t code = taosRenameFile(tfname, fname);
  if (code < 0) {
    return code;
  }

  vInfo("vnode info is committed, dir:%s", dir);
  return 0;
}

int vnodeLoadInfo(const char *dir, SVnodeInfo *pInfo) {
  int32_t   code = 0;
  int32_t   lino;
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  int64_t   size;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);

  // read info
  pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    TSDB_CHECK_CODE(code = TAOS_SYSTEM_ERROR(errno), lino, _exit);
  }

  code = taosFStatFile(pFile, &size, NULL);
  TSDB_CHECK_CODE(code, lino, _exit);

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_OUT_OF_MEMORY, lino, _exit);
  }

  if (taosReadFile(pFile, pData, size) < 0) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  pData[size] = '\0';

  // decode info
  code = vnodeDecodeInfo(pData, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    if (pFile) {
      vError("vgId:%d %s failed at %s:%d since %s", pInfo->config.vgId, __func__, __FILE__, lino, tstrerror(code));
    }
  }
  taosMemoryFree(pData);
  (void)taosCloseFile(&pFile);
  return code;
}

static int32_t vnodePrepareCommit(SVnode *pVnode, SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  char    dir[TSDB_FILENAME_LEN] = {0};
  int64_t lastCommitted = pInfo->info.state.committed;

  // wait last commit task
  (void)vnodeAWait(&pVnode->commitTask);

  code = syncNodeGetConfig(pVnode->sync, &pVnode->config.syncCfg);
  TSDB_CHECK_CODE(code, lino, _exit);

  pVnode->state.commitTerm = pVnode->state.applyTerm;

  pInfo->info.config = pVnode->config;
  pInfo->info.state.committed = pVnode->state.applied;
  pInfo->info.state.commitTerm = pVnode->state.applyTerm;
  pInfo->info.state.commitID = ++pVnode->state.commitID;
  pInfo->pVnode = pVnode;
  pInfo->txn = metaGetTxn(pVnode->pMeta);

  // save info
  (void)vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

  vDebug("vgId:%d, save config while prepare commit", TD_VID(pVnode));
  code = vnodeSaveInfo(dir, &pInfo->info);
  TSDB_CHECK_CODE(code, lino, _exit);

  (void)tsdbPreCommit(pVnode->pTsdb);

  code = metaPrepareAsyncCommit(pVnode->pMeta);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = smaPrepareAsyncCommit(pVnode->pSma);
  TSDB_CHECK_CODE(code, lino, _exit);

  (void)taosThreadMutexLock(&pVnode->mutex);
  pVnode->onCommit = pVnode->inUse;
  pVnode->inUse = NULL;
  (void)taosThreadMutexUnlock(&pVnode->mutex);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pVnode->state.commitID);
  } else {
    vDebug("vgId:%d, %s done, commit id:%" PRId64, TD_VID(pVnode), __func__, pInfo->info.state.commitID);
  }

  return code;
}
static void vnodeReturnBufPool(SVnode *pVnode) {
  (void)taosThreadMutexLock(&pVnode->mutex);

  SVBufPool *pPool = pVnode->onCommit;
  int32_t    nRef = atomic_sub_fetch_32(&pPool->nRef, 1);

  pVnode->onCommit = NULL;
  if (nRef == 0) {
    vnodeBufPoolAddToFreeList(pPool);
  } else if (nRef > 0) {
    vDebug("vgId:%d, buffer pool %p of id %d is added to recycle queue", TD_VID(pVnode), pPool, pPool->id);

    if (pVnode->recycleTail == NULL) {
      pPool->recyclePrev = pPool->recycleNext = NULL;
      pVnode->recycleHead = pVnode->recycleTail = pPool;
    } else {
      pPool->recyclePrev = pVnode->recycleTail;
      pPool->recycleNext = NULL;
      pVnode->recycleTail->recycleNext = pPool;
      pVnode->recycleTail = pPool;
    }
  } else {
    vError("vgId:%d, buffer pool %p of id %d nRef:%d", TD_VID(pVnode), pPool, pPool->id, nRef);
  }

  (void)taosThreadMutexUnlock(&pVnode->mutex);
}
static int32_t vnodeCommit(void *arg) {
  int32_t code = 0;

  SCommitInfo *pInfo = (SCommitInfo *)arg;
  SVnode      *pVnode = pInfo->pVnode;

  // commit
  if ((code = vnodeCommitImpl(pInfo))) {
    vFatal("vgId:%d, failed to commit vnode since %s", TD_VID(pVnode), terrstr());
    taosMsleep(100);
    exit(EXIT_FAILURE);
    goto _exit;
  }

  vnodeReturnBufPool(pVnode);

_exit:
  taosMemoryFree(arg);
  return code;
}

static void vnodeCommitCancel(void *arg) { taosMemoryFree(arg); }

int vnodeAsyncCommit(SVnode *pVnode) {
  int32_t code = 0;
  int32_t lino = 0;

  SCommitInfo *pInfo = (SCommitInfo *)taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  // prepare to commit
  code = vnodePrepareCommit(pVnode, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  // schedule the task
  code =
      vnodeAsync(&pVnode->commitChannel, EVA_PRIORITY_HIGH, vnodeCommit, vnodeCommitCancel, pInfo, &pVnode->commitTask);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    taosMemoryFree(pInfo);
    vError("vgId:%d %s failed at line %d since %s" PRId64, TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}

int vnodeSyncCommit(SVnode *pVnode) {
  (void)vnodeAsyncCommit(pVnode);
  (void)vnodeAWait(&pVnode->commitTask);
  return 0;
}

static int vnodeCommitImpl(SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  char    dir[TSDB_FILENAME_LEN] = {0};
  SVnode *pVnode = pInfo->pVnode;

  vInfo("vgId:%d, start to commit, commitId:%" PRId64 " version:%" PRId64 " term: %" PRId64, TD_VID(pVnode),
        pInfo->info.state.commitID, pInfo->info.state.committed, pInfo->info.state.commitTerm);

  // persist wal before starting
  if ((code = walPersist(pVnode->pWal)) < 0) {
    vError("vgId:%d, failed to persist wal since %s", TD_VID(pVnode), tstrerror(code));
    return code;
  }

  (void)vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

  (void)syncBeginSnapshot(pVnode->sync, pInfo->info.state.committed);

  code = tsdbCommitBegin(pVnode->pTsdb, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (!TSDB_CACHE_NO(pVnode->config)) {
    code = tsdbCacheCommit(pVnode->pTsdb);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (VND_IS_RSMA(pVnode)) {
    code = smaCommit(pVnode->pSma, pInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // commit info
  code = vnodeCommitInfo(dir);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tsdbCommitCommit(pVnode->pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (VND_IS_RSMA(pVnode)) {
    code = smaFinishCommit(pVnode->pSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = metaFinishCommit(pVnode->pMeta, pInfo->txn);
  TSDB_CHECK_CODE(code, lino, _exit);

  pVnode->state.committed = pInfo->info.state.committed;

  if (smaPostCommit(pVnode->pSma) < 0) {
    vError("vgId:%d, failed to post-commit sma since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  (void)syncEndSnapshot(pVnode->sync);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d, commit end", TD_VID(pVnode));
  }
  return code;
}

bool vnodeShouldRollback(SVnode *pVnode) {
  char    tFName[TSDB_FILENAME_LEN] = {0};
  int32_t offset = 0;

  (void)vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, tFName, TSDB_FILENAME_LEN);
  offset = strlen(tFName);
  snprintf(tFName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME_TMP);

  return taosCheckExistFile(tFName);
}

void vnodeRollback(SVnode *pVnode) {
  char    tFName[TSDB_FILENAME_LEN] = {0};
  int32_t offset = 0;

  (void)vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, tFName, TSDB_FILENAME_LEN);
  offset = strlen(tFName);
  snprintf(tFName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME_TMP);

  TAOS_UNUSED(taosRemoveFile(tFName));
}

static int vnodeEncodeState(const void *pObj, SJson *pJson) {
  const SVState *pState = (SVState *)pObj;

  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "commit version", pState->committed));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "commit ID", pState->commitID));
  TAOS_CHECK_RETURN(tjsonAddIntegerToObject(pJson, "commit term", pState->commitTerm));

  return 0;
}

static int vnodeDecodeState(const SJson *pJson, void *pObj) {
  SVState *pState = (SVState *)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, "commit version", pState->committed, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "commit ID", pState->commitID, code);
  if (code) return code;
  tjsonGetNumberValue(pJson, "commit term", pState->commitTerm, code);
  if (code) return code;

  return 0;
}

static int vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData) {
  int32_t code = 0;
  int32_t lino;
  SJson  *pJson = NULL;
  char   *pData = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  code = tjsonAddObject(pJson, "config", vnodeEncodeConfig, (void *)&pInfo->config);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tjsonAddObject(pJson, "state", vnodeEncodeState, (void *)&pInfo->state);
  TSDB_CHECK_CODE(code, lino, _exit);

  pData = tjsonToString(pJson);
  if (pData == NULL) {
    TSDB_CHECK_CODE(code = terrno, lino, _exit);
  }

  tjsonDelete(pJson);

_exit:
  if (code) {
    tjsonDelete(pJson);
    *ppData = NULL;
  } else {
    *ppData = pData;
  }
  return code;
}

int vnodeDecodeInfo(uint8_t *pData, SVnodeInfo *pInfo) {
  int32_t code = 0;
  int32_t lino;
  SJson  *pJson = NULL;

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_INVALID_DATA_FMT, lino, _exit);
  }

  code = tjsonToObject(pJson, "config", vnodeDecodeConfig, (void *)&pInfo->config);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = tjsonToObject(pJson, "state", vnodeDecodeState, (void *)&pInfo->state);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  tjsonDelete(pJson);
  return code;
}
