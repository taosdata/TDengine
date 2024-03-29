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

  taosThreadMutexLock(&pVnode->mutex);

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
        taosGetTimeOfDay(&tv);
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
  taosThreadMutexUnlock(&pVnode->mutex);
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
  if (metaBegin(pVnode->pMeta, META_BEGIN_HEAP_BUFFERPOOL) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // begin tsdb
  if (tsdbBegin(pVnode->pTsdb) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // begin sma
  if (VND_IS_RSMA(pVnode) && smaBegin(pVnode->pSma) < 0) {
    code = terrno;
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

  taosThreadMutexLock(&pVnode->mutex);
  if (pVnode->inUse && diskAvail) {
    needCommit = (pVnode->inUse->size > pVnode->inUse->node.size) ||
                 (atExit && (pVnode->inUse->size > 0 || pVnode->pMeta->changed ||
                             pVnode->state.applied - pVnode->state.committed > 4096));
  }
  taosThreadMutexUnlock(&pVnode->mutex);
  return needCommit;
}

int vnodeSaveInfo(const char *dir, const SVnodeInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  char     *data;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  // encode info
  data = NULL;

  if (vnodeEncodeInfo(pInfo, &data) < 0) {
    vError("failed to encode json info.");
    return -1;
  }

  // save info to a vnode_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_WRITE_THROUGH);
  if (pFile == NULL) {
    vError("failed to open info file:%s for write:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, strlen(data)) < 0) {
    vError("failed to write info file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFile) < 0) {
    vError("failed to fsync info file:%s error:%s", fname, terrstr());
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFile);

  // free info binary
  taosMemoryFree(data);

  vInfo("vgId:%d, vnode info is saved, fname:%s replica:%d selfIndex:%d changeVersion:%d", pInfo->config.vgId, fname,
        pInfo->config.syncCfg.replicaNum, pInfo->config.syncCfg.myIndex, pInfo->config.syncCfg.changeVersion);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(data);
  return -1;
}

int vnodeCommitInfo(const char *dir) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  if (taosRenameFile(tfname, fname) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  vInfo("vnode info is committed, dir:%s", dir);
  return 0;
}

int vnodeLoadInfo(const char *dir, SVnodeInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile = NULL;
  char     *pData = NULL;
  int64_t   size;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);

  // read info
  pFile = taosOpenFile(fname, TD_FILE_READ);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosFStatFile(pFile, &size, NULL) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData = taosMemoryMalloc(size + 1);
  if (pData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (taosReadFile(pFile, pData, size) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  pData[size] = '\0';

  taosCloseFile(&pFile);

  // decode info
  if (vnodeDecodeInfo(pData, pInfo) < 0) {
    taosMemoryFree(pData);
    return -1;
  }

  taosMemoryFree(pData);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(pData);
  return -1;
}

static int32_t vnodePrepareCommit(SVnode *pVnode, SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  char    dir[TSDB_FILENAME_LEN] = {0};
  int64_t lastCommitted = pInfo->info.state.committed;

  // wait last commit task
  vnodeAWait(vnodeAsyncHandle[0], pVnode->commitTask);

  if (syncNodeGetConfig(pVnode->sync, &pVnode->config.syncCfg) != 0) goto _exit;

  pVnode->state.commitTerm = pVnode->state.applyTerm;

  pInfo->info.config = pVnode->config;
  pInfo->info.state.committed = pVnode->state.applied;
  pInfo->info.state.commitTerm = pVnode->state.applyTerm;
  pInfo->info.state.commitID = ++pVnode->state.commitID;
  pInfo->pVnode = pVnode;
  pInfo->txn = metaGetTxn(pVnode->pMeta);

  // save info
  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

  vDebug("vgId:%d, save config while prepare commit", TD_VID(pVnode));
  if (vnodeSaveInfo(dir, &pInfo->info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tsdbPreCommit(pVnode->pTsdb);

  metaPrepareAsyncCommit(pVnode->pMeta);

  code = smaPrepareAsyncCommit(pVnode->pSma);
  if (code) goto _exit;

  taosThreadMutexLock(&pVnode->mutex);
  ASSERT(pVnode->onCommit == NULL);
  pVnode->onCommit = pVnode->inUse;
  pVnode->inUse = NULL;
  taosThreadMutexUnlock(&pVnode->mutex);

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
  taosThreadMutexLock(&pVnode->mutex);

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
    ASSERT(0);
  }

  taosThreadMutexUnlock(&pVnode->mutex);
}
static int32_t vnodeCommitTask(void *arg) {
  int32_t code = 0;

  SCommitInfo *pInfo = (SCommitInfo *)arg;
  SVnode      *pVnode = pInfo->pVnode;

  // commit
  code = vnodeCommitImpl(pInfo);
  if (code) {
    vFatal("vgId:%d, failed to commit vnode since %s", TD_VID(pVnode), terrstr());
    taosMsleep(100);
    exit(EXIT_FAILURE);
    goto _exit;
  }

  vnodeReturnBufPool(pVnode);

_exit:
  return code;
}

static void vnodeCompleteCommit(void *arg) { taosMemoryFree(arg); }

int vnodeAsyncCommit(SVnode *pVnode) {
  int32_t code = 0;

  SCommitInfo *pInfo = (SCommitInfo *)taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // prepare to commit
  code = vnodePrepareCommit(pVnode, pInfo);
  if (TSDB_CODE_SUCCESS != code) {
    goto _exit;
  }

  // schedule the task
  code = vnodeAsyncC(vnodeAsyncHandle[0], pVnode->commitChannel, EVA_PRIORITY_HIGH, vnodeCommitTask,
                     vnodeCompleteCommit, pInfo, &pVnode->commitTask);

_exit:
  if (code) {
    if (NULL != pInfo) {
      taosMemoryFree(pInfo);
    }
    vError("vgId:%d, %s failed since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}

int vnodeSyncCommit(SVnode *pVnode) {
  vnodeAsyncCommit(pVnode);
  vnodeAWait(vnodeAsyncHandle[0], pVnode->commitTask);
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
  if (walPersist(pVnode->pWal) < 0) {
    vError("vgId:%d, failed to persist wal since %s", TD_VID(pVnode), terrstr());
    return -1;
  }

  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, dir, TSDB_FILENAME_LEN);

  syncBeginSnapshot(pVnode->sync, pInfo->info.state.committed);

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

  if (tqCommit(pVnode->pTq) < 0) {
    code = TSDB_CODE_FAILED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // commit info
  if (vnodeCommitInfo(dir) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCommitCommit(pVnode->pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (VND_IS_RSMA(pVnode)) {
    code = smaFinishCommit(pVnode->pSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (metaFinishCommit(pVnode->pMeta, pInfo->txn) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pVnode->state.committed = pInfo->info.state.committed;

  if (smaPostCommit(pVnode->pSma) < 0) {
    vError("vgId:%d, failed to post-commit sma since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  syncEndSnapshot(pVnode->sync);

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d, commit end", TD_VID(pVnode));
  }
  return 0;
}

bool vnodeShouldRollback(SVnode *pVnode) {
  char    tFName[TSDB_FILENAME_LEN] = {0};
  int32_t offset = 0;

  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, tFName, TSDB_FILENAME_LEN);
  offset = strlen(tFName);
  snprintf(tFName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME_TMP);

  return taosCheckExistFile(tFName);
}

void vnodeRollback(SVnode *pVnode) {
  char    tFName[TSDB_FILENAME_LEN] = {0};
  int32_t offset = 0;

  vnodeGetPrimaryDir(pVnode->path, pVnode->diskPrimary, pVnode->pTfs, tFName, TSDB_FILENAME_LEN);
  offset = strlen(tFName);
  snprintf(tFName + offset, TSDB_FILENAME_LEN - offset - 1, "%s%s", TD_DIRSEP, VND_INFO_FNAME_TMP);

  (void)taosRemoveFile(tFName);
}

static int vnodeEncodeState(const void *pObj, SJson *pJson) {
  const SVState *pState = (SVState *)pObj;

  if (tjsonAddIntegerToObject(pJson, "commit version", pState->committed) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "commit ID", pState->commitID) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "commit term", pState->commitTerm) < 0) return -1;

  return 0;
}

static int vnodeDecodeState(const SJson *pJson, void *pObj) {
  SVState *pState = (SVState *)pObj;

  int32_t code;
  tjsonGetNumberValue(pJson, "commit version", pState->committed, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "commit ID", pState->commitID, code);
  if (code < 0) return -1;
  tjsonGetNumberValue(pJson, "commit term", pState->commitTerm, code);
  if (code < 0) return -1;

  return 0;
}

static int vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData) {
  SJson *pJson;
  char  *pData;

  *ppData = NULL;

  pJson = tjsonCreateObject();
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonAddObject(pJson, "config", vnodeEncodeConfig, (void *)&pInfo->config) < 0) {
    goto _err;
  }

  if (tjsonAddObject(pJson, "state", vnodeEncodeState, (void *)&pInfo->state) < 0) {
    goto _err;
  }

  pData = tjsonToString(pJson);
  if (pData == NULL) {
    goto _err;
  }

  tjsonDelete(pJson);

  *ppData = pData;
  return 0;

_err:
  tjsonDelete(pJson);
  return -1;
}

int vnodeDecodeInfo(uint8_t *pData, SVnodeInfo *pInfo) {
  SJson *pJson = NULL;

  pJson = tjsonParse(pData);
  if (pJson == NULL) {
    return -1;
  }

  if (tjsonToObject(pJson, "config", vnodeDecodeConfig, (void *)&pInfo->config) < 0) {
    goto _err;
  }

  if (tjsonToObject(pJson, "state", vnodeDecodeState, (void *)&pInfo->state) < 0) {
    goto _err;
  }

  tjsonDelete(pJson);

  return 0;

_err:
  tjsonDelete(pJson);
  return -1;
}
