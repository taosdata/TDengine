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

#include "vnd.h"
#include "vnodeInt.h"

#define VND_INFO_FNAME_TMP "vnode_tmp.json"

static int vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData);
static int vnodeCommitImpl(SCommitInfo *pInfo);

int vnodeBegin(SVnode *pVnode) {
  // alloc buffer pool
  taosThreadMutexLock(&pVnode->mutex);

  while (pVnode->pPool == NULL) {
    taosThreadCondWait(&pVnode->poolNotEmpty, &pVnode->mutex);
  }

  pVnode->inUse = pVnode->pPool;
  pVnode->inUse->nRef = 1;
  pVnode->pPool = pVnode->inUse->next;
  pVnode->inUse->next = NULL;

  taosThreadMutexUnlock(&pVnode->mutex);

  pVnode->state.commitID++;
  // begin meta
  if (metaBegin(pVnode->pMeta, META_BEGIN_HEAP_BUFFERPOOL) < 0) {
    vError("vgId:%d, failed to begin meta since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  // begin tsdb
  if (tsdbBegin(pVnode->pTsdb) < 0) {
    vError("vgId:%d, failed to begin tsdb since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  // begin sma
  if (VND_IS_RSMA(pVnode) && smaBegin(pVnode->pSma) < 0) {
    vError("vgId:%d, failed to begin sma since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  return 0;
}

void vnodeUpdCommitSched(SVnode *pVnode) {
  int64_t randNum = taosRand();
  pVnode->commitSched.commitMs = taosGetMonoTimestampMs();
  pVnode->commitSched.maxWaitMs = tsVndCommitMaxIntervalMs + (randNum % tsVndCommitMaxIntervalMs);
}

int vnodeShouldCommit(SVnode *pVnode) {
  if (!pVnode->inUse || !osDataSpaceAvailable()) {
    return false;
  }

  SVCommitSched *pSched = &pVnode->commitSched;
  int64_t nowMs = taosGetMonoTimestampMs();

  return (((pVnode->inUse->size > pVnode->inUse->node.size) && (pSched->commitMs + SYNC_VND_COMMIT_MIN_MS < nowMs)) ||
          (pVnode->inUse->size > 0 && pSched->commitMs + pSched->maxWaitMs < nowMs));
}

int vnodeShouldCommitOld(SVnode *pVnode) {
  if (pVnode->inUse) {
    return osDataSpaceAvailable() && (pVnode->inUse->size > pVnode->inUse->node.size);
  }
  return false;
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
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
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

  vInfo("vgId:%d, vnode info is saved, fname:%s replica:%d selfIndex:%d", pInfo->config.vgId, fname,
        pInfo->config.syncCfg.replicaNum, pInfo->config.syncCfg.myIndex);

  return 0;

_err:
  taosCloseFile(&pFile);
  taosMemoryFree(data);
  return -1;
}

int vnodeCommitInfo(const char *dir, const SVnodeInfo *pInfo) {
  char fname[TSDB_FILENAME_LEN];
  char tfname[TSDB_FILENAME_LEN];

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME);
  snprintf(tfname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  if (taosRenameFile(tfname, fname) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  vInfo("vgId:%d, vnode info is committed", pInfo->config.vgId);

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

  tsem_wait(&pVnode->canCommit);

  pVnode->state.commitTerm = pVnode->state.applyTerm;

  pInfo->info.config = pVnode->config;
  pInfo->info.state.committed = pVnode->state.applied;
  pInfo->info.state.commitTerm = pVnode->state.applyTerm;
  pInfo->info.state.commitID = pVnode->state.commitID;
  pInfo->pVnode = pVnode;
  pInfo->txn = metaGetTxn(pVnode->pMeta);

  // save info
  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }

  vDebug("vgId:%d, save config while prepare commit", TD_VID(pVnode));
  if (vnodeSaveInfo(dir, &pInfo->info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  tsdbPrepareCommit(pVnode->pTsdb);
  smaPrepareAsyncCommit(pVnode->pSma);

  metaPrepareAsyncCommit(pVnode->pMeta);

  vnodeBufPoolUnRef(pVnode->inUse);
  pVnode->inUse = NULL;

_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, lino,
           tstrerror(code), pVnode->state.commitID);
  } else {
    vDebug("vgId:%d, %s done", TD_VID(pVnode), __func__);
  }
  return code;
}

static int32_t vnodeCommitTask(void *arg) {
  int32_t code = 0;

  SCommitInfo *pInfo = (SCommitInfo *)arg;

  // commit
  code = vnodeCommitImpl(pInfo);
  if (code) goto _exit;

  // end commit
  tsem_post(&pInfo->pVnode->canCommit);

_exit:
  taosMemoryFree(pInfo);
  return code;
}

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
  vnodeScheduleTask(vnodeCommitTask, pInfo);

_exit:
  if (code) {
    if (NULL != pInfo) {
      taosMemoryFree(pInfo);
    }
    vError("vgId:%d, vnode async commit failed since %s, commitId:%" PRId64, TD_VID(pVnode), tstrerror(code),
           pVnode->state.commitID);
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}

int vnodeSyncCommit(SVnode *pVnode) {
  vnodeAsyncCommit(pVnode);
  tsem_wait(&pVnode->canCommit);
  tsem_post(&pVnode->canCommit);
  return 0;
}

static int vnodeCommitImpl(SCommitInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  char    dir[TSDB_FILENAME_LEN] = {0};
  SVnode *pVnode = pInfo->pVnode;

  vInfo("vgId:%d, start to commit, commitId:%" PRId64 " version:%" PRId64 " term: %" PRId64, TD_VID(pVnode),
        pInfo->info.state.commitID, pInfo->info.state.committed, pInfo->info.state.commitTerm);

  vnodeUpdCommitSched(pVnode);

  // persist wal before starting
  if (walPersist(pVnode->pWal) < 0) {
    vError("vgId:%d, failed to persist wal since %s", TD_VID(pVnode), terrstr());
    return -1;
  }

  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }

  syncBeginSnapshot(pVnode->sync, pInfo->info.state.committed);

  // commit each sub-system
  code = tsdbCommit(pVnode->pTsdb, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (VND_IS_RSMA(pVnode)) {
    code = smaCommit(pVnode->pSma, pInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (tqCommit(pVnode->pTq) < 0) {
    code = TSDB_CODE_FAILED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // commit info
  if (vnodeCommitInfo(dir, &pInfo->info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFinishCommit(pVnode->pTsdb);
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
  char tFName[TSDB_FILENAME_LEN] = {0};
  snprintf(tFName, TSDB_FILENAME_LEN, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
           VND_INFO_FNAME_TMP);

  return taosCheckExistFile(tFName);
}

void vnodeRollback(SVnode *pVnode) {
  char tFName[TSDB_FILENAME_LEN] = {0};
  snprintf(tFName, TSDB_FILENAME_LEN, "%s%s%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path, TD_DIRSEP,
           VND_INFO_FNAME_TMP);

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
