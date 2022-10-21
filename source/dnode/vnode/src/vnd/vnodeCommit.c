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

#define VND_INFO_FNAME "vnode.json"
#define VND_INFO_FNAME_TMP "vnode_tmp.json"

static int  vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData);
static int  vnodeDecodeInfo(uint8_t *pData, SVnodeInfo *pInfo);
static int  vnodeCommitImpl(void *arg);
static void vnodeWaitCommit(SVnode *pVnode);

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
  if (metaBegin(pVnode->pMeta, 0) < 0) {
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

int vnodeShouldCommit(SVnode *pVnode) {
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

  vInfo("vgId:%d, vnode info is saved, fname:%s", pInfo->config.vgId, fname);

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

int vnodeAsyncCommit(SVnode *pVnode) {
  vnodeWaitCommit(pVnode);

  // vnodeBufPoolSwitch(pVnode);
  // tsdbPrepareCommit(pVnode->pTsdb);

  vnodeScheduleTask(vnodeCommitImpl, pVnode);

  return 0;
}

int vnodeSyncCommit(SVnode *pVnode) {
  vnodeAsyncCommit(pVnode);
  vnodeWaitCommit(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

int vnodeCommit(SVnode *pVnode) {
  int32_t    code = 0;
  int32_t    lino = 0;
  SVnodeInfo info = {0};
  char       dir[TSDB_FILENAME_LEN];

  vInfo("vgId:%d, start to commit, commit ID:%" PRId64 " version:%" PRId64, TD_VID(pVnode), pVnode->state.commitID,
        pVnode->state.applied);

  pVnode->state.commitTerm = pVnode->state.applyTerm;

  // save info
  info.config = pVnode->config;
  info.state.committed = pVnode->state.applied;
  info.state.commitTerm = pVnode->state.applyTerm;
  info.state.commitID = pVnode->state.commitID;
  if (pVnode->pTfs) {
    snprintf(dir, TSDB_FILENAME_LEN, "%s%s%s", tfsGetPrimaryPath(pVnode->pTfs), TD_DIRSEP, pVnode->path);
  } else {
    snprintf(dir, TSDB_FILENAME_LEN, "%s", pVnode->path);
  }
  if (vnodeSaveInfo(dir, &info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // walBeginSnapshot(pVnode->pWal, pVnode->state.applied);
  syncBeginSnapshot(pVnode->sync, pVnode->state.applied);

  code = smaPreCommit(pVnode->pSma);
  TSDB_CHECK_CODE(code, lino, _exit);

  vnodeBufPoolUnRef(pVnode->inUse);
  pVnode->inUse = NULL;

  // commit each sub-system
  if (metaCommit(pVnode->pMeta) < 0) {
    code = TSDB_CODE_FAILED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbCommit(pVnode->pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (VND_IS_RSMA(pVnode)) {
    code = smaCommit(pVnode->pSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (tqCommit(pVnode->pTq) < 0) {
    code = TSDB_CODE_FAILED;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // commit info
  if (vnodeCommitInfo(dir, &info) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  code = tsdbFinishCommit(pVnode->pTsdb);
  TSDB_CHECK_CODE(code, lino, _exit);

  if (VND_IS_RSMA(pVnode)) {
    code = smaFinishCommit(pVnode->pSma);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (metaFinishCommit(pVnode->pMeta) < 0) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  pVnode->state.committed = info.state.committed;

  if (smaPostCommit(pVnode->pSma) < 0) {
    vError("vgId:%d, failed to post-commit sma since %s", TD_VID(pVnode), tstrerror(terrno));
    return -1;
  }

  // apply the commit (TODO)
  // walEndSnapshot(pVnode->pWal);
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

static int vnodeCommitImpl(void *arg) {
  SVnode *pVnode = (SVnode *)arg;

  // metaCommit(pVnode->pMeta);
  tqCommit(pVnode->pTq);
  // tsdbCommit(pVnode->pTsdb, );

  // vnodeBufPoolRecycle(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

static FORCE_INLINE void vnodeWaitCommit(SVnode *pVnode) { tsem_wait(&pVnode->canCommit); }

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

static int vnodeDecodeInfo(uint8_t *pData, SVnodeInfo *pInfo) {
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
