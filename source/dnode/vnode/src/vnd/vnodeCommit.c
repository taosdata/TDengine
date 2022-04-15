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

#include "vnodeInt.h"

#define VND_INFO_FNAME     "vnode.json"
#define VND_INFO_FNAME_TMP "vnode_tmp.json"

static int  vnodeEncodeInfo(const SVnodeInfo *pInfo, char **ppData);
static int  vnodeDecodeInfo(uint8_t *pData, int len, SVnodeInfo *pInfo);
static int  vnodeStartCommit(SVnode *pVnode);
static int  vnodeEndCommit(SVnode *pVnode);
static int  vnodeCommit(void *arg);
static void vnodeWaitCommit(SVnode *pVnode);

int vnodeSaveInfo(const char *dir, const SVnodeInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  char     *data;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  // encode info
  data = NULL;

  if (vnodeEncodeInfo(pInfo, &data) < 0) {
    return -1;
  }

  // save info to a vnode_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, strlen(data)) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosFsyncFile(pFile) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  taosCloseFile(&pFile);

  // free info binary
  taosMemoryFree(data);

  vInfo("vgId: %d vnode info is saved, fname: %s", pInfo->config.vgId, fname);

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

  vInfo("vgId: %d vnode info is committed", pInfo->config.vgId);

  return 0;
}

int vnodeLoadInfo(const char *dir) {
  // TODO
  return 0;
}

int vnodeAsyncCommit(SVnode *pVnode) {
  vnodeWaitCommit(pVnode);

  vnodeBufPoolSwitch(pVnode);
  tsdbPrepareCommit(pVnode->pTsdb);

  vnodeScheduleTask(vnodeCommit, pVnode);

  return 0;
}

int vnodeSyncCommit(SVnode *pVnode) {
  vnodeAsyncCommit(pVnode);
  vnodeWaitCommit(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

static int vnodeCommit(void *arg) {
  SVnode *pVnode = (SVnode *)arg;

  // metaCommit(pVnode->pMeta);
  tqCommit(pVnode->pTq);
  tsdbCommit(pVnode->pTsdb);

  vnodeBufPoolRecycle(pVnode);
  tsem_post(&(pVnode->canCommit));
  return 0;
}

static int vnodeStartCommit(SVnode *pVnode) {
  // TODO
  return 0;
}

static int vnodeEndCommit(SVnode *pVnode) {
  // TODO
  return 0;
}

static FORCE_INLINE void vnodeWaitCommit(SVnode *pVnode) { tsem_wait(&pVnode->canCommit); }

static int vnodeEncodeConfig(const void *pObj, SJson *pJson) {
  const SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  if (tjsonAddIntegerToObject(pJson, "vgId", pCfg->vgId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "dbId", pCfg->dbId) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "wsize", pCfg->wsize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "ssize", pCfg->ssize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "lsize", pCfg->lsize) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isHeap", pCfg->isHeapAllocator) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "ttl", pCfg->ttl) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep", pCfg->keep) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "streamMode", pCfg->streamMode) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "isWeak", pCfg->isWeak) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "precision", pCfg->tsdbCfg.precision) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "update", pCfg->tsdbCfg.update) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "compression", pCfg->tsdbCfg.compression) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "daysPerFile", pCfg->tsdbCfg.days) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "minRows", pCfg->tsdbCfg.minRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "maxRows", pCfg->tsdbCfg.maxRows) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep0", pCfg->tsdbCfg.keep2) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep1", pCfg->tsdbCfg.keep0) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "keep2", pCfg->tsdbCfg.keep1) < 0) return -1;
  if (tjsonAddIntegerToObject(pJson, "lruCacheSize", pCfg->tsdbCfg.lruCacheSize) < 0) return -1;

  return 0;
}

static int vnodeDecodeConfig(const SJson *pJson, void *pObj) {
  SVnodeCfg *pCfg = (SVnodeCfg *)pObj;

  if (tjsonGetNumberValue(pJson, "vgId", pCfg->vgId) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "dbId", pCfg->dbId) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "wsize", pCfg->wsize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "ssize", pCfg->ssize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "lsize", pCfg->lsize) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "isHeap", pCfg->isHeapAllocator) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "ttl", pCfg->ttl) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep", pCfg->keep) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "streamMode", pCfg->streamMode) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "isWeak", pCfg->isWeak) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "precision", pCfg->tsdbCfg.precision) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "update", pCfg->tsdbCfg.update) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "compression", pCfg->tsdbCfg.compression) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "daysPerFile", pCfg->tsdbCfg.days) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "minRows", pCfg->tsdbCfg.minRows) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "maxRows", pCfg->tsdbCfg.maxRows) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep0", pCfg->tsdbCfg.keep2) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep1", pCfg->tsdbCfg.keep0) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "keep2", pCfg->tsdbCfg.keep1) < 0) return -1;
  if (tjsonGetNumberValue(pJson, "lruCacheSize", pCfg->tsdbCfg.lruCacheSize) < 0) return -1;

  return 0;
}

static int vnodeEncodeState(const void *pObj, SJson *pJson) {
  const SVState *pState = (SVState *)pObj;

  if (tjsonAddIntegerToObject(pJson, "commit version", pState->committed) < 0) return -1;

  return 0;
}

static int vnodeDecodeState(const SJson *pJson, void *pObj) {
  SVState *pState = (SVState *)pObj;

  if (tjsonGetNumberValue(pJson, "commit version", pState->committed) < 0) return -1;

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

static int vnodeDecodeInfo(uint8_t *pData, int len, SVnodeInfo *pInfo) {
  SJson *pJson = NULL;

  pJson = tjsonCreateObject();
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
