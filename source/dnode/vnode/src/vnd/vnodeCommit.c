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

static int  vnodeEncodeInfo(const SVnodeInfo *pInfo, uint8_t **ppData, int *len);
static int  vnodeDecodeInfo(uint8_t *pData, int len, SVnodeInfo *pInfo);
static int  vnodeStartCommit(SVnode *pVnode);
static int  vnodeEndCommit(SVnode *pVnode);
static int  vnodeCommit(void *arg);
static void vnodeWaitCommit(SVnode *pVnode);

int vnodeSaveInfo(const char *dir, const SVnodeInfo *pInfo) {
  char      fname[TSDB_FILENAME_LEN];
  TdFilePtr pFile;
  uint8_t  *data;
  int       len;

  snprintf(fname, TSDB_FILENAME_LEN, "%s%s%s", dir, TD_DIRSEP, VND_INFO_FNAME_TMP);

  // encode info
  data = NULL;
  len = 0;

  if (vnodeEncodeInfo(pInfo, &data, &len) < 0) {
    return -1;
  }

  // save info to a vnode_tmp.json
  pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  if (taosWriteFile(pFile, data, len) < 0) {
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

static int vnodeEncodeInfo(const SVnodeInfo *pInfo, uint8_t **ppData, int *len) {
  // TODO
  return 0;
}

static int vnodeDecodeInfo(uint8_t *pData, int len, SVnodeInfo *pInfo) {
  // TODO
  return 0;
}
