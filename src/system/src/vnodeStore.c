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

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "dnodeSystem.h"
#include "trpc.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeStore.h"
#include "vnodeUtil.h"

#pragma GCC diagnostic ignored "-Wformat-overflow="

int vnodeCreateMeterObjFile(int vnode);

int        tsMaxVnode = -1;
int        tsOpenVnodes = 0;
SVnodeObj *vnodeList = NULL;

int vnodeInitStoreVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;

  pVnode->vnode = vnode;
  vnodeOpenMetersVnode(vnode);
  if (pVnode->cfg.maxSessions == 0) return 0;

  pVnode->firstKey = taosGetTimestamp(pVnode->cfg.precision);

  pVnode->pCachePool = vnodeOpenCachePool(vnode);
  if (pVnode->pCachePool == NULL) {
    dError("vid:%d, cache pool init failed.", pVnode->vnode);
    return -1;
  }

  if (vnodeInitFile(vnode) < 0) return -1;

  if (vnodeInitCommit(vnode) < 0) {
    dError("vid:%d, commit init failed.", pVnode->vnode);
    return -1;
  }

  pthread_mutex_init(&(pVnode->vmutex), NULL);
  dTrace("vid:%d, storage initialized, version:%ld fileId:%d numOfFiles:%d", vnode, pVnode->version, pVnode->fileId,
         pVnode->numOfFiles);

  return 0;
}

int vnodeOpenVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;

  pVnode->vnode = vnode;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;
  if (pVnode->cfg.maxSessions == 0) return 0;

  pthread_mutex_lock(&dmutex);
  vnodeOpenShellVnode(vnode);

  if (vnode > tsMaxVnode) tsMaxVnode = vnode;
  vnodeCalcOpenVnodes();

  pthread_mutex_unlock(&dmutex);

  vnodeOpenStreams(pVnode, NULL);

  dTrace("vid:%d, vnode is opened, openVnodes:%d", vnode, tsOpenVnodes);

  return 0;
}

static int32_t vnodeMarkAllMetersDropped(SVnodeObj* pVnode) {
  if (pVnode->meterList == NULL) {
    assert(pVnode->cfg.maxSessions == 0);
    return TSDB_CODE_SUCCESS;
  }

  bool ready = true;
  for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    if (!vnodeIsSafeToDeleteMeter(pVnode, sid)) {
      ready = false;
    } else { // set the meter is to be deleted
      SMeterObj* pObj = pVnode->meterList[sid];
      if (pObj != NULL) {
        pObj->state = TSDB_METER_STATE_DELETED;
      }
    }
  }

  return ready? TSDB_CODE_SUCCESS:TSDB_CODE_ACTION_IN_PROGRESS;
}

int vnodeCloseVnode(int vnode) {
  if (vnodeList == NULL) return TSDB_CODE_SUCCESS;

  SVnodeObj* pVnode = &vnodeList[vnode];

  pthread_mutex_lock(&dmutex);
  if (pVnode->cfg.maxSessions == 0) {
    pthread_mutex_unlock(&dmutex);
    return TSDB_CODE_SUCCESS;
  }

  // set the meter is dropped flag 
  if (vnodeMarkAllMetersDropped(pVnode) != TSDB_CODE_SUCCESS) {
    pthread_mutex_unlock(&dmutex);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  vnodeCloseStream(vnodeList + vnode);
  vnodeCancelCommit(vnodeList + vnode);
  vnodeCloseMetersVnode(vnode);
  vnodeCloseShellVnode(vnode);
  vnodeCloseCachePool(vnode);
  vnodeCleanUpCommit(vnode);

  pthread_mutex_destroy(&(vnodeList[vnode].vmutex));

  if (tsMaxVnode == vnode) tsMaxVnode = vnode - 1;

  tfree(vnodeList[vnode].meterIndex);
  memset(vnodeList + vnode, 0, sizeof(SVnodeObj));

  vnodeCalcOpenVnodes();

  pthread_mutex_unlock(&dmutex);
  return TSDB_CODE_SUCCESS;
}

int vnodeCreateVnode(int vnode, SVnodeCfg *pCfg, SVPeerDesc *pDesc) {
  char fileName[128];

  vnodeList[vnode].status = TSDB_STATUS_CREATING;

  sprintf(fileName, "%s/vnode%d", tsDirectory, vnode);
  mkdir(fileName, 0755);

  sprintf(fileName, "%s/vnode%d/db", tsDirectory, vnode);
  mkdir(fileName, 0755);

  vnodeList[vnode].cfg = *pCfg;
  if (vnodeCreateMeterObjFile(vnode) != 0) {
    return TSDB_CODE_VG_INIT_FAILED;
  }

  if (vnodeSaveVnodeCfg(vnode, pCfg, pDesc) != 0) {
    return TSDB_CODE_VG_INIT_FAILED;
  }

  if (vnodeInitStoreVnode(vnode) != 0) {
    return TSDB_CODE_VG_COMMITLOG_INIT_FAILED;
  }

  return vnodeOpenVnode(vnode);
}

void vnodeRemoveDataFiles(int vnode) {
  char           vnodeDir[TSDB_FILENAME_LEN];
  char           dfilePath[TSDB_FILENAME_LEN];
  char           linkFile[TSDB_FILENAME_LEN];
  struct dirent *de = NULL;
  DIR *          dir = NULL;

  sprintf(vnodeDir, "%s/vnode%d/db", tsDirectory, vnode);
  dir = opendir(vnodeDir);
  if (dir == NULL) return;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if ((strcmp(de->d_name + strlen(de->d_name) - strlen(".head"), ".head") == 0 ||
         strcmp(de->d_name + strlen(de->d_name) - strlen(".data"), ".data") == 0 ||
         strcmp(de->d_name + strlen(de->d_name) - strlen(".last"), ".last") == 0) &&
        (de->d_type & DT_LNK)) {
      sprintf(linkFile, "%s/%s", vnodeDir, de->d_name);

      memset(dfilePath, 0, TSDB_FILENAME_LEN);
      int tcode = readlink(linkFile, dfilePath, TSDB_FILENAME_LEN);
      remove(linkFile);

      if (tcode >= 0) {
        remove(dfilePath);
        dTrace("Data file %s is removed, link file %s", dfilePath, linkFile);
      }
    } else {
      remove(de->d_name);
    }
  }

  closedir(dir);
  rmdir(vnodeDir);

  sprintf(vnodeDir, "%s/vnode%d/meterObj.v%d", tsDirectory, vnode, vnode);
  remove(vnodeDir);

  sprintf(vnodeDir, "%s/vnode%d", tsDirectory, vnode);
  rmdir(vnodeDir);
  dTrace("vnode %d is removed!", vnode);
}

int vnodeRemoveVnode(int vnode) {
  if (vnodeList == NULL) return TSDB_CODE_SUCCESS;

  if (vnodeList[vnode].cfg.maxSessions > 0) {
    int32_t ret = vnodeCloseVnode(vnode);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }

    vnodeRemoveDataFiles(vnode);
  } else {
    dTrace("vid:%d, max sessions:%d, this vnode already dropped!!!", vnode, vnodeList[vnode].cfg.maxSessions);
    vnodeList[vnode].cfg.maxSessions = 0;  //reset value
    vnodeCalcOpenVnodes();
  }

  return TSDB_CODE_SUCCESS;
}

int vnodeInitStore() {
  int vnode;
  int size;

  size = sizeof(SVnodeObj) * TSDB_MAX_VNODES;
  vnodeList = (SVnodeObj *)malloc(size);
  if (vnodeList == NULL) return -1;
  memset(vnodeList, 0, size);

  for (vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    if (vnodeInitStoreVnode(vnode) < 0) {
      // one vnode is failed to recover from commit log, continue for remain
      return -1;
    }
  }

  return 0;
}

int vnodeInitVnodes() {
  int vnode;

  for (vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    if (vnodeOpenVnode(vnode) < 0) return -1;
  }

  return 0;
}

void vnodeCleanUpVnodes() {
  static int again = 0;
  if (vnodeList == NULL) return;

  pthread_mutex_lock(&dmutex);

  if (again) {
    pthread_mutex_unlock(&dmutex);
    return;
  }
  again = 1;

  for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    if (vnodeList[vnode].pCachePool) {
      vnodeList[vnode].status = TSDB_STATUS_OFFLINE;
    }
  }

  pthread_mutex_unlock(&dmutex);

  for (int vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    if (vnodeList[vnode].pCachePool) {
      vnodeProcessCommitTimer(vnodeList + vnode, NULL);
      while (vnodeList[vnode].commitThread != 0) {
        taosMsleep(10);
      }
      vnodeCleanUpCommit(vnode);
    }
  }
}

void vnodeCalcOpenVnodes() {
  int openVnodes = 0;
  for (int vnode = 0; vnode <= tsMaxVnode; ++vnode) {
    if (vnodeList[vnode].cfg.maxSessions <= 0) continue;
    openVnodes++;
  }

  __sync_val_compare_and_swap(&tsOpenVnodes, tsOpenVnodes, openVnodes);
}
