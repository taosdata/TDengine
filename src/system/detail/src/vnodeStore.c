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

#define _DEFAULT_SOURCE
#include "os.h"

#include "dnodeSystem.h"
#include "trpc.h"
#include "ttime.h"
#include "vnode.h"
#include "vnodeStore.h"
#include "vnodeUtil.h"
#include "vnodeStatus.h"

#include <dirent.h>

int        tsMaxVnode = -1;
int        tsOpenVnodes = 0;
SVnodeObj *vnodeList = NULL;

static int vnodeInitStoreVnode(int vnode) {
  SVnodeObj *pVnode = vnodeList + vnode;

  pVnode->vnode = vnode;
  vnodeOpenMetersVnode(vnode);
  if (pVnode->cfg.maxSessions <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pVnode->firstKey = taosGetTimestamp(pVnode->cfg.precision);

  pVnode->pCachePool = vnodeOpenCachePool(vnode);
  if (pVnode->pCachePool == NULL) {
    dError("vid:%d, cache pool init failed.", pVnode->vnode);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  if (vnodeInitFile(vnode) != TSDB_CODE_SUCCESS) {
    dError("vid:%d, files init failed.", pVnode->vnode);
    return TSDB_CODE_VG_INIT_FAILED;
  }

  if (vnodeInitCommit(vnode) != TSDB_CODE_SUCCESS) {
    dError("vid:%d, commit init failed.", pVnode->vnode);
    return TSDB_CODE_VG_INIT_FAILED;
  }

  pthread_mutex_init(&(pVnode->vmutex), NULL);
  dPrint("vid:%d, storage initialized, version:%" PRIu64 " fileId:%d numOfFiles:%d", vnode, pVnode->version, pVnode->fileId,
         pVnode->numOfFiles);

  return TSDB_CODE_SUCCESS;
}

int vnodeOpenVnode(int vnode) {
  int32_t code = TSDB_CODE_SUCCESS;

  SVnodeObj *pVnode = vnodeList + vnode;

  pVnode->vnode = vnode;
  pVnode->accessState = TSDB_VN_ALL_ACCCESS;

  // vnode is empty
  if (pVnode->cfg.maxSessions <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (!(pVnode->vnodeStatus == TSDB_VN_STATUS_OFFLINE || pVnode->vnodeStatus == TSDB_VN_STATUS_CREATING)) {
    dError("vid:%d, status:%s, cannot enter open operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    return TSDB_CODE_INVALID_VNODE_STATUS;
  }

  dPrint("vid:%d, status:%s, start to open", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
  pthread_mutex_lock(&dmutex);

  // not enough memory, abort
  if ((code = vnodeOpenShellVnode(vnode)) != TSDB_CODE_SUCCESS) {
    pthread_mutex_unlock(&dmutex);
    return code;
  }

  vnodeOpenPeerVnode(vnode);

  if (vnode > tsMaxVnode) tsMaxVnode = vnode;

  vnodeCalcOpenVnodes();

  pthread_mutex_unlock(&dmutex);

#ifndef CLUSTER
  vnodeOpenStreams(pVnode, NULL);
#endif

  dPrint("vid:%d, vnode is opened, openVnodes:%d, status:%s", vnode, tsOpenVnodes, taosGetVnodeStatusStr(pVnode->vnodeStatus));

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeMarkAllMetersDropped(SVnodeObj* pVnode) {
  if (pVnode->meterList == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  bool ready = true;
  for (int sid = 0; sid < pVnode->cfg.maxSessions; ++sid) {
    if (!vnodeIsSafeToDeleteMeter(pVnode, sid)) {
      ready = false;
    } else { // set the meter is to be deleted
      SMeterObj* pObj = pVnode->meterList[sid];
      if (pObj != NULL) {
        pObj->state = TSDB_METER_STATE_DROPPED;
      }
    }
  }

  return ready? TSDB_CODE_SUCCESS:TSDB_CODE_ACTION_IN_PROGRESS;
}

static int vnodeCloseVnode(int vnode) {
  if (vnodeList == NULL) return TSDB_CODE_SUCCESS;

  SVnodeObj* pVnode = &vnodeList[vnode];

  pthread_mutex_lock(&dmutex);
  if (pVnode->cfg.maxSessions == 0) {
    pthread_mutex_unlock(&dmutex);
    return TSDB_CODE_SUCCESS;
  }

  if (pVnode->vnodeStatus == TSDB_VN_STATUS_DELETING) {
    dPrint("vid:%d, status:%s, another thread performed delete operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    return TSDB_CODE_SUCCESS;
  } else {
    dPrint("vid:%d, status:%s, enter close operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
    pVnode->vnodeStatus = TSDB_VN_STATUS_CLOSING;
  }

  // set the meter is dropped flag 
  if (vnodeMarkAllMetersDropped(pVnode) != TSDB_CODE_SUCCESS) {
    pthread_mutex_unlock(&dmutex);
    return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  dPrint("vid:%d, status:%s, enter delete operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
  pVnode->vnodeStatus = TSDB_VN_STATUS_DELETING;

  vnodeCloseStream(vnodeList + vnode);
  vnodeCancelCommit(vnodeList + vnode);
  vnodeClosePeerVnode(vnode);
  vnodeCloseMetersVnode(vnode);
  vnodeCloseShellVnode(vnode);
  vnodeCloseCachePool(vnode);
  vnodeCleanUpCommit(vnode);

  pthread_mutex_destroy(&(vnodeList[vnode].vmutex));

  if (tsMaxVnode == vnode) tsMaxVnode = vnode - 1;

  tfree(vnodeList[vnode].meterIndex);

  pthread_mutex_unlock(&dmutex);
  return TSDB_CODE_SUCCESS;
}

int vnodeCreateVnode(int vnode, SVnodeCfg *pCfg, SVPeerDesc *pDesc) {
  char fileName[128];

  if (vnodeList[vnode].vnodeStatus != TSDB_VN_STATUS_OFFLINE) {
    dError("vid:%d, status:%s, cannot enter create operation", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
    return TSDB_CODE_INVALID_VNODE_STATUS;
  }

  vnodeList[vnode].vnodeStatus = TSDB_VN_STATUS_CREATING;

  sprintf(fileName, "%s/vnode%d", tsDirectory, vnode);
  if (mkdir(fileName, 0755) != 0) {
    dError("failed to create vnode:%d directory:%s, errno:%d, reason:%s", vnode, fileName, errno, strerror(errno));
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  sprintf(fileName, "%s/vnode%d/db", tsDirectory, vnode);
  if (mkdir(fileName, 0755) != 0) {
    dError("failed to create vnode:%d directory:%s, errno:%d, reason:%s", vnode, fileName, errno, strerror(errno));
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  vnodeList[vnode].cfg = *pCfg;
  int code = vnodeCreateMeterObjFile(vnode);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = vnodeSaveVnodeCfg(vnode, pCfg, pDesc);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_VG_INIT_FAILED;
  }

  code = vnodeInitStoreVnode(vnode);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return vnodeOpenVnode(vnode);
}

static void vnodeRemoveDataFiles(int vnode) {
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

      if (!vnodeRemoveDataFileFromLinkFile(linkFile, de->d_name)) {
        continue;
      }

      memset(dfilePath, 0, TSDB_FILENAME_LEN);
      int tcode = readlink(linkFile, dfilePath, TSDB_FILENAME_LEN);
      remove(linkFile);

      if (tcode >= 0) {
        remove(dfilePath);
        dPrint("Data file %s is removed, link file %s", dfilePath, linkFile);
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
  dPrint("vid:%d, vnode is removed, status:%s", vnode, taosGetVnodeStatusStr(vnodeList[vnode].vnodeStatus));
}

int vnodeRemoveVnode(int vnode) {
  if (vnodeList == NULL) return TSDB_CODE_SUCCESS;

  if (vnodeList[vnode].cfg.maxSessions > 0) {
    SVnodeObj* pVnode = &vnodeList[vnode];
    if (pVnode->vnodeStatus == TSDB_VN_STATUS_CREATING
        || pVnode->vnodeStatus == TSDB_VN_STATUS_OFFLINE
        || pVnode->vnodeStatus == TSDB_VN_STATUS_DELETING) {
      dTrace("vid:%d, status:%s, cannot enter close/delete operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
      return TSDB_CODE_ACTION_IN_PROGRESS;
    } else {
      int32_t ret = vnodeCloseVnode(vnode);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      dTrace("vid:%d, status:%s, do delete operation", vnode, taosGetVnodeStatusStr(pVnode->vnodeStatus));
      vnodeRemoveDataFiles(vnode);
    }

  } else {
    dPrint("vid:%d, max sessions:%d, this vnode already dropped!!!", vnode, vnodeList[vnode].cfg.maxSessions);
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

  if (vnodeInitInfo() < 0) return -1;

  for (vnode = 0; vnode < TSDB_MAX_VNODES; ++vnode) {
    int code = vnodeInitStoreVnode(vnode);
    if (code != TSDB_CODE_SUCCESS) {
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

void vnodeCleanUpOneVnode(int vnode) {
  static int again = 0;
  if (vnodeList == NULL) return;

  pthread_mutex_lock(&dmutex);

  if (again) {
    pthread_mutex_unlock(&dmutex);
    return;
  }
  again = 1;

  if (vnodeList[vnode].pCachePool) {
    vnodeList[vnode].vnodeStatus = TSDB_VN_STATUS_OFFLINE;
    vnodeClosePeerVnode(vnode);
  }

  pthread_mutex_unlock(&dmutex);

  if (vnodeList[vnode].pCachePool) {
    vnodeProcessCommitTimer(vnodeList + vnode, NULL);
    while (vnodeList[vnode].commitThread != 0) {
      taosMsleep(10);
    }
    vnodeCleanUpCommit(vnode);
  }
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
      vnodeList[vnode].vnodeStatus = TSDB_VN_STATUS_OFFLINE;
      vnodeClosePeerVnode(vnode);
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

  atomic_store_32(&tsOpenVnodes, openVnodes);
}

void vnodeUpdateHeadFile(int vnode, int oldTables, int newTables) {
  //todo rewrite the head file with newTables
}
