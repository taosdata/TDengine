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
#include "ihash.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tlog.h"
#include "trpc.h"
#include "tstatus.h"
#include "tsdb.h"
#include "ttime.h"
#include "ttimer.h"
#include "twal.h"
#include "dnodeMClient.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeWrite.h"
#include "vnode.h"
#include "vnodeInt.h"

extern void *tsDnodeVnodesHash;
static void vnodeCleanUp(SVnodeObj *pVnode);

int32_t vnodeCreate(SMDCreateVnodeMsg *pVnodeCfg) {
  int32_t code;

  SVnodeObj *pTemp = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, pVnodeCfg->cfg.vgId);

  if (pTemp != NULL) {
    dPrint("vgId:%d, vnode already exist, pVnode:%p", pVnodeCfg->cfg.vgId, pTemp);
    return TSDB_CODE_SUCCESS;
  } 

  STsdbCfg tsdbCfg = {0};
  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
  tsdbCfg.tsdbId              = pVnodeCfg->cfg.vgId;
  tsdbCfg.maxTables           = pVnodeCfg->cfg.maxSessions;
  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
  tsdbCfg.minRowsPerFileBlock = -1;
  tsdbCfg.maxRowsPerFileBlock = -1;
  tsdbCfg.keep                = -1;
  tsdbCfg.maxCacheSize        = -1;

  char rootDir[TSDB_FILENAME_LEN] = {0};
  sprintf(rootDir, "%s/vnode%d", tsVnodeDir, pVnodeCfg->cfg.vgId);
  if (mkdir(rootDir, 0755) != 0) {
    if (errno == EACCES) {
      return TSDB_CODE_NO_DISK_PERMISSIONS;
    } else if (errno == ENOSPC) {
      return TSDB_CODE_SERV_NO_DISKSPACE;
    } else if (errno == EEXIST) {
    } else {
      return TSDB_CODE_VG_INIT_FAILED;
    }
  }

  char tsdbDir[TSDB_FILENAME_LEN] = {0};
  sprintf(tsdbDir, "%s/vnode%d/tsdb", tsVnodeDir, pVnodeCfg->cfg.vgId);
  code = tsdbCreateRepo(tsdbDir, &tsdbCfg, NULL);
  if (code <0) {
    dError("vgId:%d, failed to create tsdb in vnode, reason:%s", pVnodeCfg->cfg.vgId, tstrerror(terrno));
    return code;
  }

  dPrint("vgId:%d, vnode is created", pVnodeCfg->cfg.vgId);
  code = vnodeOpen(pVnodeCfg->cfg.vgId, rootDir);

  return code;
}

int32_t vnodeDrop(int32_t vgId) {

  SVnodeObj *pVnode = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (pVnode == NULL) {
    dTrace("vgId:%d, failed to drop, vgId not exist", vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  dTrace("pVnode:%p vgId:%d, vnode will be dropped", pVnode, pVnode->vgId);
  pVnode->status = VN_STATUS_DELETING;
  vnodeCleanUp(pVnode);
 
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeOpen(int32_t vnode, char *rootDir) {
  char temp[TSDB_FILENAME_LEN];

  SVnodeObj vnodeObj = {0};
  vnodeObj.vgId     = vnode;
  vnodeObj.status   = VN_STATUS_INIT;
  vnodeObj.refCount = 1;
  vnodeObj.version  = 0;  
  SVnodeObj *pVnode = (SVnodeObj *)taosAddIntHash(tsDnodeVnodesHash, vnodeObj.vgId, (char *)(&vnodeObj));

  sprintf(temp, "%s/tsdb", rootDir);
  void *pTsdb = tsdbOpenRepo(temp);
  if (pTsdb == NULL) {
    dError("pVnode:%p vgId:%d, failed to open tsdb at %s(%s)", pVnode, pVnode->vgId, temp, tstrerror(terrno));
    taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
    return terrno;
  }

  pVnode->wqueue = dnodeAllocateWqueue(pVnode);
  pVnode->rqueue = dnodeAllocateRqueue(pVnode);

  sprintf(temp, "%s/wal", rootDir);
  pVnode->wal      = walOpen(temp, 3, tsCommitLog);
  pVnode->tsdb     = pTsdb;
  pVnode->sync     = NULL;
  pVnode->events   = NULL;
  pVnode->cq       = NULL;

  walRestore(pVnode->wal, pVnode, vnodeWriteToQueue);

  pVnode->status = VN_STATUS_READY;
  dTrace("pVnode:%p vgId:%d, vnode is opened in %s", pVnode, pVnode->vgId, rootDir);

  return TSDB_CODE_SUCCESS;
}

int32_t vnodeClose(void *param) {
  SVnodeObj *pVnode = (SVnodeObj *)param;

  dTrace("pVnode:%p vgId:%d, vnode will be closed", pVnode, pVnode->vgId);
  pVnode->status = VN_STATUS_CLOSING;
  vnodeCleanUp(pVnode);

  return 0;
}

void vnodeRelease(void *pVnodeRaw) {
  SVnodeObj *pVnode = pVnodeRaw;

  int32_t refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);

  if (refCount > 0) return;

  // remove read queue
  dnodeFreeRqueue(pVnode->rqueue);
  pVnode->rqueue = NULL;

  // remove write queue
  dnodeFreeWqueue(pVnode->wqueue);
  pVnode->wqueue = NULL;

  if (pVnode->status == VN_STATUS_DELETING) {
    // remove the whole directory
  }

  dTrace("pVnode:%p vgId:%d, vnode is released", pVnode, pVnode->vgId);
}

void *vnodeGetVnode(int32_t vgId) {
  SVnodeObj *pVnode = (SVnodeObj *) taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_INVALID_VGROUP_ID;
    return NULL;
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);
  dTrace("pVnode:%p vgId:%d, get vnode, refCount:%d", pVnode, pVnode->vgId, pVnode->refCount);

  return pVnode;
}

void *vnodeGetRqueue(void *pVnode) {
  return ((SVnodeObj *)pVnode)->rqueue; 
}

void *vnodeGetWqueue(int32_t vgId) {
  SVnodeObj *pVnode = vnodeGetVnode(vgId);
  if (pVnode == NULL) return NULL;
  return pVnode->wqueue;
} 

void *vnodeGetWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal; 
}

void *vnodeGetTsdb(void *pVnode) {
  return ((SVnodeObj *)pVnode)->tsdb; 
}

static void vnodeCleanUp(SVnodeObj *pVnode) {
  taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);

  //syncStop(pVnode->sync);
  tsdbCloseRepo(pVnode->tsdb);
  walClose(pVnode->wal);

  vnodeRelease(pVnode);
}
