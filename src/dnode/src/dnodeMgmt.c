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
//#include "tsdb.h"
#include "dnodeMgmt.h"
#include "dnodeRead.h"
#include "dnodeWrite.h"

typedef struct {
  int32_t vgId;     // global vnode group ID
  int32_t status;   // status: master, slave, notready, deleting
  int32_t refCount; // reference count
  int64_t version;
  void   *wworker;
  void   *rworker;
  void   *wal;
  void   *tsdb;
  void   *replica;
  void   *events;
  void   *cq;      // continuous query
} SVnodeObj;

static int32_t  dnodeOpenVnodes();
static void     dnodeCleanupVnodes();
static int32_t  dnodeOpenVnode(int32_t vgId);
static void     dnodeCleanupVnode(SVnodeObj *pVnode);
static int32_t  dnodeCreateVnode(SMDCreateVnodeMsg *cfg);
static void     dnodeDropVnode(SVnodeObj *pVnode);
static void     dnodeProcesSMDCreateVnodeMsg(SRpcMsg *pMsg);
static void     dnodeProcesSMDDropVnodeMsg(SRpcMsg *pMsg);
static void     dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static void   (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

static void * tsDnodeVnodesHash = NULL;

int32_t dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcesSMDCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcesSMDDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;

  tsDnodeVnodesHash = taosInitIntHash(TSDB_MAX_VNODES, sizeof(SVnodeObj), taosHashInt);
  if (tsDnodeVnodesHash == NULL) {
    dError("failed to init vnode list");
    return -1;
  }

  return dnodeOpenVnodes();
}

void dnodeCleanupMgmt() {
  dnodeCleanupVnodes();
  taosCleanUpIntHash(tsDnodeVnodesHash);
}

void dnodeMgmt(void *rpcMsg) {
  SRpcMsg *pMsg = rpcMsg;
  terrno = 0;

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;  
  }

  SRpcMsg rsp;
  rsp.handle = pMsg->handle;
  rsp.code   = terrno;
  rsp.pCont  = NULL;
  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);  // free the received message
}

void *dnodeGetVnode(int32_t vgId) {
  SVnodeObj *pVnode = taosGetIntHashData(tsDnodeVnodesHash, vgId);
  if (pVnode == NULL) {
    terrno = TSDB_CODE_INVALID_VGROUP_ID;
    return NULL;
  }

  if (pVnode->status != TSDB_VN_STATUS_MASTER && pVnode->status == TSDB_VN_STATUS_SLAVE) {
    terrno = TSDB_CODE_INVALID_VNODE_STATUS;
    return NULL;
  }

  atomic_add_fetch_32(&pVnode->refCount, 1);
  return pVnode;
}

int32_t dnodeGetVnodeStatus(void *pVnode) {
  return ((SVnodeObj *)pVnode)->status;
}

void *dnodeGetVnodeWworker(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wworker;
}
 
void *dnodeGetVnodeRworker(void *pVnode) {
  return ((SVnodeObj *)pVnode)->rworker;
}
 
void *dnodeGetVnodeWal(void *pVnode) {
  return ((SVnodeObj *)pVnode)->wal;
}

void *dnodeGetVnodeTsdb(void *pVnode) {
  return ((SVnodeObj *)pVnode)->tsdb;
}

void dnodeReleaseVnode(void *pVnode) {
  atomic_sub_fetch_32(&((SVnodeObj *) pVnode)->refCount, 1);
}

static int32_t dnodeOpenVnodes() {
  dPrint("open all vnodes");
  return TSDB_CODE_SUCCESS;
}

static void dnodeCleanupVnodes() {
  dPrint("clean all vnodes");
}

static int32_t dnodeOpenVnode(int32_t vgId) {
//  char rootDir[TSDB_FILENAME_LEN] = {0};
//  sprintf(rootDir, "%s/vnode%d", tsDirectory, vgId);
//
//  void *pTsdb = tsdbOpenRepo(rootDir);
//  if (pTsdb != NULL) {
//    return terrno;
//  }
//
//  SVnodeObj vnodeObj;
//  vnodeObj.vgId     = vgId;
//  vnodeObj.status   = TSDB_VN_STATUS_NOT_READY;
//  vnodeObj.refCount = 1;
//  vnodeObj.version  = 0;
//  vnodeObj.wworker  = dnodeAllocateWriteWorker();
//  vnodeObj.rworker  = dnodeAllocateReadWorker();
//  vnodeObj.wal      = NULL;
//  vnodeObj.tsdb     = pTsdb;
//  vnodeObj.replica  = NULL;
//  vnodeObj.events   = NULL;
//  vnodeObj.cq       = NULL;
//
//  taosAddIntHash(tsDnodeVnodesHash, vnodeObj.vgId, &vnodeObj);

  return TSDB_CODE_SUCCESS;
}

static void dnodeCleanupVnode(SVnodeObj *pVnode) {
//  pVnode->status = TSDB_VN_STATUS_NOT_READY;
//  int32_t count = atomic_sub_fetch_32(&pVnode->refCount, 1);
//  if (count > 0) {
//    // wait refcount
//  }
//
//  // remove replica
//
//  // remove read queue
//  dnodeFreeReadWorker(pVnode->rworker);
//  pVnode->rworker = NULL;
//
//  // remove write queue
//  dnodeFreeWriteWorker(pVnode->wworker);
//  pVnode->wworker = NULL;
//
//  // remove wal
//
//  // remove tsdb
//  if (pVnode->tsdb) {
//    tsdbCloseRepo(pVnode->tsdb);
//    pVnode->tsdb = NULL;
//  }
//
//  taosDeleteIntHash(tsDnodeVnodesHash, pVnode->vgId);
}

static int32_t dnodeCreateVnode(SMDCreateVnodeMsg *pVnodeCfg) {
//  STsdbCfg tsdbCfg;
//  tsdbCfg.precision           = pVnodeCfg->cfg.precision;
//  tsdbCfg.tsdbId              = pVnodeCfg->vnode;
//  tsdbCfg.maxTables           = pVnodeCfg->cfg.maxSessions;
//  tsdbCfg.daysPerFile         = pVnodeCfg->cfg.daysPerFile;
//  tsdbCfg.minRowsPerFileBlock = -1;
//  tsdbCfg.maxRowsPerFileBlock = -1;
//  tsdbCfg.keep                = -1;
//  tsdbCfg.maxCacheSize        = -1;

//  char rootDir[TSDB_FILENAME_LEN] = {0};
//  sprintf(rootDir, "%s/vnode%d", tsDirectory, pVnodeCfg->cfg.vgId);
//
//  void *pTsdb = tsdbCreateRepo(rootDir, &tsdbCfg, NULL);
//  if (pTsdb != NULL) {
//    return terrno;
//  }
//
//  SVnodeObj vnodeObj;
//  vnodeObj.vgId     = pVnodeCfg->cfg.vgId;
//  vnodeObj.status   = TSDB_VN_STATUS_NOT_READY;
//  vnodeObj.refCount = 1;
//  vnodeObj.version  = 0;
//  vnodeObj.wworker  = dnodeAllocateWriteWorker();
//  vnodeObj.rworker  = dnodeAllocateReadWorker();
//  vnodeObj.wal      = NULL;
//  vnodeObj.tsdb     = pTsdb;
//  vnodeObj.replica  = NULL;
//  vnodeObj.events   = NULL;
//  vnodeObj.cq       = NULL;
//
//  taosAddIntHash(tsDnodeVnodesHash, vnodeObj.vgId, &vnodeObj);

  return TSDB_CODE_SUCCESS;
}

static void dnodeDropVnode(SVnodeObj *pVnode) {
//  pVnode->status = TSDB_VN_STATUS_NOT_READY;
//
//  int32_t count = atomic_sub_fetch_32(&pVnode->refCount, 1);
//  if (count > 0) {
//    // wait refcount
//  }
//
//  if (pVnode->tsdb) {
//    tsdbDropRepo(pVnode->tsdb);
//    pVnode->tsdb = NULL;
//  }
//
//  dnodeCleanupVnode(pVnode);
}

static void dnodeProcesSMDCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDCreateVnodeMsg *pCreate = (SMDCreateVnodeMsg *) rpcMsg->pCont;
  pCreate->vnode           = htonl(pCreate->vnode);
  pCreate->cfg.vgId        = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.daysPerFile = htonl(pCreate->cfg.daysPerFile);

  SVnodeObj *pVnodeObj = taosGetIntHashData(tsDnodeVnodesHash, pCreate->cfg.vgId);
  if (pVnodeObj != NULL) {
    rpcRsp.code = TSDB_CODE_SUCCESS;
  } else {
    rpcRsp.code = dnodeCreateVnode(pCreate);
  }

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(rpcMsg->pCont);
}

static void dnodeProcesSMDDropVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDDropVnodeMsg *pDrop = (SMDCreateVnodeMsg *) rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  SVnodeObj *pVnodeObj = taosGetIntHashData(tsDnodeVnodesHash, pDrop->vgId);
  if (pVnodeObj != NULL) {
    dnodeDropVnode(pVnodeObj);
    rpcRsp.code = TSDB_CODE_SUCCESS;
  } else {
    rpcRsp.code = TSDB_CODE_INVALID_VGROUP_ID;
  }

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(rpcMsg->pCont);
}

static void dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};

  SMDCreateVnodeMsg *pCreate = (SMDCreateVnodeMsg *) rpcMsg->pCont;
  pCreate->vnode           = htonl(pCreate->vnode);
  pCreate->cfg.vgId        = htonl(pCreate->cfg.vgId);
  pCreate->cfg.maxSessions = htonl(pCreate->cfg.maxSessions);
  pCreate->cfg.daysPerFile = htonl(pCreate->cfg.daysPerFile);

  SVnodeObj *pVnodeObj = taosGetIntHashData(tsDnodeVnodesHash, pCreate->cfg.vgId);
  if (pVnodeObj != NULL) {
    rpcRsp.code = TSDB_CODE_SUCCESS;
  } else {
    rpcRsp.code = dnodeCreateVnode(pCreate);;
  }

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(rpcMsg->pCont);
}
