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
#include "tlog.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "trpc.h"
#include "dnodeWrite.h"
#include "dnodeRead.h"
#include "dnodeMgmt.h"

typedef struct {
  int32_t vgId;     // global vnode group ID
  int     status;   // status: master, slave, notready, deleting
  int     refCount; // reference count
  int64_t version;
  void    *wworker;
  void    *rworker;
  void    *wal;
  void    *tsdb;
  void    *replica;
  void    *events;
  void    *cq;      // continuous query
} SVnodeObj;

static int  dnodeOpenVnodes();
static void dnodeCleanupVnodes();
static int  dnodeCreateVnode(int32_t vgId, SCreateVnodeMsg *cfg);
static int  dnodeDropVnode(SVnodeObj *pVnode);
static void dnodeRemoveVnode(SVnodeObj *pVnode);

static void dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static void dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static void dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static void (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_FREE_VNODE] = dnodeProcessDropVnodeMsg;
  return 0;
}

void dnodeCleanupMgmt() {

}

void dnodeMgmt(SRpcMsg *pMsg) {
  
  terrno = 0;

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;  
  }

  SRpcMsg rsp;
  rsp.handle = pMsg->handle;
  rsp.code = terrno;
  rsp.pCont = NULL;
  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);  // free the received message
}
 
void *dnodeGetVnode(int vgId) {
  SVnodeObj *pVnode;

  // retrieve the pVnode from vgId

  
  // if (pVnode->status == ....) {
  //   terrno = ;
  //   return NULL;
  // }

  atomic_add_fetch_32(&pVnode->refCount, 1);
  return pVnode;
}

int dnodeGetVnodeStatus(void *pVnode) {
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

void dnodeReleaseVnode(void *param) {
  SVnodeObj *pVnode = (SVnodeObj *)param;

  int refCount = atomic_sub_fetch_32(&pVnode->refCount, 1);
  if (refCount == 0) dnodeRemoveVnode(pVnode);
}

static int dnodeOpenVnode() {
  SVnodeObj *pVnode;

  // create tsdb

  // create wal

  // allocate write worker
  pVnode->wworker = dnodeAllocateWriteWorker(); 

  // create read queue
  pVnode->rworker = dnodeAllocateReadWorker(); 

  // create the replica 

  // set the status

  pVnode->refCount = 1;

  return 0;
}

static int dnodeOpenVnodes() {
  return 0;
}

static void dnodeCleanupVnode() {

}

static void dnodeCleanupVnodes() {

}

static int dnodeCreateVnode(int32_t vgId, SCreateVnodeMsg *cfg) {

  SVnodeObj *pVnode = malloc(sizeof(SVnodeObj));
  
  // save the vnode info in non-volatile storage
  
  // add into hash, so it can be retrieved
  dnodeOpenVnode(pVnode);

  return 0;
}

static void dnodeRemoveVnode(SVnodeObj *pVnode) {
  
  // remove replica

  // remove read queue
  dnodeFreeReadWorker(pVnode->rworker);

  // remove write queue
  dnodeFreeWriteWorker(pVnode->wworker);
 
  // remove wal

  // remove tsdb

}

static int dnodeDropVnode(SVnodeObj *pVnode) {
  
  int count = atomic_sub_fetch_32(&pVnode->refCount, 1);

  if (count<=0) dnodeRemoveVnode(pVnode);

  return 0;
}

static void dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg) {

//  SVnodeObj  *pVnode;
//  int         vgId;
//  SVPeersMsg *pCfg;
  
  // check everything, if not ok, set terrno;


  // everything is ok

//  dnodeCreateVnode(vgId, pCfg);

  //if (pVnode == NULL) terrno = TSDB_CODE
}

static void dnodeProcessDropVnodeMsg(SRpcMsg *pMsg) {

  SVnodeObj *pVnode;
  int        vgId;
  
  // check everything, if not ok, set terrno;


  // everything is ok
  dnodeDropVnode(pVnode);

  //if (pVnode == NULL) terrno = TSDB_CODE
}

static void dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg) {

  SVnodeObj *pVnode;
  int        vgId;
  
  // check everything, if not ok, set terrno;


  // everything is ok
//  dnodeAlterVnode(pVnode);

  //if (pVnode == NULL) terrno = TSDB_CODE
}

