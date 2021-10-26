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

#ifndef _TD_VNODE_INT_H_
#define _TD_VNODE_INT_H_

#include "amalloc.h"
#include "meta.h"
#include "os.h"
#include "sync.h"
#include "taosmsg.h"
#include "tlog.h"
#include "tq.h"
#include "tqueue.h"
#include "trpc.h"
#include "tsdb.h"
#include "tworker.h"
#include "vnode.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int32_t vDebugFlag;

#define vFatal(...) { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", 255, __VA_ARGS__); }}
#define vError(...) { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", 255, __VA_ARGS__); }}
#define vWarn(...)  { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN ", 255, __VA_ARGS__); }}
#define vInfo(...)  { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND ", 255, __VA_ARGS__); }}
#define vDebug(...) { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}
#define vTrace(...) { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND ", vDebugFlag, __VA_ARGS__); }}

typedef struct {
  int32_t         vgId;      // global vnode group ID
  int32_t         refCount;  // reference count
  SMemAllocator  *allocator;
  SMeta          *pMeta;
  STsdb          *pTsdb;
  STQ            *pTQ;
  twalh           pWal;
  SyncNodeId      syncNode;
  taos_queue      pWriteQ;  // write queue
  taos_queue      pQueryQ;  // read query queue
  taos_queue      pFetchQ;  // read fetch/cancel queue
  SWalCfg         walCfg;
  SSyncCluster    syncCfg;
  char            db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int64_t         queuedWMsgSize;
  int32_t         queuedWMsg;
  int32_t         queuedRMsg;
  int32_t         numOfQHandle;  // current initialized and existed query handle in current dnode
  int8_t          status;
  int8_t          role;
  int8_t          accessState;
  int8_t          dropped;
  pthread_mutex_t statusMutex;
} SVnode;

typedef struct {
  int32_t len;
  void *  rsp;
  void *  qhandle;  // used by query and retrieve msg
} SVnRsp;

void vnodeSendMsgToDnode(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);
void vnodeSendMsgToMnode(struct SRpcMsg *rpcMsg);
void vnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

int32_t vnodeCreate(SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeDrop(int32_t vgId);
int32_t vnodeOpen(int32_t vgId);
int32_t vnodeAlter(SVnode *pVnode, SCreateVnodeMsg *pVnodeCfg);
int32_t vnodeSync(int32_t vgId);
int32_t vnodeClose(int32_t vgId);
void    vnodeCleanUp(SVnode *pVnode);
void    vnodeDestroy(SVnode *pVnode);
int32_t vnodeCompact(int32_t vgId);
void    vnodeBackup(int32_t vgId);
void    vnodeGetStatus(struct SStatusMsg *status);

SVnode *vnodeAcquire(int32_t vgId);
SVnode *vnodeAcquireNotClose(int32_t vgId);
void    vnodeRelease(SVnode *pVnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_INT_H_*/
