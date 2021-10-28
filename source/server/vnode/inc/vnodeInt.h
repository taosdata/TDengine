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

#include "os.h"
#include "amalloc.h"
#include "meta.h"
#include "sync.h"
#include "taosmsg.h"
#include "tglobal.h"
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

typedef struct STsdbCfg {
  int32_t cacheBlockSize;  // MB
  int32_t totalBlocks;
  int32_t daysPerFile;
  int32_t daysToKeep0;
  int32_t daysToKeep1;
  int32_t daysToKeep2;
  int32_t minRowsPerFileBlock;
  int32_t maxRowsPerFileBlock;
  uint8_t precision;  // time resolution
  int8_t  compression;
  int8_t  cacheLastRow;
  int8_t  update;
} STsdbCfg;

typedef struct SMetaCfg {
} SMetaCfg;

typedef struct SSyncCluster {
  int8_t    replica;
  int8_t    quorum;
  SNodeInfo nodes[TSDB_MAX_REPLICA];
} SSyncCfg;

typedef struct SVnodeCfg {
  char     db[TSDB_ACCT_ID_LEN + TSDB_DB_NAME_LEN];
  int8_t   dropped;
  SWalCfg  wal;
  STsdbCfg tsdb;
  SMetaCfg meta;
  SSyncCfg sync;
} SVnodeCfg;

typedef struct {
  int32_t          vgId;      // global vnode group ID
  int32_t          refCount;  // reference count
  SMemAllocator   *allocator;
  SMeta           *pMeta;
  STsdb           *pTsdb;
  STQ             *pTQ;
  twalh            pWal;
  void            *pQuery;
  SyncNodeId       syncNode;
  taos_queue       pWriteQ;  // write queue
  taos_queue       pQueryQ;  // read query queue
  taos_queue       pFetchQ;  // read fetch/cancel queue
  SVnodeCfg        cfg;
  SSyncServerState term;
  int64_t          queuedWMsgSize;
  int32_t          queuedWMsg;
  int32_t          queuedRMsg;
  int32_t          numOfQHandle;  // current initialized and existed query handle in current dnode
  int8_t           role;
  int8_t           accessState;
  int8_t           dropped;
  int8_t           status;
  pthread_mutex_t  statusMutex;
} SVnode;

typedef struct {
  int32_t len;
  void   *rsp;
  void   *qhandle;  // used by query and retrieve msg
} SVnRsp;

void vnodeSendMsgToDnode(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg);
void vnodeSendMsgToMnode(struct SRpcMsg *rpcMsg);
void vnodeGetDnodeEp(int32_t dnodeId, char *ep, char *fqdn, uint16_t *port);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_INT_H_*/
