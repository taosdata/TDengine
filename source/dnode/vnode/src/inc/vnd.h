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

#ifndef _TD_VND_H_
#define _TD_VND_H_

#include "sync.h"
#include "ttrace.h"
#include "vnodeInt.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define vFatal(...) do { if (vDebugFlag & DEBUG_FATAL) { taosPrintLog("VND FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }} while(0)
#define vError(...) do { if (vDebugFlag & DEBUG_ERROR) { taosPrintLog("VND ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }} while(0)
#define vWarn(...)  do { if (vDebugFlag & DEBUG_WARN)  { taosPrintLog("VND WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }} while(0)
#define vInfo(...)  do { if (vDebugFlag & DEBUG_INFO)  { taosPrintLog("VND INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }} while(0)
#define vDebug(...) do { if (vDebugFlag & DEBUG_DEBUG) { taosPrintLog("VND DEBUG ", DEBUG_DEBUG, vDebugFlag, __VA_ARGS__); }} while(0)
#define vTrace(...) do { if (vDebugFlag & DEBUG_TRACE) { taosPrintLog("VND TRACE ", DEBUG_TRACE, vDebugFlag, __VA_ARGS__); }} while(0)

#define vGTrace(trace, param, ...) do { if (vDebugFlag & DEBUG_TRACE) { vTrace(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGFatal(trace, param, ...) do { if (vDebugFlag & DEBUG_FATAL) { vFatal(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGError(trace, param, ...) do { if (vDebugFlag & DEBUG_ERROR) { vError(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGWarn(trace, param, ...)  do { if (vDebugFlag & DEBUG_WARN)  { vWarn(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGInfo(trace, param, ...)  do { if (vDebugFlag & DEBUG_INFO)  { vInfo(param  ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)
#define vGDebug(trace, param, ...) do { if (vDebugFlag & DEBUG_DEBUG) { vDebug(param ", QID:0x%" PRIx64 ":0x%" PRIx64, __VA_ARGS__, (trace) ? (trace)->rootId : 0, (trace) ? (trace)->msgId : 0);}} while(0)

// clang-format on

// vnodeCfg.c
extern const SVnodeCfg vnodeCfgDefault;

int32_t vnodeCheckCfg(const SVnodeCfg*);
int32_t vnodeEncodeConfig(const void* pObj, SJson* pJson);
int32_t vnodeDecodeConfig(const SJson* pJson, void* pObj);

// vnodeAsync.c
typedef enum {
  EVA_PRIORITY_HIGH = 0,
  EVA_PRIORITY_NORMAL,
  EVA_PRIORITY_LOW,
} EVAPriority;

typedef enum {
  EVA_TASK_COMMIT = 1,
  EVA_TASK_MERGE,
  EVA_TASK_COMPACT,
  EVA_TASK_RETENTION,
} EVATaskT;

#define COMMIT_TASK_ASYNC    1
#define MERGE_TASK_ASYNC     2
#define COMPACT_TASK_ASYNC   3
#define RETENTION_TASK_ASYNC 4
#define SCAN_TASK_ASYNC      5

int32_t vnodeAsyncOpen();
void    vnodeAsyncClose();
int32_t vnodeAChannelInit(int64_t async, SVAChannelID* channelID);
int32_t vnodeAChannelDestroy(SVAChannelID* channelID, bool waitRunning);
int32_t vnodeAsync(int64_t async, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*), void* arg,
                   SVATaskID* taskID);
int32_t vnodeAsyncC(SVAChannelID* channelID, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*),
                    void* arg, SVATaskID* taskID);
void    vnodeAWait(SVATaskID* taskID);
int32_t vnodeACancel(SVATaskID* taskID);
int32_t vnodeAsyncSetWorkers(int64_t async, int32_t numWorkers);
bool    vnodeATaskValid(SVATaskID* taskID);
bool    vnodeAsyncHasQueuedTask(int64_t asyncID);

const char* vnodeGetATaskName(EVATaskT task);

// vnodeBufPool.c
typedef struct SVBufPoolNode SVBufPoolNode;
struct SVBufPoolNode {
  SVBufPoolNode*  prev;
  SVBufPoolNode** pnext;
  int64_t         size;
  uint8_t*        data;
};

struct SVBufPool {
  SVBufPool* freeNext;
  SVBufPool* recycleNext;
  SVBufPool* recyclePrev;

  // query handle list
  TdThreadMutex mutex;
  int32_t       nQuery;
  SQueryNode    qList;

  SVnode*           pVnode;
  int32_t           id;
  volatile int32_t  nRef;
  int64_t           size;
  uint8_t*          ptr;
  SVBufPoolNode*    pTail;
  SVBufPoolNode     node;
};

int32_t vnodeOpenBufPool(SVnode* pVnode);
void    vnodeCloseBufPool(SVnode* pVnode);
void    vnodeBufPoolReset(SVBufPool* pPool);
void    vnodeBufPoolAddToFreeList(SVBufPool* pPool);
int32_t vnodeBufPoolRecycle(SVBufPool* pPool);

// vnodeOpen.c
void vnodeGetPrimaryDir(const char* relPath, int32_t diskPrimary, STfs* pTfs, char* buf, size_t bufLen);
void vnodeGetPrimaryPath(SVnode* pVnode, bool mount, char* buf, size_t bufLen);

// vnodeQuery.c
int32_t vnodeQueryOpen(SVnode* pVnode);
void    vnodeQueryPreClose(SVnode* pVnode);
void    vnodeQueryClose(SVnode* pVnode);
int32_t vnodeGetTableMeta(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int     vnodeGetTableCfg(SVnode* pVnode, SRpcMsg* pMsg, bool direct);
int32_t vnodeGetBatchMeta(SVnode* pVnode, SRpcMsg* pMsg);
int32_t vnodeGetVSubtablesMeta(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeGetVStbRefDbs(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeProcessVTableRefResolveReq(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeGetVTbTagCond(SVnode *pVnode, SRpcMsg *pMsg, bool direct);

// vnodeCommit.c
int32_t vnodeBegin(SVnode* pVnode);
int32_t vnodeShouldCommit(SVnode* pVnode, bool atExit);
void    vnodeRollback(SVnode* pVnode);
int32_t vnodeSaveInfo(const char* dir, const SVnodeInfo* pCfg);
int32_t vnodeCommitInfo(const char* dir);
int32_t vnodeLoadInfo(const char* dir, SVnodeInfo* pInfo);
int32_t vnodeSyncCommit(SVnode* pVnode);
int32_t vnodeAsyncCommit(SVnode* pVnode, bool forceTrimWal);
int32_t vnodeAsyncCommitEx(SVnode* pVnode, bool forceTrimWal);
bool    vnodeShouldRollback(SVnode* pVnode);

// vnodeTxnWalMgr.c — txn-atomic WAL cache for CDC consumers (tq + stream)

// Global config (dynamically updatable at runtime)
extern int32_t gTxnWalTtlDays;            // default 30; 0 = disable cache entirely
extern int32_t gTxnWalEvictAfterIdleSec;  // default 3600; evict committed slot if consumer idle > this
extern int64_t gTxnWalMaxMemBytes;        // default 20MB; trigger eviction when totalMemBytes exceeds this

// Cached copy of one WAL entry belonging to a transaction.
// Stores (walIndex, msgType, txnId) plus the raw RPC body — identical to
// pMsg->pCont / walContBody() in the commit callback and WAL-reader paths.
typedef struct SWalContCopy {
  int64_t  walIndex;  // WAL index (SWalCont.version)
  tmsg_t   msgType;   // original message type (SWalCont.msgType)
  txn_id_t txnId;     // owning transaction ID
  int32_t  bodyLen;   // length of body[] below
  char     body[];    // raw RPC body (no txnId prefix, no WAL header)
} SWalContCopy;

// In-memory cache slot for one batch-meta transaction.
// slotLock: writer = producer (put/rollback/evict), readers = consumers (get).
typedef struct STxnCacheSlot {
  SRWLatch         slotLock;  // protects pMsgs, committed, rolledBack, slotMemBytes
  txn_id_t         txnId;
  int64_t          beginIndex;     // WAL index of TXN_BEGIN (>0 = known; 0 = tombstone created without TXN_BEGIN)
  int64_t          commitIndex;    // WAL index of TXN_COMMIT (0 = not yet committed)
  volatile int64_t lastConsumeTs;  // last consumer access timestamp (ms); updated atomically
  int64_t          slotMemBytes;   // total body bytes in pMsgs (0 after rollback); under slotLock
  bool             committed;
  bool             rolledBack;  // pMsgs freed on rollback; slot kept as tombstone
  SArray*          pMsgs;       // SArray<SWalContCopy*>; NULL iff rolledBack
} STxnCacheSlot;

// Manager: one instance per SVnode, shared by tq and stream consumers.
// Locking strategy:
//   pTxnHash  — HASH_ENTRY_LOCK (per-bucket); protects slot pointer insertion/removal.
//   slotLock  — per-slot SRWLatch; protects slot fields (pMsgs, flags, slotMemBytes).
//   totalMemBytes / lastTxnConsumeTs / minTxnIndexNotVacuumed — atomic int64.
typedef struct STxnWalManager {
  SHashObj*        pTxnHash;                // txnId -> STxnCacheSlot*; HASH_ENTRY_LOCK
  SWal*            pWal;                    // back-reference for keepVersion updates
  SVnode*          pVnode;                  // back-reference for DDL txn entry scan
  volatile int64_t minTxnIndexNotVacuumed;  // oldest unconsumed txn WAL index; atomic
  volatile int64_t lastTxnConsumeTs;        // persisted in SVnodeInfo; updated atomically
  volatile int64_t totalMemBytes;           // real-time sum of all slotMemBytes; updated atomically
} STxnWalManager;

STxnWalManager *txnMgrOpen(SWal *pWal, SVnode *pVnode, int64_t lastTxnConsumeTs);
void            txnMgrClose(STxnWalManager *pMgr);

// Producer path: called from vnodeSyncCommitMsg (FpCommitCb) for txn entries.
// Handles IS_META_MSG (cache body), TXN_COMMIT (mark committed), TXN_ROLLBACK (free msgs).
int32_t txnMgrProducerPut(STxnWalManager* pMgr, txn_id_t txnId, int64_t walIndex, tmsg_t msgType, const void* body,
                          int32_t bodyLen);
// Reload path: called during walRestore on startup or lazy-load rescan.
// Identical semantics to txnMgrProducerPut; separate name for log clarity.
int32_t txnMgrReloadPut(STxnWalManager* pMgr, txn_id_t txnId, int64_t walIndex, tmsg_t msgType, const void* body,
                        int32_t bodyLen);

// Consumer path: called when consumer reads TXN_COMMIT.
// Updates lastConsumeTs and returns:
//   >0  pMsgs populated — caller delivers atomically
//    0  rolledBack      — caller skips
//   -1  slot not found  — caller triggers lazy load then calls again
int32_t txnMgrConsumerGet(STxnWalManager *pMgr, txn_id_t txnId,
                          int64_t nowMs, SArray **ppMsgs);

// Reload a WAL range [beginVer, endVer] into the cache (startup eager load).
int32_t txnMgrReloadFromWal(STxnWalManager *pMgr, SWal *pWal,
                            int64_t beginVer, int64_t endVer);

// Evict committed slots idle longer than gTxnWalEvictAfterIdleSec when over memory limit.
// Called inline from txnMgrProducerPut when totalMemBytes > gTxnWalMaxMemBytes.
void    txnMgrEvict(STxnWalManager *pMgr, int64_t nowMs);

// Returns the minimum beginIndex across all active CDC cache slots AND DDL txn entries.
// Returns INT64_MAX when both caches are empty. Skips beginIndex==0 slots.
int64_t txnMgrGetMinWalIndex(STxnWalManager *pMgr, SVnode *pVnode);

// Recompute minTxnIndexNotVacuumed (CDC + DDL) and push to WAL keep-version.
// Call after any slot/entry is removed (evict / vacuum completion / inline commit) and at vnode open.
void    txnMgrRefreshWalKeepVersion(STxnWalManager *pMgr, SWal *pWal, SVnode *pVnode);

// vnodeSync.c
int64_t vnodeClusterId(SVnode* pVnode);
int32_t vnodeNodeId(SVnode* pVnode);
int32_t vnodeSyncOpen(SVnode* pVnode, char* path, int32_t vnodeVersion);
int32_t vnodeSyncStart(SVnode* pVnode);
void    vnodeSyncPreClose(SVnode* pVnode);
void    vnodeSyncPostClose(SVnode* pVnode);
void    vnodeSyncClose(SVnode* pVnode);
void    vnodeRedirectRpcMsg(SVnode* pVnode, SRpcMsg* pMsg, int32_t code);
bool    vnodeIsLeader(SVnode* pVnode);
bool    vnodeIsRoleLeader(SVnode* pVnode);
int32_t    vnodeSetElectBaseline(SVnode* pVnode, int32_t ms);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VND_H_*/
