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

#ifndef _TD_MND_INT_H_
#define _TD_MND_INT_H_

#include "mndDef.h"

#include "sdb.h"
#include "sync.h"
#include "tcache.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tgrant.h"
#include "thttp.h"
#include "tqueue.h"
#include "ttime.h"
#include "version.h"
#include "wal.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define mFatal(...) { if (mDebugFlag & DEBUG_FATAL) { taosPrintLog("MND FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }}
#define mError(...) { if (mDebugFlag & DEBUG_ERROR) { taosPrintLog("MND ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }}
#define mWarn(...)  { if (mDebugFlag & DEBUG_WARN)  { taosPrintLog("MND WARN  ", DEBUG_WARN,  255,        __VA_ARGS__); }}
#define mInfo(...)  { if (mDebugFlag & DEBUG_INFO)  { taosPrintLog("MND INFO  ", DEBUG_INFO,  255,        __VA_ARGS__); }}
#define mDebug(...) { if (mDebugFlag & DEBUG_DEBUG) { taosPrintLog("MND DEBUG ", DEBUG_DEBUG, mDebugFlag, __VA_ARGS__); }}
#define mTrace(...) { if (mDebugFlag & DEBUG_TRACE) { taosPrintLog("MND TRACE ", DEBUG_TRACE, mDebugFlag, __VA_ARGS__); }}

#define mGFatal(param, ...) { if (mDebugFlag & DEBUG_FATAL){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mFatal(param ", QID:%s", __VA_ARGS__, buf);}} 
#define mGError(param, ...) { if (mDebugFlag & DEBUG_ERROR){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mError(param ", QID:%s", __VA_ARGS__, buf);}}
#define mGWarn(param, ...)  { if (mDebugFlag & DEBUG_WARN) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); mWarn (param ", QID:%s", __VA_ARGS__, buf);}}
#define mGInfo(param, ...)  { if (mDebugFlag & DEBUG_INFO) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); mInfo (param ", QID:%s", __VA_ARGS__, buf);}}
#define mGDebug(param, ...) { if (mDebugFlag & DEBUG_DEBUG){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mDebug(param ", QID:%s", __VA_ARGS__, buf);}}
#define mGTrace(param, ...) { if (mDebugFlag & DEBUG_TRACE){ char buf[40] = {0}; TRACE_TO_STR(trace, buf); mTrace(param ", QID:%s", __VA_ARGS__, buf);}}
// clang-format on

#define SYSTABLE_SCH_TABLE_NAME_LEN ((TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_DB_NAME_LEN    ((TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE)
#define SYSTABLE_SCH_COL_NAME_LEN   ((TSDB_COL_NAME_LEN - 1) + VARSTR_HEADER_SIZE)

typedef int32_t (*MndMsgFp)(SRpcMsg *pMsg);
typedef int32_t (*MndMsgFpExt)(SRpcMsg *pMsg, SQueueInfo *pInfo);
typedef int32_t (*MndInitFp)(SMnode *pMnode);
typedef void (*MndCleanupFp)(SMnode *pMnode);
typedef int32_t (*ShowRetrieveFp)(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
typedef void (*ShowFreeIterFp)(SMnode *pMnode, void *pIter);
typedef struct SQueueWorker SQHandle;

typedef struct {
  const char  *name;
  MndInitFp    initFp;
  MndCleanupFp cleanupFp;
} SMnodeStep;

typedef struct {
  int64_t        showId;
  ShowRetrieveFp retrieveFps[TSDB_MGMT_TABLE_MAX];
  ShowFreeIterFp freeIterFps[TSDB_MGMT_TABLE_MAX];
  SCacheObj     *cache;
} SShowMgmt;

typedef struct {
  SCacheObj *connCache;
  SCacheObj *appCache;
  // SCRAM-SHA-256 application-layer auth (libgsasl). Leader-local: all mnode RPC is leader-served,
  // so every handshake round and the bridging CONNECT land on the same mnode.
  void      *saslCtx;         // Gsasl* context (void* keeps gsasl.h out of this header)
  SCacheObj *saslSessCache;   // authId    -> in-flight Gsasl_session for a multi-round handshake
  SCacheObj *saslTokenCache;  // authToken -> user that just completed authentication
} SProfileMgmt;

typedef struct {
  TdThreadMutex  lock;
  char           email[TSDB_FQDN_LEN];
  STelemAddrMgmt addrMgt;
} STelemMgmt;

typedef struct {
  tsem_t        syncSem;
  int64_t       sync;
  int32_t       errCode;
  int32_t       transId;
  int32_t       transSec;
  int64_t       transSeq;
  TdThreadMutex lock;
  int8_t        selfIndex;
  int8_t        numOfTotalReplicas;
  int8_t        numOfReplicas;
  SReplica      replicas[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  ESyncRole     nodeRoles[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SyncIndex     lastIndex;
} SSyncMgmt;

typedef struct {
  int64_t expireTimeMS;
  int64_t timeseriesAllowed;
} SGrantInfo;

typedef struct {
  int8_t  encrypting;
  int16_t nEncrypt;
  int16_t nSuccess;
  int16_t nFailed;
} SEncryptMgmt;

// SOrphanRbKey: composite key for the orphan-rollback dedup map (txnId + vgId).
// The explicit _pad field ensures no uninitialized bytes inside sizeof, making it
// safe to use as a binary hash key.
typedef struct {
  txn_id_t txnId;  // 8 bytes
  int32_t  vgId;   // 4 bytes
  int32_t  _pad;   // 4 bytes, always 0
} SOrphanRbKey;    // 16 bytes total, no implicit padding

// SOrphanTxnEntry: in-memory record for a mystery orphan txn (not found in SDB_TXN or SDB_TXN_LOG).
// Key = SOrphanRbKey{txnId, vgId}; stored in STxnMgmt.pOrphanTxnMap.
// Surfaced via ins_transaction_logs with status='orphan' for manual investigation.
typedef struct {
  txn_id_t txnId;        // 8 bytes
  int32_t  vgId;         // which VGroup reported this orphan
  int64_t  firstSeen;    // ms timestamp of first observation
  int64_t  lastSeen;     // ms timestamp of most recent observation
  int32_t  reportCount;  // number of heartbeats from VNode reporting this orphan
} SOrphanTxnEntry;

// STxnMgmt: MNode-side runtime management context for user batch transactions, embedded in SMnode.
// activeTxnCnt: current active transaction count (atomic); used for admission control
//               (new BEGIN is rejected when the limit is reached).
// All durable transaction state lives in SDB_TXN (STxnObj), replicated via Raft.
// Note: timeout scanning is driven by the mndMain.c timer thread (TDMT_MND_TXN_TIMER);
//       no separate timer handle is needed here.

typedef struct {
  // Dedup map for orphan-txn rollbacks.
  // When a VNode reports an in-flight txn that no longer exists in SDB, the MNode fires an
  // STrans-based rollback.  Without dedup, repeated VNode idle reports would spawn N_vnodes x K
  // STrans objects.  Key = SOrphanRbKey{txnId, vgId} (per-VNode granularity so other VGroups still
  // get their own rollback); value = ms timestamp of the last rollback initiation.  A 1-hour
  // cooldown suppresses duplicates while giving TRN_POLICY_RETRY enough time to succeed.
  // GC: stale entries (cooldown elapsed) are evicted inside mndTxnTimeoutScanImpl, which runs
  //     every MND_TXN_PULLUP_INTERVAL_SEC seconds — the map does not grow without bound.
  // In-memory only: MNode failover resets this map, and the new leader re-triggers as needed.
  SHashObj        *pOrphanRollbackTs;
  // In-memory map of mystery orphan txns: txnId found in no SDB (GC'd, invalid, or never committed).
  // Key = SOrphanRbKey{txnId, vgId}; value = SOrphanTxnEntry.  Written from write-worker thread only.
  // Surfaced as stage='orphan' rows in ins_transaction_logs for operator visibility.
  // GC: entries whose lastSeen is >1h old are evicted in mndTxnTimeoutScanImpl.
  SHashObj        *pOrphanTxnMap;
  volatile int32_t activeTxnCnt;  // active txn count (maintained atomically by SDB insert/update/delete callbacks)
  // O(1) super-table conflict map.  Key = stbFName (NUL-terminated string, TSDB_TABLE_FNAME_LEN),
  // value = txn_id_t of the txn holding the shadow op for this STB.  Written from the
  // write-worker thread only (same thread as all STxnObj SDB updates).  Entries are inserted in
  // mndTxnAddShadowOp and removed in mndTxnFreeObj when the STxnObj is destroyed.
  SHashObj        *pStbConflictMap;
  txn_id_t         currentTxnId;   // highest txnId allocated so far (moved here from mndTxnSeq.c global)
  // pendingRangeId: the nextRangeId of the in-flight alloc-txn-seq STrans, or 0 if none is in flight.
  // Used for fine-grained dedup: a new trigger is suppressed only when nextRangeId <= pendingRangeId
  // (the in-flight request already covers the needed range).  A larger nextRangeId is always allowed
  // through so that range exhaustion during a slow in-flight alloc is handled correctly.
  // Stuck-alloc detection: if taosGetTimestampMs() - txnSeqAllocTime > 30s, the response was likely
  // lost; reset pendingRangeId to 0 and allow a fresh trigger.
  volatile txn_id_t pendingRangeId;  // rangeId of the in-flight allocation (0 = none)
  int64_t           txnSeqAllocTime; // timestamp (ms) of the last alloc trigger, for stuck detection
} STxnMgmt;

typedef struct SMnode {
  int32_t        selfDnodeId;
  int64_t        clusterId;
  TdThread       thread;
  TdThread       arbThread;
  TdThreadRwlock lock;
  int32_t        rpcRef;
  int32_t        syncRef;
  bool           stopped;
  bool           restored;
  bool           deploy;
  int8_t         sodPhase;
  int8_t         macActive;
  char          *path;
  SyncIndex      applied;
  SSdb          *pSdb;
  SArray        *pSteps;
  SQHandle      *pQuery;
  SHashObj      *infosMeta;
  SHashObj      *perfsMeta;
  SWal          *pWal;
  SShowMgmt      showMgmt;
  SProfileMgmt   profileMgmt;
  STelemMgmt     telemMgmt;
  SSyncMgmt      syncMgmt;
  SEncryptMgmt   encryptMgmt;
  STxnMgmt       txnMgmt;
  SGrantInfo     grant;
  MndMsgFp       msgFp[TDMT_MAX];
  MndMsgFpExt    msgFpExt[TDMT_MAX];
  SMsgCb         msgCb;
  int64_t        ipWhiteVer;
  int64_t        timeWhiteVer;
  int32_t        version;
  int32_t        encrypted;
} SMnode;

void    mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp);
void    mndSetMsgHandleExt(SMnode *pMnode, tmsg_t msgType, MndMsgFpExt fp);
int64_t mndGenerateUid(const char *name, int32_t len);
void    mndSetSoDPhase(SMnode *pMnode, int8_t status);
int8_t  mndGetSoDPhase(SMnode *pMnode);

void mndSetRestored(SMnode *pMnode, bool restored);
bool mndGetRestored(SMnode *pMnode);
void mndSetStop(SMnode *pMnode);
bool mndGetStop(SMnode *pMnode);

SArray *mndGetAllDnodeFqdns(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_INT_H_*/
