/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#ifndef _TD_MND_USER_TXN__H_
#define _TD_MND_USER_TXN__H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

#define UTXN_ID_MASK   0xFFFFFFFF00000000ULL
#define IS_UTXN_ID(id) (((id) & UTXN_ID_MASK) != 0)

// How often the MNode write-worker polls for timed-out txns.  Must be <= TSDB_TXN_HB_TIMEOUT.
// Worst-case detection latency = TSDB_TXN_HB_TIMEOUT + MND_TXN_PULLUP_INTERVAL_SEC.
#define MND_TXN_PULLUP_INTERVAL_SEC (TSDB_TXN_HB_TIMEOUT / 4)
#define TRANS_ID(id)                ((int32_t)((id) & 0xFFFFFFFF))

// EUtxnStage / EVtxnStage are defined in tdef.h (shared header); not redefined here.
// Design notes:
//   DECIDING is the 2PC point of no return. Once the MNode writes the COMMIT decision
//   to the Raft WAL and enters this stage, any subsequent failure (including a leader
//   switchover) requires the new leader to continue pushing the COMMIT  -  rollback is
//   no longer permitted.
//   The VNode does not need to distinguish between "commit complete" and "rollback
//   complete"  -  both mean the shadow data has been processed and the transaction
//   context can be destroyed. FINISHING is used for both; the VNode records the
//   specific operation type internally.

int32_t mndInitTxn(SMnode* pMnode);
void    mndCleanupTxn(SMnode* pMnode);
void    mndTxnDoTimeoutScan(
    SMnode* pMnode);  // timeout scan entry point (normally triggered by TDMT_MND_TXN_TIMER on the write worker thread)
const char* mndUtxnStageStr(EUtxnStage stage);
const char* mndVtxnStageStr(EVtxnStage stage);
const char* mndTxnStr(EUtxnStage stage);  // alias for mndUtxnStageStr; used by mndTrans.c etc.
int8_t      mndTxnIsAlive(SMnode*  pMnode,
                          txn_id_t txnId);  // keepalive query: returns non-zero if txn is still in an active stage
// Action MNode should take for a VNode-reported idle txn.
typedef enum {
  ORPHAN_TXN_ACTION_SKIP = 0,          // txn alive in SDB_TXN; MNode owns lifecycle, VNode keeps waiting
  ORPHAN_TXN_ACTION_COMMIT = 1,        // MNode committed; re-deliver COMMIT to VNode
  ORPHAN_TXN_ACTION_ROLLBACK = 2,      // MNode rolled back / abandoned; VNode must rollback
  ORPHAN_TXN_ACTION_SKIP_UNKNOWN = 3,  // not found in SDB_TXN or SDB_TXN_LOG; mystery orphan, recorded in-memory
} EOrphanTxnAction;

EOrphanTxnAction mndGetOrphanTxnAction(SMnode* pMnode, txn_id_t txnId);
void             mndRecordOrphanTxn(SMnode* pMnode, txn_id_t txnId, int32_t vgId);
void             mndTxnRefreshKeepalive(SMnode* pMnode, txn_id_t txnId);  // client HB keepalive: refresh lastActiveTime
// Returns true if txnId was forcibly rolled back by the MNode due to inactivity or lifetime timeout.
// Used by the HB handler to notify the client via HEARTBEAT_KEY_TXN_KILLED.
bool    mndTxnIsTimeoutKilled(SMnode* pMnode, txn_id_t txnId);
int32_t mndRollbackOrphanTxnOnVnode(SMnode* pMnode, txn_id_t txnId, int32_t vgId);  // Raft-safe orphan rollback
int32_t mndCommitOrphanTxnOnVnode(SMnode* pMnode, txn_id_t txnId, int32_t vgId);    // Raft-safe orphan commit

// ============================================================================
// MNode Shadow Operation Types - in-place + undo-log for STB/stream DDL within
// user batch txn (ALTER STB defers the schema change to COMMIT; see below)
// ============================================================================
//
// DDL Isolation Semantics (in-place + undo-log model):
//   - DDL within a batch txn IS applied to SDB immediately, with the object's
//     txnId/txnStatus set so that other connections filter it out.
//   - The shadow op records what was done, so ROLLBACK can undo it.
//   - On COMMIT: txnId is cleared and txnStatus returns to NORMAL (PRE_DROP /
//     PRE_CREATE_DROP objects are removed from SDB instead).
//   - On ROLLBACK: shadow ops are undone (PRE_CREATE objects deleted, PRE_DROP
//     objects unmarked).
//   - After a leader transfer or restart, mndTxnRebuildShadowOpsFromSdb()
//     reconstructs shadow ops by scanning SDB for objects with txnId != 0.
//
// Scope: CREATE/DROP STB and CREATE/DROP STREAM follow the in-place + undo-log
// model above - the marker is applied now and COMMIT/ROLLBACK only finalise it.
// ALTER STB is the exception: it marks the STB in place (PRE_ALTER marker +
// txnAlterReqs chain) but the schema change itself is deferred and replayed at
// COMMIT from the serialized request carried by the shadow op (redo-style).
//
// Super table DDL goes through MNode Trans framework, not client -> VNode direct path.
// Child/normal table shadow ops are tracked at VNode side (vnodeTxn.c), not here.
// Stream objects live only in MNode SDB, so they are tracked entirely here.

typedef enum {
  MND_SHADOW_OP_CREATE_STB = 1,  // In-place: STB written to SDB with txnId; COMMIT clears markers, ROLLBACK deletes it
  MND_SHADOW_OP_DROP_STB = 2,    // In-place: STB marked PRE_DROP; COMMIT drops it, ROLLBACK unmarks it
  MND_SHADOW_OP_ALTER_STB = 3,   // In-place marker, redo at COMMIT: schema change replayed from pReqData on COMMIT
  MND_SHADOW_OP_CREATE_STREAM =
      4,                          // In-place: stream marked PRE_CREATE; COMMIT clears the marker then async-deploys
                                  // (mndApplyTxnStreamActions, after the commit trans is prepared), ROLLBACK removes it
  MND_SHADOW_OP_DROP_STREAM = 5,  // In-place: stream marked PRE_DROP; undeploy+remove on COMMIT, unmark on ROLLBACK
} EMndShadowOpType;

typedef struct SMndShadowOp {
  int8_t   opType;                      // EMndShadowOpType
  tb_uid_t uid;                         // STB UID (suid); streamId for STREAM op types
  char name[TSDB_TABLE_FNAME_LEN];      // Fully qualified STB name, or stream name for STREAM op types
                                    // (stream name here is the bare name, TSDB_STREAM_NAME_LEN <= TSDB_TABLE_NAME_LEN,
                                    // so the buffer is sufficient; only the STB case holds a qualified name)
  char     db[TSDB_DB_FNAME_LEN];       // DB name
  void*    pReqData;                    // Serialized DDL request; only set for ALTER_STB (NULL otherwise)
  int32_t  reqDataLen;                  // Length of serialized request data
} SMndShadowOp;

// Record a shadow op within the active user txn (in-place + undo-log model).
// The DDL marker is applied to SDB immediately (the object carries txnId/txnStatus
// during the txn); the shadow op records what COMMIT must finalise and ROLLBACK
// must undo. ALTER STB is the exception: its serialized request (pReqData) is
// stored here and replayed at COMMIT. Stream ops register with pReqData = NULL -
// COMMIT/ROLLBACK act on the stream object itself.
int32_t mndTxnAddShadowOp(SMnode* pMnode, txn_id_t txnId, int8_t opType, const char* stbName, tb_uid_t uid,
                          const char* dbName, void* pReqData, int32_t reqDataLen);

// Remove the latest matching op after a statement fails before its internal transaction is proposed.
void mndTxnRemoveShadowOp(SMnode* pMnode, txn_id_t txnId, int8_t opType, const char* name, tb_uid_t uid);

// Check if any active txn (other than callerTxnId) has shadow ops on stbName.
// Returns TSDB_CODE_TXN_RESOURCE_BUSY if conflict found, 0 otherwise.
int32_t mndTxnCheckStbConflict(SMnode* pMnode, const char* stbName, txn_id_t callerTxnId);

// Check pending stream names independently of the STB namespace.
int32_t mndTxnCheckStreamConflict(SMnode* pMnode, const char* name, txn_id_t callerTxnId);

// Persisted in CREATE_STREAM STrans.userData until the output table and stream marker finish.
typedef struct {
  txn_id_t txnId;
  char     name[TSDB_STREAM_FNAME_LEN];
} SMndTxnStreamCreate;

// Return MND_TRANS_CONFLICT while this txn has a pending internal CREATE_STREAM transaction.
int32_t mndTxnCheckPendingStreamCreate(SMnode* pMnode, txn_id_t txnId);

// Merge a single vgId into the txn's participant set, so it receives TDMT_VND_TXN_COMMIT /
// TDMT_VND_TXN_ROLLBACK at the end of the txn. Needed when the mnode issues a vnode DDL on
// the client's behalf: the client's pTxnVgSet only accumulates vgIds it routed itself, and
// mndCollectTxnVgroupIds cannot derive this one from pVgList / shadow ops / pDbList either.
// Without it the vnode never hears the outcome and the shadow entry stays uncommitted forever.
// Idempotent: registering the same vgId twice is a no-op.
int32_t mndTxnAddVgId(SMnode* pMnode, txn_id_t txnId, int32_t vgId);

// Get ALTER STB shadow ops for a specific STB in a given txn.
// Returns 0 on success; *ppOps is an SArray of SMndShadowOp (caller destroys SArray, not contents).
// *ppOps is NULL if no ALTER ops found.
// Resumes deferred shadow recovery; returns MND_TRANS_CONFLICT while internal stream
// creation is pending, or the recovery error. On error *ppOps remains NULL.
// The returned ops are shallow copies of the txn's own records: pReqData remains owned by
// the txn (freed when the txn ends), so callers must NOT free pReqData and must NOT retain
// the ops beyond the current request - they are only valid while the txn is alive.
int32_t mndTxnGetAlterOpsForStb(SMnode* pMnode, txn_id_t txnId, const char* stbFName, SArray** ppOps);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN__H_*/
