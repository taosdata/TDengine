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
//   switchover) requires the new leader to continue pushing the COMMIT — rollback is
//   no longer permitted.
//   The VNode does not need to distinguish between "commit complete" and "rollback
//   complete" — both mean the shadow data has been processed and the transaction
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
// MNode Shadow Operation Types — redo-log for STB DDL within user batch txn
// ============================================================================
//
// DDL Isolation Semantics (redo-log model):
//   - STB DDL within a batch txn is NOT applied to SDB/VNodes immediately.
//   - The full serialized request is stored as a shadow op (pending redo).
//   - On COMMIT: shadow ops are replayed (STB DDL applied to SDB + broadcast).
//   - On ROLLBACK: shadow ops are simply discarded (no SDB changes to undo).
//
// Super table DDL goes through MNode Trans framework, not client→VNode direct path.
// Child/normal table shadow ops are tracked at VNode side (vnodeTxn.c), not here.

typedef enum {
  MND_SHADOW_OP_CREATE_STB = 1,  // Redo: create the super table on COMMIT
  MND_SHADOW_OP_DROP_STB = 2,    // Redo: drop the super table on COMMIT
  MND_SHADOW_OP_ALTER_STB = 3,   // Redo: alter the super table on COMMIT
} EMndShadowOpType;

typedef struct SMndShadowOp {
  int8_t   opType;                      // EMndShadowOpType
  tb_uid_t uid;                         // STB UID (suid)
  char     name[TSDB_TABLE_FNAME_LEN];  // Fully qualified STB name
  char     db[TSDB_DB_FNAME_LEN];       // DB name
  void*    pReqData;                    // Serialized DDL request (for COMMIT replay)
  int32_t  reqDataLen;                  // Length of serialized request data
} SMndShadowOp;

// Record an STB shadow op within the active user txn (redo-log model).
// The DDL request is NOT executed now; it's stored for COMMIT replay.
int32_t mndTxnAddShadowOp(SMnode* pMnode, txn_id_t txnId, int8_t opType, const char* stbName, tb_uid_t uid,
                          const char* dbName, void* pReqData, int32_t reqDataLen);

// Check if any active txn (other than callerTxnId) has shadow ops on stbName.
// Returns TSDB_CODE_TXN_RESOURCE_BUSY if conflict found, 0 otherwise.
int32_t mndTxnCheckStbConflict(SMnode* pMnode, const char* stbName, txn_id_t callerTxnId);

// Get ALTER STB shadow ops for a specific STB in a given txn.
// Returns 0 on success; *ppOps is an SArray of SMndShadowOp (caller destroys SArray, not contents).
// *ppOps is NULL if no ALTER ops found.
int32_t mndTxnGetAlterOpsForStb(SMnode* pMnode, txn_id_t txnId, const char* stbFName, SArray** ppOps);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_USER_TXN__H_*/
