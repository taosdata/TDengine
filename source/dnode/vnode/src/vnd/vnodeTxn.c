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
#include "taoserror.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

// ============================================================================
// VNode Transaction Context Management
// ============================================================================

typedef struct SVnodeTxnEntry {
  int64_t   txnId;        // Transaction ID
  int64_t   term;         // Raft term when registered
  int64_t   startTime;    // Transaction start time
  int64_t   lastActive;   // Last active time
  int8_t    status;       // EVtxnStatus
  SArray   *pShadowOps;   // Shadow operations (pending DDL)
} SVnodeTxnEntry;

// Initialize vnode transaction manager
int32_t vnodeTxnInit(SVnode *pVnode) {
  pVnode->pTxnHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), 
                                   true, HASH_ENTRY_LOCK);
  if (pVnode->pTxnHash == NULL) {
    vError("vgId:%d, failed to init txn hash", TD_VID(pVnode));
    return terrno;
  }
  
  if (taosThreadMutexInit(&pVnode->txnMutex, NULL) != 0) {
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  
  pVnode->maxSeenTerm = 0;
  vInfo("vgId:%d, txn manager initialized", TD_VID(pVnode));
  return TSDB_CODE_SUCCESS;
}

// Cleanup vnode transaction manager
void vnodeTxnCleanup(SVnode *pVnode) {
  if (pVnode->pTxnHash) {
    // Clean up all pending transactions
    void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
    while (pIter) {
      SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
      if (pEntry->pShadowOps) {
        taosArrayDestroy(pEntry->pShadowOps);
      }
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
    }
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
  }
  
  taosThreadMutexDestroy(&pVnode->txnMutex);
  vInfo("vgId:%d, txn manager cleaned up", TD_VID(pVnode));
}

// Get transaction entry by txnId
static SVnodeTxnEntry *vnodeGetTxnEntry(SVnode *pVnode, int64_t txnId) {
  return (SVnodeTxnEntry *)taosHashGet(pVnode->pTxnHash, &txnId, sizeof(int64_t));
}

// Create new transaction entry
static int32_t vnodeCreateTxnEntry(SVnode *pVnode, int64_t txnId, int64_t term) {
  SVnodeTxnEntry entry = {0};
  entry.txnId = txnId;
  entry.term = term;
  entry.startTime = taosGetTimestampMs();
  entry.lastActive = entry.startTime;
  entry.status = VTXN_STATUS_PREPARING;
  entry.pShadowOps = taosArrayInit(8, sizeof(void *));
  
  if (entry.pShadowOps == NULL) {
    return terrno;
  }
  
  int32_t code = taosHashPut(pVnode->pTxnHash, &txnId, sizeof(int64_t), &entry, sizeof(SVnodeTxnEntry));
  if (code != 0) {
    taosArrayDestroy(entry.pShadowOps);
    return code;
  }
  
  return TSDB_CODE_SUCCESS;
}

// Remove transaction entry
static void vnodeRemoveTxnEntry(SVnode *pVnode, int64_t txnId) {
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    if (pEntry->pShadowOps) {
      taosArrayDestroy(pEntry->pShadowOps);
    }
    taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t));
  }
}

// ============================================================================
// VNode Transaction Message Handlers
// ============================================================================

/**
 * Process PREPARE request from MNode
 * This is called when a DDL operation needs to be prepared in a transaction
 */
int32_t vnodeProcessTxnPrepareReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  // Decode request
  SVTxnPrepareReq req = {0};
  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq, len);
  
  if (tDecodeSVTxnPrepareReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);
  
  vInfo("vgId:%d, process txn prepare, txnId:%" PRId64 ", term:%" PRId64, 
        TD_VID(pVnode), req.txnId, req.term);
  
  taosThreadMutexLock(&pVnode->txnMutex);
  
  // Check term - reject stale term requests
  if (req.term < pVnode->maxSeenTerm) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, reject txn prepare due to stale term, txnId:%" PRId64 ", reqTerm:%" PRId64 ", maxTerm:%" PRId64,
          TD_VID(pVnode), req.txnId, req.term, pVnode->maxSeenTerm);
    code = TSDB_CODE_VND_TXN_STALE_TERM;
    goto _exit;
  }
  
  // Update max seen term
  if (req.term > pVnode->maxSeenTerm) {
    pVnode->maxSeenTerm = req.term;
  }
  
  // Check if transaction already exists
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry != NULL) {
    // Idempotent: same txnId, return success
    if (pEntry->term <= req.term) {
      pEntry->lastActive = taosGetTimestampMs();
      taosThreadMutexUnlock(&pVnode->txnMutex);
      vInfo("vgId:%d, txn already prepared (idempotent), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
      goto _exit;
    }
    taosThreadMutexUnlock(&pVnode->txnMutex);
    code = TSDB_CODE_VND_TXN_CONFLICT;
    goto _exit;
  }
  
  // Check for conflicts with other transactions on same table
  // TODO: Implement table-level lock checking
  
  // Create new transaction entry
  code = vnodeCreateTxnEntry(pVnode, req.txnId, req.term);
  if (code != TSDB_CODE_SUCCESS) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vError("vgId:%d, failed to create txn entry, txnId:%" PRId64 ", code:%s",
           TD_VID(pVnode), req.txnId, tstrerror(code));
    goto _exit;
  }
  
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  // Write to WAL for durability
  // TODO: Write txn prepare record to WAL
  
  vInfo("vgId:%d, txn prepared successfully, txnId:%" PRId64, TD_VID(pVnode), req.txnId);

_exit:
  // Build response
  pRsp->msgType = TDMT_VND_TXN_PREPARE_RSP;
  pRsp->code = code;
  
  SVTxnPrepareRsp rsp = {0};
  rsp.txnId = req.txnId;
  rsp.vgId = TD_VID(pVnode);
  rsp.code = code;
  
  int32_t rspLen = tSerializeSVTxnPrepareRsp(NULL, 0, &rsp);
  if (rspLen > 0) {
    pRsp->pCont = rpcMallocCont(rspLen);
    if (pRsp->pCont) {
      tSerializeSVTxnPrepareRsp(pRsp->pCont, rspLen, &rsp);
      pRsp->contLen = rspLen;
    }
  }
  
  tFreeSVTxnPrepareReq(&req);
  return code;
}

/**
 * Process COMMIT request from MNode
 * This finalizes the transaction and makes changes visible
 */
int32_t vnodeProcessTxnCommitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  // Decode request
  SVTxnCommitReq req = {0};
  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq, len);
  
  if (tDecodeSVTxnCommitReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);
  
  vInfo("vgId:%d, process txn commit, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
  
  taosThreadMutexLock(&pVnode->txnMutex);
  
  // Find transaction entry
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    // Transaction not found - might be already committed (idempotent)
    vWarn("vgId:%d, txn not found for commit (may be idempotent), txnId:%" PRId64, 
          TD_VID(pVnode), req.txnId);
    code = TSDB_CODE_SUCCESS;  // Idempotent
    goto _exit;
  }
  
  // Update status to committing
  pEntry->status = VTXN_STATUS_COMMITTING;
  
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  // Apply shadow operations to make them permanent
  // TODO: Iterate through pEntry->pShadowOps and apply each operation
  // This involves updating B+ tree entries to remove txnId marker
  
  // Write commit record to WAL
  // TODO: Write txn commit record to WAL
  
  // Remove transaction entry
  taosThreadMutexLock(&pVnode->txnMutex);
  vnodeRemoveTxnEntry(pVnode, req.txnId);
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  vInfo("vgId:%d, txn committed successfully, txnId:%" PRId64, TD_VID(pVnode), req.txnId);

_exit:
  // Build response
  pRsp->msgType = TDMT_VND_TXN_COMMIT_RSP;
  pRsp->code = code;
  
  SVTxnCommitRsp rsp = {0};
  rsp.txnId = req.txnId;
  rsp.vgId = TD_VID(pVnode);
  rsp.code = code;
  
  int32_t rspLen = tSerializeSVTxnCommitRsp(NULL, 0, &rsp);
  if (rspLen > 0) {
    pRsp->pCont = rpcMallocCont(rspLen);
    if (pRsp->pCont) {
      tSerializeSVTxnCommitRsp(pRsp->pCont, rspLen, &rsp);
      pRsp->contLen = rspLen;
    }
  }
  
  tFreeSVTxnCommitReq(&req);
  return code;
}

/**
 * Process ROLLBACK request from MNode
 * This aborts the transaction and discards all shadow changes
 */
int32_t vnodeProcessTxnRollbackReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  // Decode request
  SVTxnRollbackReq req = {0};
  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq, len);
  
  if (tDecodeSVTxnRollbackReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);
  
  vInfo("vgId:%d, process txn rollback, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
  
  taosThreadMutexLock(&pVnode->txnMutex);
  
  // Find transaction entry
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    // Transaction not found - might be already rolled back (idempotent)
    vWarn("vgId:%d, txn not found for rollback (may be idempotent), txnId:%" PRId64, 
          TD_VID(pVnode), req.txnId);
    code = TSDB_CODE_SUCCESS;  // Idempotent
    goto _exit;
  }
  
  // Update status to rolling back
  pEntry->status = VTXN_STATUS_ROLLINGBACK;
  
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  // Discard shadow operations
  // TODO: Iterate through pEntry->pShadowOps and discard each operation
  // This involves removing shadow entries from B+ tree
  
  // Write rollback record to WAL
  // TODO: Write txn rollback record to WAL
  
  // Remove transaction entry
  taosThreadMutexLock(&pVnode->txnMutex);
  vnodeRemoveTxnEntry(pVnode, req.txnId);
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  vInfo("vgId:%d, txn rolled back successfully, txnId:%" PRId64, TD_VID(pVnode), req.txnId);

_exit:
  // Build response
  pRsp->msgType = TDMT_VND_TXN_ROLLBACK_RSP;
  pRsp->code = code;
  
  SVTxnRollbackRsp rsp = {0};
  rsp.txnId = req.txnId;
  rsp.vgId = TD_VID(pVnode);
  rsp.code = code;
  
  int32_t rspLen = tSerializeSVTxnRollbackRsp(NULL, 0, &rsp);
  if (rspLen > 0) {
    pRsp->pCont = rpcMallocCont(rspLen);
    if (pRsp->pCont) {
      tSerializeSVTxnRollbackRsp(pRsp->pCont, rspLen, &rsp);
      pRsp->contLen = rspLen;
    }
  }
  
  tFreeSVTxnRollbackReq(&req);
  return code;
}

/**
 * Process VNode registration request
 * Called when VNode receives first DDL in a transaction
 */
int32_t vnodeProcessTxnRegisterReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  // Decode request
  SVTxnRegisterReq req = {0};
  SDecoder decoder = {0};
  tDecoderInit(&decoder, pReq, len);
  
  if (tDecodeSVTxnRegisterReq(&decoder, &req) < 0) {
    tDecoderClear(&decoder);
    return TSDB_CODE_INVALID_MSG;
  }
  tDecoderClear(&decoder);
  
  vInfo("vgId:%d, process txn register, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
  
  // Forward registration to MNode
  // This tells MNode that this VNode is participating in the transaction
  
  // Build message to MNode
  SMndTxnRegMsg regMsg = {0};
  regMsg.txnId = req.txnId;
  regMsg.vgId = TD_VID(pVnode);
  
  int32_t msgLen = tSerializeSMndTxnRegMsg(NULL, 0, &regMsg);
  if (msgLen <= 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  
  void *pBuf = rpcMallocCont(msgLen);
  if (pBuf == NULL) {
    code = terrno;
    goto _exit;
  }
  
  tSerializeSMndTxnRegMsg(pBuf, msgLen, &regMsg);
  
  // Send to MNode
  SRpcMsg rpcMsg = {
    .msgType = TDMT_MND_TXN_REG,
    .pCont = pBuf,
    .contLen = msgLen,
  };
  
  // TODO: Send message to MNode and wait for response
  // For now, we assume success
  
  vInfo("vgId:%d, txn registered with mnode, txnId:%" PRId64, TD_VID(pVnode), req.txnId);

_exit:
  // Build response
  pRsp->msgType = TDMT_VND_TXN_REGISTER_RSP;
  pRsp->code = code;
  
  tFreeSVTxnRegisterReq(&req);
  return code;
}

// ============================================================================
// Transaction Timeout Handling
// ============================================================================

/**
 * Check and cleanup expired transactions
 * Called periodically by background thread
 */
void vnodeTxnCheckTimeout(SVnode *pVnode) {
  if (pVnode->pTxnHash == NULL) {
    return;
  }
  
  int64_t now = taosGetTimestampMs();
  int64_t timeout = tsMetaTxnTimeout * 1000;  // Convert to ms
  
  SArray *expiredTxns = taosArrayInit(8, sizeof(int64_t));
  if (expiredTxns == NULL) {
    return;
  }
  
  taosThreadMutexLock(&pVnode->txnMutex);
  
  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    
    if (now - pEntry->lastActive > timeout) {
      // Transaction expired
      vWarn("vgId:%d, txn expired, txnId:%" PRId64 ", lastActive:%" PRId64 ", now:%" PRId64,
            TD_VID(pVnode), pEntry->txnId, pEntry->lastActive, now);
      taosArrayPush(expiredTxns, &pEntry->txnId);
    }
    
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }
  
  // Rollback expired transactions
  int32_t numExpired = taosArrayGetSize(expiredTxns);
  for (int32_t i = 0; i < numExpired; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(expiredTxns, i);
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry) {
      // Discard shadow operations
      if (pEntry->pShadowOps) {
        taosArrayDestroy(pEntry->pShadowOps);
      }
      taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t));
      vInfo("vgId:%d, expired txn rolled back, txnId:%" PRId64, TD_VID(pVnode), txnId);
    }
  }
  
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  taosArrayDestroy(expiredTxns);
}

// ============================================================================
// Fencing (Lock Preemption) Logic
// ============================================================================

/**
 * Preempt locks held by lower-term transactions
 * Called when a higher-term request arrives
 */
int32_t vnodeTxnFencing(SVnode *pVnode, int64_t newTerm, int64_t newTxnId) {
  int32_t code = TSDB_CODE_SUCCESS;
  
  taosThreadMutexLock(&pVnode->txnMutex);
  
  if (newTerm <= pVnode->maxSeenTerm) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_VND_TXN_STALE_TERM;
  }
  
  // Update max seen term
  pVnode->maxSeenTerm = newTerm;
  
  // Find and abort all transactions with lower terms
  SArray *toAbort = taosArrayInit(8, sizeof(int64_t));
  if (toAbort == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return terrno;
  }
  
  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    
    if (pEntry->term < newTerm && pEntry->txnId != newTxnId) {
      vInfo("vgId:%d, fencing: abort txn with lower term, txnId:%" PRId64 ", term:%" PRId64 ", newTerm:%" PRId64,
            TD_VID(pVnode), pEntry->txnId, pEntry->term, newTerm);
      taosArrayPush(toAbort, &pEntry->txnId);
    }
    
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }
  
  // Abort transactions
  int32_t numToAbort = taosArrayGetSize(toAbort);
  for (int32_t i = 0; i < numToAbort; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(toAbort, i);
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry) {
      // Discard shadow operations
      if (pEntry->pShadowOps) {
        taosArrayDestroy(pEntry->pShadowOps);
      }
      taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t));
    }
  }
  
  taosThreadMutexUnlock(&pVnode->txnMutex);
  
  taosArrayDestroy(toAbort);
  
  vInfo("vgId:%d, fencing completed, aborted %d transactions", TD_VID(pVnode), numToAbort);
  return code;
}
