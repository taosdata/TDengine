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

#include "executor.h"
#include "executorInt.h"
#include "os.h"
#include "querynodes.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "tname.h"

#include "tdatablock.h"
#include "tdataformat.h"
#include "tmsg.h"
#include "trpc.h"

#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "ttypes.h"

#include "storageapi.h"

/*
 * ============================================================================
 * TagRefSource Operator
 *
 * This operator scans tag values from a source super table for tag references.
 * It is created by the planner when a virtual table references tags from source
 * tables using the REF syntax.
 *
 * Example SQL:
 *   CREATE VTABLE vtbl USING src_stb WITH TAGS (tag1 REF src_stb.region);
 *   SELECT tag1 FROM vtbl;
 *
 * ============================================================================
 */

/**
 * TagRefSource Operator execution information
 */

#define TAG_REF_SOURCE_DEFAULT_CAPACITY 4096
typedef struct STagRefSourceOperatorInfo {
  // Result data block (single-row output per getNext call)
  SSDataBlock* pRes;
  // Pre-scanned all rows (populated in open)
  SSDataBlock* pAllRows;

  // Source table information
  uint64_t      sourceSuid;       // Source super table UID
  SName         sourceTableName;  // Source table name (db.table)
  SNodeList*    pRefCols;         // Referenced tag columns (STagRefColumn)
  SNodeList*    pScanCols;        // Columns to scan from source table
  SVgroupsInfo* pVgroupList;      // Vgroup information (for distributed)

  // Scan state
  int32_t curPos;         // Current row position for getNext iteration
  int32_t totalRows;      // Total rows loaded in open
  bool    scanCompleted;  // Scan completion flag

  // Tag scan related (reuse TagScan logic)
  SReadHandle     readHandle;
  STableListInfo* pTableListInfo;
  SStorageAPI*    pStorageAPI;
  SNode*          pTagCond;       // Tag filter condition
  SNode*          pTagIndexCond;  // Tag index condition
  SColMatchInfo   matchInfo;
  SLimitInfo      limitInfo;

  // Data block capacity
  int32_t capacity;

  // Set when the operator runs without a local vnode (a stream Runner task executes on a
  // snode): there is no storage API, so the source table config is fetched over RPC and
  // the referenced tag values are read from it instead of from a local meta reader.
  bool remoteResolve;
} STagRefSourceOperatorInfo;

// Forward declaration
static int32_t tagRefSourceOpen(SOperatorInfo* pOperator);

/**
 * Scan one table's tags and add to result block
 * Similar to doTagScanOneTable in scanoperator.c
 */
static int32_t tagRefSourceScanOneTable(SOperatorInfo* pOperator, SSDataBlock* pRes, SMetaReader* mr,
                                        int32_t rowIndex) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  SStorageAPI*               pAPI = pInfo->pStorageAPI;

  // Get current table from table list
  STableKeyInfo* pItem = tableListGetInfo(pInfo->pTableListInfo, pInfo->curPos);
  QUERY_CHECK_NULL(pItem, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // Get table entry from vnode
  code = pAPI->metaReaderFn.getTableEntryByUid(mr, pItem->uid);
  tDecoderClear(&(*mr).coder);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s: failed to get table meta for uid:0x%" PRIx64 ", code:%s", __func__, pItem->uid, tstrerror(code));
    goto _end;
  }
  // Iterate through each referenced column and extract tag value
  // Use slotIndex to map to the correct column in result block
  int32_t slotIndex = 0;
  if (pInfo->pRefCols != NULL) {
    SNode* pRefColNode = NULL;
    FOREACH(pRefColNode, pInfo->pRefCols) {
      STagRefColumn* pRefCol = (STagRefColumn*)pRefColNode;

      // Get column info by slot index (columns are in same order as pRefCols)
      SColumnInfoData* pColInfo = NULL;
      if (slotIndex < taosArrayGetSize(pRes->pDataBlock)) {
        pColInfo = taosArrayGet(pRes->pDataBlock, slotIndex);
      }

      if (pColInfo == NULL) {
        qError("%s: failed to get col info for slotIndex:%d", __func__, slotIndex);
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        goto _end;
      }

      // Extract tag value from table entry using source table's colId
      STagVal val = {0};
      val.cid = pRefCol->sourceColId;  // Use source table's tag column ID

      // Find the tag in the child table's tags
      const char* pTagVal = pAPI->metaFn.extractTagVal((*mr).me.ctbEntry.pTags, pColInfo->info.type, &val);
      char* data = NULL;
      bool  isNull = false;

      if (pColInfo->info.type != TSDB_DATA_TYPE_JSON && pTagVal != NULL) {
        data = tTagValToData((const STagVal*)pTagVal, false);
      } else {
        data = (char*)pTagVal;
      }

      if (data == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data))) {
        isNull = true;
      }

      // Set the value in the result block
      code = colDataSetVal(pColInfo, rowIndex, data, isNull);
      if (code != TSDB_CODE_SUCCESS) {
        qWarn("%s: failed to set col value, slotIndex:%d, row:%d, code:%s", __func__, slotIndex, rowIndex,
               tstrerror(code));
        if ((pColInfo->info.type != TSDB_DATA_TYPE_JSON) && (pTagVal != NULL) &&
            IS_VAR_DATA_TYPE(((const STagVal*)pTagVal)->type) && (data != NULL)) {
          taosMemoryFree(data);
        }
        goto _end;
      }

      // Free allocated data for var types
      if ((pColInfo->info.type != TSDB_DATA_TYPE_JSON) && (pTagVal != NULL) &&
          IS_VAR_DATA_TYPE(((const STagVal*)pTagVal)->type) && (data != NULL)) {
        taosMemoryFree(data);
      }

      slotIndex++;
    }
  }

_end:
  if (code == TSDB_CODE_SUCCESS) {
    pRes->info.rows++;
  }

  return code;
}

/**
 * Get next row of tag values from pre-scanned data.
 * Returns exactly 1 row per call (caller expects rows==1).
 */
static int32_t tagRefSourceGetNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = NULL;

  QUERY_CHECK_NULL(pOperator, code, lino, _end, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pOperator->info, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInfo = (STagRefSourceOperatorInfo*)pOperator->info;

  // Auto-open on first getNext call
  if (!OPTR_IS_OPENED(pOperator)) {
    code = tagRefSourceOpen(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pOperator->status == OP_EXEC_DONE || pInfo->curPos >= pInfo->totalRows) {
    *ppRes = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // Copy one row from pAllRows[curPos] into pRes
  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);
  pRes->info.rows = 0;

  int32_t numCols = taosArrayGetSize(pRes->pDataBlock);
  for (int32_t col = 0; col < numCols; col++) {
    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, col);
    SColumnInfoData* pSrc = taosArrayGet(pInfo->pAllRows->pDataBlock, col);
    if (pDst == NULL || pSrc == NULL) continue;

    bool isNull = colDataIsNull_s(pSrc, pInfo->curPos);
    if (isNull) {
      colDataSetNULL(pDst, 0);
    } else {
      char* val = colDataGetData(pSrc, pInfo->curPos);
      code = colDataSetVal(pDst, 0, val, false);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  pRes->info.rows = 1;
  pInfo->curPos++;

  if (pInfo->curPos >= pInfo->totalRows) {
    setOperatorCompleted(pOperator);
  }

  *ppRes = pRes;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
    pOperator->pTaskInfo->code = code;
  }
  return code;
}

/**
 * Destroy TagRefSource operator info
 */
static void destroyTagRefSourceOperatorInfo(void* param) {
  if (NULL == param) {
    return;
  }

  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)param;

  // Free result blocks
  blockDataDestroy(pInfo->pRes);
  blockDataDestroy(pInfo->pAllRows);

  // Free column lists
  nodesDestroyList(pInfo->pRefCols);
  nodesDestroyList(pInfo->pScanCols);

  // Free vgroup list
  taosMemoryFreeClear(pInfo->pVgroupList);

  // Free filter conditions
  nodesDestroyNode(pInfo->pTagCond);
  nodesDestroyNode(pInfo->pTagIndexCond);

  // Free match info
  taosArrayDestroy(pInfo->matchInfo.pList);

  if (pInfo->pTableListInfo != NULL) {
    tableListDestroy(pInfo->pTableListInfo);
  }

  taosMemoryFreeClear(param);
}

// Fill one output row from the source table config fetched over RPC. Mirrors
// tagRefSourceScanOneTable, but reads the tag container from STableCfg instead of a local
// meta reader and needs no storage API.
static int32_t tagRefSourceFillRemoteRow(STagRefSourceOperatorInfo* pInfo, const STableCfgRsp* pCfgRsp) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  int32_t      slot = 0;
  SNode*       pNode = NULL;
  SSDataBlock* pRes = pInfo->pAllRows;

  blockDataCleanup(pRes);
  pRes->info.rows = 0;

  FOREACH(pNode, pInfo->pRefCols) {
    // pRefCols holds STagRefColumn nodes (QUERY_NODE_TAG_REF_COLUMN), exactly like the local path
    // consumes them; casting without a node-type filter is intentional.
    STagRefColumn* pRefCol = (STagRefColumn*)pNode;
    SColumnInfoData* pColInfo =
        slot < taosArrayGetSize(pRes->pDataBlock) ? taosArrayGet(pRes->pDataBlock, slot) : NULL;
    if (pColInfo == NULL) {
      // Same contract as the local path: a slot without a column means the reference list and the
      // output schema disagree, which must surface as an error instead of a half-filled row.
      qError("%s: failed to get col info for slotIndex:%d", __func__, slot);
      lino = __LINE__;
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _end;
    }

    if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
      // A JSON tag value is carried as the table's own STag blob: metaGetTableTagVal() returns the
      // blob itself for JSON columns instead of filling STagVal, and that blob is what the local
      // path appends as well. tTagGet() must NOT be used here -- for a JSON STag the lookup is keyed
      // by pKey (tTagValJsonCmprFn -> strcmp), which an STagVal carrying only .cid does not have, so
      // the search would strcmp() a bogus pointer.
      const char* data = (const char*)pCfgRsp->pTags;
      code = colDataSetVal(pColInfo, 0, data, data == NULL || tTagIsJsonNull((void*)data));
    } else {
      STagVal val = {.cid = pRefCol->sourceColId};
      if (pCfgRsp->pTags == NULL || !tTagGet((const STag*)pCfgRsp->pTags, &val)) {
        colDataSetNULL(pColInfo, 0);
      } else {
        char* data = tTagValToData(&val, false);
        code = colDataSetVal(pColInfo, 0, data, data == NULL);
        if (IS_VAR_DATA_TYPE(val.type)) {
          taosMemoryFree(data);
        }
      }
    }
    QUERY_CHECK_CODE(code, lino, _end);

    ++slot;
  }

  pRes->info.rows = 1;
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

// Bound the wait so a stream Runner task cannot park an snode worker thread on an unreachable
// source vgroup: the local path never talks to the network at all.
#define TAGREF_SOURCE_CFG_RPC_TIMEOUT_MS 3000

// Send one TDMT_VND_TABLE_CFG request to a single source vgroup and hand back the serialized
// response. The request buffer is owned by the RPC request once handed over here (the request
// lifetime frees it), so this function never releases it; the caller owns *pRsp on success.
static int32_t tagRefSourceFetchTableCfg(void* clientRpc, SVgroupInfo* pVgroup, STableCfgReq* pReq,
                                         SRpcMsg* pRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  int32_t contLen = tSerializeSTableCfgReq(NULL, 0, pReq);
  if (contLen <= 0) {
    code = contLen < 0 ? terrno : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    qError("%s: failed to size table cfg request for %s.%s since %s", __func__, pReq->dbFName,
           pReq->tbName, tstrerror(code));
    return code;
  }

  char* buf = rpcMallocCont(contLen);
  if (buf == NULL) {
    code = terrno;
    qError("%s: failed to allocate %d bytes for table cfg request of %s.%s since %s", __func__,
           contLen, pReq->dbFName, pReq->tbName, tstrerror(code));
    return code;
  }
  if (tSerializeSTableCfgReq(buf, contLen, pReq) < 0) {
    rpcFreeCont(buf);
    code = terrno;
    qError("%s: failed to serialize table cfg request of %s.%s since %s", __func__, pReq->dbFName,
           pReq->tbName, tstrerror(code));
    return code;
  }

  // ahandle is an opaque RPC sentinel: a sync request never fires the ahandle callback, but the
  // RPC layer expects a non-NULL pointer together with notFreeAhandle=1. Same convention as the
  // other one-shot table-cfg fetches (virtualtablescanoperator.c, sysscanoperator.c).
  SRpcMsg rpcMsg = {0};
  rpcMsg.msgType = TDMT_VND_TABLE_CFG;
  rpcMsg.pCont = buf;
  rpcMsg.contLen = contLen;
  rpcMsg.info.ahandle = (void*)0x9526;
  rpcMsg.info.notFreeAhandle = 1;  // one-shot request: no link handle is registered for it

  int8_t epUpdated = 0;
  code = rpcSendRecvWithTimeout(clientRpc, &pVgroup->epSet, &rpcMsg, pRsp, &epUpdated,
                                TAGREF_SOURCE_CFG_RPC_TIMEOUT_MS);
  if (code != TSDB_CODE_SUCCESS) {
    qWarn("%s: table cfg request for %s.%s to vgId:%d failed since %s", __func__, pReq->dbFName,
          pReq->tbName, pVgroup->vgId, tstrerror(code));
    goto _end;
  }
  if (pRsp->code == TSDB_CODE_SUCCESS && (pRsp->pCont == NULL || pRsp->contLen <= 0)) {
    code = TSDB_CODE_APP_ERROR;
    goto _end;
  }

  return pRsp->code;

_end:
  return code;
}

// A table belongs to exactly one vgroup, so probing a vgroup that does not own it is worth
// continuing past. Such a probe is NOT answered with table-not-exist: vnodeGetTableCfg() runs
// vnodeValidateTableHash() before any meta lookup and reports TSDB_CODE_VND_HASH_MISMATCH when the
// table name hashes outside the vnode's range, which is exactly the "this vgroup does not own it"
// signal the codebase already groups as NEED_CLIENT_REFRESH_VG_ERROR. Both families must be
// consumed here, otherwise the probe loop stops at the first vgroup that does not own the table.
// Any other failure (unreachable endpoint, RPC timeout) will not be helped by probing the rest.
static bool tagRefSourceVgroupNotOwned(int32_t code) {
  return NEED_CLIENT_REFRESH_VG_ERROR(code) || code == TSDB_CODE_TDB_TABLE_NOT_EXIST ||
         code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_VTABLE_SRC_TABLE_NOT_EXIST;
}

// Resolve the referenced tags when the operator runs WITHOUT a local vnode. A stream Runner
// task executes on a snode, which has no vnode and therefore a zeroed storage API; the plan
// node still carries the source table's vgroup list, so a TDMT_VND_TABLE_CFG RPC returns the
// schema and tag values (no mnode lookup needed).
static int32_t tagRefSourceResolveRemote(SOperatorInfo* pOperator) {
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  STagRefSourceOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  STagRefSourcePhysiNode*    pPhy = (STagRefSourcePhysiNode*)pOperator->pPhyNode;
  STableCfgRsp               cfgRsp = {0};

  if (pPhy == NULL || pPhy->pVgroupList == NULL || pPhy->pVgroupList->numOfVgroups <= 0) {
    qError("%s: no source vgroup for %s.%s", __func__, pInfo->sourceTableName.dbname,
           pInfo->sourceTableName.tname);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  void* clientRpc = pTaskInfo->pMsgCb != NULL ? pTaskInfo->pMsgCb->clientRpc : NULL;
  if (clientRpc == NULL) {
    qError("%s: no client RPC available for %s.%s", __func__, pInfo->sourceTableName.dbname,
           pInfo->sourceTableName.tname);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  STableCfgReq req = {0};
  req.txnId = pTaskInfo->txnId;
  (void)snprintf(req.dbFName, sizeof(req.dbFName), "%d.%s", pInfo->sourceTableName.acctId,
                 pInfo->sourceTableName.dbname);
  tstrncpy(req.tbName, pInfo->sourceTableName.tname, sizeof(req.tbName));

  // The plan carries the vgroup list of the source's whole placement, while the source table
  // itself lives in exactly one of them: probe them in order and keep the first vgroup that
  // returns the table config, instead of assuming vgroups[0] owns it.
  bool    cfgFetched   = false;
  int32_t numOfVgroups = pPhy->pVgroupList->numOfVgroups;
  int32_t lastCode     = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < numOfVgroups; ++i) {
    SVgroupInfo* pVgroup = &pPhy->pVgroupList->vgroups[i];
    req.header.vgId = pVgroup->vgId;

    SRpcMsg rpcRsp = {0};
    code = tagRefSourceFetchTableCfg(clientRpc, pVgroup, &req, &rpcRsp);
    if (code == TSDB_CODE_SUCCESS) {
      code = tDeserializeSTableCfgRsp(rpcRsp.pCont, rpcRsp.contLen, &cfgRsp);
      rpcFreeCont(rpcRsp.pCont);
      QUERY_CHECK_CODE(code, lino, _end);
      cfgFetched = true;
      break;
    }
    if (rpcRsp.pCont != NULL) {
      rpcFreeCont(rpcRsp.pCont);
    }
    lastCode = code;

    if (!tagRefSourceVgroupNotOwned(code)) {
      qError("%s: probing vgId:%d for %s.%s failed since %s", __func__, pVgroup->vgId, req.dbFName,
             req.tbName, tstrerror(code));
      goto _end;
    }
  }

  if (!cfgFetched) {
    // No vgroup served the table. Keep the strongest diagnosis: a vgroup-owner mismatch is
    // actionable (NEED_CLIENT_REFRESH_VG_ERROR makes the client refresh the vgroup mapping and
    // retry), while table-not-exist from every vgroup means the source really is gone. Reporting a
    // blanket table-not-exist here would silently drop the refresh-and-retry signal.
    code = NEED_CLIENT_REFRESH_VG_ERROR(lastCode) ? lastCode : TSDB_CODE_PAR_TABLE_NOT_EXIST;
    qError("%s: none of %d vgroup(s) owns %s.%s, last code:%s", __func__, numOfVgroups, req.dbFName,
           req.tbName, tstrerror(lastCode));
    goto _end;
  }

  code = tagRefSourceFillRemoteRow(pInfo, &cfgRsp);
  QUERY_CHECK_CODE(code, lino, _end);

  qDebug("%s: resolved %d referenced tag(s) of %s.%s over RPC, rows:%" PRId64, __func__,
         (int32_t)(pInfo->pRefCols ? LIST_LENGTH(pInfo->pRefCols) : 0), req.dbFName, req.tbName,
         pInfo->pAllRows->info.rows);

_end:
  tFreeSTableCfgRsp(&cfgRsp);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (pTaskInfo != NULL) {
      pTaskInfo->code = code;
    }
  }
  return code;
}

/**
 * Open TagRefSource operator - batch process all child table tag refs into pAllRows
 */
static int32_t tagRefSourceOpen(SOperatorInfo* pOperator) {
  if (NULL == pOperator || NULL == pOperator->info) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  STagRefSourceOperatorInfo* pInfo = (STagRefSourceOperatorInfo*)pOperator->info;
  SExecTaskInfo*             pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*               pAPI = &pTaskInfo->storageAPI;
  int32_t                    code = TSDB_CODE_SUCCESS;
  int32_t                    lino = 0;
  SMetaReader                mr = {0};

  pInfo->curPos = 0;
  pInfo->totalRows = 0;
  pInfo->scanCompleted = false;

  if (pInfo->remoteResolve) {
    code = tagRefSourceResolveRemote(pOperator);
    if (code != TSDB_CODE_SUCCESS) {
      // This path never initialises a local meta reader (the storage API is zeroed on a snode),
      // so it must not fall through to _end, which clears that reader.
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      pTaskInfo->code = code;
      return code;
    }
    pInfo->totalRows = pInfo->pAllRows->info.rows;
    pInfo->scanCompleted = true;
    OPTR_SET_OPENED(pOperator);
    return code;
  }

  // Get table list size
  int32_t size = 0;
  code = tableListGetSize(pInfo->pTableListInfo, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  if (size == 0) {
    qDebug("%s: empty table list, sourceSuid:%" PRIu64, __func__, pInfo->sourceSuid);
    OPTR_SET_OPENED(pOperator);
    return TSDB_CODE_SUCCESS;
  }

  // Ensure pAllRows has capacity for all tables
  if (size > pInfo->capacity) {
    code = blockDataEnsureCapacity(pInfo->pAllRows, size);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->capacity = size;
  }

  blockDataCleanup(pInfo->pAllRows);
  pInfo->pAllRows->info.rows = 0;

  // Batch scan all child tables at once into pAllRows
  pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn, pInfo->readHandle.txnId);

  // Temporarily swap pRes to pAllRows so tagRefSourceScanOneTable writes there
  SSDataBlock* origRes = pInfo->pRes;
  pInfo->pRes = pInfo->pAllRows;

  for (int32_t i = 0; i < size; ++i) {
    pInfo->curPos = i;
    code = tagRefSourceScanOneTable(pOperator, pInfo->pAllRows, &mr, pInfo->pAllRows->info.rows);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  pInfo->pRes = origRes;
  pAPI->metaReaderFn.clearReader(&mr);

  pInfo->totalRows = pInfo->pAllRows->info.rows;
  pInfo->curPos = 0;
  pInfo->scanCompleted = true;

  OPTR_SET_OPENED(pOperator);

  qDebug("%s: TagRefSource batch scan completed, sourceSuid:%" PRIu64 ", totalRows:%d",
         __func__, pInfo->sourceSuid, pInfo->totalRows);

  return code;

_end:
  // Defence in depth: the remote path never initialises a meta reader, and must never call into one.
  if (!pInfo->remoteResolve && pAPI->metaReaderFn.clearReader != NULL) {
    pAPI->metaReaderFn.clearReader(&mr);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

/**
 * Close TagRefSource operator
 */
/**
 * Create TagRefSource Operator
 *
 * @param pTagRefSourceNode Physical plan node
 * @param pReadHandle Read handle for vnode access
 * @param pTableListInfo Table list info
 * @param pTaskInfo Execution task info
 * @param pOptrInfo Output operator info
 * @return Error code
 */
int32_t createTagRefSourceOperatorInfo(STagRefSourcePhysiNode* pTagRefSourceNode, SReadHandle* pReadHandle,
                                       STableListInfo* pTableListInfo, bool remoteResolve,
                                       SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  STagRefSourceOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STagRefSourceOperatorInfo));
  SOperatorInfo*             pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pTagRefSourceNode;
  SDataBlockDescNode* pDescNode = pTagRefSourceNode->node.pOutputDataBlockDesc;

  // Copy source table information
  pInfo->sourceSuid = pTagRefSourceNode->sourceSuid;
  memcpy(&pInfo->sourceTableName, &pTagRefSourceNode->sourceTableName, sizeof(SName));

  // Clone referenced columns
  if (pTagRefSourceNode->pRefCols) {
    code = nodesCloneList(pTagRefSourceNode->pRefCols, &pInfo->pRefCols);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Clone scan columns
  if (pTagRefSourceNode->pScanCols) {
    code = nodesCloneList(pTagRefSourceNode->pScanCols, &pInfo->pScanCols);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Clone vgroup list if exists
  if (pTagRefSourceNode->pVgroupList) {
    pInfo->pVgroupList =
        taosMemoryMalloc(sizeof(SVgroupsInfo) + pTagRefSourceNode->pVgroupList->numOfVgroups * sizeof(SVgroupInfo));
    QUERY_CHECK_NULL(pInfo->pVgroupList, code, lino, _error, terrno);
    memcpy(pInfo->pVgroupList, pTagRefSourceNode->pVgroupList,
           sizeof(SVgroupsInfo) + pTagRefSourceNode->pVgroupList->numOfVgroups * sizeof(SVgroupInfo));
  }

  // Set storage API
  pInfo->pStorageAPI = &pTaskInfo->storageAPI;

  // Set read handle
  if (pReadHandle) {
    pInfo->readHandle = *pReadHandle;
  }

  // Set table list info
  pInfo->pTableListInfo = pTableListInfo;
  // The caller decides how the referenced tags are resolved: with a local vnode they are scanned
  // through the meta reader, without one (stream Runner task on a snode) they are fetched over RPC.
  pInfo->remoteResolve = remoteResolve;

  // Initialize scan state
  pInfo->curPos = 0;
  pInfo->scanCompleted = false;

  // Create result data block (1-row output buffer)
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pRes, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  // Create pAllRows block (bulk storage, populated in open)
  pInfo->pAllRows = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pAllRows, code, lino, _error, terrno);

  // Initialize limit info
  initLimitInfo(pTagRefSourceNode->node.pLimit, pTagRefSourceNode->node.pSlimit, &pInfo->limitInfo);

  // Set capacity (use fixed default since resultInfo is not yet initialized)
  pInfo->capacity = TAG_REF_SOURCE_DEFAULT_CAPACITY;
  code = blockDataEnsureCapacity(pInfo->pAllRows, pInfo->capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  // Initialize column match info
  if (pTagRefSourceNode->pScanCols) {
    int32_t numOfCols = 0;
    code = extractColMatchInfo(pTagRefSourceNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                               &pInfo->matchInfo);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  // Set operator info
  setOperatorInfo(pOperator, "TagRefSourceOperator", QUERY_NODE_PHYSICAL_PLAN_TAG_REF_SOURCE, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);

  // Set result size
  initResultSizeInfo(&pOperator->resultInfo, pInfo->capacity);

  // Set function pointers
  pOperator->fpSet = createOperatorFpSet(tagRefSourceOpen, tagRefSourceGetNext, NULL, destroyTagRefSourceOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  qDebug("%s: TagRefSource operator created, sourceSuid:%" PRIu64 ", refCols:%d, scanCols:%d", __func__,
         pInfo->sourceSuid, pInfo->pRefCols ? LIST_LENGTH(pInfo->pRefCols) : 0,
         pInfo->pScanCols ? LIST_LENGTH(pInfo->pScanCols) : 0);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    pInfo->pTableListInfo = NULL;  // caller owns pTableListInfo on error
    destroyTagRefSourceOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }

  qError("%s failed at line %d: %s", __func__, lino, tstrerror(code));
  return code;
}
