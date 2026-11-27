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

#include "executorInt.h"
#include "query.h"
#include "querytask.h"
#include "taoserror.h"
#include "tdataformat.h"
#include "ttypes.h"

int32_t initQueryTagRefContext(SRefChainCtx* pCtx, const SReadHandle* pHandle, SExecTaskInfo* pTask,
                              int32_t accountId, SEpSet* pEpSet) {
  int32_t code = getCurrentMnodeEpset(pEpSet);
  if (code != TSDB_CODE_SUCCESS) return code;
  *pCtx = (SRefChainCtx){.vnode = pHandle->vnode,
                        .pAPI = &pTask->storageAPI,
                        .accountId = accountId,
                        .txnId = pHandle->txnId,
                        .clientRpc = pTask->pMsgCb != NULL ? pTask->pMsgCb->clientRpc : NULL,
                        .pMnodeEpSet = pEpSet,
                        .reqId = pTask->id.queryId,
                        .pTaskInfo = pTask};
  sysTagsRefCachesInit(&pCtx->pRefDbVgInfoCache, &pCtx->pRefTableCfgCache);
  sysTagsRefValueCacheInit(&pCtx->pQueryTagValueCache);
  return TSDB_CODE_SUCCESS;
}

void clearQueryTagRefContext(SRefChainCtx* pCtx) {
  sysTagsRefCachesClear(&pCtx->pRefDbVgInfoCache, &pCtx->pRefTableCfgCache);
  sysTagsRefValueCacheClear(&pCtx->pQueryTagValueCache);
}

static const SColRef* virtualTagRefById(const SColRefWrapper* pRefs, col_id_t colId) {
  for (int32_t i = 0; i < pRefs->nTagRefs; ++i) {
    if (pRefs->pTagRef[i].id == colId && pRefs->pTagRef[i].hasRef) return &pRefs->pTagRef[i];
  }
  return NULL;
}

static int32_t resolveQueryTagValue(SRefChainCtx* pCtx, const SColRef* pRef, const SColumnInfo* pCol,
                                    STagVal* pValue, bool* pOwned) {
  *pOwned = false;
  char* data = NULL;
  uint32_t len = 0;
  bool resolved = false;
  int32_t code = resolveTagRefChain(pCtx, pRef->refDbName, pRef->refTableName, pRef->refColName,
                                   &data, &len, &resolved);
  if (code != TSDB_CODE_SUCCESS) return code;
  if (!resolved) return TSDB_CODE_SUCCESS;
  pValue->cid = pCol->colId;
  pValue->type = pCol->type;
  if (IS_VAR_DATA_TYPE(pCol->type)) {
    pValue->nData = len;
    pValue->pData = (uint8_t*)data;
    *pOwned = data != NULL;
  } else {
    if (data == NULL || len != tDataTypes[pCol->type].bytes || len > sizeof(pValue->i64)) {
      code = TSDB_CODE_VTABLE_COLUMN_TYPE_MISMATCH;
      pValue->type = TSDB_DATA_TYPE_NULL;
    } else {
      memcpy(&pValue->i64, data, len);
    }
    taosMemoryFree(data);
  }
  return code;
}

static int32_t resolveQueryChildTags(SRefChainCtx* pCtx, STUidTagInfo* pTag, const SArray* pCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SMetaReader mr = {0};
  SArray* pValues = NULL;
  SArray* pBuffers = NULL;
  STag* pNewTag = NULL;
  SStorageAPI* pAPI = pCtx->pAPI;

  pAPI->metaReaderFn.initReader(&mr, pCtx->vnode, META_READER_LOCK, &pAPI->metaFn, pCtx->txnId);
  code = pAPI->metaReaderFn.getTableEntryByUid(&mr, pTag->uid);
  pAPI->metaReaderFn.readerReleaseLock(&mr);
  if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST || code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
    pTag->uid = 0;
    taosMemoryFreeClear(pTag->pTagVal);
    code = TSDB_CODE_SUCCESS;
    goto _end;
  }
  QUERY_CHECK_CODE(code, lino, _end);
  bool hasRefs = false;
  for (int32_t i = 0; i < taosArrayGetSize(pCols); ++i) {
    const SColumnInfo* pCol = taosArrayGet(pCols, i);
    // JSON is a whole tag container, not a CID-addressable value. Keep the
    // original blob, as the vnode resolver does for JSON-tag stables.
    if (pCol->type == TSDB_DATA_TYPE_JSON) goto _end;
    if (virtualTagRefById(&mr.me.colRef, pCol->colId) != NULL) hasRefs = true;
  }
  if (!hasRefs) goto _end;

  pValues = taosArrayInit(taosArrayGetSize(pCols), sizeof(STagVal));
  QUERY_CHECK_NULL(pValues, code, lino, _end, terrno);
  pBuffers = taosArrayInit(taosArrayGetSize(pCols), POINTER_BYTES);
  QUERY_CHECK_NULL(pBuffers, code, lino, _end, terrno);
  for (int32_t i = 0; i < taosArrayGetSize(pCols); ++i) {
    const SColumnInfo* pCol = taosArrayGet(pCols, i);
    if (pCol->colId < 0) continue;
    const SColRef* pRef = virtualTagRefById(&mr.me.colRef, pCol->colId);
    STagVal value = {.cid = pCol->colId};
    if (pRef != NULL) {
      bool owned = false;
      code = resolveQueryTagValue(pCtx, pRef, pCol, &value, &owned);
      QUERY_CHECK_CODE(code, lino, _end);
      if (owned && taosArrayPush(pBuffers, &value.pData) == NULL) {
        taosMemoryFree(value.pData);
        code = terrno;
        goto _end;
      }
      if (value.type == TSDB_DATA_TYPE_NULL) continue;
    } else if (!tTagGet(pTag->pTagVal, &value)) {
      continue;
    }
    QUERY_CHECK_NULL(taosArrayPush(pValues, &value), code, lino, _end, terrno);
  }
  code = tTagNew(pValues, 1, false, &pNewTag);
  QUERY_CHECK_CODE(code, lino, _end);
  taosMemoryFree(pTag->pTagVal);
  pTag->pTagVal = pNewTag;
  pNewTag = NULL;

_end:
  for (int32_t i = 0; i < taosArrayGetSize(pBuffers); ++i) {
    taosMemoryFree(taosArrayGetP(pBuffers, i));
  }
  taosArrayDestroy(pBuffers);
  taosArrayDestroy(pValues);
  taosMemoryFree(pNewTag);
  pAPI->metaReaderFn.clearReader(&mr);
  if (code != TSDB_CODE_SUCCESS) qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t resolveQueryVirtualTags(SRefChainCtx* pCtx, SArray* pTags, const SArray* pCols) {
  for (int32_t i = 0; i < taosArrayGetSize(pTags); ++i) {
    if (isTaskKilled(pCtx->pTaskInfo)) return TSDB_CODE_TSC_QUERY_KILLED;
    int32_t code = resolveQueryChildTags(pCtx, taosArrayGet(pTags, i), pCols);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  return TSDB_CODE_SUCCESS;
}
