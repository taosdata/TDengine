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
#include "filter.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tcompare.h"
#include "tmsg.h"
#include "tname.h"
#include "trpc.h"
#include "ttime.h"
#include "virtualtablescan.h"

typedef struct SVirtualTableScanInfo {
  STableScanBase base;
  // TIME_ALIGN post-merge scalar stage (PREV, phase 1)
  SExprSupp      alignSup;
  SNodeList*     pAlignTargets;  // Owns the function nodes borrowed by alignSup.
  char*          pAlignRowBuf;
  uint64_t       alignUid;
  int8_t         alignPhase;
  bool           alignUidValid;
  // NEXT deferred-output host: accumulate aligned rows in pAlignHold until no
  // NEXT column has a pending position, then publish the block.  pAlignStash
  // carries a tag-filled input block across a series-boundary publication.
  SSDataBlock*   pAlignHold;
  SSDataBlock*   pAlignStash;
  uint64_t       alignHoldUid;
  int8_t         alignHoldPhase;
  bool           alignHoldUidValid;
  bool           alignHasNext;
  bool           alignHoldPublished;
  bool           alignHoldUnpublished;  // rows accumulated but not yet returned
  SArray*        pSortInfo;
  SSortHandle*   pSortHandle;
  int32_t        bufPageSize;
  uint64_t       sortBufSize;         // max buffer size for in-memory sort
  SSDataBlock*   pIntermediateBlock;  // to hold the intermediate result
  SSDataBlock*   pInputBlock;
  SSHashObj*     dataSlotMap;
  SSHashObj*     refSlotMap;     // key: slotId, value: SArray<int32_t>*
  SArray*        refSlotGroups;  // SArray<SArray<int32_t>>
  int32_t        tsSlotId;
  int64_t        tagBlockId;
  int32_t        tagDownStreamId;
  bool           scanAllCols;
  bool           useOrgTsCol;
  SArray*        pSortCtxList;
  SArray*        pTagRefSourceBlockIds;
  SSHashObj*     pTagRefDownstreamMap;
  SArray*        pSavedTagRefBlocks;
  SNodeList*     pTargets;
  SNodeList*     pScanPseudoCols;
  SNodeList*     pRefTagCols;
  tb_uid_t       vtableUid;  // virtual table uid, used to identify the vtable scan operator
  char           vtableName[TSDB_TABLE_NAME_LEN];
  SArray*        pOwnedResolvedTags;  // owned-tag values for child scans without a DynQueryCtrl parent
  bool           ownedTagsFetched;    // whether owned tags have been resolved already
  SArray*        pConstRefResolvedTags;  // const-ref tag values (vntb/ntb source) via chain resolver
  bool           constRefTagsFetched;    // whether const-ref tags have been resolved already
  // per-instance RPC result caches for const-ref chain resolution (same pair as the
  // ins_tags operator): every distinct source table costs at most one GET_DB_INFO plus
  // one TABLE_CFG RPC per operator instead of one per tag. Single-thread (open-time)
  // use; created lazily by ensureRefTagsResolved, freed in destroy.
  SHashObj*      pRefDbVgInfoCache;
  SHashObj*      pRefTableCfgCache;
} SVirtualTableScanInfo;

typedef struct SVirtualScanMergeOperatorInfo {
  SOptrBasicInfo        binfo;
  SLimitInfo            limitInfo;
  bool                  limitReached;
  EMergeType            type;
  SVirtualTableScanInfo virtualScanInfo;
  bool                  ignoreGroupId;
  uint64_t              groupId;
  STupleHandle*         pSavedTuple;
  SSDataBlock*          pSavedTagBlock;
  bool                  twoPassMode;   // PERCENTILE/repeat-scan: drive two passes
  bool                  phase1Done;    // twoPassMode: false=phase-1 (PRE_SCAN), true=phase-2 (MAIN_SCAN)
  int32_t               twoPassErr;    // twoPassMode: sticky phase-2 restart failure (0 = none)
} SVirtualScanMergeOperatorInfo;

typedef struct SLoadNextCtx {
  SOperatorInfo*  pOperator;
  SOperatorParam* pOperatorGetParam;
  int64_t         blockId;
  STimeWindow     window;
  SSDataBlock*    pIntermediateBlock;
  int32_t*        pOutputSlotIds;
  int32_t         numOutputSlotIds;
  bool            isFederated;
} SLoadNextCtx;

static void    resetTimeAlignState(SVirtualTableScanInfo* pInfo);
static bool    timeAlignAnyPending(SVirtualTableScanInfo* pInfo);
static int32_t applyTimeAlignNext(SVirtualTableScanInfo* pInfo, SOperatorInfo* pOperator, SSDataBlock* pIn);
static int32_t applyTimeAlign(SVirtualTableScanInfo* pInfo, SOperatorInfo* pOperator, SSDataBlock* pBlock);
static int32_t createTimeAlignExprSupp(SVirtualTableScanInfo* pInfo, SNodeList* pScanCols,
                                       SNodeList* pAlignCols, SNodeList* pOutTargets, int32_t tsSlotId,
                                       const char* tzName, int8_t order, SExecTaskInfo* pTaskInfo);

static SLoadNextCtx* findLoadNextCtx(const SVirtualTableScanInfo* pInfo, int32_t blockId) {
  if (pInfo == NULL || pInfo->pSortCtxList == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); ++i) {
    SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
    if (pCtx != NULL && pCtx->blockId == blockId) {
      return pCtx;
    }
  }
  return NULL;
}

static const SSlotDescNode* findSlotDesc(const SDataBlockDescNode* pDesc, int32_t slotId) {
  if (pDesc == NULL || pDesc->pSlots == NULL) {
    return NULL;
  }

  if (slotId >= 0 && slotId < LIST_LENGTH(pDesc->pSlots)) {
    SSlotDescNode* pSlot = (SSlotDescNode*)nodesListGetNode(pDesc->pSlots, slotId);
    if (pSlot != NULL && pSlot->slotId == slotId) {
      return pSlot;
    }
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pDesc->pSlots) {
    SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
    if (pSlot->slotId == slotId) {
      return pSlot;
    }
  }
  return NULL;
}

static const SDataBlockDescNode* getFederatedDataBlockDesc(const SLoadNextCtx* pCtx) {
  if (pCtx == NULL || !pCtx->isFederated || pCtx->pOperator == NULL) {
    return NULL;
  }

  SFederatedScanOperatorInfo* pFedInfo = (SFederatedScanOperatorInfo*)pCtx->pOperator->info;
  if (pFedInfo == NULL || pFedInfo->pFedScanNode == NULL) {
    return NULL;
  }
  return pFedInfo->pFedScanNode->node.pOutputDataBlockDesc;
}

static bool federatedOutputNameMatchesTarget(const char* pOutputName, const SColumnNode* pCol,
                                             int8_t srcType) {
  if (pOutputName == NULL || pOutputName[0] == '\0' || pCol == NULL) {
    return false;
  }

  // refColName / colName carry the remote catalog spelling, so they follow the
  // source's own matching rule (exact for Oracle/PostgreSQL/InfluxDB).  Aliases
  // are TDengine-local names and keep the historical case-insensitive match.
  const char* pRefName = pCol->hasRef && pCol->refColName[0] != '\0' ? pCol->refColName : pCol->colName;
  return (pRefName[0] != '\0' &&
          extColumnNameEqual((EExtSourceType)srcType, pOutputName, pRefName)) ||
         (pCol->colName[0] != '\0' &&
          extColumnNameEqual((EExtSourceType)srcType, pOutputName, pCol->colName)) ||
         (pCol->node.aliasName[0] != '\0' && strcasecmp(pOutputName, pCol->node.aliasName) == 0) ||
         (pCol->node.userAlias[0] != '\0' && strcasecmp(pOutputName, pCol->node.userAlias) == 0);
}

static const SColumnNode* getFederatedOutputColumn(const SNode* pNode) {
  if (pNode != NULL && QUERY_NODE_TARGET == nodeType(pNode)) {
    pNode = ((const STargetNode*)pNode)->pExpr;
  }
  return pNode != NULL && QUERY_NODE_COLUMN == nodeType(pNode) ? (const SColumnNode*)pNode : NULL;
}

static bool federatedOutputColumnMatchesTarget(const SColumnNode* pOutputCol, const SColumnNode* pTargetCol,
                                               int8_t srcType) {
  if (pOutputCol == NULL || pTargetCol == NULL || pOutputCol->node.resType.type != pTargetCol->node.resType.type) {
    return false;
  }

  return federatedOutputNameMatchesTarget(pOutputCol->refColName, pTargetCol, srcType) ||
         federatedOutputNameMatchesTarget(pOutputCol->colName, pTargetCol, srcType) ||
         federatedOutputNameMatchesTarget(pOutputCol->node.aliasName, pTargetCol, srcType) ||
         federatedOutputNameMatchesTarget(pOutputCol->node.userAlias, pTargetCol, srcType);
}

static int16_t findFederatedOutputSlot(const SFederatedScanPhysiNode* pScan, const SColumnNode* pCol) {
  if (pScan == NULL || pCol == NULL || pScan->node.pOutputDataBlockDesc == NULL ||
      pScan->node.pOutputDataBlockDesc->pSlots == NULL) {
    return -1;
  }

  int32_t numOutputSlots = LIST_LENGTH(pScan->node.pOutputDataBlockDesc->pSlots);
  if (pScan->pColTypeMappings != NULL) {
    SListCell* pSlotCell =
        NULL != pScan->node.pOutputDataBlockDesc->pSlots ? pScan->node.pOutputDataBlockDesc->pSlots->pHead : NULL;
    for (int32_t i = 0; i < pScan->numColTypeMappings && i < numOutputSlots;
         ++i, pSlotCell = (NULL != pSlotCell ? pSlotCell->pNext : NULL)) {
      const SExtColTypeMapping* pMapping = &pScan->pColTypeMappings[i];
      SSlotDescNode* pSlot = (SSlotDescNode*)(NULL != pSlotCell ? pSlotCell->pNode : NULL);
      if (pMapping->tdType.type == pCol->node.resType.type &&
          federatedOutputNameMatchesTarget(pMapping->colName, pCol, pScan->sourceType) && pSlot != NULL &&
          pSlot->output &&
          !pSlot->reserve) {
        return pSlot->slotId;
      }
    }
  }

  if (pScan->pScanCols != NULL) {
    int32_t outputIdx = 0;
    SNode*  pNode = NULL;
    SNodeList* pSlots = pScan->node.pOutputDataBlockDesc->pSlots;
    SListCell* pOutputCell = NULL != pSlots ? pSlots->pHead : NULL;
    FOREACH(pNode, pScan->pScanCols) {
      if (outputIdx >= numOutputSlots) {
        break;
      }

      const SColumnNode* pOutputCol = getFederatedOutputColumn(pNode);
      SSlotDescNode* pSlot =
          NULL != pOutputCell ? (SSlotDescNode*)pOutputCell->pNode : NULL;
      ++outputIdx;
      if (NULL != pOutputCell) {
        pOutputCell = pOutputCell->pNext;
      }
      if (pSlot != NULL && pSlot->output && !pSlot->reserve &&
          federatedOutputColumnMatchesTarget(pOutputCol, pCol, pScan->sourceType)) {
        return pSlot->slotId;
      }
    }
  }
  return -1;
}

static int32_t normalizeFederatedOutputTargets(SNodeList* pTargets, SOperatorInfo** pDownstream,
                                               int32_t numOfDownstream) {
  if (pTargets == NULL || pDownstream == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pTargets) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->isPrimTs || pCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      continue;
    }

    for (int32_t i = 0; i < numOfDownstream; ++i) {
      SOperatorInfo* pChild = pDownstream[i];
      if (pChild == NULL || pChild->operatorType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN ||
          pChild->resultDataBlockId != pCol->dataBlockId) {
        continue;
      }

      SFederatedScanOperatorInfo* pFedInfo = (SFederatedScanOperatorInfo*)pChild->info;
      int16_t outputSlotId =
          pFedInfo != NULL ? findFederatedOutputSlot(pFedInfo->pFedScanNode, pCol) : -1;
      if (outputSlotId < 0) {
        // A virtual table may project a column that this particular external
        // source does not provide. Leave its virtual slot unchanged so the
        // merge path can materialize NULL for that source.
        continue;
      }
      pCol->slotId = outputSlotId;
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

typedef struct {
  int32_t downstreamId;
  int64_t blockId;
} STagRefDownstreamInfo;

typedef struct {
  int64_t      blockId;
  SSDataBlock* pBlock;
} STagRefSavedBlock;

static void destroySlotIdList(void* data) {
  if (data == NULL) {
    return;
  }

  SArray* pSlots = *(SArray**)data;
  taosArrayDestroy(pSlots);
}

static int32_t buildFedOutputSlotIdsFromScanCols(const SFederatedScanPhysiNode* pScan,
                                                 const SVirtualScanPhysiNode* pVtbNode, int32_t tsSlotId,
                                                 int32_t** ppSlotIds, int32_t* pNumSlotIds) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t* pSlotIds = NULL;

  QUERY_CHECK_NULL(pScan, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pVtbNode, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pScan->node.pOutputDataBlockDesc, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppSlotIds, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNumSlotIds, code, lino, _return, TSDB_CODE_INVALID_PARA);

  *ppSlotIds = NULL;
  *pNumSlotIds = 0;
  const SDataBlockDescNode* pDesc = pScan->node.pOutputDataBlockDesc;
  if (pDesc->pSlots == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numSlots = LIST_LENGTH(pDesc->pSlots);
  if (numSlots <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  pSlotIds = taosMemoryCalloc(numSlots, sizeof(int32_t));
  QUERY_CHECK_NULL(pSlotIds, code, lino, _return, terrno);

  SListCell* pSlotCell = NULL != pDesc->pSlots ? pDesc->pSlots->pHead : NULL;
  SListCell* pScanColCell = NULL != pScan->pScanCols ? pScan->pScanCols->pHead : NULL;
  for (int32_t i = 0; i < numSlots;
       ++i, pSlotCell = (NULL != pSlotCell ? pSlotCell->pNext : NULL),
       pScanColCell = (NULL != pScanColCell ? pScanColCell->pNext : NULL)) {
    pSlotIds[i] = -1;
    SSlotDescNode* pSlot = (SSlotDescNode*)(NULL != pSlotCell ? pSlotCell->pNode : NULL);
    if (pSlot == NULL) {
      continue;
    }

    const SColumnNode* pOutputCol =
        pScan->pScanCols != NULL && i < LIST_LENGTH(pScan->pScanCols)
            ? getFederatedOutputColumn((NULL != pScanColCell ? pScanColCell->pNode : NULL))
            : NULL;
    // Only the PRIMARY timestamp column feeds the virtual table's time slot.
    // A plain TIMESTAMP declaration is not enough: a virtual table/stable may
    // carry secondary TIMESTAMP columns, and mapping those to the time slot
    // overwrites ts with the secondary column and leaves its own slot NULL.
    // The type check stays as a fallback for the cases where the producing
    // expression is not a plain column (e.g. TBNAME) and the column identity
    // cannot be recovered.
    if (NULL != pOutputCol) {
      if (pOutputCol->isPrimTs || pOutputCol->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        pSlotIds[i] = tsSlotId;
        continue;
      }
    } else if (TSDB_DATA_TYPE_TIMESTAMP == pSlot->dataType.type) {
      pSlotIds[i] = tsSlotId;
      continue;
    }

    if (pOutputCol == NULL) {
      continue;
    }

    int32_t targetSlot = 0;
    SNode*  pTarget = NULL;
    FOREACH(pTarget, pVtbNode->pTargets) {
      const SColumnNode* pTargetCol = getFederatedOutputColumn(pTarget);
      if (pTargetCol != NULL && pTargetCol->colId == pOutputCol->colId) {
        pSlotIds[i] = targetSlot;
        break;
      }
      ++targetSlot;
    }
  }

  *ppSlotIds = pSlotIds;
  *pNumSlotIds = numSlots;
  return TSDB_CODE_SUCCESS;

_return:
  taosMemoryFree(pSlotIds);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t buildDynamicFedOutputSlotIds(const SVirtualScanPhysiNode* pVtbNode,
                                            const SForeignScanOperatorParam* pFsParam, int32_t tsSlotId,
                                            int32_t** ppSlotIds, int32_t* pNumSlotIds) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t* pSlotIds = NULL;

  QUERY_CHECK_NULL(pVtbNode, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pFsParam, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(ppSlotIds, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pNumSlotIds, code, lino, _return, TSDB_CODE_INVALID_PARA);

  *ppSlotIds = NULL;
  *pNumSlotIds = 0;
  SArray* pColMap = pFsParam->colMap;
  if (pFsParam->paramType == FOREIGN_SCAN_PARAM_BATCH && pFsParam->pBatchTables != NULL &&
      taosArrayGetSize(pFsParam->pBatchTables) > 0) {
    SForeignTableInfo* pTable = taosArrayGet(pFsParam->pBatchTables, 0);
    if (pTable != NULL) {
      pColMap = pTable->colMap;
    }
  }

  int32_t numDataCols = (int32_t)taosArrayGetSize(pColMap);
  int32_t numSlots = numDataCols + 1;
  pSlotIds = taosMemoryMalloc(numSlots * sizeof(int32_t));
  QUERY_CHECK_NULL(pSlotIds, code, lino, _return, terrno);

  for (int32_t i = 0; i < numSlots; ++i) {
    pSlotIds[i] = -1;
  }
  pSlotIds[0] = tsSlotId;

  for (int32_t i = 0; i < numDataCols; ++i) {
    SColIdNameKV* pCol = taosArrayGet(pColMap, i);
    if (pCol == NULL) {
      continue;
    }

    int32_t targetSlot = 0;
    SNode* pTarget = NULL;
    FOREACH(pTarget, pVtbNode->pTargets) {
      if (QUERY_NODE_COLUMN == nodeType(pTarget) && ((SColumnNode*)pTarget)->colId == pCol->colId) {
        pSlotIds[i + 1] = targetSlot;
        break;
      }
      ++targetSlot;
    }
  }

  *ppSlotIds = pSlotIds;
  *pNumSlotIds = numSlots;
  return TSDB_CODE_SUCCESS;

_return:
  taosMemoryFree(pSlotIds);
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

// Classify downstream exchange: tag-ref sources (from pRefTagCols) are NOT sortable,
// everything else is sortable data.  This mirrors main-branch logic (tag vs data)
// extended with one extra check for tag-ref exchanges.
static bool isSortableDataBlockId(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL) {
    return true;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pRefTagCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pCol = (SColumnNode*)pNode;
    if (pCol->dataBlockId == blockId) {
      return false;
    }
  }

  return true;
}

static STagRefSavedBlock* findSavedTagRefBlock(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pSavedTagRefBlocks == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSavedTagRefBlocks); ++i) {
    STagRefSavedBlock* pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, i);
    if (pSaved != NULL && pSaved->blockId == blockId) {
      return pSaved;
    }
  }

  return NULL;
}

static const STagVal* findResolvedTagVal(const SOperatorInfo* pOperator, col_id_t colId) {
  // 1) tags injected by the DynQueryCtrl parent (super-table / stream path)
  if (pOperator != NULL && pOperator->pOperatorGetParam != NULL) {
    const SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
    if (pParam != NULL && pParam->pResolvedTags != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pParam->pResolvedTags); ++i) {
        const STagVal* pTagVal = taosArrayGet(pParam->pResolvedTags, i);
        if (pTagVal != NULL && pTagVal->cid == colId) {
          return pTagVal;
        }
      }
    }
  }
  // 2) owned tags resolved by this operator itself (child scan without DynQueryCtrl)
  if (pOperator != NULL && pOperator->info != NULL) {
    const SVirtualTableScanInfo* pVScan =
        &((const SVirtualScanMergeOperatorInfo*)pOperator->info)->virtualScanInfo;
    if (pVScan->pOwnedResolvedTags != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pVScan->pOwnedResolvedTags); ++i) {
        const STagVal* pTagVal = taosArrayGet(pVScan->pOwnedResolvedTags, i);
        if (pTagVal != NULL && pTagVal->cid == colId) {
          return pTagVal;
        }
      }
    }
    // 3) const-ref tags (vntb/ntb source) resolved by this operator itself via the chain
    //    resolver -- table-level constants, independent of any DynQueryCtrl injection
    if (pVScan->pConstRefResolvedTags != NULL) {
      for (int32_t i = 0; i < taosArrayGetSize(pVScan->pConstRefResolvedTags); ++i) {
        const STagVal* pTagVal = taosArrayGet(pVScan->pConstRefResolvedTags, i);
        if (pTagVal != NULL && pTagVal->cid == colId) {
          return pTagVal;
        }
      }
    }
  }
  return NULL;
}

// Free the resolved-tag arrays (owned + const-ref) including deep-copied var-data
// payloads, plus the per-instance const-ref RPC caches. (Name kept broad: the
// operator teardown path owns all three.)
// pVal NULL-checked for consistency with findResolvedTagVal's read of the same arrays.
static void destroyResolvedTagsAndCaches(SVirtualTableScanInfo* pVScan) {
  if (pVScan->pOwnedResolvedTags != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pVScan->pOwnedResolvedTags); ++i) {
      STagVal* pVal = taosArrayGet(pVScan->pOwnedResolvedTags, i);
      if (pVal != NULL && IS_VAR_DATA_TYPE(pVal->type)) {
        taosMemoryFreeClear(pVal->pData);
      }
    }
    taosArrayDestroy(pVScan->pOwnedResolvedTags);
    pVScan->pOwnedResolvedTags = NULL;
  }
  if (pVScan->pConstRefResolvedTags != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pVScan->pConstRefResolvedTags); ++i) {
      STagVal* pVal = taosArrayGet(pVScan->pConstRefResolvedTags, i);
      if (pVal != NULL && IS_VAR_DATA_TYPE(pVal->type)) {
        taosMemoryFreeClear(pVal->pData);
      }
    }
    taosArrayDestroy(pVScan->pConstRefResolvedTags);
    pVScan->pConstRefResolvedTags = NULL;
  }
  // const-ref RPC caches (created lazily by ensureRefTagsResolved)
  sysTagsRefCachesClear(&pVScan->pRefDbVgInfoCache, &pVScan->pRefTableCfgCache);
}

// For a virtual normal/child-table scan that runs WITHOUT a DynQueryCtrl parent (the static
// createOperator path), pResolvedTags is never injected, so owned-tag values would be NULL.
// Fetch the table config from the owning vnode once and cache the owned-tag values so that
// findResolvedTagVal() can resolve them. Tag-refs are skipped (resolved from source blocks).
static int32_t ensureOwnedTagsResolved(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVScan = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  // Declared upfront: the early error exits below jump to _return, which frees cfgRsp --
  // declaring it after a goto target would leave it uninitialized on those paths.
  STableCfgRsp                   cfgRsp = {0};

  if (pVScan->ownedTagsFetched) {
    return TSDB_CODE_SUCCESS;
  }

  // Only the static path (no DynQueryCtrl param) needs self-resolution.
  if (pOperator->pOperatorGetParam != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  // pTaskInfo/pMsgCb/clientRpc are guaranteed non-NULL by the executor framework for a running
  // scan operator; a NULL here is an internal bug, not a condition to silently degrade from.
  if (pTaskInfo == NULL || pTaskInfo->pSubplan == NULL || pTaskInfo->pMsgCb == NULL ||
      pTaskInfo->pMsgCb->clientRpc == NULL) {
    qError("ensureOwnedTagsResolved: contract violated (pTaskInfo/pMsgCb/clientRpc NULL)");
    return TSDB_CODE_INVALID_PARA;
  }

  SVirtualScanPhysiNode* pPhy = (SVirtualScanPhysiNode*)pOperator->pPhyNode;
  if (pPhy == NULL) {
    qError("ensureOwnedTagsResolved: pPhyNode NULL");
    return TSDB_CODE_INVALID_PARA;
  }
  // No owned local tags: a genuine no-op, not a degrade. Mark fetched anyway for
  // symmetry with ensureRefTagsResolved's no-op path -- a tag-less plan stays tag-less,
  // repeated calls must not re-reach here.
  if (!pPhy->hasLocalTag) {
    pVScan->ownedTagsFetched = true;
    return TSDB_CODE_SUCCESS;
  }
  SQueryNodeAddr* pExecNode = &pTaskInfo->pSubplan->execNode;
  if (pExecNode->nodeId <= 0) {
    qError("ensureOwnedTagsResolved: execNode nodeId<=0");
    return TSDB_CODE_INVALID_PARA;
  }

  STableCfgReq req = {0};
  char         dbFName[TSDB_DB_FNAME_LEN] = {0};
  req.header.vgId = pExecNode->nodeId;
  tNameGetFullDbName(&pPhy->scan.tableName, dbFName);
  tstrncpy(req.dbFName, dbFName, sizeof(req.dbFName));
  tstrncpy(req.tbName, pPhy->scan.tableName.tname, sizeof(req.tbName));
  req.txnId = pTaskInfo->txnId;

  int32_t contLen = tSerializeSTableCfgReq(NULL, 0, &req);
  char*   buf = rpcMallocCont(contLen);
  TSDB_CHECK_NULL(buf, code, lino, _return, terrno);
  if (tSerializeSTableCfgReq(buf, contLen, &req) < 0) {
    rpcFreeCont(buf);
    TSDB_CHECK_CODE(terrno, lino, _return);
  }

  // ahandle is an opaque RPC sentinel; sync rpcSendRecv never uses the ahandle callback, but RPC
  // requires a non-NULL pointer with notFreeAhandle=1. 0x9526 mirrors fetchRemoteTableCfg's
  // convention for sync table-cfg fetches.
  SRpcMsg rpcMsg = {.msgType = TDMT_VND_TABLE_CFG,
                    .pCont = buf,
                    .contLen = contLen,
                    .info.ahandle = (void*)0x9526,
                    .info.notFreeAhandle = 1};
  SRpcMsg rpcRsp = {0};
  code = rpcSendRecv(pTaskInfo->pMsgCb->clientRpc, &pExecNode->epSet, &rpcMsg, &rpcRsp);
  if (code != TSDB_CODE_SUCCESS) {
    qError("ensureOwnedTagsResolved: rpcSendRecv failed for %s.%s, code=%s", dbFName, req.tbName, tstrerror(code));
    return code;
  }
  if (rpcRsp.code != TSDB_CODE_SUCCESS || rpcRsp.pCont == NULL || rpcRsp.contLen <= 0) {
    rpcFreeCont(rpcRsp.pCont);
    return rpcRsp.code != TSDB_CODE_SUCCESS ? rpcRsp.code : TSDB_CODE_FAILED;
  }

  code = tDeserializeSTableCfgRsp(rpcRsp.pCont, rpcRsp.contLen, &cfgRsp);
  rpcFreeCont(rpcRsp.pCont);
  TSDB_CHECK_CODE(code, lino, _return);

  // hasLocalTag can be true for non-tag pseudo-columns (e.g. TBNAME) projected from a tag-less
  // table  -  those are resolved by their own paths, not by this owned-tag fetch. A tag-less cfg
  // therefore means there are simply no owned-tag values to resolve, NOT a meta/cfg mismatch.
  // Erroring here broke `SELECT tbname FROM <vntb>` (and partition-by-tbname) on tag-less virtual
  // normal tables. Succeed with an empty resolved set instead.
  if (cfgRsp.pTags == NULL || cfgRsp.pSchemas == NULL || cfgRsp.numOfTags <= 0) {
    tFreeSTableCfgRsp(&cfgRsp);
    pVScan->ownedTagsFetched = true;  // nothing to fetch; don't retry the RPC on every GetNext
    qDebug("ensureOwnedTagsResolved: %s.%s has no owned tags (pseudo-col/tbname scan)", dbFName, req.tbName);
    return TSDB_CODE_SUCCESS;
  }

  pVScan->pOwnedResolvedTags = taosArrayInit(cfgRsp.numOfTags, sizeof(STagVal));
  TSDB_CHECK_NULL(pVScan->pOwnedResolvedTags, code, lino, _return, terrno);

  for (int32_t tagIdx = 0; tagIdx < cfgRsp.numOfTags; ++tagIdx) {
    SSchema* pSchema = &cfgRsp.pSchemas[cfgRsp.numOfColumns + tagIdx];
    // skip tag-refs: their values come from source data blocks, not this table's cfg
    bool hasRef = false;
    if (cfgRsp.pTagRefs != NULL) {
      for (int32_t r = 0; r < cfgRsp.numOfTagRefs; ++r) {
        if (cfgRsp.pTagRefs[r].hasRef && cfgRsp.pTagRefs[r].id == pSchema->colId) {
          hasRef = true;
          break;
        }
      }
    }
    if (hasRef) {
      continue;
    }
    code = appendResolvedTagVal(pVScan->pOwnedResolvedTags, pSchema->colId, pSchema, (const STag*)cfgRsp.pTags);
    TSDB_CHECK_CODE(code, lino, _return);
  }

  tFreeSTableCfgRsp(&cfgRsp);
  qDebug("ensureOwnedTagsResolved: fetched %d owned tag(s) for %s.%s",
         pVScan->pOwnedResolvedTags ? (int32_t)taosArrayGetSize(pVScan->pOwnedResolvedTags) : 0, dbFName, req.tbName);
  pVScan->ownedTagsFetched = true;  // only mark after a fully successful fetch (transient RPC errors stay retryable)
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  tFreeSTableCfgRsp(&cfgRsp);
  // fetch stays retryable (ownedTagsFetched is only set on full success), so drop any partially
  // filled array here  -  the next attempt must not leak it by overwriting the pointer.
  // destroyResolvedTagsAndCaches also clears const-ref state (array + RPC caches), so reset its
  // fetched flag as well: keeps "flag true <=> array present" true under ANY call order (today
  // owned runs before const-ref in the scan loop, so the const array is always still NULL
  // here  -  the reset is belt-and-braces against a future reorder, not a reachable-bug fix).
  destroyResolvedTagsAndCaches(pVScan);
  pVScan->constRefTagsFetched = false;
  return code;
}

// Resolve const-ref tags (tag-refs whose source is a vntb/ntb -- table-level constants) via the
// unified chain resolver. Unlike owned tags this runs regardless of a DynQueryCtrl parent: the
// value never comes from per-child injection. A broken chain (dropped source) leaves that tag
// unresolved and it projects as NULL; runtime cycles / over-depth are hard errors.
static int32_t ensureRefTagsResolved(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVScan = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  if (pVScan->constRefTagsFetched) {
    return TSDB_CODE_SUCCESS;
  }
  SVirtualScanPhysiNode* pPhy = (SVirtualScanPhysiNode*)pOperator->pPhyNode;
  if (pPhy == NULL) {
    qError("ensureRefTagsResolved: pPhyNode NULL");
    return TSDB_CODE_INVALID_PARA;
  }
  // the LIST is the single decision basis; hasConstRefTag is only a drift sentinel (kept
  // readable so planner/clone/codec bugs surface in the log, but it never steers flow)
  if (pPhy->pConstRefTags == NULL || LIST_LENGTH(pPhy->pConstRefTags) == 0) {
    pVScan->constRefTagsFetched = true;  // genuine no-op
    return TSDB_CODE_SUCCESS;
  }
  if (!pPhy->hasConstRefTag) {
    qError("ensureRefTagsResolved: hasConstRefTag unset but pConstRefTags has %d entries "
           "(plan contract drift, resolving anyway)",
           (int32_t)LIST_LENGTH(pPhy->pConstRefTags));
  }
  if (pTaskInfo == NULL || pTaskInfo->pMsgCb == NULL || pTaskInfo->pMsgCb->clientRpc == NULL) {
    qError("ensureRefTagsResolved: contract violated (pTaskInfo/pMsgCb/clientRpc NULL)");
    return TSDB_CODE_INVALID_PARA;
  }

  SRefChainCtx ctx = {0};
  ctx.vnode = pVScan->base.readHandle.vnode;
  ctx.pAPI = &pTaskInfo->storageAPI;  // task-level API; the scan-local readHandle.api may be unset
  ctx.accountId = pPhy->scan.tableName.acctId;
  ctx.txnId = pTaskInfo->txnId;
  ctx.clientRpc = pTaskInfo->pMsgCb->clientRpc;
  ctx.pMnodeEpSet = &pPhy->mgmtEpSet;
  ctx.reqId = pTaskInfo->id.queryId;
  ctx.pTaskInfo = pTaskInfo;
  // per-instance RPC caches: without them every const-ref tag re-fetches its source
  // chain (2 RPCs per hop per tag); with them every DISTINCT source costs at most one
  // GET_DB_INFO + one TABLE_CFG per operator. Single-threaded here (open-time resolve).
  sysTagsRefCachesInit(&pVScan->pRefDbVgInfoCache, &pVScan->pRefTableCfgCache);
  ctx.pRefDbVgInfoCache = pVScan->pRefDbVgInfoCache;
  ctx.pRefTableCfgCache = pVScan->pRefTableCfgCache;

  SArray* pResolved = taosArrayInit(LIST_LENGTH(pPhy->pConstRefTags), sizeof(STagVal));
  TSDB_CHECK_NULL(pResolved, code, lino, _return, terrno);

  SNode* pNode = NULL;
  FOREACH(pNode, pPhy->pConstRefTags) {
    SNode* pExpr = pNode;
    if (nodeType(pNode) == QUERY_NODE_TARGET) {
      pExpr = ((STargetNode*)pNode)->pExpr;
    }
    if (pExpr == NULL || nodeType(pExpr) != QUERY_NODE_COLUMN) {
      continue;
    }
    SColumnNode* pCol = (SColumnNode*)pExpr;
    if (!pCol->hasRef) {
      continue;
    }

    char*    data = NULL;
    uint32_t len = 0;
    bool     resolved = false;
    code = resolveTagRefChain(&ctx, pCol->refDbName, pCol->refTableName, pCol->refColName,
                              &data, &len, &resolved);
    // CIRCULAR_REF / REF_DEPTH_EXCEEDED propagate. On error the resolver contract
    // guarantees *ppData stays NULL (no partial allocation), so nothing to free here.
    TSDB_CHECK_CODE(code, lino, _return);
    if (!resolved) {
      // broken chain (dropped source / unreachable vnode): project NULL, not fatal --
      // but leave a trace: a silently-NULL const tag is otherwise very hard to diagnose
      qDebug("ensureRefTagsResolved: unresolved const-ref tag %s (ref %s.%s.%s)", pCol->colName,
             pCol->refDbName, pCol->refTableName, pCol->refColName);
      continue;
    }

    STagVal val = {.cid = pCol->colId, .type = pCol->node.resType.type};
    if (IS_VAR_DATA_TYPE(val.type)) {
      val.nData = len;
      // ownership of the resolver-allocated buffer transfers into the resolved array;
      // it is freed exclusively by destroyResolvedTagsAndCaches / the _return cleanup below
      val.pData = (uint8_t*)data;
    } else {
      // Strict width check: the resolver returns source-schema byte widths, which the
      // DDL type-match gate keeps identical to the destination type's storage width. A
      // mismatch (mixed-version window re-pointing a chain at a different-typed source,
      // or an internal contract breach) must fail loudly -- memcpy'ing a wrong-width
      // payload into i64 would silently read the missing/extra bytes as value bits.
      // IS_INVALID_TYPE guards the tDataTypes index itself: val.type comes from the plan's
      // resType (DDL-gated in practice), and an out-of-range value would read past the
      // array. (The else branch already excludes var-len types; taosMemoryFree(NULL) is
      // safe  -  taosMemFree NULL-checks.)
      if (data == NULL || IS_INVALID_TYPE(val.type) || len != tDataTypes[val.type].bytes) {
        if (IS_INVALID_TYPE(val.type)) {
          qError("ensureRefTagsResolved: invalid fixed-width payload for %s, bytes:%u, invalid type:%d",
                 pCol->colName, len, val.type);
        } else if (data == NULL) {
          qError("ensureRefTagsResolved: null fixed-width payload for %s, bytes:%u, type:%d",
                 pCol->colName, len, val.type);
        } else {
          qError("ensureRefTagsResolved: fixed-width payload for %s has %u bytes, type %d needs %d",
                 pCol->colName, len, val.type, tDataTypes[val.type].bytes);
        }
        taosMemoryFree(data);
        TSDB_CHECK_NULL(NULL, code, lino, _return, TSDB_CODE_INVALID_PARA);
      }
      memcpy(&val.i64, data, len);
      taosMemoryFree(data);
    }
    if (NULL == taosArrayPush(pResolved, &val)) {
      // taosArrayPush fails without inserting, so val is NOT in the array: freeing
      // here is safe and the _return cleanup (which walks the array only) will not
      // touch it -- no double free. Fixed-width payloads were already freed above.
      if (IS_VAR_DATA_TYPE(val.type)) {
        taosMemoryFree(val.pData);
      }
      TSDB_CHECK_NULL(NULL, code, lino, _return, terrno);
    }
  }

  pVScan->pConstRefResolvedTags = pResolved;
  pVScan->constRefTagsFetched = true;  // only set on full success; transient errors stay retryable
  qDebug("ensureRefTagsResolved: resolved %d const-ref tag(s) for %s",
         pVScan->pConstRefResolvedTags ? (int32_t)taosArrayGetSize(pVScan->pConstRefResolvedTags) : 0,
         pPhy->scan.tableName.tname);
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  // The RPC caches (created by sysTagsRefCachesInit above) are deliberately NOT cleared on
  // this failure path: constRefTagsFetched stays false, the retry reuses them (Init is
  // idempotent), and destroyResolvedTagsAndCaches at operator destroy owns the final cleanup.
  if (pResolved != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pResolved); ++i) {
      STagVal* pVal = taosArrayGet(pResolved, i);
      if (pVal != NULL && IS_VAR_DATA_TYPE(pVal->type)) {
        taosMemoryFreeClear(pVal->pData);
      }
    }
    taosArrayDestroy(pResolved);
  }
  return code;
}

static int32_t setTagValueToColumn(SColumnInfoData* pDstCol, const STagVal* pTagVal, int32_t rows) {
  if (pTagVal == NULL) {
    colDataSetNNULL(pDstCol, 0, rows);
    return TSDB_CODE_SUCCESS;
  }

  char* data = NULL;
  if (pDstCol->info.type == TSDB_DATA_TYPE_JSON) {
    data = (char*)pTagVal->pData;
  } else if (IS_VAR_DATA_TYPE(pTagVal->type)) {
    data = tTagValToData(pTagVal, false);
  } else {
    data = (char*)&pTagVal->i64;
  }

  bool    isNullVal = (data == NULL) || (pDstCol->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
  int32_t code = TSDB_CODE_SUCCESS;
  if (isNullVal) {
    colDataSetNNULL(pDstCol, 0, rows);
  } else {
    for (int32_t i = 0; i < rows; ++i) {
      code = colDataSetVal(pDstCol, i, data, false);
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
  }

  if (pDstCol->info.type != TSDB_DATA_TYPE_JSON && IS_VAR_DATA_TYPE(pTagVal->type) && data != NULL) {
    taosMemoryFree(data);
  }

  return code;
}

static int32_t setTagColumnValue(SColumnInfoData* pDstCol, SColumnInfoData* pSrcCol, const STagVal* pResolvedTagVal,
                                 int32_t rows) {
  int32_t code = TSDB_CODE_SUCCESS;

  colInfoDataCleanup(pDstCol, rows);
  if (pSrcCol == NULL || colDataIsNull_s(pSrcCol, 0) || IS_JSON_NULL(pSrcCol->info.type, colDataGetData(pSrcCol, 0))) {
    if (pResolvedTagVal != NULL) {
      return setTagValueToColumn(pDstCol, pResolvedTagVal, rows);
    }

    colDataSetNNULL(pDstCol, 0, rows);
    return TSDB_CODE_SUCCESS;
  }

  char* data = colDataGetData(pSrcCol, 0);
  for (int32_t i = 0; i < rows; ++i) {
    code = colDataSetVal(pDstCol, i, data, false);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  return code;
}

static bool isRefTagSourceBlockId(const SVirtualTableScanInfo* pInfo, int64_t blockId) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL) {
    return false;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pRefTagCols) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pColNode = (SColumnNode*)pNode;
    if (pColNode->dataBlockId == blockId) {
      return true;
    }
  }

  return false;
}

int32_t virtualScanloadNextDataBlock(void* param, SSDataBlock** ppBlock) {
  SLoadNextCtx*  pCtx = (SLoadNextCtx*)param;
  SOperatorInfo* pOperator = pCtx->pOperator;
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        line = 0;
  SSDataBlock*   pRes = NULL;

  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }

  VTS_ERR_JRET(pOperator->fpSet.getNextFn(pOperator, &pRes));
  VTS_ERR_JRET(blockDataCheck(pRes));
  if (pRes) {
    VTS_ERR_JRET(createOneDataBlock(pRes, true, &pCtx->pIntermediateBlock));
    SColumnInfoData* p = taosArrayGet(pCtx->pIntermediateBlock->pDataBlock, 0);
    QUERY_CHECK_NULL(p, code, line, _return, terrno);
    if (p->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
      p->hasNull = false;
    }
    *ppBlock = pCtx->pIntermediateBlock;
  } else {
    *ppBlock = NULL;
  }

  return code;
_return:
  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }
  qError("failed to load data block from downstream, %s code:%s, line:%d", __func__, tstrerror(code), line);
  return code;
}

/*
 * Read first/last timestamp from one data block.
 *
 * @param pBlock Input data block.
 * @param startTs Output start timestamp.
 * @param endTs Output end timestamp.
 *
 * @return TSDB_CODE_SUCCESS on success, otherwise error code.
 */
int32_t getTimeWindowOfBlock(SSDataBlock* pBlock, int64_t* startTs, int64_t* endTs) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);

  SColumnInfoData* pColData = NULL;
  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pCandidate = taosArrayGet(pBlock->pDataBlock, i);
    if (pCandidate != NULL && TSDB_DATA_TYPE_TIMESTAMP == pCandidate->info.type) {
      pColData = pCandidate;
      break;
    }
  }

  if (NULL == pColData) {
    *startTs = INT64_MIN;
    *endTs = INT64_MAX;
    return code;
  }

  pColData->hasNull = false;
  GET_TYPED_DATA(*startTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, 0), 0);
  GET_TYPED_DATA(*endTs, int64_t, TSDB_DATA_TYPE_TIMESTAMP, colDataGetNumData(pColData, pBlock->info.rows - 1), 0);

  return code;
_return:
  qError("failed to get time window of block, %s code:%s, line:%d", __func__, tstrerror(code), lino);
  return code;
}

static int32_t moveTimestampColumnToFront(SSDataBlock* pBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  QUERY_CHECK_NULL(pBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);
  QUERY_CHECK_NULL(pBlock->pDataBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);

  int32_t tsIndex = -1;
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (NULL != pCol && TSDB_DATA_TYPE_TIMESTAMP == pCol->info.type) {
      tsIndex = i;
      break;
    }
  }
  if (tsIndex <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData tsCol = *(SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, tsIndex);
  SColumnInfoData* pFirst = taosArrayGet(pBlock->pDataBlock, 0);
  memmove(pFirst + 1, pFirst, (size_t)tsIndex * sizeof(SColumnInfoData));
  *pFirst = tsCol;
  return TSDB_CODE_SUCCESS;

_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

static int32_t virtualScanLoadRawNextDataBlockFromParam(void* param, SSDataBlock** ppBlock) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  QUERY_CHECK_NULL(param, code, lino, _return, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  QUERY_CHECK_NULL(ppBlock, code, lino, _return, TSDB_CODE_INVALID_PARA);
  SLoadNextCtx*           pCtx = (SLoadNextCtx*)param;
  SOperatorInfo*          pOperator = pCtx->pOperator;
  SOperatorParam*         pOperatorGetParam = pCtx->pOperatorGetParam;
  SSDataBlock*            pRes = NULL;

  bool isFederated = pCtx->isFederated;
  if (!isFederated) {
    SExchangeOperatorParam* pParam = (SExchangeOperatorParam*)pOperatorGetParam->value;
    pParam->basic.window = pCtx->window;
  }
  pOperator->status = OP_NOT_OPENED;
  if (pCtx->pIntermediateBlock) {
    blockDataDestroy(pCtx->pIntermediateBlock);
    pCtx->pIntermediateBlock = NULL;
  }

  pOperatorGetParam->reUse = true;
  code = pOperator->fpSet.getNextExtFn(pOperator, pOperatorGetParam, &pRes);
  QUERY_CHECK_CODE(code, lino, _return);

  if (!isFederated) {
    SExchangeOperatorParam* pParam = (SExchangeOperatorParam*)pOperatorGetParam->value;
    pParam->basic.isNewParam = false;
  }
  if ((pRes)) {
    qDebug("%s load from downstream, blockId:%" PRId64, __func__, pCtx->blockId);
    (pRes)->info.id.blockId = pCtx->blockId;

    code = createOneDataBlock(pRes, true, &pCtx->pIntermediateBlock);
    QUERY_CHECK_CODE(code, lino, _return);

    // A local dynamic table scan can place its timestamp after mapped value
    // columns. Its runtime mapping is defined against the canonical
    // [timestamp, colMap...] layout, so normalize the column order first.
    if (!isFederated) {
      code = moveTimestampColumnToFront(pCtx->pIntermediateBlock);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    // Dynamic source scans use their child-plan slot IDs. Restore the virtual
    // row destination slots before merging local and federated source blocks.
    if (pCtx->pOutputSlotIds) {
      int32_t nCols = (int32_t)taosArrayGetSize(pCtx->pIntermediateBlock->pDataBlock);
      for (int32_t ci = 0; ci < nCols; ci++) {
        SColumnInfoData* pCol = taosArrayGet(pCtx->pIntermediateBlock->pDataBlock, ci);
        if (pCol) {
          pCol->info.slotId = ci < pCtx->numOutputSlotIds ? pCtx->pOutputSlotIds[ci] : -1;
        }
      }
    }

    // SORT_MULTISOURCE_TS_MERGE uses one sort-key position for every source.
    // Local table scans naturally return the primary timestamp first, whereas
    // a normalized FederatedScan follows the virtual output-slot order.  Keep
    // the input contract uniform without changing each column's destination
    // slot: timestamp is position 0 in every source block, and slotId still
    // tells VirtualScan where to write the value in the reconstructed row.
    if (isFederated) {
      code = moveTimestampColumnToFront(pCtx->pIntermediateBlock);
      QUERY_CHECK_CODE(code, lino, _return);
    }

    code = getTimeWindowOfBlock(pCtx->pIntermediateBlock, &pCtx->window.skey, &pCtx->window.ekey);
    QUERY_CHECK_CODE(code, lino, _return);
    *ppBlock = pCtx->pIntermediateBlock;
  } else {
    pCtx->window.ekey = INT64_MAX;
    *ppBlock = NULL;
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t makeTSMergeKey(SNodeList** pMergeKeys, col_id_t tsSlotId) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SNodeList*        pNodeList = NULL;
  SColumnNode*      pColumnNode = NULL;
  SOrderByExprNode* pOrderByExprNode = NULL;

  VTS_ERR_JRET(nodesMakeList(&pNodeList));

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pColumnNode));
  pColumnNode->slotId = tsSlotId;

  VTS_ERR_JRET(nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrderByExprNode));
  pOrderByExprNode->pExpr = (SNode*)pColumnNode;
  pOrderByExprNode->order = ORDER_ASC;
  pOrderByExprNode->nullOrder = NULL_ORDER_FIRST;

  VTS_ERR_JRET(nodesListAppend(pNodeList, (SNode*)pOrderByExprNode));

  *pMergeKeys = pNodeList;
  return code;
_return:
  nodesDestroyNode((SNode*)pColumnNode);
  nodesDestroyNode((SNode*)pOrderByExprNode);
  nodesDestroyList(pNodeList);
  return code;
}

static void cleanupSavedTagRefBlocks(SVirtualTableScanInfo* pInfo);

static void cleanupLoadNextCtx(SLoadNextCtx* pCtx) {
  if (pCtx == NULL) {
    return;
  }

  blockDataDestroy(pCtx->pIntermediateBlock);
  pCtx->pIntermediateBlock = NULL;
  taosMemoryFreeClear(pCtx->pOutputSlotIds);
  taosMemoryFree(pCtx);
}

void cleanUpVirtualScanInfo(SVirtualTableScanInfo* pVirtualScanInfo) {
  if (pVirtualScanInfo->pSortInfo) {
    taosArrayDestroy(pVirtualScanInfo->pSortInfo);
    pVirtualScanInfo->pSortInfo = NULL;
  }
  if (pVirtualScanInfo->pSortHandle) {
    tsortDestroySortHandle(pVirtualScanInfo->pSortHandle);
    pVirtualScanInfo->pSortHandle = NULL;
  }
  if (pVirtualScanInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pVirtualScanInfo->pSortCtxList, i);
      cleanupLoadNextCtx(pCtx);
    }
    taosArrayDestroy(pVirtualScanInfo->pSortCtxList);
    pVirtualScanInfo->pSortCtxList = NULL;
  }
  pVirtualScanInfo->vtableName[0] = '\0';

  taosArrayDestroy(pVirtualScanInfo->pTagRefSourceBlockIds);
  pVirtualScanInfo->pTagRefSourceBlockIds = NULL;
  cleanupSavedTagRefBlocks(pVirtualScanInfo);
  tSimpleHashCleanup(pVirtualScanInfo->pTagRefDownstreamMap);
  pVirtualScanInfo->pTagRefDownstreamMap = NULL;
}

static void cleanupRefSlotGroups(SVirtualTableScanInfo* pInfo) {
  if (pInfo->refSlotMap) {
    tSimpleHashCleanup(pInfo->refSlotMap);
    pInfo->refSlotMap = NULL;
  }
  if (pInfo->refSlotGroups) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->refSlotGroups); i++) {
      SArray* slots = *(SArray**)taosArrayGet(pInfo->refSlotGroups, i);
      taosArrayDestroy(slots);
    }
    taosArrayDestroy(pInfo->refSlotGroups);
    pInfo->refSlotGroups = NULL;
  }
}

static void cleanupSavedTagRefBlocks(SVirtualTableScanInfo* pInfo) {
  if (pInfo->pSavedTagRefBlocks == NULL) {
    return;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSavedTagRefBlocks); ++i) {
    STagRefSavedBlock* pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, i);
    if (pSaved != NULL) {
      blockDataDestroy(pSaved->pBlock);
    }
  }

  taosArrayDestroy(pInfo->pSavedTagRefBlocks);
  pInfo->pSavedTagRefBlocks = NULL;
}

static int32_t buildRefSlotGroupsFromParam(SVirtualTableScanInfo* pInfo, SArray* pRefColGroups) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pRefColGroups == NULL || taosArrayGetSize(pRefColGroups) == 0) {
    return code;
  }

  cleanupRefSlotGroups(pInfo);

  pInfo->refSlotGroups = taosArrayInit(taosArrayGetSize(pRefColGroups), POINTER_BYTES);
  QUERY_CHECK_NULL(pInfo->refSlotGroups, code, lino, _return, terrno)
  pInfo->refSlotMap = tSimpleHashInit(taosArrayGetSize(pRefColGroups), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
  QUERY_CHECK_NULL(pInfo->refSlotMap, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pRefColGroups); i++) {
    SRefColIdGroup* pGroup = (SRefColIdGroup*)taosArrayGet(pRefColGroups, i);
    QUERY_CHECK_NULL(pGroup, code, lino, _return, terrno)
    if (pGroup->pSlotIdList == NULL || taosArrayGetSize(pGroup->pSlotIdList) <= 1) {
      continue;
    }

    SArray* pSlots = taosArrayInit(taosArrayGetSize(pGroup->pSlotIdList), sizeof(int32_t));
    QUERY_CHECK_NULL(pSlots, code, lino, _return, terrno)
    for (int32_t j = 0; j < taosArrayGetSize(pGroup->pSlotIdList); j++) {
      int32_t* pSlotId = taosArrayGet(pGroup->pSlotIdList, j);
      if (pSlotId == NULL || *pSlotId < 0) {
        continue;
      }
      int32_t slotId = *pSlotId;
      QUERY_CHECK_NULL(taosArrayPush(pSlots, &slotId), code, lino, _return, terrno)
    }

    if (taosArrayGetSize(pSlots) > 1) {
      taosArraySort(pSlots, compareInt32Val);
      QUERY_CHECK_NULL(taosArrayPush(pInfo->refSlotGroups, &pSlots), code, lino, _return, terrno)
      for (int32_t j = 0; j < taosArrayGetSize(pSlots); j++) {
        int32_t slotId = *(int32_t*)taosArrayGet(pSlots, j);
        VTS_ERR_JRET(tSimpleHashPut(pInfo->refSlotMap, &slotId, sizeof(slotId), &pSlots, POINTER_BYTES));
      }
    } else {
      taosArrayDestroy(pSlots);
    }
  }

  return code;

_return:
  cleanupRefSlotGroups(pInfo);
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t createSortHandleFromParam(SOperatorInfo* pOperator) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SVTableScanOperatorParam*      pParam = (SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  // Count ALL sort sources: internal (pOpParamArray) + external (pForeignParamArray)
  int32_t allSortSources = (int32_t)taosArrayGetSize(pParam->pOpParamArray) +
                            (pParam->pForeignParamArray ? (int32_t)taosArrayGetSize(pParam->pForeignParamArray) : 0);
  pVirtualScanInfo->sortBufSize = pVirtualScanInfo->bufPageSize * (allSortSources + 1);
  int32_t       numOfBufPage = (int32_t)((uint64_t)pVirtualScanInfo->sortBufSize / (uint64_t)pVirtualScanInfo->bufPageSize);
  SNodeList*    pMergeKeys = NULL;
  SSortSource*  ps = NULL;
  int32_t       scanOpIndex = 0;
  SLoadNextCtx* pCtx = NULL;

  // A local LIMIT can leave a lookahead tuple owned by the previous sort handle.
  pInfo->pSavedTuple = NULL;
  cleanUpVirtualScanInfo(pVirtualScanInfo);

  VTS_ERR_JRET(buildRefSlotGroupsFromParam(pVirtualScanInfo, pParam->pRefColGroups));
  VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
  ((SOrderByExprNode*)nodesListGetNode(pMergeKeys, 0))->order =
      pInfo->binfo.outputTsOrder == ORDER_DESC ? ORDER_DESC : ORDER_ASC;
  pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys, false);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  NODES_DESTORY_LIST(pMergeKeys);

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE,
                                     pVirtualScanInfo->bufPageSize, numOfBufPage, pVirtualScanInfo->pInputBlock,
                                     pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle,
                         virtualScanLoadRawNextDataBlockFromParam, NULL, NULL);

  // Find the tag scan downstream by matching resultDataBlockId (most reliable).
  // Don't trust pParam->tagDownStreamId blindly  -  it may be stale when
  // TagRefSource exchanges shift the downstream indices.
  pVirtualScanInfo->tagDownStreamId = -1;
  scanOpIndex = -1;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      pVirtualScanInfo->tagDownStreamId = i;
      blockDataDestroy(pInfo->pSavedTagBlock);
      pInfo->pSavedTagBlock = NULL;
      break;
    }
  }
  if (pVirtualScanInfo->tagDownStreamId == -1 &&
      pParam->tagDownStreamId >= 0 &&
      pParam->tagDownStreamId < pOperator->numOfDownstream) {
    pVirtualScanInfo->tagDownStreamId = pParam->tagDownStreamId;
    blockDataDestroy(pInfo->pSavedTagBlock);
    pInfo->pSavedTagBlock = NULL;
  }

  // Find the data scan downstream (first non-tag-scan, non-TagRefSource downstream)
  scanOpIndex = -1;
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    if (i == pVirtualScanInfo->tagDownStreamId) continue;
    if (isRefTagSourceBlockId(pVirtualScanInfo, pOperator->pDownstream[i]->resultDataBlockId)) continue;
    scanOpIndex = i;
    break;
  }
  if (scanOpIndex < 0) {
    scanOpIndex = 0;
  }

  int32_t totalSources = (int32_t)taosArrayGetSize(pParam->pOpParamArray) +
                          (pParam->pForeignParamArray ? (int32_t)taosArrayGetSize(pParam->pForeignParamArray) : 0);
  pOperator->pDownstream[scanOpIndex]->status = OP_NOT_OPENED;
  pVirtualScanInfo->pSortCtxList = taosArrayInit(totalSources, POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno)

  int64_t blockIdCounter = 0;
  for (size_t i = 0; i < taosArrayGetSize((pParam)->pOpParamArray); i++) {
    SOperatorParam* pOpParam = *(SOperatorParam**)taosArrayGet((pParam)->pOpParamArray, i);

    pCtx = taosMemoryCalloc(1, sizeof(SLoadNextCtx));
    QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno)
    pCtx->blockId = blockIdCounter++;
    pCtx->pOperator = pOperator->pDownstream[scanOpIndex];
    pCtx->pOperatorGetParam = pOpParam;
    pCtx->window = pParam->window;
    pCtx->pIntermediateBlock = NULL;
    pCtx->pOutputSlotIds = NULL;
    pCtx->numOutputSlotIds = 0;
    pCtx->isFederated =
        pOpParam->opType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN ||
        pCtx->pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;
    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    QUERY_CHECK_NULL(ps, code, lino, _return, terrno)

    ps->param = pCtx;
    ps->onlyRef = true;

    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
    pCtx = NULL;
    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    ps = NULL;
  }

  // Dispatch pForeignParamArray to FederatedScan downstream children
  if (pParam->pForeignParamArray != NULL) {
    for (size_t fi = 0; fi < taosArrayGetSize(pParam->pForeignParamArray); fi++) {
      SOperatorParam* pForeignOp = *(SOperatorParam**)taosArrayGet(pParam->pForeignParamArray, fi);
      SForeignScanOperatorParam* pFsParam = (SForeignScanOperatorParam*)pForeignOp->value;

      // Propagate the virtual table's destination ts precision to the
      // federate-scan operator so that timestamps returned by the connector
      // (in the source's native precision) can be converted to the vtable's
      // declared precision before merging.
      if ((pFsParam->dstPrecision < TSDB_TIME_PRECISION_MILLI ||
           pFsParam->dstPrecision > TSDB_TIME_PRECISION_NANO) &&
          pVirtualScanInfo->pInputBlock != NULL &&
          pVirtualScanInfo->tsSlotId >= 0 &&
          pVirtualScanInfo->tsSlotId < taosArrayGetSize(pVirtualScanInfo->pInputBlock->pDataBlock)) {
        SColumnInfoData* pTsCol =
            (SColumnInfoData*)taosArrayGet(pVirtualScanInfo->pInputBlock->pDataBlock,
                                            pVirtualScanInfo->tsSlotId);
        if (pTsCol != NULL) {
          pFsParam->dstPrecision = pTsCol->info.precision;
        }
      }

      // Find the FederatedScan downstream matching this source name
      SOperatorInfo* pFedDownstream = NULL;
      for (int32_t di = 0; di < pOperator->numOfDownstream; di++) {
        SOperatorInfo* pDs = pOperator->pDownstream[di];
        if (pDs->operatorType != QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) continue;
        SFederatedScanOperatorInfo* pFedInfo = (SFederatedScanOperatorInfo*)pDs->info;
        SExtTableNode* pExtTbl = (SExtTableNode*)pFedInfo->pFedScanNode->pExtTable;
        if (pExtTbl && strcmp(pExtTbl->sourceName, pFsParam->sourceName) == 0) {
          pFedDownstream = pDs;
          break;
        }
      }
      if (pFedDownstream == NULL) {
        qError("virtualTableScan: no FederatedScan downstream found for source '%s'", pFsParam->sourceName);
        VTS_ERR_JRET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
      }

      pFedDownstream->status = OP_NOT_OPENED;

      // Flush cached table contexts from previous children so we get fresh queries
      federatedScanResetForNewChild(pFedDownstream);

      pCtx = taosMemoryCalloc(1, sizeof(SLoadNextCtx));
      QUERY_CHECK_NULL(pCtx, code, lino, _return, terrno)
      pCtx->blockId = pFedDownstream->resultDataBlockId;
      pCtx->pOperator = pFedDownstream;
      pCtx->pOperatorGetParam = pForeignOp;
      pCtx->window = pParam->window;
      pCtx->pIntermediateBlock = NULL;

      // Build output slotId mapping for this FederatedScan source.
      // FedScan VStb block columns are: [ts, colMap[0], colMap[1], ...].
      // Map each column's colId to VTableScan output slot position via pTargets.
      pCtx->pOutputSlotIds = NULL;
      pCtx->numOutputSlotIds = 0;
      pCtx->isFederated = true;
      {
        SFederatedScanOperatorInfo* pFedInfo = (SFederatedScanOperatorInfo*)pFedDownstream->info;
        const SDataBlockDescNode* pFedDesc =
            pFedInfo != NULL && pFedInfo->pFedScanNode != NULL
                ? pFedInfo->pFedScanNode->node.pOutputDataBlockDesc
                : NULL;
        if (pFedDesc != NULL && pFedDesc->pSlots != NULL) {
          code = buildFedOutputSlotIdsFromScanCols(pFedInfo->pFedScanNode,
                                                   (SVirtualScanPhysiNode*)pOperator->pPhyNode,
                                                   pVirtualScanInfo->tsSlotId, &pCtx->pOutputSlotIds,
                                                   &pCtx->numOutputSlotIds);
        } else {
          code = buildDynamicFedOutputSlotIds((SVirtualScanPhysiNode*)pOperator->pPhyNode, pFsParam,
                                              pVirtualScanInfo->tsSlotId, &pCtx->pOutputSlotIds,
                                              &pCtx->numOutputSlotIds);
        }
        QUERY_CHECK_CODE(code, lino, _return);
      }

      ps = taosMemoryCalloc(1, sizeof(SSortSource));
      QUERY_CHECK_NULL(ps, code, lino, _return, terrno)
      ps->param = pCtx;
      ps->onlyRef = true;

      QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
      pCtx = NULL;
      VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
      ps = NULL;
    }
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

_return:
  cleanupLoadNextCtx(pCtx);
  taosMemoryFree(ps);
  return code;
}

// Whether an EXCHANGE downstream's output block carries a TIMESTAMP at the merge-key slot --
// every merge-sort source must, since the loser tree compares that column across sources.
// The tag-scan exchange (whose first column is a tag/tbname, never ts) can never be a sort
// source.
//
// EXCHANGE-only: the function casts pDownstream->info to SExchangeInfo* -- callers MUST
// gate on operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE first (the schema at that
// info layout is otherwise meaningless).
//
// The unreachable-schema fallbacks return true ("stays sortable"). That direction is
// deliberate: createExchangeOperatorInfo builds pDummyBlock at operator-creation time and
// fails creation outright when it cannot (exchangeoperator.c), so a live exchange always
// carries a schema -- if these branches ever fire, the layout contract has changed and the
// qError below surfaces it. Defaulting the other way (false = exclude from sort sources)
// would silently drop rows from an ordinary ts exchange, which is strictly worse than the
// loud ASan abort the old bug produced.
static bool exchangeHasTsAtMergeSlot(const SOperatorInfo* pDownstream) {
  if (pDownstream == NULL || pDownstream->info == NULL) {
    qError("exchangeHasTsAtMergeSlot: exchange operator with NULL info -- layout contract changed?");
    return true;
  }

  const SExchangeInfo* pExchangeInfo = (const SExchangeInfo*)pDownstream->info;
  if (pExchangeInfo->pDummyBlock == NULL || taosArrayGetSize(pExchangeInfo->pDummyBlock->pDataBlock) == 0) {
    qError("exchangeHasTsAtMergeSlot: exchange without dummy-block schema -- layout contract changed?");
    return true;
  }

  SColumnInfoData* pCol0 = taosArrayGet(pExchangeInfo->pDummyBlock->pDataBlock, 0);
  return pCol0 != NULL && pCol0->info.type == TSDB_DATA_TYPE_TIMESTAMP;
}

int32_t createSortHandle(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  int32_t      numOfBufPage = (int32_t)(pVirtualScanInfo->sortBufSize / pVirtualScanInfo->bufPageSize);
  SSortSource* ps = NULL;
  SLoadNextCtx* pCtx = NULL;
  int32_t      code = 0;
  int32_t      lino = 0;

  VTS_ERR_JRET(tsortCreateSortHandle(pVirtualScanInfo->pSortInfo, SORT_MULTISOURCE_TS_MERGE,
                                     pVirtualScanInfo->bufPageSize, numOfBufPage, pVirtualScanInfo->pInputBlock,
                                     pTaskInfo->id.str, 0, 0, 0, &pVirtualScanInfo->pSortHandle));

  tsortSetForceUsePQSort(pVirtualScanInfo->pSortHandle);
  tsortSetFetchRawDataFp(pVirtualScanInfo->pSortHandle, virtualScanloadNextDataBlock, NULL, NULL);

  pVirtualScanInfo->pSortCtxList = taosArrayInit(pOperator->numOfDownstream, POINTER_BYTES);
  TSDB_CHECK_NULL(pVirtualScanInfo->pSortCtxList, code, lino, _return, terrno)

  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      VTS_ERR_JRET(pDownstream->fpSet._openFn(pDownstream));
    } else if (pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN) {
      // FederatedScan children open lazily in getNext; skip open here
    } else {
      VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INVALID_DOWNSTREAM);
    }

    if (pDownstream->resultDataBlockId == pVirtualScanInfo->tagBlockId) {
      // tag block do not need sort
      pVirtualScanInfo->tagDownStreamId = i;
      continue;
    }

    // tagBlockId stays -1 when every projected tag column is a const-ref (extractColMap only
    // derives it from plain non-ref tags). The tag-scan downstream then matches nothing and
    // would be added as a merge-sort source, but it carries no ts at the merge-key slot --
    // the loser tree would compare its var-typed first column against data blocks' ts
    // column, and colDataIsNull_var on the non-var ts column reads past the unioned
    // nullbitmap (ASan heap-buffer-overflow in msortComparFn). Exclude it by schema.
    if (pVirtualScanInfo->tagBlockId == -1 && pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE &&
        !isRefTagSourceBlockId(pVirtualScanInfo, pDownstream->resultDataBlockId) &&
        !exchangeHasTsAtMergeSlot(pDownstream)) {
      // excluded from the sort sources, but pseudo columns (pScanPseudoCols, e.g. a
      // tbname filter column planned independently of the projected targets) may still
      // need this tag-scan downstream as their tag source -- register it first, mirroring
      // the tagDownStreamId registration in the !isSortable branch below
      if (pVirtualScanInfo->tagDownStreamId == -1 && pVirtualScanInfo->pScanPseudoCols != NULL) {
        pVirtualScanInfo->tagDownStreamId = i;
      }
      continue;
    }

    if (!isSortableDataBlockId(pVirtualScanInfo, pDownstream->resultDataBlockId)) {
      if (pVirtualScanInfo->tagDownStreamId == -1 && pVirtualScanInfo->pScanPseudoCols != NULL &&
          !isRefTagSourceBlockId(pVirtualScanInfo, pDownstream->resultDataBlockId)) {
        pVirtualScanInfo->tagDownStreamId = i;
        continue;
      }

      if (pVirtualScanInfo->pTagRefSourceBlockIds == NULL) {
        pVirtualScanInfo->pTagRefSourceBlockIds =
            taosArrayInit(pOperator->numOfDownstream, sizeof(int64_t));
        TSDB_CHECK_NULL(pVirtualScanInfo->pTagRefSourceBlockIds, code, lino, _return, terrno)
      }

      if (pVirtualScanInfo->pTagRefDownstreamMap == NULL) {
        pVirtualScanInfo->pTagRefDownstreamMap =
            tSimpleHashInit(pOperator->numOfDownstream, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
        TSDB_CHECK_NULL(pVirtualScanInfo->pTagRefDownstreamMap, code, lino, _return, terrno)
      }

      QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pTagRefSourceBlockIds, &pDownstream->resultDataBlockId), code, lino,
                       _return, terrno)
      STagRefDownstreamInfo downstreamInfo = {.downstreamId = i, .blockId = pDownstream->resultDataBlockId};
      VTS_ERR_JRET(tSimpleHashPut(pVirtualScanInfo->pTagRefDownstreamMap, &downstreamInfo.blockId,
                                  sizeof(downstreamInfo.blockId), &downstreamInfo, sizeof(downstreamInfo)));
      continue;
    }

    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    TSDB_CHECK_NULL(ps, code, lino, _return, terrno)

    pCtx = taosMemoryCalloc(1, sizeof(SLoadNextCtx));
    TSDB_CHECK_NULL(pCtx, code, lino, _return, terrno)
    pCtx->pOperator = pDownstream;
    pCtx->pOperatorGetParam = NULL;
    pCtx->blockId = pDownstream->resultDataBlockId;
    pCtx->window = (STimeWindow){0};
    pCtx->pIntermediateBlock = NULL;
    pCtx->isFederated =
        pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN;

    QUERY_CHECK_NULL(taosArrayPush(pVirtualScanInfo->pSortCtxList, &pCtx), code, lino, _return, terrno)
    pCtx = NULL;
    ps->param = *(SLoadNextCtx**)taosArrayGetLast(pVirtualScanInfo->pSortCtxList);
    ps->onlyRef = true;

    VTS_ERR_JRET(tsortAddSource(pVirtualScanInfo->pSortHandle, ps));
    ps = NULL;
  }

  VTS_ERR_JRET(tsortOpen(pVirtualScanInfo->pSortHandle));

_return:
  if (code != 0) {
    qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  }
  if (pCtx != NULL) {
    taosMemoryFree(pCtx);
  }
  if (ps != NULL) {
    taosMemoryFree(ps);
  }
  return code;
}

int32_t openVirtualTableScanOperatorImpl(SOperatorInfo* pOperator) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pOperator->numOfDownstream == 0) {
    return code;
  }

  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  const SPhysiNode* pNode = pOperator->pPhyNode;
  initLimitInfo(pNode->pLimit, pNode->pSlimit, &pInfo->limitInfo);
  pInfo->limitReached = false;

  if (pOperator->pOperatorGetParam) {
    // The param is referenced by sort source contexts and may still be used by
    // async fetch callbacks when this operator is being torn down.
    pOperator->pOperatorGetParam->reUse = true;
  }

  if (pOperator->pOperatorGetParam) {
    VTS_ERR_JRET(createSortHandleFromParam(pOperator));
  } else {
    VTS_ERR_JRET(createSortHandle(pOperator));
  }

  return code;

_return:
  qError("%s failed at line %d with msg:%s", __func__, lino, tstrerror(code));
  return code;
}

int32_t openVirtualTableScanOperator(SOperatorInfo* pOperator) {
  int32_t code = 0;

  if (OPTR_IS_OPENED(pOperator)) {
    return TSDB_CODE_SUCCESS;
  }

  code = openVirtualTableScanOperatorImpl(pOperator);

  pOperator->status = OP_RES_TO_RETURN;

  VTS_ERR_RET(code);

  OPTR_SET_OPENED(pOperator);
  return code;
}

static int32_t doGetVtableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                          SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = INT64_MIN;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    int32_t blockId = (int32_t)tsortGetBlockId(pTupleHandle);
    int32_t colNum = pInfo->virtualScanInfo.scanAllCols ? 1 : (int32_t)tsortGetColNum(pTupleHandle);
    SLoadNextCtx* pSourceCtx = findLoadNextCtx(&pInfo->virtualScanInfo, blockId);
    bool isFederated = pSourceCtx != NULL && pSourceCtx->isFederated;
    const SDataBlockDescNode* pFedDesc = getFederatedDataBlockDesc(pSourceCtx);
    for (int32_t i = 0; i < colNum; i++) {
      if (isFederated) {
        const SSlotDescNode* pFedSlot = findSlotDesc(pFedDesc, i);
        if (pFedSlot == NULL) {
          qError("failed to find federated slot descriptor, blockId:%d, slotId:%d", blockId, i);
          VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
        }
        if (!pFedSlot->output || pFedSlot->reserve) {
          continue;
        }
      }

      char* pData = NULL;
      tsortGetValue(pTupleHandle, i, (void**)&pData);
      if (pData != NULL) {
        if (i == 0) {
          if (lastTs != *(int64_t*)pData) {
            if (rowNums >= capacity - 1) {
              pInfo->pSavedTuple = pTupleHandle;
              goto _return;
            }
            rowNums++;
            if (pInfo->virtualScanInfo.tsSlotId != -1) {
              VTS_ERR_JRET(
                  colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
            }
            lastTs = *(int64_t*)pData;
          }
          if (pInfo->virtualScanInfo.useOrgTsCol && rowNums >= 0) {
            int64_t slotKey = blockId << 16 | i;
            SArray** ppSlots = tSimpleHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
            if (ppSlots != NULL && *ppSlots != NULL) {
              for (int32_t j = 0; j < taosArrayGetSize(*ppSlots); ++j) {
                int32_t slotId = *(int32_t*)taosArrayGet(*ppSlots, j);
                VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, slotId), rowNums, pData, false));
              }
            }
          }
          continue;
        }
        if (tsortIsNullVal(pTupleHandle, i)) {
          continue;
        }
        int64_t slotKey = blockId << 16 | i;
        SArray** ppSlots = tSimpleHashGet(pInfo->virtualScanInfo.dataSlotMap, &slotKey, sizeof(slotKey));
        if (ppSlots == NULL || *ppSlots == NULL) {
          // A remote plan may expose output columns that this VirtualScan does
          // not reference. Reserve slots have already been excluded above.
          if (isFederated) {
            continue;
          }
          qError("failed to get slotId from dataSlotMap, blockId:%d, slotId:%d", blockId, i);
          VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
        }
        for (int32_t j = 0; j < taosArrayGetSize(*ppSlots); ++j) {
          int32_t slotId = *(int32_t*)taosArrayGet(*ppSlots, j);
          VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, slotId), rowNums, pData, false));
        }
      }
    }
  }
_return:
  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = (pInfo->twoPassMode && !pInfo->phase1Done) ? PRE_SCAN : MAIN_SCAN;
  return code;
}

static int32_t doGetVStableMergedBlockData(SVirtualScanMergeOperatorInfo* pInfo, SSortHandle* pHandle, int32_t capacity,
                                           SSDataBlock* p) {
  int32_t code = 0;
  int64_t lastTs = INT64_MIN;
  int64_t rowNums = -1;
  blockDataEmpty(p);
  for (int32_t j = 0; j < taosArrayGetSize(p->pDataBlock); j++) {
    colDataSetNItemsNull(taosArrayGet(p->pDataBlock, j), 0, capacity);
  }
  while (1) {
    STupleHandle* pTupleHandle = NULL;
    if (!pInfo->pSavedTuple) {
      code = tsortNextTuple(pHandle, &pTupleHandle);
      if (pTupleHandle == NULL || (code != 0)) {
        break;
      }
    } else {
      pTupleHandle = pInfo->pSavedTuple;
      pInfo->pSavedTuple = NULL;
    }

    size_t  colNum = tsortGetColNum(pTupleHandle);
    int32_t tsIndex = -1;
    if (pInfo->virtualScanInfo.tsSlotId >= 0 && pInfo->virtualScanInfo.tsSlotId < colNum) {
      SColumnInfoData* pTsCol = NULL;
      tsortGetColumnInfo(pTupleHandle, pInfo->virtualScanInfo.tsSlotId, &pTsCol);
      if (pTsCol != NULL && TSDB_DATA_TYPE_TIMESTAMP == pTsCol->info.type) {
        tsIndex = pInfo->virtualScanInfo.tsSlotId;
      }
    }
    for (int32_t i = 0; tsIndex < 0 && i < colNum; ++i) {
      SColumnInfoData* pCandidate = NULL;
      tsortGetColumnInfo(pTupleHandle, i, &pCandidate);
      if (pCandidate != NULL && TSDB_DATA_TYPE_TIMESTAMP == pCandidate->info.type) {
        tsIndex = i;
      }
    }
    if (tsIndex < 0) {
      qError("failed to find timestamp column in virtual stable input tuple");
      VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
    }
    int32_t blockId = (int32_t)tsortGetBlockId(pTupleHandle);
    SLoadNextCtx* pSourceCtx = findLoadNextCtx(&pInfo->virtualScanInfo, blockId);
    bool isFederated = pSourceCtx != NULL && pSourceCtx->isFederated;
    const SDataBlockDescNode* pFedDesc = getFederatedDataBlockDesc(pSourceCtx);

    char* pData = NULL;
    // first, set ts slot's data
    // then, set other slots' data
    tsortGetValue(pTupleHandle, tsIndex, (void**)&pData);

    if (pData != NULL) {
      int64_t curTs = *(int64_t*)pData;
      if (lastTs != curTs) {
        if (rowNums >= capacity - 1) {
          pInfo->pSavedTuple = pTupleHandle;
          goto _return;
        }
        rowNums++;
        if (pInfo->virtualScanInfo.tsSlotId != -1) {
          VTS_ERR_JRET(
              colDataSetVal(taosArrayGet(p->pDataBlock, pInfo->virtualScanInfo.tsSlotId), rowNums, pData, false));
        }
        lastTs = curTs;
      }
    }
    if (pInfo->virtualScanInfo.scanAllCols) {
      continue;
    }

    SListCell* pSlotCell = NULL != pFedDesc && NULL != pFedDesc->pSlots ? pFedDesc->pSlots->pHead : NULL;
    for (int32_t i = 0; i < colNum;
         i++, pSlotCell = (NULL != pSlotCell ? pSlotCell->pNext : NULL)) {
      if (i == tsIndex) {
        continue;
      }
      if (tsortIsNullVal(pTupleHandle, i)) {
        continue;
      }

      SColumnInfoData* pColInfo = NULL;
      tsortGetColumnInfo(pTupleHandle, i, &pColInfo);
      if (isFederated && pFedDesc != NULL && pFedDesc->pSlots != NULL && LIST_LENGTH(pFedDesc->pSlots) > 0) {
        const SSlotDescNode* pFedSlot =
            i < LIST_LENGTH(pFedDesc->pSlots)
                ? (const SSlotDescNode*)(NULL != pSlotCell ? pSlotCell->pNode : NULL)
                : NULL;
        if (pFedSlot == NULL) {
          qError("failed to find federated slot descriptor, blockId:%d, column:%d", blockId, i);
          VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
        }
        if (!pFedSlot->output || pFedSlot->reserve) {
          continue;
        }
      }
      tsortGetValue(pTupleHandle, i, (void**)&pData);

      if (pData != NULL) {
        if (pInfo->virtualScanInfo.refSlotMap) {
          SArray** ppSlots =
              tSimpleHashGet(pInfo->virtualScanInfo.refSlotMap, (int32_t*)&pColInfo->info.slotId, sizeof(int32_t));
          if (ppSlots && taosArrayGetSize(*ppSlots) > 1) {
            for (int32_t k = 0; k < taosArrayGetSize(*ppSlots); k++) {
              int32_t slotId = *(int32_t*)taosArrayGet(*ppSlots, k);
              VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, slotId), rowNums, pData, false));
            }
            continue;
          }
        }
        if (pColInfo->info.slotId != -1) {
          VTS_ERR_JRET(colDataSetVal(taosArrayGet(p->pDataBlock, pColInfo->info.slotId), rowNums, pData, false));
        }
      }
    }
  }
_return:
  p->info.rows = rowNums + 1;
  p->info.dataLoad = 1;
  p->info.scanFlag = (pInfo->twoPassMode && !pInfo->phase1Done) ? PRE_SCAN : MAIN_SCAN;
  return code;
}

// Tear down the current sort state (pSortInfo is kept for rebuild). Used by
// vtableTwoPassRestart both before rebuilding and to drop a partially-built phase-2
// sort state when the rebuild fails midway.
static void vtableTwoPassDestroySortState(SVirtualScanMergeOperatorInfo* pInfo) {
  SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;

  if (pVirtualScanInfo->pSortHandle != NULL) {
    tsortDestroySortHandle(pVirtualScanInfo->pSortHandle);
    pVirtualScanInfo->pSortHandle = NULL;
  }
  if (pVirtualScanInfo->pSortCtxList != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pVirtualScanInfo->pSortCtxList, i);
      cleanupLoadNextCtx(pCtx);
    }
    taosArrayDestroy(pVirtualScanInfo->pSortCtxList);
    pVirtualScanInfo->pSortCtxList = NULL;
  }
  blockDataDestroy(pVirtualScanInfo->pIntermediateBlock);
  pVirtualScanInfo->pIntermediateBlock = NULL;
  pInfo->pSavedTuple = NULL;

  // Mirror resetVirtualTableMergeOperState: createSortHandle rebuilds the tag-ref state
  // only when it is NULL, so drop it here -- otherwise a phase-1 -> phase-2 restart
  // appends duplicate resultDataBlockId entries and keeps stale saved tag blocks alive.
  // tagDownStreamId is likewise re-derived by createSortHandle (its -1 fallback
  // classification only fires while still -1), so reset it like the sibling teardowns.
  pVirtualScanInfo->tagDownStreamId = -1;
  blockDataDestroy(pInfo->pSavedTagBlock);
  pInfo->pSavedTagBlock = NULL;
  taosArrayDestroy(pVirtualScanInfo->pTagRefSourceBlockIds);
  pVirtualScanInfo->pTagRefSourceBlockIds = NULL;
  cleanupSavedTagRefBlocks(pVirtualScanInfo);
  tSimpleHashCleanup(pVirtualScanInfo->pTagRefDownstreamMap);
  pVirtualScanInfo->pTagRefDownstreamMap = NULL;
}

// Fetch the 1-row tag block from every ref-tag source downstream and stash copies keyed
// by resultDataBlockId. Shared by the first pass (virtualTableGetNext entry) and the
// two-pass restart: after vtableTwoPassDestroySortState + createSortHandle the block ids
// are new, and phase-2 re-enters via doVirtualTableMerge -- it never passes the
// virtualTableGetNext entry point, so without an explicit rebuild here the saved blocks
// stay NULL and doSetTagColumnData NULL-fills every ref-tag column in MAIN_SCAN.
static int32_t buildSavedTagRefBlocks(SOperatorInfo* pOperator) {
  int32_t                        code = 0;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;

  if (pVirtualScanInfo->pTagRefSourceBlockIds == NULL || pVirtualScanInfo->pSavedTagRefBlocks != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  pVirtualScanInfo->pSavedTagRefBlocks =
      taosArrayInit(taosArrayGetSize(pVirtualScanInfo->pTagRefSourceBlockIds), sizeof(STagRefSavedBlock));
  QUERY_CHECK_NULL(pVirtualScanInfo->pSavedTagRefBlocks, code, lino, _return, terrno)

  for (int32_t i = 0; i < taosArrayGetSize(pVirtualScanInfo->pTagRefSourceBlockIds); ++i) {
    int64_t* pBlockId = taosArrayGet(pVirtualScanInfo->pTagRefSourceBlockIds, i);
    QUERY_CHECK_NULL(pBlockId, code, lino, _return, terrno)
    STagRefDownstreamInfo* pDownstreamInfo =
        tSimpleHashGet(pVirtualScanInfo->pTagRefDownstreamMap, pBlockId, sizeof(*pBlockId));
    QUERY_CHECK_NULL(pDownstreamInfo, code, lino, _return, TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR)

    SSDataBlock*   pTagRefBlock = NULL;
    SSDataBlock*   pSavedBlock = NULL;
    SOperatorInfo* pTagRefOp = pOperator->pDownstream[pDownstreamInfo->downstreamId];
    VTS_ERR_JRET(pTagRefOp->fpSet.getNextFn(pTagRefOp, &pTagRefBlock));
    QUERY_CHECK_NULL(pTagRefBlock, code, lino, _return, TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR)
    if (pTagRefBlock->info.rows != 1) {
      VTS_ERR_JRET(TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR);
    }
    VTS_ERR_JRET(createOneDataBlock(pTagRefBlock, true, &pSavedBlock));

    STagRefSavedBlock savedBlock = {.blockId = *pBlockId, .pBlock = pSavedBlock};
    if (NULL == taosArrayPush(pVirtualScanInfo->pSavedTagRefBlocks, &savedBlock)) {
      blockDataDestroy(pSavedBlock);
      pSavedBlock = NULL;
      code = terrno;
      lino = __LINE__;
      goto _return;
    }
  }

_return:
  return code;
}

// twoPassMode phase-1 EOF: tear down the consumed sort state (keep pSortInfo for rebuild),
// reset the Exchange downstreams so the remote origin subplans re-execute, then rebuild the
// sort handle for phase-2. Mirrors FedScan two-pass restart (federatedscanoperator.c). Only
// used on the static path (no operator param); the DynQueryCtrl path is single-pass.
static int32_t vtableTwoPassRestart(SOperatorInfo* pOperator) {
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  int32_t                        code = 0;

  vtableTwoPassDestroySortState(pInfo);

  // Reset Exchange downstreams so createSortHandle re-opens them and the remote re-scans.
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream != NULL && pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE
        && pDownstream->fpSet.resetStateFn != NULL) {
      code = pDownstream->fpSet.resetStateFn(pDownstream);
      if (code != TSDB_CODE_SUCCESS) {
        // Sticky failure, same contract as the rebuild-failure path below: the sort
        // state is already torn down (pSortHandle NULL), so a doVirtualTableMerge retry
        // must surface this error instead of silently reporting EOF. The phase-2 flags
        // armed on earlier iterations are disarmed by the shared rollback tail.
        pInfo->twoPassErr = code;
        goto _rollback_flags;
      }
      pDownstream->status = OP_NOT_OPENED;
      ((SExchangeInfo*)pDownstream->info)->phase2Rescan = true;    // next fetch = phase-2 rescan
      ((SExchangeInfo*)pDownstream->info)->twoPassExchange = true; // empty !completed rsp = retry
    }
  }

  code = createSortHandle(pOperator);
  if (TSDB_CODE_SUCCESS == code) {
    // Defensive: twoPassMode plans currently have no TAG_REF_SOURCE children (the
    // planner's vtableTwoPassOptimize rejects them -- the remote tag-ref task cannot be
    // kept alive for a phase-2 re-fetch), so pTagRefSourceBlockIds is NULL here and this
    // early-returns. If tag-ref support is added later, this rebuilds the saved ref-tag
    // blocks against the rebuilt downstream map, or phase-2 (re-entered via
    // doVirtualTableMerge, not virtualTableGetNext) NULL-fills the ref-tag columns.
    code = buildSavedTagRefBlocks(pOperator);
  }
  if (code == TSDB_CODE_SUCCESS) {
    return code;
  }

  // Sticky failure: pSortHandle stays NULL, and doVirtualTableMerge must surface this
  // error on retry instead of reporting a silent EOF (empty PERCENTILE result).
  pInfo->twoPassErr = code;
  // createSortHandle may have partially built the new handle/ctx list before failing --
  // free it so the operator is not left in a half-rebuilt state until a later reset.
  vtableTwoPassDestroySortState(pInfo);

_rollback_flags:
  // Both failure paths converge here: disarm the phase-2 flags on every Exchange
  // downstream (the reset loop may already have armed some of them), so a later operator
  // reset/retry observes a consistent phase-1 state (the recursive
  // clearStatesForOperator -> resetExchangeOperState would clear them anyway, but do not
  // rely on that ordering).
  for (int32_t i = 0; i < pOperator->numOfDownstream; ++i) {
    SOperatorInfo* pDownstream = pOperator->pDownstream[i];
    if (pDownstream != NULL && pDownstream->operatorType == QUERY_NODE_PHYSICAL_PLAN_EXCHANGE) {
      ((SExchangeInfo*)pDownstream->info)->phase2Rescan = false;
      ((SExchangeInfo*)pDownstream->info)->twoPassExchange = false;
    }
  }
  return code;
}

int32_t doVirtualTableMerge(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = 0;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SSortHandle*                   pHandle = pVirtualScanInfo->pSortHandle;
  SSDataBlock*                   pDataBlock = pInfo->binfo.pRes;
  int32_t                        capacity = pOperator->resultInfo.capacity;

  if (pInfo->limitInfo.limit.limit >= 0) {
    int64_t remaining = pInfo->limitInfo.limit.limit -
                        pInfo->limitInfo.numOfOutputRows;
    if (remaining > 0 && remaining < capacity) {
      capacity = (int32_t)remaining;
    }
  }

  qDebug("start to merge final sorted rows, %s", GET_TASKID(pTaskInfo));
  blockDataCleanup(pDataBlock);
  // Default to "no output": virtualTableGetNext loops on *pResBlock and can re-enter with a
  // stale non-NULL block (post-filter re-loop), so every EOF return below must leave it NULL.
  *pResBlock = NULL;

  if (pHandle == NULL) {
    // A failed two-pass restart left pSortHandle NULL -- surface the sticky error instead
    // of silently reporting EOF (which would read as an empty PERCENTILE result).
    VTS_ERR_RET(pInfo->twoPassErr);
    return TSDB_CODE_SUCCESS;
  }

_restart_merge:
  if (pVirtualScanInfo->pIntermediateBlock == NULL) {
    VTS_ERR_RET(tsortGetSortedDataBlock(pHandle, &pVirtualScanInfo->pIntermediateBlock));
    if (pVirtualScanInfo->pIntermediateBlock != NULL) {
      VTS_ERR_RET(blockDataEnsureCapacity(pVirtualScanInfo->pIntermediateBlock, capacity));
    }
  } else {
    blockDataCleanup(pVirtualScanInfo->pIntermediateBlock);
  }

  SSDataBlock* p = pVirtualScanInfo->pIntermediateBlock;
  if (p != NULL) {
    if (pOperator->pOperatorGetParam) {
      VTS_ERR_RET(doGetVStableMergedBlockData(pInfo, pHandle, capacity, p));
    } else {
      VTS_ERR_RET(doGetVtableMergedBlockData(pInfo, pHandle, capacity, p));
    }
  }

  // Single pass-EOF detection point: either the sort handed out no block at all (already
  // exhausted on entry, e.g. an empty phase-1), or the merge helper produced zero rows
  // (tsortNextTuple returned NULL inside the helpers -- pIntermediateBlock stays non-NULL
  // between calls, so the NULL check above alone cannot see that case).
  if (p == NULL || p->info.rows == 0) {
    // The planner only sets twoPassMode on static-path scans of virtual child/normal
    // tables (REPEAT_SCAN_FUNC is parser-rejected on virtual super tables), and the
    // DynQueryCtrl param path is single-pass -- so pOperatorGetParam and twoPassMode are
    // mutually exclusive by construction. The NULL check is a defensive guard keeping
    // vtableTwoPassRestart strictly on the static path its comment documents.
    if (pInfo->twoPassMode && !pInfo->phase1Done && NULL == pOperator->pOperatorGetParam) {
      // phase-1 done: reset Exchange + rebuild sort handle, re-merge for phase-2 (MAIN_SCAN)
      VTS_ERR_RET(vtableTwoPassRestart(pOperator));
      pInfo->phase1Done = true;
      pHandle = pVirtualScanInfo->pSortHandle;  // re-read the rebuilt handle
      goto _restart_merge;
    }
    return TSDB_CODE_SUCCESS;  // pass EOF -- *pResBlock stays NULL
  }

  VTS_ERR_RET(copyDataBlock(pDataBlock, p));
  qDebug("%s %s get sorted block, groupId:0x%" PRIx64 " rows:%" PRId64, GET_TASKID(pTaskInfo), __func__,
         pDataBlock->info.id.groupId, pDataBlock->info.rows);

  *pResBlock = (pDataBlock->info.rows > 0) ? pDataBlock : NULL;
  return code;
}

static bool isRefTagOutputSlot(const SVirtualTableScanInfo* pInfo, int32_t dstSlotId) {
  if (pInfo == NULL || pInfo->pTargets == NULL || pInfo->pRefTagCols == NULL) {
    return false;
  }

  int32_t slotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    if (slotId == dstSlotId) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        return false;
      }

      SColumnNode* pCol = (SColumnNode*)pNode;
      if (pCol->colType != COLUMN_TYPE_TAG) {
        return false;
      }

      SNode* pRefNode = NULL;
      FOREACH(pRefNode, pInfo->pRefTagCols) {
        if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
          continue;
        }

        SColumnNode* pRefCol = (SColumnNode*)pRefNode;
        if (0 == strcmp(pRefCol->colName, pCol->colName)) {
          return true;
        }
      }
      return false;
    }

    ++slotId;
  }

  return false;
}

static SColumnNode* findRefTagOutputColumn(const SVirtualTableScanInfo* pInfo, const SColumnNode* pTargetCol) {
  if (pInfo == NULL || pInfo->pRefTagCols == NULL || pTargetCol == NULL) {
    return NULL;
  }

  SNode* pRefNode = NULL;
  // First: exact colName match
  FOREACH(pRefNode, pInfo->pRefTagCols) {
    if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pRefCol = (SColumnNode*)pRefNode;
    if (0 == strcmp(pRefCol->colName, pTargetCol->colName)) {
      return pRefCol;
    }
  }

  return NULL;
}

static void normalizeRefTagTargets(SVirtualTableScanInfo* pInfo) {
  if (pInfo == NULL || pInfo->pTargets == NULL || pInfo->pRefTagCols == NULL) {
    return;
  }

  SNode* pNode = NULL;
  FOREACH(pNode, pInfo->pTargets) {
    if (nodeType(pNode) != QUERY_NODE_COLUMN) {
      continue;
    }

    SColumnNode* pTargetCol = (SColumnNode*)pNode;
    SColumnNode* pRefCol = findRefTagOutputColumn(pInfo, pTargetCol);
    if (pRefCol == NULL || pTargetCol->hasRef) {
      continue;
    }

    pTargetCol->hasRef = true;
    pTargetCol->colType = pRefCol->colType;
    pTargetCol->dataBlockId = pRefCol->dataBlockId;
    pTargetCol->slotId = pRefCol->slotId;
    pTargetCol->colId = pRefCol->colId;
    tstrncpy(pTargetCol->refDbName, pRefCol->refDbName, sizeof(pTargetCol->refDbName));
    tstrncpy(pTargetCol->refTableName, pRefCol->refTableName, sizeof(pTargetCol->refTableName));
    tstrncpy(pTargetCol->refColName, pRefCol->refColName, sizeof(pTargetCol->refColName));
  }
}

int32_t vtableAddTagPseudoColumnData(const SOperatorInfo* pOperator, const SExprInfo* pExpr, int32_t numOfExpr,
                                     SSDataBlock* tagBlock, SSDataBlock* pBlock, int32_t rows,
                                     const SVirtualTableScanInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t backupRows;
  const char* tbName = NULL;
  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOperator != NULL && pOperator->pOperatorGetParam != NULL) {
    SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
    if (pParam != NULL && pParam->tbName[0] != '\0') {
      tbName = pParam->tbName;
    }
  }
  if (tbName == NULL && pInfo != NULL && pInfo->vtableName[0] != '\0') {
    tbName = pInfo->vtableName;
  }

  backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;
  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    int32_t functionId = pExpr1->pExpr->_function.functionId;
    if (!fmIsScanPseudoColumnFunc(functionId)) {
      continue;
    }

    if (isRefTagOutputSlot(pInfo, dstSlotId)) {
      continue;
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    TSDB_CHECK_NULL(pColInfoData, code, lino, _return, terrno)
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    int32_t functionType = pExpr1->pExpr->_function.functionType;
    if (fmIsScanPseudoColumnFunc(functionId) && functionType == FUNCTION_TYPE_TBNAME && tbName != NULL) {
      char tbNameBuf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(tbNameBuf, tbName);
      code = colDataSetNItems(pColInfoData, 0, tbNameBuf, pBlock->info.rows, 1, true);
      QUERY_CHECK_CODE(code, lino, _return);
    } else {
      colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;

_return:

  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doSetLocalTagPseudoColumnData(SOperatorInfo* pOperator, const SVirtualTableScanInfo* pInfo,
                                             SSDataBlock* pTagBlock, SSDataBlock* pBlock, int32_t rows) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfTagCols = pTagBlock == NULL ? 0 : taosArrayGetSize(pTagBlock->pDataBlock);

  if (pInfo == NULL || pInfo->pScanPseudoCols == NULL) {
    return code;
  }

  int32_t srcSlotId = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pInfo->pScanPseudoCols) {
    SNode*  pExpr = pNode;
    int16_t dstSlotId = srcSlotId;
    if (nodeType(pNode) == QUERY_NODE_TARGET) {
      STargetNode* pTarget = (STargetNode*)pNode;
      pExpr = pTarget->pExpr;
      dstSlotId = pTarget->slotId;
    }

    if (pExpr != NULL && nodeType(pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pExpr;
      if (pColNode->colType == COLUMN_TYPE_TAG) {
        const STagVal* pResolvedTagVal = findResolvedTagVal(pOperator, pColNode->colId);
        // const-ref tags (hasRef with a resolved table-level constant) have neither a
        // TagRefSource block nor a tag-block column -- fill from the resolved constant.
        // Plain tag-refs (no resolved value) are NOT filled here on purpose: their output
        // slots are covered by the second loop in doSetTagColumnData (pTargets +
        // findRefTagOutputColumn). Changing what either loop covers must preserve that
        // division or those slots would be left uninitialized.
        if (!pColNode->hasRef || pResolvedTagVal != NULL) {
          SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
          SColumnInfoData* pSrcCol = NULL;
          TSDB_CHECK_NULL(pDstCol, code, lino, _return, terrno)

          if (!pColNode->hasRef && pTagBlock != NULL && srcSlotId >= 0 && srcSlotId < numOfTagCols) {
            pSrcCol = taosArrayGet(pTagBlock->pDataBlock, srcSlotId);
          }

          code = setTagColumnValue(pDstCol, pSrcCol, pResolvedTagVal, rows);
          QUERY_CHECK_CODE(code, lino, _return);
        }
      }
    }

    ++srcSlotId;
  }

_return:
  return code;
}

static int32_t doSetTagColumnData(SOperatorInfo* pOperator, SVirtualTableScanInfo* pInfo, SSDataBlock* pTagBlock,
                                  SSDataBlock* pBlock, int32_t rows) {
  int32_t         code = 0;
  int32_t         lino = 0;
  STableScanBase* pTableScanInfo = &pInfo->base;
  SExprSupp*      pSup = &pTableScanInfo->pseudoSup;
  int64_t         backupRows = pBlock->info.rows;

  pBlock->info.rows = rows;
  code = doSetLocalTagPseudoColumnData(pOperator, pInfo, pTagBlock, pBlock, pBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _return);
  if (pInfo->pTargets != NULL) {
    int32_t dstSlotId = 0;
    SNode*  pNode = NULL;
    FOREACH(pNode, pInfo->pTargets) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        ++dstSlotId;
        continue;
      }

      SColumnNode* pColNode = (SColumnNode*)pNode;
      SColumnNode* pMatchedRefCol = findRefTagOutputColumn(pInfo, pColNode);
      bool         isRefTagOutput = (pMatchedRefCol != NULL);
      if (pColNode->colType != COLUMN_TYPE_TAG || !isRefTagOutput) {
        ++dstSlotId;
        continue;
      }

      SColumnInfoData* pDstCol = taosArrayGet(pBlock->pDataBlock, dstSlotId);
      SColumnInfoData* pSrcCol = NULL;
      const STagVal*   pResolvedTagVal = findResolvedTagVal(pOperator, pColNode->colId);

      if (pResolvedTagVal == NULL && pInfo->pRefTagCols != NULL) {
        SNode*  pRefNode = NULL;
        int32_t refIdx = 0;
        FOREACH(pRefNode, pInfo->pRefTagCols) {
          if (nodeType(pRefNode) != QUERY_NODE_COLUMN) {
            ++refIdx;
            continue;
          }

          SColumnNode* pRefCol = (SColumnNode*)pRefNode;
          if (0 != strcmp(pRefCol->colName, pColNode->colName)) {
            ++refIdx;
            continue;
          }

          STagRefSavedBlock* pSaved = findSavedTagRefBlock(pInfo, pRefCol->dataBlockId);
          if (pSaved == NULL && pInfo->pSavedTagRefBlocks != NULL && refIdx < taosArrayGetSize(pInfo->pSavedTagRefBlocks)) {
            pSaved = taosArrayGet(pInfo->pSavedTagRefBlocks, refIdx);
          }
          if (pSaved != NULL && pSaved->pBlock != NULL) {
            pSrcCol = taosArrayGet(pSaved->pBlock->pDataBlock, pRefCol->slotId);
            if (pSrcCol != NULL) {
              break;
            }
          }
          ++refIdx;
        }
      }
      TSDB_CHECK_NULL(pDstCol, code, lino, _return, terrno)
      code = setTagColumnValue(pDstCol, pSrcCol, pResolvedTagVal, pBlock->info.rows);
      QUERY_CHECK_CODE(code, lino, _return);

      ++dstSlotId;

    }
  }

  if (pSup->numOfExprs > 0) {
    code = vtableAddTagPseudoColumnData(pOperator, pSup->pExprInfo, pSup->numOfExprs, pTagBlock, pBlock, rows, pInfo);
    QUERY_CHECK_CODE(code, lino, _return);
  }

_return:
  pBlock->info.rows = backupRows;
  return code;
}

static bool isSingleRowTagBlockAllNull(SSDataBlock* pTagBlock) {
  if (pTagBlock == NULL || pTagBlock->info.rows != 1) {
    return false;
  }

  for (int32_t i = 0; i < taosArrayGetSize(pTagBlock->pDataBlock); ++i) {
    SColumnInfoData* pCol = taosArrayGet(pTagBlock->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }
    if (!colDataIsNull_s(pCol, 0)) {
      char* data = colDataGetData(pCol, 0);
      if (!IS_JSON_NULL(pCol->info.type, data)) {
        return false;
      }
    }
  }
  return true;
}

int32_t virtualTableGetNext(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t                        code = TSDB_CODE_SUCCESS;
  int32_t                        lino = 0;
  SVirtualScanMergeOperatorInfo* pInfo = pOperator->info;
  SVirtualTableScanInfo*         pVirtualScanInfo = &pInfo->virtualScanInfo;
  SExecTaskInfo*                 pTaskInfo = pOperator->pTaskInfo;

  if (pOperator->status == OP_EXEC_DONE && !pOperator->pOperatorGetParam) {
    *pResBlock = NULL;
    return code;
  }

  if (taosArrayGetSize(pInfo->binfo.pRes->pDataBlock) == 0) {
    // scan no cols
    *pResBlock = NULL;
    return code;
  }

  VTS_ERR_JRET(pOperator->fpSet._openFn(pOperator));

  if (pInfo->limitReached) {
    *pResBlock = NULL;
    return code;
  }

  VTS_ERR_JRET(buildSavedTagRefBlocks(pOperator));

  // Dynamic VSTB keeps the original table-scan path unchanged; when metadata is projected,
  // fetch the real 1-row tag block from the extra tag-scan child and use it only for fill.
  SVTableScanOperatorParam* pDynParam =
      pOperator->pOperatorGetParam ? (SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value : NULL;
  bool needTagBlock = (pVirtualScanInfo->tagBlockId != -1) || (pDynParam != NULL && pDynParam->pTagScanOp != NULL) ||
                      (pDynParam == NULL && pVirtualScanInfo->tagDownStreamId != -1 &&
                       pVirtualScanInfo->pScanPseudoCols != NULL && LIST_LENGTH(pVirtualScanInfo->pScanPseudoCols) > 0);
  if (needTagBlock && pVirtualScanInfo->tagDownStreamId != -1 && !pInfo->pSavedTagBlock) {
      SSDataBlock*   pTagBlock = NULL;
      SOperatorInfo* pTagScanOp = pOperator->pDownstream[pVirtualScanInfo->tagDownStreamId];
      if (pDynParam != NULL) {
        SOperatorParam* pTagOp = pDynParam->pTagScanOp;
        if (pTagOp != NULL) {
          // Ownership stays with vtbScanParam; prevent exchange from freeing.
          pTagOp->reUse = true;
          VTS_ERR_JRET(pTagScanOp->fpSet.getNextExtFn(pTagScanOp, pTagOp, &pTagBlock));
        }
      } else {
        VTS_ERR_JRET(pTagScanOp->fpSet.getNextFn(pTagScanOp, &pTagBlock));
      }

      if (pTagBlock == NULL) {
        // Tag scan may return NULL for STB scans; local tag values are provided by DynQueryCtrl.
      } else if (pTagBlock->info.rows != 1) {
      VTS_ERR_JRET(TSDB_CODE_FAILED);
    } else {
      VTS_ERR_JRET(createOneDataBlock(pTagBlock, true, &pInfo->pSavedTagBlock));
    }
  }

  // Child scans without a DynQueryCtrl parent resolve owned-tag values themselves.
  VTS_ERR_JRET(ensureOwnedTagsResolved(pOperator));
  // Const-ref tags (vntb/ntb source) are table-level constants: resolve via the chain
  // resolver on every query (cached per operator instance), regardless of DynQueryCtrl.
  VTS_ERR_JRET(ensureRefTagsResolved(pOperator));

  while (1) {
    bool fromStash = false;
    bool holdReady = false;

    if (pVirtualScanInfo->alignHasNext && pVirtualScanInfo->pAlignStash != NULL) {
      *pResBlock = pVirtualScanInfo->pAlignStash;
      pVirtualScanInfo->pAlignStash = NULL;
      fromStash = true;
    } else {
      VTS_ERR_JRET(doVirtualTableMerge(pOperator, pResBlock));
      if (*pResBlock == NULL) {
        if (pVirtualScanInfo->alignHasNext && pVirtualScanInfo->pAlignHold != NULL &&
            pVirtualScanInfo->alignHoldUnpublished) {
          // End of the current series: unresolved NEXT positions keep their
          // temporary NULLs and the held rows are published now.
          *pResBlock = pVirtualScanInfo->pAlignHold;
          pVirtualScanInfo->alignHoldPublished = true;
          pVirtualScanInfo->alignHoldUnpublished = false;
          pVirtualScanInfo->alignHoldUidValid = false;
          resetTimeAlignState(pVirtualScanInfo);
          holdReady = true;
        } else {
          setOperatorCompleted(pOperator);
          break;
        }
      }
    }

    if (!holdReady) {
      if (!fromStash) {
        if (pOperator->pOperatorGetParam) {
          uint64_t uid = ((SVTableScanOperatorParam*)pOperator->pOperatorGetParam->value)->uid;
          (*pResBlock)->info.id.uid = uid;
          qTrace("vtable scan uid:%" PRIu64, (*pResBlock)->info.id.uid);
        } else {
          (*pResBlock)->info.id.uid = pInfo->virtualScanInfo.vtableUid;
          qTrace("vtable scan vtb uid:%" PRIu64, (*pResBlock)->info.id.uid);
        }

        // Dynamic vtable scans may need local metadata fill (resolved tags/tbname) even without a saved tag block.
        bool tagAllNull = isSingleRowTagBlockAllNull(pInfo->pSavedTagBlock);
        bool needMetadataFill =
            (pInfo->pSavedTagBlock != NULL && !tagAllNull) ||
            pVirtualScanInfo->pSavedTagRefBlocks != NULL ||
            pOperator->pOperatorGetParam != NULL ||
            pVirtualScanInfo->pOwnedResolvedTags != NULL ||
            pVirtualScanInfo->pConstRefResolvedTags != NULL;
        if (needMetadataFill) {
          VTS_ERR_JRET(doSetTagColumnData(pOperator, pVirtualScanInfo, pInfo->pSavedTagBlock, (*pResBlock),
                                          (*pResBlock)->info.rows));
        }
      }

      if (!pVirtualScanInfo->alignHasNext) {
        VTS_ERR_JRET(applyTimeAlign(pVirtualScanInfo, pOperator, *pResBlock));
      } else {
        uint64_t uid = (*pResBlock)->info.id.uid;
        int8_t   phase = (*pResBlock)->info.scanFlag;
        if (!fromStash && pVirtualScanInfo->pAlignHold != NULL && pVirtualScanInfo->alignHoldUnpublished &&
            pVirtualScanInfo->alignHoldUidValid &&
            (uid != pVirtualScanInfo->alignHoldUid || phase != pVirtualScanInfo->alignHoldPhase)) {
          // Series boundary: publish the old series and keep this tag-filled
          // block for the next call (the merge block is reused by the merge).
          VTS_ERR_JRET(createOneDataBlock(*pResBlock, true, &pVirtualScanInfo->pAlignStash));
          *pResBlock = pVirtualScanInfo->pAlignHold;
          pVirtualScanInfo->alignHoldPublished = true;
          pVirtualScanInfo->alignHoldUnpublished = false;
          pVirtualScanInfo->alignHoldUidValid = false;
          resetTimeAlignState(pVirtualScanInfo);
          holdReady = true;
        } else {
          if (!pVirtualScanInfo->alignHoldUidValid) {
            pVirtualScanInfo->alignHoldUid = uid;
            pVirtualScanInfo->alignHoldPhase = phase;
            pVirtualScanInfo->alignHoldUidValid = true;
          }
          VTS_ERR_JRET(applyTimeAlignNext(pVirtualScanInfo, pOperator, *pResBlock));
          if (timeAlignAnyPending(pVirtualScanInfo)) {
            // The held block has rows whose successor is still unknown; fetch
            // another merged block before publishing anything.
            *pResBlock = NULL;
            continue;
          }
          *pResBlock = pVirtualScanInfo->pAlignHold;
          pVirtualScanInfo->alignHoldPublished = true;
          pVirtualScanInfo->alignHoldUnpublished = false;
          holdReady = true;
        }
      }
    }

    if (pOperator->exprSupp.pFilterInfo != NULL) {
      const char* filterTbName = NULL;
      if (pOperator->pOperatorGetParam != NULL) {
        SVTableScanOperatorParam* pParam = pOperator->pOperatorGetParam->value;
        if (pParam != NULL && pParam->tbName[0] != '\0') {
          filterTbName = pParam->tbName;
        }
      }
      if (filterTbName == NULL && pVirtualScanInfo->vtableName[0] != '\0') {
        filterTbName = pVirtualScanInfo->vtableName;
      }
      filterInfoSetTableCtx(pOperator->exprSupp.pFilterInfo, (void*)filterTbName);
    }
    VTS_ERR_JRET(doFilter(*pResBlock, pOperator->exprSupp.pFilterInfo, NULL, NULL));
    // Count complete virtual rows after filtering, rather than source tuples.
    pInfo->limitReached = applyLimitOffset(&pInfo->limitInfo, *pResBlock,
                                          pTaskInfo);
    if ((*pResBlock)->info.rows > 0) {
      break;
    }
    if (pInfo->limitReached) {
      *pResBlock = NULL;
      break;
    }
  }

  return code;
_return:
  qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  cleanUpVirtualScanInfo(pVirtualScanInfo);
  if (pOperator->pOperatorGetParam != NULL) {
    if (pTaskInfo->pOpParam == pOperator->pOperatorGetParam) {
      pTaskInfo->pOpParam = NULL;
    }
    freeResetOperatorParams(pOperator, OP_GET_PARAM, true);
  }
  pTaskInfo->code = code;
  T_LONG_JMP(pTaskInfo->env, code);
}

static void destroyTableScanBase(STableScanBase* pBase, TsdReader* pAPI) {
  cleanupQueryTableDataCond(&pBase->cond);

  if (pAPI->tsdReaderClose) {
    pAPI->tsdReaderClose(pBase->dataReader);
  }
  pBase->dataReader = NULL;

  if (pBase->matchInfo.pList != NULL) {
    taosArrayDestroy(pBase->matchInfo.pList);
  }

  taosLRUCacheCleanup(pBase->metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pBase->pseudoSup);
}

void destroyVirtualTableScanOperatorInfo(void* param) {
  if (!param) {
    return;
  }
  SVirtualScanMergeOperatorInfo* pOperatorInfo = (SVirtualScanMergeOperatorInfo*)param;
  SVirtualTableScanInfo*         pInfo = &pOperatorInfo->virtualScanInfo;
  blockDataDestroy(pOperatorInfo->binfo.pRes);
  pOperatorInfo->binfo.pRes = NULL;

  blockDataDestroy(pInfo->pAlignHold);
  pInfo->pAlignHold = NULL;
  blockDataDestroy(pInfo->pAlignStash);
  pInfo->pAlignStash = NULL;

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  taosArrayDestroy(pInfo->pSortInfo);
  pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = NULL;
  destroyTableScanBase(&pInfo->base, &pInfo->base.readerAPI);

  tSimpleHashCleanup(pInfo->dataSlotMap);
  cleanupRefSlotGroups(pInfo);
  cleanupSavedTagRefBlocks(pInfo);
  blockDataDestroy(pOperatorInfo->pSavedTagBlock);
  pOperatorInfo->pSavedTagBlock = NULL;
  tSimpleHashCleanup(pInfo->pTagRefDownstreamMap);
  pInfo->pTagRefDownstreamMap = NULL;
  taosArrayDestroy(pInfo->pTagRefSourceBlockIds);
  pInfo->pTagRefSourceBlockIds = NULL;

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      cleanupLoadNextCtx(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }
  destroyResolvedTagsAndCaches(pInfo);
  resetTimeAlignState(pInfo);
  cleanupExprSupp(&pInfo->alignSup);
  nodesDestroyList(pInfo->pAlignTargets);
  taosMemoryFreeClear(pInfo->pAlignRowBuf);
  taosMemoryFreeClear(param);
}

int32_t extractColMap(SNodeList* pNodeList, SSHashObj** pSlotMap, int32_t* tsSlotId, int64_t* tagBlockId,
                      bool* useOriginTs) {
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (numOfCols == 0) {
    return code;
  }

  *tsSlotId = -1;
  *tagBlockId = -1;
  *useOriginTs = false;
  *pSlotMap = tSimpleHashInit(numOfCols, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  TSDB_CHECK_NULL(*pSlotMap, code, lino, _return, terrno);
  tSimpleHashSetFreeFp(*pSlotMap, destroySlotIdList);

  int32_t i = 0;
  SNode*  node = NULL;
  FOREACH(node, pNodeList) {
    SColumnNode* pColNode = (SColumnNode*)node;
    TSDB_CHECK_NULL(pColNode, code, lino, _return, terrno)

    if (pColNode->isPrimTs || pColNode->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      *tsSlotId = i;
    } else if (pColNode->colType != COLUMN_TYPE_COLUMN && !pColNode->hasRef) {
      // Non-data, non-ref column (e.g., tbname pseudo column or local tag).
      // colType may be COLUMN_TYPE_TAG, COLUMN_TYPE_TBNAME, or 0 (unset for pseudo cols).
      *tagBlockId = pColNode->dataBlockId;
    } else if (pColNode->hasRef) {
      int64_t slotKey = pColNode->dataBlockId << 16 | pColNode->slotId;
      if (pColNode->slotId == 0) {
        *useOriginTs = true;
      }
      SArray** ppSlots = tSimpleHashGet(*pSlotMap, &slotKey, sizeof(slotKey));
      if (ppSlots == NULL) {
        SArray* pSlots = taosArrayInit(1, sizeof(int32_t));
        TSDB_CHECK_NULL(pSlots, code, lino, _return, terrno)
        if (taosArrayPush(pSlots, &i) == NULL) {
          taosArrayDestroy(pSlots);
          code = terrno;
          goto _return;
        }
        code = tSimpleHashPut(*pSlotMap, &slotKey, sizeof(slotKey), &pSlots, POINTER_BYTES);
        if (code != TSDB_CODE_SUCCESS) {
          taosArrayDestroy(pSlots);
          goto _return;
        }
      } else {
        QUERY_CHECK_NULL(taosArrayPush(*ppSlots, &i), code, lino, _return, terrno)
      }
    }
    ++i;
  }

  return code;
_return:
  tSimpleHashCleanup(*pSlotMap);
  *pSlotMap = NULL;
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t resetVirtualTableMergeOperState(SOperatorInfo* pOper) {
  int32_t                        code = 0, lino = 0;
  SVirtualScanMergeOperatorInfo* pMergeInfo = pOper->info;
  SVirtualScanPhysiNode*         pPhynode = (SVirtualScanPhysiNode*)pOper->pPhyNode;
  SVirtualTableScanInfo*         pInfo = &pMergeInfo->virtualScanInfo;

  pOper->status = OP_NOT_OPENED;
  resetBasicOperatorState(&pMergeInfo->binfo);
  resetTimeAlignState(pInfo);
  blockDataDestroy(pInfo->pAlignHold);
  pInfo->pAlignHold = NULL;
  blockDataDestroy(pInfo->pAlignStash);
  pInfo->pAlignStash = NULL;
  pInfo->alignHoldUidValid = false;
  pInfo->alignHoldPublished = false;
  pInfo->alignHoldUnpublished = false;
  pMergeInfo->phase1Done = false;  // twoPassMode: restart from phase-1 on operator reset/retry
  pMergeInfo->twoPassErr = 0;

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
  // taosArrayDestroy(pInfo->pSortInfo);
  // pInfo->pSortInfo = NULL;

  blockDataDestroy(pInfo->pIntermediateBlock);
  pInfo->pIntermediateBlock = NULL;

  blockDataDestroy(pInfo->pInputBlock);
  pInfo->pInputBlock = createDataBlockFromDescNode(((SPhysiNode*)pPhynode)->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInfo->pInputBlock, code, lino, _exit, terrno)

  pInfo->tagDownStreamId = -1;

  if (pInfo->pSortCtxList) {
    for (int32_t i = 0; i < taosArrayGetSize(pInfo->pSortCtxList); i++) {
      SLoadNextCtx* pCtx = *(SLoadNextCtx**)taosArrayGet(pInfo->pSortCtxList, i);
      cleanupLoadNextCtx(pCtx);
    }
    taosArrayDestroy(pInfo->pSortCtxList);
    pInfo->pSortCtxList = NULL;
  }

  pMergeInfo->pSavedTuple = NULL;
  blockDataDestroy(pMergeInfo->pSavedTagBlock);
  pMergeInfo->pSavedTagBlock = NULL;
  cleanupSavedTagRefBlocks(pInfo);
  tSimpleHashCleanup(pInfo->pTagRefDownstreamMap);
  pInfo->pTagRefDownstreamMap = NULL;
  taosArrayDestroy(pInfo->pTagRefSourceBlockIds);
  pInfo->pTagRefSourceBlockIds = NULL;

_exit:

  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

static SColumnNode* findTimeAlignScanCol(SNodeList* pScanCols, col_id_t colId) {
  if (pScanCols == NULL) return NULL;
  SNode* pNode = NULL;
  FOREACH(pNode, pScanCols) {
    SNode* pExpr = (QUERY_NODE_TARGET == nodeType(pNode)) ? ((STargetNode*)pNode)->pExpr : pNode;
    if (QUERY_NODE_COLUMN != nodeType(pExpr)) continue;
    SColumnNode* pCol = (SColumnNode*)pExpr;
    if (pCol->colId == colId) return pCol;
  }
  return NULL;
}

// The merged block layout mirrors the physical target list order.  The source column is
// emitted before the align outputs, so the first target with the source column id is the
// source; the align output slot is recorded by the planner.
static int32_t findTimeAlignSrcSlot(SNodeList* pTargets, col_id_t colId) {
  if (pTargets == NULL) return -1;
  int32_t idx = 0;
  SNode*  pNode = NULL;
  FOREACH(pNode, pTargets) {
    SNode* pExpr = (QUERY_NODE_TARGET == nodeType(pNode)) ? ((STargetNode*)pNode)->pExpr : pNode;
    if (QUERY_NODE_COLUMN == nodeType(pExpr) && ((SColumnNode*)pExpr)->colId == colId) return idx;
    ++idx;
  }
  return -1;
}

static int32_t findTimeAlignOutSlot(SNodeList* pTargets, SColumnNode* pOut) {
  if (pTargets == NULL) return -1;
  int32_t numTargets = LIST_LENGTH(pTargets);
  for (int32_t j = numTargets - 1; j >= 0; --j) {
    SNode* pNode = nodesListGetNode(pTargets, j);
    SNode* pExpr = (QUERY_NODE_TARGET == nodeType(pNode)) ? ((STargetNode*)pNode)->pExpr : pNode;
    if (QUERY_NODE_COLUMN == nodeType(pExpr)) {
      SColumnNode* pCol = (SColumnNode*)pExpr;
      if (pCol->colId == pOut->colId && 0 == strcasecmp(pCol->colName, pOut->colName)) return j;
    }
  }
  return -1;
}

static void resetTimeAlignState(SVirtualTableScanInfo* pInfo) {
  if (pInfo->alignSup.numOfExprs == 0 || pInfo->alignSup.pCtx == NULL) return;
  for (int32_t i = 0; i < pInfo->alignSup.numOfExprs; ++i) {
    SqlFunctionCtx* pCtx = &pInfo->alignSup.pCtx[i];
    if (pCtx->resultInfo != NULL && pCtx->fpSet.cleanup != NULL) {
      pCtx->fpSet.cleanup(pCtx);
    }
    if (pCtx->resultInfo != NULL) {
      pCtx->resultInfo->initialized = false;
    }
  }
  pInfo->alignUidValid = false;
}

static SNode* makeTimeAlignTinyIntValue(int8_t value) {
  SValueNode* pVal = NULL;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal)) {
    return NULL;
  }
  pVal->node.resType =
      (SDataType){.type = TSDB_DATA_TYPE_TINYINT, .bytes = tDataTypes[TSDB_DATA_TYPE_TINYINT].bytes};
  if (TSDB_CODE_SUCCESS != nodesSetValueNodeValue(pVal, &value)) {
    nodesDestroyNode((SNode*)pVal);
    return NULL;
  }
  pVal->translate = true;
  pVal->isNull = false;
  return (SNode*)pVal;
}

// Append the optional time-range arguments of one align column:
// (ts, spanValue, spanUnit, tzName, orderFlag).  orderFlag bit 0 marks
// monotonic delivery and bit 1 descending timestamp order.
static int32_t appendTimeAlignRangeParams(SFunctionNode* pFunc, const SColumnNode* pOut, int32_t tsSlotId,
                                          const char* tzName, bool desc) {
  if (!pOut->alignHasSpan) return TSDB_CODE_SUCCESS;
  if (tsSlotId < 0) {
    qError("TIME_ALIGN: no timestamp slot for the time range of column '%s'", pOut->colName);
    return TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR;
  }

  SColumnNode* pTs = NULL;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pTs);
  if (code != TSDB_CODE_SUCCESS) return code;
  pTs->node.resType = (SDataType){.type = TSDB_DATA_TYPE_TIMESTAMP,
                                  .bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes,
                                  .precision = TSDB_TIME_PRECISION_MILLI};
  pTs->slotId = tsSlotId;
  pTs->colId = 0;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pTs);
  if (code != TSDB_CODE_SUCCESS) return code;

  SNode* pNode = NULL;
  code = nodesMakeValueNodeFromInt64(pOut->alignSpanValue, &pNode);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
  if (code != TSDB_CODE_SUCCESS) return code;

  pNode = makeTimeAlignTinyIntValue(pOut->alignSpanUnit);
  if (pNode == NULL) return terrno;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
  if (code != TSDB_CODE_SUCCESS) return code;

  char tzBuf[TD_TIMEZONE_LEN] = {0};
  if (tzName != NULL) tstrncpy(tzBuf, tzName, sizeof(tzBuf));
  SValueNode* pTz = NULL;
  code = nodesMakeValueNodeFromString(tzBuf, &pTz);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pTz);
  if (code != TSDB_CODE_SUCCESS) return code;

  // Merged vtable blocks are monotonic per series; the descending bit tells
  // the fill function to measure the range along the delivery direction.
  pNode = makeTimeAlignTinyIntValue((int8_t)(0x1 | (desc ? 0x2 : 0)));
  if (pNode == NULL) return terrno;
  return nodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
}

static int32_t createTimeAlignExprSupp(SVirtualTableScanInfo* pInfo, SNodeList* pScanCols,
                                       SNodeList* pAlignCols, SNodeList* pOutTargets, int32_t tsSlotId,
                                       const char* tzName, int8_t order, SExecTaskInfo* pTaskInfo) {
  if (pAlignCols == NULL || LIST_LENGTH(pAlignCols) == 0) return TSDB_CODE_SUCCESS;

  bool    desc = (order == TSDB_ORDER_DESC);
  int32_t code = TSDB_CODE_SUCCESS;
  SNode*  pNode = NULL;
  FOREACH(pNode, pAlignCols) {
    if (QUERY_NODE_COLUMN != nodeType(pNode)) continue;
    SColumnNode* pOut = (SColumnNode*)pNode;
    // In a descending stream PREV maps to fill_next (pending rows resolve to
    // the next smaller timestamp) and NEXT maps to fill_forward.
    int8_t method = pOut->alignMethod;
    if (desc) {
      method = (method == ALIGN_METHOD_NEXT) ? ALIGN_METHOD_PREV : ALIGN_METHOD_NEXT;
    }
    if (method == ALIGN_METHOD_NEXT) {
      pInfo->alignHasNext = true;
    }

    SColumnNode* pSrc = findTimeAlignScanCol(pScanCols, pOut->colId);
    int32_t      srcSlotId = findTimeAlignSrcSlot(pOutTargets, pOut->colId);
    int32_t      outSlotId = pOut->slotId;
    if (outSlotId < 0 || outSlotId >= LIST_LENGTH(pOutTargets)) {
      outSlotId = findTimeAlignOutSlot(pOutTargets, pOut);
    }
    if (pSrc == NULL || srcSlotId < 0 || outSlotId < 0) {
      qError("TIME_ALIGN: source or slot not found for %s (colId=%d srcSlot=%d outSlot=%d)", pOut->colName,
             pOut->colId, srcSlotId, outSlotId);
      return TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR;
    }

    STargetNode* pTarget = NULL;
    code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
    if (code != TSDB_CODE_SUCCESS) return code;
    pTarget->slotId = outSlotId;
    code = nodesListMakeStrictAppend(&pInfo->pAlignTargets, (SNode*)pTarget);
    if (code != TSDB_CODE_SUCCESS) return code;

    code = nodesMakeNode(QUERY_NODE_FUNCTION, &pTarget->pExpr);
    if (code != TSDB_CODE_SUCCESS) return code;
    SFunctionNode* pFunc = (SFunctionNode*)pTarget->pExpr;
    tstrncpy(pFunc->functionName, (method == ALIGN_METHOD_NEXT) ? "fill_next" : "fill_forward",
             TSDB_FUNC_NAME_LEN);

    SColumnNode* pParam = NULL;
    code = nodesCloneNode((SNode*)pSrc, (SNode**)&pParam);
    if (code != TSDB_CODE_SUCCESS) return code;
    // Read the source value from its slot in the merged output block, not from the
    // column's own (unset) slot id.
    pParam->slotId = srcSlotId;

    code = nodesListMakeStrictAppend(&pFunc->pParameterList, (SNode*)pParam);
    if (code != TSDB_CODE_SUCCESS) return code;
    code = appendTimeAlignRangeParams(pFunc, pOut, tsSlotId, tzName, desc);
    if (code != TSDB_CODE_SUCCESS) return code;
    pFunc->node.resType = pOut->node.resType;
    tstrncpy(pFunc->node.aliasName, pOut->colName, TSDB_COL_NAME_LEN);
    code = fmGetFuncInfo(pFunc, NULL, 0);
    if (code != TSDB_CODE_SUCCESS) return code;
  }

  if (pInfo->pAlignTargets == NULL) return TSDB_CODE_SUCCESS;

  SExprInfo* pExprInfo = NULL;
  int32_t    numOfExprs = 0;
  code = createExprInfo(pInfo->pAlignTargets, NULL, &pExprInfo, &numOfExprs);
  if (code != TSDB_CODE_SUCCESS) return code;
  code = initExprSupp(&pInfo->alignSup, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) return code;

  SqlFunctionCtx* pCtx = pInfo->alignSup.pCtx;
  int64_t totalSize = 0;
  for (int32_t i = 0; i < numOfExprs; ++i) {
    totalSize += sizeof(SResultRowEntryInfo) + pCtx[i].resDataInfo.interBufSize;
  }
  char* pBuf = taosMemoryCalloc(1, totalSize);
  if (pBuf == NULL) return terrno;
  int64_t off = 0;
  for (int32_t i = 0; i < numOfExprs; ++i) {
    pCtx[i].resultInfo = (SResultRowEntryInfo*)(pBuf + off);
    pCtx[i].resultInfo->initialized = false;
    off += sizeof(SResultRowEntryInfo) + pCtx[i].resDataInfo.interBufSize;
  }

  pInfo->pAlignRowBuf = pBuf;
  pInfo->alignUidValid = false;
  return TSDB_CODE_SUCCESS;
}

static bool timeAlignAnyPending(SVirtualTableScanInfo* pInfo) {
  if (!pInfo->alignHasNext || pInfo->alignSup.pCtx == NULL) return false;
  for (int32_t i = 0; i < pInfo->alignSup.numOfExprs; ++i) {
    SqlFunctionCtx* pCtx = &pInfo->alignSup.pCtx[i];
    if (pCtx->fpSet.hasPending != NULL && pCtx->fpSet.hasPending(pCtx)) {
      return true;
    }
  }
  return false;
}

// Append every column of pIn to the held block.  The held block's row count is
// deliberately left at the pre-append value: projectApplyFunctions() uses
// pResult->info.rows as the output offset and advances it by the input rows.
static int32_t appendBlockToAlignHold(SVirtualTableScanInfo* pInfo, SSDataBlock* pIn) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (pInfo->pAlignHold == NULL) {
    code = createOneDataBlock(pIn, false, &pInfo->pAlignHold);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else if (pInfo->alignHoldPublished) {
    blockDataCleanup(pInfo->pAlignHold);
    pInfo->alignHoldPublished = false;
  }

  int32_t rowsBefore = pInfo->pAlignHold->info.rows;
  code = blockDataEnsureCapacity(pInfo->pAlignHold, rowsBefore + pIn->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // The held block is published in place of the merged blocks, so it must carry
  // the current series identity and scan phase (PRE_SCAN/MAIN_SCAN).  Two-pass
  // consumers such as percentile dispatch on info.scanFlag.
  pInfo->pAlignHold->info.id = pIn->info.id;
  pInfo->pAlignHold->info.scanFlag = pIn->info.scanFlag;

  int32_t capacity = (int32_t)pInfo->pAlignHold->info.capacity;
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pAlignHold->pDataBlock); ++i) {
    SColumnInfoData* pDst = taosArrayGet(pInfo->pAlignHold->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pIn->pDataBlock, i);
    if (pDst == NULL || pSrc == NULL) {
      return TSDB_CODE_VTABLE_SCAN_INTERNAL_ERROR;
    }
    code = colDataMergeCol(pDst, rowsBefore, &capacity, pSrc, pIn->info.rows);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  pInfo->alignHoldUnpublished = true;
  return TSDB_CODE_SUCCESS;
}

// NEXT path: append the tag-filled merged block to the held block and run the
// align stage with pResult=hold, pSrc=merged block.  The function contexts keep
// their pending queues across calls; publishing is decided by the caller.
static int32_t applyTimeAlignNext(SVirtualTableScanInfo* pInfo, SOperatorInfo* pOperator, SSDataBlock* pIn) {
  int32_t code = appendBlockToAlignHold(pInfo, pIn);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SVirtualScanMergeOperatorInfo* pMergeInfo = (SVirtualScanMergeOperatorInfo*)pOperator->info;
  int32_t order = (pMergeInfo != NULL) ? pMergeInfo->binfo.inputTsOrder : TSDB_ORDER_ASC;
  code = setInputDataBlock(&pInfo->alignSup, pIn, order, pIn->info.scanFlag, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for (int32_t i = 0; i < pInfo->alignSup.numOfExprs; ++i) {
    pInfo->alignSup.pCtx[i].pOutput = NULL;
  }
  code = projectApplyFunctions(pInfo->alignSup.pExprInfo, pInfo->pAlignHold, pIn, pInfo->alignSup.pCtx,
                               pInfo->alignSup.numOfExprs, NULL, NULL, pOperator->pTaskInfo);
  if (code == TSDB_CODE_SUCCESS && pInfo->tsSlotId >= 0) {
    code = blockDataUpdateTsWindow(pInfo->pAlignHold, pInfo->tsSlotId);
  }
  return code;
}

static int32_t applyTimeAlign(SVirtualTableScanInfo* pInfo, SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  if (pInfo->alignSup.numOfExprs == 0 || pBlock == NULL) return TSDB_CODE_SUCCESS;

  uint64_t uid = pBlock->info.id.uid;
  int8_t   phase = pBlock->info.scanFlag;
  if (pInfo->alignUidValid && (pInfo->alignUid != uid || pInfo->alignPhase != phase)) {
    resetTimeAlignState(pInfo);
  }
  pInfo->alignUid = uid;
  pInfo->alignPhase = phase;
  pInfo->alignUidValid = true;

  SVirtualScanMergeOperatorInfo* pMergeInfo = (SVirtualScanMergeOperatorInfo*)pOperator->info;
  int32_t order = (pMergeInfo != NULL) ? pMergeInfo->binfo.inputTsOrder : TSDB_ORDER_ASC;
  int32_t code = setInputDataBlock(&pInfo->alignSup, pBlock, order, pBlock->info.scanFlag, false);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // The align columns are never written by the merge, so their null bitmaps still mark every
  // row as NULL.  fill_forward clears nothing for fixed-length values (colDataSetDouble et al.
  // only store the data), so drop the stale null marks first; rows the function decides are
  // NULL are set again by colDataSetNULL.
  for (int32_t i = 0; i < pInfo->alignSup.numOfExprs; ++i) {
    int32_t          slotId = pInfo->alignSup.pExprInfo[i].base.resSchema.slotId;
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slotId);
    if (pCol == NULL || !pCol->hasNull) continue;
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      for (int32_t r = 0; r < pBlock->info.rows; ++r) {
        pCol->varmeta.offset[r] = 0;
      }
    } else if (pCol->nullbitmap != NULL) {
      for (int32_t r = 0; r < pBlock->info.rows; ++r) {
        colDataClearNull_f(pCol->nullbitmap, r);
      }
    }
  }

  // fill_forward's setup memsets its stale pOutput; drop the previous block's output
  // pointer so it cannot clear the column data of a block that is being filled in place.
  for (int32_t i = 0; i < pInfo->alignSup.numOfExprs; ++i) {
    pInfo->alignSup.pCtx[i].pOutput = NULL;
  }
  code = projectApplyFunctions(pInfo->alignSup.pExprInfo, pBlock, pBlock, pInfo->alignSup.pCtx,
                               pInfo->alignSup.numOfExprs, NULL, NULL, pOperator->pTaskInfo);
  return code;
}

int32_t createVirtualTableMergeOperatorInfo(SOperatorInfo** pDownstream, int32_t numOfDownstream,
                                            SVirtualScanPhysiNode* pVirtualScanPhyNode, SExecTaskInfo* pTaskInfo,
                                            SOperatorInfo** pOptrInfo) {
  SPhysiNode*                    pPhyNode = (SPhysiNode*)pVirtualScanPhyNode;
  int32_t                        lino = 0;
  int32_t                        code = TSDB_CODE_SUCCESS;
  SVirtualScanMergeOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SVirtualScanMergeOperatorInfo));
  SOperatorInfo*                 pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SDataBlockDescNode*            pDescNode = pPhyNode->pOutputDataBlockDesc;
  SNodeList*                     pMergeKeys = NULL;

  QUERY_CHECK_NULL(pInfo, code, lino, _return, terrno)
  QUERY_CHECK_NULL(pOperator, code, lino, _return, terrno)
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = pVirtualScanPhyNode;

  setOptrBasicInfoOrder(&pInfo->binfo, &pVirtualScanPhyNode->scan.node);

  SVirtualTableScanInfo* pVirtualScanInfo = &pInfo->virtualScanInfo;
  pInfo->binfo.pRes = createDataBlockFromDescNode(pDescNode);
  TSDB_CHECK_NULL(pInfo->binfo.pRes, code, lino, _return, terrno)

  SSDataBlock* pInputBlock = createDataBlockFromDescNode(pPhyNode->pOutputDataBlockDesc);
  TSDB_CHECK_NULL(pInputBlock, code, lino, _return, terrno)
  pVirtualScanInfo->pInputBlock = pInputBlock;
  pVirtualScanInfo->tagDownStreamId = -1;
  pVirtualScanInfo->vtableUid = (tb_uid_t)pVirtualScanPhyNode->scan.uid;
  tstrncpy(pVirtualScanInfo->vtableName, pVirtualScanPhyNode->scan.tableName.tname,
           sizeof(pVirtualScanInfo->vtableName));
  pVirtualScanInfo->pTargets = pVirtualScanPhyNode->pTargets;
  pVirtualScanInfo->pScanPseudoCols = pVirtualScanPhyNode->scan.pScanPseudoCols;
  pVirtualScanInfo->pRefTagCols = pVirtualScanPhyNode->pRefTagCols;
  pInfo->twoPassMode = pVirtualScanPhyNode->twoPassMode;
  if (pInfo->twoPassMode && pVirtualScanPhyNode->scan.node.dynamicOp) {
    // Planner invariant: twoPassMode (static path) never co-occurs with the DynQueryCtrl
    // dynamic-op path. Surface a violation immediately instead of silently degrading to
    // single-pass and returning wrong PERCENTILE results.
    qError("invalid two-pass mode on dynamicOp virtual scan, vtable:%s", pVirtualScanInfo->vtableName);
    VTS_ERR_JRET(TSDB_CODE_APP_ERROR);
  }
  normalizeRefTagTargets(pVirtualScanInfo);
  if (pVirtualScanPhyNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pVirtualScanInfo->base.pseudoSup;
    pSup->pExprInfo = NULL;
    VTS_ERR_JRET(createExprInfo(pVirtualScanPhyNode->scan.pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs));

    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                      &pTaskInfo->storageAPI.functionStore);
    TSDB_CHECK_NULL(pSup->pCtx, code, lino, _return, terrno)
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  TSDB_CHECK_CODE(blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity), lino, _return);

  size_t  numOfCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  int32_t rowSize = pInfo->binfo.pRes->info.rowSize;

  pVirtualScanInfo->bufPageSize = getProperSortPageSize(rowSize, numOfCols);
  pVirtualScanInfo->sortBufSize = (uint64_t)pVirtualScanInfo->bufPageSize *
                                  (uint64_t)(numOfDownstream + 1);  // one additional is reserved for merged result.
  VTS_ERR_JRET(normalizeFederatedOutputTargets(pVirtualScanPhyNode->pTargets, pDownstream, numOfDownstream));
  VTS_ERR_JRET(extractColMap(pVirtualScanPhyNode->pTargets, &pVirtualScanInfo->dataSlotMap, &pVirtualScanInfo->tsSlotId,
                             &pVirtualScanInfo->tagBlockId, &pVirtualScanInfo->useOrgTsCol));
  VTS_ERR_JRET(createTimeAlignExprSupp(
      pVirtualScanInfo, pVirtualScanPhyNode->scan.pScanCols, pVirtualScanPhyNode->pAlignCols,
      pVirtualScanPhyNode->pTargets, pVirtualScanInfo->tsSlotId, pVirtualScanPhyNode->timezoneName,
      (pVirtualScanPhyNode->scan.node.inputTsOrder == ORDER_DESC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC,
      pTaskInfo));

  if (!pVirtualScanPhyNode->scan.node.dynamicOp) {
    VTS_ERR_JRET(makeTSMergeKey(&pMergeKeys, 0));
    ((SOrderByExprNode*)nodesListGetNode(pMergeKeys, 0))->order =
        pInfo->binfo.outputTsOrder == ORDER_DESC ? ORDER_DESC : ORDER_ASC;
    pVirtualScanInfo->pSortInfo = createSortInfo(pMergeKeys, false);
    TSDB_CHECK_NULL(pVirtualScanInfo->pSortInfo, code, lino, _return, terrno)
  } else {
    pTaskInfo->dynamicTask = true;
  }

  pVirtualScanInfo->scanAllCols = pVirtualScanPhyNode->scanAllCols;

  VTS_ERR_JRET(filterInitFromNode((SNode*)pVirtualScanPhyNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo,
                                  0, pTaskInfo->pStreamRuntimeInfo));
  filterSetExecContext(pOperator->exprSupp.pFilterInfo, pTaskInfo, isTaskKilled);

  if (tsMetaEntryCache) {
    pVirtualScanInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(tsMetaEntryCacheSize, -1, .5);
    QUERY_CHECK_NULL(pVirtualScanInfo->base.metaCache.pTableMetaEntryCache, code, lino, _return, terrno);
  }

  setOperatorInfo(pOperator, "VirtualTableScanOperator", QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, false,
                  OP_NOT_OPENED, pInfo, pTaskInfo);
  pOperator->fpSet =
      createOperatorFpSet(openVirtualTableScanOperator, virtualTableGetNext, NULL, destroyVirtualTableScanOperatorInfo,
                          optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorResetStateFn(pOperator, resetVirtualTableMergeOperState);

  if (NULL != pDownstream) {
    VTS_ERR_JRET(appendDownstream(pOperator, pDownstream, numOfDownstream));
  } else {
    pVirtualScanInfo->tagDownStreamId = -1;
  }

  nodesDestroyList(pMergeKeys);
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pInfo != NULL) {
    destroyVirtualTableScanOperatorInfo(pInfo);
  }
  nodesDestroyList(pMergeKeys);
  pTaskInfo->code = code;
  destroyOperatorAndDownstreams(pOperator, pDownstream, numOfDownstream);
  return code;
}
