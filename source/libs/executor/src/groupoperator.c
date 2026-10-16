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

#include "crypt.h"
#include "filter.h"
#include "function.h"
#include "os.h"
#include "query.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tname.h"
#include "tutil.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorInt.h"
#include "groupoperatorInt.h"
#include "operator.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"


typedef struct SGroupbyOperatorInfo {
  SOptrBasicInfo binfo;
  SAggSupporter  aggSup;
  SArray*        pGroupCols;     // group by columns, SArray<SColumn>
  SArray*        pGroupColVals;  // current group column values, SArray<SGroupKeys>
  bool           isInit;         // denote if current val is initialized or not
  char*          keyBuf;         // group by keys for hash
  int32_t        groupKeyLen;    // total group by column width
  SGroupResInfo  groupResInfo;
  SExprSupp      scalarSup;
  SOperatorInfo  *pOperator;
  SLimitInfo     limitInfo;
} SGroupbyOperatorInfo;

// The sort in partition may be needed later.
enum {
  PARTITION_DEFAULT_BUF_SIZE = 100 * 1024 * 1024,
  PARTITION_MIN_PAGE_SIZE = 512,
  PARTITION_TARGET_PAGE_ROWS = 10,
  PARTITION_MAX_PAGE_SIZE = 16 * 1024,
  PARTITION_PAGE_CLASS_SHIFT = 28,
  PARTITION_PAGE_ID_MASK = 0x0fffffff,
  PARTITION_SPILL_BATCH_PERCENT = 20,
  PARTITION_SPILL_WRITE_BATCH_LIMIT = 4 * 1024 * 1024,
  // Small fragments are read once so their block headers are parsed in memory;
  // cap growth because one group can otherwise own thousands of pages.
  PARTITION_SPILL_PREFETCH_LIMIT = 64 * 1024,
};

// The pinned page a row is currently written into. Held across rows so a run
// of rows in one group costs one pin, and carries its size class so the write
// loop reads page geometry without decoding the ref again.
typedef struct SPartitionWritePage {
  void*                      pPage;
  SPartitionDataInfo*        pGroup;
  const SPartitionPageClass* pClass;
} SPartitionWritePage;

#if defined(BUILD_TEST)
static int32_t tsPartitionSpillWriteCalls = 0;
static int32_t tsPartitionSpillReadCalls = 0;
static int32_t tsPartitionSpillWriteBatchLimit = 0;
static int32_t tsPartitionPrepareOutputFailure = TSDB_CODE_SUCCESS;
static int32_t tsPartitionPageAllocationFailure = TSDB_CODE_SUCCESS;
static int32_t tsPartitionPageAllocationFailuresRemaining = 0;
static int32_t tsPartitionSpillReadFailure = TSDB_CODE_SUCCESS;
static int32_t tsPartitionSpillCryptFailure = TSDB_CODE_SUCCESS;
static int32_t tsPartitionSpillCryptFailuresRemaining = 0;

static int32_t partitionTestConsumeFailure(int32_t* pCode) {
  int32_t code = *pCode;
  *pCode = TSDB_CODE_SUCCESS;
  return code;
}

void partitionSpillTestResetIoCalls(void) {
  tsPartitionSpillWriteCalls = 0;
  tsPartitionSpillReadCalls = 0;
}

void partitionSpillTestGetIoCalls(int32_t* pWriteCalls,
                                  int32_t* pReadCalls) {
  if (pWriteCalls != NULL) {
    *pWriteCalls = tsPartitionSpillWriteCalls;
  }
  if (pReadCalls != NULL) {
    *pReadCalls = tsPartitionSpillReadCalls;
  }
}

void partitionSpillTestSetWriteBatchLimit(int32_t writeBatchLimit) {
  tsPartitionSpillWriteBatchLimit = writeBatchLimit;
}

void partitionTestSetPrepareOutputFailure(int32_t code) {
  tsPartitionPrepareOutputFailure = code;
}

void partitionTestSetPageAllocationFailure(int32_t code) {
  tsPartitionPageAllocationFailure = code;
  // An OOM allocation is retried after a forced spill, so fail both attempts.
  tsPartitionPageAllocationFailuresRemaining =
      code == TSDB_CODE_SUCCESS ? 0 : 2;
}

void partitionTestSetSpillReadFailure(int32_t code) {
  tsPartitionSpillReadFailure = code;
}

void partitionTestSetSpillCryptFailure(int32_t code) {
  tsPartitionSpillCryptFailure = code;
  // An OOM spill setup is retried by spilling the current group directly.
  tsPartitionSpillCryptFailuresRemaining =
      code == TSDB_CODE_SUCCESS ? 0 : 2;
}
#endif

static int32_t partitionGetSpillWriteBatchLimit(void) {
#if defined(BUILD_TEST)
  if (tsPartitionSpillWriteBatchLimit > 0) {
    return tsPartitionSpillWriteBatchLimit;
  }
#endif
  return PARTITION_SPILL_WRITE_BATCH_LIMIT;
}

STATIC_EXCEPT_TEST int32_t
partitionGetOrCreateGroupInfo(SPartitionOperatorInfo* pInfo,
                              SPartitionDataInfo** pGroupInfo, int32_t len);
static void* partitionGetWritablePageForRows(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroupInfo,
    int32_t pendingRows, const SPartitionPageClass** ppClass);
STATIC_EXCEPT_TEST int32_t partitionSelectGrowthClass(
    const SPartitionOperatorInfo* pInfo, int32_t currentClass,
    int32_t requiredRows);
STATIC_EXCEPT_TEST int32_t* setupColumnOffset(const SSDataBlock* pBlock,
                                              int32_t            rowCapacity);
static int32_t  setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                        int32_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup);
static int32_t  extractColumnInfo(SNodeList* pNodeList, SArray** pArrayRes);
static int32_t  getPartitionPageRowCapacity(const SSDataBlock* pBlock, size_t pageSize, int32_t extraSize);
static int32_t partitionGetInitialPageSize(const SSDataBlock* pBlock,
                                           int32_t* pPageSize);

STATIC_EXCEPT_TEST void freeGroupKey(void* param) {
  SGroupKeys* pKey = (SGroupKeys*)param;
  taosMemoryFree(pKey->pData);
}

static void destroyGroupOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;

  cleanupBasicInfo(&pInfo->binfo);
  taosMemoryFreeClear(pInfo->keyBuf);
  taosArrayDestroy(pInfo->pGroupCols);
  taosArrayDestroyEx(pInfo->pGroupColVals, freeGroupKey);
  cleanupExprSupp(&pInfo->scalarSup);

  // Per-result-row state is released by cleanupGroupbyRowState(), invoked from destroyOperator()
  // before cleanupExprSupp(). Doing it here would be too late: the supp is memset by then, so
  // numOfExprs is 0 and cleanupResultInfo() walks nothing -- silently leaking the DISTINCT dedup
  // set, spill pages and temp file of every row never emitted (early cancel, LIMIT/SLIMIT).
  pInfo->pOperator = NULL;

  cleanupGroupResInfo(&pInfo->groupResInfo);
  cleanupAggSup(&pInfo->aggSup);
  taosMemoryFreeClear(param);
}

// Paged page layout, private to this operator.
//
// Rows arrive one at a time and each is appended to whichever group it
// belongs to, so a page cannot hold a serialized block: appending a row to
// a compactly packed page would shift every column after it. Instead each
// column gets a region sized by the page's row capacity, and a row is
// written by a few memcpys at known offsets:
//
//   [rows int32][groupId uint64]
//   per column: [var: offset[capacity] | fixed: bitmap BitmapLen(capacity)]
//               [colLength int32]
//               [payload: reserved bytes * capacity, colLength of it used]
//
// setupColumnOffset computes the per-column starts, doHashPartition writes
// rows into them, and blockDataFromPagedBuf reads a page back. Spilling
// converts a page to the compact blockDataToBuf layout, which is what
// drops the reserved-but-unused space.
static int32_t blockDataGetPagedColumnReservedBytes(
    const SColumnInfoData* pColumnInfoData) {
  return pColumnInfoData->info.bytes;
}

// Bytes a column reserves ahead of its colLength prefix: an offset per row
// for var columns, a null bitmap for fixed ones. Page copying and initial
// layout setup share this rule so their calculations cannot drift.
static int32_t blockDataGetPagedColumnMetaSize(
    const SColumnInfoData* pColumnInfoData, int32_t capacity) {
  return IS_VAR_DATA_TYPE(pColumnInfoData->info.type)
             ? (int32_t)(capacity * sizeof(int32_t))
             : BitmapLen(capacity);
}

// Reads one page back into pBlock. capacity must be the row capacity the
// page was laid out with, and the caller must have sized pBlock already.
// Not interchangeable with blockDataFromBuf: that one reads the compact
// layout. Both are usually handed a SDiskbasedBuf page, so the difference
// is the layout written into the page, not where the page lives.
STATIC_EXCEPT_TEST int32_t blockDataFromPagedBuf(SSDataBlock* pBlock,
                                                 const char*  buf,
                                                 size_t       capacity) {
  pBlock->info.rows = *(int32_t*)buf;
  pBlock->info.id.groupId = *(uint64_t*)(buf + sizeof(int32_t));

  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  const char* pStart = buf + sizeof(uint32_t) + sizeof(uint64_t);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      continue;
    }

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      size_t metaSize = capacity * sizeof(int32_t);
      memcpy(pCol->varmeta.offset, pStart, metaSize);
      pStart += metaSize;
    } else {
      memcpy(pCol->nullbitmap, pStart, BitmapLen(capacity));
      pStart += BitmapLen(capacity);
    }
    // The regions above are sized by capacity, but only info.rows of them
    // carry real rows.
    colDataUpdateHasNull(pCol, pBlock->info.rows);

    int32_t colLength = *(int32_t*)pStart;
    pStart += sizeof(int32_t);

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      if (pCol->varmeta.allocLen < colLength) {
        char* tmp = taosMemoryRealloc(pCol->pData, colLength);
        if (tmp == NULL) {
          return terrno;
        }

        pCol->pData = tmp;
        pCol->varmeta.allocLen = colLength;
      }

      pCol->varmeta.length = colLength;
      if (pCol->varmeta.length > pCol->varmeta.allocLen) {
        return TSDB_CODE_FAILED;
      }
    }

    // Copy the payload even when every row is NULL. Skipping it would save
    // one memcpy of what is only zeroed slack, but would leave the previous
    // block's bytes in pData - and colDataGetLength still reports the full
    // width, so spilling this block would write those stale bytes, from
    // some other group, into this group's fragment.
    if (colLength != 0) {
      memcpy(pCol->pData, pStart, colLength);
    }

    pStart += blockDataGetPagedColumnReservedBytes(pCol) * capacity;
  }

  return TSDB_CODE_SUCCESS;
}
  
// Runs from destroyOperator() before cleanupExprSupp(), the only point where both the expression
// supp and the result rows are still valid.
static void cleanupGroupbyRowState(void* param) {
  SGroupbyOperatorInfo* pInfo = (SGroupbyOperatorInfo*)param;
  if (pInfo == NULL || pInfo->pOperator == NULL || pInfo->aggSup.pResultRowHashTable == NULL) {
    return;
  }
  cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
                    false);
}

static int32_t getPartitionPageRowCapacity(const SSDataBlock* pBlock, size_t pageSize, int32_t extraSize) {
  size_t  numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t payloadSize = pageSize - extraSize;
  int32_t payloadRowSize = 0;
  int32_t numVarCols = 0;
  int32_t numFixCols = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      return -1;
    }

    payloadRowSize += blockDataGetPagedColumnReservedBytes(pCol);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      ++numVarCols;
    } else {
      ++numFixCols;
    }
  }

  int32_t nRows = payloadSize / payloadRowSize;
  if (nRows < 1) {
    uError("rows %d in page is too small, payloadSize:%d, rowSize:%d", nRows, payloadSize, payloadRowSize);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return -1;
  }

  int32_t result = -1;
  int32_t start = 1;
  int32_t end = nRows;
  while (start <= end) {
    int32_t mid = start + (end - start) / 2;
    int64_t midSize = (int64_t)payloadRowSize * mid + (int64_t)numVarCols * sizeof(int32_t) * mid +
                      (int64_t)numFixCols * BitmapLen(mid);
    if (midSize > payloadSize) {
      result = mid;
      end = mid - 1;
    } else {
      start = mid + 1;
    }
  }

  return (result != -1) ? result - 1 : nRows;
}

// Per-row cost of the paged layout, hoisted out of the block so both the
// rows-for-a-page and the page-for-N-rows directions share one formula.
typedef struct SPartitionPageLayout {
  int32_t payloadRowSize;  // reserved payload bytes, summed over columns
  int32_t numVarCols;      // each contributes an int32 offset per row
  int32_t numFixCols;      // each contributes a bitmap bit per row
} SPartitionPageLayout;

static int32_t partitionGetPageLayout(const SSDataBlock*    pBlock,
                                      SPartitionPageLayout* pLayout) {
  size_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  *pLayout = (SPartitionPageLayout){0};

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    if (pCol == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    pLayout->payloadRowSize += blockDataGetPagedColumnReservedBytes(pCol);
    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      pLayout->numVarCols += 1;
    } else {
      pLayout->numFixCols += 1;
    }
  }
  return TSDB_CODE_SUCCESS;
}

// Payload bytes a page needs to hold rows, excluding the serial meta header.
// Returns int64 because callers probe row counts that can overflow int32.
static int64_t partitionPagedSizeForRows(const SPartitionPageLayout* pLayout,
                                         int32_t rows) {
  return (int64_t)pLayout->payloadRowSize * rows +
         (int64_t)pLayout->numVarCols * sizeof(int32_t) * rows +
         (int64_t)pLayout->numFixCols * BitmapLen(rows);
}

// Sizes class 0's page to hold PARTITION_TARGET_PAGE_ROWS rows: layout
// bytes for that many rows, plus the serial meta header, rounded up to a
// PARTITION_MIN_PAGE_SIZE multiple (and floored at that minimum so tiny
// blocks still get a usable page).
static int32_t partitionGetInitialPageSize(const SSDataBlock* pBlock,
                                           int32_t* pPageSize) {
  SPartitionPageLayout layout = {0};

  int32_t code = partitionGetPageLayout(pBlock, &layout);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int64_t requiredSize =
      blockDataGetSerialMetaSize(taosArrayGetSize(pBlock->pDataBlock)) +
      partitionPagedSizeForRows(&layout, PARTITION_TARGET_PAGE_ROWS);

  requiredSize = TMAX(requiredSize, PARTITION_MIN_PAGE_SIZE);
  if (requiredSize > INT32_MAX - PARTITION_MIN_PAGE_SIZE + 1) {
    return TSDB_CODE_INVALID_PARA;
  }

  *pPageSize = (int32_t)ALIGN_NUM(requiredSize, PARTITION_MIN_PAGE_SIZE);
  return TSDB_CODE_SUCCESS;
}

STATIC_EXCEPT_TEST SPartitionPageRef partitionEncodePageRef(
    int32_t classIndex, int32_t pageId) {
  return classIndex << PARTITION_PAGE_CLASS_SHIFT | pageId;
}

static int32_t partitionPageRefClass(SPartitionPageRef pageRef) {
  return (int32_t)((uint32_t)pageRef >> PARTITION_PAGE_CLASS_SHIFT);
}

static int32_t partitionPageRefId(SPartitionPageRef pageRef) {
  return pageRef & PARTITION_PAGE_ID_MASK;
}

// Resolves the size class a page ref belongs to. Every page attribute
// (buffer, page size, row capacity, column offsets) lives on the class, so
// callers that need several of them decode the ref once.
static const SPartitionPageClass* partitionGetPageClass(
    const SPartitionOperatorInfo* pInfo, SPartitionPageRef pageRef) {
  int32_t classIndex = partitionPageRefClass(pageRef);
  if (classIndex >= pInfo->numPageClasses) {
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }
  return &pInfo->pageClasses[classIndex];
}

// The last class is the largest one, so it bounds every page in play.
static const SPartitionPageClass* partitionGetMaxPageClass(
    const SPartitionOperatorInfo* pInfo) {
  return &pInfo->pageClasses[pInfo->numPageClasses - 1];
}

// Resolves the size class of the page ref stored at index in a page list.
// Returns NULL with terrno set when the index or the ref is out of range.
static const SPartitionPageClass* partitionGetPageClassAt(
    const SPartitionOperatorInfo* pInfo, const SArray* pPageList,
    int32_t index, SPartitionPageRef* pPageRef) {
  SPartitionPageRef* pRef = taosArrayGet(pPageList, index);
  if (pRef == NULL) {
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }
  *pPageRef = *pRef;
  return partitionGetPageClass(pInfo, *pRef);
}

// Pins the page stored at index in a page list and reports its size class,
// so callers reach a page without spelling out the ref-decode each time.
static void* partitionPinPageAt(const SPartitionOperatorInfo* pInfo,
                                const SArray* pPageList, int32_t index,
                                const SPartitionPageClass** ppClass) {
  SPartitionPageRef pageRef = 0;
  const SPartitionPageClass* pClass =
      partitionGetPageClassAt(pInfo, pPageList, index, &pageRef);
  if (pClass == NULL) {
    return NULL;
  }

  void* pPage = getBufPageNoLRU(pClass->pBuf, partitionPageRefId(pageRef));
  if (pPage == NULL) {
    return NULL;
  }
  *ppClass = pClass;
  return pPage;
}

// Releases one page's claim on the resident budget. The byte subtraction is
// guarded so a bad ref cannot drive residentBytes negative and disable
// spilling for the rest of the run.
static void partitionDropResidentPage(SPartitionOperatorInfo* pInfo,
                                      SPartitionPageRef pageRef) {
  const SPartitionPageClass* pClass = partitionGetPageClass(pInfo, pageRef);
  if (pClass == NULL) {
    return;
  }
  if (pInfo->numResidentPages > 0) {
    pInfo->numResidentPages -= 1;
  }
  if (pInfo->residentBytes >= pClass->pageSize) {
    pInfo->residentBytes -= pClass->pageSize;
  }
}

STATIC_EXCEPT_TEST int32_t initGroupOptrInfo(SArray** pGroupColVals,
                                             int32_t* keyLen, char** keyBuf,
                                             const SArray* pGroupColList) {
  *pGroupColVals = taosArrayInit(4, sizeof(SGroupKeys));
  if ((*pGroupColVals) == NULL) {
    return terrno;
  }

  int32_t numOfGroupCols = taosArrayGetSize(pGroupColList);
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn* pCol = (SColumn*)taosArrayGet(pGroupColList, i);
    if (!pCol) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    (*keyLen) += pCol->bytes;  // actual data + null_flag

    SGroupKeys key = {0};
    key.bytes = pCol->bytes;
    key.type = pCol->type;
    key.isNull = false;
    key.pData = taosMemoryCalloc(1, pCol->bytes);
    if (key.pData == NULL) {
      return terrno;
    }

    void* tmp = taosArrayPush((*pGroupColVals), &key);
    if (!tmp) {
      return terrno;
    }
  }

  int32_t nullFlagSize = sizeof(int8_t) * numOfGroupCols;
  (*keyLen) += nullFlagSize;

  (*keyBuf) = taosMemoryCalloc(1, (*keyLen));
  if ((*keyBuf) == NULL) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static bool groupKeyCompare(SArray* pGroupCols, SArray* pGroupColVals, SSDataBlock* pBlock, int32_t rowIndex,
                            int32_t numOfGroupCols) {
  SColumnDataAgg* pColAgg = NULL;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);
    if (pBlock->pBlockAgg != NULL) {
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    bool isNull = colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg);

    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
    if (pkey->isNull && isNull) {
      continue;
    }

    if (isNull || pkey->isNull) {
      return false;
    }

    char* val = colDataGetData(pColInfoData, rowIndex);

    if (pkey->type == TSDB_DATA_TYPE_JSON) {
      int32_t dataLen = getJsonValueLen(val);

      if (memcmp(pkey->pData, val, dataLen) == 0) {
        continue;
      } else {
        return false;
      }
    } else if (IS_VAR_DATA_TYPE(pkey->type)) {
      if (IS_STR_DATA_BLOB(pkey->type)) {
        int32_t len = blobDataLen(val);
        if (len == blobDataLen(pkey->pData) && memcmp(blobDataVal(pkey->pData), blobDataVal(val), len) == 0) {
          continue;
        } else {
          return false;
        }
      } else {
        int32_t len = varDataLen(val);
        if (len == varDataLen(pkey->pData) && memcmp(varDataVal(pkey->pData), varDataVal(val), len) == 0) {
          continue;
        } else {
          return false;
        }
      }
    } else {
      if (memcmp(pkey->pData, val, pkey->bytes) != 0) {
        return false;
      }
    }
  }

  return true;
}

static int32_t recordGroupKeys(SArray* pGroupCols, SArray* pGroupColVals,
                               SSDataBlock* pBlock, int32_t rowIndex,
                               void* pKey, int32_t* pKeyLen) {
  SColumnDataAgg* pColAgg = NULL;

  size_t numOfGroupCols = taosArrayGetSize(pGroupCols);
  char*  isNull = pKey;
  char*  pStart = pKey != NULL
                      ? (char*)pKey + sizeof(int8_t) * numOfGroupCols
                      : NULL;

  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SColumn*         pCol = (SColumn*)taosArrayGet(pGroupCols, i);
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pCol->slotId);

    // valid range check. todo: return error code.
    if (pCol->slotId > taosArrayGetSize(pBlock->pDataBlock)) {
      continue;
    }

    if (pBlock->pBlockAgg != NULL) {
      pColAgg = &pBlock->pBlockAgg[pCol->slotId];  // TODO is agg data matched?
    }

    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
    if (colDataIsNull(pColInfoData, pBlock->info.rows, rowIndex, pColAgg)) {
      pkey->isNull = true;
      if (isNull != NULL) {
        isNull[i] = 1;
      }
    } else {
      pkey->isNull = false;
      char* val = colDataGetData(pColInfoData, rowIndex);
      int32_t dataLen = 0;
      if (pkey->type == TSDB_DATA_TYPE_JSON) {
        dataLen = getJsonValueLen(val);
        if (dataLen > pkey->bytes) {
          qError("recordGroupKeys col:%s, colId:%" PRId16 ", json data len "
                 "%d exceeds schema bytes %d",
                 pCol->name, pCol->colId, dataLen, pkey->bytes);
          return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        }
      } else if (IS_VAR_DATA_TYPE(pkey->type)) {
        if (IS_STR_DATA_BLOB(pkey->type)) {
          dataLen = blobDataTLen(val);
          if (dataLen > pkey->bytes) {
            qError("recordGroupKeys col:%s, colId:%" PRId16 ", blob data "
                   "len %d exceeds schema bytes %d",
                   pCol->name, pCol->colId, dataLen, pkey->bytes);
            return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          }
        } else {
          dataLen = varDataTLen(val);
          if (dataLen > pkey->bytes) {
            qError("recordGroupKeys col:%s, colId:%" PRId16 ", type:%d, var "
                   "data len %d exceeds schema bytes %d",
                   pCol->name, pCol->colId, pkey->type, dataLen, pkey->bytes);
            return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
          }
        }
      } else {
        dataLen = pkey->bytes;
      }
      memcpy(pkey->pData, val, dataLen);
      if (isNull != NULL) {
        isNull[i] = 0;
        memcpy(pStart, val, dataLen);
        pStart += dataLen;
      }
    }
  }

  if (pKey != NULL && pKeyLen != NULL) {
    *pKeyLen = (int32_t)(pStart - (char*)pKey);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t recordNewGroupKeys(SArray* pGroupCols, SArray* pGroupColVals,
                                  SSDataBlock* pBlock, int32_t rowIndex) {
  return recordGroupKeys(pGroupCols, pGroupColVals, pBlock, rowIndex, NULL,
                         NULL);
}

static int32_t buildGroupKeys(void* pKey, const SArray* pGroupColVals) {
  size_t numOfGroupCols = taosArrayGetSize(pGroupColVals);

  char* isNull = (char*)pKey;
  char* pStart = (char*)pKey + sizeof(int8_t) * numOfGroupCols;
  for (int32_t i = 0; i < numOfGroupCols; ++i) {
    SGroupKeys* pkey = taosArrayGet(pGroupColVals, i);
    if (pkey->isNull) {
      isNull[i] = 1;
      continue;
    }

    isNull[i] = 0;
    if (pkey->type == TSDB_DATA_TYPE_JSON) {
      int32_t dataLen = getJsonValueLen(pkey->pData);
      memcpy(pStart, (pkey->pData), dataLen);
      pStart += dataLen;
    } else if (IS_VAR_DATA_TYPE(pkey->type)) {
      if (IS_STR_DATA_BLOB(pkey->type)) {
        blobDataCopy(pStart, pkey->pData);
        pStart += blobDataTLen(pkey->pData);
      } else {
        varDataCopy(pStart, pkey->pData);
        pStart += varDataTLen(pkey->pData);
      }
    } else {
      memcpy(pStart, pkey->pData, pkey->bytes);
      pStart += pkey->bytes;
    }
  }

  return (int32_t)(pStart - (char*)pKey);
}

// assign the group keys or user input constant values if required
static int32_t doAssignGroupKeys(SqlFunctionCtx* pCtx, int32_t numOfOutput, int32_t totalRows, int32_t rowIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < numOfOutput; ++i) {
    if (pCtx[i].functionId == -1) {  // select count(*),key from t group by key.
      SResultRowEntryInfo* pEntryInfo = GET_RES_INFO(&pCtx[i]);

      SColumnInfoData* pColInfoData = pCtx[i].input.pData[0];
      // todo OPT all/all not NULL
      if (!colDataIsNull(pColInfoData, totalRows, rowIndex, NULL)) {
        char* dest = GET_ROWCELL_INTERBUF(pEntryInfo);
        char* data = colDataGetData(pColInfoData, rowIndex);

        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          // The interbuf cell is sized by the expression's declared result width; a
          // virtual table column may carry a value wider than declared (the full source
          // value is returned by design) -- refuse to overrun it.
          code = copyVarLenCellChecked(dest, pColInfoData->info.type, data,
                                       (int32_t)pCtx[i].resDataInfo.interBufSize, NULL);
          if (TSDB_CODE_SUCCESS != code) {
            return code;
          }
        } else {
          memcpy(dest, data, pColInfoData->info.bytes);
        }
      } else {  // it is a NULL value
        pEntryInfo->isNullRes = 1;
      }

      pEntryInfo->numOfRes = 1;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static void doHashGroupbyAgg(SOperatorInfo* pOperator, SSDataBlock* pBlock) {
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;

  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;
  int32_t         numOfGroupCols = taosArrayGetSize(pInfo->pGroupCols);
  //  if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
  //  qError("QInfo:0x%" PRIx64 ", group by not supported on double/float columns, abort", GET_TASKID(pRuntimeEnv));
  //    return;
  //  }

  int32_t len = 0;
  int32_t code = TSDB_CODE_SUCCESS;
  terrno = TSDB_CODE_SUCCESS;

  int32_t num = 0;
  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    // Compare with the previous row of this column, and do not set the output buffer again if they are identical.
    if (!pInfo->isInit) {
      code = recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      pInfo->isInit = true;
      num++;
      continue;
    }

    bool equal = groupKeyCompare(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j, numOfGroupCols);
    if (equal) {
      num++;
      continue;
    }

    // The first row of a new block does not belongs to the previous existed group
    if (j == 0) {
      code = recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      num = 1;
      continue;
    }

    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    code = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf,
                                  len, pBlock->info.id.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (code != TSDB_CODE_SUCCESS) {  // null data, too many state code
      T_LONG_JMP(pTaskInfo->env, code);
    }

    int32_t rowIndex = j - num;
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                          pOperator->exprSupp.numOfExprs);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // assign the group keys or user input constant values if required
    code = doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    code = recordNewGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    num = 1;
  }

  // The data of the last group is processed here, and if there is only one group, it is also processed here.
  if (num > 0) {
    len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
    code = setGroupResultOutputBuf(pOperator, &(pInfo->binfo), pOperator->exprSupp.numOfExprs, pInfo->keyBuf,
                                          len, pBlock->info.id.groupId, pInfo->aggSup.pResultBuf, &pInfo->aggSup);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    int32_t rowIndex = pBlock->info.rows - num;
    code = applyAggFunctionOnPartialTuples(pTaskInfo, pCtx, NULL, rowIndex, num, pBlock->info.rows,
                                          pOperator->exprSupp.numOfExprs);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    code = doAssignGroupKeys(pCtx, pOperator->exprSupp.numOfExprs, pBlock->info.rows, rowIndex);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }
}

bool hasRemainResultByHash(SOperatorInfo* pOperator) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSHashObj*            pHashmap = pInfo->aggSup.pResultRowHashTable;
  return pInfo->groupResInfo.index < tSimpleHashGetSize(pHashmap);
}

void doBuildResultDatablockByHash(SOperatorInfo* pOperator, SOptrBasicInfo* pbInfo, SGroupResInfo* pGroupResInfo,
                                  SDiskbasedBuf* pBuf) {
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSHashObj*            pHashmap = pInfo->aggSup.pResultRowHashTable;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;

  SSDataBlock* pBlock = pInfo->binfo.pRes;

  // set output datablock version
  pBlock->info.version = pTaskInfo->version;

  blockDataCleanup(pBlock);
  if (!hasRemainResultByHash(pOperator)) {
    return;
  }

  pBlock->info.id.groupId = 0;
  pBlock->info.id.baseGId = 0;
  if (!pInfo->binfo.mergeResultBlock) {
    doCopyToSDataBlockByHash(pTaskInfo, pBlock, &pOperator->exprSupp, pInfo->aggSup.pResultBuf, &pInfo->groupResInfo,
                             pHashmap, pOperator->resultInfo.threshold, false);
  } else {
    while (hasRemainResultByHash(pOperator)) {
      doCopyToSDataBlockByHash(pTaskInfo, pBlock, &pOperator->exprSupp, pInfo->aggSup.pResultBuf, &pInfo->groupResInfo,
                               pHashmap, pOperator->resultInfo.threshold, true);
      if (pBlock->info.rows >= pOperator->resultInfo.threshold) {
        break;
      }
      pBlock->info.id.groupId = 0;
      pBlock->info.id.baseGId = 0;
    }

    // clear the group id info in SSDataBlock, since the client does not need it
    pBlock->info.id.groupId = 0;
    pBlock->info.id.baseGId = 0;
  }
}

static bool slimitReached(SLimitInfo* pLimitInfo) {
  if (pLimitInfo && pLimitInfo->slimit.limit >= 0 &&
      pLimitInfo->numOfOutputGroups >= pLimitInfo->slimit.limit) {
    return true;  // limit reached, stop processing further rows
  }
  return false;
}

static int32_t doGroupResultSlimit(SSDataBlock* pRes, SLimitInfo* pLimitInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if (pRes == NULL || pRes->info.rows == 0 || !pLimitInfo) {
    return TSDB_CODE_SUCCESS;
  }

  if (pLimitInfo->remainGroupOffset > 0) {
    if (pRes->info.rows <= pLimitInfo->remainGroupOffset) {
      pLimitInfo->remainGroupOffset -= pRes->info.rows;
      blockDataCleanup(pRes);
      return TSDB_CODE_SUCCESS;
    } else {
      code = blockDataTrimFirstRows(pRes, pLimitInfo->remainGroupOffset);
      QUERY_CHECK_CODE(code, lino, _end);
      pLimitInfo->remainGroupOffset = 0;
    }
  }

  if (pLimitInfo->slimit.limit >= 0 && pRes->info.rows > 0) {
    int32_t remainRows = pLimitInfo->slimit.limit - pLimitInfo->numOfOutputGroups;
    if (pRes->info.rows > remainRows) {
      blockDataKeepFirstNRows(pRes, remainRows);
    }
    pLimitInfo->numOfOutputGroups += pRes->info.rows;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* buildGroupResultDataBlockByHash(SOperatorInfo* pOperator) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SSDataBlock*          pRes = pInfo->binfo.pRes;
  SLimitInfo*           pLimitInfo = &pInfo->limitInfo;

  // after filter, if result block turn to null, get next from whole set
  while (1) {
    doBuildResultDatablockByHash(pOperator, &pInfo->binfo, &pInfo->groupResInfo, pInfo->aggSup.pResultBuf);

    code = doFilterWithTaskCorrelation(pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL, pTaskInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    code = doGroupResultSlimit(pRes, pLimitInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    if (!hasRemainResultByHash(pOperator) || slimitReached(pLimitInfo)) {
      setOperatorCompleted(pOperator);
      // Release per-result-row function state before dropping the hash. On the SLIMIT path rows are
      // left unemitted, and once the table is freed and NULLed below there is no way to reach them:
      // the teardown hook would find pResultRowHashTable == NULL and walk nothing, leaking each
      // row's DISTINCT dedup set along with its spill pages and temp file.
      cleanupResultInfoInHashMap(pTaskInfo, &pOperator->exprSupp, pInfo->aggSup.pResultBuf,
                                 &pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable);
      // clean hash after completed
      tSimpleHashCleanup(pInfo->aggSup.pResultRowHashTable);
      pInfo->aggSup.pResultRowHashTable = NULL;
      break;
    }

    if (pRes->info.rows > 0) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return (pRes->info.rows == 0) ? NULL : pRes;
}

static int32_t hashGroupbyAggregateNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SGroupbyOperatorInfo* pInfo = pOperator->info;
  SGroupResInfo*        pGroupResInfo = &pInfo->groupResInfo;
  int32_t               order = pInfo->binfo.inputTsOrder;

  QRY_PARAM_CHECK(ppRes);
  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  if (pOperator->status == OP_RES_TO_RETURN) {
    (*ppRes) = buildGroupResultDataBlockByHash(pOperator);
    return code;
  }

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    filterSetExecContext(pOperator->exprSupp.pFilterInfo, pOperator->pTaskInfo, isTaskKilled);
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;

    // the pDataBlock are always the same one, no need to call this again
    code = setInputDataBlock(&pOperator->exprSupp, pBlock, order, pBlock->info.scanFlag, true);
    QUERY_CHECK_CODE(code, lino, _end);

    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code = projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                   pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    doHashGroupbyAgg(pOperator, pBlock);
  }

  pOperator->status = OP_RES_TO_RETURN;

  // initGroupedResultInfo(&pInfo->groupResInfo, pInfo->aggSup.pResultRowHashTable, 0);
  if (pGroupResInfo->pRows != NULL) {
    taosArrayDestroy(pGroupResInfo->pRows);
  }

  if (pGroupResInfo->pBuf) {
    taosMemoryFree(pGroupResInfo->pBuf);
    pGroupResInfo->pBuf = NULL;
  }

  pGroupResInfo->index = 0;
  pGroupResInfo->iter = 0;
  pGroupResInfo->dataPos = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    (*ppRes) = buildGroupResultDataBlockByHash(pOperator);
  }

  return code;
}

static int32_t resetGroupOperState(SOperatorInfo* pOper) {
  SGroupbyOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SAggPhysiNode* pPhynode = (SAggPhysiNode*)pOper->pPhyNode;
  resetBasicOperatorState(&pInfo->binfo);
  pOper->status = OP_NOT_OPENED;

  cleanupResultInfo(pInfo->pOperator->pTaskInfo, &pInfo->pOperator->exprSupp, &pInfo->groupResInfo, &pInfo->aggSup,
    false);

  cleanupGroupResInfo(&pInfo->groupResInfo);

  qInfo("[group key] len use:%d", pInfo->groupKeyLen);
  int32_t code = resetAggSup(&pOper->exprSupp, &pInfo->aggSup, pTaskInfo, pPhynode->pAggFuncs, pPhynode->pGroupKeys,
    pInfo->groupKeyLen + POINTER_BYTES, pTaskInfo->id.str, NULL,
    &pTaskInfo->storageAPI.functionStore);

  if (code == 0){
    code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
      &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->isInit = false;

  return code;
}

int32_t createGroupOperatorInfo(SOperatorInfo* downstream, SAggPhysiNode* pAggNode, SExecTaskInfo* pTaskInfo,
                                SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SGroupbyOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SGroupbyOperatorInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);

  pOperator->pPhyNode = (SNode*)pAggNode;
  pOperator->exprSupp.hasWindowOrGroup = true;
  pOperator->exprSupp.hasWindow = false;

  SSDataBlock* pResBlock = createDataBlockFromDescNode(pAggNode->node.pOutputDataBlockDesc);
  if (pResBlock == NULL) {
    code = terrno;
    goto _error;
  }
  initBasicInfo(&pInfo->binfo, pResBlock);

  initLimitInfo(pAggNode->node.pLimit, pAggNode->node.pSlimit, &pInfo->limitInfo);

  pInfo->pGroupCols = NULL;
  code = extractColumnInfo(pAggNode->pGroupKeys, &pInfo->pGroupCols);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t    numOfScalarExpr = 0;
  SExprInfo* pScalarExprInfo = NULL;
  if (pAggNode->pExprs != NULL) {
    code = createExprInfo(pAggNode->pExprs, NULL, &pScalarExprInfo, &numOfScalarExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = initExprSupp(&pInfo->scalarSup, pScalarExprInfo, numOfScalarExpr, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  code = blockDataEnsureCapacity(pInfo->binfo.pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t    num = 0;
  SExprInfo* pExprInfo = NULL;

  code = createExprInfo(pAggNode->pAggFuncs, pAggNode->pGroupKeys, &pExprInfo, &num);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initAggSup(&pOperator->exprSupp, &pInfo->aggSup, pExprInfo, num, pInfo->groupKeyLen, pTaskInfo->id.str,
                    NULL, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  code = filterInitFromNode((SNode*)pAggNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  initResultRowInfo(&pInfo->binfo.resultRowInfo);
  setOperatorInfo(pOperator, "GroupbyAggOperator", 0, true, OP_NOT_OPENED, pInfo, pTaskInfo);

  pInfo->binfo.mergeResultBlock = pAggNode->mergeDataBlock;
  setOptrBasicInfoOrder(&pInfo->binfo, &pAggNode->node);

  pInfo->pOperator = pOperator;

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hashGroupbyAggregateNext, NULL, destroyGroupOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorRowStateCleanupFn(pOperator, cleanupGroupbyRowState);
  setOperatorResetStateFn(pOperator, resetGroupOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  QUERY_CHECK_CODE(code, lino, _error);

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) destroyGroupOperatorInfo(pInfo);
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  pTaskInfo->code = code;
  return code;
}

SSDataBlock* createBlockDataNotLoaded(const SOperatorInfo* pOperator, SSDataBlock* pDataBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pDataBlock == NULL) {
    return NULL;
  }

  SSDataBlock* pDstBlock = NULL;
  code = createDataBlock(&pDstBlock);
  QUERY_CHECK_CODE(code, lino, _end);

  pDstBlock->info = pDataBlock->info;
  pDstBlock->info.pBlockAgg = NULL;
  pDstBlock->info.id.blockId = pOperator->resultDataBlockId;
  pDstBlock->info.capacity = 0;
  pDstBlock->info.rowSize = 0;

  size_t numOfCols = pOperator->exprSupp.numOfExprs;
  if (pDataBlock->pBlockAgg) {
    pDstBlock->pBlockAgg = taosMemoryCalloc(numOfCols, sizeof(SColumnDataAgg));
    if (pDstBlock->pBlockAgg == NULL) {
      blockDataDestroy(pDstBlock);
      return NULL;
    }
    for (int i = 0; i < numOfCols; ++i) {
      pDstBlock->pBlockAgg[i].colId = -1;
    }
  }

  for (int32_t i = 0; i < pOperator->exprSupp.numOfExprs; ++i) {
    SExprInfo*       pExpr = &pOperator->exprSupp.pExprInfo[i];
    int32_t          slotId = pExpr->base.pParam[0].pCol->slotId;
    SColumnInfoData* pSrc = taosArrayGet(pDataBlock->pDataBlock, slotId);
    SColumnInfoData  colInfo = {.hasNull = true, .info = pSrc->info};
    code = blockDataAppendColInfo(pDstBlock, &colInfo);
    QUERY_CHECK_CODE(code, lino, _end);

    SColumnInfoData* pDst = taosArrayGet(pDstBlock->pDataBlock, i);
    if (pDataBlock->pBlockAgg && pDataBlock->pBlockAgg[slotId].colId != -1) {
      pDstBlock->pBlockAgg[i] = pDataBlock->pBlockAgg[slotId];
    } else {
      code = doEnsureCapacity(pDst, &pDstBlock->info, pDataBlock->info.rows, false);
      QUERY_CHECK_CODE(code, lino, _end);

      code = colDataAssign(pDst, pSrc, pDataBlock->info.rows, &pDataBlock->info);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    blockDataDestroy(pDstBlock);
    return NULL;
  }
  return pDstBlock;
}

// Recycles one spilled page. The BUILD_TEST hook injects the failure at
// this seam so tests drive the production error-handling branch itself
// instead of replicating it.
static int32_t partitionRecycleBufPage(SDiskbasedBuf* pBuf, void* pPage,
                                       int32_t pageIndex) {
  (void)pageIndex;
#if defined(BUILD_TEST)
  if (partitionSpillTestConsumeRecycleFailure(pageIndex)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
#endif
  return dBufSetBufPageRecycled(pBuf, pPage);
}

STATIC_EXCEPT_TEST int32_t partitionWriteAt(TdFilePtr pFile, int64_t offset,
                                            const void* pData, int32_t length) {
#if defined(BUILD_TEST)
  tsPartitionSpillWriteCalls += 1;
  // Deliberately leaves terrno untouched: models the failure class whose
  // code is returned but never mirrored into terrno.
  if (partitionSpillTestConsumeWriteFailure(pFile, offset, pData, length)) {
    return TSDB_CODE_FAILED;
  }
#endif
  // Deliberately seek + taosWriteFile instead of taosPWriteFile: the
  // positioned helpers issue a single pwrite/pread, while the POSIX
  // taosWriteFile/taosReadFile loop over short I/O and retry EINTR
  // (the Windows variants remain single-shot either way).
  if (taosLSeekFile(pFile, offset, SEEK_SET) < 0) {
    return terrno;
  }

  int64_t written = taosWriteFile(pFile, pData, length);
  if (written != length) {
    return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

STATIC_EXCEPT_TEST int32_t partitionReadAt(TdFilePtr pFile, int64_t offset,
                                           void* pData, int32_t length) {
#if defined(BUILD_TEST)
  tsPartitionSpillReadCalls += 1;
  int32_t failure =
      partitionTestConsumeFailure(&tsPartitionSpillReadFailure);
  if (failure != TSDB_CODE_SUCCESS) {
    return failure;
  }
#endif
  if (taosLSeekFile(pFile, offset, SEEK_SET) < 0) {
    return terrno;
  }

  int64_t numRead = taosReadFile(pFile, pData, length);
  if (numRead != length) {
    // taosReadFile loops over short reads, so a non-negative shortfall means
    // it hit EOF and left terrno untouched: only a negative return carries a
    // code for this call. Trusting terrno otherwise reports whatever failed
    // earlier instead of the truncated spill file.
    return numRead < 0 && terrno != TSDB_CODE_SUCCESS
               ? terrno
               : TSDB_CODE_FILE_CORRUPTED;
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * Lazily creates the operator-owned temporary spill file and its optional
 * per-file encryption context.
 */
static int32_t partitionEnsureSpillFile(SPartitionOperatorInfo* pInfo) {
  if (pInfo->spill.pFile != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  char path[PATH_MAX] = {0};
  taosGetTmpfilePath(tsTempDir, "partition-spill", path);
  pInfo->spill.pFile =
      taosOpenFile(path, TD_FILE_CREATE | TD_FILE_READ | TD_FILE_WRITE |
                             TD_FILE_TRUNC | TD_FILE_AUTO_DEL);
  if (pInfo->spill.pFile == NULL) {
    qError("%s %s failed at line %d, open spill file:%s, since %s",
           GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__, path,
           tstrerror(terrno));
    return terrno;
  }
  if (taosSpillCryptEnabled()) {
    int32_t code = TSDB_CODE_SUCCESS;
#if defined(BUILD_TEST)
    if (tsPartitionSpillCryptFailuresRemaining > 0) {
      tsPartitionSpillCryptFailuresRemaining -= 1;
      code = tsPartitionSpillCryptFailure;
    }
#endif
    if (code == TSDB_CODE_SUCCESS) {
      code = taosCreateSpillCryptCtx(&pInfo->spill.pCryptCtx, 0);
    }
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s %s failed at line %d, create spill crypt ctx, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             tstrerror(code));
      (void)taosCloseFile(&pInfo->spill.pFile);
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionEnsureSpillBuffer(SPartitionOperatorInfo* pInfo,
                                          int32_t requiredSize) {
  if (pInfo->spill.bufSize >= requiredSize) {
    return TSDB_CODE_SUCCESS;
  }

  char* pBuf = taosMemoryRealloc(pInfo->spill.pBuf, requiredSize);
  if (pBuf == NULL) {
    qError("%s %s failed at line %d, grow spill buffer to %d bytes, "
           "since %s", GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
           requiredSize, tstrerror(terrno));
    return terrno;
  }
  pInfo->spill.pBuf = pBuf;
  pInfo->spill.bufSize = requiredSize;
  return TSDB_CODE_SUCCESS;
}

struct SPartitionSpillBucket {
  int32_t growLevel;
  SPartitionSpillBucket* pPrev;
  SPartitionSpillBucket* pNext;
  SPartitionDataInfo* pGroupHead;
  SPartitionDataInfo* pGroupTail;
};

static void partitionInsertSpillBucket(SPartitionOperatorInfo* pInfo,
                                       SPartitionSpillBucket* pBucket,
                                       SPartitionSpillBucket* pPrev,
                                       SPartitionSpillBucket* pNext) {
  pBucket->pPrev = pPrev;
  pBucket->pNext = pNext;
  if (pPrev != NULL) {
    pPrev->pNext = pBucket;
  } else {
    pInfo->pSpillBucketHead = pBucket;
  }
  if (pNext != NULL) {
    pNext->pPrev = pBucket;
  } else {
    pInfo->pSpillBucketTail = pBucket;
  }
}

static void partitionRemoveSpillBucket(SPartitionOperatorInfo* pInfo,
                                       SPartitionSpillBucket* pBucket) {
  if (pBucket->pPrev != NULL) {
    pBucket->pPrev->pNext = pBucket->pNext;
  } else {
    pInfo->pSpillBucketHead = pBucket->pNext;
  }
  if (pBucket->pNext != NULL) {
    pBucket->pNext->pPrev = pBucket->pPrev;
  } else {
    pInfo->pSpillBucketTail = pBucket->pPrev;
  }
  taosMemoryFree(pBucket);
}

static void partitionAppendSpillGroup(SPartitionSpillBucket* pBucket,
                                      SPartitionDataInfo* pGroup) {
  pGroup->pSpillBucket = pBucket;
  pGroup->pSpillPrev = pBucket->pGroupTail;
  pGroup->pSpillNext = NULL;
  if (pBucket->pGroupTail != NULL) {
    pBucket->pGroupTail->pSpillNext = pGroup;
  } else {
    pBucket->pGroupHead = pGroup;
  }
  pBucket->pGroupTail = pGroup;
}

STATIC_EXCEPT_TEST void partitionRemoveSpillGroup(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup) {
  SPartitionSpillBucket* pBucket = pGroup->pSpillBucket;
  if (pBucket == NULL) {
    return;
  }

  if (pGroup->pSpillPrev != NULL) {
    pGroup->pSpillPrev->pSpillNext = pGroup->pSpillNext;
  } else {
    pBucket->pGroupHead = pGroup->pSpillNext;
  }
  if (pGroup->pSpillNext != NULL) {
    pGroup->pSpillNext->pSpillPrev = pGroup->pSpillPrev;
  } else {
    pBucket->pGroupTail = pGroup->pSpillPrev;
  }
  pGroup->pSpillBucket = NULL;
  pGroup->pSpillPrev = NULL;
  pGroup->pSpillNext = NULL;

  if (pBucket->pGroupHead == NULL) {
    partitionRemoveSpillBucket(pInfo, pBucket);
  }
}

STATIC_EXCEPT_TEST int32_t partitionSetSpillGroupLevel(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    int32_t growLevel) {
  if (pGroup->pSpillBucket != NULL &&
      pGroup->pSpillBucket->growLevel == growLevel) {
    return TSDB_CODE_SUCCESS;
  }

  SPartitionSpillBucket* pPrev = NULL;
  SPartitionSpillBucket* pNext = pInfo->pSpillBucketHead;
  SPartitionSpillBucket* pTarget = NULL;

  while (pNext != NULL && pNext->growLevel < growLevel) {
    pPrev = pNext;
    pNext = pNext->pNext;
  }

  if (pNext != NULL && pNext->growLevel == growLevel) {
    pTarget = pNext;
  } else {
    pTarget = taosMemoryCalloc(1, sizeof(*pTarget));
    if (pTarget == NULL) {
      return terrno;
    }
    pTarget->growLevel = growLevel;
    partitionInsertSpillBucket(pInfo, pTarget, pPrev, pNext);
  }

  partitionRemoveSpillGroup(pInfo, pGroup);
  partitionAppendSpillGroup(pTarget, pGroup);
  return TSDB_CODE_SUCCESS;
}

#if defined(BUILD_TEST)
// Test-only view of the victim policy the bucket list encodes: the tail
// bucket holds the groups at the highest growth level. Production walks it
// buckets inline in partitionCollectSpillGroups, which selects several
// groups per spill instead of one.
SPartitionDataInfo* partitionSelectSpillGroup(SPartitionOperatorInfo* pInfo) {
  SPartitionSpillBucket* pBucket = pInfo->pSpillBucketTail;
  return pBucket != NULL ? pBucket->pGroupHead : NULL;
}
#endif

STATIC_EXCEPT_TEST void partitionClearSpillBuckets(
    SPartitionOperatorInfo* pInfo) {
  SPartitionSpillBucket* pBucket = pInfo->pSpillBucketHead;
  while (pBucket != NULL) {
    SPartitionDataInfo* pGroup = pBucket->pGroupHead;
    while (pGroup != NULL) {
      SPartitionDataInfo* pNextGroup = pGroup->pSpillNext;
      pGroup->pSpillBucket = NULL;
      pGroup->pSpillPrev = NULL;
      pGroup->pSpillNext = NULL;
      pGroup = pNextGroup;
    }
    SPartitionSpillBucket* pNextBucket = pBucket->pNext;
    taosMemoryFree(pBucket);
    pBucket = pNextBucket;
  }
  pInfo->pSpillBucketHead = NULL;
  pInfo->pSpillBucketTail = NULL;
}

/*
 * Called only from the pre-commit error paths of partitionFlushPendingSpills
 * and partitionSpillGroupStreamed, when page loading, serialization,
 * encryption, file I/O, size validation, or fragment registration fails.
 * It discards bytes appended since offset and restores
 * the append cursor, preserving previously committed fragments. Normal reset
 * or destruction does not call this helper; it closes the TD_FILE_AUTO_DEL
 * spill file and discards the entire temporary file.
 */
static int32_t partitionRollbackSpill(SPartitionOperatorInfo* pInfo,
                                      int64_t offset) {
  int32_t code = taosFtruncateFile(pInfo->spill.pFile, offset);
  if (code == TSDB_CODE_SUCCESS) {
    pInfo->spill.fileSize = offset;
  }
  return code;
}

typedef struct SPartitionPendingSpill {
  SPartitionDataInfo* pGroup;
  int32_t             bufferOffset;
  int32_t             length;
  int32_t             numPages;
  int64_t             numRows;
  size_t              fragmentCountBefore;
} SPartitionPendingSpill;

static int32_t partitionEnsureSpillEncoder(SPartitionOperatorInfo* pInfo) {
  int32_t code = partitionEnsureSpillFile(pInfo);
  int32_t lino = 0;
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pInfo->spill.pBlock == NULL) {
    code = createOneDataBlock(pInfo->binfo.pRes, false, &pInfo->spill.pBlock);
    QUERY_CHECK_CODE(code, lino, _end);
    code = blockDataEnsureCapacity(
        pInfo->spill.pBlock,
        partitionGetMaxPageClass(pInfo)->rowCapacity);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s %s failed at line %d, rowCapacity:%d, since %s",
         GET_TASKID(pInfo->pTaskInfo), __func__, lino,
         partitionGetMaxPageClass(pInfo)->rowCapacity, tstrerror(code));
  return code;
}

static int32_t partitionEnsureSpillBatchBuffer(SPartitionOperatorInfo* pInfo, int64_t requiredSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t newSize = 0;

  QUERY_CHECK_CONDITION(
      requiredSize >= 0 && requiredSize <= INT32_MAX, code, lino, _end,
      TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  if (pInfo->spill.batchBufSize >= requiredSize) {
    return TSDB_CODE_SUCCESS;
  }

  newSize = TMAX(64 * 1024, pInfo->spill.batchBufSize);
  while (newSize < requiredSize) {
    newSize = TMIN((int64_t)INT32_MAX, newSize * 2);
    QUERY_CHECK_CONDITION(newSize >= requiredSize || newSize != INT32_MAX,
                          code, lino, _end,
                          TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  char* pBatchBuf = taosMemoryRealloc(pInfo->spill.pBatchBuf, newSize);
  QUERY_CHECK_NULL(pBatchBuf, code, lino, _end, terrno);
  pInfo->spill.pBatchBuf = pBatchBuf;
  pInfo->spill.batchBufSize = (int32_t)newSize;
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s %s failed at line %d, requiredSize:%" PRId64 ", newSize:%"
         PRId64 ", since %s", GET_TASKID(pInfo->pTaskInfo), __func__, lino,
         requiredSize, newSize, tstrerror(code));
  return code;
}

// Owns the on-disk record framing: one length prefix followed by the encoded
// block. Both the batched and the streamed writer append through here, so the
// layout the reader parses is defined in a single place.
static int32_t partitionAppendSpillRecord(SPartitionOperatorInfo* pInfo,
                                          int32_t* pBatchLength,
                                          const char* pWriteData,
                                          int32_t writeSize) {
  int32_t code = partitionEnsureSpillBatchBuffer(
      pInfo, (int64_t)*pBatchLength + sizeof(writeSize) + writeSize);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  char* pRecord = pInfo->spill.pBatchBuf + *pBatchLength;
  memcpy(pRecord, &writeSize, sizeof(writeSize));
  memcpy(pRecord + sizeof(writeSize), pWriteData, writeSize);
  *pBatchLength += (int32_t)sizeof(writeSize) + writeSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionEncodeSpillPage(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    SPartitionPageRef pageRef, int32_t* pRows, char** ppWriteData,
    int32_t* pWriteSize) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t blockSize = 0;
  *pRows = 0;
  *ppWriteData = NULL;
  *pWriteSize = 0;

  const SPartitionPageClass* pClass = partitionGetPageClass(pInfo, pageRef);
  QUERY_CHECK_NULL(pClass, code, lino, _end,
                   TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  SDiskbasedBuf* pBuf = pClass->pBuf;
  void*          pPage = getBufPageNoLRU(pBuf, partitionPageRefId(pageRef));
  QUERY_CHECK_NULL(pPage, code, lino, _end, terrno);

  *pRows = *(int32_t*)pPage;
  if (*pRows <= 0) {
    releaseBufPage(pBuf, pPage);
    return TSDB_CODE_SUCCESS;
  }

  blockDataCleanup(pInfo->spill.pBlock);
  code = blockDataFromPagedBuf(pInfo->spill.pBlock, pPage,
                               pClass->rowCapacity);
  releaseBufPage(pBuf, pPage);
  QUERY_CHECK_CODE(code, lino, _end);

  size_t encodedSize = blockDataGetBufSize(pInfo->spill.pBlock);
  QUERY_CHECK_CONDITION(encodedSize <= INT32_MAX, code, lino, _end,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  blockSize = (int32_t)encodedSize;
  code = partitionEnsureSpillBuffer(pInfo, blockSize);
  if (code == TSDB_CODE_SUCCESS) {
    code = blockDataToBuf(pInfo->spill.pBuf, pInfo->spill.pBlock);
  }
  *ppWriteData = pInfo->spill.pBuf;
  *pWriteSize = blockSize;
  if (code == TSDB_CODE_SUCCESS && pInfo->spill.pCryptCtx != NULL) {
    code = taosEncryptSpillBlock(pInfo->spill.pCryptCtx, pInfo->spill.pBuf,
                                 blockSize, ppWriteData, pWriteSize);
  }
  QUERY_CHECK_CODE(code, lino, _end);
  QUERY_CHECK_CONDITION(
      *pWriteSize > 0 &&
          *pWriteSize <= INT32_MAX - (int32_t)sizeof(*pWriteSize),
      code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s %s failed at line %d, group:%" PRIu64 ", classIndex:%d, "
         "rows:%d, blockSize:%d, writeSize:%d, since %s",
         GET_TASKID(pInfo->pTaskInfo), __func__, lino, pGroup->groupId,
         partitionPageRefClass(pageRef), *pRows, blockSize, *pWriteSize,
         tstrerror(code));
  return code;
}

/*
 * Appends one complete group fragment to the current physical write buffer.
 * It does not modify fragment metadata or resident-page ownership. If this
 * group would cross the write limit, the partial group bytes are discarded
 * and the caller retries it through the bounded streaming path.
 */
static int32_t partitionEncodeSpillGroup(
    SPartitionOperatorInfo* pInfo, SPartitionPendingSpill* pPending,
    int32_t* pBatchLength, bool* pNeedsStreaming) {
  SPartitionDataInfo* pGroup = pPending->pGroup;
  pPending->bufferOffset = *pBatchLength;
  pPending->length = 0;
  pPending->numRows = 0;
  pPending->numPages = taosArrayGetSize(pGroup->pPageList);
  *pNeedsStreaming = false;

  int32_t writeBatchLimit = partitionGetSpillWriteBatchLimit();
  for (int32_t i = 0; i < pPending->numPages; ++i) {
    SPartitionPageRef* pPageRef = taosArrayGet(pGroup->pPageList, i);
    int32_t  rows = 0;
    char*    pWriteData = NULL;
    int32_t  writeSize = 0;
    int32_t code = partitionEncodeSpillPage(
        pInfo, pGroup, *pPageRef, &rows, &pWriteData, &writeSize);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (rows <= 0) {
      continue;
    }

    int32_t recordSize = (int32_t)sizeof(writeSize) + writeSize;
    int64_t requiredSize = (int64_t)*pBatchLength + recordSize;
    if (requiredSize > writeBatchLimit) {
      *pBatchLength = pPending->bufferOffset;
      pPending->numRows = 0;
      *pNeedsStreaming = true;
      return TSDB_CODE_SUCCESS;
    }

    code = partitionAppendSpillRecord(pInfo, pBatchLength, pWriteData,
                                      writeSize);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pPending->numRows += rows;
  }

  pPending->length = *pBatchLength - pPending->bufferOffset;
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionReservePendingFragments(
    SPartitionOperatorInfo* pInfo, SPartitionPendingSpill* pPending,
    int32_t numGroups) {
  for (int32_t i = 0; i < numGroups; ++i) {
    SPartitionDataInfo* pGroup = pPending[i].pGroup;
    if (pPending[i].length <= 0) {
      continue;
    }
    if (pGroup->fragments == NULL) {
      pGroup->fragments = taosArrayInit(4, sizeof(SPartitionFragment));
      if (pGroup->fragments == NULL) {
        qError("%s %s failed at line %d, group:%" PRIu64 ", since %s",
               GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
               pGroup->groupId, tstrerror(terrno));
        return terrno;
      }
    }
    pPending[i].fragmentCountBefore = taosArrayGetSize(pGroup->fragments);
    int32_t code = taosArrayEnsureCap(pGroup->fragments, pPending[i].fragmentCountBefore + 1);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s %s failed at line %d, group:%" PRIu64 ", since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             pGroup->groupId, tstrerror(code));
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * The fragment is already durable and registered when this runs. Continue
 * recycling after individual failures and always clear page bookkeeping so
 * committed rows cannot be replayed from both disk and memory.
 */
static int32_t partitionCommitSpillGroup(SPartitionOperatorInfo* pInfo, const SPartitionPendingSpill* pPending) {
  SPartitionDataInfo* pGroup = pPending->pGroup;
  // The fragment is committed, so the group's rows now live on disk. Keep
  // recycling past per-page failures and clear the page bookkeeping even on
  // error: a page left in pPageList would replay rows already stored in the
  // fragment, which is worse than leaking it until destruction.
  //
  // A page that fails to recycle keeps its slot in numResidentPages while
  // dropping out of every pPageList, so the budget shrinks for the rest of
  // the run and cannot be reclaimed. Repeated failures therefore end in
  // TSDB_CODE_OUT_OF_MEMORY from partitionSpillGroupBatch once no group
  // has pages left to free - bounded, but the query does not recover.
  int32_t recycleCode = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pPending->numPages; ++i) {
    SPartitionPageRef pageRef = 0;
    const SPartitionPageClass* pClass =
        partitionGetPageClassAt(pInfo, pGroup->pPageList, i, &pageRef);
    void* pPage = NULL;
    if (pClass != NULL) {
      pPage = getBufPageNoLRU(pClass->pBuf, partitionPageRefId(pageRef));
    }
    if (pPage == NULL) {
      qError("%s %s failed at line %d, group:%" PRIu64
             ", pageIndex:%d/%d, classIndex:%d, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             pGroup->groupId, i, pPending->numPages,
             partitionPageRefClass(pageRef), tstrerror(terrno));
      if (recycleCode == TSDB_CODE_SUCCESS) {
        recycleCode = terrno;
      }
      continue;
    }
    int32_t code = partitionRecycleBufPage(pClass->pBuf, pPage, i);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s %s failed at line %d, group:%" PRIu64
             ", pageIndex:%d/%d, classIndex:%d, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             pGroup->groupId, i, pPending->numPages,
             partitionPageRefClass(pageRef), tstrerror(code));
      releaseBufPage(pClass->pBuf, pPage);
      if (recycleCode == TSDB_CODE_SUCCESS) {
        recycleCode = code;
      }
      continue;
    }
    partitionDropResidentPage(pInfo, pageRef);
  }

  taosArrayClear(pGroup->pPageList);
  pInfo->spillStat.spillCount += 1;
  pInfo->spillStat.encryptedSpillCount += pInfo->spill.pCryptCtx != NULL ? 1 : 0;
  pInfo->spillStat.spillPages += pPending->numPages;
  pInfo->spillStat.spillRows += pPending->numRows;
  pInfo->spillStat.spillBytes += pPending->length;
  pInfo->spillStat.residentRows -= pGroup->residentRows;
  pGroup->residentRows = 0;
  partitionRemoveSpillGroup(pInfo, pGroup);
  return recycleCode;
}

static void partitionUndoPendingFragments(SPartitionPendingSpill* pPending, int32_t numGroups) {
  for (int32_t i = 0; i < numGroups; ++i) {
    if (pPending[i].length > 0 && taosArrayGetSize(pPending[i].pGroup->fragments) > pPending[i].fragmentCountBefore) {
      TAOS_UNUSED(taosArrayPop(pPending[i].pGroup->fragments));
    }
  }
}

/* Writes several complete group fragments in one syscall, then registers and
 * commits them individually. A pre-commit failure keeps every page resident.
 */
static int32_t partitionFlushPendingSpills(SPartitionOperatorInfo* pInfo, SPartitionPendingSpill* pPending,
                                           int32_t numGroups, int32_t batchLength) {
  int64_t startOffset = pInfo->spill.fileSize;
  int32_t code = partitionReservePendingFragments(pInfo, pPending, numGroups);
  if (code != TSDB_CODE_SUCCESS) {
    pInfo->spillStat.spillFailures += 1;
    return code;
  }

  if (batchLength > 0) {
    code = partitionWriteAt(pInfo->spill.pFile, startOffset, pInfo->spill.pBatchBuf, batchLength);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s %s failed at line %d, offset:%" PRId64
             ", length:%d, groups:%d, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__, startOffset,
             batchLength, numGroups, tstrerror(code));
      pInfo->spillStat.spillFailures += 1;
      if (partitionRollbackSpill(pInfo, startOffset) != TSDB_CODE_SUCCESS) {
        qError("failed to rollback partition spill file since %s", terrstr());
      }
      return code;
    }
  }

  for (int32_t i = 0; i < numGroups; ++i) {
    if (pPending[i].length <= 0) {
      continue;
    }
    SPartitionFragment fragment = {
        .offset = startOffset + pPending[i].bufferOffset,
        .length = pPending[i].length,
        .numRows = pPending[i].numRows,
    };
    if (taosArrayPush(pPending[i].pGroup->fragments, &fragment) == NULL) {
      code = terrno;
      qError("%s %s failed at line %d, group:%" PRIu64
             ", offset:%" PRId64 ", length:%d, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             pPending[i].pGroup->groupId, fragment.offset, fragment.length,
             tstrerror(code));
      partitionUndoPendingFragments(pPending, numGroups);
      pInfo->spillStat.spillFailures += 1;
      if (partitionRollbackSpill(pInfo, startOffset) != TSDB_CODE_SUCCESS) {
        qError("failed to rollback partition spill file since %s", terrstr());
      }
      return code;
    }
  }
  pInfo->spill.fileSize = startOffset + batchLength;

  int32_t recycleCode = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < numGroups; ++i) {
    code = partitionCommitSpillGroup(pInfo, &pPending[i]);
    if (recycleCode == TSDB_CODE_SUCCESS && code != TSDB_CODE_SUCCESS) {
      recycleCode = code;
    }
  }
  return recycleCode;
}

static bool partitionSpillGroupMayExceedWriteLimit(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup) {
  int32_t maxBlockSize = partitionGetMaxPageClass(pInfo)->pageSize;
  if (pInfo->spill.pCryptCtx != NULL) {
    int32_t maxEncodedSize = 0;
    if (taosSpillCryptEncodedLen(maxBlockSize, &maxEncodedSize) !=
        TSDB_CODE_SUCCESS) {
      return true;
    }
    maxBlockSize = maxEncodedSize;
  }

  // rowCapacity reserves the serialized block metadata within each page, so
  // page size plus the record prefix is a conservative per-page upper bound.
  int64_t maxRecordSize = (int64_t)sizeof(int32_t) + maxBlockSize;
  int64_t maxGroupSize =
      (int64_t)taosArrayGetSize(pGroup->pPageList) * maxRecordSize;
  return maxGroupSize > partitionGetSpillWriteBatchLimit();
}

static int32_t partitionWriteSpillBuffer(SPartitionOperatorInfo* pInfo,
                                         int64_t* pWriteOffset,
                                         int32_t* pBatchLength) {
  if (*pBatchLength == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = partitionWriteAt(pInfo->spill.pFile, *pWriteOffset,
                                  pInfo->spill.pBatchBuf, *pBatchLength);
  if (code == TSDB_CODE_SUCCESS) {
    *pWriteOffset += *pBatchLength;
    *pBatchLength = 0;
  }
  return code;
}

/*
 * Writes one large group in bounded chunks while keeping one fragment. Bytes
 * remain uncommitted until every chunk is written and the fragment is
 * registered, so any earlier failure can truncate the whole group safely.
 */
static int32_t partitionSpillGroupStreamed(
    SPartitionOperatorInfo* pInfo, SPartitionPendingSpill* pPending) {
  SPartitionDataInfo* pGroup = pPending->pGroup;
  int32_t writeBatchLimit = partitionGetSpillWriteBatchLimit();
  int64_t startOffset = pInfo->spill.fileSize;
  int64_t writeOffset = startOffset;
  int32_t batchLength = 0;
  int32_t code = TSDB_CODE_SUCCESS;

  pPending->bufferOffset = 0;
  pPending->length = 0;
  pPending->numRows = 0;
  pPending->numPages = taosArrayGetSize(pGroup->pPageList);

  for (int32_t i = 0; i < pPending->numPages; ++i) {
    SPartitionPageRef* pPageRef = taosArrayGet(pGroup->pPageList, i);
    int32_t  rows = 0;
    char*    pWriteData = NULL;
    int32_t  writeSize = 0;
    code = partitionEncodeSpillPage(pInfo, pGroup, *pPageRef, &rows,
                                    &pWriteData, &writeSize);
    if (code != TSDB_CODE_SUCCESS) {
      goto _rollback;
    }
    if (rows <= 0) {
      continue;
    }

    int32_t recordSize = (int32_t)sizeof(writeSize) + writeSize;
    if (pPending->length > INT32_MAX - recordSize) {
      code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      goto _rollback;
    }
    if (batchLength > 0 && recordSize > writeBatchLimit - batchLength) {
      code = partitionWriteSpillBuffer(pInfo, &writeOffset, &batchLength);
      if (code != TSDB_CODE_SUCCESS) {
        goto _rollback;
      }
    }

    code = partitionAppendSpillRecord(pInfo, &batchLength, pWriteData,
                                      writeSize);
    if (code != TSDB_CODE_SUCCESS) {
      goto _rollback;
    }
    pPending->length += recordSize;
    pPending->numRows += rows;

    if (batchLength >= writeBatchLimit) {
      code = partitionWriteSpillBuffer(pInfo, &writeOffset, &batchLength);
      if (code != TSDB_CODE_SUCCESS) {
        goto _rollback;
      }
    }
  }

  code = partitionWriteSpillBuffer(pInfo, &writeOffset, &batchLength);
  if (code != TSDB_CODE_SUCCESS) {
    goto _rollback;
  }
  code = partitionReservePendingFragments(pInfo, pPending, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _rollback;
  }
  if (pPending->length > 0) {
    SPartitionFragment fragment = {
        .offset = startOffset,
        .length = pPending->length,
        .numRows = pPending->numRows,
    };
    if (taosArrayPush(pGroup->fragments, &fragment) == NULL) {
      code = terrno;
      goto _rollback;
    }
  }

  pInfo->spill.fileSize = writeOffset;
  return partitionCommitSpillGroup(pInfo, pPending);

_rollback:
  qError("%s %s failed at line %d, group:%" PRIu64
         ", startOffset:%" PRId64 ", written:%" PRId64
         ", pendingBatch:%d, since %s", GET_TASKID(pInfo->pTaskInfo),
         __func__, __LINE__, pGroup->groupId, startOffset,
         writeOffset - startOffset, batchLength, tstrerror(code));
  partitionUndoPendingFragments(pPending, 1);
  pInfo->spillStat.spillFailures += 1;
  if (partitionRollbackSpill(pInfo, startOffset) != TSDB_CODE_SUCCESS) {
    qError("failed to rollback partition spill file since %s", terrstr());
  }
  return code;
}

// Spills exactly one group, applying the same streamed / encode+flush
// dispatch partitionSpillGroupBatch applies to each group of a batch.
STATIC_EXCEPT_TEST int32_t partitionSpillGroup(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup) {
  int32_t code = partitionEnsureSpillEncoder(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t                batchLength = 0;
  bool                   needsStreaming =
      partitionSpillGroupMayExceedWriteLimit(pInfo, pGroup);
  SPartitionPendingSpill pending = {.pGroup = pGroup};
  if (needsStreaming) {
    return partitionSpillGroupStreamed(pInfo, &pending);
  }

  code = partitionEncodeSpillGroup(pInfo, &pending, &batchLength,
                                   &needsStreaming);
  if (code != TSDB_CODE_SUCCESS) {
    pInfo->spillStat.spillFailures += 1;
    return code;
  }
  if (needsStreaming) {
    return partitionSpillGroupStreamed(pInfo, &pending);
  }
  return partitionFlushPendingSpills(pInfo, &pending, 1, batchLength);
}

// Derived rather than cached so a group that owns several maximum-size pages
// has one source of truth for its total resident bytes.
static int64_t partitionGetGroupResidentBytes(
    const SPartitionOperatorInfo* pInfo, const SPartitionDataInfo* pGroup) {
  int64_t residentBytes = 0;
  int32_t numPages = taosArrayGetSize(pGroup->pPageList);
  for (int32_t i = 0; i < numPages; ++i) {
    SPartitionPageRef pageRef = 0;
    const SPartitionPageClass* pClass =
        partitionGetPageClassAt(pInfo, pGroup->pPageList, i, &pageRef);
    if (pClass != NULL) {
      residentBytes += pClass->pageSize;
    }
  }
  return residentBytes;
}

#if defined(BUILD_TEST)
// Debug/test invariant: pInfo->residentBytes must equal the pageSize sum
// over every page reachable from pGroups[0..numGroups). Callers supply
// the group list explicitly since where the live set of groups lives
// varies with the operator's phase (pInfo->pGroupSet during ingestion,
// pInfo->sortedGroupArray after sorting, or a single detached group in a
// test fixture that touches neither). Read-only; returns false on
// mismatch instead of asserting, so the caller can report expected vs.
// actual. Does not hold once a page has failed to recycle after its
// fragment committed (see partitionCommitSpillGroup): that page stays
// charged against residentBytes forever with no group left to sum it
// from.
bool partitionCheckResidentBytesInvariant(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* const* pGroups,
    int32_t numGroups) {
  if (pInfo == NULL || (pGroups == NULL && numGroups != 0)) {
    return false;
  }
  int64_t total = 0;
  for (int32_t i = 0; i < numGroups; ++i) {
    total += partitionGetGroupResidentBytes(pInfo, pGroups[i]);
  }
  return total == pInfo->residentBytes;
}

// Debug/test invariant: pInfo->numResidentPages must equal the page count
// reachable from pGroups[0..numGroups). Same group-list contract and same
// post-recycle-failure caveat as partitionCheckResidentBytesInvariant.
bool partitionCheckResidentPagesInvariant(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* const* pGroups,
    int32_t numGroups) {
  if (pInfo == NULL || (pGroups == NULL && numGroups != 0)) {
    return false;
  }
  int32_t total = 0;
  for (int32_t i = 0; i < numGroups; ++i) {
    total += (int32_t)taosArrayGetSize(pGroups[i]->pPageList);
  }
  return total == pInfo->numResidentPages;
}

// Debug/test invariant: a group with resident pages must sit in the spill
// bucket for its last page's class (partitionSetSpillGroupLevel keeps the
// two in lockstep on every allocate/grow); a group with no resident pages
// must not hold a bucket assignment, since partitionCommitSpillGroup and
// partitionRemoveSpillGroup clear it together with pPageList.
bool partitionCheckSpillBucketLevelInvariant(
    const SPartitionOperatorInfo* pInfo, const SPartitionDataInfo* pGroup) {
  if (pInfo == NULL || pGroup == NULL) {
    return false;
  }
  int32_t numPages = taosArrayGetSize(pGroup->pPageList);
  if (numPages == 0) {
    return pGroup->pSpillBucket == NULL;
  }
  if (pGroup->pSpillBucket == NULL) {
    return false;
  }
  SPartitionPageRef pageRef = 0;
  if (partitionGetPageClassAt(pInfo, pGroup->pPageList, numPages - 1,
                              &pageRef) == NULL) {
    return false;
  }
  return pGroup->pSpillBucket->growLevel == partitionPageRefClass(pageRef);
}
#endif

STATIC_EXCEPT_TEST int64_t partitionGetSpillBatchTargetBytes(
    int64_t residentBytes) {
  if (residentBytes <= 0) {
    return 0;
  }
  return TMAX(1, residentBytes * PARTITION_SPILL_BATCH_PERCENT / 100);
}

static int32_t partitionCollectSpillGroups(SPartitionOperatorInfo* pInfo,
                                           int64_t targetBytes) {
  if (pInfo->spill.pBatchGroups == NULL) {
    pInfo->spill.pBatchGroups =
        taosArrayInit(64, sizeof(SPartitionPendingSpill));
    if (pInfo->spill.pBatchGroups == NULL) {
      qError("%s %s failed at line %d, since %s",
             GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             tstrerror(terrno));
      return terrno;
    }
  }
  taosArrayClear(pInfo->spill.pBatchGroups);

  int64_t                selectedBytes = 0;
  SPartitionSpillBucket* pBucket = pInfo->pSpillBucketTail;
  while (pBucket != NULL && selectedBytes < targetBytes) {
    SPartitionDataInfo* pGroup = pBucket->pGroupHead;
    while (pGroup != NULL && selectedBytes < targetBytes) {
      SPartitionDataInfo* pNext = pGroup->pSpillNext;
      int32_t             numPages = taosArrayGetSize(pGroup->pPageList);
      if (numPages > 0 && pGroup != pInfo->pLastGroupInfo) {
        // numPages is filled in by the encoder that later claims the group,
        // so it is not seeded here.
        SPartitionPendingSpill pending = {.pGroup = pGroup};
        if (taosArrayPush(pInfo->spill.pBatchGroups, &pending) == NULL) {
          qError("%s %s failed at line %d, group:%" PRIu64 ", since %s",
                 GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
                 pGroup->groupId, tstrerror(terrno));
          return terrno;
        }
        selectedBytes += partitionGetGroupResidentBytes(pInfo, pGroup);
      }
      pGroup = pNext;
    }
    pBucket = pBucket->pPrev;
  }

  if (selectedBytes < targetBytes) {
    qError("%s %s failed at line %d, target:%" PRId64 ", selected:%"
           PRId64 ", resident:%" PRId64, GET_TASKID(pInfo->pTaskInfo),
           __func__, __LINE__, targetBytes, selectedBytes,
           pInfo->residentBytes);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  return TSDB_CODE_SUCCESS;
}

// Selects enough highest-level groups to release 20% of resident bytes, then
// writes small group fragments together. The current input group is protected
// while another candidate can satisfy the target.
STATIC_EXCEPT_TEST int32_t partitionSpillGroupBatch(SPartitionOperatorInfo* pInfo) {
  int64_t targetBytes =
      partitionGetSpillBatchTargetBytes(pInfo->residentBytes);
  pInfo->spillStat.spillBatchCount += 1;
  if (targetBytes == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = partitionEnsureSpillEncoder(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = partitionCollectSpillGroups(pInfo, targetBytes);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t numGroups = taosArrayGetSize(pInfo->spill.pBatchGroups);
  int32_t chunkStart = 0;
  int32_t batchLength = 0;
  for (int32_t i = 0; i < numGroups; ++i) {
    SPartitionPendingSpill* pPending = taosArrayGet(pInfo->spill.pBatchGroups, i);
    bool needsStreaming =
        partitionSpillGroupMayExceedWriteLimit(pInfo, pPending->pGroup);
    if (!needsStreaming) {
      code = partitionEncodeSpillGroup(pInfo, pPending, &batchLength,
                                       &needsStreaming);
    }
    if (code != TSDB_CODE_SUCCESS) {
      pInfo->spillStat.spillFailures += 1;
      return code;
    }

    if (needsStreaming) {
      if (i > chunkStart) {
        SPartitionPendingSpill* pChunk =
            taosArrayGet(pInfo->spill.pBatchGroups, chunkStart);
        code = partitionFlushPendingSpills(pInfo, pChunk, i - chunkStart,
                                           batchLength);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      code = partitionSpillGroupStreamed(pInfo, pPending);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      chunkStart = i + 1;
      batchLength = 0;
      continue;
    }

    if (batchLength >= partitionGetSpillWriteBatchLimit() ||
        i == numGroups - 1) {
      SPartitionPendingSpill* pChunk = taosArrayGet(pInfo->spill.pBatchGroups, chunkStart);
      code = partitionFlushPendingSpills(pInfo, pChunk, i - chunkStart + 1, batchLength);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      chunkStart = i + 1;
      batchLength = 0;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// Allocates a page within the shared byte budget. At the limit it spills a
// highest-level batch, then retries once if the selected pool still cannot
// provide a page.
static int32_t partitionGetNewPage(SPartitionOperatorInfo* pInfo,
                                   int32_t classIndex, void** ppPage,
                                   SPartitionPageRef* pPageRef) {
  *ppPage = NULL;
  if (classIndex < 0 || classIndex >= pInfo->numPageClasses) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
#if defined(BUILD_TEST)
  if (tsPartitionPageAllocationFailuresRemaining > 0) {
    tsPartitionPageAllocationFailuresRemaining -= 1;
    terrno = tsPartitionPageAllocationFailure;
    return tsPartitionPageAllocationFailure;
  }
#endif

  SPartitionPageClass* pClass = &pInfo->pageClasses[classIndex];
  int32_t code = TSDB_CODE_SUCCESS;
  if (pInfo->residentBytes + pClass->pageSize > pInfo->memBudgetBytes) {
    code = partitionSpillGroupBatch(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  int32_t pageId = 0;
  *ppPage = getNewBufPage(pClass->pBuf, &pageId);
  if (*ppPage == NULL && pInfo->numResidentPages > 0) {
    code = partitionSpillGroupBatch(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    *ppPage = getNewBufPage(pClass->pBuf, &pageId);
  }
  if (*ppPage == NULL) {
    qError("%s %s failed at line %d, classIndex:%d, pageSize:%d, "
           "residentBytes:%" PRId64 ", memBudgetBytes:%" PRId64
           ", numResidentPages:%d, since %s", GET_TASKID(pInfo->pTaskInfo),
           __func__, __LINE__, classIndex, pClass->pageSize,
           pInfo->residentBytes, pInfo->memBudgetBytes,
           pInfo->numResidentPages, tstrerror(terrno));
    return terrno;
  }

  *pPageRef = partitionEncodePageRef(classIndex, pageId);
  pInfo->numResidentPages += 1;
  pInfo->residentBytes += pClass->pageSize;
  pInfo->spillStat.peakResidentPages =
      TMAX(pInfo->spillStat.peakResidentPages, pInfo->numResidentPages);
  pInfo->spillStat.peakResidentBytes = TMAX(
      pInfo->spillStat.peakResidentBytes, pInfo->residentBytes);
  return TSDB_CODE_SUCCESS;
}

static void partitionReleaseWritePage(SPartitionWritePage* pWritePage) {
  if (pWritePage->pPage != NULL) {
    releaseBufPage(pWritePage->pClass->pBuf, pWritePage->pPage);
  }
  memset(pWritePage, 0, sizeof(*pWritePage));
}

static int32_t partitionAcquireWritePage(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup,
    int32_t pendingRows, SPartitionWritePage* pWritePage) {
  const SPartitionPageClass* pClass = NULL;
  void* pPage = partitionGetWritablePageForRows(
      pInfo, pGroup, pendingRows, &pClass);
  if (pPage == NULL) {
    return terrno;
  }

  int32_t pageRows = *(int32_t*)pPage;
  // A writable page must have room for at least one more row. Full pages are
  // grown or replaced by partitionGetWritablePageForRows before returning.
  if (pageRows < 0 || pageRows >= pClass->rowCapacity) {
    qError("%s %s invalid write page, rows:%d, capacity:%d",
           GET_TASKID(pInfo->pTaskInfo), __func__, pageRows,
           pClass->rowCapacity);
    releaseBufPage(pClass->pBuf, pPage);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pWritePage->pPage = pPage;
  pWritePage->pGroup = pGroup;
  pWritePage->pClass = pClass;
  return TSDB_CODE_SUCCESS;
}

// Scan ahead only when the group can grow to a greater row capacity.
STATIC_EXCEPT_TEST bool partitionNeedsRunSize(
    const SPartitionOperatorInfo* pInfo,
    const SPartitionDataInfo* pGroup) {
  if (pInfo == NULL || pGroup == NULL) {
    return false;
  }

  int32_t numPages = taosArrayGetSize(pGroup->pPageList);
  if (numPages == 0) {
    return false;
  }

  SPartitionPageRef pageRef = 0;
  if (partitionGetPageClassAt(pInfo, pGroup->pPageList, numPages - 1,
                              &pageRef) == NULL) {
    return false;
  }

  int32_t classIndex = partitionPageRefClass(pageRef);
  if (partitionSelectGrowthClass(pInfo, classIndex, 1) < 0) {
    return false;
  }
  return pGroup->residentRows >= pInfo->pageClasses[classIndex].rowCapacity;
}

static int32_t partitionFindRunEnd(SPartitionOperatorInfo* pInfo,
                                   SSDataBlock* pBlock,
                                   int32_t startRow,
                                   int32_t numOfGroupCols) {
  int32_t endRow = startRow + 1;
  while (endRow < pBlock->info.rows &&
         groupKeyCompare(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock,
                         endRow, numOfGroupCols)) {
    endRow += 1;
  }
  return endRow;
}

STATIC_EXCEPT_TEST void doHashPartition(SOperatorInfo* pOperator,
                                        SSDataBlock*   pBlock) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*            pPendingNotLoaded = NULL;
  SPartitionWritePage     writePage = {0};
  int32_t                 numOfGroupCols = taosArrayGetSize(pInfo->pGroupCols);
  int32_t                 runEnd = 0;

  for (int32_t j = 0; j < pBlock->info.rows; ++j) {
    int32_t             len = 0;
    SPartitionDataInfo* pGroupInfo = pInfo->pLastGroupInfo;
    // Inside a scanned run the group cannot change, so skip the compare.
    // Otherwise pGroupColVals retains the previous row's key, so consecutive
    // rows reuse its group without rebuilding the key or probing the hash.
    if (j >= runEnd &&
        (pGroupInfo == NULL ||
         !groupKeyCompare(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock, j,
                          numOfGroupCols))) {
      code = recordGroupKeys(pInfo->pGroupCols, pInfo->pGroupColVals, pBlock,
                             j, pInfo->keyBuf, &len);
      QUERY_CHECK_CODE(code, lino, _end);

      code = partitionGetOrCreateGroupInfo(pInfo, &pGroupInfo, len);
      if (code != TSDB_CODE_SUCCESS) {
        // T_LONG_JMP logs before jumping and a logging failure can rewrite
        // terrno, so use the returned group-lookup error directly.
        T_LONG_JMP(pTaskInfo->env, code);
      }
      pInfo->pLastGroupInfo = pGroupInfo;
    }

    pGroupInfo->numOfRows += 1;

    // group id
    if (pGroupInfo->groupId == 0) {
      if (len == 0) {
        len = buildGroupKeys(pInfo->keyBuf, pInfo->pGroupColVals);
      }
      uint64_t keyGroupId = calcGroupId(pInfo->keyBuf, len);
      // outerEpoch stays 0 until an outer-grouped block arrives, so a
      // non-zero epoch alone marks the composite-key mode.
      if (pInfo->phasedMode && pInfo->outerEpoch != 0) {
        uint64_t fullGroupKey[] = {pInfo->outerEpoch, keyGroupId};
        pGroupInfo->groupId = calcGroupId((char*)fullGroupKey,
                                          sizeof(fullGroupKey));
      } else {
        pGroupInfo->groupId = keyGroupId;
      }
    }

    if (pBlock->info.dataLoad) {
      if (writePage.pPage != NULL &&
          (writePage.pGroup != pGroupInfo ||
           *(int32_t*)writePage.pPage >= writePage.pClass->rowCapacity)) {
        partitionReleaseWritePage(&writePage);
      }

      if (writePage.pPage == NULL) {
        if (j >= runEnd && partitionNeedsRunSize(pInfo, pGroupInfo)) {
          // Look ahead only when the result can skip growth classes. Scanning
          // every short run penalizes narrow, high-cardinality input.
          runEnd = partitionFindRunEnd(pInfo, pBlock, j, numOfGroupCols);
        }
        // Rows still unread in the current run; the callee clamps to 1 when
        // no run is known.
        code = partitionAcquireWritePage(pInfo, pGroupInfo, runEnd - j,
                                         &writePage);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      void*          pPage = writePage.pPage;
      int32_t        rowCapacity = writePage.pClass->rowCapacity;
      const int32_t* columnOffset = writePage.pClass->columnOffset;
      int32_t        pageSize = writePage.pClass->pageSize;

      // number of rows
      int32_t* rows = (int32_t*)pPage;

      size_t numOfCols = pOperator->exprSupp.numOfExprs;
      for (int32_t i = 0; i < numOfCols; ++i) {
        SExprInfo* pExpr = &pOperator->exprSupp.pExprInfo[i];
        int32_t    slotId = pExpr->base.pParam[0].pCol->slotId;
        SColumnInfoData* pSrcColInfoData = taosArrayGet(pBlock->pDataBlock, slotId);
        SColumnInfoData* pDstColInfoData = taosArrayGet(pInfo->binfo.pRes->pDataBlock, i);
        QUERY_CHECK_NULL(pSrcColInfoData, code, lino, _end, terrno);
        QUERY_CHECK_NULL(pDstColInfoData, code, lino, _end, terrno);

        int32_t bytes = pDstColInfoData->info.bytes;
        int32_t startOffset = columnOffset[i];
        int32_t reservedBytes = blockDataGetPagedColumnReservedBytes(pDstColInfoData);

        int32_t* columnLen = NULL;
        int32_t  contentLen = 0;

        if (IS_VAR_DATA_TYPE(pDstColInfoData->info.type)) {
          int32_t* offset = (int32_t*)((char*)pPage + startOffset);
          columnLen = (int32_t*)((char*)pPage + startOffset +
                                 sizeof(int32_t) * rowCapacity);
          char* data = (char*)((char*)columnLen + sizeof(int32_t));

          if (colDataIsNull_s(pSrcColInfoData, j)) {
            offset[(*rows)] = -1;
            contentLen = 0;
          } else {
            // The page cell was reserved at the declared column width; a virtual table
            // column may carry a value wider than declared (the full source value is
            // returned by design) -- copyVarLenCellChecked refuses before the copy
            // instead of overrunning the partition page.
            offset[*rows] = (*columnLen);
            char* src = colDataGetData(pSrcColInfoData, j);
            code = copyVarLenCellChecked(data + (*columnLen), pSrcColInfoData->info.type, src, reservedBytes,
                                         &contentLen);
            if (TSDB_CODE_SUCCESS != code) {
              qError("%s variable column exceeds partition page width: row:%d expr:%d slot:%d srcType:%d "
                     "srcBytes:%d dstType:%d dstBytes:%d reserved:%d, code:%s",
                     __func__, j, i, slotId, pSrcColInfoData->info.type,
                     pSrcColInfoData->info.bytes, pDstColInfoData->info.type,
                     pDstColInfoData->info.bytes, reservedBytes, tstrerror(code));
              lino = __LINE__;
              goto _end;
            }
            int32_t v = (data + (*columnLen) + contentLen - (char*)pPage);
            QUERY_CHECK_CONDITION((v > 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
          }

          QUERY_CHECK_CONDITION(
              ((*columnLen) + contentLen <= reservedBytes * rowCapacity),
              code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
        } else {
          char* bitmap = (char*)pPage + startOffset;
          columnLen = (int32_t*)((char*)pPage + startOffset +
                                 BitmapLen(rowCapacity));
          char* data = (char*)columnLen + sizeof(int32_t);

          bool isNull = colDataIsNull_f(pSrcColInfoData, j);
          if (isNull) {
            colDataSetNull_f(bitmap, (*rows));
          } else {
            memcpy(data + (*columnLen), colDataGetData(pSrcColInfoData, j), bytes);
            QUERY_CHECK_CONDITION(
                ((data + (*columnLen) + bytes - (char*)pPage) <= pageSize),
                code, lino, _end,
                TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
          }
          contentLen = bytes;
        }

        (*columnLen) += contentLen;
      }

      (*rows) += 1;
      pGroupInfo->residentRows += 1;
      pInfo->spillStat.residentRows += 1;
      pInfo->spillStat.peakResidentRows = TMAX(
          pInfo->spillStat.peakResidentRows, pInfo->spillStat.residentRows);

      setBufPageDirty(pPage, true);
    } else {
      pPendingNotLoaded = createBlockDataNotLoaded(pOperator, pBlock);
      if (pPendingNotLoaded == NULL) {
        code = terrno;
        goto _end;
      }
      if (pGroupInfo->blockForNotLoaded == NULL) {
        pGroupInfo->blockForNotLoaded = taosArrayInit(0, sizeof(SSDataBlock*));
        QUERY_CHECK_NULL(pGroupInfo->blockForNotLoaded, code, lino, _end, terrno);
        pGroupInfo->offsetForNotLoaded = 0;
      }
      pPendingNotLoaded->info.id.groupId = pGroupInfo->groupId;
      pPendingNotLoaded->info.dataLoad = 0;
      void* tmp =
          taosArrayPush(pGroupInfo->blockForNotLoaded, &pPendingNotLoaded);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      pPendingNotLoaded = NULL;
      break;
    }
  }

_end:
  partitionReleaseWritePage(&writePage);
  blockDataDestroy(pPendingNotLoaded);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

/*
 * Finds or creates a group without allocating a buffer page. Metadata-only
 * input needs the group for ordering and ownership, but stores no resident
 * rows and must not consume the page budget or trigger spill.
 */
STATIC_EXCEPT_TEST int32_t
partitionGetOrCreateGroupInfo(SPartitionOperatorInfo* pInfo,
                              SPartitionDataInfo** pGroupInfo, int32_t len) {
  SPartitionDataInfo* p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pPageListToFree = NULL;
  if (p == NULL) {  // it is a new group
    SPartitionDataInfo gi = {0};
    // A page grows in place until the maximum class. Full maximum-size pages
    // remain in this list and are compacted into one fragment when the group
    // spills.
    gi.pPageList = taosArrayInit(1, sizeof(SPartitionPageRef));
    QUERY_CHECK_NULL(gi.pPageList, code, lino, _end, terrno);

    code = taosHashPut(pInfo->pGroupSet, pInfo->keyBuf, len, &gi,
                       sizeof(SPartitionDataInfo));
    if (code == TSDB_CODE_DUP_KEY) {
      taosArrayDestroy(gi.pPageList);
      code = TSDB_CODE_SUCCESS;
    } else if (code != TSDB_CODE_SUCCESS) {
      pPageListToFree = gi.pPageList;
      QUERY_CHECK_CODE(code, lino, _end);
    }

    p = taosHashGet(pInfo->pGroupSet, pInfo->keyBuf, len);
    if (p == NULL) {
      code = terrno != TSDB_CODE_SUCCESS
                 ? terrno
                 : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      lino = __LINE__;
      goto _end;
    }
  }

  *pGroupInfo = p;
  return TSDB_CODE_SUCCESS;

_end:
  taosArrayDestroy(pPageListToFree);
  qError("%s %s failed at line %d, since %s", GET_TASKID(pInfo->pTaskInfo),
         __func__, lino, tstrerror(code));
  return code;
}

static void partitionRollbackNewPage(SPartitionOperatorInfo* pInfo,
                                     SPartitionPageRef pageRef, void* pPage) {
  const SPartitionPageClass* pClass = partitionGetPageClass(pInfo, pageRef);
  if (pClass == NULL) {
    return;
  }
  int32_t code = dBufSetBufPageRecycled(pClass->pBuf, pPage);
  if (code == TSDB_CODE_SUCCESS) {
    partitionDropResidentPage(pInfo, pageRef);
  } else {
    // Recycling failed before it changed anything, so the page is still
    // resident and only unpinned here: keep its budget slot. See
    // partitionCommitSpillGroup for why the budget shrinks instead.
    qError("%s %s failed at line %d, classIndex:%d, since %s",
           GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
           partitionPageRefClass(pageRef), tstrerror(code));
    releaseBufPage(pClass->pBuf, pPage);
  }
}

static int32_t partitionCopyPage(SPartitionOperatorInfo*    pInfo,
                                 const SPartitionPageClass* pOldClass,
                                 const void*                pOldPage,
                                 const SPartitionPageClass* pNewClass,
                                 void*                      pNewPage) {
  int32_t oldCapacity = pOldClass->rowCapacity;
  int32_t newCapacity = pNewClass->rowCapacity;
  const int32_t* oldOffset = pOldClass->columnOffset;
  const int32_t* newOffset = pNewClass->columnOffset;
  int32_t        rows = *(int32_t*)pOldPage;
  if (rows < 0 || rows > oldCapacity || rows > newCapacity) {
    qError("%s %s failed at line %d, rows:%d, oldCapacity:%d, "
           "newCapacity:%d", GET_TASKID(pInfo->pTaskInfo), __func__,
           __LINE__, rows, oldCapacity, newCapacity);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  memset(pNewPage, 0, pNewClass->pageSize);
  memcpy(pNewPage, pOldPage, sizeof(uint32_t) + sizeof(uint64_t));
  int32_t numCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pCol =
        taosArrayGet(pInfo->binfo.pRes->pDataBlock, i);
    if (pCol == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    int32_t oldMetaSize = blockDataGetPagedColumnMetaSize(pCol, oldCapacity);
    int32_t newMetaSize = blockDataGetPagedColumnMetaSize(pCol, newCapacity);
    int32_t usedMetaSize = blockDataGetPagedColumnMetaSize(pCol, rows);
    const char* pOldStart = (const char*)pOldPage + oldOffset[i];
    char*       pNewStart = (char*)pNewPage + newOffset[i];
    memcpy(pNewStart, pOldStart, usedMetaSize);

    const int32_t* pOldLength =
        (const int32_t*)(pOldStart + oldMetaSize);
    int32_t* pNewLength = (int32_t*)(pNewStart + newMetaSize);
    int32_t reserved = blockDataGetPagedColumnReservedBytes(pCol);
    if (*pOldLength < 0 || *pOldLength > reserved * oldCapacity) {
      qError("%s %s failed at line %d, col:%d, length:%d, reserved:%d, "
             "oldCapacity:%d", GET_TASKID(pInfo->pTaskInfo), __func__,
             __LINE__, i, *pOldLength, reserved, oldCapacity);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    *pNewLength = *pOldLength;
    memcpy(pNewLength + 1, pOldLength + 1, *pOldLength);
  }
  return TSDB_CODE_SUCCESS;
}

STATIC_EXCEPT_TEST int32_t partitionSelectGrowthClass(
    const SPartitionOperatorInfo* pInfo, int32_t currentClass,
    int32_t requiredRows) {
  if (pInfo == NULL || currentClass < 0 ||
      currentClass + 1 >= pInfo->numPageClasses || requiredRows <= 0) {
    return -1;
  }

  int32_t currentCapacity = pInfo->pageClasses[currentClass].rowCapacity;
  int32_t newClass = -1;
  for (int32_t i = currentClass + 1; i < pInfo->numPageClasses; ++i) {
    int32_t capacity = pInfo->pageClasses[i].rowCapacity;
    if (capacity <= currentCapacity) {
      continue;
    }
    newClass = i;
    if (capacity >= requiredRows) {
      break;
    }
  }
  return newClass;
}

static void* partitionGrowPage(SPartitionOperatorInfo* pInfo,
                               SPartitionDataInfo* pGroupInfo,
                               SPartitionPageRef* pOldRef, int32_t newClass,
                               const SPartitionPageClass** ppClass) {
  SPartitionPageRef oldRef = *pOldRef;
  int32_t oldClass = partitionPageRefClass(oldRef);
  if (newClass <= oldClass || newClass >= pInfo->numPageClasses) {
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }

  const SPartitionPageClass* pOldClass = partitionGetPageClass(pInfo, oldRef);
  if (pOldClass == NULL) {
    return NULL;
  }

  const SPartitionPageClass* pNewClass = &pInfo->pageClasses[newClass];
  if (pNewClass->rowCapacity <= pOldClass->rowCapacity) {
    qError("%s %s failed at line %d, group:%" PRIu64 ", oldClass:%d, newClass:%d, oldCapacity:%d, newCapacity:%d",
           GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__, pGroupInfo->groupId,
           oldClass, newClass, pOldClass->rowCapacity, pNewClass->rowCapacity);
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }

  void* pNewPage = NULL;
  SPartitionPageRef newRef = 0;
  int32_t code = partitionGetNewPage(pInfo, newClass, &pNewPage,
                                     &newRef);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  SDiskbasedBuf* pOldBuf = pOldClass->pBuf;
  void*          pOldPage = getBufPageNoLRU(pOldBuf,
                                            partitionPageRefId(oldRef));
  if (pOldPage == NULL) {
    code = terrno;
    qError("%s %s failed at line %d, group:%" PRIu64 ", oldClass:%d, "
           "newClass:%d, since %s", GET_TASKID(pInfo->pTaskInfo),
           __func__, __LINE__, pGroupInfo->groupId, oldClass, newClass,
           tstrerror(code));
    partitionRollbackNewPage(pInfo, newRef, pNewPage);
    terrno = code;
    return NULL;
  }

  code = partitionCopyPage(pInfo, pOldClass, pOldPage, pNewClass, pNewPage);
  if (code == TSDB_CODE_SUCCESS) {
    code = partitionSetSpillGroupLevel(pInfo, pGroupInfo, newClass);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s %s failed at line %d, group:%" PRIu64 ", newClass:%d, "
             "since %s", GET_TASKID(pInfo->pTaskInfo), __func__, __LINE__,
             pGroupInfo->groupId, newClass, tstrerror(code));
    }
  }
  if (code != TSDB_CODE_SUCCESS) {
    releaseBufPage(pOldBuf, pOldPage);
    partitionRollbackNewPage(pInfo, newRef, pNewPage);
    terrno = code;
    return NULL;
  }

  code = dBufSetBufPageRecycled(pOldBuf, pOldPage);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d, group:%" PRIu64 ", oldClass:%d, "
           "newClass:%d, since %s", GET_TASKID(pInfo->pTaskInfo),
           __func__, __LINE__, pGroupInfo->groupId, oldClass, newClass,
           tstrerror(code));
    releaseBufPage(pOldBuf, pOldPage);
    partitionRollbackNewPage(pInfo, newRef, pNewPage);
    TAOS_UNUSED(partitionSetSpillGroupLevel(pInfo, pGroupInfo, oldClass));
    terrno = code;
    return NULL;
  }

  partitionDropResidentPage(pInfo, oldRef);
  *pOldRef = newRef;
  *ppClass = pNewClass;
  pInfo->spillStat.growCount[newClass] += 1;
  return pNewPage;
}

static void* partitionAllocatePage(SPartitionOperatorInfo* pInfo,
                                   SPartitionDataInfo* pGroupInfo,
                                   int32_t classIndex,
                                   const SPartitionPageClass** ppClass) {
  void* pPage = NULL;
  SPartitionPageRef pageRef = 0;
  int32_t code = partitionGetNewPage(pInfo, classIndex, &pPage, &pageRef);
  int32_t lino = 0;
  bool    pushed = false;
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }
  memset(pPage, 0, pInfo->pageClasses[classIndex].pageSize);

  if (taosArrayPush(pGroupInfo->pPageList, &pageRef) == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _end;
  }
  pushed = true;

  code = partitionSetSpillGroupLevel(pInfo, pGroupInfo, classIndex);
  QUERY_CHECK_CODE(code, lino, _end);
  *ppClass = &pInfo->pageClasses[classIndex];
  return pPage;

_end:
  qError("%s %s failed at line %d, group:%" PRIu64 ", classIndex:%d, "
         "since %s", GET_TASKID(pInfo->pTaskInfo), __func__, lino,
         pGroupInfo->groupId, classIndex, tstrerror(code));
  if (pushed) {
    TAOS_UNUSED(taosArrayPop(pGroupInfo->pPageList));
  }
  partitionRollbackNewPage(pInfo, pageRef, pPage);
  terrno = code;
  return NULL;
}

// A full page grows directly to the smallest class that can hold the rows
// expected from the current run. If no class has greater row capacity, the
// full page remains resident and a new page of the same class is appended.
// Memory pressure can spill the group before allocation retries.
static void* partitionGetWritablePageForRows(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroupInfo,
    int32_t pendingRows, const SPartitionPageClass** ppClass) {
  *ppClass = NULL;

  if (taosArrayGetSize(pGroupInfo->pPageList) == 0) {
    return partitionAllocatePage(pInfo, pGroupInfo, 0, ppClass);
  }

  // partitionGrowPage rewrites the ref in place, so the array slot itself is
  // needed here rather than a copy of the ref.
  SPartitionPageRef* pPageRef = taosArrayGetLast(pGroupInfo->pPageList);
  if (pPageRef == NULL) {
    terrno = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    return NULL;
  }
  const SPartitionPageClass* pClass = partitionGetPageClass(pInfo, *pPageRef);
  if (pClass == NULL) {
    return NULL;
  }
  void* pPage = getBufPageNoLRU(pClass->pBuf, partitionPageRefId(*pPageRef));
  if (pPage == NULL) {
    return NULL;
  }

  int32_t pageRows = *(int32_t*)pPage;
  if (pageRows < pClass->rowCapacity) {
    *ppClass = pClass;
    return pPage;
  }
  releaseBufPage(pClass->pBuf, pPage);

  int32_t classIndex = partitionPageRefClass(*pPageRef);
  int32_t targetClass = -1;
  if (classIndex + 1 < pInfo->numPageClasses) {
    int64_t requiredRows = (int64_t)pageRows + TMAX(1, pendingRows);
    targetClass = partitionSelectGrowthClass(
        pInfo, classIndex,
        requiredRows > INT32_MAX ? INT32_MAX : (int32_t)requiredRows);
  }
  if (targetClass >= 0) {
    pPage = partitionGrowPage(pInfo, pGroupInfo, pPageRef, targetClass,
                              ppClass);
  } else {
    pPage = partitionAllocatePage(pInfo, pGroupInfo, classIndex, ppClass);
  }

  // Either path falls through to the forced spill only when the budget is
  // exhausted; every other failure belongs to the caller.
  if (pPage != NULL || terrno != TSDB_CODE_OUT_OF_MEMORY) {
    return pPage;
  }

  int32_t code = partitionSpillGroup(pInfo, pGroupInfo);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }
  pInfo->spillStat.forcedCurrentSpills += 1;
  pPage = partitionAllocatePage(pInfo, pGroupInfo, 0, ppClass);
  if (pPage != NULL) {
    terrno = TSDB_CODE_SUCCESS;
  }
  return pPage;
}

#if defined(BUILD_TEST)
// Test-only seam: the page the production writer would pin for a single row.
void* partitionGetWritablePage(SPartitionOperatorInfo* pInfo,
                               SPartitionDataInfo* pGroupInfo) {
  const SPartitionPageClass* pClass = NULL;
  return partitionGetWritablePageForRows(pInfo, pGroupInfo, 1, &pClass);
}
#endif

int32_t* setupColumnOffset(const SSDataBlock* pBlock, int32_t rowCapacity) {
  size_t   numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  int32_t* offset = taosMemoryCalloc(numOfCols, sizeof(int32_t));
  if (!offset) {
    return NULL;
  }

  offset[0] = sizeof(int32_t) +
              sizeof(uint64_t);  // the number of rows in current page, ref to SSDataBlock paged serialization format

  for (int32_t i = 0; i < numOfCols - 1; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    int32_t payloadLen = blockDataGetPagedColumnReservedBytes(pColInfoData) * rowCapacity;

    // offset segment or bitmap + content length + payload
    offset[i + 1] = blockDataGetPagedColumnMetaSize(pColInfoData, rowCapacity) +
                    sizeof(int32_t) + payloadLen + offset[i];
  }

  return offset;
}

// Releases every resource owned by a group, including its spill descriptors.
STATIC_EXCEPT_TEST void cleanupPartitionGroupInfo(SPartitionDataInfo* pGroup) {
  if (pGroup == NULL) {
    return;
  }

  if (pGroup->blockForNotLoaded != NULL) {
    for (int32_t i = 0; i < taosArrayGetSize(pGroup->blockForNotLoaded); ++i) {
      SSDataBlock** ppBlock = taosArrayGet(pGroup->blockForNotLoaded, i);
      if (ppBlock != NULL) {
        blockDataDestroy(*ppBlock);
      }
    }
    taosArrayDestroy(pGroup->blockForNotLoaded);
    pGroup->blockForNotLoaded = NULL;
    pGroup->offsetForNotLoaded = 0;
  }

  taosArrayDestroy(pGroup->pPageList);
  pGroup->pPageList = NULL;
  taosArrayDestroy(pGroup->fragments);
  pGroup->fragments = NULL;
}

// Releases the per-group resources of every group left in the set.
STATIC_EXCEPT_TEST void partitionCleanupGroupSet(SHashObj* pGroupSet) {
  void* pIter = taosHashIterate(pGroupSet, NULL);
  while (pIter != NULL) {
    cleanupPartitionGroupInfo(pIter);
    pIter = taosHashIterate(pGroupSet, pIter);
  }
}

// Releases every resource owned by the spill info; safe to call repeatedly
// or on a zero-initialized struct.
STATIC_EXCEPT_TEST void partitionFreeSpillResources(
    SPartitionSpillInfo* pSpill) {
  if (pSpill->pFile != NULL) {
    (void)taosCloseFile(&pSpill->pFile);
  }
  pSpill->fileSize = 0;
  blockDataDestroy(pSpill->pBlock);
  pSpill->pBlock = NULL;
  taosMemoryFreeClear(pSpill->pBuf);
  pSpill->bufSize = 0;
  taosMemoryFreeClear(pSpill->pBatchBuf);
  pSpill->batchBufSize = 0;
  taosArrayDestroy(pSpill->pBatchGroups);
  pSpill->pBatchGroups = NULL;
  taosDestroySpillCryptCtx(&pSpill->pCryptCtx);
}

// Sums the flush/load counters DiskbasedBuf tracks per class, across every
// constructed class, for the one-line spill summary below.
static void partitionGetPageClassIoStats(const SPartitionOperatorInfo* pInfo,
                                         int64_t* pFlushPages,
                                         int64_t* pLoadPages) {
  *pFlushPages = 0;
  *pLoadPages = 0;
  for (int32_t i = 0; i < pInfo->numPageClasses; ++i) {
    SDiskbasedBufStatis stat = getDBufStatis(pInfo->pageClasses[i].pBuf);
    *pFlushPages += stat.flushPages;
    *pLoadPages += stat.loadPages;
  }
}

// Empties every constructed class's buffer for reuse across operator
// resets, without releasing the buffer or its column-offset table (that is
// partitionDestroyPageClasses's job, at final teardown).
static void partitionClearPageClassBuffers(SPartitionOperatorInfo* pInfo) {
  for (int32_t i = 0; i < pInfo->numPageClasses; ++i) {
    clearDiskbasedBuf(pInfo->pageClasses[i].pBuf);
  }
}

static void partitionCleanupSortedGroups(SPartitionOperatorInfo* pInfo) {
  int32_t size = taosArrayGetSize(pInfo->sortedGroupArray);
  for (int32_t i = 0; i < size; ++i) {
    SPartitionDataInfo* pGroup =
        taosArrayGet(pInfo->sortedGroupArray, i);
    cleanupPartitionGroupInfo(pGroup);
  }
  taosArrayClear(pInfo->sortedGroupArray);
}

// Clears batch-owned output while retaining reusable containers and page
// classes. Shared by the per-batch reset and the full operator clear.
static void partitionClearOutputResources(SPartitionOperatorInfo* pInfo) {
  partitionCleanupSortedGroups(pInfo);
  partitionClearPageClassBuffers(pInfo);
  pInfo->numResidentPages = 0;
  pInfo->residentBytes = 0;
  pInfo->spillStat.residentRows = 0;
}

STATIC_EXCEPT_TEST void clearPartitionOperator(
    SPartitionOperatorInfo* pInfo) {
  int32_t size = taosArrayGetSize(pInfo->sortedGroupArray);
  if (pInfo->spillStat.spillCount > 0 || pInfo->spillStat.spillFailures > 0) {
    int64_t flushPages = 0;
    int64_t loadPages = 0;
    int64_t reportedFileSize = pInfo->phasedMode
                                   ? pInfo->spillStat.spillBytes
                                   : pInfo->spill.fileSize;
    partitionGetPageClassIoStats(pInfo, &flushPages, &loadPages);

    // Class 0 is the initial allocation rather than a growth, so the counts
    // start at class 1. Formatted in a loop so adding a class cannot
    // silently drop one.
    char   grows[PARTITION_MAX_PAGE_CLASSES * 24] = {0};
    size_t growsLen = 0;
    for (int32_t i = 1; i < pInfo->numPageClasses; ++i) {
      if (growsLen >= sizeof(grows)) {
        break;
      }
      growsLen += snprintf(grows + growsLen, sizeof(grows) - growsLen,
                           growsLen == 0 ? "%" PRId64 : "/%" PRId64,
                           pInfo->spillStat.growCount[i]);
    }
    qDebug("%s partition spill completed, pageSize:%d-%d, groups:%d, "
           "peakPages:%d, peakBytes:%" PRId64 ", peakRows:%" PRId64
           ", spillBatches:%d, "
           "spills:%d, spillPages:%" PRId64 ", spillRows:%" PRId64
           ", spillBytes:%" PRId64 ", fileSize:%" PRId64 ", loads:%d, "
           "loadBytes:%" PRId64 ", encryptedSpills:%d, "
           "pageFlush/load:%" PRId64 "/%" PRId64 ", failures:%d, "
           "grows:%s, forcedCurrentSpills:%d",
           GET_TASKID(pInfo->pTaskInfo), pInfo->pageClasses[0].pageSize,
           partitionGetMaxPageClass(pInfo)->pageSize, size,
           pInfo->spillStat.peakResidentPages,
           pInfo->spillStat.peakResidentBytes,
           pInfo->spillStat.peakResidentRows,
           pInfo->spillStat.spillBatchCount, pInfo->spillStat.spillCount,
           pInfo->spillStat.spillPages, pInfo->spillStat.spillRows,
           pInfo->spillStat.spillBytes, reportedFileSize,
           pInfo->spillStat.loadCount, pInfo->spillStat.loadBytes,
           pInfo->spillStat.encryptedSpillCount, flushPages, loadPages,
           pInfo->spillStat.spillFailures, grows,
           pInfo->spillStat.forcedCurrentSpills);
  }
  partitionClearOutputResources(pInfo);
  partitionFreeSpillResources(&pInfo->spill);
  memset(&pInfo->spillStat, 0, sizeof(pInfo->spillStat));
}

static int compareDataGroupInfo(const void* group1, const void* group2) {
  const SPartitionDataInfo* pGroupInfo1 = group1;
  const SPartitionDataInfo* pGroupInfo2 = group2;

  if (pGroupInfo1->groupId == pGroupInfo2->groupId) {
    return 0;
  }

  return (pGroupInfo1->groupId < pGroupInfo2->groupId) ? -1 : 1;
}

STATIC_EXCEPT_TEST SSDataBlock* buildPartitionResultForNotLoadBlock(
    SPartitionDataInfo* pGroupInfo) {
  if (pGroupInfo->blockForNotLoaded && pGroupInfo->offsetForNotLoaded < pGroupInfo->blockForNotLoaded->size) {
    SSDataBlock** pBlock = taosArrayGet(pGroupInfo->blockForNotLoaded, pGroupInfo->offsetForNotLoaded);
    if (!pBlock) {
      return NULL;
    }
    pGroupInfo->offsetForNotLoaded++;
    return *pBlock;
  }
  return NULL;
}

// Clears the per-fragment read cursor fields; fragmentIndex itself is
// advanced or rewound separately by each caller.
static void partitionResetFragmentCursor(SPartitionSpillInfo* pSpill) {
  pSpill->fragmentReadOffset = 0;
  pSpill->fragmentDecodedRows = 0;
}

static void partitionResetSpillBatchResources(SPartitionSpillInfo* pSpill) {
  if (pSpill->pFile != NULL) {
    (void)taosCloseFile(&pSpill->pFile);
  }
  pSpill->fileSize = 0;
  taosDestroySpillCryptCtx(&pSpill->pCryptCtx);
  blockDataCleanup(pSpill->pBlock);
  taosArrayClear(pSpill->pBatchGroups);
  pSpill->fragmentIndex = 0;
  partitionResetFragmentCursor(pSpill);
}

/*
 * Loads the next block from the current fragment and advances the read
 * cursor. A fragment's first read primes the cursor from the in-memory
 * fragment record; decoding and I/O errors are propagated to the caller.
 */
STATIC_EXCEPT_TEST int32_t partitionReadFragmentBlock(
    SPartitionOperatorInfo* pInfo, SPartitionDataInfo* pGroup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t blockOffset = 0;
  int64_t blockDataOffset = 0;
  int32_t blockSize = 0;
  int32_t plainSize = 0;
  int32_t blockRows = 0;
  int64_t decodedRows = 0;
  bool    lastBlock = false;

  SPartitionFragment* pFragment =
      taosArrayGet(pGroup->fragments, pInfo->spill.fragmentIndex);
  QUERY_CHECK_CONDITION(
      pFragment != NULL && pInfo->spill.pFile != NULL &&
          pInfo->spill.pBlock != NULL,
      code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  QUERY_CHECK_CONDITION(
      pFragment->offset >= 0 && pFragment->length > 0 &&
          pFragment->offset <= INT64_MAX - pFragment->length,
      code, lino, _end, TSDB_CODE_FILE_CORRUPTED);

  // Small fragments are read once into spill.pBuf; every block of such a
  // fragment is then parsed from memory instead of from the file.
  bool fragmentBuffered =
      pFragment->length <= PARTITION_SPILL_PREFETCH_LIMIT;

  if (pInfo->spill.fragmentDecodedRows == 0) {
    pInfo->spill.fragmentReadOffset = pFragment->offset;
    pInfo->spillStat.loadCount += 1;
    pInfo->spillStat.loadBytes += pFragment->length;
    if (fragmentBuffered) {
      code = partitionEnsureSpillBuffer(pInfo, pFragment->length);
      QUERY_CHECK_CODE(code, lino, _end);
      code = partitionReadAt(pInfo->spill.pFile, pFragment->offset,
                             pInfo->spill.pBuf, pFragment->length);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  int64_t fragmentEnd = pFragment->offset + pFragment->length;
  char*   pBufferedData = NULL;
  blockOffset = pInfo->spill.fragmentReadOffset;
  if (fragmentBuffered) {
    int64_t bufferOffset = blockOffset - pFragment->offset;
    QUERY_CHECK_CONDITION(
        bufferOffset >= 0 &&
            bufferOffset + (int32_t)sizeof(blockSize) <= pFragment->length,
        code, lino, _end, TSDB_CODE_FILE_CORRUPTED);
    memcpy(&blockSize, pInfo->spill.pBuf + bufferOffset, sizeof(blockSize));
    pBufferedData = pInfo->spill.pBuf + bufferOffset + sizeof(blockSize);
  } else {
    code = partitionReadAt(pInfo->spill.pFile, blockOffset, &blockSize,
                           sizeof(blockSize));
    QUERY_CHECK_CODE(code, lino, _end);
  }

  blockDataOffset = blockOffset + sizeof(blockSize);
  QUERY_CHECK_CONDITION(
      blockSize > 0 && blockDataOffset <= fragmentEnd &&
          blockSize <= fragmentEnd - blockDataOffset,
      code, lino, _end, TSDB_CODE_FILE_CORRUPTED);

  // The crypt context is created with the file and outlives every read, so
  // its presence alone decides whether the fragment's blocks are encrypted.
  bool  encrypted = pInfo->spill.pCryptCtx != NULL;
  char* pReadData = NULL;
  if (encrypted) {
    code = taosPrepareSpillBlockRead(pInfo->spill.pCryptCtx, blockSize,
                                     &pReadData);
    if (code == TSDB_CODE_SUCCESS && fragmentBuffered) {
      memcpy(pReadData, pBufferedData, blockSize);
    }
  } else if (fragmentBuffered) {
    pReadData = pBufferedData;
  } else {
    code = partitionEnsureSpillBuffer(pInfo, blockSize);
    pReadData = pInfo->spill.pBuf;
  }
  QUERY_CHECK_CODE(code, lino, _end);
  if (!fragmentBuffered) {
    code = partitionReadAt(pInfo->spill.pFile, blockDataOffset, pReadData,
                           blockSize);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pInfo->spill.fragmentReadOffset = blockDataOffset + blockSize;

  char* pBlockData = pReadData;
  plainSize = blockSize;
  if (encrypted) {
    code = taosDecryptSpillBlock(pInfo->spill.pCryptCtx, pReadData, blockSize,
                                 &pBlockData, &plainSize);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  // A plainSize of zero (or any size below the fixed prefix) is corrupt, so
  // no separate non-positive check is needed after decryption.
  int32_t numCols = taosArrayGetSize(pInfo->binfo.pRes->pDataBlock);
  QUERY_CHECK_CONDITION(
      plainSize >= (int32_t)(sizeof(uint32_t) + numCols * sizeof(int32_t)),
      code, lino, _end, TSDB_CODE_FILE_CORRUPTED);

  // Validate the block's leading row count before decoding. Every block was
  // serialized from one page, so rowCapacity bounds it; the fragment's
  // recorded row total must be reached exactly at the last block - the one
  // whose bytes end the fragment - and not before, which also makes a
  // separate end-of-fragment reconciliation unnecessary. Structural bounds
  // within the buffer are enforced by blockDataFromBufBounded below.
  blockRows = *(int32_t*)pBlockData;
  lastBlock = pInfo->spill.fragmentReadOffset >= fragmentEnd;
  decodedRows = pInfo->spill.fragmentDecodedRows + blockRows;
  QUERY_CHECK_CONDITION(
      blockRows > 0 &&
          blockRows <= partitionGetMaxPageClass(pInfo)->rowCapacity &&
          (lastBlock ? decodedRows == pFragment->numRows
                     : decodedRows < pFragment->numRows),
      code, lino, _end, TSDB_CODE_FILE_CORRUPTED);

  blockDataCleanup(pInfo->binfo.pRes);
  // Spill records keep the full column layout. Decode every column so a
  // reserved slot cannot shift the cursor, then restore its output schema.
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pInfo->binfo.pRes->pDataBlock, i);
    pCol->info.noData = false;
  }
  code = blockDataFromBufBounded(pInfo->binfo.pRes, pBlockData, plainSize);
  for (int32_t i = 0; i < numCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pInfo->binfo.pRes->pDataBlock, i);
    SColumnInfoData* pEncoded = taosArrayGet(
        pInfo->spill.pBlock->pDataBlock, i);
    pCol->info.noData = pEncoded->info.noData;
  }
  QUERY_CHECK_CODE(code, lino, _end);
  pInfo->binfo.pRes->info.id.groupId = pGroup->groupId;
  pInfo->binfo.pRes->info.dataLoad = 1;

  pInfo->spill.fragmentDecodedRows = decodedRows;
  if (lastBlock) {
    pInfo->spill.fragmentIndex += 1;
    partitionResetFragmentCursor(&pInfo->spill);
  }
  return TSDB_CODE_SUCCESS;

_end:
  qError("%s %s failed at line %d, group:%" PRIu64 ", fragmentIndex:%d, "
         "blockOffset:%" PRId64 ", blockDataOffset:%" PRId64
         ", blockSize:%d, plainSize:%d, blockRows:%d, decodedRows:%"
         PRId64 ", lastBlock:%d, since %s", GET_TASKID(pInfo->pTaskInfo),
         __func__, lino, pGroup->groupId, pInfo->spill.fragmentIndex,
         blockOffset, blockDataOffset, blockSize, plainSize, blockRows,
         decodedRows, lastBlock, tstrerror(code));
  return code;
}

/*
 * Produces blocks for each sorted group in spill order: persisted fragments,
 * remaining resident pages, then metadata-only blocks. Cursor state is reset
 * as groups and fragments are exhausted.
 */
static SSDataBlock* partitionLoadNextBlock(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  while (true) {
    if (pInfo->groupIndex < 0) {
      pInfo->groupIndex = 0;
      pInfo->pageIndex = 0;
      pInfo->spill.fragmentIndex = 0;
    }
    if (pInfo->groupIndex >= taosArrayGetSize(pInfo->sortedGroupArray)) {
      if (pInfo->phasedMode) {
        return NULL;
      }
      setOperatorCompleted(pOperator);
      clearPartitionOperator(pInfo);
      return NULL;
    }

    SPartitionDataInfo* pGroup =
        taosArrayGet(pInfo->sortedGroupArray, pInfo->groupIndex);
    if (pGroup == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }

    if (pInfo->spill.fragmentIndex < taosArrayGetSize(pGroup->fragments)) {
      int32_t code = partitionReadFragmentBlock(pInfo, pGroup);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      pInfo->orderedRows = 0;
      return pInfo->binfo.pRes;
    }

    if (pInfo->pageIndex < taosArrayGetSize(pGroup->pPageList)) {
      const SPartitionPageClass* pClass = NULL;
      void* pPage = partitionPinPageAt(pInfo, pGroup->pPageList,
                                       pInfo->pageIndex, &pClass);
      if (pPage == NULL) {
        int32_t jmpCode = terrno;
        T_LONG_JMP(pTaskInfo->env, jmpCode);
      }
      pInfo->pageIndex += 1;
      if (*(int32_t*)pPage == 0) {
        releaseBufPage(pClass->pBuf, pPage);
        continue;
      }

      int32_t code = blockDataEnsureCapacity(pInfo->binfo.pRes,
                                             pClass->rowCapacity);
      if (code == TSDB_CODE_SUCCESS) {
        code = blockDataFromPagedBuf(pInfo->binfo.pRes, pPage,
                                     pClass->rowCapacity);
      }
      releaseBufPage(pClass->pBuf, pPage);
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
      pInfo->binfo.pRes->info.id.groupId = pGroup->groupId;
      pInfo->binfo.pRes->info.dataLoad = 1;
      pInfo->orderedRows = 0;
      return pInfo->binfo.pRes;
    }

    SSDataBlock* pNotLoaded = buildPartitionResultForNotLoadBlock(pGroup);
    if (pNotLoaded != NULL) {
      return pNotLoaded;
    }

    pInfo->groupIndex += 1;
    pInfo->pageIndex = 0;
    pInfo->spill.fragmentIndex = 0;
    partitionResetFragmentCursor(&pInfo->spill);
  }
}

static SSDataBlock* buildPartitionResult(SOperatorInfo* pOperator) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->remainRows == 0) {
    blockDataCleanup(pInfo->binfo.pRes);
    SSDataBlock* pBlock = partitionLoadNextBlock(pOperator);
    if (pBlock == NULL || pBlock != pInfo->binfo.pRes) {
      return pBlock;
    }
  } else if (pInfo->pOrderInfoArr == NULL) {
    qError("Exception, remainRows not zero, but pOrderInfoArr is NULL");
  }

  if (pInfo->pOrderInfoArr) {
    pInfo->binfo.pRes->info.rows += pInfo->remainRows;
    code = blockDataTrimFirstRows(pInfo->binfo.pRes, pInfo->orderedRows);
    QUERY_CHECK_CODE(code, lino, _end);
    pInfo->orderedRows = blockDataGetSortedRows(pInfo->binfo.pRes, pInfo->pOrderInfoArr);
    pInfo->remainRows = pInfo->binfo.pRes->info.rows - pInfo->orderedRows;
    pInfo->binfo.pRes->info.rows = pInfo->orderedRows;
  }

  code = blockDataUpdateTsWindow(pInfo->binfo.pRes, 0);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  return pInfo->binfo.pRes;
}

static int32_t partitionSetOutputOrder(SPartitionOperatorInfo* pInfo) {
  if (!pInfo->phasedMode || pInfo->pOrderInfoArr == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SBlockOrderInfo* pOrder = taosArrayGet(pInfo->pOrderInfoArr, 0);
  if (pOrder == NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  pOrder->order = pInfo->curPhase == SCAN_PHASE_PREV
                      ? ORDER_DESC
                      : ORDER_ASC;
  pOrder->pColData = NULL;
  pOrder->compFn = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t partitionPrepareOutput(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SArray* pOldGroups = pInfo->sortedGroupArray;
  if (pOldGroups != NULL && taosArrayGetSize(pOldGroups) != 0) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t code = blockDataEnsureCapacity(pInfo->binfo.pRes, 4096);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = partitionSetOutputOrder(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t numGroups = taosHashGetSize(pInfo->pGroupSet);
  bool    newGroups = pOldGroups == NULL;
  SArray* pGroups = pOldGroups;
  if (newGroups) {
    pGroups = taosArrayInit(numGroups, sizeof(SPartitionDataInfo));
    if (pGroups == NULL) {
      return terrno;
    }
  } else {
    code = taosArrayEnsureCap(pGroups, numGroups);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  void* pGroupIter = taosHashIterate(pInfo->pGroupSet, NULL);
  while (pGroupIter != NULL) {
    SPartitionDataInfo group = *(SPartitionDataInfo*)pGroupIter;
    group.pSpillBucket = NULL;
    group.pSpillPrev = NULL;
    group.pSpillNext = NULL;
    if (taosArrayPush(pGroups, &group) == NULL) {
      code = terrno;
      taosHashCancelIterate(pInfo->pGroupSet, pGroupIter);
      goto _error;
    }
#if defined(BUILD_TEST)
    if (tsPartitionPrepareOutputFailure != TSDB_CODE_SUCCESS) {
      code = tsPartitionPrepareOutputFailure;
      tsPartitionPrepareOutputFailure = TSDB_CODE_SUCCESS;
      taosHashCancelIterate(pInfo->pGroupSet, pGroupIter);
      goto _error;
    }
#endif
    pGroupIter = taosHashIterate(pInfo->pGroupSet, pGroupIter);
  }

  taosArraySort(pGroups, compareDataGroupInfo);
  pInfo->sortedGroupArray = pGroups;
  pInfo->groupIndex = -1;
  partitionClearSpillBuckets(pInfo);
  pInfo->pLastGroupInfo = NULL;
  taosHashClear(pInfo->pGroupSet);

  pOperator->status = OP_RES_TO_RETURN;
  return TSDB_CODE_SUCCESS;

_error:
  taosArrayClear(pGroups);
  if (newGroups) {
    taosArrayDestroy(pGroups);
  }
  return code;
}

static int32_t partitionProcessInputBlock(SOperatorInfo* pOperator,
                                          SSDataBlock* pBlock) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
  if (pInfo->scalarSup.pExprInfo != NULL) {
    int32_t code = projectApplyFunctions(
        pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
        pInfo->scalarSup.numOfExprs, NULL,
        GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  terrno = TSDB_CODE_SUCCESS;
  doHashPartition(pOperator, pBlock);
  return terrno;
}

static int32_t partitionHoldPhaseBlock(SPartitionOperatorInfo* pInfo,
                                       const SSDataBlock* pBlock) {
  if (pInfo->pHeldBlock != NULL) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  int32_t code = createOneDataBlock(pBlock, true, &pInfo->pHeldBlock);
  if (code == TSDB_CODE_SUCCESS) {
    pInfo->phaseState = PARTITION_PHASE_DRAINED;
  }
  return code;
}

static int32_t partitionValidateInputCursor(
    SPartitionOperatorInfo* pInfo, const SSDataBlock* pBlock,
    bool* pOuterBoundary) {
  uint64_t inputOuterEpoch = pBlock->info.id.groupId;
  *pOuterBoundary = false;

  if (!pInfo->outerEpochSet) {
    pInfo->outerEpoch = inputOuterEpoch;
    pInfo->outerEpochSet = true;
  } else if (inputOuterEpoch < pInfo->outerEpoch) {
    return TSDB_CODE_PLAN_INTERNAL_ERROR;
  } else {
    // A new epoch restarts the phase sequence, so validate against MAIN.
    *pOuterBoundary = inputOuterEpoch > pInfo->outerEpoch;
  }

  return interpValidatePhaseTransition(
      *pOuterBoundary ? SCAN_PHASE_MAIN : pInfo->curPhase,
      pBlock->info.scanPhase);
}

static int32_t partitionCollectPhasedBatch(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;

  while (pInfo->phaseState == PARTITION_PHASE_READING) {
    bool ownedBlock = pInfo->pHeldBlock != NULL;
    SSDataBlock* pBlock = pInfo->pHeldBlock;
    if (!ownedBlock) {
      pBlock = getNextBlockFromDownstream(pOperator, 0);
    }

    if (pBlock == NULL) {
      pInfo->inputDone = true;
      pInfo->phaseState = PARTITION_PHASE_DRAINED;
      return TSDB_CODE_SUCCESS;
    }

    if (pBlock->info.dataLoad != 1) {
      if (ownedBlock) {
        blockDataDestroy(pInfo->pHeldBlock);
        pInfo->pHeldBlock = NULL;
      }
      return TSDB_CODE_PLAN_INTERNAL_ERROR;
    }

    int8_t inputPhase = pBlock->info.scanPhase;
    bool outerBoundary = false;
    int32_t code = partitionValidateInputCursor(
        pInfo, pBlock, &outerBoundary);
    if (code != TSDB_CODE_SUCCESS) {
      if (ownedBlock) {
        blockDataDestroy(pInfo->pHeldBlock);
        pInfo->pHeldBlock = NULL;
      }
      return code;
    }
    if (outerBoundary || inputPhase > pInfo->curPhase) {
      if (ownedBlock) {
        blockDataDestroy(pInfo->pHeldBlock);
        pInfo->pHeldBlock = NULL;
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      return partitionHoldPhaseBlock(pInfo, pBlock);
    }

    code = partitionProcessInputBlock(pOperator, pBlock);
    if (ownedBlock) {
      blockDataDestroy(pInfo->pHeldBlock);
      pInfo->pHeldBlock = NULL;
    }
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pInfo->curPhase != SCAN_PHASE_MAIN &&
        ++pInfo->batchBlockCount >= PARTITION_SIDE_BATCH_BLOCKS) {
      pInfo->phaseState = PARTITION_PHASE_BATCH_DRAINED;
      return TSDB_CODE_SUCCESS;
    }
  }

  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

static void partitionClearPhasedBatch(SPartitionOperatorInfo* pInfo) {
  partitionClearSpillBuckets(pInfo);
  partitionCleanupGroupSet(pInfo->pGroupSet);
  taosHashClear(pInfo->pGroupSet);
  pInfo->pLastGroupInfo = NULL;
  partitionClearOutputResources(pInfo);
  partitionResetSpillBatchResources(&pInfo->spill);

  pInfo->groupIndex = 0;
  pInfo->pageIndex = 0;
  pInfo->remainRows = 0;
  pInfo->orderedRows = 0;
  blockDataCleanup(pInfo->binfo.pRes);
}

static int32_t partitionFinishPhasedDrain(SOperatorInfo* pOperator) {
  SPartitionOperatorInfo* pInfo = pOperator->info;
  EPartitionPhaseState drainedState = pInfo->phaseState;

  partitionClearPhasedBatch(pInfo);
  pOperator->status = OP_OPENED;
  pInfo->batchBlockCount = 0;

  if (drainedState == PARTITION_PHASE_BATCH_DRAINED) {
    pInfo->phaseState = PARTITION_PHASE_READING;
    return TSDB_CODE_SUCCESS;
  }
  if (drainedState != PARTITION_PHASE_DRAINED) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  if (pInfo->pHeldBlock != NULL) {
    uint64_t nextOuterEpoch = pInfo->pHeldBlock->info.id.groupId;
    int8_t nextPhase = pInfo->pHeldBlock->info.scanPhase;
    if (nextOuterEpoch < pInfo->outerEpoch ||
        nextPhase < SCAN_PHASE_MAIN || nextPhase > SCAN_PHASE_PREV) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    if (nextOuterEpoch == pInfo->outerEpoch &&
        nextPhase <= pInfo->curPhase) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pInfo->outerEpoch = nextOuterEpoch;
    pInfo->curPhase = nextPhase;
    pInfo->phaseState = PARTITION_PHASE_READING;
    return TSDB_CODE_SUCCESS;
  }

  if (!pInfo->inputDone) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  pInfo->phaseState = PARTITION_PHASE_DONE;
  setOperatorCompleted(pOperator);
  clearPartitionOperator(pInfo);
  return TSDB_CODE_SUCCESS;
}

STATIC_EXCEPT_TEST int32_t phasedHashPartitionNext(
    SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  QRY_PARAM_CHECK(ppRes);

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SPartitionOperatorInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  *ppRes = NULL;

  detachOperatorRoutingParam(pOperator);
  if (pOperator->status == OP_EXEC_DONE ||
      pInfo->phaseState == PARTITION_PHASE_DONE) {
    return TSDB_CODE_SUCCESS;
  }

  while (true) {
    if (pOperator->status == OP_RES_TO_RETURN) {
      SSDataBlock* pBlock = buildPartitionResult(pOperator);
      if (pBlock != NULL) {
        pBlock->info.scanPhase = pInfo->curPhase;
        pBlock->info.id.baseGId = pInfo->outerEpoch;
        *ppRes = pBlock;
        return TSDB_CODE_SUCCESS;
      }

      code = partitionFinishPhasedDrain(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);
      if (pInfo->phaseState == PARTITION_PHASE_DONE) {
        return TSDB_CODE_SUCCESS;
      }
      continue;
    }

    code = partitionCollectPhasedBatch(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
    code = partitionPrepareOutput(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  qError("%s failed at line %d since %s", __func__, lino,
         tstrerror(code));
  pTaskInfo->code = code;
  T_LONG_JMP(pTaskInfo->env, code);
  return code;
}

static int32_t hashPartitionNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SPartitionOperatorInfo* pInfo = pOperator->info;

  if (pOperator->status == OP_RES_TO_RETURN) {
    (*ppRes) = buildPartitionResult(pOperator);
    return code;
  }

  while (1) {
    SSDataBlock* pBlock = getNextBlockFromDownstream(pOperator, 0);
    if (pBlock == NULL) {
      break;
    }

    pInfo->binfo.pRes->info.scanFlag = pBlock->info.scanFlag;
    // there is an scalar expression that needs to be calculated right before apply the group aggregation.
    if (pInfo->scalarSup.pExprInfo != NULL) {
      code =
          projectApplyFunctions(pInfo->scalarSup.pExprInfo, pBlock, pBlock, pInfo->scalarSup.pCtx,
                                pInfo->scalarSup.numOfExprs, NULL, GET_STM_RTINFO(pOperator->pTaskInfo), pOperator->pTaskInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    terrno = TSDB_CODE_SUCCESS;
    doHashPartition(pOperator, pBlock);
    if (terrno != TSDB_CODE_SUCCESS) {  // group by json error
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  code = partitionPrepareOutput(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = buildPartitionResult(pOperator);
  return code;
}

// Companion to partitionInitPageClasses: frees every constructed class's
// buffer and column-offset table. numPageClasses counts classes whose
// buffers were actually created, so a failed init leaves the remainder
// untouched and needs no cleanup or NULL guard here either. Resets
// numPageClasses to 0 so a second call is a no-op.
static void partitionDestroyPageClasses(SPartitionOperatorInfo* pInfo) {
  for (int32_t i = 0; i < pInfo->numPageClasses; ++i) {
    taosMemoryFreeClear(pInfo->pageClasses[i].columnOffset);
    destroyDiskbasedBuf(pInfo->pageClasses[i].pBuf);
    pInfo->pageClasses[i].pBuf = NULL;
  }
  pInfo->numPageClasses = 0;
}

STATIC_EXCEPT_TEST void destroyPartitionOperatorInfo(void* param) {
  SPartitionOperatorInfo* pInfo = (SPartitionOperatorInfo*)param;
  blockDataDestroy(pInfo->pHeldBlock);
  pInfo->pHeldBlock = NULL;
  cleanupBasicInfo(&pInfo->binfo);
  taosArrayDestroy(pInfo->pGroupCols);

  for (int i = 0; i < taosArrayGetSize(pInfo->pGroupColVals); i++) {
    SGroupKeys key = *(SGroupKeys*)taosArrayGet(pInfo->pGroupColVals, i);
    taosMemoryFree(key.pData);
  }

  taosArrayDestroy(pInfo->pGroupColVals);
  taosMemoryFree(pInfo->keyBuf);

  partitionCleanupSortedGroups(pInfo);
  taosArrayDestroy(pInfo->sortedGroupArray);

  partitionClearSpillBuckets(pInfo);
  partitionCleanupGroupSet(pInfo->pGroupSet);
  taosHashCleanup(pInfo->pGroupSet);
  cleanupExprSupp(&pInfo->scalarSup);
  partitionDestroyPageClasses(pInfo);
  partitionFreeSpillResources(&pInfo->spill);
  taosArrayDestroy(pInfo->pOrderInfoArr);
  taosMemoryFreeClear(param);
}

STATIC_EXCEPT_TEST int32_t resetPartitionOperState(SOperatorInfo* pOper) {
  SPartitionOperatorInfo* pInfo = pOper->info;
  SExecTaskInfo*           pTaskInfo = pOper->pTaskInfo;
  SPartitionPhysiNode* pPhynode = (SPartitionPhysiNode*)pOper->pPhyNode;
  resetBasicOperatorState(&pInfo->binfo);

  int32_t code = resetExprSupp(&pInfo->scalarSup, pTaskInfo, pPhynode->pExprs, NULL,
    &pTaskInfo->storageAPI.functionStore);

  clearPartitionOperator(pInfo);

  partitionClearSpillBuckets(pInfo);
  partitionCleanupGroupSet(pInfo->pGroupSet);
  pInfo->pLastGroupInfo = NULL;
  taosHashClear(pInfo->pGroupSet);

  taosArrayDestroy(pInfo->sortedGroupArray);
  pInfo->sortedGroupArray = NULL;

  pInfo->groupIndex = 0;
  pInfo->pageIndex = 0;
  pInfo->spill.fragmentIndex = 0;
  partitionResetFragmentCursor(&pInfo->spill);
  pInfo->remainRows = 0;
  pInfo->orderedRows = 0;
  blockDataDestroy(pInfo->pHeldBlock);
  pInfo->pHeldBlock = NULL;
  pInfo->curPhase = SCAN_PHASE_MAIN;
  pInfo->outerEpoch = 0;
  pInfo->outerEpochSet = false;
  pInfo->phaseState = PARTITION_PHASE_READING;
  pInfo->batchBlockCount = 0;
  pInfo->inputDone = false;
  return 0;
}

static int32_t partitionInitPageClasses(SPartitionOperatorInfo* pInfo,
                                        int32_t basePageSize,
                                        const char* id) {
  int32_t maxPageSize = TMAX(basePageSize, PARTITION_MAX_PAGE_SIZE);
  int32_t pageSize = basePageSize;

  while (pInfo->numPageClasses < PARTITION_MAX_PAGE_CLASSES) {
    int32_t classIndex = pInfo->numPageClasses;
    SPartitionPageClass* pClass = &pInfo->pageClasses[classIndex];
    // Each pool gets the whole budget on purpose: pInfo->memBudgetBytes is
    // the single live limit, and a tighter per-pool cap would let the buffer
    // flush pages behind our back and desync residentBytes.
    int32_t code = createDiskbasedBuf(&pClass->pBuf, pageSize,
                                      PARTITION_DEFAULT_BUF_SIZE, id,
                                      tsTempDir);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->numPageClasses = classIndex + 1;

    pClass->pageSize = pageSize;
    pClass->rowCapacity = getPartitionPageRowCapacity(
        pInfo->binfo.pRes, pageSize,
        blockDataGetSerialMetaSize(
            taosArrayGetSize(pInfo->binfo.pRes->pDataBlock)));
    if (pClass->rowCapacity <= 0) {
      return pClass->rowCapacity < 0
                 ? terrno
                 : TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pClass->columnOffset =
        setupColumnOffset(pInfo->binfo.pRes, pClass->rowCapacity);
    if (pClass->columnOffset == NULL) {
      return terrno;
    }

    if (pageSize >= maxPageSize) {
      break;
    }
    pageSize = TMIN(pageSize * 2, maxPageSize);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t createPartitionOperatorInfo(SOperatorInfo* downstream, SPartitionPhysiNode* pPartNode,
                                           SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  SPartitionOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(SPartitionOperatorInfo));
  SOperatorInfo*          pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = code = terrno;
    goto _error;
  }
  initOperatorCostInfo(pOperator);
  pInfo->pTaskInfo = pTaskInfo;
  pInfo->memBudgetBytes = PARTITION_DEFAULT_BUF_SIZE;
  pInfo->phasedMode = pPartNode->phasedMode;
  pInfo->curPhase = SCAN_PHASE_MAIN;
  pInfo->phaseState = PARTITION_PHASE_READING;

  pOperator->pPhyNode = pPartNode;
  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = NULL;
  code = createExprInfo(pPartNode->pTargets, NULL, &pExprInfo, &numOfCols);
  QUERY_CHECK_CODE(code, lino, _error);
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->exprSupp.pExprInfo = pExprInfo;

  pInfo->pGroupCols = makeColumnArrayFromList(pPartNode->pPartitionKeys);

  if (pPartNode->needBlockOutputTsOrder) {
    SBlockOrderInfo order = {.order = ORDER_ASC, .pColData = NULL, .nullFirst = false, .slotId = pPartNode->tsSlotId};
    pInfo->pOrderInfoArr = taosArrayInit(1, sizeof(SBlockOrderInfo));
    if (!pInfo->pOrderInfoArr) {
      pTaskInfo->code = terrno;
      goto _error;
    }

    void* tmp = taosArrayPush(pInfo->pOrderInfoArr, &order);
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);
  }

  if (pPartNode->pExprs != NULL) {
    int32_t    num = 0;
    SExprInfo* pExprInfo1 = NULL;
    code = createExprInfo(pPartNode->pExprs, NULL, &pExprInfo1, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    code = initExprSupp(&pInfo->scalarSup, pExprInfo1, num, &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  _hash_fn_t hashFn = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  pInfo->pGroupSet = taosHashInit(100, hashFn, false, HASH_NO_LOCK);
  if (pInfo->pGroupSet == NULL) {
    goto _error;
  }
  int32_t pageSize = 0;

  pInfo->binfo.pRes = createDataBlockFromDescNode(pPartNode->node.pOutputDataBlockDesc);
  QUERY_CHECK_NULL(pInfo->binfo.pRes, code, lino, _error, terrno);
  blockDataGetRowSize(pInfo->binfo.pRes);
  code = partitionGetInitialPageSize(pInfo->binfo.pRes, &pageSize);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (!osTempSpaceAvailable()) {
    terrno = TSDB_CODE_NO_DISKSPACE;
    qError("Create partition operator info failed since %s, tempDir:%s", terrstr(), tsTempDir);
    goto _error;
  }

  code = partitionInitPageClasses(pInfo, pageSize, pTaskInfo->id.str);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initGroupOptrInfo(&pInfo->pGroupColVals, &pInfo->groupKeyLen, &pInfo->keyBuf, pInfo->pGroupCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  setOperatorInfo(pOperator, "PartitionOperator", QUERY_NODE_PHYSICAL_PLAN_PARTITION, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  __optr_fn_t nextFn = pInfo->phasedMode ? phasedHashPartitionNext
                                         : hashPartitionNext;
  pOperator->fpSet = createOperatorFpSet(
      optrDummyOpenFn, nextFn, NULL, destroyPartitionOperatorInfo,
      optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetPartitionOperState);
  code = appendDownstream(pOperator, &downstream, 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    destroyPartitionOperatorInfo(pInfo);
  }
  pTaskInfo->code = code;
  destroyOperatorAndDownstreams(pOperator, &downstream, 1);
  TAOS_RETURN(code);
}

int32_t setGroupResultOutputBuf(SOperatorInfo* pOperator, SOptrBasicInfo* binfo, int32_t numOfCols, char* pData,
                                int32_t bytes, uint64_t groupId, SDiskbasedBuf* pBuf, SAggSupporter* pAggSup) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SResultRowInfo* pResultRowInfo = &binfo->resultRowInfo;
  SqlFunctionCtx* pCtx = pOperator->exprSupp.pCtx;

  SResultRow* pResultRow = doSetResultOutBufByKey(pBuf, pResultRowInfo, (char*)pData, bytes, true, groupId, pTaskInfo,
                                                  false, pAggSup, false);
  if (pResultRow == NULL || pTaskInfo->code != 0) {
    return pTaskInfo->code;
  }

  return setResultRowInitCtx(pResultRow, pCtx, numOfCols, pOperator->exprSupp.rowEntryInfoOffset);
}

int32_t extractColumnInfo(SNodeList* pNodeList, SArray** pArrayRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  numOfCols = LIST_LENGTH(pNodeList);
  SArray* pList = taosArrayInit(numOfCols, sizeof(SColumn));
  if (pList == NULL) {
    code = terrno;
    (*pArrayRes) = NULL;
    QUERY_CHECK_CODE(code, lino, _end);
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    STargetNode* pNode = (STargetNode*)nodesListGetNode(pNodeList, i);
    QUERY_CHECK_NULL(pNode, code, lino, _end, terrno);

    if (nodeType(pNode->pExpr) == QUERY_NODE_COLUMN) {
      SColumnNode* pColNode = (SColumnNode*)pNode->pExpr;

      SColumn c = extractColumnFromColumnNode(pColNode);
      void*   tmp = taosArrayPush(pList, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    } else if (nodeType(pNode->pExpr) == QUERY_NODE_VALUE) {
      SValueNode* pValNode = (SValueNode*)pNode->pExpr;
      SColumn     c = {0};
      c.slotId = pNode->slotId;
      c.colId = pNode->slotId;
      c.type = pValNode->node.type;
      c.bytes = pValNode->node.resType.bytes;
      c.scale = pValNode->node.resType.scale;
      c.precision = pValNode->node.resType.precision;

      void* tmp = taosArrayPush(pList, &c);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  (*pArrayRes) = pList;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
