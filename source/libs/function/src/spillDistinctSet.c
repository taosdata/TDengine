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

#include "spillDistinctSet.h"
#include "taoserror.h"
#include "tcompare.h"
#include "tglobal.h"
#include "thash.h"
#include "tpagedbuf.h"
#include "tsimplehash.h"

#define SPILL_DISTINCT_NUM_PARTITIONS 8
#define SPILL_DISTINCT_PARTITION_BITS 3  // log2(SPILL_DISTINCT_NUM_PARTITIONS)
// Keep one cache page for every active partition tail. This prevents uniformly distributed small
// values from evicting and reloading a dirty tail on nearly every append. MurmurHash3_32 takes no
// seed, so eleven 3-bit slices cover all 32 hash bits during recursive repartitioning.
#define SPILL_DISTINCT_MAX_DEPTH      11
#define SPILL_DISTINCT_PAGE_SIZE      (16 * 1024)
#define SPILL_DISTINCT_PAGE_SIZE_SMALL 4096
#define SPILL_DISTINCT_IN_MEM_PAGES   (SPILL_DISTINCT_NUM_PARTITIONS + 1)

// One partition's page chain. Values are appended to the tail page; a value never straddles pages.
// The shared paged buffer has one cache slot per partition plus one source-page slot, so recursive
// repartitioning can keep every destination tail resident while scanning the current source page.
typedef struct SSpillPartition {
  SArray* pPageIdList;  // int32_t page ids, in append order
  int32_t tailPageId;   // page currently being filled, -1 when none
} SSpillPartition;

struct SSpillDistinctSet {
  int8_t         type;
  int32_t        valueBytes;   // fixed width, or declared max for var types
  bool           isVarType;
  int64_t        memThreshold;  // resident byte budget before spilling
  int32_t        pageSize;
  char*          id;

  SSHashObj*     pHash;    // resident membership; released on spill
  int64_t        entryBytes;  // key+data bytes in pHash, which tSimpleHashGetMemSize() omits
  bool           spilled;
  bool           finalized;
  SDiskbasedBuf* pBuf;
  bool            ownsBuf;
  SDiskbasedBuf** ppSharedBuf;
  SSpillPartition partitions[SPILL_DISTINCT_NUM_PARTITIONS];
};

// Partition on a depth-specific slice of the hash so a partition that is still too large can be
// split again by the next slice. Depth 0 uses the low bits, matching the original layout.
static uint32_t spillDistinctPartitionAt(const char* pData, int32_t len, int32_t depth) {
  uint32_t h = MurmurHash3_32(pData, len);
  return (h >> (depth * SPILL_DISTINCT_PARTITION_BITS)) % SPILL_DISTINCT_NUM_PARTITIONS;
}

static uint32_t spillDistinctPartitionOf(const char* pData, int32_t len) {
  return spillDistinctPartitionAt(pData, len, 0);
}

int32_t spillDistinctSetCreateWithBuf(SSpillDistinctSet** ppSet, int8_t type, int32_t valueBytes,
                                      int64_t memThreshold, bool perResultRow, const char* id,
                                      SDiskbasedBuf** ppSharedBuf) {
  if (ppSet == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  // valueBytes drives the minimum page size, so a non-positive value would size pages below one
  // record and a wildly large one would overflow that arithmetic. Every current caller derives it
  // from a column width, but this is a reusable allocator and should not depend on that.
  //
  // The ceiling is type-dependent: an ordinary column is capped at TSDB_MAX_FIELD_LEN, but a
  // BLOB/MEDIUMBLOB may legitimately be up to TSDB_MAX_BLOB_LEN (4 MiB), and rejecting those would
  // fail the query outright.
  int32_t maxValueBytes = IS_STR_DATA_BLOB(type) ? TSDB_MAX_BLOB_LEN : TSDB_MAX_FIELD_LEN;
  if (valueBytes <= 0 || valueBytes > maxValueBytes) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppSet = NULL;

  SSpillDistinctSet* pSet = taosMemoryCalloc(1, sizeof(SSpillDistinctSet));
  if (pSet == NULL) {
    return terrno;
  }

  pSet->type = type;
  pSet->valueBytes = valueBytes;
  pSet->isVarType = IS_VAR_DATA_TYPE(type);
  pSet->memThreshold =
      (memThreshold > 0) ? memThreshold : (int64_t)tsPQSortMemThreshold * 1024 * 1024;
  pSet->id = (id != NULL) ? taosStrdup(id) : taosStrdup("distinct");
  if (pSet->id == NULL) {
    taosMemoryFree(pSet);
    return terrno;
  }

  // Page sizing. A per-result-row set (windows / groups) gets small pages because the query holds
  // one set per output row -- the same reasoning as tMemBucketCreate().
  //
  // But a page must always be able to hold the widest single value: a var-type column may be up to
  // TSDB_MAX_FIELD_LEN (65519) bytes, far beyond either default, and a value that does not fit
  // would fail the query rather than spill. So grow the page to fit one record whenever the
  // declared width demands it, accepting the larger page as the cost of supporting wide values.
  int32_t minPageSize = (int32_t)sizeof(SFilePage) + (int32_t)sizeof(int32_t) + valueBytes;
  pSet->pageSize = perResultRow ? SPILL_DISTINCT_PAGE_SIZE_SMALL : SPILL_DISTINCT_PAGE_SIZE;
  if (pSet->pageSize < minPageSize) {
    // Round up to a 4K multiple so the buffer keeps page-aligned sizes.
    pSet->pageSize = ((minPageSize + 4095) / 4096) * 4096;
  }

  pSet->pBuf = (ppSharedBuf != NULL) ? *ppSharedBuf : NULL;
  pSet->ownsBuf = (ppSharedBuf == NULL);
  pSet->ppSharedBuf = ppSharedBuf;
  if (pSet->pBuf != NULL && getBufPageSize(pSet->pBuf) < pSet->pageSize) {
    taosMemoryFree(pSet->id);
    taosMemoryFree(pSet);
    return TSDB_CODE_INVALID_PARA;
  }

  pSet->pHash = tSimpleHashInit(64, MurmurHash3_32);
  if (pSet->pHash == NULL) {
    taosMemoryFree(pSet->id);
    taosMemoryFree(pSet);
    return terrno;
  }

  for (int32_t i = 0; i < SPILL_DISTINCT_NUM_PARTITIONS; ++i) {
    pSet->partitions[i].tailPageId = -1;
  }

  *ppSet = pSet;
  return TSDB_CODE_SUCCESS;
}

int32_t spillDistinctSetCreate(SSpillDistinctSet** ppSet, int8_t type, int32_t valueBytes,
                               int64_t memThreshold, bool perResultRow, const char* id) {
  return spillDistinctSetCreateWithBuf(ppSet, type, valueBytes, memThreshold, perResultRow, id,
                                       NULL);
}

void spillDistinctSetDestroy(SSpillDistinctSet* pSet) {
  if (pSet == NULL) {
    return;
  }
  if (pSet->pHash != NULL) {
    tSimpleHashCleanup(pSet->pHash);
  }
  for (int32_t i = 0; i < SPILL_DISTINCT_NUM_PARTITIONS; ++i) {
    if (pSet->pBuf != NULL && !pSet->ownsBuf && pSet->partitions[i].pPageIdList != NULL) {
      int32_t numPages = taosArrayGetSize(pSet->partitions[i].pPageIdList);
      for (int32_t j = 0; j < numPages; ++j) {
        int32_t* pPageId = taosArrayGet(pSet->partitions[i].pPageIdList, j);
        if (pPageId != NULL) {
          dBufRemovePageById(pSet->pBuf, *pPageId);
        }
      }
    }
    taosArrayDestroy(pSet->partitions[i].pPageIdList);
  }
  if (pSet->pBuf != NULL && pSet->ownsBuf) {
    destroyDiskbasedBuf(pSet->pBuf);
  }
  taosMemoryFree(pSet->id);
  taosMemoryFree(pSet);
}

bool spillDistinctSetSpilled(const SSpillDistinctSet* pSet) {
  return pSet != NULL && pSet->spilled;
}

bool spillDistinctSetFinalized(const SSpillDistinctSet* pSet) {
  return pSet != NULL && pSet->finalized;
}

int64_t spillDistinctSetResidentCount(const SSpillDistinctSet* pSet) {
  if (pSet == NULL || pSet->pHash == NULL) {
    return 0;
  }
  return tSimpleHashGetSize(pSet->pHash);
}

// Append one value to its partition's page chain. Each record is [int32_t len][payload].
// Append into a specific partition. Used both for the initial spill (depth 0 partitions owned by
// the set) and for re-partitioning an oversized partition during finalize.
static int32_t spillDistinctWriteToPartition(SSpillDistinctSet* pSet, SSpillPartition* pPart,
                                             const char* pData, int32_t len) {
  // Bound len before any arithmetic on it. A near-INT32_MAX length would overflow the recordLen
  // addition into a small or negative value, slipping past the capacity check and corrupting memory
  // in the memcpy below. Current callers derive len from the column width so this is unreachable,
  // but the function should not depend on that to stay memory-safe.
  int32_t maxPayload = pSet->pageSize - (int32_t)sizeof(SFilePage) - (int32_t)sizeof(int32_t);
  if (len <= 0 || len > maxPayload) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  int32_t recordLen = (int32_t)sizeof(int32_t) + len;

  if (pPart->pPageIdList == NULL) {
    pPart->pPageIdList = taosArrayInit(4, sizeof(int32_t));
    if (pPart->pPageIdList == NULL) {
      return terrno;
    }
  }

  SFilePage* pPage = NULL;
  int32_t oldTailPageId = pPart->tailPageId;
  if (pPart->tailPageId >= 0) {
    pPage = (SFilePage*)getBufPage(pSet->pBuf, pPart->tailPageId);
    if (pPage == NULL) {
      return terrno;
    }
    if ((int32_t)sizeof(SFilePage) + pPage->num + recordLen > pSet->pageSize) {
      releaseBufPage(pSet->pBuf, pPage);
      pPage = NULL;
      pPart->tailPageId = -1;
    }
  }

  if (pPage == NULL) {
    int32_t pageId = -1;
    pPage = (SFilePage*)getNewBufPage(pSet->pBuf, &pageId);
    if (pPage == NULL) {
      pPart->tailPageId = oldTailPageId;
      return terrno;
    }
    pPage->num = 0;
    if (taosArrayPush(pPart->pPageIdList, &pageId) == NULL) {
      releaseBufPage(pSet->pBuf, pPage);
      (void)dBufRemovePageById(pSet->pBuf, pageId);
      return terrno;
    }
    pPart->tailPageId = pageId;
  }

  char* pWrite = pPage->data + pPage->num;
  memcpy(pWrite, &len, sizeof(int32_t));
  memcpy(pWrite + sizeof(int32_t), pData, len);
  pPage->num += recordLen;

  setBufPageDirty(pPage, true);
  releaseBufPage(pSet->pBuf, pPage);
  return TSDB_CODE_SUCCESS;
}

// Route a value to its depth-0 partition. Used while accumulating.
static int32_t spillDistinctWriteValue(SSpillDistinctSet* pSet, const char* pData, int32_t len) {
  SSpillPartition* pPart = &pSet->partitions[spillDistinctPartitionOf(pData, len)];
  return spillDistinctWriteToPartition(pSet, pPart, pData, len);
}

static void spillDistinctClearPages(SSpillDistinctSet* pSet) {
  if (pSet->pBuf == NULL) return;
  for (int32_t i = 0; i < SPILL_DISTINCT_NUM_PARTITIONS; ++i) {
    SSpillPartition* pPart = &pSet->partitions[i];
    if (pPart->pPageIdList == NULL) continue;
    int32_t n = taosArrayGetSize(pPart->pPageIdList);
    for (int32_t j = 0; j < n; ++j) {
      int32_t* id = taosArrayGet(pPart->pPageIdList, j);
      if (id != NULL) dBufRemovePageById(pSet->pBuf, *id);
    }
    taosArrayClear(pPart->pPageIdList);
    pPart->tailPageId = -1;
  }
}

// Move the resident values to disk and drop the hash. Called once, when the budget is exceeded.
static int32_t spillDistinctSpillResident(SSpillDistinctSet* pSet) {
  // A failed migration leaves the resident hash authoritative and the partially populated buffer
  // owned by the set. Never replace that buffer on a retry: doing so would leak its pages and lose
  // the values already written there.
  int32_t code = TSDB_CODE_SUCCESS;
  if (pSet->pBuf == NULL) {
    if (pSet->ppSharedBuf != NULL && *pSet->ppSharedBuf != NULL) {
      pSet->pBuf = *pSet->ppSharedBuf;
    }
  }
  if (pSet->pBuf == NULL) {
    code = createDiskbasedBuf(&pSet->pBuf, pSet->pageSize,
                              pSet->pageSize * SPILL_DISTINCT_IN_MEM_PAGES,
                              pSet->id, tsTempDir);
    if (code != TSDB_CODE_SUCCESS) {
      spillDistinctClearPages(pSet);
      if (pSet->ownsBuf) {
        destroyDiskbasedBuf(pSet->pBuf);
        pSet->pBuf = NULL;
      }
      return code;
    }
    pSet->ownsBuf = true;
    if (pSet->ppSharedBuf != NULL) {
      *pSet->ppSharedBuf = pSet->pBuf;
      pSet->ownsBuf = false;
    }
  }

  // Write every resident value out BEFORE flipping the flag. Setting spilled=true first would
  // leave a half-migrated set on failure: the caller aborts on the error code, but if anything ever
  // continued adding, spillDistinctSetAdd() would take the spilled branch while pHash still held
  // values that were never written, and those would vanish from the result.
  void*   pIter = NULL;
  int32_t iter = 0;
  while ((pIter = tSimpleHashIterate(pSet->pHash, pIter, &iter)) != NULL) {
    size_t keyLen = 0;
    char*  pKey = tSimpleHashGetKey(pIter, &keyLen);
    code = spillDistinctWriteValue(pSet, pKey, (int32_t)keyLen);
    if (code != TSDB_CODE_SUCCESS) {
      // Still resident and still consistent: the hash remains authoritative, and destroy() frees
      // both it and the partially written buffer.
      return code;
    }
  }

  tSimpleHashCleanup(pSet->pHash);
  pSet->pHash = NULL;
  pSet->entryBytes = 0;
  pSet->spilled = true;
  return TSDB_CODE_SUCCESS;
}

int32_t spillDistinctSetAdd(SSpillDistinctSet* pSet, const char* pData, int32_t len, bool* pIsNew) {
  if (pSet == NULL || pData == NULL || len <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pIsNew != NULL) {
    *pIsNew = false;
  }

  if (pSet->spilled) {
    // Membership is no longer tracked; record the value for the finalize pass.
    return spillDistinctWriteValue(pSet, pData, len);
  }

  if (tSimpleHashGet(pSet->pHash, pData, len) != NULL) {
    return TSDB_CODE_SUCCESS;  // duplicate, *pIsNew stays false
  }

  int32_t dummy = 0;
  int32_t code = tSimpleHashPut(pSet->pHash, pData, len, &dummy, sizeof(dummy));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (pIsNew != NULL) {
    *pIsNew = true;
  }

  // Account for both halves of the hash's footprint. tSimpleHashGetMemSize() reports buckets, the
  // fixed SHNode header and the hash object, but doCreateHashNode() allocates
  // sizeof(SHNode) + keyLen + dataLen -- so the key bytes and the 4-byte payload are missing from
  // its total. On wide VARCHAR/NCHAR that omission dominates, and the set would grow far past the
  // budget before spilling. Add the per-entry payload we know we stored.
  pSet->entryBytes += len + (int64_t)sizeof(int32_t);
  if ((int64_t)tSimpleHashGetMemSize(pSet->pHash) + pSet->entryBytes > pSet->memThreshold) {
    return spillDistinctSpillResident(pSet);
  }
  return TSDB_CODE_SUCCESS;
}

// Emit every distinct value in one partition's page chain.
//
// A partition is dedup'd with an in-memory hash, but that hash is bounded by the same budget as the
// resident phase. If it exceeds the budget the partition is too skewed to dedup in one pass, so the
// remaining values are re-partitioned to disk on the next slice of the hash and each sub-partition
// is finalized recursively. Values already emitted at this level are not re-emitted: they stay in
// the bounded hash, which continues to suppress duplicates for the rest of the scan, and only
// not-yet-seen values are pushed down.
//
// depth selects which 6-bit slice of the hash splits the data, so equal values always land in the
// same sub-partition and per-partition dedup stays exact. At SPILL_DISTINCT_MAX_DEPTH the hash bits
// are exhausted; the leaf is dedup'd in memory rather than rejected, preserving correctness for
// high-cardinality skew at the cost of a bounded-depth memory spike.
static int32_t spillDistinctFinalizePartition(SSpillDistinctSet* pSet, SSpillPartition* pPart,
                                              int32_t depth, FSpillDistinctValueFn fn, void* param) {
  if (pPart->pPageIdList == NULL || taosArrayGetSize(pPart->pPageIdList) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj* pSeen = tSimpleHashInit(64, MurmurHash3_32);
  if (pSeen == NULL) {
    return terrno;
  }

  // Sub-partitions for the overflow case, allocated lazily so the common path costs nothing.
  SSpillPartition* pSubParts = NULL;
  // Scratch space for relocating a value out of its page before the write may evict that page.
  // Sized to the widest record a page can hold, which bounds any value the set accepted.
  char*            pScratch = NULL;
  int64_t          seenBytes = 0;  // key+data bytes held in pSeen, omitted by tSimpleHashGetMemSize
  int64_t          hashThreshold = pSet->memThreshold;
  int32_t          scratchCap = pSet->pageSize - (int32_t)sizeof(SFilePage) - (int32_t)sizeof(int32_t);
  bool             overflowed = false;
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          numOfPages = (int32_t)taosArrayGetSize(pPart->pPageIdList);

  // Keep enough hash capacity for useful partition deduplication even when a caller selects a
  // threshold smaller than one page. The paged buffer has its own bounded cache budget.
  if (hashThreshold < pSet->pageSize) {
    hashThreshold = pSet->pageSize;
  }

  // Consume from the end so removing a page directory entry never shifts an unprocessed entry.
  for (int32_t i = numOfPages - 1; i >= 0 && code == TSDB_CODE_SUCCESS; --i) {
    int32_t* pPageId = taosArrayGet(pPart->pPageIdList, i);
    if (pPageId == NULL) {
      code = TSDB_CODE_OUT_OF_RANGE;
      break;
    }
    SFilePage* pPage = (SFilePage*)getBufPage(pSet->pBuf, *pPageId);
    if (pPage == NULL) {
      code = terrno;
      break;
    }

    int32_t offset = 0;
    while (offset + (int32_t)sizeof(int32_t) <= pPage->num) {
      int32_t len = 0;
      memcpy(&len, pPage->data + offset, sizeof(int32_t));
      offset += (int32_t)sizeof(int32_t);
      if (len <= 0 || offset + len > pPage->num) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        break;
      }
      char* pValue = pPage->data + offset;
      offset += len;

      // Already emitted at this level: suppressed regardless of whether we have overflowed.
      if (tSimpleHashGet(pSeen, pValue, len) != NULL) {
        continue;
      }

      if (!overflowed) {
        int32_t dummy = 0;
        code = tSimpleHashPut(pSeen, pValue, len, &dummy, sizeof(dummy));
        if (code != TSDB_CODE_SUCCESS) break;
        code = fn(param, pValue, len);
        if (code != TSDB_CODE_SUCCESS) break;

        // Same accounting caveat as the resident phase: add the key/data bytes that
        // tSimpleHashGetMemSize() leaves out, or a wide-value partition blows the budget unnoticed.
        seenBytes += len + (int64_t)sizeof(int32_t);
        if ((int64_t)tSimpleHashGetMemSize(pSeen) + seenBytes > hashThreshold) {
          if (depth + 1 < SPILL_DISTINCT_MAX_DEPTH) {
            overflowed = true;
          } else {
            // Do not allow a pathological hash partition to grow without bound
            // once the recursive partitioning budget is exhausted.
            code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
            break;
          }
        }
        continue;
      }

      // Past the budget: push the value down to the next level instead of growing the hash.
      if (pSubParts == NULL) {
        pSubParts = taosMemoryCalloc(SPILL_DISTINCT_NUM_PARTITIONS, sizeof(SSpillPartition));
        if (pSubParts == NULL) {
          code = terrno;
          break;
        }
        for (int32_t k = 0; k < SPILL_DISTINCT_NUM_PARTITIONS; ++k) {
          pSubParts[k].tailPageId = -1;
        }
      }
      // Copy the value out before writing. pValue points into pPage, and the write may call
      // getNewBufPage(), which can evict that very page to make room -- leaving pValue dangling
      // mid-memcpy.
      if (pScratch == NULL) {
        pScratch = taosMemoryMalloc(scratchCap);
        if (pScratch == NULL) {
          code = terrno;
          break;
        }
      }
      if (len > scratchCap) {
        code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
        break;
      }
      memcpy(pScratch, pValue, len);
      uint32_t sub = spillDistinctPartitionAt(pScratch, len, depth + 1);
      code = spillDistinctWriteToPartition(pSet, &pSubParts[sub], pScratch, len);
      if (code != TSDB_CODE_SUCCESS) break;
    }

    // Preserve the first processing error. Do not remove a partially processed page: retaining it
    // lets the caller report the real callback/hash/I/O failure and avoids silently dropping data.
    if (code != TSDB_CODE_SUCCESS) {
      releaseBufPage(pSet->pBuf, pPage);
      break;
    }
    releaseBufPage(pSet->pBuf, pPage);
    code = dBufRemovePageById(pSet->pBuf, *pPageId);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    taosArrayRemove(pPart->pPageIdList, i);
  }


  tSimpleHashCleanup(pSeen);

  // Recurse into whatever had to be pushed down. The bounded hash above already emitted and
  // suppressed everything it held, so these are values it never saw.
  if (code == TSDB_CODE_SUCCESS && pSubParts != NULL) {
    for (int32_t k = 0; k < SPILL_DISTINCT_NUM_PARTITIONS && code == TSDB_CODE_SUCCESS; ++k) {
      code = spillDistinctFinalizePartition(pSet, &pSubParts[k], depth + 1, fn, param);
    }
  }
  if (pSubParts != NULL) {
    for (int32_t k = 0; k < SPILL_DISTINCT_NUM_PARTITIONS; ++k) {
      // Reclaim pages written into a shared buffer when recursion aborts before consuming them.
      if (code != TSDB_CODE_SUCCESS && pSubParts[k].pPageIdList != NULL) {
        int32_t numPages = taosArrayGetSize(pSubParts[k].pPageIdList);
        for (int32_t j = 0; j < numPages; ++j) {
          int32_t* pPageId = taosArrayGet(pSubParts[k].pPageIdList, j);
          if (pPageId != NULL) {
            (void)dBufRemovePageById(pSet->pBuf, *pPageId);
          }
        }
      }
      taosArrayDestroy(pSubParts[k].pPageIdList);
    }
    taosMemoryFree(pSubParts);
  }
  taosMemoryFree(pScratch);
  return code;
}

int32_t spillDistinctSetFinalize(SSpillDistinctSet* pSet, FSpillDistinctValueFn fn, void* param) {
  if (pSet == NULL || fn == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (pSet->finalized) {
    return TSDB_CODE_SUCCESS;
  }

  if (!pSet->spilled) {
    void*   pIter = NULL;
    int32_t iter = 0;
    while ((pIter = tSimpleHashIterate(pSet->pHash, pIter, &iter)) != NULL) {
      size_t  keyLen = 0;
      char*   pKey = tSimpleHashGetKey(pIter, &keyLen);
      int32_t code = fn(param, pKey, (int32_t)keyLen);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    pSet->finalized = true;
    return TSDB_CODE_SUCCESS;
  }

  // Equal values hash to the same partition, so per-partition dedup is exact and peak memory is
  // the largest partition rather than the whole set.
  for (int32_t i = 0; i < SPILL_DISTINCT_NUM_PARTITIONS; ++i) {
    int32_t code = spillDistinctFinalizePartition(pSet, &pSet->partitions[i], 0, fn, param);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  pSet->finalized = true;
  return TSDB_CODE_SUCCESS;
}
