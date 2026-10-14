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

#include "streamjoinfilter.h"

#include "taoserror.h"
#include "ttime.h"

static int32_t streamJoinFilterReadValue(const SStreamJoinFilterColumn* pDesc, const SSDataBlock* pBlock, int32_t row,
                                         bool build, const SColumnInfoData** ppCol, const char** ppValue,
                                         int32_t* pLength) {
  int32_t          slot = build ? pDesc->buildSlot : pDesc->inputSlot;
  const SDataType* pType = build ? &pDesc->buildType : &pDesc->probeType;
  if (slot < 0 || slot >= taosArrayGetSize(pBlock->pDataBlock)) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  const SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, slot);
  if (pCol->info.type != pType->type || pCol->info.bytes != pType->bytes) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  *ppCol = pCol;
  *ppValue = NULL;
  *pLength = 0;
  // Decoded cache pages may not yet have restored hasNull.
  if (IS_VAR_DATA_TYPE(pType->type)) {
    if (pCol->varmeta.offset == NULL) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    int32_t offset = pCol->varmeta.offset[row];
    if (offset == -1 || pCol->pData == NULL) {
      return TSDB_CODE_SUCCESS;
    }
    if (offset < 0 || offset > pCol->varmeta.length || pCol->varmeta.length - offset < VARSTR_HEADER_SIZE) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    VarDataLenT valueLength = 0;
    TAOS_MEMCPY(&valueLength, pCol->pData + offset, sizeof(valueLength));
    int32_t length = VARSTR_HEADER_SIZE + valueLength;
    // Virtual columns can retain source values wider than their declared type.
    if (length > pCol->varmeta.length - offset) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    *ppValue = pCol->pData + offset;
    *pLength = length;
  } else {
    if (colDataIsNull_f(pCol, row)) {
      return TSDB_CODE_SUCCESS;
    }
    if (pType->type == TSDB_DATA_TYPE_TIMESTAMP && pCol->info.precision > TSDB_TIME_PRECISION_NANO) {
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    *ppValue = pCol->pData + (size_t)pType->bytes * row;
    *pLength = pType->bytes;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t streamJoinFilterEncodeKey(const SStreamJoinFilterSpec* pSpec, const SSDataBlock* pBlock, int32_t row,
                                  bool build, char** ppScratch, int32_t* pCapacity, int32_t* pLength, bool* pNull) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  int64_t length = 0;
  if (pSpec == NULL || pSpec->pColumns == NULL || ppScratch == NULL || pCapacity == NULL || pLength == NULL ||
      pNull == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pLength = 0;
  *pNull = false;
  if (pBlock == NULL || pBlock->pDataBlock == NULL || row < 0 || row >= pBlock->info.rows) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  // Validate the complete tuple before allocating. A later NULL also rules out an oversized tuple.
  for (int32_t i = 0; i < taosArrayGetSize(pSpec->pColumns); ++i) {
    const SStreamJoinFilterColumn* pDesc = taosArrayGet(pSpec->pColumns, i);
    const SColumnInfoData*         pCol = NULL;
    const char*                    pValue = NULL;
    int32_t                        valueLength = 0;
    TAOS_CHECK_EXIT(streamJoinFilterReadValue(pDesc, pBlock, row, build, &pCol, &pValue, &valueLength));
    if (pValue == NULL) {
      *pNull = true;
      goto _exit;
    }
    length += sizeof(uint8_t) + sizeof(int32_t) + valueLength;
  }
  if (length > STREAM_JOIN_FILTER_MAX_KEY_BYTES) {
    *pLength = STREAM_JOIN_FILTER_MAX_KEY_BYTES + 1;
    goto _exit;
  }
  if (length > *pCapacity) {
    char* pScratch = taosMemoryRealloc(*ppScratch, length);
    if (pScratch == NULL) {
      code = terrno;
      goto _exit;
    }
    *ppScratch = pScratch;
    *pCapacity = length;
  }
  int32_t offset = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pSpec->pColumns); ++i) {
    const SStreamJoinFilterColumn* pDesc = taosArrayGet(pSpec->pColumns, i);
    const SColumnInfoData*         pCol = NULL;
    const char*                    pValue = NULL;
    int32_t                        valueLength = 0;
    TAOS_CHECK_EXIT(streamJoinFilterReadValue(pDesc, pBlock, row, build, &pCol, &pValue, &valueLength));
    uint8_t type = pDesc->buildType.type;
    TAOS_MEMCPY(*ppScratch + offset, &type, sizeof(type));
    offset += sizeof(type);
    TAOS_MEMCPY(*ppScratch + offset, &valueLength, sizeof(valueLength));
    offset += sizeof(valueLength);
    if (type == TSDB_DATA_TYPE_TIMESTAMP && pCol->info.precision != pDesc->keyPrecision) {
      int64_t timestamp = 0;
      TAOS_MEMCPY(&timestamp, pValue, sizeof(timestamp));
      timestamp = convertTimePrecision(timestamp, pCol->info.precision, pDesc->keyPrecision);
      TAOS_MEMCPY(*ppScratch + offset, &timestamp, sizeof(timestamp));
    } else {
      TAOS_MEMCPY(*ppScratch + offset, pValue, valueLength);
    }
    offset += valueLength;
  }
  *pLength = offset;
_exit:
  return code;
}

int32_t streamJoinFilterCreate(const SStreamJoinFilterSpec* pSpec, int32_t execId, uint64_t baseGId,
                               SStreamJoinFilter** ppFilter) {
  int32_t            code = TSDB_CODE_SUCCESS, lino = 0;
  SStreamJoinFilter* pFilter = NULL;
  if (ppFilter == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *ppFilter = NULL;
  if (pSpec == NULL || pSpec->pColumns == NULL || taosArrayGetSize(pSpec->pColumns) == 0 ||
      pSpec->pColumns->elemSize != sizeof(SStreamJoinFilterColumn)) {
    return TSDB_CODE_INVALID_PARA;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pSpec->pColumns); ++i) {
    const SStreamJoinFilterColumn* pCol = taosArrayGet(pSpec->pColumns, i);
    const SDataType*               pType = &pCol->buildType;
    if (!isValidDataType(pType->type) || pType->type == TSDB_DATA_TYPE_NULL || pType->type != pCol->probeType.type ||
        pType->scale != pCol->probeType.scale || pType->bytes <= 0 || pCol->probeType.bytes <= 0 ||
        pCol->buildSlot < 0 || pCol->inputSlot < 0 ||
        (IS_VAR_DATA_TYPE(pType->type)
             ? (pType->bytes < VARSTR_HEADER_SIZE || pCol->probeType.bytes < VARSTR_HEADER_SIZE)
             : (pType->bytes != tDataTypes[pType->type].bytes || pType->bytes != pCol->probeType.bytes)) ||
        (pType->type == TSDB_DATA_TYPE_TIMESTAMP && pCol->keyPrecision > TSDB_TIME_PRECISION_NANO)) {
      return TSDB_CODE_INVALID_PARA;
    }
  }
  pFilter = taosMemoryCalloc(1, sizeof(SStreamJoinFilter));
  if (pFilter == NULL) {
    return terrno;
  }
  pFilter->execId = execId;
  pFilter->baseGId = baseGId;
  pFilter->spec = *pSpec;
  pFilter->spec.pColumns = taosArrayDup(pSpec->pColumns, NULL);
  if (pFilter->spec.pColumns == NULL) {
    code = terrno;
    goto _exit;
  }
  pFilter->pKeys = tSimpleHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  if (pFilter->pKeys == NULL) {
    code = terrno;
    goto _exit;
  }
  *ppFilter = pFilter;
_exit:
  if (code != TSDB_CODE_SUCCESS) {
    streamJoinFilterDestroy(&pFilter);
  }
  return code;
}

int32_t streamJoinFilterAddRow(SStreamJoinFilter* pFilter, const SSDataBlock* pBlock, int32_t row,
                               EStreamJoinFilterFallback* pFallback) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  int32_t length = 0;
  bool    isNull = false;
  if (pFilter == NULL || pFallback == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pFallback = STREAM_JOIN_FILTER_NO_FALLBACK;
  TAOS_CHECK_EXIT(streamJoinFilterEncodeKey(&pFilter->spec, pBlock, row, true, &pFilter->pScratch,
                                            &pFilter->scratchCapacity, &length, &isNull));
  if (isNull) {
    goto _exit;
  }
  if (length > STREAM_JOIN_FILTER_MAX_KEY_BYTES) {
    *pFallback = STREAM_JOIN_FILTER_BYTE_LIMIT;
    goto _exit;
  }
  if (tSimpleHashGet(pFilter->pKeys, pFilter->pScratch, length) != NULL) {
    goto _exit;
  }
  if (pFilter->keyCount == STREAM_JOIN_FILTER_MAX_KEYS) {
    *pFallback = STREAM_JOIN_FILTER_KEY_LIMIT;
    goto _exit;
  }
  if (length > STREAM_JOIN_FILTER_MAX_KEY_BYTES - pFilter->keyBytes) {
    *pFallback = STREAM_JOIN_FILTER_BYTE_LIMIT;
    goto _exit;
  }
  uint8_t present = 1;
  TAOS_CHECK_EXIT(tSimpleHashPut(pFilter->pKeys, pFilter->pScratch, length, &present, sizeof(present)));
  ++pFilter->keyCount;
  pFilter->keyBytes += length;
_exit:
  return code;
}

int32_t streamJoinFilterMatchRow(SStreamJoinFilter* pFilter, const SSDataBlock* pBlock, int32_t row, bool* pMatch) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  int32_t length = 0;
  bool    isNull = false;
  if (pFilter == NULL || pMatch == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  *pMatch = false;
  TAOS_CHECK_EXIT(streamJoinFilterEncodeKey(&pFilter->spec, pBlock, row, false, &pFilter->pScratch,
                                            &pFilter->scratchCapacity, &length, &isNull));
  if (!isNull && length <= STREAM_JOIN_FILTER_MAX_KEY_BYTES) {
    *pMatch = tSimpleHashGet(pFilter->pKeys, pFilter->pScratch, length) != NULL;
  }
_exit:
  return code;
}

void streamJoinFilterSpecClear(SStreamJoinFilterSpec* pSpec) {
  if (pSpec != NULL) {
    taosArrayDestroy(pSpec->pColumns);
    TAOS_MEMSET(pSpec, 0, sizeof(*pSpec));
  }
}

void streamJoinFilterDestroy(SStreamJoinFilter** ppFilter) {
  if (ppFilter == NULL || *ppFilter == NULL) {
    return;
  }
  SStreamJoinFilter* pFilter = *ppFilter;
  streamJoinFilterSpecClear(&pFilter->spec);
  tSimpleHashCleanup(pFilter->pKeys);
  taosMemoryFree(pFilter->pScratch);
  taosMemoryFreeClear(*ppFilter);
}
