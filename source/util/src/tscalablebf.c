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

#include "tscalablebf.h"
#include "taoserror.h"
#include "tutil.h"

#define DEFAULT_GROWTH           2
#define DEFAULT_TIGHTENING_RATIO 0.5
#define DEFAULT_MAX_BLOOMFILTERS 4
#define SBF_INVALID              -1
#define SBF_VALID                0

static int32_t tScalableBfAddFilter(SScalableBf* pSBf, uint64_t expectedEntries, double errorRate,
                                    SBloomFilter** ppNormalBf);

int32_t tScalableBfInit(uint64_t expectedEntries, double errorRate, SScalableBf** ppSBf) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  const uint32_t defaultSize = 8;
  if (expectedEntries < 1 || errorRate <= 0 || errorRate >= 1.0) {
    code = TSDB_CODE_INVALID_PARA;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  SScalableBf* pSBf = taosMemoryCalloc(1, sizeof(SScalableBf));
  if (pSBf == NULL) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pSBf->maxBloomFilters = DEFAULT_MAX_BLOOMFILTERS;
  pSBf->status = SBF_VALID;
  pSBf->numBits = 0;
  pSBf->bfArray = taosArrayInit(defaultSize, sizeof(void*));
  if (!pSBf->bfArray) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SBloomFilter* pNormalBf = NULL;
  code = tScalableBfAddFilter(pSBf, expectedEntries, errorRate * DEFAULT_TIGHTENING_RATIO, &pNormalBf);
  if (code != TSDB_CODE_SUCCESS) {
    tScalableBfDestroy(pSBf);
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pSBf->growth = DEFAULT_GROWTH;
  pSBf->hashFn1 = HASH_FUNCTION_1;
  pSBf->hashFn2 = HASH_FUNCTION_2;
  (*ppSBf) = pSBf;
  return TSDB_CODE_SUCCESS;

_error:
  uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  return code;
}

int32_t tScalableBfPutNoCheck(SScalableBf* pSBf, const void* keyBuf, uint32_t len) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pSBf->status == SBF_INVALID) {
    return code;
  }
  int32_t       size = taosArrayGetSize(pSBf->bfArray);
  SBloomFilter* pNormalBf = taosArrayGetP(pSBf->bfArray, size - 1);
  if (!pNormalBf) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (tBloomFilterIsFull(pNormalBf)) {
    code = tScalableBfAddFilter(pSBf, pNormalBf->expectedEntries * pSBf->growth,
                                pNormalBf->errorRate * DEFAULT_TIGHTENING_RATIO, &pNormalBf);
    if (code != TSDB_CODE_SUCCESS) {
      pSBf->status = SBF_INVALID;
      if (code == TSDB_CODE_OUT_OF_BUFFER) {
        code = TSDB_CODE_SUCCESS;
        return code;
      }
      QUERY_CHECK_CODE(code, lino, _error);
    }
  }
  return tBloomFilterPut(pNormalBf, keyBuf, len);

_error:
  if (code != TSDB_CODE_SUCCESS) {
    uDebug("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tScalableBfPut(SScalableBf* pSBf, const void* keyBuf, uint32_t len, int32_t* winRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pSBf->status == SBF_INVALID) {
    (*winRes) = TSDB_CODE_FAILED;
    return code;
  }
  uint64_t h1 = (uint64_t)pSBf->hashFn1(keyBuf, len);
  uint64_t h2 = (uint64_t)pSBf->hashFn2(keyBuf, len);
  int32_t  size = taosArrayGetSize(pSBf->bfArray);
  for (int32_t i = size - 2; i >= 0; --i) {
    if (tBloomFilterNoContain(taosArrayGetP(pSBf->bfArray, i), h1, h2) != TSDB_CODE_SUCCESS) {
      (*winRes) = TSDB_CODE_FAILED;
      goto _end;
    }
  }

  SBloomFilter* pNormalBf = taosArrayGetP(pSBf->bfArray, size - 1);
  QUERY_CHECK_NULL(pNormalBf, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  if (tBloomFilterIsFull(pNormalBf)) {
    code = tScalableBfAddFilter(pSBf, pNormalBf->expectedEntries * pSBf->growth,
                                pNormalBf->errorRate * DEFAULT_TIGHTENING_RATIO, &pNormalBf);
    if (code != TSDB_CODE_SUCCESS) {
      pSBf->status = SBF_INVALID;
      if (code == TSDB_CODE_OUT_OF_BUFFER) {
        code = TSDB_CODE_SUCCESS;
        (*winRes) = TSDB_CODE_FAILED;
        goto _end;
      }
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }
  (*winRes) = tBloomFilterPutHash(pNormalBf, h1, h2);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tScalableBfNoContain(const SScalableBf* pSBf, const void* keyBuf, uint32_t len) {
  if (pSBf->status == SBF_INVALID) {
    return TSDB_CODE_FAILED;
  }
  uint64_t h1 = (uint64_t)pSBf->hashFn1(keyBuf, len);
  uint64_t h2 = (uint64_t)pSBf->hashFn2(keyBuf, len);
  int32_t  size = taosArrayGetSize(pSBf->bfArray);
  for (int32_t i = size - 1; i >= 0; --i) {
    if (tBloomFilterNoContain(taosArrayGetP(pSBf->bfArray, i), h1, h2) != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_FAILED;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t tScalableBfAddFilter(SScalableBf* pSBf, uint64_t expectedEntries, double errorRate,
                                    SBloomFilter** ppNormalBf) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (taosArrayGetSize(pSBf->bfArray) >= pSBf->maxBloomFilters) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  SBloomFilter* pNormalBf = NULL;
  code = tBloomFilterInit(expectedEntries, errorRate, &pNormalBf);
  QUERY_CHECK_CODE(code, lino, _error);

  if (taosArrayPush(pSBf->bfArray, &pNormalBf) == NULL) {
    tBloomFilterDestroy(pNormalBf);
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pSBf->numBits += pNormalBf->numBits;
  (*ppNormalBf) = pNormalBf;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void tScalableBfDestroy(SScalableBf* pSBf) {
  if (pSBf == NULL) {
    return;
  }
  if (pSBf->bfArray != NULL) {
    taosArrayDestroyP(pSBf->bfArray, (FDelete)tBloomFilterDestroy);
  }
  taosMemoryFree(pSBf);
}

int32_t tScalableBfEncode(const SScalableBf* pSBf, SEncoder* pEncoder) {
  if (!pSBf) {
    TAOS_CHECK_RETURN(tEncodeI32(pEncoder, 0));
    return 0;
  }
  int32_t size = taosArrayGetSize(pSBf->bfArray);
  TAOS_CHECK_RETURN(tEncodeI32(pEncoder, size));
  for (int32_t i = 0; i < size; i++) {
    SBloomFilter* pBF = taosArrayGetP(pSBf->bfArray, i);
    TAOS_CHECK_RETURN(tBloomFilterEncode(pBF, pEncoder));
  }
  TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pSBf->growth));
  TAOS_CHECK_RETURN(tEncodeU64(pEncoder, pSBf->numBits));
  TAOS_CHECK_RETURN(tEncodeU32(pEncoder, pSBf->maxBloomFilters));
  TAOS_CHECK_RETURN(tEncodeI8(pEncoder, pSBf->status));
  return 0;
}

int32_t tScalableBfDecode(SDecoder* pDecoder, SScalableBf** ppSBf) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SScalableBf* pSBf = taosMemoryCalloc(1, sizeof(SScalableBf));
  if (!pSBf) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  pSBf->hashFn1 = HASH_FUNCTION_1;
  pSBf->hashFn2 = HASH_FUNCTION_2;
  pSBf->bfArray = NULL;
  int32_t size = 0;
  if (tDecodeI32(pDecoder, &size) < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (size == 0) {
    (*ppSBf) = NULL;
    tScalableBfDestroy(pSBf);
    goto _error;
  }
  pSBf->bfArray = taosArrayInit(size * 2, POINTER_BYTES);
  if (!pSBf->bfArray) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }

  for (int32_t i = 0; i < size; i++) {
    SBloomFilter* pBF = NULL;
    code = tBloomFilterDecode(pDecoder, &pBF);
    QUERY_CHECK_CODE(code, lino, _error);
    void* tmpRes = taosArrayPush(pSBf->bfArray, &pBF);
    if (!tmpRes) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _error);
    }
  }
  if (tDecodeU32(pDecoder, &pSBf->growth) < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (tDecodeU64(pDecoder, &pSBf->numBits) < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (tDecodeU32(pDecoder, &pSBf->maxBloomFilters) < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  if (tDecodeI8(pDecoder, &pSBf->status) < 0) {
    code = terrno;
    QUERY_CHECK_CODE(code, lino, _error);
  }
  (*ppSBf) = pSBf;

_error:

  if (code != TSDB_CODE_SUCCESS) {
    tScalableBfDestroy(pSBf);
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
