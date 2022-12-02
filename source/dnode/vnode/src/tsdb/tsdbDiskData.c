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

#include "tsdb.h"

typedef struct SDiskColBuilder SDiskColBuilder;

struct SDiskColBuilder {
  int16_t        cid;
  int8_t         type;
  uint8_t        cmprAlg;
  uint8_t        calcSma;
  int8_t         flag;
  int32_t        nVal;
  uint8_t       *pBitMap;
  int32_t        offset;
  SCompressor   *pOffC;
  SCompressor   *pValC;
  SColumnDataAgg sma;
  uint8_t        minSet;
  uint8_t        maxSet;
  uint8_t       *aBuf[2];
};

// SDiskData ================================================
static int32_t tDiskDataDestroy(SDiskData *pDiskData) {
  int32_t code = 0;
  pDiskData->aDiskCol = taosArrayDestroy(pDiskData->aDiskCol);
  return code;
}

// SDiskColBuilder ================================================
#define tDiskColBuilderCreate() \
  (SDiskColBuilder) { 0 }

static int32_t tDiskColBuilderDestroy(SDiskColBuilder *pBuilder) {
  int32_t code = 0;

  tFree(pBuilder->pBitMap);
  if (pBuilder->pOffC) tCompressorDestroy(pBuilder->pOffC);
  if (pBuilder->pValC) tCompressorDestroy(pBuilder->pValC);
  for (int32_t iBuf = 0; iBuf < sizeof(pBuilder->aBuf) / sizeof(pBuilder->aBuf[0]); iBuf++) {
    tFree(pBuilder->aBuf[iBuf]);
  }

  return code;
}

static int32_t tDiskColBuilderInit(SDiskColBuilder *pBuilder, int16_t cid, int8_t type, uint8_t cmprAlg,
                                   uint8_t calcSma) {
  int32_t code = 0;

  pBuilder->cid = cid;
  pBuilder->type = type;
  pBuilder->cmprAlg = cmprAlg;
  pBuilder->calcSma = IS_VAR_DATA_TYPE(type) ? 0 : calcSma;
  pBuilder->flag = 0;
  pBuilder->nVal = 0;
  pBuilder->offset = 0;

  if (IS_VAR_DATA_TYPE(type)) {
    if (pBuilder->pOffC == NULL && (code = tCompressorCreate(&pBuilder->pOffC))) return code;
    code = tCompressStart(pBuilder->pOffC, TSDB_DATA_TYPE_INT, cmprAlg);
    if (code) return code;
  }

  if (pBuilder->pValC == NULL && (code = tCompressorCreate(&pBuilder->pValC))) return code;
  code = tCompressStart(pBuilder->pValC, type, cmprAlg);
  if (code) return code;

  if (pBuilder->calcSma) {
    pBuilder->sma = (SColumnDataAgg){.colId = cid};
    pBuilder->minSet = 0;
    pBuilder->maxSet = 0;
  }

  return code;
}

static int32_t tGnrtDiskCol(SDiskColBuilder *pBuilder, SDiskCol *pDiskCol) {
  int32_t code = 0;

  ASSERT(pBuilder->flag && pBuilder->flag != HAS_NONE);

  *pDiskCol = (SDiskCol){(SBlockCol){.cid = pBuilder->cid,
                                     .type = pBuilder->type,
                                     .smaOn = pBuilder->calcSma,
                                     .flag = pBuilder->flag,
                                     .szOrigin = 0,
                                     .szBitmap = 0,
                                     .szOffset = 0,
                                     .szValue = 0,
                                     .offset = 0},
                         .pBit = NULL, .pOff = NULL, .pVal = NULL, .agg = pBuilder->sma};

  if (pBuilder->flag == HAS_NULL) return code;

  // BITMAP
  if (pBuilder->flag != HAS_VALUE) {
    int32_t nBit;
    if (pBuilder->flag == (HAS_VALUE | HAS_NULL | HAS_NONE)) {
      nBit = BIT2_SIZE(pBuilder->nVal);
    } else {
      nBit = BIT1_SIZE(pBuilder->nVal);
    }

    code = tRealloc(&pBuilder->aBuf[0], nBit + COMP_OVERFLOW_BYTES);
    if (code) return code;

    code = tRealloc(&pBuilder->aBuf[1], nBit + COMP_OVERFLOW_BYTES);
    if (code) return code;

    pDiskCol->bCol.szBitmap =
        tsCompressTinyint(pBuilder->pBitMap, nBit, nBit, pBuilder->aBuf[0], nBit + COMP_OVERFLOW_BYTES,
                          pBuilder->cmprAlg, pBuilder->aBuf[1], nBit + COMP_OVERFLOW_BYTES);
    pDiskCol->pBit = pBuilder->aBuf[0];
  }

  // OFFSET
  if (IS_VAR_DATA_TYPE(pBuilder->type)) {
    code = tCompressEnd(pBuilder->pOffC, &pDiskCol->pOff, &pDiskCol->bCol.szOffset, NULL);
    if (code) return code;
  }

  // VALUE
  if (pBuilder->flag != (HAS_NULL | HAS_NONE)) {
    code = tCompressEnd(pBuilder->pValC, &pDiskCol->pVal, &pDiskCol->bCol.szValue, &pDiskCol->bCol.szOrigin);
    if (code) return code;
  }

  return code;
}

static FORCE_INLINE int32_t tDiskColPutValue(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    code = tCompress(pBuilder->pOffC, &pBuilder->offset, sizeof(int32_t));
    if (code) return code;
    pBuilder->offset += pColVal->value.nData;

    code = tCompress(pBuilder->pValC, pColVal->value.pData, pColVal->value.nData);
    if (code) return code;
  } else {
    code = tCompress(pBuilder->pValC, &pColVal->value.val, tDataTypes[pColVal->type].bytes);
    if (code) return code;
  }

  return code;
}
static FORCE_INLINE int32_t tDiskColAddVal00(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  pBuilder->flag = HAS_VALUE;
  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal01(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  pBuilder->flag = HAS_NONE;
  return 0;
}
static FORCE_INLINE int32_t tDiskColAddVal02(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  pBuilder->flag = HAS_NULL;
  return 0;
}
static FORCE_INLINE int32_t tDiskColAddVal10(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  // bit map
  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  memset(pBuilder->pBitMap, 0, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  // value
  pBuilder->flag |= HAS_VALUE;

  SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    code = tDiskColPutValue(pBuilder, &cv);
    if (code) return code;
  }

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal12(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  memset(pBuilder->pBitMap, 0, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  pBuilder->flag |= HAS_NULL;

  return code;
}
static FORCE_INLINE int32_t tDiskColAddVal20(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  pBuilder->flag |= HAS_VALUE;

  memset(pBuilder->pBitMap, 0, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    code = tDiskColPutValue(pBuilder, &cv);
    if (code) return code;
  }

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal21(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  pBuilder->flag |= HAS_NONE;

  memset(pBuilder->pBitMap, 255, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return code;
}
static FORCE_INLINE int32_t tDiskColAddVal30(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  pBuilder->flag |= HAS_VALUE;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal));
  }
  SET_BIT2(pBitMap, pBuilder->nVal, 2);

  tFree(pBuilder->pBitMap);
  pBuilder->pBitMap = pBitMap;

  SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    code = tDiskColPutValue(pBuilder, &cv);
    if (code) return code;
  }

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal31(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return code;
}
static FORCE_INLINE int32_t tDiskColAddVal32(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  return code;
}
static FORCE_INLINE int32_t tDiskColAddVal40(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal41(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  pBuilder->flag |= HAS_NONE;

  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  memset(pBuilder->pBitMap, 255, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal42(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  pBuilder->flag |= HAS_NULL;

  int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
  code = tRealloc(&pBuilder->pBitMap, nBit);
  if (code) return code;

  memset(pBuilder->pBitMap, 255, nBit);
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal50(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal51(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal52(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  pBuilder->flag |= HAS_NULL;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal) ? 2 : 0);
  }
  SET_BIT2(pBitMap, pBuilder->nVal, 1);

  tFree(pBuilder->pBitMap);
  pBuilder->pBitMap = pBitMap;

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal60(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal61(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  pBuilder->flag |= HAS_NONE;

  uint8_t *pBitMap = NULL;
  code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;

  for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
    SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal) ? 2 : 1);
  }
  SET_BIT2(pBitMap, pBuilder->nVal, 0);

  tFree(pBuilder->pBitMap);
  pBuilder->pBitMap = pBitMap;

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal62(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal70(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 2);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal71(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 0);

  return tDiskColPutValue(pBuilder, pColVal);
}
static FORCE_INLINE int32_t tDiskColAddVal72(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) return code;
  SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 1);

  return tDiskColPutValue(pBuilder, pColVal);
}
static int32_t (*tDiskColAddValImpl[8][3])(SDiskColBuilder *pBuilder, SColVal *pColVal) = {
    {tDiskColAddVal00, tDiskColAddVal01, tDiskColAddVal02},  // 0
    {tDiskColAddVal10, NULL, tDiskColAddVal12},              // HAS_NONE
    {tDiskColAddVal20, tDiskColAddVal21, NULL},              // HAS_NULL
    {tDiskColAddVal30, tDiskColAddVal31, tDiskColAddVal32},  // HAS_NULL|HAS_NONE
    {tDiskColAddVal40, tDiskColAddVal41, tDiskColAddVal42},  // HAS_VALUE
    {tDiskColAddVal50, tDiskColAddVal51, tDiskColAddVal52},  // HAS_VALUE|HAS_NONE
    {tDiskColAddVal60, tDiskColAddVal61, tDiskColAddVal62},  // HAS_VALUE|HAS_NULL
    {tDiskColAddVal70, tDiskColAddVal71, tDiskColAddVal72}   // HAS_VALUE|HAS_NULL|HAS_NONE
};
// extern void (*tSmaUpdateImpl[])(SColumnDataAgg *pColAgg, SColVal *pColVal, uint8_t *minSet, uint8_t *maxSet);
static int32_t tDiskColAddVal(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  if (pBuilder->calcSma) {
    if (COL_VAL_IS_VALUE(pColVal)) {
      // tSmaUpdateImpl[pBuilder->type](&pBuilder->sma, pColVal, &pBuilder->minSet, &pBuilder->maxSet);
    } else {
      pBuilder->sma.numOfNull++;
    }
  }

  if (tDiskColAddValImpl[pBuilder->flag][pColVal->flag]) {
    code = tDiskColAddValImpl[pBuilder->flag][pColVal->flag](pBuilder, pColVal);
    if (code) return code;
  }

  pBuilder->nVal++;

  return code;
}

// SDiskDataBuilder ================================================
int32_t tDiskDataBuilderCreate(SDiskDataBuilder **ppBuilder) {
  int32_t code = 0;

  *ppBuilder = (SDiskDataBuilder *)taosMemoryCalloc(1, sizeof(SDiskDataBuilder));
  if (*ppBuilder == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    return code;
  }

  return code;
}

void *tDiskDataBuilderDestroy(SDiskDataBuilder *pBuilder) {
  if (pBuilder == NULL) return NULL;

  if (pBuilder->pUidC) tCompressorDestroy(pBuilder->pUidC);
  if (pBuilder->pVerC) tCompressorDestroy(pBuilder->pVerC);
  if (pBuilder->pKeyC) tCompressorDestroy(pBuilder->pKeyC);

  if (pBuilder->aBuilder) {
    for (int32_t iBuilder = 0; iBuilder < taosArrayGetSize(pBuilder->aBuilder); iBuilder++) {
      SDiskColBuilder *pDCBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iBuilder);
      tDiskColBuilderDestroy(pDCBuilder);
    }
    taosArrayDestroy(pBuilder->aBuilder);
  }
  for (int32_t iBuf = 0; iBuf < sizeof(pBuilder->aBuf) / sizeof(pBuilder->aBuf[0]); iBuf++) {
    tFree(pBuilder->aBuf[iBuf]);
  }
  tDiskDataDestroy(&pBuilder->dd);
  taosMemoryFree(pBuilder);

  return NULL;
}

int32_t tDiskDataBuilderInit(SDiskDataBuilder *pBuilder, STSchema *pTSchema, TABLEID *pId, uint8_t cmprAlg,
                             uint8_t calcSma) {
  int32_t code = 0;

  ASSERT(pId->suid || pId->uid);

  pBuilder->suid = pId->suid;
  pBuilder->uid = pId->uid;
  pBuilder->nRow = 0;
  pBuilder->cmprAlg = cmprAlg;
  pBuilder->calcSma = calcSma;
  pBuilder->bi = (SBlkInfo){.minUid = INT64_MAX,
                            .maxUid = INT64_MIN,
                            .minKey = TSKEY_MAX,
                            .maxKey = TSKEY_MIN,
                            .minVer = VERSION_MAX,
                            .maxVer = VERSION_MIN,
                            .minTKey = TSDBKEY_MAX,
                            .maxTKey = TSDBKEY_MIN};

  if (pBuilder->pUidC == NULL && (code = tCompressorCreate(&pBuilder->pUidC))) return code;
  code = tCompressStart(pBuilder->pUidC, TSDB_DATA_TYPE_BIGINT, cmprAlg);
  if (code) return code;

  if (pBuilder->pVerC == NULL && (code = tCompressorCreate(&pBuilder->pVerC))) return code;
  code = tCompressStart(pBuilder->pVerC, TSDB_DATA_TYPE_BIGINT, cmprAlg);
  if (code) return code;

  if (pBuilder->pKeyC == NULL && (code = tCompressorCreate(&pBuilder->pKeyC))) return code;
  code = tCompressStart(pBuilder->pKeyC, TSDB_DATA_TYPE_TIMESTAMP, cmprAlg);
  if (code) return code;

  if (pBuilder->aBuilder == NULL) {
    pBuilder->aBuilder = taosArrayInit(pTSchema->numOfCols - 1, sizeof(SDiskColBuilder));
    if (pBuilder->aBuilder == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }
  }

  pBuilder->nBuilder = 0;
  for (int32_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    STColumn *pTColumn = &pTSchema->columns[iCol];

    if (pBuilder->nBuilder >= taosArrayGetSize(pBuilder->aBuilder)) {
      SDiskColBuilder dc = tDiskColBuilderCreate();
      if (taosArrayPush(pBuilder->aBuilder, &dc) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        return code;
      }
    }

    SDiskColBuilder *pDCBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, pBuilder->nBuilder);

    code = tDiskColBuilderInit(pDCBuilder, pTColumn->colId, pTColumn->type, cmprAlg,
                               (calcSma && (pTColumn->flags & COL_SMA_ON)));
    if (code) return code;

    pBuilder->nBuilder++;
  }

  return code;
}

int32_t tDiskDataBuilderClear(SDiskDataBuilder *pBuilder) {
  int32_t code = 0;
  pBuilder->suid = 0;
  pBuilder->uid = 0;
  pBuilder->nRow = 0;
  return code;
}

int32_t tDiskDataAddRow(SDiskDataBuilder *pBuilder, TSDBROW *pRow, STSchema *pTSchema, TABLEID *pId) {
  int32_t code = 0;

  ASSERT(pBuilder->suid || pBuilder->uid);
  ASSERT(pId->suid == pBuilder->suid);

  TSDBKEY kRow = TSDBROW_KEY(pRow);
  if (tsdbKeyCmprFn(&pBuilder->bi.minTKey, &kRow) > 0) pBuilder->bi.minTKey = kRow;
  if (tsdbKeyCmprFn(&pBuilder->bi.maxTKey, &kRow) < 0) pBuilder->bi.maxTKey = kRow;

  // uid
  if (pBuilder->uid && pBuilder->uid != pId->uid) {
    ASSERT(pBuilder->suid);
    for (int32_t iRow = 0; iRow < pBuilder->nRow; iRow++) {
      code = tCompress(pBuilder->pUidC, &pBuilder->uid, sizeof(int64_t));
      if (code) return code;
    }
    pBuilder->uid = 0;
  }
  if (pBuilder->uid == 0) {
    code = tCompress(pBuilder->pUidC, &pId->uid, sizeof(int64_t));
    if (code) return code;
  }
  if (pBuilder->bi.minUid > pId->uid) pBuilder->bi.minUid = pId->uid;
  if (pBuilder->bi.maxUid < pId->uid) pBuilder->bi.maxUid = pId->uid;

  // version
  code = tCompress(pBuilder->pVerC, &kRow.version, sizeof(int64_t));
  if (code) return code;
  if (pBuilder->bi.minVer > kRow.version) pBuilder->bi.minVer = kRow.version;
  if (pBuilder->bi.maxVer < kRow.version) pBuilder->bi.maxVer = kRow.version;

  // TSKEY
  code = tCompress(pBuilder->pKeyC, &kRow.ts, sizeof(int64_t));
  if (code) return code;
  if (pBuilder->bi.minKey > kRow.ts) pBuilder->bi.minKey = kRow.ts;
  if (pBuilder->bi.maxKey < kRow.ts) pBuilder->bi.maxKey = kRow.ts;

  STSDBRowIter iter = {0};
  tsdbRowIterOpen(&iter, pRow, pTSchema);

  SColVal *pColVal = tsdbRowIterNext(&iter);
  for (int32_t iBuilder = 0; iBuilder < pBuilder->nBuilder; iBuilder++) {
    SDiskColBuilder *pDCBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iBuilder);

    while (pColVal && pColVal->cid < pDCBuilder->cid) {
      pColVal = tsdbRowIterNext(&iter);
    }

    if (pColVal && pColVal->cid == pDCBuilder->cid) {
      code = tDiskColAddVal(pDCBuilder, pColVal);
      if (code) return code;
      pColVal = tsdbRowIterNext(&iter);
    } else {
      code = tDiskColAddVal(pDCBuilder, &COL_VAL_NONE(pDCBuilder->cid, pDCBuilder->type));
      if (code) return code;
    }
  }
  pBuilder->nRow++;

  return code;
}

int32_t tGnrtDiskData(SDiskDataBuilder *pBuilder, const SDiskData **ppDiskData, const SBlkInfo **ppBlkInfo) {
  int32_t code = 0;

  ASSERT(pBuilder->nRow);

  *ppDiskData = NULL;
  *ppBlkInfo = NULL;

  SDiskData *pDiskData = &pBuilder->dd;
  // reset SDiskData
  pDiskData->hdr = (SDiskDataHdr){.delimiter = TSDB_FILE_DLMT,
                                  .fmtVer = 0,
                                  .suid = pBuilder->suid,
                                  .uid = pBuilder->uid,
                                  .szUid = 0,
                                  .szVer = 0,
                                  .szKey = 0,
                                  .szBlkCol = 0,
                                  .nRow = pBuilder->nRow,
                                  .cmprAlg = pBuilder->cmprAlg};
  pDiskData->pUid = NULL;
  pDiskData->pVer = NULL;
  pDiskData->pKey = NULL;

  // UID
  if (pBuilder->uid == 0) {
    code = tCompressEnd(pBuilder->pUidC, &pDiskData->pUid, &pDiskData->hdr.szUid, NULL);
    if (code) return code;
  }

  // VERSION
  code = tCompressEnd(pBuilder->pVerC, &pDiskData->pVer, &pDiskData->hdr.szVer, NULL);
  if (code) return code;

  // TSKEY
  code = tCompressEnd(pBuilder->pKeyC, &pDiskData->pKey, &pDiskData->hdr.szKey, NULL);
  if (code) return code;

  // aDiskCol
  if (pDiskData->aDiskCol) {
    taosArrayClear(pDiskData->aDiskCol);
  } else {
    pDiskData->aDiskCol = taosArrayInit(pBuilder->nBuilder, sizeof(SDiskCol));
    if (pDiskData->aDiskCol == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }
  }

  int32_t offset = 0;
  for (int32_t iBuilder = 0; iBuilder < pBuilder->nBuilder; iBuilder++) {
    SDiskColBuilder *pDCBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iBuilder);

    if (pDCBuilder->flag == HAS_NONE) continue;

    SDiskCol dCol;

    code = tGnrtDiskCol(pDCBuilder, &dCol);
    if (code) return code;

    dCol.bCol.offset = offset;
    offset = offset + dCol.bCol.szBitmap + dCol.bCol.szOffset + dCol.bCol.szValue;

    if (taosArrayPush(pDiskData->aDiskCol, &dCol) == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }

    pDiskData->hdr.szBlkCol += tPutBlockCol(NULL, &dCol.bCol);
  }

  *ppDiskData = pDiskData;
  *ppBlkInfo = &pBuilder->bi;
  return code;
}
