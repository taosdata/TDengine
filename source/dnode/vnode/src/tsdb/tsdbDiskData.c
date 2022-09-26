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
  uint8_t       *aBuf[1];
};

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
                                     .szOrigin = 0,  // todo
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

    pDiskCol->bCol.szBitmap = tsCompressTinyint(pBuilder->pBitMap, nBit, nBit, pBuilder->aBuf[0], 0, pBuilder->cmprAlg,
                                                NULL, 0);  // todo: alloc
    pDiskCol->pBit = pBuilder->aBuf[0];
  }

  // OFFSET
  if (IS_VAR_DATA_TYPE(pBuilder->type)) {
    code = tCompressEnd(pBuilder->pOffC, &pDiskCol->pOff, &pDiskCol->bCol.szOffset);
    if (code) return code;
  }

  // VALUE
  if (pBuilder->flag != (HAS_NULL | HAS_NONE)) {
    code = tCompressEnd(pBuilder->pValC, &pDiskCol->pVal, &pDiskCol->bCol.szValue);
    if (code) return code;
  }

  return code;
}

extern void (*tSmaUpdateImpl[])(SColumnDataAgg *pColAgg, SColVal *pColVal, uint8_t *minSet, uint8_t *maxSet);
static FORCE_INLINE void tDiskColUpdateSma(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  if (!COL_VAL_IS_VALUE(pColVal)) {
    pBuilder->sma.numOfNull++;
  } else {
    tSmaUpdateImpl[pBuilder->type](&pBuilder->sma, pColVal, &pBuilder->minSet, &pBuilder->maxSet);
  }
}

static int32_t tDiskColPutValue(SDiskColBuilder *pBuilder, SColVal *pColVal) {
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
static int32_t tDiskColAddVal0(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // 0
  int32_t code = 0;

  if (COL_VAL_IS_NONE(pColVal)) {
    pBuilder->flag = HAS_NONE;
  } else if (COL_VAL_IS_NULL(pColVal)) {
    pBuilder->flag = HAS_NULL;
  } else {
    pBuilder->flag = HAS_VALUE;
    code = tDiskColPutValue(pBuilder, pColVal);
    if (code) return code;
  }

  return code;
}
static int32_t tDiskColAddVal1(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NONE
  int32_t code = 0;

  if (!COL_VAL_IS_NONE(pColVal)) {
    // bit map
    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);

    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) return code;

    memset(pBuilder->pBitMap, 0, nBit);
    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

    // value
    if (COL_VAL_IS_NULL(pColVal)) {
      pBuilder->flag |= HAS_NULL;
    } else {
      pBuilder->flag |= HAS_VALUE;

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
        code = tDiskColPutValue(pBuilder, &cv);
        if (code) return code;
      }

      code = tDiskColPutValue(pBuilder, pColVal);
      if (code) return code;
    }
  }

  return code;
}
static int32_t tDiskColAddVal2(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NULL
  int32_t code = 0;

  if (!COL_VAL_IS_NULL(pColVal)) {
    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) goto _exit;

    if (COL_VAL_IS_NONE(pColVal)) {
      pBuilder->flag |= HAS_NONE;

      memset(pBuilder->pBitMap, 255, nBit);
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      pBuilder->flag |= HAS_VALUE;

      memset(pBuilder->pBitMap, 0, nBit);
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
        code = tDiskColPutValue(pBuilder, &cv);
        if (code) goto _exit;
      }

      code = tDiskColPutValue(pBuilder, pColVal);
      if (code) goto _exit;
    }
  }

_exit:
  return code;
}
static int32_t tDiskColAddVal3(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NULL|HAS_NONE
  int32_t code = 0;

  if (COL_VAL_IS_NONE(pColVal)) {
    code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
  } else if (COL_VAL_IS_NULL(pColVal)) {
    code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);
  } else {
    pBuilder->flag |= HAS_VALUE;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal));
    }
    SET_BIT2(pBitMap, pBuilder->nVal, 2);

    tFree(pBuilder->pBitMap);
    pBuilder->pBitMap = pBitMap;

    SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
    for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
      code = tDiskColPutValue(pBuilder, &cv);
      if (code) goto _exit;
    }

    code = tDiskColPutValue(pBuilder, pColVal);
    if (code) goto _exit;
  }

_exit:
  return code;
}
static int32_t tDiskColAddVal4(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE
  int32_t code = 0;

  if (!COL_VAL_IS_VALUE(pColVal)) {
    if (COL_VAL_IS_NONE(pColVal)) {
      pBuilder->flag |= HAS_NONE;
    } else {
      pBuilder->flag |= HAS_NULL;
    }

    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) goto _exit;

    memset(pBuilder->pBitMap, 255, nBit);
    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

    code = tDiskColPutValue(pBuilder, pColVal);
    if (code) goto _exit;
  } else {
    code = tDiskColPutValue(pBuilder, pColVal);
    if (code) goto _exit;
  }

_exit:
  return code;
}
static int32_t tDiskColAddVal5(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NONE
  int32_t code = 0;

  if (COL_VAL_IS_NULL(pColVal)) {
    pBuilder->flag |= HAS_NULL;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal) ? 2 : 0);
    }
    SET_BIT2(pBitMap, pBuilder->nVal, 1);

    tFree(pBuilder->pBitMap);
    pBuilder->pBitMap = pBitMap;
  } else {
    code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    if (COL_VAL_IS_NONE(pColVal)) {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);
    }
  }
  code = tDiskColPutValue(pBuilder, pColVal);
  if (code) goto _exit;

_exit:
  return code;
}
static int32_t tDiskColAddVal6(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL
  int32_t code = 0;

  if (COL_VAL_IS_NONE(pColVal)) {
    pBuilder->flag |= HAS_NONE;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pBuilder->pBitMap, iVal) ? 2 : 1);
    }
    SET_BIT2(pBitMap, pBuilder->nVal, 0);

    tFree(pBuilder->pBitMap);
    pBuilder->pBitMap = pBitMap;
  } else {
    code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    if (COL_VAL_IS_NULL(pColVal)) {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);
    }
  }
  code = tDiskColPutValue(pBuilder, pColVal);
  if (code) goto _exit;

_exit:
  return code;
}
static int32_t tDiskColAddVal7(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL|HAS_NONE
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) goto _exit;

  if (COL_VAL_IS_NONE(pColVal)) {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 0);
  } else if (COL_VAL_IS_NULL(pColVal)) {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 1);
  } else {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 2);
  }
  code = tDiskColPutValue(pBuilder, pColVal);
  if (code) goto _exit;

_exit:
  return code;
}
static int32_t (*tDiskColAddValImpl[])(SDiskColBuilder *pBuilder, SColVal *pColVal) = {
    tDiskColAddVal0,  // 0
    tDiskColAddVal1,  // HAS_NONE
    tDiskColAddVal2,  // HAS_NULL
    tDiskColAddVal3,  // HAS_NULL|HAS_NONE
    tDiskColAddVal4,  // HAS_VALUE
    tDiskColAddVal5,  // HAS_VALUE|HAS_NONE
    tDiskColAddVal6,  // HAS_VALUE|HAS_NULL
    tDiskColAddVal7,  // HAS_VALUE|HAS_NULL|HAS_NONE
};
static int32_t tDiskColAddVal(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  if (pBuilder->calcSma) tDiskColUpdateSma(pBuilder, pColVal);

  code = tDiskColAddValImpl[pBuilder->type](pBuilder, pColVal);
  if (code) return code;

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
  taosMemoryFree(pBuilder);

  return NULL;
}

int32_t tDiskDataBuilderInit(SDiskDataBuilder *pBuilder, STSchema *pTSchema, TABLEID *pId, uint8_t cmprAlg,
                             uint8_t calcSma) {
  int32_t code = 0;

  pBuilder->suid = pId->suid;
  pBuilder->uid = pId->uid;
  pBuilder->nRow = 0;
  pBuilder->cmprAlg = cmprAlg;
  pBuilder->calcSma = calcSma;

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

    SDiskColBuilder *pDiskColBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, pBuilder->nBuilder);

    code = tDiskColBuilderInit(pDiskColBuilder, pTColumn->colId, pTColumn->type, cmprAlg,
                               (calcSma && (pTColumn->flags & COL_SMA_ON)));
    if (code) return code;

    pBuilder->nBuilder++;
  }

  return code;
}

int32_t tDiskDataBuilderAddRow(SDiskDataBuilder *pBuilder, TSDBROW *pRow, STSchema *pTSchema, TABLEID *pId) {
  int32_t code = 0;

  ASSERT(pId->suid == pBuilder->suid);

  // uid
  if (pBuilder->uid && pBuilder->uid != pId->uid) {
    ASSERT(!pBuilder->calcSma);
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

  // version
  int64_t version = TSDBROW_VERSION(pRow);
  code = tCompress(pBuilder->pVerC, &version, sizeof(int64_t));
  if (code) return code;

  // TSKEY
  TSKEY ts = TSDBROW_TS(pRow);
  code = tCompress(pBuilder->pKeyC, &ts, sizeof(int64_t));
  if (code) return code;

  SRowIter iter = {0};
  tRowIterInit(&iter, pRow, pTSchema);

  SColVal *pColVal = tRowIterNext(&iter);
  for (int32_t iBuilder = 0; iBuilder < pBuilder->nBuilder; iBuilder++) {
    SDiskColBuilder *pDCBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iBuilder);

    while (pColVal && pColVal->cid < pDCBuilder->cid) {
      pColVal = tRowIterNext(&iter);
    }

    if (pColVal == NULL || pColVal->cid > pDCBuilder->cid) {
      SColVal cv = COL_VAL_NONE(pDCBuilder->cid, pDCBuilder->type);
      code = tDiskColAddVal(pDCBuilder, &cv);
      if (code) return code;
    } else {
      code = tDiskColAddVal(pDCBuilder, pColVal);
      if (code) return code;
      pColVal = tRowIterNext(&iter);
    }
  }
  pBuilder->nRow++;

  return code;
}

int32_t tGnrtDiskData(SDiskDataBuilder *pBuilder, SDiskData *pDiskData) {
  int32_t code = 0;

  ASSERT(pBuilder->nRow);

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
  if (pDiskData->aDiskCol) {
    taosArrayClear(pDiskData->aDiskCol);
  } else {
    pDiskData->aDiskCol = taosArrayInit(pBuilder->nBuilder, sizeof(SDiskCol));
    if (pDiskData->aDiskCol == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }
  }

  // UID
  if (pBuilder->uid == 0) {
    code = tCompressEnd(pBuilder->pUidC, &pDiskData->pUid, &pDiskData->hdr.szUid);
    if (code) return code;
  }

  // VERSION
  code = tCompressEnd(pBuilder->pVerC, &pDiskData->pVer, &pDiskData->hdr.szVer);
  if (code) return code;

  // TSKEY
  code = tCompressEnd(pBuilder->pKeyC, &pDiskData->pKey, &pDiskData->hdr.szKey);
  if (code) return code;

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

  return code;
}

// SDiskData ================================================
int32_t tDiskDataDestroy(SDiskData *pDiskData) {
  int32_t code = 0;
  pDiskData->aDiskCol = taosArrayDestroy(pDiskData->aDiskCol);
  return code;
}
