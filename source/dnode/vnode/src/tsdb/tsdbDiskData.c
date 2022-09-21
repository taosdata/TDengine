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

typedef struct SDiskDataBuilder SDiskDataBuilder;
typedef struct SDiskColBuilder  SDiskColBuilder;

struct SDiskColBuilder {
  int16_t      cid;
  int8_t       type;
  int8_t       flag;
  uint8_t      cmprAlg;
  int32_t      nVal;
  uint8_t     *pBitMap;
  int32_t      offset;
  SCompressor *pOffC;
  SCompressor *pValC;
};

struct SDiskDataBuilder {
  int64_t      suid;
  int64_t      uid;
  int32_t      nRow;
  uint8_t      cmprAlg;
  SCompressor *pUidC;
  SCompressor *pVerC;
  SCompressor *pKeyC;
  int32_t      nBuilder;
  SArray      *aBuilder;
  uint8_t     *aBuf[2];
};

// SDiskColBuilder ================================================
static int32_t tDiskColInit(SDiskColBuilder *pBuilder, int16_t cid, int8_t type, uint8_t cmprAlg) {
  int32_t code = 0;

  pBuilder->cid = cid;
  pBuilder->type = type;
  pBuilder->flag = 0;
  pBuilder->cmprAlg = cmprAlg;
  pBuilder->nVal = 0;
  pBuilder->offset = 0;

  if (IS_VAR_DATA_TYPE(type)) {
    if (pBuilder->pOffC == NULL) {
      code = tCompressorCreate(&pBuilder->pOffC);
      if (code) return code;
    }
    code = tCompressorReset(pBuilder->pOffC, TSDB_DATA_TYPE_INT, cmprAlg);
    if (code) return code;
  }

  if (pBuilder->pValC == NULL) {
    code = tCompressorCreate(&pBuilder->pValC);
    if (code) return code;
  }
  code = tCompressorReset(pBuilder->pValC, type, cmprAlg);
  if (code) return code;

  return code;
}

static int32_t tDiskColClear(SDiskColBuilder *pBuilder) {
  int32_t code = 0;

  tFree(pBuilder->pBitMap);
  if (pBuilder->pOffC) tCompressorDestroy(pBuilder->pOffC);
  if (pBuilder->pValC) tCompressorDestroy(pBuilder->pValC);

  return code;
}

static int32_t tGnrtDiskCol(SDiskColBuilder *pBuilder, SDiskCol *pDiskCol) {
  int32_t code = 0;

  ASSERT(pBuilder->flag && pBuilder->flag != HAS_NONE);

  pDiskCol->bCol = (SBlockCol){.cid = pBuilder->cid,
                               .type = pBuilder->type,
                               .smaOn = 1, /* todo */
                               .flag = pBuilder->flag,
                               .szOrigin = 0,  // todo
                               .szBitmap = 0,  // todo
                               .szOffset = 0,
                               .szValue = 0,  // todo
                               .offset = 0};
  pDiskCol->pBit = NULL;
  pDiskCol->pOff = NULL;
  pDiskCol->pVal = NULL;

  if (pBuilder->flag == HAS_NULL) return code;

  // BITMAP
  if (pBuilder->flag != HAS_VALUE) {
    // TODO
  }

  // OFFSET
  if (IS_VAR_DATA_TYPE(pBuilder->type)) {
    code = tCompGen(pBuilder->pOffC, &pDiskCol->pOff, &pDiskCol->bCol.szOffset);
    if (code) return code;
  }

  // VALUE
  if (pBuilder->flag != (HAS_NULL | HAS_NONE)) {
    code = tCompGen(pBuilder->pValC, &pDiskCol->pVal, &pDiskCol->bCol.szValue);
    if (code) return code;
  }

  return code;
}

static int32_t tDiskColAddValue(SDiskColBuilder *pBuilder, SColVal *pColVal) {
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    code = tCompress(pBuilder->pOffC, &pBuilder->offset, sizeof(int32_t));
    if (code) goto _exit;
    pBuilder->offset += pColVal->value.nData;

    code = tCompress(pBuilder->pValC, pColVal->value.pData, pColVal->value.nData);
    if (code) goto _exit;
  } else {
    code = tCompress(pBuilder->pValC, &pColVal->value.val, tDataTypes[pColVal->type].bytes);
    if (code) goto _exit;
  }

_exit:
  return code;
}
static int32_t tDiskColAddVal0(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // 0
  int32_t code = 0;

  if (pColVal->isNone) {
    pBuilder->flag = HAS_NONE;
  } else if (pColVal->isNull) {
    pBuilder->flag = HAS_NULL;
  } else {
    pBuilder->flag = HAS_VALUE;
    code = tDiskColAddValue(pBuilder, pColVal);
    if (code) goto _exit;
  }
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal1(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NONE
  int32_t code = 0;

  if (!pColVal->isNone) {
    // bit map
    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);

    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) goto _exit;

    memset(pBuilder->pBitMap, 0, nBit);
    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

    // value
    if (pColVal->isNull) {
      pBuilder->flag |= HAS_NULL;
    } else {
      pBuilder->flag |= HAS_VALUE;

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
        code = tDiskColAddValue(pBuilder, &cv);
        if (code) goto _exit;
      }

      code = tDiskColAddValue(pBuilder, pColVal);
      if (code) goto _exit;
    }
  }
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal2(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NULL
  int32_t code = 0;

  if (!pColVal->isNull) {
    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) goto _exit;

    if (pColVal->isNone) {
      pBuilder->flag |= HAS_NONE;

      memset(pBuilder->pBitMap, 255, nBit);
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      pBuilder->flag |= HAS_VALUE;

      memset(pBuilder->pBitMap, 0, nBit);
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pBuilder->nVal; iVal++) {
        code = tDiskColAddValue(pBuilder, &cv);
        if (code) goto _exit;
      }

      code = tDiskColAddValue(pBuilder, pColVal);
      if (code) goto _exit;
    }
  }
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal3(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_NULL|HAS_NONE
  int32_t code = 0;

  if (pColVal->isNone) {
    code = tRealloc(&pBuilder->pBitMap, BIT1_SIZE(pBuilder->nVal + 1));
    if (code) goto _exit;

    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
  } else if (pColVal->isNull) {
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
      code = tDiskColAddValue(pBuilder, &cv);
      if (code) goto _exit;
    }

    code = tDiskColAddValue(pBuilder, pColVal);
    if (code) goto _exit;
  }
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal4(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE
  int32_t code = 0;

  if (pColVal->isNone || pColVal->isNull) {
    if (pColVal->isNone) {
      pBuilder->flag |= HAS_NONE;
    } else {
      pBuilder->flag |= HAS_NULL;
    }

    int32_t nBit = BIT1_SIZE(pBuilder->nVal + 1);
    code = tRealloc(&pBuilder->pBitMap, nBit);
    if (code) goto _exit;

    memset(pBuilder->pBitMap, 255, nBit);
    SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);

    code = tDiskColAddValue(pBuilder, pColVal);
    if (code) goto _exit;
  } else {
    code = tDiskColAddValue(pBuilder, pColVal);
    if (code) goto _exit;
  }
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal5(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NONE
  int32_t code = 0;

  if (pColVal->isNull) {
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

    if (pColVal->isNone) {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);
    }
  }
  code = tDiskColAddValue(pBuilder, pColVal);
  if (code) goto _exit;
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal6(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL
  int32_t code = 0;

  if (pColVal->isNone) {
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

    if (pColVal->isNull) {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 0);
    } else {
      SET_BIT1(pBuilder->pBitMap, pBuilder->nVal, 1);
    }
  }
  code = tDiskColAddValue(pBuilder, pColVal);
  if (code) goto _exit;
  pBuilder->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal7(SDiskColBuilder *pBuilder, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL|HAS_NONE
  int32_t code = 0;

  code = tRealloc(&pBuilder->pBitMap, BIT2_SIZE(pBuilder->nVal + 1));
  if (code) goto _exit;

  if (pColVal->isNone) {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 0);
  } else if (pColVal->isNull) {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 1);
  } else {
    SET_BIT2(pBuilder->pBitMap, pBuilder->nVal, 2);
  }
  code = tDiskColAddValue(pBuilder, pColVal);
  if (code) goto _exit;
  pBuilder->nVal++;

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

// SDiskDataBuilder ================================================
int32_t tDiskDataBuilderInit(SDiskDataBuilder *pBuilder, STSchema *pTSchema, TABLEID *pId, uint8_t cmprAlg) {
  int32_t code = 0;

  pBuilder->suid = pId->suid;
  pBuilder->uid = pId->uid;
  pBuilder->nRow = 0;
  pBuilder->cmprAlg = cmprAlg;

  if (pBuilder->pUidC == NULL) {
    code = tCompressorCreate(&pBuilder->pUidC);
    if (code) return code;
  }
  code = tCompressorReset(pBuilder->pUidC, TSDB_DATA_TYPE_BIGINT, cmprAlg);
  if (code) return code;

  if (pBuilder->pVerC == NULL) {
    code = tCompressorCreate(&pBuilder->pVerC);
    if (code) return code;
  }
  code = tCompressorReset(pBuilder->pVerC, TSDB_DATA_TYPE_BIGINT, cmprAlg);
  if (code) return code;

  if (pBuilder->pKeyC == NULL) {
    code = tCompressorCreate(&pBuilder->pKeyC);
    if (code) return code;
  }
  code = tCompressorReset(pBuilder->pKeyC, TSDB_DATA_TYPE_TIMESTAMP, cmprAlg);
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
      SDiskColBuilder dc = (SDiskColBuilder){0};
      if (taosArrayPush(pBuilder->aBuilder, &dc) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        return code;
      }
    }

    SDiskColBuilder *pDiskColBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, pBuilder->nBuilder);

    code = tDiskColInit(pDiskColBuilder, pTColumn->colId, pTColumn->type, cmprAlg);
    if (code) return code;

    pBuilder->nBuilder++;
  }

  return code;
}

int32_t tDiskDataBuilderDestroy(SDiskDataBuilder *pBuilder) {
  int32_t code = 0;

  if (pBuilder->pUidC) tCompressorDestroy(pBuilder->pUidC);
  if (pBuilder->pVerC) tCompressorDestroy(pBuilder->pVerC);
  if (pBuilder->pKeyC) tCompressorDestroy(pBuilder->pKeyC);

  if (pBuilder->aBuilder) {
    for (int32_t iDiskColBuilder = 0; iDiskColBuilder < taosArrayGetSize(pBuilder->aBuilder); iDiskColBuilder++) {
      SDiskColBuilder *pDiskColBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iDiskColBuilder);
      tDiskColClear(pDiskColBuilder);
    }
    taosArrayDestroy(pBuilder->aBuilder);
  }
  for (int32_t iBuf = 0; iBuf < sizeof(pBuilder->aBuf) / sizeof(pBuilder->aBuf[0]); iBuf++) {
    tFree(pBuilder->aBuf[iBuf]);
  }

  return code;
}

int32_t tDiskDataBuilderAddRow(SDiskDataBuilder *pBuilder, TSDBROW *pRow, STSchema *pTSchema, TABLEID *pId) {
  int32_t code = 0;

  ASSERT(pId->suid == pBuilder->suid);

  // uid
  if (pBuilder->uid && pBuilder->uid != pId->uid) {
    for (int32_t iRow = 0; iRow < pBuilder->nRow; iRow++) {
      code = tCompress(pBuilder->pUidC, &pBuilder->uid, sizeof(int64_t));
      if (code) goto _exit;
    }
    pBuilder->uid = 0;
  }
  if (pBuilder->uid == 0) {
    code = tCompress(pBuilder->pUidC, &pId->uid, sizeof(int64_t));
    if (code) goto _exit;
  }

  // version
  int64_t version = TSDBROW_VERSION(pRow);
  code = tCompress(pBuilder->pVerC, &version, sizeof(int64_t));
  if (code) goto _exit;

  // TSKEY
  TSKEY ts = TSDBROW_TS(pRow);
  code = tCompress(pBuilder->pKeyC, &ts, sizeof(int64_t));
  if (code) goto _exit;

  SRowIter iter = {0};
  tRowIterInit(&iter, pRow, pTSchema);

  SColVal *pColVal = tRowIterNext(&iter);
  for (int32_t iDiskColBuilder = 0; iDiskColBuilder < pBuilder->nBuilder; iDiskColBuilder++) {
    SDiskColBuilder *pDiskColBuilder = (SDiskColBuilder *)taosArrayGet(pBuilder->aBuilder, iDiskColBuilder);

    while (pColVal && pColVal->cid < pDiskColBuilder->cid) {
      pColVal = tRowIterNext(&iter);
    }

    if (pColVal == NULL || pColVal->cid > pDiskColBuilder->cid) {
      SColVal cv = COL_VAL_NONE(pDiskColBuilder->cid, pDiskColBuilder->type);
      code = tDiskColAddValImpl[pDiskColBuilder->flag](pDiskColBuilder, &cv);
      if (code) goto _exit;
    } else {
      code = tDiskColAddValImpl[pDiskColBuilder->flag](pDiskColBuilder, pColVal);
      if (code) goto _exit;
      pColVal = tRowIterNext(&iter);
    }
  }
  pBuilder->nRow++;

_exit:
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
    code = tCompGen(pBuilder->pUidC, &pDiskData->pUid, &pDiskData->hdr.szUid);
    if (code) return code;
  }

  // VERSION
  code = tCompGen(pBuilder->pVerC, &pDiskData->pVer, &pDiskData->hdr.szVer);
  if (code) return code;

  // TSKEY
  code = tCompGen(pBuilder->pKeyC, &pDiskData->pKey, &pDiskData->hdr.szKey);
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

    if (pDCBuilder->flag != HAS_NULL) {
      pDiskData->hdr.szBlkCol += tPutBlockCol(NULL, &dCol.bCol);
    }
  }

  return code;
}

// SDiskData ================================================
int32_t tDiskDataDestroy(SDiskData *pDiskData) {
  int32_t code = 0;
  if (pDiskData->aDiskCol) taosArrayDestroy(pDiskData->aDiskCol);
  return code;
}
