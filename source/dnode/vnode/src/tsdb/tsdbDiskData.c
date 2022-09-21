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
typedef struct SDiskCol         SDiskCol;
typedef struct SDiskData        SDiskData;

struct SDiskCol {
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

// SDiskCol ================================================
static int32_t tDiskColInit(SDiskCol *pDiskCol, int16_t cid, int8_t type, uint8_t cmprAlg) {
  int32_t code = 0;

  pDiskCol->cid = cid;
  pDiskCol->type = type;
  pDiskCol->flag = 0;
  pDiskCol->cmprAlg = cmprAlg;
  pDiskCol->nVal = 0;
  pDiskCol->offset = 0;

  if (IS_VAR_DATA_TYPE(type)) {
    if (pDiskCol->pOffC == NULL) {
      code = tCompressorCreate(&pDiskCol->pOffC);
      if (code) return code;
    }
    code = tCompressorReset(pDiskCol->pOffC, TSDB_DATA_TYPE_INT, cmprAlg);
    if (code) return code;
  }

  if (pDiskCol->pValC == NULL) {
    code = tCompressorCreate(&pDiskCol->pValC);
    if (code) return code;
  }
  code = tCompressorReset(pDiskCol->pValC, type, cmprAlg);
  if (code) return code;

  return code;
}

static int32_t tDiskColClear(SDiskCol *pDiskCol) {
  int32_t code = 0;

  tFree(pDiskCol->pBitMap);
  if (pDiskCol->pOffC) tCompressorDestroy(pDiskCol->pOffC);
  if (pDiskCol->pValC) tCompressorDestroy(pDiskCol->pValC);

  return code;
}

static int32_t tDiskColToBinary(SDiskCol *pDiskCol, const uint8_t **ppData, int32_t *nData) {
  int32_t code = 0;

  ASSERT(pDiskCol->flag && pDiskCol->flag != HAS_NONE);

  if (pDiskCol->flag == HAS_NULL) {
    return code;
  }

  // bitmap (todo)
  if (pDiskCol->flag != HAS_VALUE) {
  }

  // offset (todo)
  if (IS_VAR_DATA_TYPE(pDiskCol->type)) {
    code = tCompGen(pDiskCol->pOffC, NULL /* todo */, NULL /* todo */);
    if (code) return code;
  }

  // value (todo)
  if (pDiskCol->flag != (HAS_NULL | HAS_NONE)) {
    code = tCompGen(pDiskCol->pValC, NULL /* todo */, NULL /* todo */);
    if (code) return code;
  }

  return code;
}

static int32_t tDiskColAddValue(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    code = tCompress(pDiskCol->pOffC, &pDiskCol->offset, sizeof(int32_t));
    if (code) goto _exit;
    pDiskCol->offset += pColVal->value.nData;
  }
  code = tCompress(pDiskCol->pValC, pColVal->value.pData, pColVal->value.nData /*TODO*/);
  if (code) goto _exit;

_exit:
  return code;
}
static int32_t tDiskColAddVal0(SDiskCol *pDiskCol, SColVal *pColVal) {  // 0
  int32_t code = 0;

  if (pColVal->isNone) {
    pDiskCol->flag = HAS_NONE;
  } else if (pColVal->isNull) {
    pDiskCol->flag = HAS_NULL;
  } else {
    pDiskCol->flag = HAS_VALUE;
    code = tDiskColAddValue(pDiskCol, pColVal);
    if (code) goto _exit;
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal1(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_NONE
  int32_t code = 0;

  if (!pColVal->isNone) {
    // bit map
    int32_t nBit = BIT1_SIZE(pDiskCol->nVal + 1);

    code = tRealloc(&pDiskCol->pBitMap, nBit);
    if (code) goto _exit;

    memset(pDiskCol->pBitMap, 0, nBit);
    SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 1);

    // value
    if (pColVal->isNull) {
      pDiskCol->flag |= HAS_NULL;
    } else {
      pDiskCol->flag |= HAS_VALUE;

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
        code = tDiskColAddValue(pDiskCol, &cv);
        if (code) goto _exit;
      }

      code = tDiskColAddValue(pDiskCol, pColVal);
      if (code) goto _exit;
    }
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal2(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_NULL
  int32_t code = 0;

  if (!pColVal->isNull) {
    int32_t nBit = BIT1_SIZE(pDiskCol->nVal + 1);
    code = tRealloc(&pDiskCol->pBitMap, nBit);
    if (code) goto _exit;

    if (pColVal->isNone) {
      pDiskCol->flag |= HAS_NONE;

      memset(pDiskCol->pBitMap, 255, nBit);
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 0);
    } else {
      pDiskCol->flag |= HAS_VALUE;

      memset(pDiskCol->pBitMap, 0, nBit);
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 1);

      SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
      for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
        code = tDiskColAddValue(pDiskCol, &cv);
        if (code) goto _exit;
      }

      code = tDiskColAddValue(pDiskCol, pColVal);
      if (code) goto _exit;
    }
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal3(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_NULL|HAS_NONE
  int32_t code = 0;

  if (pColVal->isNone) {
    code = tRealloc(&pDiskCol->pBitMap, BIT1_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 0);
  } else if (pColVal->isNull) {
    code = tRealloc(&pDiskCol->pBitMap, BIT1_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 1);
  } else {
    pDiskCol->flag |= HAS_VALUE;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pDiskCol->pBitMap, iVal));
    }
    SET_BIT2(pBitMap, pDiskCol->nVal, 2);

    tFree(pDiskCol->pBitMap);
    pDiskCol->pBitMap = pBitMap;

    SColVal cv = COL_VAL_VALUE(pColVal->cid, pColVal->type, (SValue){0});
    for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
      code = tDiskColAddValue(pDiskCol, &cv);
      if (code) goto _exit;
    }

    code = tDiskColAddValue(pDiskCol, pColVal);
    if (code) goto _exit;
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal4(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_VALUE
  int32_t code = 0;

  if (pColVal->isNone || pColVal->isNull) {
    if (pColVal->isNone) {
      pDiskCol->flag |= HAS_NONE;
    } else {
      pDiskCol->flag |= HAS_NULL;
    }

    int32_t nBit = BIT1_SIZE(pDiskCol->nVal + 1);
    code = tRealloc(&pDiskCol->pBitMap, nBit);
    if (code) goto _exit;

    memset(pDiskCol->pBitMap, 255, nBit);
    SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 0);

    code = tDiskColAddValue(pDiskCol, pColVal);
    if (code) goto _exit;
  } else {
    code = tDiskColAddValue(pDiskCol, pColVal);
    if (code) goto _exit;
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal5(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_VALUE|HAS_NONE
  int32_t code = 0;

  if (pColVal->isNull) {
    pDiskCol->flag |= HAS_NULL;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pDiskCol->pBitMap, iVal) ? 2 : 0);
    }
    SET_BIT2(pBitMap, pDiskCol->nVal, 1);

    tFree(pDiskCol->pBitMap);
    pDiskCol->pBitMap = pBitMap;
  } else {
    code = tRealloc(&pDiskCol->pBitMap, BIT1_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    if (pColVal->isNone) {
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 0);
    } else {
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 1);
    }
  }
  code = tDiskColAddValue(pDiskCol, pColVal);
  if (code) goto _exit;
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal6(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL
  int32_t code = 0;

  if (pColVal->isNone) {
    pDiskCol->flag |= HAS_NONE;

    uint8_t *pBitMap = NULL;
    code = tRealloc(&pBitMap, BIT2_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    for (int32_t iVal = 0; iVal < pDiskCol->nVal; iVal++) {
      SET_BIT2(pBitMap, iVal, GET_BIT1(pDiskCol->pBitMap, iVal) ? 2 : 1);
    }
    SET_BIT2(pBitMap, pDiskCol->nVal, 0);

    tFree(pDiskCol->pBitMap);
    pDiskCol->pBitMap = pBitMap;
  } else {
    code = tRealloc(&pDiskCol->pBitMap, BIT1_SIZE(pDiskCol->nVal + 1));
    if (code) goto _exit;

    if (pColVal->isNull) {
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 0);
    } else {
      SET_BIT1(pDiskCol->pBitMap, pDiskCol->nVal, 1);
    }
  }
  code = tDiskColAddValue(pDiskCol, pColVal);
  if (code) goto _exit;
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal7(SDiskCol *pDiskCol, SColVal *pColVal) {  // HAS_VALUE|HAS_NULL|HAS_NONE
  int32_t code = 0;

  code = tRealloc(&pDiskCol->pBitMap, BIT2_SIZE(pDiskCol->nVal + 1));
  if (code) goto _exit;

  if (pColVal->isNone) {
    SET_BIT2(pDiskCol->pBitMap, pDiskCol->nVal, 0);
  } else if (pColVal->isNull) {
    SET_BIT2(pDiskCol->pBitMap, pDiskCol->nVal, 1);
  } else {
    SET_BIT2(pDiskCol->pBitMap, pDiskCol->nVal, 2);
  }
  code = tDiskColAddValue(pDiskCol, pColVal);
  if (code) goto _exit;
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t (*tDiskColAddValImpl[])(SDiskCol *pDiskCol, SColVal *pColVal) = {
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
struct SDiskDataBuilder {
  int64_t      suid;
  int64_t      uid;
  int32_t      nRow;
  uint8_t      cmprAlg;
  SCompressor *pUidC;
  SCompressor *pVerC;
  SCompressor *pKeyC;
  int32_t      nDiskCol;
  SArray      *aDiskCol;
  uint8_t     *aBuf[2];
};

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

  if (pBuilder->aDiskCol == NULL) {
    pBuilder->aDiskCol = taosArrayInit(pTSchema->numOfCols - 1, sizeof(SDiskCol));
    if (pBuilder->aDiskCol == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }
  }

  pBuilder->nDiskCol = 0;
  for (int32_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
    STColumn *pTColumn = &pTSchema->columns[iCol];

    if (pBuilder->nDiskCol >= taosArrayGetSize(pBuilder->aDiskCol)) {
      SDiskCol dc = (SDiskCol){0};
      if (taosArrayPush(pBuilder->aDiskCol, &dc) == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        return code;
      }
    }

    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, pBuilder->nDiskCol);

    code = tDiskColInit(pDiskCol, pTColumn->colId, pTColumn->type, cmprAlg);
    if (code) return code;

    pBuilder->nDiskCol++;
  }

  return code;
}

int32_t tDiskDataBuilderDestroy(SDiskDataBuilder *pBuilder) {
  int32_t code = 0;

  if (pBuilder->pUidC) tCompressorDestroy(pBuilder->pUidC);
  if (pBuilder->pVerC) tCompressorDestroy(pBuilder->pVerC);
  if (pBuilder->pKeyC) tCompressorDestroy(pBuilder->pKeyC);

  if (pBuilder->aDiskCol) {
    for (int32_t iDiskCol = 0; iDiskCol < taosArrayGetSize(pBuilder->aDiskCol); iDiskCol++) {
      SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, iDiskCol);
      tDiskColClear(pDiskCol);
    }
    taosArrayDestroy(pBuilder->aDiskCol);
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
  for (int32_t iDiskCol = 0; iDiskCol < pBuilder->nDiskCol; iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, iDiskCol);

    while (pColVal && pColVal->cid < pDiskCol->cid) {
      pColVal = tRowIterNext(&iter);
    }

    if (pColVal == NULL || pColVal->cid > pDiskCol->cid) {
      SColVal cv = COL_VAL_NONE(pDiskCol->cid, pDiskCol->type);
      code = tDiskColAddValImpl[pDiskCol->flag](pDiskCol, &cv);
      if (code) goto _exit;
    } else {
      code = tDiskColAddValImpl[pDiskCol->flag](pDiskCol, pColVal);
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

  SDiskDataHdr hdr = {.delimiter = TSDB_FILE_DLMT,
                      .fmtVer = 0,
                      .suid = pBuilder->suid,
                      .uid = pBuilder->uid,
                      .szUid = 0,
                      .szVer = 0,
                      .szKey = 0,
                      .szBlkCol = 0,
                      .nRow = pBuilder->nRow,
                      .cmprAlg = pBuilder->cmprAlg};

  // UID
  const uint8_t *pUid = NULL;
  if (pBuilder->uid == 0) {
    code = tCompGen(pBuilder->pUidC, &pUid, &hdr.szUid);
    if (code) return code;
  }

  // VERSION
  const uint8_t *pVer = NULL;
  code = tCompGen(pBuilder->pVerC, &pVer, &hdr.szVer);
  if (code) return code;

  // TSKEY
  const uint8_t *pKey = NULL;
  code = tCompGen(pBuilder->pKeyC, &pKey, &hdr.szKey);
  if (code) return code;

  int32_t offset = 0;
  for (int32_t iDiskCol = 0; iDiskCol < pBuilder->nDiskCol; iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, iDiskCol);

    if (pDiskCol->flag == HAS_NONE) continue;

    code = tDiskColToBinary(pDiskCol, NULL, NULL);
    if (code) return code;

    SBlockCol bCol = {.cid = pDiskCol->cid,
                      .type = pDiskCol->type,
                      // .smaOn = ,
                      .flag = pDiskCol->flag,
                      // .szOrigin =
                      // .szBitmap =
                      // .szOffset =
                      // .szValue =
                      .offset = offset};

    hdr.szBlkCol += tPutBlockCol(NULL, &bCol);
    offset = offset + bCol.szBitmap + bCol.szOffset + bCol.szValue;
  }

#if 0
  *nData = tPutDiskDataHdr(NULL, &hdr) + hdr.szUid + hdr.szVer + hdr.szKey + hdr.szBlkCol + offset;
  code = tRealloc(&pBuilder->aBuf[0], *nData);
  if (code) return code;
  *ppData = pBuilder->aBuf[0];

  int32_t n = 0;
  n += tPutDiskDataHdr(pBuilder->aBuf[0] + n, &hdr);
  if (hdr.szUid) {
    memcpy(pBuilder->aBuf[0] + n, pUid, hdr.szUid);
    n += hdr.szUid;
  }
  memcpy(pBuilder->aBuf[0] + n, pVer, hdr.szVer);
  n += hdr.szVer;
  memcpy(pBuilder->aBuf[0] + n, pKey, hdr.szKey);
  n += hdr.szKey;
  for (int32_t iDiskCol = 0; iDiskCol < pBuilder->nDiskCol; iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, iDiskCol);
    n += tPutBlockCol(pBuilder->aBuf[0] + n, NULL /*pDiskCol->bCol (todo) */);
  }
  for (int32_t iDiskCol = 0; iDiskCol < pBuilder->nDiskCol; iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pBuilder->aDiskCol, iDiskCol);
    // memcpy(pDiskData->aBuf[0] + n, NULL, );
    // n += 0;
  }
#endif

  return code;
}

// SDiskData ================================================
struct SDiskData {
  SDiskDataHdr   hdr;
  const uint8_t *pUid;
  const uint8_t *pVer;
  const uint8_t *pKey;
  SArray        *aBlockCol;
  SArray        *aColData;
};

int32_t tDiskDataDestroy(SDiskData *pDiskData) {
  int32_t code = 0;

  if (pDiskData->aBlockCol) taosArrayDestroy(pDiskData->aBlockCol);
  if (pDiskData->aColData) taosArrayDestroy(pDiskData->aColData);

  return code;
}
