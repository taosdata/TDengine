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

typedef struct SDiskData SDiskData;
typedef struct SDiskCol  SDiskCol;

struct SDiskCol {
  int16_t      cid;
  int8_t       type;
  int8_t       flag;
  int32_t      nVal;
  SCompressor *pBitC;
  SCompressor *pOffC;
  SCompressor *pValC;
};

// SDiskCol ================================================
static int32_t tDiskColReset(SDiskCol *pDiskCol, int16_t cid, int8_t type, uint8_t cmprAlg) {
  int32_t code = 0;

  pDiskCol->cid = cid;
  pDiskCol->type = type;
  pDiskCol->flag = 0;
  pDiskCol->nVal = 0;

  tCompressorReset(pDiskCol->pBitC, TSDB_DATA_TYPE_TINYINT, cmprAlg, 1);
  tCompressorReset(pDiskCol->pOffC, TSDB_DATA_TYPE_INT, cmprAlg, 1);
  tCompressorReset(pDiskCol->pValC, type, cmprAlg, 1);

  return code;
}

static int32_t tDiskColAddVal0(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNone) {
    pDiskCol->flag = HAS_NONE;
  } else if (pColVal->isNull) {
    pDiskCol->flag = HAS_NULL;
  } else {
    pDiskCol->flag = HAS_VALUE;
    code = tCompress(pDiskCol->pValC, pColVal->value.pData, pColVal->value.nData /*TODO*/);
    if (code) goto _exit;
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal1(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (!pColVal->isNone) {
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal2(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (!pColVal->isNull) {
  }
  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal3(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNone) {
  } else if (pColVal->isNull) {
  } else {
  }
  pDiskCol->nVal++;

  return code;
}
static int32_t tDiskColAddVal4(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNone || pColVal->isNull) {
  } else {
  }

  pDiskCol->nVal++;

_exit:
  return code;
}
static int32_t tDiskColAddVal5(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNull) {
  } else {
  }
  pDiskCol->nVal++;

  return code;
}
static int32_t tDiskColAddVal6(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNone) {
  } else {
  }
  pDiskCol->nVal++;

  return code;
}
static int32_t tDiskColAddVal7(SDiskCol *pDiskCol, SColVal *pColVal) {
  int32_t code = 0;

  if (pColVal->isNone) {
  } else if (pColVal->isNull) {
  } else {
  }
  pDiskCol->nVal++;

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

// SDiskData ================================================
struct SDiskData {
  int64_t      suid;
  int64_t      uid;
  uint8_t      cmprAlg;
  SCompressor *pUidC;
  SCompressor *pVerC;
  SCompressor *pKeyC;
  int32_t      nDiskCol;
  SArray      *aDiskCol;
};

int32_t tDiskDataCreate(SDiskData *pDiskData) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tDiskDataDestroy(SDiskData *pDiskData) {
  int32_t code = 0;
  // TODO
  return code;
}

int32_t tDiskDataReset(SDiskData *pDiskData, STSchema *pTSchema, TABLEID *pId, uint8_t cmprAlg) {
  int32_t code = 0;

  pDiskData->suid = pId->suid;
  pDiskData->uid = pId->uid;
  pDiskData->cmprAlg = cmprAlg;

  for (int32_t iCol = 1; iCol < pTSchema->numOfCols; iCol++) {
  }

  return code;
}

int32_t tDiskDataAddRow(SDiskData *pDiskData, TSDBROW *pRow, STSchema *pTSchema, TABLEID *pId) {
  int32_t code = 0;

  ASSERT(pId->suid == pDiskData->suid);

  // uid
  code = tCompress(pDiskData->pUidC, &pId->uid, sizeof(int64_t));
  if (code) goto _exit;

  // version
  int64_t version = TSDBROW_VERSION(pRow);
  code = tCompress(pDiskData->pVerC, &version, sizeof(int64_t));
  if (code) goto _exit;

  // TSKEY
  TSKEY ts = TSDBROW_TS(pRow);
  code = tCompress(pDiskData->pVerC, &ts, sizeof(int64_t));
  if (code) goto _exit;

  SRowIter iter = {0};
  tRowIterInit(&iter, pRow, pTSchema);

  SColVal *pColVal = tRowIterNext(&iter);
  for (int32_t iDiskCol = 0; iDiskCol < pDiskData->nDiskCol; iDiskCol++) {
    SDiskCol *pDiskCol = (SDiskCol *)taosArrayGet(pDiskData->aDiskCol, iDiskCol);

    while (pColVal && pColVal->cid < pDiskCol->cid) {
      pColVal = tRowIterNext(&iter);
    }

    if (pColVal == NULL || pColVal->cid > pDiskCol->cid) {
      code = tDiskColAddValImpl[pDiskCol->flag](pDiskCol, &COL_VAL_NONE(pDiskCol->cid, pDiskCol->type));
      if (code) goto _exit;
    } else {
      code = tDiskColAddValImpl[pDiskCol->flag](pDiskCol, pColVal);
      if (code) goto _exit;
      pColVal = tRowIterNext(&iter);
    }
  }

_exit:
  return code;
}
