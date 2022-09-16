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

#include "lz4.h"
#include "tsdb.h"

typedef struct {
  int8_t   type;
  int8_t   cmprAlg;
  uint8_t *aBuf[2];
  int64_t  nBuf[2];
  union {
    // Timestamp ----
    struct {
      /* data */
    };
    // Integer ----
    struct {
      /* data */
    };
    // Binary ----
    struct {
      /* data */
    };
    // Float ----
    struct {
      /* data */
    };
    // Bool ----
    struct {
      int32_t bool_n;
      uint8_t bool_b;
    };
  };
} SCompressor;

// Timestamp =====================================================
static int32_t tCompTimestamp(SCompressor *pCmprsor, TSKEY ts) {
  int32_t code = 0;
  // TODO
  return code;
}

// Integer =====================================================
#define I64_SAFE_ADD(a, b) (((a) >= 0 && (b) <= INT64_MAX - (b)) || ((a) < 0 && (b) >= INT64_MIN - (a)))
#define SIMPLE8B_MAX       ((uint64_t)1152921504606846974LL)

static int32_t tCompI64(SCompressor *pCmprsor, int64_t val) {
  int32_t code = 0;
#if 0

  // raw copy
  if (pCmprsor->rawCopy) {
    memcpy(pCmprsor->pBuf + pCmprsor->nBuf, &val, sizeof(val));
    pCmprsor->nBuf += sizeof(val);
    pCmprsor->nVal++;
    goto _exit;
  }

  if (!I64_SAFE_ADD(val, pCmprsor->prevVal)) {
    pCmprsor->rawCopy = 1;
    // TODO: decompress and copy
    pCmprsor->nVal++;
    goto _exit;
  }

  int64_t diff = val - pCmprsor->prevVal;
  uint8_t zigzag = ZIGZAGE(int64_t, diff);

  if (zigzag >= SIMPLE8B_MAX) {
    pCmprsor->rawCopy = 1;
    // TODO: decompress and copy
    pCmprsor->nVal++;
    goto _exit;
  }

_exit:
#endif
  return code;
}

// Float =====================================================
static int32_t tCompFloat() {
  int32_t code = 0;
  // TODO
  return code;
}

// Binary =====================================================
static int32_t tCompBinary(SCompressor *pCmprsor, const uint8_t *pData, int32_t nData) {
  int32_t code = 0;

  if (nData) {
    memcpy(pCmprsor->aBuf[0] + pCmprsor->nBuf[0], pData, nData);
    pCmprsor->nBuf[0] += nData;
  }

  return code;
}

// Bool =====================================================
static uint8_t BOOL_CMPR_TABLE[] = {0b01, 0b0100, 0b010000, 0b01000000};
static int32_t tCompBool(SCompressor *pCmprsor, bool vBool) {
  int32_t code = 0;

  if (vBool) {
    pCmprsor->bool_b |= BOOL_CMPR_TABLE[pCmprsor->bool_n % 4];
  }
  pCmprsor->bool_n++;

  if (pCmprsor->bool_n % 4 == 0) {
    pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = pCmprsor->bool_b;
    pCmprsor->nBuf[0]++;
    pCmprsor->bool_b = 0;
  }

  return code;
}

// SCompressor =====================================================
int32_t tCompressorCreate(SCompressor **ppCmprsor) {
  int32_t code = 0;

  *ppCmprsor = (SCompressor *)taosMemoryCalloc(1, sizeof(SCompressor));
  if ((*ppCmprsor) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}

int32_t tCompressorDestroy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor) {
    for (int32_t iBuf = 0; iBuf < sizeof(pCmprsor->aBuf) / sizeof(pCmprsor->aBuf[0]); iBuf++) {
      tFree(pCmprsor->aBuf[iBuf]);
    }
  }

  return code;
}

int32_t tCompressorReset(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->type = type;
  pCmprsor->cmprAlg = cmprAlg;

  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      pCmprsor->bool_n = 0;
      pCmprsor->bool_b = 0;
      break;

    default:
      break;
  }

  return code;
}

int32_t tCompGen(SCompressor *pCmprsor, const uint8_t **ppData, int64_t *nData) {
  int32_t code = 0;

  if (pCmprsor->cmprAlg == TWO_STAGE_COMP || IS_VAR_DATA_TYPE(pCmprsor->type)) {
    code = tRealloc(&pCmprsor->aBuf[1], pCmprsor->nBuf[0] + 1);
    if (code) goto _exit;

    int64_t ret = LZ4_compress_default(pCmprsor->aBuf[0], pCmprsor->aBuf[1] + 1, pCmprsor->nBuf[0], pCmprsor->nBuf[0]);
    if (ret) {
      pCmprsor->aBuf[1][0] = 0;
      pCmprsor->nBuf[1] = ret + 1;
    } else {
      pCmprsor->aBuf[1][0] = 1;
      memcpy(pCmprsor->aBuf[1] + 1, pCmprsor->aBuf[0], pCmprsor->nBuf[0]);
      pCmprsor->nBuf[1] = pCmprsor->nBuf[0] + 1;
    }

    *ppData = pCmprsor->aBuf[1];
    *nData = pCmprsor->nBuf[1];
  } else {
    *ppData = pCmprsor->aBuf[0];
    *nData = pCmprsor->nBuf[0];
  }

_exit:
  return code;
}
