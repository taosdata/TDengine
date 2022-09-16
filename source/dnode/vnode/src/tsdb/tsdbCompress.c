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

#define I64_SAFE_ADD(a, b) (((a) >= 0 && (b) <= INT64_MAX - (b)) || ((a) < 0 && (b) >= INT64_MIN - (a)))

typedef struct {
  int8_t   type;
  int8_t   cmprAlg;
  uint8_t *aBuf[2];
  int64_t  nBuf[2];
  union {
    // Timestamp ----
    struct {
      int8_t   ts_copy;
      int32_t  ts_n;
      int64_t  ts_prev_val;
      int64_t  ts_prev_delta;
      uint8_t *ts_flag_p;
    };
    // Integer ----
    struct {
      int8_t  i_copy;
      int32_t i_n;
    };
    // Float ----
    struct {
      int32_t  f_n;
      uint32_t f_prev;
      uint8_t *f_flag_p;
    };
    // Double ----
    struct {
      int32_t  d_n;
      uint64_t d_prev;
      uint8_t *d_flag_p;
    };
    // Bool ----
    struct {
      int32_t bool_n;
    };
    // Binary ----
    struct {
      int32_t b_n;
    };
  };
} SCompressor;

// Timestamp =====================================================
static int32_t tCompSetCopyMode(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor->ts_n) {
    code = tRealloc(&pCmprsor->aBuf[1], sizeof(int64_t) * (pCmprsor->ts_n + 1));
    if (code) return code;
    pCmprsor->nBuf[1] = 1;

    int64_t  n = 1;
    int64_t  valPrev;
    int64_t  delPrev;
    uint64_t vZigzag;
    while (n < pCmprsor->nBuf[0]) {
      uint8_t n1 = pCmprsor->aBuf[0][0] & 0xf;
      uint8_t n2 = pCmprsor->aBuf[0][0] >> 4;

      n++;

      vZigzag = 0;
      for (uint8_t i = 0; i < n1; i++) {
        vZigzag |= (((uint64_t)pCmprsor->aBuf[0][n]) << (sizeof(int64_t) * i));
        n++;
      }
      int64_t delta_of_delta = ZIGZAGD(int64_t, vZigzag);
      if (n == 2) {
        delPrev = 0;
        valPrev = delta_of_delta;
      } else {
        delPrev = delta_of_delta + delPrev;
        valPrev = delPrev + valPrev;
      }

      memcpy(pCmprsor->aBuf[1] + pCmprsor->nBuf[1], &valPrev, sizeof(int64_t));
      pCmprsor->nBuf[1] += sizeof(int64_t);

      if (n >= pCmprsor->nBuf[0]) break;

      vZigzag = 0;
      for (uint8_t i = 0; i < n2; i++) {
        vZigzag |= (((uint64_t)pCmprsor->aBuf[0][n]) << (sizeof(int64_t) * i));
        n++;
      }
      delta_of_delta = ZIGZAGD(int64_t, vZigzag);
      delPrev = delta_of_delta + delPrev;
      valPrev = delPrev + valPrev;
    }

    uint8_t *pBuf = pCmprsor->aBuf[0];
    pCmprsor->aBuf[0] = pCmprsor->aBuf[1];
    pCmprsor->aBuf[1] = pBuf;
    pCmprsor->nBuf[0] = pCmprsor->nBuf[1];
  } else {
    // TODO
  }

  pCmprsor->aBuf[0][0] = 0;
  pCmprsor->ts_copy = 1;

  return code;
}
static int32_t tCompTimestamp(SCompressor *pCmprsor, TSKEY ts) {
  int32_t code = 0;

  if (pCmprsor->ts_n == 0) {
    pCmprsor->ts_prev_val = ts;
    pCmprsor->ts_prev_delta = -ts;
  }

  if (pCmprsor->ts_copy) goto _copy_exit;

  if (!I64_SAFE_ADD(ts, -pCmprsor->ts_prev_val)) {
    code = tCompSetCopyMode(pCmprsor);
    if (code) return code;
    goto _copy_exit;
  }

  int64_t delta = ts - pCmprsor->ts_prev_val;

  if (!I64_SAFE_ADD(delta, -pCmprsor->ts_prev_delta)) {
    code = tCompSetCopyMode(pCmprsor);
    if (code) return code;
    goto _copy_exit;
  }

  int64_t  delta_of_delta = delta - pCmprsor->ts_prev_delta;
  uint64_t zigzag_value = ZIGZAGE(int64_t, delta_of_delta);

  pCmprsor->ts_prev_val = ts;
  pCmprsor->ts_prev_delta = delta;

  if (pCmprsor->ts_n & 0x1 == 0) {
    code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0] + 17 /*sizeof(int64_t) * 2 + 1*/);
    if (code) return code;

    pCmprsor->ts_flag_p = &pCmprsor->aBuf[0][pCmprsor->nBuf[0]];
    pCmprsor->nBuf[0]++;
    pCmprsor->ts_flag_p[0] = 0;

    while (zigzag_value) {
      pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = (zigzag_value & 0xff);
      pCmprsor->nBuf[0]++;
      pCmprsor->ts_flag_p[0]++;
    }
  } else {
    while (zigzag_value) {
      pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = (zigzag_value & 0xff);
      pCmprsor->nBuf[0]++;
      pCmprsor->ts_flag_p += (uint8_t)0x10;
    }
  }

  pCmprsor->ts_n++;
  return code;

_copy_exit:
  code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0] + sizeof(int64_t));
  if (code) return code;

  memcpy(pCmprsor->aBuf[0] + pCmprsor->nBuf[0], &ts, sizeof(ts));
  pCmprsor->nBuf[0] += sizeof(ts);

  pCmprsor->ts_n++;
  return code;
}

// Integer =====================================================
#define SIMPLE8B_MAX ((uint64_t)1152921504606846974LL)

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
static int32_t tCompFloat(SCompressor *pCmprsor, float f) {
  int32_t code = 0;

  union {
    float    f;
    uint32_t u;
  } val = {.f = f};

  uint32_t diff = val.u ^ pCmprsor->f_prev;
  pCmprsor->f_prev = val.u;

  int32_t clz, ctz;
  if (diff) {
    clz = BUILDIN_CLZ(diff);
    ctz = BUILDIN_CTZ(diff);
  } else {
    clz = sizeof(uint32_t);
    ctz = sizeof(uint32_t);
  }

  if (pCmprsor->f_n & 0x1 == 0) {
    code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0] + 9 /* sizeof(float) * 2 + 1 */);
    if (code) return code;

    pCmprsor->f_flag_p = &pCmprsor->aBuf[0][pCmprsor->nBuf[0]];
    pCmprsor->nBuf[0]++;

    if (clz < ctz) {
      uint8_t nBytes = sizeof(uint32_t) - ctz / BITS_PER_BYTE;
      pCmprsor->f_flag_p[0] = (0x08 | (nBytes - 1));
      diff >>= (32 - nBytes * BITS_PER_BYTE);
    } else {
      uint8_t nBytes = sizeof(uint32_t) - clz / BITS_PER_BYTE;
      pCmprsor->f_flag_p[0] = nBytes - 1;
    }
  } else {
    if (clz < ctz) {
      uint8_t nBytes = sizeof(uint32_t) - ctz / BITS_PER_BYTE;
      pCmprsor->f_flag_p[0] |= ((0x08 | (nBytes - 1)) << 4);
      diff >>= (32 - nBytes * BITS_PER_BYTE);
    } else {
      uint8_t nBytes = sizeof(uint32_t) - clz / BITS_PER_BYTE;
      pCmprsor->f_flag_p[0] |= ((nBytes - 1) << 4);
    }
  }

  while (diff) {
    pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = (diff & 0xff);
    pCmprsor->nBuf[0]++;
    diff >>= BITS_PER_BYTE;
  }

  pCmprsor->f_n++;
  return code;
}

// Double =====================================================
static int32_t tCompDouble(SCompressor *pCmprsor, double d) {
  int32_t code = 0;

  union {
    double   d;
    uint64_t u;
  } val = {.d = d};

  uint64_t diff = val.u ^ pCmprsor->d_prev;
  pCmprsor->d_prev = val.u;

  int32_t clz, ctz;
  if (diff) {
    clz = BUILDIN_CLZ(diff);
    ctz = BUILDIN_CTZ(diff);
  } else {
    clz = sizeof(uint64_t);
    ctz = sizeof(uint64_t);
  }

  if (pCmprsor->d_n & 0x1 == 0) {
    code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0] + 17 /* sizeof(double) * 2 + 1 */);
    if (code) return code;

    pCmprsor->d_flag_p = &pCmprsor->aBuf[0][pCmprsor->nBuf[0]];
    pCmprsor->nBuf[0]++;

    if (clz < ctz) {
      uint8_t nBytes = sizeof(uint64_t) - ctz / BITS_PER_BYTE;
      pCmprsor->d_flag_p[0] = (0x08 | (nBytes - 1));
      diff >>= (64 - nBytes * BITS_PER_BYTE);
    } else {
      uint8_t nBytes = sizeof(uint64_t) - clz / BITS_PER_BYTE;
      pCmprsor->d_flag_p[0] = nBytes - 1;
    }
  } else {
    if (clz < ctz) {
      uint8_t nBytes = sizeof(uint64_t) - ctz / BITS_PER_BYTE;
      pCmprsor->d_flag_p[0] |= ((0x08 | (nBytes - 1)) << 4);
      diff >>= (64 - nBytes * BITS_PER_BYTE);
    } else {
      uint8_t nBytes = sizeof(uint64_t) - clz / BITS_PER_BYTE;
      pCmprsor->d_flag_p[0] |= ((nBytes - 1) << 4);
    }
  }

  while (diff) {
    pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = (diff & 0xff);
    pCmprsor->nBuf[0]++;
    diff >>= BITS_PER_BYTE;
  }

  pCmprsor->d_n++;
  return code;
}

// Binary =====================================================
static int32_t tCompBinary(SCompressor *pCmprsor, const uint8_t *pData, int32_t nData) {
  int32_t code = 0;

  if (nData) {
    code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0] + nData);
    if (code) return code;

    memcpy(pCmprsor->aBuf[0] + pCmprsor->nBuf[0], pData, nData);
    pCmprsor->nBuf[0] += nData;
  }
  pCmprsor->b_n++;

  return code;
}

// Bool =====================================================
static const uint8_t BOOL_CMPR_TABLE[] = {0b01, 0b0100, 0b010000, 0b01000000};

static int32_t tCompBool(SCompressor *pCmprsor, bool vBool) {
  int32_t code = 0;

  int32_t mod4 = pCmprsor->bool_n & 3;
  if (vBool) {
    pCmprsor->aBuf[0][pCmprsor->nBuf[0]] |= BOOL_CMPR_TABLE[mod4];
  }
  pCmprsor->bool_n++;
  if (mod4 == 3) {
    pCmprsor->nBuf[0]++;
    pCmprsor->aBuf[0][pCmprsor->nBuf[0]] = 0;

    code = tRealloc(&pCmprsor->aBuf[0], pCmprsor->nBuf[0]);
    if (code) goto _exit;
  }

_exit:
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

  code = tRealloc(&(*ppCmprsor)->aBuf[0], 1024);
  if (code) {
    taosMemoryFree(*ppCmprsor);
    *ppCmprsor = NULL;
    goto _exit;
  }

_exit:
  return code;
}

int32_t tCompressorDestroy(SCompressor *pCmprsor) {
  int32_t code = 0;

  if (pCmprsor) {
    int32_t nBuf = sizeof(pCmprsor->aBuf) / sizeof(pCmprsor->aBuf[0]);
    for (int32_t iBuf = 0; iBuf < nBuf; iBuf++) {
      tFree(pCmprsor->aBuf[iBuf]);
    }

    taosMemoryFree(pCmprsor);
  }

  return code;
}

int32_t tCompressorReset(SCompressor *pCmprsor, int8_t type, int8_t cmprAlg) {
  int32_t code = 0;

  pCmprsor->type = type;
  pCmprsor->cmprAlg = cmprAlg;
  pCmprsor->nBuf[0] = 0;  // (todo) may or may not +/- 1

  switch (type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      pCmprsor->ts_copy = 0;
      pCmprsor->ts_n = 0;
      break;
    case TSDB_DATA_TYPE_BOOL:
      pCmprsor->bool_n = 0;
      pCmprsor->aBuf[0][0] = 0;
      break;
    case TSDB_DATA_TYPE_BINARY:
      pCmprsor->b_n = 0;
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

int32_t tCompress(SCompressor *pCmprsor, void *pData, int64_t nData) {
  int32_t code = 0;
  // TODO
  return code;
}