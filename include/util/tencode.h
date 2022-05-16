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

#ifndef _TD_UTIL_ENCODE_H_
#define _TD_UTIL_ENCODE_H_

#include "tcoding.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SEncoderNode SEncoderNode;
typedef struct SDecoderNode SDecoderNode;

typedef struct SCoderMem {
  struct SCoderMem* next;
} SCoderMem;

typedef struct {
  uint8_t*      data;
  uint32_t      size;
  uint32_t      pos;
  SCoderMem*    mList;
  SEncoderNode* eStack;
} SEncoder;

typedef struct {
  const uint8_t* data;
  uint32_t       size;
  uint32_t       pos;
  SCoderMem*     mList;
  SDecoderNode*  dStack;
} SDecoder;

#define tPut(TYPE, BUF, VAL) ((TYPE*)(BUF))[0] = (VAL)
#define tGet(TYPE, BUF, VAL) (VAL) = ((TYPE*)(BUF))[0]

#define tRPut16(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[0];

#define tRPut32(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[3]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[2]; \
  ((uint8_t*)(PDEST))[2] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[3] = ((uint8_t*)(PSRC))[0];

#define tRPut64(PDEST, PSRC)                      \
  ((uint8_t*)(PDEST))[0] = ((uint8_t*)(PSRC))[7]; \
  ((uint8_t*)(PDEST))[1] = ((uint8_t*)(PSRC))[6]; \
  ((uint8_t*)(PDEST))[2] = ((uint8_t*)(PSRC))[5]; \
  ((uint8_t*)(PDEST))[3] = ((uint8_t*)(PSRC))[4]; \
  ((uint8_t*)(PDEST))[4] = ((uint8_t*)(PSRC))[3]; \
  ((uint8_t*)(PDEST))[5] = ((uint8_t*)(PSRC))[2]; \
  ((uint8_t*)(PDEST))[6] = ((uint8_t*)(PSRC))[1]; \
  ((uint8_t*)(PDEST))[7] = ((uint8_t*)(PSRC))[0];

#define tRGet16 tRPut16
#define tRGet32 tRPut32
#define tRGet64 tRPut64

#define TD_CODER_POS(CODER)                            ((CODER)->pos)
#define TD_CODER_CURRENT(CODER)                        ((CODER)->data + (CODER)->pos)
#define TD_CODER_MOVE_POS(CODER, MOVE)                 ((CODER)->pos += (MOVE))
#define TD_CODER_CHECK_CAPACITY_FAILED(CODER, EXPSIZE) (((CODER)->size - (CODER)->pos) < (EXPSIZE))

#define tEncodeSize(E, S, SIZE, RET) \
  do {                               \
    SEncoder coder = {0};            \
    tEncoderInit(&coder, NULL, 0);   \
    if ((E)(&coder, S) == 0) {       \
      SIZE = coder.pos;              \
      RET = 0;                       \
    } else {                         \
      RET = -1;                      \
    }                                \
    tEncoderClear(&coder);           \
  } while (0)

static void* tEncoderMalloc(SEncoder* pCoder, int32_t size);
static void* tDecoderMalloc(SDecoder* pCoder, int32_t size);

/* ------------------------ ENCODE ------------------------ */
void           tEncoderInit(SEncoder* pCoder, uint8_t* data, uint32_t size);
void           tEncoderClear(SEncoder* pCoder);
int32_t        tStartEncode(SEncoder* pCoder);
void           tEndEncode(SEncoder* pCoder);
static int32_t tEncodeU8(SEncoder* pCoder, uint8_t val);
static int32_t tEncodeI8(SEncoder* pCoder, int8_t val);
static int32_t tEncodeU16(SEncoder* pCoder, uint16_t val);
static int32_t tEncodeI16(SEncoder* pCoder, int16_t val);
static int32_t tEncodeU32(SEncoder* pCoder, uint32_t val);
static int32_t tEncodeI32(SEncoder* pCoder, int32_t val);
static int32_t tEncodeU64(SEncoder* pCoder, uint64_t val);
static int32_t tEncodeI64(SEncoder* pCoder, int64_t val);
static int32_t tEncodeU16v(SEncoder* pCoder, uint16_t val);
static int32_t tEncodeI16v(SEncoder* pCoder, int16_t val);
static int32_t tEncodeU32v(SEncoder* pCoder, uint32_t val);
static int32_t tEncodeI32v(SEncoder* pCoder, int32_t val);
static int32_t tEncodeU64v(SEncoder* pCoder, uint64_t val);
static int32_t tEncodeI64v(SEncoder* pCoder, int64_t val);
static int32_t tEncodeFloat(SEncoder* pCoder, float val);
static int32_t tEncodeDouble(SEncoder* pCoder, double val);
static int32_t tEncodeBinary(SEncoder* pCoder, const uint8_t* val, uint32_t len);
static int32_t tEncodeCStrWithLen(SEncoder* pCoder, const char* val, uint32_t len);
static int32_t tEncodeCStr(SEncoder* pCoder, const char* val);

/* ------------------------ DECODE ------------------------ */
void           tDecoderInit(SDecoder* pCoder, const uint8_t* data, uint32_t size);
void           tDecoderClear(SDecoder* SDecoder);
int32_t        tStartDecode(SDecoder* pCoder);
void           tEndDecode(SDecoder* pCoder);
static bool    tDecodeIsEnd(SDecoder* pCoder);
static int32_t tDecodeU8(SDecoder* pCoder, uint8_t* val);
static int32_t tDecodeI8(SDecoder* pCoder, int8_t* val);
static int32_t tDecodeU16(SDecoder* pCoder, uint16_t* val);
static int32_t tDecodeI16(SDecoder* pCoder, int16_t* val);
static int32_t tDecodeU32(SDecoder* pCoder, uint32_t* val);
static int32_t tDecodeI32(SDecoder* pCoder, int32_t* val);
static int32_t tDecodeU64(SDecoder* pCoder, uint64_t* val);
static int32_t tDecodeI64(SDecoder* pCoder, int64_t* val);
static int32_t tDecodeU16v(SDecoder* pCoder, uint16_t* val);
static int32_t tDecodeI16v(SDecoder* pCoder, int16_t* val);
static int32_t tDecodeU32v(SDecoder* pCoder, uint32_t* val);
static int32_t tDecodeI32v(SDecoder* pCoder, int32_t* val);
static int32_t tDecodeU64v(SDecoder* pCoder, uint64_t* val);
static int32_t tDecodeI64v(SDecoder* pCoder, int64_t* val);
static int32_t tDecodeFloat(SDecoder* pCoder, float* val);
static int32_t tDecodeDouble(SDecoder* pCoder, double* val);
static int32_t tDecodeBinary(SDecoder* pCoder, const uint8_t** val, uint32_t* len);
static int32_t tDecodeCStrAndLen(SDecoder* pCoder, const char** val, uint32_t* len);
static int32_t tDecodeCStr(SDecoder* pCoder, const char** val);
static int32_t tDecodeCStrTo(SDecoder* pCoder, char* val);

/* ------------------------ IMPL ------------------------ */
#define TD_ENCODE_MACRO(CODER, VAL, TYPE, BITS)                        \
  if ((CODER)->data) {                                                 \
    if (TD_CODER_CHECK_CAPACITY_FAILED(CODER, sizeof(VAL))) return -1; \
    tPut(TYPE, TD_CODER_CURRENT(CODER), (VAL));                        \
  }                                                                    \
  TD_CODER_MOVE_POS(CODER, sizeof(VAL));                               \
  return 0;

#define TD_ENCODE_VARIANT_MACRO(CODER, VAL)                       \
  while ((VAL) >= ENCODE_LIMIT) {                                 \
    if ((CODER)->data) {                                          \
      if (TD_CODER_CHECK_CAPACITY_FAILED(CODER, 1)) return -1;    \
      TD_CODER_CURRENT(CODER)[0] = ((VAL) | ENCODE_LIMIT) & 0xff; \
    }                                                             \
                                                                  \
    (VAL) >>= 7;                                                  \
    TD_CODER_MOVE_POS(CODER, 1);                                  \
  }                                                               \
                                                                  \
  if ((CODER)->data) {                                            \
    if (TD_CODER_CHECK_CAPACITY_FAILED(CODER, 1)) return -1;      \
    TD_CODER_CURRENT(CODER)[0] = (uint8_t)(VAL);                  \
  }                                                               \
  TD_CODER_MOVE_POS(CODER, 1);                                    \
  return 0;

#define TD_DECODE_MACRO(CODER, PVAL, TYPE, BITS)                         \
  if (TD_CODER_CHECK_CAPACITY_FAILED(CODER, sizeof(*(PVAL)))) return -1; \
  tGet(TYPE, TD_CODER_CURRENT(CODER), *(PVAL));                          \
  TD_CODER_MOVE_POS(CODER, sizeof(*(PVAL)));                             \
  return 0;

#define TD_DECODE_VARIANT_MACRO(CODER, PVAL, TYPE)           \
  int32_t i = 0;                                             \
  *(PVAL) = 0;                                               \
  for (;;) {                                                 \
    if (TD_CODER_CHECK_CAPACITY_FAILED(CODER, 1)) return -1; \
    TYPE tval = TD_CODER_CURRENT(CODER)[0];                  \
    if (tval < ENCODE_LIMIT) {                               \
      *(PVAL) |= (tval << (7 * i));                          \
      TD_CODER_MOVE_POS(pCoder, 1);                          \
      break;                                                 \
    } else {                                                 \
      *(PVAL) |= (((tval) & (ENCODE_LIMIT - 1)) << (7 * i)); \
      i++;                                                   \
      TD_CODER_MOVE_POS(pCoder, 1);                          \
    }                                                        \
  }                                                          \
                                                             \
  return 0;

// 8
static FORCE_INLINE int32_t tEncodeU8(SEncoder* pCoder, uint8_t val) {
  if (pCoder->data) {
    if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, sizeof(val))) return -1;
    tPut(uint8_t, TD_CODER_CURRENT(pCoder), val);
  }
  TD_CODER_MOVE_POS(pCoder, sizeof(val));
  return 0;
}

static FORCE_INLINE int32_t tEncodeI8(SEncoder* pCoder, int8_t val) {
  if (pCoder->data) {
    if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, sizeof(val))) return -1;
    tPut(int8_t, TD_CODER_CURRENT(pCoder), val);
  }
  TD_CODER_MOVE_POS(pCoder, sizeof(val));
  return 0;
}

// 16
static FORCE_INLINE int32_t tEncodeU16(SEncoder* pCoder, uint16_t val) { TD_ENCODE_MACRO(pCoder, val, uint16_t, 16); }
static FORCE_INLINE int32_t tEncodeI16(SEncoder* pCoder, int16_t val) { TD_ENCODE_MACRO(pCoder, val, int16_t, 16); }
// 32
static FORCE_INLINE int32_t tEncodeU32(SEncoder* pCoder, uint32_t val) { TD_ENCODE_MACRO(pCoder, val, uint32_t, 32); }
static FORCE_INLINE int32_t tEncodeI32(SEncoder* pCoder, int32_t val) { TD_ENCODE_MACRO(pCoder, val, int32_t, 32); }
// 64
static FORCE_INLINE int32_t tEncodeU64(SEncoder* pCoder, uint64_t val) { TD_ENCODE_MACRO(pCoder, val, uint64_t, 64); }
static FORCE_INLINE int32_t tEncodeI64(SEncoder* pCoder, int64_t val) { TD_ENCODE_MACRO(pCoder, val, int64_t, 64); }
// 16v
static FORCE_INLINE int32_t tEncodeU16v(SEncoder* pCoder, uint16_t val) { TD_ENCODE_VARIANT_MACRO(pCoder, val); }
static FORCE_INLINE int32_t tEncodeI16v(SEncoder* pCoder, int16_t val) {
  return tEncodeU16v(pCoder, ZIGZAGE(int16_t, val));
}
// 32v
static FORCE_INLINE int32_t tEncodeU32v(SEncoder* pCoder, uint32_t val) { TD_ENCODE_VARIANT_MACRO(pCoder, val); }
static FORCE_INLINE int32_t tEncodeI32v(SEncoder* pCoder, int32_t val) {
  return tEncodeU32v(pCoder, ZIGZAGE(int32_t, val));
}
// 64v
static FORCE_INLINE int32_t tEncodeU64v(SEncoder* pCoder, uint64_t val) { TD_ENCODE_VARIANT_MACRO(pCoder, val); }
static FORCE_INLINE int32_t tEncodeI64v(SEncoder* pCoder, int64_t val) {
  return tEncodeU64v(pCoder, ZIGZAGE(int64_t, val));
}

static FORCE_INLINE int32_t tEncodeFloat(SEncoder* pCoder, float val) {
  union {
    uint32_t ui;
    float    f;
  } v;
  v.f = val;

  return tEncodeU32(pCoder, v.ui);
}

static FORCE_INLINE int32_t tEncodeDouble(SEncoder* pCoder, double val) {
  union {
    uint64_t ui;
    double   d;
  } v;
  v.d = val;

  return tEncodeU64(pCoder, v.ui);
}

static FORCE_INLINE int32_t tEncodeBinary(SEncoder* pCoder, const uint8_t* val, uint32_t len) {
  if (tEncodeU32v(pCoder, len) < 0) return -1;
  if (pCoder->data) {
    if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, len)) return -1;
    memcpy(TD_CODER_CURRENT(pCoder), val, len);
  }

  TD_CODER_MOVE_POS(pCoder, len);
  return 0;
}

static FORCE_INLINE int32_t tEncodeCStrWithLen(SEncoder* pCoder, const char* val, uint32_t len) {
  return tEncodeBinary(pCoder, (uint8_t*)val, len + 1);
}

static FORCE_INLINE int32_t tEncodeCStr(SEncoder* pCoder, const char* val) {
  return tEncodeCStrWithLen(pCoder, val, (uint32_t)strlen(val));
}

/* ------------------------ FOR DECODER ------------------------ */
// 8
static FORCE_INLINE int32_t tDecodeU8(SDecoder* pCoder, uint8_t* val) {
  if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, sizeof(*val))) return -1;
  tGet(uint8_t, TD_CODER_CURRENT(pCoder), *val);
  TD_CODER_MOVE_POS(pCoder, sizeof(*val));
  return 0;
}

static FORCE_INLINE int32_t tDecodeI8(SDecoder* pCoder, int8_t* val) {
  if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, sizeof(*val))) return -1;
  tGet(int8_t, TD_CODER_CURRENT(pCoder), *val);
  TD_CODER_MOVE_POS(pCoder, sizeof(*val));
  return 0;
}

// 16
static FORCE_INLINE int32_t tDecodeU16(SDecoder* pCoder, uint16_t* val) { TD_DECODE_MACRO(pCoder, val, uint16_t, 16); }
static FORCE_INLINE int32_t tDecodeI16(SDecoder* pCoder, int16_t* val) { TD_DECODE_MACRO(pCoder, val, int16_t, 16); }
// 32
static FORCE_INLINE int32_t tDecodeU32(SDecoder* pCoder, uint32_t* val) { TD_DECODE_MACRO(pCoder, val, uint32_t, 32); }
static FORCE_INLINE int32_t tDecodeI32(SDecoder* pCoder, int32_t* val) { TD_DECODE_MACRO(pCoder, val, int32_t, 32); }
// 64
static FORCE_INLINE int32_t tDecodeU64(SDecoder* pCoder, uint64_t* val) { TD_DECODE_MACRO(pCoder, val, uint64_t, 64); }
static FORCE_INLINE int32_t tDecodeI64(SDecoder* pCoder, int64_t* val) { TD_DECODE_MACRO(pCoder, val, int64_t, 64); }

// 16v
static FORCE_INLINE int32_t tDecodeU16v(SDecoder* pCoder, uint16_t* val) {
  TD_DECODE_VARIANT_MACRO(pCoder, val, uint16_t);
}

static FORCE_INLINE int32_t tDecodeI16v(SDecoder* pCoder, int16_t* val) {
  uint16_t tval;
  if (tDecodeU16v(pCoder, &tval) < 0) {
    return -1;
  }
  *val = ZIGZAGD(int16_t, tval);
  return 0;
}

// 32v
static FORCE_INLINE int32_t tDecodeU32v(SDecoder* pCoder, uint32_t* val) {
  TD_DECODE_VARIANT_MACRO(pCoder, val, uint32_t);
}

static FORCE_INLINE int32_t tDecodeI32v(SDecoder* pCoder, int32_t* val) {
  uint32_t tval;
  if (tDecodeU32v(pCoder, &tval) < 0) {
    return -1;
  }
  *val = ZIGZAGD(int32_t, tval);
  return 0;
}

// 64v
static FORCE_INLINE int32_t tDecodeU64v(SDecoder* pCoder, uint64_t* val) {
  TD_DECODE_VARIANT_MACRO(pCoder, val, uint64_t);
}

static FORCE_INLINE int32_t tDecodeI64v(SDecoder* pCoder, int64_t* val) {
  uint64_t tval;
  if (tDecodeU64v(pCoder, &tval) < 0) {
    return -1;
  }
  *val = ZIGZAGD(int64_t, tval);
  return 0;
}

static FORCE_INLINE int32_t tDecodeFloat(SDecoder* pCoder, float* val) {
  union {
    uint32_t ui;
    float    f;
  } v;

  if (tDecodeU32(pCoder, &(v.ui)) < 0) {
    return -1;
  }

  *val = v.f;
  return 0;
}

static FORCE_INLINE int32_t tDecodeDouble(SDecoder* pCoder, double* val) {
  union {
    uint64_t ui;
    double   d;
  } v;

  if (tDecodeU64(pCoder, &(v.ui)) < 0) {
    return -1;
  }

  *val = v.d;
  return 0;
}

static FORCE_INLINE int32_t tDecodeBinary(SDecoder* pCoder, const uint8_t** val, uint32_t* len) {
  if (tDecodeU32v(pCoder, len) < 0) return -1;

  if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, *len)) return -1;
  if (val) {
    *val = (uint8_t*)TD_CODER_CURRENT(pCoder);
  }

  TD_CODER_MOVE_POS(pCoder, *len);
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStrAndLen(SDecoder* pCoder, const char** val, uint32_t* len) {
  if (tDecodeBinary(pCoder, (const uint8_t**)val, len) < 0) return -1;
  (*len) -= 1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStr(SDecoder* pCoder, const char** val) {
  uint32_t len;
  return tDecodeCStrAndLen(pCoder, val, &len);
}

static int32_t tDecodeCStrTo(SDecoder* pCoder, char* val) {
  const char* pStr;
  uint32_t    len;
  if (tDecodeCStrAndLen(pCoder, &pStr, &len) < 0) return -1;

  memcpy(val, pStr, len + 1);
  return 0;
}

static FORCE_INLINE int32_t tDecodeBinaryAlloc(SDecoder* pCoder, void** val, uint64_t* len) {
  if (tDecodeU64v(pCoder, len) < 0) return -1;

  if (TD_CODER_CHECK_CAPACITY_FAILED(pCoder, *len)) return -1;
  *val = taosMemoryMalloc(*len);
  if (*val == NULL) return -1;
  memcpy(*val, TD_CODER_CURRENT(pCoder), *len);

  TD_CODER_MOVE_POS(pCoder, *len);
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStrAndLenAlloc(SDecoder* pCoder, char** val, uint64_t* len) {
  if (tDecodeBinaryAlloc(pCoder, (void**)val, len) < 0) return -1;
  (*len) -= 1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStrAlloc(SDecoder* pCoder, char** val) {
  uint64_t len;
  return tDecodeCStrAndLenAlloc(pCoder, val, &len);
}

static FORCE_INLINE bool tDecodeIsEnd(SDecoder* pCoder) { return (pCoder->size == pCoder->pos); }

static FORCE_INLINE void* tEncoderMalloc(SEncoder* pCoder, int32_t size) {
  void*      p = NULL;
  SCoderMem* pMem = (SCoderMem*)taosMemoryMalloc(sizeof(*pMem) + size);
  if (pMem) {
    pMem->next = pCoder->mList;
    pCoder->mList = pMem;
    p = (void*)&pMem[1];
  }
  return p;
}

static FORCE_INLINE void* tDecoderMalloc(SDecoder* pCoder, int32_t size) {
  void*      p = NULL;
  SCoderMem* pMem = (SCoderMem*)taosMemoryMalloc(sizeof(*pMem) + size);
  if (pMem) {
    pMem->next = pCoder->mList;
    pCoder->mList = pMem;
    p = (void*)&pMem[1];
  }
  return p;
}

static FORCE_INLINE int32_t tPutBinary(uint8_t* p, const uint8_t* pData, uint32_t nData) {
  int      n = 0;
  uint32_t v = nData;

  for (;;) {
    if (v <= 0x7f) {
      if (p) p[n] = v;
      n++;
      break;
    }

    if (p) p[n] = (v & 0x7f) | 0x80;
    n++;
    v >>= 7;
  }

  if (p) {
    memcpy(p + n, pData, nData);
  }
  n += nData;

  return n;
}

static FORCE_INLINE int32_t tGetBinary(const uint8_t* p, const uint8_t** ppData, uint32_t* nData) {
  int32_t  n = 0;
  uint32_t tv = 0;
  uint32_t t;

  for (;;) {
    if (p[n] <= 0x7f) {
      t = p[n];
      tv |= (t << (7 * n));
      n++;
      break;
    }

    t = p[n] & 0x7f;
    tv |= (t << (7 * n));
    n++;
  }

  if (nData) *nData = n;
  if (ppData) *ppData = p + n;

  n += tv;
  return n;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_ENCODE_H_*/
