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
#include "tutil.h"

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
  uint8_t*      data;
  uint32_t      size;
  uint32_t      pos;
  SCoderMem*    mList;
  SDecoderNode* dStack;
} SDecoder;

#define TD_CODER_CURRENT(CODER)         ((CODER)->data + (CODER)->pos)
#define TD_CODER_REMAIN_CAPACITY(CODER) ((CODER)->size - (CODER)->pos)

#define tEncodeSize(E, S, SIZE, RET) \
  do {                               \
    SEncoder coder = {0};            \
    tEncoderInit(&coder, NULL, 0);   \
    if ((E)(&coder, S) >= 0) {       \
      SIZE = coder.pos;              \
      RET = 0;                       \
    } else {                         \
      RET = -1;                      \
    }                                \
    tEncoderClear(&coder);           \
  } while (0);

static void* tEncoderMalloc(SEncoder* pCoder, int32_t size);
static void* tDecoderMalloc(SDecoder* pCoder, int32_t size);

/* ------------------------ ENCODE ------------------------ */
void           tEncoderInit(SEncoder* pCoder, uint8_t* data, uint32_t size);
void           tEncoderClear(SEncoder* pCoder);
int32_t        tStartEncode(SEncoder* pCoder);
void           tEndEncode(SEncoder* pCoder);
static int32_t tEncodeFixed(SEncoder* pCoder, const void* val, uint32_t size);
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
static int32_t tEncodeBinaryEx(SEncoder* pCoder, const uint8_t* val, uint32_t len);
static int32_t tEncodeCStrWithLen(SEncoder* pCoder, const char* val, uint32_t len);
static int32_t tEncodeCStr(SEncoder* pCoder, const char* val);

/* ------------------------ DECODE ------------------------ */
void           tDecoderInit(SDecoder* pCoder, uint8_t* data, uint32_t size);
void           tDecoderClear(SDecoder* SDecoder);
int32_t        tStartDecode(SDecoder* pCoder);
void           tEndDecode(SDecoder* pCoder);
static bool    tDecodeIsEnd(SDecoder* pCoder);
static int32_t tDecodeFixed(SDecoder* pCoder, void* val, uint32_t size);
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
static int32_t tDecodeBinary(SDecoder* pCoder, uint8_t** val, uint32_t* len);
static int32_t tDecodeCStrAndLen(SDecoder* pCoder, char** val, uint32_t* len);
static int32_t tDecodeCStr(SDecoder* pCoder, char** val);
static int32_t tDecodeCStrTo(SDecoder* pCoder, char* val);

/* ------------------------ IMPL ------------------------ */
static FORCE_INLINE int32_t tEncodeFixed(SEncoder* pCoder, const void* val, uint32_t size) {
  if (pCoder->data) {
    if (pCoder->pos + size > pCoder->size) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
    }
    TAOS_MEMCPY(pCoder->data + pCoder->pos, val, size);
  }

  pCoder->pos += size;
  return 0;
}

static FORCE_INLINE int32_t tEncodeU8(SEncoder* pCoder, uint8_t val) { return tEncodeFixed(pCoder, &val, sizeof(val)); }
static FORCE_INLINE int32_t tEncodeI8(SEncoder* pCoder, int8_t val) { return tEncodeFixed(pCoder, &val, sizeof(val)); }
static FORCE_INLINE int32_t tEncodeU16(SEncoder* pCoder, uint16_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeI16(SEncoder* pCoder, int16_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeU32(SEncoder* pCoder, uint32_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeI32(SEncoder* pCoder, int32_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeU64(SEncoder* pCoder, uint64_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeI64(SEncoder* pCoder, int64_t val) {
  return tEncodeFixed(pCoder, &val, sizeof(val));
}
static FORCE_INLINE int32_t tEncodeU16v(SEncoder* pCoder, uint16_t val) {
  while (val >= ENCODE_LIMIT) {
    TAOS_CHECK_RETURN(tEncodeU8(pCoder, (val | ENCODE_LIMIT) & 0xff));
    val >>= 7;
  }
  return tEncodeU8(pCoder, val);
}
static FORCE_INLINE int32_t tEncodeI16v(SEncoder* pCoder, int16_t val) {
  return tEncodeU16v(pCoder, ZIGZAGE(int16_t, val));
}
static FORCE_INLINE int32_t tEncodeU32v(SEncoder* pCoder, uint32_t val) {
  while (val >= ENCODE_LIMIT) {
    TAOS_CHECK_RETURN(tEncodeU8(pCoder, (val | ENCODE_LIMIT) & 0xff));
    val >>= 7;
  }
  return tEncodeU8(pCoder, val);
}
static FORCE_INLINE int32_t tEncodeI32v(SEncoder* pCoder, int32_t val) {
  return tEncodeU32v(pCoder, ZIGZAGE(int32_t, val));
}
static FORCE_INLINE int32_t tEncodeU64v(SEncoder* pCoder, uint64_t val) {
  while (val >= ENCODE_LIMIT) {
    TAOS_CHECK_RETURN(tEncodeU8(pCoder, (val | ENCODE_LIMIT) & 0xff));
    val >>= 7;
  }
  return tEncodeU8(pCoder, val);
}
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
  TAOS_CHECK_RETURN(tEncodeU32v(pCoder, len));
  if (len) {
    if (pCoder->data) {
      if (pCoder->pos + len > pCoder->size) {
        TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
      }
      TAOS_MEMCPY(pCoder->data + pCoder->pos, val, len);
    }

    pCoder->pos += len;
  }
  return 0;
}

static FORCE_INLINE int32_t tEncodeCStrWithLen(SEncoder* pCoder, const char* val, uint32_t len) {
  return tEncodeBinary(pCoder, (uint8_t*)val, len + 1);
}

static FORCE_INLINE int32_t tEncodeCStr(SEncoder* pCoder, const char* val) {
  return tEncodeCStrWithLen(pCoder, val, (uint32_t)strlen(val));
}

/* ------------------------ FOR DECODER ------------------------ */
static int32_t tDecodeFixed(SDecoder* pCoder, void* val, uint32_t size) {
  if (pCoder->pos + size > pCoder->size) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  } else if (val) {
    TAOS_MEMCPY(val, pCoder->data + pCoder->pos, size);
  }
  pCoder->pos += size;
  return 0;
}

static FORCE_INLINE int32_t tDecodeU8(SDecoder* pCoder, uint8_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}

static FORCE_INLINE int32_t tDecodeI8(SDecoder* pCoder, int8_t* val) { return tDecodeFixed(pCoder, val, sizeof(*val)); }

// 16
static FORCE_INLINE int32_t tDecodeU16(SDecoder* pCoder, uint16_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}
static FORCE_INLINE int32_t tDecodeI16(SDecoder* pCoder, int16_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}
// 32
static FORCE_INLINE int32_t tDecodeU32(SDecoder* pCoder, uint32_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}
static FORCE_INLINE int32_t tDecodeI32(SDecoder* pCoder, int32_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}
// 64
static FORCE_INLINE int32_t tDecodeU64(SDecoder* pCoder, uint64_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}
static FORCE_INLINE int32_t tDecodeI64(SDecoder* pCoder, int64_t* val) {
  return tDecodeFixed(pCoder, val, sizeof(*val));
}

// 16v
static FORCE_INLINE int32_t tDecodeU16v(SDecoder* pCoder, uint16_t* val) {
  uint8_t  byte;
  uint16_t tval = 0;
  for (int32_t i = 0;; i++) {
    TAOS_CHECK_RETURN(tDecodeU8(pCoder, &byte));
    if (byte < ENCODE_LIMIT) {
      tval |= (((uint16_t)byte) << (7 * i));
      break;
    } else {
      tval |= ((((uint16_t)byte) & (ENCODE_LIMIT - 1)) << (7 * i));
    }
  }

  if (val) {
    *val = tval;
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeI16v(SDecoder* pCoder, int16_t* val) {
  uint16_t tval;
  TAOS_CHECK_RETURN(tDecodeU16v(pCoder, &tval));

  if (val) {
    *val = ZIGZAGD(int16_t, tval);
  }

  return 0;
}

// 32v
static FORCE_INLINE int32_t tDecodeU32v(SDecoder* pCoder, uint32_t* val) {
  uint8_t  byte;
  uint32_t tval = 0;
  for (int32_t i = 0;; i++) {
    TAOS_CHECK_RETURN(tDecodeU8(pCoder, &byte));
    if (byte < ENCODE_LIMIT) {
      tval |= (((uint32_t)byte) << (7 * i));
      break;
    } else {
      tval |= ((((uint32_t)byte) & (ENCODE_LIMIT - 1)) << (7 * i));
    }
  }

  if (val) {
    *val = tval;
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeI32v(SDecoder* pCoder, int32_t* val) {
  uint32_t tval;
  TAOS_CHECK_RETURN(tDecodeU32v(pCoder, &tval));

  if (val) {
    *val = ZIGZAGD(int32_t, tval);
  }

  return 0;
}

// 64v
static FORCE_INLINE int32_t tDecodeU64v(SDecoder* pCoder, uint64_t* val) {
  uint8_t  byte;
  uint64_t tval = 0;
  for (int32_t i = 0;; i++) {
    TAOS_CHECK_RETURN(tDecodeU8(pCoder, &byte));
    if (byte < ENCODE_LIMIT) {
      tval |= (((uint64_t)byte) << (7 * i));
      break;
    } else {
      tval |= ((((uint64_t)byte) & (ENCODE_LIMIT - 1)) << (7 * i));
    }
  }

  if (val) {
    *val = tval;
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeI64v(SDecoder* pCoder, int64_t* val) {
  uint64_t tval;
  TAOS_CHECK_RETURN(tDecodeU64v(pCoder, &tval));

  if (val) {
    *val = ZIGZAGD(int64_t, tval);
  }

  return 0;
}

static FORCE_INLINE int32_t tDecodeFloat(SDecoder* pCoder, float* val) {
  union {
    uint32_t ui;
    float    f;
  } v;

  TAOS_CHECK_RETURN(tDecodeU32(pCoder, &(v.ui)));

  if (val) {
    *val = v.f;
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeDouble(SDecoder* pCoder, double* val) {
  union {
    uint64_t ui;
    double   d;
  } v;

  TAOS_CHECK_RETURN(tDecodeU64(pCoder, &(v.ui)));

  if (val) {
    *val = v.d;
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeBinary(SDecoder* pCoder, uint8_t** val, uint32_t* len) {
  uint32_t length = 0;

  TAOS_CHECK_RETURN(tDecodeU32v(pCoder, &length));
  if (len) {
    *len = length;
  }

  if (pCoder->pos + length > pCoder->size) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
  }

  if (val) {
    *val = pCoder->data + pCoder->pos;
  }

  pCoder->pos += length;
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStrAndLen(SDecoder* pCoder, char** val, uint32_t* len) {
  TAOS_CHECK_RETURN(tDecodeBinary(pCoder, (uint8_t**)val, len));
  (*len) -= 1;
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStr(SDecoder* pCoder, char** val) {
  uint32_t len;
  return tDecodeCStrAndLen(pCoder, val, &len);
}

static int32_t tDecodeCStrTo(SDecoder* pCoder, char* val) {
  char*    pStr;
  uint32_t len;
  TAOS_CHECK_RETURN(tDecodeCStrAndLen(pCoder, &pStr, &len));

  TAOS_MEMCPY(val, pStr, len + 1);
  return 0;
}

static FORCE_INLINE int32_t tDecodeBinaryAlloc(SDecoder* pCoder, void** val, uint64_t* len) {
  uint64_t length = 0;
  TAOS_CHECK_RETURN(tDecodeU64v(pCoder, &length));
  if (length) {
    if (len) *len = length;

    if (pCoder->pos + length > pCoder->size) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
    }

    *val = taosMemoryMalloc(length);
    if (*val == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }

    TAOS_MEMCPY(*val, pCoder->data + pCoder->pos, length);

    pCoder->pos += length;
  } else {
    *val = NULL;
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeBinaryAlloc32(SDecoder* pCoder, void** val, uint32_t* len) {
  uint32_t length = 0;
  TAOS_CHECK_RETURN(tDecodeU32v(pCoder, &length));
  if (length) {
    if (len) *len = length;

    if (pCoder->pos + length > pCoder->size) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_RANGE);
    }
    *val = taosMemoryMalloc(length);
    if (*val == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    TAOS_MEMCPY(*val, pCoder->data + pCoder->pos, length);

    pCoder->pos += length;
  } else {
    *val = NULL;
  }
  return 0;
}

static FORCE_INLINE int32_t tDecodeCStrAndLenAlloc(SDecoder* pCoder, char** val, uint64_t* len) {
  TAOS_CHECK_RETURN(tDecodeBinaryAlloc(pCoder, (void**)val, len));
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
  SCoderMem* pMem = (SCoderMem*)taosMemoryCalloc(1, sizeof(*pMem) + size);
  if (pMem) {
    pMem->next = pCoder->mList;
    pCoder->mList = pMem;
    p = (void*)&pMem[1];
  }
  return p;
}

static FORCE_INLINE void* tDecoderMalloc(SDecoder* pCoder, int32_t size) {
  void*      p = NULL;
  SCoderMem* pMem = (SCoderMem*)taosMemoryCalloc(1, sizeof(*pMem) + size);
  if (pMem) {
    pMem->next = pCoder->mList;
    pCoder->mList = pMem;
    p = (void*)&pMem[1];
  }
  return p;
}

// ===========================================
#define tPutV(p, v)                    \
  do {                                 \
    int32_t n = 0;                     \
    for (;;) {                         \
      if (v <= 0x7f) {                 \
        if (p) p[n] = v;               \
        n++;                           \
        break;                         \
      }                                \
      if (p) p[n] = (v & 0x7f) | 0x80; \
      n++;                             \
      v >>= 7;                         \
    }                                  \
    return n;                          \
  } while (0)

// PUT
static FORCE_INLINE int32_t tPutU8(uint8_t* p, uint8_t v) {
  if (p) ((uint8_t*)p)[0] = v;
  return sizeof(uint8_t);
}

static FORCE_INLINE int32_t tPutI8(uint8_t* p, int8_t v) {
  if (p) ((int8_t*)p)[0] = v;
  return sizeof(int8_t);
}

static FORCE_INLINE int32_t tPutU16(uint8_t* p, uint16_t v) {
  if (p) ((uint16_t*)p)[0] = v;
  return sizeof(uint16_t);
}

static FORCE_INLINE int32_t tPutI16(uint8_t* p, int16_t v) {
  if (p) ((int16_t*)p)[0] = v;
  return sizeof(int16_t);
}

static FORCE_INLINE int32_t tPutU32(uint8_t* p, uint32_t v) {
  if (p) ((uint32_t*)p)[0] = v;
  return sizeof(uint32_t);
}

static FORCE_INLINE int32_t tPutI32(uint8_t* p, int32_t v) {
  if (p) ((int32_t*)p)[0] = v;
  return sizeof(int32_t);
}

static FORCE_INLINE int32_t tPutU64(uint8_t* p, uint64_t v) {
  if (p) ((uint64_t*)p)[0] = v;
  return sizeof(uint64_t);
}

static FORCE_INLINE int32_t tPutI64(uint8_t* p, int64_t v) {
  if (p) ((int64_t*)p)[0] = v;
  return sizeof(int64_t);
}

static FORCE_INLINE int32_t tPutFloat(uint8_t* p, float f) {
  union {
    uint32_t ui;
    float    f;
  } v;
  v.f = f;

  return tPutU32(p, v.ui);
}

static FORCE_INLINE int32_t tPutDouble(uint8_t* p, double d) {
  union {
    uint64_t ui;
    double   d;
  } v;
  v.d = d;

  return tPutU64(p, v.ui);
}

static FORCE_INLINE int32_t tPutU16v(uint8_t* p, uint16_t v) { tPutV(p, v); }

static FORCE_INLINE int32_t tPutI16v(uint8_t* p, int16_t v) { return tPutU16v(p, ZIGZAGE(int16_t, v)); }

static FORCE_INLINE int32_t tPutU32v(uint8_t* p, uint32_t v) { tPutV(p, v); }

static FORCE_INLINE int32_t tPutI32v(uint8_t* p, int32_t v) { return tPutU32v(p, ZIGZAGE(int32_t, v)); }

static FORCE_INLINE int32_t tPutU64v(uint8_t* p, uint64_t v) { tPutV(p, v); }

static FORCE_INLINE int32_t tPutI64v(uint8_t* p, int64_t v) { return tPutU64v(p, ZIGZAGE(int64_t, v)); }

// GET
static FORCE_INLINE int32_t tGetU8(uint8_t* p, uint8_t* v) {
  if (v) *v = ((uint8_t*)p)[0];
  return sizeof(uint8_t);
}

static FORCE_INLINE int32_t tGetI8(uint8_t* p, int8_t* v) {
  if (v) *v = ((int8_t*)p)[0];
  return sizeof(int8_t);
}

static FORCE_INLINE int32_t tGetU16(uint8_t* p, uint16_t* v) {
  if (v) *v = ((uint16_t*)p)[0];
  return sizeof(uint16_t);
}

static FORCE_INLINE int32_t tGetI16(uint8_t* p, int16_t* v) {
  if (v) *v = ((int16_t*)p)[0];
  return sizeof(int16_t);
}

static FORCE_INLINE int32_t tGetU32(uint8_t* p, uint32_t* v) {
  if (v) *v = ((uint32_t*)p)[0];
  return sizeof(uint32_t);
}

static FORCE_INLINE int32_t tGetI32(uint8_t* p, int32_t* v) {
  if (v) *v = ((int32_t*)p)[0];
  return sizeof(int32_t);
}

static FORCE_INLINE int32_t tGetU64(uint8_t* p, uint64_t* v) {
  if (v) *v = ((uint64_t*)p)[0];
  return sizeof(uint64_t);
}

static FORCE_INLINE int32_t tGetI64(uint8_t* p, int64_t* v) {
  if (v) *v = ((int64_t*)p)[0];
  return sizeof(int64_t);
}

static FORCE_INLINE int32_t tGetU16v(uint8_t* p, uint16_t* v) {
  int32_t n = 0;

  if (v) *v = 0;
  for (;;) {
    if (p[n] <= 0x7f) {
      if (v) (*v) |= (((uint16_t)p[n]) << (7 * n));
      n++;
      break;
    }
    if (v) (*v) |= (((uint16_t)(p[n] & 0x7f)) << (7 * n));
    n++;
  }

  return n;
}

static FORCE_INLINE int32_t tGetI16v(uint8_t* p, int16_t* v) {
  int32_t  n;
  uint16_t tv;

  n = tGetU16v(p, &tv);
  if (v) *v = ZIGZAGD(int16_t, tv);

  return n;
}

static FORCE_INLINE int32_t tGetU32v(uint8_t* p, uint32_t* v) {
  int32_t n = 0;

  if (v) *v = 0;
  for (;;) {
    if (p[n] <= 0x7f) {
      if (v) (*v) |= (((uint32_t)p[n]) << (7 * n));
      n++;
      break;
    }
    if (v) (*v) |= (((uint32_t)(p[n] & 0x7f)) << (7 * n));
    n++;
  }

  return n;
}

static FORCE_INLINE int32_t tGetI32v(uint8_t* p, int32_t* v) {
  int32_t  n;
  uint32_t tv;

  n = tGetU32v(p, &tv);
  if (v) *v = ZIGZAGD(int32_t, tv);

  return n;
}

static FORCE_INLINE int32_t tGetU64v(uint8_t* p, uint64_t* v) {
  int32_t n = 0;

  if (v) *v = 0;
  for (;;) {
    if (p[n] <= 0x7f) {
      if (v) (*v) |= (((uint64_t)p[n]) << (7 * n));
      n++;
      break;
    }
    if (v) (*v) |= (((uint64_t)(p[n] & 0x7f)) << (7 * n));
    n++;
  }

  return n;
}

static FORCE_INLINE int32_t tGetI64v(uint8_t* p, int64_t* v) {
  int32_t  n;
  uint64_t tv;

  n = tGetU64v(p, &tv);
  if (v) *v = ZIGZAGD(int64_t, tv);

  return n;
}

static FORCE_INLINE int32_t tGetFloat(uint8_t* p, float* f) {
  int32_t n = 0;

  union {
    uint32_t ui;
    float    f;
  } v;

  n = tGetU32(p, &v.ui);

  *f = v.f;
  return n;
}

static FORCE_INLINE int32_t tGetDouble(uint8_t* p, double* d) {
  int32_t n = 0;

  union {
    uint64_t ui;
    double   d;
  } v;

  n = tGetU64(p, &v.ui);

  *d = v.d;
  return n;
}

// =====================
static FORCE_INLINE int32_t tPutBinary(uint8_t* p, uint8_t* pData, uint32_t nData) {
  int n = 0;

  n += tPutU32v(p ? p + n : p, nData);
  if (p) TAOS_MEMCPY(p + n, pData, nData);
  n += nData;

  return n;
}

static FORCE_INLINE int32_t tGetBinary(uint8_t* p, uint8_t** ppData, uint32_t* nData) {
  int32_t  n = 0;
  uint32_t nt;

  n += tGetU32v(p, &nt);
  if (nData) *nData = nt;
  if (ppData) *ppData = p + n;
  n += nt;

  return n;
}

static FORCE_INLINE int32_t tPutCStr(uint8_t* p, char* pData) {
  return tPutBinary(p, (uint8_t*)pData, strlen(pData) + 1);
}
static FORCE_INLINE int32_t tGetCStr(uint8_t* p, char** ppData) { return tGetBinary(p, (uint8_t**)ppData, NULL); }

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_ENCODE_H_*/
