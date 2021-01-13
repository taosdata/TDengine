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
#ifndef _TD_CODING_H_
#define _TD_CODING_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <string.h>

#include "tutil.h"

// TODO: move this to a platform file
#define ENCODE_LIMIT (((uint8_t)1) << 7)
static const int32_t TNUMBER = 1;
#define IS_LITTLE_ENDIAN() (*(uint8_t *)(&TNUMBER) != 0)

#define ZIGZAGE(T, v) ((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1)  // zigzag encode
#define ZIGZAGD(T, v) ((v) >> 1) ^ -((T)((v)&1))                                 // zigzag decode

// ---- Fixed U8
static FORCE_INLINE int taosEncodeFixedU8(void **buf, uint8_t value) {
  if (buf != NULL) {
    ((uint8_t *)(*buf))[0] = value;
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU8(void *buf, uint8_t *value) {
  *value = ((uint8_t *)buf)[0];
  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I8
static FORCE_INLINE int taosEncodeFixedI8(void **buf, int8_t value) {
  if (buf != NULL) {
    ((int8_t *)(*buf))[0] = value;
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }
  return (int)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedI8(void *buf, int8_t *value) {
  *value = ((int8_t *)buf)[0];
  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed U16
static FORCE_INLINE int taosEncodeFixedU16(void **buf, uint16_t value) {
  if (buf != NULL) {
    if (IS_LITTLE_ENDIAN()) {
      memcpy(*buf, &value, sizeof(value));
    } else {
      ((uint8_t *)(*buf))[0] = value & 0xff;
      ((uint8_t *)(*buf))[1] = (value >> 8) & 0xff;
    }
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU16(void *buf, uint16_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((uint8_t *)value)[1] = ((uint8_t *)buf)[0];
    ((uint8_t *)value)[0] = ((uint8_t *)buf)[1];
  }

  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I16
static FORCE_INLINE int taosEncodeFixedI16(void **buf, int16_t value) {
  return taosEncodeFixedU16(buf, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI16(void *buf, int16_t *value) {
  uint16_t tvalue = 0;
  void *   ret = taosDecodeFixedU16(buf, &tvalue);
  *value = ZIGZAGD(int16_t, tvalue);
  return ret;
}

// ---- Fixed U32
static FORCE_INLINE int taosEncodeFixedU32(void **buf, uint32_t value) {
  if (buf != NULL) {
    if (IS_LITTLE_ENDIAN()) {
      memcpy(*buf, &value, sizeof(value));
    } else {
      ((uint8_t *)(*buf))[0] = value & 0xff;
      ((uint8_t *)(*buf))[1] = (value >> 8) & 0xff;
      ((uint8_t *)(*buf))[2] = (value >> 16) & 0xff;
      ((uint8_t *)(*buf))[3] = (value >> 24) & 0xff;
    }
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU32(void *buf, uint32_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((uint8_t *)value)[3] = ((uint8_t *)buf)[0];
    ((uint8_t *)value)[2] = ((uint8_t *)buf)[1];
    ((uint8_t *)value)[1] = ((uint8_t *)buf)[2];
    ((uint8_t *)value)[0] = ((uint8_t *)buf)[3];
  }

  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I32
static FORCE_INLINE int taosEncodeFixedI32(void **buf, int32_t value) {
  return taosEncodeFixedU32(buf, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI32(void *buf, int32_t *value) {
  uint32_t tvalue = 0;
  void *   ret = taosDecodeFixedU32(buf, &tvalue);
  *value = ZIGZAGD(int32_t, tvalue);
  return ret;
}

// ---- Fixed U64
static FORCE_INLINE int taosEncodeFixedU64(void **buf, uint64_t value) {
  if (buf != NULL) {
    if (IS_LITTLE_ENDIAN()) {
      memcpy(*buf, &value, sizeof(value));
    } else {
      ((uint8_t *)(*buf))[0] = value & 0xff;
      ((uint8_t *)(*buf))[1] = (value >> 8) & 0xff;
      ((uint8_t *)(*buf))[2] = (value >> 16) & 0xff;
      ((uint8_t *)(*buf))[3] = (value >> 24) & 0xff;
      ((uint8_t *)(*buf))[4] = (value >> 32) & 0xff;
      ((uint8_t *)(*buf))[5] = (value >> 40) & 0xff;
      ((uint8_t *)(*buf))[6] = (value >> 48) & 0xff;
      ((uint8_t *)(*buf))[7] = (value >> 56) & 0xff;
    }

    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU64(void *buf, uint64_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((uint8_t *)value)[7] = ((uint8_t *)buf)[0];
    ((uint8_t *)value)[6] = ((uint8_t *)buf)[1];
    ((uint8_t *)value)[5] = ((uint8_t *)buf)[2];
    ((uint8_t *)value)[4] = ((uint8_t *)buf)[3];
    ((uint8_t *)value)[3] = ((uint8_t *)buf)[4];
    ((uint8_t *)value)[2] = ((uint8_t *)buf)[5];
    ((uint8_t *)value)[1] = ((uint8_t *)buf)[6];
    ((uint8_t *)value)[0] = ((uint8_t *)buf)[7];
  }

  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I64
static FORCE_INLINE int taosEncodeFixedI64(void **buf, int64_t value) {
  return taosEncodeFixedU64(buf, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI64(void *buf, int64_t *value) {
  uint64_t tvalue = 0;
  void *   ret = taosDecodeFixedU64(buf, &tvalue);
  *value = ZIGZAGD(int64_t, tvalue);
  return ret;
}

// ---- Variant U16
static FORCE_INLINE int taosEncodeVariantU16(void **buf, uint16_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    if (buf != NULL) ((uint8_t *)(*buf))[i] = (uint8_t)(value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 3);
  }

  if (buf != NULL) {
    ((uint8_t *)(*buf))[i] = (uint8_t)value;
    *buf = POINTER_SHIFT(*buf, i + 1);
  }

  return i + 1;
}

static FORCE_INLINE void *taosDecodeVariantU16(void *buf, uint16_t *value) {
  int      i = 0;
  uint16_t tval = 0;
  *value = 0;
  while (i < 3) {
    tval = (uint16_t)(((uint8_t *)buf)[i]);
    if (tval < ENCODE_LIMIT) {
      (*value) |= (tval << (7 * i));
      return POINTER_SHIFT(buf, i + 1);
    } else {
      (*value) |= ((tval & (ENCODE_LIMIT - 1)) << (7 * i));
      i++;
    }
  }

  return NULL;  // error happened
}

// ---- Variant I16
static FORCE_INLINE int taosEncodeVariantI16(void **buf, int16_t value) {
  return taosEncodeVariantU16(buf, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI16(void *buf, int16_t *value) {
  uint16_t tvalue = 0;
  void *   ret = taosDecodeVariantU16(buf, &tvalue);
  *value = ZIGZAGD(int16_t, tvalue);
  return ret;
}

// ---- Variant U32
static FORCE_INLINE int taosEncodeVariantU32(void **buf, uint32_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    if (buf != NULL) ((uint8_t *)(*buf))[i] = (value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 5);
  }

  if (buf != NULL) {
    ((uint8_t *)(*buf))[i] = value;
    *buf = POINTER_SHIFT(*buf, i + 1);
  }

  return i + 1;
}

static FORCE_INLINE void *taosDecodeVariantU32(void *buf, uint32_t *value) {
  int      i = 0;
  uint32_t tval = 0;
  *value = 0;
  while (i < 5) {
    tval = (uint32_t)(((uint8_t *)buf)[i]);
    if (tval < ENCODE_LIMIT) {
      (*value) |= (tval << (7 * i));
      return POINTER_SHIFT(buf, i + 1);
    } else {
      (*value) |= ((tval & (ENCODE_LIMIT - 1)) << (7 * i));
      i++;
    }
  }

  return NULL;  // error happened
}

// ---- Variant I32
static FORCE_INLINE int taosEncodeVariantI32(void **buf, int32_t value) {
  return taosEncodeVariantU32(buf, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI32(void *buf, int32_t *value) {
  uint32_t tvalue = 0;
  void *   ret = taosDecodeVariantU32(buf, &tvalue);
  *value = ZIGZAGD(int32_t, tvalue);
  return ret;
}

// ---- Variant U64
static FORCE_INLINE int taosEncodeVariantU64(void **buf, uint64_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    if (buf != NULL) ((uint8_t *)(*buf))[i] = (uint8_t)(value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 10);
  }

  if (buf != NULL) {
    ((uint8_t *)(*buf))[i] = (uint8_t)value;
    *buf = POINTER_SHIFT(*buf, i + 1);
  }

  return i + 1;
}

static FORCE_INLINE void *taosDecodeVariantU64(void *buf, uint64_t *value) {
  int      i = 0;
  uint64_t tval = 0;
  *value = 0;
  while (i < 10) {
    tval = (uint64_t)(((uint8_t *)buf)[i]);
    if (tval < ENCODE_LIMIT) {
      (*value) |= (tval << (7 * i));
      return POINTER_SHIFT(buf, i + 1);
    } else {
      (*value) |= ((tval & (ENCODE_LIMIT - 1)) << (7 * i));
      i++;
    }
  }

  return NULL;  // error happened
}

// ---- Variant I64
static FORCE_INLINE int taosEncodeVariantI64(void **buf, int64_t value) {
  return taosEncodeVariantU64(buf, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI64(void *buf, int64_t *value) {
  uint64_t tvalue = 0;
  void *   ret = taosDecodeVariantU64(buf, &tvalue);
  *value = ZIGZAGD(int64_t, tvalue);
  return ret;
}

// ---- string
static FORCE_INLINE int taosEncodeString(void **buf, char *value) {
  int    tlen = 0;
  size_t size = strlen(value);

  tlen += taosEncodeVariantU64(buf, size);
  if (buf != NULL) {
    memcpy(*buf, value, size);
    *buf = POINTER_SHIFT(*buf, size);
  }
  tlen += (int)size;

  return tlen;
}

static FORCE_INLINE void *taosDecodeString(void *buf, char **value) {
  uint64_t size = 0;

  buf = taosDecodeVariantU64(buf, &size);
  *value = (char *)malloc((size_t)size + 1);
  if (*value == NULL) return NULL;
  memcpy(*value, buf, (size_t)size);

  (*value)[size] = '\0';

  return POINTER_SHIFT(buf, size);
}

// ------ blank
static FORCE_INLINE int taosEncodeBlank(void **buf, int nblank) {
  // TODO
}

static FORCE_INLINE void *taosDecodeBlank(void *buf) {
  // TODO
}

#ifdef __cplusplus
}
#endif

#endif