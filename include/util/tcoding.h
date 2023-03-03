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

#ifndef _TD_UTIL_CODING_H_
#define _TD_UTIL_CODING_H_

#include "os.h"
#include "tlog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define ENCODE_LIMIT  (((uint8_t)1) << 7)
#define ZIGZAGE(T, v) (((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1))  // zigzag encode
#define ZIGZAGD(T, v) (((v) >> 1) ^ -((T)((v)&1)))                                 // zigzag decode

/* ------------------------ LEGACY CODES ------------------------ */
#if 1
// ---- Fixed U8
static FORCE_INLINE int32_t taosEncodeFixedU8(void **buf, uint8_t value) {
  if (buf != NULL) {
    ((uint8_t *)(*buf))[0] = value;
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int32_t)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU8(const void *buf, uint8_t *value) {
  *value = ((uint8_t *)buf)[0];
  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I8
static FORCE_INLINE int32_t taosEncodeFixedI8(void **buf, int8_t value) {
  if (buf != NULL) {
    ((int8_t *)(*buf))[0] = value;
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }
  return (int32_t)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedI8(const void *buf, int8_t *value) {
  *value = ((int8_t *)buf)[0];
  return POINTER_SHIFT(buf, sizeof(*value));
}

static FORCE_INLINE void *taosSkipFixedLen(const void *buf, size_t len) { return POINTER_SHIFT(buf, len); }

// --- Bool

static FORCE_INLINE int32_t taosEncodeFixedBool(void **buf, bool value) {
  if (buf != NULL) {
    ((int8_t *)(*buf))[0] = (value ? 1 : 0);
    *buf = POINTER_SHIFT(*buf, sizeof(int8_t));
  }
  return (int32_t)sizeof(int8_t);
}

static FORCE_INLINE void *taosDecodeFixedBool(const void *buf, bool *value) {
  *value = ((((int8_t *)buf)[0] == 0) ? false : true);
  return POINTER_SHIFT(buf, sizeof(int8_t));
}

// ---- Fixed U16
static FORCE_INLINE int32_t taosEncodeFixedU16(void **buf, uint16_t value) {
  if (buf != NULL) {
    if (IS_LITTLE_ENDIAN()) {
      memcpy(*buf, &value, sizeof(value));
    } else {
      ((uint8_t *)(*buf))[0] = value & 0xff;
      ((uint8_t *)(*buf))[1] = (value >> 8) & 0xff;
    }
    *buf = POINTER_SHIFT(*buf, sizeof(value));
  }

  return (int32_t)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU16(const void *buf, uint16_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((uint8_t *)value)[1] = ((uint8_t *)buf)[0];
    ((uint8_t *)value)[0] = ((uint8_t *)buf)[1];
  }

  return POINTER_SHIFT(buf, sizeof(*value));
}

// ---- Fixed I16
static FORCE_INLINE int32_t taosEncodeFixedI16(void **buf, int16_t value) {
  return taosEncodeFixedU16(buf, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI16(const void *buf, int16_t *value) {
  uint16_t tvalue = 0;
  void    *ret = taosDecodeFixedU16(buf, &tvalue);
  *value = ZIGZAGD(int16_t, tvalue);
  return ret;
}

// ---- Fixed U32
static FORCE_INLINE int32_t taosEncodeFixedU32(void **buf, uint32_t value) {
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

  return (int32_t)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU32(const void *buf, uint32_t *value) {
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
static FORCE_INLINE int32_t taosEncodeFixedI32(void **buf, int32_t value) {
  return taosEncodeFixedU32(buf, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI32(const void *buf, int32_t *value) {
  uint32_t tvalue = 0;
  void    *ret = taosDecodeFixedU32(buf, &tvalue);
  *value = ZIGZAGD(int32_t, tvalue);
  return ret;
}

// ---- Fixed U64
static FORCE_INLINE int32_t taosEncodeFixedU64(void **buf, uint64_t value) {
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

  return (int32_t)sizeof(value);
}

static FORCE_INLINE void *taosDecodeFixedU64(const void *buf, uint64_t *value) {
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
static FORCE_INLINE int32_t taosEncodeFixedI64(void **buf, int64_t value) {
  return taosEncodeFixedU64(buf, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE void *taosDecodeFixedI64(const void *buf, int64_t *value) {
  uint64_t tvalue = 0;
  void    *ret = taosDecodeFixedU64(buf, &tvalue);
  *value = ZIGZAGD(int64_t, tvalue);
  return ret;
}

// ---- Variant U16
static FORCE_INLINE int32_t taosEncodeVariantU16(void **buf, uint16_t value) {
  int32_t i = 0;
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

static FORCE_INLINE void *taosDecodeVariantU16(const void *buf, uint16_t *value) {
  int32_t  i = 0;
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
static FORCE_INLINE int32_t taosEncodeVariantI16(void **buf, int16_t value) {
  return taosEncodeVariantU16(buf, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI16(const void *buf, int16_t *value) {
  uint16_t tvalue = 0;
  void    *ret = taosDecodeVariantU16(buf, &tvalue);
  *value = ZIGZAGD(int16_t, tvalue);
  return ret;
}

// ---- Variant U32
static FORCE_INLINE int32_t taosEncodeVariantU32(void **buf, uint32_t value) {
  int32_t i = 0;
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

static FORCE_INLINE void *taosDecodeVariantU32(const void *buf, uint32_t *value) {
  int32_t  i = 0;
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
static FORCE_INLINE int32_t taosEncodeVariantI32(void **buf, int32_t value) {
  return taosEncodeVariantU32(buf, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI32(const void *buf, int32_t *value) {
  uint32_t tvalue = 0;
  void    *ret = taosDecodeVariantU32(buf, &tvalue);
  *value = ZIGZAGD(int32_t, tvalue);
  return ret;
}

// ---- Variant U64
static FORCE_INLINE int32_t taosEncodeVariantU64(void **buf, uint64_t value) {
  int32_t i = 0;
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

static FORCE_INLINE void *taosDecodeVariantU64(const void *buf, uint64_t *value) {
  int32_t  i = 0;
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
static FORCE_INLINE int32_t taosEncodeVariantI64(void **buf, int64_t value) {
  return taosEncodeVariantU64(buf, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE void *taosDecodeVariantI64(const void *buf, int64_t *value) {
  uint64_t tvalue = 0;
  void    *ret = taosDecodeVariantU64(buf, &tvalue);
  *value = ZIGZAGD(int64_t, tvalue);
  return ret;
}

// ---- string
static FORCE_INLINE int32_t taosEncodeString(void **buf, const char *value) {
  int32_t tlen = 0;
  size_t  size = strlen(value);

  tlen += taosEncodeVariantU64(buf, size);
  if (buf != NULL) {
    memcpy(*buf, value, size);
    *buf = POINTER_SHIFT(*buf, size);
  }
  tlen += (int32_t)size;

  return tlen;
}

static FORCE_INLINE void *taosDecodeString(const void *buf, char **value) {
  uint64_t size = 0;

  buf = taosDecodeVariantU64(buf, &size);
  *value = (char *)taosMemoryMalloc((size_t)size + 1);

  if (*value == NULL) return NULL;
  memcpy(*value, buf, (size_t)size);

  (*value)[size] = '\0';

  return POINTER_SHIFT(buf, size);
}

static FORCE_INLINE void *taosDecodeStringTo(const void *buf, char *value) {
  uint64_t size = 0;

  buf = taosDecodeVariantU64(buf, &size);
  memcpy(value, buf, (size_t)size);

  value[size] = '\0';

  return POINTER_SHIFT(buf, size);
}

// ---- binary
static FORCE_INLINE int32_t taosEncodeBinary(void **buf, const void *value, int32_t valueLen) {
  int32_t tlen = 0;

  if (buf != NULL) {
    memcpy(*buf, value, valueLen);
    *buf = POINTER_SHIFT(*buf, valueLen);
  }
  tlen += (int32_t)valueLen;

  return tlen;
}

static FORCE_INLINE void *taosDecodeBinary(const void *buf, void **value, int32_t valueLen) {
  *value = taosMemoryMalloc((size_t)valueLen);
  if (*value == NULL) return NULL;
  memcpy(*value, buf, (size_t)valueLen);

  return POINTER_SHIFT(buf, valueLen);
}

static FORCE_INLINE void *taosDecodeBinaryTo(const void *buf, void *value, int32_t valueLen) {
  memcpy(value, buf, (size_t)valueLen);
  return POINTER_SHIFT(buf, valueLen);
}

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_CODING_H_*/
