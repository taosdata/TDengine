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
#ifndef _TD_UTIL_CODING_H
#define _TD_UTIL_CODING_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#define ENCODE_LIMIT (((uint8_t)1) << 7)
#define ZIGZAGE(T, v) ((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1)  // zigzag encode
#define ZIGZAGD(T, v) ((v) >> 1) ^ -((T)((v)&1))                                 // zigzag decode

/* ------------------------ FIXED-LENGTH ENCODING ------------------------ */
// 16
#define tPut16b(BUF, VAL)                        \
  ({                                             \
    ((uint8_t *)(BUF))[1] = (VAL)&0xff;          \
    ((uint8_t *)(BUF))[0] = ((VAL) >> 8) & 0xff; \
    2;                                           \
  })

#define tGet16b(BUF, VAL)           \
  ({                                \
    (VAL) = ((uint8_t *)(BUF))[0];  \
    (VAL) = (VAL) << 8;             \
    (VAL) |= ((uint8_t *)(BUF))[1]; \
    2;                              \
  })

#define tPut16l(BUF, VAL)                        \
  ({                                             \
    ((uint8_t *)(BUF))[0] = (VAL)&0xff;          \
    ((uint8_t *)(BUF))[1] = ((VAL) >> 8) & 0xff; \
    2;                                           \
  })

#define tGet16l(BUF, VAL)           \
  ({                                \
    (VAL) = ((uint8_t *)(BUF))[1];  \
    (VAL) <<= 8;                    \
    (VAL) |= ((uint8_t *)(BUF))[0]; \
    2;                              \
  })

// 32
#define tPut32b(BUF, VAL)                         \
  ({                                              \
    ((uint8_t *)(BUF))[3] = (VAL)&0xff;           \
    ((uint8_t *)(BUF))[2] = ((VAL) >> 8) & 0xff;  \
    ((uint8_t *)(BUF))[1] = ((VAL) >> 16) & 0xff; \
    ((uint8_t *)(BUF))[0] = ((VAL) >> 24) & 0xff; \
    4;                                            \
  })

#define tGet32b(BUF, VAL)          \
  ({                               \
    (VAL) = ((uint8_t *)(BUF))[0]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[1]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[2]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[3]; \
    4;                             \
  })

#define tPut32l(BUF, VAL)                         \
  ({                                              \
    ((uint8_t *)(BUF))[0] = (VAL)&0xff;           \
    ((uint8_t *)(BUF))[1] = ((VAL) >> 8) & 0xff;  \
    ((uint8_t *)(BUF))[2] = ((VAL) >> 16) & 0xff; \
    ((uint8_t *)(BUF))[3] = ((VAL) >> 24) & 0xff; \
    4;                                            \
  })

#define tGet32l(BUF, VAL)          \
  ({                               \
    (VAL) = ((uint8_t *)(BUF))[3]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[2]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[1]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[0]; \
    4;                             \
  })

// 64
#define tPut64b(BUF, VAL)                         \
  ({                                              \
    ((uint8_t *)(BUF))[7] = (VAL)&0xff;           \
    ((uint8_t *)(BUF))[6] = ((VAL) >> 8) & 0xff;  \
    ((uint8_t *)(BUF))[5] = ((VAL) >> 16) & 0xff; \
    ((uint8_t *)(BUF))[4] = ((VAL) >> 24) & 0xff; \
    ((uint8_t *)(BUF))[3] = ((VAL) >> 32) & 0xff; \
    ((uint8_t *)(BUF))[2] = ((VAL) >> 40) & 0xff; \
    ((uint8_t *)(BUF))[1] = ((VAL) >> 48) & 0xff; \
    ((uint8_t *)(BUF))[0] = ((VAL) >> 56) & 0xff; \
    8;                                            \
  })

#define tGet64b(BUF, VAL)          \
  ({                               \
    (VAL) = ((uint8_t *)(BUF))[0]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[1]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[2]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[3]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[4]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[5]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[6]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[7]; \
    8;                             \
  })

#define tPut64l(BUF, VAL)                         \
  ({                                              \
    ((uint8_t *)(BUF))[0] = (VAL)&0xff;           \
    ((uint8_t *)(BUF))[1] = ((VAL) >> 8) & 0xff;  \
    ((uint8_t *)(BUF))[2] = ((VAL) >> 16) & 0xff; \
    ((uint8_t *)(BUF))[3] = ((VAL) >> 24) & 0xff; \
    ((uint8_t *)(BUF))[4] = ((VAL) >> 32) & 0xff; \
    ((uint8_t *)(BUF))[5] = ((VAL) >> 40) & 0xff; \
    ((uint8_t *)(BUF))[6] = ((VAL) >> 48) & 0xff; \
    ((uint8_t *)(BUF))[7] = ((VAL) >> 56) & 0xff; \
    8;                                            \
  })

#define tGet64l(BUF, VAL)          \
  ({                               \
    (VAL) = ((uint8_t *)(BUF))[7]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[6]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[5]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[4]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[3]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[2]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[1]; \
    (VAL) <<= 8;                   \
    (VAL) = ((uint8_t *)(BUF))[0]; \
    8;                             \
  })

#define tPut(BUF, VAL, TYPE) \
  ({                         \
    *(TYPE *)(BUF) = (VAL);  \
    sizeof(TYPE);            \
  })

#define tGet(BUF, VAL, TYPE)  \
  ({                          \
    (VAL) = (*(TYPE *)(BUF)); \
    sizeof(TYPE);             \
  })

#define tPut_uint16_t_l(BUF, VAL, TYPE) tPut16l(BUF, VAL)
#define tGet_uint16_t_l(BUF, VAL, TYPE) tGet16l(BUF, VAL)
#define tPut_int16_t_l(BUF, VAL, TYPE) tPut16l(BUF, VAL)
#define tGet_int16_t_l(BUF, VAL, TYPE) tGet16l(BUF, VAL)

#define tPut_uint32_t_l(BUF, VAL, TYPE) tPut32l(BUF, VAL)
#define tGet_uint32_t_l(BUF, VAL, TYPE) tGet32l(BUF, VAL)
#define tPut_int32_t_l(BUF, VAL, TYPE) tPut32l(BUF, VAL)
#define tGet_int32_t_l(BUF, VAL, TYPE) tGet32l(BUF, VAL)

#define tPut_uint64_t_l(BUF, VAL, TYPE) tPut64l(BUF, VAL)
#define tGet_uint64_t_l(BUF, VAL, TYPE) tGet64l(BUF, VAL)
#define tPut_int64_t_l(BUF, VAL, TYPE) tPut64l(BUF, VAL)
#define tGet_int64_t_l(BUF, VAL, TYPE) tGet64l(BUF, VAL)

#define tPut_uint16_t_b(BUF, VAL, TYPE) tPut16b(BUF, VAL)
#define tGet_uint16_t_b(BUF, VAL, TYPE) tGet16b(BUF, VAL)
#define tPut_int16_t_b(BUF, VAL, TYPE) tPut16b(BUF, VAL)
#define tGet_int16_t_b(BUF, VAL, TYPE) tGet16b(BUF, VAL)

#define tPut_uint32_t_b(BUF, VAL, TYPE) tPut32b(BUF, VAL)
#define tGet_uint32_t_b(BUF, VAL, TYPE) tGet32b(BUF, VAL)
#define tPut_int32_t_b(BUF, VAL, TYPE) tPut32b(BUF, VAL)
#define tGet_int32_t_b(BUF, VAL, TYPE) tGet32b(BUF, VAL)

#define tPut_uint64_t_b(BUF, VAL, TYPE) tPut64b(BUF, VAL)
#define tGet_uint64_t_b(BUF, VAL, TYPE) tGet64b(BUF, VAL)
#define tPut_int64_t_b(BUF, VAL, TYPE) tPut64b(BUF, VAL)
#define tGet_int64_t_b(BUF, VAL, TYPE) tGet64b(BUF, VAL)

#define tPutl(BUF, VAL, TYPE) tPut_##TYPE##_l(BUF, VAL, TYPE)

#define tGetl(BUF, VAL, TYPE) tGet_##TYPE##_l(BUF, VAL, TYPE)

#define tPutb(BUF, VAL, TYPE) tPut_##TYPE##_b(BUF, VAL, TYPE)

#define tGetb(BUF, VAL, TYPE) tGet_##TYPE##_b(BUF, VAL, TYPE)

/* ------------------------ VARIANT-LENGTH ENCODING ------------------------ */
#define vPut(BUF, VAL, SIGN)                                   \
  ({                                                           \
    uint64_t tmp = (SIGN) ? ZIGZAGE(int64_t, VAL) : (VAL);     \
    int      i = 0;                                            \
    while ((VAL) >= ENCODE_LIMIT) {                            \
      ((uint8_t *)(BUF))[i] = (uint8_t)((tmp) | ENCODE_LIMIT); \
      (tmp) >>= 7;                                             \
      i++;                                                     \
    }                                                          \
    ((uint8_t *)(BUF))[i] = (uint8_t)(tmp);                    \
    i + 1;                                                     \
  })

#define vGet(BUF, VAL, SIGN)                              \
  ({                                                      \
    uint64_t tmp;                                         \
    uint64_t tval = 0;                                    \
    int      i = 0;                                       \
    while (true) {                                        \
      tmp = (uint64_t)(((uint8_t *)(BUF))[i]);            \
      if (tmp < ENCODE_LIMIT) {                           \
        tval |= (tval << (7 * i));                        \
        break;                                            \
      } else {                                            \
        tval |= ((tval & (ENCODE_LIMIT - 1)) << (7 * i)); \
        i++;                                              \
      }                                                   \
    }                                                     \
    if (SIGN) {                                           \
      (VAL) = ZIGZAGD(int64_t, tmp);                      \
    }                                                     \
    i;                                                    \
  })

/* ------------------------ OTHER TYPE ENCODING ------------------------ */

/* ------------------------ LEGACY CODES ------------------------ */
#if 1
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
static FORCE_INLINE int taosEncodeString(void **buf, const char *value) {
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

static FORCE_INLINE void *taosDecodeStringTo(void *buf, char *value) {
  uint64_t size = 0;

  buf = taosDecodeVariantU64(buf, &size);
  memcpy(value, buf, (size_t)size);

  value[size] = '\0';

  return POINTER_SHIFT(buf, size);
}

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_CODING_H*/