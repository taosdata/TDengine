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

static FORCE_INLINE void *taosEncodeFixed8(void *buf, uint8_t value) {
  ((uint8_t *)buf)[0] = value;
  return POINTER_SHIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosEncodeFixed16(void *buf, uint16_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((uint8_t *)buf)[0] = value & 0xff;
    ((uint8_t *)buf)[1] = (value >> 8) & 0xff;
  }

  return POINTER_SHIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosEncodeFixed32(void *buf, uint32_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((uint8_t *)buf)[0] = value & 0xff;
    ((uint8_t *)buf)[1] = (value >> 8) & 0xff;
    ((uint8_t *)buf)[2] = (value >> 16) & 0xff;
    ((uint8_t *)buf)[3] = (value >> 24) & 0xff;
  }

  return POINTER_SHIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosEncodeFixed64(void *buf, uint64_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((uint8_t *)buf)[0] = value & 0xff;
    ((uint8_t *)buf)[1] = (value >> 8) & 0xff;
    ((uint8_t *)buf)[2] = (value >> 16) & 0xff;
    ((uint8_t *)buf)[3] = (value >> 24) & 0xff;
    ((uint8_t *)buf)[4] = (value >> 32) & 0xff;
    ((uint8_t *)buf)[5] = (value >> 40) & 0xff;
    ((uint8_t *)buf)[6] = (value >> 48) & 0xff;
    ((uint8_t *)buf)[7] = (value >> 56) & 0xff;
  }

  return POINTER_SHIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosDecodeFixed8(void *buf, uint8_t *value) {
  *value = ((uint8_t *)buf)[0];
  return POINTER_SHIFT(buf, sizeof(*value));
}

static FORCE_INLINE void *taosDecodeFixed16(void *buf, uint16_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((uint8_t *)value)[1] = ((uint8_t *)buf)[0];
    ((uint8_t *)value)[0] = ((uint8_t *)buf)[1];
  }

  return POINTER_SHIFT(buf, sizeof(*value));
}

static FORCE_INLINE void *taosDecodeFixed32(void *buf, uint32_t *value) {
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

static FORCE_INLINE void *taosDecodeFixed64(void *buf, uint64_t *value) {
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

static FORCE_INLINE void *taosEncodeVariant16(void *buf, uint16_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    ((uint8_t *)buf)[i] = (value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 3);
  }

  ((uint8_t *)buf)[i] = value;

  return POINTER_SHIFT(buf, i+1);
}

static FORCE_INLINE void *taosEncodeVariant32(void *buf, uint32_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    ((uint8_t *)buf)[i] = (value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 5);
  }

  ((uint8_t *)buf)[i] = value;

  return POINTER_SHIFT(buf, i + 1);
}

static FORCE_INLINE void *taosEncodeVariant64(void *buf, uint64_t value) {
  int i = 0;
  while (value >= ENCODE_LIMIT) {
    ((uint8_t *)buf)[i] = (value | ENCODE_LIMIT);
    value >>= 7;
    i++;
    ASSERT(i < 10);
  }

  ((uint8_t *)buf)[i] = value;

  return POINTER_SHIFT(buf, i + 1);
}

static FORCE_INLINE void *taosDecodeVariant16(void *buf, uint16_t *value) {
  int i = 0;
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

static FORCE_INLINE void *taosDecodeVariant32(void *buf, uint32_t *value) {
  int i = 0;
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

static FORCE_INLINE void *taosDecodeVariant64(void *buf, uint64_t *value) {
  int i = 0;
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

static FORCE_INLINE void *taosEncodeString(void *buf, char *value) {
  size_t size = strlen(value);

  buf = taosEncodeVariant64(buf, size);
  memcpy(buf, value, size);

  return POINTER_SHIFT(buf, size);
}

static FORCE_INLINE void *taosDecodeString(void *buf, char **value) {
  uint64_t size = 0;

  buf = taosDecodeVariant64(buf, &size);
  *value = (char *)malloc(size + 1);
  if (*value == NULL) return NULL;
  memcpy(*value, buf, size);

  (*value)[size] = '\0';

  return POINTER_SHIFT(buf, size);
}

#ifdef __cplusplus
}
#endif

#endif