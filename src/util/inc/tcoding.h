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

const int TNUMBER = 1;
#define IS_LITTLE_ENDIAN() (*(char *)(&TNUMBER) != 0)

static FORCE_INLINE void *taosEncodeFixed16(void *buf, uint16_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((char *)buf)[0] = value & 0xff;
    ((char *)buf)[1] = (value >> 8) & 0xff;
  }

  return POINTER_DRIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosEncodeFixed32(void *buf, uint32_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((char *)buf)[0] = value & 0xff;
    ((char *)buf)[1] = (value >> 8) & 0xff;
    ((char *)buf)[2] = (value >> 16) & 0xff;
    ((char *)buf)[3] = (value >> 24) & 0xff;
  }

  return POINTER_DRIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosEncodeFixed64(void *buf, uint64_t value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(buf, &value, sizeof(value));
  } else {
    ((char *)buf)[0] = value & 0xff;
    ((char *)buf)[1] = (value >> 8) & 0xff;
    ((char *)buf)[2] = (value >> 16) & 0xff;
    ((char *)buf)[3] = (value >> 24) & 0xff;
    ((char *)buf)[4] = (value >> 32) & 0xff;
    ((char *)buf)[5] = (value >> 40) & 0xff;
    ((char *)buf)[6] = (value >> 48) & 0xff;
    ((char *)buf)[7] = (value >> 56) & 0xff;
  }

  return POINTER_DRIFT(buf, sizeof(value));
}

static FORCE_INLINE void *taosDecodeFixed16(void *buf, uint16_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((char *)value)[1] = ((char *)buf)[0];
    ((char *)value)[0] = ((char *)buf)[1];
  }

  return POINTER_DRIFT(buf, sizeof(*value));
}

static FORCE_INLINE void *taosDecodeFixed32(void *buf, uint32_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((char *)value)[3] = ((char *)buf)[0];
    ((char *)value)[2] = ((char *)buf)[1];
    ((char *)value)[1] = ((char *)buf)[2];
    ((char *)value)[0] = ((char *)buf)[3];
  }

  return POINTER_DRIFT(buf, sizeof(*value));
}

static FORCE_INLINE void *taosDecodeFixed64(void *buf, uint64_t *value) {
  if (IS_LITTLE_ENDIAN()) {
    memcpy(value, buf, sizeof(*value));
  } else {
    ((char *)value)[7] = ((char *)buf)[0];
    ((char *)value)[6] = ((char *)buf)[1];
    ((char *)value)[5] = ((char *)buf)[2];
    ((char *)value)[4] = ((char *)buf)[3];
    ((char *)value)[3] = ((char *)buf)[4];
    ((char *)value)[2] = ((char *)buf)[5];
    ((char *)value)[1] = ((char *)buf)[6];
    ((char *)value)[0] = ((char *)buf)[7];
  }

  return POINTER_DRIFT(buf, sizeof(*value));
}

// TODO
static FORCE_INLINE void *taosEncodeVariant16(void *buf, uint16_t value) {}
static FORCE_INLINE void *taosEncodeVariant32(void *buf, uint32_t value) {}
static FORCE_INLINE void *taosEncodeVariant64(void *buf, uint64_t value) {}
static FORCE_INLINE void *taosDecodeVariant16(void *buf, uint16_t *value) {}
static FORCE_INLINE void *taosDecodeVariant32(void *buf, uint32_t *value) {}
static FORCE_INLINE void *taosDecodeVariant64(void *buf, uint64_t *value) {}

#ifdef __cplusplus
}
#endif

#endif