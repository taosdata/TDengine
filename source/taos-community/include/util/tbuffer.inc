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

#include "taoserror.h"
#include "tcoding.h"
#include "tutil.h"

struct SBuffer {
  uint32_t size;
  uint32_t capacity;
  void    *data;
};

struct SBufferReader {
  uint32_t offset;
  SBuffer *buffer;
};

// SBuffer
static FORCE_INLINE void tBufferInit(SBuffer *buffer) {
  buffer->size = 0;
  buffer->capacity = 0;
  buffer->data = NULL;
}

static FORCE_INLINE void tBufferDestroy(SBuffer *buffer) {
  buffer->size = 0;
  buffer->capacity = 0;
  if (buffer->data) {
    taosMemoryFree(buffer->data);
    buffer->data = NULL;
  }
}

static FORCE_INLINE void tBufferClear(SBuffer *buffer) { buffer->size = 0; }

static FORCE_INLINE int32_t tBufferEnsureCapacity(SBuffer *buffer, uint32_t capacity) {
  if (buffer->capacity < capacity) {
    uint32_t newCapacity = (buffer->capacity > 0) ? (buffer->capacity << 1) : 1024;
    while (newCapacity < capacity) {
      newCapacity <<= 1;
    }
    void *newData = taosMemoryRealloc(buffer->data, newCapacity);
    if (newData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    buffer->data = newData;
    buffer->capacity = newCapacity;
  }
  return 0;
}

static FORCE_INLINE int32_t tBufferPut(SBuffer *buffer, const void *data, uint32_t size) {
  TAOS_CHECK_RETURN(tBufferEnsureCapacity(buffer, buffer->size + size));
  memcpy((char *)buffer->data + buffer->size, data, size);
  buffer->size += size;
  return 0;
}

static int32_t tBufferPutAt(SBuffer *buffer, uint32_t offset, const void *data, uint32_t size) {
  if (offset + size > buffer->size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  memcpy((char *)buffer->data + offset, data, size);
  return 0;
}

static FORCE_INLINE int32_t tBufferPutI8(SBuffer *buffer, int8_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI16(SBuffer *buffer, int16_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI32(SBuffer *buffer, int32_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI64(SBuffer *buffer, int64_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU8(SBuffer *buffer, uint8_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU16(SBuffer *buffer, uint16_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU32(SBuffer *buffer, uint32_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU64(SBuffer *buffer, uint64_t value) {
  return tBufferPut(buffer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU16v(SBuffer *buffer, uint16_t value) { return tBufferPutU64v(buffer, value); }

static FORCE_INLINE int32_t tBufferPutU32v(SBuffer *buffer, uint32_t value) { return tBufferPutU64v(buffer, value); }

static FORCE_INLINE int32_t tBufferPutU64v(SBuffer *buffer, uint64_t value) {
  while (value >= 0x80) {
    TAOS_CHECK_RETURN(tBufferPutU8(buffer, (value & 0x7F) | 0x80));
    value >>= 7;
  }
  return tBufferPutU8(buffer, value);
}

static FORCE_INLINE int32_t tBufferPutI16v(SBuffer *buffer, int16_t value) {
  return tBufferPutU64v(buffer, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE int32_t tBufferPutI32v(SBuffer *buffer, int32_t value) {
  return tBufferPutU64v(buffer, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE int32_t tBufferPutI64v(SBuffer *buffer, int64_t value) {
  return tBufferPutU64v(buffer, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE int32_t tBufferPutBinary(SBuffer *buffer, const void *data, uint32_t size) {
  TAOS_CHECK_RETURN(tBufferPutU32v(buffer, size));
  return tBufferPut(buffer, data, size);
}

static FORCE_INLINE int32_t tBufferPutCStr(SBuffer *buffer, const char *str) {
  return tBufferPutBinary(buffer, str, str ? strlen(str) + 1 : 0);
}

static FORCE_INLINE int32_t tBufferPutF32(SBuffer *buffer, float value) {
  union {
    float    f;
    uint32_t u;
  } u;
  u.f = value;
  return tBufferPutU32(buffer, u.u);
}

static FORCE_INLINE int32_t tBufferPutF64(SBuffer *buffer, double value) {
  union {
    double   f;
    uint64_t u;
  } u;
  u.f = value;
  return tBufferPutU64(buffer, u.u);
}

// reader
// SBufferReader
static int32_t tBufferReaderInit(SBufferReader *reader, uint32_t offset, SBuffer *buffer) {
  reader->offset = offset;
  reader->buffer = buffer;
  return 0;
}

static FORCE_INLINE int32_t tBufferGet(SBufferReader *reader, uint32_t size, void *data) {
  if (reader->offset + size > reader->buffer->size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  if (data) {
    memcpy(data, BR_PTR(reader), size);
  }
  reader->offset += size;
  return 0;
}

static int32_t tBufferGetI8(SBufferReader *reader, int8_t *value) { return tBufferGet(reader, sizeof(*value), value); }

static int32_t tBufferGetI16(SBufferReader *reader, int16_t *value) {
  return tBufferGet(reader, sizeof(*value), value);
}

static int32_t tBufferGetI32(SBufferReader *reader, int32_t *value) {
  if (reader->offset + sizeof(int32_t) > reader->buffer->size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  if (value) {
    *value = *(int32_t*)BR_PTR(reader);
  }
  reader->offset += sizeof(int32_t);
  return 0;
}

static int32_t tBufferGetI64(SBufferReader *reader, int64_t *value) {
  if (reader->offset + sizeof(int64_t) > reader->buffer->size) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  if (value) {
    *value = *(int64_t*)BR_PTR(reader);
  }
  reader->offset += sizeof(int64_t);
  return 0;
}

static int32_t tBufferGetU8(SBufferReader *reader, uint8_t *value) { return tBufferGet(reader, sizeof(*value), value); }

static int32_t tBufferGetU16(SBufferReader *reader, uint16_t *value) {
  return tBufferGet(reader, sizeof(*value), value);
}

static int32_t tBufferGetU32(SBufferReader *reader, uint32_t *value) {
  return tBufferGet(reader, sizeof(*value), value);
}

static int32_t tBufferGetU64(SBufferReader *reader, uint64_t *value) {
  return tBufferGet(reader, sizeof(*value), value);
}

static int32_t tBufferGetU64v(SBufferReader *reader, uint64_t *value) {
  uint8_t  byte;
  int32_t  code;
  uint64_t u64 = 0;

  for (int32_t i = 0;; i++) {
    code = tBufferGetU8(reader, &byte);
    if (code) return code;

    u64 |= (((uint64_t)(byte & 0x7F)) << (i * 7));

    if (byte < 0x80) {
      break;
    }
  }

  if (value) {
    *value = u64;
  }

  return 0;
}

static int32_t tBufferGetU16v(SBufferReader *reader, uint16_t *value) {
  uint64_t u64;
  int32_t  code = tBufferGetU64v(reader, &u64);
  if (code) return code;
  if (value) {
    *value = (uint16_t)u64;
  }
  return 0;
}

static int32_t tBufferGetU32v(SBufferReader *reader, uint32_t *value) {
  uint64_t u64;
  int32_t  code = tBufferGetU64v(reader, &u64);
  if (code) return code;
  if (value) {
    *value = (uint32_t)u64;
  }
  return 0;
}

static int32_t tBufferGetI16v(SBufferReader *reader, int16_t *value) {
  uint16_t u16;
  int32_t  code = tBufferGetU16v(reader, &u16);
  if (code) return code;
  if (value) {
    *value = ZIGZAGD(int16_t, u16);
  }
  return 0;
}

static int32_t tBufferGetI32v(SBufferReader *reader, int32_t *value) {
  uint32_t u32;
  int32_t  code = tBufferGetU32v(reader, &u32);
  if (code) return code;
  if (value) {
    *value = ZIGZAGD(int32_t, u32);
  }
  return 0;
}

static int32_t tBufferGetI64v(SBufferReader *reader, int64_t *value) {
  uint64_t u64;
  int32_t  code = tBufferGetU64v(reader, &u64);
  if (code) return code;
  if (value) {
    *value = ZIGZAGD(int64_t, u64);
  }
  return 0;
}

static int32_t tBufferGetBinary(SBufferReader *reader, const void **data, uint32_t *size) {
  uint32_t tmpSize;
  int32_t  code;

  // size
  code = tBufferGetU32v(reader, &tmpSize);
  if (code) return code;
  if (size) {
    *size = tmpSize;
  }

  // data
  if (tmpSize > 0) {
    if (reader->offset + tmpSize > reader->buffer->size) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    if (data) {
      *data = BR_PTR(reader);
    }
    reader->offset += tmpSize;
  } else if (data) {
    *data = NULL;
  }

  return 0;
}

static int32_t tBufferGetCStr(SBufferReader *reader, const char **str) {
  return tBufferGetBinary(reader, (const void **)str, NULL);
}

static int32_t tBufferGetF32(SBufferReader *reader, float *value) {
  union {
    float    f;
    uint32_t u;
  } u;
  TAOS_CHECK_RETURN(tBufferGetU32(reader, &u.u));
  if (value) {
    *value = u.f;
  }
  return 0;
}

static int32_t tBufferGetF64(SBufferReader *reader, double *value) {
  union {
    double   f;
    uint64_t u;
  } u;
  TAOS_CHECK_RETURN(tBufferGetU64(reader, &u.u));
  if (value) {
    *value = u.f;
  }
  return 0;
}
