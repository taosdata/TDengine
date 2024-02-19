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

struct SBuffer {
  uint32_t size;
  uint32_t capacity;
  void    *data;
};

struct SBufferWriter {
  bool     forward;
  uint32_t offset;
  SBuffer *buffer;
};

struct SBufferReader {
  bool     forward;
  uint32_t offset;
  SBuffer *buffer;
};

// SBuffer
static FORCE_INLINE int32_t tBufferInit(SBuffer *buffer) {
  buffer->size = 0;
  buffer->capacity = 0;
  buffer->data = NULL;
  return 0;
}

static FORCE_INLINE int32_t tBufferDestroy(SBuffer *buffer) {
  buffer->size = 0;
  buffer->capacity = 0;
  if (buffer->data) {
    taosMemoryFree(buffer->data);
    buffer->data = NULL;
  }
  return 0;
}

static FORCE_INLINE int32_t tBufferClear(SBuffer *buffer) {
  buffer->size = 0;
  return 0;
}

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

static int32_t tBufferAppend(SBuffer *buffer, const void *data, uint32_t size) {
  int32_t code = tBufferEnsureCapacity(buffer, buffer->size + size);
  if (code) return code;
  memcpy((char *)buffer->data + buffer->size, data, size);
  buffer->size += size;
  return 0;
}

// SBufferWriter
static int32_t tBufferWriterInit(SBufferWriter *writer, bool forward, uint32_t offset, SBuffer *buffer) {
  writer->forward = forward;
  writer->offset = offset;
  writer->buffer = buffer;
  return 0;
}

static FORCE_INLINE int32_t tBufferPutFixed(SBufferWriter *writer, const void *data, uint32_t size) {
  if (!writer->forward && writer->offset < size) {
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  int32_t code = tBufferEnsureCapacity(writer->buffer, writer->forward ? writer->offset + size : writer->offset);
  if (code) return code;

  if (writer->forward) {
    memcpy((char *)writer->buffer->data + writer->offset, data, size);
    writer->offset += size;
  } else {
    writer->offset -= size;
    memcpy((char *)writer->buffer->data + writer->offset, data, size);
  }
  return 0;
}

static FORCE_INLINE int32_t tBufferPutI8(SBufferWriter *writer, int8_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI16(SBufferWriter *writer, int16_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI32(SBufferWriter *writer, int32_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutI64(SBufferWriter *writer, int64_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU8(SBufferWriter *writer, uint8_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU16(SBufferWriter *writer, uint16_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU32(SBufferWriter *writer, uint32_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU64(SBufferWriter *writer, uint64_t value) {
  return tBufferPutFixed(writer, &value, sizeof(value));
}

static FORCE_INLINE int32_t tBufferPutU64v(SBufferWriter *writer, uint64_t value) {
  int32_t code;
  while (value >= 0x80) {
    code = tBufferPutU8(writer, (value & 0x7F) | 0x80);
    if (code) return code;
    value >>= 7;
  }
  return tBufferPutU8(writer, value);
}

static FORCE_INLINE int32_t tBufferPutU16v(SBufferWriter *writer, uint16_t value) {
  return tBufferPutU64v(writer, value);
}

static FORCE_INLINE int32_t tBufferPutU32v(SBufferWriter *writer, uint32_t value) {
  return tBufferPutU64v(writer, value);
}

static FORCE_INLINE int32_t tBufferPutI16v(SBufferWriter *writer, int16_t value) {
  return tBufferPutU16v(writer, ZIGZAGE(int16_t, value));
}

static FORCE_INLINE int32_t tBufferPutI32v(SBufferWriter *writer, int32_t value) {
  return tBufferPutU32v(writer, ZIGZAGE(int32_t, value));
}

static FORCE_INLINE int32_t tBufferPutI64v(SBufferWriter *writer, int64_t value) {
  return tBufferPutU64v(writer, ZIGZAGE(int64_t, value));
}

static FORCE_INLINE int32_t tBufferPutBinary(SBufferWriter *writer, const void *data, uint32_t size) {
  int32_t code = tBufferPutU32(writer, size);
  if (code) return code;
  return tBufferPutFixed(writer, data, size);
}

static FORCE_INLINE int32_t tBufferPutCStr(SBufferWriter *writer, const char *str) {
  return tBufferPutBinary(writer, str, strlen(str) + 1);
}

static FORCE_INLINE int32_t tBufferPutF32(SBufferWriter *writer, float value) {
  union {
    float    f;
    uint32_t u;
  } u = {.f = value};
  return tBufferPutU32(writer, u.u);
}

static FORCE_INLINE int32_t tBufferPutF64(SBufferWriter *writer, double value) {
  union {
    double   f;
    uint64_t u;
  } u = {.f = value};
  return tBufferPutU64(writer, u.u);
}

// SBufferReader
static int32_t tBufferReaderInit(SBufferReader *reader, bool forward, uint32_t offset, SBuffer *buffer) {
  reader->forward = forward;
  reader->offset = offset;
  reader->buffer = buffer;
  return 0;
}

static int32_t tBufferGetFixed(SBufferReader *reader, void *data, uint32_t size) {
  if ((reader->forward && reader->offset + size > reader->buffer->capacity) ||
      (!reader->forward && reader->offset < size)) {
    return TSDB_CODE_OPS_NOT_SUPPORT;
  }

  if (data) {
    if (reader->forward) {
      memcpy(data, (char *)reader->buffer->data + reader->offset, size);
      reader->offset += size;
    } else {
      reader->offset -= size;
      memcpy(data, (char *)reader->buffer->data + reader->offset, size);
    }
  }
  return 0;
}

static int32_t tBufferGetI8(SBufferReader *reader, int8_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetI16(SBufferReader *reader, int16_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetI32(SBufferReader *reader, int32_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetI64(SBufferReader *reader, int64_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetU8(SBufferReader *reader, uint8_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetU16(SBufferReader *reader, uint16_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetU32(SBufferReader *reader, uint32_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetU64(SBufferReader *reader, uint64_t *value) {
  return tBufferGetFixed(reader, value, sizeof(*value));
}

static int32_t tBufferGetU64v(SBufferReader *reader, uint64_t *value) {
  uint8_t byte;
  int32_t code;

  if (value) {
    *value = 0;
  }

  for (int32_t i = 0;; i++) {
    code = tBufferGetU8(reader, &byte);
    if (code) return code;

    if (value) {
      *value |= ((uint64_t)(byte & 0x7F)) << (i * 7);
    }

    if (byte < 0x80) {
      break;
    }
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
  code = tBufferGetU32(reader, &tmpSize);
  if (code) return code;
  if (size) {
    *size = tmpSize;
  }
  // data
  if (reader->forward) {
    if (reader->offset + tmpSize > reader->buffer->capacity) {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
    if (data) {
      *data = (char *)reader->buffer->data + reader->offset;
    }
    reader->offset += tmpSize;
  } else {
    if (reader->offset < tmpSize) {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
    reader->offset -= tmpSize;
    if (data) {
      *data = (char *)reader->buffer->data + reader->offset;
    }
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
  int32_t code = tBufferGetU32(reader, &u.u);
  if (code) return code;
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
  int32_t code = tBufferGetU64(reader, &u.u);
  if (code) return code;
  if (value) {
    *value = u.f;
  }
  return 0;
}
