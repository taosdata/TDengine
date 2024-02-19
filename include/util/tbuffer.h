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

#include "os.h"

#ifndef __TD_BUFFER_H__
#define __TD_BUFFER_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBuffer       SBuffer;
typedef struct SBufferWriter SBufferWriter;
typedef struct SBufferReader SBufferReader;

// SBuffer
#define BUFFER_INITILIZER ((SBuffer){0, 0, NULL})
static int32_t tBufferInit(SBuffer *buffer);
static int32_t tBufferDestroy(SBuffer *buffer);
static int32_t tBufferClear(SBuffer *buffer);
static int32_t tBufferEnsureCapacity(SBuffer *buffer, uint32_t capacity);
static int32_t tBufferAppend(SBuffer *buffer, const void *data, uint32_t size);
#define tBufferGetSize(buffer)     ((buffer)->size)
#define tBufferGetCapacity(buffer) ((buffer)->capacity)
#define tBufferGetData(buffer)     ((const void *)(buffer)->data)

// SBufferWriter
#define tBufferWriterInit(forward, offset, buffer) ((SBufferWriter){forward, offset, buffer})
#define tBufferWriterDestroy(writer)               ((void)0)
#define tBufferWriterGetOffset(writer)             ((writer)->offset)
static int32_t tBufferPutFixed(SBufferWriter *writer, const void *data, uint32_t size);
static int32_t tBufferPutI8(SBufferWriter *writer, int8_t value);
static int32_t tBufferPutI16(SBufferWriter *writer, int16_t value);
static int32_t tBufferPutI32(SBufferWriter *writer, int32_t value);
static int32_t tBufferPutI64(SBufferWriter *writer, int64_t value);
static int32_t tBufferPutU8(SBufferWriter *writer, uint8_t value);
static int32_t tBufferPutU16(SBufferWriter *writer, uint16_t value);
static int32_t tBufferPutU32(SBufferWriter *writer, uint32_t value);
static int32_t tBufferPutU64(SBufferWriter *writer, uint64_t value);
static int32_t tBufferPutI16v(SBufferWriter *writer, int16_t value);
static int32_t tBufferPutI32v(SBufferWriter *writer, int32_t value);
static int32_t tBufferPutI64v(SBufferWriter *writer, int64_t value);
static int32_t tBufferPutU16v(SBufferWriter *writer, uint16_t value);
static int32_t tBufferPutU32v(SBufferWriter *writer, uint32_t value);
static int32_t tBufferPutU64v(SBufferWriter *writer, uint64_t value);
static int32_t tBufferPutBinary(SBufferWriter *writer, const void *data, uint32_t size);
static int32_t tBufferPutCStr(SBufferWriter *writer, const char *str);
static int32_t tBufferPutF32(SBufferWriter *writer, float value);
static int32_t tBufferPutF64(SBufferWriter *writer, double value);

// SBufferReader
#define tBufferReaderInit(forward, offset, buffer) ((SBufferReader){forward, offset, buffer})
#define tBufferReaderDestroy(reader)               ((void)0)
#define tBufferReaderGetOffset(reader)             ((reader)->offset)
static int32_t tBufferGetFixed(SBufferReader *reader, void *data, uint32_t size);
static int32_t tBufferGetI8(SBufferReader *reader, int8_t *value);
static int32_t tBufferGetI16(SBufferReader *reader, int16_t *value);
static int32_t tBufferGetI32(SBufferReader *reader, int32_t *value);
static int32_t tBufferGetI64(SBufferReader *reader, int64_t *value);
static int32_t tBufferGetU8(SBufferReader *reader, uint8_t *value);
static int32_t tBufferGetU16(SBufferReader *reader, uint16_t *value);
static int32_t tBufferGetU32(SBufferReader *reader, uint32_t *value);
static int32_t tBufferGetU64(SBufferReader *reader, uint64_t *value);
static int32_t tBufferGetI16v(SBufferReader *reader, int16_t *value);
static int32_t tBufferGetI32v(SBufferReader *reader, int32_t *value);
static int32_t tBufferGetI64v(SBufferReader *reader, int64_t *value);
static int32_t tBufferGetU16v(SBufferReader *reader, uint16_t *value);
static int32_t tBufferGetU32v(SBufferReader *reader, uint32_t *value);
static int32_t tBufferGetU64v(SBufferReader *reader, uint64_t *value);
static int32_t tBufferGetBinary(SBufferReader *reader, const void **data, uint32_t *size);
static int32_t tBufferGetCStr(SBufferReader *reader, const char **str);
static int32_t tBufferGetF32(SBufferReader *reader, float *value);
static int32_t tBufferGetF64(SBufferReader *reader, double *value);

#include "tbuffer.inc"

#ifdef __cplusplus
}
#endif

#endif /*__TD_BUFFER_H__*/