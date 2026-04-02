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
typedef struct SBufferReader SBufferReader;

// SBuffer
#define BUFFER_INITIALIZER ((SBuffer){0, 0, NULL})
static void    tBufferInit(SBuffer *buffer);
static void    tBufferDestroy(SBuffer *buffer);
static void    tBufferClear(SBuffer *buffer);
static int32_t tBufferEnsureCapacity(SBuffer *buffer, uint32_t capacity);
static int32_t tBufferPut(SBuffer *buffer, const void *data, uint32_t size);
static int32_t tBufferPutAt(SBuffer *buffer, uint32_t offset, const void *data, uint32_t size);
static int32_t tBufferPutI8(SBuffer *buffer, int8_t value);
static int32_t tBufferPutI16(SBuffer *buffer, int16_t value);
static int32_t tBufferPutI32(SBuffer *buffer, int32_t value);
static int32_t tBufferPutI64(SBuffer *buffer, int64_t value);
static int32_t tBufferPutU8(SBuffer *buffer, uint8_t value);
static int32_t tBufferPutU16(SBuffer *buffer, uint16_t value);
static int32_t tBufferPutU32(SBuffer *buffer, uint32_t value);
static int32_t tBufferPutU64(SBuffer *buffer, uint64_t value);
static int32_t tBufferPutI16v(SBuffer *buffer, int16_t value);
static int32_t tBufferPutI32v(SBuffer *buffer, int32_t value);
static int32_t tBufferPutI64v(SBuffer *buffer, int64_t value);
static int32_t tBufferPutU16v(SBuffer *buffer, uint16_t value);
static int32_t tBufferPutU32v(SBuffer *buffer, uint32_t value);
static int32_t tBufferPutU64v(SBuffer *buffer, uint64_t value);
static int32_t tBufferPutBinary(SBuffer *buffer, const void *data, uint32_t size);
static int32_t tBufferPutCStr(SBuffer *buffer, const char *str);
static int32_t tBufferPutF32(SBuffer *buffer, float value);
static int32_t tBufferPutF64(SBuffer *buffer, double value);

#define tBufferGetSize(buffer)           ((buffer)->size)
#define tBufferGetCapacity(buffer)       ((buffer)->capacity)
#define tBufferGetData(buffer)           ((buffer)->data)
#define tBufferGetDataAt(buffer, offset) ((char *)(buffer)->data + (offset))
#define tBufferGetDataEnd(buffer)        ((char *)(buffer)->data + (buffer)->size)

// SBufferReader
#define BUFFER_READER_INITIALIZER(offset, buffer) ((SBufferReader){offset, buffer})
#define BR_PTR(br)                                tBufferGetDataAt((br)->buffer, (br)->offset)
#define tBufferReaderDestroy(reader)              ((void)0)
#define tBufferReaderGetOffset(reader)            ((reader)->offset)
static int32_t tBufferGet(SBufferReader *reader, uint32_t size, void *data);
static int32_t tBufferReaderInit(SBufferReader *reader, uint32_t offset, SBuffer *buffer);
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