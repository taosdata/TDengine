/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */

#ifndef INC_COLCOMPRESS_H_
#define INC_COLCOMPRESS_H_

#include "bck.h"
#include "blockReader.h"
#include "tbuffer.h"

//
// ---------------- define ----------------
//
#define TAOSFILE_MAGIC   "TAOS"
#define TAOSBODY_MAGIC   "BLOC"
#define TAOSFILE_VERSION 2

// compressBlock->flag
#define BLOCK_FLAG_NOT_COMPRESS 0x00000001

// Special sentinel stored in colsLen[i] (block version >= 2):
// when a column is entirely NULL, we skip storing any column data and
// record this magic value so the reader can synthesise the all-NULL bitmap.
#define COL_LEN_ALL_NULL  (-1)

//
// ---------------- struct ----------------
//

typedef struct {
    char   name[65];
    int8_t type;
    int32_t bytes;
    int8_t encode;
    int8_t compress;
    int8_t level;
} FieldInfo;

// body block 20 bytes
typedef struct {
    // header
    char version;
    uint32_t flag;
    oriBlockHeader oriHeader;

    uint32_t dataLen; // columns data length
    char data[]; // two parts: cols lens + cols data
} CompressBlock;


// ---------------- interface ----------------

// compress block into caller-provided buffer (reused across blocks)
CompressBlock* compressBlock(void *block, int blockRows, FieldInfo* fieldInfos, int numFields, SBuffer *assist,
                             char **bufPtr, int32_t *bufCapPtr, int *code);

// decompress
int decompressBlock(CompressBlock* compressBlock,
                    FieldInfo*     fieldInfos,
                    int            numFields,
                    void**         uncompressBlock,
                    int32_t*       uncompressLen,
                    SBuffer*       assist);

// free
void freeCompressData(CompressBlock* compressBlock);

// fill fields info from TAOS_FIELD_E (captures precision/scale for DECIMAL types)
// For DECIMAL/DECIMAL64, FieldInfo.bytes is packed: (actualBytes<<24)|(precision<<8)|scale
// For all other types, FieldInfo.bytes is the raw declared byte width.
void fillFieldsInfo(FieldInfo* fieldInfos, TAOS_FIELD_E* fields, int numFields);

// Helpers to decode DECIMAL packed bytes (no dependency on IS_DECIMAL_TYPE / tDataTypes).
// For DECIMAL/DECIMAL64, FieldInfo.bytes is packed: (actualBytes<<24)|(precision<<8)|scale
//   → top byte (bits 24-31) is the raw byte width (8 or 16), non-zero iff new format.
// For all other types (and legacy DECIMAL files), top byte is 0 and fi->bytes is raw width.
static inline int32_t fieldGetRawBytes(const FieldInfo* fi) {
    int32_t hi = (fi->bytes >> 24) & 0xFF;
    return hi != 0 ? hi : fi->bytes;
}
static inline uint8_t fieldGetPrecision(const FieldInfo* fi) {
    return ((fi->bytes >> 24) & 0xFF) != 0 ? (uint8_t)((fi->bytes >> 8) & 0xFF) : 0;
}
static inline uint8_t fieldGetScale(const FieldInfo* fi) {
    return ((fi->bytes >> 24) & 0xFF) != 0 ? (uint8_t)(fi->bytes & 0xFF) : 0;
}

#endif  // INC_COLCOMPRESS_H
