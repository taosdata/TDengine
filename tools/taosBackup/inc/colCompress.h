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
#define TAOSFILE_VERSION 1

// compressBlock->flag
#define BLOCK_FLAG_NOT_COMPRESS 0x00000001

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

// compress
CompressBlock* compressBlock(void *block, int blockRows, FieldInfo* fieldInfos, int numFields, SBuffer *assist, int *code);

// decompress
int decompressBlock(CompressBlock* compressBlock,
                    FieldInfo*     fieldInfos,
                    int            numFields,
                    void**         uncompressBlock,
                    int32_t*       uncompressLen,
                    SBuffer*       assist);

// free
void freeCompressData(CompressBlock* compressBlock);

// fill fields info
void fillFieldsInfo(FieldInfo* fieldInfos, TAOS_FIELD* fields, int numFields);

#endif  // INC_COLCOMPRESS_H
