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

//
// ---------------- define ----------------
//
#define TAOSFILE_MAGIC   "TAOS"
#define TAOSBODY_MAGIC   "BLOC"
#define TAOSFILE_VERSION 1


//
// ---------------- struct ----------------
//

// body block 20 bytes
typedef struct {
    char magic[4];
    uint32_t dataLen;
    uint32_t numRows;
    uint16_t numCols;
    char  encode;
    char  compress;
    char reserved[4];
    char data[];
} CompressBlock;


// ---------------- interface ----------------

// create
CompressBlock* compressBlock(void *block, int blockRows, TAOS_FIELD* fields, int numFields, int *code);

// free
void freeCompressData(CompressBlock* compressBlock);

#endif  // INC_COLCOMPRESS_H_
