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

//
// ---------------- struct ----------------
//
typedef struct {
    void * data;
    int    len;
} CompressData;



// ---------------- interface ----------------
CompressData* compressBlock(void *block, int blockRows, TAOS_FIELD* fields, int numFields, int *code);

void freeCompressData(CompressData* compressData);

#endif  // INC_COLCOMPRESS_H_
