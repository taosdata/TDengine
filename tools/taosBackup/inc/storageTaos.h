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

#ifndef INC_STORAGE_TAOS_H_
#define INC_STORAGE_TAOS_H_

#include "bck.h"

//
// ---------------- define ----------------
//

//
// ---------------- struct ----------------
//

typedef struct {
    char magic[4];
    uint16_t version;
    uint32_t nBlocks;
    uint32_t numRows;
    uint32_t numFields;
    char reserved[32];
    // schema
    uint32_t schemaLen;
    char schema[];
} TaosFileHeader;

typedef struct {
    char *fileName;
    FILE *fp;
    TaosFileHeader header;
} TaosFile;

// header 64 bytes



// ---------------- interface ----------------
int resultToFileTaos(TAOS_RES *res, const char *fileName);

#endif  // INC_STORAGE_TAOS_H_
