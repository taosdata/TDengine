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
#include "colCompress.h"

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

#define TAOS_FILE_WRITE_BUF_SIZE (4 * 1024 * 1024)  // 4MB write buffer

typedef struct {
    const char *fileName;
    TdFilePtr fp;
    // write buffer for batched I/O
    char    *writeBuf;
    int32_t  writeBufPos;
    int32_t  writeBufCap;
    int64_t  fileSize;   // populated by openTaosFileForRead; avoids later stat() call
    TaosFileHeader header;  // must be last (flexible array member)
} TaosFile;

// header 64 bytes


// ---------------- read structs ----------------

// callback for processing each decompressed block
// return TSDB_CODE_SUCCESS to continue, other to stop
typedef int (*BlockCallback)(void *userData, 
                             FieldInfo *fieldInfos, 
                             int numFields,
                             void *blockData, 
                             int32_t blockLen, 
                             int32_t blockRows);


// ---------------- interface ----------------
int resultToFileTaos(TAOS_RES *res, const char *fileName, int64_t *outRows);

// open a .dat file for reading, returns header info
TaosFile* openTaosFileForRead(const char *fileName, int *code);

// read all blocks from a .dat file, calling callback for each decompressed block
int readTaosFileBlocks(TaosFile *taosFile, BlockCallback callback, void *userData);

// close a read-mode TaosFile (no header update)
int closeTaosFileRead(TaosFile *taosFile);

// read .dat file and write raw blocks back to TDengine
int restoreFileToTaos(TAOS *conn, const char *fileName, const char *dbName, const char *tbName);

#endif  // INC_STORAGE_TAOS_H_
