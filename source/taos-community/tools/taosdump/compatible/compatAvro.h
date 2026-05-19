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

#ifndef COMPAT_AVRO_H_
#define COMPAT_AVRO_H_

#include "compatAvroSchema.h"
#include "compatAvroUtil.h"
#include <taos.h>

// ---- Restore context (replaces taosdump globals) ----
typedef struct {
    const char   *dbPath;         // backup root directory
    const char   *targetDb;       // target database name (after rename)
    TAOS         *conn;           // connection for meta operations
    char          charset[64];
    bool          looseMode;
    bool          escapeChar;
    int           serverMajorVer;
    int           dataBatch;      // rows per STMT batch
    int           dataThreads;    // parallel threads for data import
    AvroDBChange *pDbChange;      // schema change tracker
} AvroRestoreCtx;

// ---- Meta import (compatAvroMeta.c) ----
int64_t avroRestoreTbTags(AvroRestoreCtx *ctx, const char *dirPath,
                          const char *fileName, AvroDBChange *pDbChange,
                          bool isVirtual);

int64_t avroRestoreNtb(AvroRestoreCtx *ctx, const char *dirPath,
                       const char *fileName, AvroDBChange *pDbChange,
                       bool isVirtual);

// ---- Data import (compatAvroData.c) ----
int64_t avroRestoreDataImpl(AvroRestoreCtx *ctx,
                            TAOS *conn,
                            const char *dirPath,
                            const char *fileName,
                            AvroDBChange *pDbChange,
                            AvroStbChange *stbChange);

// ---- SQL rename (compatAvro.c) ----
char *avroAfterRenameSql(AvroRestoreCtx *ctx, const char *sql);

#endif  // COMPAT_AVRO_H_
