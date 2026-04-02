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

#ifndef INC_BCKFILE_H_
#define INC_BCKFILE_H_

#include "tdef.h"

//
// ---------------- define ----------------
//

// ---------------- interface ----------------
int queryWriteCsv(const char *sql, const char *pathFile, char ** selectTags);

int queryWriteTxt(const char *sql, int32_t col, const char *pathFile);

// query result write to file with columnar storage
// outRows: if not NULL, receives the number of rows written
int queryWriteBinary(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile, int64_t *outRows);

// query result write to file with caller-provided write buffer (thread-level reuse)
int queryWriteBinaryEx(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile,
                       char *writeBuf, int32_t writeBufCap, int64_t *outRows);

#endif  // INC_BCKFILE_H_
