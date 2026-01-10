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

#include <stdbool.h>

//
// ---------------- define ----------------
//

// ---------------- interface ----------------
int queryWriteJson(const char *sql, const char *pathFile, char ** selectTags);

int queryWriteTxt(const char *sql, const char *pathFile);

// query result write to file with columnar storage
int queryWriteBinary(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile);

#endif  // INC_BCKFILE_H_
