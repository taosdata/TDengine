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

#ifndef INC_BCK_TYPES_H_
#define INC_BCK_TYPES_H_

/*
 * Shared enum types used across multiple taosBackup headers.
 * Placed in a separate header to avoid forward-declaration conflicts
 * when compiled as C++ (unscoped enums cannot be forward-declared
 * without an explicit underlying type in C++).
 */

typedef enum BackFileType {
    // dir
    BACK_DIR_DB      = 0,
    BACK_DIR_TAG     = 1,
    BACK_DIR_DATA    = 2,
    // dir + file
    BACK_FILE_DBSQL  = 3,
    BACK_FILE_STBSQL = 4,
    BACK_FILE_STBCSV = 5,
    BACK_FILE_TAG    = 6,
    BACK_FILE_DATA   = 7,
    BACK_FILE_NTBSQL = 8,
    BACK_DIR_NTBDATA = 9,
    BACK_FILE_VTBSQL = 10,
    BACK_DIR_VTAG     = 11,   // {outdir}/{db}/vtags/
    BACK_FILE_VTAG    = 12    // {outdir}/{db}/vtags/{vstbName}_data{N}.{ext}
} BackFileType;

typedef enum StorageFormat {
    BINARY_TAOS    = 0,
    BINARY_PARQUET = 1,
} StorageFormat;

#endif  // INC_BCK_TYPES_H_
