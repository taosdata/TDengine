/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_BACKUPDATA_H_
#define INC_BACKUPDATA_H_

#include "backup.h"

//
// ---------------- define ----------------
//


//
// ---------------- struct ----------------
//

// data
typedef struct {
    DBInfo*   dbInfo;
    StbInfo*  stbInfo;
    int       limit;
    int       offset;
    int       index;
    TAOS*     conn;
    TdThread  pid;
    int32_t   code;    // first error code from this thread
    // Thread-level reusable buffers (space-for-time optimization)
    char*     writeBuf;     // 4MB write buffer, reused across all CTBs
    int32_t   writeBufCap;
    char      stbDirPath[4096];  // pre-built directory path for this stb
    bool      stbDirCreated;
} DataThread;


//
// ---------------- interface ----------------
//
int backDatabaseData(DBInfo *dbInfo);

#endif  // INC_BACKUPDATA_H_
