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

#ifndef INC_RESTOREDATA_H_
#define INC_RESTOREDATA_H_

#include "bck.h"
#include "restore.h"

// forward declaration
struct StbChange;

//
// ---------------- struct ----------------
//

// restore data thread context
typedef struct {
    DBInfo*   dbInfo;
    StbInfo*  stbInfo;
    int       index;       // thread index (1-based)
    TAOS*     conn;
    TdThread  pid;
    // file list assigned to this thread
    char    **files;       // array of file paths (NULL-terminated)
    int       fileCnt;     // number of files
    int       code;        // first error code from this thread
    // schema change support
    struct StbChange *stbChange;  // shared StbChange for this STB (NULL if no change)
} RestoreDataThread;


//
// ---------------- interface ----------------
//

// restore database data: read .dat files and write via STMT
int restoreDatabaseData(const char *dbName);

#endif  // INC_RESTOREDATA_H_
