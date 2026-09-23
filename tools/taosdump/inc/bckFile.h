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
// progressCtr: if non-NULL, atomically incremented by row-count as blocks are fetched
int queryWriteBinary(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile,
                     int64_t *outRows, volatile int64_t *progressCtr);

// query result write to file with caller-provided write buffer (thread-level reuse)
int queryWriteBinaryEx(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile,
                       char *writeBuf, int32_t writeBufCap, int64_t *outRows, volatile int64_t *progressCtr);

#endif  // INC_BCKFILE_H_
