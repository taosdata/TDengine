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

#ifndef INC_UTIL_H_
#define INC_UTIL_H_

#include "tdef.h"
#include "bckTypes.h"
#include "bckName.h"

//
// ---------------- define ----------------
//

// ---------------- interface ----------------
unsigned int getCrc(const char *name);

void sleepMs(int ms);

void freeArrayPtr(char **ptr);

void freePtr(void *ptr);

bool errorCodeCanRetry(int code);

// True for connection-level failures (server restart / network drop) where the
// pooled handle is stale and must be rebuilt; false for transient server-side
// errors that can be retried on the same connection.
bool bckConnLevelError(int code);

// taosdump error code to string
const char* bckErrStr(int code);

// engine + taosdump error code to string
const char* bckErrMsg(int code);

int obtainFileName(BackFileType fileType, 
                      const char *dbName, 
                      const char *stbName,
                      const char *tbName,
                      int index,
                      int64_t fileCount,
                      StorageFormat format,
                      char *fileName, 
                      int len);

#endif  // INC_UTIL_H_
