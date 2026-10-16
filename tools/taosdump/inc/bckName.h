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

#ifndef INC_BCK_NAME_H_
#define INC_BCK_NAME_H_

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Percent-encode name components containing filename chars illegal on any
// platform (Windows is the most restrictive). Safe names pass through
// unchanged, so on-disk layout stays identical to historical backups.

// True if @name contains any byte/pattern that must be percent-encoded.
bool bckNameNeedsEncode(const char *name);

// Encode into @out; returns @name itself (zero copy) when no encoding is needed, NULL on error.
const char *bckNameEncode(const char *name, char *out, int outCap);

// Decode into @out; returns @name itself (zero copy) when it has no '%', NULL on error.
const char *bckNameDecode(const char *name, char *out, int outCap);

// In-place bckNameDecode (decode never grows the string); returns @name, NULL on overflow.
char *bckNameDecodeInPlace(char *name, int cap);

// Worst-case encoded name/stem size: table names are capped at ~192 bytes and
// each special char expands to "%XX" (3 bytes), so 1024 bytes is a safe bound
// for any encoded name or file stem used by the backup format.
#define BCK_NAME_ENC_STEM_MAX 1024

// Decode the percent-encoded table-name stem embedded in a backup data file
// basename ("{encodedTb}.dat" / ".par" / ".csv") into @out (capacity @outCap,
// e.g. sizeof a TSDB_TABLE_NAME_LEN buffer).  The encoded stem can be up to
// 3x longer than the decoded name (each special char becomes "%XX"), so it is
// decoded from an internal worst-case buffer before the result is copied to
// @out.  Returns 0 on success, or -1 on error (NULL args, bad capacity, or an
// encoded stem that overflows the internal buffer).
int bckNameDecodeFileStem(const char *baseName, char *out, int outCap);

// Escape @name for embedding inside a single-quoted SQL string literal
// (backslash and single quote are escaped with a leading backslash).
const char *bckSqlEscape(const char *name, char *out, int outCap);

#ifdef __cplusplus
}
#endif

#endif  // INC_BCK_NAME_H_
