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

#ifndef COMPAT_AVRO_SQL_BUF_H_
#define COMPAT_AVRO_SQL_BUF_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

// Free space kept at the end of a caller-provided SQL buffer so that a trailing
// ',' written by the tag-value helpers can always be replaced with the closing
// ')' and a NUL terminator.
#define AVRO_SQL_TAIL_RESERVE 32

// Append an Avro tag value as a quoted SQL literal to `sql` (capacity `cap`
// bytes) at `pos`.
//
// `len` is the number of value bytes to emit.  The value and its length come
// from an untrusted dump file, so `val` is NOT required to be NUL-terminated and
// the remaining capacity is compared with the worst-case encoded size
// (2 * len + quotes + comma) before anything is written: an oversized tag value
// is rejected instead of overflowing the buffer.
//
// Returns the new position, or -1 when the value does not fit (or on invalid
// arguments).
static inline int32_t avroSqlAppendTagValue(char *sql, int32_t cap, int32_t pos,
                                            const char *val, size_t len) {
    if (!sql || cap <= 0 || pos < 0) return -1;

    if (!val) {
        if ((int64_t)pos + 5 > (int64_t)cap - AVRO_SQL_TAIL_RESERVE) return -1;
        memcpy(sql + pos, "NULL,", 5);
        return pos + 5;
    }

    // Worst case is one escaping quote per input byte.
    if ((int64_t)pos + (int64_t)len * 2 + 4 > (int64_t)cap - AVRO_SQL_TAIL_RESERVE) {
        return -1;
    }

    bool hasSingle = false, hasDouble = false;
    for (size_t i = 0; i < len; i++) {
        if (val[i] == '\'') hasSingle = true;
        if (val[i] == '"')  hasDouble = true;
    }

    int32_t p = pos;
    if (!hasSingle) {
        sql[p++] = '\'';
        memcpy(sql + p, val, len);
        p += (int32_t)len;
        sql[p++] = '\'';
        sql[p++] = ',';
    } else if (!hasDouble) {
        sql[p++] = '"';
        memcpy(sql + p, val, len);
        p += (int32_t)len;
        sql[p++] = '"';
        sql[p++] = ',';
    } else {
        // Escape single quotes by doubling
        sql[p++] = '\'';
        for (size_t i = 0; i < len; i++) {
            sql[p++] = val[i];
            if (val[i] == '\'') sql[p++] = '\'';
        }
        sql[p++] = '\'';
        sql[p++] = ',';
    }
    sql[p] = '\0';
    return p;
}

// Size of the buffer needed by avroSqlBuildInsertStmt() below.  `nCols` is the
// number of bind columns, which comes from the (untrusted) dump schema.
static inline size_t avroSqlInsertStmtSize(const char *db, const char *tb,
                                           const char *partCols, int32_t nCols) {
    size_t n = (db ? strlen(db) : 0) + (tb ? strlen(tb) : 0)
        + (partCols ? strlen(partCols) : 0) + 64;
    if (nCols > 0) n += 2 * (size_t)nCols;
    return n;
}

// Build "INSERT INTO `<db>`.`<tb>` <partCols> VALUES(?,?,...)" into `buf`
// (capacity `cap`).
//
// The number of placeholders is driven by the dump file, so the statement is
// assembled with explicit bounds checks and truncated/failed builds are
// reported instead of overflowing the buffer.  Returns the number of bytes
// written (excluding the NUL terminator), or -1 when the statement does not fit
// or the arguments are invalid.  `buf` is left NUL-terminated on success.
static inline int32_t avroSqlBuildInsertStmt(char *buf, size_t cap, const char *db,
                                             const char *tb, const char *partCols,
                                             int32_t nCols) {
    if (!buf || cap == 0 || nCols <= 0) return -1;
    if (!db) db = "";
    if (!tb) tb = "";
    if (!partCols) partCols = "";

    int n = snprintf(buf, cap, "INSERT INTO `%s`.`%s` %s VALUES(?", db, tb, partCols);
    if (n < 0 || (size_t)n >= cap) return -1;

    size_t pos = (size_t)n;
    for (int32_t i = 1; i < nCols; i++) {
        if (pos + 3 > cap) return -1;
        buf[pos++] = ',';
        buf[pos++] = '?';
    }

    buf[pos++] = ')';
    buf[pos] = '\0';
    return (int32_t)pos;
}

#endif  // COMPAT_AVRO_SQL_BUF_H_
