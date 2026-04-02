/*
 * MIT License
 *
 * Copyright (c) 2022-2023 freemine <freemine@yeah.net>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _tsdb_h_
#define _tsdb_h_

#include "macros.h"
#include "typedefs.h"

#include <taos.h>

EXTERN_C_BEGIN

int tsdb_timestamp_to_string(int64_t val, int time_precision, char *buf, size_t len) FA_HIDDEN;
int tsdb_timestamp_to_SQL_C_TYPE_TIMESTAMP(int64_t val, int time_precision, SQL_TIMESTAMP_STRUCT *ts) FA_HIDDEN;

void tsdb_stmt_reset(tsdb_stmt_t *stmt) FA_HIDDEN;
void tsdb_stmt_release(tsdb_stmt_t *stmt) FA_HIDDEN;
void tsdb_paramset_reset(tsdb_paramset_t *paramset) FA_HIDDEN;
void tsdb_paramset_release(tsdb_paramset_t *paramset) FA_HIDDEN;
void tsdb_binds_release(tsdb_binds_t *tsdb_binds) FA_HIDDEN;
void tsdb_res_reset(tsdb_res_t *res) FA_HIDDEN;
void tsdb_res_release(tsdb_res_t *res) FA_HIDDEN;

void tsdb_stmt_init(tsdb_stmt_t *stmt, stmt_t *owner) FA_HIDDEN;
void tsdb_stmt_unprepare(tsdb_stmt_t *stmt) FA_HIDDEN;

SQLRETURN tsdb_stmt_query(tsdb_stmt_t *stmt, const sqlc_tsdb_t *sqlc_tsdb) FA_HIDDEN;
SQLRETURN tsdb_stmt_rebind_subtbl(tsdb_stmt_t *stmt) FA_HIDDEN;

EXTERN_C_END

#endif //  _tsdb_h_

