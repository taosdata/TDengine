/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_TMYSQLTIME_H_
#define TDENGINE_TMYSQLTIME_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Formatting buffers reserve one extra byte for the terminating NUL. */
enum {
  MYSQL_TIME_MAX_TEXT_BYTES = 4095,
  // %r expands two format bytes to eleven result bytes.
  MYSQL_TIME_MAX_FORMAT_RESULT_BYTES = (MYSQL_TIME_MAX_TEXT_BYTES / 2) * 11 + MYSQL_TIME_MAX_TEXT_BYTES % 2,
  MYSQL_TIME_FORMAT_NULL = -2,
};

typedef struct SMySqlCivilTime {
  int32_t year;
  int32_t month;
  int32_t day;
  int32_t hour;
  int32_t minute;
  int32_t second;
  int32_t microsecond;
  bool    negativeTime;
  bool    hasDate;
  bool    hasTime;
} SMySqlCivilTime;

typedef enum EMySqlTimeParseResult {
  MYSQL_TIME_PARSE_OK = 0,
  MYSQL_TIME_PARSE_INVALID = 1,
  MYSQL_TIME_PARSE_INVALID_COMPLETE_DATE = 2,
} EMySqlTimeParseResult;

EMySqlTimeParseResult tMysqlParseCivil(const char* text, int32_t len, bool allowTimeOnly, SMySqlCivilTime* result);
EMySqlTimeParseResult tMysqlParseCivilFormat(const char* text, int32_t len, const char* format, int32_t formatLen,
                                             SMySqlCivilTime* result);
int64_t               tMysqlDayNumber(int32_t year, int32_t month, int32_t day);
bool                  tMysqlFromDayNumber(int64_t dayNumber, SMySqlCivilTime* result);
// Zero month/day fields are allowed; callers enforce any stricter date policy.
bool    tMysqlWeekYearAndNumber(const SMySqlCivilTime* civil, int32_t mode, int32_t* weekYear, int32_t* weekNumber);
int32_t tMysqlFormatCivil(const SMySqlCivilTime* civil, const char* format, int32_t formatLen, char* output,
                          int32_t capacity);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TMYSQLTIME_H_
