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

#include <float.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef WINDOWS
#include <strings.h>
#endif

#include "decimal.h"
#include "functionMgt.h"
#include "osDef.h"
#include "osTime.h"
#include "scalar.h"
#include "sclInt.h"
#include "tdatablock.h"
#include "tmysqltime.h"
#include "ttime.h"

#define MYSQL_TEXT_CAP (MYSQL_TIME_MAX_TEXT_BYTES + 1)
#define MYSQL_VAR_CAP  (MYSQL_TIME_MAX_TEXT_BYTES + VARSTR_HEADER_SIZE)

typedef struct {
  char*   formatBuffer;
  int32_t formatCapacity;
  char*   textBuffer;
  int32_t textCapacity;
  char*   ncharBuffer;
  int32_t ncharCapacity;
} SMySqlTimeScratch;

typedef struct {
  bool    parsed;
  int64_t ticks;
} SMySqlOffsetInstant;

static int32_t mysqlGrowBuffer(char** buffer, int32_t* capacity, int32_t required) {
  if (*capacity >= required) return TSDB_CODE_SUCCESS;
  char* resized = *buffer ? taosMemoryRealloc(*buffer, (size_t)required) : taosMemoryMalloc((size_t)required);
  if (resized == NULL) return terrno != TSDB_CODE_SUCCESS ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  *buffer = resized;
  *capacity = required;
  return TSDB_CODE_SUCCESS;
}

static int32_t mysqlRow(const SScalarParam* param, int32_t row) { return param->numOfRows == 1 ? 0 : row; }

static int64_t mysqlInteger(const SScalarParam* param, int32_t row) {
  int64_t value = 0;
  GET_TYPED_DATA(value, int64_t, GET_PARAM_TYPE(param), colDataGetData(param->columnData, mysqlRow(param, row)),
                 typeGetTypeModFromColInfo(&param->columnData->info));
  return value;
}

static bool mysqlFromDaysNumber(const SScalarParam* param, int32_t row, int64_t* dayNumber) {
  char*  data = colDataGetData(param->columnData, mysqlRow(param, row));
  int8_t type = GET_PARAM_TYPE(param);
  double numeric = 0;
  GET_TYPED_DATA(numeric, double, type, data,
                 typeGetTypeModFromColInfo(&param->columnData->info));
  int64_t maxDay = tMysqlDayNumber(9999, 12, 31);
  bool    floating = type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE;
  if (!isfinite(numeric)) return false;
  double rounded = floating ? rint(numeric) : numeric;
  if (rounded >= (double)maxDay + 1.0) return false;
  if (rounded < 0) {
    *dayNumber = 0;
  } else {
    *dayNumber = floating ? (int64_t)rounded : mysqlInteger(param, row);
  }
  return *dayNumber <= maxDay;
}

static bool mysqlText(const SScalarParam* param, int32_t row, char* text, int32_t capacity, int32_t* length,
                      SMySqlTimeScratch* scratch, int32_t* code) {
  char*   data = colDataGetData(param->columnData, mysqlRow(param, row));
  int32_t bytes = varDataLen(data);
  if (GET_PARAM_TYPE(param) == TSDB_DATA_TYPE_NCHAR) {
    char* converted = text;
    if (bytes >= capacity) {
      *code = mysqlGrowBuffer(&scratch->ncharBuffer, &scratch->ncharCapacity, bytes);
      if (*code != TSDB_CODE_SUCCESS) return false;
      converted = scratch->ncharBuffer;
    }
    int32_t convertedBytes = taosUcs4ToMbs((TdUcs4*)varDataVal(data), bytes, converted, param->charsetCxt);
    if (convertedBytes == TSDB_CODE_OUT_OF_MEMORY) *code = convertedBytes;
    if (convertedBytes >= 0 && convertedBytes < capacity && converted != text) {
      memcpy(text, converted, (size_t)convertedBytes);
    }
    bytes = convertedBytes;
    if (bytes < 0 || bytes >= capacity) return false;
  } else {
    if (bytes >= capacity) return false;
    memcpy(text, varDataVal(data), (size_t)bytes);
  }
  text[bytes] = '\0';
  *length = bytes;
  return true;
}

static bool mysqlInputText(const SScalarParam* param, int32_t row, const char** text, int32_t* length,
                          SMySqlTimeScratch* scratch, int32_t* code) {
  char* data = colDataGetData(param->columnData, mysqlRow(param, row));
  *length = varDataLen(data);
  *text = varDataVal(data);
  // Date input is length-delimited and is not subject to the format-string limit.
  // Keep NCHAR conversion independent of the format buffer.
  if (GET_PARAM_TYPE(param) == TSDB_DATA_TYPE_NCHAR) {
    *code = mysqlGrowBuffer(&scratch->textBuffer, &scratch->textCapacity, *length + 1);
    if (*code != TSDB_CODE_SUCCESS) return false;
    if (!mysqlText(param, row, scratch->textBuffer, scratch->textCapacity, length, scratch, code)) return false;
    *text = scratch->textBuffer;
  }
  return true;
}

static bool mysqlFromTicks(int64_t ticks, int32_t precision, timezone_t zone, SMySqlCivilTime* civil) {
  if (precision < TSDB_TIME_PRECISION_MILLI || precision > TSDB_TIME_PRECISION_NANO) return false;
  int64_t ticksPerSecond = TICK_PER_SECOND[precision];
  int64_t seconds = ticks / ticksPerSecond;
  int64_t fraction = ticks % ticksPerSecond;
  if (fraction < 0) {
    --seconds;
    fraction += ticksPerSecond;
  }
  time_t epochSeconds = (time_t)seconds;
  struct tm converted = {0};
  if (taosLocalTime(&epochSeconds, &converted, NULL, 0, zone) == NULL) return false;
  if (converted.tm_year < -1900 || converted.tm_year > 8099) return false;
  *civil = (SMySqlCivilTime){.year = converted.tm_year + 1900,
                             .month = converted.tm_mon + 1,
                             .day = converted.tm_mday,
                             .hour = converted.tm_hour,
                             .minute = converted.tm_min,
                             .second = converted.tm_sec,
                             .microsecond = (int32_t)(fraction * (1000000000LL / ticksPerSecond) / 1000),
                             .hasDate = true,
                             .hasTime = true};
  return true;
}

static bool mysqlWallSeconds(const SMySqlCivilTime* civil, int64_t* seconds) {
  int64_t day = tMysqlDayNumber(civil->year, civil->month, civil->day);
  if (day < 0) return false;
  *seconds = (day - tMysqlDayNumber(1970, 1, 1)) * 86400 + civil->hour * 3600 + civil->minute * 60 + civil->second;
  return true;
}

static bool mysqlLocalWallSeconds(int64_t seconds, timezone_t zone, int64_t* wallSeconds) {
  SMySqlCivilTime local = {0};
  return mysqlFromTicks(seconds * 1000, TSDB_TIME_PRECISION_MILLI, zone, &local) &&
         mysqlWallSeconds(&local, wallSeconds);
}

static bool mysqlToTicks(const SMySqlCivilTime* civil, int32_t precision, timezone_t zone, int64_t* ticks) {
  int64_t wallSeconds = 0;
  if (!mysqlWallSeconds(civil, &wallSeconds)) return false;
  struct STm converted = {.tm = {.tm_year = civil->year - 1900,
                                 .tm_mon = civil->month - 1,
                                 .tm_mday = civil->day,
                                 .tm_hour = civil->hour,
                                 .tm_min = civil->minute,
                                 .tm_sec = civil->second,
                                 .tm_isdst = -1},
                          .fsec = (int64_t)civil->microsecond * 1000};
  int64_t actualWall = 0;
  if (taosTm2Ts(&converted, ticks, precision, zone) == TSDB_CODE_SUCCESS) {
    SMySqlCivilTime actual = {0};
    if (mysqlFromTicks(*ticks, precision, zone, &actual) && mysqlWallSeconds(&actual, &actualWall) &&
        actualWall == wallSeconds) return true;
  }

  // mktime may reject a spring gap or normalize it differently across platforms.
  // MySQL maps the whole missing interval to its first valid instant, retaining
  // microseconds. Two days on each side cover IANA offsets and skipped dates.
  int64_t low = wallSeconds - 2 * 86400;
  int64_t high = wallSeconds + 2 * 86400;
  int64_t lowWall = 0;
  int64_t highWall = 0;
  if (!mysqlLocalWallSeconds(low, zone, &lowWall) || !mysqlLocalWallSeconds(high, zone, &highWall) ||
      lowWall >= wallSeconds || highWall <= wallSeconds) return false;
  while (high - low > 1) {
    int64_t middle = low + (high - low) / 2;
    if (!mysqlLocalWallSeconds(middle, zone, &actualWall)) return false;
    if (actualWall < wallSeconds) {
      low = middle;
      lowWall = actualWall;
    } else {
      high = middle;
      highWall = actualWall;
    }
  }
  if (lowWall >= wallSeconds || highWall <= wallSeconds || highWall - lowWall <= 1) return false;
  *ticks = high * TICK_PER_SECOND[precision] +
           (int64_t)civil->microsecond * TICK_PER_SECOND[precision] / 1000000;
  return true;
}

static bool mysqlCivil(const SScalarParam* param, int32_t row, bool allowTimeOnly, SMySqlCivilTime* civil,
                       SMySqlTimeScratch* scratch, int32_t* code, SMySqlOffsetInstant* offsetInstant) {
  if (GET_PARAM_TYPE(param) == TSDB_DATA_TYPE_TIMESTAMP) {
    int64_t ticks = mysqlInteger(param, row);
    return mysqlFromTicks(ticks, param->columnData->info.precision, param->tz, civil);
  }
  const char* text = NULL;
  int32_t     length = 0;
  if (!mysqlInputText(param, row, &text, &length, scratch, code)) return false;
  EMySqlTimeParseResult status = tMysqlParseCivil(text, length, allowTimeOnly, civil);
  if (status == MYSQL_TIME_PARSE_OK) return true;
  if (status == MYSQL_TIME_PARSE_INVALID_COMPLETE_DATE || length < 11) return false;
  int32_t offsetPos = text[length - 1] == 'Z' || text[length - 1] == 'z' ? length - 1 : -1;
  if (offsetPos < 0) {
    for (int32_t i = length - 1; i >= 10 && i >= length - 7; --i) {
      if (text[i] == '+' || text[i] == '-') {
        offsetPos = i;
        break;
      }
    }
  }
  if (offsetPos < 0) return false;
  if (tMysqlParseCivil(text, offsetPos, false, civil) != MYSQL_TIME_PARSE_OK) return false;
  int32_t offsetLen = length - offsetPos;
  bool    utc = text[offsetPos] == 'Z' || text[offsetPos] == 'z';
  if ((utc ? offsetLen != 1 : (offsetLen != 3 && offsetLen != 5 && offsetLen != 6)) ||
      memchr(text + offsetPos, '\0', (size_t)offsetLen) != NULL) return false;
  char zoneText[7];
  memcpy(zoneText, text + offsetPos, (size_t)offsetLen);
  zoneText[offsetLen] = '\0';
  int64_t offset = 0;
  int64_t seconds = 0;
  if (offsetOfTimezone(zoneText, &offset) != TSDB_CODE_SUCCESS || !mysqlWallSeconds(civil, &seconds)) {
    return false;
  }
  // Preserve parsed two-digit years and valid epoch second -1. MySQL's year-zero
  // day numbers are one ahead of the native Gregorian calendar in January/February.
  if (civil->year == 0 && civil->month <= 2) seconds -= 86400;
  int64_t ticks = (seconds + offset) * 1000000 + civil->microsecond;
  if (!mysqlFromTicks(ticks, TSDB_TIME_PRECISION_MICRO, param->tz, civil)) return false;
  if (offsetInstant != NULL) {
    offsetInstant->parsed = true;
    offsetInstant->ticks = ticks;
  }
  return true;
}

static int32_t mysqlWriteText(SScalarParam* output, int32_t row, const char* text, int32_t length,
                              SMySqlTimeScratch* scratch) {
  if (length < 0 || length > MYSQL_TIME_MAX_FORMAT_RESULT_BYTES) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  char  smallData[MYSQL_VAR_CAP];
  char* data = smallData;
  if (length > MYSQL_TIME_MAX_TEXT_BYTES) {
    int32_t code = mysqlGrowBuffer(&scratch->textBuffer, &scratch->textCapacity, length + VARSTR_HEADER_SIZE);
    if (code != TSDB_CODE_SUCCESS) return code;
    data = scratch->textBuffer;
  }
  varDataSetLen(data, length);
  memcpy(varDataVal(data), text, (size_t)length);
  return colDataSetVal(output->columnData, row, data, false);
}

static int32_t mysqlWriteFormat(SScalarParam* output, int32_t row, const SMySqlCivilTime* civil, const char* format,
                                int32_t length, SMySqlTimeScratch* scratch) {
  if (length < 0 || length > MYSQL_TIME_MAX_TEXT_BYTES) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  if (length == 0) {
    colDataSetNULL(output->columnData, row);
    return TSDB_CODE_SUCCESS;
  }
  int32_t capacity = (length / 2) * 11 + length % 2 + 1;
  char    smallResult[MYSQL_TEXT_CAP];
  char*   result = smallResult;
  if (capacity > MYSQL_TEXT_CAP) {
    int32_t code = mysqlGrowBuffer(&scratch->formatBuffer, &scratch->formatCapacity, capacity);
    if (code != TSDB_CODE_SUCCESS) return code;
    result = scratch->formatBuffer;
  }
  int32_t size = tMysqlFormatCivil(civil, format, length, result, capacity);
  if (size == MYSQL_TIME_FORMAT_NULL) {
    colDataSetNULL(output->columnData, row);
    return TSDB_CODE_SUCCESS;
  }
  return size < 0 ? TSDB_CODE_FUNC_FUNTION_PARA_VALUE : mysqlWriteText(output, row, result, size, scratch);
}

static bool mysqlUnixFloatSeconds(double value, int64_t* microseconds) {
  if (!isfinite(value) || value < 0 || value >= 32536771200.0) return false;
  // Values this small round to zero; avoid the decimal parser's scale limit.
  if (value < 0.0000001) {
    *microseconds = 0;
    return true;
  }
  char    text[64];
  int32_t length = 0;
  // Keep a round-trip decimal without first rounding the binary value to six
  // places. Prefer fewer significant digits to avoid exposing binary noise.
  for (int32_t digits = DBL_DIG; digits <= DBL_DIG + 2; ++digits) {
    length = snprintf(text, sizeof(text), "%.*g", digits, value);
    if (taosStr2Double(text, NULL) == value) break;
  }
  Decimal128 rounded;
  if (decimal128FromStr(text, length, 17, 6, &rounded) != TSDB_CODE_SUCCESS) return false;
  *microseconds = (int64_t)DECIMAL128_LOW_WORD(&rounded);
  return *microseconds <= 32536771199999999LL;
}

static bool mysqlUnixSeconds(const SScalarParam* param, int32_t row, int64_t* microseconds, int32_t* fractionDigits,
                             bool* secondCarry) {
  *secondCarry = false;
  int8_t type = GET_PARAM_TYPE(param);
  if (IS_INTEGER_TYPE(type)) {
    int64_t seconds = mysqlInteger(param, row);
    if (seconds < 0 || seconds > 32536771199LL) return false;
    *microseconds = seconds * 1000000;
    *fractionDigits = 0;
    return true;
  }
  char  text[128] = {0};
  char* data = colDataGetData(param->columnData, mysqlRow(param, row));
  if (IS_DECIMAL_TYPE(type)) {
    int32_t code = decimalToStr((DecimalType*)data, type, param->columnData->info.precision,
                                param->columnData->info.scale, text, sizeof(text));
    if (code != TSDB_CODE_SUCCESS) return false;
    *fractionDigits = param->columnData->info.scale < 6 ? param->columnData->info.scale : 6;
  } else {
    double value = 0;
    GET_TYPED_DATA(value, double, type, data, typeGetTypeModFromColInfo(&param->columnData->info));
    *fractionDigits = 6;
    if (!mysqlUnixFloatSeconds(value, microseconds)) return false;
    *secondCarry = *microseconds / 1000000 > (int64_t)value;
    return true;
  }
  const char* cursor = text;
  if (*cursor == '+') ++cursor;
  if (*cursor < '0' || *cursor > '9') return false;
  int64_t seconds = 0;
  while (*cursor >= '0' && *cursor <= '9') {
    if (seconds > 32536771199LL / 10) return false;
    seconds = seconds * 10 + *cursor++ - '0';
    if (seconds > 32536771199LL) return false;
  }
  int64_t fraction = 0;
  int32_t digits = 0;
  bool    roundUp = false;
  if (*cursor == '.') {
    ++cursor;
    while (*cursor >= '0' && *cursor <= '9') {
      if (digits < 6)
        fraction = fraction * 10 + *cursor - '0';
      else if (digits == 6)
        roundUp = *cursor >= '5';
      ++digits;
      ++cursor;
    }
  }
  if (*cursor != '\0') return false;
  while (digits++ < 6) fraction *= 10;
  *microseconds = seconds * 1000000 + fraction + (roundUp ? 1 : 0);
  if (*microseconds > 32536771199999999LL) return false;
  *secondCarry = fraction == 999999 && roundUp;
  return true;
}

static bool mysqlCarryCivilSecond(SMySqlCivilTime* civil) {
  if (++civil->second < 60) return true;
  civil->second = 0;
  if (++civil->minute < 60) return true;
  civil->minute = 0;
  if (++civil->hour < 24) return true;
  civil->hour = 0;
  SMySqlCivilTime nextDay = {0};
  int64_t day = tMysqlDayNumber(civil->year, civil->month, civil->day);
  if (day < 0 || !tMysqlFromDayNumber(day + 1, &nextDay)) return false;
  civil->year = nextDay.year;
  civil->month = nextDay.month;
  civil->day = nextDay.day;
  return true;
}

static bool mysqlUnitValue(const char* unit, const SMySqlCivilTime* civil, int64_t* value) {
  int64_t day = civil->day;
  int64_t hour = civil->hour;
  int64_t minute = civil->minute;
  int64_t second = civil->second;
  int64_t micro = civil->microsecond;
  if (strcasecmp(unit, "YEAR") == 0)
    *value = civil->year;
  else if (strcasecmp(unit, "QUARTER") == 0)
    *value = (civil->month + 2) / 3;
  else if (strcasecmp(unit, "MONTH") == 0)
    *value = civil->month;
  else if (strcasecmp(unit, "DAY") == 0)
    *value = day;
  else if (strcasecmp(unit, "HOUR") == 0)
    *value = hour;
  else if (strcasecmp(unit, "MINUTE") == 0)
    *value = minute;
  else if (strcasecmp(unit, "SECOND") == 0)
    *value = second;
  else if (strcasecmp(unit, "MICROSECOND") == 0)
    *value = micro;
  else if (strcasecmp(unit, "YEAR_MONTH") == 0)
    *value = civil->year * 100 + civil->month;
  else if (strcasecmp(unit, "DAY_HOUR") == 0)
    *value = day * 100 + hour;
  else if (strcasecmp(unit, "DAY_MINUTE") == 0)
    *value = (day * 100 + hour) * 100 + minute;
  else if (strcasecmp(unit, "DAY_SECOND") == 0)
    *value = ((day * 100 + hour) * 100 + minute) * 100 + second;
  else if (strcasecmp(unit, "DAY_MICROSECOND") == 0)
    *value = (((day * 100 + hour) * 100 + minute) * 100 + second) * 1000000 + micro;
  else if (strcasecmp(unit, "HOUR_MINUTE") == 0)
    *value = hour * 100 + minute;
  else if (strcasecmp(unit, "HOUR_SECOND") == 0)
    *value = (hour * 100 + minute) * 100 + second;
  else if (strcasecmp(unit, "HOUR_MICROSECOND") == 0)
    *value = ((hour * 100 + minute) * 100 + second) * 1000000 + micro;
  else if (strcasecmp(unit, "MINUTE_SECOND") == 0)
    *value = minute * 100 + second;
  else if (strcasecmp(unit, "MINUTE_MICROSECOND") == 0)
    *value = (minute * 100 + second) * 1000000 + micro;
  else if (strcasecmp(unit, "SECOND_MICROSECOND") == 0)
    *value = second * 1000000 + micro;
  else if (strcasecmp(unit, "WEEK") == 0) {
    int32_t year = 0;
    int32_t week = 0;
    // Keep the all-zero date NULL, but allow nonzero dates with a zero month/day.
    if (civil->year == 0 && civil->month == 0 && civil->day == 0) return false;
    if (!tMysqlWeekYearAndNumber(civil, 0, &year, &week)) return false;
    *value = week;
  } else
    return false;
  if (civil->negativeTime) *value = -*value;
  return true;
}

static int32_t mysqlWriteDecimal(SScalarParam* output, int32_t row, int64_t microseconds) {
  Decimal128 value;
  // The caller bounds nonnegative microseconds; they are the DECIMAL(17,6) coefficient.
  makeDecimal128(&value, 0, (uint64_t)microseconds);
  return colDataSetVal(output->columnData, row, (char*)&value, false);
}

static int32_t mysqlWriteUnixTimestamp(SScalarParam* output, int32_t row, int64_t microseconds) {
  if (microseconds < 0 || microseconds > 32536771199999999LL) {
    colDataSetNULL(output->columnData, row);
    return TSDB_CODE_SUCCESS;
  }
  if (GET_PARAM_TYPE(output) == TSDB_DATA_TYPE_DECIMAL) return mysqlWriteDecimal(output, row, microseconds);
  int64_t seconds = microseconds / 1000000;
  return colDataSetVal(output->columnData, row, (char*)&seconds, false);
}

static int32_t mysqlTimeFunctionImpl(SScalarParam* input, int32_t inputNum, SScalarParam* output,
                                     SMySqlTimeScratch* scratch) {
  if (inputNum < 3) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  int32_t argumentCount = inputNum - 3;
  int32_t rows = 1;
  for (int32_t i = 0; i < argumentCount; ++i) {
    // Hidden one-row constants do not turn an empty user column into a result row.
    if (input[i].numOfRows == 0) {
      output->numOfRows = 0;
      return TSDB_CODE_SUCCESS;
    }
    if (input[i].numOfRows > rows) rows = input[i].numOfRows;
  }
  int32_t op = (int32_t)mysqlInteger(&input[inputNum - 2], 0) + FUNCTION_TYPE_MYSQL_CURDATE;
  int32_t precision = (int32_t)mysqlInteger(&input[inputNum - 3], 0);
  int64_t nowUs = mysqlInteger(&input[inputNum - 1], 0);
  for (int32_t row = 0; row < rows; ++row) {
    bool nullValue = false;
    for (int32_t i = 0; i < argumentCount; ++i) {
      if (colDataIsNull_s(input[i].columnData, mysqlRow(&input[i], row))) nullValue = true;
    }
    if (nullValue) {
      colDataSetNULL(output->columnData, row);
      continue;
    }
    SMySqlCivilTime civil = {0};
    SMySqlOffsetInstant offsetInstant = {0};
    int32_t         code = TSDB_CODE_SUCCESS;
    int64_t         value = 0;
    bool            valid = true;
    switch (op) {
      case FUNCTION_TYPE_MYSQL_CURDATE:
      case FUNCTION_TYPE_MYSQL_CURRENT_DATE:
      case FUNCTION_TYPE_MYSQL_CURTIME:
      case FUNCTION_TYPE_MYSQL_CURRENT_TIME:
      case FUNCTION_TYPE_MYSQL_CURRENT_TIMESTAMP:
      case FUNCTION_TYPE_MYSQL_UNIX_TIMESTAMP:
        if (argumentCount == 0 || op <= FUNCTION_TYPE_MYSQL_CURRENT_TIMESTAMP) {
          valid = mysqlFromTicks(nowUs, TSDB_TIME_PRECISION_MICRO, input[0].tz, &civil);
          if (!valid) break;
          if (op == FUNCTION_TYPE_MYSQL_CURDATE || op == FUNCTION_TYPE_MYSQL_CURRENT_DATE) {
            code = mysqlWriteFormat(output, row, &civil, "%Y-%m-%d", 8, scratch);
          } else if (op == FUNCTION_TYPE_MYSQL_CURTIME || op == FUNCTION_TYPE_MYSQL_CURRENT_TIME) {
            int32_t fsp = argumentCount ? (int32_t)mysqlInteger(&input[0], row) : 0;
            if (fsp < 0 || fsp > 6) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
            char    buffer[32];
            int32_t length = snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d", civil.hour, civil.minute, civil.second);
            if (fsp > 0) {
              char micro[8];
              snprintf(micro, sizeof(micro), "%06d", civil.microsecond);
              buffer[length++] = '.';
              memcpy(buffer + length, micro, (size_t)fsp);
              length += fsp;
            }
            code = mysqlWriteText(output, row, buffer, length, scratch);
          } else if (op == FUNCTION_TYPE_MYSQL_CURRENT_TIMESTAMP) {
            static const int64_t quantum[] = {1000000, 100000, 10000, 1000, 100, 10, 1};
            int32_t fsp = argumentCount ? (int32_t)mysqlInteger(&input[0], row) : 0;
            if (fsp < 0 || fsp > 6) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
            int64_t truncatedUs = nowUs - nowUs % quantum[fsp];
            int64_t ticks = convertTimePrecision(truncatedUs, TSDB_TIME_PRECISION_MICRO, precision);
            code = colDataSetVal(output->columnData, row, (char*)&ticks, false);
          } else {
            value = nowUs / 1000000;
            code = colDataSetVal(output->columnData, row, (char*)&value, false);
          }
          break;
        }
        if (GET_PARAM_TYPE(&input[0]) == TSDB_DATA_TYPE_TIMESTAMP) {
          int64_t rawTicks = mysqlInteger(&input[0], row);
          // Reject negative sub-microsecond values before precision conversion can truncate them to zero.
          if (rawTicks < 0) {
            valid = false;
            break;
          }
          int64_t micros = convertTimePrecision(rawTicks, input[0].columnData->info.precision,
                                                TSDB_TIME_PRECISION_MICRO);
          code = mysqlWriteUnixTimestamp(output, row, micros);
          break;
        }
        /* Text UNIX_TIMESTAMP arguments follow the general date path below. */
        goto mysql_date_path;
      case FUNCTION_TYPE_MYSQL_FROM_DAYS:
        valid = mysqlFromDaysNumber(&input[0], row, &value);
        if (!valid) break;
        if (value < 366) {
          civil = (SMySqlCivilTime){.hasDate = true};
          valid = true;
        } else {
          valid = tMysqlFromDayNumber(value, &civil);
        }
        if (valid) code = mysqlWriteFormat(output, row, &civil, "%Y-%m-%d", 8, scratch);
        break;
      case FUNCTION_TYPE_MYSQL_FROM_UNIXTIME: {
        int64_t microseconds = 0;
        int32_t digits = 0;
        bool    secondCarry = false;
        valid = mysqlUnixSeconds(&input[0], row, &microseconds, &digits, &secondCarry);
        if (!valid) break;
        // MySQL rounds after converting integer seconds to local time. Carrying in
        // UTC instead would cross a DST transition and select a different offset.
        int64_t localTicks = microseconds - (secondCarry ? 1000000 : 0);
        valid = mysqlFromTicks(localTicks, TSDB_TIME_PRECISION_MICRO, input[0].tz, &civil);
        if (valid && secondCarry) valid = mysqlCarryCivilSecond(&civil);
        if (!valid) break;
        if (argumentCount == 2) {
          char    format[MYSQL_TEXT_CAP];
          int32_t length = 0;
          valid = mysqlText(&input[1], row, format, sizeof(format), &length, scratch, &code);
          if (valid) code = mysqlWriteFormat(output, row, &civil, format, length, scratch);
        } else {
          char    result[64];
          int32_t length = snprintf(result, sizeof(result), "%04d-%02d-%02d %02d:%02d:%02d", civil.year, civil.month,
                                    civil.day, civil.hour, civil.minute, civil.second);
          if (digits > 0) {
            char micro[8];
            snprintf(micro, sizeof(micro), "%06d", civil.microsecond);
            result[length++] = '.';
            memcpy(result + length, micro, (size_t)digits);
            length += digits;
          }
          code = mysqlWriteText(output, row, result, length, scratch);
        }
        break;
      }
      case FUNCTION_TYPE_MYSQL_STR_TO_DATE: {
        const char* text = NULL;
        int32_t textLen = 0;
        char    format[MYSQL_TEXT_CAP];
        int32_t formatLen = 0;
        valid = mysqlInputText(&input[0], row, &text, &textLen, scratch, &code);
        if (!valid) break;
        valid = mysqlText(&input[1], row, format, sizeof(format), &formatLen, scratch, &code);
        if (!valid) break;
        EMySqlTimeParseResult status = tMysqlParseCivilFormat(text, textLen, format, formatLen, &civil);
        valid = status == MYSQL_TIME_PARSE_OK;
        if (!valid) break;
        bool hasFraction = false;
        for (int32_t i = 0; i + 1 < formatLen; ++i) {
          if (format[i] == '%') {
            hasFraction |= format[i + 1] == 'f';
            ++i;
          }
        }
        const char* outputFormat =
            civil.hasDate ? (civil.hasTime ? (hasFraction ? "%Y-%m-%d %H:%i:%s.%f" : "%Y-%m-%d %H:%i:%s") : "%Y-%m-%d")
                          : (hasFraction ? "%H:%i:%s.%f" : "%H:%i:%s");
        code = mysqlWriteFormat(output, row, &civil, outputFormat, (int32_t)strlen(outputFormat), scratch);
        break;
      }
      case FUNCTION_TYPE_MYSQL_EXTRACT: {
        char    unit[64];
        int32_t length = 0;
        valid = mysqlText(&input[0], row, unit, sizeof(unit), &length, scratch, &code);
        if (!valid) break;
        bool dateUnit = strcasecmp(unit, "YEAR") == 0 || strcasecmp(unit, "YEAR_MONTH") == 0 ||
                        strcasecmp(unit, "QUARTER") == 0 || strcasecmp(unit, "MONTH") == 0 ||
                        strcasecmp(unit, "WEEK") == 0 || strcasecmp(unit, "DAY") == 0;
        valid = mysqlCivil(&input[1], row, !dateUnit, &civil, scratch, &code, NULL) &&
                mysqlUnitValue(unit, &civil, &value);
        if (valid) code = colDataSetVal(output->columnData, row, (char*)&value, false);
        break;
      }
      default:
      mysql_date_path:
        valid = mysqlCivil(&input[0], row,
                           op == FUNCTION_TYPE_MYSQL_HOUR || op == FUNCTION_TYPE_MYSQL_MINUTE ||
                               op == FUNCTION_TYPE_MYSQL_SECOND || op == FUNCTION_TYPE_MYSQL_MICROSECOND,
                           &civil, scratch, &code,
                           op == FUNCTION_TYPE_MYSQL_UNIX_TIMESTAMP ? &offsetInstant : NULL);
        if (!valid) break;
        int64_t dayNumber = tMysqlDayNumber(civil.year, civil.month, civil.day);
        switch (op) {
          case FUNCTION_TYPE_MYSQL_DATEDIFF: {
            SMySqlCivilTime second;
            valid = mysqlCivil(&input[1], row, false, &second, scratch, &code, NULL);
            if (!valid) break;
            int64_t secondNumber = tMysqlDayNumber(second.year, second.month, second.day);
            valid = dayNumber >= 0 && secondNumber >= 0;
            value = dayNumber - secondNumber;
            break;
          }
          case FUNCTION_TYPE_MYSQL_DATE_FORMAT: {
            char    format[MYSQL_TEXT_CAP];
            int32_t length = 0;
            valid = mysqlText(&input[1], row, format, sizeof(format), &length, scratch, &code);
            if (valid) code = mysqlWriteFormat(output, row, &civil, format, length, scratch);
            break;
          }
          case FUNCTION_TYPE_MYSQL_DAY:
          case FUNCTION_TYPE_MYSQL_DAYOFMONTH:
            value = civil.day;
            break;
          case FUNCTION_TYPE_MYSQL_DAYNAME:
            valid = dayNumber >= 0;
            if (valid) code = mysqlWriteFormat(output, row, &civil, "%W", 2, scratch);
            break;
          case FUNCTION_TYPE_MYSQL_DAYOFYEAR:
            valid = dayNumber >= 0;
            value = dayNumber - tMysqlDayNumber(civil.year, 1, 1) + 1;
            break;
          case FUNCTION_TYPE_MYSQL_HOUR:
            value = civil.hour;
            break;
          case FUNCTION_TYPE_MYSQL_MICROSECOND:
            value = civil.microsecond;
            break;
          case FUNCTION_TYPE_MYSQL_MINUTE:
            value = civil.minute;
            break;
          case FUNCTION_TYPE_MYSQL_MONTH:
            value = civil.month;
            break;
          case FUNCTION_TYPE_MYSQL_MONTHNAME:
            valid = civil.month >= 1 && civil.month <= 12;
            if (valid) code = mysqlWriteFormat(output, row, &civil, "%M", 2, scratch);
            break;
          case FUNCTION_TYPE_MYSQL_QUARTER:
            value = (civil.month + 2) / 3;
            break;
          case FUNCTION_TYPE_MYSQL_SECOND:
            value = civil.second;
            break;
          case FUNCTION_TYPE_MYSQL_TO_DAYS:
            valid = dayNumber >= 0;
            value = dayNumber;
            break;
          case FUNCTION_TYPE_MYSQL_TO_SECONDS:
            valid = dayNumber >= 0;
            value = dayNumber * 86400 + civil.hour * 3600 + civil.minute * 60 + civil.second;
            break;
          case FUNCTION_TYPE_MYSQL_UNIX_TIMESTAMP: {
            int64_t ticks = 0;
            if (offsetInstant.parsed) {
              ticks = offsetInstant.ticks;
            } else {
              valid = mysqlToTicks(&civil, TSDB_TIME_PRECISION_MICRO, input[0].tz, &ticks);
            }
            if (valid) code = mysqlWriteUnixTimestamp(output, row, ticks);
            break;
          }
          case FUNCTION_TYPE_MYSQL_YEAR:
            value = civil.year;
            break;
          case FUNCTION_TYPE_MYSQL_YEARWEEK: {
            int64_t modeValue = argumentCount == 2 ? mysqlInteger(&input[1], row) : 0;
            int32_t year = 0;
            int32_t week = 0;
            if (modeValue < 0 || modeValue > 7) return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
            int32_t mode = (int32_t)modeValue;
            valid = dayNumber >= 0 && tMysqlWeekYearAndNumber(&civil, mode | 2, &year, &week);
            value = year < 0 ? (int64_t)(uint32_t)(year * 100 + week) : year * 100 + week;
            break;
          }
          default:
            return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
        }
        if (!valid || code != TSDB_CODE_SUCCESS) break;
        if (op != FUNCTION_TYPE_MYSQL_DATE_FORMAT && op != FUNCTION_TYPE_MYSQL_DAYNAME &&
            op != FUNCTION_TYPE_MYSQL_MONTHNAME && op != FUNCTION_TYPE_MYSQL_UNIX_TIMESTAMP) {
          code = colDataSetVal(output->columnData, row, (char*)&value, false);
        }
        break;
    }
    if (code != TSDB_CODE_SUCCESS) return code;
    if (!valid) colDataSetNULL(output->columnData, row);
  }
  output->numOfRows = rows;
  return TSDB_CODE_SUCCESS;
}

int32_t mysqlTimeFunction(SScalarParam* input, int32_t inputNum, SScalarParam* output) {
  SMySqlTimeScratch scratch = {0};
  int32_t           code = mysqlTimeFunctionImpl(input, inputNum, output, &scratch);
  taosMemoryFree(scratch.formatBuffer);
  taosMemoryFree(scratch.textBuffer);
  taosMemoryFree(scratch.ncharBuffer);
  return code;
}
