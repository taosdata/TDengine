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

#include "tmysqltime.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#ifndef WINDOWS
#include <strings.h>
#endif

#include "osDef.h"

static const int8_t      kDaysByMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const char* const kMonthNames[] = {"",     "January", "February",  "March",   "April",    "May",     "June",
                                          "July", "August",  "September", "October", "November", "December"};
static const char* const kWeekNames[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

static bool mysqlLeapYear(int32_t year) { return year != 0 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0); }

static int32_t mysqlDaysInYear(int32_t year) { return mysqlLeapYear(year) ? 366 : 365; }

static int32_t mysqlDaysInMonth(int32_t year, int32_t month) {
  if (month < 1 || month > 12) return 0;
  return kDaysByMonth[month] + (month == 2 && mysqlLeapYear(year));
}

static bool mysqlReadDigits(const char* text, int32_t len, int32_t* position, int32_t minDigits, int32_t maxDigits,
                            int32_t* value) {
  int32_t count = 0;
  int32_t number = 0;
  while (*position < len && count < maxDigits && isdigit((unsigned char)text[*position])) {
    if (number > (INT32_MAX - 9) / 10) return false;
    number = number * 10 + text[(*position)++] - '0';
    ++count;
  }
  if (count < minDigits) return false;
  *value = number;
  return true;
}

static bool mysqlReadDurationPart(const char* text, int32_t len, int32_t* position, uint32_t* value) {
  int32_t  start = *position;
  uint32_t number = 0;
  while (*position < len && isdigit((unsigned char)text[*position])) {
    uint32_t digit = (uint32_t)(text[*position] - '0');
    if (number > (UINT32_MAX - digit) / 10) return false;
    number = number * 10 + digit;
    ++*position;
  }
  if (*position == start) return false;
  *value = number;
  return true;
}

static bool mysqlConsume(const char* text, int32_t len, int32_t* position, char expected) {
  if (*position >= len || text[*position] != expected) return false;
  ++*position;
  return true;
}

static bool mysqlReadSeparator(const char* text, int32_t len, int32_t* position) {
  int32_t start = *position;
  while (*position < len && ispunct((unsigned char)text[*position])) ++*position;
  return *position > start;
}

static void mysqlSkipSpaces(const char* text, int32_t len, int32_t* position) {
  while (*position < len && isspace((unsigned char)text[*position])) ++*position;
}

static bool mysqlReadFraction(const char* text, int32_t len, int32_t* position, int32_t* micros, bool* roundUp) {
  if (*position >= len || text[*position] != '.') return true;
  ++*position;
  int32_t count = 0;
  int32_t value = 0;
  while (*position < len && isdigit((unsigned char)text[*position])) {
    if (count < 6) value = value * 10 + text[*position] - '0';
    if (count == 6) *roundUp = text[*position] >= '5';
    ++*position;
    ++count;
  }
  if (count == 0) return false;
  while (count++ < 6) value *= 10;
  *micros = value;
  return true;
}

static bool mysqlReadClock(const char* text, int32_t len, int32_t* position, bool duration, bool allowFraction,
                           SMySqlCivilTime* result, bool* roundUp) {
  int32_t first = 0;
  if (duration) {
    uint32_t number = 0;
    if (!mysqlReadDurationPart(text, len, position, &number)) return false;
    uint64_t hours = number;
    if (*position < len && isspace((unsigned char)text[*position])) {
      mysqlSkipSpaces(text, len, position);
      uint32_t remainder = 0;
      if (!mysqlReadDurationPart(text, len, position, &remainder)) return false;
      hours = hours * 24 + remainder;
    }
    // Retain an out-of-range marker until the remaining fields have been validated.
    first = hours > 838 ? 839 : (int32_t)hours;
  } else if (!mysqlReadDigits(text, len, position, 1, len, &first)) {
    return false;
  }
  if (!duration && first >= 24) return false;
  result->hour = first;
  result->hasTime = true;
  int32_t* fields[] = {&result->minute, &result->second};
  for (int32_t i = 0; i < 2; ++i) {
    // Date-time text may end after the hour or minute; omitted fields stay zero.
    if (!duration) {
      int32_t end = *position;
      mysqlSkipSpaces(text, len, &end);
      if (end == len) {
        *position = end;
        return true;
      }
    }
    if (!(duration ? mysqlConsume(text, len, position, ':') : mysqlReadSeparator(text, len, position)) ||
        !mysqlReadDigits(text, len, position, 1, len, fields[i]) || *fields[i] >= 60) {
      return false;
    }
  }
  return !allowFraction || mysqlReadFraction(text, len, position, &result->microsecond, roundUp);
}

static bool mysqlResolveParsedDayNumber(SMySqlCivilTime* result, int64_t dayNumber);

static bool mysqlRoundFraction(SMySqlCivilTime* result, bool roundUp, bool timeContext) {
  if (roundUp && ++result->microsecond == 1000000) {
    result->microsecond = 0;
    int32_t seconds = result->hour * 3600 + result->minute * 60 + result->second + 1;
    // TIME conversion keeps the date fields and can carry midnight into hour 24.
    if (result->hasDate && !timeContext) {
      // A second carry needs a complete date, including when it stays within the same day.
      int64_t day = tMysqlDayNumber(result->year, result->month, result->day);
      if (day < 0 || !mysqlResolveParsedDayNumber(result, day + seconds / 86400)) return false;
      seconds %= 86400;
    }
    result->hour = seconds / 3600;
    result->minute = seconds / 60 % 60;
    result->second = seconds % 60;
  }
  // Validate all input fields before rounding, then clip durations at the TIME boundary.
  if (!result->hasDate && (result->hour > 838 ||
                           (result->hour == 838 && result->minute == 59 && result->second == 59 &&
                            result->microsecond > 0))) {
    result->hour = 838;
    result->minute = 59;
    result->second = 59;
    result->microsecond = 0;
  }
  return true;
}

static EMySqlTimeParseResult mysqlValidateCivil(const SMySqlCivilTime* value) {
  if (!value->hasDate) return MYSQL_TIME_PARSE_OK;
  if (value->year < 0 || value->year > 9999 || value->month < 0 || value->month > 12 || value->day < 0 ||
      value->day > 31) {
    return MYSQL_TIME_PARSE_INVALID;
  }
  if (value->month > 0 && value->day > 0 && value->day > mysqlDaysInMonth(value->year, value->month)) {
    return MYSQL_TIME_PARSE_INVALID_COMPLETE_DATE;
  }
  return MYSQL_TIME_PARSE_OK;
}

EMySqlTimeParseResult tMysqlParseCivil(const char* text, int32_t len, bool allowTimeOnly, SMySqlCivilTime* result) {
  if (text == NULL || result == NULL || len <= 0) return MYSQL_TIME_PARSE_INVALID;
  memset(result, 0, sizeof(*result));
  int32_t pos = 0;
  bool    roundUp = false;
  mysqlSkipSpaces(text, len, &pos);
  if (allowTimeOnly && pos < len && text[pos] == '-') {
    result->negativeTime = true;
    ++pos;
  }
  int32_t datePos = pos;
  int32_t year = 0;
  bool    hasYear = mysqlReadDigits(text, len, &datePos, 2, 4, &year);
  int32_t yearDigits = datePos - pos;
  bool    isDate = hasYear && (yearDigits == 2 || yearDigits == 4) && datePos < len &&
                   ispunct((unsigned char)text[datePos]);
  // Colon-separated hours remain a TIME value in time-only contexts, including oversized hours.
  if (allowTimeOnly && datePos < len && text[datePos] == ':') {
    SMySqlCivilTime time = *result;
    int32_t timePos = pos;
    bool    timeRoundUp = false;
    if (mysqlReadClock(text, len, &timePos, true, true, &time, &timeRoundUp)) {
      mysqlSkipSpaces(text, len, &timePos);
      if (timePos == len) {
        if (!mysqlRoundFraction(&time, timeRoundUp, true)) return MYSQL_TIME_PARSE_INVALID;
        *result = time;
        return MYSQL_TIME_PARSE_OK;
      }
    }
  }
  if (isDate) {
    pos = datePos;
    // Separated fields may have extra leading zeroes; validate their numeric values after reading them.
    if (!mysqlReadSeparator(text, len, &pos) || !mysqlReadDigits(text, len, &pos, 1, len, &result->month) ||
        !mysqlReadSeparator(text, len, &pos) ||
        !mysqlReadDigits(text, len, &pos, 1, len, &result->day)) {
      return MYSQL_TIME_PARSE_INVALID;
    }
    if (yearDigits == 2 && (year != 0 || result->month != 0 || result->day != 0)) {
      year += year < 70 ? 2000 : 1900;
    }
    result->year = year;
    result->hasDate = true;
    result->negativeTime = false;
    EMySqlTimeParseResult status = mysqlValidateCivil(result);
    if (status != MYSQL_TIME_PARSE_OK) return status;
    if (pos < len && (text[pos] == 'T' || isspace((unsigned char)text[pos]))) {
      if (text[pos] == 'T') ++pos;
      mysqlSkipSpaces(text, len, &pos);
      if (pos < len && !mysqlReadClock(text, len, &pos, false, true, result, &roundUp)) {
        return MYSQL_TIME_PARSE_INVALID;
      }
    }
    if (yearDigits == 2 && result->year == 0 &&
        (result->hour != 0 || result->minute != 0 || result->second != 0 || result->microsecond != 0)) {
      result->year = 2000;
    }
  } else if (allowTimeOnly) {
    if (!mysqlReadClock(text, len, &pos, true, true, result, &roundUp)) return MYSQL_TIME_PARSE_INVALID;
  } else {
    return MYSQL_TIME_PARSE_INVALID;
  }
  mysqlSkipSpaces(text, len, &pos);
  return pos == len && mysqlRoundFraction(result, roundUp, allowTimeOnly) ? MYSQL_TIME_PARSE_OK : MYSQL_TIME_PARSE_INVALID;
}

// MySQL DATE_FORMAT retains zero month/day fields when calculating calendar fields.
static int64_t mysqlCalendarDayNumber(int32_t year, int32_t month, int32_t day) {
  if (year == 0 && month == 0) return 0;
  int64_t y = year;
  int64_t result = 365 * y + 31 * (month - 1) + day;
  if (month <= 2) {
    --y;
  } else {
    result -= (month * 4 + 23) / 10;
  }
  return result + y / 4 - ((y / 100 + 1) * 3) / 4;
}

int64_t tMysqlDayNumber(int32_t year, int32_t month, int32_t day) {
  if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1 || day > mysqlDaysInMonth(year, month)) {
    return -1;
  }
  return mysqlCalendarDayNumber(year, month, day);
}

bool tMysqlFromDayNumber(int64_t dayNumber, SMySqlCivilTime* result) {
  if (result == NULL || dayNumber < 1 || dayNumber > tMysqlDayNumber(9999, 12, 31)) return false;
  int32_t low = 0;
  int32_t high = 9999;
  while (low < high) {
    int32_t middle = low + (high - low + 1) / 2;
    if (tMysqlDayNumber(middle, 1, 1) <= dayNumber)
      low = middle;
    else
      high = middle - 1;
  }
  memset(result, 0, sizeof(*result));
  result->year = low;
  result->hasDate = true;
  for (int32_t month = 1; month <= 12; ++month) {
    int64_t start = tMysqlDayNumber(low, month, 1);
    int32_t days = mysqlDaysInMonth(low, month);
    if (dayNumber >= start && dayNumber < start + days) {
      result->month = month;
      result->day = (int32_t)(dayNumber - start + 1);
      return true;
    }
  }
  return false;
}

static int32_t mysqlWeekday(int64_t dayNumber, bool sundayFirst) {
  return (int32_t)((dayNumber + 5 + (sundayFirst ? 1 : 0)) % 7);
}

static void mysqlCalendarWeek(const SMySqlCivilTime* civil, int32_t mode, int32_t* weekYear, int32_t* weekNumber) {
  int64_t dayNumber = mysqlCalendarDayNumber(civil->year, civil->month, civil->day);
  int32_t flags = mode & 1 ? mode : mode ^ 4;
  bool    mondayFirst = (flags & 1) != 0;
  bool    fromOne = (flags & 2) != 0;
  bool    firstWeekday = (flags & 4) != 0;
  int32_t year = civil->year;
  int64_t first = mysqlCalendarDayNumber(year, 1, 1);
  int32_t weekday = mysqlWeekday(first, !mondayFirst);
  // Preserve MySQL's unsigned day difference for incomplete dates before week one.
  uint32_t days;
  if (civil->month == 1 && civil->day <= 7 - weekday) {
    if (!fromOne && ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4))) {
      *weekYear = year;
      *weekNumber = 0;
      return;
    }
    fromOne = true;
    days = mysqlDaysInYear(--year);
    first -= days;
    weekday = (int32_t)((weekday + 53 * 7 - days) % 7);
  }
  if ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4)) {
    days = (uint32_t)(dayNumber - (first + (7 - weekday)));
  } else {
    days = (uint32_t)(dayNumber - (first - weekday));
  }
  if (fromOne && days >= 52 * 7) {
    weekday = (weekday + mysqlDaysInYear(year)) % 7;
    if ((!firstWeekday && weekday < 4) || (firstWeekday && weekday == 0)) {
      *weekYear = year + 1;
      *weekNumber = 1;
      return;
    }
  }
  *weekYear = year;
  *weekNumber = (int32_t)(days / 7 + 1);
}

bool tMysqlWeekYearAndNumber(const SMySqlCivilTime* civil, int32_t mode, int32_t* weekYear, int32_t* weekNumber) {
  if (civil == NULL || weekYear == NULL || weekNumber == NULL || mode < 0 || mode > 7) return false;
  // EXTRACT permits incomplete dates; callers requiring a complete date validate it separately.
  if (!civil->hasDate || mysqlValidateCivil(civil) != MYSQL_TIME_PARSE_OK) return false;
  mysqlCalendarWeek(civil, mode, weekYear, weekNumber);
  return true;
}

static bool mysqlAppend(char* output, int32_t capacity, int32_t* length, const char* text, int32_t textLength) {
  if (textLength < 0 || *length > capacity - textLength - 1) return false;
  memcpy(output + *length, text, (size_t)textLength);
  *length += textLength;
  output[*length] = '\0';
  return true;
}

static bool mysqlAppendNumber(char* output, int32_t capacity, int32_t* length, int32_t number, int32_t width) {
  char    buffer[32];
  int32_t count = snprintf(buffer, sizeof(buffer), "%d", number);
  if (count < 0 || count >= (int32_t)sizeof(buffer)) return false;
  // MySQL pads the entire text, including a negative %j value (for example, "0-1").
  for (int32_t i = count; i < width; ++i) {
    if (!mysqlAppend(output, capacity, length, "0", 1)) return false;
  }
  return mysqlAppend(output, capacity, length, buffer, count);
}

static int32_t mysqlDayOfYear(const SMySqlCivilTime* civil) {
  int64_t dayNumber = mysqlCalendarDayNumber(civil->year, civil->month, civil->day);
  return (int32_t)(dayNumber - mysqlCalendarDayNumber(civil->year, 1, 1) + 1);
}

static int32_t mysqlSundayIndex(const SMySqlCivilTime* civil) {
  int64_t dayNumber = mysqlCalendarDayNumber(civil->year, civil->month, civil->day);
  return mysqlWeekday(dayNumber, true);
}

static bool mysqlFormatWeek(char token, const SMySqlCivilTime* civil, char* output, int32_t capacity, int32_t* length) {
  int32_t mode = token == 'U' ? 0 : token == 'u' ? 1 : token == 'V' || token == 'X' ? 2 : 3;
  int32_t year = 0;
  int32_t week = 0;
  mysqlCalendarWeek(civil, mode, &year, &week);
  if (year < 0 && (token == 'X' || token == 'x')) {
    return mysqlAppend(output, capacity, length, "4294967295", 10);
  }
  return mysqlAppendNumber(output, capacity, length, token == 'X' || token == 'x' ? year : week,
                           token == 'X' || token == 'x' ? 4 : 2);
}

static bool mysqlFormatToken(char token, const SMySqlCivilTime* civil, char* output, int32_t capacity,
                             int32_t* length) {
  int32_t dayOfWeek = mysqlSundayIndex(civil);
  int32_t hour12 = civil->hour % 12;
  if (hour12 == 0) hour12 = 12;
  switch (token) {
    case 'Y':
      return mysqlAppendNumber(output, capacity, length, civil->year, 4);
    case 'y':
      return mysqlAppendNumber(output, capacity, length, civil->year % 100, 2);
    case 'm':
      return mysqlAppendNumber(output, capacity, length, civil->month, 2);
    case 'c':
      return mysqlAppendNumber(output, capacity, length, civil->month, 1);
    case 'd':
      return mysqlAppendNumber(output, capacity, length, civil->day, 2);
    case 'e':
      return mysqlAppendNumber(output, capacity, length, civil->day, 1);
    case 'H':
      return mysqlAppendNumber(output, capacity, length, civil->hour, 2);
    case 'k':
      return mysqlAppendNumber(output, capacity, length, civil->hour, 1);
    case 'h':
    case 'I':
      return mysqlAppendNumber(output, capacity, length, hour12, 2);
    case 'l':
      return mysqlAppendNumber(output, capacity, length, hour12, 1);
    case 'i':
      return mysqlAppendNumber(output, capacity, length, civil->minute, 2);
    case 's':
    case 'S':
      return mysqlAppendNumber(output, capacity, length, civil->second, 2);
    case 'f':
      return mysqlAppendNumber(output, capacity, length, civil->microsecond, 6);
    case 'p': {
      const char* marker = civil->hour < 12 ? "AM" : "PM";
      return mysqlAppend(output, capacity, length, marker, 2);
    }
    case 'M':
    case 'b': {
      if (civil->month < 1 || civil->month > 12) return false;
      const char* name = kMonthNames[civil->month];
      return mysqlAppend(output, capacity, length, name, token == 'b' ? 3 : (int32_t)strlen(name));
    }
    case 'W':
    case 'a': {
      if (dayOfWeek < 0) return true;
      const char* name = kWeekNames[dayOfWeek];
      return mysqlAppend(output, capacity, length, name, token == 'a' ? 3 : (int32_t)strlen(name));
    }
    case 'j':
      return mysqlAppendNumber(output, capacity, length, mysqlDayOfYear(civil), 3);
    case 'w':
      return mysqlAppendNumber(output, capacity, length, dayOfWeek < 0 ? 0 : dayOfWeek, 1);
    case 'D': {
      if (!mysqlAppendNumber(output, capacity, length, civil->day, 1)) return false;
      const char* suffix = "th";
      if (civil->day % 100 < 11 || civil->day % 100 > 13) {
        if (civil->day % 10 == 1)
          suffix = "st";
        else if (civil->day % 10 == 2)
          suffix = "nd";
        else if (civil->day % 10 == 3)
          suffix = "rd";
      }
      return mysqlAppend(output, capacity, length, suffix, 2);
    }
    case 'U':
    case 'u':
    case 'V':
    case 'v':
    case 'X':
    case 'x':
      return mysqlFormatWeek(token, civil, output, capacity, length);
    case 'T': {
      return mysqlAppendNumber(output, capacity, length, civil->hour, 2) &&
             mysqlAppend(output, capacity, length, ":", 1) &&
             mysqlAppendNumber(output, capacity, length, civil->minute, 2) &&
             mysqlAppend(output, capacity, length, ":", 1) &&
             mysqlAppendNumber(output, capacity, length, civil->second, 2);
    }
    case 'r': {
      const char* marker = civil->hour < 12 ? " AM" : " PM";
      return mysqlAppendNumber(output, capacity, length, hour12, 2) && mysqlAppend(output, capacity, length, ":", 1) &&
             mysqlAppendNumber(output, capacity, length, civil->minute, 2) &&
             mysqlAppend(output, capacity, length, ":", 1) &&
             mysqlAppendNumber(output, capacity, length, civil->second, 2) &&
             mysqlAppend(output, capacity, length, marker, 3);
    }
    default:
      return mysqlAppend(output, capacity, length, &token, 1);
  }
}

int32_t tMysqlFormatCivil(const SMySqlCivilTime* civil, const char* format, int32_t formatLen, char* output,
                          int32_t capacity) {
  if (civil == NULL || format == NULL || output == NULL || formatLen < 0 || capacity < 1) return -1;
  int32_t length = 0;
  output[0] = '\0';
  for (int32_t i = 0; i < formatLen; ++i) {
    if (format[i] == '%' && i + 1 < formatLen) {
      char token = format[++i];
      if ((token == 'M' || token == 'b') && (civil->month < 1 || civil->month > 12)) {
        return MYSQL_TIME_FORMAT_NULL;
      }
      if ((token == 'W' || token == 'a' || token == 'w') && civil->year == 0 && civil->month == 0) {
        return MYSQL_TIME_FORMAT_NULL;
      }
      if (!mysqlFormatToken(token, civil, output, capacity, &length)) return -1;
    } else if (!mysqlAppend(output, capacity, &length, format + i, 1)) {
      return -1;
    }
  }
  return length;
}

static bool mysqlReadName(const char* text, int32_t len, int32_t* position, const char* const* names, int32_t first,
                          int32_t last, bool shortName, int32_t* value) {
  int32_t end = *position;
  while (end < len && isalpha((unsigned char)text[end])) ++end;
  int32_t wordLen = end - *position;
  if (wordLen == 0) return false;
  int32_t matched = -1;
  int32_t matches = 0;
  for (int32_t i = first; i <= last; ++i) {
    int32_t nameLen = shortName ? 3 : (int32_t)strlen(names[i]);
    if (wordLen <= nameLen && strncasecmp(text + *position, names[i], (size_t)wordLen) == 0) {
      matched = i;
      ++matches;
      if (wordLen == nameLen) {
        matches = 1;
        break;
      }
    }
  }
  if (matches != 1) return false;
  *position = end;
  *value = matched;
  return true;
}

typedef struct SMySqlFormatParseState {
  int32_t amPm;
  int32_t dayOfYear;
  int32_t weekNumber;
  int32_t weekMode;
  int32_t weekYear;
  int32_t weekYearMode;
  int32_t weekday;
  bool    is12Hour;
} SMySqlFormatParseState;

static bool mysqlReadFormatFields(const char* text, int32_t len, int32_t* position, const char* format,
                                  int32_t formatLen, SMySqlCivilTime* civil, SMySqlFormatParseState* state);

static bool mysqlReadFormatNumber(const char* text, int32_t len, int32_t* position, int32_t width,
                                   int32_t* value) {
  // MySQL's bounded integer conversion counts the optional plus sign in the field width.
  if (*position < len && text[*position] == '+') {
    ++*position;
    --width;
  }
  return mysqlReadDigits(text, len, position, 1, width, value);
}

static bool mysqlReadFormatToken(char token, const char* text, int32_t len, int32_t* position, SMySqlCivilTime* civil,
                                 SMySqlFormatParseState* state) {
  int32_t value = 0;
  switch (token) {
    case 'Y': {
      int32_t start = *position;
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 4, &civil->year);
      if (civil->hasDate && *position - start <= 2) {
        civil->year = civil->year <= 69 ? 2000 + civil->year : 1900 + civil->year;
      }
      return civil->hasDate;
    }
    case 'y':
      if (!mysqlReadFormatNumber(text, len, position, 2, &value)) return false;
      civil->year = value <= 69 ? 2000 + value : 1900 + value;
      civil->hasDate = true;
      return true;
    case 'm':
    case 'c':
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 2, &civil->month);
      return civil->hasDate;
    case 'd':
    case 'e':
    case 'D':
      if (!mysqlReadFormatNumber(text, len, position, 2, &civil->day)) return false;
      civil->hasDate = true;
      if (token == 'D') {
        int32_t remaining = len - *position;
        *position += remaining < 2 ? remaining : 2;
      }
      return true;
    case 'M':
    case 'b':
      civil->hasDate = mysqlReadName(text, len, position, kMonthNames, 1, 12, token == 'b', &civil->month);
      return civil->hasDate;
    case 'W':
    case 'a':
      civil->hasDate = mysqlReadName(text, len, position, kWeekNames, 0, 6, token == 'a', &state->weekday);
      return civil->hasDate;
    case 'H':
    case 'k':
    case 'h':
    case 'I':
    case 'l':
      state->is12Hour |= token == 'h' || token == 'I' || token == 'l';
      civil->hasTime = mysqlReadFormatNumber(text, len, position, 2, &civil->hour);
      return civil->hasTime;
    case 'i':
      civil->hasTime = mysqlReadFormatNumber(text, len, position, 2, &civil->minute);
      return civil->hasTime;
    case 's':
    case 'S':
      civil->hasTime = mysqlReadFormatNumber(text, len, position, 2, &civil->second);
      return civil->hasTime;
    case 'f': {
      int32_t start = *position;
      if (!mysqlReadFormatNumber(text, len, position, 6, &value)) return false;
      int32_t digits = *position - start;
      while (digits++ < 6) value *= 10;
      civil->microsecond = value;
      civil->hasTime = true;
      return true;
    }
    case 'p':
      if (*position + 2 > len || !state->is12Hour) return false;
      if (strncasecmp(text + *position, "AM", 2) == 0)
        state->amPm = 0;
      else if (strncasecmp(text + *position, "PM", 2) == 0)
        state->amPm = 1;
      else
        return false;
      *position += 2;
      civil->hasTime = true;
      return true;
    case 'w':
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 1, &state->weekday) && state->weekday <= 6;
      return civil->hasDate;
    case 'j':
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 3, &state->dayOfYear);
      return civil->hasDate;
    case 'U':
    case 'u':
    case 'V':
    case 'v':
      state->weekMode = token == 'U' ? 0 : token == 'u' ? 1 : token == 'V' ? 2 : 3;
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 2, &state->weekNumber);
      return civil->hasDate && state->weekNumber <= 53 && (state->weekMode < 2 || state->weekNumber > 0);
    case 'X':
    case 'x':
      state->weekYearMode = token == 'X' ? 2 : 3;
      civil->hasDate = mysqlReadFormatNumber(text, len, position, 4, &state->weekYear);
      return civil->hasDate;
    case 'T':
    case 'r': {
      SMySqlFormatParseState clockState = {.amPm = -1};
      const char* pattern = token == 'T' ? "%H:%i:%s" : "%h:%i:%s %p";
      if (!mysqlReadFormatFields(text, len, position, pattern, (int32_t)strlen(pattern), civil, &clockState)) {
        return false;
      }
      if (clockState.is12Hour) {
        if (civil->hour < 1 || civil->hour > 12) return false;
        civil->hour = civil->hour % 12 + (clockState.amPm == 1 ? 12 : 0);
      }
      return true;
    }
    default:
      return false;
  }
}

static bool mysqlReadFormatFields(const char* text, int32_t len, int32_t* position, const char* format,
                                  int32_t formatLen, SMySqlCivilTime* civil, SMySqlFormatParseState* state) {
  for (int32_t i = 0; i < formatLen && *position < len; ++i) {
    mysqlSkipSpaces(text, len, position);
    if (*position == len) break;
    if (format[i] == '%' && i + 1 < formatLen) {
      if (!mysqlReadFormatToken(format[++i], text, len, position, civil, state)) return false;
    } else if (!isspace((unsigned char)format[i]) && !mysqlConsume(text, len, position, format[i])) {
      return false;
    }
  }
  return true;
}

static bool mysqlResolveParsedDayNumber(SMySqlCivilTime* result, int64_t dayNumber) {
  // MySQL maps positive day numbers in year zero to a zero date when parsing ordinals or weeks.
  if (dayNumber > 0 && dayNumber <= 365) {
    result->year = 0;
    result->month = 0;
    result->day = 0;
    return true;
  }
  SMySqlCivilTime parsed;
  if (!tMysqlFromDayNumber(dayNumber, &parsed)) return false;
  result->year = parsed.year;
  result->month = parsed.month;
  result->day = parsed.day;
  return true;
}

static bool mysqlResolveDayOfYear(SMySqlCivilTime* result, const SMySqlFormatParseState* state) {
  // Zero leaves the other date fields intact; positive ordinals may cross a year boundary.
  if (state->dayOfYear <= 0) return true;
  return mysqlResolveParsedDayNumber(result, tMysqlDayNumber(result->year, 1, 1) + state->dayOfYear - 1);
}

static bool mysqlResolveWeek(SMySqlCivilTime* result, const SMySqlFormatParseState* state) {
  if (state->weekNumber < 0 || state->weekday < 0) return true;
  bool    strict = state->weekMode >= 2;
  if ((strict && (state->weekYear < 0 || state->weekYearMode != state->weekMode)) ||
      (!strict && state->weekYear >= 0)) {
    return false;
  }
  int32_t year = strict ? state->weekYear : result->year;
  if (year < 0 || year > 9999) return false;
  int64_t first = tMysqlDayNumber(year, 1, 1);
  bool    sundayFirst = state->weekMode == 0 || state->weekMode == 2;
  int32_t firstWeekday = mysqlWeekday(first, sundayFirst);
  int32_t weekday = state->weekday == 0 ? 7 : state->weekday;
  int64_t day = first + (int64_t)(state->weekNumber - 1) * 7;
  if (sundayFirst) {
    day += (firstWeekday == 0 ? 0 : 7) - firstWeekday + weekday % 7;
  } else {
    day += (firstWeekday <= 3 ? 0 : 7) - firstWeekday + weekday - 1;
  }
  return mysqlResolveParsedDayNumber(result, day);
}

EMySqlTimeParseResult tMysqlParseCivilFormat(const char* text, int32_t len, const char* format, int32_t formatLen,
                                             SMySqlCivilTime* result) {
  if (text == NULL || format == NULL || result == NULL || len < 0 || formatLen < 0) {
    return MYSQL_TIME_PARSE_INVALID;
  }
  memset(result, 0, sizeof(*result));
  int32_t                pos = 0;
  SMySqlFormatParseState state = {
      .amPm = -1, .dayOfYear = -1, .weekNumber = -1, .weekMode = -1, .weekYear = -1, .weekYearMode = -1,
      .weekday = -1};
  if (!mysqlReadFormatFields(text, len, &pos, format, formatLen, result, &state)) return MYSQL_TIME_PARSE_INVALID;
  if (state.is12Hour || state.amPm >= 0) {
    if (result->hour < 1 || result->hour > 12) return MYSQL_TIME_PARSE_INVALID;
    result->hour = result->hour % 12 + (state.amPm == 1 ? 12 : 0);
  }
  if (result->hour > 23 || result->minute > 59 || result->second > 59) return MYSQL_TIME_PARSE_INVALID;
  if (!mysqlResolveDayOfYear(result, &state) || !mysqlResolveWeek(result, &state)) {
    return MYSQL_TIME_PARSE_INVALID;
  }
  // The complete format determines the result shape, including unconsumed fields.
  // Day-of-month alone can belong to either a DATE or a TIME.
  result->hasDate = false;
  result->hasTime = false;
  for (int32_t i = 0; i + 1 < formatLen; ++i) {
    if (format[i] != '%') continue;
    char token = format[++i];
    if (token == '\0') continue;
    result->hasDate |= strchr("YymcMbWawjUuVvXx", token) != NULL;
    result->hasTime |= strchr("HkhIliSsfTr", token) != NULL;
  }
  if (!result->hasDate && !result->hasTime) result->hasDate = true;
  // Validate the day before folding it into hours (MySQL accepts 0..31 here).
  EMySqlTimeParseResult status = mysqlValidateCivil(result);
  if (result->day < 0 || result->day > 31) return MYSQL_TIME_PARSE_INVALID;
  if (status == MYSQL_TIME_PARSE_OK && !result->hasDate) {
    result->hour += result->day * 24;
    result->day = 0;
  }
  return status;
}
