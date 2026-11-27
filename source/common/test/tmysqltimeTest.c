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

#include <stdio.h>
#include <string.h>

#include "tmysqltime.h"

#define CHECK(condition)                                                                \
  do {                                                                                  \
    if (!(condition)) {                                                                 \
      fprintf(stderr, "tmysqltime test failed at line %d: %s\n", __LINE__, #condition); \
      return 1;                                                                         \
    }                                                                                   \
  } while (0)

static int32_t checkWeekFormatRoundTrips(void) {
  const int32_t years[] = {0, 1, 1999, 2000, 2020, 2021, 2024, 9999};
  const char* formats[] = {"%Y %U %w", "%Y %u %w", "%X %V %w", "%x %v %w"};
  for (int32_t i = 0; i < (int32_t)(sizeof(years) / sizeof(years[0])); ++i) {
    int64_t first = tMysqlDayNumber(years[i], 1, 1);
    int64_t last = tMysqlDayNumber(years[i], 12, 31);
    for (int64_t day = first; day <= last; ++day) {
      SMySqlCivilTime expected;
      CHECK(tMysqlFromDayNumber(day, &expected));
      for (int32_t mode = 0; mode < 4; ++mode) {
        if ((years[i] == 0 || years[i] == 9999) && mode >= 2) continue;
        char    text[64];
        int32_t formatLen = (int32_t)strlen(formats[mode]);
        int32_t textLen = tMysqlFormatCivil(&expected, formats[mode], formatLen, text, sizeof(text));
        CHECK(textLen > 0);
        SMySqlCivilTime actual;
        CHECK(tMysqlParseCivilFormat(text, textLen, formats[mode], formatLen, &actual) == MYSQL_TIME_PARSE_OK);
        if (years[i] == 0) {
          // MySQL's inverse day-number conversion maps year zero to a zero date.
          CHECK(actual.year == 0 && actual.month == 0 && actual.day == 0);
        } else {
          CHECK(tMysqlDayNumber(actual.year, actual.month, actual.day) == day);
        }
      }
    }
  }
  return 0;
}

static int32_t checkDayTimeFormats(void) {
  const struct {
    const char* text;
    const char* format;
    const char* expected;
    bool hasDate;
    bool hasTime;
  } cases[] = {
      {"2nd 12", "%D %H", "60:00:00.000000", false, true},
      {"02 12", "%d %H", "60:00:00.000000", false, true},
      {"2 12", "%e %H", "60:00:00.000000", false, true},
      {"2 12:34:56.123456", "%d %T.%f", "60:34:56.123456", false, true},
      {"2 12:34:56 PM", "%e %r", "60:34:56.000000", false, true},
      {"0 00:00:00", "%d %T", "00:00:00.000000", false, true},
      {"31 23:59:59", "%d %T", "767:59:59.000000", false, true},
      {"2", "%d %H", "48:00:00.000000", false, true},
      {"2 123456", "%d %f", "48:00:00.123456", false, true},
      {"2nd", "%D", "0000-00-02", true, false},
      {"02", "%d", "0000-00-02", true, false},
      {"2", "%e", "0000-00-02", true, false},
      {"2", "%d %%H", "0000-00-02", true, false},
      {"2024-02-29", "%Y-%m-%d %p", "2024-02-29", true, false},
      {"2024-02-02 12", "%Y-%m-%d %H", "2024-02-02 12:00:00.000000", true, true},
      {"2 12", "%d %H %Y", "0000-00-02 12:00:00.000000", true, true},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivilFormat(cases[i].text, (int32_t)strlen(cases[i].text), cases[i].format,
                                (int32_t)strlen(cases[i].format), &civil) == MYSQL_TIME_PARSE_OK);
    CHECK(civil.hasDate == cases[i].hasDate && civil.hasTime == cases[i].hasTime);
    const char* format = civil.hasDate ? (civil.hasTime ? "%Y-%m-%d %H:%i:%s.%f" : "%Y-%m-%d") : "%H:%i:%s.%f";
    char result[64];
    CHECK(tMysqlFormatCivil(&civil, format, (int32_t)strlen(format), result, sizeof(result)) > 0);
    CHECK(strcmp(result, cases[i].expected) == 0);
  }
  const char* invalid[] = {"32 00:00:00", "99 00:00:00", "2 24:00:00", "2 12:60:00", "2 12:00:60"};
  for (int32_t i = 0; i < (int32_t)(sizeof(invalid) / sizeof(invalid[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivilFormat(invalid[i], (int32_t)strlen(invalid[i]), "%d %T", 5, &civil) ==
          MYSQL_TIME_PARSE_INVALID);
  }
  return 0;
}

static int32_t checkFormatParsingEdges(void) {
  const struct {
    const char* text;
    const char* format;
    const char* expected;
  } cases[] = {
      {"2024q02q29", "%Y%q%m%q%d", NULL},
      {"2024%02%29", "%Y%%%m%%%d", NULL},
      {"2024q02q29", "%Yq%mq%d", "2024-02-29"},
      {"2024-02-29 08", "%Y-%m-%d %U", "2024-02-29"},
      {"2024-02-29 08", "%Y-%m-%d %u", "2024-02-29"},
      {"2024-02-29 08", "%Y-%m-%d %V", "2024-02-29"},
      {"2024-02-29 08", "%Y-%m-%d %v", "2024-02-29"},
      {"2023 367", "%Y %j", "2024-01-02"},
      {"2023 999", "%Y %j", "2025-09-25"},
      {"2024-02-29 000", "%Y-%m-%d %j", "2024-02-29"},
      {"2024 000", "%Y %j", "2024-00-00"},
      {"9999 366", "%Y %j", NULL},
      {"9999 999", "%Y %j", NULL},
      {"2024-02-1", "%Y-%m-%D suffix", "2024-02-01"},
      {"2024-02-1x", "%Y-%m-%D suffix", "2024-02-01"},
      {"2024-02-1st", "%Y-%m-%D suffix", "2024-02-01"},
      {"2024-02-1st wrong", "%Y-%m-%D suffix", NULL},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    SMySqlCivilTime civil;
    EMySqlTimeParseResult status = tMysqlParseCivilFormat(
        cases[i].text, (int32_t)strlen(cases[i].text), cases[i].format, (int32_t)strlen(cases[i].format), &civil);
    if (cases[i].expected == NULL) {
      CHECK(status != MYSQL_TIME_PARSE_OK);
      continue;
    }
    CHECK(status == MYSQL_TIME_PARSE_OK);
    char result[32];
    CHECK(tMysqlFormatCivil(&civil, "%Y-%m-%d", 8, result, sizeof(result)) == 10);
    CHECK(strcmp(result, cases[i].expected) == 0);
    CHECK(tMysqlFormatCivil(&civil, "%q", 2, result, sizeof(result)) == 1);
    CHECK(strcmp(result, "q") == 0);
  }
  return 0;
}

static int32_t checkRelaxedDateInputs(void) {
  const struct {
    const char* text;
    const char* expected;
  } cases[] = {
      {"2024/02/29", "2024-02-29 00:00:00.000000"},
      {"24-02-29", "2024-02-29 00:00:00.000000"},
      {"2024^2@29 11*30*45.123456", "2024-02-29 11:30:45.123456"},
      {"2024..02!!29T23^59@58", "2024-02-29 23:59:58.000000"},
      {"24:02:29 12:34:56", "2024-02-29 12:34:56.000000"},
      {"00/02/29", "2000-02-29 00:00:00.000000"},
      {"69/12/31", "2069-12-31 00:00:00.000000"},
      {"70/01/01", "1970-01-01 00:00:00.000000"},
      {"99/12/31", "1999-12-31 00:00:00.000000"},
      {"00/00/00", "0000-00-00 00:00:00.000000"},
      {"00/00/00 00:00:00.000001", "2000-00-00 00:00:00.000001"},
      {"0000/01/01", "0000-01-01 00:00:00.000000"},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    for (int32_t allowTime = 0; allowTime <= 1; ++allowTime) {
      SMySqlCivilTime civil;
      CHECK(tMysqlParseCivil(cases[i].text, (int32_t)strlen(cases[i].text), allowTime, &civil) == MYSQL_TIME_PARSE_OK);
      char result[64];
      const char* format = "%Y-%m-%d %H:%i:%s.%f";
      CHECK(tMysqlFormatCivil(&civil, format, (int32_t)strlen(format), result, sizeof(result)) > 0);
      CHECK(strcmp(result, cases[i].expected) == 0);
    }
  }
  const char* invalid[] = {"23/02/29", "2060/11/31", "24/13/01", "24/01/32", "202/02/29",
                           "24/02/29 24*00*00", "24/02/29 12*60*00", "24/02/29 12*00*60", "24/02/29junk"};
  for (int32_t i = 0; i < (int32_t)(sizeof(invalid) / sizeof(invalid[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivil(invalid[i], (int32_t)strlen(invalid[i]), false, &civil) != MYSQL_TIME_PARSE_OK);
  }
  SMySqlCivilTime time;
  CHECK(tMysqlParseCivil("12:34:56.123456", 15, true, &time) == MYSQL_TIME_PARSE_OK);
  CHECK(!time.hasDate && time.hour == 12 && time.microsecond == 123456);
  CHECK(tMysqlParseCivil("24:02:29", 8, true, &time) == MYSQL_TIME_PARSE_OK);
  CHECK(!time.hasDate && time.hour == 24);
  return 0;
}

static int32_t checkTimeRangeClipping(void) {
  const char* clipped[] = {"838:59:59.123456", "839:00:00", "-839:00:00", "1000:00:00", "35 00:00:00",
                           "999999:00:00", "1000000:00:00", "-1000000:00:00.123456",
                           "4294967295:00:00", "00000001000000:00:00", "1000000 23:59:59"};
  for (int32_t i = 0; i < (int32_t)(sizeof(clipped) / sizeof(clipped[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivil(clipped[i], (int32_t)strlen(clipped[i]), true, &civil) == MYSQL_TIME_PARSE_OK);
    CHECK(!civil.hasDate && civil.hasTime);
    CHECK(civil.hour == 838 && civil.minute == 59 && civil.second == 59 && civil.microsecond == 0);
    CHECK(civil.negativeTime == (clipped[i][0] == '-'));
  }
  const char* invalid[] = {"839:60:00", "839:00:60", "839:00:00junk", "1000000:60:00", "-1000000:00:60",
                           "1000000:00:00junk", "4294967296:00:00", "999999999999999999999999:00:00"};
  for (int32_t i = 0; i < (int32_t)(sizeof(invalid) / sizeof(invalid[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivil(invalid[i], (int32_t)strlen(invalid[i]), true, &civil) == MYSQL_TIME_PARSE_INVALID);
  }
  SMySqlCivilTime civil;
  CHECK(tMysqlParseCivil("838:59:58.999999", 16, true, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.second == 58 && civil.microsecond == 999999);
  return 0;
}

static int32_t checkTextFractionRounding(void) {
  const struct {
    const char* text;
    const char* expected;
  } cases[] = {
      {"2024-02-29 12:34:56.123456", "2024-02-29 12:34:56.123456"},
      {"2024-02-29 12:34:56.1234564999", "2024-02-29 12:34:56.123456"},
      {"2024-02-29 12:34:56.1234565", "2024-02-29 12:34:56.123457"},
      {"2024-02-29 12:34:56.9999995", "2024-02-29 12:34:57.000000"},
      {"2024-02-29 12:59:59.9999995", "2024-02-29 13:00:00.000000"},
      {"2024-02-28 23:59:59.9999995", "2024-02-29 00:00:00.000000"},
      {"2023-02-28 23:59:59.9999995", "2023-03-01 00:00:00.000000"},
      {"2024-02-29 23:59:59.9999995", "2024-03-01 00:00:00.000000"},
      {"2024-12-31 23:59:59.9999995", "2025-01-01 00:00:00.000000"},
      {"24/12/31 23*59*59.9999995", "2025-01-01 00:00:00.000000"},
      {"9999-12-31 23:59:59.9999994", "9999-12-31 23:59:59.999999"},
      {"0000-00-00 00:00:00.0000005", "0000-00-00 00:00:00.000001"},
      {"00/00/00 00:00:00.0000005", "0000-00-00 00:00:00.000001"},
      {"2024-00-01 00:00:00.1234565", "2024-00-01 00:00:00.123457"},
      {"0000-01-01 00:00:00.9999995", "0000-00-00 00:00:01.000000"},
      {"0000-12-31 23:59:59.9999995", "0001-01-01 00:00:00.000000"},
      {"9999-12-31 23:59:59.9999995", NULL},
      {"0000-00-00 00:00:00.9999995", NULL},
      {"2024-00-01 00:00:00.9999995", NULL},
      {"2024-01-00 00:00:00.9999995", NULL},
      {"2060-11-31 23:59:59.9999995", NULL},
      {"2024-02-29 23:59:60.9999995", NULL},
      {"2024-12-31 23:59:59.9999995junk", NULL},
  };
  const char* format = "%Y-%m-%d %H:%i:%s.%f";
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    SMySqlCivilTime civil;
    EMySqlTimeParseResult status = tMysqlParseCivil(cases[i].text, (int32_t)strlen(cases[i].text), false, &civil);
    if (cases[i].expected == NULL) {
      CHECK(status != MYSQL_TIME_PARSE_OK);
      continue;
    }
    CHECK(status == MYSQL_TIME_PARSE_OK);
    char result[64];
    CHECK(tMysqlFormatCivil(&civil, format, (int32_t)strlen(format), result, sizeof(result)) > 0);
    CHECK(strcmp(result, cases[i].expected) == 0);
  }
  const struct {
    const char* text;
    int32_t hour;
    int32_t minute;
    int32_t second;
    int32_t microsecond;
  } times[] = {
      {"12:34:56.1234565", 12, 34, 56, 123457},
      {"-00:00:00.0000005", 0, 0, 0, 1},
      {"-23:59:59.9999995", 24, 0, 0, 0},
      {"2 23:59:59.9999995", 72, 0, 0, 0},
      {"838:59:58.9999995", 838, 59, 59, 0},
      {"838:59:59.0000005", 838, 59, 59, 0},
      {"-838:59:59.9999995", 838, 59, 59, 0},
      {"1000000:59:59.9999995", 838, 59, 59, 0},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(times) / sizeof(times[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivil(times[i].text, (int32_t)strlen(times[i].text), true, &civil) == MYSQL_TIME_PARSE_OK);
    CHECK(!civil.hasDate && civil.hasTime && civil.negativeTime == (times[i].text[0] == '-'));
    CHECK(civil.hour == times[i].hour && civil.minute == times[i].minute && civil.second == times[i].second);
    CHECK(civil.microsecond == times[i].microsecond);
  }
  // TIME conversion retains calendar fields and carries midnight into hour 24.
  const struct {
    const char* text;
    int32_t year;
    int32_t month;
    int32_t day;
    int32_t hour;
    int32_t second;
  } timeDates[] = {
      {"2024-12-31 23:59:59.9999995", 2024, 12, 31, 24, 0},
      {"9999-12-31 23:59:59.9999995", 9999, 12, 31, 24, 0},
      {"0000-00-00 00:00:00.9999995", 0, 0, 0, 0, 1},
      {"2024-00-01 00:00:00.9999995", 2024, 0, 1, 0, 1},
      {"0000-01-01 00:00:00.9999995", 0, 1, 1, 0, 1},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(timeDates) / sizeof(timeDates[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlParseCivil(timeDates[i].text, (int32_t)strlen(timeDates[i].text), true, &civil) == MYSQL_TIME_PARSE_OK);
    CHECK(civil.hasDate && civil.hasTime);
    CHECK(civil.year == timeDates[i].year && civil.month == timeDates[i].month && civil.day == timeDates[i].day);
    CHECK(civil.hour == timeDates[i].hour && civil.minute == 0 && civil.second == timeDates[i].second);
    CHECK(civil.microsecond == 0);
  }
  // STR_TO_DATE consumes a six-character %f field and ignores trailing input.
  SMySqlCivilTime civil;
  const char* input = "2024-12-31 23:59:59.9999995";
  CHECK(tMysqlParseCivilFormat(input, (int32_t)strlen(input), format, (int32_t)strlen(format), &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2024 && civil.month == 12 && civil.day == 31 && civil.microsecond == 999999);
  return 0;
}

static int32_t checkFormatNumberSigns(void) {
  const struct {
    const char* text;
    const char* format;
    const char* expected;
  } cases[] = {
      {"2024-+2-29", "%Y-%m-%d", "2024-02-29"},
      {"2024-02-+9", "%Y-%m-%d", "2024-02-09"},
      {"+024-02-29", "%Y-%m-%d", "0024-02-29"},
      {"+4-02-29", "%Y-%m-%d", "2004-02-29"},
      {"+4-02-29", "%y-%m-%d", "2004-02-29"},
      {"2024 +60", "%Y %j", "2024-02-29"},
      {"2024-02-29 +8", "%Y-%m-%d %U", "2024-02-29"},
      {"+2:+3:+4", "%H:%i:%s", "02:03:04.000000"},
      {"+2 PM", "%h %p", "14:00:00.000000"},
      {"+1", "%f", "00:00:00.010000"},
      {"+12345", "%f", "00:00:00.012345"},
      {"2024-+02-29", "%Y-%m-%d", NULL},
      {"2024-+2-30", "%Y-%m-%d", NULL},
      {"2024--2-29", "%Y-%m-%d", NULL},
      {"-1:00:00", "%H:%i:%s", NULL},
      {"+:00:00", "%H:%i:%s", NULL},
      {"+12:00:00", "%H:%i:%s", NULL},
      {"+6", "%w", NULL},
      {"+", "%m", NULL},
      {"++2", "%m", NULL},
  };
  for (int32_t i = 0; i < (int32_t)(sizeof(cases) / sizeof(cases[0])); ++i) {
    SMySqlCivilTime civil;
    EMySqlTimeParseResult status = tMysqlParseCivilFormat(
        cases[i].text, (int32_t)strlen(cases[i].text), cases[i].format, (int32_t)strlen(cases[i].format), &civil);
    if (cases[i].expected == NULL) {
      CHECK(status != MYSQL_TIME_PARSE_OK);
      continue;
    }
    CHECK(status == MYSQL_TIME_PARSE_OK);
    const char* format = civil.hasDate ? "%Y-%m-%d" : "%H:%i:%s.%f";
    char result[64];
    CHECK(tMysqlFormatCivil(&civil, format, (int32_t)strlen(format), result, sizeof(result)) > 0);
    CHECK(strcmp(result, cases[i].expected) == 0);
  }
  return 0;
}

int main(void) {
  CHECK(checkTextFractionRounding() == 0);
  CHECK(checkTimeRangeClipping() == 0);
  CHECK(checkFormatNumberSigns() == 0);
  CHECK(checkFormatParsingEdges() == 0);
  CHECK(checkRelaxedDateInputs() == 0);
  CHECK(tMysqlDayNumber(0, 1, 1) == 1);
  CHECK(tMysqlDayNumber(1970, 1, 1) == 719528);
  CHECK(tMysqlDayNumber(2024, 2, 29) == tMysqlDayNumber(2024, 3, 1) - 1);
  CHECK(tMysqlDayNumber(2023, 2, 29) < 0);
  CHECK(tMysqlDayNumber(0, 2, 29) < 0);
  CHECK(tMysqlDayNumber(0, 3, 1) == 60);
  CHECK(tMysqlDayNumber(9999, 12, 31) == 3652424);

  const int64_t days[] = {1, 59, 60, 719528, 3652424};
  for (int32_t i = 0; i < (int32_t)(sizeof(days) / sizeof(days[0])); ++i) {
    SMySqlCivilTime civil;
    CHECK(tMysqlFromDayNumber(days[i], &civil));
    CHECK(tMysqlDayNumber(civil.year, civil.month, civil.day) == days[i]);
  }
  SMySqlCivilTime civil;
  CHECK(!tMysqlFromDayNumber(0, &civil));
  CHECK(!tMysqlFromDayNumber(3652425, &civil));

  CHECK(tMysqlParseCivil("2000-01-01", 10, false, &civil) == MYSQL_TIME_PARSE_OK);
  int32_t year = 0;
  int32_t week = 0;
  CHECK(tMysqlWeekYearAndNumber(&civil, 2, &year, &week));
  CHECK(year == 1999 && week == 52);
  for (int32_t mode = 0; mode <= 7; ++mode) {
    CHECK(tMysqlWeekYearAndNumber(&civil, mode, &year, &week));
    CHECK(week >= 0 && week <= 53);
  }
  CHECK(!tMysqlWeekYearAndNumber(&civil, 8, &year, &week));
  CHECK(tMysqlParseCivil("0000-01-01", 10, false, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(tMysqlWeekYearAndNumber(&civil, 0, &year, &week) && year == 0 && week == 1);
  CHECK(tMysqlWeekYearAndNumber(&civil, 1, &year, &week) && year == 0 && week == 0);
  CHECK(tMysqlWeekYearAndNumber(&civil, 3, &year, &week) && year == -1 && week == 52);
  char yearZeroWeek[64];
  CHECK(tMysqlFormatCivil(&civil, "%U %u %V %v %X %x", 17, yearZeroWeek, sizeof(yearZeroWeek)) > 0);
  CHECK(strcmp(yearZeroWeek, "01 00 01 52 0000 4294967295") == 0);

  CHECK(tMysqlParseCivil("-272:59:59.012345", 17, true, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.negativeTime && civil.hour == 272 && civil.microsecond == 12345);
  CHECK(tMysqlParseCivil("839:00:00", 9, true, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hour == 838 && civil.minute == 59 && civil.second == 59);
  CHECK(tMysqlParseCivil("2021-11-31", 10, false, &civil) == MYSQL_TIME_PARSE_INVALID_COMPLETE_DATE);
  CHECK(tMysqlParseCivil("2024-02-29 23:59:59.123456", 26, false, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.microsecond == 123456);

  char        result[128];
  const char* pattern = "%Y-%m-%d %H:%i:%s.%f %W %j";
  int32_t     len = tMysqlFormatCivil(&civil, pattern, (int32_t)strlen(pattern), result, sizeof(result));
  CHECK(len > 0 && strcmp(result, "2024-02-29 23:59:59.123456 Thursday 060") == 0);
  CHECK(tMysqlFormatCivil(&civil, pattern, (int32_t)strlen(pattern), result, 8) < 0);
  SMySqlCivilTime zeroMonth = {.year = 2024, .day = 10, .hasDate = true};
  CHECK(tMysqlFormatCivil(&zeroMonth, "%M", 2, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  CHECK(tMysqlFormatCivil(&zeroMonth, "%b", 2, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  SMySqlCivilTime zeroDate = {.hasDate = true};
  CHECK(tMysqlFormatCivil(&zeroDate, "%W", 2, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  CHECK(tMysqlFormatCivil(&zeroDate, "%a", 2, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  CHECK(tMysqlFormatCivil(&zeroDate, "%w", 2, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  CHECK(tMysqlFormatCivil(&zeroDate, "%Y %W", 5, result, sizeof(result)) == MYSQL_TIME_FORMAT_NULL);
  CHECK(tMysqlFormatCivil(&zeroDate, "%Y-%m-%d", 8, result, sizeof(result)) == 10);
  CHECK(strcmp(result, "0000-00-00") == 0);

  char longPattern[MYSQL_TIME_MAX_TEXT_BYTES + 1];
  char longResult[MYSQL_TIME_MAX_FORMAT_RESULT_BYTES + 1];
  for (int32_t i = 0; i < MYSQL_TIME_MAX_TEXT_BYTES / 2; ++i) {
    longPattern[2 * i] = '%';
    longPattern[2 * i + 1] = 'r';
  }
  longPattern[MYSQL_TIME_MAX_TEXT_BYTES - 1] = 'x';
  longPattern[MYSQL_TIME_MAX_TEXT_BYTES] = '\0';
  CHECK(tMysqlFormatCivil(&civil, longPattern, MYSQL_TIME_MAX_TEXT_BYTES, longResult, sizeof(longResult)) ==
        MYSQL_TIME_MAX_FORMAT_RESULT_BYTES);
  CHECK(strncmp(longResult, "11:59:59 PM", 11) == 0);
  CHECK(longResult[MYSQL_TIME_MAX_FORMAT_RESULT_BYTES - 1] == 'x');

  CHECK(tMysqlParseCivilFormat("May 1, 2013extra", 16, "%M %d, %Y", 9, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2013 && civil.month == 5 && civil.day == 1);
  const char* partialFormat = "%Y-%m-%d %H:%i:%s";
  CHECK(tMysqlParseCivilFormat("2024-02-29", 10, partialFormat, (int32_t)strlen(partialFormat), &civil) ==
        MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasDate && civil.hasTime && civil.year == 2024 && civil.month == 2 && civil.day == 29);
  CHECK(civil.hour == 0 && civil.minute == 0 && civil.second == 0);
  CHECK(tMysqlParseCivilFormat("2024-02-29 \t", 12, partialFormat, (int32_t)strlen(partialFormat), &civil) ==
        MYSQL_TIME_PARSE_OK);
  CHECK(tMysqlParseCivilFormat("12", 2, "%H:%i:%s", 8, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(!civil.hasDate && civil.hasTime && civil.hour == 12 && civil.minute == 0 && civil.second == 0);
  CHECK(tMysqlParseCivilFormat("12:34", 5, "%T", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasTime && civil.hour == 12 && civil.minute == 34 && civil.second == 0);
  CHECK(tMysqlParseCivilFormat("12", 2, "%r", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasTime && civil.hour == 0);
  CHECK(tMysqlParseCivilFormat("2024-02-29x", 11, partialFormat, (int32_t)strlen(partialFormat), &civil) ==
        MYSQL_TIME_PARSE_INVALID);
  CHECK(tMysqlParseCivilFormat("2024-02-29", 10, "%Y-%m-%d %%H", 12, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasDate && !civil.hasTime);
  CHECK(tMysqlParseCivilFormat("2021-11-31", 10, "%Y-%m-%d", 8, &civil) == MYSQL_TIME_PARSE_INVALID_COMPLETE_DATE);
  CHECK(tMysqlParseCivilFormat("2024 060", 8, "%Y %j", 5, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2024 && civil.month == 2 && civil.day == 29);
  CHECK(tMysqlParseCivilFormat("2023 366", 8, "%Y %j", 5, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2024 && civil.month == 1 && civil.day == 1);
  CHECK(tMysqlParseCivilFormat("24-02-29", 8, "%Y-%m-%d", 8, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2024 && civil.month == 2 && civil.day == 29);
  CHECK(tMysqlParseCivilFormat("69", 2, "%Y", 2, &civil) == MYSQL_TIME_PARSE_OK && civil.year == 2069);
  CHECK(tMysqlParseCivilFormat("70", 2, "%Y", 2, &civil) == MYSQL_TIME_PARSE_OK && civil.year == 1970);
  CHECK(tMysqlParseCivilFormat(" 2024  \t02  29", 14, "%Y %m %d", 8, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2024 && civil.month == 2 && civil.day == 29);
  CHECK(tMysqlParseCivilFormat("12:34:56.123456", 15, "%T.%f", 5, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hour == 12 && civil.minute == 34 && civil.second == 56 && civil.microsecond == 123456);
  CHECK(tMysqlParseCivilFormat("11 PM", 5, "%H %p", 5, &civil) == MYSQL_TIME_PARSE_INVALID);
  CHECK(tMysqlParseCivilFormat("11 PM", 5, "%h %p", 5, &civil) == MYSQL_TIME_PARSE_OK && civil.hour == 23);
  CHECK(tMysqlParseCivilFormat("12:34:56 PM", 11, "%r", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hour == 12 && civil.minute == 34 && civil.second == 56);
  CHECK(tMysqlParseCivilFormat("12", 2, "%h", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hour == 0);
  CHECK(tMysqlParseCivilFormat("11", 2, "%I", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hour == 11);
  CHECK(tMysqlParseCivilFormat("23", 2, "%l", 2, &civil) == MYSQL_TIME_PARSE_INVALID);
  CHECK(tMysqlParseCivilFormat("bad", 3, "%Y-%m-%d", 8, &civil) == MYSQL_TIME_PARSE_INVALID);
  CHECK(tMysqlParseCivilFormat("2nd", 3, "%D", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(tMysqlParseCivilFormat("2xx", 3, "%D", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(tMysqlParseCivilFormat("2", 1, "%D", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(tMysqlParseCivilFormat("abc", 3, "abc", 3, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasDate && !civil.hasTime && civil.year == 0 && civil.month == 0 && civil.day == 0);
  CHECK(tMysqlParseCivilFormat("Monday", 6, "%W", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasDate && !civil.hasTime);
  CHECK(tMysqlParseCivilFormat("1", 1, "%w", 2, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.hasDate && !civil.hasTime);
  CHECK(tMysqlParseCivilFormat("2021 53 1", 9, "%x %v %w", 8, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2022 && civil.month == 1 && civil.day == 3);
  CHECK(tMysqlParseCivilFormat("2024 00 0", 9, "%Y %U %w", 8, &civil) == MYSQL_TIME_PARSE_OK);
  CHECK(civil.year == 2023 && civil.month == 12 && civil.day == 31);
  CHECK(tMysqlParseCivilFormat("2024 01 1", 9, "%Y %V %w", 8, &civil) == MYSQL_TIME_PARSE_INVALID);
  CHECK(tMysqlParseCivilFormat("2024 01 1", 9, "%X %v %w", 8, &civil) == MYSQL_TIME_PARSE_INVALID);
  CHECK(checkWeekFormatRoundTrips() == 0);
  CHECK(checkDayTimeFormats() == 0);

  puts("tmysqltime tests passed");
  return 0;
}
