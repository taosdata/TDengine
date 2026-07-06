/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

/*
 * Unit tests for taosValidateAndNormalizeTimezone().
 * Covers: IANA names, fixed offsets, UTC±N, Windows names, GMT rejection,
 * ambiguous abbreviation rejection, and edge cases.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "os.h"
#include "ttime.h"

static int passed = 0;
static int failed = 0;

#define EXPECT_OK(input, expectedNorm) do { \
  char buf[TD_TIMEZONE_LEN] = {0}; \
  int32_t rc = taosValidateAndNormalizeTimezone((input), buf, sizeof(buf), NULL); \
  if (rc != TSDB_CODE_SUCCESS) { \
    fprintf(stderr, "FAIL: '%s' expected OK, got 0x%x\n", (input), rc); \
    failed++; \
  } else if (strcmp(buf, (expectedNorm)) != 0) { \
    fprintf(stderr, "FAIL: '%s' expected norm='%s', got '%s'\n", (input), (expectedNorm), buf); \
    failed++; \
  } else { \
    passed++; \
  } \
} while(0)

#define EXPECT_FAIL(input) do { \
  char buf[TD_TIMEZONE_LEN] = {0}; \
  int32_t rc = taosValidateAndNormalizeTimezone((input), buf, sizeof(buf), NULL); \
  if (rc == TSDB_CODE_SUCCESS) { \
    fprintf(stderr, "FAIL: '%s' expected rejection, but got OK norm='%s'\n", (input), buf); \
    failed++; \
  } else { \
    passed++; \
  } \
} while(0)

int main(void) {
  /* Must initialize timezone info for tzalloc to work */
  initTimezoneInfo();

  /* --- IANA names --- */
  EXPECT_OK("Asia/Shanghai", "Asia/Shanghai");
  EXPECT_OK("America/New_York", "America/New_York");
  EXPECT_OK("Europe/London", "Europe/London");
  EXPECT_OK("Etc/UTC", "Etc/UTC");

  /* --- UTC / Z --- */
  EXPECT_OK("UTC", "UTC");
  EXPECT_OK("utc", "UTC");
  EXPECT_OK("Z", "UTC");
  EXPECT_OK("z", "UTC");

  /* --- Bare fixed-offset (two-digit hours required, POSIX sign: +=west) --- */
  EXPECT_OK("+08:00", "UTC+8");        /* +08:00 = west 8 = POSIX UTC+8 */
  EXPECT_OK("-05:00", "UTC-5");        /* -05:00 = east 5 = POSIX UTC-5 */
  EXPECT_OK("+05:30", "UTC+5:30");     /* half-hour offset */
  EXPECT_OK("+0800", "UTC+8");         /* HHMM format */
  EXPECT_OK("-0530", "UTC-5:30");
  EXPECT_OK("+08", "UTC+8");           /* HH only */
  EXPECT_OK("-05", "UTC-5");
  EXPECT_OK("+00:00", "UTC+0");        /* edge: zero offset */

  /* --- UTC± short form (POSIX sign: +=west, keep as-is) --- */
  EXPECT_OK("UTC-8", "UTC-8");         /* east 8 */
  EXPECT_OK("UTC+10", "UTC+10");       /* west 10 */
  EXPECT_OK("UTC-0", "UTC-0");

  /* --- UTC± long form (sign preserved, format simplified) --- */
  EXPECT_OK("UTC+08:00", "UTC+8");     /* west 8 */
  EXPECT_OK("UTC-05:30", "UTC-5:30");  /* east 5:30 */
  EXPECT_OK("UTC+0530", "UTC+5:30");   /* west 5:30 */
  EXPECT_OK("UTC+0800", "UTC+8");      /* west 8 */
  EXPECT_OK("UTC-0800", "UTC-8");      /* east 8 */

  /* --- Windows canonical names --- */
  EXPECT_OK("China Standard Time", "Asia/Shanghai");
  EXPECT_OK("Eastern Standard Time", "America/New_York");
  EXPECT_OK("India Standard Time", "Asia/Calcutta");
  EXPECT_OK("Tokyo Standard Time", "Asia/Tokyo");

  /* --- GMT series: REJECTED --- */
  EXPECT_FAIL("GMT");
  EXPECT_FAIL("GMT+8");
  EXPECT_FAIL("GMT-5");
  EXPECT_FAIL("GMT+08:00");
  EXPECT_FAIL("gmt");
  EXPECT_FAIL("Gmt+3");

  /* --- Ambiguous abbreviations: REJECTED --- */
  EXPECT_FAIL("CST");
  EXPECT_FAIL("EST");
  EXPECT_FAIL("PST");
  EXPECT_FAIL("IST");

  /* --- Single-digit hours: REJECTED --- */
  EXPECT_FAIL("+8");
  EXPECT_FAIL("-5");

  /* --- Invalid formats --- */
  EXPECT_FAIL("");
  EXPECT_FAIL("NotATimezone");
  EXPECT_FAIL("ABC/DEF");  /* nonexistent IANA */

  /* --- Edge cases --- */
  EXPECT_OK("+14:00", "UTC-14");       /* max valid offset */
  EXPECT_FAIL("+15:00");               /* beyond max */
  EXPECT_FAIL("+08:60");               /* invalid minutes */

  printf("\n=== timezone_normalize_test: %d passed, %d failed ===\n", passed, failed);

  cleanupTimezoneInfo();
  return failed > 0 ? 1 : 0;
}
