/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
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

// This executable intentionally overrides the C runtime clock.
#define ALLOW_FORBID_FUNC

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <initializer_list>

#include "taos.h"
#include "tglobal.h"
#include "ttime.h"

// Only seconds are simulated. RPC deadlines and sleeps keep their real clocks.
static std::atomic<time_t> testNow(0);

static time_t testTime(time_t* out) {
  time_t stamp = testNow.load();
  if (out != nullptr) {
    *out = stamp;
  }
  return stamp;
}

#ifdef _WIN32
// MSVC's inline time() calls the CRT through this import pointer. The static
// client resolves it here, without patching the CRT or other processes.
extern "C" {
__time64_t(__cdecl* __imp__time64)(__time64_t*) = testTime;
}
#else
// Static TDengine objects bind to this executable's time() on Linux/macOS.
extern "C" time_t time(time_t* out)
#ifdef __linux__
    noexcept
#endif
{
  return testTime(out);
}
#endif

static bool checkTimezone(TAOS* conn, const char* sql, const char* expected) {
  TAOS_RES* result = taos_query(conn, sql);
  if (taos_errno(result) != 0) {
    fprintf(stderr, "%s failed: %s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return false;
  }
  TAOS_ROW row = taos_fetch_row(result);
  int*     lengths = row != nullptr ? taos_fetch_lengths(result) : nullptr;
  bool     matches = lengths != nullptr && row[0] != nullptr && lengths[0] == static_cast<int>(strlen(expected)) &&
                     memcmp(row[0], expected, lengths[0]) == 0;
  if (lengths != nullptr && row[0] != nullptr) {
    printf("clock=%lld sql=%s actual=%.*s expected=%s\n", static_cast<long long>(testNow.load()), sql, lengths[0],
           static_cast<const char*>(row[0]), expected);
  } else {
    fprintf(stderr, "%s returned no timezone value\n", sql);
  }
  if (taos_fetch_row(result) != nullptr || taos_errno(result) != 0) {
    matches = false;
  }
  taos_free_result(result);
  return matches;
}

static bool checkTimestampFormatting(TAOS* conn, time_t stamp, const char* expected) {
  // The expected offset comes from the test vector, not the timezone APIs
  // under test. Shift UTC by that fixed offset to derive the expected text.
  const char* offset = strrchr(expected, ',');
  char sign = 0;
  int hours = 0, minutes = 0;
  if (offset == nullptr || sscanf(offset, ", %c%2d%2d)", &sign, &hours, &minutes) != 3 ||
      (sign != '+' && sign != '-') || hours > 14 || minutes > 59) {
    fprintf(stderr, "invalid expected timezone: %s\n", expected);
    return false;
  }
  int seconds = (hours * 60 + minutes) * 60;
  time_t shifted = stamp + (sign == '-' ? -seconds : seconds);
  struct tm local = {};
  if (taosGmTimeR(&shifted, &local) == nullptr) return false;

  char date[96] = {0};
  snprintf(date, sizeof(date), "%04d-%02d-%02d %02d:%02d:%02d",
           local.tm_year + 1900, local.tm_mon + 1, local.tm_mday,
           local.tm_hour, local.tm_min, local.tm_sec);
  char expectedChar[128] = {0};
  snprintf(expectedChar, sizeof(expectedChar), "%s %c%02d:%02d", date, sign, hours, minutes);
  date[10] = 'T';
  char expectedIso[128] = {0};
  snprintf(expectedIso, sizeof(expectedIso), "%s.000%c%02d%02d", date, sign, hours, minutes);

  for (const char* from : {"", " from information_schema.ins_dnodes limit 1"}) {
    char sql[256] = {0};
    snprintf(sql, sizeof(sql),
             "select to_char(cast(%lld as timestamp), 'YYYY-MM-DD HH24:MI:SS TZ')%s",
             static_cast<long long>(stamp) * 1000, from);
    if (!checkTimezone(conn, sql, expectedChar)) return false;
    snprintf(sql, sizeof(sql), "select to_iso8601(cast(%lld as timestamp))%s",
             static_cast<long long>(stamp) * 1000, from);
    if (!checkTimezone(conn, sql, expectedIso)) return false;
  }
  return true;
}

static bool checkAt(TAOS* conn, time_t stamp, const char* expected) {
  testNow.store(stamp);
  if (time(nullptr) != stamp) {
    fprintf(stderr, "test clock override is inactive\n");
    return false;
  }
  // Check both the no-FROM rewrite and a query that fetches a server row.
  return checkTimezone(conn, "select timezone()", expected) && checkTimezone(conn,
                                                                             "select timezone() from "
                                                                             "information_schema.ins_dnodes limit 1",
                                                                             expected) &&
         checkTimestampFormatting(conn, stamp, expected);
}

static bool setSessionTimezone(TAOS* conn, const char* zone) {
  char sql[256] = {0};
  int  size = snprintf(sql, sizeof(sql), "set timezone '%s'", zone);
  if (size < 0 || size >= static_cast<int>(sizeof(sql))) {
    return false;
  }
  TAOS_RES* result = taos_query(conn, sql);
  bool      ok = taos_errno(result) == 0;
  if (!ok) {
    fprintf(stderr, "%s failed: %s\n", sql, taos_errstr(result));
  }
  taos_free_result(result);
  return ok;
}

static bool executeSql(TAOS* conn, const char* sql) {
  TAOS_RES* result = taos_query(conn, sql);
  bool      ok = taos_errno(result) == 0;
  if (!ok) fprintf(stderr, "%s failed: %s\n", sql, taos_errstr(result));
  taos_free_result(result);
  return ok;
}

static bool checkStatementTimezone(TAOS* conn, int64_t expected) {
  const char*     sql = "select ts from db_windows_tz_stmt.t where ts = cast(? as timestamp)";
  char            text[] = "2024-01-15 12:00:00";
  int32_t         length = static_cast<int32_t>(strlen(text));
  TAOS_MULTI_BIND bind = {};
  bind.buffer_type = TSDB_DATA_TYPE_BINARY;
  bind.buffer = text;
  bind.buffer_length = sizeof(text);
  bind.length = &length;
  bind.num = 1;
  TAOS_STMT* stmt = taos_stmt_init(conn);
  if (stmt == nullptr) return false;
  bool ok =
      taos_stmt_prepare(stmt, sql, 0) == 0 && taos_stmt_bind_param(stmt, &bind) == 0 && taos_stmt_execute(stmt) == 0;
  TAOS_RES* result = ok ? taos_stmt_use_result(stmt) : nullptr;
  TAOS_ROW  row = result != nullptr ? taos_fetch_row(result) : nullptr;
  ok = ok && row != nullptr && row[0] != nullptr && *static_cast<int64_t*>(row[0]) == expected;
  if (!ok) fprintf(stderr, "STMT timezone check failed: %s\n", taos_stmt_errstr(stmt));
  // The statement owns its result and releases it on close.
  taos_stmt_close(stmt);

  TAOS_STMT2_OPTION option = {};
  TAOS_STMT2*       stmt2 = taos_stmt2_init(conn, &option);
  if (stmt2 == nullptr) return false;
  TAOS_STMT2_BIND bind2 = {};
  bind2.buffer_type = TSDB_DATA_TYPE_BINARY;
  bind2.buffer = text;
  bind2.length = &length;
  bind2.num = 1;
  TAOS_STMT2_BIND* param = &bind2;
  TAOS_STMT2_BINDV bindv = {1, nullptr, nullptr, &param};
  bool             ok2 = taos_stmt2_prepare(stmt2, sql, 0) == 0 && taos_stmt2_bind_param(stmt2, &bindv, -1) == 0 &&
                         taos_stmt2_exec(stmt2, nullptr) == 0;
  result = ok2 ? taos_stmt2_result(stmt2) : nullptr;
  row = result != nullptr ? taos_fetch_row(result) : nullptr;
  ok2 = ok2 && row != nullptr && row[0] != nullptr && *static_cast<int64_t*>(row[0]) == expected;
  if (!ok2) fprintf(stderr, "STMT2 timezone check failed: %s\n", taos_stmt2_error(stmt2));
  taos_stmt2_close(stmt2);
  return ok && ok2;
}

static bool checkConnectionBehavior(TAOS* conn, const char* host, uint16_t port) {
  TAOS* other = taos_connect(host, "root", "taosdata", nullptr, port);
  if (other == nullptr) return false;
  bool ok = setSessionTimezone(conn, "America/New_York");
  ok = ok && executeSql(conn, "create database if not exists db_windows_tz_stmt vgroups 1");
  ok = ok && executeSql(conn, "create table if not exists db_windows_tz_stmt.t (ts timestamp, v int)");
  ok = ok && executeSql(conn, "insert into db_windows_tz_stmt.t values(1705338000000, 1)(1705291200000, 2)");
  ok = ok && checkStatementTimezone(conn, 1705338000000LL);
  TAOS_RES*  old = taos_query(conn, "select cast(1705320000000 as timestamp)");
  timezone_t original = static_cast<timezone_t>(taos_get_result_tz(old));
  ok = ok && taos_errno(old) == 0 && original != nullptr;
  ok = ok && taos_options_connection(conn, TSDB_OPTION_CONNECTION_TIMEZONE, "China Standard Time") == 0;
  ok = ok && checkTimezone(conn, "select timezone()", "Asia/Shanghai (CST, +0800)");
  ok = ok && checkStatementTimezone(conn, 1705291200000LL);
  ok = ok && taos_options_connection(conn, TSDB_OPTION_CONNECTION_TIMEZONE, "No/Such_Zone") != 0;
  ok = ok && checkTimezone(conn, "select timezone()", "Asia/Shanghai (CST, +0800)");
  int64_t offset = 0;
  ok = ok && taosGetTimezoneOffsetAtSeconds(1705320000, original, &offset) == 0 && offset == -5 * 3600;
  ok = ok && taosGetTimezoneOffsetAtSeconds(1721044800, original, &offset) == 0 && offset == -4 * 3600;
  TAOS_ROW oldRow = taos_fetch_row(old);
  ok = ok && oldRow != nullptr && oldRow[0] != nullptr && *static_cast<int64_t*>(oldRow[0]) == 1705320000000LL;
  taos_free_result(old);
  ok = ok && taos_options_connection(conn, TSDB_OPTION_CONNECTION_TIMEZONE, "") == 0;
  ok = ok && checkTimezone(conn, "select timezone()", "UTC (UTC, +0000)");
  ok = ok && executeSql(conn, "alter local 'timezone' 'Asia/Shanghai'");
  ok = ok && checkTimezone(conn, "select timezone()", "Asia/Shanghai (CST, +0800)");
  ok = ok && checkTimezone(other, "select timezone()", "UTC (UTC, +0000)");
  // CLEAR and NULL snapshot the current default, independently of the session.
  ok = ok && setSessionTimezone(conn, "Asia/Kolkata");
  ok = ok && taos_options_connection(conn, TSDB_OPTION_CONNECTION_CLEAR,
                                     nullptr) == 0;
  ok = ok && taos_options(TSDB_OPTION_TIMEZONE, "UTC") == 0;
  ok = ok && checkTimezone(conn, "select timezone()", "Asia/Shanghai (CST, +0800)");
  ok = ok && setSessionTimezone(conn, "Asia/Kolkata");
  ok = ok && taos_options(TSDB_OPTION_TIMEZONE, "Asia/Kathmandu") == 0;
  ok = ok && taos_options_connection(conn, TSDB_OPTION_CONNECTION_TIMEZONE,
                                     nullptr) == 0;
  ok = ok && taos_options(TSDB_OPTION_TIMEZONE, "UTC") == 0;
  ok = ok && checkTimezone(conn, "select timezone()",
                           "Asia/Kathmandu (+0545, +0545)");

  OPTIONS options = {};
  char    portText[16] = {0};
  snprintf(portText, sizeof(portText), "%u", static_cast<unsigned>(port));
  taos_set_option(&options, "ip", host);
  taos_set_option(&options, "user", "root");
  taos_set_option(&options, "pass", "taosdata");
  taos_set_option(&options, "port", portText);
  taos_set_option(&options, "timezone", "China Standard Time");
  TAOS* configured = taos_connect_with(&options);
  ok = ok && configured != nullptr;
  if (configured != nullptr) {
    ok = ok && checkTimezone(configured, "select timezone()", "Asia/Shanghai (CST, +0800)");
    taos_close(configured);
  }
  taos_close(other);
  executeSql(conn, "drop database if exists db_windows_tz_stmt");
  return ok;
}

int main(int argc, char** argv) {
  if (argc != 9) {
    fprintf(stderr,
            "usage: %s config host port boundary before after "
            "default|session|behavior zone\n",
            argv[0]);
    return 2;
  }
  char* end = nullptr;
  errno = 0;
  long long boundary = strtoll(argv[4], &end, 10);
  if (errno != 0 || *end != '\0' || boundary < 600 || boundary > INT32_MAX - 600 ||
      static_cast<long long>(static_cast<time_t>(boundary)) != boundary) {
    return 2;
  }
  long port = strtol(argv[3], &end, 10);
  if (errno != 0 || *end != '\0' || port < 1 || port > 65535 ||
      (strcmp(argv[7], "default") != 0 && strcmp(argv[7], "session") != 0 && strcmp(argv[7], "behavior") != 0)) {
    return 2;
  }
  // Capture the original display before the transition, including during init
  // and connection creation. Never reconnect or reset the zone after this.
  testNow.store(static_cast<time_t>(boundary - 600));
  if (taos_options(TSDB_OPTION_CONFIGDIR, argv[1]) != 0) {
    return 1;
  }
  // The server keeps its real clock. Permit the deliberate historical skew
  // for connect/heartbeat checks in this test executable only.
  tsTimestampDeltaLimit = INT32_MAX;
  TAOS* conn = taos_connect(argv[2], "root", "taosdata", nullptr, static_cast<uint16_t>(port));
  if (conn == nullptr) {
    fprintf(stderr, "connect failed: %s\n", taos_errstr(nullptr));
    taos_cleanup();
    return 1;
  }
  if (strcmp(argv[7], "behavior") == 0) {
    bool ok = checkConnectionBehavior(conn, argv[2], static_cast<uint16_t>(port));
    taos_close(conn);
    taos_cleanup();
    return ok ? 0 : 1;
  }
  bool ok = strcmp(argv[7], "default") == 0 || setSessionTimezone(conn, argv[8]);
  if (ok) {
    ok = checkAt(conn, static_cast<time_t>(boundary - 600), argv[5]) &&
         checkAt(conn, static_cast<time_t>(boundary - 1), argv[5]) &&
         checkAt(conn, static_cast<time_t>(boundary), argv[6]) &&
         checkAt(conn, static_cast<time_t>(boundary + 1), argv[6]) &&
         checkAt(conn, static_cast<time_t>(boundary + 600), argv[6]);
  }
  taos_close(conn);
  taos_cleanup();
  return ok ? 0 : 1;
}
