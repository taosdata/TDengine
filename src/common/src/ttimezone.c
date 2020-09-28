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

#define _DEFAULT_SOURCE
#include "os.h"
#include "tulog.h"
#include "tglobal.h"
#include "tconfig.h"
#include "tutil.h"

// TODO refactor to set the tz value through parameter
void tsSetTimeZone() {
  SGlobalCfg *cfg_timezone = taosGetConfigOption("timezone");
  if (cfg_timezone != NULL) {
    uInfo("timezone is set to %s by %s", tsTimezone, tsCfgStatusStr[cfg_timezone->cfgStatus]);
  }

#ifdef WINDOWS
  char winStr[TSDB_LOCALE_LEN * 2];
  sprintf(winStr, "TZ=%s", tsTimezone);
  putenv(winStr);
#else
  setenv("TZ", tsTimezone, 1);
#endif
  tzset();

  /*
  * get CURRENT time zone.
  * system current time zone is affected by daylight saving time(DST)
  *
  * e.g., the local time zone of London in DST is GMT+01:00,
  * otherwise is GMT+00:00
  */
#ifdef _MSC_VER
#if _MSC_VER >= 1900
  // see https://docs.microsoft.com/en-us/cpp/c-runtime-library/daylight-dstbias-timezone-and-tzname?view=vs-2019
  int64_t timezone = _timezone;
  int32_t daylight = _daylight;
  char **tzname = _tzname;
#endif
#endif

  int32_t tz = (int32_t)((-timezone * MILLISECOND_PER_SECOND) / MILLISECOND_PER_HOUR);
  tz += daylight;

  /*
  * format:
  * (CST, +0800)
  * (BST, +0100)
  */
  sprintf(tsTimezone, "(%s, %s%02d00)", tzname[daylight], tz >= 0 ? "+" : "-", abs(tz));
  tsDaylight = daylight;

  uInfo("timezone format changed to %s", tsTimezone);
}
