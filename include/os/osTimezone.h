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

#ifndef _TD_OS_TIMEZONE_H_
#define _TD_OS_TIMEZONE_H_

#ifdef __cplusplus
extern "C" {
#endif

enum TdTimezone {
  TdWestZone12 = -12,
  TdWestZone11,
  TdWestZone10,
  TdWestZone9,
  TdWestZone8,
  TdWestZone7,
  TdWestZone6,
  TdWestZone5,
  TdWestZone4,
  TdWestZone3,
  TdWestZone2,
  TdWestZone1,
  TdZeroZone,
  TdEastZone1,
  TdEastZone2,
  TdEastZone3,
  TdEastZone4,
  TdEastZone5,
  TdEastZone6,
  TdEastZone7,
  TdEastZone8,
  TdEastZone9,
  TdEastZone10,
  TdEastZone11,
  TdEastZone12
};

typedef struct state *timezone_t;
struct tm *localtime_rz(timezone_t , time_t const *, struct tm *);
time_t mktime_z(timezone_t, struct tm *);
timezone_t tzalloc(char const *);
void tzfree(timezone_t);

void    getTimezoneStr(char *tz);
int32_t taosGetSystemTimezone(char *outTimezone);
int32_t taosSetGlobalTimezone(const char *tz);
int32_t taosFormatTimezoneStr(time_t t, const char* tzStr, timezone_t sp, char *outTimezoneStr);
int32_t taosIsValidateTimezone(const char *tz);
#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_TIMEZONE_H_*/
