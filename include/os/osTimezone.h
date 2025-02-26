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

#define TdEastZone8 8*60*60
#define TZ_UNKNOWN "n/a"

extern void* pTimezoneNameMap;

#ifdef WINDOWS
typedef void *timezone_t;
#else
typedef struct state *timezone_t;

struct tm* localtime_rz(timezone_t , time_t const *, struct tm *);
time_t     mktime_z(timezone_t, struct tm *);
timezone_t tzalloc(char const *);
void       tzfree(timezone_t);
void       getTimezoneStr(char *tz);
void       truncateTimezoneString(char *tz);

#endif


int32_t taosGetLocalTimezoneOffset();
int32_t taosGetSystemTimezone(char *outTimezone);
int32_t taosSetGlobalTimezone(const char *tz);
int32_t taosFormatTimezoneStr(time_t t, const char* tzStr, timezone_t sp, char *outTimezoneStr);
#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_TIMEZONE_H_*/
