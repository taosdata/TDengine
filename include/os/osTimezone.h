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

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define tzset TZSET_FUNC_TAOS_FORBID
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

void taosGetSystemTimezone(char *outTimezone, enum TdTimezone *tsTimezone);
void taosSetSystemTimezone(const char *inTimezone, char *outTimezone, int8_t *outDaylight, enum TdTimezone *tsTimezone);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_TIMEZONE_H_*/
