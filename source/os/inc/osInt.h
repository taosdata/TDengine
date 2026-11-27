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

#ifndef _TD_OS_INT_H_
#define _TD_OS_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WINDOWS
typedef struct SPrivateFileSecurity {
  TOKEN_USER         *user;
  PACL                acl;
  SECURITY_DESCRIPTOR descriptor;
  SECURITY_ATTRIBUTES attributes;
} SPrivateFileSecurity;

int32_t taosInitPrivateFileSecurity(SPrivateFileSecurity *security);
void    taosFreePrivateFileSecurity(SPrivateFileSecurity *security);
int32_t taosCheckPrivateFileHandle(HANDLE handle, bool directory, PSID owner);

#include "tzWindows.h"

// tz must be non-NULL. The borrowed rules remain owned by tz until tzfree().
// Do not free them or retain them past tz's lifetime.
void *taosTimezoneRules(timezone_t tz);
typedef struct {
  char name[TD_TIMEZONE_LEN * 2];
  char fallback[64];
  bool recovered;
} SWindowsTimezoneConfig;

timezone_t taosLoadWindowsTimezone(const SWindowsTimezoneConfig *config);

void taosWindowsTimezoneConfig(const DYNAMIC_TIME_ZONE_INFORMATION *info,
                               DWORD type, SWindowsTimezoneConfig *config);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_INT_H_*/
