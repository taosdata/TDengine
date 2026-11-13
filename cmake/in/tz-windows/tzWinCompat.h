// Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#ifndef TD_TZ_WIN_COMPAT_H
#define TD_TZ_WIN_COMPAT_H

#define NOMINMAX 1
#define _CRT_DECLARE_NONSTDC_NAMES 1
#include <windows.h>
#include <time.h>
#include <stdint.h>
#include <stddef.h>
#include <io.h>
#include <stdlib.h>
#undef min
#undef max

typedef intptr_t ssize_t;

#ifdef TD_TZ_LIBRARY
struct taos_tz_tm {
  int tm_sec, tm_min, tm_hour, tm_mday, tm_mon, tm_year;
  int tm_wday, tm_yday, tm_isdst;
  long tm_gmtoff;
  const char *tm_zone;
};

// Keep the CRT ABI at the TDengine boundary, including on 32-bit Windows.
typedef struct tm TaosCrtTm;
#define tm taos_tz_tm
ssize_t taosTzRead(const char *name, void *buffer, size_t size);
#endif

#endif
