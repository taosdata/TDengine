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

#ifndef TD_TZ_WINDOWS_H
#define TD_TZ_WINDOWS_H

#include <stddef.h>
#include <time.h>

void *taosTzAlloc(const char *name);
void taosTzFree(void *tz);
struct tm *taosTzLocaltime(void *tz, const time_t *value,
                         struct tm *result, long *offset);
time_t taosTzMktime(void *tz, struct tm *value, long *offset);
time_t taosTzTimegm(struct tm *value);
struct tm *taosTzGmtime(const time_t *value, struct tm *result);
size_t taosTzStrftime(char *s, size_t size, const char *format,
                      const struct tm *value, void *tz);
size_t taosTzStrftimeAt(char *s, size_t size, const char *format,
                        const time_t *stamp, void *tz);

#endif
