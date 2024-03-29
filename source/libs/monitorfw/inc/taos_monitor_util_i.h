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

#ifndef TAOS_MONITOR_UTIL_I_H
#define TAOS_MONITOR_UTIL_I_H

#include <stdint.h>

void taos_monitor_split_str(char** arr, char* str, const char* del);
int  taos_monitor_count_occurrences(char *str, char *toSearch);
void taos_monitor_strip(char *s);
bool taos_monitor_is_match(const SJson* tags, char** pairs, int32_t count);

#endif  // TAOS_MONITOR_UTIL_I_H