/**
 * Copyright 2019-2020 DigitalOcean Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef TAOS_MONITOR_UTIL_I_H
#define TAOS_MONITOR_UTIL_I_H

#include <stdint.h>

void taos_monitor_split_str(char** arr, char* str, const char* del);
int  taos_monitor_count_occurrences(char *str, char *toSearch);
void taos_monitor_strip(char *s);
bool taos_monitor_is_match(const SJson* tags, char** pairs, int32_t count);

#endif  // TAOS_MONITOR_UTIL_I_H