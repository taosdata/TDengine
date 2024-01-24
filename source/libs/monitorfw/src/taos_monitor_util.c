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


#define ALLOW_FORBID_FUNC
#include "tjson.h"
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "osMemory.h"
#include "tdef.h"

#include "taos_metric_t.h"

void taos_monitor_split_str(char** arr, char* str, const char* del) {
  char *lasts;
  char* s = strsep(&str, del);
  while (s != NULL) {
    *arr++ = s;
    s = strsep(&str, del);
  }
}

void taos_monitor_split_str_metric(char** arr, taos_metric_t* metric, const char* del, char** buf) {
  int32_t size = strlen(metric->name);
  char* name = taosMemoryMalloc(size + 1);
  memset(name, 0, size + 1);
  memcpy(name, metric->name, size);

  char* s = strtok(name, del);
  while (s != NULL) {
    *arr++ = s;
    s = strtok(NULL, del);
  }

  *buf = name;
}

const char* taos_monitor_get_metric_name(taos_metric_t* metric){
  return metric->name;
}

int taos_monitor_count_occurrences(char *str, char *toSearch) {
    int count = 0;
    char *ptr = str;
    while ((ptr = strstr(ptr, toSearch)) != NULL) {
        count++;
        ptr++;
    }
    return count;
}

void taos_monitor_strip(char *s)
{
    size_t i;
    size_t len = strlen(s);
    size_t offset = 0;
    for(i = 0; i < len; ++i){
        char c = s[i];
        if(c=='\"') ++offset;
        else s[i-offset] = c;
    }
    s[len-offset] = '\0';
}

bool taos_monitor_is_match(const SJson* tags, char** pairs, int32_t count) {
  int32_t size = tjsonGetArraySize(tags);
  if(size != count) return false;

  for(int32_t i = 0; i < size; i++){
    SJson* item = tjsonGetArrayItem(tags, i);

    char item_name[MONITOR_TAG_NAME_LEN] = {0};
    tjsonGetStringValue(item, "name", item_name);

    char item_value[MONITOR_TAG_VALUE_LEN] = {0};
    tjsonGetStringValue(item, "value", item_value);

    bool isfound = false;
    for(int32_t j = 0; j < count; j++){

      char** pair = pairs + j * 2;

      char* key = *pair;
      char* value = *(pair + 1);

      
      if(strcmp(value, item_value) == 0 && strcmp(key, item_name) == 0){
        isfound = true;
        break;
      }
    }

    if(!isfound) return false;
  }

  return true;
}
