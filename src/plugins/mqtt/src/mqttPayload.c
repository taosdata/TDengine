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
#include "mqttPayload.h"
#include "cJSON.h"
#include "string.h"
#include "taos.h"
#include "mqttLog.h"
#include "os.h"
char split(char str[], char delims[], char** p_p_cmd_part, int max) {
  char*  token = strtok(str, delims);
  char   part_index = 0;
  char** tmp_part = p_p_cmd_part;
  while (token) {
    *tmp_part++ = token;
    token = strtok(NULL, delims);
    part_index++;
    if (part_index >= max) break;
  }
  return part_index;
}

char* converJsonToSql(char* json, char* _dbname, char* _tablename) {
  cJSON* jPlayload = cJSON_Parse(json);
  char   _names[102400] = {0};
  char   _values[102400] = {0};
  int    i = 0;
  int    count = cJSON_GetArraySize(jPlayload);
  for (; i < count; i++)  
  {
    cJSON* item = cJSON_GetArrayItem(jPlayload, i);
    if (cJSON_Object == item->type) {
      mqttPrint("The item '%s' is not supported", item->string);
    } else {
      strcat(_names, item->string);
      if (i < count - 1) {
        strcat(_names, ",");
      }
      char* __value_json = cJSON_Print(item);
      strcat(_values, __value_json);
      free(__value_json);
      if (i < count - 1) {
        strcat(_values, ",");
      }
    }
  }
  cJSON_free(jPlayload);
  int   sqllen = strlen(_names) + strlen(_values) + strlen(_dbname) + strlen(_tablename) + 1024;
  char* _sql = calloc(1, sqllen);
  sprintf(_sql, "INSERT INTO %s.%s (%s) VALUES(%s);", _dbname, _tablename, _names, _values);
  return _sql;
}