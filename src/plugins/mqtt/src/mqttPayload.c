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
#include "os.h"
#include "cJSON.h"
#include "mqttLog.h"
#include "mqttPayload.h"

// subscribe message like this

/*
/test {
    "timestamp": 1599121290,
    "gateway": {
        "name": "AcuLink 810 Gateway",
        "model": "AcuLink810-868",
        "serial": "S8P20200207"
    },
    "device": {
        "name": "Acuvim L V3 .221",
        "model": "Acuvim-L-V3",
        "serial": "221",
        "online": true,
        "readings": [
            {
                "param": "Freq_Hz",
                "value": "59.977539",
                "unit": "Hz"
            },
            {
                "param": "Va_V",
                "value": "122.002907",
                "unit": "V"
            },
            {
                "param": "DI4",
                "value": "5.000000",
                "unit": ""
            }
        ]
    }
}
*/

// send msg cmd
// mosquitto_pub -h test.mosquitto.org  -t "/test" -m '{"timestamp": 1599121290,"gateway": {"name": "AcuLink 810 Gateway","model": "AcuLink810-868","serial": "S8P20200207"},"device": {"name": "Acuvim L V3 .221","model": "Acuvim-L-V3","serial": "221","online": true,"readings": [{"param": "Freq_Hz","value": "59.977539","unit": "Hz"},{"param": "Va_V","value": "122.002907","unit": "V"},{"param": "DI4","value": "5.000000","unit": ""}]}}'

/*
 * This is an example, this function needs to be implemented in order to parse the json file into a sql statement
 * Note that you need to create a super table and database before writing data
 * In this case:
 *   create database mqttdb;
 *   create table mqttdb.devices(ts timestamp, value bigint) tags(name binary(32), model binary(32), serial binary(16), param binary(16), unit binary(16));
 */

char* mqttConverJsonToSql(char* json, int maxSize) {
  // const int32_t maxSize = 10240;
  maxSize *= 5;
  char* sql = malloc(maxSize);

  cJSON* root = cJSON_Parse(json);
  if (root == NULL) {
    mqttError("failed to parse msg, invalid json format");
    goto MQTT_PARSE_OVER;
  }

  cJSON* timestamp = cJSON_GetObjectItem(root, "timestamp");
  if (!timestamp || timestamp->type != cJSON_Number) {
    mqttError("failed to parse msg, timestamp not found");
    goto MQTT_PARSE_OVER;
  }

  cJSON* device = cJSON_GetObjectItem(root, "device");
  if (!device) {
    mqttError("failed to parse msg, device not found");
    goto MQTT_PARSE_OVER;
  }

  cJSON* name = cJSON_GetObjectItem(device, "name");
  if (!name || name->type != cJSON_String) {
    mqttError("failed to parse msg, name not found");
    goto MQTT_PARSE_OVER;
  }

  cJSON* model = cJSON_GetObjectItem(device, "model");
  if (!model || model->type != cJSON_String) {
    mqttError("failed to parse msg, model not found");
    goto MQTT_PARSE_OVER;
  }

  cJSON* serial = cJSON_GetObjectItem(device, "serial");
  if (!serial || serial->type != cJSON_String) {
    mqttError("failed to parse msg, serial not found");
    goto MQTT_PARSE_OVER;
  }

  cJSON* readings = cJSON_GetObjectItem(device, "readings");
  if (!readings || readings->type != cJSON_Array) {
    mqttError("failed to parse msg, readings not found");
    goto MQTT_PARSE_OVER;
  }

  int count = cJSON_GetArraySize(readings);
  if (count <= 0) {
    mqttError("failed to parse msg, readings size smaller than 0");
    goto MQTT_PARSE_OVER;
  }

  int len = snprintf(sql, maxSize, "insert into");

  for (int i = 0; i < count; ++i) {
    cJSON* reading = cJSON_GetArrayItem(readings, i);
    if (reading == NULL) continue;

    cJSON* param = cJSON_GetObjectItem(reading, "param");
    if (!param || param->type != cJSON_String) {
      mqttError("failed to parse msg, param not found");
      goto MQTT_PARSE_OVER;
    }

    cJSON* value = cJSON_GetObjectItem(reading, "value");
    if (!value || value->type != cJSON_String) {
      mqttError("failed to parse msg, value not found");
      goto MQTT_PARSE_OVER;
    }

    cJSON* unit = cJSON_GetObjectItem(reading, "unit");
    if (!unit || unit->type != cJSON_String) {
      mqttError("failed to parse msg, unit not found");
      goto MQTT_PARSE_OVER;
    }

    len += snprintf(sql + len, maxSize - len,
        " mqttdb.serial_%s_%s using mqttdb.devices tags('%s', '%s', '%s', '%s', '%s') values(%" PRId64 ", %s)",
        serial->valuestring, param->valuestring, name->valuestring, model->valuestring, serial->valuestring,
        param->valuestring, unit->valuestring, timestamp->valueint * 1000, value->valuestring);
  }

  cJSON_free(root);
  return sql;

MQTT_PARSE_OVER:
  cJSON_free(root);
  free(sql);
  return NULL;
}