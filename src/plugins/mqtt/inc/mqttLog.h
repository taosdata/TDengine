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

#ifndef TDENGINE_MQTT_LOG_H
#define TDENGINE_MQTT_LOG_H

#include "tlog.h"

extern int32_t mqttDebugFlag;

#define mqttError(...)                       \
  if (mqttDebugFlag & DEBUG_ERROR) {         \
    taosPrintLog("ERROR MQT ", 255, __VA_ARGS__); \
  }
#define mqttWarn(...)                                  \
  if ( mqttDebugFlag & DEBUG_WARN) {                    \
    taosPrintLog("WARN  MQT ",  mqttDebugFlag, __VA_ARGS__); \
  }
#define  mqttTrace(...)                           \
  if ( mqttDebugFlag & DEBUG_TRACE) {             \
    taosPrintLog("MQT ",  mqttDebugFlag, __VA_ARGS__); \
  }
#define  mqttDump(...)                                        \
  if ( mqttDebugFlag & DEBUG_TRACE) {                         \
    taosPrintLongString("MQT ",  mqttDebugFlag, __VA_ARGS__); \
  }
#define  mqttPrint(...) \
  { taosPrintLog("MQT ", 255, __VA_ARGS__); }

#endif
