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

#define mqttError(fmt, ...)                       \
  if (mqttDebugFlag & DEBUG_ERROR) {         \
    TLOG("ERROR MQT ", 255, fmt, ##__VA_ARGS__); \
  }
#define mqttWarn(fmt, ...)                                  \
  if ( mqttDebugFlag & DEBUG_WARN) {                    \
    TLOG("WARN MQT ",  mqttDebugFlag, fmt, ##__VA_ARGS__); \
  }
#define  mqttTrace(fmt, ...)                           \
  if ( mqttDebugFlag & DEBUG_TRACE) {             \
    TLOG("MQT ",  mqttDebugFlag, fmt, ##__VA_ARGS__); \
  }
#define  mqttDump(fmt, ...)                                        \
  if ( mqttDebugFlag & DEBUG_TRACE) {                         \
    TLOGLONG("MQT ",  mqttDebugFlag, fmt, ##__VA_ARGS__); \
  }
#define  mqttPrint(fmt, ...) \
  { TLOG("MQT ", 255, fmt, ##__VA_ARGS__); }

#endif
