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
 * along with this program. If not, see <mqtt://www.gnu.org/licenses/>.
 */

#ifndef TDENGINE_MQTT_LOG_H
#define TDENGINE_MQTT_LOG_H

#include "tlog.h"

extern int32_t mqttDebugFlag;

#define mqttFatal(...) { if (mqttDebugFlag & DEBUG_FATAL) { taosPrintLog("MQT FATAL ", 255, __VA_ARGS__); }}
#define mqttError(...) { if (mqttDebugFlag & DEBUG_ERROR) { taosPrintLog("MQT ERROR ", 255, __VA_ARGS__); }}
#define mqttWarn(...)  { if (mqttDebugFlag & DEBUG_WARN)  { taosPrintLog("MQT WARN ", 255, __VA_ARGS__); }}
#define mqttInfo(...)  { if (mqttDebugFlag & DEBUG_INFO)  { taosPrintLog("MQT ", 255, __VA_ARGS__); }}
#define mqttDebug(...) { if (mqttDebugFlag & DEBUG_DEBUG) { taosPrintLog("MQT ", mqttDebugFlag, __VA_ARGS__); }}
#define mqttTrace(...) { if (mqttDebugFlag & DEBUG_TRACE) { taosPrintLog("MQT ", mqttDebugFlag, __VA_ARGS__); }}

#endif
