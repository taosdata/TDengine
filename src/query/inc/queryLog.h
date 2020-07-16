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

#ifndef TDENGINE_QUERY_LOG_H
#define TDENGINE_QUERY_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tlog.h"

extern int32_t qDebugFlag;
extern int32_t tscEmbedded;

#define qFatal(...) { if (qDebugFlag & DEBUG_FATAL) { taosPrintLog("QRY FATAL ", 255, __VA_ARGS__); }}
#define qError(...) { if (qDebugFlag & DEBUG_ERROR) { taosPrintLog("QRY ERROR ", 255, __VA_ARGS__); }}
#define qWarn(...)  { if (qDebugFlag & DEBUG_WARN)  { taosPrintLog("QRY WARN ", 255, __VA_ARGS__); }}
#define qInfo(...)  { if (qDebugFlag & DEBUG_INFO)  { taosPrintLog("QRY ", 255, __VA_ARGS__); }}
#define qDebug(...) { if (qDebugFlag & DEBUG_DEBUG) { taosPrintLog("QRY ", qDebugFlag, __VA_ARGS__); }}
#define qTrace(...) { if (qDebugFlag & DEBUG_TRACE) { taosPrintLog("QRY ", qDebugFlag, __VA_ARGS__); }}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QUERY_LOG_H
