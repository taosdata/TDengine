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

extern uint32_t qDebugFlag;

#define qFatal(...) do { if (qDebugFlag & DEBUG_FATAL) { taosPrintLog("QRY FATAL ", __FILE__, __LINE__, 255, __VA_ARGS__); }}  while(0)
#define qError(...) do { if (qDebugFlag & DEBUG_ERROR) { taosPrintLog("QRY ERROR ", __FILE__, __LINE__, 255, __VA_ARGS__); }}  while(0)
#define qWarn(...)  do { if (qDebugFlag & DEBUG_WARN)  { taosPrintLog("QRY WARN ", __FILE__, __LINE__, 255, __VA_ARGS__); }}   while(0)
#define qInfo(...)  do { if (qDebugFlag & DEBUG_INFO)  { taosPrintLog("QRY ", __FILE__, __LINE__, 255, __VA_ARGS__); }}        while(0)
#define qDebug(...) do { if (qDebugFlag & DEBUG_DEBUG) { taosPrintLog("QRY ", __FILE__, __LINE__, qDebugFlag, __VA_ARGS__); }} while(0)
#define qTrace(...) do { if (qDebugFlag & DEBUG_TRACE) { taosPrintLog("QRY ", __FILE__, __LINE__, qDebugFlag, __VA_ARGS__); }} while(0)

#define qDump(a, l) do { if (qDebugFlag & DEBUG_DUMP)  { taosDumpData((unsigned char *)a, l); }} while(0)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_QUERY_LOG_H
