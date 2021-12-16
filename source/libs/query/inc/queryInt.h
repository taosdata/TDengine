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

#ifndef _TD_QUERY_INT_H_
#define _TD_QUERY_INT_H_

#ifdef __cplusplus
extern "C" {
#endif


#include "tlog.h"

extern int32_t qDebugFlag;

#define qFatal(...)  do { if (qDebugFlag & DEBUG_FATAL) { taosPrintLog("QRY FATAL ", qDebugFlag, __VA_ARGS__); }} while(0)
#define qError(...)  do { if (qDebugFlag & DEBUG_ERROR) { taosPrintLog("QRY ERROR ", qDebugFlag, __VA_ARGS__); }} while(0)
#define qWarn(...)   do { if (qDebugFlag & DEBUG_WARN)  { taosPrintLog("QRY WARN ", qDebugFlag, __VA_ARGS__); }}  while(0)
#define qInfo(...)   do { if (qDebugFlag & DEBUG_INFO)  { taosPrintLog("QRY ", qDebugFlag, __VA_ARGS__); }} while(0)
#define qDebug(...)  do { if (qDebugFlag & DEBUG_DEBUG) { taosPrintLog("QRY ", qDebugFlag, __VA_ARGS__); }} while(0)
#define qTrace(...)  do { if (qDebugFlag & DEBUG_TRACE) { taosPrintLog("QRY ", qDebugFlag, __VA_ARGS__); }} while(0)
#define qDebugL(...) do { if (qDebugFlag & DEBUG_DEBUG) { taosPrintLongString("QRY ", qDebugFlag, __VA_ARGS__); }} while(0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_INT_H_*/
