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

#ifndef BSE_UTIL_H_
#define BSE_UTIL_H_

#include "os.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif
// clang-format off
#define bseFatal(...) do { if (bseDebugFlag & DEBUG_FATAL) { taosPrintLog("BSE FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define bseError(...) do { if (bseDebugFlag & DEBUG_ERROR) { taosPrintLog("BSE ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define bseWarn(...)  do { if (bseDebugFlag & DEBUG_WARN)  { taosPrintLog("BSE WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define bseInfo(...)  do { if (bseDebugFlag & DEBUG_INFO)  { taosPrintLog("BSE ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define bseDebug(...) do { if (bseDebugFlag & DEBUG_DEBUG) { taosPrintLog("BSE ", DEBUG_DEBUG, bseDebugFlag, __VA_ARGS__); }}    while(0)
#define bseTrace(...) do { if (bseDebugFlag & DEBUG_TRACE) { taosPrintLog("BSE ", DEBUG_TRACE, bseDebugFlag, __VA_ARGS__); }}    while(0)

#define bseGTrace(param, ...) do { if (bseDebugFlag & DEBUG_TRACE) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseTrace(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGFatal(param, ...) do { if (bseDebugFlag & DEBUG_FATAL) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseFatal(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGError(param, ...) do { if (bseDebugFlag & DEBUG_ERROR) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseError(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGWarn(param, ...)  do { if (bseDebugFlag & DEBUG_WARN)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseWarn(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGInfo(param, ...)  do { if (bseDebugFlag & DEBUG_INFO)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseInfo(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGDebug(param, ...) do { if (bseDebugFlag & DEBUG_DEBUG) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseDebug(param ",QID:%s", __VA_ARGS__, buf);}}    while(0)

#define BSE_VGID(p) ((p)->cfg.vgId)

// clang-format on
#ifdef __cplusplus
}
#endif

#endif