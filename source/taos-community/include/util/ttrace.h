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
#ifndef _TD_TRACE_H_
#define _TD_TRACE_H_

#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

#pragma(push, 1)

typedef struct STraceId {
  int64_t rootId;
  int64_t msgId;
} STraceId;

#pragma(pop)

#define TRACE_SET_ROOTID(traceId, root) \
  do {                                  \
    (traceId)->rootId = root;           \
  } while (0);

#define TRACE_GET_ROOTID(traceId) (traceId)->rootId

#define TRACE_SET_MSGID(traceId, mId) \
  do {                                \
    (traceId)->msgId = mId;           \
  } while (0)

#define TRACE_GET_MSGID(traceId) (traceId)->msgId

//#define TRACE_TO_STR(traceId, buf)                              \
//  do {                                                          \
//    int64_t rootId = (traceId) != NULL ? (traceId)->rootId : 0; \
//    int64_t msgId = (traceId) != NULL ? (traceId)->msgId : 0;   \
//    sprintf(buf, "0x%" PRIx64 ":0x%" PRIx64 "", rootId, msgId); \
//  } while (0)

#define TRACE_TO_STR(_traceId, _buf)                             \
  do {                                                            \
    int64_t rootId = (_traceId) != NULL ? (_traceId)->rootId : 0; \
    int64_t msgId = (_traceId) != NULL ? (_traceId)->msgId : 0;   \
    char*   _t = _buf;                                            \
    _t[0] = '0';                                                  \
    _t[1] = 'x';                                                  \
    _t += 2;                                                      \
    _t += titoa(rootId, 16, &_t[0]);                              \
    _t[0] = ':';                                                  \
    _t[1] = '0';                                                  \
    _t[2] = 'x';                                                  \
    _t += 3;                                                      \
    _t += titoa(msgId, 16, &_t[0]);                               \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif
