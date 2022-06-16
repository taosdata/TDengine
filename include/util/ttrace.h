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

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int64_t STraceId;
typedef int32_t STraceSubId;

STraceId traceInitId(STraceSubId *h, STraceSubId *l);

void traceId2Str(STraceId *id, char *buf);

void traceSetSubId(STraceId *id, int32_t *subId);

STraceSubId traceGetParentId(STraceId *id);

STraceSubId traceGenSubId();
#ifdef __cplusplus
}
#endif

#endif
