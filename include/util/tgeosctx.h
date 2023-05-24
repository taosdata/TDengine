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

#ifndef _TD_UTIL_GEOS_CTX_H_
#define _TD_UTIL_GEOS_CTX_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <geos_c.h>

typedef struct SGeosContext {
  GEOSContextHandle_t handle;

  GEOSWKTReader *WKTReader;
  GEOSWKTWriter *WKTWriter;

  GEOSWKBReader *WKBReader;
  GEOSWKBWriter *WKBWriter;

  char errMsg[512];
} SGeosContext;

SGeosContext* getThreadLocalGeosCtx();
void destroyThreadLocalGeosCtx();

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_GEOS_CTX_H_*/
