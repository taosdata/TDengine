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

#ifndef _TD_UTIL_TVERSION_H_
#define _TD_UTIL_TVERSION_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t taosVersionStrToInt(const char *vstr, int32_t *vint);
int32_t taosVersionIntToStr(int32_t vint, char *vstr, int32_t len);
int32_t taosCheckVersionCompatible(int32_t clientVer, int32_t serverVer, int32_t comparedSegments);
int32_t taosCheckVersionCompatibleFromStr(const char *pClientVersion, const char *pServerVersion,
                                          int32_t comparedSegments);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TVERSION_H_*/
