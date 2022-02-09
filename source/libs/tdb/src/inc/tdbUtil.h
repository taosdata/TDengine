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

#ifndef _TDB_UTIL_H_
#define _TDB_UTIL_H_

#ifdef __cplusplus
extern "C" {
#endif

#define TDB_ROUND8(x) (((x) + 7) & ~7)

int tdbGnrtFileID(const char *fname, uint8_t *fileid);

#ifdef __cplusplus
}
#endif

#endif /*_TDB_UTIL_H_*/