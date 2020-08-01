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

#ifndef TDENGINE_OS_LZ4_H
#define TDENGINE_OS_LZ4_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef TAOS_OS_FUNC_LZ4
  #define BUILDIN_CLZL(val) __builtin_clzl(val)
  #define BUILDIN_CTZL(val) __builtin_ctzl(val)
  #define BUILDIN_CLZ(val) __builtin_clz(val)
  #define BUILDIN_CTZ(val) __builtin_ctz(val)
#endif

#ifdef __cplusplus
}
#endif

#endif
