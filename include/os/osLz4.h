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

#ifndef _TD_OS_LZ4_H_
#define _TD_OS_LZ4_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WINDOWS
int32_t BUILDIN_CLZL(uint64_t val);
int32_t BUILDIN_CLZ(uint32_t val);
int32_t BUILDIN_CTZL(uint64_t val);
int32_t BUILDIN_CTZ(uint32_t val);
#elif defined(_TD_LINUX_32)
#define BUILDIN_CLZL(val) __builtin_clzll(val)
#define BUILDIN_CTZL(val) __builtin_ctzll(val)
#define BUILDIN_CLZ(val)  __builtin_clz(val)
#define BUILDIN_CTZ(val)  __builtin_ctz(val)
#elif defined(_TD_ARM_32)
#define BUILDIN_CLZL(val) __builtin_clzll(val)
#define BUILDIN_CTZL(val) __builtin_ctzll(val)
#define BUILDIN_CLZ(val)  __builtin_clz(val)
#define BUILDIN_CTZ(val)  __builtin_ctz(val)
#else
#define BUILDIN_CLZL(val) __builtin_clzl(val)
#define BUILDIN_CTZL(val) __builtin_ctzl(val)
#define BUILDIN_CLZ(val)  __builtin_clz(val)
#define BUILDIN_CTZ(val)  __builtin_ctz(val)
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_LZ4_H_*/
