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

#ifndef _TD_OS_RAND_H_
#define _TD_OS_RAND_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define rand   RAND_FUNC_TAOS_FORBID
#define srand  SRAND_FUNC_TAOS_FORBID
#define rand_r RANDR_FUNC_TAOS_FORBID
#endif

void     taosSeedRand(uint32_t seed);
uint32_t taosRand(void);
uint32_t taosRandR(uint32_t* pSeed);
void     taosRandStr(char* str, int32_t size);
void     taosRandStr2(char* str, int32_t size);

uint32_t taosSafeRand(void);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_RAND_H_*/
