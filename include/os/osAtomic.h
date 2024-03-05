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

#ifndef _TD_OS_ATOMIC_H_
#define _TD_OS_ATOMIC_H_

#ifdef __cplusplus
extern "C" {
#endif

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define __atomic_load_n             __ATOMIC_LOAD_N_FUNC_TAOS_FORBID
#define __atomic_store_n            __ATOMIC_STORE_N_FUNC_TAOS_FORBID
#define __atomic_exchange_n         __ATOMIC_EXCHANGE_N_FUNC_TAOS_FORBID
#define __sync_val_compare_and_swap __SYNC_VAL_COMPARE_AND_SWAP_FUNC_TAOS_FORBID
#define __atomic_add_fetch          __ATOMIC_ADD_FETCH_FUNC_TAOS_FORBID
#define __atomic_fetch_add          __ATOMIC_FETCH_ADD_FUNC_TAOS_FORBID
#define __atomic_sub_fetch          __ATOMIC_SUB_FETCH_FUNC_TAOS_FORBID
#define __atomic_fetch_sub          __ATOMIC_FETCH_SUB_FUNC_TAOS_FORBID
#define __atomic_and_fetch          __ATOMIC_AND_FETCH_FUNC_TAOS_FORBID
#define __atomic_fetch_and          __ATOMIC_FETCH_AND_FUNC_TAOS_FORBID
#define __atomic_or_fetch           __ATOMIC_OR_FETCH_FUNC_TAOS_FORBID
#define __atomic_fetch_or           __ATOMIC_FETCH_OR_FUNC_TAOS_FORBID
#define __atomic_xor_fetch          __ATOMIC_XOR_FETCH_FUNC_TAOS_FORBID
#define __atomic_fetch_xor          __ATOMIC_FETCH_XOR_FUNC_TAOS_FORBID
#endif

int8_t  atomic_load_8(int8_t volatile *ptr);
int16_t atomic_load_16(int16_t volatile *ptr);
int32_t atomic_load_32(int32_t volatile *ptr);
int64_t atomic_load_64(int64_t volatile *ptr);
void   *atomic_load_ptr(void *ptr);
void    atomic_store_8(int8_t volatile *ptr, int8_t val);
void    atomic_store_16(int16_t volatile *ptr, int16_t val);
void    atomic_store_32(int32_t volatile *ptr, int32_t val);
void    atomic_store_64(int64_t volatile *ptr, int64_t val);
double  atomic_store_double(double volatile *ptr, double val);
void    atomic_store_ptr(void *ptr, void *val);
int8_t  atomic_exchange_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_exchange_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_exchange_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_exchange_64(int64_t volatile *ptr, int64_t val);
double  atomic_exchange_double(double volatile *ptr, int64_t val);
void   *atomic_exchange_ptr(void *ptr, void *val);
int8_t  atomic_val_compare_exchange_8(int8_t volatile *ptr, int8_t oldval, int8_t newval);
int16_t atomic_val_compare_exchange_16(int16_t volatile *ptr, int16_t oldval, int16_t newval);
int32_t atomic_val_compare_exchange_32(int32_t volatile *ptr, int32_t oldval, int32_t newval);
int64_t atomic_val_compare_exchange_64(int64_t volatile *ptr, int64_t oldval, int64_t newval);
void   *atomic_val_compare_exchange_ptr(void *ptr, void *oldval, void *newval);
int8_t  atomic_add_fetch_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_add_fetch_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_add_fetch_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_add_fetch_64(int64_t volatile *ptr, int64_t val);
void   *atomic_add_fetch_ptr(void *ptr, int64_t val);
int8_t  atomic_fetch_add_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_fetch_add_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_fetch_add_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_fetch_add_64(int64_t volatile *ptr, int64_t val);
double  atomic_fetch_add_double(double volatile *ptr, double val);
void   *atomic_fetch_add_ptr(void *ptr, void *val);
int8_t  atomic_sub_fetch_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_sub_fetch_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_sub_fetch_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_sub_fetch_64(int64_t volatile *ptr, int64_t val);
void   *atomic_sub_fetch_ptr(void *ptr, int64_t val);
int8_t  atomic_fetch_sub_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_fetch_sub_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_fetch_sub_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_fetch_sub_64(int64_t volatile *ptr, int64_t val);
double atomic_fetch_sub_double(double volatile *ptr, double val);
void   *atomic_fetch_sub_ptr(void *ptr, void *val);
int8_t  atomic_and_fetch_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_and_fetch_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_and_fetch_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_and_fetch_64(int64_t volatile *ptr, int64_t val);
void   *atomic_and_fetch_ptr(void *ptr, void *val);
int8_t  atomic_fetch_and_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_fetch_and_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_fetch_and_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_fetch_and_64(int64_t volatile *ptr, int64_t val);
void   *atomic_fetch_and_ptr(void *ptr, void *val);
int8_t  atomic_or_fetch_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_or_fetch_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_or_fetch_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_or_fetch_64(int64_t volatile *ptr, int64_t val);
void   *atomic_or_fetch_ptr(void *ptr, void *val);
int8_t  atomic_fetch_or_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_fetch_or_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_fetch_or_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_fetch_or_64(int64_t volatile *ptr, int64_t val);
void   *atomic_fetch_or_ptr(void *ptr, void *val);
int8_t  atomic_xor_fetch_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_xor_fetch_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_xor_fetch_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_xor_fetch_64(int64_t volatile *ptr, int64_t val);
void   *atomic_xor_fetch_ptr(void *ptr, void *val);
int8_t  atomic_fetch_xor_8(int8_t volatile *ptr, int8_t val);
int16_t atomic_fetch_xor_16(int16_t volatile *ptr, int16_t val);
int32_t atomic_fetch_xor_32(int32_t volatile *ptr, int32_t val);
int64_t atomic_fetch_xor_64(int64_t volatile *ptr, int64_t val);
void   *atomic_fetch_xor_ptr(void *ptr, void *val);

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_ATOMIC_H_*/
