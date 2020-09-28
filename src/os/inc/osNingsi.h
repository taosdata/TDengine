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

#ifndef TDENGINE_OS_NINGSI_H
#define TDENGINE_OS_NINGSI_H

#ifdef __cplusplus
extern "C" {
#endif

#define  TAOS_OS_FUNC_ATOMIC
/*
 * type __sync_fetch_and_add (type *ptr, type value);
 * type __sync_fetch_and_sub (type *ptr, type value);
 * type __sync_fetch_and_or (type *ptr, type value);
 * type __sync_fetch_and_and (type *ptr, type value);
 * type __sync_fetch_and_xor (type *ptr, type value);
 * type __sync_fetch_and_nand (type *ptr, type value);
 * type __sync_add_and_fetch (type *ptr, type value);
 * type __sync_sub_and_fetch (type *ptr, type value);
 * type __sync_or_and_fetch (type *ptr, type value);
 * type __sync_and_and_fetch (type *ptr, type value);
 * type __sync_xor_and_fetch (type *ptr, type value);
 * type __sync_nand_and_fetch (type *ptr, type value);
 *
 * bool __sync_bool_compare_and_swap (type*ptr, type oldval, type newval, ...)
 * type __sync_val_compare_and_swap (type *ptr, type oldval, ?type newval, ...)
 * */

#define atomic_load_8(ptr)   __sync_fetch_and_add((ptr), 0)
#define atomic_load_16(ptr)  __sync_fetch_and_add((ptr), 0)
#define atomic_load_32(ptr)  __sync_fetch_and_add((ptr), 0)
#define atomic_load_64(ptr)  __sync_fetch_and_add((ptr), 0)
#define atomic_load_ptr(ptr) __sync_fetch_and_add((ptr), 0)
  
#define atomic_store_8(ptr, val)   (*(ptr)=(val))
#define atomic_store_16(ptr, val)  (*(ptr)=(val))
#define atomic_store_32(ptr, val)  (*(ptr)=(val))
#define atomic_store_64(ptr, val)  (*(ptr)=(val))
#define atomic_store_ptr(ptr, val) (*(ptr)=(val))

int8_t  atomic_exchange_8_impl(int8_t* ptr, int8_t val );
int16_t atomic_exchange_16_impl(int16_t* ptr, int16_t val );
int32_t atomic_exchange_32_impl(int32_t* ptr, int32_t val );
int64_t atomic_exchange_64_impl(int64_t* ptr, int64_t val );
void*   atomic_exchange_ptr_impl( void **ptr, void *val );

#define atomic_exchange_8(ptr, val)   atomic_exchange_8_impl((int8_t*)ptr, (int8_t)val)
#define atomic_exchange_16(ptr, val)  atomic_exchange_16_impl((int16_t*)ptr, (int16_t)val)
#define atomic_exchange_32(ptr, val)  atomic_exchange_32_impl((int32_t*)ptr, (int32_t)val)
#define atomic_exchange_64(ptr, val)  atomic_exchange_64_impl((int64_t*)ptr, (int64_t)val)
#define atomic_exchange_ptr(ptr, val) atomic_exchange_ptr_impl((void **)ptr, (void*)val)
    
#define atomic_val_compare_exchange_8   __sync_val_compare_and_swap
#define atomic_val_compare_exchange_16  __sync_val_compare_and_swap
#define atomic_val_compare_exchange_32  __sync_val_compare_and_swap
#define atomic_val_compare_exchange_64  __sync_val_compare_and_swap
#define atomic_val_compare_exchange_ptr __sync_val_compare_and_swap
  
#define atomic_add_fetch_8(ptr, val) __sync_add_and_fetch((ptr), (val))
#define atomic_add_fetch_16(ptr, val) __sync_add_and_fetch((ptr), (val))
#define atomic_add_fetch_32(ptr, val) __sync_add_and_fetch((ptr), (val))
#define atomic_add_fetch_64(ptr, val) __sync_add_and_fetch((ptr), (val))
#define atomic_add_fetch_ptr(ptr, val) __sync_add_and_fetch((ptr), (val))
  
#define atomic_fetch_add_8(ptr, val) __sync_fetch_and_add((ptr), (val))
#define atomic_fetch_add_16(ptr, val) __sync_fetch_and_add((ptr), (val))
#define atomic_fetch_add_32(ptr, val) __sync_fetch_and_add((ptr), (val))
#define atomic_fetch_add_64(ptr, val) __sync_fetch_and_add((ptr), (val))
#define atomic_fetch_add_ptr(ptr, val) __sync_fetch_and_add((ptr), (val))
  
#define atomic_sub_fetch_8(ptr, val) __sync_sub_and_fetch((ptr), (val))
#define atomic_sub_fetch_16(ptr, val) __sync_sub_and_fetch((ptr), (val))
#define atomic_sub_fetch_32(ptr, val) __sync_sub_and_fetch((ptr), (val))
#define atomic_sub_fetch_64(ptr, val) __sync_sub_and_fetch((ptr), (val))
#define atomic_sub_fetch_ptr(ptr, val) __sync_sub_and_fetch((ptr), (val))
  
#define atomic_fetch_sub_8(ptr, val) __sync_fetch_and_sub((ptr), (val))
#define atomic_fetch_sub_16(ptr, val) __sync_fetch_and_sub((ptr), (val))
#define atomic_fetch_sub_32(ptr, val) __sync_fetch_and_sub((ptr), (val))
#define atomic_fetch_sub_64(ptr, val) __sync_fetch_and_sub((ptr), (val))
#define atomic_fetch_sub_ptr(ptr, val) __sync_fetch_and_sub((ptr), (val))
  
#define atomic_and_fetch_8(ptr, val) __sync_and_and_fetch((ptr), (val))
#define atomic_and_fetch_16(ptr, val) __sync_and_and_fetch((ptr), (val))
#define atomic_and_fetch_32(ptr, val) __sync_and_and_fetch((ptr), (val))
#define atomic_and_fetch_64(ptr, val) __sync_and_and_fetch((ptr), (val))
#define atomic_and_fetch_ptr(ptr, val) __sync_and_and_fetch((ptr), (val))
  
#define atomic_fetch_and_8(ptr, val) __sync_fetch_and_and((ptr), (val))
#define atomic_fetch_and_16(ptr, val) __sync_fetch_and_and((ptr), (val))
#define atomic_fetch_and_32(ptr, val) __sync_fetch_and_and((ptr), (val))
#define atomic_fetch_and_64(ptr, val) __sync_fetch_and_and((ptr), (val))
#define atomic_fetch_and_ptr(ptr, val) __sync_fetch_and_and((ptr), (val))
  
#define atomic_or_fetch_8(ptr, val) __sync_or_and_fetch((ptr), (val))
#define atomic_or_fetch_16(ptr, val) __sync_or_and_fetch((ptr), (val))
#define atomic_or_fetch_32(ptr, val) __sync_or_and_fetch((ptr), (val))
#define atomic_or_fetch_64(ptr, val) __sync_or_and_fetch((ptr), (val))
#define atomic_or_fetch_ptr(ptr, val) __sync_or_and_fetch((ptr), (val))
  
#define atomic_fetch_or_8(ptr, val) __sync_fetch_and_or((ptr), (val))
#define atomic_fetch_or_16(ptr, val) __sync_fetch_and_or((ptr), (val))
#define atomic_fetch_or_32(ptr, val) __sync_fetch_and_or((ptr), (val))
#define atomic_fetch_or_64(ptr, val) __sync_fetch_and_or((ptr), (val))
#define atomic_fetch_or_ptr(ptr, val) __sync_fetch_and_or((ptr), (val))
  
#define atomic_xor_fetch_8(ptr, val) __sync_xor_and_fetch((ptr), (val))
#define atomic_xor_fetch_16(ptr, val) __sync_xor_and_fetch((ptr), (val))
#define atomic_xor_fetch_32(ptr, val) __sync_xor_and_fetch((ptr), (val))
#define atomic_xor_fetch_64(ptr, val) __sync_xor_and_fetch((ptr), (val))
#define atomic_xor_fetch_ptr(ptr, val) __sync_xor_and_fetch((ptr), (val))
  
#define atomic_fetch_xor_8(ptr, val) __sync_fetch_and_xor((ptr), (val))
#define atomic_fetch_xor_16(ptr, val) __sync_fetch_and_xor((ptr), (val))
#define atomic_fetch_xor_32(ptr, val) __sync_fetch_and_xor((ptr), (val))
#define atomic_fetch_xor_64(ptr, val) __sync_fetch_and_xor((ptr), (val))
#define atomic_fetch_xor_ptr(ptr, val) __sync_fetch_and_xor((ptr), (val))

#ifdef __cplusplus
}
#endif

#endif
