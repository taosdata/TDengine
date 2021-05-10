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

#ifndef TDENGINE_OS_ATOMIC_H
#define TDENGINE_OS_ATOMIC_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #define atomic_load_8(ptr) (*(char volatile*)(ptr))
  #define atomic_load_16(ptr) (*(short volatile*)(ptr))
  #define atomic_load_32(ptr) (*(long volatile*)(ptr))
  #define atomic_load_64(ptr) (*(__int64 volatile*)(ptr))
  #define atomic_load_ptr(ptr) (*(void* volatile*)(ptr))

  #define atomic_store_8(ptr, val) ((*(char volatile*)(ptr)) = (char)(val))
  #define atomic_store_16(ptr, val) ((*(short volatile*)(ptr)) = (short)(val))
  #define atomic_store_32(ptr, val) ((*(long volatile*)(ptr)) = (long)(val))
  #define atomic_store_64(ptr, val) ((*(__int64 volatile*)(ptr)) = (__int64)(val))
  #define atomic_store_ptr(ptr, val) ((*(void* volatile*)(ptr)) = (void*)(val))

  #define atomic_exchange_8(ptr, val) _InterlockedExchange8((char volatile*)(ptr), (char)(val))
  #define atomic_exchange_16(ptr, val) _InterlockedExchange16((short volatile*)(ptr), (short)(val))
  #define atomic_exchange_32(ptr, val) _InterlockedExchange((long volatile*)(ptr), (long)(val))
  #define atomic_exchange_64(ptr, val) _InterlockedExchange64((__int64 volatile*)(ptr), (__int64)(val))
  #ifdef _WIN64 
    #define atomic_exchange_ptr(ptr, val) _InterlockedExchangePointer((void* volatile*)(ptr), (void*)(val)) 
  #else
    #define atomic_exchange_ptr(ptr, val) _InlineInterlockedExchangePointer((void* volatile*)(ptr), (void*)(val))
  #endif
  
  #ifdef _TD_GO_DLL_
    #define atomic_val_compare_exchange_8 __sync_val_compare_and_swap
  #else
    #define atomic_val_compare_exchange_8(ptr, oldval, newval) _InterlockedCompareExchange8((char volatile*)(ptr), (char)(newval), (char)(oldval))
  #endif
  #define atomic_val_compare_exchange_16(ptr, oldval, newval) _InterlockedCompareExchange16((short volatile*)(ptr), (short)(newval), (short)(oldval))
  #define atomic_val_compare_exchange_32(ptr, oldval, newval) _InterlockedCompareExchange((long volatile*)(ptr), (long)(newval), (long)(oldval))
  #define atomic_val_compare_exchange_64(ptr, oldval, newval) _InterlockedCompareExchange64((__int64 volatile*)(ptr), (__int64)(newval), (__int64)(oldval))
  #define atomic_val_compare_exchange_ptr(ptr, oldval, newval) _InterlockedCompareExchangePointer((void* volatile*)(ptr), (void*)(newval), (void*)(oldval))

  char    interlocked_add_fetch_8(char volatile *ptr, char val);
  short   interlocked_add_fetch_16(short volatile *ptr, short val);
  long    interlocked_add_fetch_32(long volatile *ptr, long val);
  __int64 interlocked_add_fetch_64(__int64 volatile *ptr, __int64 val);

  char interlocked_and_fetch_8(char volatile* ptr, char val);
  short interlocked_and_fetch_16(short volatile* ptr, short val);
  long interlocked_and_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_and_fetch_64(__int64 volatile* ptr, __int64 val);

  __int64 interlocked_fetch_and_64(__int64 volatile* ptr, __int64 val);

  char interlocked_or_fetch_8(char volatile* ptr, char val);
  short interlocked_or_fetch_16(short volatile* ptr, short val);
  long interlocked_or_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_or_fetch_64(__int64 volatile* ptr, __int64 val);

  char interlocked_xor_fetch_8(char volatile* ptr, char val);
  short interlocked_xor_fetch_16(short volatile* ptr, short val);
  long interlocked_xor_fetch_32(long volatile* ptr, long val);
  __int64 interlocked_xor_fetch_64(__int64 volatile* ptr, __int64 val);

  __int64 interlocked_fetch_xor_64(__int64 volatile* ptr, __int64 val);

  #define atomic_add_fetch_8(ptr, val) interlocked_add_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_add_fetch_16(ptr, val) interlocked_add_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_add_fetch_32(ptr, val) interlocked_add_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_add_fetch_64(ptr, val) interlocked_add_fetch_64((__int64 volatile*)(ptr), (__int64)(val))
  #ifdef _TD_GO_DLL_
    #define atomic_fetch_add_8 __sync_fetch_and_ad
    #define atomic_fetch_add_16 __sync_fetch_and_add
  #else
    #define atomic_fetch_add_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), (char)(val))
    #define atomic_fetch_add_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), (short)(val))
  #endif  
  #define atomic_fetch_add_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_add_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_add_32(ptr, val) _InterlockedExchangeAdd((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_add_64(ptr, val) _InterlockedExchangeAdd64((__int64 volatile*)(ptr), (__int64)(val))
  
  #define atomic_sub_fetch_8(ptr, val) interlocked_add_fetch_8((char volatile*)(ptr), -(char)(val))
  #define atomic_sub_fetch_16(ptr, val) interlocked_add_fetch_16((short volatile*)(ptr), -(short)(val))
  #define atomic_sub_fetch_32(ptr, val) interlocked_add_fetch_32((long volatile*)(ptr), -(long)(val))
  #define atomic_sub_fetch_64(ptr, val) interlocked_add_fetch_64((__int64 volatile*)(ptr), -(__int64)(val))

  #define atomic_fetch_sub_8(ptr, val) _InterlockedExchangeAdd8((char volatile*)(ptr), -(char)(val))
  #define atomic_fetch_sub_16(ptr, val) _InterlockedExchangeAdd16((short volatile*)(ptr), -(short)(val))
  #define atomic_fetch_sub_32(ptr, val) _InterlockedExchangeAdd((long volatile*)(ptr), -(long)(val))
  #define atomic_fetch_sub_64(ptr, val) _InterlockedExchangeAdd64((__int64 volatile*)(ptr), -(__int64)(val))

  #define atomic_and_fetch_8(ptr, val) interlocked_and_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_and_fetch_16(ptr, val) interlocked_and_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_and_fetch_32(ptr, val) interlocked_and_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_and_fetch_64(ptr, val) interlocked_and_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_and_8(ptr, val) _InterlockedAnd8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_and_16(ptr, val) _InterlockedAnd16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_and_32(ptr, val) _InterlockedAnd((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_and_64(ptr, val) interlocked_fetch_and_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_or_fetch_8(ptr, val) interlocked_or_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_or_fetch_16(ptr, val) interlocked_or_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_or_fetch_32(ptr, val) interlocked_or_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_or_fetch_64(ptr, val) interlocked_or_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_or_8(ptr, val) _InterlockedOr8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_or_16(ptr, val) _InterlockedOr16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_or_32(ptr, val) _InterlockedOr((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_or_64(ptr, val) interlocked_fetch_or_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_xor_fetch_8(ptr, val) interlocked_xor_fetch_8((char volatile*)(ptr), (char)(val))
  #define atomic_xor_fetch_16(ptr, val) interlocked_xor_fetch_16((short volatile*)(ptr), (short)(val))
  #define atomic_xor_fetch_32(ptr, val) interlocked_xor_fetch_32((long volatile*)(ptr), (long)(val))
  #define atomic_xor_fetch_64(ptr, val) interlocked_xor_fetch_64((__int64 volatile*)(ptr), (__int64)(val))

  #define atomic_fetch_xor_8(ptr, val) _InterlockedXor8((char volatile*)(ptr), (char)(val))
  #define atomic_fetch_xor_16(ptr, val) _InterlockedXor16((short volatile*)(ptr), (short)(val))
  #define atomic_fetch_xor_32(ptr, val) _InterlockedXor((long volatile*)(ptr), (long)(val))
  #define atomic_fetch_xor_64(ptr, val) interlocked_fetch_xor_64((__int64 volatile*)(ptr), (__int64)(val))

  #ifdef _WIN64
    #define atomic_add_fetch_ptr atomic_add_fetch_64
    #define atomic_fetch_add_ptr atomic_fetch_add_64
    #define atomic_sub_fetch_ptr atomic_sub_fetch_64
    #define atomic_fetch_sub_ptr atomic_fetch_sub_64
    #define atomic_and_fetch_ptr atomic_and_fetch_64
    #define atomic_fetch_and_ptr atomic_fetch_and_64
    #define atomic_or_fetch_ptr  atomic_or_fetch_64
    #define atomic_fetch_or_ptr  atomic_fetch_or_64
    #define atomic_xor_fetch_ptr atomic_xor_fetch_64
    #define atomic_fetch_xor_ptr atomic_fetch_xor_64
  #else
    #define atomic_add_fetch_ptr atomic_add_fetch_32
    #define atomic_fetch_add_ptr atomic_fetch_add_32
    #define atomic_sub_fetch_ptr atomic_sub_fetch_32
    #define atomic_fetch_sub_ptr atomic_fetch_sub_32
    #define atomic_and_fetch_ptr atomic_and_fetch_32
    #define atomic_fetch_and_ptr atomic_fetch_and_32
    #define atomic_or_fetch_ptr  atomic_or_fetch_32
    #define atomic_fetch_or_ptr  atomic_fetch_or_32
    #define atomic_xor_fetch_ptr atomic_xor_fetch_32
    #define atomic_fetch_xor_ptr atomic_fetch_xor_32
  #endif
#elif defined(_TD_NINGSI_60)
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

#else
  #define atomic_load_8(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_16(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_32(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_64(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)
  #define atomic_load_ptr(ptr) __atomic_load_n((ptr), __ATOMIC_SEQ_CST)

  #define atomic_store_8(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_16(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_32(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_64(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_store_ptr(ptr, val) __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_exchange_8(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_16(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_32(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_64(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_exchange_ptr(ptr, val) __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_val_compare_exchange_8 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_16 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_32 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_64 __sync_val_compare_and_swap
  #define atomic_val_compare_exchange_ptr __sync_val_compare_and_swap

  #define atomic_add_fetch_8(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_16(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_32(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_64(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_add_fetch_ptr(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_add_8(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_16(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_32(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_64(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_add_ptr(ptr, val) __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_sub_fetch_8(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_16(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_32(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_64(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_sub_fetch_ptr(ptr, val) __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_sub_8(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_16(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_32(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_64(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_sub_ptr(ptr, val) __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_and_fetch_8(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_16(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_32(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_64(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_and_fetch_ptr(ptr, val) __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_and_8(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_16(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_32(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_64(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_and_ptr(ptr, val) __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_or_fetch_8(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_16(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_32(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_64(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_or_fetch_ptr(ptr, val) __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_or_8(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_16(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_32(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_64(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_or_ptr(ptr, val) __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_xor_fetch_8(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_16(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_32(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_64(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_xor_fetch_ptr(ptr, val) __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST)

  #define atomic_fetch_xor_8(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_16(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_32(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_64(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
  #define atomic_fetch_xor_ptr(ptr, val) __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST)
#endif

#ifdef __cplusplus
}
#endif

#endif
