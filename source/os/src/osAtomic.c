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

#define ALLOW_FORBID_FUNC
#include "os.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)

// add
char interlocked_add_fetch_8(char volatile* ptr, char val) {
  return _InterlockedExchangeAdd8(ptr, val) + val;
}

short interlocked_add_fetch_16(short volatile* ptr, short val) {
  return _InterlockedExchangeAdd16(ptr, val) + val;
}

long interlocked_add_fetch_32(long volatile* ptr, long val) {
  return _InterlockedExchangeAdd(ptr, val) + val;
}

__int64 interlocked_add_fetch_64(__int64 volatile* ptr, __int64 val) {
  return InterlockedExchangeAdd64(ptr, val) + val;
}

char interlocked_and_fetch_8(char volatile* ptr, char val) {
  return _InterlockedAnd8(ptr, val) & val;
}

short interlocked_and_fetch_16(short volatile* ptr, short val) {
  return _InterlockedAnd16(ptr, val) & val;
}

long interlocked_and_fetch_32(long volatile* ptr, long val) {
  return _InterlockedAnd(ptr, val) & val;
}


__int64 interlocked_and_fetch_64(__int64 volatile* ptr, __int64 val) {
#ifndef _M_IX86
  return _InterlockedAnd64(ptr, val) & val;
#else
  __int64 old, res;
  do {
    old = *ptr;
    res = old & val;
  } while (_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
#endif
}

__int64 interlocked_fetch_and_64(__int64 volatile* ptr, __int64 val) {
#ifdef _M_IX86
  __int64 old;
  do {
    old = *ptr;
  } while (_InterlockedCompareExchange64(ptr, old & val, old) != old);
  return old;
#else
  return _InterlockedAnd64((__int64 volatile*)(ptr), (__int64)(val));
#endif
}

char interlocked_or_fetch_8(char volatile* ptr, char val) {
  return _InterlockedOr8(ptr, val) | val;
}

short interlocked_or_fetch_16(short volatile* ptr, short val) {
  return _InterlockedOr16(ptr, val) | val;
}

long interlocked_or_fetch_32(long volatile* ptr, long val) {
  return _InterlockedOr(ptr, val) | val;
}

__int64 interlocked_or_fetch_64(__int64 volatile* ptr, __int64 val) {
#ifdef _M_IX86
  __int64 old, res;
  do {
    old = *ptr;
    res = old | val;
  } while(_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
#else
  return _InterlockedOr64(ptr, val) & val;
#endif
}

__int64 interlocked_fetch_or_64(__int64 volatile* ptr, __int64 val) {
#ifdef _M_IX86
  __int64 old;
  do {
    old = *ptr;
  } while(_InterlockedCompareExchange64(ptr, old | val, old) != old);
  return old;
#else
  return _InterlockedOr64((__int64 volatile*)(ptr), (__int64)(val));
#endif
}

char interlocked_xor_fetch_8(char volatile* ptr, char val) {
  return _InterlockedXor8(ptr, val) ^ val;
}

short interlocked_xor_fetch_16(short volatile* ptr, short val) {
  return _InterlockedXor16(ptr, val) ^ val;
}

long interlocked_xor_fetch_32(long volatile* ptr, long val) {
  return _InterlockedXor(ptr, val) ^ val;
}

__int64 interlocked_xor_fetch_64(__int64 volatile* ptr, __int64 val) {
#ifdef _M_IX86
  __int64 old, res;
  do {
    old = *ptr;
    res = old ^ val;
  } while(_InterlockedCompareExchange64(ptr, res, old) != old);
  return res;
#else
  return _InterlockedXor64(ptr, val) ^ val;
#endif
}

__int64 interlocked_fetch_xor_64(__int64 volatile* ptr, __int64 val) {
#ifdef _M_IX86
  __int64 old;
  do {
    old = *ptr;
  } while (_InterlockedCompareExchange64(ptr, old ^ val, old) != old);
  return old;
#else
  return _InterlockedXor64((__int64 volatile*)(ptr), (__int64)(val));
#endif
}

#endif

#ifdef _TD_NINGSI_60
void* atomic_exchange_ptr_impl(void** ptr, void* val ) {
  void *old;
  do {
    old = *ptr;
  } while( !__sync_bool_compare_and_swap(ptr, old, val) );
  return old;
}
int8_t atomic_exchange_8_impl(int8_t* ptr, int8_t val ) {
  int8_t old;
  do {
    old = *ptr;
  } while( !__sync_bool_compare_and_swap(ptr, old, val) );
  return old;
}
int16_t atomic_exchange_16_impl(int16_t* ptr, int16_t val ) {
  int16_t old;
  do {
    old = *ptr;
  } while( !__sync_bool_compare_and_swap(ptr, old, val) );
  return old;
}
int32_t atomic_exchange_32_impl(int32_t* ptr, int32_t val ) {
  int32_t old;
  do {
    old = *ptr;
  } while( !__sync_bool_compare_and_swap(ptr, old, val) );
  return old;
}
int64_t atomic_exchange_64_impl(int64_t* ptr, int64_t val ) {
  int64_t old;
  do {
    old = *ptr;
  } while( !__sync_bool_compare_and_swap(ptr, old, val) );
  return old;
}
#endif


int8_t atomic_load_8(int8_t volatile *ptr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return (*(int8_t volatile*)(ptr));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), 0);
#else
  return __atomic_load_n((ptr), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_load_16(int16_t volatile *ptr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return (*(int16_t volatile*)(ptr));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), 0);
#else
  return __atomic_load_n((ptr), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_load_32(int32_t volatile *ptr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return (*(int32_t volatile*)(ptr));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), 0);
#else
  return __atomic_load_n((ptr), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_load_64(int64_t volatile *ptr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return (*(int64_t volatile*)(ptr));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), 0);
#else
  return __atomic_load_n((ptr), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_load_ptr(void *ptr) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return (*(void* volatile*)(ptr));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), 0);
#else
  return __atomic_load_n((void**)(ptr), __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  ((*(int8_t volatile*)(ptr)) = (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  (*(ptr)=(val));
#else
  __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  ((*(int16_t volatile*)(ptr)) = (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  (*(ptr)=(val));
#else
  __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  ((*(int32_t volatile*)(ptr)) = (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  (*(ptr)=(val));
#else
  __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  ((*(int64_t volatile*)(ptr)) = (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  (*(ptr)=(val));
#else
  __atomic_store_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void atomic_store_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  ((*(void* volatile*)(ptr)) = (void*)(val));
#elif defined(_TD_NINGSI_60)
  (*(ptr)=(val));
#else
  __atomic_store_n((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_exchange_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchange8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return atomic_exchange_8_impl((int8_t*)ptr, (int8_t)val);
#else
  return __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_exchange_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchange16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return atomic_exchange_16_impl((int16_t*)ptr, (int16_t)val);
#else
  return __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_exchange_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchange((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return atomic_exchange_32_impl((int32_t*)ptr, (int32_t)val);
#else
  return __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_exchange_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchange64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return atomic_exchange_64_impl((int64_t*)ptr, (int64_t)val);
#else
  return __atomic_exchange_n((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_exchange_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  #ifdef _WIN64 
  return _InterlockedExchangePointer((void* volatile*)(ptr), (void*)(val));
  #else
  return _InlineInterlockedExchangePointer((void* volatile*)(ptr), (void*)(val));
  #endif
#elif defined(_TD_NINGSI_60)
  return atomic_exchange_ptr_impl((void *)ptr, (void*)val);
#else
  return __atomic_exchange_n((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_val_compare_exchange_8(int8_t volatile *ptr, int8_t oldval, int8_t newval) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedCompareExchange8((int8_t volatile*)(ptr), (int8_t)(newval), (int8_t)(oldval));
#elif defined(_TD_NINGSI_60)
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#endif
}

int16_t atomic_val_compare_exchange_16(int16_t volatile *ptr, int16_t oldval, int16_t newval) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedCompareExchange16((int16_t volatile*)(ptr), (int16_t)(newval), (int16_t)(oldval));
#elif defined(_TD_NINGSI_60)
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#endif
}

int32_t atomic_val_compare_exchange_32(int32_t volatile *ptr, int32_t oldval, int32_t newval) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedCompareExchange((int32_t volatile*)(ptr), (int32_t)(newval), (int32_t)(oldval));
#elif defined(_TD_NINGSI_60)
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#endif
}

int64_t atomic_val_compare_exchange_64(int64_t volatile *ptr, int64_t oldval, int64_t newval) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedCompareExchange64((int64_t volatile*)(ptr), (int64_t)(newval), (int64_t)(oldval));
#elif defined(_TD_NINGSI_60)
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#endif
}

void* atomic_val_compare_exchange_ptr(void *ptr, void *oldval, void *newval) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedCompareExchangePointer((void* volatile*)(ptr), (void*)(newval), (void*)(oldval));
#elif defined(_TD_NINGSI_60)
  return __sync_val_compare_and_swap(ptr, oldval, newval);
#else
  return __sync_val_compare_and_swap((void **)ptr, oldval, newval);
#endif
}

int8_t atomic_add_fetch_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_add_and_fetch((ptr), (val));
#else
  return __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_add_fetch_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_add_and_fetch((ptr), (val));
#else
  return __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_add_fetch_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_32((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_add_and_fetch((ptr), (val));
#else
  return __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_add_fetch_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_add_and_fetch((ptr), (val));
#else
  return __atomic_add_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_add_fetch_ptr(void *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_add_and_fetch((ptr), (val));
#else
  return __atomic_add_fetch((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_fetch_add_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), (val));
#else
  return __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_fetch_add_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), (val));
#else
  return __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_fetch_add_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), (val));
#else
  return __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_fetch_add_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), (val));
#else
  return __atomic_fetch_add((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_fetch_add_ptr(void *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAddptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_add((ptr), (val));
#else
  return __atomic_fetch_add((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_sub_fetch_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_8((int8_t volatile*)(ptr), -(int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_sub_and_fetch((ptr), (val));
#else
  return __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_sub_fetch_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_16((int16_t volatile*)(ptr), -(int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_sub_and_fetch((ptr), (val));
#else
  return __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_sub_fetch_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_32((int32_t volatile*)(ptr), -(int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_sub_and_fetch((ptr), (val));
#else
  return __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_sub_fetch_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_64((int64_t volatile*)(ptr), -(int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_sub_and_fetch((ptr), (val));
#else
  return __atomic_sub_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_sub_fetch_ptr(void *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_add_fetch_ptr((void* volatile*)(ptr), -(void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_sub_and_fetch((ptr), (val));
#else
  return __atomic_sub_fetch((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_fetch_sub_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd8((int8_t volatile*)(ptr), -(int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_sub((ptr), (val));
#else
  return __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_fetch_sub_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd16((int16_t volatile*)(ptr), -(int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_sub((ptr), (val));
#else
  return __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_fetch_sub_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd((int32_t volatile*)(ptr), -(int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_sub((ptr), (val));
#else
  return __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_fetch_sub_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAdd64((int64_t volatile*)(ptr), -(int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_sub((ptr), (val));
#else
  return __atomic_fetch_sub((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_fetch_sub_ptr(void *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedExchangeAddptr((void* volatile*)(ptr), -(void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_sub((ptr), (val));
#else
  return __atomic_fetch_sub((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_and_fetch_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_and_fetch_8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_and_and_fetch((ptr), (val));
#else
  return __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_and_fetch_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_and_fetch_16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_and_and_fetch((ptr), (val));
#else
  return __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_and_fetch_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_and_fetch_32((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_and_and_fetch((ptr), (val));
#else
  return __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_and_fetch_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_and_fetch_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_and_and_fetch((ptr), (val));
#else
  return __atomic_and_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_and_fetch_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_and_fetch_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_and_and_fetch((ptr), (val));
#else
  return __atomic_and_fetch((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_fetch_and_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedAnd8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_and((ptr), (val));
#else
  return __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_fetch_and_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedAnd16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_and((ptr), (val));
#else
  return __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_fetch_and_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedAnd((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_and((ptr), (val));
#else
  return __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_fetch_and_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_and_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_and((ptr), (val));
#else
  return __atomic_fetch_and((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_fetch_and_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_and_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_and((ptr), (val));
#else
  return __atomic_fetch_and((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_or_fetch_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_or_fetch_8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_or_and_fetch((ptr), (val));
#else
  return __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_or_fetch_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_or_fetch_16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_or_and_fetch((ptr), (val));
#else
  return __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_or_fetch_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_or_fetch_32((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_or_and_fetch((ptr), (val));
#else
  return __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_or_fetch_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_or_fetch_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_or_and_fetch((ptr), (val));
#else
  return __atomic_or_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_or_fetch_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_or_fetch_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_or_and_fetch((ptr), (val));
#else
  return __atomic_or_fetch((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_fetch_or_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedOr8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_or((ptr), (val));
#else
  return __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_fetch_or_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedOr16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_or((ptr), (val));
#else
  return __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_fetch_or_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedOr((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_or((ptr), (val));
#else
  return __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_fetch_or_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_or_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_or((ptr), (val));
#else
  return __atomic_fetch_or((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_fetch_or_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_or_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_or((ptr), (val));
#else
  return __atomic_fetch_or((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_xor_fetch_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_xor_fetch_8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_xor_and_fetch((ptr), (val));
#else
  return __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_xor_fetch_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_xor_fetch_16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_xor_and_fetch((ptr), (val));
#else
  return __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_xor_fetch_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_xor_fetch_32((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_xor_and_fetch((ptr), (val));
#else
  return __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_xor_fetch_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_xor_fetch_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_xor_and_fetch((ptr), (val));
#else
  return __atomic_xor_fetch((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_xor_fetch_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_xor_fetch_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_xor_and_fetch((ptr), (val));
#else
  return __atomic_xor_fetch((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int8_t atomic_fetch_xor_8(int8_t volatile *ptr, int8_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedXor8((int8_t volatile*)(ptr), (int8_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_xor((ptr), (val));
#else
  return __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int16_t atomic_fetch_xor_16(int16_t volatile *ptr, int16_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedXor16((int16_t volatile*)(ptr), (int16_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_xor((ptr), (val));
#else
  return __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int32_t atomic_fetch_xor_32(int32_t volatile *ptr, int32_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return _InterlockedXor((int32_t volatile*)(ptr), (int32_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_xor((ptr), (val));
#else
  return __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

int64_t atomic_fetch_xor_64(int64_t volatile *ptr, int64_t val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_xor_64((int64_t volatile*)(ptr), (int64_t)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_xor((ptr), (val));
#else
  return __atomic_fetch_xor((ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

void* atomic_fetch_xor_ptr(void *ptr, void *val) {
#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  return interlocked_fetch_xor_ptr((void* volatile*)(ptr), (void*)(val));
#elif defined(_TD_NINGSI_60)
  return __sync_fetch_and_xor((ptr), (val));
#else
  return __atomic_fetch_xor((void **)(ptr), (val), __ATOMIC_SEQ_CST);
#endif
}

