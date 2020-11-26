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

#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "tglobal.h"
#include "ttimer.h"
#include "tulog.h"
#include "tutil.h"

// add
char interlocked_add_fetch_8(char volatile* ptr, char val) {
#ifdef _TD_GO_DLL_
  return __sync_fetch_and_add(ptr, val) + val;
#else
  return _InterlockedExchangeAdd8(ptr, val) + val;
#endif
}

short interlocked_add_fetch_16(short volatile* ptr, short val) {
#ifdef _TD_GO_DLL_
  return __sync_fetch_and_add(ptr, val) + val;
#else
  return _InterlockedExchangeAdd16(ptr, val) + val;
#endif
}

long interlocked_add_fetch_32(long volatile* ptr, long val) {
  return _InterlockedExchangeAdd(ptr, val) + val;
}

__int64 interlocked_add_fetch_64(__int64 volatile* ptr, __int64 val) {
//#ifdef _WIN64
  return InterlockedExchangeAdd64(ptr, val) + val;
//#else
//  return _InterlockedExchangeAdd(ptr, val) + val;
//#endif
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

