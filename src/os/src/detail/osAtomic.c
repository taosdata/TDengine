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
