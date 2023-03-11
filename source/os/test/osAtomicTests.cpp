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

#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"

TEST(osAtomicTests, Exchange16) {
  int16_t value = 123;
  int16_t new_value = 456;
  int16_t result = atomic_exchange_16(&value, new_value);
  EXPECT_EQ(result, 123);
  EXPECT_EQ(value, 456);
}

TEST(osAtomicTests, Exchange32) {
  int32_t value = 123;
  int32_t new_value = 456;
  int32_t result = atomic_exchange_32(&value, new_value);
  EXPECT_EQ(result, 123);
  EXPECT_EQ(value, 456);
}

TEST(osAtomicTests, Exchange64) {
  int64_t value = 123;
  int64_t new_value = 456;
  int64_t result = atomic_exchange_64(&value, new_value);
  EXPECT_EQ(result, 123);
  EXPECT_EQ(value, 456);
}

TEST(osAtomicTests, ExchangePtr) {
  int  value1 = 123;
  int  value2 = 456;
  int* ptr = &value1;
  int* result = (int*)atomic_exchange_ptr(&ptr, &value2);
  EXPECT_EQ(result, &value1);
  EXPECT_EQ(*ptr, 456);
}

TEST(osAtomicTests, ValCompareExchange8) {
  int8_t value = 12;
  int8_t oldval = 12;
  int8_t newval = 45;
  int8_t result = atomic_val_compare_exchange_8(&value, oldval, newval);
  EXPECT_EQ(result, 12);
  EXPECT_EQ(value, 45);

  oldval = 78;
  result = atomic_val_compare_exchange_8(&value, oldval, newval);
  EXPECT_EQ(result, 45);
  EXPECT_EQ(value, 45);
}

TEST(osAtomicTests, ValCompareExchange16) {
  int16_t value = 123;
  int16_t oldval = 123;
  int16_t newval = 456;
  int16_t result = atomic_val_compare_exchange_16(&value, oldval, newval);
  EXPECT_EQ(result, 123);
  EXPECT_EQ(value, 456);

  oldval = 789;
  result = atomic_val_compare_exchange_16(&value, oldval, newval);
  EXPECT_EQ(result, 456);
  EXPECT_EQ(value, 456);
}

TEST(osAtomicTests, TestAtomicExchange8) {
  volatile int8_t value = 42;
  int8_t          new_value = 100;
  int8_t          old_value = atomic_exchange_8(&value, new_value);
  EXPECT_EQ(old_value, 42);
  EXPECT_EQ(value, new_value);
}

TEST(osAtomicTests, TestAtomicAddFetch16) {
  volatile int16_t value = 42;
  int16_t          increment = 10;
  int16_t          new_value = atomic_add_fetch_16(&value, increment);
  EXPECT_EQ(new_value, 52);
  EXPECT_EQ(value, 52);
}

//TEST(osAtomicTests, AddFetchPtr) {
//  uintptr_t  val = 0;
//  uintptr_t* ptr = &val;
//  uintptr_t  ret = atomic_add_fetch_ptr(ptr, 10);
//  EXPECT_EQ(ret, 10);
//  EXPECT_EQ(val, 10);
//}
