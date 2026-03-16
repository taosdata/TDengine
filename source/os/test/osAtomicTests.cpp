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
#include <thread>
#include <vector>
#include <atomic>

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

TEST(osAtomicTests, atomic_load) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;
  void*   resultp = &result64;
  void*   valuep = &value64;

  result8 = atomic_load_8(&value8);
  result16 = atomic_load_16(&value16);
  result32 = atomic_load_32(&value32);
  result64 = atomic_load_64(&value64);
  resultp = atomic_load_ptr(&valuep);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(result64, 64);
  EXPECT_EQ(resultp, &value64);
  EXPECT_EQ(value8, 8);
  EXPECT_EQ(value16, 16);
  EXPECT_EQ(value32, 32);
  EXPECT_EQ(value64, 64);
  EXPECT_EQ(valuep, &value64);
}

TEST(osAtomicTests, atomic_store) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;
  double  resultd = 0, valued = 64;
  void*   resultp = &result64;
  void*   valuep = &value64;

  atomic_store_8(&result8, value8);
  atomic_store_16(&result16, value16);
  atomic_store_32(&result32, value32);
  atomic_store_64(&result64, value64);
  atomic_store_double(&resultd, valued);
  atomic_store_ptr(&resultp, valuep);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(result64, 64);
  EXPECT_DOUBLE_EQ(resultd, 64);
  EXPECT_EQ(resultp, &value64);
}

TEST(osAtomicTests, atomic_exchange) {
  int8_t  result8 = 0, value8 = 8, newval8 = 18;
  int16_t result16 = 0, value16 = 16, newval16 = 116;
  int32_t result32 = 0, value32 = 32, newval32 = 132;
  int64_t result64 = 0, value64 = 64, newval64 = 164;
  double  resultd = 0, valued = 64, newvald = 164;
  void*   valuep = &value64;
  void*   newvalp = &newval64;
  void*   resultp = &result64;

  result8 = atomic_exchange_8(&value8, newval8);
  result16 = atomic_exchange_16(&value16, newval16);
  result32 = atomic_exchange_32(&value32, newval32);
  result64 = atomic_exchange_64(&value64, newval64);
  resultd = atomic_exchange_double(&valued, newvald);
  resultp = atomic_exchange_ptr(&valuep, newvalp);

  EXPECT_EQ(value8, 18);
  EXPECT_EQ(value16, 116);
  EXPECT_EQ(value32, 132);
  EXPECT_EQ(value64, 164);
  EXPECT_DOUBLE_EQ(valued, 164);
  EXPECT_EQ(valuep, &newval64);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(result64, 64);
  EXPECT_DOUBLE_EQ(resultd, 64);
  EXPECT_EQ(resultp, &value64);
}

TEST(osAtomicTests, atomic_val_compare_exchange) {
  int8_t  result8 = 0, value8 = 8, oldval8 = 8, newval8 = 18;
  int16_t result16 = 0, value16 = 16, oldval16 = 16, newval16 = 116;
  int32_t result32 = 0, value32 = 32, oldval32 = 32, newval32 = 132;
  int64_t result64 = 0, value64 = 64, oldval64 = 64, newval64 = 164;
  void*   resultp = NULL;
  void*   valuep = &value64;
  void*   oldvalp = &value64;
  void*   newvalp = &newval64;

  result8 = atomic_val_compare_exchange_8(&value8, oldval8, newval8);
  result16 = atomic_val_compare_exchange_16(&value16, oldval16, newval16);
  result32 = atomic_val_compare_exchange_32(&value32, oldval32, newval32);
  result64 = atomic_val_compare_exchange_64(&value64, oldval64, newval64);
  resultp = atomic_val_compare_exchange_ptr(&valuep, oldvalp, newvalp);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(value8, 18);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(value16, 116);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(value32, 132);
  EXPECT_EQ(result64, 64);
  EXPECT_EQ(value64, 164);
  EXPECT_EQ(resultp, &value64);
  EXPECT_EQ(valuep, &newval64);

  oldval8 = 9;
  oldval16 = 99;
  oldval32 = 999;
  oldval64 = 9999;
  oldvalp = NULL;

  result8 = atomic_val_compare_exchange_8(&value8, oldval8, newval8);
  result16 = atomic_val_compare_exchange_16(&value16, oldval16, newval16);
  result32 = atomic_val_compare_exchange_32(&value32, oldval32, newval32);
  result64 = atomic_val_compare_exchange_64(&value64, oldval64, newval64);
  resultp = atomic_val_compare_exchange_ptr(&valuep, oldvalp, newvalp);

  EXPECT_EQ(result8, 18);
  EXPECT_EQ(value8, 18);
  EXPECT_EQ(result16, 116);
  EXPECT_EQ(value16, 116);
  EXPECT_EQ(result32, 132);
  EXPECT_EQ(value32, 132);
  EXPECT_EQ(result64, 164);
  EXPECT_EQ(value64, 164);
  EXPECT_EQ(resultp, &newval64);
  EXPECT_EQ(valuep, &newval64);
}

TEST(osAtomicTests, atomic_add_fetch) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;

  int64_t  valuex = 128;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_add_fetch_8(&value8, 10);
  result16 = atomic_add_fetch_16(&value16, 10);
  result32 = atomic_add_fetch_32(&value32, 10);
  result64 = atomic_add_fetch_64(&value64, 10);
  resultx = (int64_t)atomic_add_fetch_ptr(valuep, 10);

  EXPECT_EQ(result8, 18);
  EXPECT_EQ(value8, 18);
  EXPECT_EQ(result16, 26);
  EXPECT_EQ(value16, 26);
  EXPECT_EQ(result32, 42);
  EXPECT_EQ(value32, 42);
  EXPECT_EQ(result64, 74);
  EXPECT_EQ(value64, 74);
  EXPECT_EQ(resultx, 138);
  EXPECT_EQ(*valuep, 138);
  EXPECT_EQ(valuex, 138);
}

TEST(osAtomicTests, atomic_fetch_add) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;
  double  resultd = 0, valued = 64;

  int64_t  valuex = 128;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_fetch_add_8(&value8, 10);
  result16 = atomic_fetch_add_16(&value16, 10);
  result32 = atomic_fetch_add_32(&value32, 10);
  result64 = atomic_fetch_add_64(&value64, 10);
  resultd = atomic_fetch_add_double(&valued, 10);
  resultx = (int64_t)atomic_fetch_add_ptr(valuep, 10);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(value8, 18);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(value16, 26);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(value32, 42);
  EXPECT_EQ(result64, 64);
  EXPECT_EQ(value64, 74);
  EXPECT_DOUBLE_EQ(resultd, 64);
  EXPECT_DOUBLE_EQ(valued, 74);
  EXPECT_EQ(resultx, 128);
  EXPECT_EQ(*valuep, 138);
  EXPECT_EQ(valuex, 138);
}

TEST(osAtomicTests, atomic_sub_fetch) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;

  int64_t  valuex = 128;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_sub_fetch_8(&value8, 10);
  result16 = atomic_sub_fetch_16(&value16, 10);
  result32 = atomic_sub_fetch_32(&value32, 10);
  result64 = atomic_sub_fetch_64(&value64, 10);
  resultx = (int64_t)atomic_sub_fetch_ptr(valuep, 10);

  EXPECT_EQ(result8, -2);
  EXPECT_EQ(value8, -2);
  EXPECT_EQ(result16, 6);
  EXPECT_EQ(value16, 6);
  EXPECT_EQ(result32, 22);
  EXPECT_EQ(value32, 22);
  EXPECT_EQ(result64, 54);
  EXPECT_EQ(value64, 54);
  EXPECT_EQ(resultx, 118);
  EXPECT_EQ(*valuep, 118);
  EXPECT_EQ(valuex, 118);
}

TEST(osAtomicTests, atomic_fetch_sub) {
  int8_t  result8 = 0, value8 = 8;
  int16_t result16 = 0, value16 = 16;
  int32_t result32 = 0, value32 = 32;
  int64_t result64 = 0, value64 = 64;
  double  resultd = 0, valued = 64;

  int64_t  valuex = 128;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_fetch_sub_8(&value8, 10);
  result16 = atomic_fetch_sub_16(&value16, 10);
  result32 = atomic_fetch_sub_32(&value32, 10);
  result64 = atomic_fetch_sub_64(&value64, 10);
  resultd = atomic_fetch_sub_double(&valued, 10);
  resultx = (int64_t)atomic_fetch_sub_ptr(valuep, 10);

  EXPECT_EQ(result8, 8);
  EXPECT_EQ(value8, -2);
  EXPECT_EQ(result16, 16);
  EXPECT_EQ(value16, 6);
  EXPECT_EQ(result32, 32);
  EXPECT_EQ(value32, 22);
  EXPECT_EQ(result64, 64);
  EXPECT_EQ(value64, 54);
  EXPECT_DOUBLE_EQ(resultd, 64);
  EXPECT_DOUBLE_EQ(valued, 54);
  EXPECT_EQ(resultx, 128);
  EXPECT_EQ(*valuep, 118);
  EXPECT_EQ(valuex, 118);
}

TEST(osAtomicTests, atomic_and_fetch) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_and_fetch_8(&value8, 5);
  result16 = atomic_and_fetch_16(&value16, 5);
  result32 = atomic_and_fetch_32(&value32, 5);
  result64 = atomic_and_fetch_64(&value64, 5);
  resultx = (int64_t)atomic_and_fetch_ptr(valuep, 5);

  EXPECT_EQ(result8, 1);
  EXPECT_EQ(value8, 1);
  EXPECT_EQ(result16, 1);
  EXPECT_EQ(value16, 1);
  EXPECT_EQ(result32, 1);
  EXPECT_EQ(value32, 1);
  EXPECT_EQ(result64, 1);
  EXPECT_EQ(value64, 1);
  EXPECT_EQ(resultx, 1);
  EXPECT_EQ(*valuep, 1);
  EXPECT_EQ(valuex, 1);
}

TEST(osAtomicTests, atomic_fetch_and) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_fetch_and_8(&value8, 5);
  result16 = atomic_fetch_and_16(&value16, 5);
  result32 = atomic_fetch_and_32(&value32, 5);
  result64 = atomic_fetch_and_64(&value64, 5);
  resultx = (int64_t)atomic_fetch_and_ptr(valuep, 5);

  EXPECT_EQ(result8, 3);
  EXPECT_EQ(value8, 1);
  EXPECT_EQ(result16, 3);
  EXPECT_EQ(value16, 1);
  EXPECT_EQ(result32, 3);
  EXPECT_EQ(value32, 1);
  EXPECT_EQ(result64, 3);
  EXPECT_EQ(value64, 1);
  EXPECT_EQ(resultx, 3);
  EXPECT_EQ(*valuep, 1);
  EXPECT_EQ(valuex, 1);
}

TEST(osAtomicTests, atomic_or_fetch) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_or_fetch_8(&value8, 5);
  result16 = atomic_or_fetch_16(&value16, 5);
  result32 = atomic_or_fetch_32(&value32, 5);
  result64 = atomic_or_fetch_64(&value64, 5);
  resultx = (int64_t)atomic_or_fetch_ptr(valuep, 5);

  EXPECT_EQ(result8, 7);
  EXPECT_EQ(value8, 7);
  EXPECT_EQ(result16, 7);
  EXPECT_EQ(value16, 7);
  EXPECT_EQ(result32, 7);
  EXPECT_EQ(value32, 7);
  EXPECT_EQ(result64, 7);
  EXPECT_EQ(value64, 7);
  EXPECT_EQ(resultx, 7);
  EXPECT_EQ(*valuep, 7);
  EXPECT_EQ(valuex, 7);
}

TEST(osAtomicTests, atomic_fetch_or) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_fetch_or_8(&value8, 5);
  result16 = atomic_fetch_or_16(&value16, 5);
  result32 = atomic_fetch_or_32(&value32, 5);
  result64 = atomic_fetch_or_64(&value64, 5);
  resultx = (int64_t)atomic_fetch_or_ptr(valuep, 5);

  EXPECT_EQ(result8, 3);
  EXPECT_EQ(value8, 7);
  EXPECT_EQ(result16, 3);
  EXPECT_EQ(value16, 7);
  EXPECT_EQ(result32, 3);
  EXPECT_EQ(value32, 7);
  EXPECT_EQ(result64, 3);
  EXPECT_EQ(value64, 7);
  EXPECT_EQ(resultx, 3);
  EXPECT_EQ(*valuep, 7);
  EXPECT_EQ(valuex, 7);
}


TEST(osAtomicTests, atomic_xor_fetch) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_xor_fetch_8(&value8, 5);
  result16 = atomic_xor_fetch_16(&value16, 5);
  result32 = atomic_xor_fetch_32(&value32, 5);
  result64 = atomic_xor_fetch_64(&value64, 5);
  resultx = (int64_t)atomic_xor_fetch_ptr(valuep, 5);

  EXPECT_EQ(result8, 6);
  EXPECT_EQ(value8, 6);
  EXPECT_EQ(result16, 6);
  EXPECT_EQ(value16, 6);
  EXPECT_EQ(result32, 6);
  EXPECT_EQ(value32, 6);
  EXPECT_EQ(result64, 6);
  EXPECT_EQ(value64, 6);
  EXPECT_EQ(resultx, 6);
  EXPECT_EQ(*valuep, 6);
  EXPECT_EQ(valuex, 6);
}

TEST(osAtomicTests, atomic_fetch_xor) {
  int8_t  result8 = 0, value8 = 3;
  int16_t result16 = 0, value16 = 3;
  int32_t result32 = 0, value32 = 3;
  int64_t result64 = 0, value64 = 3;

  int64_t  valuex = 3;
  int64_t* valuep = &valuex;
  int64_t  resultx = 0;

  result8 = atomic_fetch_xor_8(&value8, 5);
  result16 = atomic_fetch_xor_16(&value16, 5);
  result32 = atomic_fetch_xor_32(&value32, 5);
  result64 = atomic_fetch_xor_64(&value64, 5);
  resultx = (int64_t)atomic_fetch_xor_ptr(valuep, 5);

  EXPECT_EQ(result8, 3);
  EXPECT_EQ(value8, 6);
  EXPECT_EQ(result16, 3);
  EXPECT_EQ(value16, 6);
  EXPECT_EQ(result32, 3);
  EXPECT_EQ(value32, 6);
  EXPECT_EQ(result64, 3);
  EXPECT_EQ(value64, 6);
  EXPECT_EQ(resultx, 3);
  EXPECT_EQ(*valuep, 6);
  EXPECT_EQ(valuex, 6);
}

TEST(osAtomicTests, atomic_add_fetch_64_relax) {
  int64_t value64 = 100;
  int64_t result64 = 0;

  // Basic functionality
  result64 = atomic_add_fetch_64_relax(&value64, 50);
  EXPECT_EQ(result64, 150);
  EXPECT_EQ(value64, 150);

  // Negative addition
  result64 = atomic_add_fetch_64_relax(&value64, -50);
  EXPECT_EQ(result64, 100);
  EXPECT_EQ(value64, 100);

  // Zero addition
  result64 = atomic_add_fetch_64_relax(&value64, 0);
  EXPECT_EQ(result64, 100);
  EXPECT_EQ(value64, 100);
}

TEST(osAtomicTests, atomic_operations_edge_cases) {
  // INT8 boundaries
  int8_t val8_max = INT8_MAX;
  int8_t val8_min = INT8_MIN;

  atomic_add_fetch_8(&val8_max, 1);  // Overflow
  EXPECT_EQ(val8_max, INT8_MIN);

  atomic_sub_fetch_8(&val8_min, 1);  // Underflow
  EXPECT_EQ(val8_min, INT8_MAX);

  // INT16 boundaries
  int16_t val16_max = INT16_MAX;
  int16_t val16_min = INT16_MIN;

  atomic_add_fetch_16(&val16_max, 1);
  EXPECT_EQ(val16_max, INT16_MIN);

  atomic_sub_fetch_16(&val16_min, 1);
  EXPECT_EQ(val16_min, INT16_MAX);

  // INT32 boundaries
  int32_t val32_max = INT32_MAX;
  int32_t val32_min = INT32_MIN;

  atomic_add_fetch_32(&val32_max, 1);
  EXPECT_EQ(val32_max, INT32_MIN);

  atomic_sub_fetch_32(&val32_min, 1);
  EXPECT_EQ(val32_min, INT32_MAX);

  // INT64 boundaries
  int64_t val64_max = INT64_MAX;
  int64_t val64_min = INT64_MIN;

  atomic_add_fetch_64(&val64_max, 1);
  EXPECT_EQ(val64_max, INT64_MIN);

  atomic_sub_fetch_64(&val64_min, 1);
  EXPECT_EQ(val64_min, INT64_MAX);
}

TEST(osAtomicTests, atomic_bitwise_edge_cases) {
  // AND with 0 (should zero out)
  int32_t val32 = 0xFFFFFFFF;
  atomic_and_fetch_32(&val32, 0);
  EXPECT_EQ(val32, 0);

  // OR with 0 (should be no-op)
  val32 = 0x12345678;
  atomic_or_fetch_32(&val32, 0);
  EXPECT_EQ(val32, 0x12345678);

  // XOR with self (should zero out)
  val32 = 0x12345678;
  atomic_xor_fetch_32(&val32, 0x12345678);
  EXPECT_EQ(val32, 0);

  // XOR with 0 (should be no-op)
  val32 = 0x12345678;
  atomic_xor_fetch_32(&val32, 0);
  EXPECT_EQ(val32, 0x12345678);

  // AND with all 1s (should be no-op)
  int64_t val64 = 0x123456789ABCDEF0LL;
  atomic_and_fetch_64(&val64, -1LL);
  EXPECT_EQ(val64, 0x123456789ABCDEF0LL);
}

TEST(osAtomicTests, atomic_negative_numbers) {
  // Negative number operations
  int32_t neg_val = -100;

  atomic_add_fetch_32(&neg_val, -50);
  EXPECT_EQ(neg_val, -150);

  atomic_sub_fetch_32(&neg_val, -50);
  EXPECT_EQ(neg_val, -100);

  atomic_fetch_add_32(&neg_val, 200);
  EXPECT_EQ(neg_val, 100);

  // Negative bitwise operations
  int32_t neg_bits = -1;  // All bits set
  atomic_and_fetch_32(&neg_bits, 0x0F0F0F0F);
  EXPECT_EQ(neg_bits, 0x0F0F0F0F);
}

TEST(osAtomicTests, atomic_double_special_values) {
  double val = 0.0;
  double result = 0.0;

  // Positive zero
  atomic_store_double(&val, 0.0);
  EXPECT_DOUBLE_EQ(val, 0.0);

  // Negative zero
  atomic_store_double(&val, -0.0);
  EXPECT_DOUBLE_EQ(val, -0.0);

  // Very small numbers
  atomic_store_double(&val, 1e-308);
  EXPECT_DOUBLE_EQ(val, 1e-308);

  // Very large numbers
  atomic_store_double(&val, 1e308);
  EXPECT_DOUBLE_EQ(val, 1e308);

  // Infinity
  atomic_store_double(&val, INFINITY);
  EXPECT_TRUE(std::isinf(val));

  // NaN
  atomic_store_double(&val, NAN);
  EXPECT_TRUE(std::isnan(val));

  // Exchange with special values
  val = 1.0;
  result = atomic_exchange_double(&val, INFINITY);
  EXPECT_DOUBLE_EQ(result, 1.0);
  EXPECT_TRUE(std::isinf(val));

  // Add operations with fractional values
  val = 0.1;
  for (int i = 0; i < 10; i++) {
    atomic_fetch_add_double(&val, 0.1);
  }
  EXPECT_NEAR(val, 1.1, 1e-10);

  // Subtract operations
  val = 1.0;
  result = atomic_fetch_sub_double(&val, 0.3);
  EXPECT_DOUBLE_EQ(result, 1.0);
  EXPECT_NEAR(val, 0.7, 1e-10);
}

TEST(osAtomicTests, atomic_pointer_null) {
  void* ptr = nullptr;
  void* result = nullptr;

  // Load null
  result = atomic_load_ptr(&ptr);
  EXPECT_EQ(result, nullptr);

  // Store null
  ptr = (void*)0x12345678;
  atomic_store_ptr(&ptr, nullptr);
  EXPECT_EQ(ptr, nullptr);

  // Exchange with null
  ptr = (void*)0x12345678;
  result = atomic_exchange_ptr(&ptr, nullptr);
  EXPECT_EQ(result, (void*)0x12345678);
  EXPECT_EQ(ptr, nullptr);

  // Compare exchange with null
  ptr = nullptr;
  result = atomic_val_compare_exchange_ptr(&ptr, nullptr, (void*)0xABCDEF);
  EXPECT_EQ(result, nullptr);
  EXPECT_EQ(ptr, (void*)0xABCDEF);
}

TEST(osAtomicTests, atomic_concurrent_increment) {
  const int NUM_THREADS = 10;
  const int INCREMENTS_PER_THREAD = 10000;

  int64_t counter = 0;
  std::vector<std::thread> threads;

  // Launch threads that increment counter
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([&counter, INCREMENTS_PER_THREAD]() {
      for (int j = 0; j < INCREMENTS_PER_THREAD; j++) {
        atomic_add_fetch_64(&counter, 1);
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  // Verify final count
  EXPECT_EQ(counter, NUM_THREADS * INCREMENTS_PER_THREAD);
}

TEST(osAtomicTests, atomic_concurrent_mixed_operations) {
  const int NUM_THREADS = 8;
  const int OPERATIONS = 5000;

  int64_t value = 1000000;
  std::vector<std::thread> threads;

  // Half threads add, half subtract
  for (int i = 0; i < NUM_THREADS; i++) {
    if (i % 2 == 0) {
      threads.emplace_back([&value, OPERATIONS]() {
        for (int j = 0; j < OPERATIONS; j++) {
          atomic_add_fetch_64(&value, 1);
        }
      });
    } else {
      threads.emplace_back([&value, OPERATIONS]() {
        for (int j = 0; j < OPERATIONS; j++) {
          atomic_sub_fetch_64(&value, 1);
        }
      });
    }
  }

  for (auto& t : threads) {
    t.join();
  }

  // Should return to original value
  EXPECT_EQ(value, 1000000);
}

TEST(osAtomicTests, atomic_concurrent_compare_exchange) {
  const int NUM_THREADS = 10;
  int64_t value = 0;
  std::atomic<int> success_count{0};
  std::vector<std::thread> threads;

  // Each thread tries to CAS from 0 to its thread ID
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([&value, &success_count, i]() {
      int64_t old_val = atomic_val_compare_exchange_64(&value, 0, i + 1);
      if (old_val == 0) {
        success_count++;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Exactly one thread should succeed
  EXPECT_EQ(success_count.load(), 1);
  EXPECT_GE(value, 1);
  EXPECT_LE(value, NUM_THREADS);
}

TEST(osAtomicTests, atomic_concurrent_bitwise) {
  const int NUM_THREADS = 8;
  const int OPERATIONS = 1000;

  int64_t value = 0;
  std::vector<std::thread> threads;

  // Each thread sets different bits
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([&value, i, OPERATIONS]() {
      int64_t bit_pattern = 1LL << (i * 8);
      for (int j = 0; j < OPERATIONS; j++) {
        atomic_or_fetch_64(&value, bit_pattern);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Verify all bits are set
  for (int i = 0; i < NUM_THREADS; i++) {
    int64_t bit_pattern = 1LL << (i * 8);
    EXPECT_NE(value & bit_pattern, 0);
  }
}

TEST(osAtomicTests, atomic_concurrent_double) {
  const int NUM_THREADS = 10;
  const int OPERATIONS = 1000;

  double value = 0.0;
  std::vector<std::thread> threads;

  // Each thread adds 0.1
  for (int i = 0; i < NUM_THREADS; i++) {
    threads.emplace_back([&value, OPERATIONS]() {
      for (int j = 0; j < OPERATIONS; j++) {
        atomic_fetch_add_double(&value, 0.1);
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // Should be approximately NUM_THREADS * OPERATIONS * 0.1
  double expected = NUM_THREADS * OPERATIONS * 0.1;
  EXPECT_NEAR(value, expected, expected * 0.01);  // 1% tolerance
}