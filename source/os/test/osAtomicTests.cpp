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