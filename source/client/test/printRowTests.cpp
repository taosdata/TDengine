/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "gtest/gtest.h"
#include <stdint.h>
#include <string.h>
#include "taos.h"

namespace {

const uint32_t kGuardLen = 32;
const uint8_t  kGuard    = 0x7f;

// the destination buffer is larger than the size handed to
// taos_print_row_with_size(), the bytes behind that size must stay untouched
char *initBuffer(char *buf, uint32_t size) {
  memset(buf, kGuard, size + kGuardLen);
  return buf;
}

bool guardIntact(const char *buf, uint32_t size) {
  for (uint32_t i = size; i < size + kGuardLen; ++i) {
    if ((uint8_t)buf[i] != kGuard) {
      return false;
    }
  }
  return true;
}

void initFields(TAOS_FIELD *fields, const int32_t *types, int32_t numOfFields) {
  memset(fields, 0, sizeof(TAOS_FIELD) * numOfFields);
  for (int32_t i = 0; i < numOfFields; ++i) {
    fields[i].type = types[i];
  }
}

}  // namespace

TEST(printRowCase, row_that_fits_is_not_truncated) {
  const uint32_t size = 8;
  char           buf[size + kGuardLen];
  int32_t        intVal   = 7;
  int8_t         tinyVal  = -3;
  void          *row[2]   = {&intVal, &tinyVal};
  int32_t        types[2] = {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_TINYINT};
  TAOS_FIELD     fields[2];

  initFields(fields, types, 2);
  int32_t len = taos_print_row_with_size(initBuffer(buf, size), size, row, fields, 2);

  EXPECT_TRUE(guardIntact(buf, size));
  EXPECT_EQ(4, len);
  EXPECT_STREQ("7 -3", buf);
}

TEST(printRowCase, null_field_in_the_middle_is_printed) {
  const uint32_t size = 8;
  char           buf[size + kGuardLen];
  int32_t        intVal   = 7;
  void          *row[2]   = {&intVal, NULL};
  int32_t        types[2] = {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT};
  TAOS_FIELD     fields[2];

  initFields(fields, types, 2);
  int32_t len = taos_print_row_with_size(initBuffer(buf, size), size, row, fields, 2);

  EXPECT_TRUE(guardIntact(buf, size));
  EXPECT_EQ(6, len);
  EXPECT_STREQ("7 NULL", buf);
}

TEST(printRowCase, value_longer_than_the_buffer_is_truncated) {
  const uint32_t size = 3;
  char           buf[size + kGuardLen];
  int32_t        intVal   = 1234567890;
  void          *row[1]   = {&intVal};
  int32_t        types[1] = {TSDB_DATA_TYPE_INT};
  TAOS_FIELD     fields[1];

  initFields(fields, types, 1);
  int32_t len = taos_print_row_with_size(initBuffer(buf, size), size, row, fields, 1);

  EXPECT_TRUE(guardIntact(buf, size));
  EXPECT_EQ(10, len);
  EXPECT_STREQ("12", buf);
}

TEST(printRowCase, trailing_null_fields_do_not_overflow_the_buffer) {
  // "NULL NU" fills the 8 byte buffer, the value of the third column does not
  // fit anymore: it must be dropped instead of being written behind the buffer
  const uint32_t size = 8;
  char           buf[size + kGuardLen];
  int32_t        intVal   = 1234567890;
  void          *row[3]   = {NULL, NULL, &intVal};
  int32_t        types[3] = {TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT,
                             TSDB_DATA_TYPE_INT};
  TAOS_FIELD     fields[3];

  initFields(fields, types, 3);
  int32_t len = taos_print_row_with_size(initBuffer(buf, size), size, row, fields, 3);

  EXPECT_TRUE(guardIntact(buf, size));
  EXPECT_LT(strlen(buf), size);
  EXPECT_EQ(9, len);
  EXPECT_STREQ("NULL NU", buf);
}

TEST(printRowCase, zero_size_leaves_the_buffer_untouched) {
  char           buf[kGuardLen];
  int32_t        intVal   = 7;
  void          *row[1]   = {&intVal};
  int32_t        types[1] = {TSDB_DATA_TYPE_INT};
  TAOS_FIELD     fields[1];

  initFields(fields, types, 1);
  int32_t len = taos_print_row_with_size(initBuffer(buf, 0), 0, row, fields, 1);

  // nothing was written at all
  EXPECT_TRUE(guardIntact(buf, 0));
  EXPECT_EQ(0, len);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
