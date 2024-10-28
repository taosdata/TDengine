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
#include "shellAuto.h"

TEST(fieldOptionsArea, autoTabTest) {
  printf("hellow world  SHELL tab test\n");

  // str false
  const char *s0[] = {
      "create table st(ts ",
      "create table st(ts timestamp, age ",
      "create table st(ts timestamp,  age",
      "create table st(ts timestamp, age int ,  name ",
      "create table st(ts timestamp, age int ,  name  binary(16)",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int  ,  addr varbinary(32)",
      "create table st( ts timestamp, age int, name binary(16)) tags( area int,    addr",
      "create table st  (ts   timestamp ,  age int, name binary(16) , area int,",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int ,addr varbinary",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' , no i",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high', no  in",
  };

  // str true
  const char *s1[] = {
      "create table st(ts timestamp ",
      "create table st(ts timestamp, age int ",
      "create table st(ts timestamp,  age  int  ",
      "create table st(ts timestamp, age int ,  name binary(16) ",
      "create table st(ts timestamp, age int ,  name  binary(16)  ",
      "create table st(ts timestamp, age int ,  name  binary(16) , addr varbinary( 32 ) ",
      "create table st(ts timestamp, age int ,  name  binary(16) ,area int, addr varbinary(32) ",
      "create table st(ts timestamp, age int ,  name  binary(16), area int,addr varbinary(32) ",
      "create table st(ts timestamp, age int, name binary(16) , area int,addr varbinary(32) ",
      "create table st( ts timestamp, age int, name binary(16) ,area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16), area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16), area int , addr varbinary(32) compress 'zlib' ",
      "create table st  (ts   timestamp ,  age int, name binary(16),  area int , addr varbinary(32) level 'high' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) , area int , addr varbinary(32) encode 'simple8b' "
      "level 'high'   ",
  };

  // s0 is false
  for (int32_t i = 0; i < sizeof(s0) / sizeof(char *); i++) {
    printf("s0 i=%d fieldOptionsArea %s expect false \n", i, s0[i]);
    ASSERT(fieldOptionsArea((char *)s0[i]) == false);
  }

  // s1 is true
  for (int32_t i = 0; i < sizeof(s1) / sizeof(char *); i++) {
    printf("s1 i=%d fieldOptionsArea %s expect true \n", i, s1[i]);
    ASSERT(fieldOptionsArea((char *)s1[i]) == true);
  }
}

TEST(isCreateFieldsArea, autoTabTest) {
  printf("hellow world  SHELL tab test\n");

  // str false
  const char *s0[] = {
      "create table st(ts )",
      "create table st(ts timestamp, age) ",
      "create table st(ts timestamp,  age)",
      "create table st(ts timestamp, age int ,  name binary(16) )",
      "create table st(ts timestamp, age int ,  name  binary(16))",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( )",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr )",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary)",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int  ,  addr varbinary(32))",
      "create table st( ts timestamp, age int, name binary(16)) tags( area int,    addr int)",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int,addr varbinary(32) )",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int ,addr varbinary(14))",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' )",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high' )  ",
  };

  // str true
  const char *s1[] = {
      "create table st(ts timestamp ",
      "create table st(ts timestamp, age int ",
      "create table st(ts timestamp,  age  int  ,",
      "create table st(ts timestamp, age int ,  name binary(16), ",
      "create table st(ts timestamp, age int ,  name  binary(16)  ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int, addr varbinary(32) ",
      "create table st(ts timestamp, age int ,  name  binary(16) ) tags( area int,addr varbinary(32)",
      "create table st(ts timestamp, age int, name binary(16)) tags(area int,addr varbinary(32) ",
      "create table st( ts timestamp, age int, name binary(16)) tags(area int,addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int, addr varbinary(32) ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) compress "
      "'zlib' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) level "
      "'high' ",
      "create table st  (ts   timestamp ,  age int, name binary(16) ) tags ( area int , addr varbinary(32) encode "
      "'simple8b' level 'high' ",
  };

  // s0 is false
  for (int32_t i = 0; i < sizeof(s0) / sizeof(char *); i++) {
    printf("s0 i=%d isCreateFieldsArea %s expect false. \n", i, s0[i]);
    ASSERT(isCreateFieldsArea((char *)s0[i]) == false);
  }

  // s1 is true
  for (int32_t i = 0; i < sizeof(s1) / sizeof(char *); i++) {
    printf("s1 i=%d isCreateFieldsArea %s expect true. \n", i, s1[i]);
    ASSERT(isCreateFieldsArea((char *)s1[i]) == true);
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}