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

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "../src/clientSml.c"
#include "taos.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, sml_Test) {
  char msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;
  SSmlLineInfo elements = {0};

  //case 1
  char *sql = "st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000    ,32,c=3";
  int ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st,t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.tags, sql + elements.measureLen + 1);
  ASSERT_EQ(elements.tagsLen, strlen("t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 1 + elements.colsLen + 1);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));

  // case 2  false
  sql = "st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_NE(ret, 0);

  // case 3  false
  sql = "st, t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 2);
  ASSERT_EQ(elements.colsLen, strlen("t1=3,t2=4,t3=t3"));


  // case 4  tag is null
  sql = "st, c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st"));

  ASSERT_EQ(elements.tags, sql + elements.measureLen + 1);
  ASSERT_EQ(elements.tagsLen, 0);

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 2);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 2 + elements.colsLen + 1);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));

  // case 5 tag is null
  sql = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64  1626006833639000000 ";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  sql++;
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st"));

  ASSERT_EQ(elements.tags, sql + elements.measureLen);
  ASSERT_EQ(elements.tagsLen, 0);

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 3);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 3 + elements.colsLen + 2);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));

  // case 6
  sql = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64   ";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);

  // case 7
  sql = " st   ,   ";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  sql++;
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 3);
  ASSERT_EQ(elements.colsLen, strlen(","));

  // case 8 false
  sql = ", st   ,   ";
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseString(sql, &elements, &msgBuf);
  ASSERT_NE(ret, 0);
}


