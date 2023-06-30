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

#include "../inc/clientSml.h"
#include "taos.h"

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, smlParseInfluxString_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;
  SSmlLineInfo elements = {0};

  SSmlHandle *info = smlBuildSmlInfo(NULL);
  info->protocol = TSDB_SML_LINE_PROTOCOL;
  info->dataFormat = false;
  // case 1
  char *tmp = "\\,st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000    ,32,c=3";
  char *sql = (char *)taosMemoryCalloc(256, 1);
  memcpy(sql, tmp, strlen(tmp) + 1);
  int ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen("\\,st"));
  ASSERT_EQ(elements.measureEscaped, true);
  ASSERT_EQ(elements.measureTagsLen, strlen("\\,st,t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.tags, sql + elements.measureLen + 1);
  ASSERT_EQ(elements.tagsLen, strlen("t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 1 + elements.colsLen + 1);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  // case 2  false
  tmp = "st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_NE(ret, 0);
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  // case 4  tag is null
  tmp = "st, c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st,"));

  ASSERT_EQ(elements.tags, sql + elements.measureTagsLen);
  ASSERT_EQ(elements.tagsLen, 0);

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 1 + elements.colsLen + 1);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  // case 5 tag is null
  tmp = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64  1626006833639000000 ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql + 1);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st"));

  ASSERT_EQ(elements.tagsLen, 0);

  ASSERT_EQ(elements.cols, sql + 1 + elements.measureTagsLen + 3);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + 1 + elements.measureTagsLen + 3 + elements.colsLen + 2);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  // case 6
  tmp = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_EQ(ret, 0);
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;
  smlClearForRerun(info);

  // case 7
  tmp = " st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_NE(ret, 0);
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  // case 8 false
  tmp = ", st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(info, sql, sql + strlen(sql), &elements);
  ASSERT_NE(ret, 0);
  taosArrayDestroy(elements.colArray);
  elements.colArray = NULL;

  taosMemoryFree(sql);
  smlDestroyInfo(info);

}

TEST(testCase, smlParseCols_Error_Test) {
  const char *data[] = {"st,t=1 c=\"89sd 1626006833639000000",  // binary, nchar
                        "st,t=1 c=j\"89sd\" 1626006833639000000",
                        "st,t=1 c=\"89sd\"k 1626006833639000000",
                        "st,t=1 c=u 1626006833639000000",  // bool
                        "st,t=1 c=truet 1626006833639000000",
                        "st,t=1 c=f64 1626006833639000000",  // double
                        "st,t=1 c=8f64f 1626006833639000000",
                        "st,t=1 c=8ef64 1626006833639000000",
                        "st,t=1 c=f32 1626006833639000000",  // float
                        "st,t=1 c=8f32f 1626006833639000000",
                        "st,t=1 c=8wef32 1626006833639000000",
                        "st,t=1 c=-3.402823466e+39f32 1626006833639000000",
                        "st,t=1 c= 1626006833639000000",  // double
                        "st,t=1 c=8f 1626006833639000000",
                        "st,t=1 c=8we 1626006833639000000",
                        "st,t=1 c=i8 1626006833639000000",  // tiny int
                        "st,t=1 c=-8i8f 1626006833639000000",
                        "st,t=1 c=8wei8 1626006833639000000",
                        "st,t=1 c=-999i8 1626006833639000000",
                        "st,t=1 c=u8 1626006833639000000",  // u tiny int
                        "st,t=1 c=8fu8 1626006833639000000",
                        "st,t=1 c=8weu8 1626006833639000000",
                        "st,t=1 c=999u8 1626006833639000000",
                        "st,t=1 c=-8u8 1626006833639000000",
                        "st,t=1 c=i16 1626006833639000000",  // small int
                        "st,t=1 c=8fi16u 1626006833639000000",
                        "st,t=1 c=8wei16 1626006833639000000",
                        "st,t=1 c=-67787i16 1626006833639000000",
                        "st,t=1 c=u16 1626006833639000000",  // u small int
                        "st,t=1 c=8u16f 1626006833639000000",
                        "st,t=1 c=8weu16 1626006833639000000",
                        "st,t=1 c=-9u16 1626006833639000000",
                        "st,t=1 c=67787u16 1626006833639000000",
                        "st,t=1 c=i32 1626006833639000000",  // int
                        "st,t=1 c=8i32f 1626006833639000000",
                        "st,t=1 c=8wei32 1626006833639000000",
                        "st,t=1 c=2147483649i32 1626006833639000000",
                        "st,t=1 c=u32 1626006833639000000",  // u int
                        "st,t=1 c=8u32f 1626006833639000000",
                        "st,t=1 c=8weu32 1626006833639000000",
                        "st,t=1 c=-4u32 1626006833639000000",
                        "st,t=1 c=42949672958u32 1626006833639000000",
                        "st,t=1 c=i64 1626006833639000000",  // big int
                        "st,t=1 c=8i64i 1626006833639000000",
                        "st,t=1 c=8wei64 1626006833639000000",
                        "st,t=1 c=-9223372036854775809i64 1626006833639000000",
                        "st,t=1 c=i 1626006833639000000",  // big int
                        "st,t=1 c=8fi 1626006833639000000",
                        "st,t=1 c=8wei 1626006833639000000",
                        "st,t=1 c=9223372036854775808i 1626006833639000000",
                        "st,t=1 c=u64 1626006833639000000",  // u big int
                        "st,t=1 c=8u64f 1626006833639000000",
                        "st,t=1 c=8weu64 1626006833639000000",
                        "st,t=1 c=-3.402823466e+39u64 1626006833639000000",
                        "st,t=1 c=-339u64 1626006833639000000",
                        "st,t=1 c=18446744073709551616u64 1626006833639000000",
                        "st,t=1 c=1=2 1626006833639000000,",
                        // escape error test
                        // measure comma,space
                        "s,t,t=1 c=1 1626006833639000000,",
                        "s t,t=1 c=1 1626006833639000000,",
                        //tag key comma,equal,space
                        "st,t,t=1 c=2 1626006833639000000,",
                        "st,t=t=1 c=2 1626006833639000000,",
                        "st,t t=1 c=2 1626006833639000000,",
                        //tag value comma,equal,space
                        "st,tt=a,a c=2 1626006833639000000,",
                        "st,t=t=a a c=2 1626006833639000000,",
                        "st,t t=a=a c=2 1626006833639000000,",
                        //field key comma,equal,space
                        "st,tt=aa c,1=2 1626006833639000000,",
                        "st,tt=aa c=1=2 1626006833639000000,",
                        "st,tt=aa c 1=2 1626006833639000000,",
                        //field value    double quote,slash
                        "st,tt=aa c=\"a\"a\" 1626006833639000000,",
                        "escape_test,tag1=\"tag1_value\",tag2=\"tag2_value\" co l0=\"col0_value\",col1=\"col1_value\" 1680918783010000000",
                        "escape_test,tag1=\"tag1_value\",tag2=\"tag2_value\" col0=\"co\"l\"0_value\",col1=\"col1_value\" 1680918783010000000"
  };

  SSmlHandle *info = smlBuildSmlInfo(NULL);
  info->protocol = TSDB_SML_LINE_PROTOCOL;
  info->dataFormat = false;
  for (int i = 0; i < sizeof(data) / sizeof(data[0]); i++) {
    char       msg[256] = {0};
    SSmlMsgBuf msgBuf;
    msgBuf.buf = msg;
    msgBuf.len = 256;
    int32_t len = strlen(data[i]);
    char   *sql = (char *)taosMemoryCalloc(256, 1);
    memcpy(sql, data[i], len + 1);
    SSmlLineInfo elements = {0};
    int32_t ret = smlParseInfluxString(info, sql, sql + len, &elements);
//    printf("i:%d\n", i);
    ASSERT_NE(ret, TSDB_CODE_SUCCESS);
    taosMemoryFree(sql);
    taosArrayDestroy(elements.colArray);
  }
  smlDestroyInfo(info);
}

TEST(testCase, smlParseCols_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;
  SSmlHandle *info = smlBuildSmlInfo(NULL);
  info->protocol = TSDB_SML_LINE_PROTOCOL;
  info->dataFormat = false;
  SSmlLineInfo elements = {0};
  info->msgBuf = msgBuf;

  const char *data =
      "st,t=1 cb\\=in=\"pass\\,it "
      "hello,c=2\",cnch=L\"ii\\=sdfsf\",cbool=false,cf64=4.31f64,cf64_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16="
      "233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,"
      "cboolf=f,cnch_=l\"iuwq\" 1626006833639000000";
  int32_t len = strlen(data);
  char   *sql = (char *)taosMemoryCalloc(1024, 1);
  memcpy(sql, data, len + 1);
  int32_t ret = smlParseInfluxString(info, sql, sql + len, &elements);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  int32_t size = taosArrayGetSize(elements.colArray);
  ASSERT_EQ(size, 20);

  // binary
  SSmlKv *kv = (SSmlKv *)taosArrayGet(elements.colArray, 1);
  ASSERT_EQ(strncasecmp(kv->key, "cb=in", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BINARY);
  ASSERT_EQ(kv->length, 18);
  ASSERT_EQ(kv->keyEscaped, true);
  ASSERT_EQ(kv->valueEscaped, false);
  ASSERT_EQ(strncasecmp(kv->value, "pass\\,it ", 9), 0);

  // nchar
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 2);
  ASSERT_EQ(strncasecmp(kv->key, "cnch", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 9);
  ASSERT_EQ(strncasecmp(kv->value, "ii\\=sd", 5), 0);

  // bool
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 3);
  ASSERT_EQ(strncasecmp(kv->key, "cbool", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);

  // double
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 4);
  ASSERT_EQ(strncasecmp(kv->key, "cf64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  // ASSERT_EQ(kv->d, 4.31);
  printf("4.31 = kv->d:%f\n", kv->d);

  // float
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 5);
  ASSERT_EQ(strncasecmp(kv->key, "cf64_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  // ASSERT_EQ(kv->f, 8.32);
  printf("8.32 = kv->d:%f\n", kv->d);

  // float
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 6);
  ASSERT_EQ(strncasecmp(kv->key, "cf32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_FLOAT);
  ASSERT_EQ(kv->length, 4);
  // ASSERT_EQ(kv->f, 8.23);
  printf("8.23 = kv->f:%f\n", kv->f);

  // tiny int
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 7);
  ASSERT_EQ(strncasecmp(kv->key, "ci8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_TINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, -34);

  // unsigned tiny int
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 8);
  ASSERT_EQ(strncasecmp(kv->key, "cu8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UTINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->u, 89);

  // small int
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 9);
  ASSERT_EQ(strncasecmp(kv->key, "ci16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_SMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 233);

  // unsigned smallint
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 10);
  ASSERT_EQ(strncasecmp(kv->key, "cu16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_USMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 898);

  // int
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 11);
  ASSERT_EQ(strncasecmp(kv->key, "ci32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 98289);

  // unsigned int
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 12);
  ASSERT_EQ(strncasecmp(kv->key, "cu32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UINT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 12323);

  // bigint
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 13);
  ASSERT_EQ(strncasecmp(kv->key, "ci64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, -89238);

  // bigint
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 14);
  ASSERT_EQ(strncasecmp(kv->key, "ci", 2), 0);
  ASSERT_EQ(kv->keyLen, 2);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, 989);

  // unsigned bigint
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 15);
  ASSERT_EQ(strncasecmp(kv->key, "cu64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UBIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->u, 8989323);

  // bool
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 16);
  ASSERT_EQ(strncasecmp(kv->key, "cbooltrue", 9), 0);
  ASSERT_EQ(kv->keyLen, 9);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);

  // bool
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 17);
  ASSERT_EQ(strncasecmp(kv->key, "cboolt", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);

  // bool
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 18);
  ASSERT_EQ(strncasecmp(kv->key, "cboolf", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);

  // nchar
  kv = (SSmlKv *)taosArrayGet(elements.colArray, 19);
  ASSERT_EQ(strncasecmp(kv->key, "cnch_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(strncasecmp(kv->value, "iuwq", 4), 0);

  taosArrayDestroy(elements.colArray);
  taosMemoryFree(sql);
  smlDestroyInfo(info);
}

TEST(testCase, smlGetTimestampLen_Test) {
  uint8_t len = smlGetTimestampLen(0);
  ASSERT_EQ(len, 1);

  len = smlGetTimestampLen(1);
  ASSERT_EQ(len, 1);

  len = smlGetTimestampLen(10);
  ASSERT_EQ(len, 2);

  len = smlGetTimestampLen(390);
  ASSERT_EQ(len, 3);

  len = smlGetTimestampLen(-1);
  ASSERT_EQ(len, 1);

  len = smlGetTimestampLen(-10);
  ASSERT_EQ(len, 2);

  len = smlGetTimestampLen(-390);
  ASSERT_EQ(len, 3);
}

TEST(testCase, smlParseNumber_Test) {
  SSmlKv     kv = {0};
  char       buf[64] = {0};
  SSmlMsgBuf msg = {0};
  msg.buf = buf;
  msg.len = 64;
  kv.value = "3.2e-900";
  kv.length = 8;
  bool res = smlParseNumber(&kv, &msg);
  printf("res:%d,v:%f, %f\n", res, kv.d, HUGE_VAL);
}

TEST(testCase, smlParseTelnetLine_error_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL);
  info->dataFormat = false;
  info->protocol = TSDB_SML_TELNET_PROTOCOL;
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "sys.procs.running 14794961040 42 host=web01",
      "sys.procs.running 14791040 42 host=web01",
      "sys.procs.running erere 42 host=web01",
      "sys.procs.running 1.6e10 42 host=web01",
      "sys.procs.running 1.47949610 42 host=web01",
      "sys.procs.running 147949610i 42 host=web01",
      "sys.procs.running -147949610 42 host=web01",
      "",
      "   ",
      "sys  ",
      "sys.procs.running 1479496100 42 ",
      "sys.procs.running 1479496100 42 host= ",
      "sys.procs.running 1479496100 42or host=web01",
//      "sys.procs.running 1479496100 true host=web01",
//      "sys.procs.running 1479496100 \"binary\" host=web01",
//      "sys.procs.running 1479496100 L\"rfr\" host=web01",
      "sys.procs.running 1479496100 42 host=web01 cpu= ",
      "sys.procs.running 1479496100 42 host=web01 host",
      "sys.procs.running 1479496100 42 host=web01=er",
      "sys.procs.running 1479496100 42 host= web01",
  };
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    SSmlLineInfo elements = {0};
    int ret = smlParseTelnetString(info, (char*)sql[i], (char*)(sql[i] + strlen(sql[i])), &elements);
//    printf("i:%d\n", i);
    ASSERT_NE(ret, 0);
  }

  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL);
  info->dataFormat = false;
  info->protocol = TSDB_SML_TELNET_PROTOCOL;
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "twudyr 1626006833641 \"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal\" id=twudyr_17102_17825 t0=t t1=127i8 t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7=\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal\" t8=L\"abcd`~!@#$%^&*()_-{[}]|:;<.>?lfjal\"",
  };
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    SSmlLineInfo elements = {0};
    int ret = smlParseTelnetString(info, (char*)sql[i], (char*)(sql[i] + strlen(sql[i])), &elements);
//    printf("i:%d\n", i);
    ASSERT_EQ(ret, 0);
  }

  smlDestroyInfo(info);
}

//TEST(testCase, smlParseTelnetLine_diff_json_type2_Test) {
//  SSmlHandle *info = smlBuildSmlInfo(NULL);
//  info->protocol = TSDB_SML_JSON_PROTOCOL;
//  ASSERT_NE(info, nullptr);
//
//  const char *sql[] = {
//      "[{\"metric\":\"sys.cpu.nice\",\"timestamp\": 1346846400,\"value\": 18,\"tags\": {\"host\": \"lga\"}},{\"metric\": \"sys.sdfa\",\"timestamp\": 1346846400,\"value\": \"18\",\"tags\": {\"host\": 8932}},]",
//  };
//  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
//    char *dataPointStart = (char *)sql[i];
//    int8_t offset[4] = {0};
//    while (1) {
//      SSmlLineInfo elements = {0};
//      if(offset[0] == 0){
//        smlJsonParseObjFirst(&dataPointStart, &elements, offset);
//      }else{
//        smlJsonParseObj(&dataPointStart, &elements, offset);
//      }
//      if(*dataPointStart == '\0') break;
//
//      SArray *tags = smlJsonParseTags(elements.tags, elements.tags + elements.tagsLen);
//      size_t num = taosArrayGetSize(tags);
//      ASSERT_EQ(num, 1);
//
//      taosArrayDestroy(tags);
//    }
//  }
//  smlDestroyInfo(info);
//}

TEST(testCase, smlParseNumber_performance_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  SSmlKv kv;

  char* str[3] = {"2893f64", "2323u32", "93u8"};
  for (int i = 0; i < 3; ++i) {
    int64_t t1 = taosGetTimestampUs();
    for (int j = 0; j < 10000000; ++j) {
      kv.value = str[i];
      kv.length = strlen(str[i]);
      smlParseNumber(&kv, &msgBuf);
    }
    printf("smlParseNumber:%s cost:%" PRId64, str[i], taosGetTimestampUs() - t1);
    printf("\n");
    int64_t t2 = taosGetTimestampUs();
    for (int j = 0; j < 10000000; ++j) {
      kv.value = str[i];
      kv.length = strlen(str[i]);
      smlParseNumberOld(&kv, &msgBuf);
    }
    printf("smlParseNumberOld:%s cost:%" PRId64, str[i], taosGetTimestampUs() - t2);
    printf("\n\n");
  }
}