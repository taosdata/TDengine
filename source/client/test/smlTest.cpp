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

  // case 1
  char *tmp = "\\,st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000    ,32,c=3";
  char *sql = (char *)taosMemoryCalloc(256, 1);
  memcpy(sql, tmp, strlen(tmp) + 1);
  int ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql);
  ASSERT_EQ(elements.measureLen, strlen(",st"));
  ASSERT_EQ(elements.measureTagsLen, strlen(",st,t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.tags, sql + elements.measureLen + 1);
  ASSERT_EQ(elements.tagsLen, strlen("t1=3,t2=4,t3=t3"));

  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + elements.measureTagsLen + 1 + elements.colsLen + 1);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));

  // case 2  false
  tmp = "st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_NE(ret, 0);

  // case 3  false
  tmp = "st, t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("t1=3,t2=4,t3=t3"));

  // case 4  tag is null
  tmp = "st, c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
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

  // case 5 tag is null
  tmp = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64  1626006833639000000 ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.measure, sql + 1);
  ASSERT_EQ(elements.measureLen, strlen("st"));
  ASSERT_EQ(elements.measureTagsLen, strlen("st"));

  ASSERT_EQ(elements.tagsLen, 0);

  ASSERT_EQ(elements.cols, sql + 1 + elements.measureTagsLen + 3);
  ASSERT_EQ(elements.colsLen, strlen("c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64"));

  ASSERT_EQ(elements.timestamp, sql + 1 + elements.measureTagsLen + 3 + elements.colsLen + 2);
  ASSERT_EQ(elements.timestampLen, strlen("1626006833639000000"));

  // case 6
  tmp = " st   c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_EQ(ret, 0);

  // case 7
  tmp = " st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_EQ(ret, 0);

  // case 8 false
  tmp = ", st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, sql + strlen(sql), &elements, &msgBuf);
  ASSERT_NE(ret, 0);
  taosMemoryFree(sql);
}

TEST(testCase, smlParseCols_Error_Test) {
  const char *data[] = {"c=\"89sd",  // binary, nchar
                        "c=j\"89sd\"",
                        "c=\"89sd\"k",
                        "c=u",  // bool
                        "c=truet",
                        "c=f64",  // double
                        "c=8f64f",
                        "c=8ef64",
                        "c=f32",  // float
                        "c=8f32f",
                        "c=8wef32",
                        "c=-3.402823466e+39f32",
                        "c=",  // double
                        "c=8f",
                        "c=8we",
                        "c=i8",  // tiny int
                        "c=-8i8f",
                        "c=8wei8",
                        "c=-999i8",
                        "c=u8",  // u tiny int
                        "c=8fu8",
                        "c=8weu8",
                        "c=999u8",
                        "c=-8u8",
                        "c=i16",  // small int
                        "c=8fi16u",
                        "c=8wei16",
                        "c=-67787i16",
                        "c=u16",  // u small int
                        "c=8u16f",
                        "c=8weu16",
                        "c=-9u16",
                        "c=67787u16",
                        "c=i32",  // int
                        "c=8i32f",
                        "c=8wei32",
                        "c=2147483649i32",
                        "c=u32",  // u int
                        "c=8u32f",
                        "c=8weu32",
                        "c=-4u32",
                        "c=42949672958u32",
                        "c=i64",  // big int
                        "c=8i64i",
                        "c=8wei64",
                        "c=-9223372036854775809i64",
                        "c=i",  // big int
                        "c=8fi",
                        "c=8wei",
                        "c=9223372036854775808i",
                        "c=u64",  // u big int
                        "c=8u64f",
                        "c=8weu64",
                        "c=-3.402823466e+39u64",
                        "c=-339u64",
                        "c=18446744073709551616u64",
                        "c=1,c=2",
                        "c=1=2"};

  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  for (int i = 0; i < sizeof(data) / sizeof(data[0]); i++) {
    char       msg[256] = {0};
    SSmlMsgBuf msgBuf;
    msgBuf.buf = msg;
    msgBuf.len = 256;
    int32_t len = strlen(data[i]);
    char   *sql = (char *)taosMemoryCalloc(256, 1);
    memcpy(sql, data[i], len + 1);
    SArray *cols = taosArrayInit(8, POINTER_BYTES);
    int32_t ret = smlParseCols(sql, len, cols, NULL, false, dumplicateKey, &msgBuf);
    printf("i:%d\n", i);
    ASSERT_NE(ret, TSDB_CODE_SUCCESS);
    taosHashClear(dumplicateKey);
    taosMemoryFree(sql);
    for (int j = 0; j < taosArrayGetSize(cols); j++) {
      void *kv = taosArrayGetP(cols, j);
      taosMemoryFree(kv);
    }
    taosArrayDestroy(cols);
  }
  taosHashCleanup(dumplicateKey);
}

TEST(testCase, smlParseCols_tag_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  ASSERT_NE(cols, nullptr);
  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  const char *data =
      "cbin=\"passit "
      "helloc\",cnch=L\"iisdfsf\",cbool=false,cf64=4.31f64,cf64_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16=233i16,cu16="
      "898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,cboolf=f,cnch_"
      "=l\"iuwq\"";
  int32_t len = strlen(data);
  int32_t ret = smlParseCols(data, len, cols, NULL, true, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  int32_t size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 19);

  // nchar
  SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, 0);
  ASSERT_EQ(strncasecmp(kv->key, "cbin", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 15);
  ASSERT_EQ(strncasecmp(kv->value, "\"passit", 7), 0);

  // nchar
  kv = (SSmlKv *)taosArrayGetP(cols, 3);
  ASSERT_EQ(strncasecmp(kv->key, "cf64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 7);
  ASSERT_EQ(strncasecmp(kv->value, "4.31f64", 7), 0);

  for (int i = 0; i < size; i++) {
    void *tmp = taosArrayGetP(cols, i);
    taosMemoryFree(tmp);
  }
  taosArrayClear(cols);

  // test tag is null
  data = "t=3e";
  len = 0;
  memset(msgBuf.buf, 0, msgBuf.len);
  taosHashClear(dumplicateKey);
  ret = smlParseCols(data, len, cols, NULL, true, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 0);

  taosArrayDestroy(cols);
  taosHashCleanup(dumplicateKey);
}

TEST(testCase, smlParseCols_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  ASSERT_NE(cols, nullptr);

  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  const char *data =
      "cb\\=in=\"pass\\,it "
      "hello,c=2\",cnch=L\"ii\\=sdfsf\",cbool=false,cf64=4.31f64,cf64_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16="
      "233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,"
      "cboolf=f,cnch_=l\"iuwq\"";
  int32_t len = strlen(data);
  char   *sql = (char *)taosMemoryCalloc(1024, 1);
  memcpy(sql, data, len + 1);
  int32_t ret = smlParseCols(sql, len, cols, NULL, false, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  int32_t size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 19);

  // binary
  SSmlKv *kv = (SSmlKv *)taosArrayGetP(cols, 0);
  ASSERT_EQ(strncasecmp(kv->key, "cb=in", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BINARY);
  ASSERT_EQ(kv->length, 17);
  ASSERT_EQ(strncasecmp(kv->value, "pass,it ", 8), 0);
  taosMemoryFree(kv);

  // nchar
  kv = (SSmlKv *)taosArrayGetP(cols, 1);
  ASSERT_EQ(strncasecmp(kv->key, "cnch", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(strncasecmp(kv->value, "ii=sd", 5), 0);
  taosMemoryFree(kv);

  // bool
  kv = (SSmlKv *)taosArrayGetP(cols, 2);
  ASSERT_EQ(strncasecmp(kv->key, "cbool", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);
  taosMemoryFree(kv);

  // double
  kv = (SSmlKv *)taosArrayGetP(cols, 3);
  ASSERT_EQ(strncasecmp(kv->key, "cf64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  // ASSERT_EQ(kv->d, 4.31);
  printf("4.31 = kv->d:%f\n", kv->d);
  taosMemoryFree(kv);

  // float
  kv = (SSmlKv *)taosArrayGetP(cols, 4);
  ASSERT_EQ(strncasecmp(kv->key, "cf64_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  // ASSERT_EQ(kv->f, 8.32);
  printf("8.32 = kv->d:%f\n", kv->d);
  taosMemoryFree(kv);

  // float
  kv = (SSmlKv *)taosArrayGetP(cols, 5);
  ASSERT_EQ(strncasecmp(kv->key, "cf32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_FLOAT);
  ASSERT_EQ(kv->length, 4);
  // ASSERT_EQ(kv->f, 8.23);
  printf("8.23 = kv->f:%f\n", kv->f);
  taosMemoryFree(kv);

  // tiny int
  kv = (SSmlKv *)taosArrayGetP(cols, 6);
  ASSERT_EQ(strncasecmp(kv->key, "ci8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_TINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, -34);
  taosMemoryFree(kv);

  // unsigned tiny int
  kv = (SSmlKv *)taosArrayGetP(cols, 7);
  ASSERT_EQ(strncasecmp(kv->key, "cu8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UTINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->u, 89);
  taosMemoryFree(kv);

  // small int
  kv = (SSmlKv *)taosArrayGetP(cols, 8);
  ASSERT_EQ(strncasecmp(kv->key, "ci16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_SMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 233);
  taosMemoryFree(kv);

  // unsigned smallint
  kv = (SSmlKv *)taosArrayGetP(cols, 9);
  ASSERT_EQ(strncasecmp(kv->key, "cu16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_USMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 898);
  taosMemoryFree(kv);

  // int
  kv = (SSmlKv *)taosArrayGetP(cols, 10);
  ASSERT_EQ(strncasecmp(kv->key, "ci32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 98289);
  taosMemoryFree(kv);

  // unsigned int
  kv = (SSmlKv *)taosArrayGetP(cols, 11);
  ASSERT_EQ(strncasecmp(kv->key, "cu32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UINT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 12323);
  taosMemoryFree(kv);

  // bigint
  kv = (SSmlKv *)taosArrayGetP(cols, 12);
  ASSERT_EQ(strncasecmp(kv->key, "ci64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, -89238);
  taosMemoryFree(kv);

  // bigint
  kv = (SSmlKv *)taosArrayGetP(cols, 13);
  ASSERT_EQ(strncasecmp(kv->key, "ci", 2), 0);
  ASSERT_EQ(kv->keyLen, 2);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, 989);
  taosMemoryFree(kv);

  // unsigned bigint
  kv = (SSmlKv *)taosArrayGetP(cols, 14);
  ASSERT_EQ(strncasecmp(kv->key, "cu64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UBIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->u, 8989323);
  taosMemoryFree(kv);

  // bool
  kv = (SSmlKv *)taosArrayGetP(cols, 15);
  ASSERT_EQ(strncasecmp(kv->key, "cbooltrue", 9), 0);
  ASSERT_EQ(kv->keyLen, 9);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);
  taosMemoryFree(kv);

  // bool
  kv = (SSmlKv *)taosArrayGetP(cols, 16);
  ASSERT_EQ(strncasecmp(kv->key, "cboolt", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);
  taosMemoryFree(kv);

  // bool
  kv = (SSmlKv *)taosArrayGetP(cols, 17);
  ASSERT_EQ(strncasecmp(kv->key, "cboolf", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);
  taosMemoryFree(kv);

  // nchar
  kv = (SSmlKv *)taosArrayGetP(cols, 18);
  ASSERT_EQ(strncasecmp(kv->key, "cnch_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(strncasecmp(kv->value, "iuwq", 4), 0);
  taosMemoryFree(kv);

  taosArrayDestroy(cols);
  taosHashCleanup(dumplicateKey);
  taosMemoryFree(sql);
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
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
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
      "sys.procs.running 1479496100 true host=web01",
      "sys.procs.running 1479496100 \"binary\" host=web01",
      "sys.procs.running 1479496100 L\"rfr\" host=web01",
      "sys.procs.running 1479496100 42 host=web01 cpu= ",
      "sys.procs.running 1479496100 42 host=web01 host=w2",
      "sys.procs.running 1479496100 42 host=web01 host",
      "sys.procs.running 1479496100 42 host=web01=er",
      "sys.procs.running 1479496100 42 host= web01",
  };
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    int ret = smlParseTelnetLine(info, (void *)sql[i], strlen(sql[i]));
    ASSERT_NE(ret, 0);
  }

  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_type_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {"sys.procs.running 1479496104000 42 host=web01",
                       "sys.procs.running 1479496104000 42u8 host=web01",
                       "appywjnuct 1626006833641 True id=\"appywjnuct_40601_49808_1\" t0=t t1=127i8 "
                       "id=\"appywjnuct_40601_49808_2\" t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 "
                       "t5=11.12345f32 t6=22.123456789f64 t7=\"binaryTagValue\" t8=L\"ncharTagValue\""};

  int ret = TSDB_CODE_SUCCESS;
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    ret = smlParseTelnetLine(info, (void *)sql[i], strlen(sql[i]));
    if (ret != TSDB_CODE_SUCCESS) break;
  }
  ASSERT_NE(ret, 0);
  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_json_error_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 13468464009999333322222223,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"host\": \"web01\",\n"
      "           \"dc\": \"lga\"\n"
      "        }\n"
      "    },\n"
      "]",
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400i,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"host\": \"web01\",\n"
      "           \"dc\": \"lga\"\n"
      "        }\n"
      "    },\n"
      "]",
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"groupid\": { \n"
      "                 \"value\" : 2,\n"
      "                 \"type\"  : \"nchar\"\n"
      "             },\n"
      "           \"location\": { \n"
      "                 \"value\" : \"北京\",\n"
      "                 \"type\"  : \"binary\"\n"
      "             },\n"
      "           \"id\": \"d1001\"\n"
      "         }\n"
      "    },\n"
      "]",
  };

  int ret = TSDB_CODE_SUCCESS;
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    ret = smlParseTelnetLine(info, (void *)sql[i], strlen(sql[i]));
    ASSERT_NE(ret, 0);
  }

  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_json_type1_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"host\": \"lga\"\n"
      "        }\n"
      "    },\n"
      "]",
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"host\": 8\n"
      "        }\n"
      "    },\n"
      "]",
  };

  int ret = TSDB_CODE_SUCCESS;
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    ret = smlParseTelnetLine(info, (void *)sql[i], strlen(sql[i]));
    if (ret != TSDB_CODE_SUCCESS) break;
  }
  ASSERT_NE(ret, 0);
  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_json_type2_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400,\n"
      "        \"value\": 18,\n"
      "        \"tags\": {\n"
      "           \"host\": \"lga\"\n"
      "        }\n"
      "    },\n"
      "]",
      "[\n"
      "    {\n"
      "        \"metric\": \"sys.cpu.nice\",\n"
      "        \"timestamp\": 1346846400,\n"
      "        \"value\": \"18\",\n"
      "        \"tags\": {\n"
      "           \"host\": \"fff\"\n"
      "        }\n"
      "    },\n"
      "]",
  };
  int ret = TSDB_CODE_SUCCESS;
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    ret = smlParseTelnetLine(info, (void *)sql[i], strlen(sql[i]));
    if (ret != TSDB_CODE_SUCCESS) break;
  }
  ASSERT_NE(ret, 0);
  smlDestroyInfo(info);
}

TEST(testCase, sml_col_4096_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "spgwgvldxv,id=spgwgvldxv_1,t0=f "
      "c0=t,c1=t,c2=t,c3=t,c4=t,c5=t,c6=t,c7=t,c8=t,c9=t,c10=t,c11=t,c12=t,c13=t,c14=t,c15=t,c16=t,c17=t,c18=t,c19=t,"
      "c20=t,c21=t,c22=t,c23=t,c24=t,c25=t,c26=t,c27=t,c28=t,c29=t,c30=t,c31=t,c32=t,c33=t,c34=t,c35=t,c36=t,c37=t,c38="
      "t,c39=t,c40=t,c41=t,c42=t,c43=t,c44=t,c45=t,c46=t,c47=t,c48=t,c49=t,c50=t,c51=t,c52=t,c53=t,c54=t,c55=t,c56=t,"
      "c57=t,c58=t,c59=t,c60=t,c61=t,c62=t,c63=t,c64=t,c65=t,c66=t,c67=t,c68=t,c69=t,c70=t,c71=t,c72=t,c73=t,c74=t,c75="
      "t,c76=t,c77=t,c78=t,c79=t,c80=t,c81=t,c82=t,c83=t,c84=t,c85=t,c86=t,c87=t,c88=t,c89=t,c90=t,c91=t,c92=t,c93=t,"
      "c94=t,c95=t,c96=t,c97=t,c98=t,c99=t,c100=t,"
      "c101=t,c102=t,c103=t,c104=t,c105=t,c106=t,c107=t,c108=t,c109=t,c110=t,c111=t,c112=t,c113=t,c114=t,c115=t,c116=t,"
      "c117=t,c118=t,c119=t,c120=t,c121=t,c122=t,c123=t,c124=t,c125=t,c126=t,c127=t,c128=t,c129=t,c130=t,c131=t,c132=t,"
      "c133=t,c134=t,c135=t,c136=t,c137=t,c138=t,c139=t,c140=t,c141=t,c142=t,c143=t,c144=t,c145=t,c146=t,c147=t,c148=t,"
      "c149=t,c150=t,c151=t,c152=t,c153=t,c154=t,c155=t,c156=t,c157=t,c158=t,c159=t,c160=t,c161=t,c162=t,c163=t,c164=t,"
      "c165=t,c166=t,c167=t,c168=t,c169=t,c170=t,c171=t,c172=t,c173=t,c174=t,c175=t,c176=t,c177=t,c178=t,c179=t,c180=t,"
      "c181=t,c182=t,c183=t,c184=t,c185=t,c186=t,c187=t,c188=t,c189=t,"
      "c190=t,c191=t,c192=t,c193=t,c194=t,c195=t,c196=t,c197=t,c198=t,c199=t,c200=t,c201=t,c202=t,c203=t,c204=t,c205=t,"
      "c206=t,c207=t,c208=t,c209=t,c210=t,c211=t,c212=t,c213=t,c214=t,c215=t,c216=t,c217=t,c218=t,c219=t,c220=t,c221=t,"
      "c222=t,c223=t,c224=t,c225=t,c226=t,c227=t,c228=t,c229=t,c230=t,c231=t,c232=t,c233=t,c234=t,c235=t,c236=t,c237=t,"
      "c238=t,c239=t,c240=t,c241=t,c242=t,c243=t,c244=t,c245=t,c246=t,c247=t,c248=t,c249=t,c250=t,c251=t,c252=t,c253=t,"
      "c254=t,c255=t,c256=t,c257=t,c258=t,c259=t,c260=t,c261=t,c262=t,c263=t,c264=t,c265=t,c266=t,c267=t,c268=t,c269=t,"
      "c270=t,c271=t,c272=t,c273=t,c274=t,c275=t,c276=t,c277=t,c278=t,"
      "c279=t,c280=t,c281=t,c282=t,c283=t,c284=t,c285=t,c286=t,c287=t,c288=t,c289=t,c290=t,c291=t,c292=t,c293=t,c294=t,"
      "c295=t,c296=t,c297=t,c298=t,c299=t,c300=t,c301=t,c302=t,c303=t,c304=t,c305=t,c306=t,c307=t,c308=t,c309=t,c310=t,"
      "c311=t,c312=t,c313=t,c314=t,c315=t,c316=t,c317=t,c318=t,c319=t,c320=t,c321=t,c322=t,c323=t,c324=t,c325=t,c326=t,"
      "c327=t,c328=t,c329=t,c330=t,c331=t,c332=t,c333=t,c334=t,c335=t,c336=t,c337=t,c338=t,c339=t,c340=t,c341=t,c342=t,"
      "c343=t,c344=t,c345=t,c346=t,c347=t,c348=t,c349=t,c350=t,c351=t,c352=t,c353=t,c354=t,c355=t,c356=t,c357=t,c358=t,"
      "c359=t,c360=t,c361=t,c362=t,c363=t,c364=t,c365=t,c366=t,c367=t,c368=t,c369=t,c370=t,c371=t,c372=t,c373=t,c374=t,"
      "c375=t,c376=t,c377=t,c378=t,c379=t,c380=t,c381=t,c382=t,c383=t,c384=t,c385=t,c386=t,c387=t,c388=t,c389=t,c390=t,"
      "c391=t,c392=t,c393=t,c394=t,c395=t,c396=t,c397=t,c398=t,c399=t,c400=t,c401=t,c402=t,c403=t,c404=t,c405=t,c406=t,"
      "c407=t,c408=t,c409=t,c410=t,c411=t,c412=t,c413=t,c414=t,c415=t,c416=t,c417=t,c418=t,c419=t,c420=t,c421=t,c422=t,"
      "c423=t,c424=t,c425=t,c426=t,c427=t,c428=t,c429=t,c430=t,c431=t,c432=t,c433=t,c434=t,c435=t,c436=t,c437=t,c438=t,"
      "c439=t,c440=t,c441=t,c442=t,c443=t,c444=t,c445=t,c446=t,"
      "c447=t,c448=t,c449=t,c450=t,c451=t,c452=t,c453=t,c454=t,c455=t,c456=t,c457=t,c458=t,c459=t,c460=t,c461=t,c462=t,"
      "c463=t,c464=t,c465=t,c466=t,c467=t,c468=t,c469=t,c470=t,c471=t,c472=t,c473=t,c474=t,c475=t,c476=t,c477=t,c478=t,"
      "c479=t,c480=t,c481=t,c482=t,c483=t,c484=t,c485=t,c486=t,c487=t,c488=t,c489=t,c490=t,c491=t,c492=t,c493=t,c494=t,"
      "c495=t,c496=t,c497=t,c498=t,c499=t,c500=t,c501=t,c502=t,c503=t,c504=t,c505=t,c506=t,c507=t,c508=t,c509=t,c510=t,"
      "c511=t,c512=t,c513=t,c514=t,c515=t,c516=t,c517=t,c518=t,c519=t,c520=t,c521=t,c522=t,c523=t,c524=t,c525=t,c526=t,"
      "c527=t,c528=t,c529=t,c530=t,c531=t,c532=t,c533=t,c534=t,c535=t,c536=t,c537=t,c538=t,c539=t,c540=t,c541=t,c542=t,"
      "c543=t,c544=t,c545=t,c546=t,c547=t,c548=t,c549=t,c550=t,c551=t,c552=t,c553=t,c554=t,c555=t,c556=t,c557=t,c558=t,"
      "c559=t,c560=t,c561=t,c562=t,c563=t,c564=t,c565=t,c566=t,c567=t,c568=t,c569=t,c570=t,c571=t,c572=t,c573=t,c574=t,"
      "c575=t,c576=t,c577=t,c578=t,c579=t,c580=t,c581=t,c582=t,c583=t,c584=t,c585=t,c586=t,c587=t,c588=t,c589=t,c590=t,"
      "c591=t,c592=t,c593=t,c594=t,c595=t,c596=t,c597=t,c598=t,c599=t,c600=t,c601=t,c602=t,c603=t,c604=t,c605=t,c606=t,"
      "c607=t,c608=t,c609=t,c610=t,c611=t,c612=t,c613=t,c614=t,"
      "c615=t,c616=t,c617=t,c618=t,c619=t,c620=t,c621=t,c622=t,c623=t,c624=t,c625=t,c626=t,c627=t,c628=t,c629=t,c630=t,"
      "c631=t,c632=t,c633=t,c634=t,c635=t,c636=t,c637=t,c638=t,c639=t,c640=t,c641=t,c642=t,c643=t,c644=t,c645=t,c646=t,"
      "c647=t,c648=t,c649=t,c650=t,c651=t,c652=t,c653=t,c654=t,c655=t,c656=t,c657=t,c658=t,c659=t,c660=t,c661=t,c662=t,"
      "c663=t,c664=t,c665=t,c666=t,c667=t,c668=t,c669=t,c670=t,c671=t,c672=t,c673=t,c674=t,c675=t,c676=t,c677=t,c678=t,"
      "c679=t,c680=t,c681=t,c682=t,c683=t,c684=t,c685=t,c686=t,c687=t,c688=t,c689=t,c690=t,c691=t,c692=t,c693=t,c694=t,"
      "c695=t,c696=t,c697=t,c698=t,c699=t,c700=t,c701=t,c702=t,c703=t,c704=t,c705=t,c706=t,c707=t,c708=t,c709=t,c710=t,"
      "c711=t,c712=t,c713=t,c714=t,c715=t,c716=t,c717=t,c718=t,c719=t,c720=t,c721=t,c722=t,c723=t,c724=t,c725=t,c726=t,"
      "c727=t,c728=t,c729=t,c730=t,c731=t,c732=t,c733=t,c734=t,c735=t,c736=t,c737=t,c738=t,c739=t,c740=t,c741=t,c742=t,"
      "c743=t,c744=t,c745=t,c746=t,c747=t,c748=t,c749=t,c750=t,c751=t,c752=t,c753=t,c754=t,c755=t,c756=t,c757=t,c758=t,"
      "c759=t,c760=t,c761=t,c762=t,c763=t,c764=t,c765=t,c766=t,c767=t,c768=t,c769=t,c770=t,c771=t,c772=t,c773=t,c774=t,"
      "c775=t,c776=t,c777=t,c778=t,c779=t,c780=t,c781=t,c782=t,"
      "c783=t,c784=t,c785=t,c786=t,c787=t,c788=t,c789=t,c790=t,c791=t,c792=t,c793=t,c794=t,c795=t,c796=t,c797=t,c798=t,"
      "c799=t,c800=t,c801=t,c802=t,c803=t,c804=t,c805=t,c806=t,c807=t,c808=t,c809=t,c810=t,c811=t,c812=t,c813=t,"
      "c814=t,c815=t,c816=t,c817=t,c818=t,c819=t,c820=t,c821=t,c822=t,c823=t,c824=t,c825=t,c826=t,c827=t,c828=t,c829=t,"
      "c830=t,c831=t,c832=t,c833=t,c834=t,c835=t,c836=t,c837=t,c838=t,c839=t,c840=t,c841=t,c842=t,c843=t,c844=t,c845=t,"
      "c846=t,c847=t,c848=t,c849=t,c850=t,c851=t,c852=t,c853=t,c854=t,c855=t,c856=t,c857=t,c858=t,c859=t,c860=t,c861=t,"
      "c862=t,"
      "c863=t,c864=t,c865=t,c866=t,c867=t,c868=t,c869=t,c870=t,c871=t,c872=t,c873=t,c874=t,c875=t,c876=t,c877=t,c878=t,"
      "c879=t,c880=t,c881=t,c882=t,c883=t,c884=t,c885=t,c886=t,c887=t,c888=t,c889=t,c890=t,c891=t,c892=t,c893=t,c894=t,"
      "c895=t,c896=t,c897=t,c898=t,c899=t,c900=t,c901=t,c902=t,c903=t,c904=t,c905=t,c906=t,c907=t,c908=t,c909=t,c910=t,"
      "c911=t,c912=t,c913=t,c914=t,c915=t,c916=t,c917=t,c918=t,c919=t,c920=t,c921=t,c922=t,c923=t,c924=t,c925=t,c926=t,"
      "c927=t,c928=t,c929=t,c930=t,c931=t,c932=t,c933=t,c934=t,c935=t,c936=t,c937=t,c938=t,c939=t,c940=t,c941=t,c942=t,"
      "c943=t,c944=t,c945=t,c946=t,c947=t,c948=t,c949=t,c950=t,c951=t,c952=t,c953=t,c954=t,c955=t,c956=t,c957=t,c958=t,"
      "c959=t,c960=t,c961=t,c962=t,c963=t,c964=t,c965=t,c966=t,c967=t,c968=t,c969=t,c970=t,c971=t,c972=t,c973=t,c974=t,"
      "c975=t,c976=t,c977=t,c978=t,c979=t,c980=t,c981=t,c982=t,c983=t,c984=t,c985=t,c986=t,c987=t,c988=t,c989=t,c990=t,"
      "c991=t,c992=t,c993=t,c994=t,c995=t,c996=t,c997=t,c998=t,c999=t,c1000=t,c1001=t,c1002=t,c1003=t,c1004=t,c1005=t,"
      "c1006=t,c1007=t,c1008=t,c1009=t,c1010=t,c1011=t,c1012=t,c1013=t,c1014=t,c1015=t,c1016=t,c1017=t,c1018=t,c1019=t,"
      "c1020=t,c1021=t,c1022=t,c1023=t,c1024=t,c1025=t,c1026=t,"
      "c1027=t,c1028=t,c1029=t,c1030=t,c1031=t,c1032=t,c1033=t,c1034=t,c1035=t,c1036=t,c1037=t,c1038=t,c1039=t,c1040=t,"
      "c1041=t,c1042=t,c1043=t,c1044=t,c1045=t,c1046=t,c1047=t,c1048=t,c1049=t,c1050=t,c1051=t,c1052=t,c1053=t,c1054=t,"
      "c1055=t,c1056=t,c1057=t,c1058=t,c1059=t,c1060=t,c1061=t,c1062=t,c1063=t,c1064=t,c1065=t,c1066=t,c1067=t,c1068=t,"
      "c1069=t,c1070=t,c1071=t,c1072=t,c1073=t,c1074=t,c1075=t,c1076=t,c1077=t,c1078=t,c1079=t,c1080=t,c1081=t,c1082=t,"
      "c1083=t,c1084=t,c1085=t,c1086=t,c1087=t,c1088=t,c1089=t,c1090=t,c1091=t,c1092=t,c1093=t,c1094=t,c1095=t,c1096=t,"
      "c1097=t,c1098=t,c1099=t,c1100=t,c1101=t,c1102=t,c1103=t,c1104=t,c1105=t,c1106=t,c1107=t,c1108=t,c1109=t,c1110=t,"
      "c1111=t,c1112=t,c1113=t,c1114=t,c1115=t,c1116=t,c1117=t,c1118=t,c1119=t,c1120=t,c1121=t,c1122=t,c1123=t,c1124=t,"
      "c1125=t,c1126=t,c1127=t,c1128=t,c1129=t,c1130=t,c1131=t,c1132=t,c1133=t,c1134=t,c1135=t,c1136=t,c1137=t,c1138=t,"
      "c1139=t,c1140=t,c1141=t,c1142=t,c1143=t,c1144=t,c1145=t,c1146=t,c1147=t,c1148=t,c1149=t,c1150=t,c1151=t,c1152=t,"
      "c1153=t,c1154=t,c1155=t,c1156=t,c1157=t,c1158=t,c1159=t,c1160=t,c1161=t,c1162=t,c1163=t,c1164=t,c1165=t,c1166=t,"
      "c1167=t,c1168=t,c1169=t,c1170=t,c1171=t,c1172=t,c1173=t,"
      "c1174=t,c1175=t,c1176=t,c1177=t,c1178=t,c1179=t,c1180=t,c1181=t,c1182=t,c1183=t,c1184=t,c1185=t,c1186=t,c1187=t,"
      "c1188=t,c1189=t,c1190=t,c1191=t,c1192=t,c1193=t,c1194=t,c1195=t,c1196=t,c1197=t,c1198=t,c1199=t,c1200=t,c1201=t,"
      "c1202=t,c1203=t,c1204=t,c1205=t,c1206=t,c1207=t,c1208=t,c1209=t,c1210=t,c1211=t,c1212=t,c1213=t,c1214=t,c1215=t,"
      "c1216=t,c1217=t,c1218=t,c1219=t,c1220=t,c1221=t,c1222=t,c1223=t,c1224=t,c1225=t,c1226=t,c1227=t,c1228=t,c1229=t,"
      "c1230=t,c1231=t,c1232=t,c1233=t,c1234=t,c1235=t,c1236=t,c1237=t,c1238=t,c1239=t,c1240=t,c1241=t,c1242=t,c1243=t,"
      "c1244=t,c1245=t,c1246=t,c1247=t,c1248=t,c1249=t,c1250=t,c1251=t,c1252=t,c1253=t,c1254=t,c1255=t,c1256=t,c1257=t,"
      "c1258=t,c1259=t,c1260=t,c1261=t,c1262=t,c1263=t,c1264=t,c1265=t,c1266=t,c1267=t,c1268=t,c1269=t,c1270=t,c1271=t,"
      "c1272=t,c1273=t,c1274=t,c1275=t,c1276=t,c1277=t,c1278=t,c1279=t,c1280=t,c1281=t,c1282=t,c1283=t,c1284=t,c1285=t,"
      "c1286=t,c1287=t,c1288=t,c1289=t,c1290=t,c1291=t,c1292=t,c1293=t,c1294=t,c1295=t,c1296=t,c1297=t,c1298=t,c1299=t,"
      "c1300=t,c1301=t,c1302=t,c1303=t,c1304=t,c1305=t,c1306=t,c1307=t,c1308=t,c1309=t,c1310=t,c1311=t,c1312=t,c1313=t,"
      "c1314=t,c1315=t,c1316=t,c1317=t,c1318=t,c1319=t,c1320=t,"
      "c1321=t,c1322=t,c1323=t,c1324=t,c1325=t,c1326=t,c1327=t,c1328=t,c1329=t,c1330=t,c1331=t,c1332=t,c1333=t,c1334=t,"
      "c1335=t,c1336=t,c1337=t,c1338=t,c1339=t,c1340=t,c1341=t,c1342=t,c1343=t,c1344=t,c1345=t,c1346=t,c1347=t,"
      "c1348=t,c1349=t,c1350=t,c1351=t,c1352=t,c1353=t,c1354=t,c1355=t,c1356=t,c1357=t,c1358=t,c1359=t,c1360=t,c1361=t,"
      "c1362=t,c1363=t,c1364=t,c1365=t,c1366=t,c1367=t,c1368=t,c1369=t,c1370=t,c1371=t,c1372=t,c1373=t,c1374=t,c1375=t,"
      "c1376=t,c1377=t,c1378=t,c1379=t,c1380=t,c1381=t,c1382=t,c1383=t,c1384=t,c1385=t,c1386=t,c1387=t,c1388=t,c1389=t,"
      "c1390=t,c1391=t,c1392=t,c1393=t,c1394=t,c1395=t,c1396=t,c1397=t,c1398=t,c1399=t,c1400=t,c1401=t,c1402=t,c1403=t,"
      "c1404=t,c1405=t,c1406=t,c1407=t,c1408=t,c1409=t,c1410=t,c1411=t,c1412=t,c1413=t,c1414=t,c1415=t,c1416=t,c1417=t,"
      "c1418=t,c1419=t,c1420=t,c1421=t,c1422=t,c1423=t,c1424=t,c1425=t,c1426=t,c1427=t,c1428=t,c1429=t,c1430=t,c1431=t,"
      "c1432=t,c1433=t,c1434=t,c1435=t,c1436=t,c1437=t,c1438=t,c1439=t,c1440=t,c1441=t,c1442=t,c1443=t,c1444=t,c1445=t,"
      "c1446=t,c1447=t,c1448=t,c1449=t,c1450=t,c1451=t,c1452=t,c1453=t,c1454=t,c1455=t,c1456=t,c1457=t,c1458=t,c1459=t,"
      "c1460=t,c1461=t,c1462=t,c1463=t,c1464=t,c1465=t,c1466=t,c1467=t,c1468=t,c1469=t,c1470=t,c1471=t,c1472=t,c1473=t,"
      "c1474=t,c1475=t,c1476=t,c1477=t,c1478=t,c1479=t,c1480=t,c1481=t,c1482=t,c1483=t,c1484=t,c1485=t,c1486=t,c1487=t,"
      "c1488=t,c1489=t,c1490=t,c1491=t,c1492=t,c1493=t,c1494=t,"
      "c1495=t,c1496=t,c1497=t,c1498=t,c1499=t,c1500=t,c1501=t,c1502=t,c1503=t,c1504=t,c1505=t,c1506=t,c1507=t,c1508=t,"
      "c1509=t,c1510=t,c1511=t,c1512=t,c1513=t,c1514=t,c1515=t,c1516=t,c1517=t,c1518=t,c1519=t,c1520=t,c1521=t,c1522=t,"
      "c1523=t,c1524=t,c1525=t,c1526=t,c1527=t,c1528=t,c1529=t,c1530=t,c1531=t,c1532=t,c1533=t,c1534=t,c1535=t,c1536=t,"
      "c1537=t,c1538=t,c1539=t,c1540=t,c1541=t,c1542=t,c1543=t,c1544=t,c1545=t,c1546=t,c1547=t,c1548=t,c1549=t,c1550=t,"
      "c1551=t,c1552=t,c1553=t,c1554=t,c1555=t,c1556=t,c1557=t,c1558=t,c1559=t,c1560=t,c1561=t,c1562=t,c1563=t,c1564=t,"
      "c1565=t,c1566=t,c1567=t,c1568=t,c1569=t,c1570=t,c1571=t,c1572=t,c1573=t,c1574=t,c1575=t,c1576=t,c1577=t,c1578=t,"
      "c1579=t,c1580=t,c1581=t,c1582=t,c1583=t,c1584=t,c1585=t,c1586=t,c1587=t,c1588=t,c1589=t,c1590=t,c1591=t,c1592=t,"
      "c1593=t,c1594=t,c1595=t,c1596=t,c1597=t,c1598=t,c1599=t,c1600=t,c1601=t,c1602=t,c1603=t,c1604=t,c1605=t,c1606=t,"
      "c1607=t,c1608=t,c1609=t,c1610=t,c1611=t,c1612=t,c1613=t,c1614=t,c1615=t,c1616=t,c1617=t,c1618=t,c1619=t,c1620=t,"
      "c1621=t,c1622=t,c1623=t,c1624=t,c1625=t,c1626=t,c1627=t,c1628=t,c1629=t,c1630=t,c1631=t,c1632=t,c1633=t,c1634=t,"
      "c1635=t,c1636=t,c1637=t,c1638=t,c1639=t,c1640=t,c1641=t,"
      "c1642=t,c1643=t,c1644=t,c1645=t,c1646=t,c1647=t,c1648=t,c1649=t,c1650=t,c1651=t,c1652=t,c1653=t,c1654=t,c1655=t,"
      "c1656=t,c1657=t,c1658=t,c1659=t,c1660=t,c1661=t,c1662=t,c1663=t,c1664=t,c1665=t,c1666=t,c1667=t,c1668=t,c1669=t,"
      "c1670=t,c1671=t,c1672=t,c1673=t,c1674=t,c1675=t,c1676=t,c1677=t,c1678=t,c1679=t,c1680=t,c1681=t,c1682=t,c1683=t,"
      "c1684=t,c1685=t,c1686=t,c1687=t,c1688=t,c1689=t,c1690=t,c1691=t,c1692=t,c1693=t,c1694=t,c1695=t,c1696=t,c1697=t,"
      "c1698=t,c1699=t,c1700=t,c1701=t,c1702=t,c1703=t,c1704=t,c1705=t,c1706=t,c1707=t,c1708=t,c1709=t,c1710=t,c1711=t,"
      "c1712=t,c1713=t,c1714=t,c1715=t,c1716=t,c1717=t,c1718=t,c1719=t,c1720=t,c1721=t,c1722=t,c1723=t,c1724=t,c1725=t,"
      "c1726=t,c1727=t,c1728=t,c1729=t,c1730=t,c1731=t,c1732=t,c1733=t,c1734=t,c1735=t,c1736=t,c1737=t,c1738=t,c1739=t,"
      "c1740=t,c1741=t,c1742=t,c1743=t,c1744=t,c1745=t,c1746=t,c1747=t,c1748=t,c1749=t,c1750=t,c1751=t,c1752=t,c1753=t,"
      "c1754=t,c1755=t,c1756=t,c1757=t,c1758=t,c1759=t,c1760=t,c1761=t,c1762=t,c1763=t,c1764=t,c1765=t,c1766=t,c1767=t,"
      "c1768=t,c1769=t,c1770=t,c1771=t,c1772=t,c1773=t,c1774=t,c1775=t,c1776=t,c1777=t,c1778=t,c1779=t,c1780=t,c1781=t,"
      "c1782=t,c1783=t,c1784=t,c1785=t,c1786=t,c1787=t,c1788=t,"
      "c1789=t,c1790=t,c1791=t,c1792=t,c1793=t,c1794=t,c1795=t,c1796=t,c1797=t,c1798=t,c1799=t,c1800=t,c1801=t,c1802=t,"
      "c1803=t,c1804=t,c1805=t,c1806=t,c1807=t,c1808=t,c1809=t,c1810=t,c1811=t,c1812=t,c1813=t,c1814=t,c1815=t,"
      "c1816=t,c1817=t,c1818=t,c1819=t,c1820=t,c1821=t,c1822=t,c1823=t,c1824=t,c1825=t,c1826=t,c1827=t,c1828=t,c1829=t,"
      "c1830=t,c1831=t,c1832=t,c1833=t,c1834=t,c1835=t,c1836=t,c1837=t,c1838=t,c1839=t,c1840=t,c1841=t,c1842=t,c1843=t,"
      "c1844=t,c1845=t,c1846=t,c1847=t,c1848=t,c1849=t,c1850=t,c1851=t,c1852=t,c1853=t,c1854=t,c1855=t,c1856=t,c1857=t,"
      "c1858=t,c1859=t,c1860=t,c1861=t,c1862=t,c1863=t,c1864=t,c1865=t,c1866=t,c1867=t,c1868=t,c1869=t,c1870=t,c1871=t,"
      "c1872=t,c1873=t,c1874=t,c1875=t,c1876=t,c1877=t,c1878=t,c1879=t,c1880=t,c1881=t,c1882=t,c1883=t,c1884=t,c1885=t,"
      "c1886=t,c1887=t,c1888=t,c1889=t,c1890=t,c1891=t,c1892=t,c1893=t,c1894=t,c1895=t,c1896=t,c1897=t,c1898=t,c1899=t,"
      "c1900=t,c1901=t,c1902=t,c1903=t,c1904=t,c1905=t,c1906=t,c1907=t,c1908=t,c1909=t,c1910=t,c1911=t,c1912=t,c1913=t,"
      "c1914=t,c1915=t,c1916=t,c1917=t,c1918=t,c1919=t,c1920=t,c1921=t,c1922=t,c1923=t,c1924=t,c1925=t,c1926=t,c1927=t,"
      "c1928=t,c1929=t,c1930=t,c1931=t,c1932=t,c1933=t,c1934=t,c1935=t,c1936=t,c1937=t,c1938=t,c1939=t,c1940=t,c1941=t,"
      "c1942=t,c1943=t,c1944=t,c1945=t,c1946=t,c1947=t,c1948=t,c1949=t,c1950=t,c1951=t,c1952=t,c1953=t,c1954=t,c1955=t,"
      "c1956=t,c1957=t,c1958=t,c1959=t,c1960=t,c1961=t,c1962=t,"
      "c1963=t,c1964=t,c1965=t,c1966=t,c1967=t,c1968=t,c1969=t,c1970=t,c1971=t,c1972=t,c1973=t,c1974=t,c1975=t,c1976=t,"
      "c1977=t,c1978=t,c1979=t,c1980=t,c1981=t,c1982=t,c1983=t,c1984=t,c1985=t,c1986=t,c1987=t,c1988=t,c1989=t,c1990=t,"
      "c1991=t,c1992=t,c1993=t,c1994=t,c1995=t,c1996=t,c1997=t,c1998=t,c1999=t,c2000=t,c2001=t,c2002=t,c2003=t,c2004=t,"
      "c2005=t,c2006=t,c2007=t,c2008=t,c2009=t,c2010=t,c2011=t,c2012=t,c2013=t,c2014=t,c2015=t,c2016=t,c2017=t,c2018=t,"
      "c2019=t,c2020=t,c2021=t,c2022=t,c2023=t,c2024=t,c2025=t,c2026=t,c2027=t,c2028=t,c2029=t,c2030=t,c2031=t,c2032=t,"
      "c2033=t,c2034=t,c2035=t,c2036=t,c2037=t,c2038=t,c2039=t,c2040=t,c2041=t,c2042=t,c2043=t,c2044=t,c2045=t,c2046=t,"
      "c2047=t,c2048=t,c2049=t,c2050=t,c2051=t,c2052=t,c2053=t,c2054=t,c2055=t,c2056=t,c2057=t,c2058=t,c2059=t,c2060=t,"
      "c2061=t,c2062=t,c2063=t,c2064=t,c2065=t,c2066=t,c2067=t,c2068=t,c2069=t,c2070=t,c2071=t,c2072=t,c2073=t,c2074=t,"
      "c2075=t,c2076=t,c2077=t,c2078=t,c2079=t,c2080=t,c2081=t,c2082=t,c2083=t,c2084=t,c2085=t,c2086=t,c2087=t,c2088=t,"
      "c2089=t,c2090=t,c2091=t,c2092=t,c2093=t,c2094=t,c2095=t,c2096=t,c2097=t,c2098=t,c2099=t,c2100=t,c2101=t,c2102=t,"
      "c2103=t,c2104=t,c2105=t,c2106=t,c2107=t,c2108=t,c2109=t,"
      "c2110=t,c2111=t,c2112=t,c2113=t,c2114=t,c2115=t,c2116=t,c2117=t,c2118=t,c2119=t,c2120=t,c2121=t,c2122=t,c2123=t,"
      "c2124=t,c2125=t,c2126=t,c2127=t,c2128=t,c2129=t,c2130=t,c2131=t,c2132=t,c2133=t,c2134=t,c2135=t,c2136=t,c2137=t,"
      "c2138=t,c2139=t,c2140=t,c2141=t,c2142=t,c2143=t,c2144=t,c2145=t,c2146=t,c2147=t,c2148=t,c2149=t,c2150=t,c2151=t,"
      "c2152=t,c2153=t,c2154=t,c2155=t,c2156=t,c2157=t,c2158=t,c2159=t,c2160=t,c2161=t,c2162=t,c2163=t,c2164=t,c2165=t,"
      "c2166=t,c2167=t,c2168=t,c2169=t,c2170=t,c2171=t,c2172=t,c2173=t,c2174=t,c2175=t,c2176=t,c2177=t,c2178=t,c2179=t,"
      "c2180=t,c2181=t,c2182=t,c2183=t,c2184=t,c2185=t,c2186=t,c2187=t,c2188=t,c2189=t,c2190=t,c2191=t,c2192=t,c2193=t,"
      "c2194=t,c2195=t,c2196=t,c2197=t,c2198=t,c2199=t,c2200=t,c2201=t,c2202=t,c2203=t,c2204=t,c2205=t,c2206=t,c2207=t,"
      "c2208=t,c2209=t,c2210=t,c2211=t,c2212=t,c2213=t,c2214=t,c2215=t,c2216=t,c2217=t,c2218=t,c2219=t,c2220=t,c2221=t,"
      "c2222=t,c2223=t,c2224=t,c2225=t,c2226=t,c2227=t,c2228=t,c2229=t,c2230=t,c2231=t,c2232=t,c2233=t,c2234=t,c2235=t,"
      "c2236=t,c2237=t,c2238=t,c2239=t,c2240=t,c2241=t,c2242=t,c2243=t,c2244=t,c2245=t,c2246=t,c2247=t,c2248=t,c2249=t,"
      "c2250=t,c2251=t,c2252=t,c2253=t,c2254=t,c2255=t,c2256=t,"
      "c2257=t,c2258=t,c2259=t,c2260=t,c2261=t,c2262=t,c2263=t,c2264=t,c2265=t,c2266=t,c2267=t,c2268=t,c2269=t,c2270=t,"
      "c2271=t,c2272=t,c2273=t,c2274=t,c2275=t,c2276=t,c2277=t,c2278=t,c2279=t,c2280=t,c2281=t,c2282=t,c2283=t,"
      "c2284=t,c2285=t,c2286=t,c2287=t,c2288=t,c2289=t,c2290=t,c2291=t,c2292=t,c2293=t,c2294=t,c2295=t,c2296=t,c2297=t,"
      "c2298=t,c2299=t,c2300=t,c2301=t,c2302=t,c2303=t,c2304=t,c2305=t,c2306=t,c2307=t,c2308=t,c2309=t,c2310=t,c2311=t,"
      "c2312=t,c2313=t,c2314=t,c2315=t,c2316=t,c2317=t,c2318=t,c2319=t,c2320=t,c2321=t,c2322=t,c2323=t,c2324=t,c2325=t,"
      "c2326=t,c2327=t,c2328=t,c2329=t,c2330=t,c2331=t,c2332=t,c2333=t,c2334=t,c2335=t,c2336=t,c2337=t,c2338=t,c2339=t,"
      "c2340=t,c2341=t,c2342=t,c2343=t,c2344=t,c2345=t,c2346=t,c2347=t,c2348=t,c2349=t,c2350=t,c2351=t,c2352=t,c2353=t,"
      "c2354=t,c2355=t,c2356=t,c2357=t,c2358=t,c2359=t,c2360=t,c2361=t,c2362=t,c2363=t,c2364=t,c2365=t,c2366=t,c2367=t,"
      "c2368=t,c2369=t,c2370=t,c2371=t,c2372=t,c2373=t,c2374=t,c2375=t,c2376=t,c2377=t,c2378=t,c2379=t,c2380=t,c2381=t,"
      "c2382=t,c2383=t,c2384=t,c2385=t,c2386=t,c2387=t,c2388=t,c2389=t,c2390=t,c2391=t,c2392=t,c2393=t,c2394=t,c2395=t,"
      "c2396=t,c2397=t,c2398=t,c2399=t,c2400=t,c2401=t,c2402=t,c2403=t,c2404=t,c2405=t,c2406=t,c2407=t,c2408=t,c2409=t,"
      "c2410=t,c2411=t,c2412=t,c2413=t,c2414=t,c2415=t,c2416=t,c2417=t,c2418=t,c2419=t,c2420=t,c2421=t,c2422=t,c2423=t,"
      "c2424=t,c2425=t,c2426=t,c2427=t,c2428=t,c2429=t,c2430=t,"
      "c2431=t,c2432=t,c2433=t,c2434=t,c2435=t,c2436=t,c2437=t,c2438=t,c2439=t,c2440=t,c2441=t,c2442=t,c2443=t,c2444=t,"
      "c2445=t,c2446=t,c2447=t,c2448=t,c2449=t,c2450=t,c2451=t,c2452=t,c2453=t,c2454=t,c2455=t,c2456=t,c2457=t,c2458=t,"
      "c2459=t,c2460=t,c2461=t,c2462=t,c2463=t,c2464=t,c2465=t,c2466=t,c2467=t,c2468=t,c2469=t,c2470=t,c2471=t,c2472=t,"
      "c2473=t,c2474=t,c2475=t,c2476=t,c2477=t,c2478=t,c2479=t,c2480=t,c2481=t,c2482=t,c2483=t,c2484=t,c2485=t,c2486=t,"
      "c2487=t,c2488=t,c2489=t,c2490=t,c2491=t,c2492=t,c2493=t,c2494=t,c2495=t,c2496=t,c2497=t,c2498=t,c2499=t,c2500=t,"
      "c2501=t,c2502=t,c2503=t,c2504=t,c2505=t,c2506=t,c2507=t,c2508=t,c2509=t,c2510=t,c2511=t,c2512=t,c2513=t,c2514=t,"
      "c2515=t,c2516=t,c2517=t,c2518=t,c2519=t,c2520=t,c2521=t,c2522=t,c2523=t,c2524=t,c2525=t,c2526=t,c2527=t,c2528=t,"
      "c2529=t,c2530=t,c2531=t,c2532=t,c2533=t,c2534=t,c2535=t,c2536=t,c2537=t,c2538=t,c2539=t,c2540=t,c2541=t,c2542=t,"
      "c2543=t,c2544=t,c2545=t,c2546=t,c2547=t,c2548=t,c2549=t,c2550=t,c2551=t,c2552=t,c2553=t,c2554=t,c2555=t,c2556=t,"
      "c2557=t,c2558=t,c2559=t,c2560=t,c2561=t,c2562=t,c2563=t,c2564=t,c2565=t,c2566=t,c2567=t,c2568=t,c2569=t,c2570=t,"
      "c2571=t,c2572=t,c2573=t,c2574=t,c2575=t,c2576=t,c2577=t,"
      "c2578=t,c2579=t,c2580=t,c2581=t,c2582=t,c2583=t,c2584=t,c2585=t,c2586=t,c2587=t,c2588=t,c2589=t,c2590=t,c2591=t,"
      "c2592=t,c2593=t,c2594=t,c2595=t,c2596=t,c2597=t,c2598=t,c2599=t,c2600=t,c2601=t,c2602=t,c2603=t,c2604=t,c2605=t,"
      "c2606=t,c2607=t,c2608=t,c2609=t,c2610=t,c2611=t,c2612=t,c2613=t,c2614=t,c2615=t,c2616=t,c2617=t,c2618=t,c2619=t,"
      "c2620=t,c2621=t,c2622=t,c2623=t,c2624=t,c2625=t,c2626=t,c2627=t,c2628=t,c2629=t,c2630=t,c2631=t,c2632=t,c2633=t,"
      "c2634=t,c2635=t,c2636=t,c2637=t,c2638=t,c2639=t,c2640=t,c2641=t,c2642=t,c2643=t,c2644=t,c2645=t,c2646=t,c2647=t,"
      "c2648=t,c2649=t,c2650=t,c2651=t,c2652=t,c2653=t,c2654=t,c2655=t,c2656=t,c2657=t,c2658=t,c2659=t,c2660=t,c2661=t,"
      "c2662=t,c2663=t,c2664=t,c2665=t,c2666=t,c2667=t,c2668=t,c2669=t,c2670=t,c2671=t,c2672=t,c2673=t,c2674=t,c2675=t,"
      "c2676=t,c2677=t,c2678=t,c2679=t,c2680=t,c2681=t,c2682=t,c2683=t,c2684=t,c2685=t,c2686=t,c2687=t,c2688=t,c2689=t,"
      "c2690=t,c2691=t,c2692=t,c2693=t,c2694=t,c2695=t,c2696=t,c2697=t,c2698=t,c2699=t,c2700=t,c2701=t,c2702=t,c2703=t,"
      "c2704=t,c2705=t,c2706=t,c2707=t,c2708=t,c2709=t,c2710=t,c2711=t,c2712=t,c2713=t,c2714=t,c2715=t,c2716=t,c2717=t,"
      "c2718=t,c2719=t,c2720=t,c2721=t,c2722=t,c2723=t,c2724=t,"
      "c2725=t,c2726=t,c2727=t,c2728=t,c2729=t,c2730=t,c2731=t,c2732=t,c2733=t,c2734=t,c2735=t,c2736=t,c2737=t,c2738=t,"
      "c2739=t,c2740=t,c2741=t,c2742=t,c2743=t,c2744=t,c2745=t,c2746=t,c2747=t,c2748=t,c2749=t,c2750=t,c2751=t,c2752=t,"
      "c2753=t,c2754=t,c2755=t,c2756=t,c2757=t,c2758=t,c2759=t,c2760=t,c2761=t,c2762=t,c2763=t,c2764=t,c2765=t,c2766=t,"
      "c2767=t,c2768=t,c2769=t,c2770=t,c2771=t,c2772=t,c2773=t,c2774=t,c2775=t,c2776=t,c2777=t,c2778=t,c2779=t,c2780=t,"
      "c2781=t,c2782=t,c2783=t,c2784=t,c2785=t,c2786=t,c2787=t,c2788=t,c2789=t,c2790=t,c2791=t,c2792=t,c2793=t,c2794=t,"
      "c2795=t,c2796=t,c2797=t,c2798=t,c2799=t,c2800=t,c2801=t,c2802=t,c2803=t,c2804=t,c2805=t,c2806=t,c2807=t,c2808=t,"
      "c2809=t,c2810=t,c2811=t,c2812=t,c2813=t,c2814=t,c2815=t,c2816=t,c2817=t,c2818=t,c2819=t,c2820=t,c2821=t,c2822=t,"
      "c2823=t,c2824=t,c2825=t,c2826=t,c2827=t,c2828=t,c2829=t,c2830=t,c2831=t,c2832=t,c2833=t,c2834=t,c2835=t,c2836=t,"
      "c2837=t,c2838=t,c2839=t,c2840=t,c2841=t,c2842=t,c2843=t,c2844=t,c2845=t,c2846=t,c2847=t,c2848=t,c2849=t,c2850=t,"
      "c2851=t,c2852=t,c2853=t,c2854=t,c2855=t,c2856=t,c2857=t,c2858=t,c2859=t,c2860=t,c2861=t,c2862=t,c2863=t,c2864=t,"
      "c2865=t,c2866=t,c2867=t,c2868=t,c2869=t,c2870=t,c2871=t,"
      "c2872=t,c2873=t,c2874=t,c2875=t,c2876=t,c2877=t,c2878=t,c2879=t,c2880=t,c2881=t,c2882=t,c2883=t,c2884=t,c2885=t,"
      "c2886=t,c2887=t,c2888=t,c2889=t,c2890=t,c2891=t,c2892=t,c2893=t,c2894=t,c2895=t,c2896=t,c2897=t,c2898=t,c2899=t,"
      "c2900=t,c2901=t,c2902=t,c2903=t,c2904=t,c2905=t,c2906=t,c2907=t,c2908=t,c2909=t,c2910=t,c2911=t,c2912=t,c2913=t,"
      "c2914=t,c2915=t,c2916=t,c2917=t,c2918=t,c2919=t,c2920=t,c2921=t,c2922=t,c2923=t,c2924=t,c2925=t,c2926=t,c2927=t,"
      "c2928=t,c2929=t,c2930=t,c2931=t,c2932=t,c2933=t,c2934=t,c2935=t,c2936=t,c2937=t,c2938=t,c2939=t,c2940=t,c2941=t,"
      "c2942=t,c2943=t,c2944=t,c2945=t,c2946=t,c2947=t,c2948=t,c2949=t,c2950=t,c2951=t,c2952=t,c2953=t,c2954=t,c2955=t,"
      "c2956=t,c2957=t,c2958=t,c2959=t,c2960=t,c2961=t,c2962=t,c2963=t,c2964=t,c2965=t,c2966=t,c2967=t,c2968=t,c2969=t,"
      "c2970=t,c2971=t,c2972=t,c2973=t,c2974=t,c2975=t,c2976=t,c2977=t,c2978=t,c2979=t,c2980=t,c2981=t,c2982=t,c2983=t,"
      "c2984=t,c2985=t,c2986=t,c2987=t,c2988=t,c2989=t,c2990=t,c2991=t,c2992=t,c2993=t,c2994=t,c2995=t,c2996=t,c2997=t,"
      "c2998=t,c2999=t,c3000=t,c3001=t,c3002=t,c3003=t,c3004=t,c3005=t,c3006=t,c3007=t,c3008=t,c3009=t,c3010=t,c3011=t,"
      "c3012=t,c3013=t,c3014=t,c3015=t,c3016=t,c3017=t,c3018=t,"
      "c3019=t,c3020=t,c3021=t,c3022=t,c3023=t,c3024=t,c3025=t,c3026=t,c3027=t,c3028=t,c3029=t,c3030=t,c3031=t,c3032=t,"
      "c3033=t,c3034=t,c3035=t,c3036=t,c3037=t,c3038=t,c3039=t,c3040=t,c3041=t,c3042=t,c3043=t,c3044=t,c3045=t,c3046=t,"
      "c3047=t,c3048=t,c3049=t,c3050=t,c3051=t,c3052=t,c3053=t,c3054=t,c3055=t,c3056=t,c3057=t,c3058=t,c3059=t,c3060=t,"
      "c3061=t,c3062=t,c3063=t,c3064=t,c3065=t,c3066=t,c3067=t,c3068=t,c3069=t,c3070=t,c3071=t,c3072=t,c3073=t,c3074=t,"
      "c3075=t,c3076=t,c3077=t,c3078=t,c3079=t,c3080=t,c3081=t,c3082=t,c3083=t,c3084=t,c3085=t,c3086=t,c3087=t,c3088=t,"
      "c3089=t,c3090=t,c3091=t,c3092=t,c3093=t,c3094=t,c3095=t,c3096=t,c3097=t,c3098=t,c3099=t,c3100=t,c3101=t,c3102=t,"
      "c3103=t,c3104=t,c3105=t,c3106=t,c3107=t,c3108=t,c3109=t,c3110=t,c3111=t,c3112=t,c3113=t,c3114=t,c3115=t,c3116=t,"
      "c3117=t,c3118=t,c3119=t,c3120=t,c3121=t,c3122=t,c3123=t,c3124=t,c3125=t,c3126=t,c3127=t,c3128=t,c3129=t,c3130=t,"
      "c3131=t,c3132=t,c3133=t,c3134=t,c3135=t,c3136=t,c3137=t,c3138=t,c3139=t,c3140=t,c3141=t,c3142=t,c3143=t,c3144=t,"
      "c3145=t,c3146=t,c3147=t,c3148=t,c3149=t,c3150=t,c3151=t,c3152=t,c3153=t,c3154=t,c3155=t,c3156=t,c3157=t,c3158=t,"
      "c3159=t,c3160=t,c3161=t,c3162=t,c3163=t,c3164=t,c3165=t,"
      "c3166=t,c3167=t,c3168=t,c3169=t,c3170=t,c3171=t,c3172=t,c3173=t,c3174=t,c3175=t,c3176=t,c3177=t,c3178=t,c3179=t,"
      "c3180=t,c3181=t,c3182=t,c3183=t,c3184=t,c3185=t,c3186=t,c3187=t,c3188=t,c3189=t,c3190=t,c3191=t,c3192=t,c3193=t,"
      "c3194=t,c3195=t,c3196=t,c3197=t,c3198=t,c3199=t,c3200=t,c3201=t,c3202=t,c3203=t,c3204=t,c3205=t,c3206=t,c3207=t,"
      "c3208=t,c3209=t,c3210=t,c3211=t,c3212=t,c3213=t,c3214=t,c3215=t,c3216=t,c3217=t,c3218=t,c3219=t,c3220=t,c3221=t,"
      "c3222=t,c3223=t,c3224=t,c3225=t,c3226=t,c3227=t,c3228=t,c3229=t,c3230=t,c3231=t,c3232=t,c3233=t,c3234=t,c3235=t,"
      "c3236=t,c3237=t,c3238=t,c3239=t,c3240=t,c3241=t,c3242=t,c3243=t,c3244=t,c3245=t,c3246=t,c3247=t,c3248=t,c3249=t,"
      "c3250=t,c3251=t,c3252=t,c3253=t,c3254=t,c3255=t,c3256=t,c3257=t,c3258=t,c3259=t,c3260=t,c3261=t,c3262=t,c3263=t,"
      "c3264=t,c3265=t,c3266=t,c3267=t,c3268=t,c3269=t,c3270=t,c3271=t,c3272=t,c3273=t,c3274=t,c3275=t,c3276=t,c3277=t,"
      "c3278=t,c3279=t,c3280=t,c3281=t,c3282=t,c3283=t,c3284=t,c3285=t,c3286=t,c3287=t,c3288=t,c3289=t,c3290=t,c3291=t,"
      "c3292=t,c3293=t,c3294=t,c3295=t,c3296=t,c3297=t,c3298=t,c3299=t,c3300=t,c3301=t,c3302=t,c3303=t,c3304=t,c3305=t,"
      "c3306=t,c3307=t,c3308=t,c3309=t,c3310=t,c3311=t,c3312=t,"
      "c3313=t,c3314=t,c3315=t,c3316=t,c3317=t,c3318=t,c3319=t,c3320=t,c3321=t,c3322=t,c3323=t,c3324=t,c3325=t,c3326=t,"
      "c3327=t,c3328=t,c3329=t,c3330=t,c3331=t,c3332=t,c3333=t,c3334=t,c3335=t,c3336=t,c3337=t,c3338=t,c3339=t,c3340=t,"
      "c3341=t,c3342=t,c3343=t,c3344=t,c3345=t,c3346=t,c3347=t,c3348=t,c3349=t,c3350=t,c3351=t,c3352=t,c3353=t,c3354=t,"
      "c3355=t,c3356=t,c3357=t,c3358=t,c3359=t,c3360=t,c3361=t,c3362=t,c3363=t,c3364=t,c3365=t,c3366=t,c3367=t,c3368=t,"
      "c3369=t,c3370=t,c3371=t,c3372=t,c3373=t,c3374=t,c3375=t,c3376=t,c3377=t,c3378=t,c3379=t,c3380=t,c3381=t,c3382=t,"
      "c3383=t,c3384=t,c3385=t,c3386=t,c3387=t,c3388=t,c3389=t,c3390=t,c3391=t,c3392=t,c3393=t,c3394=t,c3395=t,c3396=t,"
      "c3397=t,c3398=t,c3399=t,c3400=t,c3401=t,c3402=t,c3403=t,c3404=t,c3405=t,c3406=t,c3407=t,c3408=t,c3409=t,c3410=t,"
      "c3411=t,c3412=t,c3413=t,c3414=t,c3415=t,c3416=t,c3417=t,c3418=t,c3419=t,c3420=t,c3421=t,c3422=t,c3423=t,c3424=t,"
      "c3425=t,c3426=t,c3427=t,c3428=t,c3429=t,c3430=t,c3431=t,c3432=t,c3433=t,c3434=t,c3435=t,c3436=t,c3437=t,c3438=t,"
      "c3439=t,c3440=t,c3441=t,c3442=t,c3443=t,c3444=t,c3445=t,c3446=t,c3447=t,c3448=t,c3449=t,c3450=t,c3451=t,c3452=t,"
      "c3453=t,c3454=t,c3455=t,c3456=t,c3457=t,c3458=t,c3459=t,"
      "c3460=t,c3461=t,c3462=t,c3463=t,c3464=t,c3465=t,c3466=t,c3467=t,c3468=t,c3469=t,c3470=t,c3471=t,c3472=t,c3473=t,"
      "c3474=t,c3475=t,c3476=t,c3477=t,c3478=t,c3479=t,c3480=t,c3481=t,c3482=t,c3483=t,c3484=t,c3485=t,c3486=t,c3487=t,"
      "c3488=t,c3489=t,c3490=t,c3491=t,c3492=t,c3493=t,c3494=t,c3495=t,c3496=t,c3497=t,c3498=t,c3499=t,c3500=t,c3501=t,"
      "c3502=t,c3503=t,c3504=t,c3505=t,c3506=t,c3507=t,c3508=t,c3509=t,c3510=t,c3511=t,c3512=t,c3513=t,"
      "c3514=t,c3515=t,c3516=t,c3517=t,c3518=t,c3519=t,c3520=t,c3521=t,c3522=t,c3523=t,c3524=t,c3525=t,c3526=t,c3527=t,"
      "c3528=t,c3529=t,c3530=t,c3531=t,c3532=t,c3533=t,c3534=t,c3535=t,c3536=t,c3537=t,c3538=t,c3539=t,c3540=t,c3541=t,"
      "c3542=t,c3543=t,c3544=t,c3545=t,c3546=t,c3547=t,c3548=t,c3549=t,c3550=t,c3551=t,c3552=t,c3553=t,c3554=t,c3555=t,"
      "c3556=t,c3557=t,c3558=t,c3559=t,c3560=t,c3561=t,c3562=t,c3563=t,c3564=t,c3565=t,c3566=t,c3567=t,c3568=t,c3569=t,"
      "c3570=t,c3571=t,c3572=t,c3573=t,c3574=t,c3575=t,c3576=t,c3577=t,c3578=t,c3579=t,c3580=t,c3581=t,c3582=t,c3583=t,"
      "c3584=t,c3585=t,c3586=t,c3587=t,c3588=t,c3589=t,c3590=t,c3591=t,c3592=t,c3593=t,c3594=t,c3595=t,c3596=t,c3597=t,"
      "c3598=t,c3599=t,c3600=t,c3601=t,c3602=t,c3603=t,c3604=t,c3605=t,c3606=t,c3607=t,c3608=t,c3609=t,c3610=t,c3611=t,"
      "c3612=t,c3613=t,c3614=t,c3615=t,c3616=t,c3617=t,c3618=t,c3619=t,c3620=t,c3621=t,c3622=t,c3623=t,c3624=t,c3625=t,"
      "c3626=t,c3627=t,c3628=t,c3629=t,c3630=t,c3631=t,c3632=t,c3633=t,c3634=t,c3635=t,c3636=t,c3637=t,c3638=t,c3639=t,"
      "c3640=t,c3641=t,c3642=t,c3643=t,c3644=t,c3645=t,c3646=t,c3647=t,c3648=t,c3649=t,c3650=t,c3651=t,c3652=t,c3653=t,"
      "c3654=t,c3655=t,c3656=t,c3657=t,c3658=t,c3659=t,c3660=t,"
      "c3661=t,c3662=t,c3663=t,c3664=t,c3665=t,c3666=t,c3667=t,c3668=t,c3669=t,c3670=t,c3671=t,c3672=t,c3673=t,c3674=t,"
      "c3675=t,c3676=t,c3677=t,c3678=t,c3679=t,c3680=t,c3681=t,c3682=t,c3683=t,c3684=t,c3685=t,c3686=t,c3687=t,c3688=t,"
      "c3689=t,c3690=t,c3691=t,c3692=t,c3693=t,c3694=t,c3695=t,c3696=t,c3697=t,c3698=t,c3699=t,c3700=t,c3701=t,c3702=t,"
      "c3703=t,c3704=t,c3705=t,c3706=t,c3707=t,c3708=t,c3709=t,c3710=t,c3711=t,c3712=t,c3713=t,c3714=t,c3715=t,c3716=t,"
      "c3717=t,c3718=t,c3719=t,c3720=t,c3721=t,c3722=t,c3723=t,c3724=t,c3725=t,c3726=t,c3727=t,c3728=t,c3729=t,c3730=t,"
      "c3731=t,c3732=t,c3733=t,c3734=t,c3735=t,c3736=t,c3737=t,c3738=t,c3739=t,c3740=t,c3741=t,c3742=t,c3743=t,c3744=t,"
      "c3745=t,c3746=t,c3747=t,c3748=t,c3749=t,c3750=t,c3751=t,c3752=t,c3753=t,c3754=t,c3755=t,c3756=t,c3757=t,c3758=t,"
      "c3759=t,c3760=t,c3761=t,c3762=t,c3763=t,c3764=t,c3765=t,c3766=t,c3767=t,c3768=t,c3769=t,c3770=t,c3771=t,c3772=t,"
      "c3773=t,c3774=t,c3775=t,c3776=t,c3777=t,c3778=t,c3779=t,c3780=t,c3781=t,c3782=t,c3783=t,c3784=t,c3785=t,c3786=t,"
      "c3787=t,c3788=t,c3789=t,c3790=t,c3791=t,c3792=t,c3793=t,c3794=t,c3795=t,c3796=t,c3797=t,c3798=t,c3799=t,c3800=t,"
      "c3801=t,c3802=t,c3803=t,c3804=t,c3805=t,c3806=t,c3807=t,"
      "c3808=t,c3809=t,c3810=t,c3811=t,c3812=t,c3813=t,c3814=t,c3815=t,c3816=t,c3817=t,c3818=t,c3819=t,c3820=t,c3821=t,"
      "c3822=t,c3823=t,c3824=t,c3825=t,c3826=t,c3827=t,c3828=t,c3829=t,c3830=t,c3831=t,c3832=t,c3833=t,c3834=t,c3835=t,"
      "c3836=t,c3837=t,c3838=t,c3839=t,c3840=t,c3841=t,c3842=t,c3843=t,c3844=t,c3845=t,c3846=t,c3847=t,c3848=t,c3849=t,"
      "c3850=t,c3851=t,c3852=t,c3853=t,c3854=t,c3855=t,c3856=t,c3857=t,c3858=t,c3859=t,c3860=t,c3861=t,c3862=t,c3863=t,"
      "c3864=t,c3865=t,c3866=t,c3867=t,c3868=t,c3869=t,c3870=t,c3871=t,c3872=t,c3873=t,c3874=t,c3875=t,c3876=t,c3877=t,"
      "c3878=t,c3879=t,c3880=t,c3881=t,c3882=t,c3883=t,c3884=t,c3885=t,c3886=t,c3887=t,c3888=t,c3889=t,c3890=t,c3891=t,"
      "c3892=t,c3893=t,c3894=t,c3895=t,c3896=t,c3897=t,c3898=t,c3899=t,c3900=t,c3901=t,c3902=t,c3903=t,c3904=t,c3905=t,"
      "c3906=t,c3907=t,c3908=t,c3909=t,c3910=t,c3911=t,c3912=t,c3913=t,c3914=t,c3915=t,c3916=t,c3917=t,c3918=t,c3919=t,"
      "c3920=t,c3921=t,c3922=t,c3923=t,c3924=t,c3925=t,c3926=t,c3927=t,c3928=t,c3929=t,c3930=t,c3931=t,c3932=t,c3933=t,"
      "c3934=t,c3935=t,c3936=t,c3937=t,c3938=t,c3939=t,c3940=t,c3941=t,c3942=t,c3943=t,c3944=t,c3945=t,c3946=t,c3947=t,"
      "c3948=t,c3949=t,c3950=t,c3951=t,c3952=t,c3953=t,c3954=t,"
      "c3955=t,c3956=t,c3957=t,c3958=t,c3959=t,c3960=t,c3961=t,c3962=t,c3963=t,c3964=t,c3965=t,c3966=t,c3967=t,c3968=t,"
      "c3969=t,c3970=t,c3971=t,c3972=t,c3973=t,c3974=t,c3975=t,c3976=t,c3977=t,c3978=t,c3979=t,c3980=t,c3981=t,c3982=t,"
      "c3983=t,c3984=t,c3985=t,c3986=t,c3987=t,c3988=t,c3989=t,c3990=t,c3991=t,c3992=t,c3993=t,c3994=t,c3995=t,c3996=t,"
      "c3997=t,c3998=t,c3999=t,c4000=t,c4001=t,c4002=t,c4003=t,c4004=t,c4005=t,c4006=t,c4007=t,c4008=t,c4009=t,c4010=t,"
      "c4011=t,c4012=t,c4013=t,c4014=t,c4015=t,c4016=t,c4017=t,c4018=t,c4019=t,c4020=t,c4021=t,c4022=t,c4023=t,c4024=t,"
      "c4025=t,c4026=t,c4027=t,c4028=t,c4029=t,c4030=t,c4031=t,c4032=t,c4033=t,c4034=t,c4035=t,c4036=t,c4037=t,c4038=t,"
      "c4039=t,c4040=t,c4041=t,c4042=t,c4043=t,c4044=t,c4045=t,c4046=t,c4047=t,c4048=t,c4049=t,c4050=t,c4051=t,c4052=t,"
      "c4053=t,c4054=t,c4055=t,c4056=t,c4057=t,c4058=t,c4059=t,c4060=t,c4061=t,c4062=t,c4063=t,c4064=t,c4065=t,c4066=t,"
      "c4067=t,c4068=t,c4069=t,c4070=t,c4071=t,c4072=t,c4073=t,c4074=t,c4075=t,c4076=t,c4077=t,c4078=t,c4079=t,c4080=t,"
      "c4081=t,c4082=t,c4083=t,c4084=t,c4085=t,c4086=t,c4087=t,c4088=t,c4089=t,c4090=t,c4091=t,c4092=t,c4093=t "
      "1626006833640000000"};

  int ret = TSDB_CODE_SUCCESS;
  for (int i = 0; i < sizeof(sql) / sizeof(sql[0]); i++) {
    ret = smlParseInfluxLine(info, sql[i], strlen(sql[i]));
    if (ret != TSDB_CODE_SUCCESS) break;
  }
  ASSERT_NE(ret, 0);
  smlDestroyInfo(info);
}
