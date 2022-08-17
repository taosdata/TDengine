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
  char *sql = (char*)taosMemoryCalloc(256, 1);
  memcpy(sql, tmp, strlen(tmp) + 1);
  int   ret = smlParseInfluxString(sql, &elements, &msgBuf);
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
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
  ASSERT_NE(ret, 0);

  // case 3  false
  tmp = "st, t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2,c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(elements.cols, sql + elements.measureTagsLen + 1);
  ASSERT_EQ(elements.colsLen, strlen("t1=3,t2=4,t3=t3"));

  // case 4  tag is null
  tmp = "st, c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
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
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
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
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);

  // case 7
  tmp = " st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
  ASSERT_EQ(ret, 0);

  // case 8 false
  tmp = ", st   ,   ";
  memcpy(sql, tmp, strlen(tmp) + 1);
  memset(&elements, 0, sizeof(SSmlLineInfo));
  ret = smlParseInfluxString(sql, &elements, &msgBuf);
  ASSERT_NE(ret, 0);
  taosMemoryFree(sql);
}

TEST(testCase, smlParseCols_Error_Test) {
  const char *data[] = {
    "c=\"89sd",           // binary, nchar
    "c=j\"89sd\"",
    "c=\"89sd\"k",
    "c=u",                // bool
    "c=truet",
    "c=f64",              // double
    "c=8f64f",
    "c=8ef64",
    "c=f32",              // float
    "c=8f32f",
    "c=8wef32",
    "c=-3.402823466e+39f32",
    "c=",                 // double
    "c=8f",
    "c=8we",
    "c=i8",              // tiny int
    "c=-8i8f",
    "c=8wei8",
    "c=-999i8",
    "c=u8",              // u tiny int
    "c=8fu8",
    "c=8weu8",
    "c=999u8",
    "c=-8u8",
    "c=i16",              // small int
    "c=8fi16u",
    "c=8wei16",
    "c=-67787i16",
    "c=u16",              // u small int
    "c=8u16f",
    "c=8weu16",
    "c=-9u16",
    "c=67787u16",
    "c=i32",                // int
    "c=8i32f",
    "c=8wei32",
    "c=2147483649i32",
    "c=u32",                // u int
    "c=8u32f",
    "c=8weu32",
    "c=-4u32",
    "c=42949672958u32",
    "c=i64",                // big int
    "c=8i64i",
    "c=8wei64",
    "c=-9223372036854775809i64",
    "c=i",                  // big int
    "c=8fi",
    "c=8wei",
    "c=9223372036854775808i",
    "c=u64",                // u big int
    "c=8u64f",
    "c=8weu64",
    "c=-3.402823466e+39u64",
    "c=-339u64",
    "c=18446744073709551616u64",
    "c=1,c=2",
    "c=1=2"
  };

  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  for(int i = 0; i < sizeof(data)/sizeof(data[0]); i++){
    char       msg[256] = {0};
    SSmlMsgBuf msgBuf;
    msgBuf.buf = msg;
    msgBuf.len = 256;
    int32_t len = strlen(data[i]);
    char *sql = (char*)taosMemoryCalloc(256, 1);
    memcpy(sql, data[i], len + 1);
    SArray *cols = taosArrayInit(8, POINTER_BYTES);
    int32_t ret = smlParseCols(sql, len, cols, NULL, false, dumplicateKey, &msgBuf);
    printf("i:%d\n",i);
    ASSERT_NE(ret, TSDB_CODE_SUCCESS);
    taosHashClear(dumplicateKey);
    taosMemoryFree(sql);
    for(int j = 0; j < taosArrayGetSize(cols); j++){
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
      "cbin=\"passit helloc\",cnch=L\"iisdfsf\",cbool=false,cf64=4.31f64,cf64_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16=233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,cboolf=f,cnch_=l\"iuwq\"";
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

  for(int i = 0; i < size; i++){
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

  const char *data = "cb\\=in=\"pass\\,it hello,c=2\",cnch=L\"ii\\=sdfsf\",cbool=false,cf64=4.31f64,cf64_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16=233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,cboolf=f,cnch_=l\"iuwq\"";
  int32_t len = strlen(data);
  char *sql = (char*)taosMemoryCalloc(1024, 1);
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
  //ASSERT_EQ(kv->d, 4.31);
  printf("4.31 = kv->d:%f\n", kv->d);
  taosMemoryFree(kv);

  // float
  kv = (SSmlKv *)taosArrayGetP(cols, 4);
  ASSERT_EQ(strncasecmp(kv->key, "cf64_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  //ASSERT_EQ(kv->f, 8.32);
  printf("8.32 = kv->d:%f\n", kv->d);
  taosMemoryFree(kv);

  // float
  kv = (SSmlKv *)taosArrayGetP(cols, 5);
  ASSERT_EQ(strncasecmp(kv->key, "cf32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_FLOAT);
  ASSERT_EQ(kv->length, 4);
  //ASSERT_EQ(kv->f, 8.23);
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
  SSmlKv kv = {0};
  char buf[64] = {0};
  SSmlMsgBuf msg = {0};
  msg.buf = buf;
  msg.len = 64;
  kv.value = "3.2e-900";
  kv.length = 8;
  bool res = smlParseNumber(&kv, &msg);
  printf("res:%d,v:%f, %f\n", res,kv.d, HUGE_VAL);
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
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    int ret = smlParseTelnetLine(info, (void*)sql[i]);
    ASSERT_NE(ret, 0);
  }

  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_type_Test) {
  SSmlHandle *info = smlBuildSmlInfo(NULL, NULL, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  const char *sql[] = {
      "sys.procs.running 1479496104000 42 host=web01",
      "sys.procs.running 1479496104000 42u8 host=web01",
      "appywjnuct 1626006833641 True id=\"appywjnuct_40601_49808_1\" t0=t t1=127i8 id=\"appywjnuct_40601_49808_2\" t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7=\"binaryTagValue\" t8=L\"ncharTagValue\""
  };

  int ret = TSDB_CODE_SUCCESS;
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    ret = smlParseTelnetLine(info, (void*)sql[i]);
    if(ret != TSDB_CODE_SUCCESS) break;
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
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    ret = smlParseTelnetLine(info, (void*)sql[i]);
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
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    ret = smlParseTelnetLine(info, (void*)sql[i]);
    if(ret != TSDB_CODE_SUCCESS) break;
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
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    ret = smlParseTelnetLine(info, (void*)sql[i]);
    if(ret != TSDB_CODE_SUCCESS) break;
  }
  ASSERT_NE(ret, 0);
  smlDestroyInfo(info);
}
