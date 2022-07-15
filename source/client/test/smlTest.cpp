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

/*
TEST(testCase, smlProcess_influx_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists inflx_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use inflx_db");
  taos_free_result(pRes);

  const char *sql[] = {
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0 1451606401000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451607402000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,heading=221,grade=0,fuel_consumption=25 1451608403000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451609404000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 1451619405000000000",
    "readings,name=truck_1,fleet=South,driver=Albert,model=F-150,device_version=v1.5 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=72.45258,longitude=68.83761,elevation=255,velocity=0,heading=181,grade=0,fuel_consumption=25 1451606406000000000",
    "readings,name=truck_2,driver=Derek,model=F-150,device_version=v1.5 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation=428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451606407000000000",
    "readings,name=truck_2,fleet=North,driver=Derek,model=F-150 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation=428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451609408000000000",
    "readings,fleet=South,name=truck_0,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 1451629409000000000",
    "stable,t1=t1,t2=t2,t3=t3 c1=1,c2=2,c3=\"kk\",c4=4 1451629501000000000",
    "stable,t2=t2,t1=t1,t3=t3 c1=1,c3=\"\",c4=4 1451629602000000000",
  };
  pRes = taos_schemaless_insert(taos, (char**)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  // case 1
  pRes = taos_query(taos, "select * from t_91e0b182be80332b5c530cbf872f760e");
  ASSERT_NE(pRes, nullptr);
  int fieldNum = taos_field_count(pRes);
  ASSERT_EQ(fieldNum, 11);
  printf("fieldNum:%d\n", fieldNum);

  TAOS_ROW row = NULL;
  int32_t rowIndex = 0;
  while((row = taos_fetch_row(pRes)) != NULL) {
    int64_t ts = *(int64_t*)row[0];
    double load_capacity = *(double*)row[1];
    double fuel_capacity = *(double*)row[2];
    double nominal_fuel_consumption = *(double*)row[3];
    double latitude = *(double*)row[4];
    double longitude = *(double*)row[5];
    double elevation = *(double*)row[6];
    double velocity = *(double*)row[7];
    double heading = *(double*)row[8];
    double grade = *(double*)row[9];
    double fuel_consumption = *(double*)row[10];
    if(rowIndex == 0){
      ASSERT_EQ(ts, 1451606407000);
      ASSERT_EQ(load_capacity, 2000);
      ASSERT_EQ(fuel_capacity, 200);
      ASSERT_EQ(nominal_fuel_consumption, 15);
      ASSERT_EQ(latitude, 24.5208);
      ASSERT_EQ(longitude, 28.09377);
      ASSERT_EQ(elevation, 428);
      ASSERT_EQ(velocity, 0);
      ASSERT_EQ(heading, 304);
      ASSERT_EQ(grade, 0);
      ASSERT_EQ(fuel_consumption, 25);
    }else{
      ASSERT_FALSE(1);
    }
    rowIndex++;
  }
  taos_free_result(pRes);

  // case 2
  pRes = taos_query(taos, "select * from t_6885c584b98481584ee13dac399e173d");
  ASSERT_NE(pRes, nullptr);
  fieldNum = taos_field_count(pRes);
  ASSERT_EQ(fieldNum, 5);
  printf("fieldNum:%d\n", fieldNum);

  rowIndex = 0;
  while((row = taos_fetch_row(pRes)) != NULL) {
    int *length = taos_fetch_lengths(pRes);

    int64_t ts = *(int64_t*)row[0];
    double c1 = *(double*)row[1];
    double c4 = *(double*)row[4];
    if(rowIndex == 0){
      ASSERT_EQ(ts, 1451629501000);
      ASSERT_EQ(c1, 1);
      ASSERT_EQ(*(double*)row[2], 2);
      ASSERT_EQ(length[3], 2);
      ASSERT_EQ(memcmp(row[3], "kk", length[3]), 0);
      ASSERT_EQ(c4, 4);
    }else if(rowIndex == 1){
      ASSERT_EQ(ts, 1451629602000);
      ASSERT_EQ(c1, 1);
      ASSERT_EQ(row[2], nullptr);
      ASSERT_EQ(length[3], 0);
      ASSERT_EQ(c4, 4);
    }else{
      ASSERT_FALSE(1);
    }
    rowIndex++;
  }
  taos_free_result(pRes);

  // case 2
  pRes = taos_query(taos, "show tables");
  ASSERT_NE(pRes, nullptr);

  row = taos_fetch_row(pRes);
  int rowNum = taos_affected_rows(pRes);
  ASSERT_EQ(rowNum, 5);
  taos_free_result(pRes);
}

// different types
TEST(testCase, smlParseLine_error_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "st123456,t1=3i64,t2=4f64,t3=\"t3\" c1=3i64,c3=L\"passit\",c2=false,c4=4f64 1626006833639000000",
      "st123456,t1=4i64,t3=\"t4\",t2=5f64,t4=5f64 c1=3i64,c3=L\"passitagin\",c2=true,c4=5f64,c5=5f64 1626006833640000000",
      "test_stb,t2=5f64,t3=L\"ste\" c1=true,c2=4i64,c3=\"iam\" 1626056811823316532"
//      "mqjqbesqqq,t0=t c0=f,c1=\"grigbrpnjctnyhamnwfqsjrywdforgpwpabdisdnymlboguwxuoscfyyajiyusxrocjndexxcvcqrzgxceqolvtdrpeabrcokpmcnhduylzxxljospclwuebutrdbpklpdbtkrdppamenzlmkkzttacrfxaozvwodpxzralhmdhvgaurtacnyhlsaojjglfnrylswactjumeldmuuafnmwsuuyiwhpdzqludgpluvllfowkwhfbtgsjsnxdbfbcrqnrxllmokbzrkiuxkhumfcjogeugbbjowmckoeyrilsoenowqwjpuufprqnxxzjlwfxnoljtodghyfdtyyptxafertndevhewboikewxwtwvbusnjpxwpnhhcrqqyicuuxmadxqjhbodsbexgpuaicbxduewqnogdhyjhcyjyftfbvbctgbjrwrkqtmqhzwxnilkmorotbiuwsimuvloeykzxqdepdkvvdcjtzmsvdseygtprbvhuikvomoafwnfojzaojxbkbpwbjqasazgokjjpktofqhjqhxxplkdvttwflbekawvozxiuhoahajwpimnjsbzjfhqgbbcgjgrjszmuqmwupxqlosfdsqnpkertnamcipfanxxewygtkeiaqopvykygkfbihdqqvhwapyctmxbjvzdndobrooemwtotrjzuknaupwxrjbrjzmnmupbwcdwkoghsilyfrjeefwrmgordzlafyjweavrapqqsicqnmkjulambrjxcmmsnvcjbbbwrloifqnmcmqlndubcynhumpikddliddyduafrpfgcltiymwlpbhtukmyawxdaaqiscvpfvsacjdljlfaeqhearjyczdpjsyjygfwaegqtylpibtdqinncmttbtiifbsesqbhpieectocontqhoyggjgbgjiyegoypfxorfqgbewhfqhkqftjdwtcnaiconxwjwryxqyexmlauoiysodziwfyyzyfewnfjyvfnvvxkkxeajmwbypoodsfrfygadcwpcjhzvemaplczgqxpsxkgxuqbxqhchpybojemqgxlhcyxmddjvwnbkvykkhwebfdpoovtvzgpkuwneodbochwxwauggxulmkynhoohchnkkcybhtwelotxpzoqzuczhwbjxsuqckuzsapdfkeiwxkcutimncyfpuaovhzwaebebkxgbognzpcxldjptnnldzqwtzzsiyjambnbrbyuyptxdkyxhlvigovllmkylbyzecxhdxczlkvsconlfvnafqvrwhcughqbtlmwgumeponoonhqqbklqaxvslkxowcuztjikgbutrnkmizschzrjxbmzvkkdlcuchpnrbxhgvxjqxawftlbirksyqbnltbaxtpyivrrqgxcjjgbhhvlltfqogehhddmgzivwlznkfuqgfbxrtixuonnywsxsdunhsmziyitecmrmxjkzmqhivinqkqywggffljpoxnofmzxkrmnxbdkokaleaqwbqizzhvywrweklybntszygkvuvypmukyxawhgbtnltemjchaytpqjplavcbypkwjwdmfogdxiddrtwrvvoqlmhsqlrdmmibawotbouzosrmbzocqvqdhoamuzvrfeyxkrnzpzyuacffbuvlcqupmbmqughegvjrobyzcyhynnpvvjntdcyplahaajwcbbleblkhjyauehbuoudyzrsgrtqnufijaawllbiexvhveipkaxffuiyczbkpcpzdnajwkkbbfrfchpedgabsraaalfbddgypeayprqwjzfvifjmgwaexrezitgaqgjmgizaohcfizhocckuzysshxzwqolddumeomghgsoaaerfxsapupejhywucurrhgctmmlgbyfjigveayriyvdmapvafzeydlxiwxcgnnajwjaqzkecczrdlbxgtdvehelzibmogdijpdiatcafnediqldwszonasgodqnnvajqxuwuftuvcqwtvayeiyysihhckitlimuimjllslrcnbobmumpbtoakqxallkqhszloxzpogfxxclnkmbfnqqomtzfpzdgxnfyvppeybjnekchjsafvhkrpvxpiumfvraeqcwqneatdrxdykoyscehvputknetluvfexdvtnnnaitrbdwyrzwnymuimydmadqbncdfixsgrcxkdnjzzyimcfbomioddimdxzxbmqwgnrezaquhiqytxmgsqpmywzxqksahlgwpxprtuzxtghkdbgxwpmdqxastvqggkhaqkxhjdsfcwyljwlleymorfgkezzroxaalrguityckdxsgqkcpaxxcsqvttvmmmmxezdztmkgcpnxbobuggzuqaetqfnmttjbshhfqqfsmfylavatksdchwvfvhyipfsepkwzqtprzogoxxohvibkwwiuwpsybbqqgbvziecnulnudzpudxvtcosvedrdxkhnkprghljzltucqljhdwpsfrsfryxpzybmybockeswpyihgossicvoxroiuzvgkbtduxzgsmgrohrxjbdvpnqwhgtvfkjrgpmirfdqddoyaztlooxlzllljsniwxfbihodjfywxallozikruusmzigztbzlyofrxtghhjwgptdbntmqkoxmrgzaznesgsgjnbmgarjqsqvswzygkbgquhbxsulabxzfpfnopzfnbiqeivuzayjbaikqrxrhyysttcafyxfdzgbbadcxiqltlwyhbcibkcxildnhmgwskeztcnmzdncqlyzpbzifjrhflsahlecmxwxlzmvpkqdexbfflmjhdqymxmjrktxaratynebetkfaltnrjgvsbdbvdcdyqujuypensmjnjskovbeweuwnqfjueeylefnqvmdmkwjqfvbjcuaibosymddysdymzhscroykljydnfvwosgplfphpznaqsddtbcjmyxhmcnxwdesycovtwrlmixqmpnzsbyfwpgnujxxqillwpbasdnbfxzokimfkujvlabycfwlplzpcgangjagrrhhjbrtddgitpoemixmobwyabyhsnkjtbeasdejawrueegmijbupygyciwbrhiwisguhlthnkpjqyzhiwvfrpgglqphhjtirtjxsjqxvqjpmokcgqbtjpsvravymmrrpyedruuncsjbjyrysjowqsnwtmvbakadxkxbyfunxnrkqhqvoeuzffmbpzfiiwrfdaekcrevffoxpsauhzziuyyjodsisaadbnyuugaxadvyxhfhbwhbmsgaklslihzwgvprpcawdtrniispfdnjoxkatlwebopgdqnaemwsflgfcuhkdnofblftofqzsphykpirzuckdnuxarzakvwurtrtbprdryikxmzytqhdmoyuizplphpvoliisgzhiganghwvqhzdmijccnfqqvboifovqxqvziktibyzpbffaguffgaqpbujvrvxecmaqygoyyptgzmwlnrwbeyuiiapgazdrgyrobtkcmsoheumgzjjpztatlpjkckxjqgfwvlhojdwztjgjdfdvalsglxjggmlfrbvtfeyhdbkggzukvdjnjtoytyrvxgrlvbqkkgrixhmjvwmojeiugbcyetihdtsizatgeukaczqllddwfqtwzsdquxmjmsypnftypppdsrqmkrfwxpwasrbtbeaaflqiatngmxylmhzwfoczsvcvwkgmxvhzyaoxrbblpqhbcozesrkjqncpyjukhppbubshyhwclceaefzhlbncxwdgglbtmzlksugrgnwjghgscqxfydztoraxrnthpqfojlgnablbxsovkcvpboujoczpihxrdblfirvlpxzgjhgiueyinhzasfelqnwmyhwwiaahrwoivetpfyyeeponmaqofwcbpvagruzshxaugnfzpaognklwcmfjmojrmjgmhroomwinouwdosuwtbrpkrzqtjfyspdnzgtbybsjyuohmchoukdyjfgovroyigpxqavcpmwnccdouskjxpqmpkjzkmcouwmauimkpatyfgkerqazsjuhctrbmqvqfdjfogajgrjnskzmrwnfjjfszebtbsioumdvhvqzgdkkhmsciutobqaefncvepnwqhvrfajmmrqnjryniwrckbaampnegmzoiqibwszbrqcpfgvtnlzemcmzaooywydmonegybzpdtukduxedpyquadxslnvirvewqihhnarvkpsbhmoggmoypwbimrnkuuiztvdqltnrytvvzlvhovoaekomlkqacgvlhdjaxhusehccgzjljjxjdpzpfnsrfnrxbzhoopziyrcmtpuvaqpvrevjjvmucezpecyckcmyvgnzgvitbkkdoptciamgkovowlhfcjmraynfyvlepowelkcjmjnibcsnabchcesnrwiplkavzgvdjyhulhthtbjgeckloshfcgobqovmxpryfbaaxfemkkpmtllovhqncrsbgbhjaozoycdnbcilhhlyfxbzvcmumpspgjszohxqhdocwnoxatmtnkqkpvupobukdudumdpsspzjxrcxstvajlarmicnsnjgdyyxcliqftvftmjmztbktbfmddbqtrfrygqzzzplqgemtvgijkydpshxiajzgcpmxsuamhtucpnejafrjqiwdxxflmaeyhntqfftvmsovtzunqszbvmvjhxcemtorseiariixtbnmxhkcwrghzposhvfnorlcwipsolpmkmvfpwdjswietamqfggxhpwfnsbkooocopbjzzxuhqxbtkklsxmmsgqvxldrfutlgntrewlyksrxdfexgkburyxbuqzhjmvqsqdzwppzyoqibdbhavyhexuybqhstktgtvtrckzqezauehcoxlnntilnkqekvdachlmvuxcowizzbqrldzaggpbvvlfwhsqfdyqvwqwrbkqvrqpzdihtnnafxbxqulzfswevlvxsjugrsaombysnngstmnlyayizrynmofiwbggehbfugsufhmyogsctxkfzlwwwshxnvoaqvstgpjtvyczlgoueutienayowbzwuhzearmhhbukmebpyewdrlmflwbvrzfhrkixvgewburjiqfovxrkiwvvbdrswvbcsznriinohlfeukcxmgmoyrlpqzjtgpjpvsnzbriifdyljkbqqiketrpvmvimmxhmpxlfzqluenskwrtshagizqrxigmmynfppfxfzxcvwbogamdxfipiqdasphwixefwvgrihkqjcflqvqqfvzxdtqyvvnnfzeucqhwlmxjconjuqkachpnysbnhrcfadculwgxcruihnuixxuvdmztugpvesdddargavwiudrtybxwmvywqleepplrchioqkyomusvamfawlxcwdkdjnydcmgfrlmkxpvqhcuioilsahnzrvlxnrfyxmjxvtlliyilcjtcwwcuucgurbbcshnlzrzilgkhdojcivhgltssezykltyzcubevrbapzmnhfhtntgnmjytjubvasdfiagwlzzwohzaibzqwqdlsikaodfljcgnhyckowudmfbqimtuszqgyxxzvipniipgsotrpkzamiwpkngnvwmjjivjtxhpzlmrwcjznavijhjjmvhxkjdahleprpynjnqltqhyamkfdfspbridpbuphtqxkncpognjgxwwyzxnkizrzvobpdxepncwvuhdspajmooiybeksqkhpncluiwwgsapihnkvgmwektybpzlnizkhtxtgeqqgaphditecptyoquueofaleodgsvfjxhokmzgjwflceebrbbjkxvqvkymjatpvdcvatnvkecfpxrpvwgnusmuetshyeyphgzjwktlwycqjqmsfjtiqkkbhndslyfxdegaejuzfylnbqlvacephpbuytqmxvwosukulbwdoofqomqlgdptocqlnjkikcvwcvyrpubzoeegonjhdtuibklelcgtacvovyntmucnzknumratvvwcphkfcxzjfmzwbqluzpexancupokekqnykxmwnyxvclvvxstnbbylaqknrgfegxfgkrnipkrstthxkkyborfgciqgksruwjzxwfuztgizrjrilmshcmnfzxwucrsscgotmniegribamhyzwjwyeuminjukrurpspcjmfllgceyuivmqfgegjjjpbswhjijrlajtbtevijdyanduyhbtedmihjaadtwbnjgrhlxgbvxxmtqzinsclkctlvhocntuppgfeaubksbwxouqsmdeaijulvlpawxuuvadmroswmaceodnqnxaxnwxwsoogqctfkadzabezoeufgskhtxgeefigmjcwrsoymyardzujtpejrsjslnorwixaawuqkhtgtqgrbjrzoxdpgetayqwsvptbwoljgypbkaxjcfujykdtikngwvnmwlpefdecpkywsbkoqjuyiaaizknmygqiqdjhfxfzpsdnlzqosmcdgacngjdrmhhnmltesihrwsfrfjvhctfjinwolonpeuibvxhhunarulabdrrwpipkczhxaxrqxvydmuerawuoshzupvvhfhlvbdahibhygftjmfqostlufujpwrfduppuhidftnjegdoqjyfekysuglomymoybrcypfkabcgiddimrpahbmtjwropodagfdfrpffqqgffriqcmvqsbnrjqwkqrpappefsabbjkotyspncbzjdlqjobgzkxzebhuliwikfvhfroqotbwsyywapztlwnnumngdwuinqefmgmndpvfsmmzrozkzplzmgjojgkzwkfwgljxrvvfuvozeihsiwqvksibqdkbsqslxwydowhsekwuslrppizukfcvvfxuffrnnceoriukxnqoujatnhqvgjaertcqcdfccsttyirwzxytgflyoedmkhzufythspclmyrwzxlvvhhqohxdppsvzoqgcvclykgadmtkwxfnzpcoziukoajwjjaiufyzormcokrwbdpnhcotdmvyihscatzmotgqoqthdcdegnxxsxdqgtbdirmvujyvssdvpztvhzaklkqvvhkpqmqyrwbfwcygnvbjjvrfmccrmjmspvqmxadbpipprbcurcjcjyjjbnzbjdnpgobvckrdcbjiphtgmavthjedrkulplgedfiavvdupwfugxvrowmuipujzqdkzebvfgzqxxznnbdfjmfrrgjwpqkudgscpotdhtguvgyymhhwkrctnvuphhjnrwcqzwargqxxpsdvsvfynlhxrzekjfgtdmcaspmtmzdaojduyhqieipeetptyfuhrynsszfnxcgtvnfahfgkjfbxmgnuhxtifzhgtlmjlgayybpshyzixkvocjlorxlpvsjqgssxlwmxwpmwouocgylxbmyfrezwpubyewxsnqalzgetnpdfwrgxsawaargjclnfxoucwljnuqaiokxgixwogrmfhegurpyzitefhejtqawnmglkhlhxoxblmgdhzkavxnqhoeagcrbbqlssotgphffqtcgkupzvlkmljmjomnqxgcmiyysmkvziridmuijdrzozgzxsuiudhjzuxxjoatipfcpjsqqckmvcgsjdaoecooposrptdwtrdwvfltbtczbnyqhvdrkphccwyyponubffazdikxuifbxnqmoubdtqbpxrpsfyoevuwgmwlnvgblxlvshhdavmdhbmurkmlhsiepzyiqoaiugfdzwkpmtjozzpqfrxpafkiebadrglatgpoiargnyofrhsdrpfgdipxnlsxopmbhxupantpxyasrvqziefcarckihgxkbfszzgtjpoazjuuuxxccegqhjtsjqdhgshczrznrbyjrraxeyzdgciyvaeapkwgvkejkrckdsbyekoukliqozslwgghnjrfbzpqkrfwjawoutztlnasoecujozksrefzdduhnvnskvziighbejokbqyrdespapyqbidgkzwlfvapyjcxcoybgwxweivmzblrdyumcxcnddqgvlthtfjwmefwzkzvnycnfduawgvsqmullejnpapzeujmmwkbmtalkrpunhjlargfhxpjphesgxdvldteileyzxpftdikjyyqgldfwrzglixzuegwslfyhrqjceeeggllgbvfeaefztngfpjncbjeyfmyvcmdashzponstxigskortcevevfpqcbwzmqrbvbniwjwajbdhdfqlyujnwiuveihahtbakokmzkpznqqrqdbbivaettleiciafubnklnowubzzhvzhyhkfhzvvcsajxkqnruuyoaxmrahzmqnuedlmjyiioucsaxvhspmrmglcmpoxvqzwssgxgptdcclstkjxwwaqekdwkixnowusxbnftnzjectfsckbeeevhytludfcdzwdiujywcsgwrvmbecqwibvusgqhhvmztiavlsmvlwztgburxaaotbcslvxnffaohthhwhaatkyvaptdxwoztfcqovimzbpsbxwuwbjwkbdvrkuytovzsvcmkporgabibniqiiobhljsbgeqsdbofcdpuxgdiqlmpwpadfuymdmauguvvewtnrkbkgfogitcidofpaduxeetslyqppgsquivqvvmfmdpyfvmqfliuhkasezljpmlagfgqcqahtfojamfwjmptsuvgbslskjmvqhmdlhouymghfngfysjiqkjfcwbjjtorzpjblzuabghntwyxrcqrrtviijbcknzjolpatwpssnzmobrpxwyaubjgakgdzydkkvsisnfscwklbmkdhrzbopcdmimqleofwvfugtbtogbdmazqjmlslpfeukuqcpmpwggseebnoqpadfpudcnriiwlhojpzpbbqdgqoweijlyjplkxxpxawanihmdkxmmdsdlknwcxrbsmrpsxawxxoepzckcilssqxntruzwmtqqjrxsupdaedboovfkecckmdxtymhagyoweznpgtwxkpbnoqfkrnzvsxpdlgynleqcpyrodfqngjgmkweiotmvpmbujluktefwwhhprfqtusjzebtnhyztjhbhlnmfzdrcsxktxbzqsoczgwoydpcssgksstfeslmesjkdbwhlorqtswfcfsxkysbedidqzsxorpgnhgieonzdzlpyqxjkkncypuhjtgwzxvrqmpleelcampexgswcdtezuqdghfzzxkzzyulqpfojwsdgcdniblomxrxflbnylwqxtifxxfkembyxhkvhfjnmpdinrpodvticucowipekvthfobnkdvgfhoobhhtwdtppcogtwqyynixndujqclzrvwfirjqsmvfjxbhisdaugeaswspcljkdigdqcekcftqcemsjlxhplmrxootbcsjylvkvwtvvnusaxtkxcjrxazsjeheguoxrebicpecuuorpwzsgpfgztgtfpilvauzikosbtzbhrwafktgltkteknizcioxefizyvwfgyfwhbgkssmvobxrzvqfkdhcvezdmyvqqedjvspyvsgwqwovdxrecdanapoydetgehibxaslvllrqkxdzhsebmrdflqxylvgfaaghcstzrlutizgxkgfjzatylehdqcctkhqahctbyazuibdkvvgyyoqlmiocgkripiofrbmjvkavkebaelrhrizmzbskptanrhwzcpzrtofjxzkrushctxejlaziteklpjakzskzklmdgukiabxxduslretgbomoexppmgimlfhfehoswtixefjffecudfmacfvlguvvbzcbtgywrxbwifkrxlhoqvtslpwhbcanoaynjonlyiobcwstxshesdowbviqdejatogcfbllmnctasbeininbnwmtpdhmuvurvtpnkqpscvwtlzhtlpvoztdqbncxxmqymjojjnllivocansiodawzlcygkejjgisvzvvdlmacnffffhxyodgtmmlevrjhnplezrfidsuygsariqdqbyvntpqnurmtrxtentgnopsipnayoxipkvysbunxqjisyjevmjvgxoqruhxvsqedcsimagxmsbjslwohsckiivuhbjnegobkpxjdoqfnicgunugidyfngasefvcbwltaljvxamhnuefkvhgbwyozaggdszyqghnnfmcyjfvhfcamcxjrggysglomdptedlthpfxmmbqbfzlzgodcsahagnuepupqbrfxjgqldwbuenabygoeduhwgtxnfmzlsojbvxmmavdbmxivmdozdratbytpyjysrzpejdggqguhyeshcobbfodtuqnwwundapkfkblfzdlnsbylsufiuycoejkljrcovadehyazpwqordifrsomfskmjzogqciiluldojkxfgtwrlbqjekbqotuhffowjptmjkitgolgsofzkvjasgzktoophkpnidqujvcdxofcfuwwwihpgitnsfsrgxxqzvzfjlabwqptlvsusszjajgxshshzncuhafxndwqcxujigvkymfztczglcuwbzhgomvqxkdmxilzewacpnffzlkxezzpxbfvlfosxkvmdopnuwoqkbjrogfecxpzcqvyzeuadikskcwpgyknryrgcumvspxtgzzdsoebizpsehtpqfmtgnwjhrcoqthrjxjugvzyhvoglnerbyffgastsyoizzzrmmeawztdizcebilasdsthmujjvmjsssvhwlyglddnljtigltporpjaiokkoeuqreawmpbvbnjiuvhdslieeanfazyxubwacizffpahfndinebzcqdrnqnbrwdddcorvatawhqeacjtfikkastvtluavsyixwldxuifyxpmgtxqpdcpyggdiztwihzsvhtotqgtscvmwtpsakuuyuastebtsivnoemlzhdllyvyifirqcvxfapegnfyaxepsvvqhdrztwzgbbtbslrtifugxhrsiidptyafyaxbtbrpxlsvwmxvcmrpgatnpnqoghnjqqxwtfpsicpwrtwtqrxgxrdzlkamgspznezzlezvaftrvbvjatefhsrtrqusxnrxrahdckrsdgyzbtflaaelpkwfddzgapzlktcrizyawqeazasrtkcsryowkcjvmsbkvhkmdxrudjjpczpzfxtjbmgpvwhchvtlctrhdqqjijrnkunalsucruwhhfrrdsjztcrkivvrlszopymvuxnnlqklatzgcjjuxmhrmydtcyxhisvxepljzwjuhinuxvsmkdtmrrojutimnivlxxcjvgbpclzuxcppfopckrvndccoelzzmzcdyqrkuxdwompgshazcuzxwytnjeejmpwpabiuaorkhctezqydizuuontnukrkvithhctnmwwivqbabuvqwvjyxpwsgpsoyszfsnjeeofmqoyxyakfcmwrwkisglzadmtcolpwhrnpasmxbkozdfgtuchqhvdnfahlxbzqqxgfisdjrwwqsjihtcgflpnskznnfdzeotcrzylojcuvsabyngjoetcptkdbihowprmxokppjfjvxsztypzkzgwuurqmlwdzapowwsaozebryypltamqzmduirxskstryqrdaagwerlbnwgteibjiktrladowyuhsuasqppzkqtsvpxzdcxyulrqgjzspppjqujffcwrtovaxaflttvrwdlojqdmmcvgeoiieifzkpzfusoozkunhnxaafpnrnhsfraglsbylzbigxjxqbjgxfbtevzeuqrewyjywvmedtwajobluxrsdlvaghovxhfcieuudwrhffehgfuwkqvgpofqijpklraclahqmewxggvrboqxveabgovkfylybrbrxqnvljafuooyscossddrmuosmcxbthyynfuhytjyhkkvwiaqpicrfjxvwftatdwhwuxxqessofyecjfzbzhpqblvqooasrwnlaqrlzindcvxzunpizmgpgsmnangmfargzqcvclgphenedrlpkfnhcccwzkhsgswrtqnqidkaleqitfqyikkjkcjokeelqfldfbzmtmvcyfzbpcgbsjfedviylpheoilpddgtohbywwuuqdcvihhcqtkrmntgfkeytjeytnfjzxongdmvahbyubjbsdkrejpiyezopkkvrfeiwycizgexqcnrpbqwrksztcjqlrhbyhenwqlxxjkicoajfphdxjpndnkfsjrfoqsmntuenabiufbjkyhemewernlberrrlivflnehtterrvdgnrlaosrljjggogsyxpguzinyohitcbcaqebmogtkamdzhgtiufxeshimfdrcmfqrqtcbapddsmerewofiqrwprfcgdmwzuddpavlnohnybgibsnxtsnzilexfehiphpbpqghnocawhmkzakmotjkeuuilvkagummlcclpuwxbyeoubwqtqsokrnxgotuajewgabdyzzyglufirtdfebmvafinbboecugtxacqdxwmbxyhcksiygftwubfrnxlivjiofvjctzygkjqsnjlhhmoshoxpbrkhbqkztkhjcmeqxgpzxymlfwozldnpllxboixvivoquplfrvtwxljpbmyjlbvbgujcczqhjwdvqtgatnvcuwmncazwmsykjsgjpvhkgusfzyctzmigqowhmdicguijmatupdjzxqbunxbeqardupokgfbtgnkwmajdacajwzsuvpiwjmtzyimluenlcrybwpvtuztfxfsvgrgndhljizthoceovimkxsxyneohxnbzbkmnxlidsczqkknlhrbqdlhxsfypqviucqiywljmiqlzlaofolpmtkvhzgvscoowmzlkehvfidefmcfeqssjquavrehjhugjoeeuqrrskpnsituzqjxoydxxssszyzgmczdtdahjisrjjgdwlnjglrqrzrnyudairljibcutnfcojuyjzuhazszfepncfyvoxgnbiyyixzspcnlhclxddafebdukcvdkblnqmzqkonheehbszjkhhyvecpizqjdnwraosmbfntitxhocadbbniuqzxfuyjqfrpvocwnrziazjitmtxxvkewhfdcewxqfovliuxzilicokdjmuncxipcixlipdcmuwukshsotjjcabvewtorjckmmqtknmdrsvvgqzilbwnxuzlogloepyrsaiqyoxwjwmxbnwckvbiesvybwdqvjnywbwthuadbgeieblmdboggqwxugtiporxgkbroidfuykuypwyavecwgfskshqogbvajbthoemgryusercuxztwgtbzofcsiduoavtafszohwwchuqjpjbbrbxsudotmxprrtavzddwxijonauwgscsvtrjwomoqchhwxtohwatxghcvsbaqxsntzsluhxsmrajjefralceyhhympznjuzmwqummdxuwwqzwdffrgkjggfnjnyebxegzzbujfyeivmlwwgwrglrooznuhlfvguwezrqwnekgnahtocwbjamdtrtowwyyohusnsmznehzpieuayritybnlrldihnbdsbsbdqtpdpqyjkdrcecfwlljsljpdgifxetsajmymzcdlefllhtcotecgnbtputyabsmeigdjxwxywoyvyimdleebaadxpsfeadudsvebxbjrnotvqldkxutesdeimkhwpdbbyvhxsgjalsoosgpikstmbapoffdkljthvhlagsjtnglpuomrvejsdvfcxlgwhitnekotzcmagrjnvqdumqohzpshypkcijkgwozgyxvdozkaasbuohhkzaabuhllmnvtxtwqooxzkkcfaveprjtvklmaoxtftwzkdbpvvuezwbgohnzcomtjsudbbdpowrrtvqixxfellzkloxbrxdroctzwywujgzzptupqmfpstlpiowfnmdgvgkciyzlvskiinwoxsxvbgyprttxjgasztpuvjvwztcnutyxplebjnsgipbarhlcnwjkaspbohchtiurjfykknkslygfkomhqnaiocohyccfguufzebncmchjsapecxdbkouugsmtnipfmdxdamfhfoxcdoqjnjnzfpqsgdirbcaszqchlqhxupypvepxgxecyrwpkaziqndjjkjrpqjowpspvbizerthixqznivlbaflzhtujtkmqgcjdkpnjdrxktphtwfbwpcwcavxaxdrojjteqsajvizogsvgcctyinjqzsjplfkjajuxaprouznlyepxtvfswdsglgbaclhnpoiwkfqrggbmlmpdavzubxdcifoxaokwfwonulygizsuvxqxnomczdjcrcgxfduosvazmwzbzlhcuvxywlzguxjjkkvyutqwwlvtgxljaiercxbzmlwgudfdhseusqifvoxksxpbxublditsfyiflzcvzfdfpdeibmoekyjpddexbfnudsusdxbthmtfxhrgwtiirccxhbizvqffcwghjuusqfcbynbfewdsskwexmpvtrilyqsgraromzsuhbqnjldrpchnclecocjykihgzlwynfcrnhigbfxkrwblbphkdjttqjihergujyickvhoaomtnkmsjpzkyvzljexphylviqyhnbuxrgqdirpcfbevfjtmmodmarupbgicdefifsprpfqszlgjpnhzowtorkanvprqqtjiausxyhtjmtiiwfidmasztdcpynqacphntdtmfpdvpjtaekaggbevqmyxymtiokdspbzgnxubwikzaapehiabcktjhwkgjzhzldgrxjsfyuwgmghenfrtzsdauuaodxqvvyerjuebapknrmhwjhbrojwzcodwhpdbgaeninbtyrhzqxsfpdwzrvfnbruccjfqfupcdsiqjlnrfjasrkhznssbintubxchjhhiqahjtkfawlfyonocudtjpgkgjlyrrkohgrzzvqcwfwgprofintyzpwiregtwyxjywrvrusvnsqyvciubsqaotawxlmromuszooghkkfxwjpsdmvxdkjukxjwjdksmrxpkkpxtpbwbfisfqneuohxhbinrbxmaklfdjzhhpzrfnzrkpegzqnjmdlngvthppmovnrbclgpmiqnzzcwxgstfbrtmealyfigyxfnogdxpoxonzlrzjrvoplzkaimklngxqvhkiijuecthgeqrtxfsajsimbwyknlohabegrkrrwfytemczxogrdrrgibzankeqfddiufslellsggwthsvwkentzyrfppyacqczprryimfnhzowoxtrlpvmtfkstlbcgicqsnkgpysifmykfdzreydxneuydzjhqpmbwrbxgefmsojgbhuxkdfhpfxzvdbpfgdhekmnmaokhssvbsdbgqisfcpwsfzsvojfjsrqhuuduwifaywnthkiuhsrgnkrvuknmilvrowfwsqohmrusibdhuhcjvzjlvrufbtypotgjqagipmmhlcmliieerwhuizsvjnxtubqwabqaifcvsrzlklwxbgwfmxrmimdgxjnlaxtctdrerfpxvifkwbxuskrybktiyeambeebcptsvvmsmhgdxollkhlomdzlyjvyvvnrbaddfrujpvzngaisvfluqjscncriugpimqlcinkebcrtczhiyyirdanhddlusnoezbziuwphjeejhfivvznkemfbtcoiyahtljlynrwzearpvekmzhlguwvmgmmbwzadorelfxidnoiwiehpzgzefmppajnmttvdyemgzwfodtlpirdsmnzkitryomcyfukylxoinaornrtmdisoiuddnzwqitqzwhjecrmyhoretzgxciqngpsxcfgfzyneoxresrogmeebiqrcnpyehfriprzueajqfnrczmullahnexfebqaqfnzzkysvbagwemvxttmwvrvflcfjenjoizhuubutzmsxogboepyyezibsqbmgkwkwrcjyqhikbfpiqsmrjmqriwppdbijldaqzxpuiawhxkaujicxchftemfyfmscxhbxweswtjgtlmtkhpyvpybrkmtgtqvtocnqvaxpkjwkedgvvgsjiftgdqdbukackiefopjqpnhzezgrgrzpyvttugsedhmjcmrvnkeofqqignddniiazspgwgfbxolzwwklvairwvqchjxybwfjugmyflkkuuulqzgqkgsuymvrlemwrblieexszuzkygujowopflsaadzidkrqgsnmntbipofuwrahnypixrpzp\" 1626006833639000000",
//      "measure,t1=3 c1=8",
//      "measure,t2=3 c1=8u8"
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, smlProcess_telnet_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists telnet_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use telnet_db");
  taos_free_result(pRes);

  const char *sql[] = {
     "sys.if.bytes.out  1479496100 1.3E0 host=web01 interface=eth0",
     "sys.if.bytes.out  1479496101 1.3E1 interface=eth0    host=web01   ",
     "sys.if.bytes.out  1479496102 1.3E3 network=tcp",
     " sys.procs.running   1479496100 42 host=web01   "
  };

  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  // case 1
  pRes = taos_query(taos, "select * from t_8c30283b3c4131a071d1e16cf6d7094a");
  ASSERT_NE(pRes, nullptr);
  int fieldNum = taos_field_count(pRes);
  ASSERT_EQ(fieldNum, 2);

  TAOS_ROW row = taos_fetch_row(pRes);
  int64_t ts = *(int64_t*)row[0];
  double c1 = *(double*)row[1];
  ASSERT_EQ(ts, 1479496100000);
  ASSERT_EQ(c1, 42);

  int rowNum = taos_affected_rows(pRes);
  ASSERT_EQ(rowNum, 1);
  taos_free_result(pRes);

  // case 2
  pRes = taos_query(taos, "show tables");
  ASSERT_NE(pRes, nullptr);

  row = taos_fetch_row(pRes);
  rowNum = taos_affected_rows(pRes);
  ASSERT_EQ(rowNum, 3);
  taos_free_result(pRes);
}

TEST(testCase, smlProcess_json1_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists json_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use json_db");
  taos_free_result(pRes);

  const char *sql[] = {
     "[\n"
     "    {\n"
     "        \"metric\": \"sys.cpu.nice\",\n"
     "        \"timestamp\": 0,\n"
     "        \"value\": 18,\n"
     "        \"tags\": {\n"
     "           \"host\": \"web01\",\n"
     "           \"id\": \"t1\",\n"
     "           \"dc\": \"lga\"\n"
     "        }\n"
     "    },\n"
     "    {\n"
     "        \"metric\": \"sys.cpu.nice\",\n"
     "        \"timestamp\": 1346846400,\n"
     "        \"value\": 9,\n"
     "        \"tags\": {\n"
     "           \"host\": \"web02\",\n"
     "           \"dc\": \"lga\"\n"
     "        }\n"
     "    }\n"
     "]"};
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);

  // case 1
  pRes = taos_query(taos, "select * from t1");
  ASSERT_NE(pRes, nullptr);
  int fieldNum = taos_field_count(pRes);
  ASSERT_EQ(fieldNum, 2);

  TAOS_ROW row = taos_fetch_row(pRes);
  int64_t ts = *(int64_t*)row[0];
  double c1 = *(double*)row[1];
  ASSERT_EQ(ts, 1346846400000);
  ASSERT_EQ(c1, 18);

  int rowNum = taos_affected_rows(pRes);
  ASSERT_EQ(rowNum, 1);
  taos_free_result(pRes);

  // case 2
  pRes = taos_query(taos, "show tables");
  ASSERT_NE(pRes, nullptr);

  row = taos_fetch_row(pRes);
  rowNum = taos_affected_rows(pRes);
  ASSERT_EQ(rowNum, 2);
  taos_free_result(pRes);
}

TEST(testCase, smlProcess_json2_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
     "{\n"
     "    \"metric\": \"meter_current0\",\n"
     "    \"timestamp\": {\n"
     "        \"value\"  : 1346846400,\n"
     "        \"type\"   : \"s\"\n"
     "    },\n"
     "    \"value\": {\n"
     "         \"value\" : 10.3,\n"
     "         \"type\"  : \"i64\"\n"
     "    },\n"
     "    \"tags\": {\n"
     "       \"groupid\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"bigint\"\n"
     "       },\n"
     "       \"location\": { \n"
     "           \"value\" : \"北京\",\n"
     "           \"type\"  : \"binary\"\n"
     "       },\n"
     "       \"id\": \"d1001\"\n"
     "    }\n"
     "}"};
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, smlProcess_json3_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] ={
     "{\n"
     "    \"metric\": \"meter_current1\",\n"
     "    \"timestamp\": {\n"
     "        \"value\"  : 1346846400,\n"
     "        \"type\"   : \"s\"\n"
     "    },\n"
     "    \"value\": {\n"
     "         \"value\" : 10.3,\n"
     "         \"type\"  : \"i64\"\n"
     "    },\n"
     "    \"tags\": {\n"
     "       \"t1\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"bigint\"\n"
     "       },\n"
     "       \"t2\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"int\"\n"
     "       },\n"
     "       \"t3\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"i16\"\n"
     "       },\n"
     "       \"t4\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"i8\"\n"
     "       },\n"
     "       \"t5\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"f32\"\n"
     "       },\n"
     "       \"t6\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"double\"\n"
     "       },\n"
     "       \"t7\": { \n"
     "           \"value\" : \"8323\",\n"
     "           \"type\"  : \"binary\"\n"
     "       },\n"
     "       \"t8\": { \n"
     "           \"value\" : \"北京\",\n"
     "           \"type\"  : \"nchar\"\n"
     "       },\n"
     "       \"t9\": { \n"
     "           \"value\" : true,\n"
     "           \"type\"  : \"bool\"\n"
     "       },\n"
     "       \"id\": \"d1001\"\n"
     "    }\n"
     "}"};
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, smlProcess_json4_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {"{\n"
     "    \"metric\": \"meter_current2\",\n"
     "    \"timestamp\": {\n"
     "        \"value\"  : 1346846500000,\n"
     "        \"type\"   : \"ms\"\n"
     "    },\n"
     "    \"value\": \"ni\",\n"
     "    \"tags\": {\n"
     "       \"t1\": { \n"
     "           \"value\" : 20,\n"
     "           \"type\"  : \"i64\"\n"
     "       },\n"
     "       \"t2\": { \n"
     "           \"value\" : 25,\n"
     "           \"type\"  : \"i32\"\n"
     "       },\n"
     "       \"t3\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"smallint\"\n"
     "       },\n"
     "       \"t4\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"tinyint\"\n"
     "       },\n"
     "       \"t5\": { \n"
     "           \"value\" : 2,\n"
     "           \"type\"  : \"float\"\n"
     "       },\n"
     "       \"t6\": { \n"
     "           \"value\" : 0.2,\n"
     "           \"type\"  : \"f64\"\n"
     "       },\n"
     "       \"t7\": \"nsj\",\n"
     "       \"t8\": { \n"
     "           \"value\" : \"北京\",\n"
     "           \"type\"  : \"nchar\"\n"
     "       },\n"
     "       \"t9\": false,\n"
     "       \"id\": \"d1001\"\n"
     "    }\n"
     "}"};
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, smlParseTelnetLine_error_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT);
  ASSERT_NE(request, nullptr);

  STscObj* pTscObj = acquireTscObj(*(int64_t*)taos);
  SSmlHandle *info = smlBuildSmlInfo(pTscObj, request, TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  int32_t ret = 0;
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
    ret = smlParseTelnetLine(info, (void*)sql[i]);
    ASSERT_NE(ret, 0);
  }

  destroyRequest(request);
  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_type_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[] = {
      "sys.procs.running 1479496104000 42 host=web01",
      "sys.procs.running 1479496104000 42u8 host=web01",
      "appywjnuct 1626006833641 True id=\"appywjnuct_40601_49808_1\" t0=t t1=127i8 id=\"appywjnuct_40601_49808_2\" t2=32767i16 t3=2147483647i32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7=\"binaryTagValue\" t8=L\"ncharTagValue\""
//      "meters 1601481600000 -863431872.000000f32 t0=-418706150i64 t1=844637295i64 t2=482576837i64 t3=736261541i64 t4=L\"5S6jypOYDYkALfeXCf2gbUEio7iTM9vFOrMcGqYae0yNeDAEIrKHacOo0U7JTrev\"",
//      "meters 1601481600010 742480256.000000f32 t0=-418706150i64 t1=844637295i64 t2=482576837i64 t3=736261541i64 t4=L\"5S6jypOYDYkALfeXCf2gbUEio7iTM9vFOrMcGqYae0yNeDAEIrKHacOo0U7JTrev\"",
//      "meters 1601481600020 -163715920.000000f32 t0=-418706150i64 t1=844637295i64 t2=482576837i64 t3=736261541i64 t4=L\"5S6jypOYDYkALfeXCf2gbUEio7iTM9vFOrMcGqYae0yNeDAEIrKHacOo0U7JTrev\"",
//      "meters 1601481600030 63386372.000000f32 t0=-418706150i64 t1=844637295i64 t2=482576837i64 t3=736261541i64 t4=L\"5S6jypOYDYkALfeXCf2gbUEio7iTM9vFOrMcGqYae0yNeDAEIrKHacOo0U7JTrev\"",
//      "meters 1601481600040 -82687824.000000f32 t0=-418706150i64 t1=844637295i64 t2=482576837i64 t3=736261541i64 t4=L\"5S6jypOYDYkALfeXCf2gbUEio7iTM9vFOrMcGqYae0yNeDAEIrKHacOo0U7JTrev\"",
//      "meters 1601481600000 -683842112.000000f32 t0=354941102i64 t1=-228279853i64 t2=-78283134i64 t3=91718788i64 t4=L\"wQyjbkfama3csU7N9TPIVAzx3v5ZUoMg3bn3jq3tqSuHAqky8X8QnwbeQ64AjGEa\"",
//      "meters 1601481600010 362312416.000000f32 t0=354941102i64 t1=-228279853i64 t2=-78283134i64 t3=91718788i64 t4=L\"wQyjbkfama3csU7N9TPIVAzx3v5ZUoMg3bn3jq3tqSuHAqky8X8QnwbeQ64AjGEa\"",
//      "meters 1601481600020 178229296.000000f32 t0=354941102i64 t1=-228279853i64 t2=-78283134i64 t3=91718788i64 t4=L\"wQyjbkfama3csU7N9TPIVAzx3v5ZUoMg3bn3jq3tqSuHAqky8X8QnwbeQ64AjGEa\"",
//      "meters 1601481600030 977283136.000000f32 t0=354941102i64 t1=-228279853i64 t2=-78283134i64 t3=91718788i64 t4=L\"wQyjbkfama3csU7N9TPIVAzx3v5ZUoMg3bn3jq3tqSuHAqky8X8QnwbeQ64AjGEa\"",
//      "meters 1601481600040 -774479360.000000f32 t0=354941102i64 t1=-228279853i64 t2=-78283134i64 t3=91718788i64 t4=L\"wQyjbkfama3csU7N9TPIVAzx3v5ZUoMg3bn3jq3tqSuHAqky8X8QnwbeQ64AjGEa\"",
//      "meters 1601481600000 -863431872.000000f32 t0=-503950941i64 t1=-1008101453i64 t2=800907871i64 t3=688116272i64 t4=L\"5kb9hzKk1aOxqn5qnGCmryWaOYtkDPlx1ku8I5hy3UVi6OwikZvBlfzX4R7wwfUm\"",
//      "meters 1601481600010 742480256.000000f32 t0=-503950941i64 t1=-1008101453i64 t2=800907871i64 t3=688116272i64 t4=L\"5kb9hzKk1aOxqn5qnGCmryWaOYtkDPlx1ku8I5hy3UVi6OwikZvBlfzX4R7wwfUm\"",
//      "meters 1601481600020 -163715920.000000f32 t0=-503950941i64 t1=-1008101453i64 t2=800907871i64 t3=688116272i64 t4=L\"5kb9hzKk1aOxqn5qnGCmryWaOYtkDPlx1ku8I5hy3UVi6OwikZvBlfzX4R7wwfUm\"",
//      "meters 1601481600030 63386372.000000f32 t0=-503950941i64 t1=-1008101453i64 t2=800907871i64 t3=688116272i64 t4=L\"5kb9hzKk1aOxqn5qnGCmryWaOYtkDPlx1ku8I5hy3UVi6OwikZvBlfzX4R7wwfUm\"",
//      "meters 1601481600040 -82687824.000000f32 t0=-503950941i64 t1=-1008101453i64 t2=800907871i64 t3=688116272i64 t4=L\"5kb9hzKk1aOxqn5qnGCmryWaOYtkDPlx1ku8I5hy3UVi6OwikZvBlfzX4R7wwfUm\"",
//      "meters 1601481600000 -863431872.000000f32 t0=28805371i64 t1=-231884121i64 t2=940124207i64 t3=176395723i64 t4=L\"7pkY8763Ir0QeugozDbqk6NHbvRpx2drfndch74No3sqmyCJZCZaxAFwVmLgcMvh\"",
//      "meters 1601481600010 742480256.000000f32 t0=28805371i64 t1=-231884121i64 t2=940124207i64 t3=176395723i64 t4=L\"7pkY8763Ir0QeugozDbqk6NHbvRpx2drfndch74No3sqmyCJZCZaxAFwVmLgcMvh\"",
//      "meters 1601481600020 -163715920.000000f32 t0=28805371i64 t1=-231884121i64 t2=940124207i64 t3=176395723i64 t4=L\"7pkY8763Ir0QeugozDbqk6NHbvRpx2drfndch74No3sqmyCJZCZaxAFwVmLgcMvh\"",
//      "meters 1601481600030 63386372.000000f32 t0=28805371i64 t1=-231884121i64 t2=940124207i64 t3=176395723i64 t4=L\"7pkY8763Ir0QeugozDbqk6NHbvRpx2drfndch74No3sqmyCJZCZaxAFwVmLgcMvh\"",
//      "meters 1601481600040 -82687824.000000f32 t0=28805371i64 t1=-231884121i64 t2=940124207i64 t3=176395723i64 t4=L\"7pkY8763Ir0QeugozDbqk6NHbvRpx2drfndch74No3sqmyCJZCZaxAFwVmLgcMvh\"",
//      "meters 1601481600000 -863431872.000000f32 t0=-208520225i64 t1=-254703350i64 t2=-1059776552i64 t3=1056267931i64 t4=L\"1zWxWvHNZYailPvb4XxafeA6QvrUrKUf8ECU1axNWvV9ae851s34wqZcMeU2ME7J\"",
//      "meters 1601481600010 742480256.000000f32 t0=-208520225i64 t1=-254703350i64 t2=-1059776552i64 t3=1056267931i64 t4=L\"1zWxWvHNZYailPvb4XxafeA6QvrUrKUf8ECU1axNWvV9ae851s34wqZcMeU2ME7J\"",
//      "meters 1601481600020 -163715920.000000f32 t0=-208520225i64 t1=-254703350i64 t2=-1059776552i64 t3=1056267931i64 t4=L\"1zWxWvHNZYailPvb4XxafeA6QvrUrKUf8ECU1axNWvV9ae851s34wqZcMeU2ME7J\"",
//      "meters 1601481600030 63386372.000000f32 t0=-208520225i64 t1=-254703350i64 t2=-1059776552i64 t3=1056267931i64 t4=L\"1zWxWvHNZYailPvb4XxafeA6QvrUrKUf8ECU1axNWvV9ae851s34wqZcMeU2ME7J\"",
//      "meters 1601481600040 -82687824.000000f32 t0=-208520225i64 t1=-254703350i64 t2=-1059776552i64 t3=1056267931i64 t4=L\"1zWxWvHNZYailPvb4XxafeA6QvrUrKUf8ECU1axNWvV9ae851s34wqZcMeU2ME7J\""
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_TELNET_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(taos_errno(pRes), 0);
  taos_free_result(pRes);

}

TEST(testCase, smlParseTelnetLine_json_error_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  SRequestObj *request = (SRequestObj *)createRequest(*(int64_t*)taos, TSDB_SQL_INSERT);
  ASSERT_NE(request, nullptr);

  STscObj* pTscObj = acquireTscObj(*(int64_t*)taos);
  SSmlHandle *info = smlBuildSmlInfo(pTscObj, request, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(info, nullptr);

  int32_t ret = 0;
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
  for(int i = 0; i < sizeof(sql)/sizeof(sql[0]); i++){
    ret = smlParseTelnetLine(info, (void*)sql[i]);
    ASSERT_NE(ret, 0);
  }

  destroyRequest(request);
  smlDestroyInfo(info);
}

TEST(testCase, smlParseTelnetLine_diff_json_type1_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

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
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, smlParseTelnetLine_diff_json_type2_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

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
  pRes = taos_schemaless_insert(taos, (char **)sql, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS);
  ASSERT_NE(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_TD15662_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES *pRes = taos_query(taos, "create database if not exists db_15662 precision 'ns' schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use db_15662");
  taos_free_result(pRes);

  const char *sql[] = {
      "hetrey c0=f,c1=127i8 1626006833639",
      "hetrey,t1=r c0=f,c1=127i8 1626006833640",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_TD15735_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  const char *sql[1] = {
      "{'metric': 'pekoiw', 'timestamp': {'value': 1626006833639000000, 'type': 'ns'}, 'value': {'value': False, 'type': 'bool'}, 'tags': {'t0': {'value': True, 'type': 'bool'}, 't1': {'value': 127, 'type': 'tinyint'}, 't2': {'value': 32767, 'type': 'smallint'}, 't3': {'value': 2147483647, 'type': 'int'}, 't4': {'value': 9223372036854775807, 'type': 'bigint'}, 't5': {'value': 11.12345027923584, 'type': 'float'}, 't6': {'value': 22.123456789, 'type': 'double'}, 't7': {'value': 'binaryTagValue', 'type': 'binary'}, 't8': {'value': 'ncharTagValue', 'type': 'nchar'}}}",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  ASSERT_NE(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_TD15742_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists TD15742 schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use TD15742");
  taos_free_result(pRes);

  const char *sql[] = {
      "test_ms,t0=t c0=f 1626006833641",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_params_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists param");
  taos_free_result(pRes);

  const char *sql[] = {
      "test_ms,t0=t c0=f 1626006833641",
  };
  TAOS_RES* res = taos_schemaless_insert(taos, (char**)sql, 1, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  ASSERT_EQ(taos_errno(res), TSDB_CODE_PAR_DB_NOT_SPECIFIED);
  taos_free_result(res);

  pRes = taos_query(taos, "use param");
  taos_free_result(res);

  res = taos_schemaless_insert(taos, (char**)sql, 1, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  ASSERT_EQ(taos_errno(res), TSDB_CODE_SML_INVALID_DB_CONF);
  taos_free_result(res);
}

TEST(testCase, sml_16384_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists d16384 schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      "qelhxo,id=pnnqhsa,t0=t,t1=127i8 c0=t,c1=127i8 1626006833639000000",
  };

  pRes = taos_query(taos, "use d16384");
  taos_free_result(pRes);

  TAOS_RES* res = taos_schemaless_insert(taos, (char**)sql, 1, TSDB_SML_LINE_PROTOCOL, 0);
  ASSERT_EQ(taos_errno(res), 0);
  taos_free_result(res);

  const char *sql1[] = {
      "qelhxo,id=pnnqhsa,t0=t,t1=127i8 c0=f,c1=127i8,c11=L\"ncharColValue\",c10=t 1626006833639000000",
  };
  TAOS_RES* res1 = taos_schemaless_insert(taos, (char**)sql1, 1, TSDB_SML_LINE_PROTOCOL, 0);
  ASSERT_EQ(taos_errno(res1), 0);
  taos_free_result(res1);
}

TEST(testCase, sml_oom_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists oom schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
      //"test_ms,t0=t c0=f 1626006833641",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pgxbrbga\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"gviggpmi\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"cexkarjn\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"rzwwuoxu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"xphrlkey\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"llsawebj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
      "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jwpkipff\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"euzzhcvu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jumhnsvw\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"fnetgdhj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"vrmmpgqe\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lnpfjapr\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"gvbhmsfr\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"kydxrxwc\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pfyarryq\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"uxptotap\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"prolhudh\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ttxaxnac\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"dfgvmjmz\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bloextkn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"dvjxwzsi\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"aigjomaf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"refbidtf\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vuanlfpz\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nbpajxkx\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ktzzauxh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"prcwdjct\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vmbhvjtp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"liuddtuz\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"pddsktow\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"algldlvl\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"mlmnjgdl\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"oiynpcog\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"wmynbagb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"asvyulrm\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ohaacrkp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ytyejhiq\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bbznuerb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"lpebcibw\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"xmqrbafv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"lnmwpdne\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"jpcsjqun\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"mmxqmavz\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"hhsbgaow\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"uwogyuud\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ytxpaxnk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"wouwdvtt\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"iitwikkh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"lgyzuyaq\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bdtiigxi\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"qpnsvdhw\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"pjxihgvu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ksxkfetn\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ocukufqs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"qzerxmpe\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"qwcfdyxs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jldrpmmd\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lucxlfzc\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"rcewrvya\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"dknvaphs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nxtxgzdr\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"mbvuugwz\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"uikakffu\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"mwmtqsma\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"bfcxrrpa\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ksajygdj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"vmhhszyv\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"urwjgvut\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jrvytcxy\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"evqkzygh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"zitdznhg\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"tpqekrxa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"yrrbgjtk\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bnphiuyq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"huknehjn\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"iudbxfke\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"fjmolwbn\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"gukzgcjs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"bjvdtlgq\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"phxnesxh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"qgpgckvc\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"yechqtfa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pbouxywy\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"kxtuojyo\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"txaniwlj\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"fixgufrj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"okzvalwq\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"iitawgbn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"gayvmird\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"dprkfjph\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"kmuccshq\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vkslsdsd\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"dukccdqk\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"leztxmqf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"kltixbwz\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"xqhkweef\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"idxsimvz\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vbruvcpk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"uxandqkd\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"dsiosysh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"kxuyanpp\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"wkrktags\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"yvizzpiv\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ddnefben\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"novmfmbc\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"fnusxsfu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ouerfjap\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"sigognkf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"slvzhede\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"bknerect\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"tmhcdfjb\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"hpnoanpp\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"okmhelnc\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"xcernjin\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jdmiismg\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"tmnqozrf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"zgwrftkx\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zyamlwwh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nuedqcro\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lpsvyqaa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"mneitsul\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vpleinwb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"njxuaedy\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"sdgxpqmu\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"yjirrebp\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ikqndzfj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ghnfdxhr\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"hrwczpvo\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nattumpb\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zoyfzazn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"rdwemofy\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"phkgsjeg\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pyhvvjrt\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zfslyton\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"bxwjzeri\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"uovzzgjv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"cfjmacvr\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"jefqgzqx\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"njrksxmr\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"mhvabvgn\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"kfekjltr\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lexfaaby\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"zbblsmwq\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"oqcombkx\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"rcdmhzyw\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"otksuean\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"itbdvowq\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"tswtmhex\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"xoukkzid\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"guangmpq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"rayxzuky\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lspwucrv\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pdprzzkf\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"sddqrtza\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"kabndgkx\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"aglnqqxs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"fiwpzmdr\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"hxctooen\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pckjpwyh\",t8=L\"ncharTagValue\" c0=false,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ivmvsbai\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"eljdclst\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"rwgdctie\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"zlnthxoz\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"ljtxelle\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"llfggdpy\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"tvnridze\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"hxjpgube\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zmldmquq\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"bggqwcoj\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"drksfofm\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"jcsixens\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"cdwnwhaf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nngpumuq\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"hylgooci\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"cozeyjys\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"lcgpfcsa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"qdtzhtyd\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"txpubynb\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"gbslzbtu\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"buihcpcl\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"ayqezaiq\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zgkgtilj\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"bcjopqif\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"mfzxiaqt\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"xmnlqxoj\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"reyiklyf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"xssuomhk\",t8=L\"ncharTagValue\" c0=False,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"liazkjll\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"nigjlblo\",t8=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vmojyznk\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"dotkbvrz\",t8=L\"ncharTagValue\" c0=f,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"kuwdyydw\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"slsfqydw\",t8=L\"ncharTagValue\" c0=t,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"zyironhd\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pktwfhzi\",t8=L\"ncharTagValue\" c0=T,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"xybavsvh\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"pyrxemvx\",t8=L\"ncharTagValue\" c0=True,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"tlfihwjs\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000", "ogirwqci,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"neumakmg\",t8=L\"ncharTagValue\" c0=F,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"wxqingoa\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
  };
  pRes = taos_query(taos, "use oom");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char**)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_16368_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists d16368 schemaless 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use d16368");
  taos_free_result(pRes);

  const char *sql[] = {
      "[{\"metric\": \"st123456\", \"timestamp\": {\"value\": 1626006833639000, \"type\": \"us\"}, \"value\": 1, \"tags\": {\"t1\": 3, \"t2\": {\"value\": 4, \"type\": \"double\"}, \"t3\": {\"value\": \"t3\", \"type\": \"binary\"}}},\n"
      "{\"metric\": \"st123456\", \"timestamp\": {\"value\": 1626006833739000, \"type\": \"us\"}, \"value\": 2, \"tags\": {\"t1\": {\"value\": 4, \"type\": \"double\"}, \"t3\": {\"value\": \"t4\", \"type\": \"binary\"}, \"t2\": {\"value\": 5, \"type\": \"double\"}, \"t4\": {\"value\": 5, \"type\": \"double\"}}},\n"
      "{\"metric\": \"stb_name\", \"timestamp\": {\"value\": 1626006833639100, \"type\": \"us\"}, \"value\": 3, \"tags\": {\"t2\": {\"value\": 5, \"type\": \"double\"}, \"t3\": {\"value\": \"ste\", \"type\": \"nchar\"}}},\n"
      "{\"metric\": \"stf567890\", \"timestamp\": {\"value\": 1626006833639200, \"type\": \"us\"}, \"value\": 4, \"tags\": {\"t1\": {\"value\": 4, \"type\": \"bigint\"}, \"t3\": {\"value\": \"t4\", \"type\": \"binary\"}, \"t2\": {\"value\": 5, \"type\": \"double\"}, \"t4\": {\"value\": 5, \"type\": \"double\"}}},\n"
      "{\"metric\": \"st123456\", \"timestamp\": {\"value\": 1626006833639300, \"type\": \"us\"}, \"value\": {\"value\": 5, \"type\": \"double\"}, \"tags\": {\"t1\": {\"value\": 4, \"type\": \"double\"}, \"t2\": 5.0, \"t3\": {\"value\": \"t4\", \"type\": \"binary\"}}},\n"
      "{\"metric\": \"stb_name\", \"timestamp\": {\"value\": 1626006833639400, \"type\": \"us\"}, \"value\": {\"value\": 6, \"type\": \"double\"}, \"tags\": {\"t2\": 5.0, \"t3\": {\"value\": \"ste2\", \"type\": \"nchar\"}}},\n"
      "{\"metric\": \"stb_name\", \"timestamp\": {\"value\": 1626006834639400, \"type\": \"us\"}, \"value\": {\"value\": 7, \"type\": \"double\"}, \"tags\": {\"t2\": {\"value\": 5.0, \"type\": \"double\"}, \"t3\": {\"value\": \"ste2\", \"type\": \"nchar\"}}},\n"
      "{\"metric\": \"st123456\", \"timestamp\": {\"value\": 1626006833839006, \"type\": \"us\"}, \"value\": {\"value\": 8, \"type\": \"double\"}, \"tags\": {\"t1\": {\"value\": 4, \"type\": \"double\"}, \"t3\": {\"value\": \"t4\", \"type\": \"binary\"}, \"t2\": {\"value\": 5, \"type\": \"double\"}, \"t4\": {\"value\": 5, \"type\": \"double\"}}},\n"
      "{\"metric\": \"st123456\", \"timestamp\": {\"value\": 1626006833939007, \"type\": \"us\"}, \"value\": {\"value\": 9, \"type\": \"double\"}, \"tags\": {\"t1\": 4, \"t3\": {\"value\": \"t4\", \"type\": \"binary\"}, \"t2\": {\"value\": 5, \"type\": \"double\"}, \"t4\": {\"value\": 5, \"type\": \"double\"}}}]"
  };
  pRes = taos_schemaless_insert(taos, (char**)sql, 0, TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_MICRO_SECONDS);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}

TEST(testCase, sml_dup_time_Test) {
 TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
 ASSERT_NE(taos, nullptr);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists dup_time schemaless 1");
  taos_free_result(pRes);

  const char *sql[] = {
     //"test_ms,t0=t c0=f 1626006833641",
     "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=false,c1=1i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"xcxvwjvf\",c8=L\"ncharColValue\",c9=7u64 1626006833639000000",
     "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=T,c1=2i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"fixrzcuq\",c8=L\"ncharColValue\",c9=7u64 1626006834639000000",
     "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=t,c1=3i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"iupzdqub\",c8=L\"ncharColValue\",c9=7u64 1626006835639000000",
     "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=t,c1=4i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"yvvtzzof\",c8=L\"ncharColValue\",c9=7u64 1626006836639000000",
     "ubzlsr,id=qmtcvgd,t0=t,t1=127i8,t2=32767i16,t3=2147483647i32,t4=9223372036854775807i64,t5=11.12345f32,t6=22.123456789f64,t7=\"binaryTagValue\",t8=L\"ncharTagValue\" c0=t,c1=5i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=11.12345f32,c6=22.123456789f64,c7=\"vbxpilkj\",c8=L\"ncharColValue\",c9=7u64 1626006837639000000"
  };
  pRes = taos_query(taos, "use dup_time");
  taos_free_result(pRes);

  pRes = taos_schemaless_insert(taos, (char**)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL, 0);
  ASSERT_EQ(taos_errno(pRes), 0);
  taos_free_result(pRes);
}


TEST(testCase, sml_16960_Test) {
TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
ASSERT_NE(taos, nullptr);

TAOS_RES* pRes = taos_query(taos, "create database if not exists d16368 schemaless 1");
taos_free_result(pRes);

pRes = taos_query(taos, "use d16368");
taos_free_result(pRes);

const char *sql[] = {
    "[\n"
    "{\n"
    "\"timestamp\":\n"
    "\n"
    "{ \"value\": 1349020800000, \"type\": \"ms\" }\n"
    ",\n"
    "\"value\":\n"
    "\n"
    "{ \"value\": 830525384, \"type\": \"int\" }\n"
    ",\n"
    "\"tags\": {\n"
    "\"id\": \"stb00_0\",\n"
    "\"t0\":\n"
    "\n"
    "{ \"value\": 83972721, \"type\": \"int\" }\n"
    ",\n"
    "\"t1\":\n"
    "\n"
    "{ \"value\": 539147525, \"type\": \"int\" }\n"
    ",\n"
    "\"t2\":\n"
    "\n"
    "{ \"value\": 618258572, \"type\": \"int\" }\n"
    ",\n"
    "\"t3\":\n"
    "\n"
    "{ \"value\": -10536201, \"type\": \"int\" }\n"
    ",\n"
    "\"t4\":\n"
    "\n"
    "{ \"value\": 349227409, \"type\": \"int\" }\n"
    ",\n"
    "\"t5\":\n"
    "\n"
    "{ \"value\": 249347042, \"type\": \"int\" }\n"
    "},\n"
    "\"metric\": \"stb0\"\n"
    "},\n"
    "{\n"
    "\"timestamp\":\n"
    "\n"
    "{ \"value\": 1349020800001, \"type\": \"ms\" }\n"
    ",\n"
    "\"value\":\n"
    "\n"
    "{ \"value\": -588348364, \"type\": \"int\" }\n"
    ",\n"
    "\"tags\": {\n"
    "\"id\": \"stb00_0\",\n"
    "\"t0\":\n"
    "\n"
    "{ \"value\": 83972721, \"type\": \"int\" }\n"
    ",\n"
    "\"t1\":\n"
    "\n"
    "{ \"value\": 539147525, \"type\": \"int\" }\n"
    ",\n"
    "\"t2\":\n"
    "\n"
    "{ \"value\": 618258572, \"type\": \"int\" }\n"
    ",\n"
    "\"t3\":\n"
    "\n"
    "{ \"value\": -10536201, \"type\": \"int\" }\n"
    ",\n"
    "\"t4\":\n"
    "\n"
    "{ \"value\": 349227409, \"type\": \"int\" }\n"
    ",\n"
    "\"t5\":\n"
    "\n"
    "{ \"value\": 249347042, \"type\": \"int\" }\n"
    "},\n"
    "\"metric\": \"stb0\"\n"
    "},\n"
    "{\n"
    "\"timestamp\":\n"
    "\n"
    "{ \"value\": 1349020800002, \"type\": \"ms\" }\n"
    ",\n"
    "\"value\":\n"
    "\n"
    "{ \"value\": -370310823, \"type\": \"int\" }\n"
    ",\n"
    "\"tags\": {\n"
    "\"id\": \"stb00_0\",\n"
    "\"t0\":\n"
    "\n"
    "{ \"value\": 83972721, \"type\": \"int\" }\n"
    ",\n"
    "\"t1\":\n"
    "\n"
    "{ \"value\": 539147525, \"type\": \"int\" }\n"
    ",\n"
    "\"t2\":\n"
    "\n"
    "{ \"value\": 618258572, \"type\": \"int\" }\n"
    ",\n"
    "\"t3\":\n"
    "\n"
    "{ \"value\": -10536201, \"type\": \"int\" }\n"
    ",\n"
    "\"t4\":\n"
    "\n"
    "{ \"value\": 349227409, \"type\": \"int\" }\n"
    ",\n"
    "\"t5\":\n"
    "\n"
    "{ \"value\": 249347042, \"type\": \"int\" }\n"
    "},\n"
    "\"metric\": \"stb0\"\n"
    "},\n"
    "{\n"
    "\"timestamp\":\n"
    "\n"
    "{ \"value\": 1349020800003, \"type\": \"ms\" }\n"
    ",\n"
    "\"value\":\n"
    "\n"
    "{ \"value\": -811250191, \"type\": \"int\" }\n"
    ",\n"
    "\"tags\": {\n"
    "\"id\": \"stb00_0\",\n"
    "\"t0\":\n"
    "\n"
    "{ \"value\": 83972721, \"type\": \"int\" }\n"
    ",\n"
    "\"t1\":\n"
    "\n"
    "{ \"value\": 539147525, \"type\": \"int\" }\n"
    ",\n"
    "\"t2\":\n"
    "\n"
    "{ \"value\": 618258572, \"type\": \"int\" }\n"
    ",\n"
    "\"t3\":\n"
    "\n"
    "{ \"value\": -10536201, \"type\": \"int\" }\n"
    ",\n"
    "\"t4\":\n"
    "\n"
    "{ \"value\": 349227409, \"type\": \"int\" }\n"
    ",\n"
    "\"t5\":\n"
    "\n"
    "{ \"value\": 249347042, \"type\": \"int\" }\n"
    "},\n"
    "\"metric\": \"stb0\"\n"
    "},\n"
    "{\n"
    "\"timestamp\":\n"
    "\n"
    "{ \"value\": 1349020800004, \"type\": \"ms\" }\n"
    ",\n"
    "\"value\":\n"
    "\n"
    "{ \"value\": -330340558, \"type\": \"int\" }\n"
    ",\n"
    "\"tags\": {\n"
    "\"id\": \"stb00_0\",\n"
    "\"t0\":\n"
    "\n"
    "{ \"value\": 83972721, \"type\": \"int\" }\n"
    ",\n"
    "\"t1\":\n"
    "\n"
    "{ \"value\": 539147525, \"type\": \"int\" }\n"
    ",\n"
    "\"t2\":\n"
    "\n"
    "{ \"value\": 618258572, \"type\": \"int\" }\n"
    ",\n"
    "\"t3\":\n"
    "\n"
    "{ \"value\": -10536201, \"type\": \"int\" }\n"
    ",\n"
    "\"t4\":\n"
    "\n"
    "{ \"value\": 349227409, \"type\": \"int\" }\n"
    ",\n"
    "\"t5\":\n"
    "\n"
    "{ \"value\": 249347042, \"type\": \"int\" }\n"
    "},\n"
    "\"metric\": \"stb0\"\n"
    "}\n"
    "]"
};

pRes = taos_schemaless_insert(taos, (char**)sql, sizeof(sql)/sizeof(sql[0]), TSDB_SML_JSON_PROTOCOL, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
ASSERT_EQ(taos_errno(pRes), 0);
taos_free_result(pRes);
}
*/
