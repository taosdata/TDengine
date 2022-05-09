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

TEST(testCase, smlParseString_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;
  SSmlLineInfo elements = {0};

  // case 1
  char *sql = "st,t1=3,t2=4,t3=t3 c1=3i64,c3=\"passit hello,c1=2\",c2=false,c4=4f64 1626006833639000000    ,32,c=3";
  int   ret = smlParseString(sql, &elements, &msgBuf);
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
    "c=1.7976931348623158e+390f64",
    "c=f32",              // float
    "c=8f32f",
    "c=8wef32",
    "c=-3.402823466e+39f32",
    "c=",                 // float
    "c=8f",
    "c=8we",
    "c=3.402823466e+39",
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
    "c=1,c=2"
  };

  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);
  for(int i = 0; i < sizeof(data)/sizeof(data[0]); i++){
    char       msg[256] = {0};
    SSmlMsgBuf msgBuf;
    msgBuf.buf = msg;
    msgBuf.len = 256;
    int32_t len = strlen(data[i]);
    int32_t ret = smlParseCols(data[i], len, NULL, false, dumplicateKey, &msgBuf);
    ASSERT_NE(ret, TSDB_CODE_SUCCESS);
    taosHashClear(dumplicateKey);
  }
  taosHashCleanup(dumplicateKey);
}

TEST(testCase, smlParseCols_tag_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  ASSERT_NE(cols, NULL);
  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  const char *data =
      "cbin=\"passit hello,c=2\",cnch=L\"iisdfsf\",cbool=false,cf64=4.31f64,cf32_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16=233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,cboolf=f,cnch_=l\"iuwq\"";
  int32_t len = strlen(data);
  int32_t ret = smlParseCols(data, len, cols, true, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  int32_t size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 19);

  // nchar
  SSmlKv *kv = taosArrayGetP(cols, 0);
  ASSERT_EQ(strncasecmp(kv->key, "cbin", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->valueLen, 18);
  ASSERT_EQ(strncasecmp(kv->value, "\"passit", 7), 0);
  taosMemoryFree(kv);

  // nchar
  kv = taosArrayGetP(cols, 3);
  ASSERT_EQ(strncasecmp(kv->key, "cf64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->valueLen, 7);
  ASSERT_EQ(strncasecmp(kv->value, "4.31f64", 7), 0);
  taosMemoryFree(kv);

  taosArrayClear(cols);


  // test tag is null
  data = "t=3e";
  len = 0;
  memset(msgBuf.buf, 0, msgBuf.len);
  taosHashClear(dumplicateKey);
  ret = smlParseCols(data, len, cols, true, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 1);

  // nchar
  kv = taosArrayGetP(cols, 0);
  ASSERT_EQ(strncasecmp(kv->key, TAG, strlen(TAG)), 0);
  ASSERT_EQ(kv->keyLen, strlen(TAG));
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->valueLen, strlen(TAG));
  ASSERT_EQ(strncasecmp(kv->value, TAG, strlen(TAG)), 0);
  taosMemoryFree(kv);

  taosArrayDestroy(cols);
  taosHashCleanup(dumplicateKey);
}

TEST(testCase, smlParseCols_Test) {
  char       msg[256] = {0};
  SSmlMsgBuf msgBuf;
  msgBuf.buf = msg;
  msgBuf.len = 256;

  SArray *cols = taosArrayInit(16, POINTER_BYTES);
  ASSERT_NE(cols, NULL);

  SHashObj *dumplicateKey = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_NO_LOCK);

  const char *data = "cbin=\"passit hello,c=2\",cnch=L\"iisdfsf\",cbool=false,cf64=4.31f64,cf32_=8.32,cf32=8.23f32,ci8=-34i8,cu8=89u8,ci16=233i16,cu16=898u16,ci32=98289i32,cu32=12323u32,ci64=-89238i64,ci=989i,cu64=8989323u64,cbooltrue=true,cboolt=t,cboolf=f,cnch_=l\"iuwq\"";
  int32_t len = strlen(data);
  int32_t ret = smlParseCols(data, len, cols, false, dumplicateKey, &msgBuf);
  ASSERT_EQ(ret, TSDB_CODE_SUCCESS);
  int32_t size = taosArrayGetSize(cols);
  ASSERT_EQ(size, 19);

  // binary
  SSmlKv *kv = taosArrayGetP(cols, 0);
  ASSERT_EQ(strncasecmp(kv->key, "cbin", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BINARY);
  ASSERT_EQ(kv->length, 16);
  ASSERT_EQ(strncasecmp(kv->value, "passit", 6), 0);
  taosMemoryFree(kv);

  // nchar
  kv = taosArrayGetP(cols, 1);
  ASSERT_EQ(strncasecmp(kv->key, "cnch", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 7);
  ASSERT_EQ(strncasecmp(kv->value, "iisd", 4), 0);
  taosMemoryFree(kv);

  // bool
  kv = taosArrayGetP(cols, 2);
  ASSERT_EQ(strncasecmp(kv->key, "cbool", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);
  taosMemoryFree(kv);

  // double
  kv = taosArrayGetP(cols, 3);
  ASSERT_EQ(strncasecmp(kv->key, "cf64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_DOUBLE);
  ASSERT_EQ(kv->length, 8);
  //ASSERT_EQ(kv->d, 4.31);
  printf("4.31 = kv->f:%f\n", kv->d);
  taosMemoryFree(kv);

  // float
  kv = taosArrayGetP(cols, 4);
  ASSERT_EQ(strncasecmp(kv->key, "cf32_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_FLOAT);
  ASSERT_EQ(kv->length, 4);
  //ASSERT_EQ(kv->f, 8.32);
  printf("8.32 = kv->f:%f\n", kv->f);
  taosMemoryFree(kv);

  // float
  kv = taosArrayGetP(cols, 5);
  ASSERT_EQ(strncasecmp(kv->key, "cf32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_FLOAT);
  ASSERT_EQ(kv->length, 4);
  //ASSERT_EQ(kv->f, 8.23);
  printf("8.23 = kv->f:%f\n", kv->f);
  taosMemoryFree(kv);

  // tiny int
  kv = taosArrayGetP(cols, 6);
  ASSERT_EQ(strncasecmp(kv->key, "ci8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_TINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, -34);
  taosMemoryFree(kv);

  // unsigned tiny int
  kv = taosArrayGetP(cols, 7);
  ASSERT_EQ(strncasecmp(kv->key, "cu8", 3), 0);
  ASSERT_EQ(kv->keyLen, 3);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UTINYINT);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->u, 89);
  taosMemoryFree(kv);

  // small int
  kv = taosArrayGetP(cols, 8);
  ASSERT_EQ(strncasecmp(kv->key, "ci16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_SMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 233);
  taosMemoryFree(kv);

  // unsigned smallint
  kv = taosArrayGetP(cols, 9);
  ASSERT_EQ(strncasecmp(kv->key, "cu16", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_USMALLINT);
  ASSERT_EQ(kv->length, 2);
  ASSERT_EQ(kv->u, 898);
  taosMemoryFree(kv);

  // int
  kv = taosArrayGetP(cols, 10);
  ASSERT_EQ(strncasecmp(kv->key, "ci32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_INT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 98289);
  taosMemoryFree(kv);

  // unsigned int
  kv = taosArrayGetP(cols, 11);
  ASSERT_EQ(strncasecmp(kv->key, "cu32", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UINT);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(kv->u, 12323);
  taosMemoryFree(kv);


  // bigint
  kv = taosArrayGetP(cols, 12);
  ASSERT_EQ(strncasecmp(kv->key, "ci64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, -89238);
  taosMemoryFree(kv);

  // bigint
  kv = taosArrayGetP(cols, 13);
  ASSERT_EQ(strncasecmp(kv->key, "ci", 2), 0);
  ASSERT_EQ(kv->keyLen, 2);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->i, 989);
  taosMemoryFree(kv);

  // unsigned bigint
  kv = taosArrayGetP(cols, 14);
  ASSERT_EQ(strncasecmp(kv->key, "cu64", 4), 0);
  ASSERT_EQ(kv->keyLen, 4);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_UBIGINT);
  ASSERT_EQ(kv->length, 8);
  ASSERT_EQ(kv->u, 8989323);
  taosMemoryFree(kv);

  // bool
  kv = taosArrayGetP(cols, 15);
  ASSERT_EQ(strncasecmp(kv->key, "cbooltrue", 9), 0);
  ASSERT_EQ(kv->keyLen, 9);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);
  taosMemoryFree(kv);


  // bool
  kv = taosArrayGetP(cols, 16);
  ASSERT_EQ(strncasecmp(kv->key, "cboolt", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, true);
  taosMemoryFree(kv);

  // bool
  kv = taosArrayGetP(cols, 17);
  ASSERT_EQ(strncasecmp(kv->key, "cboolf", 6), 0);
  ASSERT_EQ(kv->keyLen, 6);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_BOOL);
  ASSERT_EQ(kv->length, 1);
  ASSERT_EQ(kv->i, false);
  taosMemoryFree(kv);

  // nchar
  kv = taosArrayGetP(cols, 18);
  ASSERT_EQ(strncasecmp(kv->key, "cnch_", 5), 0);
  ASSERT_EQ(kv->keyLen, 5);
  ASSERT_EQ(kv->type, TSDB_DATA_TYPE_NCHAR);
  ASSERT_EQ(kv->length, 4);
  ASSERT_EQ(strncasecmp(kv->value, "iuwq", 4), 0);
  taosMemoryFree(kv);

  taosArrayDestroy(cols);
  taosHashCleanup(dumplicateKey);
}

TEST(testCase, smlParseLine_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, NULL);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  SRequestObj *request = createRequest(taos, NULL, NULL, TSDB_SQL_INSERT);
  ASSERT_NE(request, NULL);

  SSmlHandle *info = smlBuildSmlInfo(taos, request, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS, true);
  ASSERT_NE(info, NULL);

  const char *sql[9] = {
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0 1451606400000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451607400000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 load_capacity=1500,fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,heading=221,grade=0,fuel_consumption=25 1451608400000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 fuel_capacity=150,nominal_fuel_consumption=12,latitude=52.31854,longitude=4.72037,elevation=124,velocity=0,heading=221,grade=0,fuel_consumption=25 1451609400000000000",
    "readings,name=truck_0,fleet=South,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 1451619400000000000",
    "readings,name=truck_1,fleet=South,driver=Albert,model=F-150,device_version=v1.5 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=72.45258,longitude=68.83761,elevation=255,velocity=0,heading=181,grade=0,fuel_consumption=25 1451606400000000000",
    "readings,name=truck_2,driver=Derek,model=F-150,device_version=v1.5 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation=428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451606400000000000",
    "readings,name=truck_2,fleet=North,driver=Derek,model=F-150 load_capacity=2000,fuel_capacity=200,nominal_fuel_consumption=15,latitude=24.5208,longitude=28.09377,elevation=428,velocity=0,heading=304,grade=0,fuel_consumption=25 1451609400000000000",
    "readings,fleet=South,name=truck_0,driver=Trish,model=H-2,device_version=v2.3 fuel_consumption=25,grade=0 1451629400000000000"
  };
  smlInsertLines(info, sql, 9);
//  for (int i = 0; i < 3; i++) {
//    smlParseLine(info, sql[i]);
//  }
}

TEST(testCase, smlParseLine_error_Test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, NULL);

  TAOS_RES* pRes = taos_query(taos, "create database if not exists sml_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use sml_db");
  taos_free_result(pRes);

  SRequestObj *request = createRequest(taos, NULL, NULL, TSDB_SQL_INSERT);
  ASSERT_NE(request, NULL);

  SSmlHandle *info = smlBuildSmlInfo(taos, request, TSDB_SML_LINE_PROTOCOL, TSDB_SML_TIMESTAMP_NANO_SECONDS, true);
  ASSERT_NE(info, NULL);

  const char *sql[2] = {
      "measure,t1=3 c1=8",
      "measure,t2=3 c1=8u8"
  };
  int ret = smlInsertLines(info, sql, 2);
  ASSERT_NE(ret, 0);
}

// TEST(testCase, smlParseTS_Test) {
//   char       msg[256] = {0};
//   SSmlMsgBuf msgBuf;
//   msgBuf.buf = msg;
//   msgBuf.len = 256;
//   SSmlLineInfo elements = {0};
//
//   SSmlHandle* info = smlBuildSmlInfo(taos, request, protocol, precision, dataFormat);
//   if(!info){
//     return (TAOS_RES*)request;
//   }
//   ret = smlParseTS(info, elements.timestamp, elements.timestampLen, cols);
//   if(ret != TSDB_CODE_SUCCESS){
//     uError("SML:0x%"PRIx64" smlParseTS failed", info->id);
//     return ret;
//   }
// }
