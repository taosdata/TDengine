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
#include "pub.h" 
#include <bench.h>
#include <assert.h>
#include "toolsdef.h"
#include "toolsdef.h"
#include "toolscJson.h"

#ifdef __cplusplus
extern "C" {
#include "dump.h"
#endif



SArguments*  g_arguments;
bool           g_fail = false;
uint64_t       g_memoryUsage = 0;
extern char    g_configDir[MAX_PATH_LEN];
SQueryMetaInfo g_queryInfo;
STmqMetaInfo   g_tmqInfo;
tools_cJSON*   root;
extern struct arguments g_args;
uint64_t        g_argFlag = 0;

char *typeToStr(int type);
bool replaceCopy(char *des, char *src);
int inDatabasesSeq(const char *dbName);
int processFieldsValueV2(int index, TableDes *tableDes, const void *value, int32_t len);
int readJsonFields(json_t *value, RecordSchema *recordSchema);
int32_t appendValues(char *buf, char* val);
int32_t dumpInAvroTagTinyInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
int32_t dumpInAvroTagSmallInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
int32_t dumpInAvroTagInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
int32_t dumpInAvroTagBigInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
int32_t dumpInAvroTagFloat(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
int32_t dumpInAvroTagDouble(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
// int32_t dumpInAvroTagBinary(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
// int32_t dumpInAvroTagTimestamp(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
// int32_t dumpInAvroTagNChar(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
// int32_t dumpInAvroTagUTinyInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
// int32_t dumpInAvroTagUSmallInt(FieldStruct *field, avro_value_t *value, char *sqlstr, int32_t curr_sqlstr_len);
#ifdef __cplusplus
}
#endif

TEST(taosdump, toolsSys) {
  // errorPrintReqArg3
  errorPrintReqArg3((char *)"taosdump", (char *)"test parameters");
  printf("ut function errorPrintReqArg3 ....................  [Passed]\n");

  // setConsoleEcho
  setConsoleEcho(false);
  setConsoleEcho(true);
  printf("ut function setConsoleEcho .......................  [Passed]\n");
}


TEST(taosdump, typeToStr) {
  assert(strcmp(typeToStr(1), "bool") == 0);
  assert(strcmp(typeToStr(2), "tinyint") == 0);
  assert(strcmp(typeToStr(3), "smallint") == 0);
  assert(strcmp(typeToStr(4), "int") == 0);
  assert(strcmp(typeToStr(5), "bigint") == 0);
  assert(strcmp(typeToStr(6), "float") == 0);
  assert(strcmp(typeToStr(7), "double") == 0);
  assert(strcmp(typeToStr(8), "binary") == 0);
  assert(strcmp(typeToStr(9), "timestamp") == 0);
  assert(strcmp(typeToStr(10), "nchar") == 0);
  assert(strcmp(typeToStr(11), "tinyint unsigned") == 0);
  assert(strcmp(typeToStr(12), "smallint unsigned") == 0);
  assert(strcmp(typeToStr(13), "int unsigned") == 0);
  assert(strcmp(typeToStr(14), "bigint unsigned") == 0);
  assert(strcmp(typeToStr(15), "JSON") == 0);
  assert(strcmp(typeToStr(16), "varbinary") == 0);
  assert(strcmp(typeToStr(20), "geometry") == 0);
  assert(strcmp(typeToStr(22), "unknown") == 0); 
}

TEST(taosdump, replaceCopy) {
  char buf[128];

  // 1. No dot in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "abc") == false);
  assert(strcmp(buf, "abc") == 0);

  // 2. One dot in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "a.bc") == true);
  assert(strcmp(buf, "a_bc") == 0);

  // 3. Multiple dots in source string
  strcpy(buf, "");
  assert(replaceCopy(buf, "a.b.c.d") == true);
  assert(strcmp(buf, "a_b_c_d") == 0);

  // 4. Dot at the beginning
  strcpy(buf, "");
  assert(replaceCopy(buf, ".abc") == true);
  assert(strcmp(buf, "_abc") == 0);

  // 5. Dot at the end
  strcpy(buf, "");
  assert(replaceCopy(buf, "abc.") == true);
  assert(strcmp(buf, "abc_") == 0);

  // 6. Empty string
  strcpy(buf, "xxxx");
  assert(replaceCopy(buf, "") == false);
  assert(strcmp(buf, "") == 0);
}

TEST(taosdump, processResultValue) {
    char buf[256];
    int ret;

    // BOOL
    char bool_true = 1, bool_false = 0;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BOOL, &bool_true, sizeof(bool_true));
    assert(strcmp(buf, "1") == 0);
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BOOL, &bool_false, sizeof(bool_false));
    assert(strcmp(buf, "0") == 0);

    // TINYINT
    int8_t tiny = -123;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_TINYINT, &tiny, sizeof(tiny));
    assert(strcmp(buf, "-123") == 0);

    // SMALLINT
    int16_t small = -12345;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_SMALLINT, &small, sizeof(small));
    assert(strcmp(buf, "-12345") == 0);

    // INT
    int32_t i32 = 123456;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_INT, &i32, sizeof(i32));
    assert(strcmp(buf, "123456") == 0);

    // BIGINT
    int64_t i64 = -9876543210LL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BIGINT, &i64, sizeof(i64));
    assert(strcmp(buf, "-9876543210") == 0);

    // UTINYINT
    uint8_t utiny = 200;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UTINYINT, &utiny, sizeof(utiny));
    assert(strcmp(buf, "200") == 0);

    // USMALLINT
    uint16_t usmall = 60000;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_USMALLINT, &usmall, sizeof(usmall));
    assert(strcmp(buf, "60000") == 0);

    // UINT
    uint32_t ui32 = 4000000000U;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UINT, &ui32, sizeof(ui32));
    assert(strcmp(buf, "4000000000") == 0);

    // UBIGINT
    uint64_t ui64 = 1234567890123456789ULL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_UBIGINT, &ui64, sizeof(ui64));
    assert(strcmp(buf, "1234567890123456789") == 0);

    // FLOAT
    float f = 3.14f;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_FLOAT, &f, sizeof(f));
    assert(strncmp(buf, "3.140", 5) == 0); // Accept rounding

    // DOUBLE
    double d = -2.718281828;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_DOUBLE, &d, sizeof(d));
    assert(strncmp(buf, "-2.718", 6) == 0);

    // BINARY
    char bin[] = "abc";
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_BINARY, bin, 3);
    assert(strstr(buf, "abc") != NULL);

    // NCHAR
    char nchar[] = "xyz";
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_NCHAR, nchar, 3);
    assert(strstr(buf, "xyz") != NULL);

    // TIMESTAMP
    int64_t ts = 1680000000000LL;
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_TIMESTAMP, &ts, sizeof(ts));
    assert(strcmp(buf, "1680000000000") == 0);

    // NULL value
    ret = processResultValue(buf, 0, TSDB_DATA_TYPE_INT, NULL, 0);
    assert(strcmp(buf, "NULL") == 0);

}

TEST(taosdump, inDatabasesSeq) {
  // Test with a database that exists
  g_args.databasesSeq = NULL;
  assert(inDatabasesSeq("db1") == -1);

  // Test with a database that does not exist
  g_args.databasesSeq = "db1";
  assert(inDatabasesSeq("db1") == 0);
  assert(inDatabasesSeq("db2") == -1);

  g_args.databasesSeq = "db1,db2,db3";
  assert(inDatabasesSeq("db1") == 0);
  assert(inDatabasesSeq("db2") == 0);
  assert(inDatabasesSeq("db3") == 0);
  assert(inDatabasesSeq("db4") == -1);

  g_args.databasesSeq = "db1,,db3";
  assert(inDatabasesSeq("db1") == 0);
  assert(inDatabasesSeq("db3") == 0);
  assert(inDatabasesSeq("") == 0);

  g_args.databasesSeq = ",db1,db2,";
  assert(inDatabasesSeq("db1") == 0);
  assert(inDatabasesSeq("db2") == 0);
  assert(inDatabasesSeq("") == 0);
}

TEST(taosdump, processFieldsValueV2) {
    TableDes tableDes = {0};

    // int
    tableDes.cols[0].type = 4; // TSDB_DATA_TYPE_INT
    int32_t vi = 123;
    processFieldsValueV2(0, &tableDes, &vi, sizeof(vi));
    assert(strcmp(tableDes.cols[0].value, "123") == 0);

    // bigint
    tableDes.cols[0].type = 5; // TSDB_DATA_TYPE_BIGINT
    int64_t vbig = 1234567890123LL;
    processFieldsValueV2(0, &tableDes, &vbig, sizeof(vbig));
    assert(strcmp(tableDes.cols[0].value, "1234567890123") == 0);

    // float
    tableDes.cols[0].type = 6; // TSDB_DATA_TYPE_FLOAT
    float vf = 3.14f;
    processFieldsValueV2(0, &tableDes, &vf, sizeof(vf));
    assert(strstr(tableDes.cols[0].value, "3.14") != NULL);

    // double
    tableDes.cols[0].type = 7; // TSDB_DATA_TYPE_DOUBLE
    double vd = 2.71828;
    processFieldsValueV2(0, &tableDes, &vd, sizeof(vd));
    assert(strstr(tableDes.cols[0].value, "2.718") != NULL);

    // binary
    tableDes.cols[0].type = 8; // TSDB_DATA_TYPE_BINARY
    char vb[] = "hello";
    processFieldsValueV2(0, &tableDes, vb, strlen(vb));
    assert(strstr(tableDes.cols[0].value, "hello") != NULL);

    // nchar
    tableDes.cols[0].type = 10; // TSDB_DATA_TYPE_NCHAR
    char vn[] = "世界";
    processFieldsValueV2(0, &tableDes, vn, strlen(vn));
    assert(strlen(tableDes.cols[0].value) > 0);

    // timestamp
    tableDes.cols[0].type = 9; // TSDB_DATA_TYPE_TIMESTAMP
    int64_t ts = 1710000000000LL;
    processFieldsValueV2(0, &tableDes, &ts, sizeof(ts));
    assert(strcmp(tableDes.cols[0].value, "1710000000000") == 0);

    // bool
    tableDes.cols[0].type = 1; // TSDB_DATA_TYPE_BOOL
    char vb1 = 1, vb0 = 0;
    processFieldsValueV2(0, &tableDes, &vb1, 1);
    assert(strcmp(tableDes.cols[0].value, "1") == 0);
    processFieldsValueV2(0, &tableDes, &vb0, 1);
    assert(strcmp(tableDes.cols[0].value, "0") == 0);

    // unsigned int
    tableDes.cols[0].type = 13; // TSDB_DATA_TYPE_UINT
    uint32_t vu = 123456;
    processFieldsValueV2(0, &tableDes, &vu, sizeof(vu));
    assert(strcmp(tableDes.cols[0].value, "123456") == 0);

    printf("All processFieldsValueV2 tests passed!\n");
}

TEST(taosdump, readJsonFields) {
  const char *json_text = "[{\"name\": \"id\", \"type\": \"int\"}]";
  json_error_t error;
  json_t *root = json_loads(json_text, 0, &error);
  assert(root);

  RecordSchema schema = {0};
  int ret = readJsonFields(root, &schema);
  assert(ret == 0);
  assert(schema.num_fields == 1);
  assert(strcmp(schema.fields, "id") == 0);
  // assert(strcmp(schema.fields[0], "col1") == 0);
  // 释放资源
  free(schema.fields);
  json_decref(root);

  const char *json_array = "[{\"name\": \"temp\", \"type\": [\"float\", \"null\"]}]";
  json_t *root_array = json_loads(json_array, 0, &error);
  assert(root_array); 
  RecordSchema schema_array = {0};
  ret = readJsonFields(root_array, &schema_array);
  assert(ret == 0);
  assert(schema_array.num_fields == 1);
  assert(strcmp(schema_array.fields, "temp") == 0);
  free(schema_array.fields);
  json_decref(root_array);

  const char *json_object = "[{\"name\": \"sensors\", \"type\": {\"type\": \"array\", \"items\": \"double\"}}]";
  json_t *root_object = json_loads(json_object, 0, &error);
  assert(root_object);
  RecordSchema schema_object = {0};
  ret = readJsonFields(root_object, &schema_object);
  assert(ret == 0);
  assert(schema_object.num_fields == 1);
  assert(strcmp(schema_object.fields, "sensors") == 0);
  free(schema_object.fields);
  json_decref(root_object);

}

TEST(taosdump, dumpInAvroTagTypes) {
  char buf[128] = {0};
  FieldStruct field = {0};
  avro_value_t field_value;

  avro_schema_t int_schema = avro_schema_int();
  avro_value_iface_t *int_iface = avro_generic_class_from_schema(int_schema);
  avro_generic_value_new(int_iface, &field_value);
  avro_value_set_int(&field_value, 123);    

  // not nullable, normal value
  field.nullable = 0;
  dumpInAvroTagTinyInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "123,") != NULL);
  
  // // not nullable, null value
  avro_value_set_int(&field_value, TSDB_DATA_TINYINT_NULL);
  dumpInAvroTagTinyInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);

  avro_value_set_int(&field_value, 567);
  dumpInAvroTagSmallInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "567,") != NULL);

  avro_value_set_int(&field_value, TSDB_DATA_SMALLINT_NULL);
  dumpInAvroTagSmallInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);

  avro_value_set_int(&field_value, 789);
  dumpInAvroTagInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "789,") != NULL);

  avro_value_set_int(&field_value, TSDB_DATA_INT_NULL);
  dumpInAvroTagInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);

  avro_value_set_long(&field_value, 9099999);
  dumpInAvroTagBigInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "9099999,") != NULL);

  avro_value_set_long(&field_value, TSDB_DATA_BIGINT_NULL);
  dumpInAvroTagBigInt(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);

  avro_value_set_float(&field_value, 9099.999);
  dumpInAvroTagFloat(&field, &field_value, buf, 0);
  assert(strstr(buf, "9099.999,") != NULL);

  avro_value_set_float(&field_value, TSDB_DATA_FLOAT_NULL);
  dumpInAvroTagFloat(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);

  avro_value_set_double(&field_value, 123456.789);
  dumpInAvroTagDouble(&field, &field_value, buf, 0);
  assert(strstr(buf, "123456.789,") != NULL);

  avro_value_set_double(&field_value, TSDB_DATA_DOUBLE_NULL);
  dumpInAvroTagDouble(&field, &field_value, buf, 0);
  assert(strstr(buf, "NULL,") != NULL);


}


int main(int argc, char **argv) {
  printf("hello world taosdump unit test for C\n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}