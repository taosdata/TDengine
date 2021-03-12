#include "os.h"
#include <gtest/gtest.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "tsdb.h"

#include "../../client/inc/tscUtil.h"
#include "tutil.h"
#include "tvariant.h"
#include "ttokendef.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"

namespace {
int32_t testValidateName(char* name) {
  SStrToken token = {0};
  token.z = name;
  token.n = strlen(name);
  token.type = 0;

  tSQLGetToken(name, &token.type);
  return tscValidateName(&token);
}
}

static void _init_tvariant_bool(tVariant* t) {
  t->i64 = TSDB_FALSE;
  t->nType = TSDB_DATA_TYPE_BOOL;
}

static void _init_tvariant_tinyint(tVariant* t) {
  t->i64 = -27;
  t->nType = TSDB_DATA_TYPE_TINYINT;
}

static void _init_tvariant_int(tVariant* t) {
  t->i64 = -23997659;
  t->nType = TSDB_DATA_TYPE_INT;
}

static void _init_tvariant_bigint(tVariant* t) {
  t->i64 = -3333333333333;
  t->nType = TSDB_DATA_TYPE_BIGINT;
}

static void _init_tvariant_float(tVariant* t) {
  t->dKey = -8991212199.8987878776;
  t->nType = TSDB_DATA_TYPE_FLOAT;
}

static void _init_tvariant_binary(tVariant* t) {
  tVariantDestroy(t);

  t->pz = (char*)calloc(1, 20);  //"2e3");
  t->nType = TSDB_DATA_TYPE_BINARY;
  strcpy(t->pz, "2e5");
  t->nLen = strlen(t->pz);
}

static void _init_tvariant_nchar(tVariant* t) {
  tVariantDestroy(t);

  t->wpz = (wchar_t*)calloc(1, 20 * TSDB_NCHAR_SIZE);
  t->nType = TSDB_DATA_TYPE_NCHAR;
  wcscpy(t->wpz, L"-2000000.8765");
  t->nLen = twcslen(t->wpz);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/* test validate the names for table/database */
TEST(testCase, db_table_name) {

  char t01[] = "abc";
  EXPECT_EQ(testValidateName(t01), TSDB_CODE_SUCCESS);

  char t02[] = "'abc'";
  EXPECT_EQ(testValidateName(t02), TSDB_CODE_SUCCESS);

  char t1[] = "abc.def";
  EXPECT_EQ(testValidateName(t1), TSDB_CODE_SUCCESS);
  printf("%s\n", t1);

  char t2[] = "'abc.def'";
  EXPECT_EQ(testValidateName(t2), TSDB_CODE_SUCCESS);
  printf("%s\n", t2);

  char t3[] = "'abc'.def";
  EXPECT_EQ(testValidateName(t3), TSDB_CODE_SUCCESS);
  printf("%s\n", t3);

  char t4[] = "'abc'.'def'";
  EXPECT_EQ(testValidateName(t4), TSDB_CODE_SUCCESS);

  char t5[] = "table.'def'";
  EXPECT_EQ(testValidateName(t5), TSDB_CODE_TSC_INVALID_SQL);

  char t6[] = "'table'.'def'";
  EXPECT_EQ(testValidateName(t6), TSDB_CODE_TSC_INVALID_SQL);

  char t7[] = "'_ab1234'.'def'";
  EXPECT_EQ(testValidateName(t7), TSDB_CODE_SUCCESS);
  printf("%s\n", t7);

  char t8[] = "'_ab&^%1234'.'def'";
  EXPECT_EQ(testValidateName(t8), TSDB_CODE_TSC_INVALID_SQL);

  char t9[] = "'_123'.'gtest中文'";
  EXPECT_EQ(testValidateName(t9), TSDB_CODE_TSC_INVALID_SQL);

  char t10[] = "abc.'gtest中文'";
  EXPECT_EQ(testValidateName(t10), TSDB_CODE_TSC_INVALID_SQL);

  char t10_1[] = "abc.'中文gtest'";
  EXPECT_EQ(testValidateName(t10_1), TSDB_CODE_TSC_INVALID_SQL);
  
  char t11[] = "'192.168.0.1'.abc";
  EXPECT_EQ(testValidateName(t11), TSDB_CODE_TSC_INVALID_SQL);

  char t12[] = "192.168.0.1.abc";
  EXPECT_EQ(testValidateName(t12), TSDB_CODE_TSC_INVALID_SQL);

  char t13[] = "abc.";
  EXPECT_EQ(testValidateName(t13), TSDB_CODE_TSC_INVALID_SQL);

  char t14[] = ".abc";
  EXPECT_EQ(testValidateName(t14), TSDB_CODE_TSC_INVALID_SQL);

  char t15[] = ".'abc'";
  EXPECT_EQ(testValidateName(t15), TSDB_CODE_TSC_INVALID_SQL);

  char t16[] = ".abc'";
  EXPECT_EQ(testValidateName(t16), TSDB_CODE_TSC_INVALID_SQL);

  char t17[] = "123a.\"abc\"";
  EXPECT_EQ(testValidateName(t17), TSDB_CODE_TSC_INVALID_SQL);
  printf("%s\n", t17);

  char t18[] = "a.\"abc\"";
  EXPECT_EQ(testValidateName(t18), TSDB_CODE_SUCCESS);
  printf("%s\n", t18);

  char t19[] = "'_ab1234'.'def'.'ab123'";
  EXPECT_EQ(testValidateName(t19), TSDB_CODE_TSC_INVALID_SQL);

  char t20[] = "'_ab1234*&^'";
  EXPECT_EQ(testValidateName(t20), TSDB_CODE_TSC_INVALID_SQL);

  char t21[] = "'1234_abc'";
  EXPECT_EQ(testValidateName(t21), TSDB_CODE_TSC_INVALID_SQL);


  // =======Containing capital letters=================
  char t30[] = "ABC";
  EXPECT_EQ(testValidateName(t30), TSDB_CODE_SUCCESS);

  char t31[] = "'ABC'";
  EXPECT_EQ(testValidateName(t31), TSDB_CODE_SUCCESS);

  char t32[] = "ABC.def";
  EXPECT_EQ(testValidateName(t32), TSDB_CODE_SUCCESS);

  char t33[] = "'ABC.def";
  EXPECT_EQ(testValidateName(t33), TSDB_CODE_TSC_INVALID_SQL);

  char t33_0[] = "abc.DEF'";
  EXPECT_EQ(testValidateName(t33_0), TSDB_CODE_TSC_INVALID_SQL);
  
  char t34[] = "'ABC.def'";
  //int32_t tmp0 = testValidateName(t34);
  EXPECT_EQ(testValidateName(t34), TSDB_CODE_SUCCESS);

  char t35[] = "'ABC'.def";
  EXPECT_EQ(testValidateName(t35), TSDB_CODE_SUCCESS);

  char t36[] = "'ABC'.'DEF'";
  EXPECT_EQ(testValidateName(t36), TSDB_CODE_SUCCESS);

  char t37[] = "abc.'DEF'";
  EXPECT_EQ(testValidateName(t37), TSDB_CODE_SUCCESS);

  char t37_1[] = "abc.'_123DEF'";
  EXPECT_EQ(testValidateName(t37_1), TSDB_CODE_SUCCESS);

  char t38[] = "'abc'.'DEF'";
  EXPECT_EQ(testValidateName(t38), TSDB_CODE_SUCCESS);

  // do not use key words 
  char t39[] = "table.'DEF'";
  EXPECT_EQ(testValidateName(t39), TSDB_CODE_TSC_INVALID_SQL);

  char t40[] = "'table'.'DEF'";
  EXPECT_EQ(testValidateName(t40), TSDB_CODE_TSC_INVALID_SQL);

  char t41[] = "'_abXYZ1234'.'deFF'";
  EXPECT_EQ(testValidateName(t41), TSDB_CODE_SUCCESS);

  char t42[] = "'_abDEF&^%1234'.'DIef'";
  EXPECT_EQ(testValidateName(t42), TSDB_CODE_TSC_INVALID_SQL);

  char t43[] = "'_123'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t43), TSDB_CODE_TSC_INVALID_SQL);

  char t44[] = "'aABC'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t44), TSDB_CODE_TSC_INVALID_SQL);
  
  char t45[] = "'ABC'.";
  EXPECT_EQ(testValidateName(t45), TSDB_CODE_TSC_INVALID_SQL);

  char t46[] = ".'ABC'";
  EXPECT_EQ(testValidateName(t46), TSDB_CODE_TSC_INVALID_SQL);

  char t47[] = "a.\"aTWc\"";
  EXPECT_EQ(testValidateName(t47), TSDB_CODE_SUCCESS);

 // ================has space =================
  char t60[] = " ABC ";
  EXPECT_EQ(testValidateName(t60), TSDB_CODE_TSC_INVALID_SQL);

  char t60_1[] = "   ABC ";
  EXPECT_EQ(testValidateName(t60_1), TSDB_CODE_TSC_INVALID_SQL);

  char t61[] = "' ABC '";
  EXPECT_EQ(testValidateName(t61), TSDB_CODE_TSC_INVALID_SQL);

  char t61_1[] = "'  ABC '";
  EXPECT_EQ(testValidateName(t61_1), TSDB_CODE_TSC_INVALID_SQL);

  char t62[] = " ABC . def ";
  EXPECT_EQ(testValidateName(t62), TSDB_CODE_TSC_INVALID_SQL);

  char t63[] = "' ABC . def ";
  EXPECT_EQ(testValidateName(t63), TSDB_CODE_TSC_INVALID_SQL);

  char t63_0[] = "  abc . DEF ' ";
  EXPECT_EQ(testValidateName(t63_0), TSDB_CODE_TSC_INVALID_SQL);
  
  char t64[] = " '  ABC .  def ' ";
  //int32_t tmp1 = testValidateName(t64);
  EXPECT_EQ(testValidateName(t64), TSDB_CODE_TSC_INVALID_SQL);

  char t65[] = " ' ABC  '. def ";
  EXPECT_EQ(testValidateName(t65), TSDB_CODE_TSC_INVALID_SQL);

  char t66[] = "' ABC '.'  DEF '";
  EXPECT_EQ(testValidateName(t66), TSDB_CODE_TSC_INVALID_SQL);

  char t67[] = "abc . '  DEF  '";
  EXPECT_EQ(testValidateName(t67), TSDB_CODE_TSC_INVALID_SQL);

  char t68[] = "'  abc '.'   DEF '";
  EXPECT_EQ(testValidateName(t68), TSDB_CODE_TSC_INVALID_SQL);

  // do not use key words 
  char t69[] = "table.'DEF'";
  EXPECT_EQ(testValidateName(t69), TSDB_CODE_TSC_INVALID_SQL);

  char t70[] = "'table'.'DEF'";
  EXPECT_EQ(testValidateName(t70), TSDB_CODE_TSC_INVALID_SQL);

  char t71[] = "'_abXYZ1234  '.' deFF  '";
  EXPECT_EQ(testValidateName(t71), TSDB_CODE_TSC_INVALID_SQL);

  char t72[] = "'_abDEF&^%1234'.'  DIef'";
  EXPECT_EQ(testValidateName(t72), TSDB_CODE_TSC_INVALID_SQL);

  char t73[] = "'_123'.'  Gtest中文'";
  EXPECT_EQ(testValidateName(t73), TSDB_CODE_TSC_INVALID_SQL);

  char t74[] = "' aABC'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t74), TSDB_CODE_TSC_INVALID_SQL);
  
  char t75[] = "' ABC '.";
  EXPECT_EQ(testValidateName(t75), TSDB_CODE_TSC_INVALID_SQL);

  char t76[] = ".' ABC'";
  EXPECT_EQ(testValidateName(t76), TSDB_CODE_TSC_INVALID_SQL);

  char t77[] = " a . \"aTWc\" ";
  EXPECT_EQ(testValidateName(t77), TSDB_CODE_TSC_INVALID_SQL);

  char t78[] = "  a.\"aTWc  \"";
  EXPECT_EQ(testValidateName(t78), TSDB_CODE_TSC_INVALID_SQL);


  // ===============muti string by space ===================
  // There's no such case.
  //char t160[] = "A BC";
  //EXPECT_EQ(testValidateName(t160), TSDB_CODE_TSC_INVALID_SQL);
  //printf("end:%s\n", t160);

  // There's no such case.
  //char t161[] = "' A BC '";
  //EXPECT_EQ(testValidateName(t161), TSDB_CODE_TSC_INVALID_SQL);

  char t162[] = " AB C . de f ";
  EXPECT_EQ(testValidateName(t162), TSDB_CODE_TSC_INVALID_SQL);

  char t163[] = "' AB C . de f ";
  EXPECT_EQ(testValidateName(t163), TSDB_CODE_TSC_INVALID_SQL);

  char t163_0[] = "  ab c . DE F ' ";
  EXPECT_EQ(testValidateName(t163_0), TSDB_CODE_TSC_INVALID_SQL);
  
  char t164[] = " '  AB C .  de f ' ";
  //int32_t tmp2 = testValidateName(t164);
  EXPECT_EQ(testValidateName(t164), TSDB_CODE_TSC_INVALID_SQL);

  char t165[] = " ' A BC  '. de f ";
  EXPECT_EQ(testValidateName(t165), TSDB_CODE_TSC_INVALID_SQL);

  char t166[] = "' AB C '.'  DE  F '";
  EXPECT_EQ(testValidateName(t166), TSDB_CODE_TSC_INVALID_SQL);

  char t167[] = "ab  c . '  D  EF  '";
  EXPECT_EQ(testValidateName(t167), TSDB_CODE_TSC_INVALID_SQL);

  char t168[] = "'  a bc '.'   DE  F '";
  EXPECT_EQ(testValidateName(t168), TSDB_CODE_TSC_INVALID_SQL);
  
}

/* test parse time function */
TEST(testCase, parse_time) {
  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  char t1[] = "2018-1-1 1:1:1.952798";
  char t13[] = "1970-1-1 0:0:0";

  int64_t time = 0, time1 = 0;

  taosParseTime(t1, &time, strlen(t1), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 1514739661952);

  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, timezone * MILLISECOND_PER_SECOND);

  char t2[] = "2018-1-1T1:1:1.952Z";
  taosParseTime(t2, &time, strlen(t2), TSDB_TIME_PRECISION_MILLI, 0);

  EXPECT_EQ(time, 1514739661952 + 28800000);

  char t3[] = "2018-1-1 1:01:01.952";
  taosParseTime(t3, &time, strlen(t3), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 1514739661952);

  char t4[] = "2018-1-1 1:01:01.9";
  char t5[] = "2018-1-1 1:01:1.900";
  char t6[] = "2018-01-01 1:1:1.90";
  char t7[] = "2018-01-01 01:01:01.9";
  char t8[] = "2018-01-01 01:01:01.9007865";

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t5, &time1, strlen(t5), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t6, &time1, strlen(t6), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t4, &time, strlen(t4), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t7, &time1, strlen(t7), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taosParseTime(t5, &time, strlen(t5), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t8, &time1, strlen(t8), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t9[] = "2017-4-3 1:1:2.980";
  char t10[] = "2017-4-3T2:1:2.98+9:00";
  taosParseTime(t9, &time, strlen(t9), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t10, &time1, strlen(t10), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t11[] = "2017-4-3T2:1:2.98+09:00";
  taosParseTime(t11, &time, strlen(t11), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t10, &time1, strlen(t10), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  char t12[] = "2017-4-3T2:1:2.98+0900";
  taosParseTime(t11, &time, strlen(t11), TSDB_TIME_PRECISION_MILLI, 0);
  taosParseTime(t12, &time1, strlen(t12), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, time1);

  taos_options(TSDB_OPTION_TIMEZONE, "UTC");
  taosParseTime(t13, &time, strlen(t13), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 0);

  taos_options(TSDB_OPTION_TIMEZONE, "Asia/Shanghai");
  char t14[] = "1970-1-1T0:0:0Z";
  taosParseTime(t14, &time, strlen(t14), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 0);

  char t40[] = "1970-1-1 0:0:0.999999999";
  taosParseTime(t40, &time, strlen(t40), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 999 + timezone * MILLISECOND_PER_SECOND);

  char t41[] = "1997-1-1 0:0:0.999999999";
  taosParseTime(t41, &time, strlen(t41), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 852048000999);

  int64_t k = timezone;
  char    t42[] = "1997-1-1T0:0:0.999999999Z";
  taosParseTime(t42, &time, strlen(t42), TSDB_TIME_PRECISION_MILLI, 0);
  EXPECT_EQ(time, 852048000999 - timezone * MILLISECOND_PER_SECOND);

  ////////////////////////////////////////////////////////////////////
  // illegal timestamp format
  char t15[] = "2017-12-33 0:0:0";
  EXPECT_EQ(taosParseTime(t15, &time, strlen(t15), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t16[] = "2017-12-31 99:0:0";
  EXPECT_EQ(taosParseTime(t16, &time, strlen(t16), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t17[] = "2017-12-31T9:0:0";
  EXPECT_EQ(taosParseTime(t17, &time, strlen(t17), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t18[] = "2017-12-31T9:0:0.Z";
  EXPECT_EQ(taosParseTime(t18, &time, strlen(t18), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t19[] = "2017-12-31 9:0:0.-1";
  EXPECT_EQ(taosParseTime(t19, &time, strlen(t19), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t20[] = "2017-12-31 9:0:0.1+12:99";
  EXPECT_EQ(taosParseTime(t20, &time, strlen(t20), TSDB_TIME_PRECISION_MILLI, 0), 0);
  EXPECT_EQ(time, 1514682000100);

  char t21[] = "2017-12-31T9:0:0.1+12:99";
  EXPECT_EQ(taosParseTime(t21, &time, strlen(t21), TSDB_TIME_PRECISION_MILLI, 0), -1);

  char t22[] = "2017-12-31 9:0:0.1+13:1";
  EXPECT_EQ(taosParseTime(t22, &time, strlen(t22), TSDB_TIME_PRECISION_MILLI, 0), 0);

  char t23[] = "2017-12-31T9:0:0.1+13:1";
  EXPECT_EQ(taosParseTime(t23, &time, strlen(t23), TSDB_TIME_PRECISION_MILLI, 0), 0);
}

TEST(testCase, tvariant_convert) {
  // 1. bool data to all other data types
  tVariant t = {0};
  _init_tvariant_bool(&t);

  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.dKey, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.dKey, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "FALSE");
  tVariantDestroy(&t);

  _init_tvariant_bool(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"FALSE");
  tVariantDestroy(&t);

  // 2. tinyint to other data types
  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.dKey, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.dKey, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-27");
  tVariantDestroy(&t);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-27");
  tVariantDestroy(&t);

  // 3. int to other data
  // types//////////////////////////////////////////////////////////////////
  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);
  EXPECT_EQ(t.i64, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.dKey, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.dKey, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-23997659");
  tVariantDestroy(&t);

  _init_tvariant_int(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-23997659");
  tVariantDestroy(&t);

  // 4. bigint to other data
  // type//////////////////////////////////////////////////////////////////////////////
  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.dKey, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.dKey, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-3333333333333");
  tVariantDestroy(&t);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-3333333333333");
  tVariantDestroy(&t);

  // 5. float to other data
  // types////////////////////////////////////////////////////////////////////////
  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -8991212199);

  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.dKey, -8991212199.8987885);

  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.dKey, -8991212199.8987885);

  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-8991212199.898788");
  tVariantDestroy(&t);

  _init_tvariant_float(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-8991212199.898788");
  tVariantDestroy(&t);

  // 6. binary to other data types
  // //////////////////////////////////////////////////////////////////
  t.pz = "true";
  t.nLen = strlen(t.pz);
  t.nType = TSDB_DATA_TYPE_BINARY;
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), -1);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.dKey, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.dKey, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "2e5");
  tVariantDestroy(&t);

  _init_tvariant_binary(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"2e5");
  tVariantDestroy(&t);

  // 7. nchar to other data types
  // //////////////////////////////////////////////////////////////////
  t.wpz = L"FALSE";
  t.nLen = wcslen(t.wpz);
  t.nType = TSDB_DATA_TYPE_NCHAR;
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_nchar(&t);
  EXPECT_LE(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -2000000);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.dKey, -2000000.8765);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.dKey, -2000000.8765);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-2000000.8765");
  tVariantDestroy(&t);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(tVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-2000000.8765");
  tVariantDestroy(&t);
}

TEST(testCase, tGetToken_Test) {
  char* s = ".123 ";
  uint32_t type = 0;

  int32_t len = tSQLGetToken(s, &type);
  EXPECT_EQ(type, TK_FLOAT);
  EXPECT_EQ(len, strlen(s) - 1);

  char s1[] = "1.123e10 ";
  len = tSQLGetToken(s1, &type);
  EXPECT_EQ(type, TK_FLOAT);
  EXPECT_EQ(len, strlen(s1) - 1);

  char s4[] = "0xff ";
  len = tSQLGetToken(s4, &type);
  EXPECT_EQ(type, TK_HEX);
  EXPECT_EQ(len, strlen(s4) - 1);

  // invalid data type
  char s2[] = "e10 ";
  len = tSQLGetToken(s2, &type);
  EXPECT_FALSE(type == TK_FLOAT);

  char s3[] = "1.1.1.1";
  len = tSQLGetToken(s3, &type);
  EXPECT_EQ(type, TK_IPTOKEN);
  EXPECT_EQ(len, strlen(s3));

  char s5[] = "0x ";
  len = tSQLGetToken(s5, &type);
  EXPECT_FALSE(type == TK_HEX);
}

static SStrToken createStrToken(char* s) {
  SStrToken t = {0};//.type = TK_STRING, .z = s, .n = strlen(s)};
  t.type = TK_STRING;
  t.z = s;
  t.n = strlen(s);

  return t;
}

TEST(testCase, isValidNumber_test) {
  SStrToken t1 = createStrToken("123abc");

  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createStrToken("0xabc");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_HEX);

  t1 = createStrToken("0b11101");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_BIN);

  t1 = createStrToken(".134abc");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createStrToken("1e1 ");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createStrToken("1+2");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createStrToken("-0x123");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_HEX);

  t1 = createStrToken("-1");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_INTEGER);

  t1 = createStrToken("-0b1110");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_BIN);

  t1 = createStrToken("-.234");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_FLOAT);
}

TEST(testCase, getTempFilePath_test) {
  char path[4096] = {0};
  memset(path, 1, 4096);

  taosGetTmpfilePath("new_tmp", path);
  printf("%s\n", path);
}

