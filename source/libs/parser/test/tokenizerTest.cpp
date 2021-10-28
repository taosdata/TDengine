#include <gtest/gtest.h>
#include <iostream>
#pragma GCC diagnostic ignored "-Wwrite-strings"

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "taos.h"
#include "tvariant.h"
#include "tdef.h"
#include "ttoken.h"
#include "astGenerator.h"
#include "parserUtil.h"
#include "parserInt.h"

namespace {
int32_t testValidateName(char* name) {
  SToken token = {0};
  token.z = name;
  token.n = strlen(name);
  token.type = 0;

  tGetToken(name, &token.type);
  return parserValidateIdToken(&token);
}

SToken createToken(char* s) {
  SToken t = {0};

  t.type = TK_STRING;
  t.z = s;
  t.n = strlen(s);
  return t;
}
}  // namespace

static void _init_tvariant_bool(SVariant* t) {
  t->i64 = TSDB_FALSE;
  t->nType = TSDB_DATA_TYPE_BOOL;
}

static void _init_tvariant_tinyint(SVariant* t) {
  t->i64 = -27;
  t->nType = TSDB_DATA_TYPE_TINYINT;
}

static void _init_tvariant_int(SVariant* t) {
  t->i64 = -23997659;
  t->nType = TSDB_DATA_TYPE_INT;
}

static void _init_tvariant_bigint(SVariant* t) {
  t->i64 = -3333333333333;
  t->nType = TSDB_DATA_TYPE_BIGINT;
}

static void _init_tvariant_float(SVariant* t) {
  t->d = -8991212199.8987878776;
  t->nType = TSDB_DATA_TYPE_FLOAT;
}

static void _init_tvariant_binary(SVariant* t) {
  taosVariantDestroy(t);

  t->pz = (char*)calloc(1, 20);  //"2e3");
  t->nType = TSDB_DATA_TYPE_BINARY;
  strcpy(t->pz, "2e5");
  t->nLen = strlen(t->pz);
}

static void _init_tvariant_nchar(SVariant* t) {
  taosVariantDestroy(t);

  t->wpz = (wchar_t*)calloc(1, 20 * TSDB_NCHAR_SIZE);
  t->nType = TSDB_DATA_TYPE_NCHAR;
  wcscpy(t->wpz, L"-2000000.8765");
  t->nLen = twcslen(t->wpz);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, validateToken_test) {
  char t01[] = "abc";
  EXPECT_EQ(testValidateName(t01), TSDB_CODE_SUCCESS);

  char t110[] = "`1233abc.911`";
  EXPECT_EQ(testValidateName(t110), TSDB_CODE_SUCCESS);

  char t02[] = "'abc'";
  EXPECT_EQ(testValidateName(t02), TSDB_CODE_TSC_INVALID_OPERATION);

  char t1[] = "abc.def";
  EXPECT_EQ(testValidateName(t1), TSDB_CODE_SUCCESS);
  printf("%s\n", t1);

  char t98[] = "abc.DeF";
  EXPECT_EQ(testValidateName(t98), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(t98, "abc.def");
  printf("%s\n", t98);

  char t97[] = "257.abc";
  EXPECT_EQ(testValidateName(t97), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t97);

  char t96[] = "_257.aBc";
  EXPECT_EQ(testValidateName(t96), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(t96, "_257.abc");
  printf("%s\n", t96);

  char t99[] = "abc . def";
  EXPECT_EQ(testValidateName(t99), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t99);

  char t2[] = "'abc.def'";
  EXPECT_EQ(testValidateName(t2), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t2);

  char t3[] = "'abc'.def";
  EXPECT_EQ(testValidateName(t3), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t3);

  char t4[] = "'abc'.'def'";
  EXPECT_EQ(testValidateName(t4), TSDB_CODE_TSC_INVALID_OPERATION);

  char t5[] = "table.'def'";
  EXPECT_EQ(testValidateName(t5), TSDB_CODE_TSC_INVALID_OPERATION);

  char t6[] = "'table'.'def'";
  EXPECT_EQ(testValidateName(t6), TSDB_CODE_TSC_INVALID_OPERATION);

  char t7[] = "'_ab1234'.'def'";
  EXPECT_EQ(testValidateName(t7), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t7);

  char t8[] = "'_ab&^%1234'.'def'";
  EXPECT_EQ(testValidateName(t8), TSDB_CODE_TSC_INVALID_OPERATION);

  char t9[] = "'_123'.'gtest中文'";
  EXPECT_EQ(testValidateName(t9), TSDB_CODE_TSC_INVALID_OPERATION);

  char t10[] = "abc.'gtest中文'";
  EXPECT_EQ(testValidateName(t10), TSDB_CODE_TSC_INVALID_OPERATION);

  char t10_1[] = "abc.'中文gtest'";
  EXPECT_EQ(testValidateName(t10_1), TSDB_CODE_TSC_INVALID_OPERATION);

  char t11[] = "'192.168.0.1'.abc";
  EXPECT_EQ(testValidateName(t11), TSDB_CODE_TSC_INVALID_OPERATION);

  char t12[] = "192.168.0.1.abc";
  EXPECT_EQ(testValidateName(t12), TSDB_CODE_TSC_INVALID_OPERATION);

  char t13[] = "abc.";
  EXPECT_EQ(testValidateName(t13), TSDB_CODE_TSC_INVALID_OPERATION);

  char t14[] = ".abc";
  EXPECT_EQ(testValidateName(t14), TSDB_CODE_TSC_INVALID_OPERATION);

  char t15[] = ".'abc'";
  EXPECT_EQ(testValidateName(t15), TSDB_CODE_TSC_INVALID_OPERATION);

  char t16[] = ".abc'";
  EXPECT_EQ(testValidateName(t16), TSDB_CODE_TSC_INVALID_OPERATION);

  char t17[] = "123a.\"abc\"";
  EXPECT_EQ(testValidateName(t17), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t17);

  char t18[] = "a.\"abc\"";
  EXPECT_EQ(testValidateName(t18), TSDB_CODE_TSC_INVALID_OPERATION);
  printf("%s\n", t18);

  char t19[] = "'_ab1234'.'def'.'ab123'";
  EXPECT_EQ(testValidateName(t19), TSDB_CODE_TSC_INVALID_OPERATION);

  char t20[] = "'_ab1234*&^'";
  EXPECT_EQ(testValidateName(t20), TSDB_CODE_TSC_INVALID_OPERATION);

  char t21[] = "'1234_abc'";
  EXPECT_EQ(testValidateName(t21), TSDB_CODE_TSC_INVALID_OPERATION);

  // =======Containing capital letters=================
  char t30[] = "ABC";
  EXPECT_EQ(testValidateName(t30), TSDB_CODE_SUCCESS);

  char t31[] = "'ABC'";
  EXPECT_EQ(testValidateName(t31), TSDB_CODE_TSC_INVALID_OPERATION);

  char t32[] = "ABC.def";
  EXPECT_EQ(testValidateName(t32), TSDB_CODE_SUCCESS);

  char t33[] = "'ABC.def";
  EXPECT_EQ(testValidateName(t33), TSDB_CODE_TSC_INVALID_OPERATION);

  char t33_0[] = "abc.DEF'";
  EXPECT_EQ(testValidateName(t33_0), TSDB_CODE_TSC_INVALID_OPERATION);

  char t34[] = "'ABC.def'";
  // int32_t tmp0 = testValidateName(t34);
  EXPECT_EQ(testValidateName(t34), TSDB_CODE_TSC_INVALID_OPERATION);

  char t35[] = "'ABC'.def";
  EXPECT_EQ(testValidateName(t35), TSDB_CODE_TSC_INVALID_OPERATION);

  char t36[] = "ABC.DEF";
  EXPECT_EQ(testValidateName(t36), TSDB_CODE_SUCCESS);

  char t37[] = "abc.DEF";
  EXPECT_EQ(testValidateName(t37), TSDB_CODE_SUCCESS);

  char t37_1[] = "abc._123DEF";
  EXPECT_EQ(testValidateName(t37_1), TSDB_CODE_SUCCESS);

  char t38[] = "'abc'.\"DEF\"";
  EXPECT_EQ(testValidateName(t38), TSDB_CODE_TSC_INVALID_OPERATION);

  // do not use key words
  char t39[] = "table.'DEF'";
  EXPECT_EQ(testValidateName(t39), TSDB_CODE_TSC_INVALID_OPERATION);

  char t40[] = "'table'.'DEF'";
  EXPECT_EQ(testValidateName(t40), TSDB_CODE_TSC_INVALID_OPERATION);

  char t41[] = "'_abXYZ1234'.'deFF'";
  EXPECT_EQ(testValidateName(t41), TSDB_CODE_TSC_INVALID_OPERATION);

  char t42[] = "'_abDEF&^%1234'.'DIef'";
  EXPECT_EQ(testValidateName(t42), TSDB_CODE_TSC_INVALID_OPERATION);

  char t43[] = "'_123'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t43), TSDB_CODE_TSC_INVALID_OPERATION);

  char t44[] = "'aABC'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t44), TSDB_CODE_TSC_INVALID_OPERATION);

  char t45[] = "'ABC'.";
  EXPECT_EQ(testValidateName(t45), TSDB_CODE_TSC_INVALID_OPERATION);

  char t46[] = ".'ABC'";
  EXPECT_EQ(testValidateName(t46), TSDB_CODE_TSC_INVALID_OPERATION);

  char t47[] = "a.\"aTWc\"";
  EXPECT_EQ(testValidateName(t47), TSDB_CODE_TSC_INVALID_OPERATION);

  // ================has space =================
  char t60[] = " ABC ";
  EXPECT_EQ(testValidateName(t60), TSDB_CODE_TSC_INVALID_OPERATION);

  char t60_1[] = "   ABC ";
  EXPECT_EQ(testValidateName(t60_1), TSDB_CODE_TSC_INVALID_OPERATION);

  char t61[] = "' ABC '";
  EXPECT_EQ(testValidateName(t61), TSDB_CODE_TSC_INVALID_OPERATION);

  char t61_1[] = "'  ABC '";
  EXPECT_EQ(testValidateName(t61_1), TSDB_CODE_TSC_INVALID_OPERATION);

  char t62[] = " ABC . def ";
  EXPECT_EQ(testValidateName(t62), TSDB_CODE_TSC_INVALID_OPERATION);

  char t63[] = "' ABC . def ";
  EXPECT_EQ(testValidateName(t63), TSDB_CODE_TSC_INVALID_OPERATION);

  char t63_0[] = "  abc . DEF ' ";
  EXPECT_EQ(testValidateName(t63_0), TSDB_CODE_TSC_INVALID_OPERATION);

  char t64[] = " '  ABC .  def ' ";
  // int32_t tmp1 = testValidateName(t64);
  EXPECT_EQ(testValidateName(t64), TSDB_CODE_TSC_INVALID_OPERATION);

  char t65[] = " ' ABC  '. def ";
  EXPECT_EQ(testValidateName(t65), TSDB_CODE_TSC_INVALID_OPERATION);

  char t66[] = "' ABC '.'  DEF '";
  EXPECT_EQ(testValidateName(t66), TSDB_CODE_TSC_INVALID_OPERATION);

  char t67[] = "abc . '  DEF  '";
  EXPECT_EQ(testValidateName(t67), TSDB_CODE_TSC_INVALID_OPERATION);

  char t68[] = "'  abc '.'   DEF '";
  EXPECT_EQ(testValidateName(t68), TSDB_CODE_TSC_INVALID_OPERATION);

  // do not use key words
  char t69[] = "table.'DEF'";
  EXPECT_EQ(testValidateName(t69), TSDB_CODE_TSC_INVALID_OPERATION);

  char t70[] = "'table'.'DEF'";
  EXPECT_EQ(testValidateName(t70), TSDB_CODE_TSC_INVALID_OPERATION);

  char t71[] = "'_abXYZ1234  '.' deFF  '";
  EXPECT_EQ(testValidateName(t71), TSDB_CODE_TSC_INVALID_OPERATION);

  char t72[] = "'_abDEF&^%1234'.'  DIef'";
  EXPECT_EQ(testValidateName(t72), TSDB_CODE_TSC_INVALID_OPERATION);

  char t73[] = "'_123'.'  Gtest中文'";
  EXPECT_EQ(testValidateName(t73), TSDB_CODE_TSC_INVALID_OPERATION);

  char t74[] = "' aABC'.'Gtest中文'";
  EXPECT_EQ(testValidateName(t74), TSDB_CODE_TSC_INVALID_OPERATION);

  char t75[] = "' ABC '.";
  EXPECT_EQ(testValidateName(t75), TSDB_CODE_TSC_INVALID_OPERATION);

  char t76[] = ".' ABC'";
  EXPECT_EQ(testValidateName(t76), TSDB_CODE_TSC_INVALID_OPERATION);

  char t77[] = " a . \"aTWc\" ";
  EXPECT_EQ(testValidateName(t77), TSDB_CODE_TSC_INVALID_OPERATION);

  char t78[] = "  a.\"aTWc  \"";
  EXPECT_EQ(testValidateName(t78), TSDB_CODE_TSC_INVALID_OPERATION);

  // ===============muti string by space ===================
  // There's no such case.
  // char t160[] = "A BC";
  // EXPECT_EQ(testValidateName(t160), TSDB_CODE_TSC_INVALID_OPERATION);
  // printf("end:%s\n", t160);

  // There's no such case.
  // char t161[] = "' A BC '";
  // EXPECT_EQ(testValidateName(t161), TSDB_CODE_TSC_INVALID_OPERATION);

  char t162[] = " AB C . de f ";
  EXPECT_EQ(testValidateName(t162), TSDB_CODE_TSC_INVALID_OPERATION);

  char t163[] = "' AB C . de f ";
  EXPECT_EQ(testValidateName(t163), TSDB_CODE_TSC_INVALID_OPERATION);

  char t163_0[] = "  ab c . DE F ' ";
  EXPECT_EQ(testValidateName(t163_0), TSDB_CODE_TSC_INVALID_OPERATION);

  char t164[] = " '  AB C .  de f ' ";
  // int32_t tmp2 = testValidateName(t164);
  EXPECT_EQ(testValidateName(t164), TSDB_CODE_TSC_INVALID_OPERATION);

  char t165[] = " ' A BC  '. de f ";
  EXPECT_EQ(testValidateName(t165), TSDB_CODE_TSC_INVALID_OPERATION);

  char t166[] = "' AB C '.'  DE  F '";
  EXPECT_EQ(testValidateName(t166), TSDB_CODE_TSC_INVALID_OPERATION);

  char t167[] = "ab  c . '  D  EF  '";
  EXPECT_EQ(testValidateName(t167), TSDB_CODE_TSC_INVALID_OPERATION);

  char t168[] = "'  a bc '.'   DE  F '";
  EXPECT_EQ(testValidateName(t168), TSDB_CODE_TSC_INVALID_OPERATION);
}

#if 0
TEST(testCase, tvariant_convert) {
  // 1. bool data to all other data types
  SVariant t = {0};
  _init_tvariant_bool(&t);

  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.d, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.d, 0);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "FALSE");
  taosVariantDestroy(&t);

  _init_tvariant_bool(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"FALSE");
  taosVariantDestroy(&t);

  // 2. tinyint to other data types
  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.d, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.d, -27);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-27");
  taosVariantDestroy(&t);

  _init_tvariant_tinyint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-27");
  taosVariantDestroy(&t);

  // 3. int to other data
  // types//////////////////////////////////////////////////////////////////
  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);
  EXPECT_EQ(t.i64, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.d, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.d, -23997659);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-23997659");
  taosVariantDestroy(&t);

  _init_tvariant_int(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-23997659");
  taosVariantDestroy(&t);

  // 4. bigint to other data
  // type//////////////////////////////////////////////////////////////////////////////
  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_TINYINT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_SMALLINT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_INT), 0);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_EQ(t.d, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_EQ(t.d, -3333333333333);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-3333333333333");
  taosVariantDestroy(&t);

  _init_tvariant_bigint(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-3333333333333");
  taosVariantDestroy(&t);

  // 5. float to other data
  // types////////////////////////////////////////////////////////////////////////
  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -8991212199);

  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.d, -8991212199.8987885);

  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.d, -8991212199.8987885);

  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-8991212199.898788");
  taosVariantDestroy(&t);

  _init_tvariant_float(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-8991212199.898788");
  taosVariantDestroy(&t);

  // 6. binary to other data types
  // //////////////////////////////////////////////////////////////////
  t.pz = "true";
  t.nLen = strlen(t.pz);
  t.nType = TSDB_DATA_TYPE_BINARY;
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 1);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), -1);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.d, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.d, 200000);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "2e5");
  taosVariantDestroy(&t);

  _init_tvariant_binary(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"2e5");
  taosVariantDestroy(&t);

  // 7. nchar to other data types
  // //////////////////////////////////////////////////////////////////
  t.wpz = L"FALSE";
  t.nLen = wcslen(t.wpz);
  t.nType = TSDB_DATA_TYPE_NCHAR;
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);
  EXPECT_EQ(t.i64, 0);

  _init_tvariant_nchar(&t);
  EXPECT_LE(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BOOL), 0);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BIGINT), 0);
  EXPECT_EQ(t.i64, -2000000);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_FLOAT), 0);
  EXPECT_DOUBLE_EQ(t.d, -2000000.8765);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_DOUBLE), 0);
  EXPECT_DOUBLE_EQ(t.d, -2000000.8765);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_BINARY), 0);
  EXPECT_STREQ(t.pz, "-2000000.8765");
  taosVariantDestroy(&t);

  _init_tvariant_nchar(&t);
  EXPECT_EQ(taosVariantTypeSetType(&t, TSDB_DATA_TYPE_NCHAR), 0);
  EXPECT_STREQ(t.wpz, L"-2000000.8765");
  taosVariantDestroy(&t);
}
#endif

TEST(testCase, tGetToken_Test) {
  char*    s = ".123 ";
  uint32_t type = 0;

  int32_t len = tGetToken(s, &type);
  EXPECT_EQ(type, TK_FLOAT);
  EXPECT_EQ(len, strlen(s) - 1);

  char s1[] = "1.123e10 ";
  len = tGetToken(s1, &type);
  EXPECT_EQ(type, TK_FLOAT);
  EXPECT_EQ(len, strlen(s1) - 1);

  char s4[] = "0xff ";
  len = tGetToken(s4, &type);
  EXPECT_EQ(type, TK_HEX);
  EXPECT_EQ(len, strlen(s4) - 1);

  // invalid data type
  char s2[] = "e10 ";
  len = tGetToken(s2, &type);
  EXPECT_FALSE(type == TK_FLOAT);

  char s3[] = "1.1.1.1";
  len = tGetToken(s3, &type);
  EXPECT_EQ(type, TK_IPTOKEN);
  EXPECT_EQ(len, strlen(s3));

  char s5[] = "0x ";
  len = tGetToken(s5, &type);
  EXPECT_FALSE(type == TK_HEX);
}

TEST(testCase, isValidNumber_test) {
  SToken t1 = createToken("123abc");

  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createToken("0xabc");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_HEX);

  t1 = createToken("0b11101");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_BIN);

  t1 = createToken(".134abc");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createToken("1e1 ");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createToken("1+2");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_ILLEGAL);

  t1 = createToken("-0x123");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_HEX);

  t1 = createToken("-1");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_INTEGER);

  t1 = createToken("-0b1110");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_BIN);

  t1 = createToken("-.234");
  EXPECT_EQ(tGetNumericStringType(&t1), TK_FLOAT);
}

TEST(testCase, generateAST_test) {
  SSqlInfo info = doGenerateAST("select * from t1 where ts < now");
  ASSERT_EQ(info.valid, true);

  SSqlInfo info1 = doGenerateAST("select * from `t.1abc` where ts<now+2h  and col < 20+99");
  ASSERT_EQ(info1.valid, true);

  char msg[128] = {0};

  SMsgBuf msgBuf = {0};
  msgBuf.buf = msg;
  msgBuf.len = 128;

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &msgBuf);
  ASSERT_EQ(code, 0);

  SSqlInfo info2 = doGenerateAST("select * from abc where ts<now+2");
  SSqlNode* pNode2 = (SSqlNode*) taosArrayGetP(((SArray*)info2.list), 0);
  code = evaluateSqlNode(pNode2, TSDB_TIME_PRECISION_MILLI, &msgBuf);
  ASSERT_NE(code, 0);

  destroySqlInfo(&info);
  destroySqlInfo(&info1);
  destroySqlInfo(&info2);
}

TEST(testCase, evaluateAST_test) {
  SSqlInfo info1 = doGenerateAST("select a, b+22 from `t.1abc` where ts<now+2h and `col` < 20 + 99");
  ASSERT_EQ(info1.valid, true);

  char msg[128] = {0};
  SMsgBuf msgBuf = {0};
  msgBuf.buf = msg;
  msgBuf.len = 128;

  SSqlNode* pNode = (SSqlNode*) taosArrayGetP(((SArray*)info1.list), 0);
  int32_t code = evaluateSqlNode(pNode, TSDB_TIME_PRECISION_NANO, &msgBuf);
  ASSERT_EQ(code, 0);
  destroySqlInfo(&info1);
}

TEST(testCase, extractMeta_test) {
  SSqlInfo info1 = doGenerateAST("select a, b+22 from `t.1abc` where ts<now+2h and `col` < 20 + 99");
  ASSERT_EQ(info1.valid, true);

  char msg[128] = {0};
  SMetaReq req  = {0};
  int32_t ret = qParserExtractRequestedMetaInfo(&info1, &req, msg, 128);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(taosArrayGetSize(req.pTableName), 1);

  qParserClearupMetaRequestInfo(&req);
  destroySqlInfo(&info1);
}

