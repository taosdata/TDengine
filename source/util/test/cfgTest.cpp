/**
 * @file cfgTest.cpp
 * @author slguan (slguan@taosdata.com)
 * @brief config module tests
 * @version 1.0
 * @date 2022-02-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <gtest/gtest.h>
#include "tconfig.h"

class CfgTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}
  void InitCfg(SConfig *pConfig);

  static const char *pConfig;
};

const char *CfgTest::pConfig;

TEST_F(CfgTest, 01_Str) {
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_DEFAULT), "default");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_CFG_FILE), "cfg_file");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ENV_FILE), "env_file");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ENV_VAR), "env_var");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ENV_CMD), "env_cmd");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_APOLLO_URL), "apollo_url");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ARG_LIST), "arg_list");
  EXPECT_STREQ(cfgStypeStr(ECfgSrcType(1024)), "invalid");

  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_NONE), "none");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_BOOL), "bool");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT32), "int32");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT64), "int64");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_FLOAT), "float");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_STRING), "string");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(ECfgDataType(1024)), "invalid");
}

TEST_F(CfgTest, 02_Basic) {
  SConfig *pConfig = cfgInit();
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 0), 0);

  EXPECT_EQ(cfgGetSize(pConfig), 6);

  int32_t size = taosArrayGetSize(pConfig->array);
  for (int32_t i = 0; i < size; ++i) {
    SConfigItem *pItem = (SConfigItem *)taosArrayGet(pConfig->array, i);
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->bval);
        break;
      case CFG_DTYPE_INT32:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->i32);
        break;
      case CFG_DTYPE_INT64:
        printf("index:%d, cfg:%s value:%" PRId64 "\n", size, pItem->name, pItem->i64);
        break;
      case CFG_DTYPE_FLOAT:
        printf("index:%d, cfg:%s value:%f\n", size, pItem->name, pItem->fval);
        break;
      case CFG_DTYPE_STRING:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->str);
        break;
      case CFG_DTYPE_DIR:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->str);
        break;
      default:
        printf("index:%d, cfg:%s invalid cfg dtype:%d\n", size, pItem->name, pItem->dtype);
        break;
    }
  }
  EXPECT_EQ(cfgGetSize(pConfig), 6);

  SConfigItem *pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->bval, 0);

  pItem = cfgGetItem(pConfig, "test_int32");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT32);
  EXPECT_STREQ(pItem->name, "test_int32");
  EXPECT_EQ(pItem->i32, 1);

  pItem = cfgGetItem(pConfig, "test_int64");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT64);
  EXPECT_STREQ(pItem->name, "test_int64");
  EXPECT_EQ(pItem->i64, 2);

  pItem = cfgGetItem(pConfig, "test_float");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_FLOAT);
  EXPECT_STREQ(pItem->name, "test_float");
  EXPECT_EQ(pItem->fval, 3);

  pItem = cfgGetItem(pConfig, "test_string");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_STRING);
  EXPECT_STREQ(pItem->name, "test_string");
  EXPECT_STREQ(pItem->str, "4");

  pItem = cfgGetItem(pConfig, "test_dir");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_DIR);
  EXPECT_STREQ(pItem->name, "test_dir");
  EXPECT_STREQ(pItem->str, TD_TMP_DIR_PATH);

  cfgCleanup(pConfig);
}
