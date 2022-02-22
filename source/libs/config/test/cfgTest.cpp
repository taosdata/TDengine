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
#include "config.h"

class CfgTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {}
  static void TearDownTestSuite() {}

 public:
  void SetUp() override {}
  void TearDown() override {}

  void InitializeConfig(SConfig *pConfig);

  static const char *pConfig;
};

const char *CfgTest::pConfig;

TEST_F(CfgTest, 02_Str) {
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_DEFAULT), "default");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_CFG_FILE), "cfg_file");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ENV_FILE), "env_file");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ENV_VAR), "env_var");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_APOLLO_URL), "apollo_url");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ARG_LIST), "arg_list");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_API_OPTION), "api_option");
  EXPECT_STREQ(cfgStypeStr(ECfgSrcType(1024)), "invalid");

  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_NONE), "none");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_BOOL), "bool");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT8), "int8");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_UINT16), "uint16");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT32), "int32");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT64), "int64");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_FLOAT), "float");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_STRING), "string");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_IPSTR), "ipstr");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(ECfgDataType(1024)), "invalid");

  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_NONE), "");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_GB), "(GB)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_MB), "(Mb)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_BYTE), "(byte)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_SECOND), "(s)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_MS), "(ms)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_PERCENT), "(%)");
  EXPECT_STREQ(cfgUtypeStr(ECfgUnitType(1024)), "invalid");
}

TEST_F(CfgTest, 02_Basic) {
  SConfig *pConfig = cfgInit();
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddInt8(pConfig, "test_int8", 1, 0, 16, CFG_UTYPE_GB), 0);
  EXPECT_EQ(cfgAddUInt16(pConfig, "test_uint16", 2, 0, 16, CFG_UTYPE_MB), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 3, 0, 16, CFG_UTYPE_BYTE), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 4, 0, 16, CFG_UTYPE_SECOND), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 5, 0, 16, CFG_UTYPE_MS), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "6", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddIpStr(pConfig, "test_ipstr", "192.168.0.1", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", "/tmp", CFG_UTYPE_NONE), 0);

  EXPECT_EQ(cfgGetSize(pConfig), 9);

  int32_t      size = 0;
  SConfigItem *pItem = cfgIterate(pConfig, NULL);
  while (pItem != NULL) {
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->boolVal);
        break;
      case CFG_DTYPE_INT8:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->int8Val);
        break;
      case CFG_DTYPE_UINT16:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->uint16Val);
        break;
      case CFG_DTYPE_INT32:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->int32Val);
        break;
      case CFG_DTYPE_INT64:
        printf("index:%d, cfg:%s value:%" PRId64 "\n", size, pItem->name, pItem->int64Val);
        break;
      case CFG_DTYPE_FLOAT:
        printf("index:%d, cfg:%s value:%f\n", size, pItem->name, pItem->floatVal);
        break;
      case CFG_DTYPE_STRING:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->strVal);
        break;
      case CFG_DTYPE_IPSTR:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->ipstrVal);
        break;
      case CFG_DTYPE_DIR:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->dirVal);
        break;
      default:
        printf("index:%d, cfg:%s invalid cfg dtype:%d\n", size, pItem->name, pItem->dtype);
        break;
    }
    size++;
    pItem = cfgIterate(pConfig, pItem);
  }
  cfgCancelIterate(pConfig, pItem);

  EXPECT_EQ(cfgGetSize(pConfig), 9);

  pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->boolVal, 0);

  pItem = cfgGetItem(pConfig, "test_int8");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_GB);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT8);
  EXPECT_STREQ(pItem->name, "test_int8");
  EXPECT_EQ(pItem->int8Val, 1);

  pItem = cfgGetItem(pConfig, "test_uint16");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_MB);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_UINT16);
  EXPECT_STREQ(pItem->name, "test_uint16");
  EXPECT_EQ(pItem->uint16Val, 2);

  pItem = cfgGetItem(pConfig, "test_int32");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_BYTE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT32);
  EXPECT_STREQ(pItem->name, "test_int32");
  EXPECT_EQ(pItem->int32Val, 3);

  pItem = cfgGetItem(pConfig, "test_int64");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_SECOND);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT64);
  EXPECT_STREQ(pItem->name, "test_int64");
  EXPECT_EQ(pItem->int64Val, 4);

  pItem = cfgGetItem(pConfig, "test_float");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_MS);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_FLOAT);
  EXPECT_STREQ(pItem->name, "test_float");
  EXPECT_EQ(pItem->floatVal, 5);

  pItem = cfgGetItem(pConfig, "test_string");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_STRING);
  EXPECT_STREQ(pItem->name, "test_string");
  EXPECT_STREQ(pItem->strVal, "6");

  pItem = cfgGetItem(pConfig, "test_ipstr");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_IPSTR);
  EXPECT_STREQ(pItem->name, "test_ipstr");
  EXPECT_STREQ(pItem->ipstrVal, "192.168.0.1");

  pItem = cfgGetItem(pConfig, "test_dir");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_DIR);
  EXPECT_STREQ(pItem->name, "test_dir");
  EXPECT_STREQ(pItem->dirVal, "/tmp");

  cfgCleanup(pConfig);
}
