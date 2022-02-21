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
  EXPECT_STREQ(cfgStypeStr(CFG_TYPE_DEFAULT), "default");
  EXPECT_STREQ(cfgStypeStr(CFG_TYPE_CFG_FILE), "cfg");
  EXPECT_STREQ(cfgStypeStr(CFG_TYPE_DOT_ENV), ".env");
  EXPECT_STREQ(cfgStypeStr(CFG_TYPE_ENV_VAR), "env");
  EXPECT_STREQ(cfgStypeStr(CFG_TYPE_APOLLO_URL), "apollo");
  EXPECT_STREQ(cfgStypeStr(ECfgSrcType(1024)), "invalid");

  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_NONE), "none");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_BOOL), "bool");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT8), "int8");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_UINT8), "uint8");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT16), "int16");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_UINT16), "uint16");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT32), "int32");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_UINT32), "uint32");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_INT64), "int64");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_UINT64), "uint64");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_FLOAT), "float");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DOUBLE), "double");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_STRING), "string");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_FQDN), "fqdn");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_IPSTR), "ipstr");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DIR), "dir");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_FILE), "file");
  EXPECT_STREQ(cfgDtypeStr(ECfgDataType(1024)), "invalid");

  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_NONE), "");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_PERCENT), "(%)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_GB), "(GB)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_MB), "(Mb)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_BYTE), "(byte)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_SECOND), "(s)");
  EXPECT_STREQ(cfgUtypeStr(CFG_UTYPE_MS), "(ms)");
  EXPECT_STREQ(cfgUtypeStr(ECfgUnitType(1024)), "invalid");
}

TEST_F(CfgTest, 02_Basic) {
  SConfig *pConfig = cfgInit();
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddInt8(pConfig, "test_int8", 1, CFG_UTYPE_GB), 0);
  EXPECT_EQ(cfgAddUInt8(pConfig, "test_uint8", 2, CFG_UTYPE_MB), 0);
  EXPECT_EQ(cfgAddInt16(pConfig, "test_int16", 3, CFG_UTYPE_BYTE), 0);
  EXPECT_EQ(cfgAddUInt16(pConfig, "test_uint16", 4, CFG_UTYPE_SECOND), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 5, CFG_UTYPE_MS), 0);
  EXPECT_EQ(cfgAddUInt32(pConfig, "test_uint32", 6, CFG_UTYPE_PERCENT), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 7, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddUInt64(pConfig, "test_uint64", 8, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 9, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddDouble(pConfig, "test_double", 10, CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "11", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddFqdn(pConfig, "test_fqdn", "localhost", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddIpStr(pConfig, "test_ipstr", "192.168.0.1", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", "/tmp", CFG_UTYPE_NONE), 0);
  EXPECT_EQ(cfgAddFile(pConfig, "test_file", "/tmp/file1", CFG_UTYPE_NONE), 0);

  EXPECT_EQ(cfgGetSize(pConfig), 16);

  int32_t      size = 0;
  SConfigItem *pItem = cfgIterate(pConfig, NULL);
  while (pItem != NULL) {
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->boolVal);
        break;
      case CFG_DTYPE_INT8:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->uint8Val);
        break;
      case CFG_DTYPE_UINT8:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->int8Val);
        break;
      case CFG_DTYPE_INT16:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->uint16Val);
        break;
      case CFG_DTYPE_UINT16:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->int16Val);
        break;
      case CFG_DTYPE_INT32:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->uint32Val);
        break;
      case CFG_DTYPE_UINT32:
        printf("index:%d, cfg:%s value:%d\n", size, pItem->name, pItem->int32Val);
        break;
      case CFG_DTYPE_INT64:
        printf("index:%d, cfg:%s value:%" PRIu64 "\n", size, pItem->name, pItem->uint64Val);
        break;
      case CFG_DTYPE_UINT64:
        printf("index:%d, cfg:%s value:%" PRId64 "\n", size, pItem->name, pItem->int64Val);
        break;
      case CFG_DTYPE_FLOAT:
        printf("index:%d, cfg:%s value:%f\n", size, pItem->name, pItem->floatVal);
        break;
      case CFG_DTYPE_DOUBLE:
        printf("index:%d, cfg:%s value:%f\n", size, pItem->name, pItem->doubleVal);
        break;
      case CFG_DTYPE_STRING:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->strVal);
        break;
      case CFG_DTYPE_FQDN:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->fqdnVal);
        break;
      case CFG_DTYPE_IPSTR:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->ipstrVal);
        break;
      case CFG_DTYPE_DIR:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->dirVal);
        break;
      case CFG_DTYPE_FILE:
        printf("index:%d, cfg:%s value:%s\n", size, pItem->name, pItem->fileVal);
        break;
      default:
        printf("index:%d, cfg:%s invalid cfg dtype:%d\n", size, pItem->name, pItem->dtype);
        break;
    }
    size++;
    pItem = cfgIterate(pConfig, pItem);
  }
  cfgCancelIterate(pConfig, pItem);

  EXPECT_EQ(cfgGetSize(pConfig), 16);

  pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->boolVal, 0);

  pItem = cfgGetItem(pConfig, "test_int8");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_GB);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT8);
  EXPECT_STREQ(pItem->name, "test_int8");
  EXPECT_EQ(pItem->int8Val, 1);

  pItem = cfgGetItem(pConfig, "test_uint8");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_MB);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_UINT8);
  EXPECT_STREQ(pItem->name, "test_uint8");
  EXPECT_EQ(pItem->uint8Val, 2);

  pItem = cfgGetItem(pConfig, "test_int16");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_BYTE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT16);
  EXPECT_STREQ(pItem->name, "test_int16");
  EXPECT_EQ(pItem->int16Val, 3);

  pItem = cfgGetItem(pConfig, "test_uint16");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_SECOND);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_UINT16);
  EXPECT_STREQ(pItem->name, "test_uint16");
  EXPECT_EQ(pItem->uint16Val, 4);

  pItem = cfgGetItem(pConfig, "test_int32");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_MS);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT32);
  EXPECT_STREQ(pItem->name, "test_int32");
  EXPECT_EQ(pItem->int32Val, 5);

  pItem = cfgGetItem(pConfig, "test_uint32");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_PERCENT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_UINT32);
  EXPECT_STREQ(pItem->name, "test_uint32");
  EXPECT_EQ(pItem->uint32Val, 6);

  pItem = cfgGetItem(pConfig, "test_int64");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT64);
  EXPECT_STREQ(pItem->name, "test_int64");
  EXPECT_EQ(pItem->int64Val, 7);

  pItem = cfgGetItem(pConfig, "test_uint64");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_UINT64);
  EXPECT_STREQ(pItem->name, "test_uint64");
  EXPECT_EQ(pItem->uint64Val, 8);

  pItem = cfgGetItem(pConfig, "test_float");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_FLOAT);
  EXPECT_STREQ(pItem->name, "test_float");
  EXPECT_EQ(pItem->floatVal, 9);

  pItem = cfgGetItem(pConfig, "test_double");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_DOUBLE);
  EXPECT_STREQ(pItem->name, "test_double");
  EXPECT_EQ(pItem->doubleVal, 10);

  pItem = cfgGetItem(pConfig, "test_string");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_STRING);
  EXPECT_STREQ(pItem->name, "test_string");
  EXPECT_STREQ(pItem->strVal, "11");

  pItem = cfgGetItem(pConfig, "test_fqdn");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_FQDN);
  EXPECT_STREQ(pItem->name, "test_fqdn");
  EXPECT_STREQ(pItem->strVal, "localhost");

  pItem = cfgGetItem(pConfig, "test_ipstr");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_IPSTR);
  EXPECT_STREQ(pItem->name, "test_ipstr");
  EXPECT_STREQ(pItem->ipstrVal, "192.168.0.1");

  pItem = cfgGetItem(pConfig, "test_dir");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_DIR);
  EXPECT_STREQ(pItem->name, "test_dir");
  EXPECT_STREQ(pItem->dirVal, "/tmp");

  pItem = cfgGetItem(pConfig, "test_file");
  EXPECT_EQ(pItem->stype, CFG_TYPE_DEFAULT);
  EXPECT_EQ(pItem->utype, CFG_UTYPE_NONE);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_FILE);
  EXPECT_STREQ(pItem->name, "test_file");
  EXPECT_STREQ(pItem->fileVal, "/tmp/file1");

  cfgCleanup(pConfig);
}
