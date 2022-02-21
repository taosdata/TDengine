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

TEST_F(CfgTest, 01_Basic) {
  SConfig *pConfig = cfgInit();
  cfgAddBool(pConfig, "test_bool", 0, CFG_UTYPE_NONE);
  cfgAddInt8(pConfig, "test_int8", 1, CFG_UTYPE_GB);
  cfgAddUInt8(pConfig, "test_uint8", 2, CFG_UTYPE_MB);
  cfgAddInt16(pConfig, "test_int16", 3, CFG_UTYPE_BYTE);
  cfgAddUInt16(pConfig, "test_uint16", 4, CFG_UTYPE_SECOND);
  cfgAddInt32(pConfig, "test_int32", 5, CFG_UTYPE_MS);
  cfgAddUInt32(pConfig, "test_uint32", 6, CFG_UTYPE_PERCENT);
  cfgAddInt64(pConfig, "test_int64", 7, CFG_UTYPE_NONE);
  cfgAddUInt64(pConfig, "test_uint64", 8, CFG_UTYPE_NONE);
  cfgAddFloat(pConfig, "test_float", 9, CFG_UTYPE_NONE);
  cfgAddDouble(pConfig, "test_double", 10, CFG_UTYPE_NONE);
  cfgAddString(pConfig, "test_string", "11", CFG_UTYPE_NONE);
  cfgAddFqdn(pConfig, "test_fqdn", "localhost", CFG_UTYPE_NONE);
  cfgAddIpStr(pConfig, "test_ipstr", "192.168.0.1", CFG_UTYPE_NONE);
  cfgAddDir(pConfig, "test_dir", "/tmp", CFG_UTYPE_NONE);
  cfgAddFile(pConfig, "test_file", "/tmp/file1", CFG_UTYPE_NONE);

  EXPECT_EQ(cfgGetSize(pConfig), 16);

  int32_t      size = 0;
  SConfigItem *pItem = cfgIterate(pConfig, NULL);
  while (pItem != NULL) {
    pItem = cfgIterate(pConfig, pItem);
    switch (pItem->dtype) {
      case CFG_DTYPE_BOOL:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->boolVal);
        break;
      CFG_DTYPE_INT8:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->uint8Val);
        break;
      CFG_DTYPE_UINT8:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->int8Val);
        break;
      CFG_DTYPE_INT16:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->uint16Val);
        break;
      CFG_DTYPE_UINT16:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->int16Val);
        break;
      CFG_DTYPE_INT32:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->uint32Val);
        break;
      CFG_DTYPE_UINT32:
        printf("cfg:%s, value:%d\n", pItem->name, pItem->int32Val);
        break;
      CFG_DTYPE_INT64:
        printf("cfg:%s, value:%" PRIu64, pItem->name, pItem->uint64Val);
        break;
      CFG_DTYPE_UINT64:
        printf("cfg:%s, value:%" PRId64, pItem->name, pItem->int64Val);
        break;
      CFG_DTYPE_FLOAT:
        printf("cfg:%s, value:%f\n", pItem->name, pItem->floatVal);
        break;
      CFG_DTYPE_DOUBLE:
        printf("cfg:%s, value:%f\n", pItem->name, pItem->doubleVal);
        break;
      CFG_DTYPE_STRING:
        printf("cfg:%s, value:%s\n", pItem->name, pItem->strVal);
        break;
      CFG_DTYPE_FQDN:
        printf("cfg:%s, value:%s\n", pItem->name, pItem->fqdnVal);
        break;
      CFG_DTYPE_IPSTR:
        printf("cfg:%s, value:%s\n", pItem->name, pItem->ipstrVal);
        break;
      CFG_DTYPE_DIR:
        printf("cfg:%s, value:%s\n", pItem->name, pItem->dirVal);
        break;
      CFG_DTYPE_FILE:
        printf("cfg:%s, value:%s\n", pItem->name, pItem->fileVal);
        break;
      default:
        break;
    }
    size++;
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
  EXPECT_EQ(pItem->utype, CFG_UTYPE_SECOND);
  EXPECT_EQ(pItem->dtype, CFG_UTYPE_NONE);
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
