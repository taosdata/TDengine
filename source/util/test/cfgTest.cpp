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

#ifndef WINDOWS
#include "osFile.h"
#endif

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
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_TAOS_OPTIONS), "taos_options");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ALTER_CLIENT_CMD), "alter_client_cmd");
  EXPECT_STREQ(cfgStypeStr(CFG_STYPE_ALTER_SERVER_CMD), "alter_server_cmd");
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
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_DOUBLE), "double");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_LOCALE), "locale");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_CHARSET), "charset");
  EXPECT_STREQ(cfgDtypeStr(CFG_DTYPE_TIMEZONE), "timezone");
  EXPECT_STREQ(cfgDtypeStr(ECfgDataType(1024)), "invalid");
}

TEST_F(CfgTest, 02_Basic) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0), 0);

  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 6, 0), 0);

  EXPECT_EQ(cfgGetSize(pConfig), 6);

  int32_t size = cfgGetSize(pConfig);

  SConfigItem *pItem = NULL;
  SConfigIter *pIter = NULL;
  code = cfgCreateIter(pConfig, &pIter);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pIter, nullptr);

  while ((pItem = cfgNextIter(pIter)) != NULL) {
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

  cfgDestroyIter(pIter);

  EXPECT_EQ(cfgGetSize(pConfig), 6);

  pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->bval, 0);

  pItem = cfgGetItem(pConfig, "test_int32");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT32);
  EXPECT_STREQ(pItem->name, "test_int32");
  EXPECT_EQ(pItem->i32, 1);
  code = cfgSetItem(pConfig, "test_int32", "21", CFG_STYPE_DEFAULT, true);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

  pItem = cfgGetItem(pConfig, "test_int64");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_INT64);
  EXPECT_STREQ(pItem->name, "test_int64");
  EXPECT_EQ(pItem->i64, 2);
  code = cfgSetItem(pConfig, "test_int64", "21", CFG_STYPE_DEFAULT, true);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

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

  code = cfgGetAndSetItem(pConfig, &pItem, "err_cfg", "err_val", CFG_STYPE_DEFAULT, true);
  ASSERT_EQ(code, TSDB_CODE_CFG_NOT_FOUND);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_int32", "4", false, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_INVALID_CFG);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_int64", "4", true, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_INVALID_CFG);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_bool", "3", false, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_int32", "74", true, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_int64", "74", false, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

  code = cfgCheckRangeForDynUpdate(pConfig, "test_float", "74", false, CFG_ALTER_LOCAL);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_RANGE);

  cfgCleanup(pConfig);
}

TEST_F(CfgTest, initWithArray) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 0, 0), 0);

  SArray      *pArgs = taosArrayInit(6, sizeof(SConfigPair));
  SConfigPair *pPair = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair->name = "test_bool";
  pPair->value = "1";
  taosArrayPush(pArgs, pPair);
  SConfigPair *pPair1 = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair1->name = "test_int32";
  pPair1->value = "2";
  taosArrayPush(pArgs, pPair1);
  SConfigPair *pPair2 = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair2->name = "test_int64";
  pPair2->value = "3";
  taosArrayPush(pArgs, pPair2);
  SConfigPair *pPair3 = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair3->name = "test_float";
  pPair3->value = "4";
  taosArrayPush(pArgs, pPair3);
  SConfigPair *pPair4 = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair4->name = "test_string";
  pPair4->value = "5";
  taosArrayPush(pArgs, pPair4);
  SConfigPair *pPair5 = (SConfigPair *)taosMemoryMalloc(sizeof(SConfigPair));
  pPair5->name = "test_dir";
  pPair5->value = TD_TMP_DIR_PATH;
  taosArrayPush(pArgs, pPair5);
  code = cfgLoadFromArray(pConfig, pArgs);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
}

TEST_F(CfgTest, cfgDumpItemCategory) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 100), 0);

  SConfigItem *pItem = NULL;
  pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->bval, 0);

  EXPECT_EQ(cfgDumpItemCategory(pItem, NULL, 0, 0), TSDB_CODE_INVALID_CFG);
}

TEST_F(CfgTest, cfgDumpCfgSs) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  cfgAddInt32(pConfig, "ssAutoMigrateIntervalSec", 60 * 60, 600, 100000, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,
              CFG_CATEGORY_GLOBAL);
  cfgAddInt32(pConfig, "ssPageCacheSize", 10, 4, 1024 * 1024 * 1024, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER_LAZY,
              CFG_CATEGORY_GLOBAL);
  cfgAddInt32(pConfig, "ssUploadDelaySec", 10, 1, 60 * 60 * 24 * 30, CFG_SCOPE_SERVER, CFG_DYN_ENT_SERVER,
              CFG_CATEGORY_GLOBAL);
  cfgAddDir(pConfig, "scriptDir", configDir, CFG_SCOPE_BOTH, CFG_DYN_NONE, CFG_CATEGORY_LOCAL);

  cfgDumpCfgSs(pConfig, false, false);

  cfgDumpCfgSs(pConfig, true, true);

  cfgDumpCfgSs(pConfig, false, true);

  cfgDumpCfgSs(pConfig, true, false);
}

#ifndef WINDOWS
TEST_F(CfgTest, cfgLoadFromEnvVar) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0), 0);

  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 6, 0), 0);

  setenv("test_bool", "1", 1);
  setenv("test_int32", "2", 1);
  setenv("test_int64", "3", 1);
  setenv("test_float", "4", 1);
  setenv("test_string", "5", 1);
  setenv("test_dir", TD_TMP_DIR_PATH, 1);

  ASSERT_EQ(cfgLoad(pConfig, CFG_STYPE_ENV_VAR, "test_bool"), TSDB_CODE_SUCCESS);
}

TEST_F(CfgTest, cfgLoadFromEnvCmd) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0), 0);

  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0), 0);

  const char *envCmd[] = {"test_bool=1", "test_int32=2", "test_int64=3", "test_float=4", "test_string=5", NULL};

  ASSERT_EQ(cfgLoad(pConfig, CFG_STYPE_ENV_CMD, envCmd), TSDB_CODE_SUCCESS);
}

TEST_F(CfgTest, cfgLoadFromEnvFile) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0), 0);

  TdFilePtr   envFile = NULL;
  const char *envFilePath = TD_TMP_DIR_PATH "envFile";
  envFile = taosOpenFile(envFilePath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  const char *buf = "test_bool=1\ntest_int32=2\ntest_int64=3\ntest_float=4\ntest_string=5\n";
  taosWriteFile(envFile, buf, strlen(buf));
  taosCloseFile(&envFile);
  ASSERT_EQ(cfgLoad(pConfig, CFG_STYPE_ENV_FILE, envFilePath), TSDB_CODE_SUCCESS);

  taosRemoveFile(envFilePath);
}

TEST_F(CfgTest, cfgLoadFromApollUrl) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0), 0);

  TdFilePtr   jsonFile = NULL;
  const char *jsonFilePath = TD_TMP_DIR_PATH "envJson.json";
  jsonFile = taosOpenFile(jsonFilePath, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND);
  const char *buf =
      "{\"test_bool\":\"1\",\"test_int32\":\"2\",\"test_int64\":\"3\",\"test_float\":\"4\",\"test_string\":\"5\"}";
  taosWriteFile(jsonFile, buf, strlen(buf));
  taosCloseFile(&jsonFile);

  char str[256];
  snprintf(str, sizeof(str), "jsonFile:%s", jsonFilePath);
  ASSERT_EQ(cfgLoad(pConfig, CFG_STYPE_APOLLO_URL, str), 0);

  taosRemoveFile(jsonFilePath);
}

#endif