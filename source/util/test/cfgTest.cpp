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
#include "tdef.h"

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

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 6, 0, 0), 0);
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

  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_string", "value", CFG_STYPE_DEFAULT, true), TSDB_CODE_INVALID_CFG);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_dir", TD_TMP_DIR_PATH, CFG_STYPE_DEFAULT, false),
            TSDB_CODE_INVALID_CFG);
  ASSERT_EQ(cfgAddDir(pConfig, "test_resolved_dir", TD_TMP_DIR_PATH, 0, CFG_DYN_NONE, 0, 0),
            TSDB_CODE_SUCCESS);

  const char *literalDir = "${CFG_TEST_UNEXPANDED}/literal*.dir";
  ASSERT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", literalDir, CFG_STYPE_ENV_CMD, true),
            TSDB_CODE_SUCCESS);
  pItem = cfgGetItem(pConfig, "test_resolved_dir");
  ASSERT_NE(pItem, nullptr);
  EXPECT_STREQ(pItem->str, literalDir);
  EXPECT_EQ(pItem->stype, CFG_STYPE_ENV_CMD);
  pItem->restartBaselineStr = taosStrdup("old-value");
  ASSERT_NE(pItem->restartBaselineStr, nullptr);
  pItem->restartBaselineValid = 1;
  pItem->restartRequired = 1;
  ASSERT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", TD_TMP_DIR_PATH, CFG_STYPE_DEFAULT, false),
            TSDB_CODE_SUCCESS);
  pItem = cfgGetItem(pConfig, "test_resolved_dir");
  ASSERT_NE(pItem, nullptr);
  EXPECT_EQ(pItem->restartBaselineStr, nullptr);
  EXPECT_EQ(pItem->restartBaselineValid, 0);
  EXPECT_EQ(pItem->restartRequired, 0);
  std::string maxDir(PATH_MAX - 1, 'x');
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", maxDir.c_str(), CFG_STYPE_DEFAULT, true),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", "", CFG_STYPE_DEFAULT, false),
            TSDB_CODE_SUCCESS);
  std::string tooLongDir(PATH_MAX, 'x');
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", tooLongDir.c_str(), CFG_STYPE_DEFAULT, false),
            TSDB_CODE_INVALID_CFG_VALUE);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "missing_dir", "value", CFG_STYPE_DEFAULT, false),
            TSDB_CODE_CFG_NOT_FOUND);
  ASSERT_EQ(cfgAddDir(pConfig, "test_dynamic_dir", TD_TMP_DIR_PATH, 0, CFG_DYN_CLIENT, 0, 0),
            TSDB_CODE_SUCCESS);
  pItem = cfgGetItem(pConfig, "test_dynamic_dir");
  ASSERT_NE(pItem, nullptr);
  ASSERT_NE(pItem->dynScope, CFG_DYN_NONE);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_dynamic_dir", TD_TMP_DIR_PATH, CFG_STYPE_DEFAULT, true),
            TSDB_CODE_INVALID_CFG);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", TD_TMP_DIR_PATH,
                              CFG_STYPE_ALTER_CLIENT_CMD, false),
            TSDB_CODE_INVALID_PARA);
  EXPECT_EQ(cfgSetDirResolved(pConfig, "test_resolved_dir", nullptr, CFG_STYPE_DEFAULT, false),
            TSDB_CODE_INVALID_PARA);

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

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 0, 0, 0), 0);

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

TEST_F(CfgTest, clsRefreshIntervalAlterRange) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);
  SConfigItem *pItem = NULL;

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  ASSERT_EQ(cfgAddInt32(pConfig, "clsRefreshInterval", 3600, 10, 86400, CFG_SCOPE_SERVER, CFG_DYN_SERVER,
                        CFG_CATEGORY_GLOBAL, CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);

  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "clsRefreshInterval", "9", true, CFG_ALTER_LOCAL),
            TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "clsRefreshInterval", "10", true, CFG_ALTER_LOCAL), TSDB_CODE_SUCCESS);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "clsRefreshInterval", "86400", true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "clsRefreshInterval", "86401", true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_OUT_OF_RANGE);

  pItem = cfgGetItem(pConfig, "clsRefreshInterval");
  ASSERT_NE(pItem, nullptr);
  EXPECT_EQ(pItem->i32, 3600);

  ASSERT_EQ(cfgSetItem(pConfig, "clsRefreshInterval", "9", CFG_STYPE_ALTER_CLIENT_CMD, true), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(pItem->i32, 3600);

  ASSERT_EQ(cfgSetItem(pConfig, "clsRefreshInterval", "10", CFG_STYPE_ALTER_CLIENT_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pItem->i32, 10);

  ASSERT_EQ(cfgGetAndSetItem(pConfig, &pItem, "clsRefreshInterval", "86401", CFG_STYPE_ALTER_SERVER_CMD, true),
            TSDB_CODE_OUT_OF_RANGE);
  ASSERT_NE(pItem, nullptr);
  EXPECT_EQ(pItem->i32, 10);

  ASSERT_EQ(cfgGetAndSetItem(pConfig, &pItem, "clsRefreshInterval", "86400", CFG_STYPE_ALTER_SERVER_CMD, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pItem, nullptr);
  EXPECT_EQ(pItem->i32, 86400);

  cfgCleanup(pConfig);
}

TEST_F(CfgTest, timezoneAlterValidation) {
  SConfig *pConfig = NULL;
  ASSERT_EQ(cfgInit(&pConfig), TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  ASSERT_EQ(cfgAddTimezone(pConfig, "timezone", "UTC", CFG_SCOPE_BOTH,
                           CFG_DYN_BOTH, CFG_CATEGORY_GLOBAL),
            TSDB_CODE_SUCCESS);

  // A charset value must be rejected before the mnode persists it.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "UTF-8", true,
                                      CFG_ALTER_ALL_DNODES),
            TSDB_CODE_PAR_INVALID_TIMEZONE);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "CST", true,
                                      CFG_ALTER_ALL_DNODES),
            TSDB_CODE_PAR_INVALID_TIMEZONE);

  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "Asia/Shanghai",
                                      true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_SUCCESS);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "+08:00", true,
                                      CFG_ALTER_ALL_DNODES),
            TSDB_CODE_SUCCESS);
  // ALTER LOCAL goes through the same check.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "UTF-8", false,
                                      CFG_ALTER_LOCAL),
            TSDB_CODE_PAR_INVALID_TIMEZONE);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "timezone", "UTC", false,
                                      CFG_ALTER_LOCAL),
            TSDB_CODE_SUCCESS);

  cfgCleanup(pConfig);
}

TEST_F(CfgTest, cfgDumpItemCategory) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 100, 0), 0);

  SConfigItem *pItem = NULL;
  pItem = cfgGetItem(pConfig, "test_bool");
  EXPECT_EQ(pItem->stype, CFG_STYPE_DEFAULT);
  EXPECT_EQ(pItem->dtype, CFG_DTYPE_BOOL);
  EXPECT_STREQ(pItem->name, "test_bool");
  EXPECT_EQ(pItem->bval, 0);

  EXPECT_EQ(cfgDumpItemCategory(pItem, NULL, 0, 0), TSDB_CODE_INVALID_CFG);
}

TEST_F(CfgTest, restartRequiredFlag) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  ASSERT_EQ(cfgAddInt32(pConfig, "lazyServerInt", 10, 0, 100, CFG_SCOPE_SERVER, CFG_DYN_SERVER_LAZY,
                        CFG_CATEGORY_GLOBAL, CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddInt32(pConfig, "dynServerInt", 10, 0, 100, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL,
                        CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddString(pConfig, "lazyClientStr", "abc", CFG_SCOPE_CLIENT, CFG_DYN_CLIENT_LAZY, CFG_CATEGORY_LOCAL,
                         CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddBool(pConfig, "lazyBothBool", 0, CFG_SCOPE_BOTH, CFG_DYN_BOTH_LAZY, CFG_CATEGORY_GLOBAL,
                       CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);

  char buf[TSDB_CONFIG_REBOOT_LEN + 1] = {0};
  int32_t len = 0;

  // a *_LAZY dyn item defaults to not-yet-restart-required
  SConfigItem *pLazyInt = cfgGetItem(pConfig, "lazyServerInt");
  ASSERT_NE(pLazyInt, nullptr);
  EXPECT_EQ(pLazyInt->restartRequired, 0);
  EXPECT_EQ(cfgDumpItemRestart(pLazyInt, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "false");

  // setting the exact same value should not flag a pending restart
  ASSERT_EQ(cfgSetItem(pConfig, "lazyServerInt", "10", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyInt->restartRequired, 0);

  // altering the value at runtime marks it as pending a restart before it is effective
  ASSERT_EQ(cfgSetItem(pConfig, "lazyServerInt", "20", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyInt->i32, 20);
  EXPECT_EQ(pLazyInt->restartRequired, 1);
  EXPECT_EQ(cfgDumpItemRestart(pLazyInt, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "true");

  // the flag stays set across further alters, as long as the value keeps differing
  // from the one in effect since process start (captured as the baseline on the very
  // first alter above)
  ASSERT_EQ(cfgSetItem(pConfig, "lazyServerInt", "30", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyInt->restartRequired, 1);

  // altering back to the value that's actually running (the baseline) clears the flag,
  // since no restart is needed anymore
  ASSERT_EQ(cfgSetItem(pConfig, "lazyServerInt", "10", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyInt->restartRequired, 0);
  EXPECT_EQ(cfgDumpItemRestart(pLazyInt, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "false");

  // diverging from the baseline again re-flags it
  ASSERT_EQ(cfgSetItem(pConfig, "lazyServerInt", "40", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyInt->restartRequired, 1);

  // a non-lazy dyn item takes effect immediately, so it never needs the pending-restart flag
  SConfigItem *pDynInt = cfgGetItem(pConfig, "dynServerInt");
  ASSERT_NE(pDynInt, nullptr);
  ASSERT_EQ(cfgSetItem(pConfig, "dynServerInt", "50", CFG_STYPE_ALTER_SERVER_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pDynInt->i32, 50);
  EXPECT_EQ(pDynInt->restartRequired, 0);

  // loading from a config file/env is not a runtime alter, so it never sets the flag either
  SConfigItem *pLazyBool = cfgGetItem(pConfig, "lazyBothBool");
  ASSERT_NE(pLazyBool, nullptr);
  ASSERT_EQ(cfgSetItem(pConfig, "lazyBothBool", "1", CFG_STYPE_CFG_FILE, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyBool->bval, 1);
  EXPECT_EQ(pLazyBool->restartRequired, 0);

  // string-typed lazy items detect the "changed" condition via content comparison, not identity
  SConfigItem *pLazyStr = cfgGetItem(pConfig, "lazyClientStr");
  ASSERT_NE(pLazyStr, nullptr);
  ASSERT_EQ(cfgSetItem(pConfig, "lazyClientStr", "abc", CFG_STYPE_ALTER_CLIENT_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_EQ(pLazyStr->restartRequired, 0);
  ASSERT_EQ(cfgSetItem(pConfig, "lazyClientStr", "xyz", CFG_STYPE_ALTER_CLIENT_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(pLazyStr->str, "xyz");
  EXPECT_EQ(pLazyStr->restartRequired, 1);
  EXPECT_EQ(cfgDumpItemRestart(pLazyStr, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "true");

  // altering a string item back to its baseline value also clears the flag
  ASSERT_EQ(cfgSetItem(pConfig, "lazyClientStr", "abc", CFG_STYPE_ALTER_CLIENT_CMD, true), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(pLazyStr->str, "abc");
  EXPECT_EQ(pLazyStr->restartRequired, 0);

  cfgCleanup(pConfig);
}

#ifndef WINDOWS
TEST_F(CfgTest, cfgLoadFromEnvVar) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0, 0), 0);
  EXPECT_EQ(cfgAddDir(pConfig, "test_dir", TD_TMP_DIR_PATH, 0, 6, 0, 0), 0);

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

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 6, 0, 0), 0);

  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 21, 0, 16, 0, 1, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 1, 0, 0), 0);

  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 21, 0, 16, 0, 2, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 2, 0, 0), 0);

  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 21, 0, 16, 0, 6, 0, 0), TSDB_CODE_OUT_OF_RANGE);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 6, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 6, 0, 0), 0);

  const char *envCmd[] = {"test_bool=1", "test_int32=2", "test_int64=3", "test_float=4", "test_string=5", NULL};

  ASSERT_EQ(cfgLoad(pConfig, CFG_STYPE_ENV_CMD, envCmd), TSDB_CODE_SUCCESS);
}

TEST_F(CfgTest, cfgLoadFromEnvFile) {
  SConfig *pConfig = NULL;
  int32_t  code = cfgInit(&pConfig);

  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0, 0), 0);
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

  EXPECT_EQ(cfgAddBool(pConfig, "test_bool", 0, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pConfig, "test_int32", 1, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pConfig, "test_int64", 2, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddFloat(pConfig, "test_float", 3, 0, 16, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pConfig, "test_string", "4", 0, 0, 0, 0), 0);
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

TEST_F(CfgTest, configSyncAddDelete) {
  SConfig *pGlobalConfig = NULL;
  SConfig *pSdbConfig = NULL;
  int32_t  code = cfgInit(&pGlobalConfig);
  
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pGlobalConfig, nullptr);
  
  code = cfgInit(&pSdbConfig);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pSdbConfig, nullptr);

  // Setup global config with some items
  EXPECT_EQ(cfgAddBool(pGlobalConfig, "globalBool", true, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pGlobalConfig, "globalInt32", 100, 0, 1000, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pGlobalConfig, "globalString", "test", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pGlobalConfig, "sharedConfig", "shared", 0, 0, 0, 0), 0);

  // Setup SDB config with some different items
  EXPECT_EQ(cfgAddBool(pSdbConfig, "sdbBool", false, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt64(pSdbConfig, "sdbInt64", 200, 0, 2000, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pSdbConfig, "sdbString", "sdb", 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pSdbConfig, "sharedConfig", "shared", 0, 0, 0, 0), 0);

  // Simulate the sync logic: find items to add (in global but not in SDB)
  SArray *itemsToAdd = taosArrayInit(4, sizeof(char*));
  SArray *itemsToDelete = taosArrayInit(4, sizeof(char*));
  
  int32_t globalSize = cfgGetSize(pGlobalConfig);
  int32_t sdbSize = cfgGetSize(pSdbConfig);
  
  // Find items in global config not in SDB config (to add)
  SConfigIter *pGlobalIter = NULL;
  code = cfgCreateIter(pGlobalConfig, &pGlobalIter);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  SConfigItem *pItem = NULL;
  while ((pItem = cfgNextIter(pGlobalIter)) != NULL) {
    SConfigItem *pSdbItem = cfgGetItem(pSdbConfig, pItem->name);
    if (pSdbItem == NULL) {
      char *itemName = taosStrdup(pItem->name);
      taosArrayPush(itemsToAdd, &itemName);
    }
  }
  cfgDestroyIter(pGlobalIter);
  
  // Find items in SDB config not in global config (to delete)
  SConfigIter *pSdbIter = NULL;
  code = cfgCreateIter(pSdbConfig, &pSdbIter);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  while ((pItem = cfgNextIter(pSdbIter)) != NULL) {
    SConfigItem *pGlobalItem = cfgGetItem(pGlobalConfig, pItem->name);
    if (pGlobalItem == NULL) {
      char *itemName = taosStrdup(pItem->name);
      taosArrayPush(itemsToDelete, &itemName);
    }
  }
  cfgDestroyIter(pSdbIter);
  
  // Verify the results
  EXPECT_EQ(taosArrayGetSize(itemsToAdd), 3);     // globalBool, globalInt32, globalString
  EXPECT_EQ(taosArrayGetSize(itemsToDelete), 3);  // sdbBool, sdbInt64, sdbString
  
  // Check specific items to add
  char **pItemName = (char**)taosArrayGet(itemsToAdd, 0);
  EXPECT_TRUE(strcmp(*pItemName, "globalBool") == 0 || 
              strcmp(*pItemName, "globalInt32") == 0 || 
              strcmp(*pItemName, "globalString") == 0);
  
  // Check specific items to delete
  pItemName = (char**)taosArrayGet(itemsToDelete, 0);
  EXPECT_TRUE(strcmp(*pItemName, "sdbBool") == 0 || 
              strcmp(*pItemName, "sdbInt64") == 0 || 
              strcmp(*pItemName, "sdbString") == 0);
  
  // Cleanup
  for (int i = 0; i < taosArrayGetSize(itemsToAdd); ++i) {
    char **pName = (char**)taosArrayGet(itemsToAdd, i);
    taosMemoryFree(*pName);
  }
  for (int i = 0; i < taosArrayGetSize(itemsToDelete); ++i) {
    char **pName = (char**)taosArrayGet(itemsToDelete, i);
    taosMemoryFree(*pName);
  }
  taosArrayDestroy(itemsToAdd);
  taosArrayDestroy(itemsToDelete);
  cfgCleanup(pGlobalConfig);
  cfgCleanup(pSdbConfig);
}

TEST_F(CfgTest, configSyncEmpty) {
  SConfig *pGlobalConfig = NULL;
  SConfig *pSdbConfig = NULL;
  int32_t  code = cfgInit(&pGlobalConfig);
  
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pGlobalConfig, nullptr);
  
  code = cfgInit(&pSdbConfig);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pSdbConfig, nullptr);

  // Test with empty configs
  SArray *itemsToAdd = taosArrayInit(4, sizeof(char*));
  SArray *itemsToDelete = taosArrayInit(4, sizeof(char*));
  
  // Since both configs are empty, no items should be found
  EXPECT_EQ(taosArrayGetSize(itemsToAdd), 0);
  EXPECT_EQ(taosArrayGetSize(itemsToDelete), 0);
  
  taosArrayDestroy(itemsToAdd);
  taosArrayDestroy(itemsToDelete);
  cfgCleanup(pGlobalConfig);
  cfgCleanup(pSdbConfig);
}

TEST_F(CfgTest, configSyncSameContent) {
  SConfig *pGlobalConfig = NULL;
  SConfig *pSdbConfig = NULL;
  int32_t  code = cfgInit(&pGlobalConfig);
  
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pGlobalConfig, nullptr);
  
  code = cfgInit(&pSdbConfig);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pSdbConfig, nullptr);

  // Setup identical configs
  EXPECT_EQ(cfgAddBool(pGlobalConfig, "testBool", true, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pGlobalConfig, "testInt32", 100, 0, 1000, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pGlobalConfig, "testString", "test", 0, 0, 0, 0), 0);
  
  EXPECT_EQ(cfgAddBool(pSdbConfig, "testBool", true, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddInt32(pSdbConfig, "testInt32", 100, 0, 1000, 0, 0, 0, 0), 0);
  EXPECT_EQ(cfgAddString(pSdbConfig, "testString", "test", 0, 0, 0, 0), 0);

  // Simulate the sync logic
  SArray *itemsToAdd = taosArrayInit(4, sizeof(char*));
  SArray *itemsToDelete = taosArrayInit(4, sizeof(char*));
  
  // Find items in global config not in SDB config (to add)
  SConfigIter *pGlobalIter = NULL;
  code = cfgCreateIter(pGlobalConfig, &pGlobalIter);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  SConfigItem *pItem = NULL;
  while ((pItem = cfgNextIter(pGlobalIter)) != NULL) {
    SConfigItem *pSdbItem = cfgGetItem(pSdbConfig, pItem->name);
    if (pSdbItem == NULL) {
      char *itemName = taosStrdup(pItem->name);
      taosArrayPush(itemsToAdd, &itemName);
    }
  }
  cfgDestroyIter(pGlobalIter);
  
  // Find items in SDB config not in global config (to delete)
  SConfigIter *pSdbIter = NULL;
  code = cfgCreateIter(pSdbConfig, &pSdbIter);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  
  while ((pItem = cfgNextIter(pSdbIter)) != NULL) {
    SConfigItem *pGlobalItem = cfgGetItem(pGlobalConfig, pItem->name);
    if (pGlobalItem == NULL) {
      char *itemName = taosStrdup(pItem->name);
      taosArrayPush(itemsToDelete, &itemName);
    }
  }
  cfgDestroyIter(pSdbIter);
  
  // Since configs are identical, no items should need to be added or deleted
  EXPECT_EQ(taosArrayGetSize(itemsToAdd), 0);
  EXPECT_EQ(taosArrayGetSize(itemsToDelete), 0);
  
  taosArrayDestroy(itemsToAdd);
  taosArrayDestroy(itemsToDelete);
  cfgCleanup(pGlobalConfig);
  cfgCleanup(pSdbConfig);
}
// dmProcessConfigReq re-runs cfgCheckRangeForDynUpdate on the dnode before committing a value
// from TDMT_DND_CONFIG_DNODE, because it cannot authenticate the sender and must not assume the
// mnode already validated the request. These cases pin the semantics that gate relies on.
TEST_F(CfgTest, dynUpdateRefusesNonDynamicAndClientOnlyItems) {
  SConfig *pConfig = NULL;
  ASSERT_EQ(cfgInit(&pConfig), TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  // Stand-ins for the real items a forged request targets: supportVnodes/audit are
  // CFG_DYN_ENT_SERVER, which is CFG_DYN_NONE in a community build, and fqdn/serverPort are
  // CFG_DYN_CLIENT.
  ASSERT_EQ(cfgAddInt32(pConfig, "non_dynamic_item", 16, 0, 1024, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL,
                        CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddInt32(pConfig, "client_only_item", 6030, 1, 65056, CFG_SCOPE_BOTH, CFG_DYN_CLIENT,
                        CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddInt32(pConfig, "server_dynamic_item", 16, 0, 1024, CFG_SCOPE_SERVER, CFG_DYN_SERVER,
                        CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);

  // A non-dynamic item is refused whichever scope the ALTER claims.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "non_dynamic_item", "0", true, CFG_ALTER_DNODE), TSDB_CODE_INVALID_CFG);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "non_dynamic_item", "0", true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_INVALID_CFG);

  // A client-only item is refused on the server side.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "client_only_item", "7030", true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_INVALID_CFG);

  // A server-dynamic local item is accepted, so the gate does not over-reject.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "server_dynamic_item", "8", true, CFG_ALTER_DNODE), TSDB_CODE_SUCCESS);

  // An unknown name is reported as not found, which is what lets the dnode keep handling
  // "resetlog" through its own branch.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "no_such_item", "1", true, CFG_ALTER_DNODE), TSDB_CODE_CFG_NOT_FOUND);

  cfgCleanup(pConfig);
}

TEST_F(CfgTest, dynUpdateRefusesGlobalItemOnSingleDnode) {
  SConfig *pConfig = NULL;
  ASSERT_EQ(cfgInit(&pConfig), TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  ASSERT_EQ(cfgAddBool(pConfig, "global_dynamic_item", 0, CFG_SCOPE_SERVER, CFG_DYN_SERVER, CFG_CATEGORY_GLOBAL,
                       CFG_PRIV_SECURITY),
            TSDB_CODE_SUCCESS);

  // Changing a global item on a single dnode leaves the cluster inconsistent, so it is refused
  // for CFG_ALTER_DNODE and allowed only for a cluster-wide ALTER. The dnode learns which one it
  // is from SDCfgDnodeReq::alterType.
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "global_dynamic_item", "1", true, CFG_ALTER_DNODE),
            TSDB_CODE_INVALID_CFG);
  EXPECT_EQ(cfgCheckRangeForDynUpdate(pConfig, "global_dynamic_item", "1", true, CFG_ALTER_ALL_DNODES),
            TSDB_CODE_SUCCESS);

  cfgCleanup(pConfig);
}

// ssAccessString embeds the object-storage accessKeyId and secretAccessKey. cfgDumpItemValue is
// the display path (dumpConfToDataBlock -> SHOW VARIABLES, ins_dnode_variables), so it must never
// render the raw credential regardless of caller privilege.
TEST_F(CfgTest, dumpItemValueMasksSecretItems) {
  SConfig *pConfig = NULL;
  ASSERT_EQ(cfgInit(&pConfig), TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  const char *secret = "s3:endpoint=127.0.0.1:9000;accessKeyId=AKID;secretAccessKey=SECRET";
  ASSERT_EQ(cfgAddString(pConfig, "ssAccessString", secret, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL,
                         CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);
  ASSERT_EQ(cfgAddString(pConfig, "ordinary_string", "visible", CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL,
                         CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);

  char    buf[TSDB_CONFIG_PATH_LEN] = {0};
  int32_t len = 0;

  SConfigItem *pItem = cfgGetItem(pConfig, "ssAccessString");
  ASSERT_NE(pItem, nullptr);
  ASSERT_EQ(cfgDumpItemValue(pItem, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "******");
  EXPECT_EQ(strstr(buf, "secretAccessKey"), nullptr);
  EXPECT_EQ(strstr(buf, "AKID"), nullptr);
  // The item itself is untouched, so persistence still writes the real value.
  EXPECT_STREQ(pItem->str, secret);

  // Non-secret items are still dumped verbatim.
  memset(buf, 0, sizeof(buf));
  pItem = cfgGetItem(pConfig, "ordinary_string");
  ASSERT_NE(pItem, nullptr);
  ASSERT_EQ(cfgDumpItemValue(pItem, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "visible");

  cfgCleanup(pConfig);
}

TEST_F(CfgTest, dumpItemValueKeepsEmptySecretEmpty) {
  SConfig *pConfig = NULL;
  ASSERT_EQ(cfgInit(&pConfig), TSDB_CODE_SUCCESS);
  ASSERT_NE(pConfig, nullptr);

  // An unconfigured secret must read as empty rather than as a mask, so operators can still tell
  // whether shared storage is configured at all.
  ASSERT_EQ(cfgAddString(pConfig, "ssAccessString", "", CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_GLOBAL,
                         CFG_PRIV_SYSTEM),
            TSDB_CODE_SUCCESS);

  char    buf[TSDB_CONFIG_PATH_LEN] = {0};
  int32_t len = 0;

  SConfigItem *pItem = cfgGetItem(pConfig, "ssAccessString");
  ASSERT_NE(pItem, nullptr);
  ASSERT_EQ(cfgDumpItemValue(pItem, buf, sizeof(buf), &len), TSDB_CODE_SUCCESS);
  EXPECT_STREQ(buf, "");
  EXPECT_EQ(len, 0);

  cfgCleanup(pConfig);
}
