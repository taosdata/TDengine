#include <gtest/gtest.h>
#include <inttypes.h>

#include "taos.h"
#include "tglobal.h"
#include "tconfig.h"

/* test set config function */
TEST(testCase, set_config_test1) {
  const char *config = "{\"debugFlag\":\"131\"}";
  setConfRet ret = taos_set_config(config);
  ASSERT_EQ(ret.retCode, 0);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);

  const char *config2 = "{\"debugFlag\":\"199\"}";
  ret = taos_set_config(config2);  // not take effect
  ASSERT_EQ(ret.retCode, -5);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);

  bool readResult = taosReadGlobalCfg();  // load file config, debugFlag not take effect
  ASSERT_TRUE(readResult);
  int32_t checkResult = taosCheckGlobalCfg();
  ASSERT_EQ(checkResult, 0);

  SGlobalCfg *cfg = taosGetConfigOption("debugFlag");
  ASSERT_EQ(cfg->cfgStatus, TAOS_CFG_CSTATUS_OPTION);
  int32_t result = *(int32_t *)cfg->ptr;
  ASSERT_EQ(result, 131);
}

TEST(testCase, set_config_test2) {
  const char *config = "{\"numOfCommitThreads\":\"10\"}";
  taos_set_config(config);

  bool readResult = taosReadGlobalCfg(); // load file config, debugFlag not take effect
  ASSERT_TRUE(readResult);
  int32_t checkResult = taosCheckGlobalCfg();
  ASSERT_EQ(checkResult, 0);

  SGlobalCfg *cfg = taosGetConfigOption("numOfCommitThreads");
  int32_t result = *(int32_t*)cfg->ptr;
  ASSERT_NE(result, 10);    // numOfCommitThreads not type of TSDB_CFG_CTYPE_B_CLIENT
}

TEST(testCase, set_config_test3) {
  const char *config = "{\"numOfCoitThreads\":\"10\", \"esdfa\":\"10\"}";
  setConfRet ret = taos_set_config(config);
  ASSERT_EQ(ret.retCode, -1);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);
}

TEST(testCase, set_config_test4) {
  const char *config = "{null}";
  setConfRet ret = taos_set_config(config);
  ASSERT_EQ(ret.retCode, -4);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);
}

TEST(testCase, set_config_test5) {
  const char *config = "\"ddd\"";
  setConfRet ret = taos_set_config(config);
  ASSERT_EQ(ret.retCode, -3);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);
}

TEST(testCase, set_config_test6) {
  const char *config = "{\"numOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitThreadsnumOfCoitT3333dd\":\"10\", \"esdfa\":\"10\"}";
  setConfRet ret = taos_set_config(config);
  ASSERT_EQ(ret.retCode, -1);
  printf("msg:%d->%s", ret.retCode, ret.retMsg);
}
