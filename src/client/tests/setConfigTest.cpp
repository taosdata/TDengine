#include <gtest/gtest.h>
#include <inttypes.h>

#include "taos.h"
#include "tglobal.h"
#include "tconfig.h"

/* test set config function */
TEST(testCase, set_config_test1) {
  const char *config = "{\"debugFlag\":\"131\"}";
  taos_set_config(config);

  const char *config2 = "{\"debugFlag\":\"199\"}";
  taos_set_config(config2);  // not take effect

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
