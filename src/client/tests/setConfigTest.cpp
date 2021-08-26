#include <gtest/gtest.h>
#include <inttypes.h>

#include "taos.h"
#include "tglobal.h"
#include "tconfig.h"

/* test parse time function */
TEST(testCase, set_config_test) {
  const char *config = "{\"debugFlag\":\"131\"}";
  taos_set_config(config);

  const char *config2 = "{\"debugFlag\":\"199\"}";
  taos_set_config(config2);     // not take effect

  bool readResult = taosReadGlobalCfg();
  ASSERT_TRUE(readResult);   // load file config, not take effect
  int32_t checkResult = taosCheckGlobalCfg();
  ASSERT_EQ(checkResult, 0);

  SGlobalCfg *cfg = taosGetConfigOption("debugFlag");
  ASSERT_EQ(cfg->cfgStatus, TAOS_CFG_CSTATUS_OPTION);
  int32_t result = *(int32_t*)cfg->ptr;
  ASSERT_EQ(result, 131);
}
