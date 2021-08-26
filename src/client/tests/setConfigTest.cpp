#include <gtest/gtest.h>
#include <inttypes.h>

#include "taos.h"
#include "tglobal.h"

/* test parse time function */
TEST(testCase, set_config_test) {
  const char *config = "{\"debugFlag\":\"131\"}";
  taos_set_config(config);

  const char *config2 = "{\"debugFlag\":\"199\"}";
  taos_set_config(config2);     // not take effect

  ASSERT_EQ(taosReadGlobalCfg(), true);   // load file config, not take effect
  ASSERT_EQ(taosCheckGlobalCfg(), 0);

  SGlobalCfg *cfg = taosGetConfigOption("debugFlag");
  ASSERT_EQ(cfg->cfgStatus, TAOS_CFG_CSTATUS_OPTION);
  ASSERT_EQ(*(int32_t*)cfg.ptr, 135);
}
