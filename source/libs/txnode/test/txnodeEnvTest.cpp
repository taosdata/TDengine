#include <gtest/gtest.h>

#include <cstring>

#include "txnodeInt.h"

namespace {

bool containsEnvironmentValue(char **environment, const char *expected) {
  for (int32_t index = 0; environment[index] != nullptr; ++index) {
    if (std::strcmp(environment[index], expected) == 0) {
      return true;
    }
  }
  return false;
}

}  // namespace

TEST(TxnodeEnvironmentTest, InheritsPrivateKeyWhenConfigurationIsEmpty) {
  const char *inherited[] = {"PATH=/bin", "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/inherited/key"};
  const char *overrides[] = {"XNODED_CFG_DIR=/etc/taos", "", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 2, overrides, 2, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "PATH=/bin"));
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_CFG_DIR=/etc/taos"));
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/inherited/key"));
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeEnvironmentTest, ConfiguredPrivateKeyOverridesInheritedValue) {
  const char *inherited[] = {"XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/old/key"};
  const char *overrides[] = {"XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/configured/key", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 1, overrides, 1, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/configured/key"));
  EXPECT_FALSE(containsEnvironmentValue(environment, "XNODED_XNODE_DSN_PRIVATE_KEY_FILE=/old/key"));
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeEnvironmentTest, MatchesEnvironmentNamesUsingPlatformSemantics) {
  const char *inherited[] = {"xnoded_cfg_dir=/inherited/cfg"};
  const char *overrides[] = {"XNODED_CFG_DIR=/etc/taos", nullptr};
  char **environment = nullptr;

  ASSERT_EQ(txnodeBuildXnodedEnvironment(inherited, 1, overrides, 1, &environment), 0);
  ASSERT_NE(environment, nullptr);
  EXPECT_TRUE(containsEnvironmentValue(environment, "XNODED_CFG_DIR=/etc/taos"));
#ifdef WINDOWS
  EXPECT_FALSE(containsEnvironmentValue(environment, "xnoded_cfg_dir=/inherited/cfg"));
#else
  EXPECT_TRUE(containsEnvironmentValue(environment, "xnoded_cfg_dir=/inherited/cfg"));
#endif
  txnodeFreeEnvironment(environment);
}

TEST(TxnodeSupervisionTest, DoesNotRestartForPrivateKeyConfigurationFailure) {
  EXPECT_FALSE(txnodeShouldRestartXnoded(XNODED_PRIVATE_KEY_CONFIG_EXIT_CODE, 0, false));
}

TEST(TxnodeSupervisionTest, RestartsForOtherUnexpectedExit) {
  EXPECT_TRUE(txnodeShouldRestartXnoded(1, 0, false));
}

TEST(TxnodeSupervisionTest, DoesNotRestartAfterNormalExitOrStop) {
  EXPECT_FALSE(txnodeShouldRestartXnoded(0, 0, false));
  EXPECT_FALSE(txnodeShouldRestartXnoded(1, 0, true));
}
