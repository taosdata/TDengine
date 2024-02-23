#include <gtest/gtest.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>
#include "streamBackendRocksdb.h"

class BackendEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void *backendCreate() {
  const char *streamPath = "/tmp";

  char *absPath = NULL;
  void *p = NULL;
  // SBackendWrapper *p = streamBackendInit(streamPath, -1, 2);
  // p = taskDbOpen((char *)streamPath, (char *)"test", -1);
  // p = bkdMgtCreate((char *)streamPath);

  ASSERT(p != NULL);
  return p;
}
void backendOpen() {
  void *p = backendCreate();
  ASSERT(p != NULL);
}

TEST_F(BackendEnv, checkOpen) { backendOpen(); }
TEST_F(BackendEnv, backendOpt) {}
TEST_F(BackendEnv, backendDestroy) {}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}