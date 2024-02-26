#include <gtest/gtest.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>
#include "streamBackendRocksdb.h"
#include "streamSnapshot.h"
#include "streamState.h"
#include "tstream.h"
#include "tstreamFileState.h"
#include "tstreamUpdate.h"

class BackendEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void *backendCreate() {
  const char *streamPath = "/tmp";
  void       *p = NULL;

  // char *absPath = NULL;
  // // SBackendWrapper *p = (SBackendWrapper *)streamBackendInit(streamPath, -1, 2);
  // STaskDbWrapper *p = taskDbOpen((char *)streamPath, (char *)"stream-backend", -1);
  // ASSERT(p != NULL);
  return p;
}

SStreamState *stateCreate(void *pBackend, char *keyidr) {
  const char  *streamPath = "/tmp";
  SStreamTask *pTask = (SStreamTask *)taosMemoryCalloc(1, sizeof(SStreamTask));
  pTask->ver = 1024;
  pTask->id.streamId = 1023;
  pTask->id.taskId = 1111111;

  SStreamState *p = streamStateOpen((char *)streamPath, pTask, true, 32, 32 * 1024);
  ASSERT(p != NULL);
  return p;
}
void backendOpen() {
  void *p = backendCreate();
  ASSERT(p != NULL);
  taskDbDestroy(p, true);
}

TEST_F(BackendEnv, checkOpen) {
  backendOpen();
}
TEST_F(BackendEnv, backendOpt) {}
TEST_F(BackendEnv, backendDestroy) {}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}