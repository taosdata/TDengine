#include <gtest/gtest.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>
#include <vector>
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

SStreamState *stateCreate(const char *path) {
  SStreamTask *pTask = (SStreamTask *)taosMemoryCalloc(1, sizeof(SStreamTask));
  pTask->ver = 1024;
  pTask->id.streamId = 1023;
  pTask->id.taskId = 1111111;
  SStreamMeta *pMeta = streamMetaOpen((path), NULL, NULL, 0, 0, NULL);
  pTask->pMeta = pMeta;

  SStreamState *p = streamStateOpen((char *)path, pTask, true, 32, 32 * 1024);
  ASSERT(p != NULL);
  return p;
}
void backendOpen() {
  const char   *path = "/tmp/backend";
  SStreamState *p = stateCreate(path);
  ASSERT(p != NULL);

  // write bacth
  // default/state/fill/sess/func/parname/partag
  int32_t              size = 100;
  std::vector<int64_t> tsArray;
  for (int i = 0; i < size; i++) {
    int64_t     ts = taosGetTimestampMs();
    SWinKey     key = {.groupId = (uint64_t)(i), .ts = ts};
    const char *val = "value data";
    int32_t     vlen = strlen(val);
    streamStatePut_rocksdb(p, &key, (char *)val, vlen);

    tsArray.push_back(ts);
  }
  for (int i = 0; i < size; i++) {
    int64_t ts = tsArray[i];
    SWinKey key = {.groupId = (uint64_t)(i), .ts = ts};

    const char *val = "value data";
    int32_t     len = 0;
    char       *newVal = NULL;
    streamStateGet_rocksdb(p, &key, (void **)&newVal, &len);
    ASSERT(len == strlen(val));
  }
  int64_t ts = tsArray[0];
  SWinKey key = {.groupId = (uint64_t)(0), .ts = ts};
  streamStateDel_rocksdb(p, &key);

  

  // read
  // iterator
  // rebuild chkp, reload from chkp
  // sync
  //
  streamStateClose((SStreamState *)p, true);
  // taskDbDestroy(p, true);
}

TEST_F(BackendEnv, checkOpen) { backendOpen(); }
TEST_F(BackendEnv, backendOpt) {}
TEST_F(BackendEnv, backendDestroy) {}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}