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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

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
  SStreamMeta *pMeta = NULL;

  int32_t code = streamMetaOpen((path), NULL, NULL, NULL, 0, 0, NULL, &pMeta);
  pTask->pMeta = pMeta;

  SStreamState *p = streamStateOpen((char *)path, pTask, 0, 0);
  ASSERT(p != NULL);
  return p;
}
void *backendOpen() {
  streamMetaInit();
  const char   *path = "/tmp/backend";
  SStreamState *p = stateCreate(path);
  ASSERT(p != NULL);

  // write bacth
  // default/state/fill/sess/func/parname/partag
  int32_t              size = 100;
  std::vector<int64_t> tsArray;

  for (int32_t i = 0; i < size; i++) {
    int64_t ts = taosGetTimestampMs();
    SWinKey key {0};  // = {.groupId = (uint64_t)(i), .ts = ts};
    key.groupId = (uint64_t)(i);
    key.ts = ts;
    const char *val = "value data";
    int32_t     vlen = strlen(val);
    int32_t     code = streamStatePut_rocksdb(p, &key, (char *)val, vlen);
    ASSERT(code == 0);

    tsArray.push_back(ts);
  }
  for (int32_t i = 0; i < size; i++) {
    int64_t ts = tsArray[i];
    SWinKey key = {0};  //{.groupId = (uint64_t)(i), .ts = ts};
    key.groupId = (uint64_t)(i);
    key.ts = ts;

    const char *val = "value data";
    int32_t     len = 0;
    char       *newVal = NULL;
    int32_t     code = streamStateGet_rocksdb(p, &key, (void **)&newVal, &len);
    ASSERT(code == 0);

    ASSERT(len == strlen(val));
  }
  int64_t ts = tsArray[0];
  SWinKey key = {0};  // {.groupId = (uint64_t)(0), .ts = ts};
  key.groupId = (uint64_t)(0);
  key.ts = ts;

  int32_t code = streamStateDel_rocksdb(p, &key);
  ASSERT(code == 0);

  code = streamStateClear_rocksdb(p);
  ASSERT(code == 0);

  for (int i = 0; i < size; i++) {
    int64_t ts = tsArray[i];
    SWinKey key = {0};  //{.groupId = (uint64_t)(i), .ts = ts};
    key.groupId = (uint64_t)(i);
    key.ts = ts;

    const char *val = "value data";
    int32_t     len = 0;
    char       *newVal = NULL;
    int32_t     code = streamStateGet_rocksdb(p, &key, (void **)&newVal, &len);
    ASSERT(code != 0);
  }
  tsArray.clear();

  for (int i = 0; i < size; i++) {
    int64_t ts = taosGetTimestampMs();
    tsArray.push_back(ts);

    SWinKey key = {0};  //{.groupId = (uint64_t)(i), .ts = ts};
    key.groupId = (uint64_t)(i);
    key.ts = ts;

    const char *val = "value data";
    int32_t     vlen = strlen(val);
    code = streamStatePut_rocksdb(p, &key, (char *)val, vlen);
    ASSERT(code == 0);
  }

  SWinKey winkey;
  code = streamStateGetFirst_rocksdb(p, &key);
  ASSERT(code == 0);
  ASSERT(key.ts == tsArray[0]);

  SStreamStateCur *pCurr = streamStateSeekToLast_rocksdb(p);
  ASSERT(pCurr != NULL);
  streamStateFreeCur(pCurr);

  winkey.groupId = 0;
  winkey.ts = tsArray[0];
  char   *val = NULL;
//  int32_t len = 0;

  pCurr = streamStateSeekKeyNext_rocksdb(p, &winkey);
  ASSERT(pCurr != NULL);

  streamStateFreeCur(pCurr);

  tsArray.clear();
  for (int i = 0; i < size; i++) {
    int64_t ts = taosGetTimestampMs();
    tsArray.push_back(ts);
    STupleKey key = {0};
    key.groupId = (uint64_t)(0);  //= {.groupId = (uint64_t)(0), .ts = ts, .exprIdx = i};
    key.ts = ts;
    key.exprIdx = i;

    const char *val = "Value";
    int32_t     len = strlen(val);
    code = streamStateFuncPut_rocksdb(p, &key, val, len);
    ASSERT(code == 0);
  }
  for (int i = 0; i < size; i++) {
    STupleKey key = {0};  //{.groupId = (uint64_t)(0), .ts = tsArray[i], .exprIdx = i};
    key.groupId = (uint64_t)(0);
    key.ts = tsArray[i];
    key.exprIdx = i;

    char   *val = NULL;
    int32_t len = 0;
    int32_t code = streamStateFuncGet_rocksdb(p, &key, (void **)&val, &len);
    ASSERT(code == 0);

    ASSERT(len == strlen("Value"));
  }
  for (int i = 0; i < size; i++) {
    STupleKey key = {0};  //{.groupId = (uint64_t)(0), .ts = tsArray[i], .exprIdx = i};
    key.groupId = (uint64_t)(0);
    key.ts = tsArray[i];
    key.exprIdx = i;

    char   *val = NULL;
    int32_t len = 0;
    int32_t code = streamStateFuncDel_rocksdb(p, &key);
    ASSERT(code == 0);
  }

  // session put
  tsArray.clear();

  for (int i = 0; i < size; i++) {
    SSessionKey key = {0};  //{.win = {.skey = i, .ekey = i}, .groupId = (uint64_t)(0)};
    key.win.skey = i;
    key.win.ekey = i;
    key.groupId = (uint64_t)(0);
    tsArray.push_back(i);

    const char *val = "Value";
    int32_t     len = strlen(val);
    code = streamStateSessionPut_rocksdb(p, &key, val, len);
    ASSERT(code == 0);

    char *pval = NULL;
    ASSERT(0 == streamStateSessionGet_rocksdb(p, &key, (void **)&pval, &len));
    ASSERT(strncmp(pval, val, len) == 0);
  }

  for (int i = 0; i < size; i++) {
    SSessionKey key = {0};  //{.win = {.skey = tsArray[i], .ekey = tsArray[i]}, .groupId = (uint64_t)(0)};
    key.win.skey = tsArray[i];
    key.win.ekey = tsArray[i];
    key.groupId = (uint64_t)(0);

    const char *val = "Value";
    int32_t     len = strlen(val);

    char *pval = NULL;
    ASSERT(0 == streamStateSessionGet_rocksdb(p, &key, (void **)&pval, &len));
    ASSERT(strncmp(pval, val, len) == 0);
    taosMemoryFreeClear(pval);
  }

  pCurr = streamStateSessionSeekToLast_rocksdb(p, 0);
  ASSERT(pCurr != NULL);

  {
    SSessionKey key;
    memset(&key, 0, sizeof(key));
    char   *val = NULL;
    int32_t vlen = 0;
    code = streamStateSessionGetKVByCur_rocksdb(NULL, pCurr, &key, (void **)&val, &vlen);
    ASSERT(code == 0);
    pCurr = streamStateSessionSeekKeyPrev_rocksdb(p, &key);

    code = streamStateSessionGetKVByCur_rocksdb(NULL, pCurr, &key, (void **)&val, &vlen);
    ASSERT(code == 0);

    ASSERT(key.groupId == 0 && key.win.ekey == tsArray[tsArray.size() - 2]);

    pCurr = streamStateSessionSeekKeyNext_rocksdb(p, &key);
    code = streamStateSessionGetKVByCur_rocksdb(NULL, pCurr, &key, (void **)&val, &vlen);
    ASSERT(code == 0);
    ASSERT(vlen == strlen("Value"));
    ASSERT(key.groupId == 0 && key.win.skey == tsArray[tsArray.size() - 1]);

    ASSERT(0 == streamStateSessionAddIfNotExist_rocksdb(p, &key, 10, (void **)&val, &vlen));

    ASSERT(0 ==
           streamStateStateAddIfNotExist_rocksdb(p, &key, (char *)"key", strlen("key"), NULL, (void **)&val, &vlen));
  }

  for (int i = 0; i < size; i++) {
    SSessionKey key = {0};  //{.win = {.skey = tsArray[i], .ekey = tsArray[i]}, .groupId = (uint64_t)(0)};
    key.win.skey = tsArray[i];
    key.win.ekey = tsArray[i];
    key.groupId = (uint64_t)(0);

    const char *val = "Value";
    int32_t     len = strlen(val);

    char *pval = NULL;
    ASSERT(0 == streamStateSessionDel_rocksdb(p, &key));
  }

  for (int i = 0; i < size; i++) {
    SWinKey key = {0};  // {.groupId = (uint64_t)(i), .ts = tsArray[i]};
    key.groupId = (uint64_t)(i);
    key.ts = tsArray[i];
    const char *val = "Value";
    int32_t     vlen = strlen(val);
    ASSERT(streamStateFillPut_rocksdb(p, &key, val, vlen) == 0);
  }
  for (int i = 0; i < size; i++) {
    SWinKey key = {0};  // {.groupId = (uint64_t)(i), .ts = tsArray[i]};
    key.groupId = (uint64_t)(i);
    key.ts = tsArray[i];
    char   *val = NULL;
    int32_t vlen = 0;
    ASSERT(streamStateFillGet_rocksdb(p, &key, (void **)&val, &vlen) == 0);
    taosMemoryFreeClear(val);
  }
  {
    SWinKey key = {0};  //{.groupId = (uint64_t)(0), .ts = tsArray[0]};
    key.groupId = (uint64_t)(0);
    key.ts = tsArray[0];
    SStreamStateCur *pCurr = streamStateFillGetCur_rocksdb(p, &key);
    ASSERT(pCurr != NULL);

    char   *val = NULL;
    int32_t vlen = 0;
    ASSERT(0 == streamStateFillGetKVByCur_rocksdb(pCurr, &key, (const void **)&val, &vlen));
    ASSERT(vlen == strlen("Value"));
    streamStateFreeCur(pCurr);

    pCurr = streamStateFillSeekKeyNext_rocksdb(p, &key);
    ASSERT(0 == streamStateFillGetKVByCur_rocksdb(pCurr, &key, (const void **)&val, &vlen));
    ASSERT(vlen == strlen("Value") && key.groupId == 1 && key.ts == tsArray[1]);

    key.groupId = 1;
    key.ts = tsArray[1];

    pCurr = streamStateFillSeekKeyPrev_rocksdb(p, &key);
    ASSERT(pCurr != NULL);
    ASSERT(0 == streamStateFillGetKVByCur_rocksdb(pCurr, &key, (const void **)&val, &vlen));

    ASSERT(vlen == strlen("Value") && key.groupId == 0 && key.ts == tsArray[0]);
  }

  for (int i = 0; i < size - 1; i++) {
    SWinKey key = {0};  // {.groupId = (uint64_t)(i), .ts = tsArray[i]};
    key.groupId = (uint64_t)(i);
    key.ts = tsArray[i];
    char   *val = NULL;
    int32_t vlen = 0;
    ASSERT(streamStateFillDel_rocksdb(p, &key) == 0);
    taosMemoryFreeClear(val);
  }
  streamStateSessionClear_rocksdb(p);

  for (int i = 0; i < size; i++) {
    char tbname[TSDB_TABLE_NAME_LEN] = {0};
    sprintf(tbname, "%s_%d", "tbname", i);
    ASSERT(0 == streamStatePutParName_rocksdb(p, i, tbname));
  }
  for (int i = 0; i < size; i++) {
    char *val = NULL;
    ASSERT(0 == streamStateGetParName_rocksdb(p, i, (void **)&val));
    ASSERT(strncmp(val, "tbname", strlen("tbname")) == 0);
    taosMemoryFree(val);
  }

  for (int i = 0; i < size; i++) {
    char tbname[TSDB_TABLE_NAME_LEN] = {0};
    sprintf(tbname, "%s_%d", "tbname", i);
    ASSERT(0 == streamStatePutParName_rocksdb(p, i, tbname));
  }
  for (int i = 0; i < size; i++) {
    char *val = NULL;
    ASSERT(0 == streamStateGetParName_rocksdb(p, i, (void **)&val));
    ASSERT(strncmp(val, "tbname", strlen("tbname")) == 0);
    taosMemoryFree(val);
  }
  for (int i = 0; i < size; i++) {
    char key[128] = {0};
    sprintf(key, "tbname_%d", i);
    char val[128] = {0};
    sprintf(val, "val_%d", i);
    code = streamDefaultPut_rocksdb(p, key, val, strlen(val));
    ASSERT(code == 0);
  }
  for (int i = 0; i < size; i++) {
    char key[128] = {0};
    sprintf(key, "tbname_%d", i);

    char   *val = NULL;
    int32_t len = 0;
    code = streamDefaultGet_rocksdb(p, key, (void **)&val, &len);
    ASSERT(code == 0);
  }
  SArray *result = taosArrayInit(8, sizeof(void *));
  code = streamDefaultIterGet_rocksdb(p, "tbname", "tbname_99", result);
  ASSERT(code == 0);

  ASSERT(taosArrayGetSize(result) >= 0);

  return p;
  // streamStateClose((SStreamState *)p, true);
}
TEST_F(BackendEnv, checkOpen) {
  SStreamState *p = (SStreamState *)backendOpen();
  int64_t       tsStart = taosGetTimestampMs();
  {
    void   *pBatch = streamStateCreateBatch();
    int32_t size = 0;
    for (int i = 0; i < size; i++) {
      char key[128] = {0};
      sprintf(key, "key_%d", i);
      char val[128] = {0};
      sprintf(val, "val_%d", i);
      int32_t code = streamStatePutBatch(p, "default", (rocksdb_writebatch_t *)pBatch, (void *)key, (void *)val,
                                         (int32_t)(strlen(val)), tsStart + 100000);
      ASSERT(code == 0);
    }

    int32_t code = streamStatePutBatch_rocksdb(p, pBatch);
    ASSERT(code == 0);

    streamStateDestroyBatch(pBatch);
  }
  {
    void   *pBatch = streamStateCreateBatch();
    int32_t size = 0;
    char    valBuf[256] = {0};
    for (int i = 0; i < size; i++) {
      char key[128] = {0};
      sprintf(key, "key_%d", i);
      char val[128] = {0};
      sprintf(val, "val_%d", i);
      int32_t code = streamStatePutBatchOptimize(p, 0, (rocksdb_writebatch_t *)pBatch, (void *)key, (void *)val,
                                                 (int32_t)(strlen(val)), tsStart + 100000, (void *)valBuf);
      ASSERT(code == 0);
    }
    int32_t code = streamStatePutBatch_rocksdb(p, pBatch);
    ASSERT(code == 0);
    streamStateDestroyBatch(pBatch);
  }

  SArray* pList = taosArrayInit(3, sizeof(int64_t));

  // do checkpoint 2
  int32_t code = taskDbDoCheckpoint(p->pTdbState->pOwner->pBackend, 2, 0, pList);
  ASSERT(code == 0);

  {
    void   *pBatch = streamStateCreateBatch();
    int32_t size = 0;
    char    valBuf[256] = {0};
    for (int i = 0; i < size; i++) {
      char key[128] = {0};
      sprintf(key, "key_%d", i);
      char val[128] = {0};
      sprintf(val, "val_%d", i);
      int32_t code = streamStatePutBatchOptimize(p, 0, (rocksdb_writebatch_t *)pBatch, (void *)key, (void *)val,
                                                 (int32_t)(strlen(val)), tsStart + 100000, (void *)valBuf);
      ASSERT(code == 0);
    }
    code = streamStatePutBatch_rocksdb(p, pBatch);
    ASSERT(code == 0);

    streamStateDestroyBatch(pBatch);
  }

  taosArrayClear(pList);

  code = taskDbDoCheckpoint(p->pTdbState->pOwner->pBackend, 3, 0, pList);
  ASSERT(code == 0);

  const char *path = "/tmp/backend/stream";
  const char *dump = "/tmp/backend/stream/dump";
  // taosMkDir(dump);
  taosMulMkDir(dump);
  SBkdMgt *mgt = NULL;

  code = bkdMgtCreate((char *)path, &mgt);
  SArray *result = taosArrayInit(4, sizeof(void *));
  bkdMgtGetDelta(mgt, p->pTdbState->idstr, 3, result, (char *)dump);

  taosArrayClear(pList);
  code = taskDbDoCheckpoint(p->pTdbState->pOwner->pBackend, 4, 0, pList);
  ASSERT(code == 0);

  taosArrayDestroy(pList);
  taosArrayClear(result);
  code = bkdMgtGetDelta(mgt, p->pTdbState->idstr, 4, result, (char *)dump);
  ASSERT(code == 0);

  bkdMgtDestroy(mgt);
  streamStateClose((SStreamState *)p, true);
  // {
  //   taosRemoveDir("/tmp/backend");
  //   const char *  path = "/tmp/backend";
  //   SStreamState *p = stateCreate(path);
  // }
  taosRemoveDir(path);
  // streamStateClose((SStreamState *)p, true);
}

TEST_F(BackendEnv, backendChkp) { const char *path = "/tmp"; }

typedef struct BdKV {
  uint32_t k;
  uint32_t v;
} BdKV;

BdKV kvDict[] = {{0, 2},     {1, 2},     {15, 16},     {31, 32},     {56, 64},    {100, 128},
                 {200, 256}, {500, 512}, {1000, 1024}, {2000, 2048}, {3000, 4096}};

TEST_F(BackendEnv, backendUtil) {
  for (int i = 0; i < sizeof(kvDict) / sizeof(kvDict[0]); i++) {
    ASSERT_EQ(nextPow2((uint32_t)(kvDict[i].k)), kvDict[i].v);
  }
}
TEST_F(BackendEnv, oldBackendInit) {
  const char *path = "/tmp/backend1";
  int32_t     code = taosMulMkDir(path);
  ASSERT(code == 0);

  {
    SBackendWrapper *p = NULL;
    int32_t code = streamBackendInit(path, 10, 10, &p);
    streamBackendCleanup((void *)p);
  }
  {
    SBackendWrapper *p = NULL;
    int32_t code = streamBackendInit(path, 10, 10, &p);
    streamBackendCleanup((void *)p);
  }

  taosRemoveDir(path);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}