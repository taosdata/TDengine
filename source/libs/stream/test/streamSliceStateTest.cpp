#include <gtest/gtest.h>
#include "tstream.h"
#include "streamInt.h"
#include "tglobal.h"
#include "streamState.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"


SStreamState *stateCreate2(const char *path) {
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
void *backendOpen2() {
  streamMetaInit();
  const char   *path = "/tmp/streamslicestate/";
  SStreamState *p = stateCreate2(path);
  ASSERT(p != NULL);
  return p;
}

TSKEY compareTs1(void* pKey) {
  SWinKey* pWinKey = (SWinKey*)pKey;
  return pWinKey->ts;
}

TEST(getHashSortNextRowFn, getHashSortNextRowTest) {
  void   *pState = backendOpen2();
  SStreamFileState *pFileState = NULL;
  int32_t code = streamFileStateInit(1024, sizeof(SWinKey), 10, 0, compareTs1, pState, INT64_MAX, "aaa", 123,
                                     STREAM_STATE_BUFF_HASH, &pFileState);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SWinKey key1;
  key1.groupId = 123;
  key1.ts = 456;
  char str[] = "abc";
  code = streamStateFillPut_rocksdb((SStreamState*)pState, &key1, str, sizeof(str));
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SWinKey key2;
  key2.groupId = 123;
  key2.ts = 460;
  code = streamStateFillPut_rocksdb((SStreamState*)pState, &key2, str, sizeof(str));
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  SWinKey key3;
  key3.groupId = 123;
  void* pVal = NULL;
  int32_t len = 0;
  int32_t wincode = 0;
  code = getHashSortNextRow(pFileState, &key1, &key3, &pVal, &len, &wincode);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  ASSERT_EQ(key3.ts, key2.ts);
}
