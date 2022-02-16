#include "gtest/gtest.h"

#include "tdb.h"

TEST(tdb_test, simple_test) {
  TENV *    pEnv;
  TDB *     pDb1, *pDb2, *pDb3;
  pgsz_t    pgSize = 1024;
  cachesz_t cacheSize = 10240;

  // ENV
  GTEST_ASSERT_EQ(tdbEnvCreate(&pEnv, "./testtdb"), 0);

  GTEST_ASSERT_EQ(tdbEnvSetCache(pEnv, pgSize, cacheSize), 0);

  GTEST_ASSERT_EQ(tdbEnvGetCacheSize(pEnv), cacheSize);

  GTEST_ASSERT_EQ(tdbEnvGetPageSize(pEnv), pgSize);

  GTEST_ASSERT_EQ(tdbEnvOpen(pEnv), 0);

#if 1
  // DB
  GTEST_ASSERT_EQ(tdbCreate(&pDb1), 0);

  // GTEST_ASSERT_EQ(tdbSetKeyLen(pDb1, 8), 0);

  // GTEST_ASSERT_EQ(tdbGetKeyLen(pDb1), 8);

  // GTEST_ASSERT_EQ(tdbSetValLen(pDb1, 3), 0);

  // GTEST_ASSERT_EQ(tdbGetValLen(pDb1), 3);

  // GTEST_ASSERT_EQ(tdbSetDup(pDb1, 1), 0);

  // GTEST_ASSERT_EQ(tdbGetDup(pDb1), 1);

  // GTEST_ASSERT_EQ(tdbSetCmprFunc(pDb1, NULL), 0);

  tdbEnvBeginTxn(pEnv);

  GTEST_ASSERT_EQ(tdbOpen(pDb1, "db.db", "db1", pEnv), 0);

  // char *key = "key1";
  // char *val = "value1";
  // tdbInsert(pDb1, (void *)key, strlen(key), (void *)val, strlen(val));

  tdbEnvCommit(pEnv);

#if 0
  // Insert

  // Query

  // Delete

  // Query
#endif

  // GTEST_ASSERT_EQ(tdbOpen(&pDb2, "db.db", "db2", pEnv), 0);
  // GTEST_ASSERT_EQ(tdbOpen(&pDb3, "index.db", NULL, pEnv), 0);
  // tdbClose(pDb3);
  // tdbClose(pDb2);
  tdbClose(pDb1);
#endif

  tdbEnvClose(pEnv);
}