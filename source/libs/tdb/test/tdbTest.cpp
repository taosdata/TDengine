#include "gtest/gtest.h"

#include "tdb.h"

TEST(tdb_test, simple_test) {
  TENV*     pEnv;
  TDB *     pDb1, *pDb2, *pDb3;
  pgsz_t    pgSize = 1024;
  cachesz_t cacheSize = 10240;

  // ENV
  GTEST_ASSERT_EQ(tdbEnvCreate(&pEnv, "./tdbtest"), 0);

  GTEST_ASSERT_EQ(tdbEnvSetCache(pEnv, pgSize, cacheSize), 0);

  GTEST_ASSERT_EQ(tdbEnvGetCacheSize(pEnv), cacheSize);

  GTEST_ASSERT_EQ(tdbEnvGetPageSize(pEnv), pgSize);

  GTEST_ASSERT_EQ(tdbEnvOpen(pEnv), 0);

#if 0
  // DB
  tdbOpen(&pDb1, "db.db", "db1", pEnv);
  tdbOpen(&pDb2, "db.db", "db2", pEnv);
  tdbOpen(&pDb3, "index.db", NULL, pEnv);

  // Insert

  // Query

  // Delete

  // Query

  // Close
  tdbClose(pDb1);
  tdbClose(pDb2);
#endif
  tdbEnvClose(pEnv);
}