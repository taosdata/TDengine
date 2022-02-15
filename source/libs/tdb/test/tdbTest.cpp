#include "gtest/gtest.h"

#include "tdb.h"

#define A_ASSERT(op) GTEST_ASSERT_EQ(op, 0)

TEST(tdb_test, simple_test) {
  TENV *pEnv;
  TDB * pDb1, *pDb2;

  // ENV
  tdbEnvCreate(&pEnv);
  tdbEnvSetPageSize(pEnv, 1024);
  tdbEnvSetCacheSize(pEnv, 10240);
  tdbEnvOpen(&pEnv);

  // DB
  tdbOpen(&pDb1, "db.db", "db1", pEnv);
  tdbOpen(&pDb2, "db.db", "db2", pEnv);

  // Insert

  // Query

  // Delete

  // Query

  // Close
  tdbClose(pDb1);
  tdbClose(pDb2);
  tdbEnvClose(pEnv);
}