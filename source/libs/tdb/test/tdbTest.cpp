#include <gtest/gtest.h>

#include "tdbInt.h"

TEST(tdb_test, simple_test) {
  int    ret;
  STEnv *pEnv;
  STDb  *pDb;

  // Open Env
  ret = tdbEnvOpen("tdb", 1024, 20, &pEnv);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  ret = tdbDbOpen("db.db", TDB_VARIANT_LEN, TDB_VARIANT_LEN, NULL, pEnv, &pDb);
  GTEST_ASSERT_EQ(ret, 0);

  {  // Insert some data
    char key[64];
    char val[64];

    for (int i = 1; i <= 1000; i++) {
      sprintf(key, "key%d", i);
      sprintf(val, "value%d", i);
      ret = tdbDbInsert(pDb, key, strlen(key), val, strlen(val));
      GTEST_ASSERT_EQ(ret, 0);
    }
  }

  ret = tdbDbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbDbClose(pDb);

  // Close Env
  ret = tdbEnvClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}