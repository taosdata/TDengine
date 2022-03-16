#include "gtest/gtest.h"

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
    ret = tdbDbInsert(pDb, "key1", 4, "value1", 6);
    GTEST_ASSERT_EQ(ret, 0);

    ret = tdbDbInsert(pDb, "key2", 4, "value1", 6);
    GTEST_ASSERT_EQ(ret, 0);

  //   ret = tdbDbInsert(pDb, "key3", 4, "value1", 6);
  //   GTEST_ASSERT_EQ(ret, 0);
  }

  ret = tdbDbDrop(pDb);
  GTEST_ASSERT_EQ(ret, 0);

  // Close a database
  tdbDbClose(pDb);

  // Close Env
  ret = tdbEnvClose(pEnv);
  GTEST_ASSERT_EQ(ret, 0);
}