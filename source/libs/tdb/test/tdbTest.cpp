#include "gtest/gtest.h"

#include "tdbInt.h"

TEST(tdb_test, simple_test) {
  int    ret;
  STEnv *pEnv;
  STDb  *pDb;
  int    nData = 1000;

  // Open Env
  ret = tdbEnvOpen("tdb", 1024, 256, &pEnv);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  ret = tdbDbOpen("db.db", TDB_VARIANT_LEN, TDB_VARIANT_LEN, NULL, pEnv, &pDb);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char  key[64];
    char  val[64];
    void *pVal;
    int   vLen;

    {  // Insert some data

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);
        ret = tdbDbInsert(pDb, key, strlen(key), val, strlen(val));
        GTEST_ASSERT_EQ(ret, 0);
      }
    }

    {  // Query the data
      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);

        ret = tdbDbGet(pDb, key, strlen(key), &pVal, &vLen);
        GTEST_ASSERT_EQ(ret, 0);

        GTEST_ASSERT_EQ(vLen, strlen(val));
        GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);
      }
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