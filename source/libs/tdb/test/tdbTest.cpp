#include "gtest/gtest.h"

#include "tdbInt.h"

TEST(tdb_test, simple_test) {
  int    ret;
  STEnv *pEnv;
  STDB  *pDb;
  int    nData = 10000000;

  // Open Env
  ret = tdbEnvOpen("tdb", 4096, 256000, &pEnv);
  GTEST_ASSERT_EQ(ret, 0);

  // Create a database
  ret = tdbDbOpen("db.db", TDB_VARIANT_LEN, TDB_VARIANT_LEN, NULL, pEnv, &pDb);
  GTEST_ASSERT_EQ(ret, 0);

  {
    char key[64];
    char val[64];

    {  // Insert some data

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);
        ret = tdbDbInsert(pDb, key, strlen(key), val, strlen(val));
        GTEST_ASSERT_EQ(ret, 0);
      }
    }

    {  // Query the data
      void *pVal = NULL;
      int   vLen;

      for (int i = 1; i <= nData; i++) {
        sprintf(key, "key%d", i);
        sprintf(val, "value%d", i);

        ret = tdbDbGet(pDb, key, strlen(key), &pVal, &vLen);
        GTEST_ASSERT_EQ(ret, 0);

        GTEST_ASSERT_EQ(vLen, strlen(val));
        GTEST_ASSERT_EQ(memcmp(val, pVal, vLen), 0);
      }

      TDB_FREE(pVal);
    }

    {  // Iterate to query the DB data
      STDBC *pDBC;
      void  *pKey = NULL;
      void  *pVal = NULL;
      int    vLen, kLen;
      int    count = 0;

      ret = tdbDbcOpen(pDb, &pDBC);
      GTEST_ASSERT_EQ(ret, 0);

      for (;;) {
        ret = tdbDbNext(pDBC, &pKey, &kLen, &pVal, &vLen);
        if (ret < 0) break;
        count++;
      }

      GTEST_ASSERT_EQ(count, nData);

      tdbDbcClose(pDBC);

      TDB_FREE(pKey);
      TDB_FREE(pVal);
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