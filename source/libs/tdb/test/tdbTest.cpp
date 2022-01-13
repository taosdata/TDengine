#include "gtest/gtest.h"

#include "tdb.h"

TEST(tdb_api_test, tdb_create_open_close_db_test) {
  int  ret;
  TDB *dbp;

  tdbCreateDB(&dbp, TDB_BTREE_T);

  tdbOpenDB(dbp, 0);

  tdbCloseDB(dbp, 0);
}