#include "gtest/gtest.h"

#include <iostream>

#include "tdb_mpool.h"

TEST(tdb_mpool_test, test1) {
  TDB_MPOOL * mp;
  TDB_MPFILE *mpf;
  pgno_t      pgno;
  void *      pgdata;

  // open mp
  tdbMPoolOpen(&mp, 16384, 4096);

  // open mpf
  tdbMPoolFileOpen(&mpf, "test.db", mp);

#define TEST1_TOTAL_PAGES 100
  for (int i = 0; i < TEST1_TOTAL_PAGES; i++) {
    tdbMPoolFileNewPage(mpf, &pgno, pgdata);

    *(pgno_t *)pgdata = i;
  }

  // close mpf
  tdbMPoolFileClose(mpf);

  // close mp
  tdbMPoolClose(mp);
}
