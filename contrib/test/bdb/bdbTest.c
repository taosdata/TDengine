#include <stdio.h>
#include <stdlib.h>

#include "db.h"

// refer: https://docs.oracle.com/cd/E17076_05/html/gsg/C/BerkeleyDB-Core-C-GSG.pdf

int main(int argc, char const *argv[]) {
  DB *      dbp;
  int       ret;
  u_int32_t flags;
  DBT       key, value;

  ret = db_create(&dbp, NULL, 0);
  if (ret != 0) {
    exit(1);
  }

  flags = DB_CREATE;

  ret = dbp->open(dbp, NULL, "meta.db", NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    exit(1);
  }

  dbp->close(dbp, 0);

  return 0;
}
