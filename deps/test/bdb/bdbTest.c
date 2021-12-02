#include <stdio.h>
#include <stdlib.h>

#include "db.h"

// refer: https://docs.oracle.com/cd/E17076_05/html/gsg/C/BerkeleyDB-Core-C-GSG.pdf

int main(int argc, char const *argv[]) {
  DB *     db;
  int      ret;
  uint32_t flags;

  ret = db_create(&db, NULL, 0);
  if (ret != 0) {
    exit(1);
  }

  flags = DB_CREATE;

  ret = db->open(db, NULL, "test.db", NULL, DB_BTREE, flags, 0);
  if (ret != 0) {
    exit(1);
  }

  db->close(db, 0);

  return 0;
}
