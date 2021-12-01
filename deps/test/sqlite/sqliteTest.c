#include <stdio.h>
#include "sqlite3.h"

int main(int argc, char const *argv[]) {
  sqlite3 *db;
  char *   err_msg = 0;

  int rc = sqlite3_open("test.db", &db);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);

    return 1;
  }

  char *sql =
      "DROP TABLE IF EXISTS t;"
      "CREATE TABLE t(id BIGINT);";

  rc = sqlite3_exec(db, sql, 0, 0, &err_msg);

  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", err_msg);

    sqlite3_free(err_msg);
    sqlite3_close(db);

    return 1;
  }

  // Write a lot of data
  int  nrows = 100000;
  int  batch = 1000;
  char tsql[1024];

  int v = 0;
  for (int k = 0; k < nrows / batch; k++) {
    sqlite3_exec(db, "begin;", 0, 0, &err_msg);

    for (int i = 0; i < batch; i++) {
      v++;
      sprintf(tsql, "insert into t values (%d)", v);
      rc = sqlite3_exec(db, tsql, 0, 0, &err_msg);

      if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", err_msg);

        sqlite3_free(err_msg);
        sqlite3_close(db);

        return 1;
      }
    }

    sqlite3_exec(db, "commit;", 0, 0, &err_msg);
  }

  sqlite3_close(db);

  return 0;
}
