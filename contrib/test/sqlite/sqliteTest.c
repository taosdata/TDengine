#include <stdio.h>
#include <stdlib.h>

#include "sqlite3.h"

static void count_table(sqlite3 *db) {
  int           rc;
  char *        sql = "select * from t;";
  sqlite3_stmt *stmt = NULL;
  int           nrows = 0;

  rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
  while (SQLITE_ROW == sqlite3_step(stmt)) {
    nrows++;
  }

  printf("Number of rows: %d\n", nrows);
}

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

  {
    // Write a lot of data
    int  nrows = 1000;
    int  batch = 100;
    char tsql[1024];
    int  v = 0;

    // sqlite3_exec(db, "PRAGMA journal_mode=WAL;", 0, 0, &err_msg);
    sqlite3_exec(db, "PRAGMA read_uncommitted=true;", 0, 0, &err_msg);

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

      count_table(db);
      sqlite3_exec(db, "commit;", 0, 0, &err_msg);
    }
  }

  sqlite3_close(db);

  return 0;
}
