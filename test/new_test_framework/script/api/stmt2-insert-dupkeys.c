#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "taos.h"

int CTB_NUMS = 3;
int ROW_NUMS = 3;

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void createdb(TAOS* taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create stable db.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "use db");
}

#define INIT(tbs, ts, ts_len, b, b_len, tags, paramv)               \
do {                                                                \
  /* tbname */                                                      \
  tbs = (char**)malloc(CTB_NUMS * sizeof(char*));                   \
  for (int i = 0; i < CTB_NUMS; i++) {                              \
    tbs[i] = (char*)malloc(sizeof(char) * 20);                      \
    sprintf(tbs[i], "ctb_%d", i);                                   \
  }                                                                 \
  /* col params */                                                  \
  ts = (int64_t**)malloc(CTB_NUMS * sizeof(int64_t*));              \
  b = (char**)malloc(CTB_NUMS * sizeof(char*));                     \
  ts_len = (int*)malloc(ROW_NUMS * sizeof(int));                    \
  b_len = (int*)malloc(ROW_NUMS * sizeof(int));                     \
  for (int i = 0; i < ROW_NUMS; i++) {                              \
    ts_len[i] = sizeof(int64_t);                                    \
    b_len[i] = 1;                                                   \
  }                                                                 \
  for (int i = 0; i < CTB_NUMS; i++) {                              \
    ts[i] = (int64_t*)malloc(ROW_NUMS * sizeof(int64_t));           \
    b[i] = (char*)malloc(ROW_NUMS * sizeof(char));                  \
    for (int j = 0; j < ROW_NUMS; j++) {                            \
      ts[i][j] = 1591060628000 + j;                                 \
      b[i][j] = (char)('a' + j);                                    \
    }                                                               \
  }                                                                 \
  /*tag params */                                                   \
  int t1 = 0;                                                       \
  int t1len = sizeof(int);                                          \
  int t2len = 3;                                                    \
  /* bind params */                                                 \
  paramv = (TAOS_STMT2_BIND**)malloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));                              \
  tags = (TAOS_STMT2_BIND**)malloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));                                \
  for (int i = 0; i < CTB_NUMS; i++) {                                                                  \
    /* create tags */                                                                                   \
    tags[i] = (TAOS_STMT2_BIND*)malloc(2 * sizeof(TAOS_STMT2_BIND));                                    \
    tags[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, &t1, &t1len, NULL, 0};                           \
    tags[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, "after", &t2len, NULL, 0};                    \
    /* create col params */                                                                             \
    paramv[i] = (TAOS_STMT2_BIND*)malloc(2 * sizeof(TAOS_STMT2_BIND));                                  \
    paramv[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};  \
    paramv[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};       \
  }                                                                                                     \
} while (0)

#define UINIT(tbs, ts, ts_len, b, b_len, tags, paramv)              \
do {                                                                \
  for (int i = 0; i < CTB_NUMS; i++) {                              \
    free(tbs[i]);                                                   \
  }                                                                 \
  free(tbs);                                                        \
  for (int i = 0; i < CTB_NUMS; i++) {                              \
    free(ts[i]);                                                    \
    free(b[i]);                                                     \
  }                                                                 \
  free(ts);                                                         \
  free(b);                                                          \
  free(ts_len);                                                     \
  free(b_len);                                                      \
  for (int i = 0; i < CTB_NUMS; i++) {                              \
    free(tags[i]);                                                  \
    free(paramv[i]);                                                \
  }                                                                 \
  free(tags);                                                       \
  free(paramv);                                                     \
} while (0)

void insert(TAOS* taos, char **tbs, TAOS_STMT2_BIND **tags, TAOS_STMT2_BIND **paramv, const char* sql)
{
  clock_t start, end;
  double  cpu_time_used;

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2 *stmt = taos_stmt2_init(taos, &option);
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    exit(EXIT_FAILURE);
  }

  // bind
  start = clock();
  TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, tags, paramv};
  if (taos_stmt2_bind_param(stmt, &bindv, -1)) {
    printf("failed to execute taos_stmt2_bind_param statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    exit(EXIT_FAILURE);
  }
  end = clock();
  cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("stmt2-bind [%s] insert Time used: %f seconds\n", sql, cpu_time_used);
  start = clock();

  // exec
  if (taos_stmt2_exec(stmt, NULL)) {
    printf("failed to execute taos_stmt2_exec statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    exit(EXIT_FAILURE);
  }
  end = clock();
  cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
  printf("stmt2-exec [%s] insert Time used: %f seconds\n", sql, cpu_time_used);

  taos_stmt2_close(stmt);
}

void insert_dist(TAOS* taos, const char *sql) {
  char **tbs, **b;
  int64_t **ts;
  int *ts_len, *b_len;
  TAOS_STMT2_BIND **paramv, **tags;

  INIT(tbs, ts, ts_len, b, b_len, tags, paramv);

  insert(taos, tbs, tags, paramv, sql);

  UINIT(tbs, ts, ts_len, b, b_len, tags, paramv);
}

void insert_dup_rows(TAOS* taos, const char *sql) {
  char **tbs, **b;
  int64_t **ts;
  int *ts_len, *b_len;
  TAOS_STMT2_BIND **paramv, **tags;

  INIT(tbs, ts, ts_len, b, b_len, tags, paramv);

  // insert duplicate rows
  for (int i = 0; i < CTB_NUMS; i++) {
    for (int j = 0; j < ROW_NUMS; j++) {
      ts[i][j] = 1591060628000;
      b[i][j] = (char)('x' + j);
    }
  }
  for (int i = 0; i < CTB_NUMS; i++) {
    paramv[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
    paramv[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
  }
  insert(taos, tbs, tags, paramv, sql);

  UINIT(tbs, ts, ts_len, b, b_len, tags, paramv);
}

void insert_dup_tables(TAOS* taos, const char *sql) {
  char **tbs, **b;
  int64_t **ts;
  int *ts_len, *b_len;
  TAOS_STMT2_BIND **paramv, **tags;

  INIT(tbs, ts, ts_len, b, b_len, tags, paramv);

  for (int i = 0; i < CTB_NUMS; i++) {
    sprintf(tbs[i], "ctb_%d", i % 2);
  }

  for (int i = 0; i < CTB_NUMS; i++) {
    paramv[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
    paramv[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
  }
  insert(taos, tbs, tags, paramv, sql);

  UINIT(tbs, ts, ts_len, b, b_len, tags, paramv);
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(EXIT_FAILURE);
  }

  createdb(taos);
  // insert distinct rows
  insert_dist(taos, "insert into db.? using db.stb tags(?,?)values(?,?)");
  // insert duplicate rows
  insert_dup_rows(taos, "insert into db.? values(?,?)");
  // insert duplicate tables
  insert_dup_tables(taos, "insert into db.? values(?,?)");

  taos_close(taos);
  taos_cleanup();
}

// final results
// taos> select * from ctb_0;
//            ts            |      b       |
// =========================================
//  2020-06-02 09:17:08.000 | z            |
//  2020-06-02 09:17:08.001 | b            |
//  2020-06-02 09:17:08.002 | c            |
// Query OK, 3 row(s) in set (0.003975s)
// 
// taos> select * from ctb_1;
//            ts            |      b       |
// =========================================
//  2020-06-02 09:17:08.000 | z            |
//  2020-06-02 09:17:08.001 | b            |
//  2020-06-02 09:17:08.002 | c            |
// Query OK, 3 row(s) in set (0.007241s)

// taos> select * from ctb_2;
//            ts            |      b       |
// =========================================
//  2020-06-02 09:17:08.000 | z            |
//  2020-06-02 09:17:08.001 | b            |
//  2020-06-02 09:17:08.002 | c            |
// Query OK, 3 row(s) in set (0.005443s)
