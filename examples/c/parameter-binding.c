#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <taos.h>
#include <time.h>

bool isPrint = true;

void one_batch_one_table_1(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_2(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_3(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_4(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_5(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_6(TAOS *conn, long totalRows, long batchRows);
void one_batch_one_table_7(TAOS *conn, long totalRows, long batchRows);

void one_batch_multi_table_1(TAOS *conn, long totalRows, long batchRows, int tables);
void one_batch_multi_table_2(TAOS *conn, long totalRows, long batchRows, int tables);
void one_batch_multi_table_3(TAOS *conn, long totalRows, long batchRows, int tables);

void    execute(TAOS *conn, char *sql);
void    prepare_normal_table(TAOS *conn);
void    prepare_super_and_sub_table(TAOS *conn, int subTables);
void    prepare_super_table(TAOS *conn, int subTables);
int64_t getCurrentTimeMill();

TAOS_STMT *A(TAOS *);
void       B(TAOS_STMT *stmt, char sql[]);
void       C(TAOS_STMT *stmt, char sql[]);
void       D(TAOS_STMT *stmt, char sql[], int tag);
void       E(TAOS_STMT *stmt);
void       F(TAOS_STMT *stmt, int64_t ts_start);
void       G1(TAOS_STMT *stmt, int64_t ts_start, int rows);
void       G2(TAOS_STMT *stmt, int rows);
void       H(TAOS_STMT *stmt, int64_t ts_start, int rows);
void       I(TAOS_STMT *stmt);
void       J(TAOS_STMT *stmt);
void       L(TAOS_STMT *stmt);

int main() {
  char host[] = "192.168.56.105";
  srand(time(NULL));

  // connect
  TAOS *conn = taos_connect(host, "root", "taosdata", NULL, 0);
  if (conn == NULL) {
    printf("failed to connect to:%s, reason:%s\n", host, "null taos");
    exit(-1);
  }
  execute(conn, "drop database if exists test");
  execute(conn, "create database if not exists test");
  execute(conn, "use test");

  long totalRows = 1000000;
  long batchRows = 32767;
  int  tables = 10;

  prepare_super_table(conn, 1);
  // A -> B -> D -> [F -> I]... -> J -> L
  //  one_batch_one_table_1(conn, totalRows, batchRows);
  // A -> B -> [D -> [F -> I]... -> J]... -> L
  //  one_batch_one_table_2(conn, totalRows, batchRows);
  // A -> B -> D -> [F... -> I -> J]... -> L
  //  one_batch_one_table_3(conn, totalRows, batchRows);
  // A -> B -> D -> [H -> I -> J]... -> L
  //  one_batch_one_table_4(conn, totalRows, batchRows);
  // A -> B -> [D -> H -> I -> J]... -> L
  //  one_batch_one_table_5(conn, totalRows, batchRows);
  // A -> B -> [D -> H -> I -> J]... -> L
  //  one_batch_one_table_6(conn, totalRows, batchRows);
  // A -> B -> [D -> H -> I -> J]... -> L
  //  one_batch_one_table_7(conn, totalRows, batchRows);

  // A -> B -> [D -> [F -> I]... -> J]... -> L
  //  one_batch_multi_table_1(conn, totalRows, batchRows, tables);
  // A -> B -> [D -> H -> I -> J]... -> L
  //  one_batch_multi_table_2(conn, totalRows, batchRows, tables);
  // A -> B -> [D -> G1 -> G2 -> I -> J]... -> L
  one_batch_multi_table_3(conn, totalRows, batchRows, tables);

  // close
  taos_close(conn);
  taos_cleanup();
  exit(0);
}

// A -> B -> D -> [F -> I]... -> J -> L
void one_batch_one_table_1(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();

  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) (ts, f1) values(?, ?)");
  D(stmt, "t1", 1);
  for (int i = 1; i <= totalRows; ++i) {
    F(stmt, (current + i - 1) * 1000);
    I(stmt);
    if (i % batchRows == 0 || i == totalRows) {
      J(stmt);
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

// A -> B -> D -> [F -> I]... -> J -> L
void one_batch_one_table_2(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();

  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) (ts, f1) values(?, ?)");
  for (int i = 1; i <= totalRows; ++i) {
    if (i % batchRows == 1) {
      D(stmt, "t1", 1);
    }

    F(stmt, (current + i - 1) * 1000);
    I(stmt);
    if (i % batchRows == 0 || i == totalRows) {
      J(stmt);
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_one_table_3(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();

  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) (ts, f1) values(?, ?)");
  D(stmt, "t1", 1);
  for (int i = 1; i <= totalRows; ++i) {
    F(stmt, (current + i - 1) * 1000);
    if (i % batchRows == 0 || i == totalRows) {
      I(stmt);
      J(stmt);
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_one_table_4(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?,?)");
  D(stmt, "t1", 1);
  for (int i = 1; i <= totalRows; i += batchRows) {
    int rows = (i + batchRows) > totalRows ? (totalRows + 1 - i) : batchRows;
    H(stmt, (current + i) * 1000, rows);
    I(stmt);
    J(stmt);
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_one_table_5(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?,?)");
  for (int i = 1; i <= totalRows; i += batchRows) {
    D(stmt, "t1", 1);
    int rows = (i + batchRows) > totalRows ? (totalRows + 1 - i) : batchRows;
    H(stmt, (current + i) * 1000, rows);
    I(stmt);
    J(stmt);
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_one_table_6(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?,?)");
  D(stmt, "t1", 1);
  for (int i = 1; i <= totalRows; i += batchRows) {
    int rows = (i + batchRows) > totalRows ? (totalRows + 1 - i) : batchRows;
    G1(stmt, (current + i) * 1000, rows);
    G2(stmt, rows);
    I(stmt);
    J(stmt);
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_one_table_7(TAOS *conn, long totalRows, long batchRows) {
  // given
  time_t current;
  time(&current);
  current -= totalRows;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?,?)");
  for (int i = 1; i <= totalRows; i += batchRows) {
    if (i % batchRows == 1) {
      D(stmt, "t1", 1);
    }
    int rows = (i + batchRows) > totalRows ? (totalRows + 1 - i) : batchRows;
    G1(stmt, (current + i) * 1000, rows);
    G2(stmt, rows);
    I(stmt);
    J(stmt);
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, time cost: %lld ms\n", totalRows, batchRows, (end - start));
}

void one_batch_multi_table_1(TAOS *conn, long totalRows, long batchRows, int tables) {
  // given
  time_t current;
  time(&current);
  long eachTable = (totalRows - 1) / tables + 1;
  current -= eachTable;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?, ?)");

  for (int tbIndex = 0; tbIndex < tables; ++tbIndex) {
    char tbname[10];
    sprintf(tbname, "t%d", tbIndex);

    eachTable = ((tbIndex + 1) * eachTable > totalRows) ? (totalRows - tbIndex * eachTable) : eachTable;
    for (int rowIndex = 1; rowIndex <= eachTable; ++rowIndex) {
      if (rowIndex % batchRows == 1) {
        D(stmt, tbname, tbIndex);
        if (isPrint)
          printf("\ntbIndex: %d, table_rows: %ld, rowIndex: %d, batch_rows: %ld\n", tbIndex, eachTable, rowIndex,
                 batchRows);
      }
      F(stmt, (current + rowIndex - 1) * 1000);
      I(stmt);
      if (rowIndex % batchRows == 0 || rowIndex == eachTable) {
        J(stmt);
      }
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, table: %d, eachTableRows: %ld, time cost: %lld ms\n", totalRows, batchRows,
         tables, eachTable, (end - start));
}

void one_batch_multi_table_2(TAOS *conn, long totalRows, long batchRows, int tables) {
  // given
  time_t current;
  time(&current);
  long eachTable = (totalRows - 1) / tables + 1;
  current -= eachTable;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?,?)");
  for (int tbIndex = 0; tbIndex < tables; ++tbIndex) {
    char tbname[10];
    sprintf(tbname, "t%d", tbIndex);

    eachTable = ((tbIndex + 1) * eachTable > totalRows) ? (totalRows - tbIndex * eachTable) : eachTable;
    for (int rowIndex = 1; rowIndex <= eachTable; rowIndex += batchRows) {
      int rows = (rowIndex + batchRows) > eachTable ? (eachTable + 1 - rowIndex) : batchRows;

      if (rowIndex % batchRows == 1) {
        D(stmt, tbname, tbIndex);
        if (isPrint)
          printf("\ntbIndex: %d, table_rows: %ld, rowIndex: %d, batch_rows: %d\n", tbIndex, eachTable, rowIndex, rows);
      }

      H(stmt, (current + rowIndex) * 1000, rows);
      I(stmt);
      J(stmt);
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, table: %d, eachTableRows: %ld, time cost: %lld ms\n", totalRows, batchRows,
         tables, eachTable, (end - start));
}

void one_batch_multi_table_3(TAOS *conn, long totalRows, long batchRows, int tables) {
  // given
  time_t current;
  time(&current);
  long eachTable = (totalRows - 1) / tables + 1;
  current -= eachTable;

  int64_t start = getCurrentTimeMill();
  // when
  TAOS_STMT *stmt = A(conn);
  B(stmt, "insert into ? using weather tags(?) values(?, ?)");
  for (int tbIndex = 0; tbIndex < tables; ++tbIndex) {
    char tbname[10];
    sprintf(tbname, "t%d", tbIndex);

    eachTable = ((tbIndex + 1) * eachTable > totalRows) ? (totalRows - tbIndex * eachTable) : eachTable;
    for (int rowIndex = 1; rowIndex <= eachTable; rowIndex += batchRows) {
      int rows = (rowIndex + batchRows) > eachTable ? (eachTable + 1 - rowIndex) : batchRows;

      if (rowIndex % batchRows == 1) {
        D(stmt, tbname, tbIndex);
        if (isPrint)
          printf("\ntbIndex: %d, table_rows: %ld, rowIndex: %d, batch_rows: %d\n", tbIndex, eachTable, rowIndex, rows);
      }
      G1(stmt, (current + rowIndex) * 1000, rows);
      G2(stmt, rows);
      I(stmt);
      J(stmt);
    }
  }
  L(stmt);

  int64_t end = getCurrentTimeMill();
  printf("totalRows: %ld, batchRows: %ld, table: %d, eachTableRows: %ld, time cost: %lld ms\n", totalRows, batchRows,
         tables, eachTable, (end - start));
}

void execute(TAOS *conn, char *sql) {
  TAOS_RES *psql = taos_query(conn, sql);
  if (psql == NULL) {
    printf("failed to execute: %s, reason: %s\n", sql, taos_errstr(psql));
    taos_free_result(psql);
    taos_close(conn);
    exit(-1);
  }
  taos_free_result(psql);
}

TAOS_STMT *A(TAOS *conn) {
  if (isPrint) printf("A -> ");
  return taos_stmt_init(conn);
}

void B(TAOS_STMT *stmt, char sql[]) {
  if (isPrint) printf("B -> ");

  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to prepare stmt: %s, reason: %s\n", sql, taos_stmt_errstr(stmt));
    return;
  }
}

void C(TAOS_STMT *stmt, char tbname[]) {
  if (isPrint) printf("C -> ");

  int code = taos_stmt_set_tbname(stmt, tbname);
  if (code != 0) printf("failed to set_tbname_tags, reason: %s\n", taos_stmt_errstr(stmt));
}

void D(TAOS_STMT *stmt, char tbname[], int tag) {
  if (isPrint) printf("D -> ");

  TAOS_BIND tags[1];
  tags[0].buffer_type = TSDB_DATA_TYPE_INT;
  int tag_value = tag >= 0 ? tag : rand() % 100;
  tags[0].buffer = &tag_value;
  tags[0].buffer_length = sizeof(tag_value);
  tags[0].length = &tags[0].buffer_length;
  tags[0].is_null = NULL;
  // set_tbname_tags
  int code = taos_stmt_set_tbname_tags(stmt, tbname, tags);
  if (code != 0) printf("failed to set_tbname_tags, reason: %s\n", taos_stmt_errstr(stmt));
}

void E(TAOS_STMT *stmt) {
  // TODO
}

void F(TAOS_STMT *stmt, int64_t ts) {
  if (isPrint) printf("F -> ");

  TAOS_BIND params[2];
  // timestamp
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = &ts;
  params[0].buffer_length = sizeof(ts);
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;
  // int
  int value = rand() % 100;
  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = &value;
  params[1].buffer_length = sizeof(value);
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  // bind
  int code = taos_stmt_bind_param(stmt, params);
  if (0 != code) printf("failed to bind_param, reason: %s\n", taos_stmt_errstr(stmt));
}

void H(TAOS_STMT *stmt, int64_t ts_start, int rows) {
  if (isPrint) printf("H -> ");

  TAOS_MULTI_BIND params[2];
  // timestamp
  int64_t ts[rows];
  for (int i = 0; i < rows; ++i) {
    ts[i] = ts_start + i * 1000;
  }
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = ts;
  params[0].buffer_length = sizeof(ts[0]);
  params[0].length = malloc(sizeof(int64_t) * rows);
  char is_null[rows];
  for (int i = 0; i < rows; i++) {
    is_null[i] = 0;
  }
  params[0].is_null = is_null;
  params[0].num = rows;
  // f1
  int32_t values[rows];
  for (int i = 0; i < rows; ++i) {
    values[i] = rand() % 100;
  }
  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = values;
  params[1].buffer_length = sizeof(int32_t);
  params[1].length = malloc(sizeof(int32_t) * rows);
  params[1].is_null = is_null;
  params[1].num = rows;

  int code = taos_stmt_bind_param_batch(stmt, params);
  if (code != 0) {
    printf("failed to bind_param_batch, reason: %s\n", taos_stmt_errstr(stmt));
    return;
  }
}

void G1(TAOS_STMT *stmt, int64_t ts_start, int rows) {
  if (isPrint) printf("G1 -> ");

  // timestamp
  TAOS_MULTI_BIND param0[1];
  int64_t         ts[rows];
  for (int i = 0; i < rows; ++i) {
    ts[i] = ts_start + i * 1000;
  }
  param0[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  param0[0].buffer = ts;
  param0[0].buffer_length = sizeof(ts[0]);
  param0[0].length = malloc(sizeof(int64_t) * rows);
  char is_null[rows];
  for (int i = 0; i < rows; i++) {
    is_null[i] = 0;
  }
  param0[0].is_null = is_null;
  param0[0].num = rows;
  int code = taos_stmt_bind_single_param_batch(stmt, param0, 0);
  if (code != 0) {
    printf("failed to bind_single_param_batch, reason: %s\n", taos_stmt_errstr(stmt));
    return;
  }
}

void G2(TAOS_STMT *stmt, int rows) {
  if (isPrint) printf("G2 -> ");

  // f1
  TAOS_MULTI_BIND param1[1];
  int32_t         values[rows];
  for (int i = 0; i < rows; ++i) {
    values[i] = rand() % 100;
  }
  param1[0].buffer_type = TSDB_DATA_TYPE_INT;
  param1[0].buffer = values;
  param1[0].buffer_length = sizeof(int32_t);
  param1[0].length = malloc(sizeof(int32_t) * rows);
  char is_null[rows];
  for (int i = 0; i < rows; i++) {
    is_null[i] = 0;
  }
  param1[0].is_null = is_null;
  param1[0].num = rows;

  int code = taos_stmt_bind_single_param_batch(stmt, param1, 1);
  if (code != 0) {
    printf("failed to bind_single_param_batch, reason: %s\n", taos_stmt_errstr(stmt));
    return;
  }
}

void I(TAOS_STMT *stmt) {
  if (isPrint) printf("I -> ");

  int code = taos_stmt_add_batch(stmt);
  if (code != 0) {
    printf("failed to add_batch, reason: %s\n", taos_stmt_errstr(stmt));
    return;
  }
}

void J(TAOS_STMT *stmt) {
  if (isPrint) printf("J -> ");

  int code = taos_stmt_execute(stmt);
  if (code != 0) {
    printf("failed to execute, reason: %s\n", taos_stmt_errstr(stmt));
    return;
  }
}

void L(TAOS_STMT *stmt) {
  if (isPrint) printf("L\n");

  taos_stmt_close(stmt);
}

void prepare_super_table(TAOS *conn, int subTables) {
  char sql[100] = "drop table weather";
  execute(conn, sql);
  if (isPrint) printf("sql>>> %s\n", sql);

  sprintf(sql, "create table weather(ts timestamp, f1 int) tags(t1 int)");
  execute(conn, sql);
  if (isPrint) printf("sql>>> %s\n", sql);

  for (int i = 0; i < subTables; i++) {
    sprintf(sql, "drop table t%d", i);
    if (isPrint) printf("sql>>> %s\n", sql);
    execute(conn, sql);
  }
}

void prepare_normal_table(TAOS *conn) {
  execute(conn, "drop table weather");
  execute(conn, "create table weather(ts timestamp, f1 int) tags(t1 int)");
}

void prepare_super_and_sub_table(TAOS *conn, int subTables) {
  execute(conn, "drop table weather");
  execute(conn, "create table weather(ts timestamp, f1 int) tags(t1 int)");
  for (int i = 0; i < subTables; i++) {
    char sql[100];
    sprintf(sql, "drop table t%d", i);
    if (isPrint) printf("sql>>> %s\n", sql);
    execute(conn, sql);

    sprintf(sql, "create table t%d using weather tags(%d)", i, i);
    if (isPrint) printf("sql>>> %s\n", sql);
    execute(conn, sql);
  }
}

int64_t getCurrentTimeMill() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return ((unsigned long long)tv.tv_sec * 1000 + (unsigned long long)tv.tv_usec / 1000);
}