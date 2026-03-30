#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include "taos.h"

static int64_t now_ms(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static int exec_sql(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  if (res == NULL) {
    printf("exec sql failed: %s\n", sql);
    return -1;
  }
  if (taos_errno(res) != 0) {
    printf("exec sql error: 0x%x, %s, sql: %s\n", taos_errno(res), taos_errstr(res), sql);
    taos_free_result(res);
    return -1;
  }
  taos_free_result(res);
  return 0;
}

static int init_env(TAOS *taos, const char *db_name) {
  char sql[512] = {0};

  snprintf(sql, sizeof(sql), "create database if not exists %s WAL_RETENTION_PERIOD 0", db_name);
  if (exec_sql(taos, sql) != 0) return -1;

  snprintf(sql, sizeof(sql), "use %s", db_name);
  if (exec_sql(taos, sql) != 0) return -1;

  if (exec_sql(taos, "drop table if exists stb") != 0) return -1;

  // Minimal schema for reproduction:
  // - c_decimal: the value expected to stay as "21.4300 / NULL / 87.6500"
  // - c_blob: when bound in the same stmt2 batch, decimal becomes incorrect in affected versions.
  if (exec_sql(taos, "create table stb (ts timestamp, c_decimal decimal(20,4), c_blob blob) tags(tg nchar(32))") != 0)
    return -1;

  return 0;
}

static int insert_stmt2(TAOS *taos, const char *table_name, int64_t ts_values[3]) {
  int code = 0;
  int affected = 0;
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2 *stmt = taos_stmt2_init(taos, &option);
  if (stmt == NULL) {
    printf("taos_stmt2_init(insert) failed\n");
    return -1;
  }

  code = taos_stmt2_prepare(stmt, "insert into ? using stb tags(?) values(?,?,?)", 0);
  if (code != 0) {
    printf("taos_stmt2_prepare(insert) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  char *tb_names[1] = {(char *)table_name};
  char tag[] = "tag_stmt";
  int32_t tag_len = (int32_t)strlen(tag);
  TAOS_STMT2_BIND tag_bind[1];
  TAOS_STMT2_BIND *tags[1] = {tag_bind};
  tag_bind[0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_NCHAR, tag, &tag_len, NULL, 1};

  int32_t ts_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
  char nulls[3] = {0, 1, 0};

  // Variable-length binds must use a contiguous buffer:
  // row0 bytes + row2 bytes. row1 is NULL and consumes no bytes.
  char decimal_buf[] = "21.430087.6500";
  int32_t decimal_len[3] = {7, 0, 7};

  // Blob follows the same contiguous-buffer rule.
  uint8_t blob_buf[6] = {0x2a, 0x2b, 0x2c, 0x3a, 0x3b, 0x3c};
  int32_t blob_len[3] = {3, 0, 3};

  TAOS_STMT2_BIND col_bind[3];
  TAOS_STMT2_BIND *bind_cols[1] = {col_bind};
  col_bind[0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, ts_values, ts_len, NULL, 3};
  col_bind[1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_DECIMAL, decimal_buf, decimal_len, nulls, 3};
  col_bind[2] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BLOB, blob_buf, blob_len, nulls, 3};

  TAOS_STMT2_BINDV bindv = {1, tb_names, tags, bind_cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != 0) {
    printf("taos_stmt2_bind_param(insert) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_exec(stmt, &affected);
  if (code != 0) {
    printf("taos_stmt2_exec(insert) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  printf("insert affected rows: %d\n", affected);
  taos_stmt2_close(stmt);
  return 0;
}

static int query_stmt2(TAOS *taos, const char *sql, const char *table_name, int64_t ts_start, const char *prefix) {
  int code = 0;
  int row_idx = 0;
  TAOS_RES *res = NULL;
  TAOS_ROW row = NULL;
  TAOS_FIELD *fields = NULL;
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2 *stmt = taos_stmt2_init(taos, &option);
  if (stmt == NULL) {
    printf("taos_stmt2_init(query) failed\n");
    return -1;
  }

  code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("taos_stmt2_prepare(query) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  int32_t tb_len = (int32_t)strlen(table_name);
  TAOS_STMT2_BIND qbind[2];
  TAOS_STMT2_BIND *qcols[1] = {qbind};
  qbind[0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, (void *)table_name, &tb_len, NULL, 1};
  qbind[1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, &ts_start, NULL, NULL, 1};
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, qcols};

  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  if (code != 0) {
    printf("taos_stmt2_bind_param(query) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  code = taos_stmt2_exec(stmt, NULL);
  if (code != 0) {
    printf("taos_stmt2_exec(query) failed: %s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return -1;
  }

  res = taos_stmt2_result(stmt);
  if (res == NULL || taos_errno(res) != 0) {
    printf("stmt2 query result failed: 0x%x, %s\n", taos_errno(res), taos_errstr(res));
    taos_stmt2_close(stmt);
    return -1;
  }

  fields = taos_fetch_fields(res);
  if (fields == NULL) {
    printf("taos_fetch_fields(query) failed\n");
    taos_stmt2_close(stmt);
    return -1;
  }

  printf("query sql: %s\n", sql);
  while ((row = taos_fetch_row(res)) != NULL) {
    char buf[512] = {0};
    taos_print_row_with_size(buf, sizeof(buf), row, fields, taos_num_fields(res));
    printf("%s[%d]: %s\n", prefix, row_idx, buf);
    row_idx++;
  }
  printf("%s row count: %d\n", prefix, row_idx);

  taos_stmt2_close(stmt);
  return 0;
}

int main(void) {
  char db_name[64] = {0};
  const char *table_name = "tb1";
  int64_t ts_values[3] = {1722222222456LL, 1722222223456LL, 1722222224456LL};

  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (taos == NULL) {
    printf("taos_connect failed: %s\n", taos_errstr(taos));
    return 1;
  }

  snprintf(db_name, sizeof(db_name), "db_stmt2_decimal_min_%lld", (long long)now_ms());
  printf("db_name: %s\n", db_name);

  if (init_env(taos, db_name) != 0) {
    taos_close(taos);
    taos_cleanup();
    return 1;
  }
  if (insert_stmt2(taos, table_name, ts_values) != 0) {
    taos_close(taos);
    taos_cleanup();
    return 1;
  }

  if (query_stmt2(taos, "select tg,ts,c_decimal,c_blob from stb where tbname = ? and ts >= ? order by ts", table_name,
                  ts_values[0], "full_query") != 0) {
    taos_close(taos);
    taos_cleanup();
    return 1;
  }

  if (query_stmt2(taos, "select tg,ts,c_decimal from stb where tbname = ? and ts >= ? order by ts", table_name,
                  ts_values[0], "decimal_only") != 0) {
    taos_close(taos);
    taos_cleanup();
    return 1;
  }

  taos_close(taos);
  taos_cleanup();
  return 0;
}