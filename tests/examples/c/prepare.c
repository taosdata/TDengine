// TAOS standard API example. The same syntax as MySQL, but only a subet
// to compile: gcc -o prepare prepare.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <unistd.h>

void taosMsleep(int mseconds);

void verify_prepare(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);

  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }

  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql =
      "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin "
      "binary(40), blob nchar(10), u1 tinyint unsigned, u2 smallint unsigned, u4 int unsigned, u8 bigint unsigned)";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  // insert 10 records
  struct {
    int64_t  ts;
    int8_t   b;
    int8_t   v1;
    int16_t  v2;
    int32_t  v4;
    int64_t  v8;
    float    f4;
    double   f8;
    char     bin[40];
    char     blob[80];
    uint8_t  u1;
    uint16_t u2;
    uint32_t u4;
    uint64_t u8;
  } v = {0};

  TAOS_STMT* stmt = taos_stmt_init(taos);
  TAOS_BIND  params[14];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  strcpy(v.blob, "一二三四五六七八九十");
  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = strlen(v.blob);
  params[9].buffer = v.blob;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  params[10].buffer_type = TSDB_DATA_TYPE_UTINYINT;
  params[10].buffer_length = sizeof(v.u1);
  params[10].buffer = &v.u1;
  params[10].length = &params[10].buffer_length;
  params[10].is_null = NULL;

  params[11].buffer_type = TSDB_DATA_TYPE_USMALLINT;
  params[11].buffer_length = sizeof(v.u2);
  params[11].buffer = &v.u2;
  params[11].length = &params[11].buffer_length;
  params[11].is_null = NULL;

  params[12].buffer_type = TSDB_DATA_TYPE_UINT;
  params[12].buffer_length = sizeof(v.u4);
  params[12].buffer = &v.u4;
  params[12].length = &params[12].buffer_length;
  params[12].is_null = NULL;

  params[13].buffer_type = TSDB_DATA_TYPE_UBIGINT;
  params[13].buffer_length = sizeof(v.u8);
  params[13].buffer = &v.u8;
  params[13].length = &params[13].buffer_length;
  params[13].is_null = NULL;

  int is_null = 1;

  sql = "insert into m1 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
  v.ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts += 1;
    for (int j = 1; j < 10; ++j) {
      params[j].is_null = ((i == j) ? &is_null : 0);
    }
    v.b = (int8_t)i % 2;
    v.v1 = (int8_t)i;
    v.v2 = (int16_t)(i * 2);
    v.v4 = (int32_t)(i * 4);
    v.v8 = (int64_t)(i * 8);
    v.f4 = (float)(i * 40);
    v.f8 = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin); ++j) {
      v.bin[j] = (char)(i + '0');
    }
    v.u1 = (uint8_t)i;
    v.u2 = (uint16_t)(i * 2);
    v.u4 = (uint32_t)(i * 4);
    v.u8 = (uint64_t)(i * 8);

    taos_stmt_bind_param(stmt, params);
    taos_stmt_add_batch(stmt);
  }
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);
  v.v1 = 5;
  v.v2 = 15;
  taos_stmt_bind_param(stmt, params + 2);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD* fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);
}

void verify_prepare2(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql =
      "create table m1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin "
      "binary(40), blob nchar(10))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  // insert 10 records
  struct {
    int64_t ts[10];
    int8_t  b[10];
    int8_t  v1[10];
    int16_t v2[10];
    int32_t v4[10];
    int64_t v8[10];
    float   f4[10];
    double  f8[10];
    char    bin[10][40];
    char    blob[10][80];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT*      stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[10];
  char            is_null[10] = {0};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b[0]);
  params[1].buffer = v.b;
  params[1].length = t8_len;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1[0]);
  params[2].buffer = v.v1;
  params[2].length = t8_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2[0]);
  params[3].buffer = v.v2;
  params[3].length = t16_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4[0]);
  params[4].buffer = v.v4;
  params[4].length = t32_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8[0]);
  params[5].buffer = v.v8;
  params[5].length = t64_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4[0]);
  params[6].buffer = v.f4;
  params[6].length = float_len;
  params[6].is_null = is_null;
  params[6].num = 10;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8[0]);
  params[7].buffer = v.f8;
  params[7].length = double_len;
  params[7].is_null = is_null;
  params[7].num = 10;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin[0]);
  params[8].buffer = v.bin;
  params[8].length = bin_len;
  params[8].is_null = is_null;
  params[8].num = 10;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = sizeof(v.blob[0]);
  params[9].buffer = v.blob;
  params[9].length = blob_len;
  params[9].is_null = is_null;
  params[9].num = 10;

  sql = "insert into ? (ts, b, v1, v2, v4, v8, f4, f8, bin, blob) values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  code = taos_stmt_set_tbname(stmt, "m1");
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = ts++;
    is_null[i] = 0;

    v.b[i] = (int8_t)i % 2;
    v.v1[i] = (int8_t)i;
    v.v2[i] = (int16_t)(i * 2);
    v.v4[i] = (int32_t)(i * 4);
    v.v8[i] = (int64_t)(i * 8);
    v.f4[i] = (float)(i * 40);
    v.f8[i] = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin[0]); ++j) {
      v.bin[i][j] = (char)(i + '0');
    }
    strcpy(v.blob[i], "一二三四五六七八九十");

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = (int32_t)strlen(v.blob[i]);
  }

  taos_stmt_bind_param_batch(stmt, params);
  taos_stmt_add_batch(stmt);

  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);
  TAOS_BIND qparams[2];

  int8_t  v1 = 5;
  int16_t v2 = 15;
  qparams[0].buffer_type = TSDB_DATA_TYPE_TINYINT;
  qparams[0].buffer_length = sizeof(v1);
  qparams[0].buffer = &v1;
  qparams[0].length = &qparams[0].buffer_length;
  qparams[0].is_null = NULL;

  qparams[1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  qparams[1].buffer_length = sizeof(v2);
  qparams[1].buffer = &v2;
  qparams[1].length = &qparams[1].buffer_length;
  qparams[1].is_null = NULL;

  taos_stmt_bind_param(stmt, qparams);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD* fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);

  free(t8_len);
  free(t16_len);
  free(t32_len);
  free(t64_len);
  free(float_len);
  free(double_len);
  free(bin_len);
  free(blob_len);
}

void verify_prepare3(TAOS* taos) {
  TAOS_RES* result = taos_query(taos, "drop database if exists test;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database test;");

  int code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create database, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  usleep(100000);
  taos_select_db(taos, "test");

  // create table
  const char* sql =
      "create stable st1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin "
      "binary(40), blob nchar(10)) tags (id1 int, id2 binary(40))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  TAOS_BIND tags[2];

  int32_t   id1 = 1;
  char      id2[40] = "abcdefghijklmnopqrstuvwxyz0123456789";
  uintptr_t id2_len = strlen(id2);

  tags[0].buffer_type = TSDB_DATA_TYPE_INT;
  tags[0].buffer_length = sizeof(int);
  tags[0].buffer = &id1;
  tags[0].length = NULL;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[1].buffer_length = sizeof(id2);
  tags[1].buffer = id2;
  tags[1].length = &id2_len;
  tags[1].is_null = NULL;

  // insert 10 records
  struct {
    int64_t ts[10];
    int8_t  b[10];
    int8_t  v1[10];
    int16_t v2[10];
    int32_t v4[10];
    int64_t v8[10];
    float   f4[10];
    double  f8[10];
    char    bin[10][40];
    char    blob[10][80];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT*      stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[10];
  char            is_null[10] = {0};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b[0]);
  params[1].buffer = v.b;
  params[1].length = t8_len;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1[0]);
  params[2].buffer = v.v1;
  params[2].length = t8_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2[0]);
  params[3].buffer = v.v2;
  params[3].length = t16_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4[0]);
  params[4].buffer = v.v4;
  params[4].length = t32_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8[0]);
  params[5].buffer = v.v8;
  params[5].length = t64_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4[0]);
  params[6].buffer = v.f4;
  params[6].length = float_len;
  params[6].is_null = is_null;
  params[6].num = 10;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8[0]);
  params[7].buffer = v.f8;
  params[7].length = double_len;
  params[7].is_null = is_null;
  params[7].num = 10;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin[0]);
  params[8].buffer = v.bin;
  params[8].length = bin_len;
  params[8].is_null = is_null;
  params[8].num = 10;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = sizeof(v.blob[0]);
  params[9].buffer = v.blob;
  params[9].length = blob_len;
  params[9].is_null = is_null;
  params[9].num = 10;

  sql = "insert into ? using st1 tags(?,?) values(?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  code = taos_stmt_set_tbname_tags(stmt, "m1", tags);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = ts++;
    is_null[i] = 0;

    v.b[i] = (int8_t)i % 2;
    v.v1[i] = (int8_t)i;
    v.v2[i] = (int16_t)(i * 2);
    v.v4[i] = (int32_t)(i * 4);
    v.v8[i] = (int64_t)(i * 8);
    v.f4[i] = (float)(i * 40);
    v.f8[i] = (double)(i * 80);
    for (int j = 0; j < sizeof(v.bin[0]); ++j) {
      v.bin[i][j] = (char)(i + '0');
    }
    strcpy(v.blob[i], "一二三四五六七八九十");

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = (int32_t)strlen(v.blob[i]);
  }

  taos_stmt_bind_param_batch(stmt, params);
  taos_stmt_add_batch(stmt);

  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute insert statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT * FROM m1 WHERE v1 > ? AND v2 < ?", 0);

  TAOS_BIND qparams[2];

  int8_t  v1 = 5;
  int16_t v2 = 15;
  qparams[0].buffer_type = TSDB_DATA_TYPE_TINYINT;
  qparams[0].buffer_length = sizeof(v1);
  qparams[0].buffer = &v1;
  qparams[0].length = &qparams[0].buffer_length;
  qparams[0].is_null = NULL;

  qparams[1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  qparams[1].buffer_length = sizeof(v2);
  qparams[1].buffer = &v2;
  qparams[1].length = &qparams[1].buffer_length;
  qparams[1].is_null = NULL;

  taos_stmt_bind_param(stmt, qparams);
  if (taos_stmt_execute(stmt) != 0) {
    printf("\033[31mfailed to execute select statement.error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD* fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  taos_stmt_close(stmt);

  free(t8_len);
  free(t16_len);
  free(t32_len);
  free(t64_len);
  free(float_len);
  free(double_len);
  free(bin_len);
  free(blob_len);
}

int main(int argc, char* argv[]) {
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);
  printf("************  verify prepare  *************\n");
  verify_prepare(taos);
  printf("************  verify prepare2  *************\n");
  verify_prepare2(taos);
  printf("************  verify prepare3  *************\n");
  verify_prepare3(taos);
  printf("************  verify prepare4  *************\n");
  exit(EXIT_SUCCESS);
}
