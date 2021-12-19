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

  sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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

  int is_null = 1;

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
      "binary(40), blob nchar(10), u1 tinyint unsigned, u2 smallint unsigned, u4 int unsigned, u8 bigint unsigned) "
      "tags "
      "(b_tag bool, v1_tag tinyint, v2_tag smallint, v4_tag int, v8_tag bigint, f4_tag float, f8_tag double, bin_tag "
      "binary(40), blob_tag nchar(10), u1_tag tinyint unsigned, u2_tag smallint unsigned, u4_tag int unsigned, u8_tag "
      "bigint "
      "unsigned)";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  TAOS_BIND tags[13];

  struct {
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
  } id = {0};

  id.b = (int8_t)1;
  id.v1 = (int8_t)1;
  id.v2 = (int16_t)2;
  id.v4 = (int32_t)4;
  id.v8 = (int64_t)8;
  id.f4 = (float)40;
  id.f8 = (double)80;
  for (int j = 0; j < sizeof(id.bin); ++j) {
    id.bin[j] = (char)('1' + '0');
  }
  strcpy(id.blob, "一二三四五六七八九十");
  id.u1 = (uint8_t)1;
  id.u2 = (uint16_t)2;
  id.u4 = (uint32_t)4;
  id.u8 = (uint64_t)8;

  tags[0].buffer_type = TSDB_DATA_TYPE_BOOL;
  tags[0].buffer_length = sizeof(id.b);
  tags[0].buffer = &id.b;
  tags[0].length = &tags[0].buffer_length;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_TINYINT;
  tags[1].buffer_length = sizeof(id.v1);
  tags[1].buffer = &id.v1;
  tags[1].length = &tags[1].buffer_length;
  tags[1].is_null = NULL;

  tags[2].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  tags[2].buffer_length = sizeof(id.v2);
  tags[2].buffer = &id.v2;
  tags[2].length = &tags[2].buffer_length;
  tags[2].is_null = NULL;

  tags[3].buffer_type = TSDB_DATA_TYPE_INT;
  tags[3].buffer_length = sizeof(id.v4);
  tags[3].buffer = &id.v4;
  tags[3].length = &tags[3].buffer_length;
  tags[3].is_null = NULL;

  tags[4].buffer_type = TSDB_DATA_TYPE_BIGINT;
  tags[4].buffer_length = sizeof(id.v8);
  tags[4].buffer = &id.v8;
  tags[4].length = &tags[4].buffer_length;
  tags[4].is_null = NULL;

  tags[5].buffer_type = TSDB_DATA_TYPE_FLOAT;
  tags[5].buffer_length = sizeof(id.f4);
  tags[5].buffer = &id.f4;
  tags[5].length = &tags[5].buffer_length;
  tags[5].is_null = NULL;

  tags[6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  tags[6].buffer_length = sizeof(id.f8);
  tags[6].buffer = &id.f8;
  tags[6].length = &tags[6].buffer_length;
  tags[6].is_null = NULL;

  tags[7].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[7].buffer_length = sizeof(id.bin);
  tags[7].buffer = &id.bin;
  tags[7].length = &tags[7].buffer_length;
  tags[7].is_null = NULL;

  tags[8].buffer_type = TSDB_DATA_TYPE_NCHAR;
  tags[8].buffer_length = strlen(id.blob);
  tags[8].buffer = &id.blob;
  tags[8].length = &tags[8].buffer_length;
  tags[8].is_null = NULL;

  tags[9].buffer_type = TSDB_DATA_TYPE_UTINYINT;
  tags[9].buffer_length = sizeof(id.u1);
  tags[9].buffer = &id.u1;
  tags[9].length = &tags[9].buffer_length;
  tags[9].is_null = NULL;

  tags[10].buffer_type = TSDB_DATA_TYPE_USMALLINT;
  tags[10].buffer_length = sizeof(id.u2);
  tags[10].buffer = &id.u2;
  tags[10].length = &tags[10].buffer_length;
  tags[10].is_null = NULL;

  tags[11].buffer_type = TSDB_DATA_TYPE_UINT;
  tags[11].buffer_length = sizeof(id.u4);
  tags[11].buffer = &id.u4;
  tags[11].length = &tags[11].buffer_length;
  tags[11].is_null = NULL;

  tags[12].buffer_type = TSDB_DATA_TYPE_UBIGINT;
  tags[12].buffer_length = sizeof(id.u8);
  tags[12].buffer = &id.u8;
  tags[12].length = &tags[12].buffer_length;
  tags[12].is_null = NULL;
  // insert 10 records
  struct {
    int64_t  ts[10];
    int8_t   b[10];
    int8_t   v1[10];
    int16_t  v2[10];
    int32_t  v4[10];
    int64_t  v8[10];
    float    f4[10];
    double   f8[10];
    char     bin[10][40];
    char     blob[10][80];
    uint8_t  u1[10];
    uint16_t u2[10];
    uint32_t u4[10];
    uint64_t u8[10];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);
  int32_t* u8_len = malloc(sizeof(int32_t) * 10);
  int32_t* u16_len = malloc(sizeof(int32_t) * 10);
  int32_t* u32_len = malloc(sizeof(int32_t) * 10);
  int32_t* u64_len = malloc(sizeof(int32_t) * 10);

  TAOS_STMT*      stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[14];
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

  params[10].buffer_type = TSDB_DATA_TYPE_UTINYINT;
  params[10].buffer_length = sizeof(v.u1[0]);
  params[10].buffer = v.u1;
  params[10].length = u8_len;
  params[10].is_null = is_null;
  params[10].num = 10;

  params[11].buffer_type = TSDB_DATA_TYPE_USMALLINT;
  params[11].buffer_length = sizeof(v.u2[0]);
  params[11].buffer = v.u2;
  params[11].length = u16_len;
  params[11].is_null = is_null;
  params[11].num = 10;

  params[12].buffer_type = TSDB_DATA_TYPE_UINT;
  params[12].buffer_length = sizeof(v.u4[0]);
  params[12].buffer = v.u4;
  params[12].length = u32_len;
  params[12].is_null = is_null;
  params[12].num = 10;

  params[13].buffer_type = TSDB_DATA_TYPE_UBIGINT;
  params[13].buffer_length = sizeof(v.u8[0]);
  params[13].buffer = v.u8;
  params[13].length = u64_len;
  params[13].is_null = is_null;
  params[13].num = 10;

  sql = "insert into ? using st1 tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }

  code = taos_stmt_set_tbname_tags(stmt, "m1", tags);
  if (code != 0) {
    printf("\033[31mfailed to execute taos_stmt_set_tbname_tags. error:%s\033[0m\n", taos_stmt_errstr(stmt));
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
    v.u1[i] = (uint8_t)i;
    v.u2[i] = (uint16_t)(i * 2);
    v.u4[i] = (uint32_t)(i * 4);
    v.u8[i] = (uint64_t)(i * 8);

    t8_len[i] = sizeof(int8_t);
    t16_len[i] = sizeof(int16_t);
    t32_len[i] = sizeof(int32_t);
    t64_len[i] = sizeof(int64_t);
    float_len[i] = sizeof(float);
    double_len[i] = sizeof(double);
    bin_len[i] = sizeof(v.bin[0]);
    blob_len[i] = (int32_t)strlen(v.blob[i]);
    u8_len[i] = sizeof(uint8_t);
    u16_len[i] = sizeof(uint16_t);
    u32_len[i] = sizeof(uint32_t);
    u64_len[i] = sizeof(uint64_t);
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

/**
 * @brief Verify the upper/lower case of tableName for create(by setTableName)/query/show/describe/drop.
 *  https://jira.taosdata.com:18080/browse/TS-904
 *  https://jira.taosdata.com:18090/pages/viewpage.action?pageId=129140555
 * @param taos
 */
void verify_prepare4(TAOS* taos) {
  printf("Verify the upper/lower case of tableName for create(by setTableName)/query/show/describe/drop etc.\n");

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
      "binary(40), blob nchar(10), u1 tinyint unsigned, u2 smallint unsigned, u4 int unsigned, u8 bigint unsigned) "
      "tags "
      "(b_tag bool, v1_tag tinyint, v2_tag smallint, v4_tag int, v8_tag bigint, f4_tag float, f8_tag double, bin_tag "
      "binary(40), blob_tag nchar(10), u1_tag tinyint unsigned, u2_tag smallint unsigned, u4_tag int unsigned, u8_tag "
      "bigint "
      "unsigned)";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("\033[31mfailed to create table, reason:%s\033[0m\n", taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);

  TAOS_BIND tags[13];

  struct {
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
  } id = {0};

  id.b = (int8_t)1;
  id.v1 = (int8_t)1;
  id.v2 = (int16_t)2;
  id.v4 = (int32_t)4;
  id.v8 = (int64_t)8;
  id.f4 = (float)40;
  id.f8 = (double)80;
  for (int j = 0; j < sizeof(id.bin); ++j) {
    id.bin[j] = (char)('1' + '0');
  }
  strcpy(id.blob, "一二三四五六七八九十");
  id.u1 = (uint8_t)1;
  id.u2 = (uint16_t)2;
  id.u4 = (uint32_t)4;
  id.u8 = (uint64_t)8;

  tags[0].buffer_type = TSDB_DATA_TYPE_BOOL;
  tags[0].buffer_length = sizeof(id.b);
  tags[0].buffer = &id.b;
  tags[0].length = &tags[0].buffer_length;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_TINYINT;
  tags[1].buffer_length = sizeof(id.v1);
  tags[1].buffer = &id.v1;
  tags[1].length = &tags[1].buffer_length;
  tags[1].is_null = NULL;

  tags[2].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  tags[2].buffer_length = sizeof(id.v2);
  tags[2].buffer = &id.v2;
  tags[2].length = &tags[2].buffer_length;
  tags[2].is_null = NULL;

  tags[3].buffer_type = TSDB_DATA_TYPE_INT;
  tags[3].buffer_length = sizeof(id.v4);
  tags[3].buffer = &id.v4;
  tags[3].length = &tags[3].buffer_length;
  tags[3].is_null = NULL;

  tags[4].buffer_type = TSDB_DATA_TYPE_BIGINT;
  tags[4].buffer_length = sizeof(id.v8);
  tags[4].buffer = &id.v8;
  tags[4].length = &tags[4].buffer_length;
  tags[4].is_null = NULL;

  tags[5].buffer_type = TSDB_DATA_TYPE_FLOAT;
  tags[5].buffer_length = sizeof(id.f4);
  tags[5].buffer = &id.f4;
  tags[5].length = &tags[5].buffer_length;
  tags[5].is_null = NULL;

  tags[6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  tags[6].buffer_length = sizeof(id.f8);
  tags[6].buffer = &id.f8;
  tags[6].length = &tags[6].buffer_length;
  tags[6].is_null = NULL;

  tags[7].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[7].buffer_length = sizeof(id.bin);
  tags[7].buffer = &id.bin;
  tags[7].length = &tags[7].buffer_length;
  tags[7].is_null = NULL;

  tags[8].buffer_type = TSDB_DATA_TYPE_NCHAR;
  tags[8].buffer_length = strlen(id.blob);
  tags[8].buffer = &id.blob;
  tags[8].length = &tags[8].buffer_length;
  tags[8].is_null = NULL;

  tags[9].buffer_type = TSDB_DATA_TYPE_UTINYINT;
  tags[9].buffer_length = sizeof(id.u1);
  tags[9].buffer = &id.u1;
  tags[9].length = &tags[9].buffer_length;
  tags[9].is_null = NULL;

  tags[10].buffer_type = TSDB_DATA_TYPE_USMALLINT;
  tags[10].buffer_length = sizeof(id.u2);
  tags[10].buffer = &id.u2;
  tags[10].length = &tags[10].buffer_length;
  tags[10].is_null = NULL;

  tags[11].buffer_type = TSDB_DATA_TYPE_UINT;
  tags[11].buffer_length = sizeof(id.u4);
  tags[11].buffer = &id.u4;
  tags[11].length = &tags[11].buffer_length;
  tags[11].is_null = NULL;

  tags[12].buffer_type = TSDB_DATA_TYPE_UBIGINT;
  tags[12].buffer_length = sizeof(id.u8);
  tags[12].buffer = &id.u8;
  tags[12].length = &tags[12].buffer_length;
  tags[12].is_null = NULL;
  // insert 10 records
  struct {
    int64_t  ts[10];
    int8_t   b[10];
    int8_t   v1[10];
    int16_t  v2[10];
    int32_t  v4[10];
    int64_t  v8[10];
    float    f4[10];
    double   f8[10];
    char     bin[10][40];
    char     blob[10][80];
    uint8_t  u1[10];
    uint16_t u2[10];
    uint32_t u4[10];
    uint64_t u8[10];
  } v;

  int32_t* t8_len = malloc(sizeof(int32_t) * 10);
  int32_t* t16_len = malloc(sizeof(int32_t) * 10);
  int32_t* t32_len = malloc(sizeof(int32_t) * 10);
  int32_t* t64_len = malloc(sizeof(int32_t) * 10);
  int32_t* float_len = malloc(sizeof(int32_t) * 10);
  int32_t* double_len = malloc(sizeof(int32_t) * 10);
  int32_t* bin_len = malloc(sizeof(int32_t) * 10);
  int32_t* blob_len = malloc(sizeof(int32_t) * 10);
  int32_t* u8_len = malloc(sizeof(int32_t) * 10);
  int32_t* u16_len = malloc(sizeof(int32_t) * 10);
  int32_t* u32_len = malloc(sizeof(int32_t) * 10);
  int32_t* u64_len = malloc(sizeof(int32_t) * 10);

  TAOS_MULTI_BIND params[14];
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

  params[10].buffer_type = TSDB_DATA_TYPE_UTINYINT;
  params[10].buffer_length = sizeof(v.u1[0]);
  params[10].buffer = v.u1;
  params[10].length = u8_len;
  params[10].is_null = is_null;
  params[10].num = 10;

  params[11].buffer_type = TSDB_DATA_TYPE_USMALLINT;
  params[11].buffer_length = sizeof(v.u2[0]);
  params[11].buffer = v.u2;
  params[11].length = u16_len;
  params[11].is_null = is_null;
  params[11].num = 10;

  params[12].buffer_type = TSDB_DATA_TYPE_UINT;
  params[12].buffer_length = sizeof(v.u4[0]);
  params[12].buffer = v.u4;
  params[12].length = u32_len;
  params[12].is_null = is_null;
  params[12].num = 10;

  params[13].buffer_type = TSDB_DATA_TYPE_UBIGINT;
  params[13].buffer_length = sizeof(v.u8[0]);
  params[13].buffer = v.u8;
  params[13].length = u64_len;
  params[13].is_null = is_null;
  params[13].num = 10;

// verify table names for upper/lower case
#define VERIFY_CNT 5

  typedef struct {
    char setTbName[20];
    char showName[20];
    char describeName[20];
    char queryName[20];
    char dropName[20];
  } STbNames;

  /**
   * @brief
   *  0       - success expected
   *  NonZero - fail expected
   */
  typedef struct {
    int32_t setTbName;
    int32_t showName;
    int32_t describeName;
    int32_t queryName;
    int32_t dropName;
  } STbNamesResult;

  STbNames       tbName[VERIFY_CNT] = {0};
  STbNamesResult tbNameResult[VERIFY_CNT] = {0};

  STbNames*       pTbName = NULL;
  STbNamesResult* pTbNameResult = NULL;

  pTbName = &tbName[0];
  pTbNameResult = &tbNameResult[0];
  strcpy(pTbName->setTbName, "Mn1");
  strcpy(pTbName->showName, "mn1");
  strcpy(pTbName->describeName, "mn1");
  strcpy(pTbName->queryName, "mn1");
  strcpy(pTbName->dropName, "mn1");

  pTbName = &tbName[1];
  pTbNameResult = &tbNameResult[1];
  strcpy(pTbName->setTbName, "'Mn1'");
  strcpy(pTbName->showName, "'mn1'");
  strcpy(pTbName->describeName, "'mn1'");
  strcpy(pTbName->queryName, "'mn1'");
  strcpy(pTbName->dropName, "'mn1'");

  pTbName = &tbName[2];
  pTbNameResult = &tbNameResult[2];
  strcpy(pTbName->setTbName, "\"Mn1\"");
  strcpy(pTbName->showName, "\"mn1\"");
  strcpy(pTbName->describeName, "\"mn1\"");
  strcpy(pTbName->queryName, "\"mn1\"");
  strcpy(pTbName->dropName, "\"mn1\"");

  pTbName = &tbName[3];
  pTbNameResult = &tbNameResult[3];
  strcpy(pTbName->setTbName, "\"Mn1\"");
  strcpy(pTbName->showName, "'mn1'");
  strcpy(pTbName->describeName, "'mn1'");
  strcpy(pTbName->queryName, "mn1");
  strcpy(pTbName->dropName, "\"mn1\"");

  pTbName = &tbName[4];
  pTbNameResult = &tbNameResult[4];
  strcpy(pTbName->setTbName, "`Mn1`");
  strcpy(pTbName->showName, "`Mn1`");
  strcpy(pTbName->describeName, "`Mn1`");
  strcpy(pTbName->queryName, "`Mn1`");
  strcpy(pTbName->dropName, "`Mn1`");
  pTbNameResult->setTbName = -1;
  pTbNameResult->showName = -1;
  pTbNameResult->describeName = -1;
  pTbNameResult->queryName = -1;
  pTbNameResult->dropName = -1;

  TAOS_STMT* stmt = NULL;

  for (int n = 0; n < VERIFY_CNT; ++n) {
    printf("\033[31m[%d] ===================================\033[0m\n", n);
    pTbName = &tbName[n];
    pTbNameResult = &tbNameResult[n];
    char tmpStr[256] = {0};

    // set table name
    stmt = taos_stmt_init(taos);
    if (!stmt) {
      printf("\033[31m[%d] failed to execute taos_stmt_init. error:%s\033[0m\n", n);
      exit(EXIT_FAILURE);
    }

    sql = "insert into ? using st1 tags(?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    code = taos_stmt_prepare(stmt, sql, 0);
    if (code != 0) {
      printf("\033[31mfailed to execute taos_stmt_prepare. error:%s\033[0m\n", taos_stmt_errstr(stmt));
      taos_stmt_close(stmt);
      exit(EXIT_FAILURE);
    }

    printf("[%d] taos_stmt_set_tbname_tags, tbname=%s\n", n, pTbName->setTbName);
    code = taos_stmt_set_tbname_tags(stmt, pTbName->setTbName, tags);
    if ((!pTbNameResult->setTbName && (0 != code)) || (pTbNameResult->setTbName && (0 == code))) {
      printf("\033[31m[%d] failed to execute taos_stmt_set_tbname_tags. error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
      taos_stmt_close(stmt);
      exit(EXIT_FAILURE);
    }

    if (code == 0) {
      int64_t ts = 1591060628000 + 1000 * n;
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
        v.u1[i] = (uint8_t)i;
        v.u2[i] = (uint16_t)(i * 2);
        v.u4[i] = (uint32_t)(i * 4);
        v.u8[i] = (uint64_t)(i * 8);

        t8_len[i] = sizeof(int8_t);
        t16_len[i] = sizeof(int16_t);
        t32_len[i] = sizeof(int32_t);
        t64_len[i] = sizeof(int64_t);
        float_len[i] = sizeof(float);
        double_len[i] = sizeof(double);
        bin_len[i] = sizeof(v.bin[0]);
        blob_len[i] = (int32_t)strlen(v.blob[i]);
        u8_len[i] = sizeof(uint8_t);
        u16_len[i] = sizeof(uint16_t);
        u32_len[i] = sizeof(uint32_t);
        u64_len[i] = sizeof(uint64_t);
      }

      taos_stmt_bind_param_batch(stmt, params);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("\033[31m[%d] failed to execute insert statement.error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
        taos_stmt_close(stmt);
        exit(EXIT_FAILURE);
      }
    }
    taos_stmt_close(stmt);

    // show the table
    printf("[%d] show tables, tbName = %s\n", n, pTbName->showName);
    stmt = taos_stmt_init(taos);
    sprintf(tmpStr, "show tables like %s", pTbName->showName);
    taos_stmt_prepare(stmt, tmpStr, 0);
    code = taos_stmt_execute(stmt);
    if ((!pTbNameResult->showName && (0 != code)) || (pTbNameResult->showName && (0 == code))) {
      printf("\033[31m[%d] failed to execute show tables like. error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
      taos_stmt_close(stmt);
      exit(EXIT_FAILURE);
    }
    taos_stmt_close(stmt);

    // describe the table
    printf("[%d] describe tables, tbName = %s\n", n, pTbName->describeName);
    stmt = taos_stmt_init(taos);
    sprintf(tmpStr, "describe %s", pTbName->describeName);
    taos_stmt_prepare(stmt, tmpStr, 0);
    code = taos_stmt_execute(stmt);
    if ((!pTbNameResult->describeName && (0 != code)) || (pTbNameResult->describeName && (0 == code))) {
      printf("\033[31m[%d] failed to execute describe tables. error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
      taos_stmt_close(stmt);
      exit(EXIT_FAILURE);
    }
    taos_stmt_close(stmt);

    // query the records
    printf("[%d] select statement, tbName = %s\n", n, pTbName->queryName);
    stmt = taos_stmt_init(taos);
    sprintf(tmpStr, "SELECT * FROM %s", pTbName->queryName);
    taos_stmt_prepare(stmt, tmpStr, 0);

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

    code = taos_stmt_execute(stmt);
    if ((!pTbNameResult->queryName && (0 != code)) || (pTbNameResult->queryName && (0 == code))) {
      printf("\033[31m[%d] failed to execute select statement.error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
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
      printf("[%d] row = %s\n", n, temp);
    }

    taos_free_result(result);
    taos_stmt_close(stmt);

    // drop table
    printf("[%d] drop table, tbName = %s\n", n, pTbName->dropName);
    stmt = taos_stmt_init(taos);
    sprintf(tmpStr, "drop table %s", pTbName->dropName);
    taos_stmt_prepare(stmt, tmpStr, 0);
    code = taos_stmt_execute(stmt);
    if ((!pTbNameResult->dropName && (0 != code)) || (pTbNameResult->dropName && (0 == code))) {
      printf("\033[31m[%d] failed to drop table. error:%s\033[0m\n", n, taos_stmt_errstr(stmt));
      taos_stmt_close(stmt);
      exit(EXIT_FAILURE);
    }
    taos_stmt_close(stmt);
  }

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
  verify_prepare4(taos);
  printf("************  verify end  *************\n");
  exit(EXIT_SUCCESS);
}
