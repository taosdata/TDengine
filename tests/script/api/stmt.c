#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include "taos.h"

void execute_simple_sql(void *taos, char *sql) {
  TAOS_RES *result = taos_query(taos, sql);
  if (result == NULL || taos_errno(result) != 0) {
    printf("failed to %s, Reason: %s\n", sql, taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);
}

void print_result(TAOS_RES *res) {
  if (res == NULL) {
    exit(EXIT_FAILURE);
  }
  TAOS_ROW    row = NULL;
  int         num_fields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  while ((row = taos_fetch_row(res))) {
    char temp[256] = {0};
    taos_print_row(temp, row, fields, num_fields);
    printf("get result: %s\n", temp);
  }
}

void taos_stmt_init_test() {
  printf("start taos_stmt_init test \n");
  void *     taos = NULL;
  TAOS_STMT *stmt = NULL;
  stmt = taos_stmt_init(taos);
  assert(stmt == NULL);
  // ASM ERROR
  assert(taos_stmt_close(stmt) != 0);
  taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  assert(taos_stmt_close(stmt) == 0);
  printf("finish taos_stmt_init test\n");
}
void taos_stmt_preprare_test() {
  printf("start taos_stmt_prepare test\n");
  char *     stmt_sql = calloc(1, 1048576);
  TAOS_STMT *stmt = NULL;
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) != 0);
  void *taos = NULL;
  taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos,
                     "create table super(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 "
                     "smallint, c7 tinyint, c8 bool, c9 nchar(8), c10 timestamp) tags (t1 int, t2 bigint, t3 float, t4 "
                     "double, t5 binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8))");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  // below will make client dead lock
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);

  assert(taos_stmt_close(stmt) == 0);
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  sprintf(stmt_sql, "select from ?");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_close(stmt) == 0);

  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  sprintf(stmt_sql, "insert into ? values (?,?,?,?,?,?,?,?,?,?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_close(stmt) == 0);

  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  sprintf(stmt_sql, "insert into super values (?,?,?,?,?,?,?,?,?,?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) != 0);
  assert(taos_stmt_close(stmt) == 0);

  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  sprintf(stmt_sql, "insert into ? values (?,?,?,?,?,?,?,?,1,?,?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_close(stmt) == 0);

  free(stmt_sql);
  printf("finish taos_stmt_prepare test\n");
}

void taos_stmt_set_tbname_test() {
  printf("start taos_stmt_set_tbname test\n");
  TAOS_STMT *stmt = NULL;
  char *     name = calloc(1, 200);
  // ASM ERROR
  assert(taos_stmt_set_tbname(stmt, name) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create table super(ts timestamp, c1 int)");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  assert(taos_stmt_set_tbname(stmt, name) != 0);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  sprintf(name, "super");
  assert(stmt != NULL);
  assert(taos_stmt_set_tbname(stmt, name) == 0);
  free(name);
  free(stmt_sql);
  taos_stmt_close(stmt);
  printf("finish taos_stmt_set_tbname test\n");
}

void taos_stmt_set_tbname_tags_test() {
  printf("start taos_stmt_set_tbname_tags test\n");
  TAOS_STMT *stmt = NULL;
  char *     name = calloc(1, 20);
  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND));
  // ASM ERROR
  assert(taos_stmt_set_tbname_tags(stmt, name, tags) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create stable super(ts timestamp, c1 int) tags (id int)");
  execute_simple_sql(taos, "create table tb using super tags (1)");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? using super tags (?) values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_set_tbname_tags(stmt, name, tags) != 0);
  sprintf(name, "tb");
  assert(taos_stmt_set_tbname_tags(stmt, name, tags) != 0);
  int t = 1;
  tags->buffer_length = TSDB_DATA_TYPE_INT;
  tags->buffer_length = sizeof(uint32_t);
  tags->buffer = &t;
  tags->length = &tags->buffer_length;
  tags->is_null = NULL;
  assert(taos_stmt_set_tbname_tags(stmt, name, tags) == 0);
  free(stmt_sql);
  free(name);
  free(tags);
  taos_stmt_close(stmt);
  printf("finish taos_stmt_set_tbname_tags test\n");
}

void taos_stmt_set_sub_tbname_test() {
  printf("start taos_stmt_set_sub_tbname test\n");
  TAOS_STMT *stmt = NULL;
  char *     name = calloc(1, 200);
  // ASM ERROR
  assert(taos_stmt_set_sub_tbname(stmt, name) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create stable super(ts timestamp, c1 int) tags (id int)");
  execute_simple_sql(taos, "create table tb using super tags (1)");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_set_sub_tbname(stmt, name) != 0);
  sprintf(name, "tb");
  assert(taos_stmt_set_sub_tbname(stmt, name) == 0);
  assert(taos_load_table_info(taos, "super, tb") == 0);
  assert(taos_stmt_set_sub_tbname(stmt, name) == 0);
  free(name);
  free(stmt_sql);
  assert(taos_stmt_close(stmt) == 0);
  printf("finish taos_stmt_set_sub_tbname test\n");
}

void taos_stmt_bind_param_test() {
  printf("start taos_stmt_bind_param test\n");
  TAOS_STMT *stmt = NULL;
  TAOS_BIND *binds = NULL;
  assert(taos_stmt_bind_param(stmt, binds) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create table super(ts timestamp, c1 int)");
  stmt = taos_stmt_init(taos);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_bind_param(stmt, binds) != 0);
  free(binds);
  TAOS_BIND *params = calloc(2, sizeof(TAOS_BIND));
  int64_t    ts = (int64_t)1591060628000;
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(uint64_t);
  params[0].buffer = &ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;
  int32_t i = (int32_t)21474;
  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer_length = sizeof(int32_t);
  params[1].buffer = &i;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;
  assert(taos_stmt_bind_param(stmt, params) != 0);
  assert(taos_stmt_set_tbname(stmt, "super") == 0);
  assert(taos_stmt_bind_param(stmt, params) == 0);
  free(params);
  free(stmt_sql);
  taos_stmt_close(stmt);
  printf("finish taos_stmt_bind_param test\n");
}

void taos_stmt_bind_single_param_batch_test() {
  printf("start taos_stmt_bind_single_param_batch test\n");
  TAOS_STMT *      stmt = NULL;
  TAOS_MULTI_BIND *test_bind = NULL;
  assert(taos_stmt_bind_single_param_batch(stmt, test_bind, 0) != 0);
  printf("finish taos_stmt_bind_single_param_batch test\n");
}

void taos_stmt_bind_param_batch_test() {
  printf("start taos_stmt_bind_param_batch test\n");
  TAOS_STMT *      stmt = NULL;
  TAOS_MULTI_BIND *test_bind = NULL;
  assert(taos_stmt_bind_param_batch(stmt, test_bind) != 0);
  printf("finish taos_stmt_bind_param_batch test\n");
}

void taos_stmt_add_batch_test() {
  printf("start taos_stmt_add_batch test\n");
  TAOS_STMT *stmt = NULL;
  assert(taos_stmt_add_batch(stmt) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create table super(ts timestamp, c1 int)");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_add_batch(stmt) != 0);
  TAOS_BIND *params = calloc(2, sizeof(TAOS_BIND));
  int64_t    ts = (int64_t)1591060628000;
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(uint64_t);
  params[0].buffer = &ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;
  int32_t i = (int32_t)21474;
  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer_length = sizeof(int32_t);
  params[1].buffer = &i;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;
  assert(taos_stmt_set_tbname(stmt, "super") == 0);
  assert(taos_stmt_bind_param(stmt, params) == 0);
  assert(taos_stmt_add_batch(stmt) == 0);
  free(params);
  free(stmt_sql);
  assert(taos_stmt_close(stmt) == 0);
  printf("finish taos_stmt_add_batch test\n");
}

void taos_stmt_execute_test() {
  printf("start taos_stmt_execute test\n");
  TAOS_STMT *stmt = NULL;
  assert(taos_stmt_execute(stmt) != 0);
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(taos, "create table super(ts timestamp, c1 int)");
  stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  assert(taos_stmt_execute(stmt) != 0);
  char *stmt_sql = calloc(1, 1000);
  sprintf(stmt_sql, "insert into ? values (?,?)");
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  assert(taos_stmt_execute(stmt) != 0);
  TAOS_BIND *params = calloc(2, sizeof(TAOS_BIND));
  int64_t    ts = (int64_t)1591060628000;
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(uint64_t);
  params[0].buffer = &ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;
  int32_t i = (int32_t)21474;
  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer_length = sizeof(int32_t);
  params[1].buffer = &i;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;
  assert(taos_stmt_set_tbname(stmt, "super") == 0);
  assert(taos_stmt_execute(stmt) != 0);
  assert(taos_stmt_bind_param(stmt, params) == 0);
  assert(taos_stmt_execute(stmt) != 0);
  assert(taos_stmt_add_batch(stmt) == 0);
  assert(taos_stmt_execute(stmt) == 0);
  free(params);
  free(stmt_sql);
  assert(taos_stmt_close(stmt) == 0);
  printf("finish taos_stmt_execute test\n");
}

void taos_stmt_use_result_query(void *taos, char *col, int type) {
  TAOS_STMT *stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  char *stmt_sql = calloc(1, 1024);
  struct {
    int64_t  long_value;
    int64_t  ts_value;
    uint64_t ulong_value;
    int32_t  int_value;
    uint32_t uint_value;
    int16_t  small_value;
    uint16_t usmall_value;
    int8_t   tiny_value;
    uint8_t  utiny_value;
    float    float_value;
    double   double_value;
    char     binary_value[10];
    char     nchar_value[32];
  } v = {0};
  v.ts_value = (int64_t)1591060628000;
  v.long_value = (int64_t)1;
  v.int_value = (int32_t)1;
  v.small_value = (int16_t)1;
  v.tiny_value = (int8_t)1;
  v.ulong_value = (uint64_t)1;
  v.uint_value = (uint32_t)1;
  v.usmall_value = (uint16_t)1;
  v.utiny_value = (uint8_t)1;
  v.float_value = (float)1;
  v.double_value = (double)1;
  strcpy(v.binary_value, "abcdefgh");
  strcpy(v.nchar_value, "一二三四五六七八");
  uintptr_t nchar_value_len = strlen(v.nchar_value);
  sprintf(stmt_sql, "select * from stmt_test.t1 where %s = ?", col);
  printf("stmt_sql: %s\n", stmt_sql);
  assert(taos_stmt_prepare(stmt, stmt_sql, 0) == 0);
  TAOS_BIND *params = calloc(1, sizeof(TAOS_BIND));
  params->buffer_type = type;
  params->is_null = NULL;
  switch (type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      params->buffer_length = sizeof(v.ts_value);
      params->buffer = &v.ts_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_INT:
      params->buffer_length = sizeof(v.int_value);
      params->buffer = &v.int_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      params->buffer_length = sizeof(v.long_value);
      params->buffer = &v.long_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      params->buffer_length = sizeof(v.float_value);
      params->buffer = &v.float_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      params->buffer_length = sizeof(v.double_value);
      params->buffer = &v.double_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_BINARY:
      params->buffer_length = sizeof(v.binary_value);
      params->buffer = &v.binary_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      params->buffer_length = sizeof(v.small_value);
      params->buffer = &v.small_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      params->buffer_length = sizeof(v.tiny_value);
      params->buffer = &v.tiny_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_BOOL:
      params->buffer_length = sizeof(v.tiny_value);
      params->buffer = &v.tiny_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      params->buffer_length = sizeof(v.nchar_value);
      params->buffer = &v.nchar_value;
      params->length = &nchar_value_len;
      break;
    case TSDB_DATA_TYPE_UINT:
      params->buffer_length = sizeof(v.uint_value);
      params->buffer = &v.uint_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      params->buffer_length = sizeof(v.ulong_value);
      params->buffer = &v.ulong_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      params->buffer_length = sizeof(v.usmall_value);
      params->buffer = &v.usmall_value;
      params->length = &params->buffer_length;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      params->buffer_length = sizeof(v.utiny_value);
      params->buffer = &v.utiny_value;
      params->length = &params->buffer_length;
      break;
    default:
      printf("Cannnot find type: %d\n", type);
      break;
  }
  assert(taos_stmt_bind_param(stmt, params) == 0);
  assert(taos_stmt_execute(stmt) == 0);
  TAOS_RES *result = taos_stmt_use_result(stmt);
  assert(result != NULL);
  print_result(result);
  taos_free_result(result);
  assert(taos_stmt_close(stmt) == 0);
  free(params);
  free(stmt_sql);
}

void taos_stmt_use_result_test() {
  printf("start taos_stmt_use_result test\n");
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("Cannot connect to tdengine server\n");
    exit(EXIT_FAILURE);
  }
  execute_simple_sql(taos, "drop database if exists stmt_test");
  execute_simple_sql(taos, "create database stmt_test");
  execute_simple_sql(taos, "use stmt_test");
  execute_simple_sql(
      taos,
      "create table super(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 smallint, c7 tinyint, "
      "c8 bool, c9 nchar(8), c10 timestamp, c11 int unsigned, c12 bigint unsigned, c13 smallint unsigned, c14 tinyint "
      "unsigned) tags (t1 int, t2 bigint, t3 float, t4 double, t5 binary(8), t6 smallint, t7 tinyint, t8 bool, t9 "
      "nchar(8), t10 int unsigned, t11 bigint unsigned, t12 smallint unsigned, t13 tinyint unsigned)");
  execute_simple_sql(taos,
                     "create table t1 using super tags (1, 1, 1, 1, 'abcdefgh',1,1,1,'一二三四五六七八', 1, 1, 1, 1)");
  execute_simple_sql(
      taos, "insert into t1 values (1591060628000, 1, 1, 1, 1, 'abcdefgh',1,1,1,'一二三四五六七八', now, 1, 1, 1, 1)");
  execute_simple_sql(
      taos, "insert into t1 values (1591060628001, 1, 1, 1, 1, 'abcdefgh',1,1,1,'一二三四五六七八', now, 1, 1, 1, 1)");

  taos_stmt_use_result_query(taos, "ts", TSDB_DATA_TYPE_TIMESTAMP);
  taos_stmt_use_result_query(taos, "c1", TSDB_DATA_TYPE_INT);
  taos_stmt_use_result_query(taos, "c2", TSDB_DATA_TYPE_BIGINT);
  taos_stmt_use_result_query(taos, "c3", TSDB_DATA_TYPE_FLOAT);
  taos_stmt_use_result_query(taos, "c4", TSDB_DATA_TYPE_DOUBLE);
  taos_stmt_use_result_query(taos, "c5", TSDB_DATA_TYPE_BINARY);
  taos_stmt_use_result_query(taos, "c6", TSDB_DATA_TYPE_SMALLINT);
  taos_stmt_use_result_query(taos, "c7", TSDB_DATA_TYPE_TINYINT);
  taos_stmt_use_result_query(taos, "c8", TSDB_DATA_TYPE_BOOL);
  taos_stmt_use_result_query(taos, "c9", TSDB_DATA_TYPE_NCHAR);
  taos_stmt_use_result_query(taos, "c10", TSDB_DATA_TYPE_TIMESTAMP);
  taos_stmt_use_result_query(taos, "c11", TSDB_DATA_TYPE_UINT);
  taos_stmt_use_result_query(taos, "c12", TSDB_DATA_TYPE_UBIGINT);
  taos_stmt_use_result_query(taos, "c13", TSDB_DATA_TYPE_USMALLINT);
  taos_stmt_use_result_query(taos, "c14", TSDB_DATA_TYPE_UTINYINT);

  printf("finish taos_stmt_use_result test\n");
}

void taos_stmt_close_test() {
  printf("start taos_stmt_close test\n");
  // ASM ERROR
  TAOS_STMT *stmt = NULL;
  assert(taos_stmt_close(stmt) != 0);
  printf("finish taos_stmt_close test\n");
}

void test_api_reliability() {
  // ASM catch memory leak
  taos_stmt_init_test();
  taos_stmt_preprare_test();
  taos_stmt_set_tbname_test();
  taos_stmt_set_tbname_tags_test();
  taos_stmt_set_sub_tbname_test();
  taos_stmt_bind_param_test();
  taos_stmt_bind_single_param_batch_test();
  taos_stmt_bind_param_batch_test();
  taos_stmt_add_batch_test();
  taos_stmt_execute_test();
  taos_stmt_close_test();
}

void test_query() { taos_stmt_use_result_test(); }

int main(int argc, char *argv[]) {
  test_api_reliability();
  test_query();
  return 0;
}