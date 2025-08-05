/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include "taos.h"
#include "tlog.h"
#include "types.h"

#define GET_ROW_NUM             \
  numRows = 0;                  \
  while (1) {                   \
    row = taos_fetch_row(pRes); \
    if (row != NULL) {          \
      numRows++;                \
    } else {                    \
      break;                    \
    }                           \
  }
#define RAND_MAX_TEMP 2147483647

#define NUM_OF_SUB_TABLES 100
#define NUM_OF_ROWS       100

void freeBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    taosMemFree((*table_name)[i]);
    for (int j = 0; j < 2; j++) {
      taosMemFree((*tags)[i][j].buffer);
      taosMemFree((*tags)[i][j].length);
    }
    taosMemFree((*tags)[i]);

    for (int j = 0; j < 4; j++) {
      taosMemFree((*params)[i][j].buffer);
      taosMemFree((*params)[i][j].length);
    }
    taosMemFree((*params)[i]);
  }
  taosMemFree(*table_name);
  taosMemFree(*tags);
  taosMemFree(*params);
}
void blob_sql_test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists blob_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists blob_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use blob_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_query(
      taos,
      "create stable stb (ts timestamp, c1 nchar(32), c2 blob, c3 float) tags (t1 int, t2 binary(8), t3 varbinary(8))");
  taos_free_result(pRes);

  pRes = taos_query(taos, "desc stb");

  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    char   *type = row[1];
    int32_t length = *(int32_t *)row[2];

    if (rowIndex == 2) {
      ASSERT(strncmp(type, "BLOB", strlen("BLOB") - 1) == 0);
    }

    rowIndex++;
  }
  taos_free_result(pRes);

  pRes =
      taos_query(taos, "insert into tb1 using stb tags (1, 'tb1_bin1', 'vart1') values (now, 'nchar1', 'varc1', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb1 values (now + 1s, 'nchar2', null, 0.4);");
  taos_free_result(pRes);
  pRes = taos_query(
      taos,
      "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f8290') values (now + 2s, 'nchar1', '\\x7f8290', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb3 values (now + 3s, 'nchar1', '\\x7f829000', 0.3)");
  taos_free_result(pRes);
  pRes =
      taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', '\\x') values (now + 4s, 'nchar1', '\\x', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb2 values (now + 5s, 'nchar1', '\\x00000000', 0.3)");
  taos_free_result(pRes);

  // test insert
  pRes = taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', 093) values (now + 2s, 'nchar1', 892, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(
      taos, "insert into tb3 using stb tags (3, 'tb3_bin1', 0x7f829) values (now + 3s, 'nchar1', 0x7f829, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(
      taos, "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f829') values (now + 3s, 'nchar1', '\\x7f829', 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(
      taos,
      "insert into tb4 using stb tags (4, 'tb4_bin1', 0b100000010) values (now + 4s, 'nchar1', 0b110000001, 0.3)");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  // math function test, not support
  // pRes = taos_query(taos, "select abs(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select floor(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // string function test, not support
  // pRes = taos_query(taos, "select ltrim(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select upper(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select to_json(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select TO_UNIXTIMESTAMP(c2) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select cast(c2 as varchar(16)) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select cast(c3 as varbinary(16)) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select cast(c1 as varbinary(16)) from stb");
  // ASSERT(taos_errno(pRes) != 0);
  // taos_free_result(pRes);

  // support first/last/last_row/count/hyperloglog/sample/tail/mode/length
  // pRes = taos_query(taos, "select first(c2) from stb");
  // ASSERT(taos_errno(pRes) == 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select count(c2) from stb");
  // ASSERT(taos_errno(pRes) == 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select sample(c2,2) from stb");
  // ASSERT(taos_errno(pRes) == 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select mode(c2) from stb");
  // ASSERT(taos_errno(pRes) == 0);
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select length(c2) from stb where c2 = '\\x7F8290'");
  // ASSERT(taos_errno(pRes) == 0);
  // taos_free_result(pRes);

  // // pRes = taos_query(taos, "select cast(t2 as varbinary(16)) from stb order by ts");
  // while ((row = taos_fetch_row(pRes)) != NULL) {
  //   int32_t* length = taos_fetch_lengths(pRes);
  //   void*    data = NULL;
  //   uint32_t size = 0;
  //   if (taosAscii2Hex(row[0], length[0], &data, &size) < 0) {
  //     ASSERT(0);
  //   }

  //   ASSERT(memcmp(data, "\\x7462315F62696E31", size) == 0);
  //   taosMemoryFree(data);
  //   break;
  // }
  // taos_free_result(pRes);

  // pRes = taos_query(taos, "select cast('1' as varbinary(8))");
  // while ((row = taos_fetch_row(pRes)) != NULL) {
  //   int32_t* length = taos_fetch_lengths(pRes);
  //   void*    data = NULL;
  //   uint32_t size = 0;
  //   if (taosAscii2Hex(row[0], length[0], &data, &size) < 0) {
  //     ASSERT(0);
  //   }

  //   ASSERT(memcmp(data, "\\x31", size) == 0);
  //   taosMemoryFree(data);
  // }
  // taos_free_result(pRes);

  pRes = taos_query(taos, "select ts,c2 from stb order by c2");
  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    //    int64_t ts = *(int64_t *)row[0];
    if (rowIndex == 0) {
      ASSERT(row[1] == NULL);
      rowIndex++;
      continue;
    }
    int32_t *length = taos_fetch_lengths(pRes);
    void    *data = NULL;
    uint32_t size = 0;
    if (taosAscii2Hex(row[1], length[1], &data, &size) < 0) {
      ASSERT(0);
    }

    if (rowIndex == 1) {
      //      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x", size) == 0);
    }
    if (rowIndex == 2) {
      //      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x00000000", size) == 0);
    }
    if (rowIndex == 3) {
      //      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7661726331", size) == 0);
    }
    if (rowIndex == 4) {
      //      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F8290", size) == 0);
    }
    if (rowIndex == 5) {
      //      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F829000", size) == 0);
    }
    taosMemoryFree(data);

    rowIndex++;
  }
  printf("%s result %s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  // test insert string value '\x'
  pRes = taos_query(
      taos, "insert into tb5 using stb tags (5, 'tb5_bin1', '\\\\xg') values (now + 4s, 'nchar1', '\\\\xg', 0.3)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2,t3 from stb where t3 = '\\x5C7867'");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t *length = taos_fetch_lengths(pRes);
    void    *data = NULL;
    uint32_t size = 0;
    if (taosAscii2Hex(row[0], length[0], &data, &size) < 0) {
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x5C7867", size) == 0);
    taosMemoryFree(data);

    if (taosAscii2Hex(row[1], length[1], &data, &size) < 0) {
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x5C7867", size) == 0);
    taosMemoryFree(data);
  }
  taos_free_result(pRes);

  // test insert
  char tmp[65517 * 2 + 3] = {0};
  tmp[0] = '\\';
  tmp[1] = 'x';
  memset(tmp + 2, 48, 65517 * 2);

  char sql[65517 * 2 + 3 + 256] = {0};

  pRes = taos_query(taos, "create stable stb1 (ts timestamp, c2 blob)) tags (t1 int, t2 binary(8), t3 varbinary(8))");
  taos_free_result(pRes);

  sprintf(sql, "insert into tb6 using stb1 tags (6, 'tb6_bin1', '\\\\xg') values (now + 4s, '%s')", tmp);
  pRes = taos_query(taos, sql);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2 from tb6");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t *length = taos_fetch_lengths(pRes);
    void    *data = NULL;
    uint32_t size = 0;
    if (taosAscii2Hex(row[0], length[0], &data, &size) < 0) {
      ASSERT(0);
    }

    ASSERT(memcmp(data, tmp, size) == 0);
    taosMemoryFree(data);
  }
  taos_free_result(pRes);

  taos_close(taos);
}
void checkErrorCode(TAOS_STMT2 *stmt2, int code, const char *msg) {
  if (code != 0) {
    fprintf(stderr, "%s. Code: %d, Error: %s\n", msg, code, taos_stmt2_error(stmt2));
    taos_stmt2_close(stmt2);
    exit(EXIT_FAILURE);
  }
}

/**
 * @brief Prepares data bindings for batch insertion.
 *
 * @param table_name Pointer to store allocated table names.
 * @param tags Pointer to store allocated tag bindings.
 * @param params Pointer to store allocated parameter bindings.
 */
void prepareBindData(char ***table_name, TAOS_STMT2_BIND ***tags, TAOS_STMT2_BIND ***params) {
  *table_name = (char **)taosMemMalloc(NUM_OF_SUB_TABLES * sizeof(char *));
  *tags = (TAOS_STMT2_BIND **)taosMemMalloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));
  *params = (TAOS_STMT2_BIND **)taosMemMalloc(NUM_OF_SUB_TABLES * sizeof(TAOS_STMT2_BIND *));

  int32_t len = 10 * 1024;
  for (int i = 0; i < NUM_OF_SUB_TABLES; i++) {
    // Allocate and assign table name
    (*table_name)[i] = (char *)taosMemMalloc(20 * sizeof(char));
    sprintf((*table_name)[i], "d_bind_%d", i);

    // Allocate memory for tags data
    int *gid = (int *)taosMemMalloc(sizeof(int));
    int *gid_len = (int *)taosMemMalloc(sizeof(int));
    *gid = i;
    *gid_len = sizeof(int);

    char *location = (char *)taosMemMalloc(20 * sizeof(char));
    int  *location_len = (int *)taosMemMalloc(sizeof(int));
    *location_len = sprintf(location, "location_%d", i);

    (*tags)[i] = (TAOS_STMT2_BIND *)taosMemMalloc(2 * sizeof(TAOS_STMT2_BIND));
    (*tags)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, gid, gid_len, NULL, 1};
    (*tags)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, location, location_len, NULL, 1};

    // Allocate memory for columns data
    (*params)[i] = (TAOS_STMT2_BIND *)taosMemMalloc(5 * sizeof(TAOS_STMT2_BIND));

    int64_t *ts = (int64_t *)taosMemMalloc(NUM_OF_ROWS * sizeof(int64_t));
    float   *current = (float *)taosMemMalloc(NUM_OF_ROWS * sizeof(float));
    int     *voltage = (int *)taosMemMalloc(NUM_OF_ROWS * sizeof(int));
    char   **phase = (char **)taosMemMalloc(NUM_OF_ROWS * sizeof(char *));
    int32_t *ts_len = (int32_t *)taosMemMalloc(NUM_OF_ROWS * sizeof(int32_t));
    int32_t *current_len = (int32_t *)taosMemMalloc(NUM_OF_ROWS * sizeof(int32_t));
    for (int j = 0; j < NUM_OF_ROWS; j++) {
      phase[j] = (char *)taosMemMalloc(sizeof(char) * 20);  // Allocate memory for phase
      sprintf(phase[j], "phase_%d", j);                     // Assign a value to phase
    }

    int32_t *voltage_len = (int32_t *)taosMemMalloc(NUM_OF_ROWS * sizeof(int32_t));
    int32_t *phase_len = (int32_t *)taosMemMalloc(NUM_OF_ROWS * sizeof(int32_t));
    for (int j = 0; j < NUM_OF_ROWS; j++) {
      phase_len[j] = strlen(phase[j]);  // Set length for phase
    }

    (*params)[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, ts, ts_len, NULL, NUM_OF_ROWS};
    (*params)[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_FLOAT, current, current_len, NULL, NUM_OF_ROWS};
    (*params)[i][2] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, voltage, voltage_len, NULL, NUM_OF_ROWS};
    (*params)[i][3] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BLOB, phase, phase_len, NULL, NUM_OF_ROWS};

    for (int j = 0; j < NUM_OF_ROWS; j++) {
      struct timeval tv;
      taosGetTimeOfDay(&tv);
      ts[j] = tv.tv_sec * 1000LL + tv.tv_usec / 1000 + j;
      current[j] = (float)taosRand() / RAND_MAX_TEMP * 30;
      voltage[j] = taosRand() % 300;
      // phase[j] = (char *)malloc(20 * sizeof(char));

      ts_len[j] = sizeof(int64_t);
      current_len[j] = sizeof(float);
      voltage_len[j] = sizeof(int);
      // phase_len[j] = sizeof(float);
    }
  }
}
void insertData(TAOS *taos) {
  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};
  TAOS_STMT2       *stmt2 = taos_stmt2_init(taos, &option);
  if (!stmt2) {
    fprintf(stderr, "Failed to initialize TAOS statement.\n");
    exit(EXIT_FAILURE);
  }
  // stmt2 prepare sql
  checkErrorCode(stmt2, taos_stmt2_prepare(stmt2, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 0),
                 "Statement preparation failed");

  char            **table_name;
  TAOS_STMT2_BIND **tags, **params;
  prepareBindData(&table_name, &tags, &params);
  // stmt2 bind batch
  TAOS_STMT2_BINDV bindv = {NUM_OF_SUB_TABLES, table_name, tags, params};
  checkErrorCode(stmt2, taos_stmt2_bind_param(stmt2, &bindv, -1), "Parameter binding failed");
  // stmt2 exec batch
  int affected;
  checkErrorCode(stmt2, taos_stmt2_exec(stmt2, &affected), "Execution failed");
  printf("Successfully inserted %d rows.\n", affected);
  // free and close
  freeBindData(&table_name, &tags, &params);
  taos_stmt2_close(stmt2);
}

void queryData(TAOS *taos) {
  int32_t   code = 0;
  TAOS_RES *result = NULL;

  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  int32_t  rows = 0;

  const char *sql = "select * from meters";
  result = taos_query(taos, sql);
  if (result == NULL || taos_errno(result) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }

  int         num_fields = taos_field_count(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  int32_t    *length = taos_fetch_lengths(result);
  while ((row = taos_fetch_row(result)) != NULL) {
    char temp[1024] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }
  return;
}
void blob_stmt2_test() {
  TAOS      *taos;
  TAOS_RES  *result;
  int        code;
  TAOS_STMT *stmt;

  taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    ASSERT(0);
  }

  result = taos_query(taos, "drop database power");
  taos_free_result(result);

  result = taos_query(taos, "create database power");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    ASSERT(0);
  }
  taos_free_result(result);

  result = taos_query(taos, "use power");
  taos_free_result(result);

  // // create table
  const char *sql =
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase blob) TAGS "
      "(groupId INT, location BINARY(24))";

  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create stable , reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    ASSERT(0);
  }
  taos_free_result(result);
  insertData(taos);
  queryData(taos);
}

tmq_t *build_consumer() {
  tmq_conf_t *conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");

  tmq_t *tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

void blob_tmq_test() {
  // build database

  TAOS *pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  TAOS_RES *pRes = taos_query(pConn, "drop database if exists abc");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists abc vgroups 1 wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, c2 blob) tags(t1 int, t2 varbinary(8))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, '\\x3f89')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 'hello')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add tag t3 varbinary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct0 set tag t2='894'");
  if (taos_errno(pRes) != 0) {
    printf("failed to slter child table ct3, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tb1 (ts timestamp, c1 blob)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  // create topic
  pRes = taos_query(pConn, "create topic topic_db with meta as database abc");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  // build consumer
  tmq_t      *tmq = build_consumer();
  tmq_list_t *topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_db");

  int32_t code = tmq_subscribe(tmq, topic_list);
  ASSERT(code == 0);

  int32_t cnt = 0;
  while (1) {
    TAOS_RES *tmqmessage = tmq_consumer_poll(tmq, 1000);
    if (tmqmessage) {
      if (tmq_get_res_type(tmqmessage) == TMQ_RES_TABLE_META || tmq_get_res_type(tmqmessage) == TMQ_RES_METADATA) {
        char *result = tmq_get_json_meta(tmqmessage);
        //        if (result) {
        //          printf("meta result: %s\n", result);
        //        }
        switch (cnt) {
          case 0:
            ASSERT(strcmp(result,
                          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":"
                          "\"ts\",\"type\":9},{\"name\":\"c2\",\"type\":16,\"length\":16}],\"tags\":[{\"name\":\"t1\","
                          "\"type\":4},{\"name\":\"t2\",\"type\":16,\"length\":8}]}") == 0);
            break;
          case 1:
            ASSERT(strcmp(result,
                          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\","
                          "\"tagNum\":2,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t2\","
                          "\"type\":16,\"value\":\"\\\"\\\\x3F89\\\"\"}],\"createList\":[]}") == 0);
            break;
          case 2:
            ASSERT(strcmp(result,
                          "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,"
                          "\"colName\":\"c2\",\"colType\":16,\"colLength\":64}") == 0);
            break;
          case 3:
            ASSERT(strcmp(result,
                          "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,"
                          "\"colName\":\"t3\",\"colType\":16,\"colLength\":64}") == 0);
            break;
          case 4:
            ASSERT(strcmp(result,
                          "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"alterType\":4,"
                          "\"colName\":\"t2\",\"colValue\":\"\\\"\\\\x383934\\\"\",\"colValueNull\":false}") == 0);
            break;
          case 5:
            ASSERT(strcmp(result,
                          "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":"
                          "\"ts\",\"type\":9},{\"name\":\"c1\",\"type\":16,\"length\":8}],\"tags\":[]}") == 0);
            break;
          case 6:
            ASSERT(strcmp(result,
                          "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"alterType\":5,"
                          "\"colName\":\"c2\",\"colType\":16,\"colLength\":8}") == 0);
            break;
          default:
            break;
        }
        cnt++;
        tmq_free_json_meta(result);
      }
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  code = tmq_consumer_close(tmq);
  ASSERT(code == 0);

  tmq_list_destroy(topic_list);

  pRes = taos_query(pConn, "drop topic if exists topic_db");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);
  taos_close(pConn);
  printf("%s result success\n", __FUNCTION__);
}

int main(int argc, char *argv[]) {
  int ret = 0;

  blob_tmq_test();
  blob_stmt2_test();
  blob_sql_test();
  return ret;
}
