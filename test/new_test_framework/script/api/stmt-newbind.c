#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

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

void do_stmt(TAOS* taos) {
  do_query(taos, "drop database if exists power");
  do_query(taos, "create database power");
  do_query(taos,
           "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
           "(groupId INT, location BINARY(24))");

  const char* sql = "insert into power.meters(tbname,groupId,location,ts,current,voltage,phase) values(?,?,?,?,?,?,?)";

  //  TAOS_STMT2_OPTION option = {0};
  // TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2_BIND2* bindv = malloc(sizeof(TAOS_STMT2_BIND2) * 7);
  void*             tbname = malloc(6);
  memcpy(tbname, "tb1", 3);
  memcpy(tbname + 3, "tb1", 3);
  int32_t tb_len[2] = {3, 3};
  bindv[0] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_BINARY, tbname, &tb_len[0], NULL};

  void* groupId = malloc(sizeof(int) * 2);
  *(int*)groupId = 1;
  *(int*)(groupId + sizeof(int)) = 2;
  int32_t groupId_len[2] = {sizeof(int), sizeof(int)};
  bindv[1] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_INT, groupId, &groupId_len[0], NULL};

  void* location = malloc(sizeof(char) * 24);
  memcpy(location, "location1", 9);
  memcpy(location + 9, "location2", 15);
  int32_t location_len[2] = {9, 15};
  bindv[2] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_BINARY, location, &location_len[0], NULL};

  void* ts = malloc(sizeof(int64_t) * 2);
  *(int64_t*)ts = 1672531200000;                      // 2023-01-01 00:00:00
  *(int64_t*)(ts + sizeof(int64_t)) = 1672617600000;  // 2023-01-02 00:00:00
  int32_t ts_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  bindv[3] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_TIMESTAMP, ts, &ts_len[0], NULL};

  void* current = malloc(sizeof(float) * 2);
  *(float*)current = 1.0;
  *(float*)(current + sizeof(float)) = 2.0;
  int32_t current_len[2] = {sizeof(float), sizeof(float)};
  bindv[4] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_FLOAT, current, &current_len[0], NULL};

  void* voltage = malloc(sizeof(int) * 2);
  *(int*)voltage = 1;
  *(int*)(voltage + sizeof(int)) = 2;
  int32_t voltage_len[2] = {sizeof(int), sizeof(int)};
  bindv[5] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_INT, voltage, &voltage_len[0], NULL};

  void* phase = malloc(sizeof(float) * 2);
  *(float*)phase = 1.0;
  *(float*)(phase + sizeof(float)) = 2.0;
  int32_t phase_len[2] = {sizeof(float), sizeof(float)};
  bindv[6] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_FLOAT, phase, &phase_len[0], NULL};

  // void* b = malloc(10);
  // memcpy(b, "binary1", 7);
  // memcpy(b + 7, "bi2", 3);
  // int32_t b_len[2] = {7, 3};
  // bindv[2] = (TAOS_STMT2_BIND2){TSDB_DATA_TYPE_BINARY, b, &b_len[0], NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);

  // Equivalent to :
  // const char* sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("col nums:%d\n", fieldNum);
    for (int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s, data_type:%d, field_type:%d\n", i, pFields[i].name, pFields[i].type,
             pFields[i].field_type);
    }
  }

  if (taos_stmt2_bind_param_test(stmt, bindv, 2)) {
    printf("failed to execute taos_stmt2_bind_param_test statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_free_fields(stmt, pFields);
    taos_stmt2_close(stmt);
    return;
  }

  if (taos_stmt2_exec(stmt, NULL)) {
    printf("failed to execute insert statement.error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_free_fields(stmt, pFields);
    taos_stmt2_close(stmt);
    return;
  }

  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);

  free(bindv);
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  do_stmt(taos);
  taos_close(taos);
  taos_cleanup();
}
