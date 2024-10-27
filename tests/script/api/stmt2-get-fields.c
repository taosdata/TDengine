// TAOS standard API example. The same syntax as MySQL, but only a subet
// to compile: gcc -o stmt2-get-fields stmt2-get-fields.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void do_query(TAOS *taos, const char *sql) {
  TAOS_RES *result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void do_stmt(TAOS *taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos,
           "create table db.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2 *stmt = taos_stmt2_init(taos, &option);
  const char *sql = "insert into db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";

  int code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }

  int           fieldNum = 0;
  TAOS_FIELD_E *pFields = NULL;
  code = taos_stmt2_get_all_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("col nums:%d\n", fieldNum);
    for(int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s,type:%d\n", i, pFields[i].name,pFields[i].field_type);
    }
  }


  taos_stmt2_close(stmt);
}

int main() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  do_stmt(taos);
  taos_close(taos);
  taos_cleanup();
}