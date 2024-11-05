// TAOS standard API example. The same syntax as MySQL, but only a subet
// to compile: gcc -o stmt2-get-fields stmt2-get-fields.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"

void getFields(TAOS *taos, const char *sql) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2       *stmt = taos_stmt2_init(taos, &option);
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  int             fieldNum = 0;
  TAOS_FIELD_STB *pFields = NULL;
  code = taos_stmt2_get_stb_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("col nums:%d\n", fieldNum);
    for (int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s, data_type:%d, field_type:%d\n", i, pFields[i].name, pFields[i].type,
             pFields[i].field_type);
    }
  }
  printf("====================================\n");
  taos_stmt2_free_stb_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

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
  do_query(taos, "CREATE TABLE db.d0 USING db.stb (t1,t2) TAGS (7,'Cali');");
  do_query(taos, "CREATE TABLE db.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");

  printf("field_type: TAOS_FIELD_COL = 1, TAOS_FIELD_TAG=2, TAOS_FIELD_QUERY=3, TAOS_FIELD_TBNAME=4\n");

  // case 1 : INSERT INTO db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)
  // test super table
  const char *sql = "insert into db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
  printf("====================================\n");
  printf("case 1 : %s\n", sql);
  getFields(taos, sql);

  // case 2 : INSERT INTO db.d0 VALUES (?,?)
  // test child table
  sql = "INSERT INTO db.d0(ts,b) VALUES (?,?)";
  printf("case 2 : %s\n", sql);
  getFields(taos, sql);

  // case 3 : INSERT INTO db.ntb VALUES(?,?,?,?)
  // test normal table
  sql = "INSERT INTO db.ntb VALUES(?,?,?,?)";
  printf("case 3 : %s\n", sql);
  getFields(taos, sql);

  // case 4 : INSERT INTO db.? using db.stb TAGS(?,?) VALUES(?,?)
  // not support this clause
  sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  printf("case 4 (not support): %s\n", sql);
  getFields(taos, sql);

  // case 5 :  INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)
  // no tbname error
  sql = "INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)";
  printf("case 5 (no tbname error): %s\n", sql);
  getFields(taos, sql);

  // case 6 :  INSERT INTO db.d0 using db.stb values(?,?)
  // none para for ctbname
  sql = "INSERT INTO db.d0 using db.stb values(?,?)";
  printf("case 6 (no tags error): %s\n", sql);
  getFields(taos, sql);

  // case 7 :  insert into db.stb(t1,t2,tbname) values(?,?,?)
  // no value
  sql = "insert into db.stb(t1,t2,tbname) values(?,?,?)";
  printf("case 7 (no PK error): %s\n", sql);
  getFields(taos, sql);

  // case 8 :  insert into db.stb(ts,b,tbname) values(?,?,?)
  // no tag
  sql = "insert into db.stb(ts,b,tbname) values(?,?,?)";
  printf("case 8 : %s\n", sql);
  getFields(taos, sql);

  // case 9 : insert into db.stb(ts,b,tbname) values(?,?,?,?,?)
  //  wrong para nums
  sql = "insert into db.stb(ts,b,tbname) values(?,?,?,?,?)";
  printf("case 9 (wrong para nums): %s\n", sql);
  getFields(taos, sql);

  // case 10 : insert into db.ntb(nts,ni) values(?,?,?,?,?)
  //  wrong para nums
  sql = "insert into db.ntb(nts,ni) values(?,?)";
  printf("case 10 : %s\n", sql);
  getFields(taos, sql);
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