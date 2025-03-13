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
  TAOS_FIELD_ALL *pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("bind nums:%d\n", fieldNum);
    for (int i = 0; i < fieldNum; i++) {
      printf("field[%d]: %s, data_type:%d, field_type:%d, precision:%d\n", i, pFields[i].name, pFields[i].type,
             pFields[i].field_type, pFields[i].precision);
    }
  }
  printf("====================================\n");
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void getQueryFields(TAOS *taos, const char *sql) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2       *stmt = taos_stmt2_init(taos, &option);
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  int             fieldNum = 0;
  TAOS_FIELD_ALL *pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("bind nums:%d\n", fieldNum);
  }
  printf("====================================\n");
  taos_stmt2_free_fields(stmt, pFields);
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
  do_query(taos, "create database db PRECISION 'ns'");
  do_query(taos, "use db");
  do_query(taos,
           "create table db.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(taos, "CREATE TABLE db.d0 USING db.stb (t1,t2) TAGS (7,'Cali');");
  do_query(taos, "CREATE TABLE db.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  do_query(
      taos,
      "create table if not exists all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 "
      "tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");

  printf("field_type: TAOS_FIELD_COL = 1, TAOS_FIELD_TAG=2, TAOS_FIELD_QUERY=3, TAOS_FIELD_TBNAME=4\n");

  // case 1 : INSERT INTO db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)
  // test super table
  const char *sql = "insert into db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
  printf("=================normal test===================\n");
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

  // case 4 : INSERT INTO db.ntb VALUES(?,?,?,?)
  // test random order
  sql = "insert into db.stb(t1,tbname,ts,t2,b) values(?,?,?,?,?)";
  printf("case 4 : %s\n", sql);
  getFields(taos, sql);

  // case 5 :  insert into db.stb(ts,b,tbname) values(?,?,?)
  // no tag
  sql = "insert into db.stb(ts,b,tbname) values(?,?,?)";
  printf("case 5 : %s\n", sql);
  getFields(taos, sql);

  // case 6 : INSERT INTO db.? using db.stb TAGS(?,?) VALUES(?,?)
  // normal insert clause
  sql = "insert into ? using db.stb tags(?, ?) values(?,?)";
  printf("case 6 : %s\n", sql);
  getFields(taos, sql);

  // case 7 : insert into db.? using db.stb(t2,t1) tags(?, ?) (b,ts)values(?,?)
  // disordered
  sql = "insert into db.? using db.stb(t2,t1) tags(?, ?) (b,ts)values(?,?)";
  printf("case 7 : %s\n", sql);
  getFields(taos, sql);

  // case 8 : insert into db.? using db.stb tags(?, ?) values(?,?)
  // no field name
  sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  printf("case 8 : %s\n", sql);
  getFields(taos, sql);

  // case 9 : insert into db.? using db.stb tags(?, ?) values(?,?)
  //  less para
  sql = "insert into db.? using db.stb (t2)tags(?) (ts)values(?)";
  printf("case 9 : %s\n", sql);
  getFields(taos, sql);

  // case 10 : insert into db.? using db.stb tags(?, ?) values(?,?)
  //  less para
  sql = "insert into db.d0 (ts)values(?)";
  printf("case 10 : %s\n", sql);
  getFields(taos, sql);

  // case 11 : insert into abc using stb tags(?, ?) values(?,?)
  // insert create table
  sql = "insert into abc using stb tags(?, ?) values(?,?)";
  printf("case 11 : %s\n", sql);
  getFields(taos, sql);

  // case 12 : insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // test all types
  sql = "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  printf("case 12 : %s\n", sql);
  getFields(taos, sql);

  // case 13 : insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // test all types
  sql =
      "insert into all_stb "
      "(tbname,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tv15,ts,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,"
      "v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  printf("case 13 : %s\n", sql);
  getFields(taos, sql);

  // case 14 : select * from ntb where ts = ?
  // query type
  sql = "select * from ntb where ts = ?";
  printf("case 14 : %s\n", sql);
  getQueryFields(taos, sql);

  // case 15 : select * from ntb where ts = ? and b = ?
  // query type
  sql = "select * from ntb where ts = ? and b = ?";
  printf("case 15 : %s\n", sql);
  getQueryFields(taos, sql);
  printf("=================error test===================\n");

  // case 14 :  INSERT INTO db.d0 using db.stb values(?,?)
  // none para for ctbname
  sql = "INSERT INTO db.d0 using db.stb values(?,?)";
  printf("case 14 (no tags error): %s\n", sql);
  getFields(taos, sql);

  // case 15 :  insert into db.stb(t1,t2,tbname) values(?,?,?)
  // no value
  sql = "insert into db.stb(t1,t2,tbname) values(?,?,?)";
  printf("case 15 (no PK error): %s\n", sql);
  getFields(taos, sql);

  // case 16 : insert into db.stb(ts,b,tbname) values(?,?,?,?,?)
  //  wrong para nums
  sql = "insert into db.stb(ts,b,tbname) values(?,?,?,?,?)";
  printf("case 16 (wrong para nums): %s\n", sql);
  getFields(taos, sql);

  // case 17 : insert into db.? values(?,?)
  // normal table must have tbnam
  sql = "insert into db.? values(?,?)";
  printf("case 17 (normal table must have tbname): %s\n", sql);
  getFields(taos, sql);

  // case 18 :  INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)
  // no tbname error
  sql = "INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)";
  printf("case 18 (no tbname error): %s\n", sql);
  getFields(taos, sql);

  // case 19 : insert into db.ntb(nts,ni) values(?,?,?,?,?)
  //  wrong para nums
  sql = "insert into ntb(nts,ni) values(?,?,?,?,?)";
  printf("case 19 : %s\n", sql);
  getFields(taos, sql);

  // case 20 : insert into db.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)
  // wrong simbol
  sql = "insert into db.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)";
  printf("=================normal test===================\n");
  printf("case 20 : %s\n", sql);
  getFields(taos, sql);

  // case 21 : INSERT INTO ! using db.stb TAGS(?,?) VALUES(?,?)
  // wrong simbol
  sql = "insert into ! using db.stb tags(?, ?) values(?,?)";
  printf("case 21 : %s\n", sql);
  getFields(taos, sql);

  // case 22 : INSERT INTO ! using db.stb TAGS(?,?) VALUES(?,?)
  // wrong tbname
  sql = "insert into db.stb values(?,?)";
  printf("case 22 : %s\n", sql);
  getFields(taos, sql);

  // case 23 : select * from ? where ts = ?
  // wrong query type
  sql = "select * from ? where ts = ?";
  printf("case 23 : %s\n", sql);
  getQueryFields(taos, sql);
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