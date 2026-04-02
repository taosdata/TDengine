// TAOS standard API example. The same syntax as MySQL, but only a subet 
// to compile: gcc -o demo demo.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <taos.h>  // TAOS header file

void taosMsleep(int mseconds);

int main(int argc, char *argv[])
{
  TAOS     *taos;
  char      qstr[1024];
  TAOS_RES *result;

  // connect to server
  if (argc < 2) {
    printf("please input server ip \n");
    return 0;
  }

  // init TAOS
  if (taos_init()) {
    printf("failed to init taos\n");
    exit(1);
  }

  taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  taos_query(taos, "drop database demo");
  if (taos_query(taos, "create database demo") != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  taos_query(taos, "use demo");

  // create table
  if (taos_query(taos, "create table m1 (ts timestamp, speed int)") != 0) {
    printf("failed to create table, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // sleep for one second to make sure table is created on data node
  // taosMsleep(1000);

  // insert 10 records
  for (int i = 0; i < 10; ++i) {
    sprintf(qstr, "insert into m1 values (now+%ds, %d)", i, i * 10);
    if (taos_query(taos, qstr)) {
      printf("failed to insert row: %i, reason:%s\n", i, taos_errstr(taos));
    }
  }

  // query the records
  sprintf(qstr, "SELECT * FROM m1");
  if (taos_query(taos, qstr) != 0) {
    printf("failed to select, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  result = taos_use_result(taos);

  if (result == NULL) {
    printf("failed to get result, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_field_count(taos);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    printf("%s\n", temp);
  }

  taos_free_result(result);
  return getchar();
}

