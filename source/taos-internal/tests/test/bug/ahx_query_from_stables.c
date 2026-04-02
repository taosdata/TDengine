// create 10000 tables with tag
// query these table with tag multi-times
// use millseconds

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"

void taos_print_code(int code, char *sql);
void taos_execute_query(void *taos, char *sql);
void taos_execute_update(void *taos, char *sql);

int concurrent_num = 3000;

int tablenum = 1000;
int rownum = 10;
int insert = 1;
int64_t timestamp = 1530374400000000L;
int loop = 1000;
int batch = 1000;

int main(int argc, char *argv[])
{
  if (argc >= 2) tablenum = atoi(argv[1]);
  if (argc >= 3) rownum = atoi(argv[2]);
  if (argc >= 4) insert = atoi(argv[3]);
  if (argc >= 5) loop = atoi(argv[4]);
  printf("usage: %s tablenum:%d rownum:%d insert:%d loop:%d\n", argv[0], tablenum, rownum, insert, loop);

  taos_init();

  void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  char sql[64000];

  if (insert == 1) {
    taos_execute_update(taos, "drop database ahxdb1");
    taos_execute_update(taos, "create database ahxdb1");
    taos_execute_update(taos, "create table ahxdb1.st (ts timestamp, value double) tags(a int, b binary(60))");
    for (int i = 0; i < tablenum; ++i) {
      sprintf(sql, "create table ahxdb1.t%d using ahxdb1.st tags(%d, '%d')", i, i, i);
      taos_execute_update(taos, sql);
      for (int j = 0; j < rownum; ++j) {
        int len = sprintf(sql, "insert into ahxdb1.t%d values ", i);
        for (int k = 0; k < batch; k++) {
          len += sprintf(sql+len, "(%ld, %d)", timestamp + 60000*(j*batch+k), j*batch+k);
        }
      }
      taos_execute_update(taos, sql);
    }
  }

  for (int i = 0; i < loop; ++i) {
    for (int j = 0; j < tablenum; ++j) {
      sprintf(sql, "select value from ahxdb1.st where b='%d'", j);
      taos_execute_query(taos, sql);
    }
  }

  return 0;
}

void taos_print_code(int code, char *sql)
{
  if (code != 0) {
    fprintf(stderr, "TDengine error: %d, %s\n", code, sql);
  }
}

void taos_execute_update(void *taos, char *sql)
{
  int code = taos_query(taos, sql);
  taos_print_code(code, sql);
}

void taos_execute_query(void *taos, char *sql)
{
  int code = taos_query(taos, sql);
  taos_print_code(code, sql);

  void *result = taos_use_result(taos);

  if (result == NULL) {
    fprintf(stderr, "TDengine error: result set is null\n");
    return;
  }

  TAOS_ROW row;
  int numOfRows = 0;

  while ((row = taos_fetch_row(result)))
  {
    numOfRows++;
  }

  taos_free_result(result);

  printf("sql:%s, rows:%d\n", sql, numOfRows);
}
