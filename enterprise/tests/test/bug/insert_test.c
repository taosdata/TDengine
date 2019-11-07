// TAOS asynchronous API example
// this example opens multiple tables, insert/retrieve multiple tables
// it is used by TAOS internally for one performance testing
// for a simple async example, check asyncdemo.c
// to compiple: gcc -o masync masync.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

#include "taos.h"
#include "tsclient.h"

void taos_error(TAOS *taos);
void taos_execute(void *param);

char host[100] = "192.168.0.1";
int table = 1;
int totalRows = 1000000;

int main(int argc, char *argv[])
{
  if ( argc == 1 ) {
    printf("usage: %s host tables rows cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2) strcpy(host, argv[1]);
  if (argc >= 3) table = atoi(argv[2]);
  if (argc >= 4) totalRows = atoi(argv[3]);
  if (argc >= 5 ) strcpy(configDir, argv[4]);

  taos_init();

  int64_t ts = 1519833600000L;

  void *taos = taos_connect(host, "root", "taosdata", NULL, 0);
  taos_query(taos, "drop database cdb");
  taos_query(taos, "create database cdb");

  char sql[1024];
  for (int i = 0; i < table; i++) {
    sprintf(sql, "create table cdb.t%d(ts timestamp, f1 int)", i);
    taos_query(taos, sql);
  }

  //return 0;
  int64_t start = taosGetTimestampMs();
  for (int i = 0; i < table; i++) {
    for (int j = 0; j < totalRows; ++j) {
      sprintf(sql, "insert into cdb.t%d values (%ld, %ld)", i, ts+j, j);
      taos_query(taos, sql);

      if (j % 10000 == 0) {
        printf("%d rows inserted\n", j);
      }
    }
  }

  int64_t end = taosGetTimestampMs();
  printf("%d rows inserted, time spend %ld seconds\n", totalRows * table, (end-start)/1000);

  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}
