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

void tscSelectCallBack(void *param, TAOS_RES *tres, int code);
void taos_error(TAOS *taos);

TAOS   *taos = NULL;
int     concurrent_num = 3000;
int     querySeconds = 10;
char    sql[1024] = "select * from db.t0";

int main(int argc, char *argv[])
{
  if ( argc == 1 ) {
    printf("usage: %s sql concurrent_num cfg\n", argv[0]);
    exit(0);
  }
    
  // a simple way to parse input parameters
  if (argc >= 2 ) concurrent_num = atoi(argv[1]);
  if (argc >= 3 ) querySeconds = atoi(argv[2]);

  taos_init();

  taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if ( taos == NULL)
    taos_error(taos);

  for (int i=0; i < concurrent_num; ++i) {
    taos_query_a(taos, sql, tscSelectCallBack, NULL);
  }
  
  printf("async_query program successfully start, will run %d seconds\n", querySeconds);
  sleep((unsigned)querySeconds);
  printf("async_query program  successfully stopped\n");
  
  taos_close(taos);
  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

void tscRetrieveCallBack(void *param, TAOS_RES *tres, int numOfRows)
{
  //printf("retrieve callback\n");
  if ( numOfRows > 0 ) {
    for (int i = 0; i < numOfRows; ++i) {
      taos_fetch_row(tres);
    }
    taos_fetch_rows_a(tres, tscRetrieveCallBack, NULL);
  } else {
	if (numOfRows == 0)
		taos_free_result(tres);
	taos_query_a(taos, sql, tscSelectCallBack, NULL);
  }
}

void tscSelectCallBack(void *param, TAOS_RES *tres, int code)
{
  //printf("select callback\n");
  if ( code == 0 && tres ) {
    taos_fetch_rows_a(tres, tscRetrieveCallBack, NULL);
  } else {
	taos_query_a(taos, sql, tscSelectCallBack, NULL);
  }
}
