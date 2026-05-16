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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <argp.h>

#include <taos/taos.h>
#include <stdbool.h>
#include <taosdebug.h>

TAOS   *taos = NULL;

char sql[200] = "select * from syncdb1.table0";
int  loopTimes = 1;
int  sleepInerval = 1000;

int main(int argc, char *argv[])
{
  if ( argc == 1 ) {
    tPrint("usage: %s sleepIntervl loopTimes cfg sql \n", argv[0]);
    exit(0);
  }

  strcpy(configDir, "~/work/sim/dnode1/cfg");
  
  // a simple way to parse input parameters
  if (argc >= 2 ) sleepInerval = atoi(argv[1]);
  if (argc >= 3 ) loopTimes = atoi(argv[2]); 
  if (argc >= 4 ) strcpy(configDir, argv[3]);
  if (argc >= 5 ) strcpy(sql, argv[4]);
  tPrint ("sleep:%d, loop:%d, config:%s, sql:%s\n", sleepInerval, loopTimes, configDir, sql);
  
  taos_init();

  taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
  if ( taos == NULL)
    taos_error(taos);

  for (int i=0; i < loopTimes; ++i) {
    taos_query(taos, sql);

    void *result = taos_use_result(taos);

    if (result == NULL) {
      tPrint("failed to get result, reason:%s\n", taos_errstr(taos));
      return 0;
    }

    TAOS_ROW row;
    int numOfRows = 0;

    while ((row = taos_fetch_row(result)))
    {
      numOfRows++;
	  if (numOfRows % 200 == 0) {
		  tPrint("sleep:%d, numOfRows:%d\n", sleepInerval, numOfRows);
		  taosMsleep(sleepInerval);
	  }
    }

    taos_free_result(result);
    tPrint("query times:%d/%d\n", i, loopTimes);
  }


  tPrint("once finished, press any key to exit\n");
  getchar();

  taos_close(taos);

  return 0;
}

void taos_error(TAOS *con)
{
  tPrint("TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

