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

int     concurrent_num = 3000;
char    sql[1024] = { 0 };

int main(int argc, char *argv[])
{
  if ( argc == 1 ) {
    printf("usage: %s concurrent_num cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) strcpy(sql, argv[1]);
  if (argc >= 3 ) concurrent_num = atoi(argv[2]);
  if (argc >= 4 ) strcpy(configDir, argv[3]);

  taos_init();

  pid_t pid;
  for (int i=0; i < concurrent_num; ++i) {	  
	pthread_create(&pid, NULL, taos_execute, NULL);
  }

  printf("once finished, press any key to exit\n");
  getchar();


  return 0;
}

void taos_error(TAOS *con)
{
  fprintf(stderr, "TDengine error: %s\n", taos_errstr(con));
  taos_close(con);
  exit(1);
}

void taos_execute(void *param)
{
	void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
	if ( taos == NULL)
		taos_error(taos);

	while (true) {
		taos_query(taos, sql);
		void *result = taos_use_result(taos);

		if (result == NULL) {
		  continue;
		}

		TAOS_ROW row;
		int numOfRows = 0;

		while ((row = taos_fetch_row(result)))
		{
		  numOfRows++;
		  //printf("numOfRows:%d\n", numOfRows);
		}

		taos_free_result(result);
	}
	
}
