#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>
#include "taos.h"
#include "tsclient.h"


int main(int argc, char *argv[])
{
  TAOS   *taos = NULL;
  int     concurrent_num = 10;
  int     repeat_num = 30000;

  if ( argc == 1 ) {
    printf("usage: %s repeat_num concurrent_num cfg\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) repeat_num = atoi(argv[1]);
  if (argc >= 3 ) concurrent_num = atoi(argv[2]);
  if (argc >= 4 ) strcpy(configDir, argv[3]);

  taos_init();

  for (int i = 0; i < repeat_num; ++i) { 
	taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
	if ( taos == NULL) {
		printf("connect failed:%s", taos_errstr(taos));
	}
	sleep (3);
	taos_close(taos);
	if ( i % 10000 == 0) {
		printf("test times:%d finished", i);
	}
  }

  printf("test finished\n");
  return 0;
}
