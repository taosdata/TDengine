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

typedef struct {
	pthread_t pid;
	int       index;
} ThreadObj;

int threadNum = 0;
int rowNum = 0;
int replica = 1;

int main(int argc, char *argv[])
{
  if ( argc == 1 ) {
    printf("usage: %s threadNum rowNum configDir\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) threadNum = atoi(argv[1]);
  if (argc >= 3 ) rowNum = atoi(argv[2]);
  if (argc >= 4 ) replica = atoi(argv[3]);
  if (argc >= 5 ) strcpy(configDir, argv[4]);
  
  printf("threadNum:%d rowNum:%d \n", threadNum, rowNum);
    

  taos_init();

  ThreadObj *threads = calloc(threadNum, sizeof(ThreadObj));
  for (int i=0; i < threadNum; ++i) {	  
	ThreadObj *pthread = threads + i;
	pthread_attr_t thattr;
	pthread->index = i;
	pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
	pthread_create(&pthread->pid, &thattr, taos_execute, pthread);
  }

  for (int i = 0; i < threadNum; i++) {
    pthread_join(threads[i].pid, NULL);
  }
  
  printf("all finished\n");


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
	ThreadObj *pThread = (ThreadObj*)param;
	
	void *taos = taos_connect(tsMasterIp, tsDefaultUser, tsDefaultPass, NULL, 0);
	if ( taos == NULL)
		taos_error(taos);
	
	char sql[1024] = { 0 };
	sprintf(sql, "create database db replica %d", replica);
	taos_query(taos, sql);
	
	sprintf(sql, "create table db.t%d (ts timestamp, i int, j float, k double)", pThread->index);
	taos_query(taos, sql);
	
	int64_t timestamp = 1530374400000L;
	
	sprintf(sql, "insert into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, 0, 0, 0);
	int code = taos_query(taos, sql);
	if (code != 0) 
		printf("error code:%d, sql:%s\n", code, sql);
	int affectrows = taos_affected_rows(taos);
	if (affectrows != 1)
		printf("affect rows:%d, sql:%s\n", affectrows, sql);
	
	timestamp -= 1000;
	
	int total_affect_rows = affectrows;
		
	for (int i = 1; i < rowNum; ++i) {
		
		sprintf(sql, "import into db.t%d values(%ld, %d, %d, %d)", pThread->index, timestamp, i, i, i);
		code = taos_query(taos, sql);
		if (code != 0) 
			printf("error code:%d, sql:%s\n", code, sql);
		int affectrows = taos_affected_rows(taos);
		if (affectrows != 1)
			printf("affect rows:%d, sql:%s\n", affectrows, sql);	
		
		total_affect_rows += affectrows;
		
		
		timestamp -= 1000;
	}
	
	printf("thread:%d run finished total_affect_rows:%d\n", pThread->index, total_affect_rows);
}
