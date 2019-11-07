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
    printf("usage: %s threadNum rowNum repica configDir\n", argv[0]);
    exit(0);
  }

  // a simple way to parse input parameters
  if (argc >= 2 ) threadNum = atoi(argv[1]);
  if (argc >= 3 ) rowNum = atoi(argv[2]);
  if (argc >= 4 ) replica = atoi(argv[3]);
  if (argc >= 5 ) strcpy(configDir, argv[4]);
  
  printf("threadNum:%d rowNum:%d replica:%d\n", threadNum, rowNum, replica);
    

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
	sprintf(sql, "create database d%d replica %d", pThread->index, replica);
	taos_query(taos, sql);
	
	sprintf(sql, "create table d%d.t%d (ts timestamp, i int, j float, k double)", pThread->index, pThread->index);
	taos_query(taos, sql);
	
	int64_t timestamp = 1530374400000L;
	for (int i = 0; i < rowNum; ++i) {
		
		int val = 10*i + 2;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 200, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 3;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 300, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 4;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 400, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 5;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 500, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 6;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 600, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 7;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 700, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 8;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 800, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 9;
		sprintf(sql, "insert into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 900, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 1;
		sprintf(sql, "import into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 100, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		val = 10*i + 0;
		sprintf(sql, "import into d%d.t%d values(%ld, %d, %d, %d)", pThread->index, pThread->index, timestamp + 0, val, val, val);
		//printf("%s\n", sql);
		taos_query(taos, sql);
		
		timestamp += 1000;
	}
	
	printf("thread:%d run finished\n", pThread->index);
}
