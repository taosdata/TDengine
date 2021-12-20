#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"  // TAOS header file
#include "taoserror.h"
#include "os.h"

bool verbose = false;
bool describeTableFirst = false;

typedef struct{
  TAOS* taos;
  int numThreads;
  int threadId;
  int numSTables;
} SThreadArgs;

static int executeSql(TAOS *taos, char *command) {
  if (verbose) {
    printf("sql: %s\n", command);
  }
  TAOS_RES *pSql = NULL;
  int32_t   code = TSDB_CODE_SUCCESS;

  pSql = taos_query(taos, command);
  code = taos_errno(pSql);

  if (code != 0) {
    if (verbose) fprintf(stderr, "Failed to run %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    return code;
  }

  taos_free_result(pSql);
  return 0;
}

void* threadFunc(void* args) {
  char* sqlDescribeSTable = "describe st%d";
  char* sqlCreateSTable = "create table st%d (ts timestamp, value double) "
      "tags(t0 nchar(20), t1 nchar(20), t2 nchar(20), t3 nchar(20), t4 nchar(20), "
      "t5 nchar(20), t6 nchar(20), t7 nchar(20), t8 nchar(20), t9 nchar(20))";

  char* sqlInsertData = "insert into t%d using st%d tags('t%d', 't%d', 't%d', 't%d', 't%d', 't%d', 't%d', 't%d', 't%d', 't%d') values(%lld, %d.%d)";

  SThreadArgs* param = args;

  int interval = param->numSTables/param->numThreads;
  if (param->numSTables % param->numThreads != 0) {
    ++interval;
  }
  int start = param->threadId*interval;
  int end = (param->threadId+1)*interval > param->numSTables ? param->numSTables :  (param->threadId+1)*interval;
  int     r = rand();

  for (int i = start; i < end; ++i) {
    int tableId = i;
    char sql0[1024] = {0};
    char sql1[1024] = {0};
    char sql2[1024] = {0};
    sprintf(sql0, sqlDescribeSTable, tableId);
    sprintf(sql1, sqlCreateSTable, tableId);
    time_t  ct = time(0);
    int64_t ts = ct * 1000;
    sprintf(sql2, sqlInsertData, tableId, tableId, r, r, r, r, r, r, r, r, r, r, ts + tableId, r, r);

    if (describeTableFirst) {
      executeSql(param->taos, sql0);
    }

    executeSql(param->taos, sql1);

    executeSql(param->taos, sql2);
  }
  return NULL;
}


int main(int argc, char *argv[]) {
  int numSTables = 20000;
  int numThreads = 32;

  int opt;
  while ((opt = getopt(argc, argv, "s:t:fvh")) != -1) {
    switch (opt) {
      case 's':
        numSTables = atoi(optarg);
        break;
      case 't':
        numThreads = atoi(optarg);
        break;
      case 'f':
        describeTableFirst = true;
        break;
      case 'v':
        verbose = true;
        break;	
      case 'h':
        fprintf(stderr, "Usage: %s -s supertable -t thread -FirstDescribeSTable -Verbose\n", argv[0]);
        exit(0);
      default:
        fprintf(stderr, "Usage: %s -s supertable -t thread -FirstDescribeSTable -Verbose\n", argv[0]);
        exit(-1);
    }
  }


  // connect to server
  TAOS *taos = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }
  executeSql(taos, "drop database if exists sqlsml");
  executeSql(taos, "create database sqlsml");
  executeSql(taos, "use sqlsml");
  pthread_t* tids = calloc(numThreads, sizeof(pthread_t));
  SThreadArgs* threadArgs = calloc(numThreads, sizeof(SThreadArgs));
  for (int i = 0; i < numThreads; ++i) {
    threadArgs[i].numSTables = numSTables;
    threadArgs[i].numThreads = numThreads;
    threadArgs[i].threadId = i;
    threadArgs[i].taos = taos;
  }

  int64_t begin = taosGetTimestampUs();
  for (int i = 0; i < numThreads; ++i) {
    pthread_create(tids+i, NULL, threadFunc, threadArgs+i);
  }
  for (int i = 0; i < numThreads; ++i) {
    pthread_join(tids[i], NULL);
  }
  int64_t end = taosGetTimestampUs();
  printf("TIME: %d(ms)\n", (int)((end-begin)/1000));
  printf("THROUGHPUT: %d\n", (int)((numSTables * 1e6) / (end-begin)));
  free(threadArgs);
  free(tids);
  taos_close(taos);
  taos_cleanup();
}
