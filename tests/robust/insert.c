#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
#include <pthread.h>
#include <sys/time.h>

typedef struct {
  int client;
  int stable;
  int table;
  unsigned interval;
  int insert;
  int create;
} ProArgs;

typedef struct {
  pthread_t pid;
  int threadId;
  void* taos;
} ThreadObj;

static ProArgs arguments;
void parseArg(int argc, char *argv[]);
void create();
void createImp(void *param);
void insert();
void insertImp(void *param);


int64_t getTimeStampMs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec / 1000;
}

void parseArg(int argc, char *argv[]) {
  arguments.stable = 100000;
  arguments.table = 50;
  arguments.client = 16;
  arguments.interval = 1;
  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-stable") == 0) {
      if (i < argc - 1) {
        arguments.stable = atoi(argv[++i]);
      } else {
        fprintf(stderr, "'-stable' requires a parameter, default:%d\n", arguments.stable);
      }
    } else if (strcmp(argv[i], "-table") == 0) {
        if (i < argc - 1) {
          arguments.table = atoi(argv[++i]);
        } else {
          fprintf(stderr, "'-table' requires a parameter, default:%d\n", arguments.table);
        }
    } else if (strcmp(argv[i], "-client") == 0) {
        if (i < argc - 1) {
          arguments.client = atoi(argv[++i]);
        } else {
          fprintf(stderr, "'-client' requires a parameter, default:%d\n", arguments.client);
        }
    } else if (strcmp(argv[i], "-insert") == 0) {
      arguments.insert = 1;
    } else if (strcmp(argv[i], "-create") == 0) {
      arguments.create = 1;
    }
  }
}

void create() {
  int64_t st = getTimeStampMs();
  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("TDengine error: failed to connect\n");
    exit(EXIT_FAILURE);
  }
  TAOS_RES *result = taos_query(taos, "drop database if exists db");
  if (result == NULL || taos_errno(result) != 0) {
    printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);
  int64_t elapsed = getTimeStampMs() - st;
  printf("--- spend %ld ms to drop database db\n", elapsed);
  st = getTimeStampMs();
  result = taos_query(taos, "create database if not exists db");
  if (result == NULL || taos_errno(result) != 0) {
    printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
    taos_free_result(result);
    exit(EXIT_FAILURE);
  }
  taos_free_result(result);
  elapsed = getTimeStampMs() - st;
  printf("--- spend %ld ms to create database db\n", elapsed);
  st = getTimeStampMs();
  ThreadObj *threads = calloc((size_t)arguments.client, sizeof(ThreadObj));
  for (int i = 0; i < arguments.client; i++) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->threadId = i + 1;
    pthread->taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, (void *(*)(void *))createImp, pthread);
  }
  for (int i = 0; i < arguments.client; i++) {
    pthread_join(threads[i].pid, NULL);
  }
  elapsed = getTimeStampMs() - st;
  printf("--- Spend %ld ms to create %d super tables and each %d tables\n", elapsed, arguments.stable, arguments.table);
  free(threads);
}

void createImp(void *param) {
  char command[256] = "\0";
  int sqlLen = 0;
  int count = 0;
  char *sql = calloc(1, 1024*1024);
  ThreadObj *pThread = (ThreadObj *)param;
  printf("Thread %d started create super table s%d to s%d\n", pThread->threadId, (pThread->threadId - 1) * arguments.stable / arguments.client, pThread->threadId * arguments.stable / arguments.client - 1);
  for (int i = (pThread->threadId - 1) * arguments.stable / arguments.client; i < pThread->threadId * arguments.stable / arguments.client; i++) {
    sprintf(command, "create table if not exists db.s%d(ts timestamp, c1 int, c2 int, c3 int) TAGS (id int)", i);
    TAOS_RES *result = taos_query(pThread->taos, command);
    if (result == NULL || taos_errno(result) != 0) {
      printf("In line:%d, failed to execute sql:%s, reason:%s\n", __LINE__, command, taos_errstr(result));
      taos_free_result(result);
      return;
    }
    taos_free_result(result);
    result = taos_query(pThread->taos, "use db");
    if (result == NULL || taos_errno(result) != 0) {
      printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
      taos_free_result(result);
      return;
    }
    taos_free_result(result);
    sqlLen = sprintf(sql, "create table if not exists ");
    count = 0;
    for (int j = 0; j < arguments.table; j++) {
      sqlLen += sprintf(sql + sqlLen, " s%d_%d USING s%d TAGS (%d)", i, j, i, j);
      if ((j + 1) % arguments.table == 0) {
        result = taos_query(pThread->taos, sql);
        if (result == NULL || taos_errno(result) != 0) {
          printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
          taos_free_result(result);
          return;
        }
        taos_free_result(result);
        printf("Thread %d created table s%d_%d to s%d_%d\n", pThread->threadId, i, count, i, j);
        count = j;
        sqlLen = sprintf(sql, "create table if not exists ");
      }
    }
  }
}

void insert() {
  ThreadObj *threads = calloc((size_t)arguments.client, sizeof(ThreadObj));
  for (int i = 0; i < arguments.client; i++) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->threadId = i + 1;
    pthread->taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, (void *(*)(void *))insertImp, pthread);
  }
  for (int i = 0; i < arguments.client; i++) {
    pthread_join(threads[i].pid, NULL);
  }
  free(threads);
}

void insertImp(void *param) {
  int count = 0;
  ThreadObj *pThread = (ThreadObj *)param;
  printf("Thread %d started insert data\n", pThread->threadId);
  int sqlLen = 0;
  char *sql = calloc(1, 1024*1024);
  TAOS_RES *result = taos_query(pThread->taos, "use db");
  if (result == NULL || taos_errno(result) != 0) {
    printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
  int64_t time = getTimeStampMs();
  while (true) {
    sqlLen = sprintf(sql, "insert into");
    for (int i = (pThread->threadId - 1) * arguments.stable / arguments.client; i < pThread->threadId * arguments.stable / arguments.client; i++) {
      for (int j = 0; j < arguments.table; j++) {
        sqlLen += sprintf(sql + sqlLen, " s%d_%d values (%ld, %d, %d, %d)", i, j, time, rand(), rand(), rand());
        count++;
        if ( (1048576 - sqlLen) < 49151 || i == (pThread->threadId * arguments.stable / arguments.client - 1)) {
          // printf("sql: %s\n",sql);
          result = taos_query(pThread->taos, sql);
          printf("Thread %d already insert %d rows\n", pThread->threadId, count);
          if (result == NULL || taos_errno(result) != 0) {
            printf("In line:%d, failed to execute sql, reason:%s\n", __LINE__, taos_errstr(result));
            taos_free_result(result);
            return;
          }
          taos_free_result(result);
          sqlLen = sprintf(sql, "insert into");
        }
      }
    }
    time += 1000*arguments.interval;
  }
}

int main(int argc, char *argv[]) {
  parseArg(argc, argv);
  if (arguments.insert) {
    insert();
  } else if (arguments.create) {
    create();
  } else {
    printf("select one mode: 1) -create or 2) -insert\n");
  }
}

