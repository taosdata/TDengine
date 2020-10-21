#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <taos.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <inttypes.h>

typedef struct {
  char sql[256];
  char dataDir[256];
  int filesNum;
  int clients;
  int rowsPerRequest;
  int write;
} ProArgs;

typedef struct {
  int64_t totalRows;
} StatisInfo;

typedef struct {
  pthread_t pid;
  int threadId;
  int sID;
  int eID;
} ThreadObj;

static StatisInfo statis;
static ProArgs arguments;

void parseArg(int argc, char *argv[]);

void writeData();

void readData();

int main(int argc, char *argv[]) {
  statis.totalRows = 0;
  parseArg(argc, argv);

  if (arguments.write) {
    writeData();
  } else {
    readData();
  }
}

void parseArg(int argc, char *argv[]) {
  strcpy(arguments.sql, "./sqlCmd.txt");
  strcpy(arguments.dataDir, "./testdata");
  arguments.filesNum = 2;
  arguments.clients = 1;
  arguments.rowsPerRequest = 100;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-sql") == 0) {
      if (i < argc - 1) {
        strcpy(arguments.sql, argv[++i]);
      }
      else {
        fprintf(stderr, "'-sql' requires a parameter, default:%s\n", arguments.sql);
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-dataDir") == 0) {
      if (i < argc - 1) {
        strcpy(arguments.dataDir, argv[++i]);
      }
      else {
        fprintf(stderr, "'-dataDir' requires a parameter, default:%s\n", arguments.dataDir);
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-numOfFiles") == 0) {
      if (i < argc - 1) {
        arguments.filesNum = atoi(argv[++i]);
      }
      else {
        fprintf(stderr, "'-numOfFiles' requires a parameter, default:%d\n", arguments.filesNum);
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-clients") == 0) {
      if (i < argc - 1) {
        arguments.clients = atoi(argv[++i]);
      }
      else {
        fprintf(stderr, "'-clients' requires a parameter, default:%d\n", arguments.clients);
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-rowsPerRequest") == 0) {
      if (i < argc - 1) {
        arguments.rowsPerRequest = atoi(argv[++i]);
      }
      else {
        fprintf(stderr, "'-rowsPerRequest' requires a parameter, default:%d\n", arguments.rowsPerRequest);
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-w") == 0) {
      arguments.write = 1;
    }
  }
}

static void taos_error(TAOS_RES *tres, TAOS *conn) {
  printf("TDengine error: %s\n", tres?taos_errstr(tres):"null result");
  taos_close(conn);
  exit(1);
}

int64_t getTimeStampMs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000L + (int64_t)systemTime.tv_usec / 1000;
}

void writeDataImp(void *param) {
  ThreadObj *pThread = (ThreadObj *)param;
  printf("Thread %d, writing sID %d, eID %d\n", pThread->threadId, pThread->sID, pThread->eID);

  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    // where to find errstr?
    // taos_error(NULL, taos);
    printf("TDengine error: %s\n", "failed to connect");
    exit(1);
  }

  TAOS_RES* result = taos_query(taos, "use db");
  int32_t code = taos_errno(result);
  if (code != 0) {
    taos_error(result, taos);
  }
  taos_free_result(result);

  char *sql = calloc(1, 8*1024*1024);
  int sqlLen = 0;
  int lastMachineid = 0;
  int counter = 0;
  int totalRecords = 0;

  for (int j = pThread->sID; j <= pThread->eID; j++) {
    char fileName[300];
    sprintf(fileName, "%s/testdata%d.csv", arguments.dataDir, j);

    FILE *fp = fopen(fileName, "r");
    if (fp == NULL) {
      printf("failed to open file %s\n", fileName);
      exit(1);
    }
    printf("open file %s success\n", fileName);

    char *line = NULL;
    size_t len = 0;
    while (!feof(fp)) {
      free(line);
      line = NULL;
      len = 0;

      getline(&line, &len, fp);
      if (line == NULL) break;

      if (strlen(line) < 10) continue;
      int machineid;
      char machinename[16];
      int machinegroup;
      int64_t timestamp;
      int temperature;
      float humidity;
      sscanf(line, "%d%s%d%" PRId64 "%d%f", &machineid, machinename, &machinegroup, &timestamp, &temperature, &humidity);

      if (counter == 0) {
        sqlLen = sprintf(sql, "insert into");
      }

      if (lastMachineid != machineid) {
        lastMachineid = machineid;
        sqlLen += sprintf(sql + sqlLen, " dev%d using devices tags(%d,'%s',%d) values",
                          machineid, machineid, machinename, machinegroup);
      }

      sqlLen += sprintf(sql + sqlLen, "(%" PRId64 ",%d,%f)", timestamp, temperature, humidity);
      counter++;

      if (counter >= arguments.rowsPerRequest) {
        TAOS_RES *result = taos_query(taos, sql);
        int32_t   code = taos_errno(result);
        if (code != 0) {
          printf("thread:%d error:%d reason:%s\n", pThread->threadId, code, taos_errstr(taos));
        }
        taos_free_result(result);

        totalRecords += counter;
        counter = 0;
        lastMachineid = -1;
        sqlLen = 0;
      }
    }

    fclose(fp);
  }

  if (counter > 0) {
    TAOS_RES *result = taos_query(taos, sql);
    int32_t   code = taos_errno(result);
    if (code != 0) {
      printf("thread:%d error:%d reason:%s\n", pThread->threadId, code, taos_errstr(taos));
    }
    taos_free_result(result);

    totalRecords += counter;
  }

  __sync_fetch_and_add(&statis.totalRows, totalRecords);
  free(sql);
}

void writeData() {
  printf("write data\n");
  printf("---- clients: %d\n", arguments.clients);
  printf("---- dataDir: %s\n", arguments.dataDir);
  printf("---- numOfFiles: %d\n", arguments.filesNum);
  printf("---- rowsPerRequest: %d\n", arguments.rowsPerRequest);

  taos_init();

  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    // where to find errstr?
    // taos_error(NULL, taos);
    printf("TDengine error: %s\n", "failed to connect");
    exit(1);
  }

  TAOS_RES *result = taos_query(taos, "create database if not exists db");
  int32_t   code = taos_errno(result);
  if (code != 0) {
    taos_error(result, taos);
  }
  taos_free_result(result);

  result = taos_query(taos,
                      "create table if not exists db.devices(ts timestamp, temperature int, humidity float) "
                      "tags(devid int, devname binary(16), devgroup int)");
  code = taos_errno(result);
  if (code != 0) {
    taos_error(result, taos);
  }
  taos_free_result(result);

  int64_t st = getTimeStampMs();

  int a = arguments.filesNum / arguments.clients;
  int b = arguments.filesNum % arguments.clients;
  int last = 0;

  ThreadObj *threads = calloc((size_t)arguments.clients, sizeof(ThreadObj));
  for (int i = 0; i < arguments.clients; ++i) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->threadId = i + 1;
    pthread->sID = last;
    if (i < b) {
      pthread->eID = last + a;
    } else {
      pthread->eID = last + a - 1;
    }
    last = pthread->eID + 1;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, (void *(*)(void *))writeDataImp, pthread);
  }

  for (int i = 0; i < arguments.clients; i++) {
    pthread_join(threads[i].pid, NULL);
  }

  int64_t elapsed = getTimeStampMs() - st;
  float seconds = (float)elapsed / 1000;
  float rs = (float)statis.totalRows / seconds;

  free(threads);

  printf("---- Spent %f seconds to insert %" PRId64 " records, speed: %f Rows/Second\n", seconds, statis.totalRows, rs);
}

void readDataImp(void *param)
{
  ThreadObj *pThread = (ThreadObj *)param;
  printf("Thread %d\n", pThread->threadId);
  FILE *fp = fopen(arguments.sql, "r");
  if (fp == NULL) {
    printf("failed to open file %s\n", arguments.sql);
    exit(1);
  }
  printf("open file %s success\n", arguments.sql);

  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    // where to find errstr?
    // taos_error(NULL, taos);
    printf("TDengine error: %s\n", "failed to connect");
    exit(1);
  }

  char *line = NULL;
  size_t len = 0;
  while (!feof(fp)) {
    free(line);
    line = NULL;
    len = 0;

    getline(&line, &len, fp);
    if (line == NULL) break;

    if (strlen(line) < 10) continue;

    int64_t st = getTimeStampMs();

    TAOS_RES *result = taos_query(taos, line);
    int32_t   code = taos_errno(result);
    if (code != 0) {
      taos_error(result, taos);
    }

    TAOS_ROW row;
    int rows = 0;
    //int num_fields = taos_field_count(taos);
    //TAOS_FIELD *fields = taos_fetch_fields(result);
    while ((row = taos_fetch_row(result))) {
      rows++;
      //char        temp[256];
      //taos_print_row(temp, row, fields, num_fields);
      //printf("%s\n", temp);
    }

    taos_free_result(result);

    int64_t elapsed = getTimeStampMs() - st;
    float seconds = (float)elapsed / 1000;
    printf("---- Spent %f seconds to retrieve %d records, Thread:%d query: %s\n", seconds, rows, pThread->threadId, line);
  }

  fclose(fp);
}

void readData() {
  printf("read data\n");
  printf("---- sql: %s\n", arguments.sql);
  printf("---- clients: %d\n", arguments.clients);

  void *taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    // where to find errstr?
    // taos_error(NULL, taos);
    printf("TDengine error: %s\n", "failed to connect");
    exit(1);
  }

  ThreadObj *threads = calloc((size_t)arguments.clients, sizeof(ThreadObj));

  for (int i = 0; i < arguments.clients; ++i) {
    ThreadObj *pthread = threads + i;
    pthread_attr_t thattr;
    pthread->threadId = i + 1;
    pthread_attr_init(&thattr);
    pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&pthread->pid, &thattr, (void *(*)(void *))readDataImp, pthread);
  }

  for (int i = 0; i < arguments.clients; i++) {
    pthread_join(threads[i].pid, NULL);
  }

  free(threads);
}

