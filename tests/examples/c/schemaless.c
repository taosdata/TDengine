#include "os.h"
#include "taos.h"
#include "taoserror.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define MAX_THREAD_LINE_BATCHES 1024

void printThreadId(pthread_t id, char* buf)
{
  size_t i;
  for (i = sizeof(i); i; --i)
    sprintf(buf + strlen(buf), "%02x", *(((unsigned char*) &id) + i - 1));
}

static int64_t getTimeInUs() {
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  return (int64_t)systemTime.tv_sec * 1000000L + (int64_t)systemTime.tv_usec;
}

typedef struct {
  char** lines;
  int numLines;
} SThreadLinesBatch;

typedef struct  {
  TAOS* taos;
  int numBatches;
  SThreadLinesBatch batches[MAX_THREAD_LINE_BATCHES];
  int64_t costTime;
} SThreadInsertArgs;

static void* insertLines(void* args) {
  SThreadInsertArgs* insertArgs = (SThreadInsertArgs*) args;
  char tidBuf[32] = {0};
  printThreadId(pthread_self(), tidBuf);
  for (int i = 0; i < insertArgs->numBatches; ++i) {
    SThreadLinesBatch* batch = insertArgs->batches + i;
    printf("%s, thread: 0x%s\n", "begin taos_insert_lines", tidBuf);
    int64_t begin = getTimeInUs();
    int32_t code = taos_insert_lines(insertArgs->taos, batch->lines, batch->numLines);
    int64_t end = getTimeInUs();
    insertArgs->costTime += end - begin;
    printf("code: %d, %s. time used:%"PRId64", thread: 0x%s\n", code, tstrerror(code), end - begin, tidBuf);
  }
  return NULL;
}

int32_t getLineTemplate(char* lineTemplate, int templateLen, int numFields) {
  if (numFields <= 4) {
    char* sample = "sta%d,t3=%di32 c3=2147483647i32,c4=9223372036854775807i64,c9=11.12345f32,c10=22.123456789f64 %lldms";
    snprintf(lineTemplate, templateLen, "%s", sample);
    return 0;
  }

  if (numFields <= 13) {
     char* sample = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32,t4=9223372036854775807i64,t9=11.12345f32,t10=22.123456789f64,t11=\"binaryTagValue\",t12=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=254u8,c6=32770u16,c7=2147483699u32,c8=9223372036854775899u64,c9=11.12345f32,c10=22.123456789f64,c11=\"binaryValue\",c12=L\"ncharValue\" %lldms";
     snprintf(lineTemplate, templateLen, "%s", sample);
     return 0;
  }

  char* lineFormatTable = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32 ";
  snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "%s", lineFormatTable);

  int offset[] = {numFields*2/5, numFields*4/5, numFields};

  for (int i = 0; i < offset[0]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%di32,", i, i);
  }

  for (int i=offset[0]+1; i < offset[1]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=%d.43f64,", i, i);
  }

  for (int i = offset[1]+1; i < offset[2]; ++i) {
    snprintf(lineTemplate+strlen(lineTemplate), templateLen-strlen(lineTemplate), "c%d=\"%d\",", i, i);
  }
  char* lineFormatTs = " %lldms";
  snprintf(lineTemplate+strlen(lineTemplate)-1, templateLen-strlen(lineTemplate)+1, "%s", lineFormatTs);

  return 0;
}

int main(int argc, char* argv[]) {
  int numThreads = 8;

  int numSuperTables = 1;
  int numChildTables = 256;
  int numRowsPerChildTable = 8192;
  int numFields = 13;

  int maxLinesPerBatch = 16384;

  int opt;
  while ((opt = getopt(argc, argv, "s:c:r:f:t:m:h")) != -1) {
    switch (opt) {
      case 's':
        numSuperTables = atoi(optarg);
        break;
      case 'c':
        numChildTables = atoi(optarg);
        break;
      case 'r':
        numRowsPerChildTable = atoi(optarg);
        break;
      case 'f':
        numFields = atoi(optarg);
        break;
      case 't':
        numThreads = atoi(optarg);
        break;
      case 'm':
        maxLinesPerBatch = atoi(optarg);
        break;
      case 'h':
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -m maxlines_per_batch\n",
                argv[0]);
        exit(0);
      default: /* '?' */
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -m maxlines_per_batch\n",
                argv[0]);
        exit(-1);
    }
  }

  TAOS_RES*   result;
  const char* host = "127.0.0.1";
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  if (numThreads * MAX_THREAD_LINE_BATCHES* maxLinesPerBatch < numSuperTables*numChildTables*numRowsPerChildTable) {
    printf("too many rows to be handle by threads with %d batches", MAX_THREAD_LINE_BATCHES);
    exit(2);
  }

  char* info = taos_get_server_info(taos);
  printf("server info: %s\n", info);
  info = taos_get_client_info(taos);
  printf("client info: %s\n", info);
  result = taos_query(taos, "drop database if exists db;");
  taos_free_result(result);
  usleep(100000);
  result = taos_query(taos, "create database db precision 'us';");
  taos_free_result(result);
  usleep(100000);

  (void)taos_select_db(taos, "db");

  time_t  ct = time(0);
  int64_t ts = ct * 1000;

  char* lineTemplate = calloc(65536, sizeof(char));
  getLineTemplate(lineTemplate, 65535, numFields);

  printf("setup supertables...");
  {
    char** linesStb = calloc(numSuperTables, sizeof(char*));
    for (int i = 0; i < numSuperTables; i++) {
      char* lineStb = calloc(strlen(lineTemplate)+128, 1);
      snprintf(lineStb, strlen(lineTemplate)+128, lineTemplate, i,
               numSuperTables * numChildTables,
               ts + numSuperTables * numChildTables * numRowsPerChildTable);
      linesStb[i] = lineStb;
    }
    SThreadInsertArgs args = {0};
    args.taos = taos;
    args.batches[0].lines = linesStb;
    args.batches[0].numLines = numSuperTables;
    insertLines(&args);
    for (int i = 0; i < numSuperTables; ++i) {
      free(linesStb[i]);
    }
    free(linesStb);
  }

  printf("generate lines...\n");
  pthread_t* tids = calloc(numThreads, sizeof(pthread_t));
  SThreadInsertArgs* argsThread = calloc(numThreads, sizeof(SThreadInsertArgs));
  for (int i = 0; i < numThreads; ++i) {
    argsThread[i].taos = taos;
    argsThread[i].numBatches = 0;
  }

  int64_t totalLines = numSuperTables * numChildTables * numRowsPerChildTable;
  int totalBatches = (int) ((totalLines) / maxLinesPerBatch);
  if (totalLines % maxLinesPerBatch != 0) {
    totalBatches += 1;
  }

  char*** allBatches = calloc(totalBatches, sizeof(char**));
  for (int i = 0; i < totalBatches; ++i) {
    allBatches[i] = calloc(maxLinesPerBatch, sizeof(char*));
    int threadNo = i % numThreads;
    int batchNo = i / numThreads;
    argsThread[threadNo].batches[batchNo].lines = allBatches[i];
    argsThread[threadNo].numBatches = batchNo + 1;
  }

  int l = 0;
  for (int i = 0; i < numSuperTables; ++i) {
    for (int j = 0; j < numChildTables; ++j) {
      for (int k = 0; k < numRowsPerChildTable; ++k) {
        int stIdx = i;
        int ctIdx = numSuperTables*numChildTables + j;
        char* line = calloc(strlen(lineTemplate)+128, 1);
        snprintf(line, strlen(lineTemplate)+128, lineTemplate, stIdx, ctIdx, ts + l);
        int batchNo = l / maxLinesPerBatch;
        int lineNo = l % maxLinesPerBatch;
        allBatches[batchNo][lineNo] =  line;
        argsThread[batchNo % numThreads].batches[batchNo/numThreads].numLines = lineNo + 1;
        ++l;
      }
    }
  }

  printf("begin multi-thread insertion...\n");
  int64_t begin = taosGetTimestampUs();

  for (int i=0; i < numThreads; ++i) {
    pthread_create(tids+i, NULL, insertLines, argsThread+i);
  }
  for (int i = 0; i < numThreads; ++i) {
    pthread_join(tids[i], NULL);
  }
  int64_t end = taosGetTimestampUs();

  size_t linesNum = numSuperTables*numChildTables*numRowsPerChildTable;
  printf("TOTAL LINES: %zu\n", linesNum);
  printf("THREADS: %d\n", numThreads);
  printf("TIME: %d(ms)\n", (int)(end-begin)/1000);
  double throughput = (double)(totalLines)/(double)(end-begin) * 1000000;
  printf("THROUGHPUT:%d/s\n", (int)throughput);

  for (int i = 0; i < totalBatches; ++i) {
    free(allBatches[i]);
  }
  free(allBatches);

  free(argsThread);
  free(tids);

  free(lineTemplate);
  taos_close(taos);
  return 0;
}
