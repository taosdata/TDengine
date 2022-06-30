#include "os.h"
#include "taos.h"
#include "taoserror.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

bool verbose = false;


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
  int protocol;
  int numBatches;
  SThreadLinesBatch *batches;
  int64_t costTime;
} SThreadInsertArgs;

static void* insertLines(void* args) {
  SThreadInsertArgs* insertArgs = (SThreadInsertArgs*) args;
  char tidBuf[32] = {0};
  printThreadId(pthread_self(), tidBuf);
  for (int i = 0; i < insertArgs->numBatches; ++i) {
    SThreadLinesBatch* batch = insertArgs->batches + i;
    if (verbose) printf("%s, thread: 0x%s\n", "begin taos_insert_lines", tidBuf);
    int64_t begin = getTimeInUs();
    //int32_t code = taos_insert_lines(insertArgs->taos, batch->lines, batch->numLines);
    TAOS_RES * res = taos_schemaless_insert(insertArgs->taos, batch->lines, batch->numLines, insertArgs->protocol, TSDB_SML_TIMESTAMP_MILLI_SECONDS);
    int32_t code = taos_errno(res);
    int64_t end = getTimeInUs();
    insertArgs->costTime += end - begin;
    if (verbose) printf("code: %d, %s. time used:%"PRId64", thread: 0x%s\n", code, tstrerror(code), end - begin, tidBuf);
  }
  return NULL;
}

int32_t getTelenetTemplate(char* lineTemplate, int templateLen) {
  char* sample = "sta%d %lld 44.3 t0=False t1=127i8 t2=32 t3=%di32 t4=9223372036854775807i64 t5=11.12345f32 t6=22.123456789f64 t7=\"hpxzrdiw\" t8=\"ncharTagValue\" t9=127i8";
  snprintf(lineTemplate, templateLen, "%s", sample);
  return 0;
}

int32_t getLineTemplate(char* lineTemplate, int templateLen, int numFields) {
  if (numFields <= 4) {
    char* sample = "sta%d,t3=%di32 c3=2147483647i32,c4=9223372036854775807i64,c9=11.12345f32,c10=22.123456789f64 %lld";
    snprintf(lineTemplate, templateLen, "%s", sample);
    return 0;
  }

  if (numFields <= 13) {
     char* sample = "sta%d,t0=true,t1=127i8,t2=32767i16,t3=%di32,t4=9223372036854775807i64,t9=11.12345f32,t10=22.123456789f64,t11=\"binaryTagValue\",t12=L\"ncharTagValue\" c0=true,c1=127i8,c2=32767i16,c3=2147483647i32,c4=9223372036854775807i64,c5=254u8,c6=32770u16,c7=2147483699u32,c8=9223372036854775899u64,c9=11.12345f32,c10=22.123456789f64,c11=\"binaryValue\",c12=L\"ncharValue\" %lld";
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
  char* lineFormatTs = " %lld";
  snprintf(lineTemplate+strlen(lineTemplate)-1, templateLen-strlen(lineTemplate)+1, "%s", lineFormatTs);

  return 0;
}

int32_t generateLine(char* line, int lineLen, char* lineTemplate, int protocol, int superTable, int childTable, int64_t ts) {
  if (protocol == TSDB_SML_LINE_PROTOCOL) {
    snprintf(line, lineLen, lineTemplate, superTable, childTable, ts);               
  } else if (protocol == TSDB_SML_TELNET_PROTOCOL) {
    snprintf(line, lineLen, lineTemplate, superTable, ts, childTable);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t setupSuperTables(TAOS* taos, char* lineTemplate, int protocol,
                         int numSuperTables, int numChildTables, int numRowsPerChildTable,
                         int maxBatchesPerThread, int64_t ts) {
  printf("setup supertables...");
  {
    char** linesStb = calloc(numSuperTables, sizeof(char*));
    for (int i = 0; i < numSuperTables; i++) {
      char* lineStb = calloc(strlen(lineTemplate)+128, 1);
      generateLine(lineStb, strlen(lineTemplate)+128, lineTemplate, protocol, i,
                   numSuperTables * numChildTables,
                   ts + numSuperTables * numChildTables * numRowsPerChildTable);
      linesStb[i] = lineStb;
    }
    SThreadInsertArgs args = {0};
    args.protocol = protocol;
    args.batches = calloc(maxBatchesPerThread, sizeof(maxBatchesPerThread));
    args.taos = taos;
    args.batches[0].lines = linesStb;
    args.batches[0].numLines = numSuperTables;
    insertLines(&args);
    free(args.batches);
    for (int i = 0; i < numSuperTables; ++i) {
      free(linesStb[i]);
    }
    free(linesStb);
  }
  return TSDB_CODE_SUCCESS;
}

int main(int argc, char* argv[]) {
  int numThreads = 8;
  int maxBatchesPerThread = 1024;	

  int numSuperTables = 1;
  int numChildTables = 256;
  int numRowsPerChildTable = 8192;
  int numFields = 13;

  int maxLinesPerBatch = 16384;

  int protocol = TSDB_SML_TELNET_PROTOCOL;
  int assembleSTables = 0;

  int opt;
  while ((opt = getopt(argc, argv, "s:c:r:f:t:b:p:w:a:hv")) != -1) {
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
      case 'b':
        maxLinesPerBatch = atoi(optarg);
        break;
      case 'v':
        verbose = true;
        break;
      case 'a':
        assembleSTables = atoi(optarg);
        break;
      case 'p':
        if (optarg[0] == 't') {
          protocol = TSDB_SML_TELNET_PROTOCOL;
        } else if (optarg[0] == 'l') {
          protocol = TSDB_SML_LINE_PROTOCOL;
        } else if (optarg[0] == 'j') {
          protocol = TSDB_SML_JSON_PROTOCOL;
        }
        break;
      case 'h':
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -b maxlines_per_batch -p [t|l|j] -a assemble-stables -v\n",
                argv[0]);
        exit(0);
      default: /* '?' */
        fprintf(stderr, "Usage: %s -s supertable -c childtable -r rows -f fields -t threads -b maxlines_per_batch -p [t|l|j] -a assemble-stables -v\n",
                argv[0]);
        exit(-1);
    }
  }

  TAOS_RES*   result;
  //const char* host = "127.0.0.1";
  const char* host = NULL;
  const char* user = "root";
  const char* passwd = "taosdata";

  taos_options(TSDB_OPTION_TIMEZONE, "GMT-8");
  TAOS* taos = taos_connect(host, user, passwd, "", 0);
  if (taos == NULL) {
    printf("\033[31mfailed to connect to db, reason:%s\033[0m\n", taos_errstr(taos));
    exit(1);
  }

  maxBatchesPerThread = (numSuperTables*numChildTables*numRowsPerChildTable)/(numThreads * maxLinesPerBatch) + 1;

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
  int64_t ts = ct * 1000 ;

  char* lineTemplate = calloc(65536, sizeof(char));
  if (protocol == TSDB_SML_LINE_PROTOCOL) {
    getLineTemplate(lineTemplate, 65535, numFields);
  } else if (protocol == TSDB_SML_TELNET_PROTOCOL ) {
    getTelenetTemplate(lineTemplate, 65535);
  }

  if (assembleSTables) {
    setupSuperTables(taos, lineTemplate, protocol,
                     numSuperTables, numChildTables, numRowsPerChildTable, maxBatchesPerThread, ts);
  }

  printf("generate lines...\n");
  pthread_t* tids = calloc(numThreads, sizeof(pthread_t));
  SThreadInsertArgs* argsThread = calloc(numThreads, sizeof(SThreadInsertArgs));
  for (int i = 0; i < numThreads; ++i) {
    argsThread[i].batches = calloc(maxBatchesPerThread, sizeof(SThreadLinesBatch));	  
    argsThread[i].taos = taos;
    argsThread[i].numBatches = 0;
    argsThread[i].protocol = protocol;
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
        generateLine(line, strlen(lineTemplate)+128, lineTemplate, protocol, stIdx, ctIdx, ts + l);
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

  for (int i = 0; i < numThreads; i++) {
    free(argsThread[i].batches);
  }    
  free(argsThread);
  free(tids);

  free(lineTemplate);
  taos_close(taos);
  return 0;
}
