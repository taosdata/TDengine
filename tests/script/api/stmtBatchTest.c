// TAOS standard API example. The same syntax as MySQL, but only a subet 
// to compile: gcc -o prepare prepare.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>
#include "../../../include/client/taos.h"
#include "taoserror.h"

#define    MAX_ROWS_OF_PER_COLUMN   32770
#define    MAX_BINARY_DEF_LEN       (1024*16)

typedef struct {
    int64_t *ts;
    int8_t    b[MAX_ROWS_OF_PER_COLUMN];
    int8_t   v1[MAX_ROWS_OF_PER_COLUMN];
    int16_t  v2[MAX_ROWS_OF_PER_COLUMN];
    int32_t  v4[MAX_ROWS_OF_PER_COLUMN];
    int64_t  v8[MAX_ROWS_OF_PER_COLUMN];
    float    f4[MAX_ROWS_OF_PER_COLUMN];
    double   f8[MAX_ROWS_OF_PER_COLUMN];
    //char     br[MAX_ROWS_OF_PER_COLUMN][MAX_BINARY_DEF_LEN];      
    //char     nr[MAX_ROWS_OF_PER_COLUMN][MAX_BINARY_DEF_LEN];
    char     *br;      
    char     *nr;
    int64_t ts2[MAX_ROWS_OF_PER_COLUMN];
} sampleValue;


typedef struct {
  TAOS   *taos;
  int     idx;
} ThreadInfo;

//void taosMsleep(int mseconds);

int g_runTimes = 5;


unsigned long long getCurrentTime(){
    struct timeval tv;
    if (taosGetTimeOfDay(&tv) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);
    }

    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}

static int stmt_bind_case_001(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


static int stmt_bind_case_002(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }
  }

  code = taos_stmt_execute(stmt);
  if (code != 0) {
    printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


static int stmt_bind_case_003(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;

    params[i+1].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+1].buffer_length = sizeof(int32_t);
    params[i+1].buffer = v->v4;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static int stmt_bind_case_004(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum * 2));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * 2 * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum * 2; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum * 2; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
  
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }

      // ===================================start==============================================//
      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
      // ===================================end==============================================//

    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static int stmt_bind_error_case_001(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum * 2));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * 2 * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum * 2; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum * 2; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
  
      //------- add one batch ------//
      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      //----------------------------//      
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}


static int stmt_bind_error_case_002(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum * 2));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * 2 * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum * 2; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum * 2; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
  
      //code = taos_stmt_add_batch(stmt);
      //if (code != 0) {
      //  printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
      //  return -1;
      //}
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static int stmt_bind_error_case_003(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum * 2));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * 2 * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum * 2; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum * 2; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
    
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      //==================add one=================//
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  
      //==========================================//

      for (int col=0; col < columnNum; ++col) {

        //==================add one=================//
        if (1==col) {
          code = taos_stmt_set_tbname(stmt, buf);
          if (code != 0){
            printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
            return -1;
          }  
        }
        //==========================================//
      
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
  
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static void checkResult(TAOS     *taos, char *tname, int printr, int expected) {
  char sql[255] = "SELECT * FROM ";
  TAOS_RES *result;

  strcat(sql, tname);

  result = taos_query(taos, sql);
  int code = taos_errno(result);
  if (code != 0) {
    printf("failed to query table: %s, reason:%s\n", tname, taos_errstr(result));
    taos_free_result(result);
    return;
  }

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);
  char        temp[256];

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    rows++;
    if (printr) {
      memset(temp, 0, sizeof(temp));
      taos_print_row(temp, row, fields, num_fields);
      printf("[%s]\n", temp);
    }
  }
  
  if (rows == expected) {
    printf("%d rows are fetched as expectation from %s\n", rows, tname);
  } else {
    printf("!!!expect %d rows, but %d rows are fetched from %s\n", expected, rows, tname);
    return;
  }

  taos_free_result(result);

}


static void prepareV(TAOS     *taos, int schemaCase, int tableNum, int lenOfBinaryDef) {
  TAOS_RES *result;
  int      code;

  result = taos_query(taos, "drop database if exists demo"); 
  taos_free_result(result);

  result = taos_query(taos, "create database demo");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  result = taos_query(taos, "use demo");
  taos_free_result(result);
  
  // create table
  for (int i = 0 ; i < tableNum; i++) {
    char buf[1024];
    if (schemaCase) {
      sprintf(buf, "create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)", i, lenOfBinaryDef, lenOfBinaryDef) ;
    } else {
      sprintf(buf, "create table m%d (ts timestamp, b int)", i) ;
    }
    
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      return;
    }
    taos_free_result(result);
  }

}

static void prepareV_long(TAOS     *taos, int schemaCase, int tableNum, int lenOfBinaryDef) {
  TAOS_RES *result;
  int      code;

  result = taos_query(taos, "drop database if exists demol"); 
  taos_free_result(result);

  result = taos_query(taos, "create database demol");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  result = taos_query(taos, "use demol");
  taos_free_result(result);
  
  // create table
  for (int i = 0 ; i < tableNum; i++) {
    char buf[1024];
    if (schemaCase) {
      sprintf(buf, "create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)", i, lenOfBinaryDef, lenOfBinaryDef) ;
    } else {
      sprintf(buf, "create table m%d (ts timestamp, b int)", i) ;
    }
    
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      return;
    }
    taos_free_result(result);
  }

}


static void prepareVcolumn_autoCreateTbl(TAOS     *taos, int schemaCase, int tableNum, int lenOfBinaryDef, char* dbName) {
  TAOS_RES *result;
  int      code;
  char     sqlstr[1024] = {0};
  sprintf(sqlstr, "drop database if exists %s;", dbName);
  result = taos_query(taos, sqlstr); 
  taos_free_result(result);

  sprintf(sqlstr, "create database %s;", dbName);
  result = taos_query(taos, sqlstr);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  sprintf(sqlstr, "use %s;", dbName);
  result = taos_query(taos, sqlstr);
  taos_free_result(result);
  
  // create table
  char buf[1024] = {0};
  //if (bigsize) {
    sprintf(buf, "create stable stb1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)" 
                 " tags(id1 int, id2 bool, id3 tinyint, id4 smallint, id5 bigint, id6 float, id7 double, id8 binary(%d), id9 nchar(%d))", lenOfBinaryDef, lenOfBinaryDef, lenOfBinaryDef, lenOfBinaryDef) ;
  //} else {
  //  sprintf(buf, "create stable stb1 (ts timestamp, b int) tags(id1 int, id2 bool, id3 tinyint, id4 smallint, id5 bigint, id6 float, id7 double, id8 binary(40), id9 nchar(40))") ;
  //}
    
  result = taos_query(taos, buf);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create table, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}


static void prepareVcolumn(TAOS     *taos, int schemaCase, int tableNum, int lenOfBinaryDef, char* dbName) {
  TAOS_RES *result;
  int      code;
  char     sqlstr[1024] = {0};
  sprintf(sqlstr, "drop database if exists %s;", dbName);
  result = taos_query(taos, sqlstr); 
  taos_free_result(result);

  sprintf(sqlstr, "create database %s;", dbName);
  result = taos_query(taos, sqlstr);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);

  sprintf(sqlstr, "use %s;", dbName);
  result = taos_query(taos, sqlstr);
  taos_free_result(result);
  
  // create table
  for (int i = 0 ; i < tableNum; i++) {
    char buf[1024];
    if (schemaCase) {
      sprintf(buf, "create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)", i, lenOfBinaryDef, lenOfBinaryDef) ;
    } else {
      sprintf(buf, "create table m%d (ts timestamp, b int)", i) ;
    }
    
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      return;
    }
    taos_free_result(result);
  }

}

//void runcase(TAOS     *taos, int idx) {
static void runCase(TAOS *taos) {
  TAOS_STMT *stmt = NULL;
  
  int tableNum;
  int lenOfBinaryDef;
  int rowsOfPerColum;
  int bingNum;
  int lenOfBinaryAct;
  int columnNum;

  int totalRowsPerTbl;

//=======================================================================//
//=============================== single table ==========================//
//========== case 1: ======================//
#if 1
{
  stmt = taos_stmt_init(taos);

  tableNum = 1;
  rowsOfPerColum = 1;
  bingNum = 1;
  lenOfBinaryDef = 40;
  lenOfBinaryAct = 16;
  columnNum = 11;
  
  prepareV(taos, 1, tableNum, lenOfBinaryDef);
  stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

  totalRowsPerTbl = rowsOfPerColum * bingNum;
  checkResult(taos, "m0", 0, totalRowsPerTbl);
  taos_stmt_close(stmt);
  printf("case 1 check result end\n\n");
}
#endif  

  //========== case 2: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 1;
    rowsOfPerColum = 5;
    bingNum = 1;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 33;
    columnNum = 11;
    
    prepareV(taos, 1, tableNum, lenOfBinaryDef);
    stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 2 check result end\n\n");
}
#endif  

    //========== case 3: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 33;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 3 check result end\n\n");
  }
#endif  

      //========== case 4: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
      //checkResult(taos, "m1", 0, totalRowsPerTbl);
      //checkResult(taos, "m2", 0, totalRowsPerTbl);  
      //checkResult(taos, "m3", 0, totalRowsPerTbl);
      //checkResult(taos, "m4", 0, totalRowsPerTbl);
      //checkResult(taos, "m5", 0, totalRowsPerTbl);
      //checkResult(taos, "m6", 0, totalRowsPerTbl);  
      //checkResult(taos, "m7", 0, totalRowsPerTbl);
      //checkResult(taos, "m8", 0, totalRowsPerTbl);  
      //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 4 check result end\n\n");
    }
#endif  

//=======================================================================//
//=============================== multi tables ==========================//
  //========== case 5: ======================//
#if 1
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 5;
    rowsOfPerColum = 1;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 16;
    columnNum = 11;
    
    prepareV(taos, 1, tableNum, lenOfBinaryDef);
    stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 5 check result end\n\n");
  }
#endif  
  
    //========== case 6: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 5;
      rowsOfPerColum = 5;
      bingNum = 1;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 33;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      checkResult(taos, "m2", 0, totalRowsPerTbl);  
      checkResult(taos, "m3", 0, totalRowsPerTbl);
      checkResult(taos, "m4", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 6 check result end\n\n");
  }
#endif  
  
      //========== case 7: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 5;
        rowsOfPerColum = 1;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 7 check result end\n\n");
    }
#endif  
  
        //========== case 8: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 5;
    rowsOfPerColum = 5;
    bingNum = 5;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 33;
    columnNum = 11;
    
    prepareV(taos, 1, tableNum, lenOfBinaryDef);
    stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 8 check result end\n\n");
}
#endif  

  //=======================================================================//
  //=============================== multi-rows to single table ==========================//
  //========== case 9: ======================//
#if 1
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 1;
    rowsOfPerColum = 23740;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 16;
    columnNum = 11;
    
    prepareV(taos, 1, tableNum, lenOfBinaryDef);
    stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 9 check result end\n\n");
  }
#endif  

    //========== case 10: ======================//
#if 1
    {
      printf("====case 10 error test start\n");
      stmt = taos_stmt_init(taos);
    
      tableNum = 1;
      rowsOfPerColum = 23741;  // WAL size exceeds limit
      bingNum = 1;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 16;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("====case 10 check result end\n\n");
    }
#endif  

  
    //========== case 11: ======================//
#if 1
    {
      stmt = taos_stmt_init(taos);
    
      tableNum = 1;
      rowsOfPerColum = 32767;
      bingNum = 1;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 16;
      columnNum = 2;
      
      prepareV(taos, 0, tableNum, lenOfBinaryDef);
      stmt_bind_case_003(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 11 check result end\n\n");
    }
#endif  

      //========== case 12: ======================//
#if 1
      {
        printf("====case 12 error test start\n");
        stmt = taos_stmt_init(taos);
      
        tableNum = 1;
        rowsOfPerColum = 32768; // invalid parameter
        bingNum = 1;
        lenOfBinaryDef = 40;
        lenOfBinaryAct = 16;
        columnNum = 2;
        
        prepareV(taos, 0, tableNum, lenOfBinaryDef);
        stmt_bind_case_003(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
      
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("====case 12 check result end\n\n");
      }
#endif  

  //=======================================================================//
  //=============================== multi tables, multi bind one same table ==========================//
    //========== case 13: ======================//
#if 1
    {
      stmt = taos_stmt_init(taos);
    
      tableNum = 5;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 16;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      checkResult(taos, "m2", 0, totalRowsPerTbl);  
      checkResult(taos, "m3", 0, totalRowsPerTbl);
      checkResult(taos, "m4", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 13 check result end\n\n");
    }
#endif  
    
      //========== case 14: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 5;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 14 check result end\n\n");
    }
#endif  

    
  //========== case 15: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1000;
      rowsOfPerColum = 10;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 8;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m111", 0, totalRowsPerTbl);
      checkResult(taos, "m222", 0, totalRowsPerTbl);  
      checkResult(taos, "m333", 0, totalRowsPerTbl);
      checkResult(taos, "m999", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 15 check result end\n\n");
  }
#endif  

    //========== case 16: ======================//
#if 1
  {
      printf("====case 16 error test start\n");
      stmt = taos_stmt_init(taos);
      
      tableNum = 10;
      rowsOfPerColum = 10;
      bingNum = 1;
      lenOfBinaryDef = 100;
      lenOfBinaryAct = 8;
      columnNum = 11;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_bind_error_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      //checkResult(taos, "m11", 0, totalRowsPerTbl);
      //checkResult(taos, "m22", 0, totalRowsPerTbl);  
      //checkResult(taos, "m33", 0, totalRowsPerTbl);
      //checkResult(taos, "m99", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("====case 16 check result end\n\n");
  }
#endif  

    //========== case 17: ======================//
#if 1
    {
        //printf("case 17 test start\n");
        stmt = taos_stmt_init(taos);
        
        tableNum = 10;
        rowsOfPerColum = 10;
        bingNum = 1;
        lenOfBinaryDef = 100;
        lenOfBinaryAct = 8;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_case_004(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum * 2;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m11", 0, totalRowsPerTbl);
        //checkResult(taos, "m22", 0, totalRowsPerTbl);  
        //checkResult(taos, "m33", 0, totalRowsPerTbl);
        //checkResult(taos, "m99", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 17 check result end\n\n");
    }
#endif 

      //========== case 18: ======================//
#if 1
    {
        printf("====case 18 error test start\n");
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 10;
        bingNum = 1;
        lenOfBinaryDef = 100;
        lenOfBinaryAct = 8;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_error_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m11", 0, totalRowsPerTbl);
        //checkResult(taos, "m22", 0, totalRowsPerTbl);  
        //checkResult(taos, "m33", 0, totalRowsPerTbl);
        //checkResult(taos, "m99", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("====case 18 check result end\n\n");
    }
#endif  

      //========== case 19: ======================//
#if 1
    {
        printf("====case 19 error test start\n");
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 10;
        bingNum = 1;
        lenOfBinaryDef = 100;
        lenOfBinaryAct = 8;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_error_case_003(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m11", 0, totalRowsPerTbl);
        //checkResult(taos, "m22", 0, totalRowsPerTbl);  
        //checkResult(taos, "m33", 0, totalRowsPerTbl);
        //checkResult(taos, "m99", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("====case 19 check result end\n\n");
    }
#endif  

  return ;

}


static int stmt_bind_case_001_long(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum, int64_t* startTs) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = *startTs;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v->v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v->v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v->v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v->v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = rowsOfPerColum;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v->f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = rowsOfPerColum;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v->f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = rowsOfPerColum;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+8].buffer = v->br;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = rowsOfPerColum;

    params[i+9].buffer_type = TSDB_DATA_TYPE_NCHAR;
    params[i+9].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+9].buffer = v->nr;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = rowsOfPerColum;

    params[i+10].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+10].buffer_length = sizeof(int64_t);
    params[i+10].buffer = v->ts2;
    params[i+10].length = NULL;
    params[i+10].is_null = is_null;
    params[i+10].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  *startTs = tts + totalRowsPerTbl * tableNum; // return to next

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static void runCase_long(TAOS *taos) {
  TAOS_STMT *stmt = NULL;
  
  int tableNum;
  int lenOfBinaryDef;
  int rowsOfPerColum;
  int bingNum;
  int lenOfBinaryAct;
  int columnNum;

  int totalRowsPerTbl;

//=======================================================================//
    //========== long case 14: ======================//
#if 0
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1000;
        rowsOfPerColum = 10;
        bingNum = 5000000;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 11;
        
        prepareV(taos, 1, tableNum, lenOfBinaryDef);
        stmt_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("long case 14 check result end\n\n");
    }
#endif  

    
  //========== case 15: ======================//
#if 1
  {
      printf("====long case 15 test start\n\n");
      
      tableNum = 200;
      rowsOfPerColum = 110;
      bingNum = 100;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 8;
      columnNum = 11;

      int64_t startTs = 1591060628000;
      prepareV_long(taos, 1, tableNum, lenOfBinaryDef);

      totalRowsPerTbl = 0;
      for (int i = 0; i < g_runTimes; i++) {
        stmt = taos_stmt_init(taos);
        stmt_bind_case_001_long(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum, &startTs);
      
        totalRowsPerTbl += rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m11", 0, totalRowsPerTbl);
        checkResult(taos, "m22", 0, totalRowsPerTbl);  
        checkResult(taos, "m133", 0, totalRowsPerTbl);
        checkResult(taos, "m199", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
      }
      
      printf("====long case 15 check result end\n\n");
  }
#endif  

  return;

}

/*=======================*/
/*
test scene:   insert into tb1 (ts,f1) values (?,?)
*/
static int stmt_specifyCol_bind_case_001(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  char *sql = "insert into m0 (ts,b,v4,f4,br) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      //char buf[32];
      //sprintf(buf, "m%d", zz);
      //code = taos_stmt_set_tbname(stmt, buf);
      //if (code != 0){
      //  printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
      //  return -1;
      //}  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

/*=======================*/
/*
test scene:   insert into ? (ts,f1) values (?,?)
*/
static int stmt_specifyCol_bind_case_002(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  char *sql = "insert into ? (ts,b,v4,f4,br) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

/*=======================*/
/*
test scene:   insert into tb1 (ts,f1) values (?,?)
*/
static int stmt_specifyCol_bind_case_001_maxRows(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+1].buffer_length = sizeof(int32_t);
    params[i+1].buffer = v->v4;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  char *sql = "insert into m0 (ts,b,v4,f4,br) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      //char buf[32];
      //sprintf(buf, "m%d", zz);
      //code = taos_stmt_set_tbname(stmt, buf);
      //if (code != 0){
      //  printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
      //  return -1;
      //}  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static void SpecifyColumnBatchCase(TAOS *taos) {
  TAOS_STMT *stmt = NULL;

  int tableNum;
  int lenOfBinaryDef;
  int rowsOfPerColum;
  int bingNum;
  int lenOfBinaryAct;
  int columnNum;

  int totalRowsPerTbl;

//=======================================================================//
//=============================== single table ==========================//
//========== case 1: ======================//
#if 1
{
  stmt = taos_stmt_init(taos);

  tableNum = 1;
  rowsOfPerColum = 1;
  bingNum = 1;
  lenOfBinaryDef = 40;
  lenOfBinaryAct = 8;
  columnNum = 5;
  
  prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db1");
  stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

  totalRowsPerTbl = rowsOfPerColum * bingNum;
  checkResult(taos, "m0", 0, totalRowsPerTbl);
  taos_stmt_close(stmt);
  printf("case 1 check result end\n\n");
}
#endif  

  //========== case 2: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 1;
    rowsOfPerColum = 5;
    bingNum = 1;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 15;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2");
    stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 2 check result end\n\n");
}
#endif  

      //========== case 2-1: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 32767;
        bingNum = 1;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 15;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2_1");
        stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m1", 0, totalRowsPerTbl);
        //checkResult(taos, "m2", 0, totalRowsPerTbl);  
        //checkResult(taos, "m3", 0, totalRowsPerTbl);
        //checkResult(taos, "m4", 0, totalRowsPerTbl);
        //checkResult(taos, "m5", 0, totalRowsPerTbl);
        //checkResult(taos, "m6", 0, totalRowsPerTbl);  
        //checkResult(taos, "m7", 0, totalRowsPerTbl);
        //checkResult(taos, "m8", 0, totalRowsPerTbl);  
        //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 2-1 check result end\n\n");
    }
#endif  
          //========== case 2-2: ======================//
#if 1
        {
            printf("====case 2-2 error test start\n");
            stmt = taos_stmt_init(taos);
            
            tableNum = 1;
            rowsOfPerColum = 32768;
            bingNum = 1;
            lenOfBinaryDef = 1000;
            lenOfBinaryAct = 15;
            columnNum = 5;
            
            prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2_2");
            stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
        
            totalRowsPerTbl = rowsOfPerColum * bingNum;
            checkResult(taos, "m0", 0, totalRowsPerTbl);
            //checkResult(taos, "m1", 0, totalRowsPerTbl);
            //checkResult(taos, "m2", 0, totalRowsPerTbl);  
            //checkResult(taos, "m3", 0, totalRowsPerTbl);
            //checkResult(taos, "m4", 0, totalRowsPerTbl);
            //checkResult(taos, "m5", 0, totalRowsPerTbl);
            //checkResult(taos, "m6", 0, totalRowsPerTbl);  
            //checkResult(taos, "m7", 0, totalRowsPerTbl);
            //checkResult(taos, "m8", 0, totalRowsPerTbl);  
            //checkResult(taos, "m9", 0, totalRowsPerTbl);
            taos_stmt_close(stmt);
            printf("====case 2-2 check result end\n\n");
        }
#endif  


    //========== case 3: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 20;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db3");
      stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 3 check result end\n\n");
  }
#endif  

      //========== case 4: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db4");
        stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
      //checkResult(taos, "m1", 0, totalRowsPerTbl);
      //checkResult(taos, "m2", 0, totalRowsPerTbl);  
      //checkResult(taos, "m3", 0, totalRowsPerTbl);
      //checkResult(taos, "m4", 0, totalRowsPerTbl);
      //checkResult(taos, "m5", 0, totalRowsPerTbl);
      //checkResult(taos, "m6", 0, totalRowsPerTbl);  
      //checkResult(taos, "m7", 0, totalRowsPerTbl);
      //checkResult(taos, "m8", 0, totalRowsPerTbl);  
      //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 4 check result end\n\n");
    }
#endif  

//=======================================================================//
//=============================== multi tables ==========================//
  //========== case 5: ======================//
#if 1
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 5;
    rowsOfPerColum = 1;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 16;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db5");
    stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 5 check result end\n\n");
  }
#endif  
  
    //========== case 6: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 5;
      rowsOfPerColum = 5;
      bingNum = 1;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 20;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db6");
      stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      checkResult(taos, "m2", 0, totalRowsPerTbl);  
      checkResult(taos, "m3", 0, totalRowsPerTbl);
      checkResult(taos, "m4", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 6 check result end\n\n");
  }
#endif  
  
      //========== case 7: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 5;
        rowsOfPerColum = 1;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db7");
        stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 7 check result end\n\n");
    }
#endif  
  
        //========== case 8: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 5;
    rowsOfPerColum = 5;
    bingNum = 5;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 40;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db8");
    stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 8 check result end\n\n");
}
#endif  

  //=======================================================================//
  //=============================== multi-rows to single table ==========================//
  //========== case 9: ======================//
#if 1
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 1;
    rowsOfPerColum = 23740;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 8;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db9");
    stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 9 check result end\n\n");
  }
#endif  

    //========== case 10: ======================//
#if 0
    {
      printf("====case 10 error test start\n");
      stmt = taos_stmt_init(taos);
    
      tableNum = 1;
      rowsOfPerColum = 23741;  // WAL size exceeds limit
      bingNum = 1;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 8;
      columnNum = 5;
      
      prepareV(taos, 1, tableNum, lenOfBinaryDef);
      stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("====case 10 check result end\n\n");
    }
#endif  


  //=======================================================================//
  //=============================== multi tables, multi bind one same table ==========================//
    //========== case 13: ======================//
#if 1
    {
      stmt = taos_stmt_init(taos);
    
      tableNum = 5;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 28;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db13");
      stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      checkResult(taos, "m2", 0, totalRowsPerTbl);  
      checkResult(taos, "m3", 0, totalRowsPerTbl);
      checkResult(taos, "m4", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 13 check result end\n\n");
    }
#endif  
    
      //========== case 14: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 5;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db14");
        stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 14 check result end\n\n");
    }
#endif  

    
  //========== case 15: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1000;
      rowsOfPerColum = 10;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 8;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db15");
      stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m111", 0, totalRowsPerTbl);
      checkResult(taos, "m222", 0, totalRowsPerTbl);  
      checkResult(taos, "m333", 0, totalRowsPerTbl);
      checkResult(taos, "m500", 0, totalRowsPerTbl);  
      checkResult(taos, "m999", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 15 check result end\n\n");
  }
#endif  

    //========== case 17: ======================//
#if 1
    {
        //printf("case 17 test start\n");
        stmt = taos_stmt_init(taos);
        
        tableNum = 10;
        rowsOfPerColum = 100;
        bingNum = 1;
        lenOfBinaryDef = 100;
        lenOfBinaryAct = 8;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db17");
        stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m5", 0, totalRowsPerTbl);  
        checkResult(taos, "m8", 0, totalRowsPerTbl);
        checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 17 check result end\n\n");
    }
#endif 

  return ;

}


/*=======================*/
/*
test scene:   insert into tb1 (ts,f1) values (?,?)
*/
static int stmt_specifyCol_bind_case_001_autoCreateTbl(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));

  TAOS_BIND       *tags   = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  int one_not_null = 0;

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v->v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v->b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v->v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v->v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v->v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v->f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v->f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v->br;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v->nr;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  //char *sql = "insert into ? (ts,b,v4,f4,br) using stb1 tags (?,?,?,?,?,?,?,?,?) values(?,?,?,?,?)";
  char *sql = "insert into ? using stb1 tags (?,?,?,?,?,?,?,?,?) (ts,b,v4,f4,br) values(?,?,?,?,?)";

  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname_tags(stmt, buf, tags);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(tags);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static int stmt_specifyCol_bind_case_002_autoCreateTbl(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));

  TAOS_BIND       *tags   = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  int one_not_null = 0;

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  for (int i = 0; i < 1; ++i) {
    //tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    //tags[i+0].buffer = v->v4;
    //tags[i+0].is_null = &one_not_null;
    //tags[i+0].length = NULL;

    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v->b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    //tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    //tags[i+2].buffer = v->v1;
    //tags[i+2].is_null = &one_not_null;
    //tags[i+2].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v->v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+2].buffer = v->v8;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+3].buffer = v->f4;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+4].buffer = v->f8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+5].buffer = v->br;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = (uintptr_t *)lb;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+6].buffer = v->nr;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  //char *sql = "insert into ? (ts,b,v4,f4,br) using stb1 tags (?,?,?,?,?,?,?,?,?) values(?,?,?,?,?)";
  char *sql = "insert into ? using stb1 tags (33,?,99,?,?,?,?,?,?) (ts,b,v4,f4,br) values(?,?,?,?,?)";

  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname_tags(stmt, buf, tags);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(tags);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

//  some tags are null
static int stmt_specifyCol_bind_case_003_autoCreateTbl(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));

  TAOS_BIND       *tags   = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  int one_not_null = 0;
  int one_is_null = 1;

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v->v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v->b;
    tags[i+1].is_null = &one_is_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v->v1;
    tags[i+2].is_null = &one_is_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v->v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v->v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v->f4;
    tags[i+5].is_null = &one_is_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v->f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v->br;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v->nr;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  //char *sql = "insert into ? (ts,b,v4,f4,br) using stb1 tags (?,?,?,?,?,?,?,?,?) values(?,?,?,?,?)";
  char *sql = "insert into ? using stb1 tags (?,?,?,?,?,?,?,?,?) (ts,b,v4,f4,br) values(?,?,?,?,?)";

  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname_tags(stmt, buf, tags);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(tags);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

// specify tags field, and not support , then is error case
static int stmt_specifyCol_bind_case_004_autoCreateTbl(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));

  TAOS_BIND       *tags   = taosMemoryCalloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  int one_not_null = 0;

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  for (int i = 0; i < 1; ++i) {
    //tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    //tags[i+0].buffer = v->v4;
    //tags[i+0].is_null = &one_not_null;
    //tags[i+0].length = NULL;

    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v->b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    //tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    //tags[i+2].buffer = v->v1;
    //tags[i+2].is_null = &one_not_null;
    //tags[i+2].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v->v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+2].buffer = v->v8;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+3].buffer = v->f4;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+4].buffer = v->f8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+5].buffer = v->br;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = (uintptr_t *)lb;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+6].buffer = v->nr;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  //char *sql = "insert into ? (ts,b,v4,f4,br) using stb1 tags (?,?,?,?,?,?,?,?,?) values(?,?,?,?,?)";
  char *sql = "insert into ? using stb1 (id1, id2, id3, id4, id5, id6, id7, id8, id9) tags (33,?,99,?,?,?,?,?,?) (ts,b,v4,f4,br) values(?,?,?,?,?)";

  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname_tags(stmt, buf, tags);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(tags);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static void SpecifyColumnBatchCase_autoCreateTbl(TAOS *taos) {
  TAOS_STMT *stmt = NULL;

  int tableNum;
  int lenOfBinaryDef;
  int rowsOfPerColum;
  int bingNum;
  int lenOfBinaryAct;
  int columnNum;

  int totalRowsPerTbl;

//=======================================================================//
//=============================== single table ==========================//
//========== case 1: ======================//
#if 1
{
  stmt = taos_stmt_init(taos);

  tableNum = 1;
  rowsOfPerColum = 1;
  bingNum = 1;
  lenOfBinaryDef = 40;
  lenOfBinaryAct = 8;
  columnNum = 5;
  
  prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db1");
  stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

  totalRowsPerTbl = rowsOfPerColum * bingNum;
  checkResult(taos, "m0", 0, totalRowsPerTbl);
  taos_stmt_close(stmt);
  printf("case 1 check result end\n\n");
}
#endif  

  //========== case 2: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 1;
    rowsOfPerColum = 5;
    bingNum = 1;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 15;
    columnNum = 5;
    
    prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db2");
    stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 2 check result end\n\n");
}
#endif  

      //========== case 2-1: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 32767;
        bingNum = 1;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 15;
        columnNum = 5;
        
        prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db2_1");
        stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m1", 0, totalRowsPerTbl);
        //checkResult(taos, "m2", 0, totalRowsPerTbl);  
        //checkResult(taos, "m3", 0, totalRowsPerTbl);
        //checkResult(taos, "m4", 0, totalRowsPerTbl);
        //checkResult(taos, "m5", 0, totalRowsPerTbl);
        //checkResult(taos, "m6", 0, totalRowsPerTbl);  
        //checkResult(taos, "m7", 0, totalRowsPerTbl);
        //checkResult(taos, "m8", 0, totalRowsPerTbl);  
        //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 2-1 check result end\n\n");
    }
#endif  
          //========== case 2-2: ======================//
#if 1
        {
            printf("====case 2-2 error test start\n");
            stmt = taos_stmt_init(taos);
            
            tableNum = 1;
            rowsOfPerColum = 32768;
            bingNum = 1;
            lenOfBinaryDef = 1000;
            lenOfBinaryAct = 15;
            columnNum = 5;
            
            prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db2_2");
            stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
        
            totalRowsPerTbl = rowsOfPerColum * bingNum;
            checkResult(taos, "m0", 0, totalRowsPerTbl);
            //checkResult(taos, "m1", 0, totalRowsPerTbl);
            //checkResult(taos, "m2", 0, totalRowsPerTbl);  
            //checkResult(taos, "m3", 0, totalRowsPerTbl);
            //checkResult(taos, "m4", 0, totalRowsPerTbl);
            //checkResult(taos, "m5", 0, totalRowsPerTbl);
            //checkResult(taos, "m6", 0, totalRowsPerTbl);  
            //checkResult(taos, "m7", 0, totalRowsPerTbl);
            //checkResult(taos, "m8", 0, totalRowsPerTbl);  
            //checkResult(taos, "m9", 0, totalRowsPerTbl);
            taos_stmt_close(stmt);
            printf("====case 2-2 check result end\n\n");
        }
#endif  


    //========== case 3: ======================//
#if 1
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 20;
      columnNum = 5;
      
      prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db3");
      stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 3 check result end\n\n");
  }
#endif  

      //========== case 4: ======================//
#if 1
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db4");
        stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
      //checkResult(taos, "m1", 0, totalRowsPerTbl);
      //checkResult(taos, "m2", 0, totalRowsPerTbl);  
      //checkResult(taos, "m3", 0, totalRowsPerTbl);
      //checkResult(taos, "m4", 0, totalRowsPerTbl);
      //checkResult(taos, "m5", 0, totalRowsPerTbl);
      //checkResult(taos, "m6", 0, totalRowsPerTbl);  
      //checkResult(taos, "m7", 0, totalRowsPerTbl);
      //checkResult(taos, "m8", 0, totalRowsPerTbl);  
      //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 4 check result end\n\n");
    }
#endif  

  //=======================================================================//
  //=============================== multi-rows to single table ==========================//
  //========== case 5: ======================//
#if 1
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 1;
    rowsOfPerColum = 23740;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 8;
    columnNum = 5;
    
    prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db5");
    stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 5 check result end\n\n");
  }
#endif  

//  ==============  error test: 1.multi table, 2.specify some tags
    //========== case 6: ======================//
#if 1
    {
      stmt = taos_stmt_init(taos);
    
      tableNum = 2;
      rowsOfPerColum = 5;
      bingNum = 1;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 8;
      columnNum = 5;
      
      prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db6");
      stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 6 check result end\n\n");
    }
#endif  

          //========== case 7: ======================//
#if 1
          {
            stmt = taos_stmt_init(taos);
          
            tableNum = 200;
            rowsOfPerColum = 60;
            bingNum = 1;
            lenOfBinaryDef = 40;
            lenOfBinaryAct = 8;
            columnNum = 5;
            
            prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db7");
            stmt_specifyCol_bind_case_001_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
          
            totalRowsPerTbl = rowsOfPerColum * bingNum;
            checkResult(taos, "m0", 0, totalRowsPerTbl);
            checkResult(taos, "m1", 0, totalRowsPerTbl);
            checkResult(taos, "m99", 0, totalRowsPerTbl);
            checkResult(taos, "m139", 0, totalRowsPerTbl);
            checkResult(taos, "m199", 0, totalRowsPerTbl);
            taos_stmt_close(stmt);
            printf("case 7 check result end\n\n");
          }
#endif  

      //========== case 8: ======================//
#if 1
      {
        stmt = taos_stmt_init(taos);
      
        tableNum = 1;
        rowsOfPerColum = 5;
        bingNum = 1;
        lenOfBinaryDef = 40;
        lenOfBinaryAct = 8;
        columnNum = 5;
        
        prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db8");
        stmt_specifyCol_bind_case_002_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
      
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 8 check result end\n\n");
      }
#endif  

        //========== case 9: ======================//

#if 1
        {
          stmt = taos_stmt_init(taos);
        
          tableNum = 10;
          rowsOfPerColum = 5;
          bingNum = 1;
          lenOfBinaryDef = 40;
          lenOfBinaryAct = 8;
          columnNum = 5;
          
          prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db9");
          stmt_specifyCol_bind_case_002_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
        
          totalRowsPerTbl = rowsOfPerColum * bingNum;
          checkResult(taos, "m0", 0, totalRowsPerTbl);
          checkResult(taos, "m3", 0, totalRowsPerTbl);
          checkResult(taos, "m6", 0, totalRowsPerTbl);
          checkResult(taos, "m9", 0, totalRowsPerTbl);
          taos_stmt_close(stmt);
          printf("case 9 check result end\n\n");
        }
#endif  

    //========== case 10: ======================//
#if 1
    {
      stmt = taos_stmt_init(taos);
    
      tableNum = 1;
      rowsOfPerColum = 23740;
      bingNum = 1;
      lenOfBinaryDef = 40;
      lenOfBinaryAct = 8;
      columnNum = 5;
      
      prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db10");
      stmt_specifyCol_bind_case_003_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 10 check result end\n\n");
    }
#endif  
  
      //========== case 11: ======================//
#if 1
      {
        stmt = taos_stmt_init(taos);
      
        tableNum = 2;
        rowsOfPerColum = 5;
        bingNum = 1;
        lenOfBinaryDef = 40;
        lenOfBinaryAct = 8;
        columnNum = 5;
        
        prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db11");
        stmt_specifyCol_bind_case_003_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
      
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 11 check result end\n\n");
      }
#endif  

            //========== case 12: ======================//
#if 1
            {
              stmt = taos_stmt_init(taos);
            
              tableNum = 200;
              rowsOfPerColum = 60;
              bingNum = 1;
              lenOfBinaryDef = 40;
              lenOfBinaryAct = 8;
              columnNum = 5;
              
              prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db12");
              stmt_specifyCol_bind_case_003_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
            
              totalRowsPerTbl = rowsOfPerColum * bingNum;
              checkResult(taos, "m0", 0, totalRowsPerTbl);
              checkResult(taos, "m1", 0, totalRowsPerTbl);
              checkResult(taos, "m99", 0, totalRowsPerTbl);
              checkResult(taos, "m139", 0, totalRowsPerTbl);
              checkResult(taos, "m199", 0, totalRowsPerTbl);
              taos_stmt_close(stmt);
              printf("case 12 check result end\n\n");
            }
#endif  

  
              //========== case 13: ======================//
#if 1
              {
                printf("====case 13 error test start\n");
                stmt = taos_stmt_init(taos);
              
                tableNum = 1;
                rowsOfPerColum = 8;
                bingNum = 1;
                lenOfBinaryDef = 40;
                lenOfBinaryAct = 8;
                columnNum = 5;
                
                prepareVcolumn_autoCreateTbl(taos, 1, tableNum, lenOfBinaryDef, "db13");
                stmt_specifyCol_bind_case_004_autoCreateTbl(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
              
                totalRowsPerTbl = rowsOfPerColum * bingNum;
                checkResult(taos, "m0", 0, totalRowsPerTbl);
                taos_stmt_close(stmt);
                printf("====case 13 check result end\n\n");
              }
#endif  

  return ;

}


/*=======================*/
/*
char * taos_stmt_errstr(TAOS_STMT *stmt) 用于在其他stmt API 返回错误（返回错误码或空指针）时获取错误信息，返回的错误信息可能是以下3种的一个：
1. 当stmt或sqlobj为空时的terrno对于的错误消息;
2. （可有的）详细错误消息；
3. 返回的错误码对于的错误消息；
*/
static int stmt_specifyCol_bind_error_case_001(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d-0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  char *sql = "insert into m0 (ts,b,v4,f4,br) values(?,?,?,?,?)";

  int code;
  //TAOS_STMT *stmtNull = NULL;
  //int code = taos_stmt_prepare(stmtNull, sql, 0);
  //if (code != 0){
  //  printf("failed to execute taos_stmt_prepare. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmtNull));
  //  return -1;
  //}

  //char* sqlNull = NULL;
  //code = taos_stmt_prepare(stmt, sqlNull, 0);
  //if (code != 0){
  //  printf("failed to execute taos_stmt_prepare. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
  //}
  
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      //char buf[32];
      //sprintf(buf, "m%d", zz);
      //code = taos_stmt_set_tbname(stmt, buf);
      //if (code != 0){
      //  printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s]\n", code, tstrerror(code));
      //  return -1;
      //}  

      //for (int col=0; col < columnNum + 1000; ++col) {
      for (int col=0; col < columnNum; ++col) {
        if (col >= columnNum) col = 0;
        //code = taos_stmt_bind_single_param_batch(NULL, params + id, col);
        //code = taos_stmt_bind_single_param_batch(stmt, NULL, col);
        //code = taos_stmt_bind_single_param_batch(stmt, params + id, columnNum);
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
          return -1;
        }
        id++;
      }
      
      //code = taos_stmt_add_batch(NULL);
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
        return -1;
      }      
      
      //code = taos_stmt_add_batch(stmt);
      //if (code != 0) {
      //  printf("failed to execute taos_stmt_add_batch second. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
      //  return -1;
      //}
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static int stmt_specifyCol_bind_error_case_002(TAOS_STMT *stmt, int tableNum, int rowsOfPerColum, int bingNum, int lenOfBinaryDef, int lenOfBinaryAct, int columnNum) {
  sampleValue* v = (sampleValue *)taosMemoryCalloc(1, sizeof(sampleValue));

  int totalRowsPerTbl = rowsOfPerColum * bingNum;

  v->ts = (int64_t *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * tableNum));
  v->br = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  v->nr = (char *)taosMemoryMalloc(sizeof(int64_t) * (size_t)(totalRowsPerTbl * lenOfBinaryDef));
  
  int *lb = (int *)taosMemoryMalloc(MAX_ROWS_OF_PER_COLUMN * sizeof(int));
  
  TAOS_MULTI_BIND *params = taosMemoryCalloc(1, sizeof(TAOS_MULTI_BIND) * (size_t)(bingNum * columnNum * (tableNum+1) * rowsOfPerColum));
  char* is_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);
  char* no_null = taosMemoryMalloc(sizeof(char) * MAX_ROWS_OF_PER_COLUMN);

  int64_t tts = 1591060628000;

  for (int i = 0; i < rowsOfPerColum; ++i) {
    lb[i] = lenOfBinaryAct;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v->b[i]  = (int8_t)(i % 2);
    v->v1[i] = (int8_t)((i+1) % 2);
    v->v2[i] = (int16_t)i;
    v->v4[i] = (int32_t)(i+1);
    v->v8[i] = (int64_t)(i+2);
    v->f4[i] = (float)(i+3);
    v->f8[i] = (double)(i+4);
    char tbuf[MAX_BINARY_DEF_LEN];
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "binary-%d",  i%10);
    memcpy(v->br + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    memset(tbuf, 0, MAX_BINARY_DEF_LEN);
    sprintf(tbuf, "nchar-%d",  i%10);
    memcpy(v->nr + i*lenOfBinaryDef, tbuf, (size_t)lenOfBinaryAct);
    v->ts2[i] = tts + i;
  }

  int i = 0;
  for (int j = 0; j < bingNum * tableNum; j++) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v->ts[j*rowsOfPerColum];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = rowsOfPerColum;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v->b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = rowsOfPerColum;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v->v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = rowsOfPerColum;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v->f4;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = rowsOfPerColum;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = (uintptr_t)lenOfBinaryDef;
    params[i+4].buffer = v->br;
    params[i+4].length = lb;
    params[i+4].is_null = is_null;
    params[i+4].num = rowsOfPerColum;

    i+=columnNum;
  }

  //int64_t tts = 1591060628000;
  for (int i = 0; i < totalRowsPerTbl * tableNum; ++i) {
    v->ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

// create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, br binary(%d), nr nchar(%d), ts2 timestamp)
  char *sql = "insert into ? (ts,b,v4,f4,br) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x[%s]\n", code, tstrerror(code));
    return -1;
  }

  int id = 0;
  for (int l = 0; l < bingNum; l++) {
    for (int zz = 0; zz < tableNum; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
    
      //stmt=NULL;
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x[%s][%s]\n", code, tstrerror(code),  taos_stmt_errstr(stmt));
        return -1;
      }  

      for (int col=0; col < columnNum; ++col) {
        code = taos_stmt_bind_single_param_batch(stmt, params + id, col);
        if (code != 0){
          printf("failed to execute taos_stmt_bind_single_param_batch. code:0x%x[%s]\n", code, tstrerror(code));
          return -1;
        }
        id++;
      }
      
      code = taos_stmt_add_batch(stmt);
      if (code != 0) {
        printf("failed to execute taos_stmt_add_batch. code:0x%x[%s]\n", code, tstrerror(code));
        return -1;
      }
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
      printf("failed to execute taos_stmt_execute. code:0x%x[%s]\n", code, tstrerror(code));
      return -1;
    }
  }

  unsigned long long endtime = getCurrentTime();
  unsigned long long totalRows = (uint32_t)(totalRowsPerTbl * tableNum);
  printf("insert total %d records, used %u seconds, avg:%u useconds per record\n", totalRows, (endtime-starttime)/1000000UL, (endtime-starttime)/totalRows);

  taosMemoryFree(v->ts);  
  taosMemoryFree(v->br);  
  taosMemoryFree(v->nr);  
  taosMemoryFree(v);
  taosMemoryFree(lb);
  taosMemoryFree(params);
  taosMemoryFree(is_null);
  taosMemoryFree(no_null);

  return 0;
}

static void SpecifyColumnBatchErrorCase(TAOS *taos) {
  TAOS_STMT *stmt = NULL;

  int tableNum;
  int lenOfBinaryDef;
  int rowsOfPerColum;
  int bingNum;
  int lenOfBinaryAct;
  int columnNum;

  int totalRowsPerTbl;

//=======================================================================//
//=============================== single table ==========================//
//========== case 1: ======================//
#if 0
{
  stmt = taos_stmt_init(taos);

  tableNum = 1;
  rowsOfPerColum = 1;
  bingNum = 1;
  lenOfBinaryDef = 40;
  lenOfBinaryAct = 8;
  columnNum = 5;
  
  prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db1");
  stmt_specifyCol_bind_error_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

  totalRowsPerTbl = rowsOfPerColum * bingNum;
  checkResult(taos, "m0", 0, totalRowsPerTbl);
  taos_stmt_close(stmt);
  printf("case 1 check result end\n\n");
}
#endif  

  //========== case 2: ======================//
#if 1
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 1;
    rowsOfPerColum = 5;
    bingNum = 1;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 15;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2");
    stmt_specifyCol_bind_error_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 2 check result end\n\n");
}
#endif  

      //========== case 2-1: ======================//
#if 0
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 32767;
        bingNum = 1;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 15;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2_1");
        stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        //checkResult(taos, "m1", 0, totalRowsPerTbl);
        //checkResult(taos, "m2", 0, totalRowsPerTbl);  
        //checkResult(taos, "m3", 0, totalRowsPerTbl);
        //checkResult(taos, "m4", 0, totalRowsPerTbl);
        //checkResult(taos, "m5", 0, totalRowsPerTbl);
        //checkResult(taos, "m6", 0, totalRowsPerTbl);  
        //checkResult(taos, "m7", 0, totalRowsPerTbl);
        //checkResult(taos, "m8", 0, totalRowsPerTbl);  
        //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 2-1 check result end\n\n");
    }
#endif  
          //========== case 2-2: ======================//
#if 0
        {
            printf("====case 2-2 error test start\n");
            stmt = taos_stmt_init(taos);
            
            tableNum = 1;
            rowsOfPerColum = 32768;
            bingNum = 1;
            lenOfBinaryDef = 1000;
            lenOfBinaryAct = 15;
            columnNum = 5;
            
            prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db2_2");
            stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
        
            totalRowsPerTbl = rowsOfPerColum * bingNum;
            checkResult(taos, "m0", 0, totalRowsPerTbl);
            //checkResult(taos, "m1", 0, totalRowsPerTbl);
            //checkResult(taos, "m2", 0, totalRowsPerTbl);  
            //checkResult(taos, "m3", 0, totalRowsPerTbl);
            //checkResult(taos, "m4", 0, totalRowsPerTbl);
            //checkResult(taos, "m5", 0, totalRowsPerTbl);
            //checkResult(taos, "m6", 0, totalRowsPerTbl);  
            //checkResult(taos, "m7", 0, totalRowsPerTbl);
            //checkResult(taos, "m8", 0, totalRowsPerTbl);  
            //checkResult(taos, "m9", 0, totalRowsPerTbl);
            taos_stmt_close(stmt);
            printf("====case 2-2 check result end\n\n");
        }
#endif  


    //========== case 3: ======================//
#if 0
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 1;
      rowsOfPerColum = 1;
      bingNum = 5;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 20;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db3");
      stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
    //checkResult(taos, "m1", 0, totalRowsPerTbl);
    //checkResult(taos, "m2", 0, totalRowsPerTbl);  
    //checkResult(taos, "m3", 0, totalRowsPerTbl);
    //checkResult(taos, "m4", 0, totalRowsPerTbl);
    //checkResult(taos, "m5", 0, totalRowsPerTbl);
    //checkResult(taos, "m6", 0, totalRowsPerTbl);  
    //checkResult(taos, "m7", 0, totalRowsPerTbl);
    //checkResult(taos, "m8", 0, totalRowsPerTbl);  
    //checkResult(taos, "m9", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 3 check result end\n\n");
  }
#endif  

      //========== case 4: ======================//
#if 0
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 1;
        rowsOfPerColum = 5;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db4");
        stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
      //checkResult(taos, "m1", 0, totalRowsPerTbl);
      //checkResult(taos, "m2", 0, totalRowsPerTbl);  
      //checkResult(taos, "m3", 0, totalRowsPerTbl);
      //checkResult(taos, "m4", 0, totalRowsPerTbl);
      //checkResult(taos, "m5", 0, totalRowsPerTbl);
      //checkResult(taos, "m6", 0, totalRowsPerTbl);  
      //checkResult(taos, "m7", 0, totalRowsPerTbl);
      //checkResult(taos, "m8", 0, totalRowsPerTbl);  
      //checkResult(taos, "m9", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 4 check result end\n\n");
    }
#endif  

//=======================================================================//
//=============================== multi tables ==========================//
  //========== case 5: ======================//
#if 0
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 5;
    rowsOfPerColum = 1;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 16;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db5");
    stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 5 check result end\n\n");
  }
#endif  
  
    //========== case 6: ======================//
#if 0
  {
      stmt = taos_stmt_init(taos);
      
      tableNum = 5;
      rowsOfPerColum = 5;
      bingNum = 1;
      lenOfBinaryDef = 1000;
      lenOfBinaryAct = 20;
      columnNum = 5;
      
      prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db6");
      stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
      totalRowsPerTbl = rowsOfPerColum * bingNum;
      checkResult(taos, "m0", 0, totalRowsPerTbl);
      checkResult(taos, "m1", 0, totalRowsPerTbl);
      checkResult(taos, "m2", 0, totalRowsPerTbl);  
      checkResult(taos, "m3", 0, totalRowsPerTbl);
      checkResult(taos, "m4", 0, totalRowsPerTbl);
      taos_stmt_close(stmt);
      printf("case 6 check result end\n\n");
  }
#endif  
  
      //========== case 7: ======================//
#if 0
    {
        stmt = taos_stmt_init(taos);
        
        tableNum = 5;
        rowsOfPerColum = 1;
        bingNum = 5;
        lenOfBinaryDef = 1000;
        lenOfBinaryAct = 33;
        columnNum = 5;
        
        prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db7");
        stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
    
        totalRowsPerTbl = rowsOfPerColum * bingNum;
        checkResult(taos, "m0", 0, totalRowsPerTbl);
        checkResult(taos, "m1", 0, totalRowsPerTbl);
        checkResult(taos, "m2", 0, totalRowsPerTbl);  
        checkResult(taos, "m3", 0, totalRowsPerTbl);
        checkResult(taos, "m4", 0, totalRowsPerTbl);
        taos_stmt_close(stmt);
        printf("case 7 check result end\n\n");
    }
#endif  
  
        //========== case 8: ======================//
#if 0
{
    stmt = taos_stmt_init(taos);
    
    tableNum = 5;
    rowsOfPerColum = 5;
    bingNum = 5;
    lenOfBinaryDef = 1000;
    lenOfBinaryAct = 40;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db8");
    stmt_specifyCol_bind_case_002(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);

    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    checkResult(taos, "m1", 0, totalRowsPerTbl);
    checkResult(taos, "m2", 0, totalRowsPerTbl);  
    checkResult(taos, "m3", 0, totalRowsPerTbl);
    checkResult(taos, "m4", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 8 check result end\n\n");
}
#endif  

  //=======================================================================//
  //=============================== multi-rows to single table ==========================//
  //========== case 9: ======================//
#if 0
  {
    stmt = taos_stmt_init(taos);
  
    tableNum = 1;
    rowsOfPerColum = 23740;
    bingNum = 1;
    lenOfBinaryDef = 40;
    lenOfBinaryAct = 8;
    columnNum = 5;
    
    prepareVcolumn(taos, 1, tableNum, lenOfBinaryDef, "db9");
    stmt_specifyCol_bind_case_001(stmt, tableNum, rowsOfPerColum, bingNum, lenOfBinaryDef, lenOfBinaryAct, columnNum);
  
    totalRowsPerTbl = rowsOfPerColum * bingNum;
    checkResult(taos, "m0", 0, totalRowsPerTbl);
    taos_stmt_close(stmt);
    printf("case 9 check result end\n\n");
  }
#endif  

  return ;

}

int main(int argc, char *argv[])
{
  TAOS *taos;
  char  host[32] = "127.0.0.1";
  char* serverIp = NULL;
  //int   threadNum = 1;
  
  // connect to server
  if (argc == 1) {
    serverIp = host;
  } else if (argc == 2) {
    serverIp = argv[1];
  } else if (argc == 3) {
    serverIp = argv[1];
    //threadNum = atoi(argv[2]);
  } else if (argc == 4) {
    serverIp = argv[1];
    //threadNum = atoi(argv[2]);
    g_runTimes = atoi(argv[3]);
  }

  printf("server:%s, runTimes:%d\n\n", serverIp, g_runTimes);

#if 0
  printf("server:%s, threadNum:%d, rows:%d\n\n", serverIp, threadNum, g_rows);

  TdThread *pThreadList = (TdThread *) taosMemoryCalloc(sizeof(TdThread), (size_t)threadNum);
  ThreadInfo* threadInfo = (ThreadInfo *) taosMemoryCalloc(sizeof(ThreadInfo), (size_t)threadNum);

  ThreadInfo*  tInfo = threadInfo;
  for (int i = 0; i < threadNum; i++) {
    taos = taos_connect(serverIp, "root", "taosdata", NULL, 0);
    if (taos == NULL) {
      printf("failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
      return -1;
    }   

    tInfo->taos = taos;
    tInfo->idx = i;
    if (0 == i) {
      //taosThreadCreate(&(pThreadList[0]), NULL, runCase, (void *)tInfo);      
      taosThreadCreate(&(pThreadList[0]), NULL, SpecifyColumnBatchCase, (void *)tInfo);
    } else if (1 == i){
      taosThreadCreate(&(pThreadList[0]), NULL, runCase_long, (void *)tInfo);
    }
    tInfo++;
  }

  for (int i = 0; i < threadNum; i++) {
    taosThreadJoin(pThreadList[i], NULL);
  }

  taosMemoryFree(pThreadList);
  taosMemoryFree(threadInfo);
#endif

  taos = taos_connect(serverIp, "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to TDengine, reason:%s\n", taos_errstr(taos));
    return -1;
  }

  //runCase(taos);
  //runCase_long(taos);
  //SpecifyColumnBatchCase(taos);
  SpecifyColumnBatchCase_autoCreateTbl(taos);
  //SpecifyColumnBatchErrorCase(taos);
  return 0;
}

