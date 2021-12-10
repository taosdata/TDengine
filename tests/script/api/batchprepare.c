// TAOS standard API example. The same syntax as MySQL, but only a subet 
// to compile: gcc -o prepare prepare.c -ltaos

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

typedef struct {
  TAOS     *taos;
  int idx;
}T_par;

void taosMsleep(int mseconds);

unsigned long long getCurrentTime(){
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        perror("Failed to get current time in ms");
        exit(EXIT_FAILURE);
    }

    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}





int stmt_scol_func1(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[1].buffer_length = sizeof(v.v1);
  params[1].buffer = &v.v1;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[2].buffer_length = sizeof(v.v2);
  params[2].buffer = &v.v2;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(v.f4);
  params[3].buffer = &v.f4;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[4].buffer_length = sizeof(v.bin);
  params[4].buffer = v.bin;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer_length = sizeof(v.bin);
  params[5].buffer = v.bin;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  char *sql = "insert into ? (ts, v1,v2,f4,bin,bin2) values(?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }
  
  for (int zz = 0; zz < 10; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname(stmt, buf);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      exit(1);
    }  
    v.ts = 1591060628000 + zz * 10;
    for (int i = 0; i < 10; ++i) {
      v.ts += 1;

      v.b = (int8_t)(i+zz*10) % 2;
      v.v1 = (int8_t)(i+zz*10);
      v.v2 = (int16_t)((i+zz*10) * 2);
      v.v4 = (int32_t)((i+zz*10) * 4);
      v.v8 = (int64_t)((i+zz*10) * 8);
      v.f4 = (float)((i+zz*10) * 40);
      v.f8 = (double)((i+zz*10) * 80);
      for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
        v.bin[j] = (char)((i)%10 + '0');
      }

      taos_stmt_bind_param(stmt, params);
      taos_stmt_add_batch(stmt);
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  return 0;
}



int stmt_scol_func2(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[1].buffer_length = sizeof(v.v1);
  params[1].buffer = &v.v1;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[2].buffer_length = sizeof(v.v2);
  params[2].buffer = &v.v2;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(v.f4);
  params[3].buffer = &v.f4;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[4].buffer_length = sizeof(v.bin);
  params[4].buffer = v.bin;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer_length = sizeof(v.bin);
  params[5].buffer = v.bin;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  char *sql = "insert into m0 (ts, v1,v2,f4,bin,bin2) values(?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }
  
  for (int zz = 0; zz < 10; zz++) {
    v.ts = 1591060628000 + zz * 10;
    for (int i = 0; i < 10; ++i) {
      v.ts += 1;

      v.b = (int8_t)(i+zz*10) % 2;
      v.v1 = (int8_t)(i+zz*10);
      v.v2 = (int16_t)((i+zz*10) * 2);
      v.v4 = (int32_t)((i+zz*10) * 4);
      v.v8 = (int64_t)((i+zz*10) * 8);
      v.f4 = (float)((i+zz*10) * 40);
      v.f8 = (double)((i+zz*10) * 80);
      for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
        v.bin[j] = (char)((i)%10 + '0');
      }

      taos_stmt_bind_param(stmt, params);
      taos_stmt_add_batch(stmt);
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  return 0;
}




//300 tables 60 records
int stmt_scol_func3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.v1;
    params[i+1].length = NULL;
    params[i+1].is_null = no_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+2].buffer_length = sizeof(int16_t);
    params[i+2].buffer = v.v2;
    params[i+2].length = NULL;
    params[i+2].is_null = no_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+3].buffer_length = sizeof(float);
    params[i+3].buffer = v.f4;
    params[i+3].length = NULL;
    params[i+3].is_null = no_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+4].buffer_length = 40;
    params[i+4].buffer = v.bin;
    params[i+4].length = lb;
    params[i+4].is_null = no_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+5].buffer_length = 40;
    params[i+5].buffer = v.bin;
    params[i+5].length = lb;
    params[i+5].is_null = no_null;
    params[i+5].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? (ts, v1,v2,f4,bin,bin2) values(?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 2; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

    ++id;
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}



//10 tables 10 records single column bind
int stmt_scol_func4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 2;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = no_null;
    params[i+1].num = 2;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v.v4;
    params[i+2].length = NULL;
    params[i+2].is_null = no_null;
    params[i+2].num = 2;

    params[i+3].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+3].buffer_length = sizeof(int64_t);
    params[i+3].buffer = v.v8;
    params[i+3].length = NULL;
    params[i+3].is_null = no_null;
    params[i+3].num = 2;

    params[i+4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+4].buffer_length = sizeof(double);
    params[i+4].buffer = v.f8;
    params[i+4].length = NULL;
    params[i+4].is_null = no_null;
    params[i+4].num = 2;
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 60000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? (ts,b,v4,v8,f8) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 10; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      for (int col=0; col < 10; ++col) {
        taos_stmt_bind_single_param_batch(stmt, params + id++, col);
      }
      
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}



int stmt_func1(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }
  
  for (int zz = 0; zz < 10; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname(stmt, buf);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
    }  
    v.ts = 1591060628000 + zz * 10;
    for (int i = 0; i < 10; ++i) {
      v.ts += 1;
      for (int j = 1; j < 10; ++j) {
        params[j].is_null = ((i == j) ? &is_null : 0);
      }
      v.b = (int8_t)(i+zz*10) % 2;
      v.v1 = (int8_t)(i+zz*10);
      v.v2 = (int16_t)((i+zz*10) * 2);
      v.v4 = (int32_t)((i+zz*10) * 4);
      v.v8 = (int64_t)((i+zz*10) * 8);
      v.f4 = (float)((i+zz*10) * 40);
      v.f8 = (double)((i+zz*10) * 80);
      for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
        v.bin[j] = (char)((i+zz)%10 + '0');
      }

      taos_stmt_bind_param(stmt, params);
      taos_stmt_add_batch(stmt);
    }
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  return 0;
}


int stmt_func2(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  for (int l = 0; l < 100; l++) {
    for (int zz = 0; zz < 10; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  
      v.ts = 1591060628000 + zz * 100 * l;
      for (int i = 0; i < zz; ++i) {
        v.ts += 1;
        for (int j = 1; j < 10; ++j) {
          params[j].is_null = ((i == j) ? &is_null : 0);
        }
        v.b = (int8_t)(i+zz*10) % 2;
        v.v1 = (int8_t)(i+zz*10);
        v.v2 = (int16_t)((i+zz*10) * 2);
        v.v4 = (int32_t)((i+zz*10) * 4);
        v.v8 = (int64_t)((i+zz*10) * 8);
        v.f4 = (float)((i+zz*10) * 40);
        v.f8 = (double)((i+zz*10) * 80);
        for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
          v.bin[j] = (char)((i+zz)%10 + '0');
        }

        taos_stmt_bind_param(stmt, params);
        taos_stmt_add_batch(stmt);
      }
    }

    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

  }


  return 0;
}




int stmt_func3(TAOS_STMT *stmt) {
  struct {
      int64_t ts;
      int8_t b;
      int8_t v1;
      int16_t v2;
      int32_t v4;
      int64_t v8;
      float f4;
      double f8;
      char bin[40];
      char blob[80];
  } v = {0};
  
  TAOS_BIND params[10];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &params[0].buffer_length;
  params[0].is_null = NULL;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &params[1].buffer_length;
  params[1].is_null = NULL;

  params[2].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[2].buffer_length = sizeof(v.v1);
  params[2].buffer = &v.v1;
  params[2].length = &params[2].buffer_length;
  params[2].is_null = NULL;

  params[3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[3].buffer_length = sizeof(v.v2);
  params[3].buffer = &v.v2;
  params[3].length = &params[3].buffer_length;
  params[3].is_null = NULL;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer_length = sizeof(v.v4);
  params[4].buffer = &v.v4;
  params[4].length = &params[4].buffer_length;
  params[4].is_null = NULL;

  params[5].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[5].buffer_length = sizeof(v.v8);
  params[5].buffer = &v.v8;
  params[5].length = &params[5].buffer_length;
  params[5].is_null = NULL;

  params[6].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[6].buffer_length = sizeof(v.f4);
  params[6].buffer = &v.f4;
  params[6].length = &params[6].buffer_length;
  params[6].is_null = NULL;

  params[7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[7].buffer_length = sizeof(v.f8);
  params[7].buffer = &v.f8;
  params[7].length = &params[7].buffer_length;
  params[7].is_null = NULL;

  params[8].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[8].buffer_length = sizeof(v.bin);
  params[8].buffer = v.bin;
  params[8].length = &params[8].buffer_length;
  params[8].is_null = NULL;

  params[9].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[9].buffer_length = sizeof(v.bin);
  params[9].buffer = v.bin;
  params[9].length = &params[9].buffer_length;
  params[9].is_null = NULL;

  int is_null = 1;

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  for (int l = 0; l < 100; l++) {
    for (int zz = 0; zz < 10; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  
      v.ts = 1591060628000 + zz * 100 * l;
      for (int i = 0; i < zz; ++i) {
        v.ts += 1;
        for (int j = 1; j < 10; ++j) {
          params[j].is_null = ((i == j) ? &is_null : 0);
        }
        v.b = (int8_t)(i+zz*10) % 2;
        v.v1 = (int8_t)(i+zz*10);
        v.v2 = (int16_t)((i+zz*10) * 2);
        v.v4 = (int32_t)((i+zz*10) * 4);
        v.v8 = (int64_t)((i+zz*10) * 8);
        v.f4 = (float)((i+zz*10) * 40);
        v.f8 = (double)((i+zz*10) * 80);
        for (int j = 0; j < sizeof(v.bin) - 1; ++j) {
          v.bin[j] = (char)((i+zz)%10 + '0');
        }

        taos_stmt_bind_param(stmt, params);
        taos_stmt_add_batch(stmt);
      }
    }
  }

  
  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }


  return 0;
}



//1 tables 10 records
int stmt_funcb_autoctb1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}




//1 tables 10 records
int stmt_funcb_autoctb2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(1,true,2,3,4,5.0,6.0,'a','b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}





//1 tables 10 records
int stmt_funcb_autoctb3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(1,?,2,?,4,?,6.0,?,'b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}







//1 tables 10 records
int stmt_funcb_autoctb4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*5);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 5; i+=5) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+2].buffer_length = sizeof(int32_t);
    params[i+2].buffer = v.v4;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+3].buffer_length = sizeof(int64_t);
    params[i+3].buffer = v.v8;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+4].buffer_length = sizeof(double);
    params[i+4].buffer = v.f8;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(1,?,2,?,4,?,6.0,?,'b') (ts,b,v4,v8,f8) values(?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 5);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}





//1 tables 10 records
int stmt_funcb_autoctb_e1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+0].buffer = v.b;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+1].buffer = v.v2;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+2].buffer = v.f4;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+3].buffer = v.bin;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 (id1,id2,id3,id4,id5,id6,id7,id8,id9) tags(1,?,2,?,4,?,6.0,?,'b') values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. error:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}

int stmt_multi_insert_check(TAOS_STMT *stmt) {
  char *sql;

  // The number of tag column list is not equal to the number of tag value list
  sql = "insert into ? using stb1 (id1) tags(1,?) values(?,?,?,?,?,?,?,?,?,?)";
  if (0 == taos_stmt_prepare(stmt, sql, 0)) {
    printf("failed to check taos_stmt_prepare. sql:%s\n", sql);
    exit(1);
  }

  // The number of column list is not equal to the number of value list
  sql = "insert into ? using stb1 tags(1,?,2,?,4,?,6.0,?,'b') "
      "(ts, b, v1, v2, v4, v8, f4, f8, bin) values(?,?,?,?,?,?,?,?,?,?)";
  if (0 == taos_stmt_prepare(stmt, sql, 0)) {
    printf("failed to check taos_stmt_prepare. sql:%s\n", sql);
    exit(1);
  }

  sql = "insert into ? using stb1 () tags(1,?) values(?,?,?,?,?,?,?,?,?,?)";
  if (0 == taos_stmt_prepare(stmt, sql, 0)) {
    printf("failed to check taos_stmt_prepare. sql:%s\n", sql);
    exit(1);
  }

  sql = "insert into ? using stb1 ( tags(1,?) values(?,?,?,?,?,?,?,?,?,?)";
  if (0 == taos_stmt_prepare(stmt, sql, 0)) {
    printf("failed to check taos_stmt_prepare. sql:%s\n", sql);
    exit(1);
  }

  sql = "insert into ? using stb1 ) tags(1,?) values(?,?,?,?,?,?,?,?,?,?)";
  if (0 == taos_stmt_prepare(stmt, sql, 0)) {
    printf("failed to check taos_stmt_prepare. sql:%s\n", sql);
    exit(1);
  }

  return 0;
}

//1 tables 10 records
int stmt_funcb_autoctb_e2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, NULL);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:%s\n", taos_stmt_errstr(stmt));
      goto exit;
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

exit:
  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}







//1 tables 10 records
int stmt_funcb_autoctb_e3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 (id1,id2,id3,id4,id5,id6,id7,id8,id9) tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
    goto exit;
    //exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, NULL);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. code:0x%x\n", code);
      return -1;
    }  

    taos_stmt_bind_param_batch(stmt, params + id * 10);
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

exit:
  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}




//1 tables 10 records
int stmt_funcb_autoctb_e4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
    exit(1);
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }  

    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
    
    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      goto exit;
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));

exit:
  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}






//1 tables 10 records
int stmt_funcb_autoctb_e5(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[10];
      int8_t v1[10];
      int16_t v2[10];
      int32_t v4[10];
      int64_t v8[10];
      float f4[10];
      double f8[10];
      char bin[10][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 1 * 10);
  
  int *lb = malloc(10 * sizeof(int));

  TAOS_BIND *tags = calloc(1, sizeof(TAOS_BIND) * 9 * 1);
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 1*10);

//  int one_null = 1;
  int one_not_null = 0;
  
  char* is_null = malloc(sizeof(char) * 10);
  char* no_null = malloc(sizeof(char) * 10);

  for (int i = 0; i < 10; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 10; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[10*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 10;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 10;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 10;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 10;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 10;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 10;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 10;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 10;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 10;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 10;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts[i] = tts + i;
  }


  for (int i = 0; i < 1; ++i) {
    tags[i+0].buffer_type = TSDB_DATA_TYPE_INT;
    tags[i+0].buffer = v.v4;
    tags[i+0].is_null = &one_not_null;
    tags[i+0].length = NULL;

    tags[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    tags[i+1].buffer = v.b;
    tags[i+1].is_null = &one_not_null;
    tags[i+1].length = NULL;

    tags[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    tags[i+2].buffer = v.v1;
    tags[i+2].is_null = &one_not_null;
    tags[i+2].length = NULL;

    tags[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    tags[i+3].buffer = v.v2;
    tags[i+3].is_null = &one_not_null;
    tags[i+3].length = NULL;

    tags[i+4].buffer_type = TSDB_DATA_TYPE_BIGINT;
    tags[i+4].buffer = v.v8;
    tags[i+4].is_null = &one_not_null;
    tags[i+4].length = NULL;

    tags[i+5].buffer_type = TSDB_DATA_TYPE_FLOAT;
    tags[i+5].buffer = v.f4;
    tags[i+5].is_null = &one_not_null;
    tags[i+5].length = NULL;

    tags[i+6].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    tags[i+6].buffer = v.f8;
    tags[i+6].is_null = &one_not_null;
    tags[i+6].length = NULL;

    tags[i+7].buffer_type = TSDB_DATA_TYPE_BINARY;
    tags[i+7].buffer = v.bin;
    tags[i+7].is_null = &one_not_null;
    tags[i+7].length = (uintptr_t *)lb;

    tags[i+8].buffer_type = TSDB_DATA_TYPE_NCHAR;
    tags[i+8].buffer = v.bin;
    tags[i+8].is_null = &one_not_null;
    tags[i+8].length = (uintptr_t *)lb;
  }


  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(NULL, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(NULL));
    return -1;
  }

  int id = 0;
  for (int zz = 0; zz < 1; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname_tags(stmt, buf, tags);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname_tags. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }  

    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      exit(1);
    }
    
    code = taos_stmt_bind_param_batch(stmt, params + id * 10);
    if (code != 0) {
      printf("failed to execute taos_stmt_bind_param_batch. error:%s\n", taos_stmt_errstr(stmt));
      goto exit;
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  ++id;

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 10, (endtime-starttime)/1000000UL, (endtime-starttime)/(10));


exit:
  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);
  free(tags);

  return 0;
}


int stmt_funcb_autoctb_e6(TAOS_STMT *stmt) {
  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(now,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("case success:failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
  }

  return 0;
}


int stmt_funcb_autoctb_e7(TAOS_STMT *stmt) {
  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,true,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("case success:failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
  }

  return 0;
}


int stmt_funcb_autoctb_e8(TAOS_STMT *stmt) {
  char *sql = "insert into ? using stb1 tags(?,?,?,?,?,?,?,?,?) values(?,?,1,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("case success:failed to execute taos_stmt_prepare. code:%s\n", taos_stmt_errstr(stmt));
  }

  return 0;
}


//300 tables 60 records
int stmt_funcb1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

    ++id;
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


//1table 18000 reocrds
int stmt_funcb2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[18000];
      int8_t v1[18000];
      int16_t v2[18000];
      int32_t v4[18000];
      int64_t v8[18000];
      float f4[18000];
      double f8[18000];
      char bin[18000][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(18000 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 3000*10);
  char* is_null = malloc(sizeof(char) * 18000);
  char* no_null = malloc(sizeof(char) * 18000);

  for (int i = 0; i < 18000; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 30000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[18000*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 18000;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 18000;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 18000;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 18000;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 18000;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 18000;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 18000;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 18000;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 18000;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 18000;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      ++id;

    }
 
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


//disorder
int stmt_funcb3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  int64_t ttt = 0;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
    if (i > 0 && i%60 == 0) {
      ttt = v.ts[i-1];
      v.ts[i-1] = v.ts[i-60];
      v.ts[i-60] = ttt;
    }
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

    ++id;
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}




//samets
int stmt_funcb4(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }

    ++id;
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}




//1table 18000 reocrds
int stmt_funcb5(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[18000];
      int8_t v1[18000];
      int16_t v2[18000];
      int32_t v4[18000];
      int64_t v8[18000];
      float f4[18000];
      double f8[18000];
      char bin[18000][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(18000 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 3000*10);
  char* is_null = malloc(sizeof(char) * 18000);
  char* no_null = malloc(sizeof(char) * 18000);

  for (int i = 0; i < 18000; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 30000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[18000*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 18000;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 18000;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 18000;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 18000;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 18000;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 18000;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 18000;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 18000;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 18000;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 18000;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into m0 values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 1; zz++) {
      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      ++id;

    }
 
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


//1table 200000 reocrds
int stmt_funcb_ssz1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int b[30000];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 30000 * 3000);
  
  int *lb = malloc(30000 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 3000*10);
  char* no_null = malloc(sizeof(int) * 200000);

  for (int i = 0; i < 30000; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    v.b[i] = (int8_t)(i % 2);
  }
  
  for (int i = 0; i < 30000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[30000*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 30000;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+1].buffer_length = sizeof(int);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = no_null;
    params[i+1].num = 30000;
  }

  int64_t tts = 0;
  for (int64_t i = 0; i < 90000000LL; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 10; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      ++id;

    }
 
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(no_null);

  return 0;
}


//one table 60 records one time
int stmt_funcb_s1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      taos_stmt_bind_param_batch(stmt, params + id * 10);
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }
      
      ++id;
    }
 
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}






//300 tables 60 records single column bind
int stmt_funcb_sc1(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      for (int col=0; col < 10; ++col) {
        taos_stmt_bind_single_param_batch(stmt, params + id++, col);
      }
      
      taos_stmt_add_batch(stmt);
    }
 
    if (taos_stmt_execute(stmt) != 0) {
      printf("failed to execute insert statement.\n");
      exit(1);
    }
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


//1 tables 60 records single column bind
int stmt_funcb_sc2(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 900000 * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 900000*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }
  
  for (int i = 0; i < 9000000; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[60*i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = 60;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = 60;

    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = 60;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = 60;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = 60;

    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = 60;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = 60;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = 60;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = 60;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = 60;
    
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 54000000; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int l = 0; l < 3000; l++) {
    for (int zz = 0; zz < 300; zz++) {
      char buf[32];
      sprintf(buf, "m%d", zz);
      code = taos_stmt_set_tbname(stmt, buf);
      if (code != 0){
        printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
      }  

      for (int col=0; col < 10; ++col) {
        taos_stmt_bind_single_param_batch(stmt, params + id++, col);
      }
      
      taos_stmt_add_batch(stmt);

      if (taos_stmt_execute(stmt) != 0) {
        printf("failed to execute insert statement.\n");
        exit(1);
      }

    }
 
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


//10 tables [1...10] records single column bind
int stmt_funcb_sc3(TAOS_STMT *stmt) {
  struct {
      int64_t *ts;
      int8_t b[60];
      int8_t v1[60];
      int16_t v2[60];
      int32_t v4[60];
      int64_t v8[60];
      float f4[60];
      double f8[60];
      char bin[60][40];
  } v = {0};

  v.ts = malloc(sizeof(int64_t) * 60);
  
  int *lb = malloc(60 * sizeof(int));
  
  TAOS_MULTI_BIND *params = calloc(1, sizeof(TAOS_MULTI_BIND) * 60*10);
  char* is_null = malloc(sizeof(char) * 60);
  char* no_null = malloc(sizeof(char) * 60);

  for (int i = 0; i < 60; ++i) {
    lb[i] = 40;
    no_null[i] = 0;
    is_null[i] = (i % 10 == 2) ? 1 : 0;
    v.b[i] = (int8_t)(i % 2);
    v.v1[i] = (int8_t)((i+1) % 2);
    v.v2[i] = (int16_t)i;
    v.v4[i] = (int32_t)(i+1);
    v.v8[i] = (int64_t)(i+2);
    v.f4[i] = (float)(i+3);
    v.f8[i] = (double)(i+4);
    memset(v.bin[i], '0'+i%10, 40);
  }

  int g = 0;
  for (int i = 0; i < 600; i+=10) {
    params[i+0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[i+0].buffer_length = sizeof(int64_t);
    params[i+0].buffer = &v.ts[i/10];
    params[i+0].length = NULL;
    params[i+0].is_null = no_null;
    params[i+0].num = g%10+1;
    
    params[i+1].buffer_type = TSDB_DATA_TYPE_BOOL;
    params[i+1].buffer_length = sizeof(int8_t);
    params[i+1].buffer = v.b;
    params[i+1].length = NULL;
    params[i+1].is_null = is_null;
    params[i+1].num = g%10+1;
    
    params[i+2].buffer_type = TSDB_DATA_TYPE_TINYINT;
    params[i+2].buffer_length = sizeof(int8_t);
    params[i+2].buffer = v.v1;
    params[i+2].length = NULL;
    params[i+2].is_null = is_null;
    params[i+2].num = g%10+1;

    params[i+3].buffer_type = TSDB_DATA_TYPE_SMALLINT;
    params[i+3].buffer_length = sizeof(int16_t);
    params[i+3].buffer = v.v2;
    params[i+3].length = NULL;
    params[i+3].is_null = is_null;
    params[i+3].num = g%10+1;

    params[i+4].buffer_type = TSDB_DATA_TYPE_INT;
    params[i+4].buffer_length = sizeof(int32_t);
    params[i+4].buffer = v.v4;
    params[i+4].length = NULL;
    params[i+4].is_null = is_null;
    params[i+4].num = g%10+1;
    
    params[i+5].buffer_type = TSDB_DATA_TYPE_BIGINT;
    params[i+5].buffer_length = sizeof(int64_t);
    params[i+5].buffer = v.v8;
    params[i+5].length = NULL;
    params[i+5].is_null = is_null;
    params[i+5].num = g%10+1;

    params[i+6].buffer_type = TSDB_DATA_TYPE_FLOAT;
    params[i+6].buffer_length = sizeof(float);
    params[i+6].buffer = v.f4;
    params[i+6].length = NULL;
    params[i+6].is_null = is_null;
    params[i+6].num = g%10+1;

    params[i+7].buffer_type = TSDB_DATA_TYPE_DOUBLE;
    params[i+7].buffer_length = sizeof(double);
    params[i+7].buffer = v.f8;
    params[i+7].length = NULL;
    params[i+7].is_null = is_null;
    params[i+7].num = g%10+1;

    params[i+8].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+8].buffer_length = 40;
    params[i+8].buffer = v.bin;
    params[i+8].length = lb;
    params[i+8].is_null = is_null;
    params[i+8].num = g%10+1;

    params[i+9].buffer_type = TSDB_DATA_TYPE_BINARY;
    params[i+9].buffer_length = 40;
    params[i+9].buffer = v.bin;
    params[i+9].length = lb;
    params[i+9].is_null = is_null;
    params[i+9].num = g%10+1;
    ++g;
  }

  int64_t tts = 1591060628000;
  for (int i = 0; i < 60; ++i) {
    v.ts[i] = tts + i;
  }

  unsigned long long starttime = getCurrentTime();

  char *sql = "insert into ? values(?,?,?,?,?,?,?,?,?,?)";
  int code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }

  int id = 0;
  for (int zz = 0; zz < 10; zz++) {
    char buf[32];
    sprintf(buf, "m%d", zz);
    code = taos_stmt_set_tbname(stmt, buf);
    if (code != 0){
      printf("failed to execute taos_stmt_set_tbname. code:0x%x\n", code);
    }  

    for (int col=0; col < 10; ++col) {
      taos_stmt_bind_single_param_batch(stmt, params + id++, col);
    }
    
    taos_stmt_add_batch(stmt);
  }

  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    exit(1);
  }

  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%u useconds\n", 3000*300*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*300*60));

  free(v.ts);  
  free(lb);
  free(params);
  free(is_null);
  free(no_null);

  return 0;
}


void check_result(TAOS     *taos, char *tname, int printr, int expected) {
  char sql[255] = "SELECT * FROM ";
  TAOS_RES *result;

  //FORCE NO PRINT
  printr = 0;

  strcat(sql, tname);

  result = taos_query(taos, sql);
  int code = taos_errno(result);
  if (code != 0) {
    printf("failed to query table, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
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
    printf("%d rows are fetched as expectation\n", rows);
  } else {
    printf("!!!expect %d rows, but %d rows are fetched\n", expected, rows);
    exit(1);
  }

  taos_free_result(result);

}



//120table 60 record each table
int sql_perf1(TAOS     *taos) {
  char *sql[3000] = {0};
  TAOS_RES *result;

  for (int i = 0; i < 3000; i++) {
    sql[i] = calloc(1, 1048576);
  }

  int len = 0;
  int tss = 0;
  for (int l = 0; l < 3000; ++l) {
    len = sprintf(sql[l], "insert into ");
    for (int t = 0; t < 120; ++t) {
      len += sprintf(sql[l] + len, "m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[l] + len, "(%d, %d, %d, %d, %d, %d, %f, %f, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") ", tss++, m, m, m, m, m, m+1.0, m+1.0);
      }
    }
  }


  unsigned long long starttime = getCurrentTime();
  for (int i = 0; i < 3000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 3000; i++) {
    free(sql[i]);
  }

  return 0;
}





//one table 60 records one time
int sql_perf_s1(TAOS     *taos) {
  char **sql = calloc(1, sizeof(char*) * 360000);
  TAOS_RES *result;

  for (int i = 0; i < 360000; i++) {
    sql[i] = calloc(1, 9000);
  }

  int len = 0;
  int tss = 0;
  int id = 0;
  for (int t = 0; t < 120; ++t) {
    for (int l = 0; l < 3000; ++l) {
      len = sprintf(sql[id], "insert into m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[id] + len, "(%d, %d, %d, %d, %d, %d, %f, %f, \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\", \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\") ", tss++, m, m, m, m, m, m+1.0, m+1.0);
      }
      if (len >= 9000) {
        printf("sql:%s,len:%d\n", sql[id], len);
        exit(1);
      }
      ++id;
    }
  }


  unsigned long long starttime = getCurrentTime();
  for (int i = 0; i < 360000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 360000; i++) {
    free(sql[i]);
  }

  free(sql);

  return 0;
}


//small record size
int sql_s_perf1(TAOS     *taos) {
  char *sql[3000] = {0};
  TAOS_RES *result;

  for (int i = 0; i < 3000; i++) {
    sql[i] = calloc(1, 1048576);
  }

  int len = 0;
  int tss = 0;
  for (int l = 0; l < 3000; ++l) {
    len = sprintf(sql[l], "insert into ");
    for (int t = 0; t < 120; ++t) {
      len += sprintf(sql[l] + len, "m%d values ", t);
      for (int m = 0; m < 60; ++m) {
        len += sprintf(sql[l] + len, "(%d, %d) ", tss++, m%2);
      }
    }
  }


  unsigned long long starttime = getCurrentTime();
  for (int i = 0; i < 3000; ++i) {
      result = taos_query(taos, sql[i]);
      int code = taos_errno(result);
      if (code != 0) {
        printf("failed to query table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
    }

    taos_free_result(result);
  }
  unsigned long long endtime = getCurrentTime();
  printf("insert total %d records, used %u seconds, avg:%.1f useconds\n", 3000*120*60, (endtime-starttime)/1000000UL, (endtime-starttime)/(3000*120*60));

  for (int i = 0; i < 3000; i++) {
    free(sql[i]);
  }

  return 0;
}


void prepare(TAOS     *taos, int bigsize, int createChildTable) {
  TAOS_RES *result;
  int      code;

  result = taos_query(taos, "drop database demo"); 
  taos_free_result(result);

  result = taos_query(taos, "create database demo keep 36500");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }
  taos_free_result(result);

  result = taos_query(taos, "use demo");
  taos_free_result(result);

  if (createChildTable) {
    // create table
    for (int i = 0 ; i < 300; i++) {
      char buf[1024];
      if (bigsize) {
        sprintf(buf, "create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), bin2 binary(40))", i) ;
      } else {
        sprintf(buf, "create table m%d (ts timestamp, b int)", i) ;
      }
      result = taos_query(taos, buf);
      code = taos_errno(result);
      if (code != 0) {
        printf("failed to create table, reason:%s\n", taos_errstr(result));
        taos_free_result(result);
        exit(1);
      }
      taos_free_result(result);
    }
  } else {
    char buf[1024];
    if (bigsize) {
      sprintf(buf, "create stable stb1 (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), bin2 binary(40))" 
        " tags(id1 int, id2 bool, id3 tinyint, id4 smallint, id5 bigint, id6 float, id7 double, id8 binary(40), id9 nchar(40))") ;
    } else {
      sprintf(buf, "create stable stb1 (ts timestamp, b int) tags(id1 int, id2 bool, id3 tinyint, id4 smallint, id5 bigint, id6 float, id7 double, id8 binary(40), id9 nchar(40))") ;
    }
    
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      exit(1);
    }
    taos_free_result(result);
  }

}



void preparem(TAOS     *taos, int bigsize, int idx) {
  TAOS_RES *result;
  int      code;
  char dbname[32],sql[255];

  sprintf(dbname, "demo%d", idx);
  sprintf(sql, "drop database %s", dbname);
  

  result = taos_query(taos, sql); 
  taos_free_result(result);

  sprintf(sql, "create database %s keep 36500", dbname);
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    exit(1);
  }
  taos_free_result(result);

  sprintf(sql, "use %s", dbname);
  result = taos_query(taos, sql);
  taos_free_result(result);
  
  // create table
  for (int i = 0 ; i < 300; i++) {
    char buf[1024];
    if (bigsize) {
      sprintf(buf, "create table m%d (ts timestamp, b bool, v1 tinyint, v2 smallint, v4 int, v8 bigint, f4 float, f8 double, bin binary(40), bin2 binary(40))", i) ;
    } else {
      sprintf(buf, "create table m%d (ts timestamp, b int)", i) ;
    }
    result = taos_query(taos, buf);
    code = taos_errno(result);
    if (code != 0) {
      printf("failed to create table, reason:%s\n", taos_errstr(result));
      taos_free_result(result);
      exit(1);
    }
    taos_free_result(result);
  }

}



//void runcase(TAOS     *taos, int idx) {
void* runcase(void *par) {
  T_par* tpar = (T_par *)par;
  TAOS *taos = tpar->taos;
  int idx = tpar->idx;
  
  TAOS_STMT *stmt;

  (void)idx;

#if 1
    prepare(taos, 1, 1);
  
    stmt = taos_stmt_init(taos);
  
    printf("10t+10records+specifycol start\n");
    stmt_scol_func1(stmt);
    printf("10t+10records+specifycol end\n");
    printf("check result start\n");
    check_result(taos, "m0", 1, 10);
    check_result(taos, "m1", 1, 10);
    check_result(taos, "m2", 1, 10);
    check_result(taos, "m3", 1, 10);
    check_result(taos, "m4", 1, 10);
    check_result(taos, "m5", 1, 10);
    check_result(taos, "m6", 1, 10);
    check_result(taos, "m7", 1, 10);
    check_result(taos, "m8", 1, 10);
    check_result(taos, "m9", 1, 10);
    printf("check result end\n");
    taos_stmt_close(stmt);
#endif


#if 1
    prepare(taos, 1, 1);
  
    stmt = taos_stmt_init(taos);
  
    printf("1t+100records+specifycol start\n");
    stmt_scol_func2(stmt);
    printf("1t+100records+specifycol end\n");
    printf("check result start\n");
    check_result(taos, "m0", 1, 100);
    printf("check result end\n");
    taos_stmt_close(stmt);
#endif


#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+10r+bm+specifycol start\n");
  stmt_scol_func3(stmt);
  printf("300t+10r+bm+specifycol end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 20);
  check_result(taos, "m1", 1, 20);
  check_result(taos, "m111", 1, 20);  
  check_result(taos, "m223", 1, 20);
  check_result(taos, "m299", 1, 20);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+2r+bm+specifycol start\n");
  stmt_scol_func4(stmt);
  printf("10t+2r+bm+specifycol end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 20);
  check_result(taos, "m1", 1, 20);
  check_result(taos, "m2", 1, 20);
  check_result(taos, "m3", 1, 20);
  check_result(taos, "m4", 1, 20);
  check_result(taos, "m5", 1, 20);
  check_result(taos, "m6", 1, 20);
  check_result(taos, "m7", 1, 20);
  check_result(taos, "m8", 1, 20);
  check_result(taos, "m9", 1, 20);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+10records start\n");
  stmt_func1(stmt);
  printf("10t+10records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  check_result(taos, "m1", 1, 10);
  check_result(taos, "m2", 1, 10);
  check_result(taos, "m3", 1, 10);
  check_result(taos, "m4", 1, 10);
  check_result(taos, "m5", 1, 10);
  check_result(taos, "m6", 1, 10);
  check_result(taos, "m7", 1, 10);
  check_result(taos, "m8", 1, 10);
  check_result(taos, "m9", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif

#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[0,1,2...9]records start\n");
  stmt_func2(stmt);
  printf("10t+[0,1,2...9]records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 0);
  check_result(taos, "m1", 0, 100);
  check_result(taos, "m2", 0, 200);
  check_result(taos, "m3", 0, 300);
  check_result(taos, "m4", 0, 400);
  check_result(taos, "m5", 0, 500);
  check_result(taos, "m6", 0, 600);
  check_result(taos, "m7", 0, 700);
  check_result(taos, "m8", 0, 800);
  check_result(taos, "m9", 0, 900);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif

#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[0,100,200...900]records start\n");
  stmt_func3(stmt);
  printf("10t+[0,100,200...900]records end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 0);
  check_result(taos, "m1", 0, 100);
  check_result(taos, "m2", 0, 200);
  check_result(taos, "m3", 0, 300);
  check_result(taos, "m4", 0, 400);
  check_result(taos, "m5", 0, 500);
  check_result(taos, "m6", 0, 600);
  check_result(taos, "m7", 0, 700);
  check_result(taos, "m8", 0, 800);
  check_result(taos, "m9", 0, 900);
  printf("check result end\n");
  taos_stmt_close(stmt);
  
#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+bm start\n");
  stmt_funcb1(stmt);
  printf("300t+60r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb1 start\n");
  stmt_funcb_autoctb1(stmt);
  printf("1t+10r+bm+autoctb1 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb2 start\n");
  stmt_funcb_autoctb2(stmt);
  printf("1t+10r+bm+autoctb2 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb3 start\n");
  stmt_funcb_autoctb3(stmt);
  printf("1t+10r+bm+autoctb3 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb4 start\n");
  stmt_funcb_autoctb4(stmt);
  printf("1t+10r+bm+autoctb4 end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif


#if 1 
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e1 start\n");
  stmt_funcb_autoctb_e1(stmt);
  printf("1t+10r+bm+autoctb+e1 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e2 start\n");
  stmt_funcb_autoctb_e2(stmt);
  printf("1t+10r+bm+autoctb+e2 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e3 start\n");
  stmt_funcb_autoctb_e3(stmt);
  printf("1t+10r+bm+autoctb+e3 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e4 start\n");
  stmt_funcb_autoctb_e4(stmt);
  printf("1t+10r+bm+autoctb+e4 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("1t+10r+bm+autoctb+e5 start\n");
  stmt_funcb_autoctb_e5(stmt);
  printf("1t+10r+bm+autoctb+e5 end\n");
  printf("check result start\n");
  //check_result(taos, "m0", 1, 0);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("e6 start\n");
  stmt_funcb_autoctb_e6(stmt);
  printf("e6 end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("e7 start\n");
  stmt_funcb_autoctb_e7(stmt);
  printf("e7 end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("e8 start\n");
  stmt_funcb_autoctb_e8(stmt);
  printf("e8 end\n");
  taos_stmt_close(stmt);
  
#endif


#if 1
  prepare(taos, 1, 0);

  stmt = taos_stmt_init(taos);

  printf("stmt_multi_insert_check start\n");
  stmt_multi_insert_check(stmt);
  printf("stmt_multi_insert_check end\n");
  taos_stmt_close(stmt);
#endif

#if 1
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+18000r+bm start\n");
  stmt_funcb2(stmt);
  printf("1t+18000r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+disorder+bm start\n");
  stmt_funcb3(stmt);
  printf("300t+60r+disorder+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+samets+bm start\n");
  stmt_funcb4(stmt);
  printf("300t+60r+samets+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 1);
  check_result(taos, "m1", 0, 1);
  check_result(taos, "m111", 0, 1);  
  check_result(taos, "m223", 0, 1);
  check_result(taos, "m299", 0, 1);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+18000r+nodyntable+bm start\n");
  stmt_funcb5(stmt);
  printf("1t+18000r+nodyntable+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("300t+60r+bm+sc start\n");
  stmt_funcb_sc1(stmt);
  printf("300t+60r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);
#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+60r+bm+sc start\n");
  stmt_funcb_sc2(stmt);
  printf("1t+60r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

#if 1  
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("10t+[1...10]r+bm+sc start\n");
  stmt_funcb_sc3(stmt);
  printf("10t+[1...10]r+bm+sc end\n");
  printf("check result start\n");
  check_result(taos, "m0", 1, 1);
  check_result(taos, "m1", 1, 2);
  check_result(taos, "m2", 1, 3);
  check_result(taos, "m3", 1, 4);
  check_result(taos, "m4", 1, 5);
  check_result(taos, "m5", 1, 6);
  check_result(taos, "m6", 1, 7);
  check_result(taos, "m7", 1, 8);
  check_result(taos, "m8", 1, 9);
  check_result(taos, "m9", 1, 10);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1 
  prepare(taos, 1, 1);

  stmt = taos_stmt_init(taos);

  printf("1t+60r+bm start\n");
  stmt_funcb_s1(stmt);
  printf("1t+60r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m111", 0, 180000);  
  check_result(taos, "m223", 0, 180000);
  check_result(taos, "m299", 0, 180000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif


#if 1
  prepare(taos, 1, 1);

  (void)stmt;
  printf("120t+60r+sql start\n");
  sql_perf1(taos);
  printf("120t+60r+sql end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m34", 0, 180000);  
  check_result(taos, "m67", 0, 180000);
  check_result(taos, "m99", 0, 180000);
  printf("check result end\n");
#endif  

#if 1
  prepare(taos, 1, 1);

  (void)stmt;
  printf("1t+60r+sql start\n");
  sql_perf_s1(taos);
  printf("1t+60r+sql end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 180000);
  check_result(taos, "m1", 0, 180000);
  check_result(taos, "m34", 0, 180000);  
  check_result(taos, "m67", 0, 180000);
  check_result(taos, "m99", 0, 180000);
  printf("check result end\n");
#endif  

#if 1 
  preparem(taos, 0, idx);

  stmt = taos_stmt_init(taos);

  printf("1t+30000r+bm start\n");
  stmt_funcb_ssz1(stmt);
  printf("1t+30000r+bm end\n");
  printf("check result start\n");
  check_result(taos, "m0", 0, 300000);
  check_result(taos, "m1", 0, 300000);
  check_result(taos, "m111", 0, 300000);  
  check_result(taos, "m223", 0, 300000);
  check_result(taos, "m299", 0, 300000);
  printf("check result end\n");
  taos_stmt_close(stmt);

#endif

  printf("test end\n");

  return NULL;

}

int main(int argc, char *argv[])
{
  TAOS     *taos[4];

  // connect to server
  if (argc < 2) {
    printf("please input server ip \n");
    return 0;
  }

  taos[0] = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }   

  taos[1] = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }   

  taos[2] = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }   

  taos[3] = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }     

  pthread_t *pThreadList = (pthread_t *) calloc(sizeof(pthread_t), 4);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);
  T_par par[4];

  par[0].taos = taos[0];
  par[0].idx = 0;
  par[1].taos = taos[1];
  par[1].idx = 1;
  par[2].taos = taos[2];
  par[2].idx = 2;
  par[3].taos = taos[3];
  par[3].idx = 3;
  
  pthread_create(&(pThreadList[0]), &thattr, runcase, (void *)&par[0]);
  //pthread_create(&(pThreadList[1]), &thattr, runcase, (void *)&par[1]);
  //pthread_create(&(pThreadList[2]), &thattr, runcase, (void *)&par[2]);
  //pthread_create(&(pThreadList[3]), &thattr, runcase, (void *)&par[3]);

  while(1) {
    sleep(1);
  }
  return 0;
}

