#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "taos.h"

int CTB_NUMS = 100;
int ROW_NUMS = 10;
int CYC_NUMS = 1;

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void do_ins(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  if (code) {
    printf("failed to query: %s, reason:%s\n", sql, taos_errstr(result));
    taos_free_result(result);
    return;
  }
  taos_free_result(result);
}

void createCtb(TAOS* taos, const char* tbname) {
  char* tmp = (char*)malloc(sizeof(char) * 100);
  sprintf(tmp, "create table db.%s using db.stb tags(0, 'after')", tbname);
  do_query(taos, tmp);
}

void initEnv(TAOS* taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b blob) tags(t1 int, t2 binary(10))");
  do_query(taos, "use db");
}

void do_stmt(TAOS* taos, const char* sql, bool hasTag) {
  initEnv(taos);

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  //   code = taos_stmt2_get_stb_fields(stmt, &fieldNum, &pFields);
  //   if (code != 0) {
  //     printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  //   } else {
  //     printf("col nums:%d\n", fieldNum);
  //     for (int i = 0; i < fieldNum; i++) {
  //       printf("field[%d]: %s, data_type:%d, field_type:%d\n", i, pFields[i].name, pFields[i].type,
  //              pFields[i].field_type);
  //     }
  //   }

  // tbname
  char** tbs = (char**)malloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)malloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    createCtb(taos, tbs[i]);
  }

  double bind_time = 0;
  double exec_time = 0;
  for (int r = 0; r < CYC_NUMS; r++) {
    // col params
    int64_t** ts = (int64_t**)malloc(CTB_NUMS * sizeof(int64_t*));
    char**    b = (char**)malloc(CTB_NUMS * sizeof(char*));
    int*      ts_len = (int*)malloc(ROW_NUMS * sizeof(int));
    int*      b_len = (int*)malloc(ROW_NUMS * sizeof(int));
    for (int i = 0; i < ROW_NUMS; i++) {
      ts_len[i] = sizeof(int64_t);
      b_len[i] = 1;
    }
    for (int i = 0; i < CTB_NUMS; i++) {
      ts[i] = (int64_t*)malloc(ROW_NUMS * sizeof(int64_t));
      b[i] = (char*)malloc(ROW_NUMS * sizeof(char));
      for (int j = 0; j < ROW_NUMS; j++) {
        ts[i][j] = 1591060628000 + r * 100000 + j;
        b[i][j] = 'a' + j;
      }
    }
    // tag params
    int t1 = 0;
    int t1len = sizeof(int);
    int t2len = 3;
    //   TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};

    // bind params
    TAOS_STMT2_BIND** paramv = (TAOS_STMT2_BIND**)malloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
    TAOS_STMT2_BIND** tags = (TAOS_STMT2_BIND**)malloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
    for (int i = 0; i < CTB_NUMS; i++) {
      // create tags
      tags[i] = (TAOS_STMT2_BIND*)malloc(2 * sizeof(TAOS_STMT2_BIND));
      tags[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_INT, &t1, &t1len, NULL, 0};
      tags[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, "after", &t2len, NULL, 0};

      // create col params
      paramv[i] = (TAOS_STMT2_BIND*)malloc(2 * sizeof(TAOS_STMT2_BIND));
      paramv[i][0] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
      paramv[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BLOB, &b[i][0], &b_len[0], NULL, ROW_NUMS};
    }
    // bind
    struct timespec start, end;
    clock_gettime(CLOCK_MONOTONIC, &start);  // 获取开始时间
    TAOS_STMT2_BINDV bindv;
    if (hasTag) {
      bindv = (TAOS_STMT2_BINDV){CTB_NUMS, tbs, tags, paramv};
    } else {
      bindv = (TAOS_STMT2_BINDV){CTB_NUMS, tbs, NULL, paramv};
    }
    if (taos_stmt2_bind_param(stmt, &bindv, -1)) {
      printf("failed to execute taos_stmt2_bind_param statement.error:%s\n", taos_stmt2_error(stmt));
      taos_stmt2_close(stmt);
      return;
    }

    clock_gettime(CLOCK_MONOTONIC, &end);  // 获取开始时间    TAOS_STMT2_BINDV bindv;
    bind_time += ((double)(end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9);

    clock_gettime(CLOCK_MONOTONIC, &start);  // 获取开始时间
    // exec
    if (taos_stmt2_exec(stmt, NULL)) {
      printf("failed to execute taos_stmt2_exec statement.error:%s\n", taos_stmt2_error(stmt));
      taos_stmt2_close(stmt);
      return;
    }
    clock_gettime(CLOCK_MONOTONIC, &end);  // 获取开始时间
    exec_time += ((double)(end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9);

    for (int i = 0; i < CTB_NUMS; i++) {
      free(tags[i]);
      free(paramv[i]);
      free(ts[i]);
      free(b[i]);
    }
    free(ts);
    free(b);
    free(ts_len);
    free(b_len);
    free(paramv);
    free(tags);
  }

  printf("stmt2-bind [%s] insert Time used: %f seconds\n", sql, bind_time);
  printf("stmt2-exec [%s] insert Time used: %f seconds\n", sql, exec_time);

  for (int i = 0; i < CTB_NUMS; i++) {
    free(tbs[i]);
  }
  free(tbs);

  //   taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void do_taosc(TAOS* taos) {
  initEnv(taos);
  // cols
  int64_t** ts = (int64_t*)malloc(CTB_NUMS * sizeof(int64_t));
  char**    b = (char**)malloc(CTB_NUMS * sizeof(char*));
  // ctbnames
  char** tbs = (char**)malloc(CTB_NUMS * sizeof(char*));

  // create table before insert
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)malloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    createCtb(taos, tbs[i]);
  }

  for (int i = 0; i < CTB_NUMS; i++) {
    ts[i] = (int64_t*)malloc(ROW_NUMS * sizeof(int64_t));
    b[i] = (char*)malloc(ROW_NUMS * sizeof(char));
    for (int j = 0; j < ROW_NUMS; j++) {
      ts[i][j] = 1591060628000 + j;
      b[i][j] = 'a' + j;
    }
  }
  for (int r = 0; r < CYC_NUMS; r++) {
    clock_t start, end;
    double  cpu_time_used;
    char*   tsc_sql = malloc(sizeof(char) * 1000000);
    sprintf(tsc_sql, "insert into db.stb(tbname,ts,b,t1,t2) values");

    for (int i = 0; i < CTB_NUMS; i++) {
      snprintf(tbs[i], sizeof(tbs[i]), "ctb_%d", i);
      for (int j = 0; j < ROW_NUMS; j++) {
        if (i == CTB_NUMS - 1 && j == ROW_NUMS - 1) {
          sprintf(tsc_sql + strlen(tsc_sql), "('%s',%lld,'%c',0,'after')", tbs[i], ts[i][j] + r * 10000, b[i][j]);
        } else {
          sprintf(tsc_sql + strlen(tsc_sql), "('%s',%lld,'%c',0,'after'),", tbs[i], ts[i][j] + r * 10000, b[i][j]);
        }
      }
    }

    start = clock();
    // printf("%s", tsc_sql);
    do_ins(taos, tsc_sql);
    end = clock();
    cpu_time_used = ((double)(end - start)) / CLOCKS_PER_SEC;
    printf("taosc insert Time used: %f seconds\n", cpu_time_used);
  }
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }

  // do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", true);
  // printf("no interlace\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", true);

  // do_stmt(taos, "insert into db.? values(?,?)", false);

  // do_taosc(taos);
  taos_close(taos);
  taos_cleanup();
}
