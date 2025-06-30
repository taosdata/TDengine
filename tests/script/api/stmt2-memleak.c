/*
    test stmt2 memleak
    gcc -o stmt2-memleak stmt2-memleak.c -ltaos
    valgrind --tool=memcheck --leak-check=full --show-reachable=no --track-origins=yes --show-leak-kinds=all -v
   --workaround-gcc296-bugs=yes --log-file=/tmp/val.log ./stmt2-memleak
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"

int CTB_NUMS = 3;
int ROW_NUMS = 3;
int CYC_NUMS = 3;

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

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {}

void initEnv(TAOS* taos) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "use db");
}

void createCtb(TAOS* taos, const char* tbname) {
  char* tmp = (char*)malloc(sizeof(char) * 100);
  sprintf(tmp, "create table db.%s using db.stb tags(0, 'after')", tbname);
  do_query(taos, tmp);
}

void do_stmt(TAOS* taos, const char* sql, bool interlaceMode, bool hasTag, bool preCreateTb, bool asyncExec) {
  initEnv(taos);

  TAOS_STMT2_OPTION option;
  if (interlaceMode && asyncExec) {
    option = (TAOS_STMT2_OPTION){0, true, true, stmtAsyncQueryCb, NULL};
  } else if (interlaceMode && !asyncExec) {
    option = (TAOS_STMT2_OPTION){0, true, true, NULL, NULL};
  } else if (!interlaceMode && !asyncExec) {
    option = (TAOS_STMT2_OPTION){0, false, true, NULL, NULL};
  } else {
    option = (TAOS_STMT2_OPTION){0, false, true, stmtAsyncQueryCb, NULL};
  }

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  }
  taos_stmt2_free_fields(stmt, pFields);

  // tbname
  char** tbs = (char**)malloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)malloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    if (preCreateTb) {
      createCtb(taos, tbs[i]);
    }
  }

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
      paramv[i][1] = (TAOS_STMT2_BIND){TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
    }
    // bind
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

    // exec
    if (taos_stmt2_exec(stmt, NULL)) {
      printf("failed to execute taos_stmt2_exec statement.error:%s\n", taos_stmt2_error(stmt));
      taos_stmt2_close(stmt);
      return;
    }

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
  for (int i = 0; i < CTB_NUMS; i++) {
    free(tbs[i]);
  }
  free(tbs);

  taos_stmt2_close(stmt);
}

int main() {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!taos) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    exit(1);
  }
  //   interlace
  printf("[test begin]interlace:true, hagTag:true, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", true, true, true, true);

  printf("[test begin]interlace:true, hagTag:true, preCreateTb:false, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", true, true, false, true);

  printf("[test begin]interlace:true, hagTag:false, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b) values(?,?,?)", true, false, true, true);

  printf("[test begin]interlace:true, hagTag:false, preCreateTb:true, asyncExec:false\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b) values(?,?,?)", true, false, true, false);
  //  none-interlace
  printf("[test begin]interlace:false, hagTag:true, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", false, true, true, true);

  printf("[test begin]interlace:false, hagTag:true, preCreateTb:false, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", false, true, false, true);

  printf("[test begin]interlace:false, hagTag:false, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b) values(?,?,?)", false, false, true, true);

  printf("[test begin]interlace:false, hagTag:false, preCreateTb:true, asyncExec:false\n");
  do_stmt(taos, "insert into `db`.`stb` (tbname,ts,b) values(?,?,?)", false, false, true, false);

  // interlace
  printf("[test begin]interlace:true, hagTag:true, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", true, true, true, true);

  printf("[test begin]interlace:true, hagTag:true, preCreateTb:false, asyncExec:true\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", true, true, false, true);

  printf("[test begin]interlace:true, hagTag:true, preCreateTb:true, asyncExec:false\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", true, true, true, false);

  printf("[test begin]interlace:true, hagTag:true, preCreateTb:true, asyncExec:false\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", true, true, false, false);
  //  none-interlace
  printf("[test begin]interlace:false, hagTag:true, preCreateTb:true, asyncExec:true\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", false, true, true, true);

  printf("[test begin]interlace:false, hagTag:true, preCreateTb:false, asyncExec:true\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", false, true, false, true);

  printf("[test begin]interlace:false, hagTag:true, preCreateTb:true, asyncExec:false\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", false, true, true, false);

  printf("[test begin]interlace:false, hagTag:true, preCreateTb:false, asyncExec:false\n");
  do_stmt(taos, "insert into db.? using db.stb tags(?,?)values(?,?)", false, true, false, false);

  printf("all test finish\n");
  taos_close(taos);
  taos_cleanup();
}