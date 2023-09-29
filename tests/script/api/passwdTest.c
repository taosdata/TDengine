/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

// TAOS standard API example. The same syntax as MySQL, but only a subset
// to compile: gcc -o demo demo.c -ltaos

/**
 *  passwdTest.c
 *   - Run the test case in clear TDengine environment with default root passwd 'taosdata'
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"  // TAOS header file

#define nDup     1
#define nRoot    10
#define nUser    10
#define USER_LEN 24
#define BUF_LEN  1024

typedef uint16_t VarDataLenT;

#define TSDB_NCHAR_SIZE    sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x)  (*(float *)(x))
#define GET_DOUBLE_VAL(x) (*(double *)(x))

#define varDataLen(v) ((VarDataLenT *)(v))[0]

void createUsers(TAOS *taos, const char *host, char *qstr);
void passVerTestMulti(const char *host, char *qstr);
void sysInfoTest(TAOS *taos, const char *host, char *qstr);
void userDroppedTest(TAOS *taos, const char *host, char *qstr);
void clearTestEnv(TAOS *taos, const char *host, char *qstr);

int   nPassVerNotified = 0;
int   nUserDropped = 0;
TAOS *taosu[nRoot] = {0};
char  users[nUser][USER_LEN] = {0};

void __taos_notify_cb(void *param, void *ext, int type) {
  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      ++nPassVerNotified;
      printf("%s:%d type:%d user:%s passVer:%d\n", __func__, __LINE__, type, param ? (char *)param : "NULL", *(int *)ext);
      break;
    }
    case TAOS_NOTIFY_USER_DROPPED: {
      ++nUserDropped;
      printf("%s:%d type:%d user:%s dropped\n", __func__, __LINE__, type, param ? (char *)param : "NULL");
      break;
    }
    default:
      printf("%s:%d unknown notify type:%d\n", __func__, __LINE__, type);
      break;
  }
}

static void queryDB(TAOS *taos, char *command) {
  int       i;
  TAOS_RES *pSql = NULL;
  int32_t   code = -1;

  for (i = 0; i < nDup; ++i) {
    if (NULL != pSql) {
      taos_free_result(pSql);
      pSql = NULL;
    }

    pSql = taos_query(taos, command);
    code = taos_errno(pSql);
    if (0 == code) {
      break;
    }
  }

  if (code != 0) {
    fprintf(stderr, "failed to run: %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    taos_close(taos);
    exit(EXIT_FAILURE);
  } else {
    fprintf(stderr, "success to run: %s\n", command);
  }

  taos_free_result(pSql);
}

int printRow(char *str, TAOS_ROW row, TAOS_FIELD *fields, int numFields) {
  int  len = 0;
  char split = ' ';

  for (int i = 0; i < numFields; ++i) {
    if (i > 0) {
      str[len++] = split;
    }

    if (row[i] == NULL) {
      len += sprintf(str + len, "%s", "NULL");
      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_TINYINT:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_UTINYINT:
        len += sprintf(str + len, "%u", *((uint8_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        len += sprintf(str + len, "%d", *((int16_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        len += sprintf(str + len, "%u", *((uint16_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_INT:
        len += sprintf(str + len, "%d", *((int32_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_UINT:
        len += sprintf(str + len, "%u", *((uint32_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        len += sprintf(str + len, "%" PRIu64, *((uint64_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float fv = 0;
        fv = GET_FLOAT_VAL(row[i]);
        len += sprintf(str + len, "%f", fv);
      } break;
      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = 0;
        dv = GET_DOUBLE_VAL(row[i]);
        len += sprintf(str + len, "%lf", dv);
      } break;
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_VARBINARY:
      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_GEOMETRY: {
        int32_t charLen = varDataLen((char *)row[i] - VARSTR_HEADER_SIZE);
        memcpy(str + len, row[i], charLen);
        len += charLen;
      } break;
      case TSDB_DATA_TYPE_TIMESTAMP:
        len += sprintf(str + len, "%" PRId64, *((int64_t *)row[i]));
        break;
      case TSDB_DATA_TYPE_BOOL:
        len += sprintf(str + len, "%d", *((int8_t *)row[i]));
      default:
        break;
    }
  }
  return len;
}

static int printResult(TAOS_RES *res, char *output) {
  int         numFields = taos_num_fields(res);
  TAOS_FIELD *fields = taos_fetch_fields(res);
  char        header[BUF_LEN] = {0};
  int         len = 0;
  for (int i = 0; i < numFields; ++i) {
    len += sprintf(header + len, "%s ", fields[i].name);
  }
  puts(header);
  if (output) {
    strncpy(output, header, BUF_LEN);
  }

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(res))) {
    char temp[BUF_LEN] = {0};
    printRow(temp, row, fields, numFields);
    puts(temp);
  }
}

int main(int argc, char *argv[]) {
  char qstr[1024];

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  TAOS *taos = taos_connect(argv[1], "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    exit(1);
  }
  createUsers(taos, argv[1], qstr);
  passVerTestMulti(argv[1], qstr);
  sysInfoTest(taos, argv[1], qstr);
  userDroppedTest(taos, argv[1], qstr);
  clearTestEnv(taos, argv[1], qstr);

  taos_close(taos);
  taos_cleanup();
}

void createUsers(TAOS *taos, const char *host, char *qstr) {
  // users
  for (int i = 0; i < nUser; ++i) {
    sprintf(users[i], "user%d", i);
    sprintf(qstr, "CREATE USER %s PASS 'taosdata'", users[i]);
    queryDB(taos, qstr);

    taosu[i] = taos_connect(host, users[i], "taosdata", NULL, 0);
    if (taosu[i] == NULL) {
      printf("failed to connect to server, user:%s, reason:%s\n", users[i], "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }

    int code = taos_set_notify_cb(taosu[i], __taos_notify_cb, users[i], TAOS_NOTIFY_PASSVER);

    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb(TAOS_NOTIFY_PASSVER) for user:%s since %d\n", users[i], code);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb(TAOS_NOTIFY_PASSVER) for user:%s\n", users[i]);
    }

    // alter pass for users
    sprintf(qstr, "alter user %s pass 'taos'", users[i]);
    queryDB(taos, qstr);
  }
}

void passVerTestMulti(const char *host, char *qstr) {
  // root
  TAOS *taos[nRoot] = {0};
  char  userName[USER_LEN] = "root";

  for (int i = 0; i < nRoot; ++i) {
    taos[i] = taos_connect(host, "root", "taosdata", NULL, 0);
    if (taos[i] == NULL) {
      printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }

    int code = taos_set_notify_cb(taos[i], __taos_notify_cb, userName, TAOS_NOTIFY_PASSVER);

    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb since %d\n", code);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb\n");
    }
  }

  queryDB(taos[0], "create database if not exists demo1 vgroups 1 minrows 10");
  queryDB(taos[0], "create database if not exists demo2 vgroups 1 minrows 10");
  queryDB(taos[0], "create database if not exists demo3 vgroups 1 minrows 10");

  queryDB(taos[0], "create table if not exists demo1.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taos[0], "create table if not exists demo2.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taos[0], "create table if not exists demo3.stb (ts timestamp, c1 int) tags(t1 int)");

  strcpy(qstr, "alter user root pass 'taos'");
  queryDB(taos[0], qstr);

  // calculate the nPassVerNotified for root and users
  int nConn = nRoot + nUser;

  for (int i = 0; i < 15; ++i) {
    printf("%s:%d [%d] second(s) elasped, passVer notification received:%d, total:%d\n", __func__, __LINE__, i,
           nPassVerNotified, nConn);
    if (nPassVerNotified >= nConn) break;
    sleep(1);
  }

  // close the taos_conn
  for (int i = 0; i < nRoot; ++i) {
    taos_close(taos[i]);
    printf("%s:%d close taos[%d]\n", __func__, __LINE__, i);
    // sleep(1);
  }

  for (int i = 0; i < nUser; ++i) {
    taos_close(taosu[i]);
    printf("%s:%d close taosu[%d]\n", __func__, __LINE__, i);
    // sleep(1);
  }

  fprintf(stderr, "######## %s #########\n", __func__);
  if (nPassVerNotified == nConn) {
    fprintf(stderr, ">>> succeed to get passVer notification since nNotify %d == nConn %d\n", nPassVerNotified,
            nConn);
  } else {
    fprintf(stderr, ">>> failed to get passVer notification since nNotify %d != nConn %d\n", nPassVerNotified, nConn);
    exit(1);
  }
  fprintf(stderr, "######## %s #########\n", __func__);
  // sleep(300);
}

void sysInfoTest(TAOS *taosRoot, const char *host, char *qstr) {
  fprintf(stderr, "######## %s entry #########\n", __func__);
  TAOS *taos[nRoot] = {0};
  char  userName[USER_LEN] = "user0";

  for (int i = 0; i < nRoot; ++i) {
    taos[i] = taos_connect(host, "user0", "taos", NULL, 0);
    if (taos[i] == NULL) {
      fprintf(stderr, "failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }
  }

  queryDB(taosRoot, "create database if not exists demo11 vgroups 1 minrows 10");
  queryDB(taosRoot, "create database if not exists demo12 vgroups 1 minrows 10");
  queryDB(taosRoot, "create database if not exists demo13 vgroups 1 minrows 10");

  queryDB(taosRoot, "create table if not exists demo11.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taosRoot, "create table if not exists demo12.stb (ts timestamp, c1 int) tags(t1 int)");
  queryDB(taosRoot, "create table if not exists demo13.stb (ts timestamp, c1 int) tags(t1 int)");

  sprintf(qstr, "show grants");
  char      output[BUF_LEN];
  TAOS_RES *res = NULL;
  int32_t   nRep = 0;

_REP: 
  fprintf(stderr, "######## %s loop:%d #########\n", __func__, nRep);
  res = taos_query(taos[0], qstr);
  if (taos_errno(res) != 0) {
    fprintf(stderr, "%s:%d failed to execute: %s since %s\n", __func__, __LINE__, qstr, taos_errstr(res));
    taos_free_result(res);
    exit(EXIT_FAILURE);
  }
  printResult(res, output);
  taos_free_result(res);
  if (!strstr(output, "timeseries")) {
    fprintf(stderr, "%s:%d expected output: 'timeseries' not occur\n", __func__, __LINE__);
    exit(EXIT_FAILURE);
  }

  queryDB(taosRoot, "alter user user0 sysinfo 0");

  fprintf(stderr, "%s:%d sleep 2 seconds to wait HB take effect\n", __func__, __LINE__);
  for (int i = 1; i <= 2; ++i) {
    sleep(1);
  }

  res = taos_query(taos[0], qstr);
  if (taos_errno(res) != 0) {
    if (!strstr(taos_errstr(res), "Permission denied")) {
      fprintf(stderr, "%s:%d expected error: 'Permission denied' not occur\n", __func__, __LINE__);
      taos_free_result(res);
      exit(EXIT_FAILURE);
    }
  }
  taos_free_result(res);

  queryDB(taosRoot, "alter user user0 sysinfo 1");
  fprintf(stderr, "%s:%d sleep 2 seconds to wait HB take effect\n", __func__, __LINE__);
  for (int i = 1; i <= 2; ++i) {
    sleep(1);
  }

  if(++nRep < 5) {
    goto _REP;
  }

  // close the taos_conn
  for (int i = 0; i < nRoot; ++i) {
    taos_close(taos[i]);
    fprintf(stderr, "%s:%d close taos[%d]\n", __func__, __LINE__, i);
  }

  fprintf(stderr, "######## %s #########\n", __func__);
  fprintf(stderr, ">>> succeed to run sysInfoTest\n");
  fprintf(stderr, "######## %s #########\n", __func__);
}
static bool isDropUser = true;
void userDroppedTest(TAOS *taos, const char *host, char *qstr) {
  // users
  int nTestUsers = nUser;
  int nLoop = 0;
_loop:
  ++nLoop;
  printf("\n\n%s:%d LOOP %d, nTestUsers:%d\n", __func__, __LINE__, nLoop, nTestUsers);
  for (int i = 0; i < nTestUsers; ++i) {
    // sprintf(users[i], "user%d", i);
    taosu[i] = taos_connect(host, users[i], "taos", NULL, 0);
    if (taosu[i] == NULL) {
      printf("failed to connect to server, user:%s, reason:%s\n", users[i], "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }
    int code = taos_set_notify_cb(taosu[i], __taos_notify_cb, users[i], TAOS_NOTIFY_USER_DROPPED);
    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb:%d for user:%s since %d\n", TAOS_NOTIFY_USER_DROPPED, users[i],
              code);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb:%d for user:%s\n", TAOS_NOTIFY_USER_DROPPED, users[i]);
    }
  }

  for (int i = 0; i < nTestUsers; ++i) {
    // drop user0 ... user${nUser}
    sprintf(qstr, "drop user %s", users[i]);
    queryDB(taos, qstr);
  }

  // calculate the nUserDropped for users
  int nConn = nTestUsers;

  for (int i = 0; i < 15; ++i) {
    printf("%s:%d [%d] second(s) elasped, user dropped notification received:%d, total:%d\n", __func__, __LINE__, i,
           nUserDropped, nConn);
    if (nUserDropped >= nConn) break;
    sleep(1);
  }

  for (int i = 0; i < nTestUsers; ++i) {
    taos_close(taosu[i]);
    printf("%s:%d close taosu[%d]\n", __func__, __LINE__, i);
  }

  fprintf(stderr, "######## %s #########\n", __func__);
  if (nUserDropped == nConn) {
    fprintf(stderr, ">>> succeed to get user dropped notification since nNotify %d == nConn %d\n", nUserDropped, nConn);
  } else {
    fprintf(stderr, ">>> failed to get user dropped notification since nNotify %d != nConn %d\n", nUserDropped, nConn);
    exit(1);
  }
  fprintf(stderr, "######## %s #########\n", __func__);

  if (nLoop < 5) {
    nUserDropped = 0;
    for (int i = 0; i < nTestUsers; ++i) {
      sprintf(users[i], "user%d", i);
      sprintf(qstr, "CREATE USER %s PASS 'taos'", users[i]);
      fprintf(stderr, "%s:%d create user:%s\n", __func__, __LINE__, users[i]);
      queryDB(taos, qstr);
    }
    goto _loop;
  }
  isDropUser = false;
}

void clearTestEnv(TAOS *taos, const char *host, char *qstr) {
  fprintf(stderr, "######## %s start #########\n", __func__);
  // restore  password
  sprintf(qstr, "alter user root pass 'taosdata'");
  queryDB(taos, qstr);

  if (isDropUser) {
    for (int i = 0; i < nUser; ++i) {
      sprintf(qstr, "drop user %s", users[i]);
      queryDB(taos, qstr);
    }
  }
  // sleep(3000);
  fprintf(stderr, "######## %s end #########\n", __func__);
}