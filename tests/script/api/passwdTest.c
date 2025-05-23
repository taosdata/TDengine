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
#ifdef WINDOWS
#include <winsock2.h>
#include <windows.h>
#include <stdint.h>

#ifndef PRId64
#define PRId64 "I64d"
#endif

#ifndef PRIu64
#define PRIu64 "I64u"
#endif

#else
#include <inttypes.h>
#include <unistd.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"  // TAOS header file

#define nDup     1
#define nRoot    1
#define nUser    1
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

void taosMsleep(int64_t ms) {
  if (ms < 0) return;
#ifdef WINDOWS
  Sleep(ms);
#else
  usleep((__useconds_t)ms * 1000);
#endif
}
	

int   nPassVerNotified = 0;
int   nUserDropped = 0;
TAOS *taosu[nRoot] = {0};
char  users[nUser][USER_LEN] = {0};

void __taos_notify_cb(void *param, void *ext, int type) {
  switch (type) {
    case TAOS_NOTIFY_PASSVER: {
      ++nPassVerNotified;
      printf("%s:%d type:%d user:%s passVer:%d\n", __func__, __LINE__, type, param ? (char *)param : "NULL",
             *(int *)ext);
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
  return 0;
}

static void queryDB(TAOS *taos, char *command, bool isPrintResult) {
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
    if (isPrintResult) {
      char output[BUF_LEN] = {0};
      printResult(pSql, output);
    }
  }
  taos_free_result(pSql);
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
  // sysInfoTest(taos, argv[1], qstr);
  // userDroppedTest(taos, argv[1], qstr);
  // clearTestEnv(taos, argv[1], qstr);

  taos_close(taos);
  taos_cleanup();
  exit(EXIT_SUCCESS);
}

void createUsers(TAOS *taos, const char *host, char *qstr) {
  // users
  for (int i = 0; i < nUser; ++i) {
    sprintf(users[i], "user%d", i);
    sprintf(qstr, "CREATE USER %s PASS 'taosdata'", users[i]);
    queryDB(taos, qstr, false);

    taosu[i] = taos_connect(host, users[i], "taosdata", NULL, 0);
    if (taosu[i] == NULL) {
      printf("failed to connect to server, user:%s, reason:%s\n", users[i], "null taos" /*taos_errstr(taos)*/);
      exit(1);
    }

    int code = taos_set_notify_cb(taosu[i], __taos_notify_cb, users[i], TAOS_NOTIFY_PASSVER);

    if (code != 0) {
      fprintf(stderr, "failed to run: taos_set_notify_cb(TAOS_NOTIFY_PASSVER) for user:%s since %d\n", users[i], code);
      exit(EXIT_FAILURE);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb(TAOS_NOTIFY_PASSVER) for user:%s\n", users[i]);
    }

    // alter pass for users
    sprintf(qstr, "alter user %s pass 'taos@123'", users[i]);
    queryDB(taos, qstr, false);
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
      exit(EXIT_FAILURE);
    } else {
      fprintf(stderr, "success to run: taos_set_notify_cb\n");
    }
  }

  queryDB(taos[0], "create database if not exists demo1 vgroups 1 minrows 10", false);
  queryDB(taos[0], "create database if not exists demo2 vgroups 1 minrows 10", false);
  queryDB(taos[0], "create database if not exists demo3 vgroups 1 minrows 10", false);

  queryDB(taos[0], "create table if not exists demo1.stb (ts timestamp, c1 int, c2 varchar(20)) tags(t1 int)", false);
  queryDB(taos[0], "create table if not exists demo1.`ctb0``` using demo1.stb tags('1')", false);
  queryDB(taos[0], "insert into demo1.`ctb0``` values(now, 1, 'a\\'bc')", false);
  queryDB(taos[0], "insert into demo1.`ctb0``` values(now, 2, 'd''ef')", false);
  queryDB(taos[0], "select * from demo1.`ctb0```", true);
  

  strcpy(qstr, "alter user root pass 'taos@123'");
  queryDB(taos[0], qstr, false);

  // calculate the nPassVerNotified for root and users
  int nConn = nRoot + nUser;

  for (int i = 0; i < 15; ++i) {
    printf("%s:%d [%d] second(s) elasped, passVer notification received:%d, total:%d\n", __func__, __LINE__, i,
           nPassVerNotified, nConn);
    if (nPassVerNotified >= nConn) break;
    taosMsleep(1000);
  }

  // close the taos_conn
  for (int i = 0; i < nRoot; ++i) {
    taos_close(taos[i]);
    printf("%s:%d close taos[%d]\n", __func__, __LINE__, i);
    // taosMsleep(1000);
  }

  for (int i = 0; i < nUser; ++i) {
    taos_close(taosu[i]);
    printf("%s:%d close taosu[%d]\n", __func__, __LINE__, i);
    // taosMsleep(1000);
  }

  fprintf(stderr, "######## %s #########\n", __func__);
  if (nPassVerNotified == nConn) {
    fprintf(stderr, ">>> succeed to get passVer notification since nNotify %d == nConn %d\n", nPassVerNotified, nConn);
  } else {
    fprintf(stderr, ">>> failed to get passVer notification since nNotify %d != nConn %d\n", nPassVerNotified, nConn);
    exit(1);
  }
  fprintf(stderr, "######## %s #########\n", __func__);
  // sleep(300);
}
