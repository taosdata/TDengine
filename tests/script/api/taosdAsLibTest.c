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
#include "taoserror.h"

#define nDup     5
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

#define TAOS_CHECK_EXIT(CMD)        \
  do {                              \
    code = (CMD);                   \
    if (code < TSDB_CODE_SUCCESS) { \
      lino = __LINE__;              \
      goto _exit;                   \
    }                               \
  } while (0)

static int32_t printResult(TAOS_RES *res, char *output);
static int32_t basicTest(TAOS *taosRoot, const char *host, char *qstr);

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

char output[1024] = {0};

static int32_t queryDB(TAOS *taos, char *command, bool isPrintResult) {
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
    fprintf(stderr, "[%d-%d]: exec: %s, code:%d, %s\n", nDup, i, command, code, tstrerror(code));
    if (0 == code) {
      break;
    }
    sleep(1);
  }

  if (code != 0) {
    fprintf(stderr, "failed to run: %s, reason: %s\n", command, taos_errstr(pSql));
    taos_free_result(pSql);
    return code;
  } else {
    fprintf(stderr, "success to run: %s\n", command);
  }

  if (isPrintResult) printResult(pSql, output);

  taos_free_result(pSql);
  return code;
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

static int32_t printResult(TAOS_RES *res, char *output) {
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
  int32_t code = 0, lino = 0;
  char    qstr[1024];
  TAOS   *taos = NULL;
  char   *host = argv[1];

  // connect to server
  if (argc < 2) {
    printf("please input server-ip \n");
    return 0;
  }

  if ((code = taos_init())) {
    printf("taos_init failed, reason:%s\n", tstrerror(code));
    TAOS_CHECK_EXIT(code);
  } else {
    printf("taos_init success\n");
  }

  taos = taos_connect(host, "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to server, reason:%s\n", "null taos" /*taos_errstr(taos)*/);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(basicTest(taos, host, qstr));

_exit:
  if (code) {
    printf("failed to run %s, reason: %s\n", argv[0], tstrerror(code));
  } else {
    printf("success to run %s\n", argv[0]);
  }
  printf("taos close\n");
  taos_close(taos);
  printf("taos clean up\n");
  taos_cleanup();
  printf("exited\n");
  return code;
}

int32_t basicTest(TAOS *taosRoot, const char *host, char *qstr) {
  int32_t code = 0, lino = 0;
  TAOS_CHECK_EXIT(queryDB(taosRoot, "show dnodes", true));

  TAOS_CHECK_EXIT(queryDB(taosRoot, "create database if not exists demo11 vgroups 1 minrows 10", false));
  TAOS_CHECK_EXIT(queryDB(taosRoot, "create database if not exists demo12 vgroups 1 minrows 10", false));
  TAOS_CHECK_EXIT(queryDB(taosRoot, "create database if not exists demo13 vgroups 1 minrows 10", false));

  TAOS_CHECK_EXIT(
      queryDB(taosRoot, "create table if not exists demo11.stb (ts timestamp, c1 int) tags(t1 int)", false));
  TAOS_CHECK_EXIT(
      queryDB(taosRoot, "create table if not exists demo12.stb (ts timestamp, c1 int) tags(t1 int)", false));
  TAOS_CHECK_EXIT(
      queryDB(taosRoot, "create table if not exists demo13.stb (ts timestamp, c1 int) tags(t1 int)", false));
_exit:
  return code;
}