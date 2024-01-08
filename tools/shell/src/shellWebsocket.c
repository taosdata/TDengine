
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
#ifdef WEBSOCKET
#include <taosws.h>
#include <shellInt.h>

// save current database name
char curDBName[128] = ""; // TDB_MAX_DBNAME_LEN is 24, put large

int shell_conn_ws_server(bool first) {
  char cuttedDsn[SHELL_WS_DSN_BUFF] = {0};
  int dsnLen = strlen(shell.args.dsn);
  snprintf(cuttedDsn,
           ((dsnLen-SHELL_WS_DSN_MASK) > SHELL_WS_DSN_BUFF)?
            SHELL_WS_DSN_BUFF:(dsnLen-SHELL_WS_DSN_MASK),
           "%s", shell.args.dsn);
  fprintf(stdout, "trying to connect %s****** ", cuttedDsn);
  fflush(stdout);
  for (int i = 0; i < shell.args.timeout; i++) {
    if(shell.args.is_bi_mode) {
      size_t len = strlen(shell.args.dsn);
      char * dsn = taosMemoryMalloc(len + 32);
      sprintf(dsn, "%s&conn_mode=1", shell.args.dsn);
      shell.ws_conn = ws_connect_with_dsn(dsn);
      taosMemoryFree(dsn);
    } else {
      shell.ws_conn = ws_connect_with_dsn(shell.args.dsn);
    }

    if (NULL == shell.ws_conn) {
      int errNo = ws_errno(NULL);
      if (0xE001 == errNo) {
        fprintf(stdout, ".");
        fflush(stdout);
        taosMsleep(1000);  // sleep 1 second then try again
        continue;
      } else {
        fprintf(stderr, "\nfailed to connect %s***, reason: %s\n",
            cuttedDsn, ws_errstr(NULL));
        return -1;
      }
    } else {
      break;
    }
  }
  if (NULL == shell.ws_conn) {
    fprintf(stdout, "\n timeout\n");
    fprintf(stderr, "\nfailed to connect %s***, reason: %s\n",
      cuttedDsn, ws_errstr(NULL));
    return -1;
  } else {
    fprintf(stdout, "\n");
  }
  if (first && shell.args.restful) {
    fprintf(stdout, "successfully connected to %s\n\n",
        shell.args.dsn);
  } else if (first && shell.args.cloud) {
    if(shell.args.local) {
      const char* host = strstr(shell.args.dsn, "@");
      if(host) {
        host += 1;
      } else {
        host = shell.args.dsn;
      }
      fprintf(stdout, "successfully connected to %s\n", host);
    } else {
      fprintf(stdout, "successfully connected to cloud service\n");
    }
  }
  fflush(stdout);

  // switch to current database if have
  if(curDBName[0] !=0) {
    char command[256];
    sprintf(command, "use %s;", curDBName);
    shellRunSingleCommandWebsocketImp(command);
  }

  return 0;
}

static int horizontalPrintWebsocket(WS_RES* wres, double* execute_time) {
  const void* data = NULL;
  int rows;
  ws_fetch_block(wres, &data, &rows);
  if (wres) {
    *execute_time += (double)(ws_take_timing(wres)/1E6);
  }
  if (!rows) {
    return 0;
  }
  int num_fields = ws_field_count(wres);
  TAOS_FIELD* fields = (TAOS_FIELD*)ws_fetch_fields(wres);
  int precision = ws_result_precision(wres);

  int width[TSDB_MAX_COLUMNS];
  for (int col = 0; col < num_fields; col++) {
    width[col] = shellCalcColWidth(fields + col, precision);
  }

  shellPrintHeader(fields, width, num_fields);

  int numOfRows = 0;
  do {
    uint8_t ty;
    uint32_t len;
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < num_fields; j++) {
        putchar(' ');
        const void *value = ws_get_value_in_block(wres, i, j, &ty, &len);
        shellPrintField((const char*)value, fields+j, width[j], len, precision);
        putchar(' ');
        putchar('|');
      }
      putchar('\r');
      putchar('\n');
    }
    numOfRows += rows;
    ws_fetch_block(wres, &data, &rows);
  } while (rows && !shell.stop_query);
  return numOfRows;
}

static int verticalPrintWebsocket(WS_RES* wres, double* pexecute_time) {
  int rows = 0;
  const void* data = NULL;
  ws_fetch_block(wres, &data, &rows);
  if (wres) {
    *pexecute_time += (double)(ws_take_timing(wres)/1E6);
  }
  if (!rows) {
    return 0;
  }
  int num_fields = ws_field_count(wres);
  TAOS_FIELD* fields = (TAOS_FIELD*)ws_fetch_fields(wres);
  int precision = ws_result_precision(wres);

  int maxColNameLen = 0;
  for (int col = 0; col < num_fields; col++) {
    int len = (int)strlen(fields[col].name);
    if (len > maxColNameLen) {
      maxColNameLen = len;
    }
  }
  int numOfRows = 0;
  do {
    uint8_t ty;
    uint32_t len;
    for (int i = 0; i < rows; i++) {
      printf("*************************** %d.row ***************************\n",
        numOfRows + 1);
      for (int j = 0; j < num_fields; j++) {
        TAOS_FIELD* field = fields + j;
        int padding = (int)(maxColNameLen - strlen(field->name));
        printf("%*.s%s: ", padding, " ", field->name);
        const void *value = ws_get_value_in_block(wres, i, j, &ty, &len);
        shellPrintField((const char*)value, field, 0, len, precision);
        putchar('\n');
      }
      numOfRows++;
    }
    ws_fetch_block(wres, &data, &rows);
  } while (rows && !shell.stop_query);
  return numOfRows;
}

static int dumpWebsocketToFile(const char* fname, WS_RES* wres,
                               double* pexecute_time) {
  char fullname[PATH_MAX] = {0};
  if (taosExpandDir(fname, fullname, PATH_MAX) != 0) {
    tstrncpy(fullname, fname, PATH_MAX);
  }

  TdFilePtr pFile = taosOpenFile(fullname,
      TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "failed to open file: %s\r\n", fullname);
    return -1;
  }
  int rows = 0;
  const void* data = NULL;
  ws_fetch_block(wres, &data, &rows);
  if (wres) {
    *pexecute_time += (double)(ws_take_timing(wres)/1E6);
  }
  if (!rows) {
    taosCloseFile(&pFile);
    return 0;
  }
  int numOfRows = 0;
  TAOS_FIELD* fields = (TAOS_FIELD*)ws_fetch_fields(wres);
  int num_fields = ws_field_count(wres);
  int precision = ws_result_precision(wres);
  for (int col = 0; col < num_fields; col++) {
    if (col > 0) {
      taosFprintfFile(pFile, ",");
    }
    taosFprintfFile(pFile, "%s", fields[col].name);
  }
  taosFprintfFile(pFile, "\r\n");
  do {
    uint8_t ty;
    uint32_t len;
    numOfRows += rows;
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < num_fields; j++) {
        if (j > 0) {
          taosFprintfFile(pFile, ",");
        }
        const void *value = ws_get_value_in_block(wres, i, j, &ty, &len);
        shellDumpFieldToFile(pFile, (const char*)value,
                             fields + j, len, precision);
      }
      taosFprintfFile(pFile, "\r\n");
    }
    ws_fetch_block(wres, &data, &rows);
  } while (rows && !shell.stop_query);
  taosCloseFile(&pFile);
  return numOfRows;
}

static int shellDumpWebsocket(WS_RES *wres, char *fname,
                              int *error_no, bool vertical,
                              double* pexecute_time) {
  int numOfRows = 0;
  if (fname != NULL) {
    numOfRows = dumpWebsocketToFile(fname, wres, pexecute_time);
  } else if (vertical) {
    numOfRows = verticalPrintWebsocket(wres, pexecute_time);
  } else {
    numOfRows = horizontalPrintWebsocket(wres, pexecute_time);
  }
  *error_no = ws_errno(wres);
  return numOfRows;
}

char * strendG(const char* pstr);
void shellRunSingleCommandWebsocketImp(char *command) {
  int64_t st, et;
  char   *sptr = NULL;
  char   *cptr = NULL;
  char   *fname = NULL;
  bool    printMode = false;

  if ((sptr = strstr(command, ">>")) != NULL) {
    fname = sptr + 2;
    while (*fname == ' ') fname++;
    *sptr = '\0';

    cptr = strstr(fname, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }
  }

  if ((sptr = strendG(command)) != NULL) {
    *sptr = '\0';
    printMode = true;  // When output to a file, the switch does not work.
  }

  shell.stop_query = false;
  WS_RES* res;

  for (int reconnectNum = 0; reconnectNum < 2; reconnectNum++) {
    if (!shell.ws_conn && shell_conn_ws_server(0) || shell.stop_query) {
      return;
    }
    st = taosGetTimestampUs();

    res = ws_query_timeout(shell.ws_conn, command, shell.args.timeout);
    int code = ws_errno(res);
    if (code != 0 && !shell.stop_query) {
      // if it's not a ws connection error
      if (TSDB_CODE_WS_DSN_ERROR != (code&TSDB_CODE_WS_DSN_ERROR)) {
        et = taosGetTimestampUs();
        fprintf(stderr, "\nDB: error: %s (%.6fs)\n",
                ws_errstr(res), (et - st)/1E6);
        ws_free_result(res);
        return;
      }
      if (code == TSDB_CODE_WS_SEND_TIMEOUT
                || code == TSDB_CODE_WS_RECV_TIMEOUT) {
        fprintf(stderr, "Hint: use -T to increase the timeout in seconds\n");
      } else if (code == TSDB_CODE_WS_INTERNAL_ERRO
                    || code == TSDB_CODE_WS_CLOSED) {
        shell.ws_conn = NULL;
      }
      ws_free_result(res);
      if (reconnectNum == 0) {
        continue;
      } else {
        fprintf(stderr, "The server is disconnected, will try to reconnect\n");
      }
      return;
    }
    break;
  }

  double execute_time = 0;
  if (res) {
    execute_time = ws_take_timing(res)/1E6;
  }

  if (shellRegexMatch(command, "^\\s*use\\s+[a-zA-Z0-9_]+\\s*;\\s*$",
                      REG_EXTENDED | REG_ICASE)) {

    // copy dbname to curDBName
    char *p         = command;
    bool firstStart = false;
    bool firstEnd   = false;
    int  i          = 0;
    while (*p != 0) {
      if (*p != ' ') {
        // not blank
        if (!firstStart) {
          firstStart = true;
        } else if (firstEnd) {
          if(*p == ';' && *p != '\\') {
            break;
          }
          // database name
          curDBName[i++] = *p;
          if(i + 4 > sizeof(curDBName)) {
            // DBName is too long, reset zero and break
            i = 0;
            break;
          }
        }
      } else {
        // blank
        if(firstStart == true && firstEnd == false){
          firstEnd = true;
        }
        if(firstStart && firstEnd && i > 0){
          // blank after database name
          break;
        }
      }
      // move next
      p++;
    }
    // append end
    curDBName[i] = 0;

    fprintf(stdout, "Database changed to %s.\r\n\r\n", curDBName);
    fflush(stdout);
    ws_free_result(res);
    return;
  }

  int numOfRows = 0;
  if (ws_is_update_query(res)) {
    numOfRows = ws_affected_rows(res);
    et = taosGetTimestampUs();
    double total_time = (et - st)/1E3;
    double net_time = total_time - (double)execute_time;
    printf("Query Ok, %d of %d row(s) in database\n", numOfRows, numOfRows);
    printf("Execute: %.2f ms Network: %.2f ms Total: %.2f ms\n",
           execute_time, net_time, total_time);
  } else {
    int error_no = 0;
    numOfRows  = shellDumpWebsocket(res, fname, &error_no,
                                    printMode, &execute_time);
    if (numOfRows < 0) {
      ws_free_result(res);
      return;
    }
    et = taosGetTimestampUs();
    double total_time = (et - st) / 1E3;
    double net_time = total_time - execute_time;
    if (error_no == 0 && !shell.stop_query) {
      printf("Query OK, %d row(s) in set\n", numOfRows);
      printf("Execute: %.2f ms Network: %.2f ms Total: %.2f ms\n",
             execute_time, net_time, total_time);
    } else {
      printf("Query interrupted, %d row(s) in set (%.6fs)\n", numOfRows,
          (et - st)/1E6);
    }
  }
  printf("\n");
  ws_free_result(res);
}
#endif
