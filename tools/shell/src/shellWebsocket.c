
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
#include "taosws.h"
#include "shellInt.h"

int shell_conn_ws_server(bool first) {
	shell.ws_conn = ws_connect_with_dsn(shell.args.dsn);
	if (!shell.ws_conn) {
		fprintf(stderr, "failed to connect %s, reason: %s\n",
				shell.args.dsn, ws_errstr(NULL));
		return -1;
	}
	if (first && shell.args.restful) {
		fprintf(stdout, "successfully connect to %s\n\n",
				shell.args.dsn);
	} else if (first && shell.args.cloud) {
		fprintf(stdout, "successfully connect to cloud service\n");
	}
	return 0;
}

static int horizontalPrintWebsocket(WS_RES* wres) {
  const void* data = NULL;
  int rows;
  ws_fetch_block(wres, &data, &rows);
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

static int verticalPrintWebsocket(WS_RES* wres) {
  int rows = 0;
  const void* data = NULL;
  ws_fetch_block(wres, &data, &rows);
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

static int dumpWebsocketToFile(const char* fname, WS_RES* wres) {
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
        shellDumpFieldToFile(pFile, (const char*)value, fields + j, len, precision);
      }
      taosFprintfFile(pFile, "\r\n");
    }
    ws_fetch_block(wres, &data, &rows);
  } while (rows && !shell.stop_query);
  taosCloseFile(&pFile);
  return numOfRows;
}

static int shellDumpWebsocket(WS_RES *wres, char *fname, int *error_no, bool vertical) {
  int numOfRows = 0;
  if (fname != NULL) {
    numOfRows = dumpWebsocketToFile(fname, wres);
  } else if (vertical) {
    numOfRows = verticalPrintWebsocket(wres);
  } else {
    numOfRows = horizontalPrintWebsocket(wres);
  }
  *error_no = ws_errno(wres);
  return numOfRows;
}

void shellRunSingleCommandWebsocketImp(char *command) {
  int64_t st, et;
  char   *sptr = NULL;
  char   *cptr = NULL;
  char   *fname = NULL;
  bool    printMode = false;

  if ((sptr = strstr(command, ">>")) != NULL) {
    cptr = strstr(command, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }

    fname = sptr + 2;
    while (*fname == ' ') fname++;
    *sptr = '\0';
  }

  if ((sptr = strstr(command, "\\G")) != NULL) {
    cptr = strstr(command, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }

    *sptr = '\0';
    printMode = true;  // When output to a file, the switch does not work.
  }

  if (!shell.ws_conn && shell_conn_ws_server(0)) {
	return;
  }

  shell.stop_query = false;
  st = taosGetTimestampUs();

  WS_RES* res = ws_query_timeout(shell.ws_conn, command, shell.args.timeout);
  int code = ws_errno(res);
  if (code != 0) {
	et = taosGetTimestampUs();
	fprintf(stderr, "\nDB: error: %s (%.6fs)\n", ws_errstr(res), (et - st)/1E6);
	if (code == TSDB_CODE_WS_SEND_TIMEOUT || code == TSDB_CODE_WS_RECV_TIMEOUT) {
		fprintf(stderr, "Hint: use -t to increase the timeout in seconds\n");
	} else if (code == TSDB_CODE_WS_INTERNAL_ERRO || code == TSDB_CODE_WS_CLOSED) {
		fprintf(stderr, "TDengine server is down, will try to reconnect\n");
		shell.ws_conn = NULL;
	}
	ws_free_result(res);
	return;
  }

  if (shellRegexMatch(command, "^\\s*use\\s+[a-zA-Z0-9_]+\\s*;\\s*$", REG_EXTENDED | REG_ICASE)) {
    fprintf(stdout, "Database changed.\r\n\r\n");
    fflush(stdout);
	ws_free_result(res);
    return;
  }

  int numOfRows = 0;
  if (ws_is_update_query(res)) {
	numOfRows = ws_affected_rows(res);
	et = taosGetTimestampUs();
	printf("Query Ok, %d of %d row(s) in database (%.6fs)\n", numOfRows, numOfRows,
			(et - st)/1E6);
  } else {
	int error_no = 0;
	numOfRows  = shellDumpWebsocket(res, fname, &error_no, printMode);
	if (numOfRows < 0) {
		ws_free_result(res);
		return;
	}
	et = taosGetTimestampUs();
	if (error_no == 0 && !shell.stop_query) {
		printf("Query OK, %d row(s) in set (%.6fs)\n", numOfRows,
				(et - st)/1E6);
	} else {
		printf("Query interrupted, %d row(s) in set (%.6fs)\n", numOfRows,
				(et - st)/1E6);
	}
  }
  printf("\n");
  ws_free_result(res);
}
#endif
