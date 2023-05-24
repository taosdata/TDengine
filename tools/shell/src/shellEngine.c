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

#define ALLOW_FORBID_FUNC
#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE
#include "shellInt.h"
#include "shellAuto.h"
#include "geosWrapper.h"

static bool    shellIsEmptyCommand(const char *cmd);
static int32_t shellRunSingleCommand(char *command);
static void    shellRecordCommandToHistory(char *command);
static int32_t shellRunCommand(char *command, bool recordHistory);
static void    shellRunSingleCommandImp(char *command);
static char   *shellFormatTimestamp(char *buf, int64_t val, int32_t precision);
static int32_t shellDumpResultToFile(const char *fname, TAOS_RES *tres);
static void    shellPrintNChar(const char *str, int32_t length, int32_t width);
static void    shellPrintGeometry(const unsigned char *str, int32_t length, int32_t width);
static int32_t shellVerticalPrintResult(TAOS_RES *tres, const char *sql);
static int32_t shellHorizontalPrintResult(TAOS_RES *tres, const char *sql);
static int32_t shellDumpResult(TAOS_RES *tres, char *fname, int32_t *error_no, bool vertical, const char *sql);
static void    shellReadHistory();
static void    shellWriteHistory();
static void    shellPrintError(TAOS_RES *tres, int64_t st);
static bool    shellIsCommentLine(char *line);
static void    shellSourceFile(const char *file);
static void    shellGetGrantInfo();

static void    shellCleanup(void *arg);
static void   *shellCancelHandler(void *arg);
static void   *shellThreadLoop(void *arg);

bool shellIsEmptyCommand(const char *cmd) {
  for (char c = *cmd++; c != 0; c = *cmd++) {
    if (c != ' ' && c != '\t' && c != ';') {
      return false;
    }
  }
  return true;
}

int32_t shellRunSingleCommand(char *command) {
  if (shellIsEmptyCommand(command)) {
    return 0;
  }

  if (shellRegexMatch(command, "^[ \t]*(quit|q|exit)[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    return -1;
  }

  if (shellRegexMatch(command, "^[\t ]*clear[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
          system("clear");
#pragma GCC diagnostic pop
    return 0;
  }

  if (shellRegexMatch(command, "^[\t ]*set[ \t]+max_binary_display_width[ \t]+(default|[1-9][0-9]*)[ \t;]*$",
                      REG_EXTENDED | REG_ICASE)) {
    strtok(command, " \t");
    strtok(NULL, " \t");
    char *p = strtok(NULL, " \t");
    if (strncasecmp(p, "default", 7) == 0) {
      shell.args.displayWidth = SHELL_DEFAULT_MAX_BINARY_DISPLAY_WIDTH;
    } else {
      int32_t displayWidth = atoi(p);
      displayWidth = TRANGE(displayWidth, 1, 10 * 1024);
      shell.args.displayWidth = displayWidth;
    }
    return 0;
  }

  if (shellRegexMatch(command, "^[ \t]*source[\t ]+[^ ]+[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    /* If source file. */
    char *c_ptr = strtok(command, " ;");
    if (c_ptr == NULL) {
      shellRunSingleCommandImp(command);
      return 0;
    }
    c_ptr = strtok(NULL, " ;");
    if (c_ptr == NULL) {
      shellRunSingleCommandImp(command);
      return 0;
    }
    shellSourceFile(c_ptr);
    return 0;
  }
#ifdef WEBSOCKET
  if (shell.args.restful || shell.args.cloud) {
    shellRunSingleCommandWebsocketImp(command);
  } else {
#endif
    shellRunSingleCommandImp(command);
#ifdef WEBSOCKET
  }
#endif
  return 0;
}

void shellRecordCommandToHistory(char *command) {
  if (strncasecmp(command, "create user ", 12) == 0 || strncasecmp(command, "alter user ", 11) == 0) {
    if (taosStrCaseStr(command, " pass ")) {
      // have password command forbid record to history because security
      return;
    }
  }

  SShellHistory *pHistory = &shell.history;
  if (pHistory->hstart == pHistory->hend ||
      pHistory->hist[(pHistory->hend + SHELL_MAX_HISTORY_SIZE - 1) % SHELL_MAX_HISTORY_SIZE] == NULL ||
      strcmp(command, pHistory->hist[(pHistory->hend + SHELL_MAX_HISTORY_SIZE - 1) % SHELL_MAX_HISTORY_SIZE]) != 0) {
    if (pHistory->hist[pHistory->hend] != NULL) {
      taosMemoryFreeClear(pHistory->hist[pHistory->hend]);
    }
    pHistory->hist[pHistory->hend] = taosStrdup(command);

    pHistory->hend = (pHistory->hend + 1) % SHELL_MAX_HISTORY_SIZE;
    if (pHistory->hend == pHistory->hstart) {
      pHistory->hstart = (pHistory->hstart + 1) % SHELL_MAX_HISTORY_SIZE;
    }
  }
}

int32_t shellRunCommand(char *command, bool recordHistory) {
  if (shellIsEmptyCommand(command)) {
    return 0;
  }

  // add help or help; 
  if(strncasecmp(command, "help;", 5) == 0) {
    showHelp();
    return 0;
  }

  if (recordHistory) shellRecordCommandToHistory(command);

  char quote = 0, *cmd = command;
  for (char c = *command++; c != 0; c = *command++) {
    if (c == '\\' && (*command == '\'' || *command == '"' || *command == '`')) {
      command++;
      continue;
    }

    if (quote == c) {
      quote = 0;
    } else if (quote == 0 && (c == '\'' || c == '"' || c == '`')) {
      quote = c;
    } else if (c == ';' && quote == 0) {
      c = *command;
      *command = 0;
      if (shellRunSingleCommand(cmd) < 0) {
        return -1;
      }
      *command = c;
      cmd = command;
    }
  }
  return shellRunSingleCommand(cmd);
}

void shellRunSingleCommandImp(char *command) {
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

  st = taosGetTimestampUs();

  TAOS_RES *pSql = taos_query(shell.conn, command);
  if (taos_errno(pSql)) {
    shellPrintError(pSql, st);
    return;
  }

  if (shellRegexMatch(command, "^\\s*use\\s+[a-zA-Z0-9_]+\\s*;\\s*$", REG_EXTENDED | REG_ICASE)) {
    fprintf(stdout, "Database changed.\r\n\r\n");
    fflush(stdout);

    // call back auto tab module
    callbackAutoTab(command, pSql, true);

    taos_free_result(pSql);

    return;
  }

  // pre string
  char * pre = "Query OK";
  if (shellRegexMatch(command, "^\\s*delete\\s*from\\s*.*", REG_EXTENDED | REG_ICASE)) {
    pre = "Delete OK";
  } else if(shellRegexMatch(command, "^\\s*insert\\s*into\\s*.*", REG_EXTENDED | REG_ICASE)) {
    pre = "Insert OK";
  } else if(shellRegexMatch(command, "^\\s*create\\s*.*", REG_EXTENDED | REG_ICASE)) {
    pre = "Create OK";
  } else if(shellRegexMatch(command, "^\\s*drop\\s*.*", REG_EXTENDED | REG_ICASE)) {
    pre = "Drop OK";
  }

  TAOS_FIELD *pFields = taos_fetch_fields(pSql);
  if (pFields != NULL) {  // select and show kinds of commands
    int32_t error_no = 0;

    int32_t numOfRows = shellDumpResult(pSql, fname, &error_no, printMode, command);
    if (numOfRows < 0) return;

    et = taosGetTimestampUs();
    if (error_no == 0) {
      printf("Query OK, %d row(s) in set (%.6fs)\r\n", numOfRows, (et - st) / 1E6);
    } else {
      printf("Query interrupted (%s), %d row(s) in set (%.6fs)\r\n", taos_errstr(pSql), numOfRows, (et - st) / 1E6);
    }
    taos_free_result(pSql);
  } else {
    int64_t num_rows_affacted = taos_affected_rows64(pSql);
    taos_free_result(pSql);
    et = taosGetTimestampUs();
    printf("%s, %" PRId64 " row(s) affected (%.6fs)\r\n", pre, num_rows_affacted, (et - st) / 1E6);

    // call auto tab
    callbackAutoTab(command, NULL, false);
  }

  printf("\r\n");
}

char *shellFormatTimestamp(char *buf, int64_t val, int32_t precision) {
  if (shell.args.is_raw_time) {
    sprintf(buf, "%" PRId64, val);
    return buf;
  }

  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }

  struct tm ptm = {0};
  if (taosLocalTime(&tt, &ptm, buf) == NULL) {
    return buf;
  }
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", &ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

char *shellDumpHexValue(char *buf, const char *val, int32_t length) {
  for (int32_t i = 0; i < length; i++) {
    sprintf(buf + (i * 2), "%02X", val[i]);
  }
  buf[length * 2] = 0;

  return buf;
}

void shellDumpFieldToFile(TdFilePtr pFile, const char *val, TAOS_FIELD *field, int32_t length, int32_t precision) {
  if (val == NULL) {
    taosFprintfFile(pFile, "NULL");
    return;
  }

  char quotationStr[2];
  quotationStr[0] = '\"';
  quotationStr[1] = 0;

  int  n;
  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      taosFprintfFile(pFile, "%d", ((((int32_t)(*((char *)val))) == 1) ? 1 : 0));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      taosFprintfFile(pFile, "%d", *((int8_t *)val));
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      taosFprintfFile(pFile, "%u", *((uint8_t *)val));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      taosFprintfFile(pFile, "%d", *((int16_t *)val));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      taosFprintfFile(pFile, "%u", *((uint16_t *)val));
      break;
    case TSDB_DATA_TYPE_INT:
      taosFprintfFile(pFile, "%d", *((int32_t *)val));
      break;
    case TSDB_DATA_TYPE_UINT:
      taosFprintfFile(pFile, "%u", *((uint32_t *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      taosFprintfFile(pFile, "%" PRId64, *((int64_t *)val));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      taosFprintfFile(pFile, "%" PRIu64, *((uint64_t *)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (tsEnableScience) {
        taosFprintfFile(pFile, "%e", GET_FLOAT_VAL(val));
      } else {
        taosFprintfFile(pFile, "%.5f", GET_FLOAT_VAL(val));
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (tsEnableScience) {
        snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%*.9e", 23, GET_DOUBLE_VAL(val));
        taosFprintfFile(pFile, "%s", buf);
      } else {
        n = snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%*.9f", length, GET_DOUBLE_VAL(val));
        if (n > TMAX(25, length)) {
          taosFprintfFile(pFile, "%*.15e", length, GET_DOUBLE_VAL(val));
        } else {
          taosFprintfFile(pFile, "%s", buf);
        }
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      {
        int32_t bufIndex = 0;
        for (int32_t i = 0; i < length; i++) {
          buf[bufIndex] = val[i];
          bufIndex++;
          if (val[i] == '\"') {
            buf[bufIndex] = val[i];
            bufIndex++;
          }
        }
        buf[bufIndex] = 0;
        
        taosFprintfFile(pFile, "%s%s%s", quotationStr, buf, quotationStr);
      }
      break;
    case TSDB_DATA_TYPE_GEOMETRY:
      shellDumpHexValue(buf, val, length);
      taosFprintfFile(pFile, "%s", buf);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      shellFormatTimestamp(buf, *(int64_t *)val, precision);
      taosFprintfFile(pFile, "%s%s%s", quotationStr, buf, quotationStr);
      break;
    default:
      break;
  }
}

int32_t shellDumpResultToFile(const char *fname, TAOS_RES *tres) {
  char fullname[PATH_MAX] = {0};
  if (taosExpandDir(fname, fullname, PATH_MAX) != 0) {
    tstrncpy(fullname, fname, PATH_MAX);
  }

  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  TdFilePtr pFile = taosOpenFile(fullname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "failed to open file: %s\r\n", fullname);
    return -1;
  }

  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int32_t     num_fields = taos_num_fields(tres);
  int32_t     precision = taos_result_precision(tres);

  for (int32_t col = 0; col < num_fields; col++) {
    if (col > 0) {
      taosFprintfFile(pFile, ",");
    }
    taosFprintfFile(pFile, "%s", fields[col].name);
  }
  taosFprintfFile(pFile, "\r\n");

  int32_t numOfRows = 0;
  do {
    int32_t *length = taos_fetch_lengths(tres);
    for (int32_t i = 0; i < num_fields; i++) {
      if (i > 0) {
        taosFprintfFile(pFile, ",");
      }
      shellDumpFieldToFile(pFile, (const char *)row[i], fields + i, length[i], precision);
    }
    taosFprintfFile(pFile, "\r\n");

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  taosCloseFile(&pFile);

  return numOfRows;
}

void shellPrintNChar(const char *str, int32_t length, int32_t width) {
  TdWchar tail[3];
  int32_t pos = 0, cols = 0, totalCols = 0, tailLen = 0;

  while (pos < length) {
    TdWchar wc;
    int32_t bytes = taosMbToWchar(&wc, str + pos, MB_CUR_MAX);
    if (bytes <= 0) {
      break;
    }

    if (pos + bytes > length) {
      break;
    }
    int w = 0;
    if (*(str + pos) == '\t' || *(str + pos) == '\n' || *(str + pos) == '\r') {
      w = bytes;
    } else {
      w = taosWcharWidth(wc);
    }
    pos += bytes;

    if (w <= 0) {
      continue;
    }

    if (width <= 0) {
      printf("%lc", wc);
      continue;
    }

    totalCols += w;
    if (totalCols > width) {
      break;
    }
    if (totalCols <= (width - 3)) {
      printf("%lc", wc);
      cols += w;
    } else {
      tail[tailLen] = wc;
      tailLen++;
    }
  }

  if (totalCols > width) {
    // width could be 1 or 2, so printf("...") cannot be used
    for (int32_t i = 0; i < 3; i++) {
      if (cols >= width) {
        break;
      }
      putchar('.');
      ++cols;
    }
  } else {
    for (int32_t i = 0; i < tailLen; i++) {
      printf("%lc", tail[i]);
    }
    cols = totalCols;
  }

  for (; cols < width; cols++) {
    putchar(' ');
  }
}

void shellPrintString(const char *str, int32_t width) {
  int32_t len = strlen(str);

  if (width == 0) {
    printf("%s", str);
  }
  else if (len > width) {
    if (width <= 3) {
      printf("%.*s.", width - 1, str);
    }
    else {
      printf("%.*s...", width - 3, str);
    }
  } else {
    printf("%s%*.s", str, width - len, "");
  }
}

void shellPrintGeometry(const unsigned char *val, int32_t length, int32_t width) {
  if (length == 0) {  //empty value
    shellPrintString("", width);
    return;
  }

  int32_t code = TSDB_CODE_FAILED;

  code = initCtxAsText();
  if (code != TSDB_CODE_SUCCESS) {
    shellPrintString(getThreadLocalGeosCtx()->errMsg, width);
    return;
  }

  char *outputWKT = NULL;
  code = doAsText(val, length, &outputWKT);
  if (code != TSDB_CODE_SUCCESS) {
    shellPrintString(getThreadLocalGeosCtx()->errMsg, width);  //should NOT happen
    return;
  }

  shellPrintString(outputWKT, width);

  geosFreeBuffer(outputWKT);
}

void shellPrintField(const char *val, TAOS_FIELD *field, int32_t width, int32_t length, int32_t precision) {
  if (val == NULL) {
    shellPrintString(TSDB_DATA_NULL_STR, width);
    return;
  }

  int  n;
  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      shellPrintString(((((int32_t)(*((char *)val))) == 1) ? "true" : "false"), width);
      break;
    case TSDB_DATA_TYPE_TINYINT:
      printf("%*d", width, *((int8_t *)val));
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      printf("%*u", width, *((uint8_t *)val));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      printf("%*d", width, *((int16_t *)val));
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      printf("%*u", width, *((uint16_t *)val));
      break;
    case TSDB_DATA_TYPE_INT:
      printf("%*d", width, *((int32_t *)val));
      break;
    case TSDB_DATA_TYPE_UINT:
      printf("%*u", width, *((uint32_t *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      printf("%*" PRId64, width, *((int64_t *)val));
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      printf("%*" PRIu64, width, *((uint64_t *)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      if (tsEnableScience) {
        printf("%*e", width, GET_FLOAT_VAL(val));
      } else {
        n = snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%*.5f", width, GET_FLOAT_VAL(val));
        if (n > TMAX(20, width)) {
            printf("%*e", width, GET_FLOAT_VAL(val));
        } else {
            printf("%s", buf);
        }
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      if (tsEnableScience) {
        snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%.9e", GET_DOUBLE_VAL(val));
        printf("%*s", width, buf);
      } else {
        n = snprintf(buf, TSDB_MAX_BYTES_PER_ROW, "%*.9f", width, GET_DOUBLE_VAL(val));
        if (n > TMAX(25, width)) {
            printf("%*.15e", width, GET_DOUBLE_VAL(val));
        } else {
            printf("%s", buf);
        }
      }
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
      shellPrintNChar(val, length, width);
      break;
    case TSDB_DATA_TYPE_GEOMETRY:
      shellPrintGeometry(val, length, width);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      shellFormatTimestamp(buf, *(int64_t *)val, precision);
      printf("%s", buf);
      break;
    default:
      break;
  }
}

// show whole result for this query return true, like limit or describe
bool shellIsShowWhole(const char *sql) {
  // limit
  if (taosStrCaseStr(sql, " limit ") != NULL) {
    return true;
  }
  // describe
  if (taosStrCaseStr(sql, "describe ") != NULL) {
    return true;
  }
  // show
  if (taosStrCaseStr(sql, "show ") != NULL) {
    return true;
  }

  return false;
}

bool shellIsShowQuery(const char *sql) {
  // todo refactor
  if (taosStrCaseStr(sql, "show ") != NULL) {
    return true;
  }

  return false;
}

int32_t shellVerticalPrintResult(TAOS_RES *tres, const char *sql) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  int32_t     num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int32_t     precision = taos_result_precision(tres);

  int32_t maxColNameLen = 0;
  for (int32_t col = 0; col < num_fields; col++) {
    int32_t len = (int32_t)strlen(fields[col].name);
    if (len > maxColNameLen) {
      maxColNameLen = len;
    }
  }

  uint64_t resShowMaxNum = UINT64_MAX;

  if (shell.args.commands == NULL && shell.args.file[0] == 0 && !shellIsShowWhole(sql)) {
    resShowMaxNum = SHELL_DEFAULT_RES_SHOW_NUM;
  }

  int32_t numOfRows = 0;
  int32_t showMore = 1;
  do {
    if (numOfRows < resShowMaxNum) {
      printf("*************************** %d.row ***************************\r\n", numOfRows + 1);

      int32_t *length = taos_fetch_lengths(tres);

      for (int32_t i = 0; i < num_fields; i++) {
        TAOS_FIELD *field = fields + i;

        int32_t padding = (int32_t)(maxColNameLen - strlen(field->name));
        printf("%*.s%s: ", padding, " ", field->name);

        shellPrintField((const char *)row[i], field, 0, length[i], precision);
        putchar('\r');
        putchar('\n');
      }
    } else if (showMore) {
      printf("\r\n");
      printf(" Notice: The result shows only the first %d rows.\r\n", SHELL_DEFAULT_RES_SHOW_NUM);
      printf("         You can use the `LIMIT` clause to get fewer result to show.\r\n");
      printf("           Or use '>>' to redirect the whole set of the result to a specified file.\r\n");
      printf("\r\n");
      printf("         You can use Ctrl+C to stop the underway fetching.\r\n");
      printf("\r\n");
      showMore = 0;
    }

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  return numOfRows;
}

int32_t shellCalcColWidth(TAOS_FIELD *field, int32_t precision) {
  int32_t width = (int32_t)strlen(field->name);

  switch (field->type) {
    case TSDB_DATA_TYPE_NULL:
      return TMAX(4, width);  // null
    case TSDB_DATA_TYPE_BOOL:
      return TMAX(5, width);  // 'false'

    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      return TMAX(4, width);  // '-127'

    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      return TMAX(6, width);  // '-32767'

    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
      return TMAX(11, width);  // '-2147483648'

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
      return TMAX(21, width);  // '-9223372036854775807'

    case TSDB_DATA_TYPE_FLOAT:
      return TMAX(20, width);

    case TSDB_DATA_TYPE_DOUBLE:
      return TMAX(25, width);

    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_GEOMETRY:
      if (field->bytes > shell.args.displayWidth) {
        return TMAX(shell.args.displayWidth, width);
      } else {
        return TMAX(field->bytes, width);
      }

    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON: {
      uint16_t bytes = field->bytes * TSDB_NCHAR_SIZE;
      if (bytes > shell.args.displayWidth) {
        return TMAX(shell.args.displayWidth, width);
      } else {
        return TMAX(bytes, width);
      }
    }

    case TSDB_DATA_TYPE_TIMESTAMP:
      if (shell.args.is_raw_time) {
        return TMAX(14, width);
      }
      if (precision == TSDB_TIME_PRECISION_NANO) {
        return TMAX(29, width);
      } else if (precision == TSDB_TIME_PRECISION_MICRO) {
        return TMAX(26, width);  // '2020-01-01 00:00:00.000000'
      } else {
        return TMAX(23, width);  // '2020-01-01 00:00:00.000'
      }

    default:
      ASSERT(false);
  }

  return 0;
}

void shellPrintHeader(TAOS_FIELD *fields, int32_t *width, int32_t num_fields) {
  int32_t rowWidth = 0;
  for (int32_t col = 0; col < num_fields; col++) {
    TAOS_FIELD *field = fields + col;
    int32_t     padding = (int32_t)(width[col] - strlen(field->name));
    int32_t     left = padding / 2;
    printf(" %*.s%s%*.s |", left, " ", field->name, padding - left, " ");
    rowWidth += width[col] + 3;
  }

  putchar('\r');
  putchar('\n');
  for (int32_t i = 0; i < rowWidth; i++) {
    putchar('=');
  }
  putchar('\r');
  putchar('\n');
}

int32_t shellHorizontalPrintResult(TAOS_RES *tres, const char *sql) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  int32_t     num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int32_t     precision = taos_result_precision(tres);

  int32_t width[TSDB_MAX_COLUMNS];
  for (int32_t col = 0; col < num_fields; col++) {
    width[col] = shellCalcColWidth(fields + col, precision);
  }

  shellPrintHeader(fields, width, num_fields);

  uint64_t resShowMaxNum = UINT64_MAX;

  if (shell.args.commands == NULL && shell.args.file[0] == 0 && !shellIsShowWhole(sql)) {
    resShowMaxNum = SHELL_DEFAULT_RES_SHOW_NUM;
  }

  int32_t numOfRows = 0;
  int32_t showMore = 1;

  do {
    int32_t *length = taos_fetch_lengths(tres);
    if (numOfRows < resShowMaxNum) {
      for (int32_t i = 0; i < num_fields; i++) {
        putchar(' ');
        shellPrintField((const char *)row[i], fields + i, width[i], length[i], precision);
        putchar(' ');
        putchar('|');
      }
      putchar('\r');
      putchar('\n');
    } else if (showMore) {
      printf("\r\n");
      printf(" Notice: The result shows only the first %d rows.\r\n", SHELL_DEFAULT_RES_SHOW_NUM);
      if (shellIsShowQuery(sql)) {
        printf("         You can use '>>' to redirect the whole set of the result to a specified file.\r\n");
      } else {
        printf("         You can use the `LIMIT` clause to get fewer result to show.\r\n");
        printf("           Or use '>>' to redirect the whole set of the result to a specified file.\r\n");
      }
      printf("\r\n");
      printf("         You can use Ctrl+C to stop the underway fetching.\r\n");
      printf("\r\n");
      showMore = 0;
    }

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  return numOfRows;
}

int32_t shellDumpResult(TAOS_RES *tres, char *fname, int32_t *error_no, bool vertical, const char *sql) {
  int32_t numOfRows = 0;
  if (fname != NULL) {
    numOfRows = shellDumpResultToFile(fname, tres);
  } else if (vertical) {
    numOfRows = shellVerticalPrintResult(tres, sql);
  } else {
    numOfRows = shellHorizontalPrintResult(tres, sql);
  }

  *error_no = taos_errno(tres);
  return numOfRows;
}

void shellReadHistory() {
  SShellHistory *pHistory = &shell.history;
  TdFilePtr      pFile = taosOpenFile(pHistory->file, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) return;

  char    *line = taosMemoryMalloc(TSDB_MAX_ALLOWED_SQL_LEN + 1);
  int32_t read_size = 0;
  while ((read_size = taosGetsFile(pFile, TSDB_MAX_ALLOWED_SQL_LEN, line)) != -1) {
    line[read_size - 1] = '\0';
    taosMemoryFree(pHistory->hist[pHistory->hend]);
    pHistory->hist[pHistory->hend] = taosStrdup(line);

    pHistory->hend = (pHistory->hend + 1) % SHELL_MAX_HISTORY_SIZE;

    if (pHistory->hend == pHistory->hstart) {
      pHistory->hstart = (pHistory->hstart + 1) % SHELL_MAX_HISTORY_SIZE;
    }
  }

  taosMemoryFreeClear(line);
  taosCloseFile(&pFile);
  int64_t file_size;
  if (taosStatFile(pHistory->file, &file_size, NULL) == 0 && file_size > SHELL_MAX_COMMAND_SIZE) {
    TdFilePtr      pFile = taosOpenFile(pHistory->file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_STREAM | TD_FILE_TRUNC);
    if (pFile == NULL) return;
    int32_t endIndex = pHistory->hstart;
    if (endIndex != 0) {
      endIndex = pHistory->hend;
    }
    for (int32_t i = (pHistory->hend + SHELL_MAX_HISTORY_SIZE - 1) % SHELL_MAX_HISTORY_SIZE; i != endIndex;) {
      taosFprintfFile(pFile, "%s\n", pHistory->hist[i]);
      i = (i + SHELL_MAX_HISTORY_SIZE - 1) % SHELL_MAX_HISTORY_SIZE;
    }
    taosFprintfFile(pFile, "%s\n", pHistory->hist[endIndex]);

    /* coverity[+retval] */
    taosFsyncFile(pFile);
    taosCloseFile(&pFile);
  }
  pHistory->hstart = pHistory->hend;
}

void shellWriteHistory() {
  SShellHistory *pHistory = &shell.history;
  if (pHistory->hend == pHistory->hstart) return;
  TdFilePtr      pFile = taosOpenFile(pHistory->file, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_STREAM | TD_FILE_APPEND);
  if (pFile == NULL) return;

  for (int32_t i = pHistory->hstart; i != pHistory->hend;) {
    if (pHistory->hist[i] != NULL) {
      taosFprintfFile(pFile, "%s\n", pHistory->hist[i]);
      taosMemoryFree(pHistory->hist[i]);
      pHistory->hist[i] = NULL;
    }
    i = (i + 1) % SHELL_MAX_HISTORY_SIZE;
  }
  taosCloseFile(&pFile);
}

void shellCleanupHistory() {
  SShellHistory *pHistory = &shell.history;
  for (int32_t i = 0; i < SHELL_MAX_HISTORY_SIZE; ++i) {
    if (pHistory->hist[i] != NULL) {
      taosMemoryFree(pHistory->hist[i]);
      pHistory->hist[i] = NULL;
    }
  }
}

void shellPrintError(TAOS_RES *tres, int64_t st) {
  int64_t et = taosGetTimestampUs();
  fprintf(stderr, "\r\nDB error: %s (%.6fs)\r\n", taos_errstr(tres), (et - st) / 1E6);
  taos_free_result(tres);
}

bool shellIsCommentLine(char *line) {
  if (line == NULL) return true;
  return shellRegexMatch(line, "^\\s*#.*", REG_EXTENDED);
}

void shellSourceFile(const char *file) {
  int32_t read_len = 0;
  char   *cmd = taosMemoryCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN + 1);
  size_t  cmd_len = 0;
  char    fullname[PATH_MAX] = {0};
  char    sourceFileCommand[PATH_MAX + 8] = {0};

  if (taosExpandDir(file, fullname, PATH_MAX) != 0) {
    tstrncpy(fullname, file, PATH_MAX);
  }

  sprintf(sourceFileCommand, "source %s;",fullname);
  shellRecordCommandToHistory(sourceFileCommand);

  TdFilePtr pFile = taosOpenFile(fullname, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "failed to open file %s\r\n", fullname);
    taosMemoryFree(cmd);
    return;
  }

  char   *line = taosMemoryMalloc(TSDB_MAX_ALLOWED_SQL_LEN + 1);
  while ((read_len = taosGetsFile(pFile, TSDB_MAX_ALLOWED_SQL_LEN, line)) != -1) {
    if (read_len >= TSDB_MAX_ALLOWED_SQL_LEN) continue;
    line[--read_len] = '\0';

    if (read_len == 0 || shellIsCommentLine(line)) {  // line starts with #
      continue;
    }

    if (line[read_len - 1] == '\\') {
      line[read_len - 1] = ' ';
      memcpy(cmd + cmd_len, line, read_len);
      cmd_len += read_len;
      continue;
    }

    if (line[read_len - 1] == '\r') {
      line[read_len - 1] = ' ';
    }

    memcpy(cmd + cmd_len, line, read_len);
    printf("%s%s\r\n", shell.info.promptHeader, cmd);
    shellRunCommand(cmd, false);
    memset(cmd, 0, TSDB_MAX_ALLOWED_SQL_LEN);
    cmd_len = 0;
  }

  taosMemoryFree(cmd);
  taosMemoryFreeClear(line);
  taosCloseFile(&pFile);
}

void shellGetGrantInfo() {
  char sinfo[1024] = {0};
  tstrncpy(sinfo, taos_get_server_info(shell.conn), sizeof(sinfo));
  strtok(sinfo, "\r\n");

  char sql[] = "show grants";

  TAOS_RES *tres = taos_query(shell.conn, sql);

  int32_t code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    if (code != TSDB_CODE_OPS_NOT_SUPPORT && code != TSDB_CODE_MND_NO_RIGHTS && code != TSDB_CODE_PAR_PERMISSION_DENIED) {
      fprintf(stderr, "Failed to check Server Edition, Reason:0x%04x:%s\r\n\r\n", code, taos_errstr(tres));
    }
    return;
  }

  int32_t num_fields = taos_field_count(tres);
  if (num_fields == 0) {
    fprintf(stderr, "\r\nInvalid grant information.\r\n");
    exit(0);
  } else {
    if (tres == NULL) {
      fprintf(stderr, "\r\nGrant information is null.\r\n");
      exit(0);
    }

    TAOS_FIELD *fields = taos_fetch_fields(tres);
    TAOS_ROW    row = taos_fetch_row(tres);
    if (row == NULL) {
      fprintf(stderr, "\r\nFailed to get grant information from server. Abort.\r\n");
      exit(0);
    }

    char serverVersion[32] = {0};
    char expiretime[32] = {0};
    char expired[32] = {0};

    memcpy(serverVersion, row[0], fields[0].bytes);
    memcpy(expiretime, row[1], fields[1].bytes);
    memcpy(expired, row[2], fields[2].bytes);

    if (strcmp(serverVersion, "community") == 0) {
      fprintf(stdout, "Server is Community Edition.\r\n");
    } else if (strcmp(expiretime, "unlimited") == 0) {
      fprintf(stdout, "Server is Enterprise %s Edition, %s and will never expire.\r\n", serverVersion, sinfo);
    } else {
      fprintf(stdout, "Server is Enterprise %s Edition, %s and will expire at %s.\r\n", serverVersion, sinfo, expiretime);
    }

    taos_free_result(tres);
  }

  fprintf(stdout, "\r\n");
}

#ifdef WINDOWS
BOOL shellQueryInterruptHandler(DWORD fdwCtrlType) {
  tsem_post(&shell.cancelSem);
  return TRUE;
}
#else
void shellQueryInterruptHandler(int32_t signum, void *sigInfo, void *context) { tsem_post(&shell.cancelSem); }
#endif

void shellCleanup(void *arg) { taosResetTerminalMode(); }

void *shellCancelHandler(void *arg) {
  setThreadName("shellCancelHandler");
  while (1) {
    if (shell.exit == true) {
      break;
    }

    if (tsem_wait(&shell.cancelSem) != 0) {
      taosMsleep(10);
      continue;
    }

#ifdef WEBSOCKET
    if (shell.args.restful || shell.args.cloud) {
      shell.stop_query = true;
    } else {
#endif
      if (shell.conn) {
        taos_kill_query(shell.conn);
      }
#ifdef WEBSOCKET
    }
#endif
  #ifdef WINDOWS
    printf("\n%s", shell.info.promptHeader);
  #endif
  }

  return NULL;
}

void *shellThreadLoop(void *arg) {
  setThreadName("shellThreadLoop");
  taosGetOldTerminalMode();
  taosThreadCleanupPush(shellCleanup, NULL);

  do {
    char *command = taosMemoryMalloc(SHELL_MAX_COMMAND_SIZE);
    if (command == NULL) {
      printf("failed to malloc command\r\n");
      break;
    }

    do {
      memset(command, 0, SHELL_MAX_COMMAND_SIZE);
      taosSetTerminalMode();

      if (shellReadCommand(command) != 0) {
        break;
      }

      taosResetTerminalMode();
    } while (shellRunCommand(command, true) == 0);

    destroyThreadLocalGeosCtx();
    taosMemoryFreeClear(command);
    shellWriteHistory();
    shellExit();
  } while (0);

  taosThreadCleanupPop(1);
  return NULL;
}

int32_t shellExecute() {
  printf(shell.info.clientVersion, shell.info.cusName,
         taos_get_client_info(), shell.info.cusName);
  fflush(stdout);

  SShellArgs *pArgs = &shell.args;
#ifdef WEBSOCKET
  if (shell.args.restful || shell.args.cloud) {
    if (shell_conn_ws_server(1)) {
      return -1;
    }
  } else {
#endif
    if (shell.args.auth == NULL) {
      shell.conn = taos_connect(pArgs->host, pArgs->user, pArgs->password, pArgs->database, pArgs->port);
    } else {
      shell.conn = taos_connect_auth(pArgs->host, pArgs->user, pArgs->auth, pArgs->database, pArgs->port);
    }

    if (shell.conn == NULL) {
      printf("failed to connect to server, reason: %s\n", taos_errstr(NULL));
      fflush(stdout);
      return -1;
    }
#ifdef WEBSOCKET
  }
#endif

  bool runOnce = pArgs->commands != NULL || pArgs->file[0] != 0;
  shellSetConn(shell.conn, runOnce);
  shellReadHistory();

  if (runOnce) {
    if (pArgs->commands != NULL) {
      printf("%s%s\r\n", shell.info.promptHeader, pArgs->commands);
      char *cmd = taosStrdup(pArgs->commands);
      shellRunCommand(cmd, true);
      taosMemoryFree(cmd);
    }

    if (pArgs->file[0] != 0) {
      shellSourceFile(pArgs->file);
    }
#ifdef WEBSOCKET
    if (shell.args.restful || shell.args.cloud) {
      ws_close(shell.ws_conn);
    } else {
#endif
      taos_close(shell.conn);
#ifdef WEBSOCKET
    }
#endif

    shellWriteHistory();
    shellCleanupHistory();
    return 0;
  }

  if (tsem_init(&shell.cancelSem, 0, 0) != 0) {
    printf("failed to create cancel semaphore\r\n");
    return -1;
  }

  TdThread spid = {0};
  taosThreadCreate(&spid, NULL, shellCancelHandler, NULL);

  taosSetSignal(SIGTERM, shellQueryInterruptHandler);
  taosSetSignal(SIGHUP, shellQueryInterruptHandler);
  taosSetSignal(SIGINT, shellQueryInterruptHandler);
  
#ifdef WEBSOCKET
  if (!shell.args.restful && !shell.args.cloud) {
#endif
#ifndef WINDOWS
    printfIntroduction();
#endif  
    shellGetGrantInfo();
#ifdef WEBSOCKET
  }
#endif
  while (1) {
    taosThreadCreate(&shell.pid, NULL, shellThreadLoop, NULL);
    taosThreadJoin(shell.pid, NULL);
    taosThreadClear(&shell.pid);
    if (shell.exit) {
      tsem_post(&shell.cancelSem);
      break;
    }
  }
  taosThreadJoin(spid, NULL);

  shellCleanupHistory();
  taos_kill_query(shell.conn);
  taos_close(shell.conn);

  return 0;
}
