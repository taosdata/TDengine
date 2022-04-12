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

#define _BSD_SOURCE
#define _GNU_SOURCE
#define _XOPEN_SOURCE
#define _DEFAULT_SOURCE

#include "os.h"
#include "shell.h"
#include "shellCommand.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tglobal.h"
#include "ttypes.h"
#include "tutil.h"
#include "tconfig.h"

#include <regex.h>
#include <wordexp.h>

/**************** Global variables ****************/
#ifdef _TD_POWER_
char      CLIENT_VERSION[] = "Welcome to the PowerDB shell from %s, Client Version:%s\n"
                             "Copyright (c) 2020 by PowerDB, Inc. All rights reserved.\n\n";
char      PROMPT_HEADER[] = "power> ";

char      CONTINUE_PROMPT[] = "    -> ";
int       prompt_size = 7;
#elif (_TD_TQ_ == true)
char      CLIENT_VERSION[] = "Welcome to the TQ shell from %s, Client Version:%s\n"
                             "Copyright (c) 2020 by TQ, Inc. All rights reserved.\n\n";
char      PROMPT_HEADER[] = "tq> ";

char      CONTINUE_PROMPT[] = "    -> ";
int       prompt_size = 4;
#else
char      CLIENT_VERSION[] = "Welcome to the TDengine shell from %s, Client Version:%s\n"
                             "Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.\n\n";
char      PROMPT_HEADER[] = "taos> ";

char      CONTINUE_PROMPT[] = "   -> ";
int       prompt_size = 6;
#endif

int64_t result = 0;
SShellHistory   history;

#define DEFAULT_MAX_BINARY_DISPLAY_WIDTH 30
extern int32_t tsMaxBinaryDisplayWidth;
extern TAOS   *taos_connect_auth(const char *ip, const char *user, const char *auth, const char *db, uint16_t port);

/*
 * FUNCTION: Initialize the shell.
 */
TAOS *shellInit(SShellArguments *_args) {
  printf("\n");
  if (!_args->is_use_passwd) {
#ifdef TD_WINDOWS
    strcpy(tsOsName, "Windows");
#elif defined(TD_DARWIN)
    strcpy(tsOsName, "Darwin");
#endif
    printf(CLIENT_VERSION, tsOsName, taos_get_client_info());
  }

  fflush(stdout);

  // set options before initializing
  if (_args->timezone != NULL) {
    taos_options(TSDB_OPTION_TIMEZONE, _args->timezone);
  }

  if (!_args->is_use_passwd) {
    _args->password = TSDB_DEFAULT_PASS;
  }

  if (_args->user == NULL) {
    _args->user = TSDB_DEFAULT_USER;
  }

  SConfig *pCfg = cfgInit();
  if (NULL == pCfg) return NULL;

  if (0 != taosAddClientLogCfg(pCfg)) return NULL;

  // Connect to the database.
  TAOS *con = NULL;
  if (_args->auth == NULL) {
    con = taos_connect(_args->host, _args->user, _args->password, _args->database, _args->port);
  } else {
    con = taos_connect_auth(_args->host, _args->user, _args->auth, _args->database, _args->port);
  }

  if (con == NULL) {
    fflush(stdout);
    return con;
  }

  /* Read history TODO : release resources here*/
  read_history();

  // Check if it is temperory run
  if (_args->commands != NULL || _args->file[0] != 0) {
    if (_args->commands != NULL) {
      printf("%s%s\n", PROMPT_HEADER, _args->commands);
      shellRunCommand(con, _args->commands);
    }

    if (_args->file[0] != 0) {
      source_file(con, _args->file);
    }

    taos_close(con);
    write_history();
    exit(EXIT_SUCCESS);
  }

#if 0
#ifndef WINDOWS
  if (_args->dir[0] != 0) {
    source_dir(con, _args);
    taos_close(con);
    exit(EXIT_SUCCESS);
  }

  if (_args->check != 0) {
    shellCheck(con, _args);
    taos_close(con);
    exit(EXIT_SUCCESS);
  }
#endif
#endif

  return con;
}

static bool isEmptyCommand(const char *cmd) {
  for (char c = *cmd++; c != 0; c = *cmd++) {
    if (c != ' ' && c != '\t' && c != ';') {
      return false;
    }
  }
  return true;
}

static int32_t shellRunSingleCommand(TAOS *con, char *command) {
  /* If command is empty just return */
  if (isEmptyCommand(command)) {
    return 0;
  }

  // Analyse the command.
  if (regex_match(command, "^[ \t]*(quit|q|exit)[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    taos_close(con);
    write_history();
#ifdef WINDOWS
    exit(EXIT_SUCCESS);
#endif
    return -1;
  }

  if (regex_match(command, "^[\t ]*clear[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    // If clear the screen.
    system("clear");
    return 0;
  }

  if (regex_match(command, "^[\t ]*set[ \t]+max_binary_display_width[ \t]+(default|[1-9][0-9]*)[ \t;]*$",
                  REG_EXTENDED | REG_ICASE)) {
    strtok(command, " \t");
    strtok(NULL, " \t");
    char *p = strtok(NULL, " \t");
    if (strcasecmp(p, "default") == 0) {
      tsMaxBinaryDisplayWidth = DEFAULT_MAX_BINARY_DISPLAY_WIDTH;
    } else {
      tsMaxBinaryDisplayWidth = atoi(p);
    }
    return 0;
  }

  if (regex_match(command, "^[ \t]*source[\t ]+[^ ]+[ \t;]*$", REG_EXTENDED | REG_ICASE)) {
    /* If source file. */
    char *c_ptr = strtok(command, " ;");
    assert(c_ptr != NULL);
    c_ptr = strtok(NULL, " ;");
    assert(c_ptr != NULL);
    source_file(con, c_ptr);
    return 0;
  }

  shellRunCommandOnServer(con, command);
  return 0;
}

int32_t shellRunCommand(TAOS *con, char *command) {
  /* If command is empty just return */
  if (isEmptyCommand(command)) {
    return 0;
  }

  /* Update the history vector. */
  if (history.hstart == history.hend ||
      history.hist[(history.hend + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE] == NULL ||
      strcmp(command, history.hist[(history.hend + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE]) != 0) {
    if (history.hist[history.hend] != NULL) {
      taosMemoryFreeClear(history.hist[history.hend]);
    }
    history.hist[history.hend] = strdup(command);

    history.hend = (history.hend + 1) % MAX_HISTORY_SIZE;
    if (history.hend == history.hstart) {
      history.hstart = (history.hstart + 1) % MAX_HISTORY_SIZE;
    }
  }

  bool esc = false;
  char quote = 0, *cmd = command, *p = command;
  for (char c = *command++; c != 0; c = *command++) {
    if (esc) {
      switch (c) {
        case 'n':
          c = '\n';
          break;
        case 'r':
          c = '\r';
          break;
        case 't':
          c = '\t';
          break;
        case 'G':
          *p++ = '\\';
          break;
        case '\'':
        case '"':
          if (quote) {
            *p++ = '\\';
          }
          break;
      }
      *p++ = c;
      esc = false;
      continue;
    }

    if (c == '\\') {
      if (quote != 0 && (*command == '_' || *command == '\\')) {
        // DO nothing
      } else {
        esc = true;
        continue;
      }
    }

    if (quote == c) {
      quote = 0;
    } else if (quote == 0 && (c == '\'' || c == '"')) {
      quote = c;
    }

    *p++ = c;
    if (c == ';' && quote == 0) {
      c = *p;
      *p = 0;
      if (shellRunSingleCommand(con, cmd) < 0) {
        return -1;
      }
      *p = c;
      p = cmd;
    }
  }

  *p = 0;
  return shellRunSingleCommand(con, cmd);
}

void freeResultWithRid(int64_t rid) {
#if 0
  SSqlObj* pSql = taosAcquireRef(tscObjRef, rid);
  if(pSql){
    taos_free_result(pSql);
    taosReleaseRef(tscObjRef, rid);
  }
#endif
}

void shellRunCommandOnServer(TAOS *con, char command[]) {
  int64_t   st, et;
  wordexp_t full_path;
  char     *sptr = NULL;
  char     *cptr = NULL;
  char     *fname = NULL;
  bool      printMode = false;

  if ((sptr = strstr(command, ">>")) != NULL) {
    cptr = strstr(command, ";");
    if (cptr != NULL) {
      *cptr = '\0';
    }

    if (wordexp(sptr + 2, &full_path, 0) != 0) {
      fprintf(stderr, "ERROR: invalid filename: %s\n", sptr + 2);
      return;
    }
    *sptr = '\0';
    fname = full_path.we_wordv[0];
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

  TAOS_RES *pSql = taos_query(con, command);
  if (taos_errno(pSql)) {
    taos_error(pSql, st);
    return;
  }

  int64_t oresult = atomic_load_64(&result);

  if (regex_match(command, "^\\s*use\\s+[a-zA-Z0-9_]+\\s*;\\s*$", REG_EXTENDED | REG_ICASE)) {
    fprintf(stdout, "Database changed.\n\n");
    fflush(stdout);

    atomic_store_64(&result, 0);
    freeResultWithRid(oresult);
    return;
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pSql);
  if (pFields != NULL) {  // select and show kinds of commands
    int error_no = 0;

    int numOfRows = shellDumpResult(pSql, fname, &error_no, printMode);
    if (numOfRows < 0) {
      atomic_store_64(&result, 0);
      freeResultWithRid(oresult);
      return;
    }

    et = taosGetTimestampUs();
    if (error_no == 0) {
      printf("Query OK, %d row(s) in set (%.6fs)\n", numOfRows, (et - st) / 1E6);
    } else {
      printf("Query interrupted (%s), %d row(s) in set (%.6fs)\n", taos_errstr(pSql), numOfRows, (et - st) / 1E6);
    }
    taos_free_result(pSql);    
  } else {
    int num_rows_affacted = taos_affected_rows(pSql);
    taos_free_result(pSql);
    et = taosGetTimestampUs();
    printf("Query OK, %d of %d row(s) in database (%.6fs)\n", num_rows_affacted, num_rows_affacted, (et - st) / 1E6);
  }

  printf("\n");

  if (fname != NULL) {
    wordfree(&full_path);
  }

  atomic_store_64(&result, 0);
  freeResultWithRid(oresult);
}

/* Function to do regular expression check */
int regex_match(const char *s, const char *reg, int cflags) {
  regex_t regex;
  char    msgbuf[100] = {0};

  /* Compile regular expression */
  if (regcomp(&regex, reg, cflags) != 0) {
    fprintf(stderr, "Fail to compile regex");
    exitShell();
  }

  /* Execute regular expression */
  int reti = regexec(&regex, s, 0, NULL, 0);
  if (!reti) {
    regfree(&regex);
    return 1;
  } else if (reti == REG_NOMATCH) {
    regfree(&regex);
    return 0;
  } else {
    regerror(reti, &regex, msgbuf, sizeof(msgbuf));
    fprintf(stderr, "Regex match failed: %s\n", msgbuf);
    regfree(&regex);
    exitShell();
  }

  return 0;
}

static char *formatTimestamp(char *buf, int64_t val, int precision) {
  if (args.is_raw_time) {
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

  /* comment out as it make testcases like select_with_tags.sim fail.
    but in windows, this may cause the call to localtime crash if tt < 0,
    need to find a better solution.
    if (tt < 0) {
      tt = 0;
    }
    */

#ifdef WINDOWS
  if (tt < 0) tt = 0;
#endif
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

  struct tm *ptm = localtime(&tt);
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}

static void dumpFieldToFile(TdFilePtr pFile, const char *val, TAOS_FIELD *field, int32_t length, int precision) {
  if (val == NULL) {
    taosFprintfFile(pFile, "%s", TSDB_DATA_NULL_STR);
    return;
  }

  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      taosFprintfFile(pFile, "%d", ((((int32_t)(*((char *)val))) == 1) ? 1 : 0));
      break;
    case TSDB_DATA_TYPE_TINYINT:
      taosFprintfFile(pFile, "%d", *((int8_t *)val));
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      taosFprintfFile(pFile, "%d", *((int16_t *)val));
      break;
    case TSDB_DATA_TYPE_INT:
      taosFprintfFile(pFile, "%d", *((int32_t *)val));
      break;
    case TSDB_DATA_TYPE_BIGINT:
      taosFprintfFile(pFile, "%" PRId64, *((int64_t *)val));
      break;
    case TSDB_DATA_TYPE_FLOAT:
      taosFprintfFile(pFile, "%.5f", GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      taosFprintfFile(pFile, "%.9f", GET_DOUBLE_VAL(val));
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      memcpy(buf, val, length);
      buf[length] = 0;
      taosFprintfFile(pFile, "\'%s\'", buf);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      formatTimestamp(buf, *(int64_t *)val, precision);
      taosFprintfFile(pFile, "'%s'", buf);
      break;
    default:
      break;
  }
}

static int dumpResultToFile(const char *fname, TAOS_RES *tres) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  wordexp_t full_path;

  if (wordexp((char *)fname, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: invalid file name: %s\n", fname);
    return -1;
  }

  // FILE *fp = fopen(full_path.we_wordv[0], "w");
  TdFilePtr pFile = taosOpenFile(full_path.we_wordv[0], TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "ERROR: failed to open file: %s\n", full_path.we_wordv[0]);
    wordfree(&full_path);
    return -1;
  }

  wordfree(&full_path);

  int         num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int         precision = taos_result_precision(tres);

  for (int col = 0; col < num_fields; col++) {
    if (col > 0) {
      taosFprintfFile(pFile, ",");
    }
    taosFprintfFile(pFile, "%s", fields[col].name);
  }
  taosFprintfFile(pFile, "\n");

  int numOfRows = 0;
  do {
    int32_t *length = taos_fetch_lengths(tres);
    for (int i = 0; i < num_fields; i++) {
      if (i > 0) {
        taosFprintfFile(pFile, "\n");
      }
      dumpFieldToFile(pFile, (const char *)row[i], fields + i, length[i], precision);
    }
    taosFprintfFile(pFile, "\n");

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  result = 0;
  taosCloseFile(&pFile);

  return numOfRows;
}

static void shellPrintNChar(const char *str, int length, int width) {
  TdWchar tail[3];
  int     pos = 0, cols = 0, totalCols = 0, tailLen = 0;

  while (pos < length) {
    TdWchar wc;
    int     bytes = taosMbToWchar(&wc, str + pos, MB_CUR_MAX);
    if (bytes == 0) {
      break;
    }
    pos += bytes;
    if (pos > length) {
      break;
    }

#ifdef WINDOWS
    int w = bytes;
#else
    int w = taosWcharWidth(wc);
#endif
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
    for (int i = 0; i < 3; i++) {
      if (cols >= width) {
        break;
      }
      putchar('.');
      ++cols;
    }
  } else {
    for (int i = 0; i < tailLen; i++) {
      printf("%lc", tail[i]);
    }
    cols = totalCols;
  }

  for (; cols < width; cols++) {
    putchar(' ');
  }
}

static void printField(const char *val, TAOS_FIELD *field, int width, int32_t length, int precision) {
  if (val == NULL) {
    int w = width;
    if (field->type < TSDB_DATA_TYPE_TINYINT || field->type > TSDB_DATA_TYPE_DOUBLE) {
      w = 0;
    }
    w = printf("%*s", w, TSDB_DATA_NULL_STR);
    for (; w < width; w++) {
      putchar(' ');
    }
    return;
  }

  char buf[TSDB_MAX_BYTES_PER_ROW];
  switch (field->type) {
    case TSDB_DATA_TYPE_BOOL:
      printf("%*s", width, ((((int32_t)(*((char *)val))) == 1) ? "true" : "false"));
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
      printf("%*.5f", width, GET_FLOAT_VAL(val));
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      printf("%*.9f", width, GET_DOUBLE_VAL(val));
      break;
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_NCHAR:
      shellPrintNChar(val, length, width);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      formatTimestamp(buf, *(int64_t *)val, precision);
      printf("%s", buf);
      break;
    default:
      break;
  }
}

bool isSelectQuery(TAOS_RES *tres) {
#if 0
  char *sql = tscGetSqlStr(tres);

  if (regex_match(sql, "^[\t ]*select[ \t]*", REG_EXTENDED | REG_ICASE)) {
    return true;
  }
#endif
  return false;
}

static int verticalPrintResult(TAOS_RES *tres) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  int         num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int         precision = taos_result_precision(tres);

  int maxColNameLen = 0;
  for (int col = 0; col < num_fields; col++) {
    int len = (int)strlen(fields[col].name);
    if (len > maxColNameLen) {
      maxColNameLen = len;
    }
  }

  uint64_t resShowMaxNum = UINT64_MAX;

  if (args.commands == NULL && args.file[0] == 0 && isSelectQuery(tres) /*&& !tscIsQueryWithLimit(tres)*/) {
    resShowMaxNum = DEFAULT_RES_SHOW_NUM;
  }

  int numOfRows = 0;
  int showMore = 1;
  do {
    if (numOfRows < resShowMaxNum) {
      printf("*************************** %d.row ***************************\n", numOfRows + 1);

      int32_t *length = taos_fetch_lengths(tres);

      for (int i = 0; i < num_fields; i++) {
        TAOS_FIELD *field = fields + i;

        int padding = (int)(maxColNameLen - strlen(field->name));
        printf("%*.s%s: ", padding, " ", field->name);

        printField((const char *)row[i], field, 0, length[i], precision);
        putchar('\n');
      }
    } else if (showMore) {
      printf("[100 Rows showed, and more rows are fetching but will not be showed. You can ctrl+c to stop or wait.]\n");
      printf("[You can add limit statement to get more or redirect results to specific file to get all.]\n");
      showMore = 0;
    }

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  return numOfRows;
}

static int calcColWidth(TAOS_FIELD *field, int precision) {
  int width = (int)strlen(field->name);

  switch (field->type) {
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
      if (field->bytes > tsMaxBinaryDisplayWidth) {
        return TMAX(tsMaxBinaryDisplayWidth, width);
      } else {
        return TMAX(field->bytes, width);
      }

    case TSDB_DATA_TYPE_NCHAR: {
      int16_t bytes = field->bytes * TSDB_NCHAR_SIZE;
      if (bytes > tsMaxBinaryDisplayWidth) {
        return TMAX(tsMaxBinaryDisplayWidth, width);
      } else {
        return TMAX(bytes, width);
      }
    }

    case TSDB_DATA_TYPE_TIMESTAMP:
      if (args.is_raw_time) {
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
      assert(false);
  }

  return 0;
}

static void printHeader(TAOS_FIELD *fields, int *width, int num_fields) {
  int rowWidth = 0;
  for (int col = 0; col < num_fields; col++) {
    TAOS_FIELD *field = fields + col;
    int         padding = (int)(width[col] - strlen(field->name));
    int         left = padding / 2;
    printf(" %*.s%s%*.s |", left, " ", field->name, padding - left, " ");
    rowWidth += width[col] + 3;
  }

  putchar('\n');
  for (int i = 0; i < rowWidth; i++) {
    putchar('=');
  }
  putchar('\n');
}

static int horizontalPrintResult(TAOS_RES *tres) {
  TAOS_ROW row = taos_fetch_row(tres);
  if (row == NULL) {
    return 0;
  }

  int         num_fields = taos_num_fields(tres);
  TAOS_FIELD *fields = taos_fetch_fields(tres);
  int         precision = taos_result_precision(tres);

  int width[TSDB_MAX_COLUMNS];
  for (int col = 0; col < num_fields; col++) {
    width[col] = calcColWidth(fields + col, precision);
  }

  printHeader(fields, width, num_fields);

  uint64_t resShowMaxNum = UINT64_MAX;

  if (args.commands == NULL && args.file[0] == 0 && isSelectQuery(tres) /* && !tscIsQueryWithLimit(tres)*/) {
    resShowMaxNum = DEFAULT_RES_SHOW_NUM;
  }

  int numOfRows = 0;
  int showMore = 1;

  do {
    int32_t *length = taos_fetch_lengths(tres);
    if (numOfRows < resShowMaxNum) {
      for (int i = 0; i < num_fields; i++) {
        putchar(' ');
        printField((const char *)row[i], fields + i, width[i], length[i], precision);
        putchar(' ');
        putchar('|');
      }
      putchar('\n');
    } else if (showMore) {
      printf("[100 Rows showed, and more rows are fetching but will not be showed. You can ctrl+c to stop or wait.]\n");
      printf("[You can add limit statement to show more or redirect results to specific file to get all.]\n");
      showMore = 0;
    }

    numOfRows++;
    row = taos_fetch_row(tres);
  } while (row != NULL);

  return numOfRows;
}

int shellDumpResult(TAOS_RES *tres, char *fname, int *error_no, bool vertical) {
  int numOfRows = 0;
  if (fname != NULL) {
    numOfRows = dumpResultToFile(fname, tres);
  } else if (vertical) {
    numOfRows = verticalPrintResult(tres);
  } else {
    numOfRows = horizontalPrintResult(tres);
  }

  *error_no = taos_errno(tres);
  return numOfRows;
}

void read_history() {
  // Initialize history
  memset(history.hist, 0, sizeof(char *) * MAX_HISTORY_SIZE);
  history.hstart = 0;
  history.hend = 0;
  char  *line = NULL;
  int    read_size = 0;

  char f_history[TSDB_FILENAME_LEN];
  get_history_path(f_history);

  // FILE *f = fopen(f_history, "r");
  TdFilePtr pFile = taosOpenFile(f_history, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
#ifndef WINDOWS
    if (errno != ENOENT) {
      fprintf(stderr, "Failed to open file %s, reason:%s\n", f_history, strerror(errno));
    }
#endif
    return;
  }

  while ((read_size = taosGetLineFile(pFile, &line)) != -1) {
    line[read_size - 1] = '\0';
    history.hist[history.hend] = strdup(line);

    history.hend = (history.hend + 1) % MAX_HISTORY_SIZE;

    if (history.hend == history.hstart) {
      history.hstart = (history.hstart + 1) % MAX_HISTORY_SIZE;
    }
  }

  if(line != NULL) taosMemoryFree(line);
  taosCloseFile(&pFile);
}

void write_history() {
  char f_history[TSDB_FILENAME_LEN];
  get_history_path(f_history);

  // FILE *f = fopen(f_history, "w");
  TdFilePtr pFile = taosOpenFile(f_history, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (pFile == NULL) {
#ifndef WINDOWS
    fprintf(stderr, "Failed to open file %s for write, reason:%s\n", f_history, strerror(errno));
#endif
    return;
  }

  for (int i = history.hstart; i != history.hend;) {
    if (history.hist[i] != NULL) {
      taosFprintfFile(pFile, "%s\n", history.hist[i]);
      taosMemoryFreeClear(history.hist[i]);
    }
    i = (i + 1) % MAX_HISTORY_SIZE;
  }
  taosCloseFile(&pFile);
}

void taos_error(TAOS_RES *tres, int64_t st) {
  int64_t et = taosGetTimestampUs();
  atomic_store_ptr(&result, 0);
  fprintf(stderr, "\nDB error: %s (%.6fs)\n", taos_errstr(tres), (et - st) / 1E6);
  taos_free_result(tres);
}

int isCommentLine(char *line) {
  if (line == NULL) return 1;

  return regex_match(line, "^\\s*#.*", REG_EXTENDED);
}

void source_file(TAOS *con, char *fptr) {
  wordexp_t full_path;
  int       read_len = 0;
  char     *cmd = taosMemoryCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN + 1);
  size_t    cmd_len = 0;
  char     *line = NULL;

  if (wordexp(fptr, &full_path, 0) != 0) {
    fprintf(stderr, "ERROR: illegal file name\n");
    taosMemoryFree(cmd);
    return;
  }

  char *fname = full_path.we_wordv[0];

  /*
  if (access(fname, F_OK) != 0) {
    fprintf(stderr, "ERROR: file %s is not exist\n", fptr);

    wordfree(&full_path);
    taosMemoryFree(cmd);
    return;
  }
  */

  // FILE *f = fopen(fname, "r");
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ | TD_FILE_STREAM);
  if (pFile == NULL) {
    fprintf(stderr, "ERROR: failed to open file %s\n", fname);
    wordfree(&full_path);
    taosMemoryFree(cmd);
    return;
  }

  while ((read_len = taosGetLineFile(pFile, &line)) != -1) {
    if (read_len >= TSDB_MAX_ALLOWED_SQL_LEN) continue;
    line[--read_len] = '\0';

    if (read_len == 0 || isCommentLine(line)) {  // line starts with #
      continue;
    }

    if (line[read_len - 1] == '\\') {
      line[read_len - 1] = ' ';
      memcpy(cmd + cmd_len, line, read_len);
      cmd_len += read_len;
      continue;
    }

    memcpy(cmd + cmd_len, line, read_len);
    printf("%s%s\n", PROMPT_HEADER, cmd);
    shellRunCommand(con, cmd);
    memset(cmd, 0, TSDB_MAX_ALLOWED_SQL_LEN);
    cmd_len = 0;
  }

  taosMemoryFree(cmd);
  if(line != NULL) taosMemoryFree(line);
  wordfree(&full_path);
  taosCloseFile(&pFile);
}

void shellGetGrantInfo(void *con) {
  return;
#if 0
  char sql[] = "show grants";

  TAOS_RES* tres = taos_query(con, sql);

  int code = taos_errno(tres);
  if (code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_COM_OPS_NOT_SUPPORT) {
      fprintf(stdout, "Server is Community Edition, version is %s\n\n", taos_get_server_info(con));
    } else {
      fprintf(stderr, "Failed to check Server Edition, Reason:%d:%s\n\n", taos_errno(con), taos_errstr(con));
    }
    return;
  }

  int num_fields = taos_field_count(tres);
  if (num_fields == 0) {
    fprintf(stderr, "\nInvalid grant information.\n");
    exit(0);
  } else {
    if (tres == NULL) {
      fprintf(stderr, "\nGrant information is null.\n");
      exit(0);
    }

    TAOS_FIELD *fields = taos_fetch_fields(tres);
    TAOS_ROW row = taos_fetch_row(tres);
    if (row == NULL) {
      fprintf(stderr, "\nFailed to get grant information from server. Abort.\n");
      exit(0);
    }

    char serverVersion[32] = {0};
    char expiretime[32] = {0};
    char expired[32] = {0};

    memcpy(serverVersion, row[0], fields[0].bytes);
    memcpy(expiretime, row[1], fields[1].bytes);
    memcpy(expired, row[2], fields[2].bytes);

    if (strcmp(expiretime, "unlimited") == 0) {
      fprintf(stdout, "Server is Enterprise %s Edition, version is %s and will never expire.\n", serverVersion, taos_get_server_info(con));
    } else {
      fprintf(stdout, "Server is Enterprise %s Edition, version is %s and will expire at %s.\n", serverVersion, taos_get_server_info(con), expiretime);
    }

    result = NULL;
    taos_free_result(tres);
  }

  fprintf(stdout, "\n");
#endif
}
