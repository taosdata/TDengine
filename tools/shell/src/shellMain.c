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

#define __USE_XOPEN
#include "os.h"
#include "shell.h"
#include "tglobal.h"
#include "tconfig.h"
#include "shellCommand.h"
#include "tbase64.h"
#include "tlog.h"
#include "version.h"

#include <wordexp.h>
#include <argp.h>
#include <termio.h>

#define OPT_ABORT 1 /* abort */


int indicator = 1;

void insertChar(Command *cmd, char *c, int size);
void taosNetTest(char *role, char *host, int32_t port, int32_t pkgLen,
                 int32_t pkgNum, char *pkgType);
const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "";

TdThread pid;
static tsem_t cancelSem;

static struct argp_option options[] = {
  {"host",       'h', "HOST",       0,                   "TDengine server FQDN to connect. The default host is localhost."},
  {"password",   'p', 0,   0,                   "The password to use when connecting to the server."},
  {"port",       'P', "PORT",       0,                   "The TCP/IP port number to use for the connection."},
  {"user",       'u', "USER",       0,                   "The user name to use when connecting to the server."},
  {"auth",       'A', "Auth",       0,                   "The auth string to use when connecting to the server."},
  {"config-dir", 'c', "CONFIG_DIR", 0,                   "Configuration directory."},
  {"dump-config", 'C', 0,           0,                   "Dump configuration."},
  {"commands",   's', "COMMANDS",   0,                   "Commands to run without enter the shell."},
  {"raw-time",   'r', 0,            0,                   "Output time as uint64_t."},
  {"file",       'f', "FILE",       0,                   "Script to run without enter the shell."},
  {"directory",  'D', "DIRECTORY",  0,                   "Use multi-thread to import all SQL files in the directory separately."},
  {"thread",     'T', "THREADNUM",  0,                   "Number of threads when using multi-thread to import data."},
  {"check",      'k', "CHECK",      0,                   "Check tables."},
  {"database",   'd', "DATABASE",   0,                   "Database to use when connecting to the server."},
  {"timezone",   'z', "TIMEZONE",   0,                   "Time zone of the shell, default is local."},
  {"netrole",    'n', "NETROLE",    0,                   "Net role when network connectivity test, default is startup, options: client|server|rpc|startup|sync|speed|fqdn."},
  {"pktlen",     'l', "PKTLEN",     0,                   "Packet length used for net test, default is 1000 bytes."},
  {"pktnum",     'N', "PKTNUM",     0,                   "Packet numbers used for net test, default is 100."},
// Shuduo: 3.0 does not support UDP any more
//  {"pkttype",    'S', "PKTTYPE",    0,                   "Packet type used for net test, default is TCP."},
  {0}};

static error_t parse_opt(int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
  know is a pointer to our arguments structure. */
  SShellArguments *arguments = state->input;
  wordexp_t full_path;

  switch (key) {
    case 'h':
      arguments->host = arg;
      break;
    case 'p':
      break;
    case 'P':
      if (arg) {
        arguments->port  = atoi(arg);
      } else {
        fprintf(stderr, "Invalid port\n");
        return -1;
      }

      break;
    case 'z':
      arguments->timezone = arg;
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'A':
      arguments->auth = arg;
      break;
    case 'c':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      if (strlen(full_path.we_wordv[0]) >= TSDB_FILENAME_LEN) {
        fprintf(stderr, "config file path: %s overflow max len %d\n", full_path.we_wordv[0], TSDB_FILENAME_LEN - 1);
        wordfree(&full_path);
        return -1;
      }
      tstrncpy(configDir, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'C':
      arguments->dump_config = true;
      break;
    case 's':
      arguments->commands = arg;
      break;
    case 'r':
      arguments->is_raw_time = true;
      break;
    case 'f':
      if ((0 == strlen(arg)) || (wordexp(arg, &full_path, 0) != 0)) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      tstrncpy(arguments->file, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'D':
      if (wordexp(arg, &full_path, 0) != 0) {
        fprintf(stderr, "Invalid path %s\n", arg);
        return -1;
      }
      tstrncpy(arguments->dir, full_path.we_wordv[0], TSDB_FILENAME_LEN);
      wordfree(&full_path);
      break;
    case 'T':
      if (arg) {
        arguments->threadNum = atoi(arg);
      } else {
        fprintf(stderr, "Invalid number of threads\n");
        return -1;
      }
      break;
    case 'k':
      arguments->check = atoi(arg);
      break;
    case 'd':
      arguments->database = arg;
      break;
    case 'n':
      arguments->netTestRole = arg;
      break;
    case 'l':
      if (arg) {
        arguments->pktLen = atoi(arg);
      } else {
        fprintf(stderr, "Invalid packet length\n");
        return -1;
      }
      break;
    case 'N':
      if (arg) {
        arguments->pktNum = atoi(arg);
      } else {
        fprintf(stderr, "Invalid packet number\n");
        return -1;
      }
      break;
    case 'S':
      arguments->pktType = arg;
      break;
    case OPT_ABORT:
      arguments->abort = 1;
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

/* Our argp parser. */
static struct argp argp = {options, parse_opt, args_doc, doc};

char      LINUXCLIENT_VERSION[] = "Welcome to the TDengine shell from %s, Client Version:%s\n"
                             "Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.\n\n";
char g_password[SHELL_MAX_PASSWORD_LEN];

static void parse_args(
        int argc, char *argv[], SShellArguments *arguments) {
    for (int i = 1; i < argc; i++) {
        if ((strncmp(argv[i], "-p", 2) == 0)
              || (strncmp(argv[i], "--password", 10) == 0)) {
            printf(LINUXCLIENT_VERSION, tsOsName, taos_get_client_info());
            if ((strlen(argv[i]) == 2)
                  || (strncmp(argv[i], "--password", 10) == 0)) {
                printf("Enter password: ");
                taosSetConsoleEcho(false);
                if (scanf("%20s", g_password) > 1) {
                    fprintf(stderr, "password reading error\n");
                }
                taosSetConsoleEcho(true);
                if (EOF == getchar()) {
                    fprintf(stderr, "getchar() return EOF\n");
                }
            } else {
                tstrncpy(g_password, (char *)(argv[i] + 2), SHELL_MAX_PASSWORD_LEN);
                strcpy(argv[i], "-p");
            }
            arguments->password = g_password;
            arguments->is_use_passwd = true;
        }
    }
}

void shellParseArgument(int argc, char *argv[], SShellArguments *arguments) {
  static char verType[32] = {0};
  sprintf(verType, "version: %s\n", version);

  argp_program_version = verType;

  if (argc > 1) {
    parse_args(argc, argv, arguments);
  }

  argp_parse(&argp, argc, argv, 0, 0, arguments);
  if (arguments->abort) {
    #ifndef _ALPINE
    #if 0
      error(10, 0, "ABORTED");
    #endif  
    #else
      abort();
    #endif
  }
}

int32_t shellReadCommand(TAOS *con, char *command) {
  unsigned hist_counter = history.hend;
  char utf8_array[10] = "\0";
  Command cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.buffer = (char *)taosMemoryCalloc(1, MAX_COMMAND_SIZE);
  cmd.command = (char *)taosMemoryCalloc(1, MAX_COMMAND_SIZE);
  showOnScreen(&cmd);

  // Read input.
  char c;
  while (1) {
    c = (char)getchar(); // getchar() return an 'int' value

    if (c == EOF) {
      return c;
    }

    if (c < 0) {  // For UTF-8
      int count = countPrefixOnes(c);
      utf8_array[0] = c;
      for (int k = 1; k < count; k++) {
        c = (char)getchar();
        utf8_array[k] = c;
      }
      insertChar(&cmd, utf8_array, count);
    } else if (c < '\033') {
      // Ctrl keys.  TODO: Implement ctrl combinations
      switch (c) {
        case 1:  // ctrl A
          positionCursorHome(&cmd);
          break;
        case 3:
          printf("\n");
          resetCommand(&cmd, "");
          kill(0, SIGINT);
          break;
        case 4:  // EOF or Ctrl+D
          printf("\n");
          taos_close(con);
          // write the history
          write_history();
          exitShell();
          break;
        case 5:  // ctrl E
          positionCursorEnd(&cmd);
          break;
        case 8:
          backspaceChar(&cmd);
          break;
        case '\n':
        case '\r':
          printf("\n");
          if (isReadyGo(&cmd)) {
            sprintf(command, "%s%s", cmd.buffer, cmd.command);
            taosMemoryFreeClear(cmd.buffer);
            taosMemoryFreeClear(cmd.command);
            return 0;
          } else {
            updateBuffer(&cmd);
          }
          break;
        case 11:  // Ctrl + K;
          clearLineAfter(&cmd);
          break;
        case 12:  // Ctrl + L;
          system("clear");
          showOnScreen(&cmd);
          break;
        case 21:  // Ctrl + U;
          clearLineBefore(&cmd);
          break;
      }
    } else if (c == '\033') {
      c = (char)getchar();
      switch (c) {
        case '[':
          c = (char)getchar();
          switch (c) {
            case 'A':  // Up arrow
              if (hist_counter != history.hstart) {
                hist_counter = (hist_counter + MAX_HISTORY_SIZE - 1) % MAX_HISTORY_SIZE;
                resetCommand(&cmd, (history.hist[hist_counter] == NULL) ? "" : history.hist[hist_counter]);
              }
              break;
            case 'B':  // Down arrow
              if (hist_counter != history.hend) {
                int next_hist = (hist_counter + 1) % MAX_HISTORY_SIZE;

                if (next_hist != history.hend) {
                  resetCommand(&cmd, (history.hist[next_hist] == NULL) ? "" : history.hist[next_hist]);
                } else {
                  resetCommand(&cmd, "");
                }
                hist_counter = next_hist;
              }
              break;
            case 'C':  // Right arrow
              moveCursorRight(&cmd);
              break;
            case 'D':  // Left arrow
              moveCursorLeft(&cmd);
              break;
            case '1':
              if ((c = (char)getchar()) == '~') {
                // Home key
                positionCursorHome(&cmd);
              }
              break;
            case '2':
              if ((c = (char)getchar()) == '~') {
                // Insert key
              }
              break;
            case '3':
              if ((c = (char)getchar()) == '~') {
                // Delete key
                deleteChar(&cmd);
              }
              break;
            case '4':
              if ((c = (char)getchar()) == '~') {
                // End key
                positionCursorEnd(&cmd);
              }
              break;
            case '5':
              if ((c = (char)getchar()) == '~') {
                // Page up key
              }
              break;
            case '6':
              if ((c = (char)getchar()) == '~') {
                // Page down key
              }
              break;
            case 72:
              // Home key
              positionCursorHome(&cmd);
              break;
            case 70:
              // End key
              positionCursorEnd(&cmd);
              break;
          }
          break;
      }
    } else if (c == 0x7f) {
      // press delete key
      backspaceChar(&cmd);
    } else {
      insertChar(&cmd, &c, 1);
    }
  }

  return 0;
}

void *shellLoopQuery(void *arg) {
  if (indicator) {
    getOldTerminalMode();
    indicator = 0;
  }

  TAOS *con = (TAOS *)arg;

  setThreadName("shellLoopQuery");

  taosThreadCleanupPush(cleanup_handler, NULL);

  char *command = taosMemoryMalloc(MAX_COMMAND_SIZE);
  if (command == NULL){
    uError("failed to malloc command");
    return NULL;
  }

  int32_t err = 0;
  
  do {
    // Read command from shell.
    memset(command, 0, MAX_COMMAND_SIZE);
    setTerminalMode();
    err = shellReadCommand(con, command);
    if (err) {
      break;
    }
    resetTerminalMode();
  } while (shellRunCommand(con, command) == 0);
  
  taosMemoryFreeClear(command);
  exitShell();

  taosThreadCleanupPop(1);
  
  return NULL;
}

void get_history_path(char *_history) { snprintf(_history, TSDB_FILENAME_LEN, "%s/%s", getenv("HOME"), HISTORY_FILE); }

void clearScreen(int ecmd_pos, int cursor_pos) {
  struct winsize w;
  if (ioctl(0, TIOCGWINSZ, &w) < 0 || w.ws_col == 0 || w.ws_row == 0) {
    //fprintf(stderr, "No stream device, and use default value(col 120, row 30)\n");
    w.ws_col = 120;
    w.ws_row = 30;
  }

  int cursor_x = cursor_pos / w.ws_col;
  int cursor_y = cursor_pos % w.ws_col;
  int command_x = ecmd_pos / w.ws_col;
  positionCursor(cursor_y, LEFT);
  positionCursor(command_x - cursor_x, DOWN);
  fprintf(stdout, "\033[2K");
  for (int i = 0; i < command_x; i++) {
    positionCursor(1, UP);
    fprintf(stdout, "\033[2K");
  }
  fflush(stdout);
}

void showOnScreen(Command *cmd) {
  struct winsize w;
  if (ioctl(0, TIOCGWINSZ, &w) < 0 || w.ws_col == 0 || w.ws_row == 0) {
    //fprintf(stderr, "No stream device\n");
    w.ws_col = 120;
    w.ws_row = 30;
  }

  TdWchar wc;
  int size = 0;

  // Print out the command.
  char *total_string = taosMemoryMalloc(MAX_COMMAND_SIZE);
  memset(total_string, '\0', MAX_COMMAND_SIZE);
  if (strcmp(cmd->buffer, "") == 0) {
    sprintf(total_string, "%s%s", PROMPT_HEADER, cmd->command);
  } else {
    sprintf(total_string, "%s%s", CONTINUE_PROMPT, cmd->command);
  }

  int remain_column = w.ws_col;
  /* size = cmd->commandSize + prompt_size; */
  for (char *str = total_string; size < cmd->commandSize + prompt_size;) {
    int ret = taosMbToWchar(&wc, str, MB_CUR_MAX);
    if (ret < 0) break;
    size += ret;
    /* assert(size >= 0); */
    int width = taosWcharWidth(wc);
    if (remain_column > width) {
      printf("%lc", wc);
      remain_column -= width;
    } else {
      if (remain_column == width) {
        printf("%lc\n\r", wc);
        remain_column = w.ws_col;
      } else {
        printf("\n\r%lc", wc);
        remain_column = w.ws_col - width;
      }
    }

    str = total_string + size;
  }

  taosMemoryFree(total_string);
  /* for (int i = 0; i < size; i++){ */
  /*     char c = total_string[i]; */
  /*     if (k % w.ws_col == 0) { */
  /*         printf("%c\n\r", c); */
  /*     } */
  /*     else { */
  /*         printf("%c", c); */
  /*     } */
  /*     k += 1; */
  /* } */

  // Position the cursor
  int cursor_pos = cmd->screenOffset + prompt_size;
  int ecmd_pos = cmd->endOffset + prompt_size;

  int cursor_x = cursor_pos / w.ws_col;
  int cursor_y = cursor_pos % w.ws_col;
  // int cursor_y = cursor % w.ws_col;
  int command_x = ecmd_pos / w.ws_col;
  int command_y = ecmd_pos % w.ws_col;
  // int command_y = (command.size() + prompt_size) % w.ws_col;
  positionCursor(command_y, LEFT);
  positionCursor(command_x, UP);
  positionCursor(cursor_x, DOWN);
  positionCursor(cursor_y, RIGHT);
  fflush(stdout);
}

void cleanup_handler(void *arg) { resetTerminalMode(); }

void exitShell() {
  /*int32_t ret =*/ resetTerminalMode();
  taos_cleanup();
  exit(EXIT_SUCCESS);
}
void shellQueryInterruptHandler(int32_t signum, void *sigInfo, void *context) {
  tsem_post(&cancelSem);
}

void *cancelHandler(void *arg) {
  setThreadName("cancelHandler");

  while (1) {
    if (tsem_wait(&cancelSem) != 0) {
      taosMsleep(10);
      continue;
    }

#ifdef LINUX
#if 0
    int64_t rid = atomic_val_compare_exchange_64(&result, result, 0);
    SSqlObj* pSql = taosAcquireRef(tscObjRef, rid);
    taos_stop_query(pSql);
    taosReleaseRef(tscObjRef, rid);
#endif    
#else
    resetTerminalMode();
    printf("\nReceive ctrl+c or other signal, quit shell.\n");
    exit(0);
#endif
    resetTerminalMode();
    printf("\nReceive ctrl+c or other signal, quit shell.\n");
    exit(0);
  }

  return NULL;
}

int checkVersion() {
  if (sizeof(int8_t) != 1) {
    printf("taos int8 size is %d(!= 1)", (int)sizeof(int8_t));
    return 0;
  }
  if (sizeof(int16_t) != 2) {
    printf("taos int16 size is %d(!= 2)", (int)sizeof(int16_t));
    return 0;
  }
  if (sizeof(int32_t) != 4) {
    printf("taos int32 size is %d(!= 4)", (int)sizeof(int32_t));
    return 0;
  }
  if (sizeof(int64_t) != 8) {
    printf("taos int64 size is %d(!= 8)", (int)sizeof(int64_t));
    return 0;
  }
  return 1;
}

// Global configurations
SShellArguments args = {.host = NULL,
#ifndef TD_WINDOWS
                        .password = NULL,
#endif
                        .user = NULL,
                        .database = NULL,
                        .timezone = NULL,
                        .is_raw_time = false,
                        .is_use_passwd = false,
                        .dump_config = false,
                        .file = "\0",
                        .dir = "\0",
                        .threadNum = 5,
                        .commands = NULL,
                        .pktLen = 1000,
                        .pktNum = 100,
                        .pktType = "TCP",
                        .netTestRole = NULL};

/*
 * Main function.
 */
int main(int argc, char *argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */

  if (!checkVersion()) {
    exit(EXIT_FAILURE);
  }

  shellParseArgument(argc, argv, &args);

  if (args.dump_config) {
    taosInitCfg(configDir, NULL, NULL, NULL, 1);

    SConfig *pCfg = taosGetCfg();
    if (NULL == pCfg) {
      printf("TDengine read global config failed!\n");
      exit(EXIT_FAILURE);
    }
    cfgDumpCfg(pCfg, 0, 1);
    exit(0);
  }

  if (args.netTestRole && args.netTestRole[0] != 0) {
    TAOS *con = NULL;
    if (args.auth == NULL) {
      con = taos_connect(args.host, args.user, args.password, args.database, args.port);
    } else {
      con = taos_connect_auth(args.host, args.user, args.auth, args.database, args.port);
    }

/*    if (taos_init()) {
      printf("Failed to init taos");
      exit(EXIT_FAILURE);
    }
    */
    taosNetTest(args.netTestRole, args.host, args.port, args.pktLen, args.pktNum, args.pktType);
    taos_close(con);
    exit(0);
  }

  /* Initialize the shell */
  TAOS *con = shellInit(&args);
  if (con == NULL) {
    exit(EXIT_FAILURE);
  }

  if (tsem_init(&cancelSem, 0, 0) != 0) {
    printf("failed to create cancel semphore\n");
    exit(EXIT_FAILURE);
  }

  TdThread spid;
  taosThreadCreate(&spid, NULL, cancelHandler, NULL);

  /* Interrupt handler. */
  taosSetSignal(SIGTERM, shellQueryInterruptHandler);
  taosSetSignal(SIGINT, shellQueryInterruptHandler);
  taosSetSignal(SIGHUP, shellQueryInterruptHandler);
  taosSetSignal(SIGABRT, shellQueryInterruptHandler);

  /* Get grant information */
  shellGetGrantInfo(con);

  /* Loop to query the input. */
  while (1) {
    taosThreadCreate(&pid, NULL, shellLoopQuery, con);
    taosThreadJoin(pid, NULL);
  }
}
