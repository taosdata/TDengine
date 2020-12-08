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
#include "tglobal.h"
#include "shell.h"
#include "shellCommand.h"
#include "tkey.h"
#include "tulog.h"

#define OPT_ABORT 1 /* �Cabort */

int indicator = 1;
struct termios oldtio;

extern int wcwidth(wchar_t c);
void insertChar(Command *cmd, char *c, int size);
const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";
static char doc[] = "";
static char args_doc[] = "";
static struct argp_option options[] = {
  {"host",       'h', "HOST",       0,                   "TDengine server IP address to connect. The default host is localhost."},
  {"password",   'p', "PASSWORD",   OPTION_ARG_OPTIONAL, "The password to use when connecting to the server."},
  {"port",       'P', "PORT",       0,                   "The TCP/IP port number to use for the connection."},
  {"user",       'u', "USER",       0,                   "The user name to use when connecting to the server."},
  {"user",       'A', "Auth",       0,                   "The user auth to use when connecting to the server."},
  {"config-dir", 'c', "CONFIG_DIR", 0,                   "Configuration directory."},
  {"commands",   's', "COMMANDS",   0,                   "Commands to run without enter the shell."},
  {"raw-time",   'r', 0,            0,                   "Output time as uint64_t."},
  {"file",       'f', "FILE",       0,                   "Script to run without enter the shell."},
  {"directory",  'D', "DIRECTORY",  0,                   "Use multi-thread to import all SQL files in the directory separately."},
  {"thread",     'T', "THREADNUM",  0,                   "Number of threads when using multi-thread to import data."},
  {"database",   'd', "DATABASE",   0,                   "Database to use when connecting to the server."},
  {"timezone",   't', "TIMEZONE",   0,                   "Time zone of the shell, default is local."},
  {"netrole",    'n', "NETROLE",    0,                   "Net role when network connectivity test, default is startup, options: client|server|rpc|startup."},
  {"pktlen",     'l', "PKTLEN",     0,                   "Packet length used for net test, default is 1000 bytes."},
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
      arguments->is_use_passwd = true;
      if (arg) arguments->password = arg;
      break;
    case 'P':
      if (arg) {
        tsDnodeShellPort = atoi(arg);
        arguments->port  = atoi(arg);
      } else {
        fprintf(stderr, "Invalid port\n");
        return -1;
      }

      break;
    case 't':
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
    case 's':
      arguments->commands = arg;
      break;
    case 'r':
      arguments->is_raw_time = true;
      break;
    case 'f':
      if (wordexp(arg, &full_path, 0) != 0) {
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

void shellParseArgument(int argc, char *argv[], SShellArguments *arguments) {
  static char verType[32] = {0};
  sprintf(verType, "version: %s\n", version);

  argp_program_version = verType;
  
  argp_parse(&argp, argc, argv, 0, 0, arguments);
  if (arguments->abort) {
    #ifndef _ALPINE
      error(10, 0, "ABORTED");
    #else
      abort();
    #endif
  }
}

void shellReadCommand(TAOS *con, char *command) {
  unsigned hist_counter = history.hend;
  char utf8_array[10] = "\0";
  Command cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.buffer = (char *)calloc(1, MAX_COMMAND_SIZE);
  cmd.command = (char *)calloc(1, MAX_COMMAND_SIZE);
  showOnScreen(&cmd);

  // Read input.
  char c;
  while (1) {
    c = (char)getchar(); // getchar() return an 'int' value

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
            tfree(cmd.buffer);
            tfree(cmd.command);
            return;
          } else {
            updateBuffer(&cmd);
          }
          break;
        case 12:  // Ctrl + L;
          system("clear");
          showOnScreen(&cmd);
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
}

void *shellLoopQuery(void *arg) {
  if (indicator) {
    get_old_terminal_mode(&oldtio);
    indicator = 0;
  }

  TAOS *con = (TAOS *)arg;

  pthread_cleanup_push(cleanup_handler, NULL);

  char *command = malloc(MAX_COMMAND_SIZE);
  if (command == NULL){
    uError("failed to malloc command");
    return NULL;
  }
  
  do {
    // Read command from shell.
    memset(command, 0, MAX_COMMAND_SIZE);
    set_terminal_mode();
    shellReadCommand(con, command);
    reset_terminal_mode();
  } while (shellRunCommand(con, command) == 0);
  
  tfree(command);
  exitShell();

  pthread_cleanup_pop(1);
  
  return NULL;
}

int get_old_terminal_mode(struct termios *tio) {
  /* Make sure stdin is a terminal. */
  if (!isatty(STDIN_FILENO)) {
    return -1;
  }

  // Get the parameter of current terminal
  if (tcgetattr(0, &oldtio) != 0) {
    return -1;
  }

  return 1;
}

void reset_terminal_mode() {
  if (tcsetattr(0, TCSANOW, &oldtio) != 0) {
    fprintf(stderr, "Fail to reset the terminal properties!\n");
    exit(EXIT_FAILURE);
  }
}

void set_terminal_mode() {
  struct termios newtio;

  /* if (atexit(reset_terminal_mode) != 0) { */
  /*     fprintf(stderr, "Error register exit function!\n"); */
  /*     exit(EXIT_FAILURE); */
  /* } */

  memcpy(&newtio, &oldtio, sizeof(oldtio));

  // Set new terminal attributes.
  newtio.c_iflag &= ~(IXON | IXOFF | ICRNL | INLCR | IGNCR | IMAXBEL | ISTRIP);
  newtio.c_iflag |= IGNBRK;

  // newtio.c_oflag &= ~(OPOST|ONLCR|OCRNL|ONLRET);
  newtio.c_oflag |= OPOST;
  newtio.c_oflag |= ONLCR;
  newtio.c_oflag &= ~(OCRNL | ONLRET);

  newtio.c_lflag &= ~(IEXTEN | ICANON | ECHO | ECHOE | ECHONL | ECHOCTL | ECHOPRT | ECHOKE | ISIG);
  newtio.c_cc[VMIN] = 1;
  newtio.c_cc[VTIME] = 0;

  if (tcsetattr(0, TCSANOW, &newtio) != 0) {
    fprintf(stderr, "Fail to set terminal properties!\n");
    exit(EXIT_FAILURE);
  }
}

void get_history_path(char *history) { snprintf(history, TSDB_FILENAME_LEN, "%s/%s", getenv("HOME"), HISTORY_FILE); }

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

  wchar_t wc;
  int size = 0;

  // Print out the command.
  char *total_string = malloc(MAX_COMMAND_SIZE);
  memset(total_string, '\0', MAX_COMMAND_SIZE);
  if (strcmp(cmd->buffer, "") == 0) {
    sprintf(total_string, "%s%s", PROMPT_HEADER, cmd->command);
  } else {
    sprintf(total_string, "%s%s", CONTINUE_PROMPT, cmd->command);
  }

  int remain_column = w.ws_col;
  /* size = cmd->commandSize + prompt_size; */
  for (char *str = total_string; size < cmd->commandSize + prompt_size;) {
    int ret = mbtowc(&wc, str, MB_CUR_MAX);
    if (ret < 0) break;
    size += ret;
    /* assert(size >= 0); */
    int width = wcwidth(wc);
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

  free(total_string);
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

void cleanup_handler(void *arg) { tcsetattr(0, TCSANOW, &oldtio); }

void exitShell() {
  /*int32_t ret =*/ tcsetattr(STDIN_FILENO, TCSANOW, &oldtio);
  taos_cleanup();
  exit(EXIT_SUCCESS);
}
