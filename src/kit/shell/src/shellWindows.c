/*******************************************************************
*           Copyright (c) 2017 by TAOS Technologies, Inc.
*                     All rights reserved.
*
*  This file is proprietary and confidential to TAOS Technologies.
*  No part of this file may be reproduced, stored, transmitted,
*  disclosed or used in any form or by any means other than as
*  expressly provided by the written permission from Jianhui Tao
*
* ****************************************************************/

#include <assert.h>
#include <regex.h>
#include <stdio.h>
#include "os.h"
#include "shell.h"
#include "taos.h"
#include "shellCommand.h"

#define SHELL_INPUT_MAX_COMMAND_SIZE 10000

extern char configDir[];

char      WINCLIENT_VERSION[] = "Welcome to the TDengine shell from %s, Client Version:%s\n"
                             "Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.\n\n";

void printVersion() {
  printf("version: %s\n", version);
}

void printHelp() {
  char indent[10] = "        ";
  printf("taos shell is used to test the TDengine database\n");

  printf("%s%s\n", indent, "-h");
  printf("%s%s%s\n", indent, indent, "TDengine server FQDN to connect. The default host is localhost.");
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s\n", indent, indent, "The password to use when connecting to the server.");
  printf("%s%s\n", indent, "-P");
  printf("%s%s%s\n", indent, indent, "The TCP/IP port number to use for the connection");
  printf("%s%s\n", indent, "-u");
  printf("%s%s%s\n", indent, indent, "The user name to use when connecting to the server.");
  printf("%s%s\n", indent, "-A");
  printf("%s%s%s\n", indent, indent, "The user auth to use when connecting to the server.");
  printf("%s%s\n", indent, "-c");
  printf("%s%s%s\n", indent, indent, "Configuration directory.");
  printf("%s%s\n", indent, "-C");
  printf("%s%s%s\n", indent, indent, "Dump configuration.");
  printf("%s%s\n", indent, "-s");
  printf("%s%s%s\n", indent, indent, "Commands to run without enter the shell.");
  printf("%s%s\n", indent, "-r");
  printf("%s%s%s\n", indent, indent, "Output time as unsigned long..");
  printf("%s%s\n", indent, "-f");
  printf("%s%s%s\n", indent, indent, "Script to run without enter the shell.");
  printf("%s%s\n", indent, "-d");
  printf("%s%s%s\n", indent, indent, "Database to use when connecting to the server.");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n", indent, indent, "Time zone of the shell, default is local.");
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s\n", indent, indent, "Net role when network connectivity test, default is startup, options: client|server|rpc|startup|sync|speed|fqdn.");
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s\n", indent, indent, "Packet length used for net test, default is 1000 bytes.");
  printf("%s%s\n", indent, "-N");
  printf("%s%s%s\n", indent, indent, "Packet numbers used for net test, default is 100.");
  printf("%s%s\n", indent, "-S");
  printf("%s%s%s\n", indent, indent, "Packet type used for net test, default is TCP.");
  printf("%s%s\n", indent, "-V");
  printf("%s%s%s\n", indent, indent, "Print program version.");

  exit(EXIT_SUCCESS);
}

char g_password[SHELL_MAX_PASSWORD_LEN];

void shellParseArgument(int argc, char *argv[], SShellArguments *arguments) {
  for (int i = 1; i < argc; i++) {
    // for host
    if (strcmp(argv[i], "-h") == 0) {
      if (i < argc - 1) {
        arguments->host = argv[++i];
      } else {
        fprintf(stderr, "option -h requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for password
    else if ((strncmp(argv[i], "-p", 2) == 0)
            || (strncmp(argv[i], "--password", 10) == 0)) {
        arguments->is_use_passwd = true;
        strcpy(tsOsName, "Windows");
        printf(WINCLIENT_VERSION, tsOsName, taos_get_client_info());
        if ((strlen(argv[i]) == 2)
                  || (strncmp(argv[i], "--password", 10) == 0)) {
            printf("Enter password: ");
            taosSetConsoleEcho(false);
            if (scanf("%s", g_password) > 1) {
                fprintf(stderr, "password read error!\n");
            }
            taosSetConsoleEcho(true);
            getchar();
        } else {
            tstrncpy(g_password, (char *)(argv[i] + 2), SHELL_MAX_PASSWORD_LEN);
        }
        arguments->password = g_password;
        strcpy(argv[i], "");
        argc -= 1;
    }
    // for management port
    else if (strcmp(argv[i], "-P") == 0) {
      if (i < argc - 1) {
        arguments->port = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -P requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for user
    else if (strcmp(argv[i], "-u") == 0) {
      if (i < argc - 1) {
        arguments->user = argv[++i];
      } else {
        fprintf(stderr, "option -u requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-A") == 0) {
      if (i < argc - 1) {
        arguments->auth = argv[++i];
      } else {
        fprintf(stderr, "option -A requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        char *tmp = argv[++i];
        if (strlen(tmp) >= TSDB_FILENAME_LEN) {
          fprintf(stderr, "config file path: %s overflow max len %d\n", tmp, TSDB_FILENAME_LEN - 1);
          exit(EXIT_FAILURE);
        }
        strcpy(configDir, tmp);
      } else {
        fprintf(stderr, "Option -c requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      arguments->dump_config = true;
    } else if (strcmp(argv[i], "-s") == 0) {
      if (i < argc - 1) {
        arguments->commands = argv[++i];
      } else {
        fprintf(stderr, "option -s requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-r") == 0) {
      arguments->is_raw_time = true;
    }
    // For temperory batch commands to run TODO
    else if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        strcpy(arguments->file, argv[++i]);
      } else {
        fprintf(stderr, "option -f requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // for default database
    else if (strcmp(argv[i], "-d") == 0) {
      if (i < argc - 1) {
        arguments->database = argv[++i];
      } else {
        fprintf(stderr, "option -d requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    // For time zone
    else if (strcmp(argv[i], "-t") == 0) {
      if (i < argc - 1) {
        arguments->timezone = argv[++i];
      } else {
        fprintf(stderr, "option -t requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-n") == 0) {
      if (i < argc - 1) {
        arguments->netTestRole = argv[++i];
      } else {
        fprintf(stderr, "option -n requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-l") == 0) {
      if (i < argc - 1) {
        arguments->pktLen = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -l requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-N") == 0) {
      if (i < argc - 1) {
        arguments->pktNum = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -N requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-S") == 0) {
      if (i < argc - 1) {
        arguments->pktType = argv[++i];
      } else {
        fprintf(stderr, "option -S requires an argument\n");
        exit(EXIT_FAILURE);
      }
    }
    else if (strcmp(argv[i], "-V") == 0) {
      printVersion();
      exit(EXIT_SUCCESS);
    }
    // For temperory command TODO
    else if (strcmp(argv[i], "--help") == 0) {
      printHelp();
      exit(EXIT_SUCCESS);
    } else {
      fprintf(stderr, "wrong options\n");
      printHelp();
      exit(EXIT_FAILURE);
    }
  }
}

void shellPrintContinuePrompt() { printf("%s", CONTINUE_PROMPT); }

void shellPrintPrompt() { printf("%s", PROMPT_HEADER); }

void updateBuffer(Command *cmd) {
  if (regex_match(cmd->buffer, "(\\s+$)|(^$)", REG_EXTENDED)) strcat(cmd->command, " ");
  strcat(cmd->buffer, cmd->command);

  memset(cmd->command, 0, MAX_COMMAND_SIZE);
  cmd->cursorOffset = 0;
}

int isReadyGo(Command *cmd) {
  char *total = malloc(MAX_COMMAND_SIZE);
  memset(total, 0, MAX_COMMAND_SIZE);
  sprintf(total, "%s%s", cmd->buffer, cmd->command);

  char *reg_str =
      "(^.*;\\s*$)|(^\\s*$)|(^\\s*exit\\s*$)|(^\\s*q\\s*$)|(^\\s*quit\\s*$)|(^"
      "\\s*clear\\s*$)";
  if (regex_match(total, reg_str, REG_EXTENDED | REG_ICASE)) {
    free(total);
    return 1;
  }

  free(total);
  return 0;
}

void insertChar(Command *cmd, char c) {
  // TODO: Check if the length enough.
  if (cmd->cursorOffset >= MAX_COMMAND_SIZE) {
    fprintf(stdout, "sql is larger than %d bytes", MAX_COMMAND_SIZE);
    return;
  }

  cmd->command[cmd->cursorOffset++] = c;
}

int32_t shellReadCommand(TAOS *con, char command[]) {
  Command cmd;
  memset(&cmd, 0, sizeof(cmd));
  cmd.buffer = (char *)calloc(1, MAX_COMMAND_SIZE);
  cmd.command = (char *)calloc(1, MAX_COMMAND_SIZE);

  // Read input.
  void *console = GetStdHandle(STD_INPUT_HANDLE);
  unsigned long read;
  wchar_t *c= (wchar_t *)calloc(SHELL_INPUT_MAX_COMMAND_SIZE, sizeof(wchar_t));
  char mbStr[16];
  while (1) {
    int ret = ReadConsole(console, c, SHELL_INPUT_MAX_COMMAND_SIZE, &read, NULL);
    for (int input_index = 0; input_index < read; input_index++) {
      int size = WideCharToMultiByte(CP_UTF8, 0, &c[input_index], 1, mbStr, sizeof(mbStr), NULL, NULL);
      mbStr[size] = 0;
      switch (c[input_index]) {
        case '\n':
          if (isReadyGo(&cmd)) {
            sprintf(command, "%s%s", cmd.buffer, cmd.command);
            free(cmd.buffer);
            cmd.buffer = NULL;
            free(cmd.command);
            cmd.command = NULL;
            free(c);
            return 0;
          } else {
            shellPrintContinuePrompt();
            updateBuffer(&cmd);
          }
          break;
        case '\r':
          break;
        default:
          for (int i = 0; i < size; ++i) {
            insertChar(&cmd, mbStr[i]);
          }
      }
    }
  }

  return 0;
}

void *shellLoopQuery(void *arg) {
  TAOS *con = (TAOS *)arg;
  char *command = malloc(MAX_COMMAND_SIZE);
  if (command == NULL) return NULL;

  int32_t err = 0;

  do {
    memset(command, 0, MAX_COMMAND_SIZE);
    shellPrintPrompt();

    // Read command from shell.
    err = shellReadCommand(con, command);
    if (err) {
      break;
    }
  } while (shellRunCommand(con, command) == 0);

  return NULL;
}

void get_history_path(char *history) {
#ifdef _TD_POWER_
  sprintf(history, "C:/PowerDB/%s", HISTORY_FILE); 
#elif (_TD_TQ_ == true)
  sprintf(history, "C:/TQueue/%s", HISTORY_FILE); 
#elif (_TD_PRO_ == true)
  sprintf(history, "C:/ProDB/%s", HISTORY_FILE); 
#elif (_TD_KH_ == true)
  sprintf(history, "C:/KingHistorian/%s", HISTORY_FILE); 
#elif (_TD_JH_ == true)
  sprintf(history, "C:/jh_iot/%s", HISTORY_FILE); 
#else
  sprintf(history, "C:/TDengine/%s", HISTORY_FILE); 
#endif
}

void exitShell() { exit(EXIT_SUCCESS); }
