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

#include "shellInt.h"

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32) || defined(_TD_DARWIN_64)
void shellPrintHelp() {
  char indent[10] = "        ";
  printf("taos shell is used to test the TDengine database\n");
  printf("%s%s\n", indent, "-h");
  printf("%s%s%s\n", indent, indent, "TDengine server FQDN to connect. The default host is localhost.");
  printf("%s%s\n", indent, "-P");
  printf("%s%s%s\n", indent, indent, "The TCP/IP port number to use for the connection");
  printf("%s%s\n", indent, "-u");
  printf("%s%s%s\n", indent, indent, "The user name to use when connecting to the server.");
  printf("%s%s\n", indent, "-p");
  printf("%s%s%s\n", indent, indent, "The password to use when connecting to the server.");
  printf("%s%s\n", indent, "-a");
  printf("%s%s%s\n", indent, indent, "The user auth to use when connecting to the server.");
  printf("%s%s\n", indent, "-A");
  printf("%s%s%s\n", indent, indent, "Generate auth string from password.");
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
  printf("%s%s\n", indent, "-k");
  printf("%s%s%s\n", indent, indent, "Check the service status.");
  printf("%s%s\n", indent, "-t");
  printf("%s%s%s\n", indent, indent, "Check the details of the service status.");
  printf("%s%s\n", indent, "-w");
  printf("%s%s%s\n", indent, indent, "Set the default binary display width.");
  printf("%s%s\n", indent, "-n");
  printf("%s%s%s\n", indent, indent, "Net role when network connectivity test, options: client|server.");
  printf("%s%s\n", indent, "-l");
  printf("%s%s%s\n", indent, indent, "Packet length used for net test, default is 1000 bytes.");
  printf("%s%s\n", indent, "-N");
  printf("%s%s%s\n", indent, indent, "Packet numbers used for net test, default is 100.");
  printf("%s%s\n", indent, "-V");
  printf("%s%s%s\n", indent, indent, "Print program version.");
}

void shellParseArgsInWindows(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0) {
      if (i < argc - 1) {
        arguments->host = argv[++i];
      } else {
        fprintf(stderr, "option -h requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-P") == 0) {
      if (i < argc - 1) {
        arguments->port = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -P requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-u") == 0) {
      if (i < argc - 1) {
        arguments->user = argv[++i];
      } else {
        fprintf(stderr, "option -u requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if ((strncmp(argv[i], "-p", 2) == 0) || (strncmp(argv[i], "--password", 10) == 0)) {
      continue;
    } else if (strcmp(argv[i], "-a") == 0) {
      if (i < argc - 1) {
        arguments->auth = argv[++i];
      } else {
        fprintf(stderr, "option -a requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-A") == 0) {
      arguments->is_gen_auth = true;
    } else if (strcmp(argv[i], "-c") == 0) {
      if (i < argc - 1) {
        arguments->cfgdir = argv[++i];
      } else {
        fprintf(stderr, "Option -c requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-C") == 0) {
      arguments->is_dump_config = true;
    } else if (strcmp(argv[i], "-s") == 0) {
      if (i < argc - 1) {
        arguments->commands = argv[++i];
      } else {
        fprintf(stderr, "option -s requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-r") == 0) {
      arguments->is_raw_time = true;
    } else if (strcmp(argv[i], "-f") == 0) {
      if (i < argc - 1) {
        strcpy(arguments->file, argv[++i]);
      } else {
        fprintf(stderr, "option -f requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-d") == 0) {
      if (i < argc - 1) {
        arguments->database = argv[++i];
      } else {
        fprintf(stderr, "option -d requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-k") == 0) {
      arguments->is_check = true;
    } else if (strcmp(argv[i], "-t") == 0) {
      arguments->is_startup = true;
    } else if (strcmp(argv[i], "-w") == 0) {
      if (i < argc - 1) {
        arguments->displayWidth = argv[++i];
      } else {
        fprintf(stderr, "option -w requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-n") == 0) {
      if (i < argc - 1) {
        arguments->netTestRole = argv[++i];
      } else {
        fprintf(stderr, "option -n requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-l") == 0) {
      if (i < argc - 1) {
        arguments->pktLen = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -l requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-N") == 0) {
      if (i < argc - 1) {
        arguments->pktNum = atoi(argv[++i]);
      } else {
        fprintf(stderr, "option -N requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-S") == 0) {
      if (i < argc - 1) {
        arguments->pktType = argv[++i];
      } else {
        fprintf(stderr, "option -S requires an argument\n");
        exit(EXIT_FAILURE);
      }
    } else if (strcmp(argv[i], "-V") == 0) {
      arguments->is_version = true;
    } else if (strcmp(argv[i], "--help") == 0) {
      arguments->is_help = true;
    } else {
      fprintf(stderr, "wrong options\n");
      arguments->is_help = true;
    }
  }
}

#else

#include <argp.h>
#include <termio.h>
const char *argp_program_version = version;
const char *argp_program_bug_address = "<support@taosdata.com>";

static struct argp_option shellOptions[] = {
    {"host", 'h', "HOST", 0, "TDengine server FQDN to connect. The default host is localhost."},
    {"port", 'P', "PORT", 0, "The TCP/IP port number to use for the connection."},
    {"user", 'u', "USER", 0, "The user name to use when connecting to the server."},
    {"password", 'p', "PASSWORD", 0, "The password to use when connecting to the server."},
    {"auth", 'a', "AUTH", 0, "The auth string to use when connecting to the server."},
    {"generate-auth", 'A', 0, 0, "Generate auth string from password."},
    {"config-dir", 'c', "CONFIG_DIR", 0, "Configuration directory."},
    {"dump-config", 'C', 0, 0, "Dump configuration."},
    {"commands", 's', "COMMANDS", 0, "Commands to run without enter the shell."},
    {"raw-time", 'r', 0, 0, "Output time as uint64_t."},
    {"file", 'f', "FILE", 0, "Script to run without enter the shell."},
    {"database", 'd', "DATABASE", 0, "Database to use when connecting to the server."},
    {"check", 'k', 0, 0, "Check the service status."},
    {"startup", 't', 0, 0, "Check the details of the service status."},
    {"display-width", 'w', 0, 0, "Set the default binary display width."},
    {"netrole", 'n', "NETROLE", 0, "Net role when network connectivity test, options: client|server."},
    {"pktlen", 'l', "PKTLEN", 0, "Packet length used for net test, default is 1000 bytes."},
    {"pktnum", 'N', "PKTNUM", 0, "Packet numbers used for net test, default is 100."},
    {"version", 'V', 0, 0, "Print client version number."},
    {0},
};

static error_t shellParseOpt(int32_t key, char *arg, struct argp_state *state) {
  SShellArgs *arguments = &shell.args;
  wordexp_t   full_path = {0};

  switch (key) {
    case 'h':
      arguments->host = arg;
      break;
    case 'P':
      arguments->port = atoi(arg);
      break;
    case 'u':
      arguments->user = arg;
      break;
    case 'p':
      break;
    case 'a':
      arguments->auth = arg;
      break;
    case 'A':
      arguments->is_gen_auth = true;
      break;
    case 'c':
      arguments->cfgdir = arg;
      break;
    case 'C':
      arguments->is_dump_config = true;
      break;
    case 's':
      arguments->commands = arg;
      break;
    case 'r':
      arguments->is_raw_time = true;
      break;
    case 'f':
      arguments->file = arg;
      break;
    case 'd':
      arguments->database = arg;
      break;
    case 'k':
      arguments->is_check = true;
      break;
    case 't':
      arguments->is_startup = true;
      break;
    case 'w':
      arguments->displayWidth = atoi(arg);
      break;
    case 'n':
      arguments->netrole = arg;
      break;
    case 'l':
      arguments->pktLen = atoi(arg);
      break;
    case 'N':
      arguments->pktNum = atoi(arg);
      break;
    case 'V':
      arguments->is_version = true;
      break;
    case 1:
      arguments->abort = 1;
      break;
    default:
      return ARGP_ERR_UNKNOWN;
  }
  return 0;
}

static struct argp shellArgp = {shellOptions, shellParseOpt, "", ""};

static void shellParseArgsInLinux(int argc, char *argv[]) {
  argp_program_version = shell.info.programVersion;
  argp_parse(&shellArgp, argc, argv, 0, 0, NULL);
}

#endif

static void shellInitArgs(int argc, char *argv[]) {
  for (int i = 1; i < argc; i++) {
    if ((strncmp(argv[i], "-p", 2) == 0) || (strncmp(argv[i], "--password", 10) == 0)) {
      printf(shell.info.clientVersion, tsOsName, taos_get_client_info());
      if ((strlen(argv[i]) == 2) || (strncmp(argv[i], "--password", 10) == 0)) {
        printf("Enter password: ");
        taosSetConsoleEcho(false);
        if (scanf("%20s", shell.args.password) > 1) {
          fprintf(stderr, "password reading error\n");
        }
        taosSetConsoleEcho(true);
        if (EOF == getchar()) {
          fprintf(stderr, "getchar() return EOF\n");
        }
      } else {
        tstrncpy(shell.args.password, (char *)(argv[i] + 2), sizeof(shell.args.password));
        strcpy(argv[i], "-p");
      }
    }
  }
  if (strlen(shell.args.password) == 0) {
    tstrncpy(shell.args.password, TSDB_DEFAULT_PASS, sizeof(shell.args.password));
  }

  shell.args.pktLen = 1024;
  shell.args.pktNum = 100;
  shell.args.displayWidth = SHELL_DEFAULT_MAX_BINARY_DISPLAY_WIDTH;
  shell.args.user = TSDB_DEFAULT_USER;
}

static int32_t shellCheckArgs() { return 0; }

int32_t shellParseArgs(int32_t argc, char *argv[]) {
  shellInitArgs(argc, argv);
  shell.info.clientVersion =
      "Welcome to the TDengine shell from %s, Client Version:%s\n"
      "Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.\n\n";
  shell.info.promptHeader = "taos> ";
  shell.info.promptContinue = "   -> ";
  shell.info.promptSize = 6;
  snprintf(shell.info.programVersion, sizeof(shell.info.programVersion), "version: %s\n", version);

#if defined(_TD_WINDOWS_64) || defined(_TD_WINDOWS_32)
  shell.info.osname = "Windows";
  snprintf(shell.history.file, TSDB_FILENAME_LEN, "C:/TDengine/%s", SHELL_HISTORY_FILE);
  shellParseArgsInLinuxAndDarwin();
#elif defined(_TD_DARWIN_64)
  shell.info.osname = "Darwin";
  snprintf(shell.history.file, TSDB_FILENAME_LEN, "%s/%s", getpwuid(getuid())->pw_dir, SHELL_HISTORY_FILE);
  shellParseArgsInLinuxAndDarwin();
#else
  shell.info.osname = "Linux";
  snprintf(shell.history.file, TSDB_FILENAME_LEN, "%s/%s", getenv("HOME"), SHELL_HISTORY_FILE);
  shellParseArgsInLinux(argc, argv);
#endif

  if (shell.args.abort) {
    return -1;
  }

  return shellCheckArgs();
}
