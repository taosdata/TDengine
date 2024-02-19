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
#include "shellInt.h"
#include "shellAuto.h"

SShellObj shell = {0};


void shellCrashHandler(int signum, void *sigInfo, void *context) {
  taosIgnSignal(SIGTERM);
  taosIgnSignal(SIGHUP);
  taosIgnSignal(SIGINT);
  taosIgnSignal(SIGBREAK);

#if !defined(WINDOWS)
  taosIgnSignal(SIGBUS);
#endif
  taosIgnSignal(SIGABRT);
  taosIgnSignal(SIGFPE);
  taosIgnSignal(SIGSEGV);

  tscWriteCrashInfo(signum, sigInfo, context);

#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

int main(int argc, char *argv[]) {
  shell.exit = false;
#ifdef WEBSOCKET
  shell.args.timeout = SHELL_WS_TIMEOUT;
  shell.args.cloud = true;
  shell.args.local = false;
#endif

#if !defined(WINDOWS)
  taosSetSignal(SIGBUS, shellCrashHandler);
#endif
  taosSetSignal(SIGABRT, shellCrashHandler);
  taosSetSignal(SIGFPE, shellCrashHandler);
  taosSetSignal(SIGSEGV, shellCrashHandler);

  if (shellCheckIntSize() != 0) {
    return -1;
  }

  if (shellParseArgs(argc, argv) != 0) {
    return -1;
  }

  if (shell.args.is_version) {
    shellPrintVersion();
    return 0;
  }

  if (shell.args.is_gen_auth) {
    shellGenerateAuth();
    return 0;
  }

  if (shell.args.is_help) {
    shellPrintHelp();
    return 0;
  }
#ifdef WEBSOCKET
  shellCheckConnectMode();
#endif
  if (taos_init() != 0) {
    return -1;
  }

  // kill heart-beat thread when quit
  taos_set_hb_quit(1);

  if (shell.args.is_dump_config) {
    shellDumpConfig();
    taos_cleanup();
    return 0;
  }

  if (shell.args.is_startup || shell.args.is_check) {
    shellCheckServerStatus();
    taos_cleanup();
    return 0;
  }

  if (shell.args.netrole != NULL) {
    shellTestNetWork();
    taos_cleanup();
    return 0;
  }

  // support port feature
  shellAutoInit();
  int32_t ret = shellExecute();
  shellAutoExit();
  return ret;
}
