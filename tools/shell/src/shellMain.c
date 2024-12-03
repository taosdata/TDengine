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
#include "shellAuto.h"
#include "shellInt.h"

extern SShellObj shell;

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

  taosGenCrashJsonMsg(signum, sigInfo, "shell", 0, 0);

#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

int main(int argc, char *argv[]) {
  shell.exit = false;
  shell.args.timeout = SHELL_WS_TIMEOUT;
#ifdef WEBSOCKET
  shell.args.is_internal = false;
#else
  shell.args.is_internal = true;
#endif

#if 0
#if !defined(WINDOWS)
  taosSetSignal(SIGBUS, shellCrashHandler);
#endif
  taosSetSignal(SIGABRT, shellCrashHandler);
  taosSetSignal(SIGFPE, shellCrashHandler);
  taosSetSignal(SIGSEGV, shellCrashHandler);
#endif

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

  if (shell.args.netrole != NULL) {
    shellTestNetWork();
    return 0;
  }

  if (shellCheckDsn() != 0) {
    return -1;
  }

  const char *driverType = shell.args.is_internal ? "internal" : "websocket";
  if (taos_options(TSDB_OPTION_DRIVER, driverType) != 0) {
    printf("failed to load driver since %s [0x%x]\r\n", terrstr(), terrno);
    return -1;
  }

  if (taos_init() != 0) {
    printf("failed to init shell since %s [0x%x]\r\n", terrstr(), terrno);
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
