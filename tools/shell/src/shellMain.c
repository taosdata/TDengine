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
  taosIgnSignal(SIGABRT);
  taosIgnSignal(SIGFPE);
  taosIgnSignal(SIGSEGV);
#if !defined(WINDOWS)
  taosIgnSignal(SIGBUS);
#endif
#ifdef USE_REPORT
  taos_write_crashinfo(signum, sigInfo, context);
#endif
#ifdef _TD_DARWIN_64
  exit(signum);
#elif defined(WINDOWS)
  exit(signum);
#endif
}

// init arguments
void initArgument(SShellArgs *pArgs) {  // 初始化参数，默认值
  pArgs->host     = NULL;
  pArgs->port     = 0;
  pArgs->user     = NULL;
  pArgs->database = NULL;

  // conn mode
  pArgs->dsn      = NULL;
  pArgs->connMode = CONN_MODE_INVALID;

  pArgs->port_inputted = false;
}

int main(int argc, char *argv[]) {
  // 这里是shell 入口
  int code  = 0;
#if !defined(WINDOWS)
  taosSetSignal(SIGBUS, shellCrashHandler);  //注册信号处理函数
#endif
  taosSetSignal(SIGABRT, shellCrashHandler);
  taosSetSignal(SIGFPE, shellCrashHandler);
  taosSetSignal(SIGSEGV, shellCrashHandler);

  initArgument(&shell.args); // 传入参数处理

  if (shellCheckIntSize() != 0) {
    return -1;
  }

  if (shellParseArgs(argc, argv) != 0) { // 解析参数
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

  if (shell.args.is_help) {  // 打印帮助信息
    shellPrintHelp();
    return 0;
  }

  if (shell.args.netrole != NULL) {
    shellTestNetWork();
    return 0;
  }

  if (getDsnEnv() != 0) {
    return -1;
  }

  // first taos_option(TSDB_OPTION_DRIVER ...) no load driver
  if (setConnMode(shell.args.connMode, shell.args.dsn, false)) { // 是否是BI模式
    return -1;
  }

  // second taos_option(TSDB_OPTION_CONFIGDIR ...) set configDir global
  if (configDirShell[0] != 0) {  // 加载配之项
    code = taos_options(TSDB_OPTION_CONFIGDIR, configDirShell);
    if (code) {
      fprintf(stderr, "failed to set config dir:%s  code:[0x%08X]\r\n", configDirShell, code);
      return -1;
    }
    //printf("Load with input config dir:%s\n", configDirShell);
  }  

#ifndef TD_ASTRA
  // dump config
  if (shell.args.is_dump_config) {
    shellDumpConfig();   // 输出信息
    return 0;
  }
#endif

  // taos_init
  if (taos_init() != 0) {  // 必要的初始化操作
    fprintf(stderr, "failed to init shell since %s [0x%08X]\r\n", taos_errstr(NULL), taos_errno(NULL));
    return -1;
  }

  // kill heart-beat thread when quit
  taos_set_hb_quit(1);   // 心跳线程

#ifndef TD_ASTRA
  if (shell.args.is_startup || shell.args.is_check) {
    shellCheckServerStatus();   // 检查服务器状态
    taos_cleanup();
    return 0;
  }

  if (shell.args.netrole != NULL) {
    shellTestNetWork(); // 测试网络连接
    taos_cleanup();
    return 0;
  }
#endif
  // support port feature
  shellAutoInit();  // 这个里边初始化了线程
  int32_t ret = shellExecute(argc, argv);
  shellAutoExit();

  return ret;
}
