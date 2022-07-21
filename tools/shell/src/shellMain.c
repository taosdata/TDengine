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

SShellObj shell = {0};

int main(int argc, char *argv[]) {
  shell.args.timeout = 10;
  shell.args.cloud = true;

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
 
  shellCheckConnectMode();

  taos_init();

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

  return shellExecute();
}
