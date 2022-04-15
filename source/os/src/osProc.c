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
#define _DEFAULT_SOURCE
#include "os.h"

static char *tsProcPath = NULL;

int32_t taosNewProc(char **args) {
  int32_t pid = fork();
  if (pid == 0) {
    args[0] = tsProcPath;
    // close(STDIN_FILENO);
    // close(STDOUT_FILENO);
    // close(STDERR_FILENO);
    return execvp(tsProcPath, args);
  } else {
    return pid;
  }
}

void taosWaitProc(int32_t pid) {
  int32_t status = -1;
  waitpid(pid, &status, 0);
}

void taosKillProc(int32_t pid) { kill(pid, SIGINT); }

bool taosProcExist(int32_t pid) {
  int32_t p = getpgid(pid);
  return p >= 0;
}

// the length of the new name must be less than the original name to take effect
void taosSetProcName(int32_t argc, char **argv, const char *name) {
  prctl(PR_SET_NAME, name);

  for (int32_t i = 0; i < argc; ++i) {
    int32_t len = strlen(argv[i]);
    for (int32_t j = 0; j < len; ++j) {
      argv[i][j] = 0;
    }
    if (i == 0) {
      tstrncpy(argv[0], name, len + 1);
    }
  }
}

void taosSetProcPath(int32_t argc, char **argv) { tsProcPath = argv[0]; }
