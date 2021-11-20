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

#define _DEFAULT_SOURCE
#include "dnode.h"
#include "os.h"
#include "ulog.h"

static bool stop = false;

static void sigintHandler(int32_t signum, void *info, void *ctx) { stop = true; }

static void setSignalHandler() {
  taosSetSignal(SIGTERM, sigintHandler);
  taosSetSignal(SIGHUP, sigintHandler);
  taosSetSignal(SIGINT, sigintHandler);
  taosSetSignal(SIGABRT, sigintHandler);
  taosSetSignal(SIGBREAK, sigintHandler);
}

int main(int argc, char const *argv[]) {
  const char *path = "/etc/taos";

  SDnode *pDnode = dnodeInit(path);
  if (pDnode == NULL) {
    uInfo("Failed to start TDengine, please check the log at %s", tsLogDir);
    exit(EXIT_FAILURE);
  }

  uInfo("Started TDengine service successfully.");

  setSignalHandler();
  while (!stop) {
    taosMsleep(100);
  }

  uInfo("TDengine is shut down!");
  dnodeCleanup(pDnode);

  return 0;
}
