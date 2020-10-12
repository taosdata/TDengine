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

#include "os.h"
#include "os.h"
#include "tglobal.h"
#include "taoserror.h"
#include "httpSystem.h"

void signal_handler(int signum) {
  httpStopSystem();
  httpCleanUpSystem();
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  struct sigaction act;
  act.sa_handler = signal_handler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGHUP, &act, NULL);
  sigaction(SIGINT, &act, NULL);
  sigaction(SIGABRT, &act, NULL);

  // Initialize the system
  if (httpInitSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  if (httpStartSystem() < 0) {
    exit(EXIT_FAILURE);
  }

  while (1) {
    sleep(1000);
  }
}
