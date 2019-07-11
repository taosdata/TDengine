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

#include <locale.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <wordexp.h>

#include "shell.h"
#include "tsclient.h"
#include "tutil.h"

TAOS*     con;
pthread_t pid;
int32_t   TIMESTAMP_OUTPUT_LENGTH = 22;

// TODO: IMPLEMENT INTERRUPT HANDLER.
void interruptHandler(int signum) {
  TAOS_RES* res = taos_use_result(con);
  taos_stop_query(res);
  if (res != NULL) {
    /*
     * we need to free result in async model, in order to avoid free
     * results while the master thread is waiting for server response.
     */
    tscQueueAsyncFreeResult(res);
  }
  result = NULL;
}

int checkVersion() {
  if (sizeof(int8_t) != 1) {
    printf("taos int8 size is %d(!= 1)", (int)sizeof(int8_t));
    return 0;
  }
  if (sizeof(int16_t) != 2) {
    printf("taos int16 size is %d(!= 2)", (int)sizeof(int16_t));
    return 0;
  }
  if (sizeof(int32_t) != 4) {
    printf("taos int32 size is %d(!= 4)", (int)sizeof(int32_t));
    return 0;
  }
  if (sizeof(int64_t) != 8) {
    printf("taos int64 size is %d(!= 8)", (int)sizeof(int64_t));
    return 0;
  }
  return 1;
}

// Global configurations
struct arguments args = {NULL, NULL, NULL, NULL, NULL, false, false, "\0", NULL};

/*
 * Main function.
 */
int main(int argc, char* argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */

  if (!checkVersion()) {
    exit(EXIT_FAILURE);
  }

  shellParseArgument(argc, argv, &args);

  /* Initialize the shell */
  con = shellInit(&args);
  if (con == NULL) {
    taos_error(con);
    exit(EXIT_FAILURE);
  }

  /* Interupt handler. */
  struct sigaction act;
  act.sa_handler = interruptHandler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGINT, &act, NULL);

  /* Loop to query the input. */
  while (1) {
    pthread_create(&pid, NULL, shellLoopQuery, con);
    pthread_join(pid, NULL);
  }
  return 0;
}
