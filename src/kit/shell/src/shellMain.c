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
#include "shell.h"
#include "tnettest.h"

pthread_t pid;

void shellQueryInterruptHandler(int signum) {
#ifdef LINUX
  void* pResHandle = atomic_val_compare_exchange_64(&result, result, 0);
  taos_stop_query(pResHandle);
#else
  printf("\nReceive ctrl+c or other signal, quit shell.\n");
  exit(0);
#endif
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
SShellArguments args = {
  .host = NULL,
  .password = NULL,
  .user = NULL,
  .database = NULL,
  .timezone = NULL,
  .is_raw_time = false,
  .is_use_passwd = false,
  .file = "\0",
  .dir = "\0",
  .threadNum = 5,
  .commands = NULL,  
  .endPort = 6042,
  .pktLen = 1000,
  .netTestRole = NULL
};

/*
 * Main function.
 */
int main(int argc, char* argv[]) {
  /*setlocale(LC_ALL, "en_US.UTF-8"); */

  if (!checkVersion()) {
    exit(EXIT_FAILURE);
  }

  shellParseArgument(argc, argv, &args);

  if (args.netTestRole && args.netTestRole[0] != 0) {
    taos_init();
    CmdArguments cmdArgs;
    memcpy(&cmdArgs, &args, sizeof(SShellArguments));
    taosNetTest(&cmdArgs);
    exit(0);
  }

  /* Initialize the shell */
  TAOS* con = shellInit(&args);
  if (con == NULL) {
    exit(EXIT_FAILURE);
  }

  /* Interrupt handler. */
  struct sigaction act;
  memset(&act, 0, sizeof(struct sigaction));
  
  act.sa_handler = shellQueryInterruptHandler;
  sigaction(SIGTERM, &act, NULL);
  sigaction(SIGINT, &act, NULL);

  /* Get grant information */
  shellGetGrantInfo(con);

  /* Loop to query the input. */
  while (1) {
    pthread_create(&pid, NULL, shellLoopQuery, con);
    pthread_join(pid, NULL);
  }
}
