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

//#define _DEFAULT_SOURCE
#include "os.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tglobal.h"
#include "tlog.h"
#include "tcq.h"

int64_t  ver = 0;
void    *pCq = NULL;

int writeToQueue(void *pVnode, void *data, int type) {
  return 0;
}

int main(int argc, char *argv[]) {
  char path[128] = "~/cq";

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      strcpy(path, argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      ddebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p path]: wal file path default is:%s\n", path);
      printf("  [-d debugFlag]: debug flag, default:%d\n", ddebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  } 

  taosInitLog("cq.log", 100000, 10);

  SCqCfg cqCfg;
  strcpy(cqCfg.user, "root");
  strcpy(cqCfg.pass, "taosdata");
  strcpy(cqCfg.path, path);
  cqCfg.vgId = 2;
  cqCfg.cqWrite = writeToQueue;

  pCq = cqOpen(NULL, &cqCfg);
  if (pCq == NULL) {
    printf("failed to open CQ\n");
    exit(-1);
  }

  SSchema *pSchema = NULL;
  for (int sid =1; sid<10; ++sid) {
    cqCreate(pCq, 1, "select avg(speed) from t1 sliding(1s) interval(5s)", pSchema, 2);
  }

  while (1) {
    char c = getchar();
    
    switch(c) {
      case 's':
        cqStart(pCq);
        break;
      case 't':
        cqStop(pCq);
        break;
      case 'c':
        // create a CQ 
        break;
      case 'd':
        // drop a CQ
        break;
      case 'q':
        break;
    }

    if (c=='q') break;
  }

  cqClose(pCq);

  return 0;
}
