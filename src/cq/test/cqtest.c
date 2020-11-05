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

int writeToQueue(void *pVnode, void *data, int type, void *pMsg) { 
  return 0;
}

int main(int argc, char *argv[]) {
  int num = 3;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      dDebugFlag = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-n") == 0 && i <argc-1) {
      num = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-n num]: number of streams, default:%d\n", num);
      printf("  [-d debugFlag]: debug flag, default:%d\n", dDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  } 

  taosInitLog("cq.log", 100000, 10);

  SCqCfg cqCfg;
  strcpy(cqCfg.user, TSDB_DEFAULT_USER);
  strcpy(cqCfg.pass, TSDB_DEFAULT_PASS);
  cqCfg.vgId = 2;
  cqCfg.cqWrite = writeToQueue;

  pCq = cqOpen(NULL, &cqCfg);
  if (pCq == NULL) {
    printf("failed to open CQ\n");
    exit(-1);
  }

  STSchemaBuilder schemaBuilder = {0};

  tdInitTSchemaBuilder(&schemaBuilder, 0);
  tdAddColToSchema(&schemaBuilder, TSDB_DATA_TYPE_TIMESTAMP, 0, 8);
  tdAddColToSchema(&schemaBuilder, TSDB_DATA_TYPE_INT, 1, 4);

  STSchema *pSchema = tdGetSchemaFromBuilder(&schemaBuilder);

  tdDestroyTSchemaBuilder(&schemaBuilder);

  for (int sid =1; sid<10; ++sid) {
    cqCreate(pCq, sid, sid, "select avg(speed) from demo.t1 sliding(1s) interval(5s)", pSchema);
  }

  tdFreeSchema(pSchema);

  while (1) {
    char c = (char)getchar();
    
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
      default:
        printf("invalid command:%c", c);
    }

    if (c=='q') break;
  }

  cqClose(pCq);

  taosCloseLog();

  return 0;
}
