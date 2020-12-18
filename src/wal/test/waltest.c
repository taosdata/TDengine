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
#include "tutil.h"
#include "tglobal.h"
#include "tlog.h"
#include "twal.h"

int64_t  ver = 0;
void    *pWal = NULL;

int writeToQueue(void *pVnode, void *data, int type, void *pMsg) {
  // do nothing
  SWalHead *pHead = data;

  if (pHead->version > ver)
    ver = pHead->version;

  walWrite(pWal, pHead);

  return 0;
}

int main(int argc, char *argv[]) {
  char path[128] = "/home/jhtao/test/wal";
  int  level = 2;
  int  total = 5;
  int  rows = 10000;
  int  size = 128;
  int  keep = 0;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-p")==0 && i < argc-1) {
      tstrncpy(path, argv[++i], sizeof(path));
    } else if (strcmp(argv[i], "-l")==0 && i < argc-1) {
      level = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-r")==0 && i < argc-1) {
      rows = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-k")==0 && i < argc-1) {
      keep = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t")==0 && i < argc-1) {
      total = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s")==0 && i < argc-1) {
      size = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-v")==0 && i < argc-1) {
      ver = atoll(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
      dDebugFlag = atoi(argv[++i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-p path]: wal file path default is:%s\n", path);
      printf("  [-l level]: log level, default is:%d\n", level);
      printf("  [-t total]: total wal files, default is:%d\n", total);
      printf("  [-r rows]: rows of records per wal file, default is:%d\n", rows);
      printf("  [-k keep]: keep the wal after closing, default is:%d\n", keep);
      printf("  [-v version]: initial version, default is:%" PRId64 "\n", ver);
      printf("  [-d debugFlag]: debug flag, default:%d\n", dDebugFlag);
      printf("  [-h help]: print out this help\n\n");
      exit(0);
    }
  } 

  taosInitLog("wal.log", 100000, 10);

  SWalCfg walCfg = {0};
  walCfg.walLevel = level;
  walCfg.keep = keep;

  pWal = walOpen(path, &walCfg);
  if (pWal == NULL) {
    printf("failed to open wal\n");
    exit(-1);
  }

  int ret = walRestore(pWal, NULL, writeToQueue);
  if (ret <0) {
    printf("failed to restore wal\n");
    exit(-1);
  }

  printf("version starts from:%" PRId64 "\n", ver);
  
  int contLen = sizeof(SWalHead) + size;
  SWalHead *pHead = (SWalHead *) malloc(contLen);

  for (int i=0; i<total; ++i) {
    for (int k=0; k<rows; ++k) {
      pHead->version = ++ver;
      pHead->len = size;
      walWrite(pWal, pHead);
    }
       
    printf("renew a wal, i:%d\n", i);
    walRenew(pWal);
  }

  printf("%d wal files are written\n", total);

  int64_t index = 0;
  char    name[256];

  while (1) {
    int code = walGetWalFile(pWal, name, &index);
    if (code == -1) {
      printf("failed to get wal file, index:%" PRId64 "\n", index);
      break;
    }

    printf("index:%" PRId64 " wal:%s\n", index, name);
    if (code == 0) break;

    index++;
  }

  getchar();

  walClose(pWal);

  return 0;
}
