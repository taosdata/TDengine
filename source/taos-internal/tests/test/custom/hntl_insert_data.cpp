/*******************************************************************
*           Copyright (c) 2017 by TAOS Technologies, Inc.
*                     All rights reserved.
*
*  This file is proprietary and confidential to TAOS Technologies.
*  No part of this file may be reproduced, stored, transmitted,
*  disclosed or used in any form or by any means other than as
*  expressly provided by the written permission from Jianhui Tao
*
* ****************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <pthread.h>
#include <errno.h>
#include <signal.h>
#include "taos.h"
#include <fstream>
#include <iostream>
#include "string.h"
#include <map>
#include <sstream>
using namespace std;

bool parsedata(char *line, int64_t *tbId, float *data);

int main(int argc, char *argv[])
{
  TAOS   *con;
  char    qstr[1024] = { 0 };
  char    dataFileName[128];
  int64_t dataTs = 0;
  char    tablePrefix[20] = { 0 };
  int     tableNum;
  int     loopTimes;

  if (argc != 6) {
    printf("argument formats: hntl.data ts(1483200000000) tablePrefix tableNum loopTimes.");
    exit(0);
  }

  strcpy(dataFileName, argv[1]);
  dataTs = atoll(argv[2]);
  strcpy(tablePrefix, argv[3]);
  tableNum = atoi(argv[4]);
  loopTimes = atoi(argv[5]);

  taos_init();
  con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    printf("failed to connect to DB, reason:%s.\n", taos_errstr(con));
    exit(1);
  }

  sprintf(qstr, "use db");
  taos_query(con, qstr);

  ifstream dataFile(dataFileName);
  if (!dataFile.is_open()) {
    printf("file:%s open failed, exit program.\n", dataFileName);
    exit(0);
  }
  printf("file:%s open success.\n", dataFileName);
 
  char line[100000];
  int64_t tbId;
  float data[6144];
  int readIndex = 0;
  do {
    if (!(dataFile >> tbId)) {
      printf("file:%s readIndex:%d, tableNum:%d, tbid read null\n", dataFileName, readIndex, tableNum);
      exit(0);
    }
    //8*8*86 = 6144

    for (int i = 0; i < 6144; ++i) {
      if (!(dataFile >> data[i])) {
        printf("file:%s readIndex:%d, tableNum:%d, data read null\n", dataFileName, readIndex, tableNum);
        exit(0);
      }
    }

    readIndex++;
    if (readIndex > tableNum) {
      printf("file:%s readIndex:%d, tableNum:%d, finish\n", dataFileName, readIndex, tableNum);
      exit(0);
    }

    if (readIndex % 1000 == 0) {
      printf("file:%s parse:%d.\n", dataFileName, readIndex);
      std::cout << std::flush;
    }

    int64_t ts = dataTs;
    for (int loop = 0; loop < loopTimes; ++loop) {
      int datapos = 0;
      for (int day = 0; day < 8; ++day) {
        
        ostringstream oss;
        oss << "insert into ";
        oss << tablePrefix;
        oss << tbId;

        for (int row = 0; row < 96; ++row) {
          oss << " values(" << ts << ",";

          for (int col = 0; col < 8; ++col) {
            if (data[datapos] > -0.0001 && data[datapos] < 0.0001)
            {
              oss << 0;
            }
            else if (data[datapos] > 10000) {
              oss << 10000;
            }
            else if (data[datapos] < -10000) {
              oss << -10000;
            }
            else {
              oss << data[datapos];
            }

            if (col == 7){
              oss << ")";
            }
            else {
              oss << ",";
            }
            datapos++;
          }

          ts += 15 * 60 * 1000;
        }

        string a = oss.str();
        char * sql = (char*)a.c_str();
        //printf(sql);

        int code = 0;

        for (int t = 0; t < 5; ++t) {
          code = taos_query(con, sql);
          if (code != 0) {
            continue;
          }
          else {
            break;
          }
        }
        if (code != 0) {
          printf("failed to insert data code:%d, sql:%s\n", code, sql);
        }
        //oss.str("");
      }
    }
  } while (true);

  dataFile.close();

  printf("===>  insert table finished.\n");

  return 0;
}
