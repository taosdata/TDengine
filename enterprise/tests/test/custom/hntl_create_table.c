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
#include "tlog.h"

void t(int argc, char *argv[])
{
  TAOS   *con;
  char    qstr[2048];
  char    tableFile[128];
  char    tablePrefix[20];
  char    tableFormat[1024];
  int     tableNum = -1;
  if (argc < 5) {
    tPrint("argument formats: table.txt tablePefix tableNum tableFormat configDir.");
    exit(0);
  }

  strcpy(tableFile, argv[1]);
  strcpy(tablePrefix, argv[2]);
  tableNum = atoi(argv[3]);
  strcpy(tableFormat, argv[4]);
  if (argc >= 6) {
    strcpy(configDir, argv[5]);
  }
  tPrint("argument argc:%d %s %s %d %s %s\n", argc, tableFile, tablePrefix, tableNum, tableFormat, configDir);
  int  i = 0;
  taos_init();
  
  con = taos_connect(NULL, "root", "taosdata", NULL, 0);
  if (con == NULL) {
    tPrint("failed to connect to DB, reason:%s.", taos_errstr(con));
    exit(1);
  }
  
  sprintf(qstr, "create database db days 365");
  taos_query(con, qstr);
  
  sprintf(qstr, "use db");
  taos_query(con, qstr);

  sprintf(qstr, "create table mt (%s) tags(orgno int)", tableFormat);
  int code = taos_query(con, qstr);
  if (code != 0) {
    tPrint("===>failed to create metrics:%s, code:%d.", qstr, code);
    taosMsleep(1000);
  }

  FILE *fp = fopen(tableFile, "r");
  if (fp == NULL) {
    tPrint("failed to openfile:%s.", tableFile);
    exit(1);
  }

  int index = 0;
  char *line = NULL;
  size_t len;

  while (!feof(fp)) {
    taosMemoryFree(line);
    line = NULL;
    getline(&line, &len, fp);
    if (line == NULL) break;

    int64_t id;
    int64_t orgno;
    sscanf(line, "%lld %lld", &id, &orgno);

    sprintf(qstr, "create table %s%lld using mt tags(%lld)", tablePrefix, id, orgno);
    int code = taos_query(con, qstr);
    if (code != 0) {
      tPrint("failed to create table:%s, code:%d.", qstr, code);
    }
    index++;
    if (index % 100000 == 0) {
      tPrint("create table:%lld index:%d finished.", id, index);
    }

    if (index >= tableNum) {
      tPrint("create table index:%d equal to %d finished.", index, tableNum);
      break;
    }
  }
  tPrint("===>  create table finished.");
  //fclose(fp);
}

int main(int argc, char *argv[]) 
{
  t(argc, argv);

  return 0;
}
