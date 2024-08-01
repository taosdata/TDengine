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

#include <gtest/gtest.h>
#include <iostream>
#include "clientInt.h"
#include "clientMonitor.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "executor.h"
#include "taos.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

//TEST(clientMonitorTest, monitorTest) {
//  const char* cluster1 = "cluster1";
//  const char* cluster2 = "cluster2";
//  SEpSet epSet;
//  clientMonitorInit(cluster1, epSet, NULL);
//  const char* counterName1 = "slow_query";
//  const char* counterName2 = "select_count";
//  const char* help1 = "test for slowQuery";
//  const char* help2 = "test for selectSQL";
//  const char* lables[] = {"lable1"};
//  taos_counter_t* c1 = createClusterCounter(cluster1, counterName1, help1, 1, lables);
//  ASSERT_TRUE(c1 != NULL);
//  taos_counter_t* c2 = createClusterCounter(cluster1, counterName2, help2, 1, lables);
//  ASSERT_TRUE(c2 != NULL);
//  ASSERT_TRUE(c1 != c2);
//  taos_counter_t* c21 = createClusterCounter(cluster2, counterName1, help2, 1, lables);
//  ASSERT_TRUE(c21 == NULL);
//  clientMonitorInit(cluster2, epSet, NULL);
//  c21 = createClusterCounter(cluster2, counterName1, help2, 1, lables);
//  ASSERT_TRUE(c21 != NULL);
//  int i = 0;
//  while (i < 12) {
//      taosMsleep(10);
//      ++i;
//  }
//  clusterMonitorClose(cluster1);
//  clusterMonitorClose(cluster2);
//}

static char* readFile(TdFilePtr pFile, int64_t *offset, int64_t size){
  if(taosLSeekFile(pFile, *offset, SEEK_SET) < 0){
    uError("failed to seek file:%p code: %d", pFile, errno);
    return NULL;
  }

  ASSERT(size > *offset);
  char* pCont = NULL;
  int64_t totalSize = 0;
  if (size - *offset >= SLOW_LOG_SEND_SIZE_MAX) {
    pCont = (char*)taosMemoryCalloc(1, 2 * SLOW_LOG_SEND_SIZE_MAX);
    totalSize = 2 * SLOW_LOG_SEND_SIZE_MAX;
  }else{
    pCont = (char*)taosMemoryCalloc(1, 2 * (size - *offset));
    totalSize = 2 * (size - *offset);
  }

  if(pCont == NULL){
    uError("failed to allocate memory for slow log, size:%" PRId64, totalSize);
    return NULL;
  }
  char*  buf = pCont;
  (void)strcat(buf++, "[");
  int64_t readSize = taosReadFile(pFile, buf, SLOW_LOG_SEND_SIZE_MAX);
  if (readSize <= 0) {
    if (readSize < 0){
      uError("failed to read len from file:%p since %s", pFile, terrstr());
    }
    taosMemoryFree(pCont);
    return NULL;
  }

  totalSize = 0;
  while(1){
    size_t len = strlen(buf);
    totalSize += (len+1);
    if (totalSize > readSize || len == 0) {
      *(buf-1) = ']';
      *buf = '\0';
      break;
    }
    buf[len] = ','; // replace '\0' with ','
    buf += (len + 1);
    *offset += (len+1);
  }

  uDebug("[monitor] monitorReadSendSlowLog slow log:%s", pCont);
  return pCont;
}

static int64_t getFileSize(char* path){
  int64_t fileSize = 0;
  if (taosStatFile(path, &fileSize, NULL, NULL) < 0) {
    return -1;
  }

  return fileSize;
}

TEST(clientMonitorTest, sendTest) {
  TAOS*       taos = taos_connect("127.0.0.1", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != NULL);
  printf("connect taosd sucessfully.\n");

  int64_t rid = *(int64_t *)taos;
  slowQueryLog(rid, false, -1, 1000);
  int i = 0;
  while (i < 20) {
    slowQueryLog(rid, false, 0, i * 1000);
    taosMsleep(10);
    ++i;
  }

  taos_close(taos);
}

TEST(clientMonitorTest, ReadOneFile) {
  // Create a TdFilePtr object and set it up for testing

  TdFilePtr pFile = taosOpenFile("./tdengine-1-wewe", TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
  if (pFile == NULL) {
    uError("failed to open file:./test.txt since %s", terrstr());
    return;
  }

  const int batch = 10;
  const int size = 10;
  for(int i = 0; i < batch; i++){
    char value[size] = {0};
    (void)memset(value, '0' + i, size - 1);
    if (taosWriteFile(pFile, value, strlen(value) + 1) < 0){
      uError("failed to write len to file:%p since %s", pFile, terrstr());
    }
  }

  // Create a void pointer and set it up for testing
  void* pTransporter = NULL;

  // Create an SEpSet object and set it up for testing
  SEpSet* epSet = NULL;

  int64_t  fileSize = getFileSize("./tdengine-1-wewe");
  // Call the function to be tested
  int64_t offset = 0;
  while(1){
    if (offset >= fileSize) {
      break;
    }
    char* val = readFile(pFile, &offset, fileSize);
    printf("offset:%lld,fileSize:%lld,val:%s\n", offset, fileSize, val);
  }

//  char value[size] = {0};
//  memset(value, '0', size - 1);
//  if (taosWriteFile(pFile, value, strlen(value) + 1) < 0){
//    uError("failed to write len to file:%p since %s", pFile, terrstr());
//  }

//  monitorReadSendSlowLog(pFile, (int64_t)pTransporter, epSet);

  // Clean up any resources created for testing
  taosCloseFile(&pFile);
}

//TEST(clientMonitorTest, ReadTwoFile) {
//  // Create a TdFilePtr object and set it up for testing
//
//  TdFilePtr pFile = taosOpenFile("/tmp/tdengine_slow_log/tdengine-1-wewe", TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
//  if (pFile == NULL) {
//    uError("failed to open file:./test.txt since %s", terrstr());
//    return;
//  }
//
//  const int batch = 10;
//  const int size = SLOW_LOG_SEND_SIZE/batch;
//  for(int i = 0; i < batch + 1; i++){
//    char value[size] = {0};
//    memset(value, '0' + i, size - 1);
//    if (taosWriteFile(pFile, value, strlen(value) + 1) < 0){
//      uError("failed to write len to file:%p since %s", pFile, terrstr());
//    }
//  }
//
//  taosFsyncFile(pFile);
//  taosCloseFile(&pFile);
//
//  pFile = taosOpenFile("/tmp/tdengine_slow_log/tdengine-2-wewe", TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_APPEND | TD_FILE_READ | TD_FILE_TRUNC);
//  if (pFile == NULL) {
//    uError("failed to open file:./test.txt since %s", terrstr());
//    return;
//  }
//
//  for(int i = 0; i < batch + 1; i++){
//    char value[size] = {0};
//    memset(value, '0' + i, size - 1);
//    if (taosWriteFile(pFile, value, strlen(value) + 1) < 0){
//      uError("failed to write len to file:%p since %s", pFile, terrstr());
//    }
//  }
//
//  taosFsyncFile(pFile);
//  taosCloseFile(&pFile);
//
//  SAppInstInfo pAppInfo = {0};
//  pAppInfo.clusterId = 2;
//  pAppInfo.monitorParas.tsEnableMonitor = 1;
//  strcpy(tsTempDir,"/tmp");
////  monitorSendAllSlowLogFromTempDir(&pAppInfo);
//
//}