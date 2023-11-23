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
#include "taoserror.h"
#include "tglobal.h"
#include "thttp.h"
#include "transLog.h"
#include "trpc.h"
#include "tutil.h"
#include "tversion.h"

void initLogEnv() {
  const char *  logDir = "/tmp/trans_cli";
  const char *  defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 1000000;
  tsAsyncLog = 0;
  // rpcDebugflag = 143;
  strcpy(tsLogDir, (char *)logDir);
  taosRemoveDir(tsLogDir);
  taosMkDir(tsLogDir);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}
typedef struct TThread {
  TdThread thread;
  int      idx;
} TThread;

void *proces(void *arg) {
  char *monitor = "172.26.10.94";
  while (1) {
    int32_t len = 512;
    char *  msg = taosMemoryCalloc(1, len);
    memset(msg, 1, len);
    int32_t code = taosSendHttpReport(monitor, "/crash", 6050, msg, 10, HTTP_FLAT);
    taosMemoryFree(msg);
    taosUsleep(10);
  }
}
int main(int argc, char *argv[]) {
  initLogEnv();
  int32_t  numOfThreads = 10;
  TThread *thread = taosMemoryCalloc(1, sizeof(TThread) * numOfThreads);

  for (int i = 0; i < numOfThreads; i++) {
    thread[i].idx = i;
    taosThreadCreate(&(thread[i].thread), NULL, proces, (void *)&thread[i]);
  }
  while (1) {
    taosMsleep(5000);
  }

  taosCloseLog();

  return 0;
}
