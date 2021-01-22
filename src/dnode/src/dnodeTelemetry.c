/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
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

#define _DEFAULT_SOURCE
#include "os.h"
#include "osTime.h"
#include "tsocket.h"
#include "tbuffer.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeCluster.h"
#include "mnodeDnode.h"
#include "mnodeVgroup.h"
#include "mnodeMnode.h"
#include "mnodeTable.h"
#include "mnodeSdb.h"
#include "mnodeAcct.h"
#include "dnodeTelemetry.h"

static tsem_t tsExitSem;
static pthread_t tsTelemetryThread;

#define TELEMETRY_SERVER "telemetry.taosdata.com"
#define TELEMETRY_PORT 80
#define REPORT_INTERVAL 86400 

static void beginObject(SBufferWriter* bw) {
  tbufWriteChar(bw, '{');
}

static void closeObject(SBufferWriter* bw) {
  size_t len = tbufTell(bw);
  if (tbufGetData(bw, false)[len - 1] == ',') {
    tbufWriteCharAt(bw, len - 1, '}');
  } else {
    tbufWriteChar(bw, '}');
  }
  tbufWriteChar(bw, ',');
}

#if 0
static void beginArray(SBufferWriter* bw) {
  tbufWriteChar(bw, '[');
}

static void closeArray(SBufferWriter* bw) {
  size_t len = tbufTell(bw);
  if (tbufGetData(bw, false)[len - 1] == ',') {
    tbufWriteCharAt(bw, len - 1, ']');
  } else {
    tbufWriteChar(bw, ']');
  }
  tbufWriteChar(bw, ',');
}
#endif

static void writeString(SBufferWriter* bw, const char* str) {
  tbufWriteChar(bw, '"');
  tbufWrite(bw, str, strlen(str));
  tbufWriteChar(bw, '"');
}

static void addIntField(SBufferWriter* bw, const char* k, int64_t v) {
  writeString(bw, k);
  tbufWriteChar(bw, ':');
  char buf[32];
  sprintf(buf, "%" PRId64, v);
  tbufWrite(bw, buf, strlen(buf));
  tbufWriteChar(bw, ',');
}

static void addStringField(SBufferWriter* bw, const char* k, const char* v) {
  writeString(bw, k);
  tbufWriteChar(bw, ':');
  writeString(bw, v);
  tbufWriteChar(bw, ',');
}

static void addCpuInfo(SBufferWriter* bw) {
  char * line = NULL;
  size_t size = 0;
  int done = 0;

  FILE* fp = fopen("/proc/cpuinfo", "r");
  if (fp == NULL) {
    return;
  }

  while (done != 3 && (size = getline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (((done&1) == 0) && strncmp(line, "model name", 10) == 0) {
      const char* v = strchr(line, ':') + 2;
      addStringField(bw, "cpuModel", v);
      done |= 1;
    } else if (((done&2)==0) && strncmp(line, "cpu cores", 9) == 0) {
      const char* v = strchr(line, ':') + 2;
      writeString(bw, "numOfCpu");
      tbufWriteChar(bw, ':');
      tbufWrite(bw, v, strlen(v));
      tbufWriteChar(bw, ',');
      done |= 2;
    }
  }

  free(line);
  fclose(fp);
}

static void addOsInfo(SBufferWriter* bw) {
  char * line = NULL;
  size_t size = 0;

  FILE* fp = fopen("/etc/os-release", "r");
  if (fp == NULL) {
    return;
  }

  while ((size = getline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "PRETTY_NAME", 11) == 0) {
      const char* p = strchr(line, '=') + 1;
      if (*p == '"') {
        p++;
        line[size - 2] = 0;
      }
      addStringField(bw, "os", p);
      break;
    }
  }

  free(line);
  fclose(fp);
}

static void addMemoryInfo(SBufferWriter* bw) {
  char * line = NULL;
  size_t size = 0;

  FILE* fp = fopen("/proc/meminfo", "r");
  if (fp == NULL) {
    return;
  }

  while ((size = getline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "MemTotal", 8) == 0) {
      const char* p = strchr(line, ':') + 1;
      while (*p == ' ') p++;
      addStringField(bw, "memory", p);
      break;
    }
  }

  free(line);
  fclose(fp);
}

static void addVersionInfo(SBufferWriter* bw) {
  addStringField(bw, "version", version);
  addStringField(bw, "buildInfo", buildinfo);
  addStringField(bw, "gitInfo", gitinfo);  
  addStringField(bw, "email", tsEmail);  
}

static void addRuntimeInfo(SBufferWriter* bw) {
  addIntField(bw, "numOfDnode", mnodeGetDnodesNum());
  addIntField(bw, "numOfMnode", mnodeGetMnodesNum());
  addIntField(bw, "numOfVgroup", mnodeGetVgroupNum());
  addIntField(bw, "numOfDatabase", mnodeGetDbNum());
  addIntField(bw, "numOfSuperTable", mnodeGetSuperTableNum());
  addIntField(bw, "numOfChildTable", mnodeGetChildTableNum());

  SAcctInfo info;
  mnodeGetStatOfAllAcct(&info);
  addIntField(bw, "numOfColumn", info.numOfTimeSeries);
  addIntField(bw, "numOfPoint", info.totalPoints);
  addIntField(bw, "totalStorage", info.totalStorage);
  addIntField(bw, "compStorage", info.compStorage);
  // addStringField(bw, "installTime", "2020-08-01T00:00:00Z");
}

static void sendTelemetryReport() {
  char buf[128];
  uint32_t ip = taosGetIpv4FromFqdn(TELEMETRY_SERVER);
  if (ip == 0xffffffff) {
    dTrace("failed to get IP address of " TELEMETRY_SERVER ", reason:%s", strerror(errno));
    return;
  }
  int fd = taosOpenTcpClientSocket(ip, TELEMETRY_PORT, 0);
  if (fd < 0) {
    dTrace("failed to create socket for telemetry, reason:%s", strerror(errno));
    return;
  }

  SBufferWriter bw = tbufInitWriter(NULL, false);
  beginObject(&bw);
  addStringField(&bw, "instanceId", mnodeGetClusterId());
  addIntField(&bw, "reportVersion", 1);
  addOsInfo(&bw);
  addCpuInfo(&bw);
  addMemoryInfo(&bw);
  addVersionInfo(&bw);
  addRuntimeInfo(&bw);
  closeObject(&bw);

  const char* header = "POST /report HTTP/1.1\n"
    "Host: " TELEMETRY_SERVER "\n"
    "Content-Type: application/json\n"
    "Content-Length: ";

  taosWriteSocket(fd, header, strlen(header));
  int contLen = tbufTell(&bw) - 1;
  sprintf(buf, "%d\n\n", contLen);
  taosWriteSocket(fd, buf, strlen(buf));
  taosWriteSocket(fd, tbufGetData(&bw, false), contLen);
  tbufCloseWriter(&bw);

  // read something to avoid nginx error 499
  if (taosReadSocket(fd, buf, 10) < 0) {
    dTrace("failed to receive response, reason:%s", strerror(errno));
  }
  taosCloseSocket(fd);
}

static void* telemetryThread(void* param) {
  struct timespec end = {0};
  clock_gettime(CLOCK_REALTIME, &end);
  end.tv_sec += 300; // wait 5 minutes before send first report

  while (1) {
    if (tsem_timedwait(&tsExitSem, &end) == 0) {
      break;
    } else if (errno != ETIMEDOUT) {
      continue;
    }

    if (sdbIsMaster()) {
      sendTelemetryReport();
    }
    end.tv_sec += REPORT_INTERVAL;
  }

  return NULL;
}

static void dnodeGetEmail(char* filepath) {
  int fd = open(filepath, O_RDONLY);
  if (fd < 0) {
    return;
  }
  
  if (taosRead(fd, (void *)tsEmail, TSDB_FQDN_LEN) < 0) {
    dError("failed to read %d bytes from file %s since %s", TSDB_FQDN_LEN, filepath, strerror(errno));
  } 

  close(fd);   
}


int32_t dnodeInitTelemetry() {
  if (!tsEnableTelemetryReporting) {
    return 0;
  }

  dnodeGetEmail("/usr/local/taos/email");  

  if (tsem_init(&tsExitSem, 0, 0) == -1) {
    // just log the error, it is ok for telemetry to fail
    dTrace("failed to create semaphore for telemetry, reason:%s", strerror(errno));
    return 0;
  }

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

  int32_t code = pthread_create(&tsTelemetryThread, &attr, telemetryThread, NULL);
  pthread_attr_destroy(&attr);
  if (code != 0) {
    dTrace("failed to create telemetry thread, reason:%s", strerror(code));
  }

  dInfo("dnode telemetry is initialized");
  return 0;
}

void dnodeCleanupTelemetry() {
  if (!tsEnableTelemetryReporting) {
    return;
  }

  if (tsTelemetryThread) {
    tsem_post(&tsExitSem);
    pthread_join(tsTelemetryThread, NULL);
    tsem_destroy(&tsExitSem);
  }
}
