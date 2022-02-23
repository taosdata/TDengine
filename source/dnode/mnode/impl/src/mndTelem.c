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
#include "mndTelem.h"
#include "mndCluster.h"
#include "mndSync.h"
#include "tbuffer.h"
#include "tversion.h"

#define TELEMETRY_SERVER "telemetry.taosdata.com"
#define TELEMETRY_PORT   80
#define REPORT_INTERVAL  86400

static void mndBeginObject(SBufferWriter* bw) { tbufWriteChar(bw, '{'); }

static void mndCloseObject(SBufferWriter* bw) {
  size_t len = tbufTell(bw);
  if (tbufGetData(bw, false)[len - 1] == ',') {
    tbufWriteCharAt(bw, len - 1, '}');
  } else {
    tbufWriteChar(bw, '}');
  }
}

static void mndWriteString(SBufferWriter* bw, const char* str) {
  tbufWriteChar(bw, '"');
  tbufWrite(bw, str, strlen(str));
  tbufWriteChar(bw, '"');
}

static void mndAddIntField(SBufferWriter* bw, const char* k, int64_t v) {
  mndWriteString(bw, k);
  tbufWriteChar(bw, ':');
  char buf[32] = {0};
  sprintf(buf, "%" PRId64, v);
  tbufWrite(bw, buf, strlen(buf));
  tbufWriteChar(bw, ',');
}

static void mndAddStringField(SBufferWriter* bw, const char* k, const char* v) {
  mndWriteString(bw, k);
  tbufWriteChar(bw, ':');
  mndWriteString(bw, v);
  tbufWriteChar(bw, ',');
}

static void mndAddCpuInfo(SMnode* pMnode, SBufferWriter* bw) {
  char*   line = NULL;
  size_t  size = 0;
  int32_t done = 0;

  FILE* fp = fopen("/proc/cpuinfo", "r");
  if (fp == NULL) {
    return;
  }

  while (done != 3 && (size = tgetline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (((done & 1) == 0) && strncmp(line, "model name", 10) == 0) {
      const char* v = strchr(line, ':') + 2;
      mndAddStringField(bw, "cpuModel", v);
      done |= 1;
    } else if (((done & 2) == 0) && strncmp(line, "cpu cores", 9) == 0) {
      const char* v = strchr(line, ':') + 2;
      mndWriteString(bw, "numOfCpu");
      tbufWriteChar(bw, ':');
      tbufWrite(bw, v, strlen(v));
      tbufWriteChar(bw, ',');
      done |= 2;
    }
  }

  free(line);
  fclose(fp);
}

static void mndAddOsInfo(SMnode* pMnode, SBufferWriter* bw) {
  char*  line = NULL;
  size_t size = 0;

  FILE* fp = fopen("/etc/os-release", "r");
  if (fp == NULL) {
    return;
  }

  while ((size = tgetline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "PRETTY_NAME", 11) == 0) {
      const char* p = strchr(line, '=') + 1;
      if (*p == '"') {
        p++;
        line[size - 2] = 0;
      }
      mndAddStringField(bw, "os", p);
      break;
    }
  }

  free(line);
  fclose(fp);
}

static void mndAddMemoryInfo(SMnode* pMnode, SBufferWriter* bw) {
  char*  line = NULL;
  size_t size = 0;

  FILE* fp = fopen("/proc/meminfo", "r");
  if (fp == NULL) {
    return;
  }

  while ((size = tgetline(&line, &size, fp)) != -1) {
    line[size - 1] = '\0';
    if (strncmp(line, "MemTotal", 8) == 0) {
      const char* p = strchr(line, ':') + 1;
      while (*p == ' ') p++;
      mndAddStringField(bw, "memory", p);
      break;
    }
  }

  free(line);
  fclose(fp);
}

static void mndAddVersionInfo(SMnode* pMnode, SBufferWriter* bw) {
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  char vstr[32] = {0};
  taosVersionIntToStr(pMnode->cfg.sver, vstr, 32);

  mndAddStringField(bw, "version", vstr);
  mndAddStringField(bw, "buildInfo", pMnode->cfg.buildinfo);
  mndAddStringField(bw, "gitInfo", pMnode->cfg.gitinfo);
  mndAddStringField(bw, "email", pMgmt->email);
}

static void mndAddRuntimeInfo(SMnode* pMnode, SBufferWriter* bw) {
  SMnodeLoad load = {0};
  if (mndGetLoad(pMnode, &load) != 0) {
    return;
  }

  mndAddIntField(bw, "numOfDnode", load.numOfDnode);
  mndAddIntField(bw, "numOfMnode", load.numOfMnode);
  mndAddIntField(bw, "numOfVgroup", load.numOfVgroup);
  mndAddIntField(bw, "numOfDatabase", load.numOfDatabase);
  mndAddIntField(bw, "numOfSuperTable", load.numOfSuperTable);
  mndAddIntField(bw, "numOfChildTable", load.numOfChildTable);
  mndAddIntField(bw, "numOfColumn", load.numOfColumn);
  mndAddIntField(bw, "numOfPoint", load.totalPoints);
  mndAddIntField(bw, "totalStorage", load.totalStorage);
  mndAddIntField(bw, "compStorage", load.compStorage);
}

static void mndSendTelemetryReport(SMnode* pMnode) {
  STelemMgmt*   pMgmt = &pMnode->telemMgmt;
  SBufferWriter bw = tbufInitWriter(NULL, false);
  int32_t       code = -1;
  char          buf[128] = {0};
  SOCKET        fd = 0;

  char clusterName[64] = {0};
  if (mndGetClusterName(pMnode, clusterName, sizeof(clusterName)) != 0) {
    goto SEND_OVER;
  }

  mndBeginObject(&bw);
  mndAddStringField(&bw, "instanceId", clusterName);
  mndAddIntField(&bw, "reportVersion", 1);
  mndAddOsInfo(pMnode, &bw);
  mndAddCpuInfo(pMnode, &bw);
  mndAddMemoryInfo(pMnode, &bw);
  mndAddVersionInfo(pMnode, &bw);
  mndAddRuntimeInfo(pMnode, &bw);
  mndCloseObject(&bw);

  uint32_t ip = taosGetIpv4FromFqdn(TELEMETRY_SERVER);
  if (ip == 0xffffffff) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to get ip of %s since :%s", TELEMETRY_SERVER, terrstr());
    goto SEND_OVER;
  }

  fd = taosOpenTcpClientSocket(ip, TELEMETRY_PORT, 0);
  if (fd < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to create socket to %s:%d since:%s", TELEMETRY_SERVER, TELEMETRY_PORT, terrstr());
    goto SEND_OVER;
  }

  const char* header =
      "POST /report HTTP/1.1\n"
      "Host: " TELEMETRY_SERVER
      "\n"
      "Content-Type: application/json\n"
      "Content-Length: ";
  if (taosWriteSocket(fd, (void*)header, (int32_t)strlen(header)) < 0) {
    goto SEND_OVER;
  }

  int32_t contLen = (int32_t)(tbufTell(&bw));
  sprintf(buf, "%d\n\n", contLen);
  if (taosWriteSocket(fd, buf, (int32_t)strlen(buf)) < 0) {
    goto SEND_OVER;
  }

  const char* pCont = tbufGetData(&bw, false);
  if (taosWriteSocket(fd, (void*)pCont, contLen) < 0) {
    goto SEND_OVER;
  }

  // read something to avoid nginx error 499
  if (taosReadSocket(fd, buf, 10) < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    mError("failed to receive response since %s", terrstr());
    goto SEND_OVER;
  }

  mInfo("send telemetry to %s:%d, len:%d content: %s", TELEMETRY_SERVER, TELEMETRY_PORT, contLen, pCont);
  code = 0;

SEND_OVER:
  tbufCloseWriter(&bw);
  taosCloseSocket(fd);

  if (code != 0) {
    mError("failed to send telemetry to %s:%d since %s", TELEMETRY_SERVER, TELEMETRY_PORT, terrstr());
  }
}

static int32_t mndProcessTelemTimer(SMnodeMsg* pReq) {
  SMnode*     pMnode = pReq->pMnode;
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  if (!pMgmt->enable) return 0;

  taosWLockLatch(&pMgmt->lock);
  mndSendTelemetryReport(pMnode);
  taosWUnLockLatch(&pMgmt->lock);
  return 0;
}

static void mndGetEmail(SMnode* pMnode, char* filepath) {
  STelemMgmt* pMgmt = &pMnode->telemMgmt;

  int32_t fd = taosOpenFileRead(filepath);
  if (fd < 0) {
    return;
  }

  if (taosReadFile(fd, (void*)pMgmt->email, TSDB_FQDN_LEN) < 0) {
    mError("failed to read %d bytes from file %s since %s", TSDB_FQDN_LEN, filepath, strerror(errno));
  }

  taosCloseFile(fd);
}

int32_t mndInitTelem(SMnode* pMnode) {
  STelemMgmt* pMgmt = &pMnode->telemMgmt;
  pMgmt->enable = pMnode->cfg.enableTelem;
  taosInitRWLatch(&pMgmt->lock);
  mndGetEmail(pMnode, "/usr/local/taos/email");

  mndSetMsgHandle(pMnode, TDMT_MND_TELEM_TIMER, mndProcessTelemTimer);
  mDebug("mnode telemetry is initialized");
  return 0;
}

void mndCleanupTelem(SMnode* pMnode) {}
