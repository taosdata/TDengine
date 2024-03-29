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

#define _DEFAULT_SOURCE
#include "tmisce.h"
#include "tglobal.h"
#include "tjson.h"
int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp) {
  pEp->port = 0;
  memset(pEp->fqdn, 0, TSDB_FQDN_LEN);
  strncpy(pEp->fqdn, ep, TSDB_FQDN_LEN - 1);

  char* temp = strchr(pEp->fqdn, ':');
  if (temp) {
    *temp = 0;
    pEp->port = atoi(temp + 1);
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
  }

  return 0;
}

void addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port) {
  if (pEpSet == NULL || fqdn == NULL || strlen(fqdn) == 0) {
    return;
  }

  int32_t index = pEpSet->numOfEps;
  tstrncpy(pEpSet->eps[index].fqdn, fqdn, tListLen(pEpSet->eps[index].fqdn));
  pEpSet->eps[index].port = port;
  pEpSet->numOfEps += 1;
}

bool isEpsetEqual(const SEpSet* s1, const SEpSet* s2) {
  if (s1->numOfEps != s2->numOfEps || s1->inUse != s2->inUse) {
    return false;
  }

  for (int32_t i = 0; i < s1->numOfEps; i++) {
    if (s1->eps[i].port != s2->eps[i].port || strncmp(s1->eps[i].fqdn, s2->eps[i].fqdn, TSDB_FQDN_LEN) != 0)
      return false;
  }
  return true;
}

void epsetAssign(SEpSet* pDst, const SEpSet* pSrc) {
  if (pSrc == NULL || pDst == NULL) {
    return;
  }

  pDst->inUse = pSrc->inUse;
  pDst->numOfEps = pSrc->numOfEps;
  for (int32_t i = 0; i < pSrc->numOfEps; ++i) {
    pDst->eps[i].port = pSrc->eps[i].port;
    tstrncpy(pDst->eps[i].fqdn, pSrc->eps[i].fqdn, tListLen(pSrc->eps[i].fqdn));
  }
}
void epAssign(SEp* pDst, SEp* pSrc) {
  if (pSrc == NULL || pDst == NULL) {
    return;
  }
  memset(pDst->fqdn, 0, tListLen(pSrc->fqdn));
  tstrncpy(pDst->fqdn, pSrc->fqdn, tListLen(pSrc->fqdn));
  pDst->port = pSrc->port;
}
void epsetSort(SEpSet* pDst) {
  if (pDst->numOfEps <= 1) {
    return;
  }
  int validIdx = false;
  SEp ep = {0};
  if (pDst->inUse >= 0 && pDst->inUse < pDst->numOfEps) {
    validIdx = true;
    epAssign(&ep, &pDst->eps[pDst->inUse]);
  }

  for (int i = 0; i < pDst->numOfEps - 1; i++) {
    for (int j = 0; j < pDst->numOfEps - 1 - i; j++) {
      SEp* f = &pDst->eps[j];
      SEp* s = &pDst->eps[j + 1];
      int  cmp = strncmp(f->fqdn, s->fqdn, sizeof(f->fqdn));
      if (cmp > 0 || (cmp == 0 && f->port > s->port)) {
        SEp ep = {0};
        epAssign(&ep, f);
        epAssign(f, s);
        epAssign(s, &ep);
      }
    }
  }
  if (validIdx == true)
    for (int i = 0; i < pDst->numOfEps; i++) {
      int cmp = strncmp(ep.fqdn, pDst->eps[i].fqdn, sizeof(ep.fqdn));
      if (cmp == 0 && ep.port == pDst->eps[i].port) {
        pDst->inUse = i;
        break;
      }
    }
}

void updateEpSet_s(SCorEpSet* pEpSet, SEpSet* pNewEpSet) {
  taosCorBeginWrite(&pEpSet->version);
  pEpSet->epSet = *pNewEpSet;
  taosCorEndWrite(&pEpSet->version);
}

SEpSet getEpSet_s(SCorEpSet* pEpSet) {
  SEpSet ep = {0};
  taosCorBeginRead(&pEpSet->version);
  ep = pEpSet->epSet;
  taosCorEndRead(&pEpSet->version);

  return ep;
}

int32_t taosGenCrashJsonMsg(int signum, char** pMsg, int64_t clusterId, int64_t startTime) {
  SJson* pJson = tjsonCreateObject();
  if (pJson == NULL) return -1;
  char tmp[4096] = {0};

  tjsonAddDoubleToObject(pJson, "reportVersion", 1);

  tjsonAddIntegerToObject(pJson, "clusterId", clusterId);
  tjsonAddIntegerToObject(pJson, "startTime", startTime);

  // Do NOT invoke the taosGetFqdn here.
  // this function may be invoked when memory exception occurs,so we should assume that it is running in a memory locked
  // environment. The lock operation by taosGetFqdn may cause this program deadlock.
  tjsonAddStringToObject(pJson, "fqdn", tsLocalFqdn);

  tjsonAddIntegerToObject(pJson, "pid", taosGetPId());

  taosGetAppName(tmp, NULL);
  tjsonAddStringToObject(pJson, "appName", tmp);

  if (taosGetOsReleaseName(tmp, NULL, NULL, sizeof(tmp)) == 0) {
    tjsonAddStringToObject(pJson, "os", tmp);
  }

  float numOfCores = 0;
  if (taosGetCpuInfo(tmp, sizeof(tmp), &numOfCores) == 0) {
    tjsonAddStringToObject(pJson, "cpuModel", tmp);
    tjsonAddDoubleToObject(pJson, "numOfCpu", numOfCores);
  } else {
    tjsonAddDoubleToObject(pJson, "numOfCpu", tsNumOfCores);
  }

  snprintf(tmp, sizeof(tmp), "%" PRId64 " kB", tsTotalMemoryKB);
  tjsonAddStringToObject(pJson, "memory", tmp);

  tjsonAddStringToObject(pJson, "version", version);
  tjsonAddStringToObject(pJson, "buildInfo", buildinfo);
  tjsonAddStringToObject(pJson, "gitInfo", gitinfo);

  tjsonAddIntegerToObject(pJson, "crashSig", signum);
  tjsonAddIntegerToObject(pJson, "crashTs", taosGetTimestampUs());

#ifdef _TD_DARWIN_64
  taosLogTraceToBuf(tmp, sizeof(tmp), 4);
#elif !defined(WINDOWS)
  taosLogTraceToBuf(tmp, sizeof(tmp), 3);
#else
  taosLogTraceToBuf(tmp, sizeof(tmp), 8);
#endif

  tjsonAddStringToObject(pJson, "stackInfo", tmp);

  char* pCont = tjsonToString(pJson);
  tjsonDelete(pJson);

  *pMsg = pCont;

  return TSDB_CODE_SUCCESS;
}
