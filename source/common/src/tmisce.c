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
#include "tdatablock.h"
#include "tglobal.h"
#include "tjson.h"
#include "tmisce.h"
#include "tcompare.h"

int32_t taosGetIpv4FromEp(const char* ep, SEp* pEp) {
  pEp->port = 0;
  memset(pEp->fqdn, 0, TSDB_FQDN_LEN);
  tstrncpy(pEp->fqdn, ep, TSDB_FQDN_LEN);

  char* temp = strrchr(pEp->fqdn, ':');
  if (temp) {
    *temp = 0;
    pEp->port = taosStr2UInt16(temp + 1, NULL, 10);
    if (pEp->port < 0) {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
  }

  if (pEp->port <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return 0;
}

static int8_t isValidPort(const char* str) {
  if (!str || !*str) return 0;
  for (const char* p = str; *p; ++p) {
    if (*p < '0' || *p > '9') {
      return 0;
    }
  }
  return 1;
}

static int32_t taosGetDualIpFromEp(const char* ep, SEp* pEp) {
  memset(pEp->fqdn, 0, TSDB_FQDN_LEN);
  pEp->port = 0;
  char buf[TSDB_FQDN_LEN] = {0};

  if (ep[0] == '[') {
    // [IPv6]:port format
    const char* end = strchr(ep, ']');
    if (!end) return TSDB_CODE_INVALID_PARA;

    int ipLen = end - ep - 1;
    if (ipLen >= TSDB_FQDN_LEN) ipLen = TSDB_FQDN_LEN - 1;

    strncpy(pEp->fqdn, ep + 1, ipLen);
    pEp->fqdn[ipLen] = '\0';

    if (*(end + 1) == ':' && *(end + 2)) {
      pEp->port = taosStr2UInt16(end + 2, NULL, 10);
    }
  } else {
    // Compatible with ::1:6030, ::1, IPv4:port, hostname:port, etc.
    strncpy(buf, ep, TSDB_FQDN_LEN - 1);
    buf[TSDB_FQDN_LEN - 1] = 0;

    char* lastColon = strrchr(buf, ':');
    char* firstColon = strchr(buf, ':');

    if (lastColon && firstColon != lastColon) {
      // Multiple colons, possibly IPv6:port or pure IPv6
      char* portStr = lastColon + 1;
      if (isValidPort(portStr) && lastColon != buf && (*(lastColon - 1) != ':')) {
        *lastColon = 0;
        tstrncpy(pEp->fqdn, buf, TSDB_FQDN_LEN);
        pEp->port = taosStr2UInt16(portStr, NULL, 10);
      } else {
        tstrncpy(pEp->fqdn, ep, TSDB_FQDN_LEN);
      }
    } else if (lastColon) {
      // Only one colon, IPv4:port or hostname:port
      char* portStr = lastColon + 1;
      if (isValidPort(portStr) && lastColon != buf) {
        *lastColon = 0;
        tstrncpy(pEp->fqdn, buf, TSDB_FQDN_LEN);
        pEp->port = taosStr2UInt16(portStr, NULL, 10);
      } else {
        tstrncpy(pEp->fqdn, ep, TSDB_FQDN_LEN);
      }
    } else {
      // No colon, pure hostname or IPv6 without port
      tstrncpy(pEp->fqdn, ep, TSDB_FQDN_LEN);
    }
  }

  if (pEp->port == 0) {
    pEp->port = tsServerPort;
  }

  if (pEp->port <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}
int32_t taosGetFqdnPortFromEp(const char* ep, SEp* pEp) {
  if (tsEnableIpv6) {
    return taosGetDualIpFromEp(ep, pEp);
  } else {
    return taosGetIpv4FromEp(ep, pEp);
  }
}

int32_t addEpIntoEpSet(SEpSet* pEpSet, const char* fqdn, uint16_t port) {
  if (pEpSet == NULL || fqdn == NULL || strlen(fqdn) == 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t index = pEpSet->numOfEps;
  if (index >= sizeof(pEpSet->eps) / sizeof(pEpSet->eps[0])) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  tstrncpy(pEpSet->eps[index].fqdn, fqdn, tListLen(pEpSet->eps[index].fqdn));
  pEpSet->eps[index].port = port;
  pEpSet->numOfEps += 1;
  return 0;
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
        SEp ep1 = {0};
        epAssign(&ep1, f);
        epAssign(f, s);
        epAssign(s, &ep1);
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

int32_t epsetToStr(const SEpSet* pEpSet, char* pBuf, int32_t cap) {
  int32_t ret = 0;
  int32_t nwrite = 0;

  nwrite = snprintf(pBuf + nwrite, cap, "epset:{");
  if (nwrite <= 0 || nwrite >= cap) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }
  cap -= nwrite;

  for (int _i = 0; (_i < pEpSet->numOfEps) && (cap > 0); _i++) {
    if (_i == pEpSet->numOfEps - 1) {
      ret = snprintf(pBuf + nwrite, cap, "%d. %s:%d", _i, pEpSet->eps[_i].fqdn, pEpSet->eps[_i].port);
    } else {
      ret = snprintf(pBuf + nwrite, cap, "%d. %s:%d, ", _i, pEpSet->eps[_i].fqdn, pEpSet->eps[_i].port);
    }

    if (ret <= 0 || ret >= cap) {
      return TSDB_CODE_OUT_OF_BUFFER;
    }

    nwrite += ret;
    cap -= ret;
  }

  if (cap <= 0) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }

  ret = snprintf(pBuf + nwrite, cap, "}, inUse:%d", pEpSet->inUse);
  if (ret <= 0 || ret >= cap) {
    return TSDB_CODE_OUT_OF_BUFFER;
  } else {
    return TSDB_CODE_SUCCESS;
  }
}

int32_t taosGenCrashJsonMsg(int signum, char** pMsg, int64_t clusterId, int64_t startTime) {
  int32_t code = 0;
  SJson*  pJson = tjsonCreateObject();
  if (pJson == NULL) return terrno;

  char tmp[4096] = {0};

  TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "reportVersion", 1), NULL, _exit);

  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "clusterId", clusterId), NULL, _exit);
  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "startTime", startTime), NULL, _exit);

  // Do NOT invoke the taosGetFqdn here.
  // this function may be invoked when memory exception occurs,so we should assume that it is running in a memory locked
  // environment. The lock operation by taosGetFqdn may cause this program deadlock.
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "fqdn", tsLocalFqdn), NULL, _exit);

  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "pid", taosGetPId()), NULL, _exit);

  code = taosGetAppName(tmp, NULL);
  if (code != 0) {
    TAOS_CHECK_GOTO(code, NULL, _exit);
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "appName", tmp), NULL, _exit);

  if (taosGetOsReleaseName(tmp, NULL, NULL, sizeof(tmp)) == 0) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "os", tmp), NULL, _exit);
  } else {
    // do nothing
  }

  float numOfCores = 0;
  if (taosGetCpuInfo(tmp, sizeof(tmp), &numOfCores) == 0) {
    TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "cpuModel", tmp), NULL, _exit);
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfCpu", numOfCores), NULL, _exit);
  } else {
    TAOS_CHECK_GOTO(tjsonAddDoubleToObject(pJson, "numOfCpu", tsNumOfCores), NULL, _exit);
  }

  int32_t nBytes = snprintf(tmp, sizeof(tmp), "%" PRId64 " kB", tsTotalMemoryKB);
  if (nBytes <= 9 || nBytes >= sizeof(tmp)) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_RANGE, NULL, _exit);
  }
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "memory", tmp), NULL, _exit);

  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "version", td_version), NULL, _exit);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "buildInfo", td_buildinfo), NULL, _exit);
  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "gitInfo", td_gitinfo), NULL, _exit);

  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "crashSig", signum), NULL, _exit);
  TAOS_CHECK_GOTO(tjsonAddIntegerToObject(pJson, "crashTs", taosGetTimestampUs()), NULL, _exit);

#if 0
#ifdef _TD_DARWIN_64
  taosLogTraceToBuf(tmp, sizeof(tmp), 4);
#elif !defined(WINDOWS)
  taosLogTraceToBuf(tmp, sizeof(tmp), 3);
#else
  taosLogTraceToBuf(tmp, sizeof(tmp), 8);
#endif

  TAOS_CHECK_GOTO(tjsonAddStringToObject(pJson, "stackInfo", tmp), NULL, _exit);
#endif
  char* pCont = tjsonToString(pJson);
  if (pCont == NULL) {
    code = terrno;
    TAOS_CHECK_GOTO(code, NULL, _exit);
    goto _exit;
  }

  tjsonDelete(pJson);
  *pMsg = pCont;
  pJson = NULL;
_exit:
  tjsonDelete(pJson);
  TAOS_RETURN(code);
}

int32_t dumpConfToDataBlock(SSDataBlock* pBlock, int32_t startCol, char* likePattern) {
  int32_t  code = 0;
  SConfig* pConf = taosGetCfg();
  if (pConf == NULL) {
    return TSDB_CODE_INVALID_CFG;
  }

  int32_t      numOfRows = 0;
  int32_t      col = startCol;
  SConfigItem* pItem = NULL;
  SConfigIter* pIter = NULL;

  int8_t locked = 0;

  size_t       exSize = 0;
  size_t       index = 0;
  SConfigItem* pDataDirItem = cfgGetItem(pConf, "dataDir");
  if (pDataDirItem) {
    exSize = TMAX(taosArrayGetSize(pDataDirItem->array), 1) - 1;
  }

  TAOS_CHECK_GOTO(blockDataEnsureCapacity(pBlock, cfgGetSize(pConf) + exSize), NULL, _exit);

  TAOS_CHECK_GOTO(cfgCreateIter(pConf, &pIter), NULL, _exit);

  cfgLock(pConf);
  locked = 1;

  while ((pItem = cfgNextIter(pIter)) != NULL) {
  _start:
    col = startCol;

    // GRANT_CFG_SKIP;
    char name[TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE] = {0};
    if (likePattern && rawStrPatternMatch(pItem->name, likePattern) != TSDB_PATTERN_MATCH) {
      continue;
    }
    STR_WITH_MAXSIZE_TO_VARSTR(name, pItem->name, TSDB_CONFIG_OPTION_LEN + VARSTR_HEADER_SIZE);

    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, col++);
    if (pColInfo == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, NULL, _exit);
    }

    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, name, false), NULL, _exit);

    char      value[TSDB_CONFIG_PATH_LEN + VARSTR_HEADER_SIZE] = {0};
    int32_t   valueLen = 0;
    SDiskCfg* pDiskCfg = NULL;
    if (strcasecmp(pItem->name, "dataDir") == 0 && exSize > 0) {
      char* buf = &value[VARSTR_HEADER_SIZE];
      pDiskCfg = taosArrayGet(pItem->array, index);
      valueLen = tsnprintf(buf, TSDB_CONFIG_PATH_LEN, "%s", pDiskCfg->dir);
      index++;
    } else {
      TAOS_CHECK_GOTO(cfgDumpItemValue(pItem, &value[VARSTR_HEADER_SIZE], TSDB_CONFIG_PATH_LEN, &valueLen), NULL,
                      _exit);
    }
    varDataSetLen(value, valueLen);

    pColInfo = taosArrayGet(pBlock->pDataBlock, col++);
    if (pColInfo == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, NULL, _exit);
    }

    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, value, false), NULL, _exit);

    char scope[TSDB_CONFIG_SCOPE_LEN + VARSTR_HEADER_SIZE] = {0};
    TAOS_CHECK_GOTO(cfgDumpItemScope(pItem, &scope[VARSTR_HEADER_SIZE], TSDB_CONFIG_SCOPE_LEN, &valueLen), NULL, _exit);
    varDataSetLen(scope, valueLen);

    pColInfo = taosArrayGet(pBlock->pDataBlock, col++);
    if (pColInfo == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, NULL, _exit);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, scope, false), NULL, _exit);

    char category[TSDB_CONFIG_CATEGORY_LEN + VARSTR_HEADER_SIZE] = {0};
    TAOS_CHECK_GOTO(cfgDumpItemCategory(pItem, &category[VARSTR_HEADER_SIZE], TSDB_CONFIG_CATEGORY_LEN, &valueLen),
                    NULL, _exit);
    varDataSetLen(category, valueLen);

    pColInfo = taosArrayGet(pBlock->pDataBlock, col++);
    if (pColInfo == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, NULL, _exit);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, category, false), NULL, _exit);

    char info[TSDB_CONFIG_INFO_LEN + VARSTR_HEADER_SIZE] = {0};
    if (strcasecmp(pItem->name, "dataDir") == 0 && pDiskCfg) {
      char* buf = &info[VARSTR_HEADER_SIZE];
      valueLen = tsnprintf(buf, TSDB_CONFIG_INFO_LEN, "level %d primary %d disabled %" PRIi8, pDiskCfg->level,
                           pDiskCfg->primary, pDiskCfg->disable);
    } else {
      valueLen = 0;
    }
    varDataSetLen(info, valueLen);

    pColInfo = taosArrayGet(pBlock->pDataBlock, col++);
    if (pColInfo == NULL) {
      code = terrno;
      TAOS_CHECK_GOTO(code, NULL, _exit);
    }
    TAOS_CHECK_GOTO(colDataSetVal(pColInfo, numOfRows, info, false), NULL, _exit);

    numOfRows++;
    if (index > 0 && index <= exSize) {
      goto _start;
    }
  }
  pBlock->info.rows = numOfRows;
_exit:
  if (locked) cfgUnLock(pConf);
  cfgDestroyIter(pIter);
  TAOS_RETURN(code);
}
