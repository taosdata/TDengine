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
#include "dmRepairCopy.h"
#include "tconfig.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlog.h"

// Lightweight TFS model — uses SDiskCfg directly with a parallel array
// of tier-local IDs assigned during construction.
typedef struct SRepairTfs {
  int32_t   ndisk;
  int32_t   nlevel;
  SDiskCfg *disks;       // array of ndisk disks (owned)
  int32_t   primaryIdx;  // index into disks[] for the primary disk
} SRepairTfs;

static int32_t compareInt32(const void *a, const void *b) {
  int32_t va = *(const int32_t *)a;
  int32_t vb = *(const int32_t *)b;
  if (va < vb) return -1;
  if (va > vb) return 1;
  return 0;
}

SArray *dmParseVnodeIds(const char *str) {
  if (str == NULL || str[0] == '\0') return NULL;

  SArray *pArr = taosArrayInit(8, sizeof(int32_t));
  if (pArr == NULL) return NULL;

  const char *p = str;
  while (*p != '\0') {
    // skip leading whitespace
    while (*p == ' ' || *p == '\t') p++;
    if (*p == '\0') break;

    // parse first number
    char   *end = NULL;
    int32_t lo = taosStr2Int32(p, &end, 10);
    if (end == p || lo <= 0) goto _err;

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == '-') {
      // range: lo-hi
      end++;
      int32_t hi = taosStr2Int32(end, &end, 10);
      if (hi < lo) goto _err;
      for (int32_t id = lo; id <= hi; id++) {
        if (taosArrayPush(pArr, &id) == NULL) goto _err;
      }
    } else {
      if (taosArrayPush(pArr, &lo) == NULL) goto _err;
    }

    // skip whitespace
    while (*end == ' ' || *end == '\t') end++;

    if (*end == ',') {
      end++;
    } else if (*end != '\0') {
      goto _err;
    }
    p = end;
  }

  if (taosArrayGetSize(pArr) == 0) goto _err;

  taosArraySort(pArr, compareInt32);
  taosArrayRemoveDuplicate(pArr, compareInt32, NULL);

  return pArr;

_err:
  taosArrayDestroy(pArr);
  return NULL;
}

// Fetch a remote file to a local path via SSH.
// Returns 0 on success, -1 on error.
static int32_t dmSshFetchFile(const char *host, const char *remotePath, const char *localPath) {
  char cmd[1024];
  snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes %s cat '%s' > '%s' 2>/dev/null", host, remotePath, localPath);
  TdCmdPtr pCmd = taosOpenCmd(cmd);
  if (pCmd == NULL) {
    uError("repair: failed to run ssh command");
    return -1;
  }
  char buf[256];
  while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {}
  taosCloseCmd(&pCmd);

  // Verify file has content
  int64_t fsize = 0;
  if (taosStatFile(localPath, &fsize, NULL, NULL) != 0 || fsize <= 0) {
    uError("repair: ssh fetch returned empty file for %s:%s", host, remotePath);
    (void)taosRemoveFile(localPath);
    return -1;
  }
  return 0;
}

// Parse a taos.cfg file and extract SDiskCfg entries from the dataDir items.
// Returns 0 on success. On success, caller must free *ppDisks.
static int32_t dmParseSourceCfg(const char *cfgPath, SDiskCfg **ppDisks, int32_t *pNumDisks) {
  SConfig *pCfg = NULL;
  int32_t  code = cfgInit(&pCfg);
  if (code != 0) {
    uError("repair: cfgInit failed: %s", tstrerror(code));
    return -1;
  }

  // Register dataDir so cfgLoad knows how to handle it
  code = cfgAddDir(pCfg, "dataDir", "/tmp", CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL);
  if (code != 0) {
    uError("repair: cfgAddDir failed: %s", tstrerror(code));
    cfgCleanup(pCfg);
    return -1;
  }

  code = cfgLoad(pCfg, CFG_STYPE_CFG_FILE, cfgPath);
  if (code != 0) {
    uError("repair: cfgLoad failed for %s: %s", cfgPath, tstrerror(code));
    cfgCleanup(pCfg);
    return -1;
  }

  SConfigItem *pItem = cfgGetItem(pCfg, "dataDir");
  if (pItem == NULL) {
    uError("repair: no dataDir found in %s", cfgPath);
    cfgCleanup(pCfg);
    return -1;
  }

  int32_t ndisk = 0;
  if (pItem->array != NULL) {
    ndisk = taosArrayGetSize(pItem->array);
  }

  SDiskCfg *disks = NULL;
  if (ndisk <= 0) {
    // Single default dataDir from pItem->str
    ndisk = 1;
    disks = taosMemoryCalloc(1, sizeof(SDiskCfg));
    if (disks == NULL) {
      cfgCleanup(pCfg);
      return -1;
    }
    tstrncpy(disks[0].dir, pItem->str, TSDB_FILENAME_LEN);
    disks[0].level = 0;
    disks[0].primary = 1;
    disks[0].disable = 0;
  } else {
    if (ndisk > TFS_MAX_DISKS) ndisk = TFS_MAX_DISKS;
    disks = taosMemoryCalloc(ndisk, sizeof(SDiskCfg));
    if (disks == NULL) {
      cfgCleanup(pCfg);
      return -1;
    }
    for (int32_t i = 0; i < ndisk; i++) {
      SDiskCfg *pSrc = taosArrayGet(pItem->array, i);
      disks[i] = *pSrc;
    }
  }

  cfgCleanup(pCfg);
  *ppDisks = disks;
  *pNumDisks = ndisk;
  return 0;
}

// Build a lightweight source TFS model from SDiskCfg array.
// Assigns tier-local IDs to each disk. Returns 0 on success.
static int32_t dmBuildRepairTfs(const SDiskCfg *pCfgArr, int32_t ndisk, SRepairTfs *pTfs) {
  pTfs->ndisk = ndisk;
  pTfs->nlevel = 0;
  pTfs->primaryIdx = -1;
  pTfs->disks = taosMemoryCalloc(ndisk, sizeof(SDiskCfg));
  if (pTfs->disks == NULL) return -1;

  for (int32_t i = 0; i < ndisk; i++) {
    int32_t lvl = pCfgArr[i].level;
    if (lvl < 0 || lvl >= TFS_MAX_TIERS) {
      uError("repair: invalid disk level %d for %s", lvl, pCfgArr[i].dir);
      taosMemoryFree(pTfs->disks);
      pTfs->disks = NULL;
      return -1;
    }

    pTfs->disks[i] = pCfgArr[i];

    if (lvl + 1 > pTfs->nlevel) {
      pTfs->nlevel = lvl + 1;
    }
    if (pCfgArr[i].primary && lvl == 0) {
      pTfs->primaryIdx = i;
    }
  }

  if (pTfs->primaryIdx < 0) {
    uError("repair: no primary disk found in source config");
    taosMemoryFree(pTfs->disks);
    pTfs->disks = NULL;
    return -1;
  }

  return 0;
}

static void dmDestroyRepairTfs(SRepairTfs *pTfs) {
  if (pTfs == NULL) return;
  taosMemoryFreeClear(pTfs->disks);
  pTfs->ndisk = 0;
  pTfs->nlevel = 0;
  pTfs->primaryIdx = -1;
}

// Validate source disk paths exist (local mode only).
static int32_t dmValidateSourceDisksLocal(const SRepairTfs *pTfs) {
  for (int32_t i = 0; i < pTfs->ndisk; i++) {
    if (!taosDirExist(pTfs->disks[i].dir)) {
      uError("repair: source dataDir does not exist: %s", pTfs->disks[i].dir);
      return -1;
    }
  }
  return 0;
}

// Validate source disk paths exist (remote mode).
static int32_t dmValidateSourceDisksRemote(const char *host, const SRepairTfs *pTfs) {
  for (int32_t i = 0; i < pTfs->ndisk; i++) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "ssh -o BatchMode=yes %s test -d '%s' && echo YES", host, pTfs->disks[i].dir);
    TdCmdPtr pCmd = taosOpenCmd(cmd);
    if (pCmd == NULL) {
      uError("repair: ssh connectivity failed");
      return -1;
    }
    bool found = false;
    char buf[64];
    while (taosGetsCmd(pCmd, sizeof(buf), buf) > 0) {
      if (strncmp(buf, "YES", 3) == 0) found = true;
    }
    taosCloseCmd(&pCmd);
    if (!found) {
      uError("repair: remote dataDir does not exist: %s:%s", host, pTfs->disks[i].dir);
      return -1;
    }
  }
  return 0;
}

// Load dnodeId from {tsDataDir}/dnode/dnode.json.
// Returns 0 on success, -1 on error (file missing or parse failure).
static int32_t dmLoadDnodeInfo(int32_t *pDnodeId) {
  char file[PATH_MAX] = {0};
  snprintf(file, sizeof(file), "%s%sdnode%sdnode.json", tsDataDir, TD_DIRSEP, TD_DIRSEP);

  if (taosStatFile(file, NULL, NULL, NULL) < 0) {
    uError("repair: dnode.json not found: %s", file);
    return -1;
  }

  TdFilePtr pFile = taosOpenFile(file, TD_FILE_READ);
  if (pFile == NULL) {
    uError("repair: failed to open %s", file);
    return -1;
  }

  int64_t fsize = 0;
  if (taosStatFile(file, &fsize, NULL, NULL) != 0 || fsize <= 0) {
    uError("repair: dnode.json is empty: %s", file);
    taosCloseFile(&pFile);
    return -1;
  }

  char *content = taosMemoryMalloc(fsize + 1);
  if (content == NULL) {
    taosCloseFile(&pFile);
    return -1;
  }

  int64_t nread = taosReadFile(pFile, content, fsize);
  taosCloseFile(&pFile);
  if (nread <= 0) {
    uError("repair: failed to read %s", file);
    taosMemoryFree(content);
    return -1;
  }
  content[nread] = '\0';

  SJson *pJson = tjsonParse(content);
  taosMemoryFree(content);
  if (pJson == NULL) {
    uError("repair: failed to parse dnode.json");
    return -1;
  }

  int32_t code = 0;
  int32_t dnodeId = 0;
  tjsonGetInt32ValueFromDouble(pJson, "dnodeId", dnodeId, code);
  if (code < 0 || dnodeId <= 0) {
    uError("repair: invalid or missing dnodeId in dnode.json");
    tjsonDelete(pJson);
    return -1;
  }

  tjsonDelete(pJson);
  *pDnodeId = dnodeId;
  return 0;
}

// Open target TFS from global tsDiskCfg[]/tsDiskCfgNum.
// Returns 0 on success. Caller must call tfsClose(ppTfs) to free.
static int32_t dmOpenTargetTfs(STfs **ppTfs) {
  SDiskCfg *pDisks = tsDiskCfg;
  int32_t   numOfDisks = tsDiskCfgNum;

  SDiskCfg tmpDisk = {0};
  if (numOfDisks <= 0) {
    // Fallback: single disk from tsDataDir
    tmpDisk.level = 0;
    tmpDisk.primary = 1;
    tmpDisk.disable = 0;
    tstrncpy(tmpDisk.dir, tsDataDir, TSDB_FILENAME_LEN);
    pDisks = &tmpDisk;
    numOfDisks = 1;
  }

  int32_t code = tfsOpen(pDisks, numOfDisks, ppTfs);
  if (code != 0) {
    uError("repair: failed to open target TFS: %s", tstrerror(code));
    return -1;
  }
  return 0;
}

int32_t dmRepairCopyMode(const SRepairCopyOpts *pOpts) {
  bool isRemote = (pOpts->sourceHost[0] != '\0');

  printf("repair: starting copy-mode repair (%s mode)\n", isRemote ? "remote" : "local");
  printf("repair: source config: %s\n", pOpts->sourceCfg);
  if (isRemote) {
    printf("repair: source host: %s\n", pOpts->sourceHost);
  }
  int32_t nVnodes = taosArrayGetSize(pOpts->vnodeIds);
  printf("repair: vnodes to repair: %d\n", nVnodes);

  // Phase 2: Parse source config file
  const char *cfgPathToLoad = pOpts->sourceCfg;
  char        tmpCfgPath[PATH_MAX] = {0};

  if (isRemote) {
    // Fetch remote config via SSH
    snprintf(tmpCfgPath, sizeof(tmpCfgPath), "/tmp/tdrepair_%d.cfg", (int)taosGetPId());
    if (dmSshFetchFile(pOpts->sourceHost, pOpts->sourceCfg, tmpCfgPath) != 0) {
      printf("repair: failed to fetch remote config via SSH (exit code 2)\n");
      return 2;
    }
    cfgPathToLoad = tmpCfgPath;
  }

  SDiskCfg *srcDisks = NULL;
  int32_t   srcDiskNum = 0;
  int32_t   code = dmParseSourceCfg(cfgPathToLoad, &srcDisks, &srcDiskNum);
  if (tmpCfgPath[0] != '\0') {
    (void)taosRemoveFile(tmpCfgPath);
  }
  if (code != 0) {
    printf("repair: failed to parse source config file\n");
    return isRemote ? 2 : 1;
  }

  printf("repair: source config has %d disk(s)\n", srcDiskNum);

  // Build source TFS model
  SRepairTfs srcTfs = {0};
  if (dmBuildRepairTfs(srcDisks, srcDiskNum, &srcTfs) != 0) {
    printf("repair: failed to build source TFS model\n");
    taosMemoryFree(srcDisks);
    return 1;
  }
  taosMemoryFree(srcDisks);

  printf("repair: source TFS: %d level(s), %d disk(s), primary=%s\n",
         srcTfs.nlevel, srcTfs.ndisk, srcTfs.disks[srcTfs.primaryIdx].dir);

  // Validate source disk paths exist
  if (isRemote) {
    if (dmValidateSourceDisksRemote(pOpts->sourceHost, &srcTfs) != 0) {
      printf("repair: source disk validation failed (exit code 2)\n");
      dmDestroyRepairTfs(&srcTfs);
      return 2;
    }
  } else {
    if (dmValidateSourceDisksLocal(&srcTfs) != 0) {
      printf("repair: source disk validation failed\n");
      dmDestroyRepairTfs(&srcTfs);
      return 1;
    }
  }

  printf("repair: source disk validation passed\n");

  // Phase 3: Load dnode info and open target TFS
  int32_t dnodeId = 0;
  if (dmLoadDnodeInfo(&dnodeId) != 0) {
    printf("repair: failed to load dnode.json (exit code 1)\n");
    dmDestroyRepairTfs(&srcTfs);
    return 1;
  }
  printf("repair: local dnodeId = %d\n", dnodeId);

  STfs *pTgtTfs = NULL;
  if (dmOpenTargetTfs(&pTgtTfs) != 0) {
    printf("repair: failed to open target TFS (exit code 1)\n");
    dmDestroyRepairTfs(&srcTfs);
    return 1;
  }
  printf("repair: target TFS: %d level(s), primary=%s\n",
         tfsGetLevel(pTgtTfs), tfsGetPrimaryPath(pTgtTfs));

  // TODO: Phase 4+ — per-vnode repair loop

  tfsClose(pTgtTfs);
  dmDestroyRepairTfs(&srcTfs);
  return 0;
}
