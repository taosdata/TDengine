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

#include "sma.h"

// =================================================================================================

static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output);

/**
 * @brief Open RSma FS from qTaskInfo files
 *
 * @param pSma
 * @param version
 * @return int32_t
 */
int32_t tdRSmaFSOpen(SSma *pSma, int64_t version) {
  SVnode    *pVnode = pSma->pVnode;
  int64_t    commitID = pVnode->state.commitID;
  SSmaEnv   *pEnv = SMA_RSMA_ENV(pSma);
  SRSmaStat *pStat = NULL;
  SArray    *output = NULL;
  SRSmaFS   *fs = NULL;

  if (!pEnv) {
    return TSDB_CODE_SUCCESS;
  }

  if (tdFetchQTaskInfoFiles(pSma, version, &output) < 0) {
    goto _end;
  }

  pStat = (SRSmaStat *)SMA_ENV_STAT(pEnv);
  fs = &pStat->fs;

  for (int32_t i = 0; i < taosArrayGetSize(output); ++i) {
    int32_t vid = 0;
    int64_t version = -1;
    sscanf((const char *)taosArrayGetP(output, i), "v%dqinfo.v%" PRIi64, &vid, &version);
    SQTaskFile qTaskFile = {.version = version, .nRef = 1};
    if ((terrno = tdRSmaFSUpsertQFile(fs, &qTaskFile)) < 0) {
      goto _end;
    }
    smaInfo("vgId:%d, open fs, version:%" PRIi64 ", ref:%" PRIi64, TD_VID(pVnode), qTaskFile.version, qTaskFile.nRef);
  }

_end:
  for (int32_t i = 0; i < taosArrayGetSize(output); ++i) {
    void *ptr = taosArrayGetP(output, i);
    taosMemoryFreeClear(ptr);
  }
  taosArrayDestroy(output);
  
  if (terrno != 0) {
    smaError("vgId:%d, open rsma fs failed since %s", TD_VID(pVnode), terrstr());
    return TSDB_CODE_FAILED;
  }
  return TSDB_CODE_SUCCESS;
}

void tdRSmaFSClose(SRSmaFS *fs) { taosArrayDestroy(fs->aQTaskInf); }

/**
 * @brief Fetch qtaskfiles no more than version
 *
 * @param pSma
 * @param version
 * @param output
 * @return int32_t
 */
static int32_t tdFetchQTaskInfoFiles(SSma *pSma, int64_t version, SArray **output) {
  SVnode       *pVnode = pSma->pVnode;
  TdDirPtr      pDir = NULL;
  TdDirEntryPtr pDirEntry = NULL;
  char          dir[TSDB_FILENAME_LEN];
  const char   *pattern = "v[0-9]+qinf\\.v([0-9]+)?$";
  regex_t       regex;
  int           code = 0;

  tdGetVndDirName(TD_VID(pVnode), tfsGetPrimaryPath(pVnode->pTfs), VNODE_RSMA_DIR, true, dir);

  // Resource allocation and init
  if ((code = regcomp(&regex, pattern, REG_EXTENDED)) != 0) {
    terrno = TSDB_CODE_RSMA_REGEX_MATCH;
    char errbuf[128];
    regerror(code, &regex, errbuf, sizeof(errbuf));
    smaWarn("vgId:%d, fetch qtask files, regcomp for %s failed since %s", TD_VID(pVnode), dir, errbuf);
    return TSDB_CODE_FAILED;
  }

  if ((pDir = taosOpenDir(dir)) == NULL) {
    regfree(&regex);
    terrno = TAOS_SYSTEM_ERROR(errno);
    smaDebug("vgId:%d, fetch qtask files, open dir %s failed since %s", TD_VID(pVnode), dir, terrstr());
    return TSDB_CODE_FAILED;
  }

  int32_t    dirLen = strlen(dir);
  char      *dirEnd = POINTER_SHIFT(dir, dirLen);
  regmatch_t regMatch[2];
  while ((pDirEntry = taosReadDir(pDir)) != NULL) {
    char *entryName = taosGetDirEntryName(pDirEntry);
    if (!entryName) {
      continue;
    }

    code = regexec(&regex, entryName, 2, regMatch, 0);

    if (code == 0) {
      // match
      smaInfo("vgId:%d, fetch qtask files, max ver:%" PRIi64 ", %s found", TD_VID(pVnode), version, entryName);

      int64_t ver = -1;
      sscanf((const char *)POINTER_SHIFT(entryName, regMatch[1].rm_so), "%" PRIi64, &ver);
      if ((ver <= version) && (ver > -1)) {
        if (!(*output)) {
          if (!(*output = taosArrayInit(1, POINTER_BYTES))) {
            terrno = TSDB_CODE_OUT_OF_MEMORY;
            goto _end;
          }
        }
        char *entryDup = strdup(entryName);
        if (!entryDup) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
        if (!taosArrayPush(*output, &entryDup)) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
      }
    } else if (code == REG_NOMATCH) {
      // not match
      smaTrace("vgId:%d, fetch qtask files, not match %s", TD_VID(pVnode), entryName);
      continue;
    } else {
      // has other error
      char errbuf[128];
      regerror(code, &regex, errbuf, sizeof(errbuf));
      smaWarn("vgId:%d, fetch qtask files, regexec failed since %s", TD_VID(pVnode), errbuf);
      terrno = TSDB_CODE_RSMA_REGEX_MATCH;
      goto _end;
    }
  }

_end:
  taosCloseDir(&pDir);
  regfree(&regex);
  return terrno == 0 ? TSDB_CODE_SUCCESS : TSDB_CODE_FAILED;
}

static int32_t tdQTaskFileCmprFn(const void *p1, const void *p2) {
  if (((SQTaskFile *)p1)->version < ((SQTaskFile *)p2)->version) {
    return -1;
  } else if (((SQTaskFile *)p1)->version > ((SQTaskFile *)p2)->version) {
    return 1;
  }

  return 0;
}

int32_t tdRSmaFSUpsertQFile(SRSmaFS *pFS, SQTaskFile *qTaskFile) {
  int32_t code = 0;
  int32_t idx = taosArraySearchIdx(pFS->aQTaskInf, qTaskFile, tdQTaskFileCmprFn, TD_GE);

  if (idx < 0) {
    idx = taosArrayGetSize(pFS->aQTaskInf);
  } else {
    SQTaskFile *pTaskF = (SQTaskFile *)taosArrayGet(pFS->aQTaskInf, idx);
    int32_t     c = tdQTaskFileCmprFn(pTaskF, qTaskFile);
    if (c == 0) {
      pTaskF->nRef = qTaskFile->nRef;
      pTaskF->version = qTaskFile->version;
      pTaskF->size = qTaskFile->size;
      goto _exit;
    }
  }

  if (taosArrayInsert(pFS->aQTaskInf, idx, qTaskFile) == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  return code;
}
