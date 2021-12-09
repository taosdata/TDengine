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
#include "os.h"
#include "taoserror.h"
#include "tref.h"
#include "tfile.h"
#include "cJSON.h"
#include "walInt.h"

#include <libgen.h>
#include <regex.h>

int walRollFileInfo(SWal* pWal) {
  int64_t ts = taosGetTimestampSec();

  SArray* pArray = pWal->fileInfoSet;
  if(taosArrayGetSize(pArray) != 0) {
    WalFileInfo *pInfo = taosArrayGetLast(pArray);
    pInfo->lastVer = pWal->lastVersion;
    pInfo->closeTs = ts;
  }

  WalFileInfo *pNewInfo = malloc(sizeof(WalFileInfo));
  if(pNewInfo == NULL) {
    return -1;
  }
  pNewInfo->firstVer = pWal->lastVersion + 1;
  pNewInfo->lastVer = -1;
  pNewInfo->createTs = ts;
  pNewInfo->closeTs = -1;
  pNewInfo->fileSize = 0;
  taosArrayPush(pWal->fileInfoSet, pNewInfo);
  return 0;
}

char* walFileInfoSerialize(SWal* pWal) {
  char buf[30];
  if(pWal == NULL || pWal->fileInfoSet == NULL) return 0;
  int sz = pWal->fileInfoSet->size;
  cJSON* root = cJSON_CreateArray();
  cJSON* field;
  if(root == NULL) {
    //TODO
    return NULL;
  }
  WalFileInfo* pData = pWal->fileInfoSet->pData;
  for(int i = 0; i < sz; i++) {
    WalFileInfo* pInfo = &pData[i];
    cJSON_AddItemToArray(root, field = cJSON_CreateObject());
    if(field == NULL) {
      cJSON_Delete(root);
      return NULL;
    }
    //cjson only support int32_t or double
    //string are used to prohibit the loss of precision
    sprintf(buf, "%ld", pInfo->firstVer);
    cJSON_AddStringToObject(field, "firstVer", buf);
    sprintf(buf, "%ld", pInfo->lastVer);
    cJSON_AddStringToObject(field, "lastVer", buf);
    sprintf(buf, "%ld", pInfo->createTs);
    cJSON_AddStringToObject(field, "createTs", buf);
    sprintf(buf, "%ld", pInfo->closeTs);
    cJSON_AddStringToObject(field, "closeTs", buf);
    sprintf(buf, "%ld", pInfo->fileSize);
    cJSON_AddStringToObject(field, "fileSize", buf);
  }
  return cJSON_Print(root);
}

SArray* walFileInfoDeserialize(const char* bytes) {
  cJSON *root, *pInfoJson, *pField;
  root = cJSON_Parse(bytes);
  int sz = cJSON_GetArraySize(root);
  //deserialize
  SArray* pArray = taosArrayInit(sz, sizeof(WalFileInfo));
  WalFileInfo *pData = pArray->pData;
  for(int i = 0; i < sz; i++) {
    cJSON* pInfoJson = cJSON_GetArrayItem(root, i);
    WalFileInfo* pInfo = &pData[i];
    pField = cJSON_GetObjectItem(pInfoJson, "firstVer");
    pInfo->firstVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "lastVer");
    pInfo->lastVer = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "createTs");
    pInfo->createTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "closeTs");
    pInfo->closeTs = atoll(cJSON_GetStringValue(pField));
    pField = cJSON_GetObjectItem(pInfoJson, "fileSize");
    pInfo->fileSize = atoll(cJSON_GetStringValue(pField));
  }
  taosArraySetSize(pArray, sz);
  return pArray;
}

static inline int walBuildMetaName(SWal* pWal, int metaVer, char* buf) {
  return sprintf(buf, "%s/meta-ver%d", pWal->path, metaVer);
}

static int walFindCurMetaVer(SWal* pWal) {
  const char * pattern = "^meta-ver[0-9]+$";
  regex_t walMetaRegexPattern;
  regcomp(&walMetaRegexPattern, pattern, REG_EXTENDED);

  DIR *dir = opendir(pWal->path); 
  if(dir == NULL) {
    wError("vgId:%d, path:%s, failed to open since %s", pWal->vgId, pWal->path, strerror(errno));
    return -1;
  }

  struct dirent* ent;

  //find existing meta-ver[x].json
  int metaVer = -1;
  while((ent = readdir(dir)) != NULL) {
    char *name = basename(ent->d_name);
    int code = regexec(&walMetaRegexPattern, name, 0, NULL, 0);
    if(code == 0) {
      sscanf(name, "meta-ver%d", &metaVer);
      break;
    }
  }
  return metaVer;
}

int walWriteMeta(SWal* pWal) {
  int metaVer = walFindCurMetaVer(pWal);
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer+1, fnameStr);
  int metaTfd = tfOpenCreateWrite(fnameStr);
  if(metaTfd < 0) {
    return -1;
  }
  char* serialized = walFileInfoSerialize(pWal);
  int len = strlen(serialized);
  if(len != tfWrite(metaTfd, serialized, len)) {
    //TODO:clean file
    return -1;
  }
  
  tfClose(metaTfd);
  //delete old file
  if(metaVer > -1) {
    walBuildMetaName(pWal, metaVer, fnameStr);
    remove(fnameStr);
  }
  return 0;
}

int walReadMeta(SWal* pWal) {
  ASSERT(pWal->fileInfoSet->size == 0);
  //find existing meta file
  int metaVer = walFindCurMetaVer(pWal);
  if(metaVer == -1) {
    return 0;
  }
  char fnameStr[WAL_FILE_LEN];
  walBuildMetaName(pWal, metaVer, fnameStr);
  //read metafile
  struct stat statbuf;
  stat(fnameStr, &statbuf);
  int size = statbuf.st_size;
  char* buf = malloc(size + 5);
  if(buf == NULL) {
    return -1;
  }
  int tfd = tfOpenRead(fnameStr);
  if(tfRead(tfd, buf, size) != size) {
    free(buf);
    return -1;
  }
  //load into fileInfoSet
  pWal->fileInfoSet = walFileInfoDeserialize(buf);
  if(pWal->fileInfoSet == NULL) {
    free(buf);
    return -1;
  }
  free(buf);
  return 0;
}
