#include <stdio.h>

#include <tsdbDataFileRW.h>
#include <tsdbReadUtil.h>
// #include <tsdbUtil2.h>
#include <vnode.h>

static int32_t load_json(const char *fname, cJSON **json) {
  int32_t code = 0;
  char   *data = NULL;

  TdFilePtr fp = taosOpenFile(fname, TD_FILE_READ);
  if (fp == NULL) return TAOS_SYSTEM_ERROR(code);

  int64_t size;
  if (taosFStatFile(fp, &size, NULL) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  data = taosMemoryMalloc(size + 1);
  if (data == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (taosReadFile(fp, data, size) < 0) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }
  data[size] = '\0';

  json[0] = cJSON_Parse(data);
  if (json[0] == NULL) {
    code = TSDB_CODE_FILE_CORRUPTED;
    goto _exit;
  }

_exit:
  taosCloseFile(&fp);
  if (data) taosMemoryFree(data);
  if (code) json[0] = NULL;
  return code;
}

int32_t loadTFile(const char *fname, STFile *pTf) {
  int32_t code = 0;

  cJSON *json = NULL;
  code = load_json(fname, &json);
  if (code) {
    printf("failed to load json file:%s, code:%d\n", fname, code);
    goto _exit;
  }

  // parse json
  const cJSON *item1;

  /* fmtv */
  item1 = cJSON_GetObjectItem(json, "fmtv");
  if (cJSON_IsNumber(item1)) {
    if (item1->valuedouble != 1) {
      printf("unsupported fmtv:%d\n", (int)item1->valuedouble);
      code = -1;
      goto _exit;
    }
  } else {
    code = -1;
    printf("failed to parse fmtv\n");
    goto _exit;
  }

  /* fset */
  item1 = cJSON_GetObjectItem(json, "fset");
  if (cJSON_IsArray(item1)) {
    const cJSON *item2;
    cJSON_ArrayForEach(item2, item1) {
      code = tsdbJsonToTFile(item2, TSDB_FTYPE_HEAD, pTf);  // only parse first head
      if (code) {
        printf("failed to parse first head\n");
        goto _exit;
      }

      break;
    }
  } else {
    code = -1;
    printf("failed to parse fset\n");
    goto _exit;
  }

  if (code == 0) {
    printf("tf: type:%d,did:[%d,%d],lcn:%d,fid:%d,size:%ld,minVer:%ld,maxVer:%ld\n", pTf->type, pTf->did.id,
           pTf->did.level, pTf->lcn, pTf->fid, pTf->size, pTf->minVer, pTf->maxVer);
  }

_exit:
  if (json) cJSON_Delete(json);
  return code;
}

SDataFileReader *initFileReader(STsdb *pTsdb, STFile *pTf) {
  SDataFileReader *pFileReader = NULL;

  SDataFileReaderConfig conf = {
      .tsdb = pTsdb, .szPage = pTsdb->pVnode->config.tsdbPageSize, .files[0].exist = true, .files[0].file = *pTf};
  // const char *filesName[TSDB_FTYPE_MAX] = {"/var/lib/taos/dnode1/vnode/vnode2/tsdb/v2f1853ver8.head"};

  if (tsdbDataFileReaderOpen(NULL, &conf, &pFileReader)) {
    printf("failed to open data file reader\n");
    return NULL;
  }

  printf("headPath: %s\n", pFileReader->fd[0]->path);

  return pFileReader;
}

void readBrinBlk(SDataFileReader *pFileReader, const TBrinBlkArray **pBrinBlkArray) {
  tsdbDataFileReadBrinBlk(pFileReader, pBrinBlkArray);

  for (int i = 0; i < (*pBrinBlkArray)->size; i++) {
    SBrinBlk *pBrinBlk = TARRAY2_GET_PTR((*pBrinBlkArray), i);
    char      buf[1024] = {0};
    int       offset = 0;
    for (int i = 0; i < 15; i++) {
      offset += snprintf(buf + offset, 1024 - offset, "%d,", pBrinBlk->size[i]);
    }

    printf(
        "brinBlk[%d]: dp[%ld,%ld],minTbid[%ld,%ld],maxTbid[%ld,%ld],vers[%ld,%ld],numRec:%d,"
        "size[%s],cmprAlg:%d,numOfPKs:%d\n",
        i, pBrinBlk->dp->offset, pBrinBlk->dp->size, pBrinBlk->minTbid.suid, pBrinBlk->minTbid.uid,
        pBrinBlk->maxTbid.suid, pBrinBlk->maxTbid.uid, pBrinBlk->minVer, pBrinBlk->maxVer, pBrinBlk->numRec, buf,
        pBrinBlk->cmprAlg, pBrinBlk->numOfPKs);
  }
}

void dumpRecord(SDataFileReader *pFileReader, const TBrinBlkArray *brinBlkArray, int index) {
  printf("----------------------- dump block %d -----------------------\n", index);

  SBrinBlk *pBrinBlk = TARRAY2_GET_PTR(brinBlkArray, index);

  SArray *pIndexList = taosArrayInit(1, sizeof(SBrinBlk));
  taosArrayPush(pIndexList, pBrinBlk);

  SBrinRecordIter iter = {0};
  initBrinRecordIter(&iter, pFileReader, pIndexList);

  SBrinRecord *pRecord = NULL;
  int32_t      i = 0;
  int32_t      code = 0;
  while (0 == (code = getNextBrinRecord(&iter, &pRecord))) {
    if (pRecord == NULL) {
      break;
    }

    printf(
        "record[%d]: "
        "suid,uid[%ld,%ld],firstKey[%ld,%u,%ld],lastKey[%ld,%u,%ld],vers[%ld,%ld],blockOffset:%ld,smaOffset:%ld,block[%"
        "d,%d],smaSize:%d,numRow:%d,count:%d\n",
        i, pRecord->suid, pRecord->uid, pRecord->firstKey.key.ts, pRecord->firstKey.key.numOfPKs,
        pRecord->firstKey.version, pRecord->lastKey.key.ts, pRecord->lastKey.key.numOfPKs, pRecord->lastKey.version,
        pRecord->minVer, pRecord->maxVer, pRecord->blockOffset, pRecord->smaOffset, pRecord->blockSize,
        pRecord->blockKeySize, pRecord->smaSize, pRecord->numRow, pRecord->count);
    i++;
  }

  if (code != 0) {
    printf("failed to get next record, code:%d\n", code);
  }
}

void printUsage() { printf("Usage: headdump -p <tsdbPath> -g <vgId>\n"); }

int main(int argc, char *argv[]) {
  int32_t vgId = -1;
  char   *path = NULL;

  int         op;
  const char *optStr = "p:g:";
  while ((op = getopt(argc, argv, optStr)) != -1) {
    switch (op) {
      case 'p':
        path = optarg;
        printf("path: %s\n", optarg);
        break;
      case 'g':
        vgId = atoi(optarg);
        printf("vgId: %d\n", vgId);
        break;
      case '?':
        printUsage();
        return -1;
      default:
        break;
    }
  }

  if (vgId == -1 || path == NULL) {
    printUsage();
    return -1;
  }

  SVnode vnode = {.config.tsdbPageSize = 4096, .config.vgId = vgId};
  STsdb  tsdb = {.pVnode = &vnode, .path = path};

  printf("tsdbPath: %s\n", tsdb.path);

  char currentPath[1024] = {0};
  sprintf(currentPath, "%s/current.json", tsdb.path);

  printf("currentPath: %s\n", currentPath);

  STFile tf;
  if (loadTFile(currentPath, &tf) != 0) {
    return -1;
  }

  SDataFileReader *pFileReader = initFileReader(&tsdb, &tf);
  if (!pFileReader) {
    return -1;
  }

  const TBrinBlkArray *brinBlkArray;
  readBrinBlk(pFileReader, &brinBlkArray);

  for (int i = 0; i < brinBlkArray->size; i++) {
    dumpRecord(pFileReader, brinBlkArray, i);
  }

  tsdbDataFileReaderClose(&pFileReader);
  return 0;
}
