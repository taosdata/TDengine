#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/time.h>

#include "tsdb.h"
#include "tsdbMain.h"

static double getCurTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec * 1E-6;
}

typedef struct {
  STsdbRepo *pRepo;
  bool       isAscend;
  int        tid;
  uint64_t   uid;
  int        sversion;
  TSKEY      startTime;
  TSKEY      interval;
  int        totalRows;
  int        rowsPerSubmit;
  STSchema * pSchema;
} SInsertInfo;

static int insertData(SInsertInfo *pInfo) {
  SSubmitMsg *pMsg =
      (SSubmitMsg *)malloc(sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + dataRowMaxBytesFromSchema(pInfo->pSchema) * pInfo->rowsPerSubmit);
  if (pMsg == NULL) return -1;
  TSKEY start_time = pInfo->startTime;

  // Loop to write data
  double stime = getCurTime();

  for (int k = 0; k < pInfo->totalRows/pInfo->rowsPerSubmit; k++) {
    memset((void *)pMsg, 0, sizeof(SSubmitMsg));
    SSubmitBlk *pBlock = (SSubmitBlk *)pMsg->blocks;
    pBlock->uid = pInfo->uid;
    pBlock->tid = pInfo->tid;
    pBlock->sversion = pInfo->sversion;
    pBlock->dataLen = 0;
    pBlock->schemaLen = 0;
    pBlock->numOfRows = 0;
    for (int i = 0; i < pInfo->rowsPerSubmit; i++) {
      // start_time += 1000;
      if (pInfo->isAscend) {
        start_time += pInfo->interval;
      } else {
        start_time -= pInfo->interval;
      }
      SDataRow row = (SDataRow)(pBlock->data + pBlock->dataLen);
      tdInitDataRow(row, pInfo->pSchema);

      for (int j = 0; j < schemaNCols(pInfo->pSchema); j++) {
        STColumn *pTCol = schemaColAt(pInfo->pSchema, j);
        if (j == 0) {  // Just for timestamp
          tdAppendColVal(row, (void *)(&start_time), pTCol->type, pTCol->bytes, pTCol->offset);
        } else {  // For int
          int val = 10;
          tdAppendColVal(row, (void *)(&val), pTCol->type, pTCol->bytes, pTCol->offset);
        }
      }
      pBlock->dataLen += dataRowLen(row);
      pBlock->numOfRows++;
    }
    pMsg->length = sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + pBlock->dataLen;
    pMsg->numOfBlocks = 1;

    pBlock->dataLen = htonl(pBlock->dataLen);
    pBlock->numOfRows = htonl(pBlock->numOfRows);
    pBlock->schemaLen = htonl(pBlock->schemaLen);
    pBlock->uid = htobe64(pBlock->uid);
    pBlock->tid = htonl(pBlock->tid);

    pBlock->sversion = htonl(pBlock->sversion);
    pBlock->padding = htonl(pBlock->padding);

    pMsg->length = htonl(pMsg->length);
    pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);

    if (tsdbInsertData(pInfo->pRepo, pMsg, NULL) < 0) {
      tfree(pMsg);
      return -1;
    }
  }

  double etime = getCurTime();

  printf("Spent %f seconds to write %d records\n", etime - stime, pInfo->totalRows);
  tfree(pMsg);
  return 0;
}

static void tsdbSetCfg(STsdbCfg *pCfg, int32_t tsdbId, int32_t cacheBlockSize, int32_t totalBlocks, int32_t maxTables,
                       int32_t daysPerFile, int32_t keep, int32_t minRows, int32_t maxRows, int8_t precision,
                       int8_t compression) {
  pCfg->tsdbId = tsdbId;
  pCfg->cacheBlockSize = cacheBlockSize;
  pCfg->totalBlocks = totalBlocks;
  // pCfg->maxTables = maxTables;
  pCfg->daysPerFile = daysPerFile;
  pCfg->keep = keep;
  pCfg->minRowsPerFileBlock = minRows;
  pCfg->maxRowsPerFileBlock = maxRows;
  pCfg->precision = precision;
  pCfg->compression = compression;
}

static void tsdbSetTableCfg(STableCfg *pCfg) {
  STSchemaBuilder schemaBuilder = {0};

  pCfg->type = TSDB_NORMAL_TABLE;
  pCfg->superUid = TSDB_INVALID_SUPER_TABLE_ID;
  pCfg->tableId.tid = 1;
  pCfg->tableId.uid = 5849583783847394;
  tdInitTSchemaBuilder(&schemaBuilder, 0);

  int colId = 0;
  for (int i = 0; i < 5; i++) {
    tdAddColToSchema(&schemaBuilder, (colId == 0) ? TSDB_DATA_TYPE_TIMESTAMP : TSDB_DATA_TYPE_INT, colId, 0);
    colId++;
  }

  pCfg->schema = tdGetSchemaFromBuilder(&schemaBuilder);
  pCfg->name = strdup("t1");

  tdDestroyTSchemaBuilder(&schemaBuilder);
}

TEST(TsdbTest, testInsertSpeed) {
  int         vnode = 1;
  int         ret = 0;
  STsdbCfg    tsdbCfg;
  STableCfg   tableCfg;
  std::string testDir = "./test";
  char *      rootDir = strdup((testDir + "/vnode" + std::to_string(vnode)).c_str());

  tsdbDebugFlag = 131; //NOTE: you must set the flag

  taosRemoveDir(rootDir);

  // Create and open repository
  tsdbSetCfg(&tsdbCfg, 1, 16, 4, -1, -1, -1, -1, -1, -1, -1);
  tsdbCreateRepo(rootDir, &tsdbCfg);
  STsdbRepo *repo = tsdbOpenRepo(rootDir, NULL);
  ASSERT_NE(repo, nullptr);

  // Create table
  tsdbSetTableCfg(&tableCfg);
  tsdbCreateTable(repo, &tableCfg);

  // Insert data
  SInsertInfo iInfo = {repo, true, 1, 5849583783847394, 0, 1590000000000, 10, 10000000, 100, tableCfg.schema};

  insertData(&iInfo);

  tsdbCloseRepo(repo, 1);
}

static char *getTKey(const void *data) {
  return (char *)data;
}