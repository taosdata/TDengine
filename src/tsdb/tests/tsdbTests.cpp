#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/time.h>

#include "tdataformat.h"
#include "tsdbMain.h"
#include "tskiplist.h"

static double getCurTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec * 1E-6;
}

typedef struct {
  TsdbRepoT *pRepo;
  bool       isAscend;
  int        tid;
  int64_t    uid;
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
    SSubmitBlk *pBlock = pMsg->blocks;
    pBlock->uid = pInfo->uid;
    pBlock->tid = pInfo->tid;
    pBlock->sversion = pInfo->sversion;
    pBlock->len = 0;
    for (int i = 0; i < pInfo->rowsPerSubmit; i++) {
      // start_time += 1000;
      if (pInfo->isAscend) {
        start_time += pInfo->interval;
      } else {
        start_time -= pInfo->interval;
      }
      SDataRow row = (SDataRow)(pBlock->data + pBlock->len);
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
      pBlock->len += dataRowLen(row);
    }
    pMsg->length = pMsg->length + sizeof(SSubmitBlk) + pBlock->len;
    pMsg->numOfBlocks = 1;

    pBlock->len = htonl(pBlock->len);
    pBlock->numOfRows = htonl(pBlock->numOfRows);
    pBlock->uid = htobe64(pBlock->uid);
    pBlock->tid = htonl(pBlock->tid);

    pBlock->sversion = htonl(pBlock->sversion);
    pBlock->padding = htonl(pBlock->padding);

    pMsg->length = htonl(pMsg->length);
    pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);
    pMsg->compressed = htonl(pMsg->numOfBlocks);

    if (tsdbInsertData(pInfo->pRepo, pMsg) < 0) {
      tfree(pMsg);
      return -1;
    }
  }

  double etime = getCurTime();

  printf("Spent %f seconds to write %d records\n", etime - stime, pInfo->totalRows);
  tfree(pMsg);
  return 0;
}

TEST(TsdbTest, DISABLED_tableEncodeDecode) {
// TEST(TsdbTest, tableEncodeDecode) {
  STable *pTable = (STable *)malloc(sizeof(STable));

  pTable->type = TSDB_NORMAL_TABLE;
  pTable->tableId.uid = 987607499877672L;
  pTable->tableId.tid = 0;
  pTable->superUid = -1;
  pTable->sversion = 0;
  pTable->tagSchema = NULL;
  pTable->tagVal = NULL;
  int nCols = 5;
  STSchema *schema = tdNewSchema(nCols);

  for (int i = 0; i < nCols; i++) {
    if (i == 0) {
      tdSchemaAddCol(schema, TSDB_DATA_TYPE_TIMESTAMP, i, -1);
    } else {
      tdSchemaAddCol(schema, TSDB_DATA_TYPE_INT, i, -1);
    }
  }

  pTable->schema = schema;

  int bufLen = 0;
  void *buf = tsdbEncodeTable(pTable, &bufLen);

  STable *tTable = tsdbDecodeTable(buf, bufLen);

  ASSERT_EQ(pTable->type, tTable->type);
  ASSERT_EQ(pTable->tableId.uid, tTable->tableId.uid);
  ASSERT_EQ(pTable->tableId.tid, tTable->tableId.tid);
  ASSERT_EQ(pTable->superUid, tTable->superUid);
  ASSERT_EQ(pTable->sversion, tTable->sversion);
  ASSERT_EQ(memcmp(pTable->schema, tTable->schema, sizeof(STSchema) + sizeof(STColumn) * nCols), 0);
}

// TEST(TsdbTest, DISABLED_createRepo) {
TEST(TsdbTest, createRepo) {
  STsdbCfg config;
  STsdbRepo *repo;

  // 1. Create a tsdb repository
  tsdbSetDefaultCfg(&config);
  ASSERT_EQ(tsdbCreateRepo("/home/ubuntu/work/ttest/vnode0", &config, NULL), 0);

  TsdbRepoT *pRepo = tsdbOpenRepo("/home/ubuntu/work/ttest/vnode0", NULL);
  ASSERT_NE(pRepo, nullptr);

  // 2. Create a normal table
  STableCfg tCfg;
  ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_SUPER_TABLE, 987607499877672L, 0), -1);
  ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_NORMAL_TABLE, 987607499877672L, 0), 0);
  tsdbTableSetName(&tCfg, "test", false);

  int       nCols = 5;
  STSchema *schema = tdNewSchema(nCols);

  for (int i = 0; i < nCols; i++) {
    if (i == 0) {
      tdSchemaAddCol(schema, TSDB_DATA_TYPE_TIMESTAMP, i, -1);
    } else {
      tdSchemaAddCol(schema, TSDB_DATA_TYPE_INT, i, -1);
    }
  }

  tsdbTableSetSchema(&tCfg, schema, true);

  tsdbCreateTable(pRepo, &tCfg);

  // Insert Some Data
  SInsertInfo iInfo = {
    .pRepo = pRepo,
    // .isAscend = true,
    .isAscend = false,
    .tid = tCfg.tableId.tid,
    .uid = tCfg.tableId.uid,
    .sversion = tCfg.sversion,
    .startTime = 1584081000000,
    .interval = 1000,
    .totalRows = 10000000,
    .rowsPerSubmit = 1,
    .pSchema = schema
  };

  ASSERT_EQ(insertData(&iInfo), 0);

  // Close the repository
  tsdbCloseRepo(pRepo);

  // Open the repository again
  pRepo = tsdbOpenRepo("/home/ubuntu/work/ttest/vnode0", NULL);
  repo = (STsdbRepo *)pRepo;
  ASSERT_NE(pRepo, nullptr);

  // // Insert more data
  // iInfo.startTime = iInfo.startTime + iInfo.interval * iInfo.totalRows;
  // iInfo.totalRows = 10;
  // iInfo.pRepo = pRepo;
  // ASSERT_EQ(insertData(&iInfo), 0);

  // // Close the repository
  // tsdbCloseRepo(pRepo);

  // // Open the repository again
  // pRepo = tsdbOpenRepo("/home/ubuntu/work/ttest/vnode0", NULL);
  // repo = (STsdbRepo *)pRepo;
  // ASSERT_NE(pRepo, nullptr);

  // // Read from file
  // SRWHelper rhelper;
  // tsdbInitReadHelper(&rhelper, repo);

  // SFileGroup *pFGroup = tsdbSearchFGroup(repo->tsdbFileH, 1833);
  // ASSERT_NE(pFGroup, nullptr);
  // ASSERT_GE(tsdbSetAndOpenHelperFile(&rhelper, pFGroup), 0);

  // STable *pTable = tsdbGetTableByUid(repo->tsdbMeta, tCfg.tableId.uid);
  // ASSERT_NE(pTable, nullptr);
  // tsdbSetHelperTable(&rhelper, pTable, repo);

  // ASSERT_EQ(tsdbLoadCompInfo(&rhelper, NULL), 0);
  // ASSERT_EQ(tsdbLoadBlockData(&rhelper, blockAtIdx(&rhelper, 0), NULL), 0);

  int k = 0;
}

TEST(TsdbTest, DISABLED_openRepo) {
// TEST(TsdbTest, openRepo) {
  // tsdb_repo_t *repo = tsdbOpenRepo("/home/ubuntu/work/build/test/data/vnode/vnode2/tsdb", NULL);
  // ASSERT_NE(repo, nullptr);

  // STsdbRepo *pRepo = (STsdbRepo *)repo;

  // SFileGroup *pGroup = tsdbSearchFGroup(pRepo->tsdbFileH, 1655);

//   for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
//     tsdbOpenFile(&pGroup->files[type], O_RDONLY);
//   }

//   SCompIdx *pIdx = (SCompIdx *)calloc(pRepo->config.maxTables, sizeof(SCompIdx));
//   tsdbLoadCompIdx(pGroup, (void *)pIdx, pRepo->config.maxTables);

//   SCompInfo *pCompInfo = (SCompInfo *)malloc(sizeof(SCompInfo) + pIdx[1].len);

  // tsdbLoadCompBlocks(pGroup, &pIdx[1], (void *)pCompInfo);

//   int blockIdx = 0;
//   SCompBlock *pBlock = &(pCompInfo->blocks[blockIdx]);

//   SCompData *pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pBlock->numOfCols);

//   tsdbLoadCompCols(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock, (void *)pCompData);

  // STable *pTable = tsdbGetTableByUid(pRepo->tsdbMeta, pCompData->uid);
  // SDataCols *pDataCols = tdNewDataCols(tdMaxRowBytesFromSchema(tsdbGetTableSchema(pRepo->tsdbMeta, pTable)), 5);
  // tdInitDataCols(pDataCols, tsdbGetTableSchema(pRepo->tsdbMeta, pTable));

//   tsdbLoadDataBlock(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock, 1, pDataCols, pCompData);

  // tdResetDataCols(pDataCols);

  // tsdbLoadDataBlock(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock + 1, 1, pDataCols, pCompData);


//   int k = 0;

}

TEST(TsdbTest, DISABLED_createFileGroup) {
  SFileGroup fGroup;

  // ASSERT_EQ(tsdbCreateFileGroup("/home/ubuntu/work/ttest/vnode0/data", 1820, &fGroup, 1000), 0);

  int k = 0;
}

static char *getTKey(const void *data) {
  return (char *)data;
}

static void insertSkipList(bool isAscend) {
  TSKEY start_time = 1587393453000;
  TSKEY interval = 1000;

  SSkipList *pList = tSkipListCreate(5, TSDB_DATA_TYPE_TIMESTAMP, sizeof(TSKEY), 0, 0, 1, getTKey);
  ASSERT_NE(pList, nullptr);

  for (size_t i = 0; i < 20000000; i++)
  {
    TSKEY time = isAscend ? (start_time + i * interval) : (start_time - i * interval);
    int32_t level = 0;
    int32_t headSize = 0;

    tSkipListNewNodeInfo(pList, &level, &headSize);
    SSkipListNode *pNode = (SSkipListNode *)malloc(headSize + sizeof(TSKEY));
    ASSERT_NE(pNode, nullptr);
    pNode->level = level;
    *(TSKEY *)((char *)pNode + headSize) = time;
    tSkipListPut(pList, pNode);
  }

  tSkipListDestroy(pList);
}

TEST(TsdbTest, DISABLED_testSkipList) {
// TEST(TsdbTest, testSkipList) {
  double stime = getCurTime();
  insertSkipList(true);
  double etime = getCurTime();

  printf("Time used to insert 100000000 records takes %f seconds\n", etime-stime);

  stime = getCurTime();
  insertSkipList(false);
  etime = getCurTime();

  printf("Time used to insert 100000000 records takes %f seconds\n", etime-stime);
}