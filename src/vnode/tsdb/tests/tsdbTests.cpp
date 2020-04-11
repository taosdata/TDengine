#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/time.h>

#include "dataformat.h"
#include "tsdbMain.h"

double getCurTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec * 1E-6;
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
      tdSchemaAppendCol(schema, TSDB_DATA_TYPE_TIMESTAMP, i, -1);
    } else {
      tdSchemaAppendCol(schema, TSDB_DATA_TYPE_INT, i, -1);
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

TEST(TsdbTest, DISABLED_createRepo) {
// TEST(TsdbTest, createRepo) {
  // STsdbCfg config;

  // // 1. Create a tsdb repository
  // tsdbSetDefaultCfg(&config);
  // tsdb_repo_t *pRepo = tsdbCreateRepo("/home/ubuntu/work/ttest/vnode0", &config, NULL);
  // ASSERT_NE(pRepo, nullptr);

  // // 2. Create a normal table
  // STableCfg tCfg;
  // ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_SUPER_TABLE, 987607499877672L, 0), -1);
  // ASSERT_EQ(tsdbInitTableCfg(&tCfg, TSDB_NORMAL_TABLE, 987607499877672L, 0), 0);

  // int       nCols = 5;
  // STSchema *schema = tdNewSchema(nCols);

  // for (int i = 0; i < nCols; i++) {
  //   if (i == 0) {
  //     tdSchemaAppendCol(schema, TSDB_DATA_TYPE_TIMESTAMP, i, -1);
  //   } else {
  //     tdSchemaAppendCol(schema, TSDB_DATA_TYPE_INT, i, -1);
  //   }
  // }

  // tsdbTableSetSchema(&tCfg, schema, true);

  // tsdbCreateTable(pRepo, &tCfg);

  // // // 3. Loop to write some simple data
  // int nRows = 1;
  // int rowsPerSubmit = 1;
  // int64_t start_time = 1584081000000;

  // SSubmitMsg *pMsg = (SSubmitMsg *)malloc(sizeof(SSubmitMsg) + sizeof(SSubmitBlk) + tdMaxRowBytesFromSchema(schema) * rowsPerSubmit);

  // double stime = getCurTime();

  // for (int k = 0; k < nRows/rowsPerSubmit; k++) {
  //   memset((void *)pMsg, 0, sizeof(SSubmitMsg));
  //   SSubmitBlk *pBlock = pMsg->blocks;
  //   pBlock->uid = 987607499877672L;
  //   pBlock->tid = 0;
  //   pBlock->sversion = 0;
  //   pBlock->len = 0;
  //   for (int i = 0; i < rowsPerSubmit; i++) {
  //     // start_time += 1000;
  //     start_time += 1000;
  //     SDataRow row = (SDataRow)(pBlock->data + pBlock->len);
  //     tdInitDataRow(row, schema);

  //     for (int j = 0; j < schemaNCols(schema); j++) {
  //       if (j == 0) {  // Just for timestamp
  //         tdAppendColVal(row, (void *)(&start_time), schemaColAt(schema, j));
  //       } else {  // For int
  //         int val = 10;
  //         tdAppendColVal(row, (void *)(&val), schemaColAt(schema, j));
  //       }
  //     }
  //     pBlock->len += dataRowLen(row);
  //   }
  //   pMsg->length = pMsg->length + sizeof(SSubmitBlk) + pBlock->len;
  //   pMsg->numOfBlocks = 1;

  //   pBlock->len = htonl(pBlock->len);
  //   pBlock->numOfRows = htonl(pBlock->numOfRows);
  //   pBlock->uid = htobe64(pBlock->uid);
  //   pBlock->tid = htonl(pBlock->tid);

  //   pBlock->sversion = htonl(pBlock->sversion);
  //   pBlock->padding = htonl(pBlock->padding);

  //   pMsg->length = htonl(pMsg->length);
  //   pMsg->numOfBlocks = htonl(pMsg->numOfBlocks);
  //   pMsg->compressed = htonl(pMsg->numOfBlocks);

  //   tsdbInsertData(pRepo, pMsg);
  // }

  // double etime = getCurTime();

  // void *ptr = malloc(150000);
  // free(ptr);

  // printf("Spent %f seconds to write %d records\n", etime - stime, nRows);

  // tsdbCloseRepo(pRepo);

}

// TEST(TsdbTest, DISABLED_openRepo) {
TEST(TsdbTest, openRepo) {
  tsdb_repo_t *repo = tsdbOpenRepo("/home/ubuntu/work/build/test/data/vnode/vnode2/tsdb", NULL);
  ASSERT_NE(repo, nullptr);

  STsdbRepo *pRepo = (STsdbRepo *)repo;

  SFileGroup *pGroup = tsdbSearchFGroup(pRepo->tsdbFileH, 1655);

  for (int type = TSDB_FILE_TYPE_HEAD; type < TSDB_FILE_TYPE_MAX; type++) {
    tsdbOpenFile(&pGroup->files[type], O_RDONLY);
  }

  SCompIdx *pIdx = (SCompIdx *)calloc(pRepo->config.maxTables, sizeof(SCompIdx));
  tsdbLoadCompIdx(pGroup, (void *)pIdx, pRepo->config.maxTables);

  SCompInfo *pCompInfo = (SCompInfo *)malloc(sizeof(SCompInfo) + pIdx[1].len);

  tsdbLoadCompBlocks(pGroup, &pIdx[1], (void *)pCompInfo);

  int blockIdx = 0;
  SCompBlock *pBlock = &(pCompInfo->blocks[blockIdx]);

  SCompData *pCompData = (SCompData *)malloc(sizeof(SCompData) + sizeof(SCompCol) * pBlock->numOfCols);

  tsdbLoadCompCols(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock, (void *)pCompData);

  STable *pTable = tsdbGetTableByUid(pRepo->tsdbMeta, pCompData->uid);
  SDataCols *pDataCols = tdNewDataCols(tdMaxRowBytesFromSchema(tsdbGetTableSchema(pRepo->tsdbMeta, pTable)), 5, 10);
  tdInitDataCols(pDataCols, tsdbGetTableSchema(pRepo->tsdbMeta, pTable));

  tsdbLoadDataBlock(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock, 1, pDataCols, pCompData);

  tdResetDataCols(pDataCols);

  tsdbLoadDataBlock(&pGroup->files[TSDB_FILE_TYPE_DATA], pBlock + 1, 1, pDataCols, pCompData);


  int k = 0;

}

TEST(TsdbTest, DISABLED_createFileGroup) {
  SFileGroup fGroup;

  // ASSERT_EQ(tsdbCreateFileGroup("/home/ubuntu/work/ttest/vnode0/data", 1820, &fGroup, 1000), 0);

  int k = 0;
}