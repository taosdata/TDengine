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

#include <gtest/gtest.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "executor.h"
#include "executorInt.h"
#include "function.h"
#include "operator.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tvariant.h"

namespace {

enum {
  data_rand = 0x1,
  data_asc = 0x2,
  data_desc = 0x3,
};

TEST(testCase, windowFunctionTest) {
  int64_t tsCol[100000];
  int32_t rows = 100000;
  for (int32_t i = 0; i < rows; i++) {
    tsCol[i] = 1648791213000 + i;
  }
  int32_t ekeyNum = 50000;
  int32_t pos = 40000;
  int64_t ekey = tsCol[ekeyNum];
  int32_t num = getForwardStepsInBlock(rows, binarySearchForKey, ekey, pos, TSDB_ORDER_ASC, tsCol);
  ASSERT_EQ(num, ekeyNum - pos + 1);
}

typedef struct SDummyInputInfo {
  int32_t      totalPages;  // numOfPages
  int32_t      current;
  int32_t      startVal;
  int32_t      type;
  int32_t      numOfRowsPerPage;
  int32_t      numOfCols;  // number of columns
  int64_t      tsStart;
  SSDataBlock* pBlock;
} SDummyInputInfo;

SSDataBlock* getDummyBlock(SOperatorInfo* pOperator) {
  SDummyInputInfo* pInfo = static_cast<SDummyInputInfo*>(pOperator->info);
  if (pInfo->current >= pInfo->totalPages) {
    return NULL;
  }

  if (pInfo->pBlock == NULL) {
    pInfo->pBlock = createDataBlock();

    SColumnInfoData colInfo = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1);
    blockDataAppendColInfo(pInfo->pBlock, &colInfo);
    blockDataEnsureCapacity(pInfo->pBlock, pInfo->numOfRowsPerPage);

    //    SColumnInfoData colInfo1 = {0};
    //    colInfo1.info.type = TSDB_DATA_TYPE_BINARY;
    //    colInfo1.info.bytes = 40;
    //    colInfo1.info.colId = 2;
    //
    //    colInfo1.varmeta.allocLen = 0;//numOfRows * sizeof(int32_t);
    //    colInfo1.varmeta.length = 0;
    //    colInfo1.varmeta.offset = static_cast<int32_t*>(taosMemoryCalloc(1, numOfRows * sizeof(int32_t)));
    //
    //    taosArrayPush(pInfo->pBlock->pDataBlock, &colInfo1);
  } else {
    blockDataCleanup(pInfo->pBlock);
  }

  SSDataBlock* pBlock = pInfo->pBlock;

  char    buf[128] = {0};
  char    b1[128] = {0};
  int32_t v = 0;
  for (int32_t i = 0; i < pInfo->numOfRowsPerPage; ++i) {
    SColumnInfoData* pColInfo = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 0));

    if (pInfo->type == data_desc) {
      v = (--pInfo->startVal);
    } else if (pInfo->type == data_asc) {
      v = ++pInfo->startVal;
    } else if (pInfo->type == data_rand) {
      v = taosRand();
    }

    colDataSetVal(pColInfo, i, reinterpret_cast<const char*>(&v), false);

    //    sprintf(buf, "this is %d row", i);
    //    STR_TO_VARSTR(b1, buf);
    //
    //    SColumnInfoData* pColInfo2 = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 1));
    //    colDataSetVal(pColInfo2, i, b1, false);
  }

  pBlock->info.rows = pInfo->numOfRowsPerPage;

  pInfo->current += 1;
  return pBlock;
}

SSDataBlock* get2ColsDummyBlock(SOperatorInfo* pOperator) {
  SDummyInputInfo* pInfo = static_cast<SDummyInputInfo*>(pOperator->info);
  if (pInfo->current >= pInfo->totalPages) {
    return NULL;
  }

  if (pInfo->pBlock == NULL) {
    pInfo->pBlock = createDataBlock();

    SColumnInfoData colInfo = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
    blockDataAppendColInfo(pInfo->pBlock, &colInfo);

    SColumnInfoData colInfo1 = createColumnInfoData(TSDB_DATA_TYPE_INT, 4, 2);
    blockDataAppendColInfo(pInfo->pBlock, &colInfo1);

    blockDataEnsureCapacity(pInfo->pBlock, pInfo->numOfRowsPerPage);
  } else {
    blockDataCleanup(pInfo->pBlock);
  }

  SSDataBlock* pBlock = pInfo->pBlock;

  char    buf[128] = {0};
  char    b1[128] = {0};
  int64_t ts = 0;
  int32_t v = 0;
  for (int32_t i = 0; i < pInfo->numOfRowsPerPage; ++i) {
    SColumnInfoData* pColInfo = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 0));

    ts = (++pInfo->tsStart);
    colDataSetVal(pColInfo, i, reinterpret_cast<const char*>(&ts), false);

    SColumnInfoData* pColInfo1 = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 1));
    if (pInfo->type == data_desc) {
      v = (--pInfo->startVal);
    } else if (pInfo->type == data_asc) {
      v = ++pInfo->startVal;
    } else if (pInfo->type == data_rand) {
      v = taosRand();
    }

    colDataSetVal(pColInfo1, i, reinterpret_cast<const char*>(&v), false);

    //    sprintf(buf, "this is %d row", i);
    //    STR_TO_VARSTR(b1, buf);
    //
    //    SColumnInfoData* pColInfo2 = static_cast<SColumnInfoData*>(TARRAY_GET_ELEM(pBlock->pDataBlock, 1));
    //    colDataSetVal(pColInfo2, i, b1, false);
  }

  pBlock->info.rows = pInfo->numOfRowsPerPage;

  pInfo->current += 1;

  pBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pBlock, 0);
  return pBlock;
}

SOperatorInfo* createDummyOperator(int32_t startVal, int32_t numOfBlocks, int32_t rowsPerPage, int32_t type,
                                   int32_t numOfCols) {
  SOperatorInfo* pOperator = static_cast<SOperatorInfo*>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  pOperator->name = "dummyInputOpertor4Test";

  if (numOfCols == 1) {
    pOperator->fpSet.getNextFn = getDummyBlock;
  } else {
    pOperator->fpSet.getNextFn = get2ColsDummyBlock;
  }

  SDummyInputInfo* pInfo = (SDummyInputInfo*)taosMemoryCalloc(1, sizeof(SDummyInputInfo));
  pInfo->totalPages = numOfBlocks;
  pInfo->startVal = startVal;
  pInfo->numOfRowsPerPage = rowsPerPage;
  pInfo->type = type;
  pInfo->tsStart = 1620000000000;

  pOperator->info = pInfo;
  return pOperator;
}
}  // namespace
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, build_executor_tree_Test) {
  const char* msg =
      "{\n"
      "    \"NodeType\": \"48\",\n"
      "    \"Name\": \"PhysiSubplan\",\n"
      "    \"PhysiSubplan\": {\n"
      "        \"Id\": {\n"
      "            \"QueryId\": \"0\",\n"
      "            \"TemplateId\": \"0\",\n"
      "            \"SubplanId\": \"0\"\n"
      "        },\n"
      "        \"SubplanType\": \"0\",\n"
      "        \"MsgType\": \"515\",\n"
      "        \"Level\": \"0\",\n"
      "        \"NodeAddr\": {\n"
      "            \"Id\": \"1\",\n"
      "            \"InUse\": \"0\",\n"
      "            \"NumOfEps\": \"1\",\n"
      "            \"Eps\": [\n"
      "                {\n"
      "                    \"Fqdn\": \"node1\",\n"
      "                    \"Port\": \"6030\"\n"
      "                }\n"
      "            ]\n"
      "        },\n"
      "        \"RootNode\": {\n"
      "            \"NodeType\": \"41\",\n"
      "            \"Name\": \"PhysiProject\",\n"
      "            \"PhysiProject\": {\n"
      "                \"OutputDataBlockDesc\": {\n"
      "                    \"NodeType\": \"19\",\n"
      "                    \"Name\": \"TupleDesc\",\n"
      "                    \"TupleDesc\": {\n"
      "                        \"DataBlockId\": \"1\",\n"
      "                        \"Slots\": [\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"0\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"9\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"1\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"4\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"4\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"2\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"8\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"20\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"3\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"5\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"4\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"5\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            }\n"
      "                        ]\n"
      "                    }\n"
      "                },\n"
      "                \"Children\": [\n"
      "                    {\n"
      "                        \"NodeType\": \"38\",\n"
      "                        \"Name\": \"PhysiTableScan\",\n"
      "                        \"PhysiTableScan\": {\n"
      "                            \"OutputDataBlockDesc\": {\n"
      "                                \"NodeType\": \"19\",\n"
      "                                \"Name\": \"TupleDesc\",\n"
      "                                \"TupleDesc\": {\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"Slots\": [\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"0\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"9\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        },\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"1\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"4\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"4\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        },\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"2\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"8\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"20\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        },\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"3\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"5\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        },\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"4\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"7\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        },\n"
      "                                        {\n"
      "                                            \"NodeType\": \"20\",\n"
      "                                            \"Name\": \"SlotDesc\",\n"
      "                                            \"SlotDesc\": {\n"
      "                                                \"SlotId\": \"5\",\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"7\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"Reserve\": false,\n"
      "                                                \"Output\": true\n"
      "                                            }\n"
      "                                        }\n"
      "                                    ]\n"
      "                                }\n"
      "                            },\n"
      "                            \"ScanCols\": [\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"0\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"9\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"ts\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"1\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"ts\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                },\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"1\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"4\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"4\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"c1\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"2\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"c1\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                },\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"2\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"8\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"20\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"c2\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"3\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"c2\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                },\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"3\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"5\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"c3\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"4\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"c3\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                },\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"4\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"7\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"c4\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"5\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"c4\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                },\n"
      "                                {\n"
      "                                    \"NodeType\": \"18\",\n"
      "                                    \"Name\": \"Target\",\n"
      "                                    \"Target\": {\n"
      "                                        \"DataBlockId\": \"0\",\n"
      "                                        \"SlotId\": \"5\",\n"
      "                                        \"Expr\": {\n"
      "                                            \"NodeType\": \"1\",\n"
      "                                            \"Name\": \"Column\",\n"
      "                                            \"Column\": {\n"
      "                                                \"DataType\": {\n"
      "                                                    \"Type\": \"7\",\n"
      "                                                    \"Precision\": \"0\",\n"
      "                                                    \"Scale\": \"0\",\n"
      "                                                    \"Bytes\": \"8\"\n"
      "                                                },\n"
      "                                                \"AliasName\": \"c5\",\n"
      "                                                \"TableId\": \"0\",\n"
      "                                                \"ColId\": \"6\",\n"
      "                                                \"ColType\": \"1\",\n"
      "                                                \"DbName\": \"test\",\n"
      "                                                \"TableName\": \"t1\",\n"
      "                                                \"TableAlias\": \"t1\",\n"
      "                                                \"ColName\": \"c5\",\n"
      "                                                \"DataBlockId\": \"0\",\n"
      "                                                \"SlotId\": \"0\"\n"
      "                                            }\n"
      "                                        }\n"
      "                                    }\n"
      "                                }\n"
      "                            ],\n"
      "                            \"TableId\": \"1\",\n"
      "                            \"TableType\": \"3\",\n"
      "                            \"ScanOrder\": \"1\",\n"
      "                            \"ScanCount\": \"1\",\n"
      "                            \"ReverseScanCount\": \"0\",\n"
      "                            \"ScanFlag\": \"0\",\n"
      "                            \"StartKey\": \"-9223372036854775808\",\n"
      "                            \"EndKey\": \"9223372036854775807\"\n"
      "                        }\n"
      "                    }\n"
      "                ],\n"
      "                \"Projections\": [\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"0\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"9\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"ts\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"1\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"ts\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"0\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    },\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"1\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"4\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"4\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"c1\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"2\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"c1\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"1\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    },\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"2\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"8\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"20\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"c2\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"3\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"c2\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"2\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    },\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"3\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"5\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"c3\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"4\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"c3\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"3\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    },\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"4\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"c4\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"5\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"c4\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"4\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    },\n"
      "                    {\n"
      "                        \"NodeType\": \"18\",\n"
      "                        \"Name\": \"Target\",\n"
      "                        \"Target\": {\n"
      "                            \"DataBlockId\": \"1\",\n"
      "                            \"SlotId\": \"5\",\n"
      "                            \"Expr\": {\n"
      "                                \"NodeType\": \"1\",\n"
      "                                \"Name\": \"Column\",\n"
      "                                \"Column\": {\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"AliasName\": \"c5\",\n"
      "                                    \"TableId\": \"0\",\n"
      "                                    \"ColId\": \"6\",\n"
      "                                    \"ColType\": \"1\",\n"
      "                                    \"DbName\": \"test\",\n"
      "                                    \"TableName\": \"t1\",\n"
      "                                    \"TableAlias\": \"t1\",\n"
      "                                    \"ColName\": \"c5\",\n"
      "                                    \"DataBlockId\": \"0\",\n"
      "                                    \"SlotId\": \"5\"\n"
      "                                }\n"
      "                            }\n"
      "                        }\n"
      "                    }\n"
      "                ]\n"
      "            }\n"
      "        },\n"
      "        \"DataSink\": {\n"
      "            \"NodeType\": \"46\",\n"
      "            \"Name\": \"PhysiDispatch\",\n"
      "            \"PhysiDispatch\": {\n"
      "                \"InputDataBlockDesc\": {\n"
      "                    \"NodeType\": \"19\",\n"
      "                    \"Name\": \"TupleDesc\",\n"
      "                    \"TupleDesc\": {\n"
      "                        \"DataBlockId\": \"1\",\n"
      "                        \"Slots\": [\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"0\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"9\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"1\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"4\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"4\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"2\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"8\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"20\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"3\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"5\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"4\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            },\n"
      "                            {\n"
      "                                \"NodeType\": \"20\",\n"
      "                                \"Name\": \"SlotDesc\",\n"
      "                                \"SlotDesc\": {\n"
      "                                    \"SlotId\": \"5\",\n"
      "                                    \"DataType\": {\n"
      "                                        \"Type\": \"7\",\n"
      "                                        \"Precision\": \"0\",\n"
      "                                        \"Scale\": \"0\",\n"
      "                                        \"Bytes\": \"8\"\n"
      "                                    },\n"
      "                                    \"Reserve\": false,\n"
      "                                    \"Output\": false\n"
      "                                }\n"
      "                            }\n"
      "                        ]\n"
      "                    }\n"
      "                }\n"
      "            }\n"
      "        }\n"
      "    }\n"
      "}";

  SExecTaskInfo* pTaskInfo = nullptr;
  DataSinkHandle sinkHandle = nullptr;
  SReadHandle    handle = {reinterpret_cast<void*>(0x1), reinterpret_cast<void*>(0x1), NULL};

  struct SSubplan* plan = NULL;
  int32_t          code = qStringToSubplan(msg, &plan);
  ASSERT_EQ(code, 0);

  code = qCreateExecTask(&handle, 2, 1, plan, (void**)&pTaskInfo, &sinkHandle, NULL, OPTR_EXEC_MODEL_BATCH);
  ASSERT_EQ(code, 0);
}
#if 0

TEST(testCase, inMem_sort_Test) {
  SArray* pOrderVal = taosArrayInit(4, sizeof(SOrder));
  SOrder o = {.order = TSDB_ORDER_ASC};
  o.col.info.colId = 1;
  o.col.info.type = TSDB_DATA_TYPE_INT;
  taosArrayPush(pOrderVal, &o);

  SArray* pExprInfo = taosArrayInit(4, sizeof(SExprInfo));
  SExprInfo *exp = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp->base.resSchema = createSchema(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1, "res");
  taosArrayPush(pExprInfo, &exp);

  SExprInfo *exp1 = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp1->base.resSchema = createSchema(TSDB_DATA_TYPE_BINARY, 40, 2, "res1");
  taosArrayPush(pExprInfo, &exp1);

  SOperatorInfo* pOperator = createSortOperatorInfo(createDummyOperator(10000, 5, 1000, data_asc, 1), pExprInfo, pOrderVal, NULL);

  bool newgroup = false;
  SSDataBlock* pRes = pOperator->getNextFn(pOperator, &newgroup);

  SColumnInfoData* pCol1 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 0));
  SColumnInfoData* pCol2 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 1));
  for(int32_t i = 0; i < pRes->info.rows; ++i) {
    char* p = colDataGetData(pCol2, i);
    printf("%d: %d, %s\n", i, ((int32_t*)pCol1->pData)[i], (char*)varDataVal(p));
  }
}

#endif

typedef struct su {
  int32_t v;
  char*   c;
} su;

int32_t cmp(const void* p1, const void* p2) {
  su* v1 = (su*)p1;
  su* v2 = (su*)p2;

  int32_t x1 = *(int32_t*)v1->c;
  int32_t x2 = *(int32_t*)v2->c;
  if (x1 == x2) {
    return 0;
  } else {
    return x1 < x2 ? -1 : 1;
  }
}

#if 0
TEST(testCase, external_sort_Test) {
#if 0
  su* v = static_cast<su*>(taosMemoryCalloc(1000000, sizeof(su)));
  for(int32_t i = 0; i < 1000000; ++i) {
    v[i].v = taosRand();
    v[i].c = static_cast<char*>(taosMemoryMalloc(4));
    *(int32_t*) v[i].c = i;
  }

  qsort(v, 1000000, sizeof(su), cmp);
//  for(int32_t i = 0; i < 1000; ++i) {
//    printf("%d ", v[i]);
//  }
//  printf("\n");
  return;
#endif

  taosSeedRand(taosGetTimestampSec());

  SArray* pOrderVal = taosArrayInit(4, sizeof(SOrder));
  SOrder o = {0};
  o.order = TSDB_ORDER_ASC;
  o.col.info.colId = 1;
  o.col.info.type = TSDB_DATA_TYPE_INT;
  taosArrayPush(pOrderVal, &o);

  SArray* pExprInfo = taosArrayInit(4, sizeof(SExprInfo));
  SExprInfo *exp = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp->base.resSchema = createSchema(TSDB_DATA_TYPE_INT, sizeof(int32_t), 1, "res");
  taosArrayPush(pExprInfo, &exp);

  SExprInfo *exp1 = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp1->base.resSchema = createSchema(TSDB_DATA_TYPE_BINARY, 40, 2, "res1");
//  taosArrayPush(pExprInfo, &exp1);

  SOperatorInfo* pOperator = createSortOperatorInfo(createDummyOperator(10000, 1500, 1000, data_desc, 1), pExprInfo, pOrderVal, NULL);

  bool newgroup = false;
  SSDataBlock* pRes = NULL;

  int32_t total = 1;

  int64_t s1 = taosGetTimestampUs();
  int32_t t = 1;

  while(1) {
    int64_t s = taosGetTimestampUs();
    pRes = pOperator->getNextFn(pOperator, &newgroup);

    int64_t e = taosGetTimestampUs();
    if (t++ == 1) {
      printf("---------------elapsed:%" PRId64 "\n", e - s);
    }

    if (pRes == NULL) {
      break;
    }

    SColumnInfoData* pCol1 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 0));
//    SColumnInfoData* pCol2 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 1));
    for (int32_t i = 0; i < pRes->info.rows; ++i) {
//      char* p = colDataGetData(pCol2, i);
      printf("%d: %d\n", total++, ((int32_t*)pCol1->pData)[i]);
//      printf("%d: %d, %s\n", total++, ((int32_t*)pCol1->pData)[i], (char*)varDataVal(p));
    }
  }

  int64_t s2 = taosGetTimestampUs();
  printf("total:%" PRId64 "\n", s2 - s1);

  pOperator->closeFn(pOperator->info, 2);
  taosMemoryFreeClear(exp);
  taosMemoryFreeClear(exp1);
  taosArrayDestroy(pExprInfo);
  taosArrayDestroy(pOrderVal);
}

TEST(testCase, sorted_merge_Test) {
  taosSeedRand(taosGetTimestampSec());

  SArray* pOrderVal = taosArrayInit(4, sizeof(SOrder));
  SOrder o = {0};
  o.order = TSDB_ORDER_ASC;
  o.col.info.colId = 1;
  o.col.info.type = TSDB_DATA_TYPE_INT;
  taosArrayPush(pOrderVal, &o);

  SArray* pExprInfo = taosArrayInit(4, sizeof(SExprInfo));
  SExprInfo *exp = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp->base.resSchema = createSchema(TSDB_DATA_TYPE_BIGINT, sizeof(int64_t), 1, "count_result");
  exp->base.pColumns = static_cast<SColumn*>(taosMemoryCalloc(1, sizeof(SColumn)));
  exp->base.pColumns->flag = TSDB_COL_NORMAL;
  exp->base.pColumns->info = (SColumnInfo) {.colId = 1, .type = TSDB_DATA_TYPE_INT, .bytes = 4};
  exp->base.numOfCols = 1;

  taosArrayPush(pExprInfo, &exp);

  SExprInfo *exp1 = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp1->base.resSchema = createSchema(TSDB_DATA_TYPE_BINARY, 40, 2, "res1");
//  taosArrayPush(pExprInfo, &exp1);

  int32_t numOfSources = 10;
  SOperatorInfo** plist = (SOperatorInfo**) taosMemoryCalloc(numOfSources, sizeof(void*));
  for(int32_t i = 0; i < numOfSources; ++i) {
    plist[i] = createDummyOperator(1, 1, 1, data_asc, 1);
  }

  SOperatorInfo* pOperator = createSortedMergeOperatorInfo(plist, numOfSources, pExprInfo, pOrderVal, NULL, NULL);

  bool newgroup = false;
  SSDataBlock* pRes = NULL;

  int32_t total = 1;

  int64_t s1 = taosGetTimestampUs();
  int32_t t = 1;

  while(1) {
    int64_t s = taosGetTimestampUs();
    pRes = pOperator->getNextFn(pOperator, &newgroup);

    int64_t e = taosGetTimestampUs();
    if (t++ == 1) {
      printf("---------------elapsed:%" PRId64 "\n", e - s);
    }

    if (pRes == NULL) {
      break;
    }

    SColumnInfoData* pCol1 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 0));
//    SColumnInfoData* pCol2 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 1));
    for (int32_t i = 0; i < pRes->info.rows; ++i) {
//      char* p = colDataGetData(pCol2, i);
      printf("%d: %" PRId64 "\n", total++, ((int64_t*)pCol1->pData)[i]);
//      printf("%d: %d, %s\n", total++, ((int32_t*)pCol1->pData)[i], (char*)varDataVal(p));
    }
  }

  int64_t s2 = taosGetTimestampUs();
  printf("total:%" PRId64 "\n", s2 - s1);

  pOperator->closeFn(pOperator->info, 2);
  taosMemoryFreeClear(exp);
  taosMemoryFreeClear(exp1);
  taosArrayDestroy(pExprInfo);
  taosArrayDestroy(pOrderVal);
}

TEST(testCase, time_interval_Operator_Test) {
  taosSeedRand(taosGetTimestampSec());

  SArray* pOrderVal = taosArrayInit(4, sizeof(SOrder));
  SOrder o = {0};
  o.order = TSDB_ORDER_ASC;
  o.col.info.colId = 1;
  o.col.info.type = TSDB_DATA_TYPE_INT;
  taosArrayPush(pOrderVal, &o);

  SArray* pExprInfo = taosArrayInit(4, sizeof(SExprInfo));
  SExprInfo *exp = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp->base.resSchema = createSchema(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1, "ts");
  exp->base.pColumns = static_cast<SColumn*>(taosMemoryCalloc(1, sizeof(SColumn)));
  exp->base.pColumns->flag = TSDB_COL_NORMAL;
  exp->base.pColumns->info = (SColumnInfo) {.colId = 1, .type = TSDB_DATA_TYPE_TIMESTAMP, .bytes = 8};
  exp->base.numOfCols = 1;

  taosArrayPush(pExprInfo, &exp);

  SExprInfo *exp1 = static_cast<SExprInfo*>(taosMemoryCalloc(1, sizeof(SExprInfo)));
  exp1->base.resSchema = createSchema(TSDB_DATA_TYPE_BIGINT, 8, 2, "res1");
  exp1->base.pColumns = static_cast<SColumn*>(taosMemoryCalloc(1, sizeof(SColumn)));
  exp1->base.pColumns->flag = TSDB_COL_NORMAL;
  exp1->base.pColumns->info = (SColumnInfo) {.colId = 1, .type = TSDB_DATA_TYPE_INT, .bytes = 4};
  exp1->base.numOfCols = 1;

  taosArrayPush(pExprInfo, &exp1);

  SOperatorInfo* p = createDummyOperator(1, 1, 2000, data_asc, 2);

  SExecTaskInfo ti = {0};
  SInterval interval = {0};
  interval.sliding = interval.interval = 1000;
  interval.slidingUnit = interval.intervalUnit = 'a';

  SOperatorInfo* pOperator = createIntervalOperatorInfo(p, pExprInfo, &interval, &ti);

  bool newgroup = false;
  SSDataBlock* pRes = NULL;

  int32_t total = 1;

  int64_t s1 = taosGetTimestampUs();
  int32_t t = 1;

  while(1) {
    int64_t s = taosGetTimestampUs();
    pRes = pOperator->getNextFn(pOperator, &newgroup);

    int64_t e = taosGetTimestampUs();
    if (t++ == 1) {
      printf("---------------elapsed:%" PRId64 "\n", e - s);
    }

    if (pRes == NULL) {
      break;
    }

    SColumnInfoData* pCol1 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 0));
//    SColumnInfoData* pCol2 = static_cast<SColumnInfoData*>(taosArrayGet(pRes->pDataBlock, 1));
    for (int32_t i = 0; i < pRes->info.rows; ++i) {
//      char* p = colDataGetData(pCol2, i);
      printf("%d: %" PRId64 "\n", total++, ((int64_t*)pCol1->pData)[i]);
//      printf("%d: %d, %s\n", total++, ((int32_t*)pCol1->pData)[i], (char*)varDataVal(p));
    }
  }

  int64_t s2 = taosGetTimestampUs();
  printf("total:%" PRId64 "\n", s2 - s1);

  pOperator->closeFn(pOperator->info, 2);
  taosMemoryFreeClear(exp);
  taosMemoryFreeClear(exp1);
  taosArrayDestroy(pExprInfo);
  taosArrayDestroy(pOrderVal);
}
#endif

#pragma GCC diagnosti
