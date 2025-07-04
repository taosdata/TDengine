/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software
 * Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_BENCHDATA_H_
#define INC_BENCHDATA_H_

#include <bench.h>
/***** Global variables ******/
/***** Declare functions *****/
void rand_string(char *str, int size, bool chinese);
void rand_geometry(char *str, int fieldLen, int maxType);
int      geoCalcBufferSize(int fieldLen);
int      getGeoMaxType(int fieldLen);
int64_t  getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio, int disorderRange);
int generateRandData(SSuperTable *stbInfo, char *sampleDataBuf,
        int64_t bufLen,
        int lenOfOneRow, BArray * fields, int64_t loop,
        bool tag, BArray *childCols, int64_t loopBegin);
// prepare
int prepareStmt (TAOS_STMT  *stmt,  SSuperTable *stbInfo, char* tagData, uint64_t tableSeq, char *db);
int prepareStmt2(TAOS_STMT2 *stmt2, SSuperTable *stbInfo, char* tagData, uint64_t tableSeq, char *db);

uint32_t bindParamBatch(threadInfo *pThreadInfo,
        uint32_t batch, int64_t startTime, int64_t pos,
        SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n, int64_t *delay2, int64_t *delay3);
int prepareSampleData(SDataBase* database, SSuperTable* stbInfo);
void generateSmlJsonTags(tools_cJSON *tagsList,
        char **sml_tags_json_array,
        SSuperTable *stbInfo,
        uint64_t start_table_from, int tbSeq);
void generateSmlJsonCols(tools_cJSON *array,
        tools_cJSON *tag, SSuperTable *stbInfo,
        uint32_t time_precision, int64_t timestamp);
void generateSmlTaosJsonTags(tools_cJSON *tagsList,
        SSuperTable *stbInfo,
        uint64_t start_table_from, int tbSeq);
void generateSmlTaosJsonCols(tools_cJSON *array,
        tools_cJSON *tag, SSuperTable *stbInfo,
        uint32_t time_precision, int64_t timestamp);
uint32_t accumulateRowLen(BArray *fields, int iface);
void generateSmlJsonValues(
        char **sml_tags_json_array, SSuperTable *stbInfo, int tableSeq);

// generateTag data from random or csv file, cnt is get count for each
bool generateTagData(SSuperTable *stbInfo, char *buf, int64_t cnt, FILE* csv, BArray* tagsStmt, int64_t loopBegin);
// get tag from csv file
FILE* openTagCsv(SSuperTable* stbInfo, uint64_t seek);

//
// STMT2 bind cols param progressive
//
uint32_t bindVTags(TAOS_STMT2_BINDV *bindv, int32_t tbIndex, int32_t w, BArray* fields);

uint32_t bindVCols(TAOS_STMT2_BINDV *bindv, int32_t tbIndex,
                 threadInfo *pThreadInfo,
                 uint32_t batch, int64_t startTime, int64_t pos,
                 SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n);


uint32_t bindVColsInterlace(TAOS_STMT2_BINDV *bindv, int32_t tbIndex,
                 threadInfo *pThreadInfo,
                 uint32_t batch, int64_t startTime, int64_t pos,
                 SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n);


uint32_t bindVColsProgressive(TAOS_STMT2_BINDV *bindv, int32_t tbIndex,
                 threadInfo *pThreadInfo,
                 uint32_t batch, int64_t startTime, int64_t pos,
                 SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n);

void prepareTagsStmt(SSuperTable* stbInfo);
#endif  // INC_BENCHDATA_H_
