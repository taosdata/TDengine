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

#ifndef __DEMODATA__
#define __DEMODATA__
#include "cJSON.h"
#include "demo.h"
/***** Global variables ******/

extern char *    g_sampleDataBuf;
extern char *    g_sampleBindBatchArray;
extern int32_t * g_randint;
extern uint32_t *g_randuint;
extern int64_t * g_randbigint;
extern uint64_t *g_randubigint;
extern float *   g_randfloat;
extern double *  g_randdouble;
extern char *    g_randbool_buff;
extern char *    g_randint_buff;
extern char *    g_randuint_buff;
extern char *    g_rand_voltage_buff;
extern char *    g_randbigint_buff;
extern char *    g_randubigint_buff;
extern char *    g_randsmallint_buff;
extern char *    g_randusmallint_buff;
extern char *    g_randtinyint_buff;
extern char *    g_randutinyint_buff;
extern char *    g_randfloat_buff;
extern char *    g_rand_current_buff;
extern char *    g_rand_phase_buff;
extern char *    g_randdouble_buff;
/***** Declare functions *****/
int                 init_rand_data();
char *              rand_bool_str();
int32_t             rand_bool();
char *              rand_tinyint_str();
int32_t             rand_tinyint();
char *              rand_utinyint_str();
int32_t             rand_utinyint();
char *              rand_smallint_str();
int32_t             rand_smallint();
char *              rand_usmallint_str();
int32_t             rand_usmallint();
char *              rand_int_str();
int32_t             rand_int();
char *              rand_uint_str();
int32_t             rand_uint();
char *              rand_bigint_str();
int64_t             rand_bigint();
char *              rand_ubigint_str();
int64_t             rand_ubigint();
char *              rand_float_str();
float               rand_float();
char *              demo_current_float_str();
float UNUSED_FUNC   demo_current_float();
char *              demo_voltage_int_str();
int32_t UNUSED_FUNC demo_voltage_int();
char *              demo_phase_float_str();
float UNUSED_FUNC   demo_phase_float();
void                rand_string(char *str, int size);
char *              rand_double_str();
double              rand_double();

int     generateTagValuesForStb(SSuperTable *stbInfo, int64_t tableSeq,
                                char *tagsValBuf);
int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange);
int32_t prepareStbStmtBindTag(char *bindArray, SSuperTable *stbInfo,
                              char *tagsVal, int32_t timePrec);
int32_t prepareStmtWithoutStb(threadInfo *pThreadInfo, char *tableName,
                              uint32_t batch, int64_t insertRows,
                              int64_t recordFrom, int64_t startTime);
int32_t generateStbInterlaceData(threadInfo *pThreadInfo, char *tableName,
                                 uint32_t batchPerTbl, uint64_t i,
                                 uint32_t batchPerTblTimes, uint64_t tableSeq,
                                 char *buffer, int64_t insertRows,
                                 int64_t startTime, uint64_t *pRemainderBufLen);
int64_t generateInterlaceDataWithoutStb(char *tableName, uint32_t batch,
                                        uint64_t tableSeq, char *dbName,
                                        char *buffer, int64_t insertRows,
                                        int64_t   startTime,
                                        uint64_t *pRemainderBufLen);
int32_t generateStbProgressiveData(SSuperTable *stbInfo, char *tableName,
                                   int64_t tableSeq, char *dbName, char *buffer,
                                   int64_t insertRows, uint64_t recordFrom,
                                   int64_t startTime, int64_t *pSamplePos,
                                   int64_t *pRemainderBufLen);
int32_t generateProgressiveDataWithoutStb(
    char *tableName, threadInfo *pThreadInfo, char *buffer, int64_t insertRows,
    uint64_t recordFrom, int64_t startTime, int64_t *pRemainderBufLen);
int64_t generateStbRowData(SSuperTable *stbInfo, char *recBuf,
                           int64_t remainderBufLen, int64_t timestamp);
int     prepareSampleForStb(SSuperTable *stbInfo);
int     prepareSampleForNtb();
int     parseSamplefileToStmtBatch(SSuperTable *stbInfo);
int     parseStbSampleToStmtBatchForThread(threadInfo * pThreadInfo,
                                           SSuperTable *stbInfo, uint32_t timePrec,
                                           uint32_t batch);
int     parseNtbSampleToStmtBatchForThread(threadInfo *pThreadInfo,
                                           uint32_t timePrec, uint32_t batch);
int     prepareSampleData();
int32_t generateSmlConstPart(char *sml, SSuperTable *stbInfo,
                             threadInfo *pThreadInfo, int tbSeq);

int32_t generateSmlMutablePart(char *line, char *sml, SSuperTable *stbInfo,
                               threadInfo *pThreadInfo, int64_t timestamp);
int32_t generateSmlJsonTags(cJSON *tagsList, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int tbSeq);
int32_t generateSmlJsonCols(cJSON *array, cJSON *tag, SSuperTable *stbInfo,
                            threadInfo *pThreadInfo, int64_t timestamp);
#endif