/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_TSCPARSELINE_H
#define TDENGINE_TSCPARSELINE_H

#ifdef __cplusplus
extern "C" {
#endif

#define SML_TIMESTAMP_SECOND_DIGITS 10
#define SML_TIMESTAMP_MILLI_SECOND_DIGITS 13

typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

typedef struct {
  char* key;
  uint8_t type;
  int16_t length;
  char* value;
  uint32_t fieldSchemaIdx;
} TAOS_SML_KV;

typedef struct {
  char* stableName;

  char* childTableName;
  TAOS_SML_KV* tags;
  int32_t tagNum;

  // first kv must be timestamp
  TAOS_SML_KV* fields;
  int32_t fieldNum;

  uint32_t schemaIdx;
} TAOS_SML_DATA_POINT;

typedef enum {
  SML_TIME_STAMP_NOT_CONFIGURED,
  SML_TIME_STAMP_HOURS,
  SML_TIME_STAMP_MINUTES,
  SML_TIME_STAMP_SECONDS,
  SML_TIME_STAMP_MILLI_SECONDS,
  SML_TIME_STAMP_MICRO_SECONDS,
  SML_TIME_STAMP_NANO_SECONDS,
  SML_TIME_STAMP_NOW
} SMLTimeStampType;

typedef struct SSmlSqlInsertBatch {
  uint64_t id;
  int32_t index;

  char* sql;
  int32_t code;
  int32_t tryTimes;
  tsem_t sem;
  int32_t affectedRows;
  bool tryAgain;
  bool resetQueryCache;
  bool sleep;
} SSmlSqlInsertBatch;

#define MAX_SML_SQL_INSERT_BATCHES 512

typedef struct {
  uint64_t id;
  SMLProtocolType protocol;
  SMLTimeStampType tsType;
  SHashObj* smlDataToSchema;

  int32_t affectedRows;

  pthread_mutex_t batchMutex;
  pthread_cond_t batchCond;
  int32_t numBatches;
  SSmlSqlInsertBatch batches[MAX_SML_SQL_INSERT_BATCHES];
} SSmlLinesInfo;

char* addEscapeCharToString(char *str, int32_t len);
int tscSmlInsert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint, SSmlLinesInfo* info);
bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlLinesInfo* info);
bool isValidInteger(char *str);
bool isValidFloat(char *str);

int32_t isValidChildTableName(const char *pTbName, int16_t len, SSmlLinesInfo* info);

bool convertSmlValueType(TAOS_SML_KV *pVal, char *value,
                         uint16_t len, SSmlLinesInfo* info, bool isTag);
int32_t convertSmlTimeStamp(TAOS_SML_KV *pVal, char *value,
                            uint16_t len, SSmlLinesInfo* info);

void destroySmlDataPoint(TAOS_SML_DATA_POINT* point);

int taos_insert_lines(TAOS* taos, char* lines[], int numLines, SMLProtocolType protocol,
                      SMLTimeStampType tsType, int* affectedRows);
int taos_insert_telnet_lines(TAOS* taos, char* lines[], int numLines, SMLProtocolType protocol,
                             SMLTimeStampType tsType, int* affectedRows);
int taos_insert_json_payload(TAOS* taos, char* payload, SMLProtocolType protocol,
                             SMLTimeStampType tsType, int* affectedRows);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCPARSELINE_H
