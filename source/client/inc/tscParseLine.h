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

#include "thash.h"
#include "clientint.h"

#define SML_TIMESTAMP_SECOND_DIGITS 10
#define SML_TIMESTAMP_MILLI_SECOND_DIGITS 13

typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

typedef struct {
  const char* key;
  int32_t keyLen;
  uint8_t type;
  int16_t length;
  const char* value;
  int32_t valueLen;
} TAOS_SML_KV;

typedef struct {
  const char* measure;
  const char* tags;
  const char* cols;
  const char* timestamp;

  int32_t measureLen;
  int32_t measureTagsLen;
  int32_t tagsLen;
  int32_t colsLen;
} TAOS_PARSE_ELEMENTS;

typedef struct {
  char* childTableName;

  SArray* tags;
  SArray *cols;
} TAOS_SML_DATA_POINT_TAGS;

typedef struct SSmlSTableMeta {
  char     *sTableName;   // super table name
  uint8_t  sTableNameLen;
  uint8_t  precision;     // the number of precision
  SHashObj* tagHash;
  SHashObj* fieldHash;
} SSmlSTableMeta;

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

typedef struct {
  uint64_t id;

  STscObj*      taos;
  SCatalog*     pCatalog;

  SMLProtocolType protocol;
  SMLTimeStampType tsType;

  int32_t affectedRows;

  SHashObj* childTables;
  SHashObj* superTables;
} SSmlLinesInfo;

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
