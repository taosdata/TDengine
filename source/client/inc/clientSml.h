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

#ifndef TDENGINE_CLIENTSML_H
#define TDENGINE_CLIENTSML_H

#ifdef __cplusplus
extern "C" {
#endif

#include "thash.h"
#include "clientInt.h"
#include "catalog.h"

typedef TSDB_SML_PROTOCOL_TYPE SMLProtocolType;

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
  const char     *sTableName;   // super table name
  uint8_t        sTableNameLen;
  char    childTableName[TSDB_TABLE_NAME_LEN];
  uint64_t uid;

  SArray* tags;
  SArray *cols;
} TAOS_SML_DATA_POINT_TAGS;

typedef struct SSmlSTableMeta {
//  char     *sTableName;   // super table name
//  uint8_t  sTableNameLen;
  uint8_t  precision;     // the number of precision
  SHashObj* tagHash;
  SHashObj* fieldHash;
} SSmlSTableMeta;

typedef struct {
  uint64_t id;

  SMLProtocolType protocol;
  int32_t tsType;

  SHashObj* childTables;
  SHashObj* superTables;

  SHashObj* metaHashObj;
  SHashObj* pVgHash;

  void* exec;

  STscObj*      taos;
  SCatalog*     pCatalog;
  SRequestObj* pRequest;
  SQuery* pQuery;

  int32_t affectedRows;
  char *msgBuf;
  int16_t msgLen;
} SSmlLinesInfo;

int smlInsert(TAOS* taos, SSmlLinesInfo* info);

bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlLinesInfo* info);
bool isValidInteger(char *str);
bool isValidFloat(char *str);

int32_t isValidChildTableName(const char *pTbName, int16_t len, SSmlLinesInfo* info);

bool convertSmlValueType(SSmlKv *pVal, char *value,
                         uint16_t len, SSmlLinesInfo* info, bool isTag);
int32_t convertSmlTimeStamp(SSmlKv *pVal, char *value,
                            uint16_t len, SSmlLinesInfo* info);


int sml_insert_lines(TAOS* taos, SRequestObj* request, char* lines[], int numLines, SMLProtocolType protocol,
                      SMLTimeStampType tsType);
int sml_insert_telnet_lines(TAOS* taos, char* lines[], int numLines, SMLProtocolType protocol,
                             SMLTimeStampType tsType, int* affectedRows);
int sml_insert_json_payload(TAOS* taos, char* payload, SMLProtocolType protocol,
                             SMLTimeStampType tsType, int* affectedRows);


#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CLIENTSML_H
