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

typedef struct {
  char* key;
  uint8_t type;
  int16_t length;
  char* value;
} TAOS_SML_KV;

typedef struct {
  char* stableName;

  char* childTableName;
  TAOS_SML_KV* tags;
  int32_t tagNum;

  // first kv must be timestamp
  TAOS_SML_KV* fields;
  int32_t fieldNum;
} TAOS_SML_DATA_POINT;

typedef enum {
  SML_TIME_STAMP_NOW,
  SML_TIME_STAMP_SECONDS,
  SML_TIME_STAMP_MILLI_SECONDS,
  SML_TIME_STAMP_MICRO_SECONDS,
  SML_TIME_STAMP_NANO_SECONDS
} SMLTimeStampType;

typedef struct {
  uint64_t id;
  SHashObj* smlDataToSchema;
} SSmlLinesInfo;

int tscSmlInsert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint, SSmlLinesInfo* info);
int taos_sml_insert(TAOS* taos, TAOS_SML_DATA_POINT* points, int numPoint);
bool checkDuplicateKey(char *key, SHashObj *pHash, SSmlLinesInfo* info);
int32_t isValidChildTableName(const char *pTbName, int16_t len);

bool convertSmlValueType(TAOS_SML_KV *pVal, char *value,
                         uint16_t len, SSmlLinesInfo* info);

int32_t convertSmlTimeStamp(TAOS_SML_KV *pVal, char *value,
                            uint16_t len, SSmlLinesInfo* info);

void destroySmlDataPoint(TAOS_SML_DATA_POINT* point);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TSCPARSELINE_H
