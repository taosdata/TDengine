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

#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include "taosmsg.h"
#include "tschemautil.h"
#include "ttypes.h"
#include "tutil.h"

bool isValidSchema(struct SSchema* pSchema, int32_t numOfCols) {
  if (!VALIDNUMOFCOLS(numOfCols)) {
    return false;
  }

  /* first column must be the timestamp, which is a primary key */
  if (pSchema[0].type != TSDB_DATA_TYPE_TIMESTAMP) {
    return false;
  }

  /* type is valid, length is valid */
  int32_t rowLen = 0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    // 1. valid types
    if (pSchema[i].type > TSDB_DATA_TYPE_TIMESTAMP || pSchema[i].type < TSDB_DATA_TYPE_BOOL) {
      return false;
    }

    // 2. valid length for each type
    if (pSchema[i].type == TSDB_DATA_TYPE_TIMESTAMP) {
      if (pSchema[i].bytes > TSDB_MAX_BINARY_LEN) {
        return false;
      }
    } else {
      if (pSchema[i].bytes != tDataTypeDesc[pSchema[i].type].nSize) {
        return false;
      }
    }

    // 3. valid column names
    for (int32_t j = i + 1; j < numOfCols; ++j) {
      if (strncasecmp(pSchema[i].name, pSchema[j].name, TSDB_COL_NAME_LEN) == 0) {
        return false;
      }
    }

    rowLen += pSchema[i].bytes;
  }

  // valid total length
  return (rowLen <= TSDB_MAX_BYTES_PER_ROW);
}

struct SSchema* tsGetSchema(SMeterMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return tsGetSchemaColIdx(pMeta, 0);
}

struct SSchema* tsGetTagSchema(SMeterMeta* pMeta) {
  if (pMeta == NULL || pMeta->numOfTags == 0) {
    return NULL;
  }

  return tsGetSchemaColIdx(pMeta, pMeta->numOfColumns);
}

struct SSchema* tsGetSchemaColIdx(SMeterMeta* pMeta, int32_t startCol) {
  if (pMeta->pSchema == 0) {
    pMeta->pSchema = sizeof(SMeterMeta);
  }

  return (SSchema*)(((char*)pMeta + pMeta->pSchema) + startCol * sizeof(SSchema));
}

char* tsGetTagsValue(SMeterMeta* pMeta) {
  if (pMeta->tags == 0) {
    int32_t numOfTotalCols = pMeta->numOfColumns + pMeta->numOfTags;
    pMeta->tags = sizeof(SMeterMeta) + numOfTotalCols * sizeof(SSchema);
  }

  return ((char*)pMeta + pMeta->tags);
}

bool tsMeterMetaIdentical(SMeterMeta* p1, SMeterMeta* p2) {
  if (p1 == NULL || p2 == NULL || p1->uid != p2->uid || p1->sversion != p2->sversion) {
    return false;
  }

  if (p1 == p2) {
    return true;
  }

  size_t size = sizeof(SMeterMeta) + p1->numOfColumns * sizeof(SSchema);

  for (int32_t i = 0; i < p1->numOfTags; ++i) {
    SSchema* pColSchema = tsGetSchemaColIdx(p1, i + p1->numOfColumns);
    size += pColSchema->bytes;
  }

  return memcmp(p1, p2, size) == 0;
}

static FORCE_INLINE char* skipSegments(char* input, char delimiter, int32_t num) {
  for (int32_t i = 0; i < num; ++i) {
    while (*input != 0 && *input++ != delimiter) {
    };
  }
  return input;
}

static FORCE_INLINE void copySegment(char* dst, char* src, char delimiter) {
  while (*src != delimiter && *src != 0) {
    *dst++ = *src++;
  }
}

/**
 * extract meter name from meterid, which the format of userid.dbname.metername
 * @param meterId
 * @return
 */
void extractMeterName(char* meterId, char* name) {
  char* r = skipSegments(meterId, TS_PATH_DELIMITER[0], 2);
  copySegment(name, r, TS_PATH_DELIMITER[0]);
}

void extractDBName(char* meterId, char* name) {
  char* r = skipSegments(meterId, TS_PATH_DELIMITER[0], 1);
  copySegment(name, r, TS_PATH_DELIMITER[0]);
}
