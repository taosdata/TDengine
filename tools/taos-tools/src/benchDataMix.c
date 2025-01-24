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
 */

#include "bench.h"
#include "benchLog.h"
#include "benchDataMix.h"
#include <float.h>

#define VBOOL_CNT 3

int32_t inul = 20; // interval null count

#define SIGNED_RANDOM(type, high, format)  \
      {                                 \
        type max = high/2;              \
        type min = (max-1) * -1;        \
        type mid = RD(max);             \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, format, mid);      \
      }                                 \

#define UNSIGNED_RANDOM(type, high, format)   \
      {                                 \
        type max = high;                \
        type min = 0;                   \
        type mid = RD(max);             \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, format, mid);      \
      }                                 \


#define FLOAT_RANDOM(type, min, max)    \
      {                                 \
        type mid =  RD(100000000);   \
        mid += RD(1000000)/800001.1;    \
        if(RD(50) == 0) {               \
            mid = max;                  \
        } else {                        \
            if(RD(50) == 0) mid = min;  \
        }                               \
        sprintf(val, "%f", mid);        \
      }                                 \


// 32 ~ 126 + '\r' '\n' '\t' radom char
uint32_t genRadomString(char* val, uint32_t len, char* prefix) {
  uint32_t size = RD(len) ;
  if (size < 3) {
    strcpy(val, VAL_NULL);
    return sizeof(VAL_NULL);
  }

  val[0] = '\'';

  int32_t pos = 1;
  int32_t preLen = strlen(prefix);
  if(preLen > 0 && size > preLen + 3) {
    strcpy(&val[1], prefix);
    pos += preLen;
  }

  char spe[] = {'\\', '\r', '\n', '\t'};
  for (int32_t i = pos; i < size - 1; i++) {
    if (false) {
      val[i] = spe[RD(sizeof(spe))];
    } else {
      val[i] = 32 + RD(94);
    }
    if (val[i] == '\'' || val[i] == '\"' || val[i] == '%' || val[i] == '\\') {
      val[i] = 'x';
    }
  }

  val[size - 1] = '\'';
  // set string end
  val[size] = 0;
  return size;
}


// data row generate by randowm
uint32_t dataGenByField(Field* fd, char* pstr, uint32_t len, char* prefix, int64_t *k, char* nullVal) {
    uint32_t size = 0;
    int64_t  nowts= 0;
    char val[512] = {0};
    if( fd->fillNull && RD(inul) == 0 ) {
        size = sprintf(pstr + len, ",%s", nullVal);
        return size;
    }

    const char * format = ",%s";

    switch (fd->type) {    
    case TSDB_DATA_TYPE_BOOL:
        sprintf(val, "%d", tmpBool(fd));
        break;
    // timestamp    
    case TSDB_DATA_TYPE_TIMESTAMP:
        nowts = toolsGetTimestampMs();
        strcpy(val, "\'");
        toolsFormatTimestamp(val, nowts, TSDB_TIME_PRECISION_MILLI);
        strcat(val, "\'");
        break;
    // signed    
    case TSDB_DATA_TYPE_TINYINT:
        sprintf(val, "%d", tmpInt8Impl(fd, *k));
        break;        
    case TSDB_DATA_TYPE_SMALLINT:
        sprintf(val, "%d", tmpInt16Impl(fd, *k));
        break;
    case TSDB_DATA_TYPE_INT:
        sprintf(val, "%d", tmpInt32Impl(fd, 0, 0, *k));
        break;
    case TSDB_DATA_TYPE_BIGINT:
        sprintf(val, "%"PRId64, tmpInt64Impl(fd, 0, *k));
        break;
    // unsigned    
    case TSDB_DATA_TYPE_UTINYINT:
        sprintf(val, "%u", tmpUint8Impl(fd, *k));
        break;
    case TSDB_DATA_TYPE_USMALLINT:
        sprintf(val, "%u", tmpUint16Impl(fd, *k));
        break;
    case TSDB_DATA_TYPE_UINT:
        sprintf(val, "%u", tmpUint32Impl(fd, 0, 0, *k));
        break;
    case TSDB_DATA_TYPE_UBIGINT:
        sprintf(val, "%"PRIu64, tmpUint64Impl(fd, 0, *k));
        break;
    // float double
    case TSDB_DATA_TYPE_FLOAT:
        sprintf(val, "%f", tmpFloatImpl(fd, 0, 0, *k));
        break;
    case TSDB_DATA_TYPE_DOUBLE:
        sprintf(val, "%f", tmpDoubleImpl(fd, 0, *k));
        break;
    // binary nchar
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
        format = ",\'%s\'";
        tmpStr(val, 0, fd, *k);
        break;
    case TSDB_DATA_TYPE_GEOMETRY:
        tmpGeometry(val, 0, fd, 0);
        break;
    default:
        break;
    }

    size += sprintf(pstr + len, format, val);
    return size;
}


// data row generate by ts calc
uint32_t dataGenByCalcTs(Field* fd, char* pstr, uint32_t len, int64_t ts) {
    uint32_t size = 0;
    char val[512] = VAL_NULL;

    switch (fd->type) {    
    case TSDB_DATA_TYPE_BOOL:
        strcpy(val, (ts % 2 == 0) ? "true" : "false");
        break;
    // timestamp
    case TSDB_DATA_TYPE_TIMESTAMP:
        sprintf(val, "%" PRId64, ts);
        break;
    // signed    
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_SMALLINT:
        sprintf(val, "%d", (int32_t)(ts%100)); 
        break;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_BIGINT:
        sprintf(val, "%d", (int32_t)(ts%1000000)); 
        break;
    // unsigned    
    case TSDB_DATA_TYPE_UTINYINT:
    case TSDB_DATA_TYPE_USMALLINT:
        sprintf(val, "%u", (uint32_t)(ts%100)); 
        break;    
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_UBIGINT:
        sprintf(val, "%u", (uint32_t)(ts%1000000)); 
        break;
    // float double
    case TSDB_DATA_TYPE_FLOAT:
    case TSDB_DATA_TYPE_DOUBLE:
        sprintf(val, "%u.%u", (uint32_t)(ts/10000), (uint32_t)(ts%10000)); 
        break;
    // binary nchar
    case TSDB_DATA_TYPE_BINARY:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_NCHAR:
        sprintf(val, "%" PRId64, ts);
        break;
    default:
        break;
    }

    size += sprintf(pstr + len, ",%s", val);
    return size;
}
