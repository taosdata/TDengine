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

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>


#include <bench.h>
#include "benchLog.h"
#include <math.h>
#include <benchData.h>
#include "decimal.h"


const char charset[] =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

const char* locations[] = {
    "California.SanFrancisco", "California.LosAngles",
    "California.SanDiego", "California.SanJose",
    "California.PaloAlto", "California.Campbell",
    "California.MountainView", "California.Sunnyvale",
    "California.SantaClara", "California.Cupertino"};

const char* locations_sml[] = {
    "California.SanFrancisco", "California.LosAngles",
    "California.SanDiego", "California.SanJose",
    "California.PaloAlto", "California.Campbell",
    "California.MountainView", "California.Sunnyvale",
    "California.SantaClara", "California.Cupertino"};

#ifdef WINDOWS
    // TODO: why define ssize_t in this way?
    #define ssize_t int
    // #if _MSC_VER >= 1910
    //     #include "benchLocations.h"
    // #else
    //     #include "benchLocationsWin.h"
    // #endif
    // NOTE: benchLocations.h is UTF-8 encoded, while benchLocationsWin.h is ANSI/GB18030 encoded.
    //       we don't want to use /utf-8 option in MSVC which will bring more considerations,
    //       so we use ANSI/GB18030 encoded file for the moment.
    #include "benchLocationsWin.h"
#else
    #include "benchLocations.h"
#endif

int32_t funCount(int32_t min, int32_t max, int32_t step, int32_t loop) {
    if(step == 0) {
        step = 1;
    }

    int32_t range = abs(max - min);
    int32_t maxCnt = range / step;
    int32_t val = min + (loop % maxCnt) * step ;

    return val;
}

int32_t funSaw(int32_t min, int32_t max, int32_t period, int32_t loop) {
    if(period == 0) {
        period = 1;
    }
    int32_t range = abs(max - min);
    int32_t step = range / period;
    int32_t val = min + (loop % period) * step ;
    return val;
}

int32_t funSquare(int32_t min, int32_t max, int32_t period, int32_t loop) {
    if(period == 0) {
        period = 1;
    }
    int32_t change = (loop/period) % 2;
    if (change)
       return min;
    else
       return max;
}

int32_t funTriAngle(int32_t min, int32_t max, int32_t period, int32_t loop) {
    if(period == 0) {
        period = 1;
    }
    int32_t range = abs(max - min);
    int32_t change = (loop/period) % 2;
    int32_t step = range/period;
    int32_t cnt = 0;
    if(change)
       cnt = period - loop % period;
    else
       cnt = loop % period;

    return min + cnt * step;
}


// calc expression value like 10*sin(x) + 100
float funValueFloat(Field *field, int32_t angle, int32_t loop) {
    float radian = ATOR(angle);
    float funVal = 0;

    if (field->funType == FUNTYPE_SIN)
       funVal = sin(radian);
    else if (field->funType == FUNTYPE_COS)
       funVal = cos(radian);
    else if (field->funType == FUNTYPE_COUNT)
       funVal = (float)funCount(field->min, field->max, field->step, loop);
    else if (field->funType == FUNTYPE_SAW)
       funVal = (float)funSaw(field->min, field->max, field->period, loop + field->offset );
    else if (field->funType == FUNTYPE_SQUARE)
       funVal = (float)funSquare(field->min, field->max, field->period, loop + field->offset);
    else if (field->funType == FUNTYPE_TRI)
       funVal = (float)funTriAngle(field->min, field->max, field->period, loop + field->offset);

    if(field->multiple != 0)
       funVal *= field->multiple;

    if ( field->addend !=0 && field->random > 0 ) {
        float rate = taosRandom() % field->random;
        funVal += field->addend * (rate/100);
    } else if(field->addend !=0 ) {
        funVal += field->addend;
    }

    funVal += field->base;
    return funVal;
}

// calc expression value like 10*sin(x) + 100
int32_t funValueInt32(Field *field, int32_t angle, int32_t loop) {
    float radian = ATOR(angle);
    int32_t funVal = 0;

    if (field->funType == FUNTYPE_SIN)
       funVal = (int32_t)sin(radian);
    else if (field->funType == FUNTYPE_COS)
       funVal = (int32_t)cos(radian);
    else if (field->funType == FUNTYPE_COUNT)
       funVal = funCount(field->min, field->max, field->step, loop);
    else if (field->funType == FUNTYPE_SAW)
       funVal = funSaw(field->min, field->max, field->period, loop + field->offset );
    else if (field->funType == FUNTYPE_SQUARE)
       funVal = funSquare(field->min, field->max, field->period, loop + field->offset);
    else if (field->funType == FUNTYPE_TRI)
       funVal = funTriAngle(field->min, field->max, field->period, loop + field->offset);

    if(field->multiple != 0)
       funVal *= field->multiple;

    if ( field->addend !=0 && field->random > 0 ) {
        float rate = taosRandom() % field->random;
        funVal += field->addend * (rate/100);
    } else if(field->addend !=0 ) {
        funVal += field->addend;
    }

    funVal += field->base;

    return funVal;
}


static int usc2utf8(char *p, int unic) {
    int ret = 0;
    if (unic <= 0x0000007F) {
        *p = (unic & 0x7F);
        ret = 1;
    } else if (unic <= 0x000007FF) {
        *(p + 1) = (unic & 0x3F) | 0x80;
        *p = ((unic >> 6) & 0x1F) | 0xC0;
        ret = 2;
    } else if (unic <= 0x0000FFFF) {
        *(p + 2) = (unic & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 6) & 0x3F) | 0x80;
        *p = ((unic >> 12) & 0x0F) | 0xE0;
        ret = 3;
    } else if (unic <= 0x001FFFFF) {
        *(p + 3) = (unic & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 12) & 0x3F) | 0x80;
        *p = ((unic >> 18) & 0x07) | 0xF0;
        ret = 4;
    } else if (unic <= 0x03FFFFFF) {
        *(p + 4) = (unic & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 18) & 0x3F) | 0x80;
        *p = ((unic >> 24) & 0x03) | 0xF8;
        ret = 5;
    // } else if (unic >= 0x04000000) {
    } else {
        *(p + 5) = (unic & 0x3F) | 0x80;
        *(p + 4) = ((unic >> 6) & 0x3F) | 0x80;
        *(p + 3) = ((unic >> 12) & 0x3F) | 0x80;
        *(p + 2) = ((unic >> 18) & 0x3F) | 0x80;
        *(p + 1) = ((unic >> 24) & 0x3F) | 0x80;
        *p = ((unic >> 30) & 0x01) | 0xFC;
        ret = 6;
    }

    return ret;
}

void rand_string(char *str, int size, bool chinese) {
    if (chinese) {
        char *pstr = str;
        while (size > 0) {
            // Chinese Character need 3 bytes space
            if (size < 3) {
                break;
            }
            // Basic Chinese Character's Unicode is from 0x4e00 to 0x9fa5
            int unic = 0x4e00 + taosRandom() % (0x9fa5 - 0x4e00);
            int move = usc2utf8(pstr, unic);
            pstr += move;
            size -= move;
        }
    } else {
        str[0] = 0;
        if (size > 0) {
            // --size;
            int n;
            for (n = 0; n < size; n++) {
                int key = taosRandom() % (unsigned int)(sizeof(charset) - 1);
                str[n] = charset[key];
            }
            str[n] = 0;
        }
    }
}

// generate prepare sql
char* genPrepareSql(SSuperTable *stbInfo, char* tagData, uint64_t tableSeq, char *db) {
    int   len = 0;
    char *prepare = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
    int n;
    char *tagQ = NULL;
    char *colQ = genQMark(stbInfo->cols->size);
    char *colNames = NULL;
    bool  tagQFree = false;

    if(tagData == NULL) {
        // if no tagData , replace with QMark
        tagQ = genQMark(stbInfo->tags->size);
        tagQFree = true;
    } else {
        tagQ = tagData + stbInfo->lenOfTags * tableSeq;
    }

    if (stbInfo->autoTblCreating) {
        char ttl[SMALL_BUFF_LEN] = "";
        if (stbInfo->ttl != 0) {
            snprintf(ttl, SMALL_BUFF_LEN, "TTL %d", stbInfo->ttl);
        }
        n = snprintf(prepare + len,
                       TSDB_MAX_ALLOWED_SQL_LEN - len,
                       "INSERT INTO ? USING `%s`.`%s` TAGS (%s) %s VALUES(?,%s)",
                       db, stbInfo->stbName, tagQ, ttl, colQ);
    } else {
        if (workingMode(g_arguments->connMode, g_arguments->dsn) == CONN_MODE_NATIVE) {
            // native
            n = snprintf(prepare + len, TSDB_MAX_ALLOWED_SQL_LEN - len,
                "INSERT INTO ? VALUES(?,%s)", colQ);
        } else {
            // websocket
            bool ntb = stbInfo->tags == NULL || stbInfo->tags->size == 0; // normal table
            colNames = genColNames(stbInfo->cols, !ntb, stbInfo->primaryKeyName);
            n = snprintf(prepare + len, TSDB_MAX_ALLOWED_SQL_LEN - len,
                "INSERT INTO `%s`.`%s`(%s) VALUES(%s,%s)", db, stbInfo->stbName, colNames,
                ntb ? "?" : "?,?", colQ);
        }
    }
    len += n;

    // free
    if (tagQFree) {
        tmfree(tagQ);
    }
    tmfree(colQ);
    tmfree(colNames);

    // check valid
    if (g_arguments->prepared_rand < g_arguments->reqPerReq) {
        infoPrint(
                  "in stmt mode, batch size(%u) can not larger than prepared "
                  "sample data size(%" PRId64
                  "), restart with larger prepared_rand or batch size will be "
                  "auto set to %" PRId64 "\n",
                  g_arguments->reqPerReq, g_arguments->prepared_rand,
                  g_arguments->prepared_rand);
        g_arguments->reqPerReq = g_arguments->prepared_rand;
    }

    return prepare;
}

int prepareStmt(TAOS_STMT *stmt, SSuperTable *stbInfo, char* tagData, uint64_t tableSeq, char *db) {
    char *prepare = genPrepareSql(stbInfo, tagData, tableSeq, db);
    if (taos_stmt_prepare(stmt, prepare, strlen(prepare))) {
        errorPrint("taos_stmt_prepare(%s) failed. errstr=%s\n", prepare, taos_stmt_errstr(stmt));
        tmfree(prepare);
        return -1;
    }
    debugPrint("succ call taos_stmt_prepare sql:%s\n", prepare);
    tmfree(prepare);
    return 0;
}

int prepareStmt2(TAOS_STMT2 *stmt2, SSuperTable *stbInfo, char* tagData, uint64_t tableSeq, char *db) {
    char *prepare = genPrepareSql(stbInfo, tagData, tableSeq, db);
    if (taos_stmt2_prepare(stmt2, prepare, strlen(prepare))) {
        errorPrint("taos_stmt2_prepare(%s) failed. errstr=%s\n", prepare, taos_stmt2_error(stmt2));
        tmfree(prepare);
        return -1;
    }
    debugPrint("succ call taos_stmt2_prepare. sql=%s\n", prepare);
    tmfree(prepare);
    return 0;
}


static bool getSampleFileNameByPattern(char *filePath,
                                       SSuperTable *stbInfo,
                                       int64_t child) {
    char *pos = strstr(stbInfo->childTblSample, "XXXX");
    snprintf(filePath, MAX_PATH_LEN, "%s", stbInfo->childTblSample);
    int64_t offset = (int64_t)pos - (int64_t)stbInfo->childTblSample;
    snprintf(filePath + offset,
             MAX_PATH_LEN - offset,
            "%s",
            stbInfo->childTblArray[child]->name);
    size_t len = strlen(stbInfo->childTblArray[child]->name);
    snprintf(filePath + offset + len,
            MAX_PATH_LEN - offset - len,
            "%s", pos +4);
    return true;
}

static int generateSampleFromCsv(char *buffer, char* file, FILE* fp, int32_t length, int64_t size) {
    size_t  n = 0;
    char *  line = NULL;
    int     getRows = 0;
    bool    needClose = false;

    if (file != NULL && fp == NULL) {
        fp = fopen(file, "r");
        if (fp == NULL) {
            errorPrint("open csv file failed. file=%s\n", file);
            return -1;
        }
        needClose = true;
    }

    while (1) {
        ssize_t readLen = 0;
#if defined(WIN32) || defined(WIN64)
        toolsGetLineFile(&line, &n, fp);
        readLen = n;
        if (0 == readLen) {
#else
        readLen = getline(&line, &n, fp);
        if (-1 == readLen) {
#endif
            if (0 != fseek(fp, 0, SEEK_SET)) {
                errorPrint("Failed to fseek , reason:%s\n", strerror(errno));
                if(needClose)
                    fclose(fp);
                return -1;
            }
            continue;
        }

        int32_t pos = (int32_t)(readLen - 1);
        if(pos > 0) {
            if (('\r' == line[pos]) || ('\n' == line[pos])) {
                line[pos] = 0;
            }
        }
        pos = (int32_t)(readLen - 2);
        if(pos > 0) {
            if (('\r' == line[pos]) || ('\n' == line[pos])) {
                line[pos] = 0;
            }
        }

        if (readLen == 0) {
            continue;
        }

        int64_t offset = ((int64_t)getRows) * length;
        memcpy(buffer + offset, line, readLen + 1);
        getRows++;

        if (getRows == size) {
            break;
        }
    }

    if(needClose) {
        fclose(fp);
    }

    tmfree(line);
    infoPrint("read data from csv file %s, read rows=%d\n", file, getRows);
    return 0;
}

static int getAndSetRowsFromCsvFile(char *sampleFile, uint64_t *insertRows) {
    FILE *  fp = fopen(sampleFile, "r");
    if (NULL == fp) {
        errorPrint("Failed to open sample file: %s, reason:%s\n",
                   sampleFile, strerror(errno));
        return -1;
    }

    int     line_count = 0;
    char *  buf = NULL;

    buf = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, false);
    if (NULL == buf) {
        errorPrint("%s() failed to allocate memory!\n", __func__);
        fclose(fp);
        return -1;
    }

    while (fgets(buf, TSDB_MAX_ALLOWED_SQL_LEN, fp)) {
        line_count++;
    }
    *insertRows = line_count;
    fclose(fp);
    tmfree(buf);
    return 0;
}

uint32_t accumulateRowLen(BArray *fields, int iface) {
    uint32_t len = 0;
    for (int i = 0; i < fields->size; ++i) {
        Field *field = benchArrayGet(fields, i);
        switch (field->type) {
            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_GEOMETRY:
            case TSDB_DATA_TYPE_NCHAR:
                len += field->length + 3;
                break;
            case TSDB_DATA_TYPE_INT:
            case TSDB_DATA_TYPE_UINT:
                len += INT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BIGINT:
            case TSDB_DATA_TYPE_UBIGINT:
                len += BIGINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_SMALLINT:
            case TSDB_DATA_TYPE_USMALLINT:
                len += SMALLINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TINYINT:
            case TSDB_DATA_TYPE_UTINYINT:
                len += TINYINT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_BOOL:
                len += BOOL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_FLOAT:
                len += FLOAT_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DOUBLE:
                len += DOUBLE_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DECIMAL:
                len += DECIMAL_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_DECIMAL64:
                len += DECIMAL64_BUFF_LEN;
                break;

            case TSDB_DATA_TYPE_TIMESTAMP:
                len += TIMESTAMP_BUFF_LEN;
                break;
            case TSDB_DATA_TYPE_JSON:
                len += field->length * fields->size;
                return len;
            case TSDB_DATA_TYPE_BLOB:
                len += field->length;
                break;
        }
        len += 1;
        if (iface == SML_REST_IFACE || iface == SML_IFACE) {
            len += SML_LINE_SQL_SYNTAX_OFFSET + strlen(field->name);
        }
    }
    if (iface == SML_IFACE || iface == SML_REST_IFACE) {
        len += 2 * TSDB_TABLE_NAME_LEN * 2 + SML_LINE_SQL_SYNTAX_OFFSET;
    }
    len += TIMESTAMP_BUFF_LEN;
    return len;
}


int tmpStr(char *tmp, int iface, Field *field, int64_t k) {
    if (field->values) {
        int arraySize = tools_cJSON_GetArraySize(field->values);
        if (arraySize) {
            tools_cJSON *buf = tools_cJSON_GetArrayItem(
                    field->values,
                    taosRandom() % arraySize);
            snprintf(tmp, field->length,
                     "%s", buf->valuestring);
        } else {
            errorPrint("%s() cannot read correct value "
                       "from json file. array size: %d\n",
                       __func__, arraySize);
            return -1;
        }
    } else if (g_arguments->demo_mode) {
        unsigned int tmpRand = taosRandom();
        if (g_arguments->chinese) {
            snprintf(tmp, field->length, "%s",
                     locations_chinese[tmpRand % 10]);
        } else if (SML_IFACE == iface) {
            snprintf(tmp, field->length, "%s",
                     locations_sml[tmpRand % 10]);
        } else {
            snprintf(tmp, field->length, "%s",
                     locations[tmpRand % 10]);
        }
    } else {
        if(field->gen == GEN_ORDER) {
            snprintf(tmp, field->length, "%"PRId64, k);
        } else {
            rand_string(tmp, taosRandom() % field->length, g_arguments->chinese);
        }
    }
    return 0;
}

int tmpGeometry(char *tmp, int iface, Field *field, int64_t k) {
    // values
    if (field->values) {
        int arraySize = tools_cJSON_GetArraySize(field->values);
        if (arraySize) {
            tools_cJSON *buf = tools_cJSON_GetArrayItem(
                    field->values,
                    taosRandom() % arraySize);
            snprintf(tmp, field->length,
                     "%s", buf->valuestring);
        } else {
            errorPrint("%s() cannot read correct value "
                       "from json file. array size: %d\n",
                       __func__, arraySize);
            return -1;
        }
        return 0;
    }

    // gen point count
    int32_t cnt = field->length / 24;
    if(cnt < 2) {
        snprintf(tmp, field->length, "POINT(%d %d)", tmpUint16(field), tmpUint16(field));
        return 0;
    }

    int32_t pos = snprintf(tmp, field->length, "LINESTRING(");
    char * format = "%d %d,";
    for(int32_t i = 0; i < cnt; i++) {
        if (i == cnt - 1) {
            format = "%d %d";
        }
        pos += snprintf(tmp + pos, field->length - pos, format, tmpUint16(field), tmpUint16(field));
    }
    strcat(tmp, ")");

    return 0;
}

bool tmpBool(Field *field) {
    bool boolTmp;
    if (field->min == field->max) {
        boolTmp = (field->min)?1:0;
    } else {
        boolTmp = (taosRandom() % 2)&1;
    }
    return boolTmp;
}

int8_t tmpInt8Impl(Field *field, int64_t k) {
    int8_t tinyint = field->min;
    if (field->min != field->max) {
        tinyint += COL_GEN % (field->max - field->min);
    }
    return tinyint;
}

uint8_t tmpUint8Impl(Field *field, int64_t k) {
    uint8_t utinyint = field->min;
    if (field->min != field->max) {
        utinyint += (COL_GEN % (field->max - field->min));
    }
    return utinyint;
}

int16_t tmpInt16Impl(Field *field, int64_t k) {
    int16_t smallint = field->min;
    if (field->min != field->max) {
        smallint += (COL_GEN % (field->max - field->min));
    }
    return smallint;
}

uint16_t tmpUint16Impl(Field *field, int64_t k) {
    uint16_t usmallintTmp = field->min;
    if (field->max != field->min) {
        usmallintTmp += (COL_GEN % (field->max - field->min));
    }
    return usmallintTmp;
}

int tmpInt32Impl(Field *field, int i, int angle, int32_t k) {
    int intTmp;
    if (field->funType != FUNTYPE_NONE) {
        // calc from function
        intTmp = funValueInt32(field, angle, k);
    } else if ((g_arguments->demo_mode) && (i == 0)) {
        unsigned int tmpRand = taosRandom();
        intTmp = tmpRand % 10 + 1;
    } else if ((g_arguments->demo_mode) && (i == 1)) {
        intTmp = 105 + taosRandom() % 10;
    } else {
        if (field->min < (-1 * (RAND_MAX >> 1))) {
            field->min = -1 * (RAND_MAX >> 1);
        }
        if (field->max > (RAND_MAX >> 1)) {
            field->max = RAND_MAX >> 1;
        }
        intTmp = field->min;
        if (field->max != field->min) {
            intTmp += (COL_GEN % (field->max - field->min));
        }
    }
    return intTmp;
}

int tmpInt32ImplTag(Field *field, int i, int k) {
    int intTmp;

    if (field->min < (-1 * (RAND_MAX >> 1))) {
        field->min = -1 * (RAND_MAX >> 1);
    }
    if (field->max > (RAND_MAX >> 1)) {
        field->max = RAND_MAX >> 1;
    }
    intTmp = field->min;
    if (field->max != field->min) {
        intTmp += (COL_GEN % (field->max - field->min));
    }
    return intTmp;
}


uint32_t tmpUint32Impl(Field *field, int i, int angle, int64_t k) {
    uint32_t intTmp;
    if (field->funType != FUNTYPE_NONE) {
        // calc from function
        intTmp = funValueInt32(field, angle, k);
    } else if ((g_arguments->demo_mode) && (i == 0)) {
        unsigned int tmpRand = taosRandom();
        intTmp = tmpRand % 10 + 1;
    } else if ((g_arguments->demo_mode) && (i == 1)) {
        intTmp = 105 + taosRandom() % 10;
    } else {
        intTmp = field->min;
        if (field->max != field->min) {
            intTmp += (COL_GEN % (field->max - field->min));
        }
    }
    return intTmp;
}

int64_t tmpInt64Impl(Field *field, int32_t angle, int32_t k) {
    int64_t bigintTmp = field->min;
    if(field->funType != FUNTYPE_NONE) {
        bigintTmp = funValueInt32(field, angle, k);
    } else if (field->min != field->max) {
        bigintTmp += (COL_GEN % (field->max - field->min));
    }
    return bigintTmp;
}

uint64_t tmpUint64Impl(Field *field, int32_t angle, int64_t k) {
    uint64_t bigintTmp = field->min;
    if(field->funType != FUNTYPE_NONE) {
        bigintTmp = funValueInt32(field, angle, k);
    } else if (field->min != field->max) {
        bigintTmp += (COL_GEN % (field->max - field->min));
    }
    return bigintTmp;
}

float tmpFloatImpl(Field *field, int i, int32_t angle, int32_t k) {
    float floatTmp = (float)field->min;
    if(field->funType != FUNTYPE_NONE) {
        floatTmp = funValueFloat(field, angle, k);
    } else {
        if (field->max != field->min) {
            if (field->gen == GEN_ORDER) {
                floatTmp += (k % (field->max - field->min));
            } else {
                floatTmp += ((taosRandom() %
                        (field->max - field->min))
                    + (taosRandom() % 1000) / 1000.0);
            }
        }
        if (g_arguments->demo_mode && i == 0) {
            floatTmp = (float)(9.8 + 0.04 * (taosRandom() % 10)
                + floatTmp / 1000000000);
        } else if (g_arguments->demo_mode && i == 2) {
            floatTmp = (float)((105 + taosRandom() % 10
                + floatTmp / 1000000000) / 360);
        }
    }

    if (field->scalingFactor > 0) {
        if (field->scalingFactor > 1)
            floatTmp = floatTmp / field->scalingFactor;

        if (floatTmp > field->maxInDbl)
            floatTmp = field->maxInDbl;
        else if (floatTmp < field->minInDbl)
            floatTmp = field->minInDbl;
    }

    return floatTmp;
}

double tmpDoubleImpl(Field *field, int32_t angle, int32_t k) {
    double doubleTmp = (double)(field->min);
    if(field->funType != FUNTYPE_NONE) {
        doubleTmp = funValueFloat(field, angle, k);
    } else if (field->max != field->min) {
        if(field->gen == GEN_ORDER) {
            doubleTmp += k % (field->max - field->min);
        } else {
            doubleTmp += ((taosRandom() %
                (field->max - field->min)) +
                taosRandom() % 1000000 / 1000000.0);
        }
    }

    if (field->scalingFactor > 0) {
        if (field->scalingFactor > 1) {
            doubleTmp /= field->scalingFactor;
        }

        if (doubleTmp > field->maxInDbl)
            doubleTmp = field->maxInDbl;
        else if (doubleTmp < field->minInDbl)
            doubleTmp = field->minInDbl;
    }

    return doubleTmp;
}

static int tmpJson(char *sampleDataBuf,
                   int bufLen, int64_t pos,
                   int fieldsSize, Field *field) {
    int n = snprintf(sampleDataBuf + pos, bufLen - pos, "'{");
    if (n < 0 || n >= bufLen - pos) {
        errorPrint("%s() LN%d snprintf overflow\n",
                   __func__, __LINE__);
        return -1;
    }
    for (int j = 0; j < fieldsSize; ++j) {
        // key
        n += snprintf(sampleDataBuf + pos + n, bufLen - pos - n,
                        "\"k%d\":", j);
        if (n < 0 || n >= bufLen - pos) {
            errorPrint("%s() LN%d snprintf overflow\n",
                       __func__, __LINE__);
            return -1;
        }
        // value
        char *buf = benchCalloc(1, field->length + 1, false);
        rand_string(buf, 12, g_arguments->chinese);
        n += snprintf(sampleDataBuf + pos + n, bufLen - pos - n,
                        "\"%s\",", buf);
        if (n < 0 || n >= bufLen - pos) {
            errorPrint("%s() LN%d snprintf overflow\n",
                       __func__, __LINE__);
            tmfree(buf);
            return -1;
        }
        tmfree(buf);
    }
    n += snprintf(sampleDataBuf + pos + n - 1,
                    bufLen - pos - n, "}'");
    if (n < 0 || n >= bufLen - pos) {
        errorPrint("%s() LN%d snprintf overflow\n",
                   __func__, __LINE__);
        return -1;
    }

    return n;
}


static uint64_t generateRandomUint64(uint64_t range) {
    uint64_t randomValue;

    if (range <= (uint64_t)RAND_MAX) {
        randomValue = (uint64_t)rand() % range;
    } else {
        int bitsPerRand = 0;
        for (uint64_t r = RAND_MAX; r > 0; r >>= 1) {
            bitsPerRand++;
        }

        uint64_t result;
        uint64_t threshold;

        do {
            result = 0;
            int bitsAccumulated = 0;

            while (bitsAccumulated < 64) {
                uint64_t part = (uint64_t)rand();
                int bits = (64 - bitsAccumulated) < bitsPerRand ? (64 - bitsAccumulated) : bitsPerRand;
                part &= (1ULL << bits) - 1;
                result |= part << bitsAccumulated;
                bitsAccumulated += bits;
            }

            // rejecting sample
            threshold = (UINT64_MAX / range) * range;
            threshold = (threshold == 0) ? 0 : threshold - 1;

        } while (result > threshold);

        randomValue = result % range;
    }

    return randomValue;
}


static uint64_t randUint64(uint64_t min, uint64_t max) {
    if (min >= max || (max - min) == UINT64_MAX) {
        return min;
    }

    uint64_t range = max - min + 1;
    return min + generateRandomUint64(range);
}


static int64_t randInt64(int64_t min, int64_t max) {
    if (min >= max || ((uint64_t)max - (uint64_t)min) == UINT64_MAX) {
        return min;
    }

    uint64_t range = (uint64_t)max - (uint64_t)min + 1;
    return (int64_t)(min + generateRandomUint64(range));
}


static void decimal64Rand(Decimal64* result, const Decimal64* min, const Decimal64* max) {
    int64_t temp = 0;

    do {
        temp = randInt64(DECIMAL64_GET_VALUE(min), DECIMAL64_GET_VALUE(max));
    } while (temp < DECIMAL64_GET_VALUE(min) || temp > DECIMAL64_GET_VALUE(max));

    DECIMAL64_SET_VALUE(result, temp);
}


static void decimal128Rand(Decimal128* result, const Decimal128* min, const Decimal128* max) {
    int64_t  high   = 0;
    uint64_t low    = 0;
    Decimal128 temp = {0};

    int64_t minHigh = DECIMAL128_HIGH_WORD(min);
    int64_t maxHigh = DECIMAL128_HIGH_WORD(max);
    uint64_t minLow = DECIMAL128_LOW_WORD(min);
    uint64_t maxLow = DECIMAL128_LOW_WORD(max);

    do {
        // high byte
        high = randInt64(minHigh, maxHigh);

        // low byte
        if (high == minHigh && high == maxHigh) {
            low = randUint64(minLow, maxLow);
        } else if (high == minHigh) {
            low = randUint64(minLow, UINT64_MAX);
        } else if (high == maxHigh) {
            low = randUint64(0, maxLow);
        } else {
            low = randUint64(0, UINT64_MAX);
        }

        DECIMAL128_SET_HIGH_WORD(&temp, high);
        DECIMAL128_SET_LOW_WORD(&temp, low);

    } while (decimal128BCompare(&temp, min) < 0 || decimal128BCompare(&temp, max) > 0);

    *result = temp;
}


Decimal64 tmpDecimal64Impl(Field* field, int32_t angle, int32_t k) {
    (void)angle;
    (void)k;

    Decimal64 result = {0};
    decimal64Rand(&result, &field->decMin.dec64, &field->decMax.dec64);
    return result;
}


Decimal128 tmpDecimal128Impl(Field* field, int32_t angle, int32_t k) {
    (void)angle;
    (void)k;

    Decimal128 result = {0};
    decimal128Rand(&result, &field->decMin.dec128, &field->decMax.dec128);
    return result;
}


static int generateRandDataSQL(SSuperTable *stbInfo, char *sampleDataBuf,
                     int64_t bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag, int64_t loopBegin) {

    int64_t index = loopBegin;
    int angle = stbInfo->startTimestamp % 360; // 0 ~ 360
    for (int64_t k = 0; k < loop; ++k, ++index) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            if (field->none) {
                continue;
            }

            int n = 0;
            if (field->null) {
                n = snprintf(sampleDataBuf + pos, bufLen - pos, "null,");
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                    continue;
                }
            }
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool boolTmp = tmpBool(field);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s,", boolTmp ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint = tmpInt8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%d,", tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = tmpUint8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos, "%u,", utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = tmpInt16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = tmpUint16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32Impl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = tmpInt64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = tmpUint32Impl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp = tmpUint64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloatImpl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ =  tmpDoubleImpl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", double_);
                    break;
                }
                case TSDB_DATA_TYPE_DECIMAL: {
                    Decimal128 dec = tmpDecimal128Impl(field, angle, index);
                    int ret = decimal128ToString(&dec, field->precision, field->scale, sampleDataBuf + pos, bufLen - pos);
                    if (ret != 0) {
                        errorPrint("%s() LN%d precision: %d, scale: %d, high: %" PRId64 ", low: %" PRIu64 "\n",
                                __func__, __LINE__, field->precision, field->scale, DECIMAL128_HIGH_WORD(&dec), DECIMAL128_LOW_WORD(&dec));
                        return -1;
                    }
                    size_t decLen = strlen(sampleDataBuf + pos);
                    n = snprintf(sampleDataBuf + pos + decLen, bufLen - pos - decLen, ",");
                    n += decLen;
                    break;
                }
                case TSDB_DATA_TYPE_DECIMAL64: {
                    Decimal64 dec = tmpDecimal64Impl(field, angle, index);
                    int ret = decimal64ToString(&dec, field->precision, field->scale, sampleDataBuf + pos, bufLen - pos);
                    if (ret != 0) {
                        errorPrint("%s() LN%d precision: %d, scale: %d, value: %" PRId64 "\n",
                                __func__, __LINE__, field->precision, field->scale, DECIMAL64_GET_VALUE(&dec));
                        return -1;
                    }
                    size_t decLen = strlen(sampleDataBuf + pos);
                    n = snprintf(sampleDataBuf + pos + decLen, bufLen - pos - decLen, ",");
                    n += decLen;
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, index)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_BLOB: {
                    // field->length = 1024;
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, index)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos, "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_GEOMETRY: {
                    int   bufferSize = field->length + 1;
                    char *tmp = benchCalloc(1, bufferSize, false);
                    if (0 != tmpGeometry(tmp, stbInfo->iface, field, i)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos, "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos, fieldsSize, field);
                    if (n == -1) {
                        return -1;
                    }
                    pos += n;
                    goto skip_sql;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_sql:
        *(sampleDataBuf + pos - 1) = 0;
        angle += stbInfo->timestamp_step/stbInfo->angle_step;
        if (angle > 360) {
            angle -= 360;
        }
}

    return 0;
}

static int fillStmt(
    SSuperTable *stbInfo,
    char *sampleDataBuf,
    int64_t bufLen,
    int lenOfOneRow, BArray *fields,
    int64_t loop, bool tag, BArray *childCols, int64_t loopBegin) {
    int angle = stbInfo->startTimestamp % 360; // 0 ~ 360
    debugPrint("fillStml stbname=%s loop=%"PRId64" istag=%d  fieldsSize=%d\n", stbInfo->stbName, loop, tag, (int32_t)fields->size);
    int64_t index = loopBegin;
    for (int64_t k = 0; k < loop; ++k, ++index) {
        int64_t pos = k * lenOfOneRow;
        char* line = sampleDataBuf + pos;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            ChildField *childCol = NULL;
            if (childCols) {
                childCol = benchArrayGet(childCols, i);
            }
            int64_t n = 0;

            //
            if (childCol) {
                childCol->stmtData.is_null[k] = 0;
                childCol->stmtData.lengths[k] = field->length;
            } else {
                field->stmtData.is_null[k] = 0;
                field->stmtData.lengths[k] = field->length;
            }

            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool boolTmp = tmpBool(field);
                    if (childCol) {
                        ((bool *)childCol->stmtData.data)[k] = boolTmp;
                    } else {
                        ((bool *)field->stmtData.data)[k] = boolTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                 "%d,", boolTmp);
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyintTmp = tmpInt8Impl(field, index);
                    if (childCol) {
                        ((int8_t *)childCol->stmtData.data)[k] = tinyintTmp;
                    } else {
                        ((int8_t *)field->stmtData.data)[k] = tinyintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                 "%d,", tinyintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyintTmp = tmpUint8Impl(field, index);
                    if (childCol) {
                        ((uint8_t *)childCol->stmtData.data)[k] = utinyintTmp;
                    } else {
                        ((uint8_t *)field->stmtData.data)[k] = utinyintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%u,", utinyintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallintTmp = tmpInt16Impl(field, index);
                    if (childCol) {
                        ((int16_t *)childCol->stmtData.data)[k] = smallintTmp;
                    } else {
                        ((int16_t *)field->stmtData.data)[k] = smallintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallintTmp = tmpUint16Impl(field, index);
                    if (childCol) {
                        ((uint16_t *)childCol->stmtData.data)[k] = usmallintTmp;
                    } else {
                        ((uint16_t *)field->stmtData.data)[k] = usmallintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32Impl(field, i, angle, index);
                    if (childCol) {
                        ((int32_t *)childCol->stmtData.data)[k] = intTmp;
                    } else {
                        ((int32_t *)field->stmtData.data)[k] = intTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = tmpInt64Impl(field, angle, index);
                    if (childCol) {
                        ((int64_t *)childCol->stmtData.data)[k] = bigintTmp;
                    } else {
                        ((int64_t *)field->stmtData.data)[k] = bigintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = tmpUint32Impl(field, i, angle, index);
                    if (childCol) {
                        ((uint32_t *)childCol->stmtData.data)[k] = uintTmp;
                    } else {
                        ((uint32_t *)field->stmtData.data)[k] = uintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp = tmpUint64Impl(field, angle, index);
                    if (childCol) {
                        ((uint64_t *)childCol->stmtData.data)[k] = ubigintTmp;
                    } else {
                        ((uint64_t *)field->stmtData.data)[k] = ubigintTmp;
                    }
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloatImpl(field, i, angle, index);
                    if (childCol) {
                        ((float *)childCol->stmtData.data)[k] = floatTmp;
                    } else {
                        ((float *)field->stmtData.data)[k] = floatTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double doubleTmp = tmpDoubleImpl(field, angle, index);
                    if (childCol) {
                        ((double *)childCol->stmtData.data)[k] = doubleTmp;
                    } else {
                        ((double *)field->stmtData.data)[k] = doubleTmp;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", doubleTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, k)) {
                        free(tmp);
                        return -1;
                    }
                    if (childCol) {
                        snprintf((char *)childCol->stmtData.data
                                    + k * field->length,
                                 field->length,
                                "%s", tmp);
                    } else {
                        snprintf((char *)field->stmtData.data
                                    + k * field->length,
                                 field->length,
                                "%s", tmp);
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_BLOB: {
                    // field->length = 1024;
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, k)) {
                        free(tmp);
                        return -1;
                    }
                    if (childCol) {
                        snprintf((char *)childCol->stmtData.data + k * field->length, field->length, "%s", tmp);
                    } else {
                        snprintf((char *)field->stmtData.data + k * field->length, field->length, "%s", tmp);
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos, "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_GEOMETRY: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpGeometry(tmp, stbInfo->iface, field, k)) {
                        tmfree(tmp);
                        return -1;
                    }
                    if (childCol) {
                        snprintf((char *)childCol->stmtData.data
                                    + k * field->length,
                                 field->length,
                                "%s", tmp);
                    } else {
                        snprintf((char *)field->stmtData.data
                                    + k * field->length,
                                 field->length,
                                "%s", tmp);
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos, fieldsSize, field);
                    if (n == -1) {
                        return -1;
                    }
                    pos += n;
                    goto skip_stmt;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
        debugPrint(" k=%" PRId64 " pos=%" PRId64 " line=%s\n", k, pos, line);

skip_stmt:
        if (pos > 0)
            *(sampleDataBuf + pos - 1) = 0;
        angle += stbInfo->timestamp_step/stbInfo->angle_step;
        if (angle > 360) {
            angle -= 360;
        }

    }
    return 0;
}

static int generateRandDataStmtForChildTable(
    SSuperTable *stbInfo,
    char *sampleDataBuf,
    int64_t bufLen,
    int lenOfOneRow, BArray *fields,
    int64_t loop, BArray *childCols, int64_t loopBegin) {
    //  generateRandDataStmtForChildTable()
    for (int i = 0; i < fields->size; ++i) {
        Field *field = benchArrayGet(fields, i);
        ChildField *childField = benchArrayGet(childCols, i);
        if (field->type == TSDB_DATA_TYPE_BINARY
                || field->type == TSDB_DATA_TYPE_NCHAR) {
            childField->stmtData.data = benchCalloc(
                        1, loop * (field->length + 1), true);
        } else {
            childField->stmtData.data = benchCalloc(
                    1, loop * field->length, true);
        }

        // is_null
        childField->stmtData.is_null = benchCalloc(sizeof(char), loop, true);
        // lengths
        childField->stmtData.lengths = benchCalloc(sizeof(int32_t), loop, true);

        // log
        debugPrint("i=%d generateRandDataStmtForChildTable fields=%p %s malloc stmtData.data=%p\n", i, fields, field->name ,field->stmtData.data);

    }
    return fillStmt(
        stbInfo,
        sampleDataBuf,
        bufLen,
        lenOfOneRow, fields,
        loop, false, childCols, loopBegin);
}

static int generateRandDataStmt(
    SSuperTable *stbInfo,
    char *sampleDataBuf,
    int64_t bufLen,
    int lenOfOneRow, BArray *fields,
    int64_t loop, bool tag, int64_t loopBegin) {
    // generateRandDataStmt()
    for (int i = 0; i < fields->size; ++i) {
        Field *field = benchArrayGet(fields, i);
        if (field->stmtData.data == NULL) {
            // data
            if (field->type == TSDB_DATA_TYPE_BINARY
                    || field->type == TSDB_DATA_TYPE_NCHAR) {
                field->stmtData.data = benchCalloc(1, loop * (field->length + 1), true);
            } else {
                field->stmtData.data = benchCalloc(1, loop * field->length, true);
            }

            // is_null
            field->stmtData.is_null = benchCalloc(sizeof(char), loop, true);
            // lengths
            field->stmtData.lengths = benchCalloc(sizeof(int32_t), loop, true);

            // log
            debugPrint("i=%d generateRandDataStmt tag=%d fields=%p %s malloc stmtData.data=%p\n", i, tag, fields, field->name ,field->stmtData.data);
        }
    }

    return fillStmt(
        stbInfo,
        sampleDataBuf,
        bufLen,
        lenOfOneRow, fields,
        loop, tag, NULL, loopBegin);
}

static int generateRandDataSmlTelnet(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag, int64_t loopBegin) {
    int64_t index = loopBegin;
    int angle = stbInfo->startTimestamp % 360; // 0 ~ 360
    for (int64_t k = 0; k < loop; ++k, ++index) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        if (!tag) {
            fieldsSize = 1;
        }
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            int n = 0;
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool boolTmp = tmpBool(field);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                     "%s=%s ", field->name,
                                        boolTmp ? "true" : "false");
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s ", boolTmp ? "true" : "false");
                    }
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint = tmpInt8Impl(field, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%di8 ", field->name, tinyint);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos, "%di8 ", tinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = tmpUint8Impl(field, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%uu8 ", field->name, utinyint);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos, "%uu8 ", utinyint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = tmpInt16Impl(field, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%di16 ", field->name, smallint);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%di16 ", smallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = tmpUint16Impl(field, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%uu16 ", field->name, usmallint);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%uu16 ", usmallint);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32Impl(field, i, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%di32 ",
                                        field->name, intTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%di32 ", intTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = tmpInt64Impl(field, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                     "%s=%"PRId64"i64 ",
                                     field->name, bigintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos, "%"PRId64"i64 ", bigintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = tmpUint32Impl(field, i, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%s=%uu32 ",
                                        field->name, uintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos,
                                        "%uu32 ", uintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp = tmpUint64Impl(field, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                     "%s=%"PRIu64"u64 ",
                                     field->name, ubigintTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos, "%"PRIu64"u64 ", ubigintTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloatImpl(field, i, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%ff32 ", field->name, floatTmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos,
                                     bufLen - pos, "%ff32 ", floatTmp);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ = tmpDoubleImpl(field, angle, index);
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=%ff64 ", field->name, double_);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                     "%ff64 ", double_);
                    }
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, index)) {
                        tmfree(tmp);
                        return -1;
                    }
                    if (field->type == TSDB_DATA_TYPE_BINARY || field->type == TSDB_DATA_TYPE_VARBINARY) {
                        if (tag) {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "%s=L\"%s\" ",
                                           field->name, tmp);
                        } else {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "\"%s\" ", tmp);
                        }
                        if (n < 0 || n >= bufLen - pos) {
                            errorPrint("%s() LN%d snprintf overflow\n",
                                       __func__, __LINE__);
                            tmfree(tmp);
                            return -1;
                        } else {
                            pos += n;
                        }
                    } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
                        if (tag) {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                            "%s=L\"%s\" ",
                                           field->name, tmp);
                        } else {
                            n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                         "L\"%s\" ", tmp);
                        }
                        if (n < 0 || n >= bufLen - pos) {
                            errorPrint("%s() LN%d snprintf overflow\n",
                                       __func__, __LINE__);
                            tmfree(tmp);
                            return -1;
                        } else {
                            pos += n;
                        }
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_GEOMETRY: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpGeometry(tmp, stbInfo->iface, field, index)) {
                        tmfree(tmp);
                        return -1;
                    }
                    if (tag) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=L\"%s\" ",
                                        field->name, tmp);
                    } else {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "\"%s\" ", tmp);
                    }
                    if (n < 0 || n >= bufLen - pos) {
                        errorPrint("%s() LN%d snprintf overflow\n",
                                    __func__, __LINE__);
                        tmfree(tmp);
                        return -1;
                    } else {
                        pos += n;
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos, fieldsSize, field);
                    if (n == -1) {
                        return -1;
                    }
                    pos += n;
                    goto skip_telnet;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_telnet:
        *(sampleDataBuf + pos - 1) = 0;
        angle += stbInfo->timestamp_step/stbInfo->angle_step;
        if (angle > 360) {
            angle -= 360;
        }

    }

    return 0;
}

static int generateRandDataSmlJson(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag, int64_t loopBegin) {
    int64_t index = loopBegin;
    int angle = stbInfo->startTimestamp % 360; // 0 ~ 360
    for (int64_t k = 0; k < loop; ++k, ++index) {
        int64_t pos = k * lenOfOneRow;
        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            int n = 0;
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool boolTmp = tmpBool(field);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s,", boolTmp ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint = tmpInt8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = tmpUint8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos,
                                        bufLen - pos, "%u,", utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = tmpInt16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%d,", smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = tmpUint16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%u,", usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp = tmpInt32Impl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%d,", intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = tmpInt64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRId64",", bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = tmpUint32Impl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%u,", uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp = tmpUint64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                 bufLen - pos, "%"PRIu64",", ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloatImpl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double double_ = tmpDoubleImpl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%f,", double_);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, k)) {
                        free(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "'%s',", tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos, fieldsSize, field);
                    if (n == -1) {
                        return -1;
                    }
                    pos += n;
                    goto skip_json;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_json:
        *(sampleDataBuf + pos - 1) = 0;
        angle += stbInfo->timestamp_step/stbInfo->angle_step;
        if (angle > 360) {
            angle -= 360;
        }
    }

    return 0;
}

static int generateRandDataSmlLine(SSuperTable *stbInfo, char *sampleDataBuf,
                     int bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag, int64_t loopBegin) {
    int64_t index = loopBegin;
    int angle = stbInfo->startTimestamp % 360; // 0 ~ 360
    for (int64_t k = 0; k < loop; ++k, ++index) {
        int64_t pos = k * lenOfOneRow;
        int n = 0;
        if (tag) {
            n = snprintf(sampleDataBuf + pos,
                           bufLen - pos,
                           "%s,", stbInfo->stbName);
            if (n < 0 || n >= bufLen - pos) {
                errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
                return -1;
            } else {
                pos += n;
            }
        }

        int fieldsSize = fields->size;
        for (int i = 0; i < fieldsSize; ++i) {
            Field * field = benchArrayGet(fields, i);
            switch (field->type) {
                case TSDB_DATA_TYPE_BOOL: {
                    bool boolTmp = tmpBool(field);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos, "%s=%s,",
                                 field->name, boolTmp ? "true" : "false");
                    break;
                }
                case TSDB_DATA_TYPE_TINYINT: {
                    int8_t tinyint = tmpInt8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%di8,", field->name, tinyint);
                    break;
                }
                case TSDB_DATA_TYPE_UTINYINT: {
                    uint8_t utinyint = tmpUint8Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu8,", field->name, utinyint);
                    break;
                }
                case TSDB_DATA_TYPE_SMALLINT: {
                    int16_t smallint = tmpInt16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%di16,", field->name, smallint);
                    break;
                }
                case TSDB_DATA_TYPE_USMALLINT: {
                    uint16_t usmallint = tmpUint16Impl(field, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu16,",
                                    field->name, usmallint);
                    break;
                }
                case TSDB_DATA_TYPE_INT: {
                    int32_t intTmp;
                    if (tag) {
                        intTmp = tmpInt32ImplTag(field, i, index);
                    } else {
                        intTmp = tmpInt32Impl(field, i, angle, index);
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%di32,",
                                    field->name, intTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BIGINT: {
                    int64_t bigintTmp = tmpInt64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                 "%s=%"PRId64"i64,", field->name, bigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UINT: {
                    uint32_t uintTmp = tmpUint32Impl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=%uu32,", field->name, uintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_UBIGINT:
                case TSDB_DATA_TYPE_TIMESTAMP: {
                    uint64_t ubigintTmp = tmpUint64Impl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                 "%s=%"PRIu64"u64,", field->name, ubigintTmp);
                    break;
                }
                case TSDB_DATA_TYPE_FLOAT: {
                    float floatTmp = tmpFloatImpl(field, i, angle, index);
                    n = snprintf(sampleDataBuf + pos,
                                    bufLen - pos, "%s=%ff32,",
                                    field->name, floatTmp);
                    break;
                }
                case TSDB_DATA_TYPE_DOUBLE: {
                    double doubleTmp = tmpDoubleImpl(field, angle, index);
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                 "%s=%ff64,", field->name, doubleTmp);
                    break;
                }
                case TSDB_DATA_TYPE_BINARY:
                case TSDB_DATA_TYPE_VARBINARY:
                case TSDB_DATA_TYPE_NCHAR: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpStr(tmp, stbInfo->iface, field, k)) {
                        free(tmp);
                        return -1;
                    }
                    if (field->type == TSDB_DATA_TYPE_BINARY) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=\"%s\",",
                                       field->name, tmp);
                    } else if (field->type == TSDB_DATA_TYPE_NCHAR) {
                        n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                        "%s=L\"%s\",",
                                       field->name, tmp);
                    }
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_GEOMETRY: {
                    char *tmp = benchCalloc(1, field->length + 1, false);
                    if (0 != tmpGeometry(tmp, stbInfo->iface, field, index)) {
                        tmfree(tmp);
                        return -1;
                    }
                    n = snprintf(sampleDataBuf + pos, bufLen - pos,
                                    "%s=\"%s\",",
                                    field->name, tmp);
                    tmfree(tmp);
                    break;
                }
                case TSDB_DATA_TYPE_JSON: {
                    n = tmpJson(sampleDataBuf, bufLen, pos,
                                   fieldsSize, field);
                    if (n < 0 || n >= bufLen) {
                        errorPrint("%s() LN%d snprintf overflow\n",
                                   __func__, __LINE__);
                        return -1;
                    } else {
                        pos += n;
                    }
                    goto skip_line;
                }
            }
            if (TSDB_DATA_TYPE_JSON != field->type) {
                if (n < 0 || n >= bufLen - pos) {
                    errorPrint("%s() LN%d snprintf overflow\n",
                               __func__, __LINE__);
                    return -1;
                } else {
                    pos += n;
                }
            }
        }
skip_line:
        *(sampleDataBuf + pos - 1) = 0;
        angle += stbInfo->timestamp_step/stbInfo->angle_step;
        if (angle > 360) {
            angle -= 360;
        }

    }

    return 0;
}

static int generateRandDataSml(SSuperTable *stbInfo, char *sampleDataBuf,
                     int64_t bufLen,
                      int lenOfOneRow, BArray * fields, int64_t loop,
                      bool tag, int64_t loopBegin) {
    int     protocol = stbInfo->lineProtocol;

    switch (protocol) {
        case TSDB_SML_LINE_PROTOCOL:
            return generateRandDataSmlLine(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
        case TSDB_SML_TELNET_PROTOCOL:
            return generateRandDataSmlTelnet(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
        default:
            return generateRandDataSmlJson(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
    }

    return -1;
}

int generateRandData(SSuperTable *stbInfo, char *sampleDataBuf,
                     int64_t bufLen,
                     int lenOfOneRow, BArray *fields,
                     int64_t loop,
                     bool tag, BArray *childCols, int64_t loopBegin) {
    int     iface = stbInfo->iface;
    switch (iface) {
        case TAOSC_IFACE:
            return generateRandDataSQL(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
        // REST
        case REST_IFACE:
            return generateRandDataSQL(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
        case STMT_IFACE:
        case STMT2_IFACE:
            if (childCols) {
                return generateRandDataStmtForChildTable(stbInfo,
                                                         sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop,
                                                         childCols, loopBegin);
            } else {
                return generateRandDataStmt(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
            }
        case SML_IFACE:
        case SML_REST_IFACE: // REST
            return generateRandDataSml(stbInfo, sampleDataBuf,
                                    bufLen, lenOfOneRow, fields, loop, tag, loopBegin);
        default:
            errorPrint("Unknown iface: %d\n", iface);
            break;
    }

    return -1;
}

static BArray *initChildCols(int colsSize) {
    BArray *childCols = benchArrayInit(colsSize,
                                       sizeof(ChildField));
    for (int col = 0; col < colsSize; col++) {
        ChildField *childCol = benchCalloc(
                1, sizeof(ChildField), true);
        benchArrayPush(childCols, childCol);
    }
    return childCols;
}

int prepareSampleData(SDataBase* database, SSuperTable* stbInfo) {
    stbInfo->lenOfCols = accumulateRowLen(stbInfo->cols, stbInfo->iface);
    stbInfo->lenOfTags = accumulateRowLen(stbInfo->tags, stbInfo->iface);
    if (stbInfo->useTagTableName) {
        // add tag table name length
        stbInfo->lenOfTags += TSDB_TABLE_NAME_LEN + 1; // +1 for comma
    }
    if (stbInfo->partialColNum != 0
            && ((stbInfo->iface == TAOSC_IFACE
                || stbInfo->iface == REST_IFACE))) {
        // check valid
        if(stbInfo->partialColFrom >= stbInfo->cols->size) {
            stbInfo->partialColFrom = 0;
            infoPrint("stbInfo->partialColFrom(%d) is large than stbInfo->cols->size(%zd) \n ",stbInfo->partialColFrom,stbInfo->cols->size);
        }

        if (stbInfo->partialColFrom + stbInfo->partialColNum > stbInfo->cols->size) {
            stbInfo->partialColNum = stbInfo->cols->size - stbInfo->partialColFrom ;
        }

        if(stbInfo->partialColNum < stbInfo->cols->size) {
            stbInfo->partialColNameBuf =
                    benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
            int pos = 0;
            int n;
            n = snprintf(stbInfo->partialColNameBuf + pos,
                            TSDB_MAX_ALLOWED_SQL_LEN - pos, "%s",
                            stbInfo->primaryKeyName);
            if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - pos) {
                errorPrint("%s() LN%d snprintf overflow\n",
                           __func__, __LINE__);
            } else {
                pos += n;
            }
            for (int i = stbInfo->partialColFrom; i < stbInfo->partialColFrom + stbInfo->partialColNum; ++i) {
                Field * col = benchArrayGet(stbInfo->cols, i);
                n = snprintf(stbInfo->partialColNameBuf+pos,
                                TSDB_MAX_ALLOWED_SQL_LEN - pos,
                               ",%s", col->name);
                if (n < 0 || n > TSDB_MAX_ALLOWED_SQL_LEN - pos) {
                    errorPrint("%s() LN%d snprintf overflow at %d\n",
                               __func__, __LINE__, i);
                } else {
                    pos += n;
                }
            }

            // first part set noen
            for (uint32_t i = 0; i < stbInfo->partialColFrom; ++i) {
                Field * col = benchArrayGet(stbInfo->cols, i);
                col->none = true;
            }
            // last part set none
            for (uint32_t i = stbInfo->partialColFrom + stbInfo->partialColNum; i < stbInfo->cols->size; ++i) {
                Field * col = benchArrayGet(stbInfo->cols, i);
                col->none = true;
            }
            debugPrint("partialColNameBuf: %s\n",
                       stbInfo->partialColNameBuf);
        }
    } else {
        stbInfo->partialColNum = stbInfo->cols->size;
    }
    stbInfo->sampleDataBuf =
            benchCalloc(
                1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
    infoPrint(
              "generate stable<%s> columns data with lenOfCols<%u> * "
              "prepared_rand<%" PRIu64 ">\n",
              stbInfo->stbName, stbInfo->lenOfCols, g_arguments->prepared_rand);
    if (stbInfo->random_data_source) {
        if (g_arguments->mistMode) {
            infoPrint("Each child table using different random prepare data pattern. need "
            "all memory(%d M) = childs(%"PRId64") * prepared_rand(%"PRId64") * lenOfCols(%d) \n",
            (int32_t)(stbInfo->childTblCount*g_arguments->prepared_rand*stbInfo->lenOfCols/1024/1024),
            stbInfo->childTblCount, g_arguments->prepared_rand, stbInfo->lenOfCols);
            for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
                SChildTable *childTbl = stbInfo->childTblArray[child];
                if (STMT_IFACE == stbInfo->iface || STMT2_IFACE == stbInfo->iface) {
                    childTbl->childCols = initChildCols(stbInfo->cols->size);
                }
                childTbl->sampleDataBuf =
                    benchCalloc(
                        1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
                if (generateRandData(stbInfo, childTbl->sampleDataBuf,
                             stbInfo->lenOfCols*g_arguments->prepared_rand,
                             stbInfo->lenOfCols,
                             stbInfo->cols,
                             g_arguments->prepared_rand,
                             false, childTbl->childCols, 0)) {
                    errorPrint("Failed to generate data for table %s\n",
                               childTbl->name);
                    return -1;
                }
                childTbl->useOwnSample = true;
            }
        } else {
            if (generateRandData(stbInfo, stbInfo->sampleDataBuf,
                             stbInfo->lenOfCols*g_arguments->prepared_rand,
                             stbInfo->lenOfCols,
                             stbInfo->cols,
                             g_arguments->prepared_rand,
                             false, NULL, 0)) {
                return -1;
            }
        }
        debugPrint("sampleDataBuf: %s\n", stbInfo->sampleDataBuf);
    } else {
        if (stbInfo->useSampleTs) {
            if (getAndSetRowsFromCsvFile(
                    stbInfo->sampleFile, &stbInfo->insertRows)) {
                return -1;
            }
        }
        if (generateSampleFromCsv(stbInfo->sampleDataBuf,
                                        stbInfo->sampleFile, NULL, stbInfo->lenOfCols,
                                        g_arguments->prepared_rand)) {
            errorPrint("Failed to generate sample from csv file %s\n",
                    stbInfo->sampleFile);
            return -1;
        }

        debugPrint("sampleDataBuf: %s\n", stbInfo->sampleDataBuf);
        if (stbInfo->childTblSample) {
            if (NULL == strstr(stbInfo->childTblSample, "XXXX")) {
                errorPrint("Child table sample file pattern has no %s\n",
                   "XXXX");
                return -1;
            }
            for (int64_t child = 0; child < stbInfo->childTblCount; child++) {
                char sampleFilePath[MAX_PATH_LEN] = {0};
                getSampleFileNameByPattern(sampleFilePath, stbInfo, child);
                if (0 != access(sampleFilePath, F_OK)) {
                    continue;
                }
                SChildTable *childTbl = stbInfo->childTblArray[child];
                infoPrint("Found specified sample file for table %s\n",
                          childTbl->name);
                if (getAndSetRowsFromCsvFile(sampleFilePath,
                                             &(childTbl->insertRows))) {
                    errorPrint("Failed to get sample data rows for table %s\n",
                          childTbl->name);
                    return -1;
                }

                childTbl->sampleDataBuf =
                    benchCalloc(
                        1, stbInfo->lenOfCols*g_arguments->prepared_rand, true);
                if (generateSampleFromCsv(
                            childTbl->sampleDataBuf,
                            sampleFilePath,
                            NULL,
                            stbInfo->lenOfCols,
                            g_arguments->prepared_rand)) {
                    errorPrint("Failed to generate sample from file "
                                   "for child table %"PRId64"\n",
                                    child);
                    return -1;
                }
                if (STMT_IFACE == stbInfo->iface || STMT2_IFACE == stbInfo->iface) {
                    childTbl->childCols = initChildCols(stbInfo->cols->size);
                }
                childTbl->useOwnSample = true;
                debugPrint("sampleDataBuf: %s\n", childTbl->sampleDataBuf);
            }
        }
    }

    // rest need convert server ip
    if (isRest(stbInfo->iface)) {
        if ( 0 != convertServAddr(
                stbInfo->iface,
                stbInfo->tcpTransfer,
                stbInfo->lineProtocol)) {
            return -1;
        }
    }
    return 0;
}

int64_t getTSRandTail(int64_t timeStampStep, int32_t seq, int disorderRatio,
                      int disorderRange) {
    int64_t randTail = timeStampStep * seq;
    if (disorderRatio > 0) {
        int rand_num = taosRandom() % 100;
        if (rand_num < disorderRatio) {
            randTail = (randTail + (taosRandom() % disorderRange + 1)) * (-1);
        }
    }
    return randTail;
}

uint32_t bindParamBatch(threadInfo *pThreadInfo,
                        uint32_t batch, int64_t startTime, int64_t pos,
                        SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n, int64_t *delay2, int64_t *delay3) {
    TAOS_STMT   *stmt = pThreadInfo->conn->stmt;
    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint32_t     columnCount = stbInfo->cols->size;

    //if (!pThreadInfo->stmtBind || stbInfo->interlaceRows > 0 ) {
    {
        pThreadInfo->stmtBind = true;
        memset(pThreadInfo->bindParams, 0,
            (sizeof(TAOS_MULTI_BIND) * (columnCount + 1)));

        for (int c = 0; c <= columnCount; c++) {
            TAOS_MULTI_BIND *param =
                (TAOS_MULTI_BIND *)(pThreadInfo->bindParams +
                                    sizeof(TAOS_MULTI_BIND) * c);
            char data_type;
            if (c == 0) {
                data_type = TSDB_DATA_TYPE_TIMESTAMP;
                param->buffer_length = sizeof(int64_t);
                if (stbInfo->useSampleTs) {
                    param->buffer = pThreadInfo->bind_ts_array + pos;
                } else {
                    param->buffer = pThreadInfo->bind_ts_array;
                }
            } else {
                Field *col = benchArrayGet(stbInfo->cols, c - 1);
                data_type = col->type;
                if (childTbl->useOwnSample) {
                    ChildField *childCol = benchArrayGet(childTbl->childCols, c-1);
                    param->buffer = (char *)childCol->stmtData.data + pos * col->length;
                    param->is_null = childCol->stmtData.is_null + pos;
                } else {
                    param->buffer = (char *)col->stmtData.data + pos * col->length;
                    param->is_null = col->stmtData.is_null + pos;
                }
                param->buffer_length = col->length;
                debugPrint("col[%d]: type: %s, len: %d\n", c,
                        convertDatatypeToString(data_type),
                        col->length);
            }
            param->buffer_type = data_type;
            param->length = pThreadInfo->lengths[c];

            for (int b = 0; b < batch; b++) {
                param->length[b] = (int32_t)param->buffer_length;
            }
            param->num = batch;
        }
    }

    if (!stbInfo->useSampleTs) {
        // set first column ts array values
        for (uint32_t k = 0; k < batch; k++) {
            /* columnCount + 1 (ts) */
            if (stbInfo->disorderRatio) {
                *(pThreadInfo->bind_ts_array + k) =
                    startTime + getTSRandTail(stbInfo->timestamp_step, *n,
                                            stbInfo->disorderRatio,
                                            stbInfo->disorderRange);
            } else {
                *(pThreadInfo->bind_ts_array + k) = startTime + stbInfo->timestamp_step * (*n);
            }

            // check n need add
            if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
                *n = *n + 1;
            }
        }
    }

    /*
      1. The last batch size may be smaller than the previous batch size.
      2. When inserting another table, the batch size reset again(bigger than lastBatchSize)
    */
    int lastBatchSize = ((TAOS_MULTI_BIND *) pThreadInfo->bindParams)->num;
    if (batch != lastBatchSize) {
        for (int c = 0; c < columnCount + 1; c++) {
            TAOS_MULTI_BIND *param =
                    (TAOS_MULTI_BIND *) (pThreadInfo->bindParams +
                    sizeof(TAOS_MULTI_BIND) * c);
            param->num = batch;
        }
    }

    int64_t start = toolsGetTimestampUs();
    if (taos_stmt_bind_param_batch(
            stmt, (TAOS_MULTI_BIND *)pThreadInfo->bindParams)) {
        errorPrint("taos_stmt_bind_param_batch() failed! reason: %s\n",
                   taos_stmt_errstr(stmt));
        return 0;
    }
    *delay2 += toolsGetTimestampUs() - start;

    if(stbInfo->autoTblCreating) {
        start = toolsGetTimestampUs();
        if (taos_stmt_add_batch(pThreadInfo->conn->stmt) != 0) {
            errorPrint("taos_stmt_add_batch() failed! reason: %s\n",
                    taos_stmt_errstr(pThreadInfo->conn->stmt));
            return 0;
        }
        if(delay3) {
            *delay3 += toolsGetTimestampUs() - start;
        }
    }
    return batch;
}

void generateSmlJsonTags(tools_cJSON *tagsList,
                         char **sml_tags_json_array,
                         SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    tools_cJSON * tags = tools_cJSON_CreateObject();
    char *  tbName = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64,
             stbInfo->childTblPrefix, start_table_from + tbSeq);
    char *tagName = benchCalloc(1, TSDB_MAX_TAGS, true);
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field * tag = benchArrayGet(stbInfo->tags, i);
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (tag->type) {
            case TSDB_DATA_TYPE_BOOL: {
                bool boolTmp = tmpBool(tag);
                tools_cJSON_AddNumberToObject(tags, tagName, boolTmp);
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float floatTmp = tmpFloat(tag);
                tools_cJSON_AddNumberToObject(tags, tagName, floatTmp);
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double doubleTmp = tmpDouble(tag);
                tools_cJSON_AddNumberToObject(tags, tagName, doubleTmp);
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)benchCalloc(tag->length + 1, 1, false);
                rand_string(buf, tag->length, g_arguments->chinese);
                if (tag->type == TSDB_DATA_TYPE_BINARY || tag->type == TSDB_DATA_TYPE_VARBINARY) {
                    tools_cJSON_AddStringToObject(tags, tagName, buf);
                } else {
                    tools_cJSON_AddStringToObject(tags, tagName, buf);
                }
                tmfree(buf);
                break;
            }
            default: {
                int tagTmp = tag->min;
                if (tag->max != tag->min) {
                    tagTmp += (taosRandom() % (tag->max - tag->min));
                }
                tools_cJSON_AddNumberToObject(
                        tags, tagName, tagTmp);
                break;
            }
        }
    }
    tools_cJSON_AddItemToArray(tagsList, tags);
    debugPrintJsonNoTime(tags);
    char *tags_text = tools_cJSON_PrintUnformatted(tags);
    debugPrintNoTimestamp("%s() LN%d, No.%"PRIu64" table's tags text: %s\n",
                          __func__, __LINE__,
                          start_table_from + tbSeq, tags_text);
    sml_tags_json_array[tbSeq] = tags_text;
    tmfree(tagName);
    tmfree(tbName);
}

void generateSmlTaosJsonTags(tools_cJSON *tagsList, SSuperTable *stbInfo,
                            uint64_t start_table_from, int tbSeq) {
    tools_cJSON * tags = tools_cJSON_CreateObject();
    char *  tbName = benchCalloc(1, TSDB_TABLE_NAME_LEN, true);
    snprintf(tbName, TSDB_TABLE_NAME_LEN, "%s%" PRIu64,
             stbInfo->childTblPrefix, tbSeq + start_table_from);
    tools_cJSON_AddStringToObject(tags, "id", tbName);
    char *tagName = benchCalloc(1, TSDB_MAX_TAGS, true);
    for (int i = 0; i < stbInfo->tags->size; i++) {
        Field * tag = benchArrayGet(stbInfo->tags, i);
        tools_cJSON *tagObj = tools_cJSON_CreateObject();
        snprintf(tagName, TSDB_MAX_TAGS, "t%d", i);
        switch (tag->type) {
            case TSDB_DATA_TYPE_BOOL: {
                bool boolTmp = tmpBool(tag);
                tools_cJSON_AddBoolToObject(tagObj, "value", boolTmp);
                tools_cJSON_AddStringToObject(tagObj, "type", "bool");
                break;
            }
            case TSDB_DATA_TYPE_FLOAT: {
                float floatTmp = tmpFloat(tag);
                tools_cJSON_AddNumberToObject(tagObj, "value", floatTmp);
                tools_cJSON_AddStringToObject(tagObj, "type", "float");
                break;
            }
            case TSDB_DATA_TYPE_DOUBLE: {
                double doubleTmp = tmpDouble(tag);
                tools_cJSON_AddNumberToObject(tagObj, "value", doubleTmp);
                tools_cJSON_AddStringToObject(tagObj, "type", "double");
                break;
            }

            case TSDB_DATA_TYPE_BINARY:
            case TSDB_DATA_TYPE_VARBINARY:
            case TSDB_DATA_TYPE_NCHAR: {
                char *buf = (char *)benchCalloc(tag->length + 1, 1, false);
                rand_string(buf, tag->length, g_arguments->chinese);
                if (tag->type == TSDB_DATA_TYPE_BINARY || tag->type == TSDB_DATA_TYPE_VARBINARY) {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "binary");
                } else {
                    tools_cJSON_AddStringToObject(tagObj, "value", buf);
                    tools_cJSON_AddStringToObject(tagObj, "type", "nchar");
                }
                tmfree(buf);
                break;
            }
            default: {
                int64_t tagTmp = tag->min;
                if (tag->max != tag->min) {
                    tagTmp += (taosRandom() % (tag->max - tag->min));
                }
                tools_cJSON_AddNumberToObject(tagObj, "value", tagTmp);
                        tools_cJSON_AddStringToObject(tagObj, "type",
                                        convertDatatypeToString(tag->type));
                break;
            }
        }
        tools_cJSON_AddItemToObject(tags, tagName, tagObj);
    }
    tools_cJSON_AddItemToArray(tagsList, tags);
    tmfree(tagName);
    tmfree(tbName);
}

void generateSmlJsonValues(
        char **sml_json_value_array, SSuperTable *stbInfo, int tableSeq) {
    char *value_buf = NULL;
    Field* col = benchArrayGet(stbInfo->cols, 0);
    int len_key = strlen("\"value\":,");
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL: {
            bool boolTmp = tmpBool(col);
            value_buf = benchCalloc(len_key + 6, 1, true);
            snprintf(value_buf, len_key + 6,
                     "\"value\":%s,", boolTmp?"true":"false");
            break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
            value_buf = benchCalloc(len_key + 20, 1, true);
            float floatTmp = tmpFloat(col);
            snprintf(value_buf, len_key + 20,
                     "\"value\":%f,", floatTmp);
            break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
            value_buf = benchCalloc(len_key + 40, 1, true);
            double doubleTmp = tmpDouble(col);
            snprintf(
                value_buf, len_key + 40, "\"value\":%f,", doubleTmp);
            break;
        }
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            value_buf = benchCalloc(len_key + col->length + 3, 1, true);
            snprintf(value_buf, len_key + col->length + 3,
                     "\"value\":\"%s\",", buf);
            tmfree(buf);
            break;
        }
        case TSDB_DATA_TYPE_GEOMETRY: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            tmpGeometry(buf, stbInfo->iface, col, 0);
            value_buf = benchCalloc(len_key + col->length + 3, 1, true);
            snprintf(value_buf, len_key + col->length + 3,
                     "\"value\":\"%s\",", buf);
            tmfree(buf);
            break;
        }
        default: {
            value_buf = benchCalloc(len_key + 20, 1, true);
            double doubleTmp = tmpDouble(col);
            snprintf(value_buf, len_key + 20, "\"value\":%f,", doubleTmp);
            break;
        }
    }
    sml_json_value_array[tableSeq] = value_buf;
}

void generateSmlJsonCols(tools_cJSON *array, tools_cJSON *tag,
                         SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    tools_cJSON * record = tools_cJSON_CreateObject();
    tools_cJSON_AddNumberToObject(record, "timestamp", (double)timestamp);
    Field* col = benchArrayGet(stbInfo->cols, 0);
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL: {
            bool boolTmp = tmpBool(col);
            tools_cJSON_AddBoolToObject(record, "value", boolTmp);
            break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
            float floatTmp = tmpFloat(col);
            tools_cJSON_AddNumberToObject(record, "value", floatTmp);
            break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
            double doubleTmp = tmpDouble(col);
            tools_cJSON_AddNumberToObject(record, "value", doubleTmp);
            break;
        }
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            if (col->type == TSDB_DATA_TYPE_BINARY) {
                tools_cJSON_AddStringToObject(record, "value", buf);
            } else {
                tools_cJSON_AddStringToObject(record, "value", buf);
            }
            tmfree(buf);
            break;
        }
        case TSDB_DATA_TYPE_GEOMETRY: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            tmpGeometry(buf, stbInfo->iface, col, 0);
            tools_cJSON_AddStringToObject(record, "value", buf);
            tmfree(buf);
            break;
        }
        default: {
            double doubleTmp = tmpDouble(col);
            tools_cJSON_AddNumberToObject(record, "value", doubleTmp);
            break;
        }
    }
    tools_cJSON_AddItemToObject(record, "tags", tag);
    tools_cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    tools_cJSON_AddItemToArray(array, record);
}

void generateSmlTaosJsonCols(tools_cJSON *array, tools_cJSON *tag,
                         SSuperTable *stbInfo,
                            uint32_t time_precision, int64_t timestamp) {
    tools_cJSON * record = tools_cJSON_CreateObject();
    tools_cJSON * ts = tools_cJSON_CreateObject();
    tools_cJSON_AddNumberToObject(ts, "value", (double)timestamp);
    if (time_precision == TSDB_SML_TIMESTAMP_MILLI_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ms");
    } else if (time_precision == TSDB_SML_TIMESTAMP_MICRO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "us");
    } else if (time_precision == TSDB_SML_TIMESTAMP_NANO_SECONDS) {
        tools_cJSON_AddStringToObject(ts, "type", "ns");
    }
    tools_cJSON *value = tools_cJSON_CreateObject();
    Field* col = benchArrayGet(stbInfo->cols, 0);
    switch (col->type) {
        case TSDB_DATA_TYPE_BOOL: {
            bool boolTmp = tmpBool(col);
            tools_cJSON_AddBoolToObject(value, "value", boolTmp);
            tools_cJSON_AddStringToObject(value, "type", "bool");
            break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
            float floatTmp = tmpFloat(col);
            tools_cJSON_AddNumberToObject(value, "value", floatTmp);
            tools_cJSON_AddStringToObject(value, "type", "float");
            break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
            double dblTmp = tmpDouble(col);
            tools_cJSON_AddNumberToObject(value, "value", dblTmp);
            tools_cJSON_AddStringToObject(value, "type", "double");
            break;
        }
        case TSDB_DATA_TYPE_BINARY:
        case TSDB_DATA_TYPE_VARBINARY:
        case TSDB_DATA_TYPE_NCHAR: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            rand_string(buf, col->length, g_arguments->chinese);
            if (col->type == TSDB_DATA_TYPE_BINARY || col->type == TSDB_DATA_TYPE_VARBINARY) {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "binary");
            } else {
                tools_cJSON_AddStringToObject(value, "value", buf);
                tools_cJSON_AddStringToObject(value, "type", "nchar");
            }
            tmfree(buf);
            break;
        }
        case TSDB_DATA_TYPE_GEOMETRY: {
            char *buf = (char *)benchCalloc(col->length + 1, 1, false);
            tmpGeometry(buf, stbInfo->iface, col, 0);
            tools_cJSON_AddStringToObject(value, "value", buf);
            tools_cJSON_AddStringToObject(value, "type", "geometry");
            tmfree(buf);
        }
        default: {
            double dblTmp = (double)col->min;
            if (col->max != col->min) {
                dblTmp += (double)((taosRandom() % (col->max - col->min)));
            }
            tools_cJSON_AddNumberToObject(value, "value", dblTmp);
            tools_cJSON_AddStringToObject(
                    value, "type", convertDatatypeToString(col->type));
            break;
        }
    }
    tools_cJSON_AddItemToObject(record, "timestamp", ts);
    tools_cJSON_AddItemToObject(record, "value", value);
    tools_cJSON_AddItemToObject(record, "tags", tag);
    tools_cJSON_AddStringToObject(record, "metric", stbInfo->stbName);
    tools_cJSON_AddItemToArray(array, record);
}

// generateTag data from random or csv file
bool generateTagData(SSuperTable *stbInfo, char *buf, int64_t cnt, FILE* csv, BArray* tagsStmt, int64_t loopBegin) {
    if(csv) {
        if (generateSampleFromCsv(
                buf, NULL, csv,
                stbInfo->lenOfTags,
                cnt)) {
            return false;
        }
    } else {
        if (generateRandData(stbInfo,
                            buf,
                            cnt * stbInfo->lenOfTags,
                            stbInfo->lenOfTags,
                            tagsStmt ? tagsStmt : stbInfo->tags,
                            cnt, true, NULL, loopBegin)) {
            errorPrint("Generate Tag Rand Data Failed. stb=%s\n", stbInfo->stbName);
            return false;
        }
    }

    return true;
}

static int seekFromCsv(FILE* fp, int64_t seek) {
    size_t  n = 0;
    char *  line = NULL;

    if (seek > 0) {
        for (size_t i = 0; i < seek; i++){
            ssize_t readLen = 0;
#if defined(WIN32) || defined(WIN64)
            toolsGetLineFile(&line, &n, fp);
            readLen = n;
            if (0 == readLen) {
#else
            readLen = getline(&line, &n, fp);
            if (-1 == readLen) {
#endif
                if (0 != fseek(fp, 0, SEEK_SET)) {
                    return -1;
                }
                continue;
            }
        }
    }

    tmfree(line);
    infoPrint("seek data from csv file, seek rows=%" PRId64 "\n", seek);
    return 0;
}

// open tag from csv file
FILE* openTagCsv(SSuperTable* stbInfo, uint64_t seek) {
    FILE* csvFile = NULL;
    if (stbInfo->tagsFile[0] != 0) {
        csvFile = fopen(stbInfo->tagsFile, "r");
        if (csvFile == NULL) {
            errorPrint("Failed to open tag sample file: %s, reason:%s\n", stbInfo->tagsFile, strerror(errno));
            return NULL;
        }

        if (seekFromCsv(csvFile, seek)) {
            fclose(csvFile);
            errorPrint("Failed to seek csv file: %s, reason:%s\n", stbInfo->tagsFile, strerror(errno));
            return NULL;

        }
        infoPrint("open tag csv file :%s \n", stbInfo->tagsFile);

    }
    return csvFile;
}

//
// STMT2 bind cols param progressive
//
uint32_t bindVColsProgressive(TAOS_STMT2_BINDV *bindv, int32_t tbIndex,
                 threadInfo *pThreadInfo,
                 uint32_t batch, int64_t startTime, int64_t pos,
                 SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n) {

    SSuperTable *stbInfo = pThreadInfo->stbInfo;
    uint32_t     columnCount = stbInfo->cols->size;

    // clear
    memset(pThreadInfo->bindParams, 0, sizeof(TAOS_STMT2_BIND) * (columnCount + 1));
    debugPrint("stmt2 bindVColsProgressive child=%s batch=%d pos=%" PRId64 "\n", childTbl->name, batch, pos);
    // loop cols
    for (int c = 0; c <= columnCount; c++) {
        // des
        TAOS_STMT2_BIND *param = (TAOS_STMT2_BIND *)(pThreadInfo->bindParams + sizeof(TAOS_STMT2_BIND) * c);
        char data_type;
        int32_t length = 0;
        if (c == 0) {
            data_type = TSDB_DATA_TYPE_TIMESTAMP;
            if (stbInfo->useSampleTs) {
                param->buffer = pThreadInfo->bind_ts_array + pos;
            } else {
                param->buffer = pThreadInfo->bind_ts_array;
            }
            length = sizeof(int64_t);
        } else {
            Field *col = benchArrayGet(stbInfo->cols, c - 1);
            data_type = col->type;
            length    = col->length;
            if (childTbl->useOwnSample) {
                ChildField *childCol = benchArrayGet(childTbl->childCols, c-1);
                param->buffer = (char *)childCol->stmtData.data + pos * col->length;
                param->is_null = childCol->stmtData.is_null + pos;
            } else {
                param->buffer = (char *)col->stmtData.data + pos * col->length;
                param->is_null = col->stmtData.is_null + pos;
            }
            debugPrint("col[%d]: type: %s, len: %d\n", c,
                    convertDatatypeToString(data_type),
                    col->length);
        }
        param->buffer_type = data_type;
        param->length = pThreadInfo->lengths[c];

        for (int b = 0; b < batch; b++) {
            param->length[b] = length;
        }
        param->num = batch;
    }

    // ts key
    if (!stbInfo->useSampleTs) {
        // set first column ts array values
        for (uint32_t k = 0; k < batch; k++) {
            /* columnCount + 1 (ts) */
            if (stbInfo->disorderRatio) {
                *(pThreadInfo->bind_ts_array + k) =
                    startTime + getTSRandTail(stbInfo->timestamp_step, *n,
                                            stbInfo->disorderRatio,
                                            stbInfo->disorderRange);
            } else {
                *(pThreadInfo->bind_ts_array + k) = startTime + stbInfo->timestamp_step * (*n);
            }

            // check n need add
            if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
                *n = *n + 1;
            }
        }
    }

    // set to bindv (only one table, so always is 0 index table)
    bindv->bind_cols[tbIndex] = (TAOS_STMT2_BIND *)pThreadInfo->bindParams;
    return batch;
}


//
// STMT2 bind tags param progressive
//
uint32_t bindVTags(TAOS_STMT2_BINDV *bindv, int32_t tbIndex, int32_t w, BArray* fields) {

    TAOS_STMT2_BIND *tagsTb = bindv->tags[tbIndex];

    // loop
    for (int32_t i = 0; i < fields->size; i++) {
        Field* field = benchArrayGet(fields, i);

        // covert field data to bind struct
        tagsTb[i].buffer      = (char *)(field->stmtData.data) + field->length * w ;
        tagsTb[i].buffer_type = field->type;
        tagsTb[i].is_null     = field->stmtData.is_null;
        if (IS_VAR_DATA_TYPE(field->type)) {
            // only var set length
            tagsTb[i].length  = field->stmtData.lengths;
        }

        // tag always one line
        tagsTb[i].num = 1;
    }

    return 1;
}

//
// STMT2 bind cols param progressive
//
uint32_t bindVColsInterlace(TAOS_STMT2_BINDV *bindv, int32_t tbIndex,
                 threadInfo *pThreadInfo,
                 uint32_t batch, int64_t startTime, int64_t pos,
                 SChildTable *childTbl, int32_t *pkCur, int32_t *pkCnt, int32_t *n) {
    // count
    bindv->count += 1;
    // info
    SSuperTable *stbInfo    = pThreadInfo->stbInfo;
    TAOS_STMT2_BIND *colsTb = bindv->bind_cols[tbIndex];
    BArray* fields          = stbInfo->cols;


    // loop
    for (int32_t i = 0; i < fields->size + 1; i++) {
        // col bind
        if (i == 0) {
            // ts
            colsTb[i].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
            colsTb[i].length      = pThreadInfo->lengths[0];
            for (int32_t j = 0; j < batch; j++) {
                colsTb[i].length[j] = sizeof(int64_t);
            }
            if (stbInfo->useSampleTs) {
                colsTb[i].buffer = pThreadInfo->bind_ts_array + pos;
            } else {
                colsTb[i].buffer = pThreadInfo->bind_ts_array;
            }
            // no need set is_null for main key
        } else {
            Field* field = benchArrayGet(fields, i - 1);
            colsTb[i].buffer_type = field->type;

            if (childTbl->useOwnSample) {
                ChildField *childCol = benchArrayGet(childTbl->childCols, i - 1);
                colsTb[i].buffer  = (char *)childCol->stmtData.data + pos * field->length;
                colsTb[i].is_null = childCol->stmtData.is_null + pos;
                colsTb[i].length  = childCol->stmtData.lengths + pos;
            } else {
                colsTb[i].buffer  = (char *)field->stmtData.data + pos * field->length;
                colsTb[i].is_null = field->stmtData.is_null + pos;
                colsTb[i].length  = field->stmtData.lengths + pos;
            }
        }

        // set batch
        colsTb[i].num = batch;
    }

    // ts key
    if (!stbInfo->useSampleTs) {
        // set first column ts array values
        for (uint32_t k = 0; k < batch; k++) {
            /* columnCount + 1 (ts) */
            if (stbInfo->disorderRatio) {
                *(pThreadInfo->bind_ts_array + k) =
                    startTime + getTSRandTail(stbInfo->timestamp_step, *n,
                                            stbInfo->disorderRatio,
                                            stbInfo->disorderRange);
            } else {
                *(pThreadInfo->bind_ts_array + k) = startTime + stbInfo->timestamp_step * (*n);
            }

            // check n need add
            if (!stbInfo->primary_key || needChangeTs(stbInfo, pkCur, pkCnt)) {
                *n = *n + 1;
            }
        }
    }

    return batch;
}

// early malloc tags for stmt
void prepareTagsStmt(SSuperTable* stbInfo) {
    BArray *fields = stbInfo->tags;
    int32_t loop   = TAG_BATCH_COUNT;
    for (int i = 0; i < fields->size; ++i) {
        Field *field = benchArrayGet(fields, i);
        if (field->stmtData.data == NULL) {
            // data
            if (field->type == TSDB_DATA_TYPE_BINARY
                    || field->type == TSDB_DATA_TYPE_NCHAR) {
                field->stmtData.data = benchCalloc(1, loop * (field->length + 1), true);
            } else {
                field->stmtData.data = benchCalloc(1, loop * field->length, true);
            }

            // is_null
            field->stmtData.is_null = benchCalloc(sizeof(char), loop, true);
            // lengths
            field->stmtData.lengths = benchCalloc(sizeof(int32_t), loop, true);

            // log
            debugPrint("i=%d prepareTags fields=%p %s malloc stmtData.data=%p\n", i, fields, field->name ,field->stmtData.data);
        }
    }
}
