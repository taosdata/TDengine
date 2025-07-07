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

#include <stdlib.h>
#include <sys/stat.h>
#include <bench.h>
#include "benchLog.h"

#include "tdef.h"
#include "decimal.h"

extern char      g_configDir[MAX_PATH_LEN];

char funsName [FUNTYPE_CNT] [32] = {
    "sin(",
    "cos(",
    "count(",
    "saw(",
    "square(",
    "tri(",
};

int32_t parseFunArgs(char* value, uint8_t funType, int64_t* min ,int64_t* max, int32_t* step ,int32_t* period ,int32_t* offset) {
    char* buf = strdup(value);
    char* p[4] = {NULL};
    int32_t i = 0; 
    // find ")" fun end brance
    char* end = strstr(buf,")");
    if(end) {
        *end = 0;
    }
    int32_t argsLen = strlen(buf) + 1;

    // find first
    char* token = strtok(buf, ",");
    if(token == NULL) {
        free(buf);
        return 0;
    }
    p[i++] = token; 

    // find others
    while((token = strtok(NULL, ",")) && i < 4) {
        p[i++] = token; 
    }

    if(i != 4) {
        // must 4 params
        free(buf);
        return 0;
    }

    // parse fun
    if(funType == FUNTYPE_COUNT) {
        *min    = atoi(p[0]);
        *max    = atoi(p[1]);
        *step   = atoi(p[2]);
        *offset = atoi(p[3]);
    } else {
        *min    = atoi(p[0]);
        *max    = atoi(p[1]);
        *period = atoi(p[2]);
        *offset = atoi(p[3]);
    }

    free(buf);
    return argsLen;
}

uint8_t parseFuns(char* expr, float* multiple, float* addend, float* base, int32_t* random, 
            int64_t* min ,int64_t* max, int32_t* step ,int32_t* period ,int32_t* offset) {
    // check valid
    if (expr == NULL || multiple == NULL || addend == NULL || base == NULL) {
        return FUNTYPE_NONE;
    }

    size_t len = strlen(expr); 
    if(len > 100) {
        return FUNTYPE_NONE;
    }

    //parse format 10*sin(x) + 100 * random(5)
    char value[128];
    size_t n = 0;
    // remove blank
    for (size_t i = 0; i < len; i++) {
        if (expr[i] != ' ') {
            value[n++] = expr[i];
        }
    }
    // set end
    value[n] = 0;

    // multiple
    char* key1 = strstr(value, "*");
    if(key1) {
        // excpet tri(-20,40,20,5)+20+50*random(12)
        bool found = true;
        char* p1 = strstr(value+1, "+");
        char* p2 = strstr(value+1, "-");
        if(p1 && key1 > p1 ) 
           found = false;
        if(p2 && key1 > p2 ) 
           found = false;

        if(found) {
            *key1 = 0;
            *multiple = atof(value);
            key1 += 1;
        } else {
            key1 = value;
        }
    } else {
        key1 = value;
    }

    // funType
    uint8_t funType = FUNTYPE_NONE;
    char* key2 = NULL;
    for (int i = 0; i < FUNTYPE_CNT - 1; i++) {
        key2 = strstr(key1, funsName[i]);
        if(key2) {
            funType = i + 1;
            key2 += strlen(funsName[i]);
            int32_t argsLen = parseFunArgs(key2, funType, min, max, step, period, offset);
            if(len <= 0){
                return FUNTYPE_NONE;
            }
            key2 += argsLen;

            break;
        }
    }
    if (key2 == NULL)
        return FUNTYPE_NONE;

    char* key3 = strstr(key2, "+");
    if(key3) {
        *addend = atof(key3 + 1);
        key3 += 1;
    } else {
        key3 = strstr(key2, "-");
        if(key3) {
           *addend = atof(key3 + 1) * -1;
           key3 += 1;
        }
    }
    

    // random
    if(key3) {
        char* key4 = strstr(key3, "*random(");
        if(key4) {
            *random = atoi(key4 + 8);
            key3 += 9;
        }
    }

    // base
    if(key3) {
        char* key5 = strstr(key3, "+");
        if(key5){
            *base = atof(key5+1);
        } else {
            key5 = strstr(key3, "-");
            if(key5)
              *base = atof(key5+1) * -1;
        }
    }

    return funType;
}

static int getColumnAndTagTypeFromInsertJsonFile(
    tools_cJSON * superTblObj, SSuperTable *stbInfo) {
    int32_t code = -1;

    // columns
    tools_cJSON *columnsObj =
        tools_cJSON_GetObjectItem(superTblObj, "columns");
    if (!tools_cJSON_IsArray(columnsObj)) {
        goto PARSE_OVER;
    }
    benchArrayClear(stbInfo->cols);

    int columnSize = tools_cJSON_GetArraySize(columnsObj);

    int index = 0;
    for (int k = 0; k < columnSize; ++k) {
        bool sma = false;
        bool customName = false;
        uint8_t type = 0;
        int count = 1;
        int64_t max = RAND_MAX >> 1;
        int64_t min = 0;
        double  maxInDbl = max;
        double  minInDbl = min;
        uint8_t precision = TSDB_DECIMAL128_MAX_PRECISION;
        uint8_t scale = 0;
        uint32_t scalingFactor = 1;
        BDecimal decMax = {0};
        BDecimal decMin = {0};
        int32_t length  = 4;
        // fun type
        uint8_t funType = FUNTYPE_NONE;
        float   multiple = 0;
        float   addend   = 0;
        float   base     = 0;
        int32_t random   = 0;
        int32_t step     = 0;
        int32_t period   = 0;
        int32_t offset   = 0;
        uint8_t gen      = GEN_RANDOM;
        bool    fillNull = true;
        char*   encode   = NULL;
        char*   compress = NULL;
        char*   level    = NULL;

        tools_cJSON *column = tools_cJSON_GetArrayItem(columnsObj, k);
        if (!tools_cJSON_IsObject(column)) {
            errorPrint("%s", "Invalid column format in json\n");
            goto PARSE_OVER;
        }
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(column, "count");
        if (tools_cJSON_IsNumber(countObj)) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        tools_cJSON *dataName = tools_cJSON_GetObjectItem(column, "name");
        if (tools_cJSON_IsString(dataName)) {
            customName = true;
        }

        // column info
        tools_cJSON *dataType = tools_cJSON_GetObjectItem(column, "type");
        if (!tools_cJSON_IsString(dataType)) {
            goto PARSE_OVER;
        }
        if (0 == strCompareN(dataType->valuestring, "decimal", 0)) {
            tools_cJSON *dataPrecision = tools_cJSON_GetObjectItem(column, "precision");
            if (tools_cJSON_IsNumber(dataPrecision)) {
                precision = dataPrecision->valueint;
                if (precision > TSDB_DECIMAL128_MAX_PRECISION || precision <= 0) {
                    errorPrint("Invalid precision value of decimal type in json, precision: %d\n", precision);
                    goto PARSE_OVER;
                }
            } else {
                precision = TSDB_DECIMAL128_MAX_PRECISION;
            }
        }
        type = convertStringToDatatype(dataType->valuestring, 0, &precision);

        bool existMax = false;
        tools_cJSON *dataMax = tools_cJSON_GetObjectItem(column, "max");
        if (tools_cJSON_IsNumber(dataMax)) {
            existMax = true;
            max = dataMax->valueint;
            maxInDbl = dataMax->valuedouble;
        } else {
            max = convertDatatypeToDefaultMax(type);
            maxInDbl = max;
        }

        bool existMin = false;
        tools_cJSON *dataMin = tools_cJSON_GetObjectItem(column, "min");
        if (tools_cJSON_IsNumber(dataMin)) {
            existMin = true;
            min = dataMin->valueint;
            minInDbl = dataMin->valuedouble;
        } else {
            min = convertDatatypeToDefaultMin(type);
            minInDbl = min;
        }

        if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE
            || type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64) {
            double valueRange = maxInDbl - minInDbl;
            uint8_t maxScale = 0;
            if (type == TSDB_DATA_TYPE_FLOAT) maxScale = 6;
            else if (type == TSDB_DATA_TYPE_DOUBLE) maxScale = 15;
            else maxScale = precision;
            
            tools_cJSON *dataScale = tools_cJSON_GetObjectItem(column, "scale");
            if (tools_cJSON_IsNumber(dataScale)) {
                scale = dataScale->valueint;
                if (scale < 0 || scale > maxScale) {
                    errorPrint("Invalid scale value of decimal type in json, precision: %d, scale: %d\n", precision, scale);
                    goto PARSE_OVER;
                }
            } else {
                if (type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64 || valueRange > 1) {
                    scale = 0;
                } else {
                    scale = 3;
                }
            }

            if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
                scalingFactor = pow(10, scale);
                max = maxInDbl * scalingFactor;
                min = minInDbl * scalingFactor;
            }
        }

        if (type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64) {
            char* strDecMax = NULL;
            char* strDecMin = NULL;
            tools_cJSON *dataDecMax = tools_cJSON_GetObjectItem(column, "dec_max");
            if (tools_cJSON_IsString(dataDecMax)) {
                strDecMax = dataDecMax->valuestring;
            }
            tools_cJSON *dataDecMin = tools_cJSON_GetObjectItem(column, "dec_min");
            if (tools_cJSON_IsString(dataDecMin)) {
                strDecMin = dataDecMin->valuestring;
            }

            if (type == TSDB_DATA_TYPE_DECIMAL) {
                Decimal128 decOne = {{1LL, 0}};

                if (strDecMax) {
                    stringToDecimal128(strDecMax, precision, scale, &decMax.dec128);
                } else if (existMax == true) {
                    doubleToDecimal128(maxInDbl, precision, scale, &decMax.dec128);
                } else {
                    getDecimal128DefaultMax(precision, scale, &decMax.dec128);
                }
                const SDecimalOps* ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
                ops->subtract(&decMax.dec128, &decOne, DECIMAL_WORD_NUM(Decimal128));

                if (strDecMin) {
                    stringToDecimal128(strDecMin, precision, scale, &decMin.dec128);
                } else if (existMin == true) {
                    doubleToDecimal128(minInDbl, precision, scale, &decMin.dec128);
                } else {
                    getDecimal128DefaultMin(precision, scale, &decMin.dec128);
                }

                if (decimal128BCompare(&decMax.dec128, &decMin.dec128) < 0) {
                    errorPrint("Invalid dec_min/dec_max value of decimal type in json, dec_min: %s, dec_max: %s\n",
                            strDecMin ? strDecMin : "", strDecMax ? strDecMax : "");
                    goto PARSE_OVER;
                }
            } else {
                if (precision > TSDB_DECIMAL64_MAX_PRECISION) {
                    precision = TSDB_DECIMAL64_MAX_PRECISION;
                }

                Decimal64 decOne = {{1LL}};

                if (strDecMax) {
                    stringToDecimal64(strDecMax, precision, scale, &decMax.dec64);
                } else if (existMax == true) {
                    doubleToDecimal64(maxInDbl, precision, scale, &decMax.dec64);
                } else {
                    getDecimal64DefaultMax(precision, scale, &decMax.dec64);
                }

                const SDecimalOps* ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
                ops->subtract(&decMax.dec64, &decOne, DECIMAL_WORD_NUM(Decimal64));

                if (strDecMin) {
                    stringToDecimal64(strDecMin, precision, scale, &decMin.dec64);
                } else if (existMin == true) {
                    doubleToDecimal64(minInDbl, precision, scale, &decMin.dec64);
                } else {
                    getDecimal64DefaultMin(precision, scale, &decMin.dec64);
                }

                if (decimal64BCompare(&decMax.dec64, &decMin.dec64) < 0) {
                    errorPrint("Invalid dec_min/dec_max value of decimal type in json, dec_min: %s, dec_max: %s\n",
                            strDecMin ? strDecMin : "", strDecMax ? strDecMax : "");
                    goto PARSE_OVER;
                }
            }
        }

        // gen
        tools_cJSON *dataGen = tools_cJSON_GetObjectItem(column, "gen");
        if (tools_cJSON_IsString(dataGen)) {
            if (strcasecmp(dataGen->valuestring, "order") == 0) {
                gen = GEN_ORDER;
            }
        }
        // fillNull
        tools_cJSON *dataNull = tools_cJSON_GetObjectItem(column, "fillNull");
        if (tools_cJSON_IsString(dataNull)) {
            if (strcasecmp(dataNull->valuestring, "false") == 0) {
                fillNull = false;
            }
        }

        // encode
        tools_cJSON *dataEncode = tools_cJSON_GetObjectItem(column, "encode");
        if (tools_cJSON_IsString(dataEncode)) {
            encode = dataEncode->valuestring;
        }
        // compress
        tools_cJSON *dataCompress = tools_cJSON_GetObjectItem(column, "compress");
        if (tools_cJSON_IsString(dataCompress)) {
            compress = dataCompress->valuestring;
        }
        // level
        tools_cJSON *dataLevel = tools_cJSON_GetObjectItem(column, "level");
        if (tools_cJSON_IsString(dataLevel)) {
            level = dataLevel->valuestring;
        }

        // fun
        tools_cJSON *fun = tools_cJSON_GetObjectItem(column, "fun");
        if (tools_cJSON_IsString(fun)) {
            funType = parseFuns(fun->valuestring, &multiple, &addend, &base, &random, &min, &max, &step, &period, &offset);
        }

        tools_cJSON *dataValues = tools_cJSON_GetObjectItem(column, "values");

        if (g_arguments->taosc_version == 3) {
            tools_cJSON *sma_value = tools_cJSON_GetObjectItem(column, "sma");
            if (tools_cJSON_IsString(sma_value) &&
                (0 == strcasecmp(sma_value->valuestring, "yes"))) {
                sma = true;
            }
        }

        tools_cJSON * dataLen = tools_cJSON_GetObjectItem(column, "len");
        if (tools_cJSON_IsNumber(dataLen)) {
            length = (int32_t)dataLen->valueint;
        } else {
            if (type == TSDB_DATA_TYPE_BINARY
                || type == TSDB_DATA_TYPE_JSON
                || type == TSDB_DATA_TYPE_NCHAR
                || type == TSDB_DATA_TYPE_GEOMETRY) {
                length = g_arguments->binwidth;
            } else {
                length = convertTypeToLength(type);
            }
        }

        for (int n = 0; n < count; ++n) {
            Field * col = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->cols, col);
            col = benchArrayGet(stbInfo->cols, stbInfo->cols->size - 1);
            col->type = type;
            col->length = length;
            if (col->type != TSDB_DATA_TYPE_BLOB) {
                if (length == 0) {
                    col->null = true;
                }
            }
            col->sma = sma;
            col->max = max;
            col->min = min;
            col->maxInDbl = maxInDbl;
            col->minInDbl = minInDbl;
            col->precision = precision;
            col->scale = scale;
            col->scalingFactor = scalingFactor;
            col->decMax = decMax;
            col->decMin = decMin;
            col->gen = gen;
            col->fillNull = fillNull;
            col->values = dataValues;
            // fun
            col->funType  = funType;
            col->multiple = multiple;
            col->addend   = addend;
            col->base     = base;
            col->random   = random;
            col->step     = step;
            col->period   = period;
            col->offset   = offset;

            if (customName) {
                if (n >= 1) {
                    snprintf(col->name, TSDB_COL_NAME_LEN,
                             "%s_%d", dataName->valuestring, n);
                } else {
                    snprintf(col->name, TSDB_COL_NAME_LEN,
                            "%s", dataName->valuestring);
                }
            } else {
                snprintf(col->name, TSDB_COL_NAME_LEN, "c%d", index);
            }

            // encode
            if(encode) {
                if (strlen(encode) < COMP_NAME_LEN) {
                    strcpy(col->encode, encode);
                } else {
                    errorPrint("encode name length over (%d) bytes, ignore. name=%s", COMP_NAME_LEN, encode);
                }
            }
            // compress
            if(compress) {
                if (strlen(compress) < COMP_NAME_LEN) {
                    strcpy(col->compress, compress);
                } else {
                    errorPrint("compress name length over (%d) bytes, ignore. name=%s", COMP_NAME_LEN, compress);
                }
            }
            // level
            if(level) {
                if (strlen(level) < COMP_NAME_LEN) {
                    strcpy(col->level, level);
                } else {
                    errorPrint("level name length over (%d) bytes, ignore. name=%s", COMP_NAME_LEN, level);
                }
            }

            index++;
        }
    }

    index = 0;
    // tags
    benchArrayClear(stbInfo->tags);
    tools_cJSON *tags = tools_cJSON_GetObjectItem(superTblObj, "tags");
    if (!tools_cJSON_IsArray(tags)) {
        return 0;
    }

    int tagSize = tools_cJSON_GetArraySize(tags);

    stbInfo->use_metric = true;
    for (int k = 0; k < tagSize; ++k) {
        bool customName = false;
        uint8_t type = 0;
        int count = 1;
        int64_t max = RAND_MAX >> 1;
        int64_t min = 0;
        double  maxInDbl = max;
        double  minInDbl = min;
        uint8_t precision = TSDB_DECIMAL128_MAX_PRECISION;
        uint8_t scale = 0;
        uint32_t scalingFactor = 1;
        BDecimal decMax = {0};
        BDecimal decMin = {0};
        int32_t length = 4;
        uint8_t tagGen = GEN_RANDOM;
        tools_cJSON *tagObj = tools_cJSON_GetArrayItem(tags, k);
        if (!tools_cJSON_IsObject(tagObj)) {
            errorPrint("%s", "Invalid tag format in json\n");
            goto PARSE_OVER;
        }
        tools_cJSON *countObj = tools_cJSON_GetObjectItem(tagObj, "count");
        if (tools_cJSON_IsNumber(countObj)) {
            count = (int)countObj->valueint;
        } else {
            count = 1;
        }

        tools_cJSON *dataName = tools_cJSON_GetObjectItem(tagObj, "name");
        if (tools_cJSON_IsString(dataName)) {
            customName = true;
        }

        tools_cJSON *dataType = tools_cJSON_GetObjectItem(tagObj, "type");
        if (!tools_cJSON_IsString(dataType)) {
            goto PARSE_OVER;
        }
        // if (0 == strCompareN(dataType->valuestring, "decimal", 0)) {
        //     tools_cJSON *dataPrecision = tools_cJSON_GetObjectItem(tagObj, "precision");
        //     if (tools_cJSON_IsNumber(dataPrecision)) {
        //         precision = dataPrecision->valueint;
        //         if (precision > TSDB_DECIMAL128_MAX_PRECISION || precision < 1) {
        //             errorPrint("Invalid precision value in json, precision: %d\n", precision);
        //             goto PARSE_OVER;
        //         }
        //     } else {
        //         precision = TSDB_DECIMAL128_MAX_PRECISION;
        //     }
        // }
        type = convertStringToDatatype(dataType->valuestring, 0, &precision);

        if(type == TSDB_DATA_TYPE_JSON) {
            if (tagSize > 1) {
                // if tag type is json, must one tag column
                errorPrint("tag datatype is json, only one column tag is allowed, currently tags columns count is %d. quit programe.\n", tagSize);
                code = -1;
                goto PARSE_OVER;
            }

            // create on stbInfo->tags
            Field * tag = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->tags, tag);
            tag = benchArrayGet(stbInfo->tags, stbInfo->tags->size - 1);
            if (customName) {
                snprintf(tag->name, TSDB_COL_NAME_LEN,
                         "%s", dataName->valuestring);
            } else {
                snprintf(tag->name, TSDB_COL_NAME_LEN, "jtag");
            }
            tag->type = type;
            tag->length = JSON_FIXED_LENGTH; // json datatype is fixed length: 4096
            return 0;
        }

        tools_cJSON *dataMax = tools_cJSON_GetObjectItem(tagObj, "max");
        if (tools_cJSON_IsNumber(dataMax)) {
            max = dataMax->valueint;
            maxInDbl = dataMax->valuedouble;
        } else {
            max = convertDatatypeToDefaultMax(type);
            maxInDbl = max;
        }

        tools_cJSON *dataMin = tools_cJSON_GetObjectItem(tagObj, "min");
        if (tools_cJSON_IsNumber(dataMin)) {
            min = dataMin->valueint;
            minInDbl = dataMin->valuedouble;
        } else {
            min = convertDatatypeToDefaultMin(type);
            minInDbl = min;
        }

        if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE
            || type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64) {
            double valueRange = maxInDbl - minInDbl;
            uint8_t maxScale = 0;
            if (type == TSDB_DATA_TYPE_FLOAT) maxScale = 6;
            else if (type == TSDB_DATA_TYPE_DOUBLE) maxScale = 15;
            else maxScale = precision;

            tools_cJSON *dataScale = tools_cJSON_GetObjectItem(tagObj, "scale");
            if (tools_cJSON_IsNumber(dataScale)) {
                scale = dataScale->valueint;
                if (scale > maxScale) {
                    errorPrint("Invalid scale value in json, precision: %d, scale: %d\n", precision, scale);
                    goto PARSE_OVER;
                }
            } else {
                if (type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64 || valueRange > 1) {
                    scale = 0;
                } else {
                    scale = 3;
                }
            }

            if (type == TSDB_DATA_TYPE_FLOAT || type == TSDB_DATA_TYPE_DOUBLE) {
                scalingFactor = pow(10, scale);
                max = maxInDbl * scalingFactor;
                min = minInDbl * scalingFactor;
            }
        }

        // if (type == TSDB_DATA_TYPE_DECIMAL || type == TSDB_DATA_TYPE_DECIMAL64) {
        //     char* strDecMax = NULL;
        //     char* strDecMin = NULL;
        //     tools_cJSON *dataDecMax = tools_cJSON_GetObjectItem(tagObj, "dec_max");
        //     if (tools_cJSON_IsString(dataDecMax)) {
        //         strDecMax = dataDecMax->valuestring;
        //     }
        //     tools_cJSON *dataDecMin = tools_cJSON_GetObjectItem(tagObj, "dec_min");
        //     if (tools_cJSON_IsString(dataDecMin)) {
        //         strDecMin = dataDecMin->valuestring;
        //     }

        //     if (type == TSDB_DATA_TYPE_DECIMAL) {
        //         Decimal128 decOne = {{1LL, 0}};

        //         if (strDecMax) {
        //             stringToDecimal128(strDecMax, precision, scale, &decMax.dec128);
        //         } else {
        //             doubleToDecimal128(maxInDbl, precision, scale, &decMax.dec128);
        //         }
        //         const SDecimalOps* ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL);
        //         ops->subtract(&decMax.dec128, &decOne, DECIMAL_WORD_NUM(Decimal128));

        //         if (strDecMin) {
        //             stringToDecimal128(strDecMin, precision, scale, &decMin.dec128);
        //         } else {
        //             doubleToDecimal128(minInDbl, precision, scale, &decMin.dec128);
        //         }
        //     } else {
        //         if (precision > TSDB_DECIMAL64_MAX_PRECISION) {
        //             precision = TSDB_DECIMAL64_MAX_PRECISION;
        //         }

        //         Decimal64 decOne = {{1LL}};

        //         if (strDecMax) {
        //             stringToDecimal64(strDecMax, precision, scale, &decMax.dec64);
        //         } else {
        //             doubleToDecimal64(maxInDbl, precision, scale, &decMax.dec64);
        //         }
        //         const SDecimalOps* ops = getDecimalOps(TSDB_DATA_TYPE_DECIMAL64);
        //         ops->subtract(&decMax.dec64, &decOne, DECIMAL_WORD_NUM(Decimal64));

        //         if (strDecMin) {
        //             stringToDecimal64(strDecMin, precision, scale, &decMin.dec64);
        //         } else {
        //             doubleToDecimal64(minInDbl, precision, scale, &decMin.dec64);
        //         }
        //     }
        // }

        tools_cJSON *dataValues = tools_cJSON_GetObjectItem(tagObj, "values");

        tools_cJSON * dataLen = tools_cJSON_GetObjectItem(tagObj, "len");
        if (tools_cJSON_IsNumber(dataLen)) {
            length = (int32_t)dataLen->valueint;
        } else {
            if (type == TSDB_DATA_TYPE_BINARY
                || type == TSDB_DATA_TYPE_JSON
                || type == TSDB_DATA_TYPE_VARBINARY
                || type == TSDB_DATA_TYPE_GEOMETRY
                || type == TSDB_DATA_TYPE_NCHAR) {
                length = g_arguments->binwidth;
            } else {
                length = convertTypeToLength(type);
            }
        }

        tools_cJSON *tagDataGen = tools_cJSON_GetObjectItem(tagObj, "gen");
        if (tools_cJSON_IsString(tagDataGen)) {
            if (strcasecmp(tagDataGen->valuestring, "order") == 0) {
                tagGen = GEN_ORDER;
            }
        }

        for (int n = 0; n < count; ++n) {
            Field * tag = benchCalloc(1, sizeof(Field), true);
            benchArrayPush(stbInfo->tags, tag);
            tag = benchArrayGet(stbInfo->tags, stbInfo->tags->size - 1);
            tag->type = type;
            tag->length = length;
            if (length == 0) {
                tag->null = true;
            }
            tag->max = max;
            tag->min = min;
            tag->maxInDbl = maxInDbl;
            tag->minInDbl = minInDbl;
            tag->precision = precision;
            tag->scale = scale;
            tag->scalingFactor = scalingFactor;
            tag->decMax = decMax;
            tag->decMin = decMin;
            tag->values = dataValues;
            tag->gen = tagGen;
            if (customName) {
                if (n >= 1) {
                    snprintf(tag->name, TSDB_COL_NAME_LEN,
                             "%s_%d", dataName->valuestring, n);
                } else {
                    snprintf(tag->name, TSDB_COL_NAME_LEN,
                             "%s", dataName->valuestring);
                }
            } else {
                snprintf(tag->name, TSDB_COL_NAME_LEN, "t%d", index);
            }
            index++;
        }
    }
    code = 0;
PARSE_OVER:
    return code;
}

int32_t getDurationVal(tools_cJSON *jsonObj) {
    int32_t durMinute = 0;
    // get duration value
    if (tools_cJSON_IsString(jsonObj)) {
        char *val = jsonObj->valuestring;
        // like 10d or 10h or 10m
        int32_t len = strlen(val);
        if (len == 0) return 0;
        durMinute = atoi(val);
        if (strchr(val, 'h') || strchr(val, 'H')) {
            // hour
            durMinute *= 60;
        } else if (strchr(val, 'm') || strchr(val, 'M')) {
            // minute
            durMinute *= 1;
        } else {
            // day
            durMinute *= 24 * 60;
        }
    } else if (tools_cJSON_IsNumber(jsonObj)) {
        durMinute = jsonObj->valueint * 24 * 60;
    }

    return durMinute;
}

void setDBCfgString(SDbCfg* cfg , char * value) {
    int32_t len = strlen(value);

    // need add quotation
    bool add = false;
    if (0 == trimCaseCmp(cfg->name, "cachemodel") ||
        0 == trimCaseCmp(cfg->name, "dnodes"    ) ||
        0 == trimCaseCmp(cfg->name, "precision" ) ) {
            add = true;
    }    

    if (value[0] == '\'' || value[0] == '\"') {
        // already have quotation
        add = false;
    }

    if (!add) {
        // unnecesary add
        cfg->valuestring = value;
        cfg->free = false;
        return ;
    }

    // new
    int32_t nlen = len + 2 + 1;
    char * nval  = calloc(nlen, sizeof(char));
    nval[0]      = '\'';
    memcpy(nval + 1, value, len);
    nval[nlen - 2] = '\'';
    nval[nlen - 1] = 0;
    cfg->valuestring = nval;
    cfg->free = true;
    return ;
}

static int getDatabaseInfo(tools_cJSON *dbinfos, int index) {
    SDataBase *database;
    if (index > 0) {
        database = benchCalloc(1, sizeof(SDataBase), true);
        database->superTbls = benchArrayInit(1, sizeof(SSuperTable));
        benchArrayPush(g_arguments->databases, database);
    }
    database = benchArrayGet(g_arguments->databases, index);
    if (database->cfgs == NULL) {
        database->cfgs = benchArrayInit(1, sizeof(SDbCfg));
    }

    // check command line input no
    if(!(g_argFlag & ARG_OPT_NODROP)) {
        database->drop = true;
    }
  
    database->flush = false;
    database->precision = TSDB_TIME_PRECISION_MILLI;
    database->sml_precision = TSDB_SML_TIMESTAMP_MILLI_SECONDS;
    tools_cJSON *dbinfo = tools_cJSON_GetArrayItem(dbinfos, index);
    tools_cJSON *db = tools_cJSON_GetObjectItem(dbinfo, "dbinfo");
    if (!tools_cJSON_IsObject(db)) {
        errorPrint("%s", "Invalid dbinfo format in json\n");
        return -1;
    }

    tools_cJSON* cfg_object = db->child;

    while (cfg_object) {
        if (0 == strcasecmp(cfg_object->string, "name")) {
            if (tools_cJSON_IsString(cfg_object)) {
                database->dbName = cfg_object->valuestring;
            }
        } else if (0 == strcasecmp(cfg_object->string, "drop")) {
            if (tools_cJSON_IsString(cfg_object)
                && (0 == strcasecmp(cfg_object->valuestring, "no"))) {
                database->drop = false;
            }
        } else if (0 == strcasecmp(cfg_object->string, "flush_each_batch")) {
            if (tools_cJSON_IsString(cfg_object)
                && (0 == strcasecmp(cfg_object->valuestring, "yes"))) {
                database->flush = true;
            }
        } else if (0 == trimCaseCmp(cfg_object->string, "precision")) {
            if (tools_cJSON_IsString(cfg_object)) {
                if (0 == trimCaseCmp(cfg_object->valuestring, "us")) {
                    database->precision = TSDB_TIME_PRECISION_MICRO;
                    database->sml_precision = TSDB_SML_TIMESTAMP_MICRO_SECONDS;
                } else if (0 == trimCaseCmp(cfg_object->valuestring, "ns")) {
                    database->precision = TSDB_TIME_PRECISION_NANO;
                    database->sml_precision = TSDB_SML_TIMESTAMP_NANO_SECONDS;
                }
            }
        } else {
            SDbCfg* cfg = benchCalloc(1, sizeof(SDbCfg), true);
            cfg->name = cfg_object->string;

            // get duration value
            if (0 == trimCaseCmp(cfg_object->string, "duration")) {
                database->durMinute = getDurationVal(cfg_object);
            }

            if (tools_cJSON_IsString(cfg_object)) {
                setDBCfgString(cfg, cfg_object->valuestring);
            } else if (tools_cJSON_IsNumber(cfg_object)) {
                cfg->valueint = (int)cfg_object->valueint;
                cfg->valuestring = NULL;
            } else {
                errorPrint("Invalid value format for %s\n", cfg->name);
                free(cfg);
                return -1;
            }
            benchArrayPush(database->cfgs, cfg);
        }
        cfg_object = cfg_object->next;
    }

    // set default
    if (database->durMinute == 0) {
        database->durMinute = TSDB_DEFAULT_DURATION_PER_FILE;
    }

    if (database->dbName  == NULL) {
        errorPrint("%s", "miss name in dbinfo\n");
        return -1;
    }

    return 0;
}

static int get_tsma_info(tools_cJSON* stb_obj, SSuperTable* stbInfo) {
    stbInfo->tsmas = benchArrayInit(1, sizeof(TSMA));
    tools_cJSON* tsmas_obj = tools_cJSON_GetObjectItem(stb_obj, "tsmas");
    if (tsmas_obj == NULL) {
        return 0;
    }
    if (!tools_cJSON_IsArray(tsmas_obj)) {
        errorPrint("%s", "invalid tsmas format in json\n");
        return -1;
    }
    for (int i = 0; i < tools_cJSON_GetArraySize(tsmas_obj); ++i) {
        tools_cJSON* tsma_obj = tools_cJSON_GetArrayItem(tsmas_obj, i);
        if (!tools_cJSON_IsObject(tsma_obj)) {
            errorPrint("%s", "Invalid tsma format in json\n");
            return -1;
        }
        TSMA* tsma = benchCalloc(1, sizeof(TSMA), true);
        if (NULL == tsma) {
            errorPrint("%s() failed to allocate memory\n", __func__);
        }
        tools_cJSON* tsma_name_obj = tools_cJSON_GetObjectItem(tsma_obj,
                                                               "name");
        if (!tools_cJSON_IsString(tsma_name_obj)) {
            errorPrint("%s", "Invalid tsma name format in json\n");
            free(tsma);
            return -1;
        }
        tsma->name = tsma_name_obj->valuestring;

        tools_cJSON* tsma_func_obj =
            tools_cJSON_GetObjectItem(tsma_obj, "function");
        if (!tools_cJSON_IsString(tsma_func_obj)) {
            errorPrint("%s", "Invalid tsma function format in json\n");
            free(tsma);
            return -1;
        }
        tsma->func = tsma_func_obj->valuestring;

        tools_cJSON* tsma_interval_obj =
            tools_cJSON_GetObjectItem(tsma_obj, "interval");
        if (!tools_cJSON_IsString(tsma_interval_obj)) {
            errorPrint("%s", "Invalid tsma interval format in json\n");
            free(tsma);
            return -1;
        }
        tsma->interval = tsma_interval_obj->valuestring;

        tools_cJSON* tsma_sliding_obj =
            tools_cJSON_GetObjectItem(tsma_obj, "sliding");
        if (!tools_cJSON_IsString(tsma_sliding_obj)) {
            errorPrint("%s", "Invalid tsma sliding format in json\n");
            free(tsma);
            return -1;
        }
        tsma->sliding = tsma_sliding_obj->valuestring;

        tools_cJSON* tsma_custom_obj =
            tools_cJSON_GetObjectItem(tsma_obj, "custom");
        tsma->custom = tsma_custom_obj->valuestring;

        tools_cJSON* tsma_start_obj =
            tools_cJSON_GetObjectItem(tsma_obj, "start_when_inserted");
        if (!tools_cJSON_IsNumber(tsma_start_obj)) {
            tsma->start_when_inserted = 0;
        } else {
            tsma->start_when_inserted = (int)tsma_start_obj->valueint;
        }

        benchArrayPush(stbInfo->tsmas, tsma);
    }

    return 0;
}

void parseStringToIntArray(char *str, BArray *arr) {
    benchArrayClear(arr);
    if (NULL == strstr(str, ",")) {
        int *val = benchCalloc(1, sizeof(int), true);
        *val = atoi(str);
        benchArrayPush(arr, val);
    } else {
        char *dup_str = strdup(str);
        char *running = dup_str;
        char *token = strsep(&running, ",");
        while (token) {
            int *val = benchCalloc(1, sizeof(int), true);
            *val = atoi(token);
            benchArrayPush(arr, val);
            token = strsep(&running, ",");
        }
        tmfree(dup_str);
    }
}

// get interface name
uint16_t getInterface(char *name) {
    uint16_t iface = TAOSC_IFACE;
    if (0 == strcasecmp(name, "rest")) {
        iface = REST_IFACE;
    } else if (0 == strcasecmp(name, "stmt")) {
        iface = STMT_IFACE;
    } else if (0 == strcasecmp(name, "stmt2")) {
        iface = STMT2_IFACE;
    } else if (0 == strcasecmp(name, "sml")) {
        iface = SML_IFACE;
    } else if (0 == strcasecmp(name, "sml-rest")) {
        iface = SML_REST_IFACE;
    }

    return iface;
}

static int getStableInfo(tools_cJSON *dbinfos, int index) {
    SDataBase *database = benchArrayGet(g_arguments->databases, index);
    tools_cJSON *dbinfo = tools_cJSON_GetArrayItem(dbinfos, index);
    tools_cJSON *stables = tools_cJSON_GetObjectItem(dbinfo, "super_tables");
    if (!tools_cJSON_IsArray(stables)) {
        infoPrint("create database %s without stables\n", database->dbName);
        return 0;
    }
    for (int i = 0; i < tools_cJSON_GetArraySize(stables); ++i) {
        SSuperTable *superTable;
        if (index > 0 || i > 0) {
            superTable = benchCalloc(1, sizeof(SSuperTable), true);
            benchArrayPush(database->superTbls, superTable);
            superTable = benchArrayGet(database->superTbls, i);
            superTable->cols = benchArrayInit(1, sizeof(Field));
            superTable->tags = benchArrayInit(1, sizeof(Field));
        } else {
            superTable = benchArrayGet(database->superTbls, i);
        }
        superTable->autoTblCreating = false;
        superTable->batchTblCreatingNum = DEFAULT_CREATE_BATCH;
        superTable->batchTblCreatingNumbers = NULL;
        superTable->batchTblCreatingIntervals = NULL;
        superTable->childTblExists = false;
        superTable->random_data_source = true;
        superTable->iface = TAOSC_IFACE;
        superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
        superTable->tcpTransfer = false;
        superTable->childTblOffset = 0;
        superTable->timestamp_step = 1;
        superTable->angle_step = 1;
        superTable->useSampleTs = false;
        superTable->non_stop = false;
        superTable->insertRows = 0;
        superTable->interlaceRows = 0;
        superTable->disorderRatio = 0;
        superTable->disorderRange = DEFAULT_DISORDER_RANGE;
        superTable->insert_interval = g_arguments->insert_interval;
        superTable->max_sql_len = TSDB_MAX_ALLOWED_SQL_LEN;
        superTable->partialColNum = 0;
        superTable->partialColFrom = 0;
        superTable->comment = NULL;
        superTable->delay = -1;
        superTable->file_factor = -1;
        superTable->rollup = NULL;
        tools_cJSON *stbInfo = tools_cJSON_GetArrayItem(stables, i);
        tools_cJSON *itemObj;

        tools_cJSON *stbName = tools_cJSON_GetObjectItem(stbInfo, "name");
        if (tools_cJSON_IsString(stbName)) {
            superTable->stbName = stbName->valuestring;
        }

        tools_cJSON *prefix =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_prefix");
        if (tools_cJSON_IsString(prefix)) {
            superTable->childTblPrefix = prefix->valuestring;
        }
        tools_cJSON *childTbleSample =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_sample_file");
        if (tools_cJSON_IsString(childTbleSample)) {
            superTable->childTblSample = childTbleSample->valuestring;
        }
        tools_cJSON *autoCreateTbl =
            tools_cJSON_GetObjectItem(stbInfo, "auto_create_table");
        if (tools_cJSON_IsString(autoCreateTbl)
                && (0 == strcasecmp(autoCreateTbl->valuestring, "yes"))) {
            superTable->autoTblCreating = true;
        }
        tools_cJSON *batchCreateTbl =
            tools_cJSON_GetObjectItem(stbInfo, "batch_create_tbl_num");
        if (tools_cJSON_IsNumber(batchCreateTbl)) {
            superTable->batchTblCreatingNum = batchCreateTbl->valueint;
        }
        tools_cJSON *batchTblCreatingNumbers =
            tools_cJSON_GetObjectItem(stbInfo, "batch_create_tbl_numbers");
        if (tools_cJSON_IsString(batchTblCreatingNumbers)) {
            superTable->batchTblCreatingNumbers
                = batchTblCreatingNumbers->valuestring;
            superTable->batchTblCreatingNumbersArray =
                benchArrayInit(1, sizeof(int));
            parseStringToIntArray(superTable->batchTblCreatingNumbers,
                                  superTable->batchTblCreatingNumbersArray);
        }
        tools_cJSON *batchTblCreatingIntervals =
            tools_cJSON_GetObjectItem(stbInfo, "batch_create_tbl_intervals");
        if (tools_cJSON_IsString(batchTblCreatingIntervals)) {
            superTable->batchTblCreatingIntervals
                = batchTblCreatingIntervals->valuestring;
            superTable->batchTblCreatingIntervalsArray =
                benchArrayInit(1, sizeof(int));
            parseStringToIntArray(superTable->batchTblCreatingIntervals,
                                  superTable->batchTblCreatingIntervalsArray);
        }
        tools_cJSON *childTblExists =
            tools_cJSON_GetObjectItem(stbInfo, "child_table_exists");
        if (tools_cJSON_IsString(childTblExists)
                && (0 == strcasecmp(childTblExists->valuestring, "yes"))
                && !database->drop) {
            superTable->childTblExists = true;
            superTable->autoTblCreating = false;
        }

        tools_cJSON *childTableCount =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_count");
        if (tools_cJSON_IsNumber(childTableCount)) {
            superTable->childTblCount = childTableCount->valueint;
            g_arguments->totalChildTables += superTable->childTblCount;
        } else {
            superTable->childTblCount = 0;
            g_arguments->totalChildTables += superTable->childTblCount;
        }

        tools_cJSON *dataSource =
            tools_cJSON_GetObjectItem(stbInfo, "data_source");
        if (tools_cJSON_IsString(dataSource)
                && (0 == strcasecmp(dataSource->valuestring, "sample"))) {
            superTable->random_data_source = false;
        }

        tools_cJSON *stbIface =
            tools_cJSON_GetObjectItem(stbInfo, "insert_mode");
        if (tools_cJSON_IsString(stbIface)) {
            superTable->iface = getInterface(stbIface->valuestring);
            if (superTable->iface == STMT_IFACE) {
                if (g_arguments->reqPerReq > g_arguments->prepared_rand) {
                    g_arguments->prepared_rand = g_arguments->reqPerReq;
                }
            } else if (superTable->iface == SML_IFACE) {
                if (g_arguments->reqPerReq > SML_MAX_BATCH) {
                    errorPrint("reqPerReq (%u) larger than maximum (%d)\n",
                               g_arguments->reqPerReq, SML_MAX_BATCH);
                    return -1;
                }
            } else if (isRest(superTable->iface)) {
                if (g_arguments->reqPerReq > SML_MAX_BATCH) {
                    errorPrint("reqPerReq (%u) larger than maximum (%d)\n",
                            g_arguments->reqPerReq, SML_MAX_BATCH);
                    return -1;
                }
                if (0 != convertServAddr(REST_IFACE,
                                        false,
                                        1)) {
                    errorPrint("%s", "Failed to convert server address\n");
                    return -1;
                }
                encodeAuthBase64();
                g_arguments->rest_server_ver_major =
                    getServerVersionRest(g_arguments->port + TSDB_PORT_HTTP);
            }            
        }


        tools_cJSON *stbLineProtocol =
            tools_cJSON_GetObjectItem(stbInfo, "line_protocol");
        if (tools_cJSON_IsString(stbLineProtocol)) {
            if (0 == strcasecmp(stbLineProtocol->valuestring, "line")) {
                superTable->lineProtocol = TSDB_SML_LINE_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring,
                                       "telnet")) {
                superTable->lineProtocol = TSDB_SML_TELNET_PROTOCOL;
            } else if (0 == strcasecmp(stbLineProtocol->valuestring, "json")) {
                superTable->lineProtocol = TSDB_SML_JSON_PROTOCOL;
            } else if (0 == strcasecmp(
                        stbLineProtocol->valuestring, "taosjson")) {
                superTable->lineProtocol = SML_JSON_TAOS_FORMAT;
            }
        }
        tools_cJSON *transferProtocol =
            tools_cJSON_GetObjectItem(stbInfo, "tcp_transfer");
        if (tools_cJSON_IsString(transferProtocol)
                && (0 == strcasecmp(transferProtocol->valuestring, "yes"))) {
            superTable->tcpTransfer = true;
        }
        tools_cJSON *childTbl_limit =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_limit");
        if (tools_cJSON_IsNumber(childTbl_limit)) {
            if (childTbl_limit->valueint >= 0) {
                superTable->childTblLimit = childTbl_limit->valueint;
                if (superTable->childTblLimit > superTable->childTblCount) {
                    warnPrint("child table limit %"PRId64" "
                            "is more than %"PRId64", set to %"PRId64"\n",
                          childTbl_limit->valueint,
                          superTable->childTblCount,
                          superTable->childTblCount);
                    superTable->childTblLimit = superTable->childTblCount;
                }
            } else {
                warnPrint("child table limit %"PRId64" is invalid, set to zero. \n",childTbl_limit->valueint);
                superTable->childTblLimit = 0;
            }
        }
        tools_cJSON *childTbl_offset =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_offset");
        if (tools_cJSON_IsNumber(childTbl_offset)) {
            superTable->childTblOffset = childTbl_offset->valueint;
        }

        // check limit offset 
        if( superTable->childTblOffset + superTable->childTblLimit > superTable->childTblCount ) {
            errorPrint("json config invalid. childtable_offset(%"PRId64") + childtable_limit(%"PRId64") > childtable_count(%"PRId64")",
                  superTable->childTblOffset, superTable->childTblLimit, superTable->childTblCount);
            return -1;          
        }

        tools_cJSON *childTbl_from =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_from");
        if (tools_cJSON_IsNumber(childTbl_from)) {
            if (childTbl_from->valueint >= 0) {
                superTable->childTblFrom = childTbl_from->valueint;
            } else {
                warnPrint("child table _from_ %"PRId64" is invalid, set to 0\n",
                          childTbl_from->valueint);
                superTable->childTblFrom = 0;
            }
        }
        tools_cJSON *childTbl_to =
            tools_cJSON_GetObjectItem(stbInfo, "childtable_to");
        if (tools_cJSON_IsNumber(childTbl_to)) {
            superTable->childTblTo = childTbl_to->valueint;
            if (superTable->childTblTo < superTable->childTblFrom) {
                errorPrint("json config invalid. child table _to_ is invalid number,"
                    "%"PRId64" < %"PRId64"\n",
                    superTable->childTblTo, superTable->childTblFrom);
                return -1;
            }
        }

        // check childtable_from and childtable_to valid
        if (superTable->childTblCount > 0 && superTable->childTblFrom >= superTable->childTblCount) {
            errorPrint("json config invalid. childtable_from(%"PRId64") is equal or large than childtable_count(%"PRId64")\n", 
                    superTable->childTblFrom, superTable->childTblCount);
            return -1;
        }  
        if (superTable->childTblCount > 0 && superTable->childTblTo > superTable->childTblCount) {
            errorPrint("json config invalid. childtable_to(%"PRId64") is large than childtable_count(%"PRId64")\n", 
                    superTable->childTblTo, superTable->childTblCount);
            return -1;
        }

        // read from super table
        tools_cJSON *continueIfFail =
            tools_cJSON_GetObjectItem(stbInfo, "continue_if_fail");  // yes, no,
        if (tools_cJSON_IsString(continueIfFail)) {
            if (0 == strcasecmp(continueIfFail->valuestring, "no")) {
                superTable->continueIfFail = NO_IF_FAILED;
            } else if (0 == strcasecmp(continueIfFail->valuestring, "yes")) {
                superTable->continueIfFail = YES_IF_FAILED;
            } else if (0 == strcasecmp(continueIfFail->valuestring, "smart")) {
                superTable->continueIfFail = SMART_IF_FAILED;
            } else {
                errorPrint("cointinue_if_fail has unknown mode %s\n",
                           continueIfFail->valuestring);
                return -1;
            }
        } else {
            // default value is common specialed
            superTable->continueIfFail = g_arguments->continueIfFail;
        }

        // start_fillback_time
        superTable->startFillbackTime = 0;
        tools_cJSON *ts = tools_cJSON_GetObjectItem(stbInfo, "start_fillback_time");
        if (tools_cJSON_IsString(ts)) {
            if(0 == strcasecmp(ts->valuestring, "auto")) {
                superTable->autoFillback = true;
                superTable->startFillbackTime = 0;
            }
            else if (toolsParseTime(ts->valuestring,
                                &(superTable->startFillbackTime),
                                (int32_t)strlen(ts->valuestring),
                                database->precision, 0)) {
                errorPrint("failed to parse time %s\n", ts->valuestring);
                return -1;
            }
        } else {
            if (tools_cJSON_IsNumber(ts)) {
                superTable->startFillbackTime = ts->valueint;
            }
        }

        // start_timestamp
        ts = tools_cJSON_GetObjectItem(stbInfo, "start_timestamp");
        if (tools_cJSON_IsString(ts)) {
            if (0 == strcasecmp(ts->valuestring, "now")) {
                superTable->startTimestamp =
                    toolsGetTimestamp(database->precision);
                superTable->useNow = true;
                //  fill time with now conflict with check_sql
                g_arguments->check_sql = false;
            } else if (0 == strncasecmp(ts->valuestring, "now", 3)) {
                // like now - 7d expression
                superTable->calcNow = ts->valuestring;
            } else {
                if (toolsParseTime(ts->valuestring,
                                   &(superTable->startTimestamp),
                                   (int32_t)strlen(ts->valuestring),
                                   database->precision, 0)) {
                    errorPrint("failed to parse time %s\n",
                               ts->valuestring);
                    return -1;
                }
            }
        } else {
            if (tools_cJSON_IsNumber(ts)) {
                superTable->startTimestamp = ts->valueint;
            } else {
                superTable->startTimestamp =
                    toolsGetTimestamp(database->precision);
            }
        }

        tools_cJSON *timestampStep =
            tools_cJSON_GetObjectItem(stbInfo, "timestamp_step");
        if (tools_cJSON_IsNumber(timestampStep)) {
            superTable->timestamp_step = timestampStep->valueint;
        }

        tools_cJSON *angleStep =
            tools_cJSON_GetObjectItem(stbInfo, "angle_step");
        if (tools_cJSON_IsNumber(angleStep)) {
            superTable->angle_step = angleStep->valueint;
        }

        tools_cJSON *keepTrying =
            tools_cJSON_GetObjectItem(stbInfo, "keep_trying");
        if (tools_cJSON_IsNumber(keepTrying)) {
            superTable->keep_trying = keepTrying->valueint;
        }

        tools_cJSON *tryingInterval =
            tools_cJSON_GetObjectItem(stbInfo, "trying_interval");
        if (tools_cJSON_IsNumber(tryingInterval)) {
            superTable->trying_interval = (uint32_t)tryingInterval->valueint;
        }

        tools_cJSON *sampleFile =
            tools_cJSON_GetObjectItem(stbInfo, "sample_file");
        if (tools_cJSON_IsString(sampleFile)) {
            TOOLS_STRNCPY(
                superTable->sampleFile, sampleFile->valuestring,
                MAX_FILE_NAME_LEN);
        } else {
            memset(superTable->sampleFile, 0, MAX_FILE_NAME_LEN);
        }

        tools_cJSON *useSampleTs =
            tools_cJSON_GetObjectItem(stbInfo, "use_sample_ts");
        if (tools_cJSON_IsString(useSampleTs) &&
            (0 == strcasecmp(useSampleTs->valuestring, "yes"))) {
            superTable->useSampleTs = true;
        }

        tools_cJSON *nonStop =
            tools_cJSON_GetObjectItem(stbInfo, "non_stop_mode");
        if (tools_cJSON_IsString(nonStop) &&
            (0 == strcasecmp(nonStop->valuestring, "yes"))) {
            superTable->non_stop = true;
        }

        tools_cJSON* max_sql_len_obj =
            tools_cJSON_GetObjectItem(stbInfo, "max_sql_len");
        if (tools_cJSON_IsNumber(max_sql_len_obj)) {
            superTable->max_sql_len = max_sql_len_obj->valueint;
        }

        tools_cJSON *tagsFile =
            tools_cJSON_GetObjectItem(stbInfo, "tags_file");
        if (tools_cJSON_IsString(tagsFile)) {
            TOOLS_STRNCPY(superTable->tagsFile, tagsFile->valuestring,
                     MAX_FILE_NAME_LEN);
        } else {
            memset(superTable->tagsFile, 0, MAX_FILE_NAME_LEN);
        }

        tools_cJSON *insertRows =
            tools_cJSON_GetObjectItem(stbInfo, "insert_rows");
        if (tools_cJSON_IsNumber(insertRows)) {
            superTable->insertRows = insertRows->valueint;
        }

        tools_cJSON *stbInterlaceRows =
            tools_cJSON_GetObjectItem(stbInfo, "interlace_rows");
        if (tools_cJSON_IsNumber(stbInterlaceRows)) {
            superTable->interlaceRows = (uint32_t)stbInterlaceRows->valueint;
        }

        // disorder
        tools_cJSON *disorderRatio =
            tools_cJSON_GetObjectItem(stbInfo, "disorder_ratio");
        if (tools_cJSON_IsNumber(disorderRatio)) {
            if (disorderRatio->valueint > 100) disorderRatio->valueint = 100;
            if (disorderRatio->valueint < 0) disorderRatio->valueint = 0;

            superTable->disorderRatio = (int)disorderRatio->valueint;
            superTable->disRatio = (uint8_t)disorderRatio->valueint;
        }
        tools_cJSON *disorderRange =
            tools_cJSON_GetObjectItem(stbInfo, "disorder_range");
        if (tools_cJSON_IsNumber(disorderRange)) {
            superTable->disorderRange = (int)disorderRange->valueint;
            superTable->disRange = disorderRange->valueint;
        }
        tools_cJSON *disFill =
            tools_cJSON_GetObjectItem(stbInfo, "disorder_fill_interval");
        if (tools_cJSON_IsNumber(disFill)) {
            superTable->fillIntervalDis = (int)disFill->valueint;
        }


        // update
        tools_cJSON *updRatio =
            tools_cJSON_GetObjectItem(stbInfo, "update_ratio");
        if (tools_cJSON_IsNumber(updRatio)) {
            if (updRatio->valueint > 100) updRatio->valueint = 100;
            if (updRatio->valueint < 0) updRatio->valueint = 0;
            superTable->updRatio = (int8_t)updRatio->valueint;
        }
        tools_cJSON *updFill =
            tools_cJSON_GetObjectItem(stbInfo, "update_fill_interval");
        if (tools_cJSON_IsNumber(updFill)) {
            superTable->fillIntervalUpd = (uint64_t)updFill->valueint;
        }

        // delete
        tools_cJSON *delRatio =
            tools_cJSON_GetObjectItem(stbInfo, "delete_ratio");
        if (tools_cJSON_IsNumber(delRatio)) {
            if (delRatio->valueint > 100) delRatio->valueint = 100;
            if (delRatio->valueint < 0) delRatio->valueint = 0;
            superTable->delRatio = (int8_t)delRatio->valueint;
        }

        // generate row rule
        tools_cJSON *rowRule =
            tools_cJSON_GetObjectItem(stbInfo, "generate_row_rule");
        if (tools_cJSON_IsNumber(rowRule)) {
            superTable->genRowRule = (int8_t)rowRule->valueint;
        }

        // binary prefix
        tools_cJSON *binPrefix =
            tools_cJSON_GetObjectItem(stbInfo, "binary_prefix");
        if (tools_cJSON_IsString(binPrefix)) {
            superTable->binaryPrefex = binPrefix->valuestring;
        } else {
            superTable->binaryPrefex = NULL;
        }

        // nchar prefix
        tools_cJSON *ncharPrefix =
            tools_cJSON_GetObjectItem(stbInfo, "nchar_prefix");
        if (tools_cJSON_IsString(ncharPrefix)) {
            superTable->ncharPrefex = ncharPrefix->valuestring;
        } else {
            superTable->ncharPrefex = NULL;
        }

        // write future random
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "random_write_future");
        if (tools_cJSON_IsString(itemObj)
                && (0 == strcasecmp(itemObj->valuestring, "yes"))) {
            superTable->writeFuture = true;
        }

        // check_correct_interval
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "check_correct_interval");
        if (tools_cJSON_IsNumber(itemObj)) {
            superTable->checkInterval = itemObj->valueint;
        }

        tools_cJSON *insertInterval =
            tools_cJSON_GetObjectItem(stbInfo, "insert_interval");
        if (tools_cJSON_IsNumber(insertInterval)) {
            superTable->insert_interval = insertInterval->valueint;
        }

        tools_cJSON *pPartialColNum =
            tools_cJSON_GetObjectItem(stbInfo, "partial_col_num");
        if (tools_cJSON_IsNumber(pPartialColNum)) {
            superTable->partialColNum = pPartialColNum->valueint;
        }

        tools_cJSON *pPartialColFrom =
            tools_cJSON_GetObjectItem(stbInfo, "partial_col_from");
        if (tools_cJSON_IsNumber(pPartialColFrom)) {
            superTable->partialColFrom = pPartialColFrom->valueint;
        }

        if (g_arguments->taosc_version == 3) {
            tools_cJSON *delay = tools_cJSON_GetObjectItem(stbInfo, "delay");
            if (tools_cJSON_IsNumber(delay)) {
                superTable->delay = (int)delay->valueint;
            }

            tools_cJSON *file_factor =
                tools_cJSON_GetObjectItem(stbInfo, "file_factor");
            if (tools_cJSON_IsNumber(file_factor)) {
                superTable->file_factor = (int)file_factor->valueint;
            }

            tools_cJSON *rollup = tools_cJSON_GetObjectItem(stbInfo, "rollup");
            if (tools_cJSON_IsString(rollup)) {
                superTable->rollup = rollup->valuestring;
            }

            tools_cJSON *ttl = tools_cJSON_GetObjectItem(stbInfo, "ttl");
            if (tools_cJSON_IsNumber(ttl)) {
                superTable->ttl = (int)ttl->valueint;
            }

            tools_cJSON *max_delay_obj =
                tools_cJSON_GetObjectItem(stbInfo, "max_delay");
            if (tools_cJSON_IsString(max_delay_obj)) {
                superTable->max_delay = max_delay_obj->valuestring;
            }

            tools_cJSON *watermark_obj =
                tools_cJSON_GetObjectItem(stbInfo, "watermark");
            if (tools_cJSON_IsString(watermark_obj)) {
                superTable->watermark = watermark_obj->valuestring;
            }

            if (get_tsma_info(stbInfo, superTable)) {
                return -1;
            }
        }

        if (getColumnAndTagTypeFromInsertJsonFile(stbInfo, superTable)) {
            return -1;
        }

        // primary key
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "primary_key");
        if (tools_cJSON_IsNumber(itemObj)) {
            superTable->primary_key = itemObj->valueint == 1;
        }
        // repeat_ts_min
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "repeat_ts_min");
        if (tools_cJSON_IsNumber(itemObj)) {
            superTable->repeat_ts_min = (int)itemObj->valueint;
        }
        // repeat_ts_max
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "repeat_ts_max");
        if (tools_cJSON_IsNumber(itemObj)) {
            superTable->repeat_ts_max = (int)itemObj->valueint;
        }

        // sqls
        itemObj = tools_cJSON_GetObjectItem(stbInfo, "sqls");
        if (tools_cJSON_IsArray(itemObj)) {
            int cnt = tools_cJSON_GetArraySize(itemObj);
            if(cnt > 0) {
                char ** sqls = (char **)benchCalloc(cnt + 1, sizeof(char *), false); // +1 add end
                superTable->sqls = sqls;
                for(int j = 0; j < cnt; j++) {
                    tools_cJSON *sqlObj = tools_cJSON_GetArrayItem(itemObj, j);
                    if(sqlObj && tools_cJSON_IsString(sqlObj)) {
                        *sqls = strdup(sqlObj->valuestring);
                        sqls++;
                    }
                }
            }
        }

        // csv file prefix
        tools_cJSON* csv_fp = tools_cJSON_GetObjectItem(stbInfo, "csv_file_prefix");
        if (csv_fp && csv_fp->type == tools_cJSON_String && csv_fp->valuestring != NULL) {
            superTable->csv_file_prefix = csv_fp->valuestring;
        } else {
            superTable->csv_file_prefix = "data";
        }

        // csv timestamp format
        tools_cJSON* csv_tf = tools_cJSON_GetObjectItem(stbInfo, "csv_ts_format");
        if (csv_tf && csv_tf->type == tools_cJSON_String && csv_tf->valuestring != NULL) {
            superTable->csv_ts_format = csv_tf->valuestring;
        } else {
            superTable->csv_ts_format = NULL;
        }

        // csv timestamp format
        tools_cJSON* csv_ti = tools_cJSON_GetObjectItem(stbInfo, "csv_ts_interval");
        if (csv_ti && csv_ti->type == tools_cJSON_String && csv_ti->valuestring != NULL) {
            superTable->csv_ts_interval = csv_ti->valuestring;
        } else {
            superTable->csv_ts_interval = "1d";
        }

        // csv output header
        superTable->csv_output_header = true;
        tools_cJSON* oph = tools_cJSON_GetObjectItem(stbInfo, "csv_output_header");
        if (oph && oph->type == tools_cJSON_String && oph->valuestring != NULL) {
            if (0 == strcasecmp(oph->valuestring, "yes")) {
                superTable->csv_output_header = true;
            } else if (0 == strcasecmp(oph->valuestring, "no")) {
                superTable->csv_output_header = false;
            }
        }

        // csv tbname alias
        tools_cJSON* tba = tools_cJSON_GetObjectItem(stbInfo, "csv_tbname_alias");
        if (tba && tba->type == tools_cJSON_String && tba->valuestring != NULL) {
            superTable->csv_tbname_alias = tba->valuestring;
        } else {
            superTable->csv_tbname_alias = "device_id";
        }

        // csv compression level
        tools_cJSON* cl = tools_cJSON_GetObjectItem(stbInfo, "csv_compress_level");
        if (cl && cl->type == tools_cJSON_String && cl->valuestring != NULL) {
            if (0 == strcasecmp(cl->valuestring, "none")) {
                superTable->csv_compress_level = CSV_COMPRESS_NONE;
            } else if (0 == strcasecmp(cl->valuestring, "fast")) {
                superTable->csv_compress_level = CSV_COMPRESS_FAST;
            } else if (0 == strcasecmp(cl->valuestring, "balance")) {
                superTable->csv_compress_level = CSV_COMPRESS_BALANCE;
            } else if (0 == strcasecmp(cl->valuestring, "best")) {
                superTable->csv_compress_level = CSV_COMPRESS_BEST;
            }
        } else {
            superTable->csv_compress_level = CSV_COMPRESS_NONE;
        }
    }
    return 0;
}

static int getStreamInfo(tools_cJSON* json) {
    tools_cJSON* streamsObj = tools_cJSON_GetObjectItem(json, "streams");
    if (tools_cJSON_IsArray(streamsObj)) {
        int streamCnt = tools_cJSON_GetArraySize(streamsObj);
        for (int i = 0; i < streamCnt; ++i) {
            tools_cJSON* streamObj = tools_cJSON_GetArrayItem(streamsObj, i);
            if (!tools_cJSON_IsObject(streamObj)) {
                errorPrint("%s", "invalid stream format in json\n");
                return -1;
            }
            tools_cJSON* stream_name =
                tools_cJSON_GetObjectItem(streamObj, "stream_name");
            tools_cJSON* stream_stb =
                tools_cJSON_GetObjectItem(streamObj, "stream_stb");
            tools_cJSON* source_sql =
                tools_cJSON_GetObjectItem(streamObj, "source_sql");
            if (!tools_cJSON_IsString(stream_name)
                || !tools_cJSON_IsString(stream_stb)
                || !tools_cJSON_IsString(source_sql)) {
                errorPrint("%s", "Invalid or miss "
                           "'stream_name'/'stream_stb'/'source_sql' "
                           "key in json\n");
                return -1;
            }
            SSTREAM * stream = benchCalloc(1, sizeof(SSTREAM), true);
            TOOLS_STRNCPY(stream->stream_name, stream_name->valuestring,
                     TSDB_TABLE_NAME_LEN);
            TOOLS_STRNCPY(stream->stream_stb, stream_stb->valuestring,
                     TSDB_TABLE_NAME_LEN);
            TOOLS_STRNCPY(stream->source_sql, source_sql->valuestring,
                     TSDB_DEFAULT_PKT_SIZE);

            tools_cJSON* trigger_mode =
                tools_cJSON_GetObjectItem(streamObj, "trigger_mode");
            if (tools_cJSON_IsString(trigger_mode)) {
                TOOLS_STRNCPY(stream->trigger_mode, trigger_mode->valuestring,
                         BIGINT_BUFF_LEN);
            }

            tools_cJSON* watermark =
                tools_cJSON_GetObjectItem(streamObj, "watermark");
            if (tools_cJSON_IsString(watermark)) {
                TOOLS_STRNCPY(stream->watermark, watermark->valuestring,
                         BIGINT_BUFF_LEN);
            }

            tools_cJSON* ignore_expired =
                tools_cJSON_GetObjectItem(streamObj, "ignore_expired");
            if (tools_cJSON_IsString(ignore_expired)) {
                TOOLS_STRNCPY(stream->ignore_expired, ignore_expired->valuestring,
                         BIGINT_BUFF_LEN);
            }

            tools_cJSON* ignore_update =
                tools_cJSON_GetObjectItem(streamObj, "ignore_update");
            if (tools_cJSON_IsString(ignore_update)) {
                TOOLS_STRNCPY(stream->ignore_update, ignore_update->valuestring,
                         BIGINT_BUFF_LEN);
            }

            tools_cJSON* fill_history =
                tools_cJSON_GetObjectItem(streamObj, "fill_history");
            if (tools_cJSON_IsString(fill_history)) {
                TOOLS_STRNCPY(stream->fill_history, fill_history->valuestring,
                         BIGINT_BUFF_LEN);
            }

            tools_cJSON* stream_stb_field =
                tools_cJSON_GetObjectItem(streamObj, "stream_stb_field");
            if (tools_cJSON_IsString(stream_stb_field)) {
                TOOLS_STRNCPY(stream->stream_stb_field,
                         stream_stb_field->valuestring,
                         TSDB_DEFAULT_PKT_SIZE);
            }

            tools_cJSON* stream_tag_field =
                tools_cJSON_GetObjectItem(streamObj, "stream_tag_field");
            if (tools_cJSON_IsString(stream_tag_field)) {
                TOOLS_STRNCPY(stream->stream_tag_field,
                         stream_tag_field->valuestring,
                         TSDB_DEFAULT_PKT_SIZE);
            }

            tools_cJSON* subtable =
                tools_cJSON_GetObjectItem(streamObj, "subtable");
            if (tools_cJSON_IsString(subtable)) {
                TOOLS_STRNCPY(stream->subtable, subtable->valuestring,
                         TSDB_DEFAULT_PKT_SIZE);
            }

            tools_cJSON* drop = tools_cJSON_GetObjectItem(streamObj, "drop");
            if (tools_cJSON_IsString(drop)) {
                if (0 == strcasecmp(drop->valuestring, "yes")) {
                    stream->drop = true;
                } else if (0 == strcasecmp(drop->valuestring, "no")) {
                    stream->drop = false;
                } else {
                    errorPrint("invalid value for drop field: %s\n",
                               drop->valuestring);
                    return -1;
                }
            }
            benchArrayPush(g_arguments->streams, stream);
        }
    }
    return 0;
}

// read common item
static int getMetaFromCommonJsonFile(tools_cJSON *json) {
    int32_t code = -1;
    tools_cJSON *cfgdir = tools_cJSON_GetObjectItem(json, "cfgdir");
    if (cfgdir && (cfgdir->type == tools_cJSON_String)
            && (cfgdir->valuestring != NULL)) {
        if (!g_arguments->cfg_inputted) {
            TOOLS_STRNCPY(g_configDir, cfgdir->valuestring, MAX_FILE_NAME_LEN);
            debugPrint("configDir from cfg: %s\n", g_configDir);
        } else {
            warnPrint("configDir set by command line, so ignore cfg. cmd: %s\n", g_configDir);
        }        
    }

    // dsn
    tools_cJSON *dsn = tools_cJSON_GetObjectItem(json, "dsn");
    if (tools_cJSON_IsString(dsn) && strlen(dsn->valuestring) > 0) {
        if (g_arguments->dsn == NULL) {
            g_arguments->dsn = dsn->valuestring;
            infoPrint("read dsn from json. dsn=%s\n", g_arguments->dsn);
        }
    }    

    // host
    tools_cJSON *host = tools_cJSON_GetObjectItem(json, "host");
    if (host && host->type == tools_cJSON_String && host->valuestring != NULL) {
        if(g_arguments->host == NULL) {
            g_arguments->host = host->valuestring;
            infoPrint("read host from json: %s .\n", g_arguments->host);
        }     
    }

    // port
    tools_cJSON *port = tools_cJSON_GetObjectItem(json, "port");
    if (port && port->type == tools_cJSON_Number) {
        if (g_arguments->port_inputted) {
            // command line input port first
            warnPrint("command port: %d, json port ignored.\n", g_arguments->port);
        } else {
            // default port set auto port
            if (port->valueint != DEFAULT_PORT) {
                g_arguments->port = (uint16_t)port->valueint;
                infoPrint("read port form json: %d .\n", g_arguments->port);
                g_arguments->port_inputted = true;    
            }
        }
    }

    // user
    tools_cJSON *user = tools_cJSON_GetObjectItem(json, "user");
    if (user && user->type == tools_cJSON_String && user->valuestring != NULL) {
        if (g_arguments->user == NULL) {
            g_arguments->user = user->valuestring;
            infoPrint("read user from json: %s .\n", g_arguments->user);
        }
    }

    // pass
    tools_cJSON *password = tools_cJSON_GetObjectItem(json, "password");
    if (password && password->type == tools_cJSON_String &&
        password->valuestring != NULL) {
        if(g_arguments->password == NULL) {
            g_arguments->password = password->valuestring;
            infoPrint("read password from json: %s .\n", "******");
        }        
    }

    // yes, no
    tools_cJSON *answerPrompt = tools_cJSON_GetObjectItem(json, "confirm_parameter_prompt");
    if (answerPrompt && answerPrompt->type == tools_cJSON_String
            && answerPrompt->valuestring != NULL) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    // read from common
    tools_cJSON *continueIfFail =
        tools_cJSON_GetObjectItem(json, "continue_if_fail");  // yes, no,
    if (tools_cJSON_IsString(continueIfFail)) {
        if (0 == strcasecmp(continueIfFail->valuestring, "no")) {
            g_arguments->continueIfFail = NO_IF_FAILED;
        } else if (0 == strcasecmp(continueIfFail->valuestring, "yes")) {
            g_arguments->continueIfFail = YES_IF_FAILED;
        } else if (0 == strcasecmp(continueIfFail->valuestring, "smart")) {
            g_arguments->continueIfFail = SMART_IF_FAILED;
        } else {
            errorPrint("cointinue_if_fail has unknown mode %s\n",
                       continueIfFail->valuestring);
            return -1;
        }
    }

    // output dir
    tools_cJSON* opp = tools_cJSON_GetObjectItem(json, "output_dir");
    if (opp && opp->type == tools_cJSON_String && opp->valuestring != NULL) {
        g_arguments->output_path = opp->valuestring;
    } else {
        g_arguments->output_path = "./output/";
    }
    (void)mkdir(g_arguments->output_path, 0775);

    code = 0;
    return code;
}

static int getMetaFromInsertJsonFile(tools_cJSON *json) {
    int32_t code = -1;

    // check after inserted
    tools_cJSON *checkSql = tools_cJSON_GetObjectItem(json, "check_sql");
    if (tools_cJSON_IsString(checkSql)) {
        if (0 == strcasecmp(checkSql->valuestring, "yes")) {
            g_arguments->check_sql = true;
        }
    }

    g_arguments->pre_load_tb_meta = false;
    tools_cJSON *preLoad = tools_cJSON_GetObjectItem(json, "pre_load_tb_meta");
    if (tools_cJSON_IsString(preLoad)) {
        if (0 == strcasecmp(preLoad->valuestring, "yes")) {
            g_arguments->pre_load_tb_meta = true;
        }
    }

    tools_cJSON *resultfile = tools_cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == tools_cJSON_String
            && resultfile->valuestring != NULL) {
        g_arguments->output_file = resultfile->valuestring;
    }

    tools_cJSON *resultJsonFile = tools_cJSON_GetObjectItem(json, "result_json_file");
    if (resultJsonFile && resultJsonFile->type == tools_cJSON_String
            && resultJsonFile->valuestring != NULL) {
        g_arguments->output_json_file = resultJsonFile->valuestring;
        if (check_write_permission(g_arguments->output_json_file)) {
            errorPrint("json file %s does not have write permission.\n", g_arguments->output_json_file);
            goto PARSE_OVER;
        }
    }

    tools_cJSON *threads = tools_cJSON_GetObjectItem(json, "thread_count");
    if (threads && threads->type == tools_cJSON_Number) {
        if(!(g_argFlag & ARG_OPT_THREAD)) {
            // only command line no -T use json value
            g_arguments->nthreads = (uint32_t)threads->valueint;
        }
    }

    tools_cJSON *bindVGroup = tools_cJSON_GetObjectItem(json, "thread_bind_vgroup");
    if (tools_cJSON_IsString(bindVGroup)) {
        if (0 == strcasecmp(bindVGroup->valuestring, "yes")) {
            g_arguments->bind_vgroup = true;
        }
    }

    tools_cJSON *keepTrying = tools_cJSON_GetObjectItem(json, "keep_trying");
    if (keepTrying && keepTrying->type == tools_cJSON_Number) {
        g_arguments->keep_trying = (int32_t)keepTrying->valueint;
    }

    tools_cJSON *tryingInterval =
        tools_cJSON_GetObjectItem(json, "trying_interval");
    if (tryingInterval && tryingInterval->type == tools_cJSON_Number) {
        g_arguments->trying_interval = (uint32_t)tryingInterval->valueint;
    }

    tools_cJSON *table_theads =
        tools_cJSON_GetObjectItem(json, "create_table_thread_count");
    if (tools_cJSON_IsNumber(table_theads)) {
        g_arguments->table_threads = (uint32_t)table_theads->valueint;
    }

    tools_cJSON *numRecPerReq =
        tools_cJSON_GetObjectItem(json, "num_of_records_per_req");
    if (numRecPerReq && numRecPerReq->type == tools_cJSON_Number) {
        g_arguments->reqPerReq = (uint32_t)numRecPerReq->valueint;
        if ((int32_t)g_arguments->reqPerReq <= 0) {
            infoPrint("waring: num_of_records_per_req item in json config must over zero, current = %d. now reset to default. \n", g_arguments->reqPerReq);
            g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
        }

        if (g_arguments->reqPerReq > INT32_MAX) {
            infoPrint("warning: num_of_records_per_req item in json config need less than 32768. current = %d. now reset to default.\n", g_arguments->reqPerReq);
            g_arguments->reqPerReq = DEFAULT_REQ_PER_REQ;
        }

    }

    tools_cJSON *prepareRand =
        tools_cJSON_GetObjectItem(json, "prepared_rand");
    if (prepareRand && prepareRand->type == tools_cJSON_Number) {
        g_arguments->prepared_rand = prepareRand->valueint;
    }

    tools_cJSON *chineseOpt =
        tools_cJSON_GetObjectItem(json, "chinese");  // yes, no,
    if (chineseOpt && chineseOpt->type == tools_cJSON_String
            && chineseOpt->valuestring != NULL) {
        if (0 == strncasecmp(chineseOpt->valuestring, "yes", 3)) {
            g_arguments->chinese = true;
        }
    }

    tools_cJSON *escapeChar =
        tools_cJSON_GetObjectItem(json, "escape_character");  // yes, no,
    if (escapeChar && escapeChar->type == tools_cJSON_String
            && escapeChar->valuestring != NULL) {
        if (0 == strncasecmp(escapeChar->valuestring, "yes", 3)) {
            g_arguments->escape_character = true;
        }
    }

    tools_cJSON *top_insertInterval =
        tools_cJSON_GetObjectItem(json, "insert_interval");
    if (top_insertInterval && top_insertInterval->type == tools_cJSON_Number) {
        g_arguments->insert_interval = top_insertInterval->valueint;
    }

    tools_cJSON *insert_mode = tools_cJSON_GetObjectItem(json, "insert_mode");
    if (insert_mode && insert_mode->type == tools_cJSON_String
            && insert_mode->valuestring != NULL) {
        g_arguments->iface = getInterface(insert_mode->valuestring);
    }

    tools_cJSON *dbinfos = tools_cJSON_GetObjectItem(json, "databases");
    if (!tools_cJSON_IsArray(dbinfos)) {
        errorPrint("%s", "Invalid databases format in json\n");
        return -1;
    }
    int dbSize = tools_cJSON_GetArraySize(dbinfos);

    for (int i = 0; i < dbSize; ++i) {
        if (getDatabaseInfo(dbinfos, i)) {
            goto PARSE_OVER;
        }
        if (getStableInfo(dbinfos, i)) {
            goto PARSE_OVER;
        }
    }

    if (g_arguments->taosc_version == 3) {
        if (getStreamInfo(json)) {
            goto PARSE_OVER;
        }
    }

    code = 0;

PARSE_OVER:
    return code;
}

// Spec Query
int32_t readSpecQueryJson(tools_cJSON * specifiedQuery) {
    g_queryInfo.specifiedQueryInfo.concurrent = 1;
    if (tools_cJSON_IsObject(specifiedQuery)) {
        tools_cJSON *queryInterval =
            tools_cJSON_GetObjectItem(specifiedQuery, "query_interval");
        if (tools_cJSON_IsNumber(queryInterval)) {
            g_queryInfo.specifiedQueryInfo.queryInterval =
                queryInterval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryInterval = 0;
        }

        tools_cJSON *specifiedQueryTimes =
            tools_cJSON_GetObjectItem(specifiedQuery, "query_times");
        if (tools_cJSON_IsNumber(specifiedQueryTimes)) {
            g_queryInfo.specifiedQueryInfo.queryTimes =
                specifiedQueryTimes->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        tools_cJSON *mixedQueryObj =
            tools_cJSON_GetObjectItem(specifiedQuery, "mixed_query");
        if (tools_cJSON_IsString(mixedQueryObj)) {
            if (0 == strcasecmp(mixedQueryObj->valuestring, "yes")) {
                g_queryInfo.specifiedQueryInfo.mixed_query = true;
                infoPrint("%s\n","mixed_query is True");
            } else if (0 == strcasecmp(mixedQueryObj->valuestring, "no")) {
                g_queryInfo.specifiedQueryInfo.mixed_query = false;
                infoPrint("%s\n","mixed_query is False");
            } else {
                errorPrint("Invalid mixed_query value: %s\n",
                           mixedQueryObj->valuestring);
                return -1;
            }
        }

        // batchQuery
        tools_cJSON *batchQueryObj =
            tools_cJSON_GetObjectItem(specifiedQuery, "batch_query");
        if (tools_cJSON_IsString(batchQueryObj)) {
            if (0 == strcasecmp(batchQueryObj->valuestring, "yes")) {
                g_queryInfo.specifiedQueryInfo.batchQuery = true;
                infoPrint("%s\n","batch_query is True");
            } else if (0 == strcasecmp(batchQueryObj->valuestring, "no")) {
                g_queryInfo.specifiedQueryInfo.batchQuery = false;
                infoPrint("%s\n","batch_query is False");
            } else {
                errorPrint("Invalid batch_query value: %s\n",
                    batchQueryObj->valuestring);
                return -1;
            }
        }        

        tools_cJSON *concurrent =
            tools_cJSON_GetObjectItem(specifiedQuery, "concurrent");
        if (tools_cJSON_IsNumber(concurrent)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)concurrent->valueint;
        }

        tools_cJSON *threads =
            tools_cJSON_GetObjectItem(specifiedQuery, "threads");
        if (tools_cJSON_IsNumber(threads)) {
            g_queryInfo.specifiedQueryInfo.concurrent =
                (uint32_t)threads->valueint;
        }

        tools_cJSON *specifiedAsyncMode =
            tools_cJSON_GetObjectItem(specifiedQuery, "mode");
        if (tools_cJSON_IsString(specifiedAsyncMode)) {
            if (0 == strcmp("async", specifiedAsyncMode->valuestring)) {
                g_queryInfo.specifiedQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.asyncMode = SYNC_MODE;
        }

        tools_cJSON *subscribe_interval =
            tools_cJSON_GetObjectItem(specifiedQuery, "subscribe_interval");
        if (tools_cJSON_IsNumber(subscribe_interval)) {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                subscribe_interval->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeInterval =
                DEFAULT_SUB_INTERVAL;
        }

        tools_cJSON *specifiedSubscribeTimes =
            tools_cJSON_GetObjectItem(specifiedQuery, "subscribe_times");
        if (tools_cJSON_IsNumber(specifiedSubscribeTimes)) {
            g_queryInfo.specifiedQueryInfo.subscribeTimes =
                specifiedSubscribeTimes->valueint;
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeTimes =
                g_queryInfo.query_times;
        }

        tools_cJSON *restart =
            tools_cJSON_GetObjectItem(specifiedQuery, "restart");
        if (tools_cJSON_IsString(restart)) {
            if (0 == strcmp("no", restart->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeRestart = true;
        }

        tools_cJSON *keepProgress =
            tools_cJSON_GetObjectItem(specifiedQuery, "keepProgress");
        if (tools_cJSON_IsString(keepProgress)) {
            if (0 == strcmp("yes", keepProgress->valuestring)) {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.specifiedQueryInfo.subscribeKeepProgress = 0;
        }

        // read sqls from file
        tools_cJSON *sqlFileObj =
            tools_cJSON_GetObjectItem(specifiedQuery, "sql_file");
        if (tools_cJSON_IsString(sqlFileObj)) {
            FILE * fp = fopen(sqlFileObj->valuestring, "r");
            if (fp == NULL) {
                errorPrint("failed to open file: %s\n",
                           sqlFileObj->valuestring);
                return -1;
            }
            char *buf = benchCalloc(1, TSDB_MAX_ALLOWED_SQL_LEN, true);
            while (fgets(buf, TSDB_MAX_ALLOWED_SQL_LEN, fp)) {
                SSQL * sql = benchCalloc(1, sizeof(SSQL), true);
                benchArrayPush(g_queryInfo.specifiedQueryInfo.sqls, sql);
                sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls,
                        g_queryInfo.specifiedQueryInfo.sqls->size - 1);
                int bufLen = strlen(buf) + 1;
                sql->command = benchCalloc(1, bufLen, true);
                sql->delay_list = benchCalloc(
                        g_queryInfo.specifiedQueryInfo.queryTimes
                        * g_queryInfo.specifiedQueryInfo.concurrent,
                        sizeof(int64_t), true);
                TOOLS_STRNCPY(sql->command, buf, bufLen - 1);
                debugPrint("read file buffer: %s\n", sql->command);
                memset(buf, 0, TSDB_MAX_ALLOWED_SQL_LEN);
            }
            free(buf);
            fclose(fp);
        }
        // sqls
        tools_cJSON *specifiedSqls =
            tools_cJSON_GetObjectItem(specifiedQuery, "sqls");
        if (tools_cJSON_IsArray(specifiedSqls)) {
            int specifiedSqlSize = tools_cJSON_GetArraySize(specifiedSqls);
            for (int j = 0; j < specifiedSqlSize; ++j) {
                tools_cJSON *sqlObj =
                    tools_cJSON_GetArrayItem(specifiedSqls, j);
                if (tools_cJSON_IsObject(sqlObj)) {
                    SSQL * sql = benchCalloc(1, sizeof(SSQL), true);
                    benchArrayPush(g_queryInfo.specifiedQueryInfo.sqls, sql);
                    sql = benchArrayGet(g_queryInfo.specifiedQueryInfo.sqls,
                            g_queryInfo.specifiedQueryInfo.sqls->size -1);
                    sql->delay_list = benchCalloc(
                            g_queryInfo.specifiedQueryInfo.queryTimes
                            * g_queryInfo.specifiedQueryInfo.concurrent,
                            sizeof(int64_t), true);

                    tools_cJSON *sqlStr =
                        tools_cJSON_GetObjectItem(sqlObj, "sql");
                    if (tools_cJSON_IsString(sqlStr)) {
                        int strLen = strlen(sqlStr->valuestring) + 1;
                        sql->command = benchCalloc(1, strLen, true);
                        TOOLS_STRNCPY(sql->command, sqlStr->valuestring, strLen);
                        // default value is -1, which mean infinite loop
                        g_queryInfo.specifiedQueryInfo.endAfterConsume[j] = -1;
                        tools_cJSON *endAfterConsume =
                            tools_cJSON_GetObjectItem(specifiedQuery,
                                                      "endAfterConsume");
                        if (tools_cJSON_IsNumber(endAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.endAfterConsume[j] =
                                (int)endAfterConsume->valueint;
                        }
                        if (g_queryInfo.specifiedQueryInfo
                                .endAfterConsume[j] < -1) {
                            g_queryInfo.specifiedQueryInfo
                                .endAfterConsume[j] = -1;
                        }

                        g_queryInfo.specifiedQueryInfo
                                .resubAfterConsume[j] = -1;
                        tools_cJSON *resubAfterConsume =
                            tools_cJSON_GetObjectItem(
                                    specifiedQuery, "resubAfterConsume");
                        if (tools_cJSON_IsNumber(resubAfterConsume)) {
                            g_queryInfo.specifiedQueryInfo.resubAfterConsume[j]
                                = (int)resubAfterConsume->valueint;
                        }

                        if (g_queryInfo.specifiedQueryInfo
                                .resubAfterConsume[j] < -1)
                            g_queryInfo.specifiedQueryInfo
                                    .resubAfterConsume[j] = -1;

                        tools_cJSON *result =
                            tools_cJSON_GetObjectItem(sqlObj, "result");
                        if (tools_cJSON_IsString(result)) {
                            TOOLS_STRNCPY(sql->result, result->valuestring,
                                     MAX_FILE_NAME_LEN);
                        } else {
                            memset(sql->result, 0, MAX_FILE_NAME_LEN);
                        }
                    } else {
                        errorPrint("%s", "Invalid sql in json\n");
                        return -1;
                    }
                }
            }
        }
    }

    // succ
    return 0;
}

// Super Query
int32_t readSuperQueryJson(tools_cJSON * superQuery) {
    g_queryInfo.superQueryInfo.threadCnt = 1;
    if (!superQuery || superQuery->type != tools_cJSON_Object) {
        g_queryInfo.superQueryInfo.sqlCount = 0;
    } else {
        tools_cJSON *subrate =
            tools_cJSON_GetObjectItem(superQuery, "query_interval");
        if (subrate && subrate->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.queryInterval = subrate->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryInterval = 0;
        }

        tools_cJSON *superQueryTimes =
            tools_cJSON_GetObjectItem(superQuery, "query_times");
        if (superQueryTimes && superQueryTimes->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.queryTimes = superQueryTimes->valueint;
        } else {
            g_queryInfo.superQueryInfo.queryTimes = g_queryInfo.query_times;
        }

        tools_cJSON *concurrent =
            tools_cJSON_GetObjectItem(superQuery, "concurrent");
        if (concurrent && concurrent->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt =
                (uint32_t)concurrent->valueint;
        }

        tools_cJSON *threads = tools_cJSON_GetObjectItem(superQuery, "threads");
        if (threads && threads->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.threadCnt = (uint32_t)threads->valueint;
        }

        tools_cJSON *stblname =
            tools_cJSON_GetObjectItem(superQuery, "stblname");
        if (stblname && stblname->type == tools_cJSON_String
                && stblname->valuestring != NULL) {
            TOOLS_STRNCPY(g_queryInfo.superQueryInfo.stbName,
                     stblname->valuestring,
                     TSDB_TABLE_NAME_LEN);
        }

        tools_cJSON *superAsyncMode =
            tools_cJSON_GetObjectItem(superQuery, "mode");
        if (superAsyncMode && superAsyncMode->type == tools_cJSON_String
                && superAsyncMode->valuestring != NULL) {
            if (0 == strcmp("async", superAsyncMode->valuestring)) {
                g_queryInfo.superQueryInfo.asyncMode = ASYNC_MODE;
            } else {
                g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
            }
        } else {
            g_queryInfo.superQueryInfo.asyncMode = SYNC_MODE;
        }

        tools_cJSON *superInterval =
            tools_cJSON_GetObjectItem(superQuery, "interval");
        if (superInterval && superInterval->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.subscribeInterval =
                superInterval->valueint;
        } else {
            g_queryInfo.superQueryInfo.subscribeInterval =
                DEFAULT_QUERY_INTERVAL;
        }

        tools_cJSON *subrestart =
            tools_cJSON_GetObjectItem(superQuery, "restart");
        if (subrestart && subrestart->type == tools_cJSON_String
                && subrestart->valuestring != NULL) {
            if (0 == strcmp("no", subrestart->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeRestart = false;
            } else {
                g_queryInfo.superQueryInfo.subscribeRestart = true;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeRestart = true;
        }

        tools_cJSON *superkeepProgress =
            tools_cJSON_GetObjectItem(superQuery, "keepProgress");
        if (superkeepProgress && superkeepProgress->type == tools_cJSON_String
            && superkeepProgress->valuestring != NULL) {
            if (0 == strcmp("yes", superkeepProgress->valuestring)) {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 1;
            } else {
                g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
            }
        } else {
            g_queryInfo.superQueryInfo.subscribeKeepProgress = 0;
        }

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.endAfterConsume = -1;
        tools_cJSON *superEndAfterConsume =
            tools_cJSON_GetObjectItem(superQuery, "endAfterConsume");
        if (superEndAfterConsume &&
            superEndAfterConsume->type == tools_cJSON_Number) {
            g_queryInfo.superQueryInfo.endAfterConsume =
                (int)superEndAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.endAfterConsume < -1)
            g_queryInfo.superQueryInfo.endAfterConsume = -1;

        // default value is -1, which mean do not resub
        g_queryInfo.superQueryInfo.resubAfterConsume = -1;
        tools_cJSON *superResubAfterConsume =
            tools_cJSON_GetObjectItem(superQuery, "resubAfterConsume");
        if ((superResubAfterConsume) &&
            (superResubAfterConsume->type == tools_cJSON_Number) &&
            (superResubAfterConsume->valueint >= 0)) {
            g_queryInfo.superQueryInfo.resubAfterConsume =
                (int)superResubAfterConsume->valueint;
        }
        if (g_queryInfo.superQueryInfo.resubAfterConsume < -1)
            g_queryInfo.superQueryInfo.resubAfterConsume = -1;

        // supert table sqls
        tools_cJSON *superSqls = tools_cJSON_GetObjectItem(superQuery, "sqls");
        if (!superSqls || superSqls->type != tools_cJSON_Array) {
            g_queryInfo.superQueryInfo.sqlCount = 0;
        } else {
            int superSqlSize = tools_cJSON_GetArraySize(superSqls);
            if (superSqlSize > MAX_QUERY_SQL_COUNT) {
                errorPrint(
                    "failed to read json, query sql size overflow, max is %d\n",
                    MAX_QUERY_SQL_COUNT);
                return -1;
            }

            g_queryInfo.superQueryInfo.sqlCount = superSqlSize;
            for (int j = 0; j < superSqlSize; ++j) {
                tools_cJSON *sql = tools_cJSON_GetArrayItem(superSqls, j);
                if (sql == NULL) continue;

                tools_cJSON *sqlStr = tools_cJSON_GetObjectItem(sql, "sql");
                if (sqlStr && sqlStr->type == tools_cJSON_String) {
                    TOOLS_STRNCPY(g_queryInfo.superQueryInfo.sql[j],
                             sqlStr->valuestring, TSDB_MAX_ALLOWED_SQL_LEN);
                }

                tools_cJSON *result = tools_cJSON_GetObjectItem(sql, "result");
                if (result != NULL && result->type == tools_cJSON_String
                    && result->valuestring != NULL) {
                    TOOLS_STRNCPY(g_queryInfo.superQueryInfo.result[j],
                             result->valuestring, MAX_FILE_NAME_LEN);
                } else {
                    memset(g_queryInfo.superQueryInfo.result[j], 0,
                           MAX_FILE_NAME_LEN);
                }
            }
        }
    }
    
    // succ
    return 0;
}

// read query json 
static int getMetaFromQueryJsonFile(tools_cJSON *json) {
    int32_t code = -1;

    // read common
    tools_cJSON *telnet_tcp_port =
        tools_cJSON_GetObjectItem(json, "telnet_tcp_port");
    if (tools_cJSON_IsNumber(telnet_tcp_port)) {
        g_arguments->telnet_tcp_port = (uint16_t)telnet_tcp_port->valueint;
    }

    tools_cJSON *gQueryTimes = tools_cJSON_GetObjectItem(json, "query_times");
    if (tools_cJSON_IsNumber(gQueryTimes)) {
        g_queryInfo.query_times = gQueryTimes->valueint;
    } else {
        g_queryInfo.query_times = 1;
    }

    tools_cJSON *gKillSlowQueryThreshold =
        tools_cJSON_GetObjectItem(json, "kill_slow_query_threshold");
    if (tools_cJSON_IsNumber(gKillSlowQueryThreshold)) {
        g_queryInfo.killQueryThreshold = gKillSlowQueryThreshold->valueint;
    } else {
        g_queryInfo.killQueryThreshold = 0;
    }

    tools_cJSON *gKillSlowQueryInterval =
        tools_cJSON_GetObjectItem(json, "kill_slow_query_interval");
    if (tools_cJSON_IsNumber(gKillSlowQueryInterval)) {
        g_queryInfo.killQueryInterval = gKillSlowQueryInterval->valueint;
    } else {
        g_queryInfo.killQueryInterval = 1;  /* by default, interval 1s */
    }

    tools_cJSON *resetCache =
        tools_cJSON_GetObjectItem(json, "reset_query_cache");
    if (tools_cJSON_IsString(resetCache)) {
        if (0 == strcasecmp(resetCache->valuestring, "yes")) {
            g_queryInfo.reset_query_cache = true;
        }
    } else {
        g_queryInfo.reset_query_cache = false;
    }

    tools_cJSON *respBuffer =
        tools_cJSON_GetObjectItem(json, "response_buffer");
    if (tools_cJSON_IsNumber(respBuffer)) {
        g_queryInfo.response_buffer = respBuffer->valueint;
    } else {
        g_queryInfo.response_buffer = RESP_BUF_LEN;
    }

    tools_cJSON *dbs = tools_cJSON_GetObjectItem(json, "databases");
    if (tools_cJSON_IsString(dbs)) {
        g_queryInfo.dbName = dbs->valuestring;
    }

    tools_cJSON *queryMode = tools_cJSON_GetObjectItem(json, "query_mode");
    if (tools_cJSON_IsString(queryMode)) {
        if (0 == strcasecmp(queryMode->valuestring, "rest")) {
            g_queryInfo.iface = REST_IFACE;
        } else if (0 == strcasecmp(queryMode->valuestring, "taosc")) {
            g_queryInfo.iface = TAOSC_IFACE;
        } else {
            errorPrint("Invalid query_mode value: %s\n",
                       queryMode->valuestring);
            return -1;
        }
    }

    tools_cJSON *resultJsonFile = tools_cJSON_GetObjectItem(json, "result_json_file");
    if (resultJsonFile && resultJsonFile->type == tools_cJSON_String
            && resultJsonFile->valuestring != NULL) {
        g_arguments->output_json_file = resultJsonFile->valuestring;
        if (check_write_permission(g_arguments->output_json_file)) {
            errorPrint("json file %s does not have write permission.\n", g_arguments->output_json_file);
            return -1;
        }
    }

    // init sqls
    g_queryInfo.specifiedQueryInfo.sqls = benchArrayInit(1, sizeof(SSQL));

    // specified_table_query
    tools_cJSON *specifiedQuery = tools_cJSON_GetObjectItem(json, "specified_table_query");
    if (specifiedQuery) {
        code = readSpecQueryJson(specifiedQuery);
        if(code) {
            errorPrint("failed to readSpecQueryJson code=%d \n", code);
            return code; 
        }
    }

    // super_table_query
    tools_cJSON *superQuery = tools_cJSON_GetObjectItem(json, "super_table_query");
    if (superQuery) {
        code = readSuperQueryJson(superQuery);
        if(code) {
            errorPrint("failed to readSuperQueryJson code=%d \n", code);
            return code; 
        }
    }

    // only have one
    const char* errType = "json config invalid:";
    if (specifiedQuery && superQuery) {
        errorPrint("%s only appear one for 'specified_table_query' and 'super_table_query' \n", errType);
        return -1;
    }

    // must have one
    if (specifiedQuery == NULL && superQuery == NULL ) {
        errorPrint("%s must have one for 'specified_table_query' or 'super_table_query' \n", errType);
        return -1;
    }

    // succ
    return 0;
}

static int getMetaFromTmqJsonFile(tools_cJSON *json) {
    int32_t code = -1;
    tools_cJSON *resultfile = tools_cJSON_GetObjectItem(json, "result_file");
    if (resultfile && resultfile->type == tools_cJSON_String
            && resultfile->valuestring != NULL) {
        g_arguments->output_file = resultfile->valuestring;
    }

    tools_cJSON *resultJsonFile = tools_cJSON_GetObjectItem(json, "result_json_file");
    if (resultJsonFile && resultJsonFile->type == tools_cJSON_String
            && resultJsonFile->valuestring != NULL) {
        g_arguments->output_json_file = resultJsonFile->valuestring;
        if (check_write_permission(g_arguments->output_json_file)) {
            errorPrint("json file %s does not have write permission.\n", g_arguments->output_json_file);
            goto TMQ_PARSE_OVER;
        }
    }

    tools_cJSON *answerPrompt =
        tools_cJSON_GetObjectItem(json,
                                  "confirm_parameter_prompt");  // yes, no,
    if (tools_cJSON_IsString(answerPrompt)) {
        if (0 == strcasecmp(answerPrompt->valuestring, "no")) {
            g_arguments->answer_yes = true;
        }
    }

    // consumer info
    tools_cJSON *tmqInfo = tools_cJSON_GetObjectItem(json, "tmq_info");
    g_tmqInfo.consumerInfo.concurrent = 1;

    tools_cJSON *concurrent = tools_cJSON_GetObjectItem(tmqInfo, "concurrent");
    if (tools_cJSON_IsNumber(concurrent)) {
        g_tmqInfo.consumerInfo.concurrent = (uint32_t)concurrent->valueint;
    }

    // sequential,	parallel
    tools_cJSON *createMode = tools_cJSON_GetObjectItem(tmqInfo, "create_mode");
    if (tools_cJSON_IsString(createMode)) {
        g_tmqInfo.consumerInfo.createMode = createMode->valuestring;
    }

    // share, independent
    tools_cJSON *groupMode = tools_cJSON_GetObjectItem(tmqInfo, "group_mode");
    if (tools_cJSON_IsString(groupMode)) {
        g_tmqInfo.consumerInfo.groupMode = groupMode->valuestring;
    }


    tools_cJSON *pollDelay = tools_cJSON_GetObjectItem(tmqInfo, "poll_delay");
    if (tools_cJSON_IsNumber(pollDelay)) {
        g_tmqInfo.consumerInfo.pollDelay = (uint32_t)pollDelay->valueint;
    }

    tools_cJSON *autoCommitInterval = tools_cJSON_GetObjectItem(
            tmqInfo, "auto.commit.interval.ms");
    if (tools_cJSON_IsNumber(autoCommitInterval)) {
        g_tmqInfo.consumerInfo.autoCommitIntervalMs =
            (uint32_t)autoCommitInterval->valueint;
    }

    tools_cJSON *groupId = tools_cJSON_GetObjectItem(tmqInfo, "group.id");
    if (tools_cJSON_IsString(groupId)) {
        g_tmqInfo.consumerInfo.groupId = groupId->valuestring;
    }

    tools_cJSON *clientId = tools_cJSON_GetObjectItem(tmqInfo, "client.id");
    if (tools_cJSON_IsString(clientId)) {
        g_tmqInfo.consumerInfo.clientId = clientId->valuestring;
    }

    tools_cJSON *autoOffsetReset = tools_cJSON_GetObjectItem(
            tmqInfo, "auto.offset.reset");
    if (tools_cJSON_IsString(autoOffsetReset)) {
        g_tmqInfo.consumerInfo.autoOffsetReset = autoOffsetReset->valuestring;
    }

    tools_cJSON *enableAutoCommit = tools_cJSON_GetObjectItem(
            tmqInfo, "enable.auto.commit");
    if (tools_cJSON_IsString(enableAutoCommit)) {
        g_tmqInfo.consumerInfo.enableAutoCommit = enableAutoCommit->valuestring;
    }

    tools_cJSON *enableManualCommit = tools_cJSON_GetObjectItem(
            tmqInfo, "enable.manual.commit");
    if (tools_cJSON_IsString(enableManualCommit)) {
        g_tmqInfo.consumerInfo.enableManualCommit =
            enableManualCommit->valuestring;
    }

    tools_cJSON *snapshotEnable = tools_cJSON_GetObjectItem(
            tmqInfo, "experimental.snapshot.enable");
    if (tools_cJSON_IsString(snapshotEnable)) {
        g_tmqInfo.consumerInfo.snapshotEnable = snapshotEnable->valuestring;
    }

    tools_cJSON *msgWithTableName = tools_cJSON_GetObjectItem(
            tmqInfo, "msg.with.table.name");
    if (tools_cJSON_IsString(msgWithTableName)) {
        g_tmqInfo.consumerInfo.msgWithTableName = msgWithTableName->valuestring;
    }

    tools_cJSON *rowsFile = tools_cJSON_GetObjectItem(tmqInfo, "rows_file");
    if (tools_cJSON_IsString(rowsFile)) {
        g_tmqInfo.consumerInfo.rowsFile = rowsFile->valuestring;
    }

    g_tmqInfo.consumerInfo.expectRows = -1;
    tools_cJSON *expectRows = tools_cJSON_GetObjectItem(tmqInfo, "expect_rows");
    if (tools_cJSON_IsNumber(expectRows)) {
        g_tmqInfo.consumerInfo.expectRows = (uint32_t)expectRows->valueint;
    }

    tools_cJSON *topicList = tools_cJSON_GetObjectItem(tmqInfo, "topic_list");
    if (tools_cJSON_IsArray(topicList)) {
        int topicCount = tools_cJSON_GetArraySize(topicList);
        for (int j = 0; j < topicCount; ++j) {
            tools_cJSON *topicObj = tools_cJSON_GetArrayItem(topicList, j);
            if (tools_cJSON_IsObject(topicObj)) {
                tools_cJSON *topicName = tools_cJSON_GetObjectItem(
                        topicObj, "name");
                if (tools_cJSON_IsString(topicName)) {
                    //  int strLen = strlen(topicName->valuestring) + 1;
                    TOOLS_STRNCPY(g_tmqInfo.consumerInfo.topicName[
                                g_tmqInfo.consumerInfo.topicCount],
                             topicName->valuestring, 255);

                } else {
                    errorPrint("%s", "Invalid topic name in json\n");
                    goto TMQ_PARSE_OVER;
                }

                tools_cJSON *sqlString = tools_cJSON_GetObjectItem(
                        topicObj, "sql");
                if (tools_cJSON_IsString(sqlString)) {
                    //  int strLen = strlen(sqlString->valuestring) + 1;
                    TOOLS_STRNCPY(g_tmqInfo.consumerInfo.topicSql[
                                g_tmqInfo.consumerInfo.topicCount],
                             sqlString->valuestring, 255);

                } else {
                    errorPrint("%s", "Invalid topic sql in json\n");
                    goto TMQ_PARSE_OVER;
                }
                g_tmqInfo.consumerInfo.topicCount++;
            }
        }
    }
    code = 0;
TMQ_PARSE_OVER:
    return code;
}

int readJsonConfig(char * file) {
    int32_t code = -1;
    FILE *  fp = fopen(file, "r");
    if (!fp) {
        errorPrint("failed to read %s, reason:%s\n", file,
                   strerror(errno));
        return code;
    }

    int   maxLen = MAX_JSON_BUFF;
    char *content = benchCalloc(1, maxLen + 1, false);
    int   len = (int)fread(content, 1, maxLen, fp);
    if (len <= 0) {
        errorPrint("failed to read %s, content is null", file);
        goto PARSE_OVER;
    }

    content[len] = 0;
    root = tools_cJSON_Parse(content);
    if (root == NULL) {
        errorPrint("failed to cjson parse %s, invalid json format\n",
                   file);
        goto PARSE_OVER;
    }

    char *pstr = tools_cJSON_Print(root);
    infoPrint("%s\n%s\n", file, pstr);
    tmfree(pstr);

    tools_cJSON *filetype = tools_cJSON_GetObjectItem(root, "filetype");
    if (tools_cJSON_IsString(filetype)) {
        if (0 == strcasecmp("insert", filetype->valuestring)) {
            g_arguments->test_mode = INSERT_TEST;
        } else if (0 == strcasecmp("query", filetype->valuestring)) {
            g_arguments->test_mode = QUERY_TEST;
        } else if (0 == strcasecmp("subscribe", filetype->valuestring)) {
            g_arguments->test_mode = SUBSCRIBE_TEST;
        } else if (0 == strcasecmp("csvfile", filetype->valuestring)) {
            g_arguments->test_mode = CSVFILE_TEST;
        } else {
            errorPrint("%s",
                       "failed to read json, filetype not support\n");
            goto PARSE_OVER;
        }
    } else {
        g_arguments->test_mode = INSERT_TEST;
    }

    // read common item
    code = getMetaFromCommonJsonFile(root);
    if (INSERT_TEST == g_arguments->test_mode || CSVFILE_TEST == g_arguments->test_mode) {
        // insert 
        code = getMetaFromInsertJsonFile(root);
    } else if (QUERY_TEST == g_arguments->test_mode) {
        // query
        memset(&g_queryInfo, 0, sizeof(SQueryMetaInfo));
        code = getMetaFromQueryJsonFile(root);
    } else if (SUBSCRIBE_TEST == g_arguments->test_mode) {
        // subscribe
        memset(&g_tmqInfo, 0, sizeof(STmqMetaInfo));
        code = getMetaFromTmqJsonFile(root);
    }

PARSE_OVER:
    free(content);
    fclose(fp);
    return code;
}
